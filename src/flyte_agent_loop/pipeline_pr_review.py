"""Pipeline 2 — address review comments on agent PRs. Runs every 15 minutes.

Flow (each stage grouped via ``flyte.group`` so its agent/tool sub-actions are
chunked together in the UI):

1. ``claim`` — find open PRs authored by the agent and call *dibs* on the first
   claimable one. A PR already approved (LGTM) whose claim is still held and has
   no new human comment is skipped, so an approved PR is not re-reviewed every run
   — it is re-examined only once the claim's TTL expires or a human comments.
2. ``review`` — a reviewer agent does its own code review of the PR's diff and
   designs scoped fixes for concrete problems it finds. Human review comments, if
   any, are used as additional guidance (not a prerequisite for reviewing).
3. ``verify`` — a verifier sub-agent checks the fixes are correct and aligned
   with the review (the agent's findings and any human comments).
4. ``push`` — only if verified, push the fixes to the PR head branch, then
   RELEASE the dibs so a later run can pick up additional follow-up comments.
5. Record the run in shared memory for the evals pipeline.

Any runtime error is caught at the top level: the claim (if held) is released so
a future run can retry, and an ``error`` RunRecord is returned instead of the
task crashing — so the agent loop recovers gracefully.
"""

from __future__ import annotations

import flyte
import flyte.report

from .agents import (
    build_reviewer_agent,
    build_verifier_agent,
    parse_plan,
    parse_verdict,
    render_plan_files,
)
from .common import iso, run_id, utcnow
from .config import Settings, load_settings
from . import dibs
from .environments import env
from .evals import RunRecord
from .github_client import GitHubClient, needs_lgtm
from .memory_context import read_shared_context, record_run
from .report_style import finalize_report, render_memory_tab
from .tools import PR_VERIFIER_TOOLS, push_changes_to_pr

TRIGGER = flyte.Trigger(
    name="pr_review_every_15m",
    automation=flyte.Cron("*/15 * * * *"),
    description="Address review comments on agent-authored PRs.",
)


@env.task(report=True, triggers=[TRIGGER])
async def pr_review() -> RunRecord:
    settings = load_settings()
    now = utcnow()
    rid = run_id()
    flyte.report.log(f"<h2>pr_review</h2><p>run <code>{rid}</code> on <b>{settings.repo}</b></p>")

    claimed: int | None = None
    try:
        context = await read_shared_context(settings)
        render_memory_tab(context)  # show the shared-memory context the agents run with

        # 1. Claim an agent-authored open PR.
        with flyte.group("claim"):
            target = _claim_agent_pr(settings, now)
        if target is None:
            return await _finish(settings, _record(rid, now, "no_work", summary="No claimable open agent PRs."))
        number = claimed = target["number"]
        flyte.report.log(f"<p>claimed PR #{number}: {target['title']}</p>")

        # 2. Reviewer designs fixes.
        with flyte.group("review"):
            reviewer = build_reviewer_agent(settings, context)
            result = await reviewer.run.aio(
                f"Review PR #{number} in repo {settings.repo} (title: {target['title']}) and fix any "
                f"concrete problems you find. Use any human review comments as additional guidance."
            )
        plan = parse_plan(result.summary)
        if plan.action == "no_changes" or not plan.has_changes:
            # The reviewer deems the PR good: post an approving "looks good" comment
            # (deduped so it isn't repeated every run) and HOLD the dibs claim (do not
            # release). While the claim is held and no new human comment arrives, later
            # runs skip re-reviewing it (see _claim_agent_pr).
            summary = plan.summary or "No actionable feedback; the changes look good."
            posted = _approve(settings, number, summary)
            flyte.report.log(
                f"<p>reviewed PR #{number}: looks good, holding claim"
                f"{' (comment posted)' if posted else ''}</p>"
            )
            return await _finish(settings, _record(rid, now, "no_work", number=number, summary=summary))
        if plan.error:
            _release(settings, number, now)
            return await _finish(settings, _record(rid, now, "error", number=number, summary=plan.error))

        # 3. Verifier checks alignment with comments AND correctness.
        addressed = plan.raw.get("addressed") or []
        with flyte.group("verify"):
            verdict = parse_verdict(await _verify(settings, number, plan, addressed))
        flyte.report.log(f"<p>verifier: {'PASS' if verdict.verified else 'FAIL'} — {verdict.notes}</p>")

        if not verdict.verified:
            _comment_and_release(
                settings, number, now,
                f"\U0001f916 flyte-agent-loop drafted fixes but the verifier flagged them: "
                f"{verdict.notes}\n\nReleasing for a follow-up run.",
            )
            return await _finish(
                settings,
                _record(
                    rid, now, "error", number=number, verified=False,
                    verifier_notes=verdict.notes, summary=f"Verification failed: {plan.summary}",
                ),
            )

        # 4. Push fixes, then release dibs for future follow-up comments.
        with flyte.group("push"):
            push = await push_changes_to_pr.aio(
                pr_number=number,
                files=plan.files,
                message=str(plan.raw.get("message") or f"Address review feedback on #{number}"),
            )
        _comment_and_release(
            settings, number, now,
            f"\U0001f916 flyte-agent-loop pushed fixes ({push['commit'][:7]}) addressing: "
            + "; ".join(str(a) for a in addressed),
        )
        flyte.report.log(f"<p>pushed {push['commit'][:7]} to {push['branch']} and released dibs</p>")
        return await _finish(
            settings,
            _record(
                rid, now, "pushed_fixes", number=number, verified=True,
                verifier_notes=verdict.notes, summary=plan.summary,
            ),
        )

    except Exception as exc:  # graceful recovery from any runtime error
        flyte.logger.exception("pr_review failed")
        flyte.report.log(f"<p style='color:#b00'>pipeline error: {exc}</p>")
        if claimed is not None:
            _safe_release(settings, claimed, now)
        return await _finish(
            settings,
            _record(rid, now, "error", number=claimed, summary=f"Pipeline error: {exc}", error=str(exc)),
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _record(
    rid: str, now, action: str, *, number: int | None = None,
    verified: bool = False, verifier_notes: str = "", summary: str = "", error: str = "",
) -> RunRecord:
    return RunRecord(
        pipeline="pr_review",
        run_id=rid,
        timestamp=iso(now),
        action=action,
        target_kind="pr" if number is not None else "",
        target_number=number,
        pr_number=number,
        verified=verified,
        verifier_notes=verifier_notes,
        summary=summary,
        error=error,
    )


def _is_approved_and_held(comments: list[dict], agent_id: str, bot_login: str, now) -> bool:
    """Whether an approved PR should be skipped this run.

    True when we still hold an active dibs claim on it AND there is no new human
    comment since our approval — so we don't re-review a PR we already approved
    until the claim's TTL expires or a human comments.
    """
    held = dibs.held_by_me(dibs.parse_markers(c["body"] for c in comments), "pr", agent_id, now)
    return held and not needs_lgtm(comments, bot_login)


def _claim_agent_pr(settings: Settings, now) -> dict | None:
    with GitHubClient(settings) as gh:
        author = gh.authenticated_login()
        for pr in gh.list_pull_requests(author=author):
            number = pr["number"]
            comments = gh.list_comments(number)
            if _is_approved_and_held(comments, settings.agent_id, author, now):
                flyte.report.log(f"<p>PR #{number} skipped: approved, holding claim, no new feedback</p>")
                continue
            claim = gh.try_claim(number, "pr", now=now)
            if claim.claimed:
                return pr
            flyte.report.log(f"<p>PR #{number} skipped: {claim.reason}</p>")
    return None


async def _verify(settings: Settings, number: int, plan, addressed) -> str:
    verifier = build_verifier_agent(settings, PR_VERIFIER_TOOLS)
    # The proposed fixes are NOT pushed yet, so the PR branch still has the old
    # files — the verifier reviews the in-memory contents below. It may read the
    # PR *comments* (via tools) to check the fixes are aligned with them.
    result = await verifier.run.aio(
        f"Objective: correctly improve PR #{number} via the reviewer's fixes.\n\n"
        f"The proposed fixes claim to address: {addressed}\n"
        f"Summary: {plan.summary}\n\n"
        f"IMPORTANT: the fixes below are NOT yet pushed, so DO NOT expect to find them on the PR "
        f"branch. Review the proposed file contents shown here directly. You may read the PR diff and "
        f"comments (via tools) for context.\n\n"
        f"Proposed changes ({len(plan.files)} file(s)):\n\n"
        f"{render_plan_files(plan.files)}\n\n"
        f"Note: a file may end with a '[... omitted from THIS PROMPT ...]' marker — that is only a "
        f"display length limit, NOT a truncated/incomplete file. Do not fail the work for it.\n\n"
        f"Verify the fixes are (a) correct and (b) consistent with the review (the problems the "
        f"reviewer identified and any human review comments)."
    )
    return result.summary


def _release(settings: Settings, number: int, now) -> None:
    with GitHubClient(settings) as gh:
        gh.release(number, "pr", now=now)


def _approve(settings: Settings, number: int, summary: str) -> bool:
    """Post a deduped 'looks good' approval and HOLD the dibs (no release).

    Holding the claim lets later runs skip re-reviewing this approved PR until the
    claim's TTL expires or a human comments. Returns whether a comment was posted.
    """
    with GitHubClient(settings) as gh:
        return gh.post_lgtm(number, summary)


def _comment_and_release(settings: Settings, number: int, now, body: str) -> None:
    with GitHubClient(settings) as gh:
        gh.add_comment(number, body)
        gh.release(number, "pr", now=now)


def _safe_release(settings: Settings, number: int, now) -> None:
    """Best-effort dibs release; never raises (used on the error path)."""
    try:
        _release(settings, number, now)
    except Exception:
        flyte.logger.warning(f"failed to release dibs on PR #{number}")


async def _finish(settings: Settings, record: RunRecord) -> RunRecord:
    # Return the RunRecord dataclass (not .to_dict()): Flyte serializes dataclass
    # outputs natively, including Optional/None fields. A ``dict[str, Any]`` output
    # would be pickled per-value, and the pickle transformer rejects None values.
    #
    # Persisting to memory + flushing the report are best-effort: a failure here
    # must not turn a completed run into a task crash.
    record.repo = settings.repo
    try:
        await record_run(settings, record)
    except Exception:
        flyte.logger.warning("failed to persist run record to memory")
    try:
        await finalize_report()
    except Exception:
        flyte.logger.warning("failed to flush report")
    return record

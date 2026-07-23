"""Pipeline 2 — address review comments on agent PRs. Runs every 5 minutes.

Flow (each stage grouped via ``flyte.group`` so its agent/tool sub-actions are
chunked together in the UI):

1. ``claim`` — find open PRs authored by the agent and call *dibs* on the first
   claimable one. **At most one PR is claimed and processed per run.** Once the
   reviewer has approved a PR (posted an LGTM), it is skipped **indefinitely** — no
   future run re-reviews it unless a human posts a ``/flyte-agent-loop <message>``
   command after the approval. The approval comment explains this.
2 + 3. ``review`` <-> ``verify`` loop — a reviewer agent does its own code review
   of the PR's diff and stages scoped fixes, then a verifier sub-agent checks them.
   If the verifier fails, its feedback and the prior staged fixes are fed back to
   the reviewer, which gets up to ``FLYTE_AGENT_MAX_TRIES`` (default 3) attempts.
   Human review comments are additional guidance, not a prerequisite for reviewing.
4. ``push`` — once verified, push the fixes to the PR head branch, then RELEASE
   the dibs so a later run can pick up additional follow-up comments. If all
   attempts are exhausted, post the verifier's feedback and release.
5. Record the run in shared memory for the distiller pipeline.

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
    parse_verdict,
    render_plan_files,
)
from .staging import ChangeStage, pr_reviewer_tools
from .common import iso, run_id, run_name, utcnow
from .config import Settings, load_settings
from .environments import env
from .evals import RunRecord
from .github_client import GitHubClient, approved_awaiting_command
from .memory_context import read_shared_context, record_run
from .report_style import finalize_report, install_live_report_flush, link, render_memory_tab
from .tools import PR_VERIFIER_TOOLS, push_changes_to_pr

TRIGGER = flyte.Trigger(
    name="reviewer_every_5m",
    automation=flyte.Cron("*/5 * * * *"),
    description="Address review comments on agent-authored PRs.",
)


@env.task(report=True, triggers=[TRIGGER])
async def reviewer() -> RunRecord:
    settings = load_settings()
    now = utcnow()
    rid = run_id()
    log = flyte.logger
    log.info("reviewer start: run=%s repo=%s model=%s max_tokens=%s",
             rid, settings.repo, settings.model, settings.max_tokens)
    flyte.report.log(f"<h2>reviewer</h2><p>run <code>{rid}</code> on <b>{settings.repo}</b></p>")

    claimed: int | None = None
    try:
        install_live_report_flush()  # flush the report after every agent event (live updates)
        context = await read_shared_context(settings)
        log.info("reviewer: loaded shared context (%d chars)", len(context))
        render_memory_tab(context)  # show the shared-memory context the agents run with

        # 1. Claim an agent-authored open PR.
        with flyte.group("claim"):
            target = _claim_agent_pr(settings, now)
        if target is None:
            log.info("reviewer: no claimable open agent PR; nothing to do")
            return await _finish(settings, _record(rid, now, "no_work", summary="No claimable open agent PRs."))
        number = claimed = target["number"]
        log.info("reviewer: claimed PR #%s: %s", number, target["title"])
        flyte.report.log(
            f"<p>claimed PR {link(target.get('url', ''), f'#{number}')}: {target['title']}</p>"
        )

        # 2 + 3. Review <-> verify loop. The reviewer gets up to settings.max_tries
        # attempts to satisfy the verifier; each retry feeds back the verifier's
        # feedback AND the prior staged fixes so it can revise rather than restart.
        plan = None
        verdict = None
        addressed: list = []
        attempt = 0
        while attempt < settings.max_tries:
            attempt += 1
            stage = ChangeStage(kind="pr")
            message = (
                _review_message(settings.repo, number, target)
                if plan is None
                else _retry_message(number, target, plan, verdict, attempt, settings.max_tries)
            )
            with flyte.group(f"review:attempt-{attempt}"):
                reviewer = build_reviewer_agent(settings, context, extra_tools=pr_reviewer_tools(stage))
                result = await reviewer.run.aio(message)
            plan = stage.to_plan()
            log.info(
                "reviewer: attempt %d/%d reviewer staged action=%s files=%d has_changes=%s error=%r",
                attempt, settings.max_tries, plan.action, len(plan.files), plan.has_changes, plan.error,
            )
            if plan.error:
                log.warning(
                    "reviewer: reviewer did not submit a decision for PR #%s (attempt %d): %s. "
                    "Final message: %s",
                    number, attempt, plan.error, (result.summary or "")[-800:],
                )
                _release(settings, number, now)
                return await _finish(
                    settings, _record(rid, now, "error", number=number, attempts=attempt, summary=plan.error)
                )
            if plan.action == "no_changes" or not plan.has_changes:
                # The reviewer deems the PR good: post a deduped "looks good" comment and
                # HOLD the dibs claim (no retry — this is a valid terminal outcome).
                summary = plan.summary or "No actionable feedback; the changes look good."
                log.info("reviewer: PR #%s looks good (no changes); approving", number)
                posted = _approve(settings, number, now, summary)
                flyte.report.log(
                    f"<p>reviewed PR {link(target.get('url', ''), f'#{number}')}: looks good — approved"
                    f"{' (comment posted)' if posted else ''}. Comment "
                    f"<code>/flyte-agent-loop &lt;message&gt;</code> to re-activate.</p>"
                )
                return await _finish(
                    settings, _record(rid, now, "no_work", number=number, attempts=attempt, summary=summary)
                )

            addressed = plan.raw.get("addressed") or []
            with flyte.group(f"verify:attempt-{attempt}"):
                verdict = parse_verdict(await _verify(settings, number, plan, addressed))
            log.info(
                "reviewer: attempt %d/%d verifier verified=%s notes=%s",
                attempt, settings.max_tries, verdict.verified, verdict.notes,
            )
            flyte.report.log(
                f"<p>attempt {attempt}/{settings.max_tries} verifier: "
                f"{'PASS' if verdict.verified else 'FAIL'} — {verdict.notes}</p>"
            )
            if verdict.verified:
                break
            log.info("reviewer: attempt %d/%d FAILED verification for PR #%s", attempt, settings.max_tries, number)

        if not verdict.verified:
            # Exhausted every attempt without passing verification.
            log.info("reviewer: exhausted %d attempt(s) for PR #%s; releasing", attempt, number)
            _comment_and_release(
                settings, number, now,
                f"\U0001f916 flyte-agent-loop drafted fixes {attempt} time(s) but the verifier still "
                f"flags them: {verdict.notes}\n\nReleasing for a follow-up run.",
            )
            return await _finish(
                settings,
                _record(
                    rid, now, "error", number=number, attempts=attempt, verified=False,
                    verifier_notes=verdict.notes,
                    summary=f"Verification failed after {attempt} attempt(s): {plan.summary}",
                ),
            )

        # 4. Verified: push fixes, then release dibs for future follow-up comments.
        log.info("reviewer: verification PASSED for PR #%s on attempt %d; pushing %d file(s)",
                 number, attempt, len(plan.files))
        with flyte.group("push"):
            push = await push_changes_to_pr.aio(
                pr_number=number,
                files=plan.files,
                message=str(plan.raw.get("message") or f"Address review feedback on #{number}"),
            )
        log.info("reviewer: pushed %s to %s on PR #%s", push["commit"][:7], push["branch"], number)
        _comment_and_release(
            settings, number, now,
            f"\U0001f916 flyte-agent-loop pushed fixes ({push['commit'][:7]}) addressing: "
            + "; ".join(str(a) for a in addressed),
        )
        flyte.report.log(
            f"<p>pushed {push['commit'][:7]} to PR {link(target.get('url', ''), f'#{number}')} "
            f"({push['branch']}) and released dibs</p>"
        )
        return await _finish(
            settings,
            _record(
                rid, now, "pushed_fixes", number=number, attempts=attempt, verified=True,
                verifier_notes=verdict.notes, summary=plan.summary,
            ),
        )

    except Exception as exc:  # graceful recovery from any runtime error
        flyte.logger.exception("reviewer failed")
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
    verified: bool = False, verifier_notes: str = "", attempts: int = 1,
    summary: str = "", error: str = "",
) -> RunRecord:
    return RunRecord(
        pipeline="reviewer",
        run_id=rid,
        timestamp=iso(now),
        action=action,
        target_kind="pr" if number is not None else "",
        target_number=number,
        pr_number=number,
        verified=verified,
        verifier_notes=verifier_notes,
        attempts=attempts,
        summary=summary,
        error=error,
    )


def _review_message(repo: str, number: int, target: dict) -> str:
    """The reviewer's first-attempt instruction."""
    return (
        f"Review PR #{number} in repo {repo} (title: {target['title']}) and fix any concrete "
        f"problems you find. Use any human review comments as additional guidance."
    )


def _retry_message(number: int, target: dict, prior_plan, verdict, attempt: int, max_tries: int) -> str:
    """Retry instruction: feed back the verifier's feedback + the prior staged fixes."""
    return (
        f"Your previous fixes for PR #{number} ({target['title']}) were REJECTED by the verifier. "
        f"This is attempt {attempt} of {max_tries}.\n\n"
        f"Verifier feedback (address ALL of it):\n{verdict.notes}\n\n"
        f"Your previous staged fixes are below. Revise them to fix the verifier's concerns, then "
        f"re-stage EVERY file you want changed (staging replaces the previous staging) and submit "
        f"again with submit_fix. Keep the parts that were correct; change only what's needed.\n\n"
        f"Prior summary: {prior_plan.summary}\n\n"
        f"Prior staged files ({len(prior_plan.files)}):\n\n{render_plan_files(prior_plan.files)}"
    )


def _claim_agent_pr(settings: Settings, now) -> dict | None:
    """Claim and return the FIRST eligible open agent PR (at most one per run).

    An already-approved PR is skipped indefinitely — future runs never re-review it
    unless a human posts a ``/flyte-agent-loop <message>`` command after the approval.
    """
    with GitHubClient(settings) as gh:
        author = gh.authenticated_login()
        for pr in gh.list_pull_requests(author=author):
            number = pr["number"]
            comments = gh.list_comments(number)
            if approved_awaiting_command(comments, author):
                flyte.report.log(
                    f"<p>PR #{number} skipped: approved — comment "
                    f"<code>/flyte-agent-loop &lt;message&gt;</code> to re-activate</p>"
                )
                continue
            claim = gh.try_claim(number, "pr", now=now)
            if claim.claimed:
                return pr  # stop at the first claim — one PR per run
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


def _approve(settings: Settings, number: int, now, summary: str) -> bool:
    """Post a deduped 'looks good' approval and release the dibs.

    Once approved, future runs skip the PR entirely (see ``approved_awaiting_command``)
    until a human posts a ``/flyte-agent-loop <message>`` command — no TTL involved,
    so the dibs claim is released here. Returns whether a comment was posted.
    """
    with GitHubClient(settings) as gh:
        posted = gh.post_lgtm(number, summary)
        gh.release(number, "pr", now=now)
    return posted


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
    record.run_name = run_name()
    try:
        await record_run(settings, record)
    except Exception:
        flyte.logger.warning("failed to persist run record to memory")
    try:
        await finalize_report()
    except Exception:
        flyte.logger.warning("failed to flush report")
    return record

"""Pipeline 1 — pick up a GitHub issue and open a PR. Runs every 5 minutes.

Flow (each stage grouped via ``flyte.group`` so its agent/tool sub-actions are
chunked together in the UI):

1. ``claim`` — pick the first open issue that (a) has no associated open PR yet
   and (b) is claimable, then call *dibs* on it so concurrent/future scheduled
   runs skip it.
2. ``build`` — a builder agent reads the issue + repo and designs an
   implementation (code + tests + example + docs) as a change plan.
3. ``verify`` — a verifier sub-agent checks the plan for correctness/completeness.
4. ``open_pr`` — only if verified, open a PR with the changes. Otherwise post the
   verifier's feedback to the issue and release the claim so it can be retried.
5. Record the run in shared memory for the evals pipeline.

Any runtime error is caught at the top level: the claim (if held) is released so
a future run can retry, and an ``error`` RunRecord is returned instead of the
task crashing — so the agent loop recovers gracefully.
"""

from __future__ import annotations

import flyte
import flyte.report

from .agents import (
    build_issue_agent,
    build_verifier_agent,
    parse_plan,
    parse_verdict,
    render_plan_files,
)
from .common import iso, run_id, utcnow
from .config import Settings, load_settings
from .environments import env
from .evals import RunRecord
from .github_client import GitHubClient
from .memory_context import read_shared_context, record_run
from .report_style import finalize_report, render_memory_tab
from .tools import open_pr_with_changes

TRIGGER = flyte.Trigger(
    name="issue_to_pr_every_5m",
    automation=flyte.Cron("*/5 * * * *"),
    description="Pick up an open GitHub issue and open a PR implementing it.",
)


@env.task(report=True, triggers=[TRIGGER])
async def issue_to_pr() -> RunRecord:
    settings = load_settings()
    now = utcnow()
    rid = run_id()
    flyte.report.log(f"<h2>issue_to_pr</h2><p>run <code>{rid}</code> on <b>{settings.repo}</b></p>")

    claimed: int | None = None
    try:
        context = await read_shared_context(settings)
        render_memory_tab(context)  # show the shared-memory context the agents run with

        # 1. Claim an open issue.
        with flyte.group("claim"):
            target = _claim_open_issue(settings, now)
        if target is None:
            return await _finish(settings, _record(rid, now, "no_work", summary="No claimable open issues."))
        number = claimed = target["number"]
        flyte.report.log(f"<p>claimed issue #{number}: {target['title']}</p>")

        # 2. Builder designs the change.
        with flyte.group("build"):
            builder = build_issue_agent(settings, context)
            result = await builder.run.aio(
                f"Implement GitHub issue #{number} in repo {settings.repo}. Title: {target['title']}"
            )
        plan = parse_plan(result.summary)
        if plan.error or not plan.has_changes:
            _release(settings, number, now)
            return await _finish(
                settings,
                _record(
                    rid, now, "no_work", number=number,
                    summary=plan.summary or plan.error or "Builder proposed no changes.",
                ),
            )

        # 3. Verifier checks the plan.
        with flyte.group("verify"):
            verifier = build_verifier_agent(settings)
            verdict = parse_verdict((await verifier.run.aio(_verify_prompt(number, target, plan))).summary)
        flyte.report.log(f"<p>verifier: {'PASS' if verdict.verified else 'FAIL'} — {verdict.notes}</p>")

        if not verdict.verified:
            _comment_and_release(
                settings, number, now,
                f"\U0001f916 flyte-agent-loop attempted this but the verifier flagged it: "
                f"{verdict.notes}\n\nReleasing for a follow-up run.",
            )
            return await _finish(
                settings,
                _record(
                    rid, now, "error", number=number, verified=False,
                    verifier_notes=verdict.notes, summary=f"Verification failed: {plan.summary}",
                ),
            )

        # 4. Apply only if verified.
        with flyte.group("open_pr"):
            branch = str(plan.raw.get("branch") or f"agent/issue-{number}")
            pr = await open_pr_with_changes.aio(
                issue_number=number,
                branch=branch,
                title=str(plan.raw.get("title") or target["title"]),
                body=str(plan.raw.get("body") or plan.summary),
                files=plan.files,
            )
        flyte.report.log(f"<p>opened PR <a href='{pr['url']}'>#{pr['number']}</a></p>")
        return await _finish(
            settings,
            _record(
                rid, now, "opened_pr", number=number, pr_number=pr["number"], pr_url=pr["url"],
                verified=True, verifier_notes=verdict.notes, summary=plan.summary,
            ),
        )

    except Exception as exc:  # graceful recovery from any runtime error
        flyte.logger.exception("issue_to_pr failed")
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
    rid: str, now, action: str, *, number: int | None = None, pr_number: int | None = None,
    pr_url: str = "", verified: bool = False, verifier_notes: str = "", summary: str = "", error: str = "",
) -> RunRecord:
    return RunRecord(
        pipeline="issue_to_pr",
        run_id=rid,
        timestamp=iso(now),
        action=action,
        target_kind="issue" if number is not None else "",
        target_number=number,
        pr_number=pr_number,
        pr_url=pr_url,
        verified=verified,
        verifier_notes=verifier_notes,
        summary=summary,
        error=error,
    )


def _verify_prompt(number: int, target: dict, plan) -> str:
    # The proposed changes are NOT committed yet and there is no PR — so the
    # verifier must review the in-memory file contents below, not the remote repo.
    body = (target.get("body") or "").strip()
    if len(body) > 4000:
        body = body[:4000] + "\n… [truncated]"
    description = f"\n\nIssue description:\n{body}" if body else ""
    return (
        f"Objective: correctly implement issue #{number} ({target['title']}) as described."
        f"{description}\n\n"
        f"Proposed summary: {plan.summary}\n\n"
        f"IMPORTANT: the changes below are the agent's proposed files. They are NOT yet committed "
        f"and there is no pull request yet, so DO NOT expect to find them in the repository. Review "
        f"the file contents shown here directly. (Repo-reading tools reflect only the current base "
        f"state, useful for checking how the change integrates.)\n\n"
        f"Proposed changes ({len(plan.files)} file(s)):\n\n"
        f"{render_plan_files(plan.files)}\n\n"
        f"Note: a file may end with a '[... omitted from THIS PROMPT ...]' marker — that is only a "
        f"display length limit, NOT a truncated/incomplete file. Do not fail the work for it.\n\n"
        f"Verify the changes correctly and completely satisfy what issue #{number} asks for. Judge "
        f"tests/examples/docs by what THIS issue warrants — require them only where the change calls "
        f"for them, not as a blanket checklist."
    )


def _claim_open_issue(settings: Settings, now) -> dict | None:
    with GitHubClient(settings) as gh:
        # Skip issues that already have an associated open PR — they're being (or
        # have been) implemented; a new run should not re-implement them.
        linked = gh.issues_with_open_prs()
        for issue in gh.list_open_issues():
            if issue["number"] in linked:
                flyte.report.log(f"<p>issue #{issue['number']} skipped: already has an open PR</p>")
                continue
            claim = gh.try_claim(issue["number"], "issue", now=now)
            if claim.claimed:
                return issue
            flyte.report.log(f"<p>issue #{issue['number']} skipped: {claim.reason}</p>")
    return None


def _release(settings: Settings, number: int, now) -> None:
    with GitHubClient(settings) as gh:
        gh.release(number, "issue", now=now)


def _comment_and_release(settings: Settings, number: int, now, body: str) -> None:
    with GitHubClient(settings) as gh:
        gh.add_comment(number, body)
        gh.release(number, "issue", now=now)


def _safe_release(settings: Settings, number: int, now) -> None:
    """Best-effort dibs release; never raises (used on the error path)."""
    try:
        _release(settings, number, now)
    except Exception:
        flyte.logger.warning(f"failed to release dibs on issue #{number}")


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

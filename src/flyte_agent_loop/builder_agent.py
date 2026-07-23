"""Pipeline 1 — pick up a GitHub issue and open a PR. Runs every 5 minutes.

Flow (each stage grouped via ``flyte.group`` so its agent/tool sub-actions are
chunked together in the UI):

1. ``claim`` — pick the first open issue that (a) has no associated open PR yet
   and (b) is claimable, then call *dibs* on it so concurrent/future scheduled
   runs skip it.
2 + 3. ``build`` <-> ``verify`` loop — a builder agent stages each file of its
   implementation via a ``stage_file`` tool, then a verifier sub-agent checks it.
   If the verifier fails, its feedback and the prior staged solution are fed back
   to the builder, which gets up to ``FLYTE_AGENT_MAX_TRIES`` (default 3) attempts
   to satisfy the verifier.
4. ``open_pr`` — once verified, open a PR with the changes. If all attempts are
   exhausted, post the verifier's feedback to the issue and release the claim.
5. Record the run in shared memory for the distiller pipeline.

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
    parse_verdict,
    render_plan_files,
)
from .staging import ChangeStage, issue_builder_tools
from .common import iso, run_id, run_name, utcnow
from .config import Settings, load_settings
from .environments import env
from .evals import RunRecord
from .github_client import GitHubClient
from .memory_context import read_shared_context, record_run
from .report_style import finalize_report, install_live_report_flush, link, render_memory_tab
from .tools import open_pr_with_changes

TRIGGER = flyte.Trigger(
    name="builder_every_5m",
    automation=flyte.Cron("*/5 * * * *"),
    description="Pick up an open GitHub issue and open a PR implementing it.",
)


@env.task(report=True, triggers=[TRIGGER])
async def builder() -> RunRecord:
    settings = load_settings()
    now = utcnow()
    rid = run_id()
    log = flyte.logger
    log.info("builder start: run=%s repo=%s model=%s max_tokens=%s",
             rid, settings.repo, settings.model, settings.max_tokens)
    flyte.report.log(f"<h2>builder</h2><p>run <code>{rid}</code> on <b>{settings.repo}</b></p>")

    claimed: int | None = None
    try:
        install_live_report_flush()  # flush the report after every agent event (live updates)
        context = await read_shared_context(settings)
        log.info("builder: loaded shared context (%d chars)", len(context))
        render_memory_tab(context)  # show the shared-memory context the agents run with

        # 1. Claim an open issue.
        with flyte.group("claim"):
            target = _claim_open_issue(settings, now)
        if target is None:
            log.info("builder: no claimable open issue; nothing to do")
            return await _finish(settings, _record(rid, now, "no_work", summary="No claimable open issues."))
        number = claimed = target["number"]
        log.info("builder: claimed issue #%s: %s", number, target["title"])
        flyte.report.log(
            f"<p>claimed issue {link(target.get('url', ''), f'#{number}')}: {target['title']}</p>"
        )

        # 2 + 3. Build <-> verify loop. The builder gets up to settings.max_tries
        # attempts to satisfy the verifier; each retry feeds back the verifier's
        # feedback AND the prior staged solution so it can revise rather than restart.
        plan = None
        verdict = None
        attempt = 0
        while attempt < settings.max_tries:
            attempt += 1
            stage = ChangeStage(kind="issue")
            message = (
                _build_message(settings.repo, number, target)
                if plan is None
                else _retry_message(number, target, plan, verdict, attempt, settings.max_tries)
            )
            with flyte.group(f"build:attempt-{attempt}"):
                builder = build_issue_agent(settings, context, extra_tools=issue_builder_tools(stage))
                result = await builder.run.aio(message)
            plan = stage.to_plan()
            log.info(
                "builder: attempt %d/%d staged action=%s files=%d has_changes=%s error=%r",
                attempt, settings.max_tries, plan.action, len(plan.files), plan.has_changes, plan.error,
            )
            if plan.error or not plan.has_changes:
                # Builder declined (skip) or produced nothing to verify — stop looping.
                if plan.error:
                    log.warning(
                        "builder: builder did not submit a change for issue #%s (attempt %d): %s. "
                        "Final message: %s",
                        number, attempt, plan.error, (result.summary or "")[-800:],
                    )
                else:
                    log.info("builder: builder proposed no changes for issue #%s: %s", number, plan.summary)
                _release(settings, number, now)
                return await _finish(
                    settings,
                    _record(
                        rid, now, "no_work", number=number, attempts=attempt,
                        summary=plan.summary or plan.error or "Builder proposed no changes.",
                    ),
                )

            with flyte.group(f"verify:attempt-{attempt}"):
                verifier = build_verifier_agent(settings)
                verdict = parse_verdict((await verifier.run.aio(_verify_prompt(number, target, plan))).summary)
            log.info(
                "builder: attempt %d/%d verifier verified=%s notes=%s",
                attempt, settings.max_tries, verdict.verified, verdict.notes,
            )
            flyte.report.log(
                f"<p>attempt {attempt}/{settings.max_tries} verifier: "
                f"{'PASS' if verdict.verified else 'FAIL'} — {verdict.notes}</p>"
            )
            if verdict.verified:
                break
            log.info("builder: attempt %d/%d FAILED verification for issue #%s", attempt, settings.max_tries, number)

        if not verdict.verified:
            # Exhausted every attempt without passing verification.
            log.info("builder: exhausted %d attempt(s) for issue #%s; releasing", attempt, number)
            _comment_and_release(
                settings, number, now,
                f"\U0001f916 flyte-agent-loop tried {attempt} time(s) but the verifier still flags it: "
                f"{verdict.notes}\n\nReleasing for a follow-up run.",
            )
            return await _finish(
                settings,
                _record(
                    rid, now, "error", number=number, attempts=attempt, verified=False,
                    verifier_notes=verdict.notes,
                    summary=f"Verification failed after {attempt} attempt(s): {plan.summary}",
                ),
            )

        # 4. Verified: open the PR.
        log.info("builder: verification PASSED for issue #%s on attempt %d; opening PR", number, attempt)
        with flyte.group("open_pr"):
            branch = str(plan.raw.get("branch") or f"agent/issue-{number}")
            pr = await open_pr_with_changes.aio(
                issue_number=number,
                branch=branch,
                title=str(plan.raw.get("title") or target["title"]),
                body=str(plan.raw.get("body") or plan.summary),
                files=plan.files,
            )
        log.info("builder: opened PR #%s (%s) for issue #%s", pr["number"], pr["url"], number)
        pr_link = link(pr["url"], f"#{pr['number']}")
        issue_link = link(target.get("url", ""), f"#{number}")
        flyte.report.log(f"<p>opened PR {pr_link} for issue {issue_link}</p>")
        return await _finish(
            settings,
            _record(
                rid, now, "opened_pr", number=number, attempts=attempt,
                pr_number=pr["number"], pr_url=pr["url"],
                verified=True, verifier_notes=verdict.notes, summary=plan.summary,
            ),
        )

    except Exception as exc:  # graceful recovery from any runtime error
        flyte.logger.exception("builder failed")
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
    pr_url: str = "", verified: bool = False, verifier_notes: str = "", attempts: int = 1,
    summary: str = "", error: str = "",
) -> RunRecord:
    return RunRecord(
        pipeline="builder",
        run_id=rid,
        timestamp=iso(now),
        action=action,
        target_kind="issue" if number is not None else "",
        target_number=number,
        pr_number=pr_number,
        pr_url=pr_url,
        verified=verified,
        verifier_notes=verifier_notes,
        attempts=attempts,
        summary=summary,
        error=error,
    )


def _build_message(repo: str, number: int, target: dict) -> str:
    """The builder's first-attempt instruction."""
    return f"Implement GitHub issue #{number} in repo {repo}. Title: {target['title']}"


def _retry_message(number: int, target: dict, prior_plan, verdict, attempt: int, max_tries: int) -> str:
    """Retry instruction: feed back the verifier's feedback + the prior solution."""
    return (
        f"Your previous attempt to implement issue #{number} ({target['title']}) was REJECTED by the "
        f"verifier. This is attempt {attempt} of {max_tries}.\n\n"
        f"Verifier feedback (address ALL of it):\n{verdict.notes}\n\n"
        f"Your previous solution is below. Revise it to fix the verifier's concerns, then re-stage "
        f"EVERY file you want in the PR (staging replaces the previous staging) and submit again. "
        f"Keep the parts that were correct; change only what's needed.\n\n"
        f"Prior summary: {prior_plan.summary}\n\n"
        f"Prior staged files ({len(prior_plan.files)}):\n\n{render_plan_files(prior_plan.files)}"
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
    """Claim and return the FIRST eligible open issue (at most one per run)."""
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
                return issue  # stop at the first claim — one issue per run
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

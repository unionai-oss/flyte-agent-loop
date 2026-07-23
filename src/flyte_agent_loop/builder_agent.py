"""Pipeline 1 — pick up a GitHub issue and either open a PR or file issues. Every 5m.

Depending on what the claimed issue asks for, the builder produces ONE of two
outcomes: a **pull request** (when the issue wants code) or a set of **new issues**
(when the issue asks it to read a spec and break the work into separate, dependency-
linked issues). Both follow the same propose → verify → durable-write shape.

Flow (each stage grouped via ``flyte.group`` so its agent/tool sub-actions are
chunked together in the UI):

1. ``claim`` — pick the first open issue that (a) has no associated open PR yet,
   (b) has no unresolved upstream dependency (issues can declare ``depends on #N`` /
   the ``flyte-agent-loop:depends-on`` marker; an issue is skipped while any upstream
   is still open), and (c) is claimable, then call *dibs* on it so concurrent/future
   scheduled runs skip it.
2 + 3. ``build`` <-> ``verify`` loop — the builder agent stages a proposal with
   read-only tools: either files (``stage_file`` → ``submit_implementation``) or
   sub-issues (``stage_issue`` → ``submit_decomposition``). A verifier sub-agent
   checks it; on failure its feedback and the prior proposal are fed back, up to
   ``FLYTE_AGENT_MAX_TRIES`` (default 3) attempts.
4. Apply, once verified — ``open_pr`` commits the files and opens a PR, OR
   ``open_issues`` durably creates the sub-issues (wiring their dependencies) and
   closes the spec. If all attempts are exhausted, post the verifier's feedback to the
   issue and release the claim.
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
from .github_client import GitHubClient, blocking_dependencies
from .memory_context import read_shared_context, record_run
from .report_style import finalize_report, install_live_report_flush, link, render_memory_tab
from .tools import open_issues_from_decomposition, open_pr_with_changes

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
                "builder: attempt %d/%d staged action=%s files=%d issues=%d has_work=%s error=%r",
                attempt, settings.max_tries, plan.action, len(plan.files), len(plan.issues),
                plan.has_work, plan.error,
            )
            if plan.error or not plan.has_work:
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
                verify_prompt = (
                    _verify_decomposition_prompt(number, target, plan)
                    if plan.action == "decompose"
                    else _verify_prompt(number, target, plan)
                )
                verdict = parse_verdict((await verifier.run.aio(verify_prompt)).summary)
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

        # 4. Verified. Apply the plan durably — the two outcomes the pipeline supports:
        #    a code change becomes a PR; a spec decomposition becomes new issues.
        issue_link = link(target.get("url", ""), f"#{number}")

        if plan.action == "decompose":
            log.info("builder: verification PASSED for spec #%s on attempt %d; filing %d issue(s)",
                     number, attempt, len(plan.issues))
            with flyte.group("open_issues"):
                result = await open_issues_from_decomposition.aio(spec_number=number, issues=plan.issues)
            created = result.get("created", [])
            log.info("builder: filed %d issue(s) from spec #%s and closed it", len(created), number)
            links = ", ".join(link(c["url"], f"#{c['number']}") for c in created) or "(none)"
            flyte.report.log(f"<p>decomposed spec {issue_link} into {links} (spec closed)</p>")
            return await _finish(
                settings,
                _record(
                    rid, now, "opened_issues", number=number, attempts=attempt,
                    verified=True, verifier_notes=verdict.notes,
                    summary=f"Filed {len(created)} issue(s) from spec #{number}: "
                            f"{', '.join('#' + str(c['number']) for c in created)}".strip(),
                ),
            )

        # Code change → open the PR.
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


def _render_staged_issues(issues: list[dict]) -> str:
    """Human-readable rendering of the builder's staged sub-issue breakdown."""
    if not issues:
        return "(no issues staged)"
    lines = []
    for i in issues:
        deps = ", ".join(i.get("depends_on", []) or []) or "none"
        body = (i.get("body") or "").strip()
        if len(body) > 600:
            body = body[:600] + " …[truncated]"
        lines.append(f"- [{i.get('key')}] {i.get('title')} (depends_on: {deps})\n  {body}")
    return "\n".join(lines)


def _retry_message(number: int, target: dict, prior_plan, verdict, attempt: int, max_tries: int) -> str:
    """Retry instruction: feed back the verifier's feedback + the prior solution."""
    if prior_plan.action == "decompose":
        return (
            f"Your previous decomposition of spec issue #{number} ({target['title']}) was REJECTED by "
            f"the verifier. This is attempt {attempt} of {max_tries}.\n\n"
            f"Verifier feedback (address ALL of it):\n{verdict.notes}\n\n"
            f"Your previous breakdown is below. Revise it, then re-stage EVERY sub-issue you want "
            f"(staging replaces the previous staging) via stage_issue and call submit_decomposition "
            f"again. Keep the parts that were right; change only what's needed.\n\n"
            f"Prior summary: {prior_plan.summary}\n\n"
            f"Prior staged issues ({len(prior_plan.issues)}):\n{_render_staged_issues(prior_plan.issues)}"
        )
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


def _verify_decomposition_prompt(number: int, target: dict, plan) -> str:
    """Ask the verifier to review a staged spec decomposition (no code, no PR)."""
    body = (target.get("body") or "").strip()
    if len(body) > 4000:
        body = body[:4000] + "\n… [truncated]"
    description = f"\n\nSpec issue description:\n{body}" if body else ""
    return (
        f"Objective: correctly decompose spec issue #{number} ({target['title']}) into well-scoped "
        f"sub-issues.{description}\n\n"
        f"Proposed summary: {plan.summary}\n\n"
        f"The agent proposes filing these sub-issues (NOT yet created). Each has a local key and the "
        f"keys of the siblings it depends on:\n\n{_render_staged_issues(plan.issues)}\n\n"
        f"You may read the referenced spec/repo files to check coverage. Verify that the breakdown: "
        f"(1) covers the spec's work without large gaps or duplication, (2) is split into independently "
        f"workable, well-scoped issues, and (3) has sensible, acyclic dependencies (an item depends on "
        f"another only when it genuinely needs it first). Do not require more granularity than the spec "
        f"warrants."
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
    """Claim and return the FIRST eligible open issue (at most one per run).

    An issue is skipped when it already has an associated open PR, or when it declares
    a dependency on an upstream issue that is still open (unresolved). Issues with no
    dependencies — or whose upstreams are all closed — are eligible.
    """
    with GitHubClient(settings) as gh:
        open_issues = gh.list_open_issues()
        open_numbers = {i["number"] for i in open_issues}
        linked = gh.issues_with_open_prs()
        for issue in open_issues:
            number = issue["number"]
            if number in linked:
                flyte.report.log(f"<p>issue #{number} skipped: already has an open PR</p>")
                continue
            blocking = blocking_dependencies(issue["body"], number, open_numbers)
            if blocking:
                refs = ", ".join(f"#{n}" for n in sorted(blocking))
                flyte.report.log(f"<p>issue #{number} skipped: blocked by unresolved {refs}</p>")
                continue
            claim = gh.try_claim(number, "issue", now=now)
            if claim.claimed:
                return issue  # stop at the first claim — one issue per run
            flyte.report.log(f"<p>issue #{number} skipped: {claim.reason}</p>")
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

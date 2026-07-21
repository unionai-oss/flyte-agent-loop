"""Pipeline 1 — pick up a GitHub issue and open a PR. Runs every 5 minutes.

Flow:

1. Call *dibs* on the first claimable open issue (posts a marker comment) so
   concurrent/future scheduled runs skip it.
2. A builder agent reads the issue + repo and designs an implementation
   (code + tests + example + docs) as a change plan.
3. A verifier sub-agent checks the plan for correctness and completeness.
4. Only if verified, open a PR with the changes. Otherwise post the verifier's
   feedback to the issue and release the claim so it can be retried.
5. Record the run in shared memory for the evals pipeline.
"""

from __future__ import annotations

from typing import Any

import flyte
import flyte.report

from .agents import build_issue_agent, build_verifier_agent, parse_plan, parse_verdict
from .common import iso, run_id, utcnow
from .config import load_settings
from .environments import env
from .evals import RunRecord
from .github_client import GitHubClient
from .memory_context import read_shared_context, record_run
from .tools import open_pr_with_changes

TRIGGER = flyte.Trigger(
    name="issue_to_pr_every_5m",
    automation=flyte.Cron("*/5 * * * *"),
    description="Pick up an open GitHub issue and open a PR implementing it.",
)


@env.task(report=True, triggers=[TRIGGER])
async def issue_to_pr() -> dict[str, Any]:
    settings = load_settings()
    now = utcnow()
    rid = run_id()
    context = await read_shared_context(settings)

    flyte.report.log(f"<h2>issue_to_pr</h2><p>run <code>{rid}</code> on <b>{settings.repo}</b></p>")

    with GitHubClient(settings) as gh:
        target = None
        for issue in gh.list_open_issues():
            claim = gh.try_claim(issue["number"], "issue", now=now)
            if claim.claimed:
                target = issue
                break
            flyte.report.log(f"<p>issue #{issue['number']} skipped: {claim.reason}</p>")

        if target is None:
            return await _finish(
                settings,
                RunRecord(
                    pipeline="issue_to_pr",
                    run_id=rid,
                    timestamp=iso(now),
                    action="no_work",
                    summary="No claimable open issues.",
                ),
            )

        number = target["number"]
        flyte.report.log(f"<p>claimed issue #{number}: {target['title']}</p>")

        # 2. Builder designs the change.
        builder = build_issue_agent(settings, context)
        result = await builder.run.aio(
            f"Implement GitHub issue #{number} in repo {settings.repo}. "
            f"Title: {target['title']}"
        )
        plan = parse_plan(result.summary)
        if plan.error or not plan.has_changes:
            gh.release(number, "issue", now=now)
            return await _finish(
                settings,
                RunRecord(
                    pipeline="issue_to_pr",
                    run_id=rid,
                    timestamp=iso(now),
                    action="no_work",
                    target_kind="issue",
                    target_number=number,
                    summary=plan.summary or plan.error or "Builder proposed no changes.",
                ),
            )

        # 3. Verifier checks the plan.
        verifier = build_verifier_agent(settings)
        verdict = parse_verdict(
            (
                await verifier.run.aio(
                    f"Objective: fully implement issue #{number} ({target['title']}) "
                    f"with implementation, tests, an example, and docs.\n"
                    f"Proposed summary: {plan.summary}\n"
                    f"Files to be written: {sorted(plan.files)}\n"
                    f"Verify correctness and completeness."
                )
            ).summary
        )
        flyte.report.log(f"<p>verifier: {'PASS' if verdict.verified else 'FAIL'} — {verdict.notes}</p>")

        # 4. Apply only if verified.
        if not verdict.verified:
            gh.add_comment(
                number,
                f"\U0001f916 flyte-agent-loop attempted this but the verifier flagged it: "
                f"{verdict.notes}\n\nReleasing for a follow-up run.",
            )
            gh.release(number, "issue", now=now)
            return await _finish(
                settings,
                RunRecord(
                    pipeline="issue_to_pr",
                    run_id=rid,
                    timestamp=iso(now),
                    action="error",
                    target_kind="issue",
                    target_number=number,
                    verified=False,
                    verifier_notes=verdict.notes,
                    summary=f"Verification failed: {plan.summary}",
                ),
            )

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
            RunRecord(
                pipeline="issue_to_pr",
                run_id=rid,
                timestamp=iso(now),
                action="opened_pr",
                target_kind="issue",
                target_number=number,
                pr_number=pr["number"],
                verified=True,
                verifier_notes=verdict.notes,
                summary=plan.summary,
            ),
        )


async def _finish(settings, record: RunRecord) -> dict[str, Any]:
    await record_run(settings, record)
    await flyte.report.flush.aio()
    return record.to_dict()

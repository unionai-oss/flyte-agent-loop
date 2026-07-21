"""Pipeline 2 — address review comments on agent PRs. Runs every 15 minutes.

Flow:

1. Find open PRs authored by the agent (the GitHub token's user).
2. Call *dibs* on the first claimable PR so a concurrent/future run knows it is
   being worked on.
3. A reviewer agent reads the PR + all its comments and designs scoped fixes.
4. A verifier sub-agent checks that the fixes are aligned with the comments AND
   correct.
5. Only if verified, push the fixes to the PR head branch, then RELEASE the dibs
   so a later run can pick up any additional follow-up comments.
6. Record the run in shared memory for the evals pipeline.
"""

from __future__ import annotations

from typing import Any

import flyte
import flyte.report

from .agents import build_reviewer_agent, build_verifier_agent, parse_plan, parse_verdict
from .common import iso, run_id, utcnow
from .config import load_settings
from .environments import env
from .evals import RunRecord
from .github_client import GitHubClient
from .memory_context import read_shared_context, record_run
from .tools import push_changes_to_pr

TRIGGER = flyte.Trigger(
    name="pr_review_every_15m",
    automation=flyte.Cron("*/15 * * * *"),
    description="Address review comments on agent-authored PRs.",
)


@env.task(report=True, triggers=[TRIGGER])
async def pr_review() -> dict[str, Any]:
    settings = load_settings()
    now = utcnow()
    rid = run_id()
    context = await read_shared_context(settings)

    flyte.report.log(f"<h2>pr_review</h2><p>run <code>{rid}</code> on <b>{settings.repo}</b></p>")

    with GitHubClient(settings) as gh:
        author = gh.authenticated_login()
        target = None
        for pr in gh.list_pull_requests(author=author):
            claim = gh.try_claim(pr["number"], "pr", now=now)
            if claim.claimed:
                target = pr
                break
            flyte.report.log(f"<p>PR #{pr['number']} skipped: {claim.reason}</p>")

        if target is None:
            return await _finish(
                settings,
                RunRecord(
                    pipeline="pr_review",
                    run_id=rid,
                    timestamp=iso(now),
                    action="no_work",
                    summary="No claimable open agent PRs.",
                ),
            )

        number = target["number"]
        flyte.report.log(f"<p>claimed PR #{number}: {target['title']}</p>")

        # 3. Reviewer designs fixes.
        reviewer = build_reviewer_agent(settings, context)
        result = await reviewer.run.aio(
            f"Address review feedback on PR #{number} in repo {settings.repo}. "
            f"Title: {target['title']}"
        )
        plan = parse_plan(result.summary)
        if plan.action == "no_changes" or not plan.has_changes:
            # Nothing actionable: release so a later run can re-check.
            gh.release(number, "pr", now=now)
            return await _finish(
                settings,
                RunRecord(
                    pipeline="pr_review",
                    run_id=rid,
                    timestamp=iso(now),
                    action="no_work",
                    target_kind="pr",
                    target_number=number,
                    pr_number=number,
                    summary=plan.summary or "No actionable unaddressed feedback.",
                ),
            )
        if plan.error:
            gh.release(number, "pr", now=now)
            return await _finish(
                settings,
                RunRecord(
                    pipeline="pr_review",
                    run_id=rid,
                    timestamp=iso(now),
                    action="error",
                    target_kind="pr",
                    target_number=number,
                    pr_number=number,
                    summary=plan.error,
                ),
            )

        # 4. Verifier checks alignment with comments AND correctness.
        addressed = plan.raw.get("addressed") or []
        verdict = parse_verdict(
            (
                await verifier_check(settings, number, plan, addressed)
            )
        )
        flyte.report.log(f"<p>verifier: {'PASS' if verdict.verified else 'FAIL'} — {verdict.notes}</p>")

        if not verdict.verified:
            gh.add_comment(
                number,
                f"\U0001f916 flyte-agent-loop drafted fixes but the verifier flagged them: "
                f"{verdict.notes}\n\nReleasing for a follow-up run.",
            )
            gh.release(number, "pr", now=now)
            return await _finish(
                settings,
                RunRecord(
                    pipeline="pr_review",
                    run_id=rid,
                    timestamp=iso(now),
                    action="error",
                    target_kind="pr",
                    target_number=number,
                    pr_number=number,
                    verified=False,
                    verifier_notes=verdict.notes,
                    summary=f"Verification failed: {plan.summary}",
                ),
            )

        # 5. Push fixes, then release dibs for future follow-up comments.
        push = await push_changes_to_pr.aio(
            pr_number=number,
            files=plan.files,
            message=str(plan.raw.get("message") or f"Address review feedback on #{number}"),
        )
        gh.add_comment(
            number,
            f"\U0001f916 flyte-agent-loop pushed fixes ({push['commit'][:7]}) addressing: "
            + "; ".join(str(a) for a in addressed),
        )
        gh.release(number, "pr", now=now)
        flyte.report.log(f"<p>pushed {push['commit'][:7]} to {push['branch']} and released dibs</p>")
        return await _finish(
            settings,
            RunRecord(
                pipeline="pr_review",
                run_id=rid,
                timestamp=iso(now),
                action="pushed_fixes",
                target_kind="pr",
                target_number=number,
                pr_number=number,
                verified=True,
                verifier_notes=verdict.notes,
                summary=plan.summary,
            ),
        )


async def verifier_check(settings, number: int, plan, addressed) -> str:
    verifier = build_verifier_agent(settings)
    result = await verifier.run.aio(
        f"Objective: correctly address the review comments on PR #{number}.\n"
        f"The proposed fixes claim to address: {addressed}\n"
        f"Files to be written: {sorted(plan.files)}\n"
        f"Summary: {plan.summary}\n"
        f"Read the PR comments and verify the fixes are (a) aligned with the "
        f"comments and (b) correct."
    )
    return result.summary


async def _finish(settings, record: RunRecord) -> dict[str, Any]:
    await record_run(settings, record)
    await flyte.report.flush.aio()
    return record.to_dict()

"""Pipeline 3 — distill prior runs into shared memory. Every 10 minutes.

Reads the run history of the *builder* and *reviewer* agents and uses a
``flyte.ai.agents.Agent`` (the *distiller agent*) to **dedupe, consolidate, and
keep the highest-signal lessons** in a compact, token-efficient memory that is fed
back to those agents as context — so the MemoryStore carries as much useful signal
as possible per token.

Steps are wrapped with the decorator that fits their compute needs:

* ``@flyte.trace`` (``_load_records``, ``_ingest``, ``_evaluate``) — light memory
  I/O + pure computation that runs fine in the distiller task's own container.
* ``@env.task`` (``_snapshot_memory``, ``_introspect_runs``) — reads that grow
  unbounded with history and have no report side effects, isolated with their own
  retries/resources.

The distiller agent itself runs in the task body (in-process, like the builder /
reviewer agents). Shared memory is only rewritten when there are genuinely NEW
records — already-ingested issues/PRs never trigger a re-consolidation.

"No work" runs — ones that exited early without claiming any issue/PR, so they
dispatched no actions and carry no signal — are excluded from every downstream step
and **retroactively deleted** from the ``-runs`` store (``_prune_no_work``), keeping
the memory (and its Run Traces view) to runs that actually did something.

Steps avoid taking :class:`Settings` as an argument (it holds the GitHub token,
which would be checkpointed into stored literals); each calls ``load_settings()``
from the injected environment instead.
"""

from __future__ import annotations

from typing import Any, List, Tuple

import flyte
import flyte.report

from .agents import build_distiller_agent
from .common import iso, run_id, utcnow
from .config import Settings, load_settings
from .environments import env
from .evals import (
    EvalSummary,
    IngestState,
    RunRecord,
    did_no_work,
    evaluate,
    ingest_new_records,
    render_ingested_targets,
    render_records_brief,
    render_report_html,
    select_new_records,
)
from .introspect import SubAction, trace_runs
from .memory_context import (
    MemoryFile,
    delete_run_records,
    load_ingest_state,
    load_run_records_with_ids,
    read_lessons,
    save_ingest_state,
    snapshot_memory,
    write_context_digest,
    write_lessons,
)
from .report_style import (
    finalize_report,
    install_live_report_flush,
    render_memory_store_tab,
    render_run_traces_tab,
)

# How many of the most recent runs to introspect for their sub-action trace.
_MAX_INTROSPECT_RUNS = 10

TRIGGER = flyte.Trigger(
    name="distiller_every_10m",
    automation=flyte.Cron("*/10 * * * *"),
    description="Distill agent run history into high-signal shared memory.",
)


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------
@flyte.trace
async def _load_records() -> List[Tuple[str, RunRecord]]:
    """Load all run records (with their unique memory-path ids) from shared memory."""
    return await load_run_records_with_ids(load_settings())


@flyte.trace
async def _prune_no_work(rel_paths: List[str]) -> int:
    """Retroactively delete "no work" run memories (exited early, no actions) from the
    shared ``-runs`` store. ``rel_paths`` are their memory-path ids. Returns the count
    removed. Best-effort — never fails the distiller."""
    return await delete_run_records(load_settings(), rel_paths)


@flyte.trace
async def _ingest(records_with_ids: List[Tuple[str, RunRecord]], now_iso: str) -> Tuple[IngestState, List[RunRecord]]:
    """Fold records not seen before into the ledger and persist it — but ONLY when
    there is something new. Returns the updated state and the NEW records (empty when
    nothing is new, in which case shared memory is left untouched)."""
    settings = load_settings()
    state = await load_ingest_state(settings)
    new_pairs = select_new_records(records_with_ids, state)
    if not new_pairs:
        return state, []
    state, _count = ingest_new_records(state, new_pairs, now_iso=now_iso)
    await save_ingest_state(settings, state)
    return state, [rec for _, rec in new_pairs]


@flyte.trace
async def _evaluate(records: List[RunRecord]) -> EvalSummary:
    """Compute success/verification/error metrics over the full run history (pure)."""
    return evaluate(records)


@env.task
async def _snapshot_memory() -> List[MemoryFile]:
    """Read the entire shared-memory filesystem (paths + truncated contents).

    A separate action: the read grows unbounded with run history and has no report
    side effects, so it is isolated with its own retries/resources.
    """
    return await snapshot_memory(load_settings())


@env.task
async def _introspect_runs(run_names: List[str]) -> List[SubAction]:
    """Read the flat sub-action trace (metadata + I/O) of the given runs.

    A separate action: it makes many control-plane + I/O reads (one listing per run
    plus I/O per sub-action), so it's isolated with its own retries/resources. Runs
    entirely best-effort — a remote-API failure yields an error row, not a crash.
    """
    return await trace_runs(run_names)


def _recent_run_names(records: List[RunRecord], limit: int) -> List[str]:
    """Distinct Flyte run names of the ``limit`` most recent records (newest first).

    Records predating run-name capture (``run_name == ""``) are skipped.
    """
    seen: set[str] = set()
    names: List[str] = []
    for rec in sorted(records, key=lambda r: r.timestamp, reverse=True):
        rn = rec.run_name
        if rn and rn not in seen:
            seen.add(rn)
            names.append(rn)
        if len(names) >= limit:
            break
    return names


def _distill_prompt(prior_lessons: str, new_records: List[RunRecord], summary: EvalSummary) -> str:
    return (
        f"Headline metrics (whole history): success rate {summary.success_rate:.0%}, "
        f"verification rate {summary.verification_rate:.0%}, error rate {summary.error_rate:.0%}, "
        f"PRs opened {summary.prs_opened}, fixes pushed {summary.fixes_pushed}.\n\n"
        f"CURRENT lessons memory:\n{prior_lessons.strip() or '(none yet)'}\n\n"
        f"NEW run records since the last distillation ({len(new_records)}):\n"
        f"{render_records_brief(new_records)}\n\n"
        f"Produce the updated, deduped, token-efficient lessons memory."
    )


async def _distill_lessons(settings: Settings, new_records: List[RunRecord], summary: EvalSummary) -> str:
    """Run the distiller agent to consolidate the prior lessons + new records."""
    prior = await read_lessons(settings)
    agent = build_distiller_agent(settings)
    result = await agent.run.aio(_distill_prompt(prior, new_records, summary))
    lessons = (result.summary or "").strip() or prior  # fall back to prior if empty
    await write_lessons(settings, lessons)
    return lessons


# ---------------------------------------------------------------------------
# Task
# ---------------------------------------------------------------------------
@env.task(report=True, triggers=[TRIGGER])
async def distiller() -> dict[str, Any]:
    settings = load_settings()
    rid = run_id()
    now = utcnow()
    log = flyte.logger
    log.info("distiller start: run=%s repo=%s", rid, settings.repo)
    flyte.report.log(f"<h2>distiller</h2><p>run <code>{rid}</code> on <b>{settings.repo}</b></p>")

    try:
        install_live_report_flush()  # surface the distiller agent's consolidation live

        records_with_ids = await _load_records()
        log.info("distiller: loaded %d run record(s)", len(records_with_ids))

        # Exclude (and retroactively delete) "no work" runs — ones that exited early
        # without claiming a target, so they dispatched no actions and carry no signal.
        # Everything downstream (ingest, metrics, distillation, run traces) sees only
        # the records that did real work.
        no_work_ids = [rid for rid, rec in records_with_ids if did_no_work(rec)]
        records_with_ids = [(rid, rec) for rid, rec in records_with_ids if not did_no_work(rec)]
        records = [rec for _, rec in records_with_ids]
        if no_work_ids:
            with flyte.group("prune"):
                pruned = await _prune_no_work(no_work_ids)
            log.info("distiller: pruned %d no-work run memory/memories (of %d flagged)",
                     pruned, len(no_work_ids))
            flyte.report.log(
                f"<p>pruned {pruned} no-work run record(s) "
                f"(exited early, no actions) from shared memory</p>"
            )

        state, new_records = await _ingest(records_with_ids, iso(now))
        ingested_count = len(new_records)
        summary = await _evaluate(records)

        # Distill: an Agent dedupes + consolidates the highest-signal lessons into a
        # token-efficient memory. Only when there are new records (else memory is
        # untouched and no tokens are spent).
        if new_records:
            with flyte.group("distill"):
                lessons = await _distill_lessons(settings, new_records, summary)
                digest = lessons + "\n\n" + render_ingested_targets(state)
                await write_context_digest(settings, digest)
            log.info("distiller: consolidated %d new record(s) into memory (%d chars of lessons)",
                     ingested_count, len(lessons))
        else:
            log.info("distiller: no new records; shared memory unchanged")

        # Publish the metrics + the shared-memory filesystem view.
        flyte.report.get_tab("Metrics").replace(render_report_html(summary, records))
        flyte.report.log(
            f"<p>{ingested_count} new record(s) distilled this run "
            f"(shared memory {'updated' if ingested_count else 'unchanged'}); "
            f"{len(state.processed_record_ids)} total across "
            f"{len(state.targets)} issue(s)/PR(s). Success rate {summary.success_rate:.0%}.</p>"
        )
        render_memory_store_tab(await _snapshot_memory.aio())

        # Introspect the most recent runs into a flat sub-action trace ("reasoning
        # trace") — the durable actions each builder / reviewer run dispatched.
        run_names = _recent_run_names(records, _MAX_INTROSPECT_RUNS)
        log.info("distiller: introspecting %d recent run(s) for their sub-action traces", len(run_names))
        render_run_traces_tab(await _introspect_runs.aio(run_names))

        await finalize_report()
        log.info("distiller done: %d total runs, success_rate=%.0f%%, %d target(s) tracked",
                 summary.total_runs, summary.success_rate * 100, len(state.targets))

        return {
            "repo": settings.repo,
            "summary": summary.to_dict(),
            "distilled_this_run": ingested_count,
            "total_ingested": len(state.processed_record_ids),
            "targets_tracked": len(state.targets),
        }

    except Exception as exc:  # graceful recovery: report the error instead of crashing
        flyte.logger.exception("distiller failed")
        flyte.report.log(f"<p style='color:#b00'>pipeline error: {exc}</p>")
        try:
            await finalize_report()
        except Exception:
            flyte.logger.warning("failed to flush report")
        return {"repo": settings.repo, "error": str(exc)}

"""Pipeline 3 — evaluate prior runs and refresh shared memory. Every 10 minutes.

The work is split into steps, each wrapped with the decorator that fits its
compute needs:

* ``@flyte.trace`` (``_load_records``, ``_ingest``, ``_evaluate``) — light memory
  I/O + pure computation that runs fine in the evals task's own container. Tracing
  gives per-step spans + checkpoint durability WITHOUT the overhead of a separate
  container, and keeps report writes in the task's report context.
* ``@env.task`` (``_snapshot_memory``) — a full read of the shared-memory
  filesystem (grows unbounded with run history) with no report side effects, so it
  is isolated as its own action with independent retries/resources.

Shared memory is only written when there are genuinely NEW records to ingest —
issues/PRs already ingested never trigger a re-write of the ledger or digest.

Steps avoid taking :class:`Settings` as an argument (it holds the GitHub token,
which would be checkpointed into stored literals); each calls ``load_settings()``
from the injected environment instead.
"""

from __future__ import annotations

from typing import Any, List, Tuple

import flyte
import flyte.report

from .common import iso, run_id, utcnow
from .config import load_settings
from .environments import env
from .evals import (
    EvalSummary,
    IngestState,
    RunRecord,
    compact_context,
    evaluate,
    ingest_new_records,
    render_ingested_targets,
    select_new_records,
    render_report_html,
)
from .introspect import SubAction, trace_runs
from .memory_context import (
    MemoryFile,
    load_ingest_state,
    load_run_records_with_ids,
    save_ingest_state,
    snapshot_memory,
    write_context_digest,
)
from .report_style import finalize_report, render_memory_store_tab, render_run_traces_tab

# How many of the most recent runs to introspect for their sub-action trace.
_MAX_INTROSPECT_RUNS = 10

TRIGGER = flyte.Trigger(
    name="evals_every_10m",
    automation=flyte.Cron("*/10 * * * *"),
    description="Compact agent run history into memory and publish evals.",
)


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------
@flyte.trace
async def _load_records() -> List[Tuple[str, RunRecord]]:
    """Load all run records (with their unique memory-path ids) from shared memory."""
    return await load_run_records_with_ids(load_settings())


@flyte.trace
async def _ingest(records_with_ids: List[Tuple[str, RunRecord]], now_iso: str) -> Tuple[IngestState, int]:
    """Ingest ONLY records not seen before, then persist the ledger — but only when
    there is something new. If every record was already ingested, shared memory is
    left untouched."""
    settings = load_settings()
    state = await load_ingest_state(settings)
    new_records = select_new_records(records_with_ids, state)
    if not new_records:
        return state, 0  # nothing new -> do NOT re-write the ledger
    state, ingested_count = ingest_new_records(state, new_records, now_iso=now_iso)
    await save_ingest_state(settings, state)
    return state, ingested_count


@flyte.trace
async def _evaluate(records: List[RunRecord], state: IngestState, ingested_count: int) -> EvalSummary:
    """Compute metrics over the full history and, ONLY when new records were
    ingested, refresh the shared context digest fed back to pipelines 1 & 2."""
    summary = evaluate(records)
    if ingested_count > 0:
        digest = compact_context(records, summary) + "\n\n" + render_ingested_targets(state)
        await write_context_digest(load_settings(), digest)
    return summary


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


# ---------------------------------------------------------------------------
# Task
# ---------------------------------------------------------------------------
@env.task(report=True, triggers=[TRIGGER])
async def evals() -> dict[str, Any]:
    settings = load_settings()
    rid = run_id()
    now = utcnow()
    log = flyte.logger
    log.info("evals start: run=%s repo=%s", rid, settings.repo)
    flyte.report.log(f"<h2>evals</h2><p>run <code>{rid}</code> on <b>{settings.repo}</b></p>")

    try:
        records_with_ids = await _load_records()
        records = [rec for _, rec in records_with_ids]
        log.info("evals: loaded %d run record(s)", len(records))

        state, ingested_count = await _ingest(records_with_ids, iso(now))
        log.info("evals: ingested %d new record(s) this run (memory %s)",
                 ingested_count, "updated" if ingested_count else "unchanged")

        summary = await _evaluate(records, state, ingested_count)

        # Publish the evals metrics + the shared-memory filesystem view.
        flyte.report.get_tab("Evals").replace(render_report_html(summary, records))
        flyte.report.log(
            f"<p>{ingested_count} new record(s) ingested this run "
            f"(shared memory {'updated' if ingested_count else 'unchanged'}); "
            f"{len(state.processed_record_ids)} total across "
            f"{len(state.targets)} issue(s)/PR(s). Success rate {summary.success_rate:.0%}.</p>"
        )
        render_memory_store_tab(await _snapshot_memory.aio())

        # Introspect the most recent runs into a flat sub-action trace ("reasoning
        # trace") — the durable actions each issue_to_pr / pr_review run dispatched.
        run_names = _recent_run_names(records, _MAX_INTROSPECT_RUNS)
        log.info("evals: introspecting %d recent run(s) for their sub-action traces", len(run_names))
        render_run_traces_tab(await _introspect_runs.aio(run_names))

        await finalize_report()
        log.info("evals done: %d total runs, success_rate=%.0f%%, %d target(s) tracked",
                 summary.total_runs, summary.success_rate * 100, len(state.targets))

        return {
            "repo": settings.repo,
            "summary": summary.to_dict(),
            "ingested_this_run": ingested_count,
            "total_ingested": len(state.processed_record_ids),
            "targets_tracked": len(state.targets),
        }

    except Exception as exc:  # graceful recovery: report the error instead of crashing
        flyte.logger.exception("evals failed")
        flyte.report.log(f"<p style='color:#b00'>pipeline error: {exc}</p>")
        try:
            await finalize_report()
        except Exception:
            flyte.logger.warning("failed to flush report")
        return {"repo": settings.repo, "error": str(exc)}

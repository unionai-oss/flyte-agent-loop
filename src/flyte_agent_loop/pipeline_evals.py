"""Pipeline 3 — evaluate prior runs and refresh shared memory. Every 10 minutes.

Flow:

1. Load all run records emitted by pipelines 1 and 2 from shared memory.
2. Compute success-rate and related metrics.
3. Compact the records into a short context digest and write it back to memory;
   pipelines 1 and 2 read this digest as agent context on their next run.
4. Publish the metrics as a Flyte report.
"""

from __future__ import annotations

from typing import Any

import flyte
import flyte.report

from .common import iso, run_id, utcnow
from .config import load_settings
from .environments import env
from .evals import (
    compact_context,
    evaluate,
    ingest_new_records,
    render_ingested_targets,
    render_report_html,
    select_new_records,
)
from .memory_context import (
    load_ingest_state,
    load_run_records_with_ids,
    save_ingest_state,
    write_context_digest,
)

TRIGGER = flyte.Trigger(
    name="evals_every_10m",
    automation=flyte.Cron("*/10 * * * *"),
    description="Compact agent run history into memory and publish evals.",
)


@env.task(report=True, triggers=[TRIGGER])
async def evals() -> dict[str, Any]:
    settings = load_settings()
    rid = run_id()
    now = utcnow()

    # 1. Load run records (with their unique memory-path ids) and the ingestion
    #    ledger tracking what has already been processed.
    records_with_ids = await load_run_records_with_ids(settings)
    records = [rec for _, rec in records_with_ids]
    state = await load_ingest_state(settings)

    # 2. Ingest ONLY records not seen before, so previously ingested issues/PRs
    #    are never double-counted into the per-target rollup.
    new_records = select_new_records(records_with_ids, state)
    state, ingested_count = ingest_new_records(state, new_records, now_iso=iso(now))
    await save_ingest_state(settings, state)

    # 3. Evaluate the full history (metrics are a fresh aggregate, not ingestion).
    summary = evaluate(records)

    # 4. Refresh the shared context digest fed back to pipelines 1 & 2. It carries
    #    the metrics, recent verifier lessons, and the ingested issue/PR rollup.
    digest = compact_context(records, summary) + "\n\n" + render_ingested_targets(state)
    await write_context_digest(settings, digest)

    # 5. Publish the report.
    tab = flyte.report.get_tab("Evals")
    tab.replace(render_report_html(summary, records))
    flyte.report.log(
        f"<h2>evals</h2><p>run <code>{rid}</code>: {ingested_count} new record(s) ingested "
        f"this run; {len(state.processed_record_ids)} total across "
        f"{len(state.targets)} issue(s)/PR(s). Success rate {summary.success_rate:.0%}.</p>"
    )
    await flyte.report.flush.aio()

    return {
        "summary": summary.to_dict(),
        "ingested_this_run": ingested_count,
        "total_ingested": len(state.processed_record_ids),
        "targets_tracked": len(state.targets),
    }

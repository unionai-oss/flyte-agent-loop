"""Pure evaluation logic over agent run records.

Pipelines 1 and 2 emit a :class:`RunRecord` per invocation. Pipeline 3 reads
the accumulated records from durable memory and computes success metrics and a
compacted context digest that is fed back to pipelines 1 and 2.

Kept free of Flyte/network dependencies so the metrics are unit testable.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Sequence

# Actions that represent the agent productively completing its objective.
PRODUCTIVE_ACTIONS = {"opened_pr", "pushed_fixes"}
# Actions that are legitimate no-ops (nothing to do / already claimed elsewhere).
NEUTRAL_ACTIONS = {"skipped", "no_work"}


@dataclass
class RunRecord:
    """One agent invocation's outcome, appended to shared memory."""

    pipeline: str  # "issue_to_pr" | "pr_review"
    run_id: str
    timestamp: str  # ISO-8601
    action: str  # opened_pr | pushed_fixes | skipped | no_work | error
    repo: str = ""  # target GitHub repo, "owner/name"
    run_name: str = ""  # Flyte run name, for cross-run sub-action introspection
    target_kind: str = ""  # "issue" | "pr"
    target_number: int | None = None
    pr_number: int | None = None
    pr_url: str = ""  # html_url of the opened/updated PR
    verified: bool = False
    verifier_notes: str = ""
    attempts: int = 1  # number of build->verify attempts made
    summary: str = ""
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RunRecord":
        fields = {f for f in cls.__dataclass_fields__}  # type: ignore[attr-defined]
        return cls(**{k: v for k, v in data.items() if k in fields})


@dataclass
class EvalSummary:
    """Aggregated metrics over a set of run records."""

    total_runs: int = 0
    productive_runs: int = 0
    neutral_runs: int = 0
    error_runs: int = 0
    verified_runs: int = 0
    prs_opened: int = 0
    fixes_pushed: int = 0
    per_pipeline: dict[str, int] = field(default_factory=dict)
    # Success rate = verified productive work / attempts that tried to do work.
    success_rate: float = 0.0
    verification_rate: float = 0.0
    error_rate: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class IngestState:
    """Ledger of what pipeline 3 has already ingested into memory.

    Persisted in the shared store so ingestion is incremental: each run only
    processes run records it has not seen before, and never double-ingests
    content from an issue/PR that was already folded into memory.
    """

    # Unique ids (the ``runs/<ts>_<run>.json`` memory paths) of ingested records.
    processed_record_ids: list[str] = field(default_factory=list)
    # Per-target rollup, keyed by ``"issue:<n>"`` / ``"pr:<n>"``.
    targets: dict[str, dict[str, Any]] = field(default_factory=dict)
    last_ingested_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> "IngestState":
        data = data or {}
        return cls(
            processed_record_ids=list(data.get("processed_record_ids", [])),
            targets=dict(data.get("targets", {})),
            last_ingested_at=str(data.get("last_ingested_at", "")),
        )

    @property
    def processed(self) -> set[str]:
        return set(self.processed_record_ids)


def target_key(record: RunRecord) -> str | None:
    """Stable ``"<kind>:<number>"`` key for a record's issue/PR, or ``None``."""
    if record.target_kind and record.target_number is not None:
        return f"{record.target_kind}:{record.target_number}"
    return None


def select_new_records(
    records_with_ids: Sequence[tuple[str, RunRecord]], state: IngestState
) -> list[tuple[str, RunRecord]]:
    """Return only the ``(id, record)`` pairs not yet ingested per ``state``."""
    seen = state.processed
    return [(rid, rec) for rid, rec in records_with_ids if rid not in seen]


def ingest_new_records(
    state: IngestState,
    new_records_with_ids: Sequence[tuple[str, RunRecord]],
    *,
    now_iso: str = "",
) -> tuple[IngestState, int]:
    """Fold new records into ``state`` (pure). Returns the updated state + count.

    Idempotent: ids already present are ignored, so re-running with overlapping
    input never double-counts a target.
    """
    seen = state.processed
    processed_ids = list(state.processed_record_ids)
    targets = {k: dict(v) for k, v in state.targets.items()}
    ingested = 0

    for rid, rec in new_records_with_ids:
        if rid in seen:
            continue
        seen.add(rid)
        processed_ids.append(rid)
        ingested += 1

        key = target_key(rec)
        if key is None:
            continue
        entry = targets.setdefault(
            key,
            {"kind": rec.target_kind, "number": rec.target_number, "ingest_count": 0},
        )
        entry["ingest_count"] = int(entry.get("ingest_count", 0)) + 1
        entry["last_action"] = rec.action
        entry["last_verified"] = rec.verified
        entry["last_summary"] = rec.summary
        entry["last_ts"] = rec.timestamp
        if rec.pr_number is not None:
            entry["pr_number"] = rec.pr_number

    return (
        IngestState(
            processed_record_ids=processed_ids,
            targets=targets,
            last_ingested_at=now_iso or state.last_ingested_at,
        ),
        ingested,
    )


def render_ingested_targets(state: IngestState, max_targets: int = 20) -> str:
    """Render the per-target ingest rollup for the shared context digest."""
    if not state.targets:
        return "## Processed issues/PRs\n- (nothing ingested yet)"
    ordered = sorted(
        state.targets.values(), key=lambda t: str(t.get("last_ts", "")), reverse=True
    )
    lines = ["## Processed issues/PRs (already ingested — do not re-litigate)"]
    for t in ordered[:max_targets]:
        status = "verified" if t.get("last_verified") else "unverified"
        pr = f" -> PR #{t['pr_number']}" if t.get("pr_number") else ""
        lines.append(
            f"- {t.get('kind')} #{t.get('number')}{pr}: last={t.get('last_action')} "
            f"({status}, seen {t.get('ingest_count')}x) — {t.get('last_summary', '')}"
        )
    return "\n".join(lines)


def evaluate(records: Sequence[RunRecord]) -> EvalSummary:
    """Compute an :class:`EvalSummary` from run records."""
    summary = EvalSummary(total_runs=len(records))
    if not records:
        return summary

    work_attempts = 0
    for r in records:
        summary.per_pipeline[r.pipeline] = summary.per_pipeline.get(r.pipeline, 0) + 1
        if r.action in PRODUCTIVE_ACTIONS:
            summary.productive_runs += 1
            work_attempts += 1
            if r.action == "opened_pr":
                summary.prs_opened += 1
            elif r.action == "pushed_fixes":
                summary.fixes_pushed += 1
        elif r.action in NEUTRAL_ACTIONS:
            summary.neutral_runs += 1
        elif r.action == "error":
            summary.error_runs += 1
            work_attempts += 1
        if r.verified:
            summary.verified_runs += 1

    summary.error_rate = summary.error_runs / summary.total_runs
    if work_attempts:
        summary.success_rate = summary.verified_runs / work_attempts
    if summary.productive_runs:
        summary.verification_rate = summary.verified_runs / summary.productive_runs
    return summary


def compact_context(records: Sequence[RunRecord], summary: EvalSummary, max_lessons: int = 12) -> str:
    """Build a short, model-friendly context digest fed back to pipelines 1 & 2.

    Emphasizes recent verifier feedback (the highest-signal lessons) plus the
    headline metrics, so the builder/reviewer agents can steer away from past
    mistakes.
    """
    lines = [
        "# flyte-agent-loop shared memory",
        "",
        "Aggregate performance of prior agent runs (use this to avoid repeating mistakes):",
        f"- total runs: {summary.total_runs}",
        f"- PRs opened: {summary.prs_opened}, fixes pushed: {summary.fixes_pushed}",
        f"- success rate (verified work / work attempts): {summary.success_rate:.0%}",
        f"- verification rate: {summary.verification_rate:.0%}",
        f"- error rate: {summary.error_rate:.0%}",
        "",
        "## Recent lessons from the verifier",
    ]
    lessons: list[str] = []
    for r in reversed(records):  # most recent first
        note = (r.verifier_notes or "").strip()
        if not note:
            continue
        status = "PASS" if r.verified else "FAIL"
        target = f"{r.target_kind} #{r.target_number}" if r.target_number else r.pipeline
        lessons.append(f"- [{status}] {target}: {note}")
        if len(lessons) >= max_lessons:
            break
    lines.extend(lessons or ["- (no verifier feedback recorded yet)"])
    return "\n".join(lines)


def render_report_html(summary: EvalSummary, records: Sequence[RunRecord]) -> str:
    """Render an HTML fragment for a Flyte report tab."""
    import html

    def cell(v: Any) -> str:
        return html.escape(str(v))

    rows = "".join(
        f"<tr><td>{cell(r.timestamp)}</td><td>{cell(r.pipeline)}</td>"
        f"<td>{cell(r.action)}</td><td>{cell(r.target_kind)} "
        f"{cell(r.target_number or '')}</td><td>{'✅' if r.verified else '❌'}</td>"
        f"<td>{cell(r.summary)}</td></tr>"
        for r in list(reversed(records))[:50]
    )
    return f"""
    <h2>flyte-agent-loop evaluation</h2>
    <ul>
      <li><b>Total runs:</b> {summary.total_runs}</li>
      <li><b>PRs opened:</b> {summary.prs_opened} &nbsp; <b>Fixes pushed:</b> {summary.fixes_pushed}</li>
      <li><b>Success rate:</b> {summary.success_rate:.0%}</li>
      <li><b>Verification rate:</b> {summary.verification_rate:.0%}</li>
      <li><b>Error rate:</b> {summary.error_rate:.0%}</li>
      <li><b>Per pipeline:</b> {html.escape(str(summary.per_pipeline))}</li>
    </ul>
    <h3>Recent runs</h3>
    <table>
      <tr><th>Time</th><th>Pipeline</th><th>Action</th><th>Target</th><th>Verified</th><th>Summary</th></tr>
      {rows}
    </table>
    """

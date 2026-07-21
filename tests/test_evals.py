"""Tests for the pure evaluation logic."""

from flyte_agent_loop.evals import (
    IngestState,
    RunRecord,
    compact_context,
    evaluate,
    ingest_new_records,
    render_ingested_targets,
    render_report_html,
    select_new_records,
    target_key,
)


def _rec(**kw):
    base = dict(pipeline="issue_to_pr", run_id="r", timestamp="2026-07-17T12:00:00Z", action="no_work")
    base.update(kw)
    return RunRecord(**base)


def test_empty_records():
    s = evaluate([])
    assert s.total_runs == 0
    assert s.success_rate == 0.0


def test_success_rate_counts_verified_work_over_attempts():
    records = [
        _rec(action="opened_pr", verified=True),
        _rec(action="opened_pr", verified=False),  # work attempt, unverified
        _rec(action="pushed_fixes", verified=True, pipeline="pr_review"),
        _rec(action="no_work"),  # not a work attempt
        _rec(action="skipped"),  # neutral
        _rec(action="error"),  # work attempt, failed
    ]
    s = evaluate(records)
    assert s.total_runs == 6
    assert s.prs_opened == 2
    assert s.fixes_pushed == 1
    assert s.productive_runs == 3
    assert s.error_runs == 1
    assert s.verified_runs == 2
    # work attempts = 2 opened + 1 pushed + 1 error = 4; verified = 2
    assert s.success_rate == 0.5
    # verification rate = verified / productive = 2/3
    assert round(s.verification_rate, 3) == round(2 / 3, 3)
    assert round(s.error_rate, 3) == round(1 / 6, 3)
    assert s.per_pipeline == {"issue_to_pr": 5, "pr_review": 1}


def test_run_record_roundtrip():
    r = _rec(action="opened_pr", verified=True, verifier_notes="looks good", pr_number=7)
    assert RunRecord.from_dict(r.to_dict()) == r


def test_from_dict_ignores_unknown_keys():
    r = RunRecord.from_dict(
        {"pipeline": "x", "run_id": "y", "timestamp": "t", "action": "no_work", "bogus": 1}
    )
    assert r.pipeline == "x"


def test_compact_context_surfaces_recent_lessons():
    records = [
        _rec(action="opened_pr", verified=True, verifier_notes="good tests", target_number=1),
        _rec(action="error", verified=False, verifier_notes="missing docs", target_number=2),
    ]
    s = evaluate(records)
    ctx = compact_context(records, s)
    assert "flyte-agent-loop shared memory" in ctx
    # Most recent lesson first.
    assert ctx.index("missing docs") < ctx.index("good tests")
    assert "[FAIL]" in ctx and "[PASS]" in ctx


def test_compact_context_handles_no_lessons():
    ctx = compact_context([_rec()], evaluate([_rec()]))
    assert "no verifier feedback recorded yet" in ctx


def test_render_report_html_smoke():
    records = [_rec(action="opened_pr", verified=True, summary="did a thing")]
    html = render_report_html(evaluate(records), records)
    assert "flyte-agent-loop evaluation" in html
    assert "did a thing" in html


# --- incremental ingestion (pipeline 3 dedup) -------------------------------
def _with_ids(*records):
    return [(f"runs/{i}.json", r) for i, r in enumerate(records)]


def test_target_key():
    assert target_key(_rec(target_kind="issue", target_number=5)) == "issue:5"
    assert target_key(_rec(target_kind="", target_number=None)) is None


def test_select_new_records_filters_processed():
    items = _with_ids(_rec(target_number=1), _rec(target_number=2))
    state = IngestState(processed_record_ids=["runs/0.json"])
    new = select_new_records(items, state)
    assert [rid for rid, _ in new] == ["runs/1.json"]


def test_ingest_tracks_targets_and_ids():
    items = _with_ids(
        _rec(action="opened_pr", target_kind="issue", target_number=5, pr_number=9, verified=True),
        _rec(action="pushed_fixes", target_kind="pr", target_number=9, verified=True),
    )
    state, n = ingest_new_records(IngestState(), items, now_iso="2026-07-17T12:00:00Z")
    assert n == 2
    assert set(state.processed_record_ids) == {"runs/0.json", "runs/1.json"}
    assert state.targets["issue:5"]["ingest_count"] == 1
    assert state.targets["issue:5"]["pr_number"] == 9
    assert state.last_ingested_at == "2026-07-17T12:00:00Z"


def test_ingest_is_idempotent_no_double_count():
    items = _with_ids(_rec(target_kind="issue", target_number=5))
    state, n1 = ingest_new_records(IngestState(), items)
    # Re-feeding the SAME records (e.g. next scheduled run) ingests nothing new.
    new = select_new_records(items, state)
    state2, n2 = ingest_new_records(state, new)
    assert n1 == 1
    assert n2 == 0
    assert state2.targets["issue:5"]["ingest_count"] == 1  # not doubled
    assert len(state2.processed_record_ids) == 1


def test_ingest_increments_count_on_genuinely_new_record_for_same_target():
    first = _with_ids(_rec(target_kind="issue", target_number=5))
    state, _ = ingest_new_records(IngestState(), first)
    # A brand-new record (different id) touching the same issue counts again.
    second = [("runs/later.json", _rec(target_kind="issue", target_number=5, action="opened_pr"))]
    new = select_new_records(second, state)
    state, n = ingest_new_records(state, new)
    assert n == 1
    assert state.targets["issue:5"]["ingest_count"] == 2
    assert state.targets["issue:5"]["last_action"] == "opened_pr"


def test_ingest_state_roundtrip():
    state, _ = ingest_new_records(
        IngestState(), _with_ids(_rec(target_kind="pr", target_number=3)), now_iso="t"
    )
    assert IngestState.from_dict(state.to_dict()) == state
    assert IngestState.from_dict(None) == IngestState()


def test_render_ingested_targets():
    state, _ = ingest_new_records(
        IngestState(),
        _with_ids(_rec(target_kind="issue", target_number=5, action="opened_pr", verified=True)),
    )
    out = render_ingested_targets(state)
    assert "issue #5" in out
    assert "verified" in out
    assert render_ingested_targets(IngestState()).endswith("nothing ingested yet)")

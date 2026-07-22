"""Tests for run introspection helpers (pure parts)."""

from flyte_agent_loop.evals import RunRecord
from flyte_agent_loop.introspect import SubAction
from flyte_agent_loop.pipeline_evals import _recent_run_names
from flyte_agent_loop.report_style import render_run_traces_html


def _rec(ts, run_name, action="opened_pr"):
    return RunRecord("issue_to_pr", "r", ts, action, run_name=run_name)


def test_recent_run_names_newest_first_deduped_and_skips_empty():
    recs = [
        _rec("2026-07-22T01:00:00Z", "runA"),
        _rec("2026-07-22T03:00:00Z", "runB"),
        _rec("2026-07-22T00:00:00Z", ""),        # predates run-name capture -> skipped
        _rec("2026-07-22T02:00:00Z", "runB"),    # dup of runB (older) -> deduped
    ]
    assert _recent_run_names(recs, 10) == ["runB", "runA"]


def test_recent_run_names_respects_limit():
    recs = [_rec(f"2026-07-22T0{i}:00:00Z", f"run{i}") for i in range(1, 6)]
    assert _recent_run_names(recs, 2) == ["run5", "run4"]


def test_render_run_traces_html_groups_by_run_and_shows_io():
    subs = [
        SubAction("runA", "a0", "flyte_agent_loop.issue_to_pr", "succeeded", "", "", ""),
        SubAction("runA", "n0", "flyte_agent_loop.read_issue", "succeeded", "",
                  'inputs: {"issue_number": 5}', 'outputs: {"title": "..."}'),
        SubAction("runB", "n1", "flyte_agent_loop.open_pr_with_changes", "failed",
                  "HTTPStatusError: 409", "", ""),
    ]
    html = render_run_traces_html(subs)
    assert "runA" in html and "runB" in html                  # grouped by run
    assert "flyte_agent_loop.read_issue" in html              # task names shown
    assert "issue_number" in html                             # I/O preview present
    assert "HTTPStatusError: 409" in html                     # error surfaced
    assert "succeeded" in html and "failed" in html           # phases shown


def test_render_run_traces_html_empty():
    assert "No run traces" in render_run_traces_html([])

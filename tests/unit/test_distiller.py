"""Tests for the distiller agent inputs (record rendering + consolidation prompt)."""

import asyncio

from flyte_agent_loop.config import Settings
from flyte_agent_loop.evals import RunRecord, evaluate, render_records_brief
from flyte_agent_loop.distiller_agent import _distill_prompt
from flyte_agent_loop.memory_context import delete_run_records


def _settings():
    return Settings(
        repo="a/b", github_token="t", model="claude-sonnet-4-5", agent_id="x",
        dibs_ttl_minutes=30, memory_key="k", github_api_url="u", max_tokens=32000, max_tries=3,
    )


def _rec(pipeline="builder", action="opened_pr", **kw):
    base = dict(run_id="r", timestamp="2026-07-22T00:00:00Z")
    base.update(kw)
    return RunRecord(pipeline, base["run_id"], base["timestamp"], action,
                     target_kind=kw.get("target_kind", ""), target_number=kw.get("target_number"),
                     verified=kw.get("verified", False), verifier_notes=kw.get("verifier_notes", ""),
                     summary=kw.get("summary", ""))


def test_render_records_brief_prioritizes_verifier_notes():
    recs = [_rec("builder", "error", target_kind="issue", target_number=5,
                 verifier_notes="missing tests", summary="tried foo")]
    brief = render_records_brief(recs)
    assert "[builder] error issue#5 verified=False" in brief
    assert "missing tests" in brief          # verifier notes preferred over summary
    assert "tried foo" not in brief


def test_render_records_brief_falls_back_to_summary_and_handles_empty():
    recs = [_rec("reviewer", "pushed_fixes", target_kind="pr", target_number=9, summary="fixed null deref")]
    assert "fixed null deref" in render_records_brief(recs)
    assert render_records_brief([]) == "(none)"


def test_build_distiller_agent_has_no_tools():
    from flyte_agent_loop.agents import build_distiller_agent

    agent = build_distiller_agent(_settings())
    assert agent.name == "distiller"
    assert len(agent._registry) == 0  # single-shot consolidation, no tools


def test_distill_prompt_includes_prior_lessons_new_records_and_metrics():
    recs = [_rec("builder", "error", target_kind="issue", target_number=5, verifier_notes="missing tests")]
    prompt = _distill_prompt("# Lessons\n- always add tests", recs, evaluate(recs))
    assert "always add tests" in prompt      # prior lessons carried in
    assert "missing tests" in prompt         # new record signal
    assert "success rate" in prompt          # headline metrics
    assert "NEW run records" in prompt


def test_distill_prompt_handles_no_prior_lessons():
    recs = [_rec()]
    assert "(none yet)" in _distill_prompt("", recs, evaluate(recs))


def test_delete_run_records_noop_on_empty_needs_no_backend():
    # With nothing to prune the distiller must not touch shared memory at all
    # (no store open, no filesystem call) — so an empty list short-circuits to 0.
    assert asyncio.run(delete_run_records(_settings(), [])) == 0

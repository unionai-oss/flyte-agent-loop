"""Tests for the build<->verify retry messaging in issue_to_pr."""

from flyte_agent_loop.agents import Plan, Verdict
from flyte_agent_loop.pipeline_builder import _build_message, _retry_message


def test_build_message_first_attempt():
    msg = _build_message("acme/widgets", 5, {"title": "Add foo"})
    assert "issue #5" in msg and "acme/widgets" in msg and "Add foo" in msg


def test_retry_message_feeds_back_feedback_and_prior_solution():
    prior = Plan(
        action="implement",
        files={"src/foo.py": "def foo():\n    return 41\n"},
        summary="added foo",
        raw={"branch": "agent/issue-5-foo"},
    )
    verdict = Verdict(verified=False, notes="foo() should return 42, not 41")
    msg = _retry_message(5, {"title": "Add foo"}, prior, verdict, attempt=2, max_tries=5)

    # Includes the verifier feedback...
    assert "should return 42" in msg
    assert "attempt 2 of 5" in msg
    # ...and the prior staged solution (actual file content, not just paths).
    assert "return 41" in msg
    assert "added foo" in msg
    # ...and instructs a full re-stage.
    assert "re-stage" in msg.lower()

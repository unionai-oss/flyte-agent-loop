"""Tests for the review<->verify retry messaging in pr_review."""

from flyte_agent_loop.agents import Plan, Verdict
from flyte_agent_loop.pipeline_reviewer import _retry_message, _review_message


def test_review_message_first_attempt():
    msg = _review_message("acme/widgets", 12, {"title": "Add bar"})
    assert "PR #12" in msg and "acme/widgets" in msg and "Add bar" in msg


def test_retry_message_feeds_back_feedback_and_prior_fixes():
    prior = Plan(
        action="fix",
        files={"src/bar.py": "def bar():\n    return None\n"},
        summary="fixed bar",
        raw={"message": "fix bar", "addressed": ["null check"]},
    )
    verdict = Verdict(verified=False, notes="bar() still returns None on the empty case")
    msg = _retry_message(12, {"title": "Add bar"}, prior, verdict, attempt=2, max_tries=3)

    assert "still returns None" in msg          # verifier feedback
    assert "attempt 2 of 3" in msg
    assert "return None" in msg                  # prior staged content, not just paths
    assert "fixed bar" in msg
    assert "submit_fix" in msg                   # instructs a re-stage + resubmit

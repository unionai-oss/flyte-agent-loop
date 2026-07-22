"""Tests for the 'skip an already-approved PR' decision in pipeline_pr_review."""

from datetime import datetime, timedelta, timezone

from flyte_agent_loop import dibs
from flyte_agent_loop.github_client import LGTM_MARKER
from flyte_agent_loop.pipeline_pr_review import _is_approved_and_held

NOW = datetime(2026, 7, 21, 12, 0, 0, tzinfo=timezone.utc)
AGENT = "agentA"  # dibs agent id
BOT = "agent-bot"  # github login


def _claim(minutes=30):
    # dibs claim comment authored by the bot
    return {"user": BOT, "body": dibs.render_claim("pr", AGENT, "r1", NOW + timedelta(minutes=minutes))}


def _lgtm():
    return {"user": BOT, "body": f"{LGTM_MARKER}\nlooks good"}


def _human():
    return {"user": "alice", "body": "one more thing please"}


def test_skip_when_held_and_approved_no_new_feedback():
    comments = [_claim(), _lgtm()]
    assert _is_approved_and_held(comments, AGENT, BOT, NOW + timedelta(minutes=5)) is True


def test_not_skip_when_claim_expired():
    comments = [_claim(minutes=30), _lgtm()]
    # 31 min later the claim has lapsed -> re-review is allowed.
    assert _is_approved_and_held(comments, AGENT, BOT, NOW + timedelta(minutes=31)) is False


def test_not_skip_when_human_comments_after_approval():
    comments = [_claim(), _lgtm(), _human()]
    assert _is_approved_and_held(comments, AGENT, BOT, NOW + timedelta(minutes=5)) is False


def test_not_skip_when_held_but_not_yet_approved():
    comments = [_claim()]  # claimed but no LGTM yet
    assert _is_approved_and_held(comments, AGENT, BOT, NOW + timedelta(minutes=5)) is False


def test_not_skip_when_not_held():
    comments = [_lgtm()]  # approved but no active claim
    assert _is_approved_and_held(comments, AGENT, BOT, NOW) is False

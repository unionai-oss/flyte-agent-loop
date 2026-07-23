"""Tests for the approved-PR skip / re-activation logic in pr_review."""

from flyte_agent_loop.github_client import (
    LGTM_MARKER,
    REACTIVATE_COMMAND,
    approved_awaiting_command,
)

BOT = "agent-bot"


def _lgtm():
    # The bot's approval comment includes the re-activation instructions, which
    # mention the command — this must NOT count as a human command.
    return {"user": BOT, "body": f"{LGTM_MARKER}\nlooks good; comment {REACTIVATE_COMMAND} to re-activate"}


def _human(body):
    return {"user": "alice", "body": body}


def test_not_approved_is_not_skipped():
    # No LGTM yet -> normal flow, don't skip.
    assert approved_awaiting_command([], BOT) is False
    assert approved_awaiting_command([_human("please fix this")], BOT) is False


def test_approved_with_nothing_since_is_skipped():
    assert approved_awaiting_command([_human("old note"), _lgtm()], BOT) is True


def test_approved_then_plain_human_comment_still_skipped():
    # A regular human comment does NOT re-activate — only the command does.
    comments = [_lgtm(), _human("thanks, looks great!")]
    assert approved_awaiting_command(comments, BOT) is True


def test_approved_then_reactivation_command_is_not_skipped():
    comments = [_lgtm(), _human(f"{REACTIVATE_COMMAND} please re-check error handling")]
    assert approved_awaiting_command(comments, BOT) is False


def test_reactivation_command_is_case_insensitive():
    comments = [_lgtm(), _human("/Flyte-Agent-Loop take another look")]
    assert approved_awaiting_command(comments, BOT) is False


def test_command_before_approval_does_not_reactivate():
    # A command that predates the latest approval is already handled.
    comments = [_human(f"{REACTIVATE_COMMAND} do X"), _lgtm()]
    assert approved_awaiting_command(comments, BOT) is True


def test_bot_mentioning_command_does_not_reactivate():
    # The bot's own later comment mentioning the command must not re-trigger.
    comments = [_lgtm(), {"user": BOT, "body": f"note: {REACTIVATE_COMMAND} is how to re-activate"}]
    assert approved_awaiting_command(comments, BOT) is True

"""Tests for the pure agent-output parsers (plan + verdict)."""

from flyte_agent_loop.agents import parse_plan, parse_verdict


def test_parse_verdict_pass():
    v = parse_verdict('Looks complete.\n{"verified": true, "notes": "tests present"}')
    assert v.verified is True
    assert v.notes == "tests present"


def test_parse_verdict_fail():
    v = parse_verdict('{"verified": false, "notes": "no docs"}')
    assert v.verified is False
    assert v.notes == "no docs"


def test_parse_verdict_unparseable_is_not_verified():
    assert parse_verdict("I think it's fine").verified is False
    assert parse_verdict("").verified is False


def test_parse_plan_implement():
    text = (
        'Here is my plan.\n'
        '{"action": "implement", "branch": "agent/issue-3-foo", "title": "Add foo", '
        '"body": "adds foo", "files": {"foo.py": "print(1)\\n"}, "summary": "added foo"}'
    )
    plan = parse_plan(text)
    assert plan.action == "implement"
    assert plan.has_changes is True
    assert plan.files == {"foo.py": "print(1)\n"}
    assert plan.raw["branch"] == "agent/issue-3-foo"


def test_parse_plan_skip_has_no_changes():
    plan = parse_plan('{"action": "skip", "summary": "not needed"}')
    assert plan.action == "skip"
    assert plan.has_changes is False
    assert plan.summary == "not needed"


def test_parse_plan_rejects_bad_files_map():
    plan = parse_plan('{"action": "implement", "files": {"a": 5}}')
    assert plan.action == "error"
    assert "files" in plan.error


def test_parse_plan_unparseable():
    plan = parse_plan("no json here")
    assert plan.action == "error"
    assert plan.has_changes is False

"""Tests for the pure agent-output parsers (plan + verdict) and helpers."""

from flyte_agent_loop.agents import parse_plan, parse_verdict, render_plan_files


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


def test_render_plan_files_includes_paths_and_full_content():
    out = render_plan_files({"foo.py": "print(1)\n", "docs/readme.md": "# hi"})
    assert "foo.py" in out and "print(1)" in out
    assert "docs/readme.md" in out and "# hi" in out


def test_render_plan_files_truncates_large_files():
    out = render_plan_files({"big.log": "a" * 100}, max_file_chars=10)
    # The cutoff marker must make clear it's a display limit, not a defect.
    assert "90 more characters" in out
    assert "display limit only" in out
    assert "a" * 10 in out  # the shown prefix
    assert "a" * 11 not in out  # but not the full 100-char run


def test_render_plan_files_empty():
    assert render_plan_files({}) == "(no files)"


def test_verify_prompt_embeds_file_contents_not_just_paths():
    # Regression guard: the issue verifier must see the actual proposed file
    # contents (not just the file list), since they aren't committed yet.
    from flyte_agent_loop.agents import Plan
    from flyte_agent_loop.pipeline_issue_to_pr import _verify_prompt

    plan = Plan(action="implement", files={"src/foo.py": "def foo():\n    return 42\n"}, summary="add foo", raw={})
    prompt = _verify_prompt(3, {"title": "Add foo"}, plan)
    assert "return 42" in prompt  # actual content, not just the path
    assert "NOT yet committed" in prompt

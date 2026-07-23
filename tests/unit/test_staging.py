"""Tests for the in-memory change staging (tool-based, no JSON blob)."""

import asyncio

from flyte_agent_loop.staging import ChangeStage, issue_builder_tools, pr_reviewer_tools


def _by_name(tools):
    return {t.__name__: t for t in tools}


def test_issue_staging_accumulates_and_submits():
    stage = ChangeStage(kind="issue")
    t = _by_name(issue_builder_tools(stage))
    t["stage_file"]("src/foo.py", "x = 1\n")
    t["stage_file"]("tests/test_foo.py", "def test(): pass\n")
    t["submit_implementation"]("agent/issue-3-foo", "Add foo", "adds foo", "added foo")

    plan = stage.to_plan()
    assert plan.action == "implement"
    assert plan.has_changes
    assert set(plan.files) == {"src/foo.py", "tests/test_foo.py"}
    assert plan.files["src/foo.py"] == "x = 1\n"
    assert plan.raw == {"branch": "agent/issue-3-foo", "title": "Add foo", "body": "adds foo"}
    assert plan.summary == "added foo"


def test_stage_overwrite_and_unstage():
    stage = ChangeStage(kind="issue")
    t = _by_name(issue_builder_tools(stage))
    t["stage_file"]("a.py", "v1")
    t["stage_file"]("a.py", "v2")  # overwrite
    t["stage_file"]("b.py", "b")
    t["unstage_file"]("b.py")
    assert stage.files == {"a.py": "v2"}


def test_issue_skip():
    stage = ChangeStage(kind="issue")
    _by_name(issue_builder_tools(stage))["skip_issue"]("nothing to do")
    plan = stage.to_plan()
    assert plan.action == "skip"
    assert not plan.has_changes
    assert plan.summary == "nothing to do"


def test_issue_decomposition_staging_and_plan():
    stage = ChangeStage(kind="issue")
    t = _by_name(issue_builder_tools(stage))
    t["stage_issue"]("schema", "Define the schema", "schema body", [])
    t["stage_issue"]("api", "Build the API", "api body", ["schema"])
    t["stage_issue"]("api", "Build the API v2", "api body v2", ["schema"])  # overwrite by key
    t["submit_decomposition"]("split the spec into schema + api")

    plan = stage.to_plan()
    assert plan.action == "decompose"
    assert plan.has_work
    assert not plan.has_changes  # no files -> no PR
    assert [i["key"] for i in plan.issues] == ["schema", "api"]
    api = next(i for i in plan.issues if i["key"] == "api")
    assert api["title"] == "Build the API v2"  # overwrite took effect
    assert api["depends_on"] == ["schema"]
    assert plan.summary == "split the spec into schema + api"


def test_empty_decomposition_is_not_work():
    stage = ChangeStage(kind="issue")
    _by_name(issue_builder_tools(stage))["submit_decomposition"]("nothing to file")
    plan = stage.to_plan()
    assert plan.action == "decompose"
    assert not plan.has_work  # no staged issues -> pipeline treats as no_work


def test_no_submit_is_error():
    stage = ChangeStage(kind="issue")
    t = _by_name(issue_builder_tools(stage))
    t["stage_file"]("a.py", "x")  # staged but never submitted
    plan = stage.to_plan()
    assert plan.action == "error"
    assert not plan.has_changes
    assert "without submitting" in plan.error


def test_pr_fix_staging_and_addressed():
    stage = ChangeStage(kind="pr")
    t = _by_name(pr_reviewer_tools(stage))
    t["stage_file"]("src/bug.py", "fixed\n")
    t["submit_fix"]("Fix null deref", "fixed the bug", ["addressed the null-deref comment"])
    plan = stage.to_plan()
    assert plan.action == "fix"
    assert plan.files == {"src/bug.py": "fixed\n"}
    assert plan.raw["message"] == "Fix null deref"
    assert plan.raw["addressed"] == ["addressed the null-deref comment"]


def test_pr_no_changes():
    stage = ChangeStage(kind="pr")
    _by_name(pr_reviewer_tools(stage))["no_changes"]("looks good")
    plan = stage.to_plan()
    assert plan.action == "no_changes"
    assert not plan.has_changes


def test_tools_invoke_async_via_agent_tool():
    # The agent harness may await tools; the AgentTool wrapper supports .aio too.
    from flyte_agent_loop.agents import build_issue_agent
    from flyte_agent_loop.config import Settings

    s = Settings(
        repo="a/b", github_token="t", model="claude-sonnet-4-5", agent_id="x",
        dibs_ttl_minutes=30, memory_key="k", github_api_url="u", max_tokens=1000, max_tries=5,
    )
    stage = ChangeStage(kind="issue")
    agent = build_issue_agent(s, "", extra_tools=issue_builder_tools(stage))
    tools = {t.name: t for t in agent._registry.values()}

    async def run():
        await tools["stage_file"].aio(path="x.py", content="1")
        await tools["submit_implementation"].aio(branch="b", title="t", body="b", summary="s")

    asyncio.run(run())
    assert stage.to_plan().action == "implement"
    assert agent.parallel_tool_calls is False

"""Tests for GitHubClient using an in-memory httpx mock transport."""

import json
from datetime import datetime, timezone

import httpx
import pytest

from flyte_agent_loop import dibs
from flyte_agent_loop.config import Settings
from flyte_agent_loop.github_client import GitHubClient

NOW = datetime(2026, 7, 17, 12, 0, 0, tzinfo=timezone.utc)


def make_settings(**kw) -> Settings:
    base = dict(
        repo="acme/widgets",
        github_token="t0ken",
        model="claude-sonnet-4-5",
        agent_id="agentA",
        dibs_ttl_minutes=30,
        memory_key="k",
        github_api_url="https://api.github.com",
        max_tokens=32000,
        max_tries=5,
    )
    base.update(kw)
    return Settings(**base)


class FakeGitHub:
    """A tiny stateful GitHub API used to back httpx.MockTransport."""

    def __init__(self, issues=None, comments=None):
        self.issues = issues or []
        self.comments = comments or {}  # number -> [ {user, body} ]
        self.posted = []  # (number, body)

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        method = request.method
        if method == "GET" and path == "/repos/acme/widgets/issues":
            return httpx.Response(200, json=self.issues)
        if method == "GET" and path.endswith("/comments"):
            number = int(path.split("/")[-2])
            return httpx.Response(200, json=self.comments.get(number, []))
        if method == "POST" and path.endswith("/comments"):
            number = int(path.split("/")[-2])
            body = json.loads(request.content)["body"]
            self.posted.append((number, body))
            self.comments.setdefault(number, []).append(
                {"user": {"login": "agentA"}, "body": body}
            )
            return httpx.Response(201, json={"id": 1, "body": body})
        return httpx.Response(404, json={"message": f"unhandled {method} {path}"})


def client_for(fake: FakeGitHub, settings=None) -> GitHubClient:
    settings = settings or make_settings()
    http = httpx.Client(
        base_url=settings.github_api_url,
        transport=httpx.MockTransport(fake.handler),
    )
    return GitHubClient(settings, client=http)


def test_list_open_issues_excludes_pull_requests():
    fake = FakeGitHub(
        issues=[
            {"number": 1, "title": "Bug", "body": "b", "labels": [], "updated_at": "x"},
            {
                "number": 2,
                "title": "A PR",
                "body": "",
                "labels": [],
                "updated_at": "x",
                "pull_request": {"url": "..."},
            },
        ]
    )
    with client_for(fake) as gh:
        issues = gh.list_open_issues()
    assert [i["number"] for i in issues] == [1]


def test_try_claim_posts_when_unclaimed():
    fake = FakeGitHub(comments={5: []})
    with client_for(fake) as gh:
        result = gh.try_claim(5, "issue", now=NOW)
    assert result.claimed is True
    assert len(fake.posted) == 1
    number, body = fake.posted[0]
    assert number == 5
    # The posted comment is a valid, parseable claim marker.
    markers = dibs.parse_markers([body])
    assert markers[0].agent == "agentA"


def test_try_claim_blocked_by_other_agent_does_not_post():
    other = dibs.render_claim("issue", "agentB", "r9", NOW.replace(hour=13))
    fake = FakeGitHub(comments={5: [{"user": {"login": "agentB"}, "body": other}]})
    with client_for(fake) as gh:
        result = gh.try_claim(5, "issue", now=NOW)
    assert result.claimed is False
    assert result.holder == "agentB"
    assert fake.posted == []


def test_try_claim_is_reentrant_for_same_agent():
    mine = dibs.render_claim("issue", "agentA", "r1", NOW.replace(hour=13))
    fake = FakeGitHub(comments={5: [{"user": {"login": "agentA"}, "body": mine}]})
    with client_for(fake) as gh:
        result = gh.try_claim(5, "issue", now=NOW)
    assert result.claimed is True
    assert fake.posted == []  # already held; no duplicate claim


def test_release_posts_release_marker():
    fake = FakeGitHub(comments={5: []})
    with client_for(fake) as gh:
        gh.release(5, "pr", now=NOW)
    _, body = fake.posted[0]
    markers = dibs.parse_markers([body])
    assert markers[0].op is dibs.Op.RELEASE


def test_is_sha_distinguishes_branches_from_shas():
    from flyte_agent_loop.github_client import _is_sha

    assert _is_sha("a" * 40) is True
    assert _is_sha("main") is False
    assert _is_sha("agent/issue-5") is False  # branch names with '/' are not SHAs
    assert _is_sha("A" * 40) is False  # must be lowercase hex
    assert _is_sha("a" * 39) is False


def test_list_files_resolves_slash_branch_via_ref_not_as_sha():
    calls = []

    def handler(request):
        calls.append((request.method, request.url.path))
        if request.url.path == "/repos/acme/widgets/git/ref/heads/agent/issue-5":
            return httpx.Response(200, json={"object": {"sha": "d" * 40}})
        if request.url.path == f"/repos/acme/widgets/git/trees/{'d' * 40}":
            return httpx.Response(200, json={"tree": [{"path": "a.py", "type": "blob"}]})
        return httpx.Response(404, json={"m": request.url.path})

    settings = make_settings()
    http = httpx.Client(base_url=settings.github_api_url, transport=httpx.MockTransport(handler))
    with GitHubClient(settings, client=http) as gh:
        files = gh.list_files("agent/issue-5")
    assert files == ["a.py"]
    # It resolved the ref (did not pass the branch name straight to git/trees).
    assert ("GET", "/repos/acme/widgets/git/ref/heads/agent/issue-5") in calls


def _client_with(handler):
    settings = make_settings()
    http = httpx.Client(base_url=settings.github_api_url, transport=httpx.MockTransport(handler))
    return GitHubClient(settings, client=http)


def test_list_files_returns_empty_for_empty_repo_409():
    # An empty repo returns 409 on git/ref/heads/<branch>.
    def handler(request):
        if request.url.path.endswith("/git/ref/heads/main"):
            return httpx.Response(409, json={"message": "Git Repository is empty."})
        return httpx.Response(404, json={"m": request.url.path})

    with _client_with(handler) as gh:
        assert gh.list_files("main") == []


def test_read_file_raises_file_not_found_on_404():
    def handler(request):
        return httpx.Response(404, json={"message": "Not Found"})

    with _client_with(handler) as gh:
        with pytest.raises(FileNotFoundError):
            gh.read_file("README.md", "main")


def test_branch_exists_false_on_empty_repo():
    def handler(request):
        return httpx.Response(409, json={"message": "Git Repository is empty."})

    with _client_with(handler) as gh:
        assert gh.branch_exists("main") is False


def test_ensure_base_branch_seeds_empty_repo():
    calls = []

    def handler(request):
        calls.append((request.method, request.url.path))
        if request.method == "GET" and request.url.path.endswith("/git/ref/heads/main"):
            return httpx.Response(409, json={"message": "Git Repository is empty."})
        if request.method == "PUT" and request.url.path.endswith("/contents/README.md"):
            return httpx.Response(201, json={"content": {"path": "README.md"}})
        return httpx.Response(404, json={"m": request.url.path})

    with _client_with(handler) as gh:
        gh.ensure_base_branch("main")
    # It seeded an initial commit via the Contents API.
    assert ("PUT", "/repos/acme/widgets/contents/README.md") in calls


def test_ensure_base_branch_noop_when_branch_exists():
    calls = []

    def handler(request):
        calls.append((request.method, request.url.path))
        if request.url.path.endswith("/git/ref/heads/main"):
            return httpx.Response(200, json={"object": {"sha": "a" * 40}})
        return httpx.Response(404, json={"m": request.url.path})

    with _client_with(handler) as gh:
        gh.ensure_base_branch("main")
    # No PUT (seed) happened because the branch already exists.
    assert not any(method == "PUT" for method, _ in calls)


def test_referenced_issue_numbers_from_branch_and_keywords():
    from flyte_agent_loop.github_client import referenced_issue_numbers

    # agent branch convention
    assert referenced_issue_numbers({"head": "agent/issue-12-add-foo", "body": ""}) == {12}
    # closing keywords + our "Implements"
    assert referenced_issue_numbers({"head": "feature", "body": "Closes #3"}) == {3}
    assert referenced_issue_numbers({"head": "x", "body": "fixes #4 and resolves #5"}) == {4, 5}
    assert referenced_issue_numbers({"head": "x", "body": "Implements #7"}) == {7}
    # a bare mention (no keyword, non-agent branch) is NOT treated as associated
    assert referenced_issue_numbers({"head": "x", "body": "see #9 for context"}) == set()
    # union of branch + body
    assert referenced_issue_numbers({"head": "agent/issue-1-x", "body": "Closes #2"}) == {1, 2}


def test_issues_with_open_prs_aggregates():
    prs = [
        {"number": 100, "user": {"login": "u"}, "title": "t", "body": "Closes #1",
         "head": {"ref": "agent/issue-1-x"}, "base": {"ref": "main"}, "updated_at": "x"},
        {"number": 101, "user": {"login": "u"}, "title": "t", "body": "Implements #2",
         "head": {"ref": "feature"}, "base": {"ref": "main"}, "updated_at": "x"},
    ]

    def handler(request):
        if request.url.path == "/repos/acme/widgets/pulls":
            return httpx.Response(200, json=prs)
        return httpx.Response(404, json={"m": request.url.path})

    with _client_with(handler) as gh:
        assert gh.issues_with_open_prs() == {1, 2}


def test_list_pr_files_returns_paths_and_patches():
    files = [
        {"filename": "src/a.py", "status": "modified", "additions": 3, "deletions": 1,
         "patch": "@@ -1 +1 @@\n-old\n+new"},
        {"filename": "b.md", "status": "added", "additions": 5, "deletions": 0, "patch": "+docs"},
    ]

    def handler(request):
        if request.url.path == "/repos/acme/widgets/pulls/7/files":
            return httpx.Response(200, json=files)
        return httpx.Response(404, json={"m": request.url.path})

    with _client_with(handler) as gh:
        out = gh.list_pr_files(7)
    assert [f["path"] for f in out] == ["src/a.py", "b.md"]
    assert out[0]["patch"].startswith("@@") and out[0]["status"] == "modified"


def test_needs_lgtm_dedup_logic():
    from flyte_agent_loop.github_client import LGTM_MARKER, needs_lgtm

    bot = "agent-bot"
    lgtm = {"user": bot, "body": f"{LGTM_MARKER}\nlooks good"}
    human = {"user": "alice", "body": "please tweak this"}
    bot_other = {"user": bot, "body": "dibs claim marker"}

    # No prior approval -> should post.
    assert needs_lgtm([], bot) is True
    assert needs_lgtm([human], bot) is True
    # Already approved, nothing since -> skip.
    assert needs_lgtm([human, lgtm], bot) is False
    # Bot's own later comments (dibs, etc.) don't re-trigger.
    assert needs_lgtm([lgtm, bot_other], bot) is False
    # A human comment after approval -> re-approve after re-review.
    assert needs_lgtm([lgtm, human], bot) is True


def test_post_lgtm_posts_once_then_skips():
    posted = []
    state = {"comments": []}

    def handler(request):
        if request.url.path == "/user":
            return httpx.Response(200, json={"login": "agentA"})
        if request.method == "GET" and request.url.path.endswith("/issues/7/comments"):
            return httpx.Response(200, json=state["comments"])
        if request.method == "POST" and request.url.path.endswith("/issues/7/comments"):
            body = json.loads(request.content)["body"]
            posted.append(body)
            state["comments"].append({"user": {"login": "agentA"}, "body": body})
            return httpx.Response(201, json={"body": body})
        return httpx.Response(404, json={"m": request.url.path})

    with _client_with(handler) as gh:
        assert gh.post_lgtm(7, "great work") is True   # first time posts
        assert gh.post_lgtm(7, "great work") is False  # already approved, no new human comment
    assert len(posted) == 1
    assert "look good" in posted[0]
    # The approval comment explains how to re-activate the agent.
    assert "/flyte-agent-loop" in posted[0]
    assert "review it again" in posted[0].lower() or "take another look" in posted[0].lower()


def test_missing_repo_setting_raises(monkeypatch):
    from flyte_agent_loop.config import load_settings

    monkeypatch.delenv("FLYTE_AGENT_REPO", raising=False)
    with pytest.raises(RuntimeError):
        load_settings()

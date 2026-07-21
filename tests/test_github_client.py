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


def test_missing_repo_setting_raises(monkeypatch):
    from flyte_agent_loop.config import load_settings

    monkeypatch.delenv("FLYTE_AGENT_REPO", raising=False)
    with pytest.raises(RuntimeError):
        load_settings()

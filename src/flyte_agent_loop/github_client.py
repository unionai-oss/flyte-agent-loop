"""A small, synchronous GitHub REST client used by the agent tools.

Only the endpoints the pipelines need are implemented. All state-changing logic
that is worth testing (dibs) lives in :mod:`flyte_agent_loop.dibs`; this client
is a thin, mockable transport layer over the GitHub REST + Git Data APIs.

The client is constructed from :class:`flyte_agent_loop.config.Settings` and is
easy to test with ``respx`` (or any ``httpx`` mock transport) because it takes
an injectable :class:`httpx.Client`.
"""

from __future__ import annotations

import base64
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from . import dibs
from .config import Settings

_SHA_RE = re.compile(r"\A[0-9a-f]{40}\Z")


def _is_sha(ref: str) -> bool:
    """Whether ``ref`` looks like a full 40-hex git object SHA (vs a branch name)."""
    return bool(_SHA_RE.match(ref))


@dataclass
class ClaimResult:
    """Outcome of attempting to claim dibs on an issue or PR."""

    claimed: bool
    reason: str
    holder: str | None = None


class GitHubClient:
    """Thin wrapper over the GitHub REST API scoped to a single repo."""

    def __init__(self, settings: Settings, client: httpx.Client | None = None):
        self.settings = settings
        self.repo = settings.repo
        self._owns_client = client is None
        self._http = client or httpx.Client(
            base_url=settings.github_api_url,
            headers={
                "Authorization": f"Bearer {settings.github_token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "flyte-agent-loop",
            },
            timeout=30.0,
        )

    # -- lifecycle -----------------------------------------------------------
    def close(self) -> None:
        if self._owns_client:
            self._http.close()

    def __enter__(self) -> "GitHubClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _get(self, path: str, **params: Any) -> Any:
        resp = self._http.get(path, params=params or None)
        resp.raise_for_status()
        return resp.json()

    def _post(self, path: str, json: Any) -> Any:
        resp = self._http.post(path, json=json)
        resp.raise_for_status()
        return resp.json()

    def _patch(self, path: str, json: Any) -> Any:
        resp = self._http.patch(path, json=json)
        resp.raise_for_status()
        return resp.json()

    # -- identity ------------------------------------------------------------
    def authenticated_login(self) -> str:
        """Return the login of the token's owner (used to find agent PRs)."""
        return self._get("/user")["login"]

    # -- issues --------------------------------------------------------------
    def list_open_issues(self) -> list[dict[str, Any]]:
        """Open issues (excluding PRs), most recently updated first."""
        raw = self._get(
            f"/repos/{self.repo}/issues",
            state="open",
            sort="updated",
            direction="desc",
            per_page=50,
        )
        return [
            {
                "number": it["number"],
                "title": it["title"],
                "body": it.get("body") or "",
                "labels": [lbl["name"] for lbl in it.get("labels", [])],
                "updated_at": it["updated_at"],
            }
            for it in raw
            if "pull_request" not in it  # the issues endpoint also returns PRs
        ]

    def get_issue(self, number: int) -> dict[str, Any]:
        it = self._get(f"/repos/{self.repo}/issues/{number}")
        return {
            "number": it["number"],
            "title": it["title"],
            "body": it.get("body") or "",
            "labels": [lbl["name"] for lbl in it.get("labels", [])],
            "state": it["state"],
        }

    def list_comments(self, number: int) -> list[dict[str, Any]]:
        """Issue/PR conversation comments (chronological)."""
        raw = self._get(f"/repos/{self.repo}/issues/{number}/comments", per_page=100)
        return [{"user": c["user"]["login"], "body": c.get("body") or ""} for c in raw]

    def add_comment(self, number: int, body: str) -> dict[str, Any]:
        return self._post(f"/repos/{self.repo}/issues/{number}/comments", {"body": body})

    # -- pull requests -------------------------------------------------------
    def list_pull_requests(self, author: str | None = None) -> list[dict[str, Any]]:
        """Open PRs, optionally filtered to those authored by ``author``."""
        raw = self._get(
            f"/repos/{self.repo}/pulls", state="open", sort="updated", direction="desc", per_page=50
        )
        prs = [
            {
                "number": pr["number"],
                "title": pr["title"],
                "body": pr.get("body") or "",
                "author": pr["user"]["login"],
                "head": pr["head"]["ref"],
                "base": pr["base"]["ref"],
                "updated_at": pr["updated_at"],
            }
            for pr in raw
        ]
        if author is not None:
            prs = [pr for pr in prs if pr["author"] == author]
        return prs

    def get_pull_request(self, number: int) -> dict[str, Any]:
        pr = self._get(f"/repos/{self.repo}/pulls/{number}")
        return {
            "number": pr["number"],
            "title": pr["title"],
            "body": pr.get("body") or "",
            "author": pr["user"]["login"],
            "head": pr["head"]["ref"],
            "base": pr["base"]["ref"],
            "state": pr["state"],
        }

    def list_review_comments(self, number: int) -> list[dict[str, Any]]:
        """Inline (diff) review comments on a PR."""
        raw = self._get(f"/repos/{self.repo}/pulls/{number}/comments", per_page=100)
        return [
            {"user": c["user"]["login"], "body": c.get("body") or "", "path": c.get("path", "")}
            for c in raw
        ]

    def open_pull_request(self, *, title: str, head: str, base: str, body: str) -> dict[str, Any]:
        pr = self._post(
            f"/repos/{self.repo}/pulls",
            {"title": title, "head": head, "base": base, "body": body},
        )
        return {"number": pr["number"], "url": pr["html_url"], "head": head}

    # -- git data (branches, files, commits) --------------------------------
    def get_ref_sha(self, branch: str) -> str:
        data = self._get(f"/repos/{self.repo}/git/ref/heads/{branch}")
        return data["object"]["sha"]

    def default_branch(self) -> str:
        return self._get(f"/repos/{self.repo}")["default_branch"]

    def read_file(self, path: str, ref: str) -> str:
        """Return the decoded text content of a file at ``ref``."""
        data = self._get(f"/repos/{self.repo}/contents/{path}", ref=ref)
        if isinstance(data, list):
            raise ValueError(f"{path} is a directory, not a file")
        return base64.b64decode(data["content"]).decode("utf-8")

    def list_files(self, ref: str, subdir: str = "") -> list[str]:
        """List file paths in the repo tree at ``ref`` (recursive).

        ``ref`` may be a branch name (including ones containing ``/`` such as
        ``agent/issue-5``) or a full commit SHA.
        """
        sha = ref if _is_sha(ref) else self.get_ref_sha(ref)
        tree = self._get(f"/repos/{self.repo}/git/trees/{sha}", recursive=1)
        paths = [e["path"] for e in tree.get("tree", []) if e["type"] == "blob"]
        if subdir:
            prefix = subdir.rstrip("/") + "/"
            paths = [p for p in paths if p.startswith(prefix)]
        return paths

    def create_branch(self, new_branch: str, from_branch: str) -> str:
        base_sha = self.get_ref_sha(from_branch)
        try:
            self._post(
                f"/repos/{self.repo}/git/refs",
                {"ref": f"refs/heads/{new_branch}", "sha": base_sha},
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code != 422:  # already exists
                raise
        return base_sha

    def commit_files(
        self, *, branch: str, files: dict[str, str], message: str
    ) -> str:
        """Commit a ``{path: content}`` map onto ``branch`` in a single commit.

        Uses the Git Data API (blobs -> tree -> commit -> ref) so multiple files
        land atomically. Returns the new commit SHA.
        """
        parent_sha = self.get_ref_sha(branch)
        base_commit = self._get(f"/repos/{self.repo}/git/commits/{parent_sha}")
        base_tree = base_commit["tree"]["sha"]

        tree_items = []
        for path, content in files.items():
            blob = self._post(
                f"/repos/{self.repo}/git/blobs", {"content": content, "encoding": "utf-8"}
            )
            tree_items.append(
                {"path": path, "mode": "100644", "type": "blob", "sha": blob["sha"]}
            )
        new_tree = self._post(
            f"/repos/{self.repo}/git/trees", {"base_tree": base_tree, "tree": tree_items}
        )
        commit = self._post(
            f"/repos/{self.repo}/git/commits",
            {"message": message, "tree": new_tree["sha"], "parents": [parent_sha]},
        )
        self._patch(
            f"/repos/{self.repo}/git/refs/heads/{branch}", {"sha": commit["sha"], "force": False}
        )
        return commit["sha"]

    # -- dibs ----------------------------------------------------------------
    def try_claim(self, number: int, kind: str, *, now: datetime | None = None) -> ClaimResult:
        """Attempt to claim dibs on issue/PR ``number``.

        Reads the existing comments, and if no other agent holds an unexpired
        claim, posts a claim comment. Idempotent for the current agent.
        """
        now = now or datetime.now(timezone.utc)
        agent = self.settings.agent_id
        markers = dibs.parse_markers(c["body"] for c in self.list_comments(number))

        active = dibs.active_claim(markers, kind, now)
        if active is not None and active.agent != agent:
            return ClaimResult(False, f"held by {active.agent} until {active.until}", active.agent)
        if dibs.held_by_me(markers, kind, agent, now):
            return ClaimResult(True, "already held by this agent", agent)

        until = now + timedelta(minutes=self.settings.dibs_ttl_minutes)
        run = _run_id()
        self.add_comment(number, dibs.render_claim(kind, agent, run, until))
        return ClaimResult(True, "claimed", agent)

    def release(self, number: int, kind: str, *, now: datetime | None = None) -> dict[str, Any]:
        """Post a release marker so follow-up runs may pick this up again."""
        now = now or datetime.now(timezone.utc)
        return self.add_comment(
            number, dibs.render_release(kind, self.settings.agent_id, _run_id(), now)
        )


def _run_id() -> str:
    """Best-effort unique run identifier for dibs provenance.

    Uses the Flyte action name when running inside a task, otherwise a short
    random token.
    """
    try:
        import flyte

        ctx = flyte.ctx()
        name = getattr(getattr(ctx, "action", None), "name", None)
        if name:
            return str(name)
    except Exception:
        pass
    import uuid

    return uuid.uuid4().hex[:12]

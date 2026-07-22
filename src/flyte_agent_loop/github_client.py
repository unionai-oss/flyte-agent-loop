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


# Signals that a PR is "associated" with an issue:
#  - the agent's own head-branch convention ``agent/issue-<N>-...``, and
#  - GitHub closing keywords (plus our "Implements") followed by ``#<N>`` in the body.
_ISSUE_BRANCH_RE = re.compile(r"agent/issue-(\d+)")
_CLOSING_RE = re.compile(
    r"(?i)\b(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?|implement(?:s|ed)?)\s+#(\d+)"
)


def referenced_issue_numbers(pr: dict[str, Any]) -> set[int]:
    """Issue numbers a PR is associated with (via head branch or closing keywords)."""
    nums: set[int] = set()
    for m in _ISSUE_BRANCH_RE.finditer(pr.get("head", "") or ""):
        nums.add(int(m.group(1)))
    for m in _CLOSING_RE.finditer(pr.get("body", "") or ""):
        nums.add(int(m.group(1)))
    return nums


# Hidden marker on the agent's "looks good" (LGTM) approval comment, used to
# avoid re-posting the approval on every scheduled run.
LGTM_MARKER = "<!-- flyte-agent-loop:lgtm v1 -->"

# The command a human comments to re-activate the reviewer on an approved PR.
REACTIVATE_COMMAND = "/flyte-agent-loop"


def needs_lgtm(comments: list[dict[str, Any]], bot_login: str) -> bool:
    """Whether an approving "looks good" comment should be posted.

    True when no prior LGTM comment exists, or a human (non-bot) comment appeared
    after the most recent one — i.e. new feedback arrived and a re-review still
    found the PR good. Prevents re-approving an unchanged PR every run.
    """
    last_lgtm = -1
    last_human = -1
    for i, c in enumerate(comments):
        if LGTM_MARKER in (c.get("body") or ""):
            last_lgtm = i
        elif (c.get("user") or "") != bot_login:
            last_human = i
    if last_lgtm == -1:
        return True
    return last_human > last_lgtm


def approved_awaiting_command(comments: list[dict[str, Any]], bot_login: str) -> bool:
    """Whether an approved PR should be skipped (no re-activation requested).

    Once the reviewer has approved a PR (posted an LGTM), future runs skip it
    entirely — regardless of dibs TTL — UNLESS a human posts a ``/flyte-agent-loop``
    command AFTER the last approval. Returns True (skip) when the PR is approved and
    no such re-activation command has arrived since; False otherwise (not approved,
    or a re-activation command is pending).
    """
    last_lgtm = -1
    last_command = -1
    for i, c in enumerate(comments):
        body = c.get("body") or ""
        if LGTM_MARKER in body:
            last_lgtm = i
        elif (c.get("user") or "") != bot_login and REACTIVATE_COMMAND in body.lower():
            # A human's re-activation command (the bot's own comments — including the
            # approval text that mentions the command — are excluded by the author check).
            last_command = i
    if last_lgtm == -1:
        return False  # never approved -> normal flow, don't skip
    return last_command <= last_lgtm  # approved and no command since -> skip


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

    def _put(self, path: str, json: Any) -> Any:
        resp = self._http.put(path, json=json)
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
                "url": it.get("html_url", ""),
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
            "url": it.get("html_url", ""),
        }

    def list_comments(self, number: int) -> list[dict[str, Any]]:
        """Issue/PR conversation comments (chronological)."""
        raw = self._get(f"/repos/{self.repo}/issues/{number}/comments", per_page=100)
        return [{"user": c["user"]["login"], "body": c.get("body") or ""} for c in raw]

    def add_comment(self, number: int, body: str) -> dict[str, Any]:
        return self._post(f"/repos/{self.repo}/issues/{number}/comments", {"body": body})

    def post_lgtm(self, number: int, summary: str = "") -> bool:
        """Post an approving "looks good" comment on a PR, deduped via marker.

        Skips (returns ``False``) when the PR has already been approved and no new
        human comment has arrived since; otherwise posts and returns ``True``.
        """
        bot = self.authenticated_login()
        if not needs_lgtm(self.list_comments(number), bot):
            return False
        body = (
            f"{LGTM_MARKER}\n\U0001f916 **flyte-agent-loop** reviewed this PR — the changes look "
            f"good and no further changes are needed.\n\n"
            f"I won't review this PR again on my own. To have me take another look, comment "
            f"`{REACTIVATE_COMMAND} <your instructions>` — for example, "
            f"`{REACTIVATE_COMMAND} please re-check the error handling`."
        )
        if summary:
            body += f"\n\n{summary}"
        self.add_comment(number, body)
        return True

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
                "url": pr.get("html_url", ""),
            }
            for pr in raw
        ]
        if author is not None:
            prs = [pr for pr in prs if pr["author"] == author]
        return prs

    def issues_with_open_prs(self) -> set[int]:
        """Issue numbers that already have an associated *open* PR (any author)."""
        refs: set[int] = set()
        for pr in self.list_pull_requests():
            refs |= referenced_issue_numbers(pr)
        return refs

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
            "url": pr.get("html_url", ""),
        }

    def list_review_comments(self, number: int) -> list[dict[str, Any]]:
        """Inline (diff) review comments on a PR."""
        raw = self._get(f"/repos/{self.repo}/pulls/{number}/comments", per_page=100)
        return [
            {"user": c["user"]["login"], "body": c.get("body") or "", "path": c.get("path", "")}
            for c in raw
        ]

    def list_pr_files(self, number: int) -> list[dict[str, Any]]:
        """Files changed by a PR, with their diff patches (for code review)."""
        raw = self._get(f"/repos/{self.repo}/pulls/{number}/files", per_page=100)
        return [
            {
                "path": f["filename"],
                "status": f["status"],
                "additions": f.get("additions", 0),
                "deletions": f.get("deletions", 0),
                "patch": f.get("patch", ""),
            }
            for f in raw
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
        """Return the decoded text content of a file at ``ref``.

        Raises :class:`FileNotFoundError` if the file (or ref) does not exist —
        a normal condition when the agent explores a repo before creating files.
        """
        try:
            data = self._get(f"/repos/{self.repo}/contents/{path}", ref=ref)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in (404, 409):
                raise FileNotFoundError(f"{path} not found at {ref}") from exc
            raise
        if isinstance(data, list):
            raise IsADirectoryError(f"{path} is a directory, not a file")
        return base64.b64decode(data["content"]).decode("utf-8")

    def list_files(self, ref: str, subdir: str = "") -> list[str]:
        """List file paths in the repo tree at ``ref`` (recursive).

        ``ref`` may be a branch name (including ones containing ``/`` such as
        ``agent/issue-5``) or a full commit SHA. Returns ``[]`` for an empty
        repository or a ref/tree that does not exist yet.
        """
        try:
            sha = ref if _is_sha(ref) else self.get_ref_sha(ref)
            tree = self._get(f"/repos/{self.repo}/git/trees/{sha}", recursive=1)
        except httpx.HTTPStatusError as exc:
            # 409 = empty repository (no commits); 404 = ref/tree missing.
            if exc.response.status_code in (404, 409):
                return []
            raise
        paths = [e["path"] for e in tree.get("tree", []) if e["type"] == "blob"]
        if subdir:
            prefix = subdir.rstrip("/") + "/"
            paths = [p for p in paths if p.startswith(prefix)]
        return paths

    def branch_exists(self, branch: str) -> bool:
        """Whether ``branch`` has a commit (False for an empty repo)."""
        try:
            self.get_ref_sha(branch)
            return True
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in (404, 409):
                return False
            raise

    def ensure_base_branch(self, branch: str) -> None:
        """Guarantee ``branch`` exists with at least one commit.

        A brand-new GitHub repo has no commits, so there is nothing to branch a
        PR off of. This seeds an initial commit on ``branch`` (via the Contents
        API, which initializes an empty repo) so downstream branch/PR creation
        works. No-op when the branch already exists.
        """
        if self.branch_exists(branch):
            return
        content = base64.b64encode(
            b"# Initialized by flyte-agent-loop\n\nThis commit seeds the default "
            b"branch so the agent can open pull requests against it.\n"
        ).decode()
        self._put(
            f"/repos/{self.repo}/contents/README.md",
            {"message": "Initialize repository", "content": content, "branch": branch},
        )

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

"""GitHub tools exposed to the agents.

Each tool is a durable ``@env.task``: when an agent calls it, Flyte dispatches a
tracked, reproducible sub-action. Tools load their own :class:`GitHubClient`
from the injected environment, so the LLM only supplies the semantic arguments.

Tool signatures use plain JSON-friendly types (``int``, ``str``,
``dict[str, str]``) so the harness can present clean JSON schemas to the model.
"""

from __future__ import annotations

from typing import Any

from .config import load_settings
from .environments import env
from .github_client import GitHubClient


def _client() -> GitHubClient:
    return GitHubClient(load_settings())


# ---------------------------------------------------------------------------
# Read tools (used by builder, reviewer, and verifier agents)
# ---------------------------------------------------------------------------
@env.task
async def read_issue(issue_number: int) -> dict[str, Any]:
    """Fetch a GitHub issue's title, body, labels, and state."""
    with _client() as gh:
        return gh.get_issue(issue_number)


@env.task
async def read_issue_comments(issue_number: int) -> list[dict[str, Any]]:
    """Fetch the conversation comments on an issue or PR (chronological)."""
    with _client() as gh:
        return gh.list_comments(issue_number)


@env.task
async def read_pr(pr_number: int) -> dict[str, Any]:
    """Fetch a pull request's metadata (title, body, head/base branches, author)."""
    with _client() as gh:
        return gh.get_pull_request(pr_number)


@env.task
async def read_pr_comments(pr_number: int) -> dict[str, Any]:
    """Fetch both conversation and inline review comments on a PR."""
    with _client() as gh:
        return {
            "conversation": gh.list_comments(pr_number),
            "review_comments": gh.list_review_comments(pr_number),
        }


@env.task
async def list_repo_files(subdir: str = "") -> list[str]:
    """List file paths in the repo's default branch, optionally under ``subdir``."""
    with _client() as gh:
        return gh.list_files(gh.default_branch(), subdir=subdir)


@env.task
async def read_repo_file(path: str, ref: str = "") -> str:
    """Read a repo file's text content at ``ref`` (default branch when empty)."""
    with _client() as gh:
        return gh.read_file(path, ref or gh.default_branch())


# ---------------------------------------------------------------------------
# Write tools
# ---------------------------------------------------------------------------
@env.task
async def open_pr_with_changes(
    issue_number: int,
    branch: str,
    title: str,
    body: str,
    files: dict[str, str],
) -> dict[str, Any]:
    """Create ``branch`` off the default branch, commit ``files``, and open a PR.

    ``files`` maps repository paths to their full new text content. The PR body
    is annotated so merging it closes the originating issue.
    """
    with _client() as gh:
        base = gh.default_branch()
        gh.create_branch(branch, base)
        gh.commit_files(
            branch=branch,
            files=files,
            message=f"{title}\n\nImplements #{issue_number}",
        )
        pr = gh.open_pull_request(
            title=title,
            head=branch,
            base=base,
            body=f"{body}\n\nCloses #{issue_number}\n\n_Opened by flyte-agent-loop._",
        )
        return pr


@env.task
async def push_changes_to_pr(
    pr_number: int, files: dict[str, str], message: str
) -> dict[str, Any]:
    """Commit ``files`` onto the head branch of an existing PR."""
    with _client() as gh:
        pr = gh.get_pull_request(pr_number)
        sha = gh.commit_files(branch=pr["head"], files=files, message=message)
        return {"pr_number": pr_number, "branch": pr["head"], "commit": sha}


# Tool groups handed to the agents. The builder/reviewer agents are given
# READ-ONLY tools: they *propose* a change plan, which the pipeline verifies and
# only then applies via ``open_pr_with_changes`` / ``push_changes_to_pr`` as
# durable, tracked write actions. This enforces the "implement -> verify ->
# write" ordering from the spec.
ISSUE_BUILDER_TOOLS = [read_issue, read_issue_comments, read_repo_file, list_repo_files]
PR_REVIEWER_TOOLS = [read_pr, read_pr_comments, read_repo_file, list_repo_files]
VERIFIER_TOOLS = [read_issue, read_pr, read_repo_file, list_repo_files]

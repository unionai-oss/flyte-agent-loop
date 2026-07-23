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
from .github_client import GitHubClient, topological_order


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
async def read_pr_changes(pr_number: int) -> list[dict[str, Any]]:
    """List the files a PR changes, with their diff patches — the basis for a code review."""
    with _client() as gh:
        return gh.list_pr_files(pr_number)


@env.task
async def list_repo_files(subdir: str = "") -> list[str]:
    """List file paths in the repo's default branch, optionally under ``subdir``."""
    with _client() as gh:
        return gh.list_files(gh.default_branch(), subdir=subdir)


@env.task
async def read_repo_file(path: str, ref: str = "") -> str:
    """Read a repo file's text content at ``ref`` (default branch when empty).

    Returns a ``(not found: ...)`` marker instead of failing when the file does
    not exist — a normal condition when the agent explores a new/empty repo.
    """
    with _client() as gh:
        try:
            return gh.read_file(path, ref or gh.default_branch())
        except FileNotFoundError as exc:
            return f"(not found: {exc})"


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
        gh.ensure_base_branch(base)  # seed an initial commit if the repo is empty
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


@env.task
async def open_issues_from_decomposition(
    spec_number: int, issues: list[dict[str, Any]]
) -> dict[str, Any]:
    """Create decomposed sub-issues and close the originating spec issue.

    ``issues`` is the builder's staged breakdown — each a dict of ``{key, title, body,
    depends_on}`` where ``depends_on`` lists sibling ``key`` s (not GitHub numbers).
    Issues are created in dependency order so an upstream's real number is known before
    its dependents are created; each new issue records its resolved upstream numbers
    (so the builder later skips a sub-issue until its upstreams close). Finally the
    spec issue is closed with a comment linking the created issues.
    """
    with _client() as gh:
        key_to_number: dict[str, int] = {}
        created: list[dict[str, Any]] = []
        for item in topological_order(issues):
            dep_numbers = [key_to_number[k] for k in item.get("depends_on", []) if k in key_to_number]
            result = gh.create_issue(
                title=item["title"], body=item.get("body", ""), depends_on=dep_numbers
            )
            key_to_number[item["key"]] = result["number"]
            created.append({"key": item["key"], "number": result["number"], "url": result["url"]})
        refs = ", ".join(f"#{c['number']}" for c in created)
        gh.close_issue(
            spec_number,
            comment=f"🤖 flyte-agent-loop decomposed this spec into {refs or '(no sub-issues)'}.",
        )
        return {"spec_number": spec_number, "created": created}


# Tool groups handed to the agents. The builder/reviewer agents are given
# READ-ONLY tools: they *propose* a change plan, which the pipeline verifies and
# only then applies via ``open_pr_with_changes`` / ``push_changes_to_pr`` /
# ``open_issues_from_decomposition`` as durable, tracked write actions. This enforces
# the "propose -> verify -> write" ordering from the spec.
ISSUE_BUILDER_TOOLS = [read_issue, read_issue_comments, read_repo_file, list_repo_files]
PR_REVIEWER_TOOLS = [read_pr, read_pr_comments, read_pr_changes, read_repo_file, list_repo_files]

# Verifier tools are scoped to the verification context. The issue verifier runs
# in pipeline 1 BEFORE any PR exists, so it must NOT have PR tools (otherwise the
# model may call read_pr on a not-yet-created PR and 404). The PR verifier runs in
# pipeline 2 against an existing PR.
ISSUE_VERIFIER_TOOLS = [read_issue, read_issue_comments, read_repo_file, list_repo_files]
PR_VERIFIER_TOOLS = [read_pr, read_pr_comments, read_pr_changes, read_repo_file, list_repo_files]

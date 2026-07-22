"""Agent factories: the issue builder, the PR reviewer, and the verifier.

Each is a :class:`flyte.ai.agents.Agent`. The builder and reviewer are given
GitHub tools and the shared-memory context digest; the verifier is a stricter,
tool-light sub-agent that checks the work and emits a structured verdict.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

from flyte.ai.agents import Agent

from typing import Any, Sequence

from .config import Settings
from .tools import ISSUE_BUILDER_TOOLS, ISSUE_VERIFIER_TOOLS, PR_REVIEWER_TOOLS

# ---------------------------------------------------------------------------
# System prompts
# ---------------------------------------------------------------------------
_BUILDER_INSTRUCTIONS = """\
You are an autonomous software engineer working a GitHub issue to completion.

Your objective, given an issue number:
1. Read the issue (and its comments) to understand the requirements.
2. Explore the repository (list files, read relevant files) to match existing
   conventions.
3. Design the change to match what the issue actually asks for — scope your work
   to the issue, and include only the parts it warrants:
   - the implementation itself,
   - tests when you add or change behavior,
   - a usage example and/or documentation when the change is user-facing.
   Not every issue needs all of these: a small fix, config tweak, or doc-only
   issue may need none of the extras, while a new feature usually needs tests.
   Don't pad the PR with artifacts the issue doesn't call for.

You do NOT open the pull request yourself; a verifier reviews your plan first.
Once your design is complete, respond with ONLY a JSON object on the final line:

{"action": "implement",
 "branch": "agent/issue-<number>-<slug>",
 "title": "<PR title>",
 "body": "<PR description>",
 "files": {"path/to/file.py": "<full file content>", ...},
 "summary": "<what you did, one or two sentences>"}

Provide COMPLETE file contents (not diffs) in "files". If no code change is
warranted, respond with {"action": "skip", "summary": "<why>"}.
"""

_REVIEWER_INSTRUCTIONS = """\
You are an autonomous code reviewer for a pull request the agent previously
opened. You always perform your own review of the code — human review comments,
if any, are ADDITIONAL guidance, never a prerequisite for reviewing.

Your objective, given a PR number:
1. Read the PR and the files it changes with their diff (read_pr, read_pr_changes),
   and the current files on the PR head branch as needed to understand the change.
2. Read the PR's comments (conversation + inline review). Treat any human comments
   as extra guidance/context. Ignore the bot's own dibs/claim comments.
3. Review the change yourself for CONCRETE problems: correctness bugs, missing or
   incorrect tests for behavior it changes, missing docs/examples for user-facing
   changes, and anything raised in review comments that is not yet addressed.
   Judge by what the change warrants — don't demand tests/docs/examples a small or
   non-user-facing change doesn't need.
4. If you find concrete problems (or there are unaddressed review comments), design
   tightly-scoped fixes. Do NOT invent stylistic nitpicks or rewrite working code.

You do NOT push changes yourself; a verifier reviews your plan first. Respond
with ONLY a JSON object on the final line:

{"action": "fix",
 "message": "<commit message>",
 "files": {"path/to/file.py": "<full file content>", ...},
 "addressed": ["<problem you found or comment you addressed, and how>", ...],
 "summary": "<one or two sentences>"}

Provide COMPLETE file contents (not diffs). If the PR is already correct and
complete with nothing actionable, respond with
{"action": "no_changes", "summary": "<why the PR is already good>"}.
"""

_VERIFIER_INSTRUCTIONS = """\
You are a meticulous verifier. You did NOT write the work under review. Judge
whether the work actually satisfies what was ASKED — the issue description (or the
review feedback) — and is correct.

Check specifically:
- Does the change do what the issue / feedback actually asks for — no more, no less?
- Is it correct? Any obvious bugs, missing edge cases, or inconsistencies?
- Are supporting artifacts present TO THE EXTENT the change warrants them: tests
  when behavior changes, an example or docs when it is user-facing. Do NOT require
  tests, examples, or docs that this particular change does not need (e.g. a typo
  fix, a config tweak, or a doc-only change).

Judge against the issue's scope, not a fixed checklist. You may read repository
files to check claims. Then respond with ONLY a JSON object on the final line, of
the exact form:

{"verified": true|false, "notes": "<one or two sentences of specific feedback>"}

Set "verified" to true when the change correctly and completely satisfies what was
asked. Be strict about correctness, but do not fail work for lacking artifacts the
issue did not call for.
"""


@dataclass
class Verdict:
    verified: bool
    notes: str


@dataclass
class Plan:
    """A parsed change plan proposed by the builder or reviewer agent."""

    action: str  # implement | skip | fix | no_changes
    files: dict[str, str]
    summary: str
    raw: dict
    error: str = ""

    @property
    def has_changes(self) -> bool:
        return self.action in {"implement", "fix"} and bool(self.files)


def _model(settings: Settings) -> str:
    return settings.model


def _with_context(instructions: str, context: str) -> str:
    if not context.strip():
        return instructions
    return f"{instructions}\n\n--- Shared memory from prior runs ---\n{context}\n"


def build_issue_agent(settings: Settings, context: str = "") -> Agent:
    return Agent(
        name="issue-builder",
        instructions=_with_context(_BUILDER_INSTRUCTIONS, context),
        model=_model(settings),
        tools=ISSUE_BUILDER_TOOLS,
        max_turns=30,
    )


def build_reviewer_agent(settings: Settings, context: str = "") -> Agent:
    return Agent(
        name="pr-reviewer",
        instructions=_with_context(_REVIEWER_INSTRUCTIONS, context),
        model=_model(settings),
        tools=PR_REVIEWER_TOOLS,
        max_turns=30,
    )


def build_verifier_agent(settings: Settings, tools: Sequence[Any] = ISSUE_VERIFIER_TOOLS) -> Agent:
    """Build a verifier agent.

    ``tools`` must match the verification context: pass ``ISSUE_VERIFIER_TOOLS``
    when verifying a proposed issue implementation (no PR exists yet) and
    ``PR_VERIFIER_TOOLS`` when verifying fixes on an existing PR.
    """
    return Agent(
        name="verifier",
        instructions=_VERIFIER_INSTRUCTIONS,
        model=_model(settings),
        tools=tools,
        max_turns=12,
    )


_JSON_RE = re.compile(r"\{.*\}", re.DOTALL)


def _extract_json(text: str) -> dict | None:
    match = _JSON_RE.search(text or "")
    if not match:
        return None
    try:
        data = json.loads(match.group(0))
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def parse_plan(text: str) -> Plan:
    """Parse a builder/reviewer agent's final message into a :class:`Plan`."""
    data = _extract_json(text)
    if data is None:
        return Plan("error", {}, "", {}, error=f"Unparseable plan: {text[:200]}")
    files = data.get("files") or {}
    if not isinstance(files, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in files.items()
    ):
        return Plan("error", {}, "", data, error="'files' must be a {path: content} map")
    return Plan(
        action=str(data.get("action", "")),
        files=files,
        summary=str(data.get("summary", "")).strip(),
        raw=data,
    )


def parse_verdict(text: str) -> Verdict:
    """Parse the verifier's final message into a :class:`Verdict`.

    Falls back to ``verified=False`` when no parseable JSON verdict is found,
    so an unparseable verifier response never counts as a pass.
    """
    data = _extract_json(text)
    if data is None:
        return Verdict(False, f"Unparseable verifier response: {text[:200]}")
    return Verdict(bool(data.get("verified", False)), str(data.get("notes", "")).strip())


# Cap per-file content in a verifier prompt so a pathologically huge generated
# file can't blow the context window. Set generously so real files render in full;
# when a file does exceed it, the marker makes clear the cutoff is a display limit,
# not a defect, so the verifier does not reject complete work as "truncated".
MAX_FILE_CHARS = 30000


def render_plan_files(files: dict[str, str], max_file_chars: int = MAX_FILE_CHARS) -> str:
    """Render proposed ``{path: content}`` changes for a verifier prompt.

    The verifier reviews these *in-memory* changes directly: at verification time
    they are not yet committed (no PR / no push), so they cannot be read back from
    the repo.
    """
    parts: list[str] = []
    for path, content in files.items():
        shown = content
        if len(content) > max_file_chars:
            omitted = len(content) - max_file_chars
            shown = content[:max_file_chars] + (
                f"\n\n[... {omitted} more characters of this file are omitted from THIS PROMPT for "
                f"length. This is a display limit only — the proposed file is complete. Do NOT treat "
                f"this cutoff as a defect or as an incomplete file.]"
            )
        parts.append(f"===== {path} =====\n{shown}")
    return "\n\n".join(parts) if parts else "(no files)"

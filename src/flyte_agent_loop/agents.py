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

from .config import Settings
from .tools import ISSUE_BUILDER_TOOLS, PR_REVIEWER_TOOLS, VERIFIER_TOOLS

# ---------------------------------------------------------------------------
# System prompts
# ---------------------------------------------------------------------------
_BUILDER_INSTRUCTIONS = """\
You are an autonomous software engineer working a GitHub issue to completion.

Your objective, given an issue number:
1. Read the issue (and its comments) to understand the requirements.
2. Explore the repository (list files, read relevant files) to match existing
   conventions.
3. Design the change. ALWAYS include, where applicable:
   - the implementation itself,
   - tests that cover the new behavior,
   - a short usage example,
   - documentation updates.

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
You are an autonomous software engineer addressing review feedback on a pull
request you previously opened.

Your objective, given a PR number:
1. Read the PR and ALL of its comments (conversation + inline review comments).
2. Identify every actionable piece of feedback that has not yet been addressed
   (ignore the bot's own dibs/claim comments).
3. Read the current files on the PR branch to ground your changes.
4. Design tightly-scoped fixes that address that feedback.

You do NOT push changes yourself; a verifier reviews your plan first. Respond
with ONLY a JSON object on the final line:

{"action": "fix",
 "message": "<commit message>",
 "files": {"path/to/file.py": "<full file content>", ...},
 "addressed": ["<comment 1 and how>", "<comment 2 and how>"],
 "summary": "<one or two sentences>"}

Provide COMPLETE file contents (not diffs). If there is no actionable,
unaddressed feedback, respond with {"action": "no_changes", "summary": "<why>"}.
"""

_VERIFIER_INSTRUCTIONS = """\
You are a meticulous verifier. You did NOT write the work under review. Judge
whether the work actually satisfies the stated objective and is correct.

Check specifically:
- Does the change satisfy every requirement / review comment it claims to?
- Are tests, examples, and docs present when the objective called for them?
- Are there obvious correctness bugs, missing edge cases, or inconsistencies?

You may read repository files to check claims. Then respond with ONLY a JSON
object on the final line, of the exact form:

{"verified": true|false, "notes": "<one or two sentences of specific feedback>"}

Set "verified" to true only if the work is correct AND complete. Be strict.
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


def build_verifier_agent(settings: Settings) -> Agent:
    return Agent(
        name="verifier",
        instructions=_VERIFIER_INSTRUCTIONS,
        model=_model(settings),
        tools=VERIFIER_TOOLS,
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

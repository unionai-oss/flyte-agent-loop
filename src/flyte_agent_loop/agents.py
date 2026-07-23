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
from .llm import build_call_llm
from .tools import ISSUE_BUILDER_TOOLS, ISSUE_VERIFIER_TOOLS, PR_REVIEWER_TOOLS

# ---------------------------------------------------------------------------
# System prompts
# ---------------------------------------------------------------------------
_BUILDER_INSTRUCTIONS = """\
You are an autonomous software engineer working a GitHub issue to completion.

Workflow, given an issue number:
1. Read the issue (and its comments) to understand exactly what it asks for.
2. Explore the repository (list_repo_files, read_repo_file) to match existing
   conventions.
3. Implement the change, scoped to what the issue actually asks for — include only
   the parts it warrants: tests when you add or change behavior, and an example
   and/or documentation when the change is user-facing. A small fix, config tweak,
   or doc-only issue may need none of the extras; a new feature usually needs
   tests. Don't pad the PR with artifacts the issue doesn't call for.
4. Stage each file for the PR by calling `stage_file(path, content)` with the
   COMPLETE file content — one call per file (staging the same path overwrites it;
   `unstage_file(path)` removes one). Never paste file contents into your text
   replies; always use `stage_file`.
5. When ALL files are staged, call `submit_implementation(branch, title, body,
   summary)` to finalize, using a branch like `agent/issue-<number>-<slug>`.

You do NOT open the pull request yourself; a verifier reviews the staged change
first. If no code change is warranted, call `skip_issue(reason)` instead. After
submitting (or skipping), reply with a brief plain-text summary.
"""

_REVIEWER_INSTRUCTIONS = """\
You are an autonomous code reviewer for a pull request the agent previously
opened. You always perform your own review of the code — human review comments,
if any, are ADDITIONAL guidance, never a prerequisite for reviewing.

Workflow, given a PR number:
1. Read the PR and the files it changes with their diff (read_pr, read_pr_changes),
   and the current files on the PR head branch as needed to understand the change.
2. Read the PR's comments (conversation + inline review). Treat any human comments
   as extra guidance/context. Ignore the bot's own dibs/claim comments.
3. Review the change yourself for CONCRETE problems: correctness bugs, missing or
   incorrect tests for behavior it changes, missing docs/examples for user-facing
   changes, and anything raised in review comments not yet addressed. Judge by what
   the change warrants — don't demand tests/docs/examples a small or non-user-facing
   change doesn't need, and don't invent stylistic nitpicks or rewrite working code.
4. For each file you need to change, call `stage_file(path, content)` with the
   COMPLETE new file content — one call per file (`unstage_file(path)` to undo).
   Never paste file contents into your text replies; always use `stage_file`.
5. When all fixes are staged, call `submit_fix(message, summary, addressed)` where
   `addressed` lists each problem/comment you handled and how.

You do NOT push changes yourself; a verifier reviews the staged fixes first. If the
PR is already correct and complete with nothing actionable, call
`no_changes(reason)` instead. After submitting, reply with a brief plain-text summary.
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


def build_issue_agent(
    settings: Settings, context: str = "", extra_tools: Sequence[Any] = ()
) -> Agent:
    return Agent(
        name="issue-builder",
        instructions=_with_context(_BUILDER_INSTRUCTIONS, context),
        model=_model(settings),
        tools=[*ISSUE_BUILDER_TOOLS, *extra_tools],
        max_turns=40,
        # Sequential tool calls: each stage_file turn emits one file, bounding
        # per-turn output, and the in-process staging state mutates race-free.
        parallel_tool_calls=False,
        call_llm=build_call_llm(settings.max_tokens),
    )


def build_reviewer_agent(
    settings: Settings, context: str = "", extra_tools: Sequence[Any] = ()
) -> Agent:
    return Agent(
        name="pr-reviewer",
        instructions=_with_context(_REVIEWER_INSTRUCTIONS, context),
        model=_model(settings),
        tools=[*PR_REVIEWER_TOOLS, *extra_tools],
        max_turns=40,
        parallel_tool_calls=False,
        call_llm=build_call_llm(settings.max_tokens),
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
        call_llm=build_call_llm(settings.max_tokens),
    )


_DISTILLER_INSTRUCTIONS = """\
You are the MEMORY DISTILLER for a team of two autonomous agents that work a GitHub
repo on a schedule: a *builder* (implements issues, opens PRs) and a *reviewer*
(reviews PRs, pushes fixes). Their run outcomes are recorded, and a compact "lessons"
memory is fed back to them as context on every run so they repeat what works and avoid
what fails.

Given the CURRENT lessons memory plus the NEW run records since the last distillation,
produce an UPDATED lessons memory that packs as much useful signal as possible into as
few tokens as possible:

- DEDUPE: merge lessons that make the same point; never state a point twice.
- CONSOLIDATE: fold the new records' insights into the existing lessons rather than
  appending a parallel list.
- PRIORITIZE SIGNAL: keep concrete, actionable lessons — especially from verifier
  feedback (what made work fail vs. pass). Drop vague, one-off, or low-value notes.
  If a lesson recurs, it is higher signal — keep it, and you may note it's common.
- BE TOKEN-EFFICIENT: terse bullet points, no preamble, no meta-commentary. Cap the
  list — if there are many candidates, keep only the highest-impact lessons.

Output ONLY the updated lessons memory in markdown: a short ``# Lessons`` heading
followed by concise bullets. Nothing else.
"""


def build_distiller_agent(settings: Settings) -> Agent:
    """Agent that consolidates run history into a compact, high-signal lessons memory."""
    return Agent(
        name="distiller",
        instructions=_DISTILLER_INSTRUCTIONS,
        model=_model(settings),
        max_turns=2,  # single-shot consolidation, no tools
        call_llm=build_call_llm(settings.max_tokens),
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

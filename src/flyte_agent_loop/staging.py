"""In-memory change staging via tools, instead of one giant JSON blob.

The builder/reviewer agents used to emit their entire change — full file contents
plus metadata — as a single JSON object in their final message. For any non-trivial
change that output exceeds the model's max tokens and gets truncated into invalid
JSON, which silently drops the run into the ``no_work`` branch (no verify, no PR).

Instead, the agent stages each file with its own ``stage_file(path, content)`` tool
call (one file per turn, so per-turn output is bounded by the largest single file),
then calls ``submit_*`` to finalize. These are *plain closure tools*: the agent
harness invokes them in-process, so they accumulate state in a :class:`ChangeStage`
held by the pipeline. After the run, the pipeline reads the stage directly — no
free-text parsing.
"""

from __future__ import annotations

import flyte
from dataclasses import dataclass, field
from typing import Any, Callable

from .agents import Plan


@dataclass
class ChangeStage:
    """Accumulates the files + metadata an agent stages during a run."""

    kind: str  # "issue" | "pr"
    files: dict[str, str] = field(default_factory=dict)
    action: str = ""  # implement | fix | skip | no_changes | "" (never submitted)
    branch: str = ""
    title: str = ""
    body: str = ""
    message: str = ""
    summary: str = ""
    addressed: list[str] = field(default_factory=list)

    def to_plan(self) -> Plan:
        """Convert the staged state into the :class:`Plan` the pipeline expects."""
        if self.action == "implement":
            return Plan(
                action="implement",
                files=dict(self.files),
                summary=self.summary,
                raw={"branch": self.branch, "title": self.title, "body": self.body},
            )
        if self.action == "fix":
            return Plan(
                action="fix",
                files=dict(self.files),
                summary=self.summary,
                raw={"message": self.message, "addressed": list(self.addressed)},
            )
        if self.action in ("skip", "no_changes"):
            return Plan(action=self.action, files={}, summary=self.summary, raw={})
        return Plan(
            action="error",
            files={},
            summary="",
            raw={},
            error="Agent finished without submitting a change (no submit_* / skip call).",
        )


def _stage_file_tools(stage: ChangeStage) -> list[Callable[..., Any]]:
    def stage_file(path: str, content: str) -> str:
        """Stage the COMPLETE content of one file to include in the change.

        Call once per file, passing the entire final file body in ``content``.
        Staging the same ``path`` again overwrites it.
        """
        stage.files[path] = content
        return f"staged {path} ({len(content)} bytes); {len(stage.files)} file(s) staged so far"

    def unstage_file(path: str) -> str:
        """Remove a file you previously staged by mistake."""
        stage.files.pop(path, None)
        return f"unstaged {path}; {len(stage.files)} file(s) staged"

    return [stage_file, unstage_file]


def issue_builder_tools(stage: ChangeStage) -> list[Callable[..., Any]]:
    """Staging tools for the issue builder (stage files, then submit or skip)."""
    tools = _stage_file_tools(stage)

    @flyte.trace
    def submit_implementation(branch: str, title: str, body: str, summary: str) -> str:
        """Finalize the implementation once ALL files are staged.

        Provide the PR ``branch`` (e.g. ``agent/issue-<number>-<slug>``), PR
        ``title``, PR ``body``, and a one/two-sentence ``summary`` of what you did.
        """
        stage.action = "implement"
        stage.branch, stage.title, stage.body, stage.summary = branch, title, body, summary
        return f"submitted implementation with {len(stage.files)} file(s) staged"

    @flyte.trace
    def skip_issue(reason: str) -> str:
        """Declare that no code change is warranted for this issue, with a reason."""
        stage.action = "skip"
        stage.summary = reason
        return "recorded: no change (skip)"

    return [*tools, submit_implementation, skip_issue]


def pr_reviewer_tools(stage: ChangeStage) -> list[Callable[..., Any]]:
    """Staging tools for the PR reviewer (stage fixes, then submit or no-changes)."""
    tools = _stage_file_tools(stage)

    def submit_fix(message: str, summary: str, addressed: list[str]) -> str:
        """Finalize the fix once ALL changed files are staged.

        Provide a commit ``message``, a short ``summary``, and ``addressed`` — the
        list of problems you found or review comments you handled (and how).
        """
        stage.action = "fix"
        stage.message, stage.summary = message, summary
        stage.addressed = list(addressed or [])
        return f"submitted fix with {len(stage.files)} file(s) staged"

    def no_changes(reason: str) -> str:
        """Declare the PR is already correct and complete — no changes needed."""
        stage.action = "no_changes"
        stage.summary = reason
        return "recorded: no changes needed"

    return [*tools, submit_fix, no_changes]

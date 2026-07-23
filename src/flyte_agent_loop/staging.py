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
    action: str = ""  # implement | decompose | fix | skip | no_changes | "" (never submitted)
    branch: str = ""
    title: str = ""
    body: str = ""
    message: str = ""
    summary: str = ""
    addressed: list[str] = field(default_factory=list)
    # Sub-issues staged for a spec decomposition: {key, title, body, depends_on:[keys]}.
    issues: list[dict[str, Any]] = field(default_factory=list)

    def to_plan(self) -> Plan:
        """Convert the staged state into the :class:`Plan` the pipeline expects."""
        if self.action == "implement":
            return Plan(
                action="implement",
                files=dict(self.files),
                summary=self.summary,
                raw={"branch": self.branch, "title": self.title, "body": self.body},
            )
        if self.action == "decompose":
            return Plan(
                action="decompose",
                files={},
                summary=self.summary,
                raw={"issues": [dict(i) for i in self.issues]},
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
    """Staging tools for the issue builder.

    Two mutually-exclusive outcomes, both *staged* for the pipeline to verify then
    apply as durable writes (the agent itself performs no writes):

    * **Code change** — ``stage_file`` per file, then ``submit_implementation`` → PR.
    * **Spec decomposition** — ``stage_issue`` per work item, then
      ``submit_decomposition`` → the pipeline opens those issues.

    Or ``skip_issue`` when neither is warranted.
    """
    tools = _stage_file_tools(stage)

    @flyte.trace
    def submit_implementation(branch: str, title: str, body: str, summary: str) -> str:
        """Finalize a CODE change once ALL files are staged (leads to a PR).

        Provide the PR ``branch`` (e.g. ``agent/issue-<number>-<slug>``), PR
        ``title``, PR ``body``, and a one/two-sentence ``summary`` of what you did.
        """
        stage.action = "implement"
        stage.branch, stage.title, stage.body, stage.summary = branch, title, body, summary
        return f"submitted implementation with {len(stage.files)} file(s) staged"

    def stage_issue(key: str, title: str, body: str = "", depends_on: list[str] | None = None) -> str:
        """Stage ONE sub-issue to file when decomposing a spec (no PR/code).

        ``key`` is a short local id you choose (e.g. ``"api"``) so other staged issues
        can depend on it; ``depends_on`` lists the keys of sibling issues this one
        depends on (independent items → leave empty so they can be worked in parallel).
        Real GitHub numbers are assigned when the pipeline creates the issues in
        dependency order. Staging the same ``key`` again overwrites it.
        """
        stage.issues = [i for i in stage.issues if i["key"] != key]
        stage.issues.append(
            {"key": key, "title": title, "body": body or "", "depends_on": list(depends_on or [])}
        )
        return f"staged issue '{key}': {title} (depends_on={depends_on or []}); {len(stage.issues)} staged"

    @flyte.trace
    def submit_decomposition(summary: str) -> str:
        """Finalize a spec DECOMPOSITION once ALL sub-issues are staged.

        The pipeline creates the staged issues (wiring their dependencies) and closes
        the spec issue. Use this only when the issue asks you to break a spec into
        separate issues — NOT to write code.
        """
        stage.action = "decompose"
        stage.summary = summary
        return f"submitted decomposition with {len(stage.issues)} issue(s) staged"

    @flyte.trace
    def skip_issue(reason: str) -> str:
        """Declare that neither a code change nor a decomposition is warranted, with a reason."""
        stage.action = "skip"
        stage.summary = reason
        return "recorded: no change (skip)"

    return [*tools, submit_implementation, stage_issue, submit_decomposition, skip_issue]


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

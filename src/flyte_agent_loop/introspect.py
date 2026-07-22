"""Cross-run introspection: the flat sub-action trace of a Flyte run.

Given a run name (captured on each ``RunRecord.run_name``), this reads the run's
actions from the control plane — the durable sub-actions an ``issue_to_pr`` /
``pr_review`` run dispatched (tool calls, PR/commit operations, etc.) — with their
metadata and (truncated) inputs/outputs. This is the "reasoning trace" of a run.

Everything is best-effort: the remote API may be unavailable or a specific action's
I/O may not be reconstructable, so each layer degrades to a message rather than
raising, and the evals pipeline treats the whole thing as an optional report.
"""

from __future__ import annotations

from dataclasses import dataclass

# Per-field cap on the input/output previews embedded in the trace.
_MAX_IO = 600
# Overall cap on how many sub-actions we collect across all runs in one pass.
_MAX_SUBACTIONS = 300


@dataclass
class SubAction:
    """One durable action within a run, with metadata + truncated I/O."""

    run_name: str
    action: str  # action name
    task: str  # task name (e.g. flyte_agent_loop.read_issue), "" if none
    phase: str  # succeeded | failed | aborted | ...
    error: str
    inputs: str  # truncated preview
    outputs: str  # truncated preview


def _truncate(value: object) -> str:
    s = "" if value is None else str(value)
    return s if len(s) <= _MAX_IO else s[:_MAX_IO] + f"\n… [+{len(s) - _MAX_IO} chars]"


async def _action_io(action) -> tuple[str, str]:
    try:
        details = await action.details()
    except Exception as exc:
        return (f"(details unavailable: {type(exc).__name__})", "")
    try:
        ins = _truncate(await details.inputs())
    except Exception as exc:
        ins = f"(inputs unavailable: {type(exc).__name__})"
    try:
        outs = _truncate(await details.outputs())
    except Exception as exc:
        outs = f"(outputs unavailable: {type(exc).__name__})"
    return ins, outs


async def trace_run(run_name: str, *, with_io: bool = True) -> list[SubAction]:
    """Return the flat list of sub-actions for ``run_name`` (best-effort)."""
    from flyte.remote import Action

    try:
        actions = [a async for a in Action.listall.aio(for_run_name=run_name)]
    except Exception as exc:
        return [SubAction(run_name, "(could not list actions)", "", "error", f"{type(exc).__name__}: {exc}", "", "")]

    subs: list[SubAction] = []
    for a in actions:
        try:
            phase = a.phase.value
        except Exception:
            phase = "unknown"
        error = ""
        try:
            if a.pb2.HasField("error_info"):
                error = f"{a.pb2.error_info.kind}: {a.pb2.error_info.message}"
        except Exception:
            pass
        ins, outs = await _action_io(a) if with_io else ("", "")
        subs.append(
            SubAction(
                run_name=run_name,
                action=(getattr(a, "name", "") or ""),
                task=(getattr(a, "task_name", "") or ""),
                phase=phase,
                error=error,
                inputs=ins,
                outputs=outs,
            )
        )
    return subs


async def trace_runs(run_names: list[str], *, with_io: bool = True) -> list[SubAction]:
    """Flat sub-action trace across several runs, capped at ``_MAX_SUBACTIONS``."""
    out: list[SubAction] = []
    for run_name in run_names:
        if not run_name:
            continue
        out.extend(await trace_run(run_name, with_io=with_io))
        if len(out) >= _MAX_SUBACTIONS:
            out.append(SubAction("", f"… trace capped at {_MAX_SUBACTIONS} sub-actions", "", "", "", "", ""))
            break
    return out

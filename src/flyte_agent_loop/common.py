"""Small shared helpers used across pipelines."""

from __future__ import annotations

from datetime import datetime, timezone

from .github_client import _run_id


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def run_id() -> str:
    """Stable-ish identifier for the current run (Flyte action name or random)."""
    return _run_id()


def run_name() -> str:
    """The Flyte *run* name of the current execution (for cross-run introspection).

    Empty string when not running inside a task context.
    """
    try:
        import flyte

        ctx = flyte.ctx()
        return getattr(getattr(ctx, "action", None), "run_name", "") or ""
    except Exception:
        return ""

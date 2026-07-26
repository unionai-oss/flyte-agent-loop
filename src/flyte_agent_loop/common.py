"""Tiny shared helpers for the current run's identity and time.

These wrap Flyte's execution context so the rest of the codebase doesn't reach into
it directly:

* ``run_id()`` / ``run_name()`` identify the current execution — used as *dibs*
  provenance (so concurrent runs can tell their claims apart) and for cross-run
  introspection by the distiller.
* ``utcnow()`` / ``iso()`` are the single source of UTC timestamps across the
  pipelines, so every recorded time is formatted the same way.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def run_id() -> str:
    """Best-effort unique id for the current run: the Flyte action name, else random.

    Stamped into dibs claim markers so two runs racing for the same issue/PR can tell
    their claims apart (they share an agent id, but not a run id).
    """
    try:
        import flyte

        ctx = flyte.ctx()
        name = getattr(getattr(ctx, "action", None), "name", None)
        if name:
            return str(name)
    except Exception:
        pass
    return uuid.uuid4().hex[:12]


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

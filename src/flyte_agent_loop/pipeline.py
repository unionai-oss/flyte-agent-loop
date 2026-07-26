"""Shared bookkeeping for the builder and reviewer pipelines.

Both pipelines follow the same shape — *claim* a target, run a *propose ↔ verify*
loop, *apply* the result, and *record* the run — and the mechanical parts of that
shape are identical between them. They live here, once, so each pipeline file can
focus on the interesting, agent-specific logic (what to build, how to verify, what to
apply) rather than repeating the plumbing.

What's shared:

* :func:`new_record` — build the :class:`~flyte_agent_loop.evals.RunRecord` that every
  run returns (and that the distiller later reads).
* :func:`release_claim` / :func:`comment_and_release` / :func:`safe_release` — let go
  of a *dibs* claim so a future run can retry the target.
* :func:`finish_run` — persist the record to shared memory and flush the report, both
  best-effort so a bookkeeping hiccup never turns a completed run into a task crash.
"""

from __future__ import annotations

import flyte

from .common import iso, run_name
from .config import Settings
from .evals import RunRecord
from .github_client import GitHubClient
from .memory import record_run
from .reports import finalize_report


def new_record(
    pipeline: str,
    kind: str,
    rid: str,
    now,
    action: str,
    *,
    number: int | None = None,
    pr_number: int | None = None,
    pr_url: str = "",
    verified: bool = False,
    verifier_notes: str = "",
    attempts: int = 1,
    summary: str = "",
    error: str = "",
) -> RunRecord:
    """Assemble the :class:`RunRecord` a pipeline returns for one run.

    ``pipeline`` is ``"builder"`` / ``"reviewer"`` and ``kind`` is ``"issue"`` / ``"pr"``
    — the target kind is only set when a target was actually claimed (``number`` given).
    """
    return RunRecord(
        pipeline=pipeline,
        run_id=rid,
        timestamp=iso(now),
        action=action,
        target_kind=kind if number is not None else "",
        target_number=number,
        pr_number=pr_number,
        pr_url=pr_url,
        verified=verified,
        verifier_notes=verifier_notes,
        attempts=attempts,
        summary=summary,
        error=error,
    )


def release_claim(settings: Settings, number: int, kind: str, now) -> None:
    """Post a dibs *release* marker so a later run may pick this target up again."""
    with GitHubClient(settings) as gh:
        gh.release(number, kind, now=now)


def comment_and_release(settings: Settings, number: int, kind: str, now, body: str) -> None:
    """Leave a comment (e.g. the verifier's feedback), then release the dibs claim."""
    with GitHubClient(settings) as gh:
        gh.add_comment(number, body)
        gh.release(number, kind, now=now)


def safe_release(settings: Settings, number: int, kind: str, now) -> None:
    """Best-effort dibs release; never raises (used on the error-recovery path)."""
    try:
        release_claim(settings, number, kind, now)
    except Exception:
        flyte.logger.warning(f"failed to release dibs on {kind} #{number}")


async def finish_run(settings: Settings, record: RunRecord) -> RunRecord:
    """Stamp, persist, and flush a completed run — then return its record.

    Returns the :class:`RunRecord` dataclass itself (not ``.to_dict()``): Flyte
    serializes dataclass outputs natively, including ``Optional``/``None`` fields,
    whereas a ``dict[str, Any]`` would be pickled per-value and the pickle transformer
    rejects ``None``.

    Persisting to memory and flushing the report are best-effort: a failure here must
    not turn a completed run into a task crash.
    """
    record.repo = settings.repo
    record.run_name = run_name()
    try:
        await record_run(settings, record)
    except Exception:
        flyte.logger.warning("failed to persist run record to memory")
    try:
        await finalize_report()
    except Exception:
        flyte.logger.warning("failed to flush report")
    return record

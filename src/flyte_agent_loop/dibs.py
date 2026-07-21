"""Cooperative "dibs" over GitHub issues and PRs, implemented as comment markers.

Multiple scheduled runs of the same pipeline may fire while a previous run is
still working. To avoid two runs picking up the same issue/PR, a run posts a
*dibs* comment containing a machine-readable marker before it starts working.
Future runs parse the comments, and if an unexpired claim by another agent is
present, they skip that issue/PR.

The marker is an HTML comment (invisible in GitHub's rendered view) of the form::

    <!-- flyte-agent-loop:dibs v1 op=claim kind=issue agent=<id> run=<run> until=<iso8601> -->

This module is deliberately pure: it operates on already-fetched comment bodies
and an explicit ``now`` timestamp, so the claim state machine is fully unit
testable without any network or Flyte context.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Iterable, Sequence

MARKER_PREFIX = "flyte-agent-loop:dibs"
_MARKER_RE = re.compile(
    r"<!--\s*"
    + re.escape(MARKER_PREFIX)
    + r"\s+v1\s+"
    r"op=(?P<op>claim|release)\s+"
    r"kind=(?P<kind>\w+)\s+"
    r"agent=(?P<agent>[^\s]+)\s+"
    r"run=(?P<run>[^\s]+)\s+"
    r"until=(?P<until>[^\s]+)\s*-->"
)


class Op(str, Enum):
    CLAIM = "claim"
    RELEASE = "release"


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


@dataclass(frozen=True)
class Marker:
    """A parsed dibs marker."""

    op: Op
    kind: str
    agent: str
    run: str
    until: datetime


def render_claim(kind: str, agent: str, run: str, until: datetime) -> str:
    """Render the full comment body an agent posts when claiming dibs."""
    marker = (
        f"<!-- {MARKER_PREFIX} v1 op=claim kind={kind} "
        f"agent={agent} run={run} until={_iso(until)} -->"
    )
    return (
        f"{marker}\n"
        f"\U0001f916 **flyte-agent-loop** (`{agent}`) has claimed this {kind} and is "
        f"working on it. Other scheduled runs will skip it until "
        f"`{_iso(until)}` unless released sooner."
    )


def render_release(kind: str, agent: str, run: str, now: datetime) -> str:
    """Render the comment body an agent posts to release its dibs."""
    marker = (
        f"<!-- {MARKER_PREFIX} v1 op=release kind={kind} "
        f"agent={agent} run={run} until={_iso(now)} -->"
    )
    return (
        f"{marker}\n"
        f"\U0001f513 **flyte-agent-loop** (`{agent}`) released its claim on this "
        f"{kind}; follow-up runs may pick it up."
    )


def parse_markers(comment_bodies: Iterable[str]) -> list[Marker]:
    """Extract all dibs markers from a chronologically-ordered list of comments.

    Comments without a marker are ignored. Order is preserved so the last
    marker reflects the most recent state.
    """
    markers: list[Marker] = []
    for body in comment_bodies:
        m = _MARKER_RE.search(body or "")
        if not m:
            continue
        markers.append(
            Marker(
                op=Op(m.group("op")),
                kind=m.group("kind"),
                agent=m.group("agent"),
                run=m.group("run"),
                until=_parse_iso(m.group("until")),
            )
        )
    return markers


def active_claim(markers: Sequence[Marker], kind: str, now: datetime) -> Marker | None:
    """Return the currently-active claim for ``kind``, or ``None``.

    A claim is active when the most recent marker for that kind is a ``claim``
    whose ``until`` is still in the future. A ``release`` marker (or an expired
    claim) means there is no active claim.
    """
    latest: Marker | None = None
    for marker in markers:
        if marker.kind == kind:
            latest = marker
    if latest is None:
        return None
    if latest.op is Op.RELEASE:
        return None
    if latest.until <= now:
        return None
    return latest


def can_claim(markers: Sequence[Marker], kind: str, agent: str, now: datetime) -> bool:
    """Whether ``agent`` may claim ``kind`` right now.

    True when there is no active claim, or the active claim is already held by
    ``agent`` (claims are idempotent / re-entrant for their owner).
    """
    active = active_claim(markers, kind, now)
    return active is None or active.agent == agent


def held_by_me(markers: Sequence[Marker], kind: str, agent: str, now: datetime) -> bool:
    """Whether ``agent`` currently holds an active claim on ``kind``."""
    active = active_claim(markers, kind, now)
    return active is not None and active.agent == agent

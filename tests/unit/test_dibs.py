"""Tests for the pure dibs claim state machine."""

from datetime import datetime, timedelta, timezone

from flyte_agent_loop import dibs

NOW = datetime(2026, 7, 17, 12, 0, 0, tzinfo=timezone.utc)


def _claim(agent="agentA", kind="issue", minutes=30, run="r1"):
    return dibs.render_claim(kind, agent, run, NOW + timedelta(minutes=minutes))


def test_render_and_parse_roundtrip():
    body = _claim()
    markers = dibs.parse_markers([body])
    assert len(markers) == 1
    m = markers[0]
    assert m.op is dibs.Op.CLAIM
    assert m.kind == "issue"
    assert m.agent == "agentA"
    assert m.run == "r1"


def test_non_marker_comments_ignored():
    markers = dibs.parse_markers(["just a normal comment", "", "another one"])
    assert markers == []


def test_no_markers_means_claimable():
    markers = dibs.parse_markers(["hello"])
    assert dibs.active_claim(markers, "issue", NOW) is None
    assert dibs.can_claim(markers, "issue", "agentA", NOW) is True


def test_active_claim_blocks_other_agent():
    markers = dibs.parse_markers([_claim(agent="agentA")])
    assert dibs.can_claim(markers, "issue", "agentB", NOW) is False
    assert dibs.can_claim(markers, "issue", "agentA", NOW) is True  # re-entrant
    assert dibs.held_by_me(markers, "issue", "agentA", NOW) is True


def test_expired_claim_is_reclaimable():
    markers = dibs.parse_markers([_claim(agent="agentA", minutes=30)])
    later = NOW + timedelta(minutes=31)
    assert dibs.active_claim(markers, "issue", later) is None
    assert dibs.can_claim(markers, "issue", "agentB", later) is True


def test_release_supersedes_claim():
    claim = _claim(agent="agentA")
    release = dibs.render_release("issue", "agentA", "r1", NOW + timedelta(minutes=5))
    markers = dibs.parse_markers([claim, release])
    assert dibs.active_claim(markers, "issue", NOW + timedelta(minutes=6)) is None
    assert dibs.can_claim(markers, "issue", "agentB", NOW + timedelta(minutes=6)) is True


def test_reclaim_after_release():
    markers = dibs.parse_markers(
        [
            _claim(agent="agentA"),
            dibs.render_release("issue", "agentA", "r1", NOW + timedelta(minutes=5)),
            _claim(agent="agentB", run="r2"),
        ]
    )
    active = dibs.active_claim(markers, "issue", NOW + timedelta(minutes=6))
    assert active is not None
    assert active.agent == "agentB"


def test_kinds_are_independent():
    markers = dibs.parse_markers([_claim(agent="agentA", kind="issue")])
    # A claim on the issue does not block claiming the (same-numbered) PR kind.
    assert dibs.can_claim(markers, "pr", "agentB", NOW) is True
    assert dibs.can_claim(markers, "issue", "agentB", NOW) is False

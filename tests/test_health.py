"""Unit tests for watchtower.health.queue_status's claimable-depth gating."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from watchtower import health


def _item(status="open", readiness="", claimable=True, created_at=None, closed_at=None, claimed_at=None):
    it = {"status": status, "readiness": readiness, "claimable": claimable}
    if created_at:
        it["created_at"] = created_at
    if closed_at:
        it["closed_at"] = closed_at
    if claimed_at:
        it["claimed_at"] = claimed_at
    return it


def test_needs_shaping_and_needs_spec_are_not_claimable():
    """Tickets awaiting human shaping/spec don't count toward claimable_depth --
    claim_next won't hand them to a default worker, so counting them as
    claimable makes the spawner spin up workers that can never claim anything."""
    old = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    items = [
        _item(readiness="needs-shaping", created_at=old),
        _item(readiness="needs-spec", created_at=old),
    ]
    row = health.queue_status("Q", items)
    assert row["depth"] == 2
    assert row["claimable_depth"] == 0
    # No claimable work at all -> not "stuck", just an unshaped backlog.
    assert row["stuck"] is False


def test_ready_ticket_still_counts_as_claimable_alongside_needs_spec():
    old = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    items = [
        _item(readiness="ready", created_at=old),
        _item(readiness="needs-spec", created_at=old),
    ]
    row = health.queue_status("Q", items)
    assert row["depth"] == 2
    assert row["claimable_depth"] == 1
    assert row["stuck"] is True


def test_fresh_claim_on_old_ticket_is_not_immediately_stuck():
    """A ticket can sit open for a long time before anyone claims it. The
    instant a worker claims it, that's progress -- the queue must not read
    "stuck" (and get the worker nudged) before it's had any chance to work."""
    old = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    just_now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    items = [_item(status="in_progress", created_at=old, claimed_at=just_now)]
    row = health.queue_status("Q", items)
    assert row["since_progress_s"] < 60
    assert row["stuck"] is False


def test_stale_claim_with_no_close_is_still_stuck():
    """A claim resets the clock once, but doesn't grant amnesty forever -- if
    a worker claimed a ticket stuck_minutes ago and still hasn't closed
    anything, that's a real stuck queue."""
    old = (datetime.now(timezone.utc) - timedelta(minutes=20)).strftime("%Y-%m-%dT%H:%M:%SZ")
    items = [
        _item(status="in_progress", created_at=old, claimed_at=old),
        _item(readiness="ready", created_at=old),
    ]
    row = health.queue_status("Q", items)
    assert row["claimable_depth"] == 1
    assert row["stuck"] is True

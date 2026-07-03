"""Unit tests for watchtower.health.queue_status's claimable-depth gating."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from watchtower import health


def _item(status="open", readiness="", claimable=True, created_at=None, closed_at=None):
    it = {"status": status, "readiness": readiness, "claimable": claimable}
    if created_at:
        it["created_at"] = created_at
    if closed_at:
        it["closed_at"] = closed_at
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

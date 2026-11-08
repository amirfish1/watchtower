"""WT-15: a `wt defer`-ed ticket is owned by the clock. The liveness table
classifies it ``time.deferred`` and the backstop leaves it alone -- before,
it read as open+claimable+unstaffed and got backstop-marked (OPS-92)."""

from __future__ import annotations

import time

from liveness_golden import wt  # noqa: F401  (fixture)
from test_liveness_backstop import LATER, PQ, _iso, lv  # noqa: F401  (fixture)


def test_deferred_open_ticket_is_time_owned_and_untouched_by_the_backstop(lv):
    ref = lv.q.enqueue(project=PQ, note="waiting on data", source="test")["ref"]
    lv.q.defer(ref, "", _iso(time.time() + 2 * LATER), "waiting for a day of metrics")
    now = time.time() + LATER                       # past STALL_S, still deferred

    item = lv.q.get(ref)
    assert lv.liveness.classify(lv.liveness.project(item, now=now)).id == "time.deferred"
    assert lv.liveness.sweep(now=now) == []
    item = lv.q.get(ref)
    assert not item.get("backstop")
    assert item["status"] == "open" and lv.spawned == []

    # Once the window ends it is an ordinary claimable ticket again.
    later = time.time() + 3 * LATER
    assert lv.liveness.classify(lv.liveness.project(item, now=later)).id != "time.deferred"


def _post_window_sweeps(lv, defer: bool):
    lv.config.set_auto_drain(PQ, True)
    lv.config.set_repo_path(PQ, str(lv.tmp))
    ref = lv.q.enqueue(project=PQ, note="waiting on data", source="test")["ref"]
    if defer:
        lv.q.defer(ref, "", _iso(time.time() + 3600), "waiting for a day of metrics")
    first = lv.liveness.sweep(now=time.time() + 2 * LATER)
    second = lv.liveness.sweep(now=time.time() + 4 * LATER)
    return ref, [a["action"] for a in first], [a["action"] for a in second]


def test_second_stall_after_an_expired_defer_window_does_not_block_the_owner(lv):
    """CCC-375: staffing latency after a defer window is not an owner block."""
    ref, first, second = _post_window_sweeps(lv, defer=True)
    assert first == ["spawn"] and second == ["spawn"]
    assert not lv.q.get(ref).get("needs_input")


def test_second_stall_without_a_defer_still_escalates(lv):
    ref, first, second = _post_window_sweeps(lv, defer=False)
    assert first == ["spawn"] and second == ["escalate"]

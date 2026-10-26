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

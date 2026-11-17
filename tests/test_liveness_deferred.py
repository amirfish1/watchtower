"""WT-87: the liveness table has no timer-owned ticket state."""
from __future__ import annotations
import time
import pytest
from liveness_golden import wt  # noqa: F401
from test_liveness_backstop import LATER, PQ, _iso, lv  # noqa: F401


def test_retired_api_leaves_claim_and_liveness_unchanged(lv):
    ref = lv.q.enqueue(project=PQ, note='Implement a permanent check', source='test')['ref']
    lv.q.claim_by_ref(ref, 'w1')
    before = lv.q.get(ref)
    with pytest.raises(ValueError, match='retired'):
        lv.q.defer(ref, 'w1', _iso(time.time()+LATER), 'natural evidence')
    assert lv.q.get(ref) == before
    assert lv.liveness.classify(lv.liveness.project(before)).id == 'work.claimed'


def test_legacy_field_never_changes_admission_or_liveness_when_clock_passes(lv):
    ref = lv.q.enqueue(project=PQ, note='Actionable original work', source='test')['ref']
    it = dict(lv.q.get(ref), deferred_until=_iso(time.time()+LATER))
    before = time.time()
    assert lv.liveness.project(it, now=before) == lv.liveness.project(it, now=before+2*LATER)
    assert [x["ref"] for x in lv.q._claim_candidates([it])] == [x["ref"] for x in lv.q._claim_candidates([dict(it, deferred_until=_iso(before-1))])]
    assert all(row.id != 'time.deferred' for row in lv.liveness.ROWS)


def test_ordinary_second_stall_still_escalates(lv):
    lv.config.set_auto_drain(PQ, True)
    lv.config.set_repo_path(PQ, str(lv.tmp))
    ref = lv.q.enqueue(project=PQ, note='Unfinished work', source='test')['ref']
    first = lv.liveness.sweep(now=time.time()+2*LATER)
    second = lv.liveness.sweep(now=time.time()+4*LATER)
    assert [a['action'] for a in first] == ['spawn']
    assert [a['action'] for a in second] == ['escalate']

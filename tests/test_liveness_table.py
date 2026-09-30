"""WT-31 D2: the liveness state table held to the code.

D2.1 coverage/disjointness, D2.3 ``stages.desired()`` agreement, D2.4 frozen
vocabularies, D2.5 vocabulary mutation, D2.6 the answer-transition scan.
"""

from __future__ import annotations

import itertools
import random
import time

import pytest

import watchtower.liveness as lv
import watchtower.queue as q
import watchtower.stages as stages
from liveness_golden import BY_REF, coverage_problems, github_by_name, synth


# ------------------------------------------------------------------ D2.1
def test_every_state_has_one_row_or_is_unreachable():
    assert coverage_problems(lv) == []


def test_independent_sample_classifies_exactly_once():
    """Brute-force self-check, independent of the box algebra: random points
    of the product match exactly one row unless UNREACHABLE lists them."""
    rnd = random.Random(31)
    vocab = lv.ALL()
    for _ in range(20000):
        s = lv.State(*(rnd.choice(vocab[d]) for d in lv.DIMS))
        hits = [r.id for r in lv.ROWS
                if any(all(getattr(s, d) in b[d] for d in lv.DIMS) for b in r.boxes)]
        unreachable = any(all(getattr(s, d) in b[d] for d in lv.DIMS)
                          for u in lv.UNREACHABLE for b in u.boxes)
        assert unreachable or len(hits) == 1, (s, hits)


def test_dep_stuck_without_block_stays_reachable():
    s = lv.State(status="open", gated=False, backend="file", plan="", disc="none",
                 awaiting="none", gate_pending="none", assessment="none", block="none",
                 readiness="claimable", dep="stuck", claimed=False, parked=False,
                 answer="none")
    assert lv.classify(s).id == "dep.stuck"


def test_rows_treat_block_kinds_alike():
    """Lets D2.3 enumerate one block kind: every box admits none, all kinds,
    or both."""
    kinds = frozenset(q.BLOCK_KINDS)
    for entry in list(lv.ROWS) + list(lv.UNREACHABLE):
        for b in entry.boxes:
            ks = b["block"] - {"none"}
            assert ks in (frozenset(), kinds), entry.id


def test_undeclared_stored_values_raise():
    item = {"ref": "LQ-1", "project": "LQ", "status": "open", "gates": [],
            "plan": {"status": "sketching"}}
    with pytest.raises(lv.UndeclaredState):
        lv.project(item, BY_REF)
    with pytest.raises(lv.UndeclaredState):
        lv.project({"status": "open", "gates": [], "pending_answer":
                    {"gen": 1, "state": "affinity_expired"}}, BY_REF)
    with pytest.raises(ValueError):
        q._pa_set_state({}, {}, "affinity_expired", "now")


# ------------------------------------------------------------------ D2.3
def _agreement(sts, now, check_projection_every=7):
    items = [synth(s, i, now) for i, s in enumerate(sts)]
    want = {d["ref"]: (d["role"], d["key"]) for d in stages.desired(items)}
    for i, (s, it) in enumerate(zip(sts, items)):
        if i % check_projection_every == 0:
            assert lv.project(it, BY_REF, now) == s
        row = lv.classify(s)
        proof = lv.prove(it, row)
        got = want.get(it["ref"])
        if proof["kind"] == "stage_session":
            assert got == (proof["role"], proof["key"]), (s, row.id, got)
        else:
            assert got is None, (s, row.id, got)
        if s.answer in q.ANSWER_INFLIGHT:
            assert got is None, (s, "desired() must stay empty while an answer is in flight")


def _reachable(pools):
    pi = lv.DIMS.index("parked")
    for combo in itertools.product(*pools):
        c = list(combo)
        c[pi] = c[0] == "awaiting_answer"
        s = lv.State(*c)
        if lv.unreachable_reason(s) is None:
            yield s


def test_desired_agrees_with_stage_rows(monkeypatch):
    """Over every combination of the dims desired() or a stage row can read
    (block: one kind, see above; parked follows status): desired() emits the
    stage session iff the row's proof is that stage session."""
    monkeypatch.setattr(q, "_github_backend_for_project", github_by_name)
    vocab = lv.ALL()
    fixed = {"readiness": ("claimable",), "dep": ("ok",), "parked": (None,),
             "block": ("none", "input")}
    pools = [fixed.get(d, vocab[d]) for d in lv.DIMS]
    sts = list(_reachable(pools))
    assert len(sts) > 100000
    _agreement(sts, time.time())


def test_desired_agrees_over_readiness_and_dependencies(monkeypatch):
    monkeypatch.setattr(q, "_github_backend_for_project", github_by_name)
    vocab = lv.ALL()
    fixed = {"plan": ("", "reviewing", "accepted"), "disc": ("none",),
             "awaiting": ("none",), "gate_pending": ("none", "verify"),
             "assessment": ("none", "due"), "parked": (None,), "block": ("none", "input")}
    pools = [fixed.get(d, vocab[d]) for d in lv.DIMS]
    _agreement(list(_reachable(pools)), time.time())


# ------------------------------------------------------------------ D2.4
def test_frozen_vocabularies_equal_live_ones():
    assert lv.ALL() == lv.live_vocab()
    assert lv.SETTLED == q.ANSWER_SETTLED
    assert lv.INFLIGHT == q.ANSWER_INFLIGHT
    assert set(q._PA_STORED) == set(q.ANSWER_STATES) - {"none", "affinity_expired"}
    assert set(q.ANSWER_SETTLED) | set(q.ANSWER_INFLIGHT) == set(q.ANSWER_STATES)


# ------------------------------------------------------------------ D2.5
_VOCABS = ("VALID_STATUSES", "BACKENDS", "PLAN_STATUSES", "DISC_STATUSES", "DISC_AWAITING",
           "GATE_KINDS", "ASSESSMENT_STATUSES", "BLOCK_KINDS", "UNCLAIMABLE_READINESS",
           "DEP_VERDICTS", "ANSWER_STATES")


@pytest.mark.parametrize("name", _VOCABS)
def test_new_vocabulary_value_fails_the_table(monkeypatch, name):
    monkeypatch.setattr(q, name, tuple(getattr(q, name)) + ("x",))
    assert lv.ALL() != lv.live_vocab()                       # D2.4
    problems = coverage_problems(lv)                          # D2.1
    assert problems and "'x'" in problems[0], problems[:1]

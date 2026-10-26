"""WT-31 D2: the liveness state table held to the code.

D2.1 coverage/disjointness, D2.3 ``stages.desired()`` agreement, D2.4 frozen
vocabularies, D2.5 vocabulary mutation, D2.6 the answer-transition scan.
"""

from __future__ import annotations

import itertools
import os
import random
import time

import pytest

import watchtower.liveness as lv
import watchtower.queue as q
import watchtower.stages as stages
from liveness_golden import BY_REF, Scan, coverage_problems, github_by_name, package_sources, synth


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


_NO_GROUP = {"group": ("none",), "gplan": ("n/a",), "gint": ("n/a",), "gproof": ("n/a",)}


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
             "block": ("none", "input"), **_NO_GROUP}
    pools = [fixed.get(d, vocab[d]) for d in lv.DIMS]
    sts = list(_reachable(pools))
    assert len(sts) > 100000
    _agreement(sts, time.time())


def test_desired_agrees_over_readiness_and_dependencies(monkeypatch):
    monkeypatch.setattr(q, "_github_backend_for_project", github_by_name)
    vocab = lv.ALL()
    fixed = {"plan": ("", "reviewing", "accepted"), "disc": ("none",),
             "awaiting": ("none",), "gate_pending": ("none", "verify"),
             "assessment": ("none", "due"), "parked": (None,), "block": ("none", "input"),
             **_NO_GROUP}
    pools = [fixed.get(d, vocab[d]) for d in lv.DIMS]
    _agreement(list(_reachable(pools)), time.time())


def test_desired_agrees_over_plan_groups(monkeypatch):
    """WT-33: parents (every plan / integration / proof state) and members
    (both group-plan states): desired() emits the stage the row proves -- a
    parent's plan keys carry ``:m<mv>``, its integration verifier is the
    ``verify:<cycle>`` stage -- and nothing else."""
    monkeypatch.setattr(q, "_github_backend_for_project", github_by_name)
    vocab = lv.ALL()
    parent = {"group": ("parent",), "gplan": ("pending", "settled"), "backend": ("file",),
              "status": ("open", "in_review", "closed"), "disc": ("none", "active"),
              "assessment": ("none", "due"), "readiness": ("claimable",),
              "dep": ("ok", "waiting"), "claimed": (False,), "parked": (None,),
              "answer": ("none",), "block": ("none", "input"), "gproof": vocab["gproof"][1:],
              "gint": vocab["gint"][1:]}
    sts = list(_reachable([parent.get(d, vocab[d]) for d in lv.DIMS]))
    rows = {lv.classify(s).id for s in sts}
    assert {"group.parent.forming", "group.parent.wait", "group.parent.ready",
            "group.parent.gating", "group.parent.gating_stale", "group.parent.capped",
            "plan.start", "plan.planning", "plan.reviewing", "review.verify",
            "review.gate", "closed", "human.block"} <= rows, rows
    _agreement(sts, time.time())
    child = {"group": ("child",), "gplan": ("pending", "settled"), "gint": ("n/a",),
             "gproof": ("n/a",), "backend": ("file",), "gated": (False,),
             "plan": ("", "accepted"), "disc": ("none",), "awaiting": ("none",),
             "assessment": ("none", "due"), "readiness": ("claimable", "needs-spec"),
             "dep": ("ok", "waiting"), "parked": (None,), "block": ("none", "input")}
    sts = list(_reachable([child.get(d, vocab[d]) for d in lv.DIMS]))
    rows = {lv.classify(s).id for s in sts}
    assert {"group.child.plan_wait", "work.open", "work.claimed", "dep.waiting"} <= rows, rows
    _agreement(sts, time.time(), check_projection_every=1)


def test_done_parent_stays_classified_after_a_member_moves():
    """D8 (amended): a done parent is never re-evaluated against the live
    child map -- done x stale is the terminal row; only an unfinished
    integration (verifying / reviewing) must match its proof."""
    base = dict(status="closed", gated=True, backend="file", plan="accepted", disc="none",
                awaiting="none", gate_pending="none", assessment="none", block="none",
                readiness="claimable", dep="waiting", claimed=False, parked=False,
                answer="none", group="parent", gplan="settled", gint="done")
    for proof in ("match", "stale", "forced"):
        assert lv.classify(lv.State(**dict(base, gproof=proof))).id == "closed"
    live = dict(base, status="in_review", gate_pending="verify", gint="verifying", dep="ok")
    assert lv.classify(lv.State(**dict(live, gproof="match"))).id == "review.verify"
    for proof in ("none", "stale", "forced"):
        assert lv.unreachable_reason(lv.State(**dict(live, gproof=proof))).id == "group_proof"


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
           "DEP_VERDICTS", "ANSWER_STATES", "GROUP_ROLES", "GROUP_PLAN_STATES",
           "INTEGRATION_STATES", "GROUP_PROOF_STATES")


@pytest.mark.parametrize("name", _VOCABS)
def test_new_vocabulary_value_fails_the_table(monkeypatch, name):
    monkeypatch.setattr(q, name, tuple(getattr(q, name)) + ("x",))
    assert lv.ALL() != lv.live_vocab()                       # D2.4
    problems = coverage_problems(lv)                          # D2.1
    assert problems and "'x'" in problems[0], problems[:1]


# ------------------------------------------------------------------ D2.6
def test_scan_has_no_violations():
    """D2.6f: since phase C (verified delivery) every edge into delivered
    comes from a RECEIPT_CONFIRMED_WRITER; the former unreceipted writers
    (answers._deliver_bound, answers._check_queued) no longer write it."""
    scan = Scan(lv, q)
    assert scan.violations == [], scan.violations
    # the scan sees every wrapper call site it should
    assert {"route_answer", "_fallback_reopen", "_retry", "route_pending_answers",
            "_confirm_stage_answer", "_on_answer_confirmed"} <= set(scan.edges)
    into_delivered = {fn for fn, edges in scan.edges.items()
                      if any(new == "delivered" and old != "delivered" for old, new, _, _ in edges)}
    assert into_delivered <= set(lv.RECEIPT_CONFIRMED_WRITERS), into_delivered


def test_inlock_writer_edges_are_declared():
    ids = {e["id"] for e in q.ANSWER_TRANSITIONS}
    for fn, edges in lv.PA_INLOCK_WRITERS.items():
        assert set(edges) <= ids, fn
        for e in edges:
            assert fn in next(x for x in q.ANSWER_TRANSITIONS if x["id"] == e)["writers"]


def _mutated(module, old=None, new=None, append=""):
    src = package_sources()
    if old is not None:
        assert src[module].count(old) == 1, old
        src[module] = src[module].replace(old, new)
    src[module] += append
    return src


def _new_violations(src):
    base = set(Scan(lv, q).violations)
    return set(Scan(lv, q, src).violations) - base


_MUTATIONS = {
    "bogus_to_state": dict(module="answers", append=(
        "\n\ndef _m(ref, gen):\n"
        "    q.pa_transition(ref, gen, 'routing', 'bogus', from_status='awaiting_answer')\n")),
    "undeclared_valid_edge": dict(module="answers", append=(
        "\n\ndef _m(ref, gen):\n"
        "    q.pa_transition(ref, gen, 'affinity', 'delivering', from_status='open')\n")),
    "variable_from_state": dict(module="answers", append=(
        "\n\ndef _m(ref, gen, s):\n"
        "    q.pa_transition(ref, gen, s, 'handed_off', from_status='open')\n")),
    "wrapper_hardcodes_delivered": dict(
        module="queue", old="_pa_set_state(it, pa, to_state, now)",
        new="_pa_set_state(it, pa, 'delivered', now)"),
    "new_pa_set_state_caller": dict(module="queue", append=(
        "\n\ndef _m(it, pa):\n    _pa_set_state(it, pa, 'handed_off', 'now')\n")),
    "new_pa_cas_caller": dict(module="queue", append=(
        "\n\ndef _m(ref, gen):\n    return _pa_cas(ref, gen, 'routing', 'open', None)\n")),
    "delivered_outside_receipt_writers": dict(module="answers", append=(
        "\n\ndef _on_answer_confirmed_(ref, gen):\n"
        "    q.pa_transition(ref, gen, 'delivering', 'delivered', from_status='in_progress')\n")),
    "non_literal_reopen": dict(module="answers", append=(
        "\n\ndef _fallback_reopen_(ref, gen, r):\n"
        "    q.pa_transition(ref, gen, 'routing', 'handed_off', from_status='awaiting_answer',"
        " reopen=r)\n")),
    "pa_state_write": dict(module="answers", append=(
        "\n\ndef _m(pa):\n    pa['state'] = 'delivered'\n")),
}


@pytest.mark.parametrize("name", sorted(_MUTATIONS))
def test_scan_mutation_fails(name):
    assert _new_violations(_mutated(**_MUTATIONS[name])), name


def test_runtime_edge_check_raises_on_an_undeclared_mutate(monkeypatch):
    assert os.environ.get("WATCHTOWER_STRICT_EDGES") == "1"
    it = {"ref": "LQ-1", "status": "open"}
    with pytest.raises(q.UndeclaredEdge):
        q._check_answer_edge(it, ("affinity", "delivering", "open", "open"))
    q._check_answer_edge(it, ("affinity", "handed_off", "open", "open"))   # E11
    monkeypatch.delenv("WATCHTOWER_STRICT_EDGES")
    logged = []
    monkeypatch.setattr(q, "_log", lambda kind, msg, **kw: logged.append(kind))
    q._check_answer_edge(it, ("affinity", "delivering", "open", "open"))   # logs only
    assert logged == ["ANSWER_EDGE_UNDECLARED"]

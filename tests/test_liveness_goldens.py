"""WT-31 D2.2 / D2.9-D2.11: real transitions on a temp store.

Every step goes through the real queue/answers/stages code and asserts the
edge it took (declared, with the called function as writer or caller), the
row the ticket lands in, its owner and ``desired()``.
"""

from __future__ import annotations

import pytest

from liveness_golden import SID, SID2, Golden, pa_state, wt  # noqa: F401  (fixture)

PQ = "LQ"


def _claimed(wt, worker="w1", sid=SID, **kw):
    it = wt.q.enqueue(project=PQ, note="ticket", source="test", **kw)
    wt.q.claim_by_ref(it["ref"], worker, session_uuid=sid)
    return it["ref"]


def _set_plan(wt, ref, status):
    def _do(it, plan):
        plan.update(status=status, round=1)
    wt.q._plan_update(ref, _do)


def _gate_on(wt, ref, status="planning"):
    wt.q.update(ref, gates=["plan"])
    if status:
        _set_plan(wt, ref, status)


def _gen(wt, ref):
    return int(wt.q.get(ref)["pending_answer"]["gen"])


def _parked_routing(wt, g):
    """claim -> worker block (parks) -> human answer (E1)."""
    g.step(wt.q.block, g.ref, session_id="w1", question="which way?", origin="worker",
           row="answer.await_human", owner="human", desired=[])
    g.step(wt.q.answer, g.ref, "left", edge="E1", row="answer.routing", desired=[])


def _handed_off_open(wt, g):
    _parked_routing(wt, g)
    g.step(wt.answers.route_answer, g.ref, _gen(wt, g.ref), edge="E5",
           row="work.open", desired=[])


# ------------------------------------------------------------ plain answers
def test_park_answer_handoff_claim(wt):
    g = Golden(wt, _claimed(wt))
    g.check(row="work.claimed")
    _handed_off_open(wt, g)
    got = g.step(wt.q.claim_next, "w2", project=PQ, edge="E12", row="work.claimed")
    assert got["ref"] == g.ref and got["pending_answer"]["state"] == "delivered"


def test_unowned_in_progress_is_a_reconciler_row(wt):
    """D3: ``update_status(in_progress, worker='')`` -> ``work.unowned``, owned by
    the reconciler (the backstop reopens it); a claim moves it to work.claimed."""
    g = Golden(wt, wt.q.enqueue(project=PQ, note="ticket", source="test")["ref"])
    g.check(row="work.open")
    g.step(wt.q.update_status, g.ref, "in_progress", "", row="work.unowned",
           owner="reconciler", desired=[])
    assert wt.liveness.ROWS_BY_ID["work.unowned"].recover == "reopen"
    g.step(wt.q.update_status, g.ref, "open", "", row="work.open", desired=[])
    g.step(wt.q.claim_next, "w2", project=PQ, row="work.claimed", owner="worker")


def test_affinity_then_claim_by_prior_worker(wt, monkeypatch):
    g = Golden(wt, _claimed(wt))
    _parked_routing(wt, g)
    monkeypatch.setattr(wt.answers, "worker_alive", lambda wid: True)
    g.step(wt.answers.route_answer, g.ref, _gen(wt, g.ref), edge="E4", row="answer.affinity",
           desired=[])
    g.step(wt.q.claim_next, "w1", project=PQ, edge="E12", row="work.claimed")


def test_affinity_expires(wt, monkeypatch):
    g = Golden(wt, _claimed(wt))
    _parked_routing(wt, g)
    monkeypatch.setattr(wt.answers, "worker_alive", lambda wid: True)
    monkeypatch.setattr(wt.answers, "affinity_s", lambda: -60)
    g.step(wt.answers.route_answer, g.ref, _gen(wt, g.ref), edge="E4",
           row="answer.affinity_expired", desired=[])
    g.step(wt.answers.route_pending_answers, edge="E11", row="work.open")


def _resuming(wt, g, monkeypatch, status):
    """Parked + answered + resumable: resume_claim (E3), then ``_deliver``
    reports ``status``."""
    _parked_routing(wt, g)
    monkeypatch.setattr(wt.answers, "_resumable", lambda engine, sid: True)
    monkeypatch.setattr(wt.answers, "_deliver",
                        lambda item, pa: {"status": status, "msg_id": "m1", "transport": "t"})


def test_resume_in_flight_then_gives_up(wt, monkeypatch):
    g = Golden(wt, _claimed(wt))
    _resuming(wt, g, monkeypatch, "in_flight")
    g.step(wt.answers.route_answer, g.ref, _gen(wt, g.ref), edge="E3",
           row="answer.delivering", desired=[])
    it = g.item()
    assert it["claim_proc"]["session_id"] == SID
    monkeypatch.setattr(wt.answers, "ROUTE_LEASE_S", 0)
    monkeypatch.setattr(wt.answers, "MAX_DELIVERY_ATTEMPTS", 1)
    g.step(wt.answers.route_pending_answers, edge="E10", row="work.open")


def test_queued_then_row_gone_then_redelivered(wt, monkeypatch):
    g = Golden(wt, _claimed(wt))
    _resuming(wt, g, monkeypatch, "queued")
    gen = _gen(wt, g.ref)
    bound = g.step(wt.q.resume_claim, g.ref, gen, edge="E3", row="answer.delivering")
    g.step(wt.answers._deliver_bound, bound, gen, edge="E6", row="answer.queued", desired=[])
    import watchtower.messages as messages
    monkeypatch.setattr(messages, "outbox_row", lambda mid: None)
    monkeypatch.setattr(wt.answers, "_deliver",
                        lambda item, pa: {"status": "in_flight"})
    g.step(wt.answers.route_pending_answers, edge="E7", row="answer.delivering")


def test_queued_pending_is_a_noop(wt, monkeypatch):
    g = Golden(wt, _claimed(wt))
    _resuming(wt, g, monkeypatch, "queued")
    wt.answers.route_answer(g.ref, _gen(wt, g.ref))
    import watchtower.messages as messages
    monkeypatch.setattr(messages, "outbox_row", lambda mid: {"status": "pending"})
    g.step(wt.answers.route_pending_answers, row="answer.queued")


# ------------------------------------------------------------- R1 reopens
def test_delivered_release_reopens_then_next_claim_gets_it(wt):
    g = Golden(wt, _claimed(wt))
    _handed_off_open(wt, g)
    wt.q.claim_next("w2", project=PQ)
    g.step(wt.q.release, g.ref, edge="E14", row="work.open")
    g.step(wt.q.claim_next, "w3", project=PQ, edge="E12", row="work.claimed")


def test_delivered_reopen_and_orphan_sweep(wt):
    g = Golden(wt, _claimed(wt))
    _handed_off_open(wt, g)
    wt.q.claim_next("w2", project=PQ)
    g.step(wt.q.reopen, g.ref, reason="triage", edge="E14", row="work.open")


def test_delivering_release_then_late_confirm_is_a_noop(wt, monkeypatch):
    g = Golden(wt, _claimed(wt))
    _resuming(wt, g, monkeypatch, "in_flight")
    gen = _gen(wt, g.ref)
    wt.answers.route_answer(g.ref, gen)
    g.step(wt.q.release, g.ref, edge="E14", row="work.open")
    assert wt.q.pa_transition(g.ref, gen, ("delivering", "queued"), "delivered",
                              from_status="in_progress") is None


def test_forced_release_of_parked_routing_discards(wt):
    g = Golden(wt, _claimed(wt))
    _parked_routing(wt, g)
    g.step(wt.q.release, g.ref, force=True, edge="E15", row="work.open")


def test_close_clears_the_answer(wt):
    g = Golden(wt, _claimed(wt))
    _handed_off_open(wt, g)
    wt.q.claim_next("w2", project=PQ)
    g.step(wt.q.close, g.ref, force=True, edge="E16", row="closed")


def test_block_on_delivered_parks_and_carries(wt):
    g = Golden(wt, _claimed(wt))
    _handed_off_open(wt, g)
    wt.q.claim_next("w2", project=PQ)
    g.step(wt.q.block, g.ref, session_id="w2", question="again?", origin="worker",
           edge="E17", row="answer.await_human")
    carried = g.item()["carried_answers"]
    assert carried and carried[-1]["answer"] == "left"


def test_update_needs_input_supersedes(wt):
    g = Golden(wt, _claimed(wt))
    _handed_off_open(wt, g)
    g.step(wt.q.update, g.ref, needs_input=True, block_question="hm?", edge="E17",
           row="human.block")


# ------------------------------------------------ R6-1 block supersession
def test_r6_1_block_from_open_handed_off_under_plan_gate(wt):
    g = Golden(wt, _claimed(wt))
    _handed_off_open(wt, g)
    _gate_on(wt, g.ref)
    g.check(row="plan.planning", desired=["planner"])
    g.step(wt.q.block, g.ref, session_id="w9", question="plan q?", origin="worker",
           edge="E17", row="human.block", owner="human", desired=[])
    it = g.item()
    assert it["status"] == "in_progress" and not it.get("parked")


def test_r6_1_block_from_open_delivered_under_plan_gate(wt):
    g = Golden(wt, _claimed(wt))
    _handed_off_open(wt, g)
    _gate_on(wt, g.ref)
    g.step(wt.stages._confirm_stage_answer, g.ref, _gen(wt, g.ref), edge="E13",
           row="plan.planning", desired=["planner"])
    assert g.item()["status"] == "open"
    g.step(wt.q.block, g.ref, session_id="w9", question="plan q?", origin="worker",
           edge="E17", row="human.block", desired=[])
    assert g.item()["status"] == "in_progress"


def test_r6_1_block_from_open_handed_off_parks_without_plan_gate(wt):
    g = Golden(wt, _claimed(wt))
    _handed_off_open(wt, g)
    g.step(wt.q.block, g.ref, session_id="w9", question="q?", origin="worker",
           edge="E17", row="answer.await_human")


def test_new_undeclared_edge_fails(wt, monkeypatch):
    """Mutation: an E17 without the R6-1 status pair must fail the runtime
    check (strict in tests)."""
    g = Golden(wt, _claimed(wt))
    _handed_off_open(wt, g)
    _gate_on(wt, g.ref)
    narrowed = wt.q._ANSWER_EDGE_SET - {("handed_off", "none", "open", "in_progress")}
    monkeypatch.setattr(wt.q, "_ANSWER_EDGE_SET", narrowed)
    with pytest.raises(wt.q.UndeclaredEdge):
        wt.q.block(g.ref, session_id="w9", question="q?", origin="worker")
    assert pa_state(wt.q.get(g.ref)) == "handed_off"   # nothing saved


# ------------------------------------------------------- gate after park
def test_gate_on_while_routing_hands_to_plan_stage(wt):
    g = Golden(wt, _claimed(wt))
    _parked_routing(wt, g)
    _gate_on(wt, g.ref)
    g.check(row="answer.plan_conflict", desired=[])
    out = g.step(wt.answers.route_answer, g.ref, _gen(wt, g.ref), edge="E5",
                 row="plan.planning", desired=["planner"])
    assert out["reason"] == wt.answers.PLAN_ACTIVE_REASON
    goal = wt.stages._goal(g.item(), "planner", token="t", respawn=False, note="")
    assert "left" in goal
    g.step(wt.stages._confirm_stage_answer, g.ref, _gen(wt, g.ref), edge="E13",
           row="plan.planning")


def test_gate_on_during_affinity(wt, monkeypatch):
    g = Golden(wt, _claimed(wt))
    _parked_routing(wt, g)
    monkeypatch.setattr(wt.answers, "worker_alive", lambda wid: True)
    wt.answers.route_answer(g.ref, _gen(wt, g.ref))
    _gate_on(wt, g.ref)
    g.check(row="answer.plan_conflict", desired=[])
    g.step(wt.answers.route_pending_answers, edge="E11", row="plan.planning",
           desired=["planner"])


@pytest.mark.parametrize("status", ["in_flight", "queued"])
def test_gate_on_during_delivery(wt, monkeypatch, status):
    g = Golden(wt, _claimed(wt))
    _resuming(wt, g, monkeypatch, status)
    wt.answers.route_answer(g.ref, _gen(wt, g.ref))
    _gate_on(wt, g.ref)
    g.check(row="answer.plan_conflict", desired=[])
    import watchtower.messages as messages
    monkeypatch.setattr(messages, "outbox_row", lambda mid: {"status": "pending"})
    g.step(wt.answers.route_pending_answers, edge="E10", row="plan.planning",
           desired=["planner"])


# ----------------------------------------------------------- D2.9 dep.stuck
def test_dep_stuck_escalates_then_answer_frees_it(wt):
    blocker = wt.q.enqueue(project=PQ, note="blocker", source="test")["ref"]
    dep = wt.q.enqueue(project=PQ, note="dependent", source="test", blocked_by=[blocker])["ref"]
    g = Golden(wt, dep)
    g.check(row="dep.waiting")
    wt.q.claim_by_ref(blocker, "w1", session_uuid=SID)
    wt.q.close(blocker, force=True, resolution={"summary": "half", "unresolved": ["x"]})
    g.check(row="human.block", owner="human", desired=[])
    g.step(wt.q.answer, dep, "go anyway", row="work.open")


# ------------------------------------------------------ D2.10 routing floor
def test_stale_routing_floor_then_claim(wt):
    g = Golden(wt, _claimed(wt))
    _parked_routing(wt, g)
    import time
    g.step(wt.answers.route_pending_answers, time.time() + 3600, edge="E5", row="work.open")
    g.step(wt.q.claim_next, "w2", project=PQ, edge="E12")


def test_gen_bumping_block_fails_the_router_cleanly(wt):
    g = Golden(wt, _claimed(wt))
    _parked_routing(wt, g)
    gen = _gen(wt, g.ref)
    g.step(wt.q.block, g.ref, session_id="w1", question="more?", origin="worker",
           edge="E17", row="answer.await_human")
    assert wt.answers.route_answer(g.ref, gen) == {"route": "noop", "reason": "nothing to route"}
    assert wt.q.pa_transition(g.ref, gen, "routing", "handed_off",
                              from_status="awaiting_answer", reopen=True) is None


# ---------------------------------------------------- D2.11 park boundary
_PA_PLANS = ("", "planning", "reviewing", "discussing", "blocked")


@pytest.mark.parametrize("plan", _PA_PLANS)
@pytest.mark.parametrize("start", ["open", "in_progress"])
def test_worker_block_never_parks_under_plan_gate(wt, plan, start):
    ref = (_claimed(wt) if start == "in_progress"
           else wt.q.enqueue(project=PQ, note="t", source="test")["ref"])
    _gate_on(wt, ref, plan)
    it = wt.q.block(ref, session_id="w1", question="q?", origin="worker")
    assert it["status"] == "in_progress" and not it.get("parked")
    assert wt.liveness.row_of(it).owner == "human"
    assert wt.q.migrate_legacy_blocks() == []
    g = Golden(wt, ref)
    g.step(wt.q.answer, ref, "fine")
    assert g.check().id.startswith(("plan.", "work.", "human."))


@pytest.mark.parametrize("plan", _PA_PLANS)
def test_parked_answer_under_plan_gate_never_resumes(wt, monkeypatch, plan):
    g = Golden(wt, _claimed(wt))
    _parked_routing(wt, g)
    _gate_on(wt, g.ref, plan)
    monkeypatch.setattr(wt.answers, "_resumable", lambda engine, sid: True)
    assert wt.q.resume_claim(g.ref, _gen(wt, g.ref)) is None
    out = g.step(wt.answers.route_answer, g.ref, _gen(wt, g.ref), edge="E5", desired=None)
    assert out == {"route": "reopen", "reason": wt.answers.PLAN_ACTIVE_REASON}
    assert g.item()["status"] == "open"


# ------------------------------------------------------------- plan stage
def test_plan_reject_at_cap_blocks_for_a_human(wt):
    ref = wt.q.enqueue(project=PQ, note="t", source="test", gates=["plan"])["ref"]
    g = Golden(wt, ref)
    g.check(row="plan.start", desired=["planner"])
    g.step(wt.q.plan_start, ref, row="plan.planning", desired=["planner"])
    g.step(wt.q.plan_submit, ref, "plan v1", row="plan.reviewing", desired=["plan_reviewer"])
    wt.q._plan_update(ref, lambda it, plan: plan.update(revision_limit=0))
    g.step(wt.q.plan_verdict, ref, False, "no", row="plan.blocked", desired=[])
    g.step(wt.q.block, ref, question="plan rejected", origin="plan_gate",
           row="human.block", owner="human", desired=[])

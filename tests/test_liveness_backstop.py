"""WT-31 D3/D5: the liveness backstop (``liveness.sweep``), ``recover_claim``
races, the orphan fast path through the resolver, ``wt liveness`` and the
health changes. Real store; worker processes are real dead/live children."""

from __future__ import annotations

import datetime as _dt
import json
import os
import subprocess
import time

import pytest

from liveness_golden import SID, SID2, Golden, github_by_name, wt  # noqa: F401  (fixture)

PQ = "LQ"
LATER = 3 * 3600.0   # well past STALL_S


def _iso(ts: float) -> str:
    return _dt.datetime.fromtimestamp(ts, _dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _worker(wt, wid: str, sid: str, engine: str = "claude"):
    p = subprocess.Popen(["sleep", "60"])
    rec = {"worker_id": wid, "session_id": sid, "engine": engine, "pid": p.pid,
           "pid_started": wt.workers._pid_start_token(p.pid),
           "started_at": _iso(time.time() - 5), "exit_file": "", "queue": PQ}
    data = wt.workers._load()
    data["workers"].append(rec)
    wt.workers._save(data)
    return p


@pytest.fixture()
def lv(wt, tmp_path, monkeypatch):
    (tmp_path / "claude-home" / "sessions").mkdir(parents=True)   # registry readable
    # ``config`` caches its path at import; the golden fixture does not reload
    # it, so pin it here or ``set_auto_drain`` writes a real config file.
    import watchtower.config as config
    monkeypatch.setattr(config, "CONFIG_FILE", tmp_path / "config.json")
    wt.config = config
    procs = []

    def worker(wid="w1", sid=SID, engine="claude"):
        p = _worker(wt, wid, sid, engine)
        procs.append(p)
        return p

    def kill():
        for p in procs:
            if p.poll() is None:
                p.kill()
                p.wait()
    wt.worker, wt.kill = worker, kill
    wt.stages_run = []
    monkeypatch.setattr(wt.stages, "reconcile_stages",
                        lambda only_ref="": wt.stages_run.append(only_ref) or [])
    wt.spawned = []
    monkeypatch.setattr(wt.workers, "spawn_workers",
                        lambda queue, n=1, **k: wt.spawned.append((queue, n, k)) or [])
    wt.resumed = []
    monkeypatch.setattr(wt.cli, "_resume_rejected",
                        lambda item, reason, engine="": wt.resumed.append(item["ref"]) or 0)
    yield wt
    for p in procs:
        if p.poll() is None:
            p.kill()
            p.wait()


def _file(wt, note="t", **kw):
    return wt.q.enqueue(project=PQ, note=note, source="test", **kw)["ref"]


def _claimed(wt, worker="w1", sid=SID, **kw):
    ref = _file(wt, **kw)
    wt.q.claim_by_ref(ref, worker, session_uuid=sid)
    return ref


def _sweep(wt, off=LATER, **kw):
    return wt.liveness.sweep(now=time.time() + off, **kw)


def _log(wt) -> str:
    try:
        return open(os.environ["WATCHTOWER_ACTIVITY_LOG"]).read()
    except OSError:
        return ""


def _patch(wt, ref, **fields):
    data = wt.q._load_unlocked()
    for it in data["items"]:
        if it["ref"] == ref:
            it.update(fields)
    wt.q._save_unlocked(data)


def _assess(wt, ref, off=LATER):
    items = wt.q.list_items()
    return wt.liveness.assess(wt.q.get(ref), wt.q._refs_index(items),
                              wt.liveness.ResolverContext(time.time() + off))


# --------------------------------------------------------- recover_claim
def _snapshot(wt, ref):
    it = wt.q.get(ref)
    return it, wt.liveness._expect(it, wt.q._refs_index(wt.q.list_items()))


def test_recover_claim_reopens_and_records_the_backstop(lv):
    ref = _claimed(lv)
    _, exp = _snapshot(lv, ref)
    it = lv.q.recover_claim(ref, expect=exp, action="reopen", reason="gone",
                            evidence="dead: process gone", state="work.claimed")
    assert it["status"] == "open" and it["claimed_by"] is None
    bs = it["backstop"]
    assert bs["state"] == "work.claimed" and bs["action"] == "reopen" and bs["count"] == 1
    assert bs["fingerprint"] == lv.liveness.fingerprint(it, lv.q._refs_index(lv.q.list_items()))
    assert [h["event"] for h in it["history"]][-2:] == ["reopen", "backstop"]
    assert it["history"][-2]["displaced_session_id"] == SID
    assert it["prior_claim_proc"]["worker_id"] == "w1"


@pytest.mark.parametrize("race", ["reclaim", "block", "answer", "comment", "rebind"])
def test_recover_claim_race_is_a_noop(lv, race):
    ref = _claimed(lv)
    _, exp = _snapshot(lv, ref)
    q = lv.q
    if race == "reclaim":
        q.release(ref)
        q.claim_by_ref(ref, "w2", session_uuid=SID2)
    elif race == "block":
        q.block(ref, "", question="?", origin="system")
    elif race == "answer":
        q.answer(ref, "go on")
    elif race == "comment":
        q.comment(ref, "still here")
    else:
        data = q._load_unlocked()
        for it in data["items"]:
            if it["ref"] == ref:
                it["claim_proc"] = dict(it["claim_proc"], pid=1)
        q._save_unlocked(data)
    before = q.get(ref)
    assert q.recover_claim(ref, expect=exp, action="reopen", reason="x",
                           state="work.claimed") is None
    after = q.get(ref)
    assert after == before
    assert "BACKSTOP_SKIP" in _log(lv)


def test_recover_claim_is_local_only(lv, monkeypatch):
    monkeypatch.setattr(lv.q, "_github_backend_for_project", github_by_name)
    assert lv.q.recover_claim("GH-1", expect={}, action="reopen", reason="x") is None


def test_recover_claim_reopen_hands_off_a_delivered_answer(lv):
    g = Golden(lv, _claimed(lv))
    g.step(lv.q.block, g.ref, session_id="w1", question="?", origin="worker")
    g.step(lv.q.answer, g.ref, "yes", edge="E1")
    g.step(lv.answers.route_answer, g.ref, int(g.item()["pending_answer"]["gen"]), edge="E5")
    lv.q.claim_next("w2", project=PQ)
    assert g.item()["pending_answer"]["state"] == "delivered"
    _, exp = _snapshot(lv, g.ref)
    g.step(lv.q.recover_claim, g.ref, expect=exp, action="reopen", reason="gone",
           state="work.claimed", edge="E14", row="work.open")


# ---------------------------------------------------------- work.claimed
def test_dead_owner_is_reopened_only_after_stall(lv):
    lv.worker()
    ref = _claimed(lv)
    lv.kill()
    assert _sweep(lv, off=60) == []                       # idle < STALL_S
    acted = _sweep(lv)
    assert [(a["ref"], a["action"], a["result"]) for a in acted] == [(ref, "reopen", "done")]
    it = lv.q.get(ref)
    assert it["status"] == "open" and it["backstop"]["action"] == "reopen"
    line = [l for l in _log(lv).splitlines() if "BACKSTOP " in l][-1]
    for part in (f"{ref} state=work.claimed", "owner=worker", "proof=claim_owner",
                 "verdict=dead", "action=reopen", "idle="):
        assert part in line


def test_alive_owner_is_never_touched(lv):
    lv.worker()
    ref = _claimed(lv)
    assert _sweep(lv) == []
    assert lv.q.get(ref)["status"] == "in_progress"


def test_unproven_owner_escalates_then_stays_human(lv):
    ref = _claimed(lv, worker="someone", sid=SID2)            # ambient: never dead
    acted = _sweep(lv)
    assert [a["action"] for a in acted] == ["escalate"]
    it = lv.q.get(ref)
    assert it["needs_input"] and "backstop" in it["block_question"]
    assert it["status"] == "in_progress" and it["claimed_by"] == "someone"
    assert lv.liveness.row_of(it).id == "human.block"
    assert _sweep(lv) == []


def test_github_claim_escalates_through_block(lv, monkeypatch):
    blocked = []
    item = {"ref": "GH-1", "project": "GH", "status": "in_progress", "claimed_by": "x",
            "claimed_session_id": SID2, "updated_at": "2000-01-01T00:00:00Z", "history": []}
    monkeypatch.setattr(lv.q, "_github_backend_for_project", github_by_name)
    monkeypatch.setattr(lv.q, "list_items", lambda *a, **k: [dict(item)])
    monkeypatch.setattr(lv.q, "block", lambda ref, sid="", **k: blocked.append(ref) or {"ref": ref})
    acted = _sweep(lv)
    assert [a["action"] for a in acted] == ["escalate"] and blocked == ["GH-1"]


def test_unowned_in_progress_is_reopened_after_stall(lv):
    """``update_status(in_progress, worker='')`` leaves a non-terminal ticket
    with no claimer, no live owner and no human block (``work.unowned``). The
    backstop reopens it after STALL_S idle instead of skipping it forever."""
    ref = _file(lv)
    lv.q.update_status(ref, "in_progress", "")
    it = lv.q.get(ref)
    assert it["claimed_by"] is None and not it.get("needs_input")
    assert lv.liveness.row_of(it).id == "work.unowned"
    a = _assess(lv, ref, off=60)
    assert a["row"] == "work.unowned" and a["action"] == ""     # idle < STALL_S
    a = _assess(lv, ref)
    assert (a["owner"], a["verdict"], a["action"]) == ("reconciler", "unowned", "reopen")
    assert _sweep(lv, off=60) == []
    acted = _sweep(lv)
    assert [(a["ref"], a["row"], a["action"], a["result"]) for a in acted] == \
        [(ref, "work.unowned", "reopen", "done")]
    it = lv.q.get(ref)
    assert it["status"] == "open" and it["backstop"]["state"] == "work.unowned"
    assert lv.liveness.row_of(it).id == "work.open"
    line = [l for l in _log(lv).splitlines() if "BACKSTOP " in l][-1]
    for part in (f"{ref} state=work.unowned", "owner=reconciler", "verdict=unowned",
                 "action=reopen"):
        assert part in line


def test_unowned_with_a_live_bound_session_is_left_alone(lv, monkeypatch):
    ref = _file(lv)
    lv.q.update_status(ref, "in_progress", "")
    _patch(lv, ref, claim_proc={"worker_id": "", "session_id": SID, "engine": "claude",
                                "pid": 0, "bound": "ambient"})
    monkeypatch.setattr(lv.liveness, "_registry_alive", lambda sid: sid == SID)
    a = _assess(lv, ref)
    assert a["row"] == "work.unowned" and a["verdict"] == "alive" and a["action"] == ""
    assert _sweep(lv) == []
    assert lv.q.get(ref)["status"] == "in_progress"


def test_github_unowned_escalates_through_block(lv, monkeypatch):
    blocked = []
    item = {"ref": "GH-2", "project": "GH", "status": "in_progress", "claimed_by": None,
            "updated_at": "2000-01-01T00:00:00Z", "history": []}
    monkeypatch.setattr(lv.q, "_github_backend_for_project", github_by_name)
    monkeypatch.setattr(lv.q, "list_items", lambda *a, **k: [dict(item)])
    monkeypatch.setattr(lv.q, "block", lambda ref, sid="", **k: blocked.append(ref) or {"ref": ref})
    acted = _sweep(lv)
    assert [(a["row"], a["action"]) for a in acted] == [("work.unowned", "escalate")]
    assert blocked == ["GH-2"]


def test_live_sent_back_claim_is_left_to_its_release_window(lv):
    lv.worker()
    ref = _claimed(lv)
    lv.q.update_status(ref, "closed", "w1", hold_review="review")
    lv.q.reject_with(ref, "not yet")
    lv.kill()
    assert lv.q.get(ref)["sent_back"]
    a = _assess(lv, ref, off=10 * 60)
    assert a["action"] == "" and "release window" in a["evidence"]
    os.environ["WATCHTOWER_STALL_S"] = "60"
    try:
        assert _sweep(lv, off=10 * 60) == []
    finally:
        del os.environ["WATCHTOWER_STALL_S"]


def test_dead_rejected_builder_is_resumed_first_then_reopened(lv, monkeypatch):
    lv.worker()
    ref = _claimed(lv)
    lv.q.update_status(ref, "closed", "w1", hold_review="review")
    lv.q.reject_with(ref, "missing test")
    lv.kill()
    monkeypatch.setattr(lv.answers, "_resumable", lambda engine, sid: True)
    acted = _sweep(lv)
    assert [a["action"] for a in acted] == ["resume"] and lv.resumed == [ref]
    it = lv.q.get(ref)
    assert it["status"] == "in_progress" and it["claim_proc"]["resumed_at"]
    assert not it.get("needs_input")
    # the resumed session made no progress: same fingerprint -> handoff reopen
    acted = _sweep(lv)
    assert [a["action"] for a in acted] == ["reopen"]
    it = lv.q.get(ref)
    assert it["status"] == "open" and it["gate_feedback"].endswith("missing test")
    assert not it.get("needs_input")


def test_orphan_fast_path_resumes_first_and_reopens_through_recover_claim(lv, monkeypatch):
    lv.worker()
    ref = _claimed(lv)
    lv.q.update_status(ref, "closed", "w1", hold_review="review")
    lv.q.reject_with(ref, "missing test")
    lv.kill()
    base = time.time()
    monkeypatch.setattr(time, "time", lambda: base + 600)
    monkeypatch.setattr(lv.answers, "_resumable", lambda engine, sid: True)
    assert lv.workers.requeue_orphaned_tickets() == []
    assert lv.resumed == [ref]
    assert lv.q.get(ref)["backstop"]["action"] == "resume"
    # not resumable -> the fast path reopens with the rejection kept
    lv.q.set_resume_state(ref, SID, "failed", error="gone")
    monkeypatch.setattr(lv.answers, "_resumable", lambda engine, sid: False)
    reopened = lv.workers.requeue_orphaned_tickets()
    assert [i["ref"] for i in reopened] == [ref]
    it = lv.q.get(ref)
    assert it["status"] == "open" and it["gate_feedback"] and it["backstop"]["action"] == "reopen"


def test_orphan_fast_path_never_reopens_a_provably_alive_session(lv, monkeypatch):
    lv.worker()                               # the spawned process dies...
    ref = _claimed(lv)
    lv.kill()
    monkeypatch.setattr(lv.liveness, "_registry_alive", lambda sid: sid == SID)  # ...its session lives
    base = time.time()
    monkeypatch.setattr(time, "time", lambda: base + 600)
    assert lv.workers.requeue_orphaned_tickets() == []
    assert lv.q.get(ref)["status"] == "in_progress"


# ------------------------------------------------------------ stage rows
def _verify_pending(wt):
    ref = _claimed(wt, gates=["verify"])
    wt.q.update_status(ref, "closed", "w1", hold_review="verify")
    return ref


def test_stalled_stage_row_runs_the_supervisor_then_escalates(lv):
    ref = _verify_pending(lv)
    assert _assess(lv, ref, off=60)["action"] == ""
    acted = _sweep(lv)
    assert [a["action"] for a in acted] == ["stage"] and lv.stages_run == [ref]
    assert lv.q.get(ref)["backstop"]["action"] == "mark"
    acted = _sweep(lv)                        # nothing moved: loop guard
    assert [a["action"] for a in acted] == ["escalate"]
    assert lv.liveness.row_of(lv.q.get(ref)).id == "human.block"


def test_stage_row_without_supervisor_escalates(lv, monkeypatch):
    ref = _verify_pending(lv)
    monkeypatch.setattr(lv.stages, "desired", lambda items: [])
    a = _assess(lv, ref)
    assert a["action"] == "escalate" and "no supervisor for state review.verify" in a["reason"]


def test_live_stage_session_is_alive(lv):
    ref = _verify_pending(lv)
    lv.worker("ver-1", SID2)
    lv.q.stage_session_update(ref, lambda it, ss: ss.update(
        key="verify:1", role="verifier", worker_id="ver-1"))
    a = _assess(lv, ref)
    assert a["verdict"] == "alive" and a["action"] == ""


def test_stalled_filing_resumes_the_assessment_ops(lv, monkeypatch):
    ref = _file(lv)
    lv.q.update_status(ref, "closed", "w1")
    data = lv.q._load_unlocked()
    for it in data["items"]:
        if it["ref"] == ref:
            it["assessment"] = {"status": "filing", "cycle": 1, "token": "t",
                                "pending": {"ops": []}}
    lv.q._save_unlocked(data)
    ran = []
    monkeypatch.setattr(lv.q, "assessment_run_ops", lambda r, token="": ran.append(r))
    acted = _sweep(lv)
    assert [a["action"] for a in acted] == ["assessment_run_ops"] and ran == [ref]


# ----------------------------------------------------------------- spawn
def test_unstaffed_open_work_spawns_once_per_queue(lv):
    lv.q.enqueue(project=PQ, note="a", source="test")
    lv.q.enqueue(project=PQ, note="b", source="test")
    lv.config.set_auto_drain(PQ, True)
    lv.config.set_repo_path(PQ, str(lv.tmp))
    acted = _sweep(lv)
    spawns = [a for a in acted if a["action"] == "spawn"]
    assert [a["result"] for a in spawns][:1] == ["done"]
    assert len(lv.spawned) == 1 and lv.spawned[0][0] == PQ


def test_auto_drain_off_open_work_is_a_backlog(lv):
    ref = _file(lv)
    a = _assess(lv, ref)
    assert a["row"] == "work.open" and a["action"] == "" and "auto_drain off" in a["evidence"]


# ------------------------------------------------------------ system ops
def test_dep_stuck_is_escalated_and_no_escalation_is_logged(lv, monkeypatch):
    blocker = _file(lv, "blocker")
    lv.q.update_status(blocker, "closed", "w1")
    _patch(lv, blocker, product_nack={"by": "h"})
    dep = _file(lv, "dependent", blocked_by=[blocker])
    assert lv.liveness.row_of(lv.q.get(dep)).id == "dep.stuck"
    real = lv.q.escalate_stuck_blockers
    monkeypatch.setattr(lv.q, "escalate_stuck_blockers", lambda: False)
    acted = _sweep(lv, off=0)
    assert [a["result"] for a in acted] == ["no_escalation"]
    assert "BACKSTOP_NO_ESCALATION" in _log(lv)
    monkeypatch.setattr(lv.q, "escalate_stuck_blockers", real)
    acted = _sweep(lv, off=0)
    assert [a["result"] for a in acted] == ["done"]
    assert lv.liveness.row_of(lv.q.get(dep)).id == "human.block"


def test_unblocked_plan_is_escalated_after_one_tick(lv):
    ref = _file(lv, gates=["plan"])
    lv.q._plan_update(ref, lambda it, plan: plan.update(status="blocked", round=1))
    assert lv.liveness.row_of(lv.q.get(ref)).id == "plan.blocked"
    assert _sweep(lv, off=10) == []                        # transient between writes
    acted = _sweep(lv, off=lv.liveness.ONE_TICK_S + 5)
    assert [a["action"] for a in acted] == ["unblocked_plan"]
    assert "BACKSTOP_UNBLOCKED_PLAN" in _log(lv)
    assert lv.liveness.row_of(lv.q.get(ref)).id == "human.block"


# ---------------------------------------------------------- answer floors
def _parked_routing(wt):
    g = Golden(wt, _claimed(wt))
    g.step(wt.q.block, g.ref, session_id="w1", question="?", origin="worker")
    g.step(wt.q.answer, g.ref, "left", edge="E1", row="answer.routing")
    return g


def test_stale_routing_floor_hands_off(lv):
    g = _parked_routing(lv)
    assert _sweep(lv, off=lv.answers.ROUTE_LEASE_S) == []    # the router's lease
    g.step(lv.liveness.sweep, now=time.time() + LATER, edge="E5", row="work.open")
    assert "BACKSTOP_ANSWER_HANDOFF" in _log(lv)
    g.step(lv.q.claim_next, "w2", project=PQ, edge="E12", row="work.claimed")


def test_queued_answer_floor_at_ttl_plus_lease(lv, monkeypatch):
    g = _parked_routing(lv)
    monkeypatch.setattr(lv.answers, "_resumable", lambda engine, sid: True)
    monkeypatch.setattr(lv.answers, "_deliver",
                        lambda item, pa: {"status": "queued", "msg_id": "m1"})
    lv.answers.route_answer(g.ref, int(g.item()["pending_answer"]["gen"]))
    g.check(row="answer.queued")
    assert _sweep(lv, off=lv.answers.ANSWER_QUEUE_TTL_S) == []
    g.step(lv.liveness.sweep,
           now=time.time() + lv.answers.ANSWER_QUEUE_TTL_S + lv.answers.ROUTE_LEASE_S + 5,
           edge="E10", row="work.open")


def test_delivering_to_a_dead_session_floor(lv, monkeypatch):
    lv.worker()
    g = _parked_routing(lv)
    lv.kill()
    monkeypatch.setattr(lv.answers, "_resumable", lambda engine, sid: True)
    monkeypatch.setattr(lv.answers, "_deliver", lambda item, pa: {"status": "in_flight"})
    lv.answers.route_answer(g.ref, int(g.item()["pending_answer"]["gen"]))
    g.check(row="answer.delivering")
    assert _assess(lv, g.ref)["verdict"] == "dead"
    g.step(lv.liveness.sweep, now=time.time() + lv.answers.ROUTE_LEASE_S + 1, edge="E10", row="work.open")


def test_affinity_expired_floor(lv, monkeypatch):
    g = _parked_routing(lv)
    monkeypatch.setattr(lv.answers, "worker_alive", lambda wid: True)
    monkeypatch.setattr(lv.answers, "affinity_s", lambda: -60)
    lv.answers.route_answer(g.ref, int(g.item()["pending_answer"]["gen"]))
    g.check(row="answer.affinity_expired")
    g.step(lv.liveness.sweep, now=time.time() + 0, edge="E11", row="work.open")


def test_plan_conflict_floor(lv):
    g = _parked_routing(lv)
    lv.q.update(g.ref, gates=["plan"])
    g.check(row="answer.plan_conflict")
    g.step(lv.liveness.sweep, now=time.time() + 0, edge="E5", row="plan.start")
    assert "BACKSTOP_ANSWER_PLAN_CONFLICT" in _log(lv)


def test_parked_bare_is_reopened(lv):
    ref = _claimed(lv)
    lv.q.block(ref, session_id="w1", question="?", origin="worker")
    data = lv.q._load_unlocked()
    for it in data["items"]:
        if it["ref"] == ref:
            it["needs_input"] = False
    lv.q._save_unlocked(data)
    assert lv.liveness.row_of(lv.q.get(ref)).id == "answer.parked_bare"
    acted = _sweep(lv, off=0)
    assert [a["action"] for a in acted] == ["reopen"]
    assert lv.q.get(ref)["status"] == "open" and "parked" not in lv.q.get(ref)


# ------------------------------------------------------- wiring and CLI
def test_reconcile_runs_the_sweep_after_the_stage_pass(lv, monkeypatch):
    order = []
    monkeypatch.setattr(lv.stages, "reconcile_stages",
                        lambda only_ref="": order.append("stages") or [])
    monkeypatch.setattr(lv.liveness, "sweep", lambda *a, **k: order.append("sweep") or [])
    lv.workers.reconcile_once(dry_run=False, supervise_stages=True)
    assert order == ["stages", "sweep"]
    order.clear()
    lv.workers.reconcile_once(dry_run=False, supervise_stages=False)
    lv.workers.reconcile_once(dry_run=True, supervise_stages=True)
    assert order == []


def test_stages_tick_runs_the_sweep(lv, monkeypatch, capsys):
    called = []
    monkeypatch.setattr(lv.liveness, "sweep", lambda only_ref="", **k: called.append(only_ref) or [
        {"ref": "LQ-9", "action": "reopen", "result": "done", "reason": "gone"}])
    assert lv.cli.main(["stages", "tick", "--ref", "LQ-9"]) == 0
    assert called == ["LQ-9"] and "LQ-9: backstop reopen (done)" in capsys.readouterr().out


def test_wt_liveness_is_read_only(lv, capsys):
    lv.worker()
    ref = _claimed(lv)
    lv.kill()
    rev = lv.q.revision()
    assert lv.cli.main(["liveness", "-q", PQ]) == 0
    out = capsys.readouterr().out
    assert ref in out and "work.claimed" in out and "owner=worker" in out
    assert lv.cli.main(["liveness", ref, "--json"]) == 0
    rows = json.loads(capsys.readouterr().out)
    assert rows[0]["row"] == "work.claimed" and rows[0]["verdict"] == "dead"
    assert lv.q.revision() == rev and "backstop" not in lv.q.get(ref)
    assert lv.cli.main(["liveness", "LQ-999"]) == 1


def test_stall_s_env(monkeypatch, lv):
    monkeypatch.setenv("WATCHTOWER_STALL_S", "42")
    assert lv.liveness.stall_s() == 42.0
    monkeypatch.setenv("WATCHTOWER_STALL_S", "junk")
    assert lv.liveness.stall_s() == lv.liveness.STALL_S


# ---------------------------------------------------------------- health
def _ago(minutes: float) -> str:
    return _iso(time.time() - minutes * 60)


def test_in_review_only_queue_with_a_dead_verifier_reads_stage_stuck():
    from watchtower import health
    item = {"ref": "Q-1", "project": "Q", "status": "in_review", "gate_pending": "verify",
            "verify_cycle": 1, "updated_at": _ago(120), "created_at": _ago(180),
            "history": [{"event": "stage_spawn", "at": _ago(120)}]}
    row = health.queue_status("Q", [item])
    assert row["depth"] == 0 and row["stage_owned"] == 1
    assert row["stage_stuck"] is True and row["state"] == "stuck"
    assert row["stuck_reason"] == "stage" and row["stuck"] is False
    fresh = dict(item, history=[{"event": "stage_spawn", "at": _ago(1)}])
    row = health.queue_status("Q", [fresh])
    assert row["stage_stuck"] is False and row["state"] == "clear"


def test_plan_pending_and_reserved_affinity_are_not_claimable():
    from watchtower import health
    until = _iso(time.time() + 600)
    items = [
        {"ref": "Q-1", "status": "open", "gates": ["plan"], "created_at": _ago(60)},
        {"ref": "Q-2", "status": "open", "created_at": _ago(60),
         "pending_answer": {"gen": 1, "state": "affinity", "affinity_until": until}},
    ]
    row = health.queue_status("Q", items)
    assert row["depth"] == 2 and row["claimable_depth"] == 0 and row["stuck"] is False


def test_stage_events_count_as_progress():
    from watchtower import health
    items = [{"ref": "Q-1", "status": "open", "created_at": _ago(120)},
             {"ref": "Q-2", "status": "in_progress", "claimed_at": _ago(120),
              "history": [{"event": "plan_submit", "at": _ago(1)}]}]
    row = health.queue_status("Q", items)
    assert row["since_progress_s"] < 120 and row["stuck"] is False

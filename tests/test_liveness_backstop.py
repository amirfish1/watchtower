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
    monkeypatch.setattr(lv.q, "get", lambda ref: dict(item))      # the fresh read
    monkeypatch.setattr(lv.q, "block", lambda ref, sid="", **k: blocked.append(ref) or {"ref": ref})
    acted = _sweep(lv)
    assert [a["action"] for a in acted] == ["escalate"] and blocked == ["GH-1"]


# WT-38: a GitHub-backed claim stores no claim_proc. A native session bound to
# that exact claim and writing now is the owner at work, not "legacy unproven".
def _gh_legacy(lv, monkeypatch, wid="becky-1", sid=SID2, **over):
    item = {"ref": "GH-1", "project": "GH", "status": "in_progress", "claimed_by": wid,
            "claimed_session_id": sid, "claimed_machine": lv.q.machine_tag(),
            "claimed_at": "2000-01-01T00:00:00Z", "updated_at": "2000-01-01T00:00:00Z",
            "history": []}
    item.update(over)
    blocked = []
    monkeypatch.setattr(lv.q, "_github_backend_for_project", github_by_name)
    monkeypatch.setattr(lv.q, "list_items", lambda *a, **k: [dict(item)])
    monkeypatch.setattr(lv.q, "get", lambda ref: dict(item))      # the fresh read
    monkeypatch.setattr(lv.q, "block", lambda ref, sid="", **k: blocked.append(ref) or {"ref": ref})
    reopened = []
    monkeypatch.setattr(lv.q, "recover_claim",
                        lambda ref, **k: reopened.append((ref, k.get("action"))) or None)
    lv.routes = []      # owner-host recovery requests (WT-38): enqueue kwargs
    monkeypatch.setattr(lv.q, "enqueue", lambda **k: lv.routes.append(k) or
                        {"ref": f"OPS-{len(lv.routes)}", "dedupe_key": k.get("dedupe_key")})
    return item, blocked, reopened


def _hosted_thread(lv, monkeypatch, wid, sid, age_s, now):
    """A hosted Codex thread registered to ``wid`` whose rollout was last
    written ``age_s`` before ``now`` (its codex exec PID is long gone)."""
    monkeypatch.setenv("CODEX_HOME", str(lv.tmp / "codex"))
    monkeypatch.setenv("WATCHTOWER_CODEX_THREAD_REGISTRY", str(lv.tmp / "codex-registry.json"))
    from watchtower import codex_registry
    codex_registry.upsert(sid, worker_id=wid, queue="GH", wt={"worker_id": wid})
    rollout = lv.tmp / "codex/sessions/2026/10/29" / f"rollout-{sid}.jsonl"
    rollout.parent.mkdir(parents=True, exist_ok=True)
    rollout.write_text('{"type":"response_item"}\n')
    os.utime(rollout, (now - age_s, now - age_s))
    return rollout


def _owner(lv, item, now):
    return lv.liveness.claim_owner(item, lv.liveness.ResolverContext(now))


def test_github_legacy_claim_with_bound_hosted_thread_is_alive(lv, monkeypatch):
    item, blocked, reopened = _gh_legacy(lv, monkeypatch)
    now = time.time() + LATER
    _hosted_thread(lv, monkeypatch, "becky-1", SID2, 30, now)
    v = _owner(lv, item, now)
    assert (v.verdict, v.source) == ("alive", "hosted_codex_thread")
    a = lv.liveness.assess(item, {}, lv.liveness.ResolverContext(now))
    assert (a["verdict"], a["action"]) == ("alive", "")
    assert _sweep(lv) == [] and blocked == [] and reopened == []


def test_github_legacy_claim_with_quiet_bound_thread_still_escalates(lv, monkeypatch):
    item, blocked, reopened = _gh_legacy(lv, monkeypatch)
    now = time.time() + LATER
    _hosted_thread(lv, monkeypatch, "becky-1", SID2, LATER, now)    # stale rollout
    assert _owner(lv, item, now).verdict == "unproven"
    acted = _sweep(lv)
    assert [a["action"] for a in acted] == ["escalate"] and blocked == ["GH-1"]
    assert reopened == []


def test_github_legacy_claim_unrelated_session_does_not_veto(lv, monkeypatch):
    """A fresh thread of the same worker under another session id, or a fresh
    rollout for the claim's session registered to another worker, is not the
    claim's owner at work."""
    item, blocked, _ = _gh_legacy(lv, monkeypatch)
    now = time.time() + LATER
    _hosted_thread(lv, monkeypatch, "becky-1", SID, 30, now)       # other session
    assert _owner(lv, item, now).verdict == "unproven"
    assert [a["action"] for a in _sweep(lv)] == ["escalate"] and blocked == ["GH-1"]
    _hosted_thread(lv, monkeypatch, "someone-else", SID2, 30, now)  # other worker
    v = _owner(lv, item, now)
    assert (v.verdict, v.evidence) == ("unproven", "no claim_proc (legacy claim)")


def test_github_legacy_claim_held_on_another_host_is_not_proven_here(lv, monkeypatch):
    """A fresh local rollout for the claim's session is not proof for a claim
    held on another host, and with no owning-host heartbeat the full
    assess -> sweep -> apply path routes it to the owning host: no block, no
    recovery, nothing written on the claimed issue (WT-38)."""
    item, blocked, reopened = _gh_legacy(lv, monkeypatch, claimed_machine="zz9")
    now = time.time() + LATER
    _hosted_thread(lv, monkeypatch, "becky-1", SID2, 30, now)
    assert _owner(lv, item, now).verdict == "unproven"
    a = lv.liveness.assess(item, {}, lv.liveness.ResolverContext(now))
    assert (a["verdict"], a["action"]) == ("unobserved", "route_owner_host")
    assert "held on zz9; no owning-host heartbeat" in a["evidence"]
    acted = _sweep(lv)
    assert [(x["action"], x["result"]) for x in acted] == [("route_owner_host", "done")]
    assert blocked == [] and reopened == [] and len(lv.routes) == 1


def test_github_legacy_claim_lookup_failure_escalates_not_recovers(lv, monkeypatch):
    item, blocked, reopened = _gh_legacy(lv, monkeypatch)
    now = time.time() + LATER
    _hosted_thread(lv, monkeypatch, "becky-1", SID2, 30, now)

    def boom(*a, **k):
        raise OSError("probe failed")
    monkeypatch.setattr(lv.liveness, "_transcript_mtime", boom)
    assert _owner(lv, item, now).verdict == "unproven"
    acted = _sweep(lv)
    assert [a["action"] for a in acted] == ["escalate"] and blocked == ["GH-1"]
    assert reopened == []


def test_local_legacy_claim_with_bound_resumed_session(lv):
    """A local claim with no claim_proc whose worker record (started after the
    claim, so ``_legacy_proc`` declines it) carries the claim's exact session:
    a fresh transcript is the owner at work; a quiet one is recoverable."""
    ref = _claimed(lv)
    _patch(lv, ref, claim_proc=None)
    lv.worker()
    data = lv.workers._load()
    data["workers"][-1]["started_at"] = _iso(time.time() + 60)
    lv.workers._save(data)
    lv.kill()
    proj = lv.tmp / "claude-home" / "projects" / "-repo"
    proj.mkdir(parents=True)
    transcript = proj / f"{SID}.jsonl"
    transcript.write_text("{}\n")
    now = time.time() + LATER
    os.utime(transcript, (now - 30, now - 30))
    v = _owner(lv, lv.q.get(ref), now)
    assert (v.verdict, v.source) == ("alive", "wt_record_session")
    assert _sweep(lv) == [] and lv.q.get(ref)["status"] == "in_progress"
    os.utime(transcript, (now - LATER, now - LATER))
    assert _owner(lv, lv.q.get(ref), now).verdict == "unproven"
    assert [a["action"] for a in _sweep(lv)] == ["escalate"]


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
    monkeypatch.setattr(lv.q, "get", lambda ref: dict(item))      # the fresh read
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


def test_requeue_leaves_ambient_rebound_claim_of_a_known_worker(lv, monkeypatch):
    """OPS-1333: a spawned worker whose claim was rebound ambient (pid 0) while
    its session keeps working is not reopened by the orphan sweep."""
    lv.worker()
    ref = _claimed(lv)
    _patch(lv, ref, claim_proc={"worker_id": "w1", "session_id": SID, "engine": "codex",
                                "pid": 0, "bound": "ambient"})
    lv.kill()
    base = time.time()
    monkeypatch.setattr(time, "time", lambda: base + 600)
    monkeypatch.setattr(lv.liveness, "_file_activity", lambda it, ctx: base + 590)
    assert lv.workers.requeue_orphaned_tickets() == []
    assert lv.q.get(ref)["status"] == "in_progress"
    # the session went quiet: the sweep reopens it again
    monkeypatch.setattr(lv.liveness, "_file_activity", lambda it, ctx: base - 3600)
    assert [i["ref"] for i in lv.workers.requeue_orphaned_tickets()] == [ref]


def test_requeue_preserves_resumed_bound_claim_while_transcript_advances(lv, monkeypatch):
    """OPS-102: the original exec PID can die while its resumed thread works."""
    monkeypatch.setenv("CODEX_HOME", str(lv.tmp / "codex"))
    monkeypatch.setenv("WATCHTOWER_CODEX_THREAD_REGISTRY", str(lv.tmp / "codex-registry.json"))
    lv.worker(engine="codex")
    ref = _claimed(lv)
    assert lv.q.get(ref)["claim_proc"]["pid"]
    lv.kill()
    base = time.time()
    monkeypatch.setattr(time, "time", lambda: base + 600)
    rollout = lv.tmp / "codex/sessions/2026/10/25" / f"rollout-{SID}.jsonl"
    rollout.parent.mkdir(parents=True)
    rollout.write_text('{"type":"response_item"}\n')
    os.utime(rollout, (base + 590, base + 590))

    assert lv.workers.requeue_orphaned_tickets() == []
    assert lv.q.get(ref)["status"] == "in_progress"
    # A dead process with a quiet transcript must still be recoverable.
    os.utime(rollout, (base - 3600, base - 3600))
    assert [it["ref"] for it in lv.workers.requeue_orphaned_tickets()] == [ref]


def test_requeue_reads_codex_rollout_of_an_engineless_ambient_claim(lv, monkeypatch):
    """OPS-102: an ambient claim records no engine; a hosted Codex session's
    live rollout must still count as activity, not only a Claude transcript."""
    monkeypatch.setenv("CODEX_HOME", str(lv.tmp / "codex"))
    lv.worker(engine="codex")
    ref = _claimed(lv)
    _patch(lv, ref, claim_proc={"worker_id": "w1", "session_id": SID, "engine": "",
                                "pid": 0, "bound": "ambient"})
    lv.kill()
    base = time.time()
    monkeypatch.setattr(time, "time", lambda: base + 600)
    rollout = lv.tmp / "codex/sessions/2026/10/25" / f"rollout-{SID}.jsonl"
    rollout.parent.mkdir(parents=True)
    rollout.write_text('{"type":"response_item"}\n')
    os.utime(rollout, (base + 590, base + 590))

    assert lv.workers.requeue_orphaned_tickets() == []
    assert lv.q.get(ref)["status"] == "in_progress"
    os.utime(rollout, (base - 3600, base - 3600))
    assert [it["ref"] for it in lv.workers.requeue_orphaned_tickets()] == [ref]


def test_requeue_preserves_github_claim_while_resumed_transcript_advances(lv, monkeypatch):
    """OPS-107: a GitHub-backed claim whose exec PID died while its resumed
    thread kept working was reopened to a second worker."""
    monkeypatch.setenv("CODEX_HOME", str(lv.tmp / "codex"))
    monkeypatch.setenv("WATCHTOWER_CODEX_THREAD_REGISTRY", str(lv.tmp / "codex-registry.json"))
    lv.worker(engine="codex")
    ref = _claimed(lv)
    lv.kill()
    monkeypatch.setattr(lv.liveness, "_github", lambda project: True)
    reopened = []
    monkeypatch.setattr(lv.q, "update_status",
                        lambda r, status, **kw: reopened.append(r) or {"ref": r})
    base = time.time()
    monkeypatch.setattr(time, "time", lambda: base + 600)
    rollout = lv.tmp / "codex/sessions/2026/10/25" / f"rollout-{SID}.jsonl"
    rollout.parent.mkdir(parents=True)
    rollout.write_text('{"type":"response_item"}\n')
    os.utime(rollout, (base + 590, base + 590))

    assert lv.workers.requeue_orphaned_tickets() == [] and reopened == []
    # A quiet transcript still lets the dead claim recover.
    os.utime(rollout, (base - 3600, base - 3600))
    assert [it["ref"] for it in lv.workers.requeue_orphaned_tickets()] == [ref]


# WT-39: a GitHub claim held on another host. This host defers only to the
# owning host's renewal of exactly this claim (claim_heartbeat), fresh within
# the stall window; anything else routes one recovery request to the owning
# host and never blocks the claim (WT-38).
FOREIGN = "zz9"


def _hb(now, age_s, machine=FOREIGN, wid="becky-1", sid=SID2):
    return {"machine": machine, "at": _iso(now - age_s), "claimed_by": wid,
            "session_id": sid}


def _foreign(lv, monkeypatch, now, hb_age=None, **hb):
    over = {"claimed_machine": FOREIGN}
    if hb_age is not None:
        over["claim_heartbeat"] = _hb(now, hb_age, **hb)
    return _gh_legacy(lv, monkeypatch, **over)


def _decide(lv, item, now):
    a = lv.liveness.assess(item, {}, lv.liveness.ResolverContext(now))
    return a["verdict"], a["action"], a["evidence"]


def test_foreign_claim_with_fresh_owner_heartbeat_takes_no_action(lv, monkeypatch):
    now = time.time() + LATER
    item, blocked, reopened = _foreign(lv, monkeypatch, now, hb_age=120)
    verdict, action, evidence = _decide(lv, item, now)
    assert (verdict, action) == ("remote", "")
    assert evidence == "held on zz9; owning host renewed it 120s ago"
    assert _sweep(lv) == [] and blocked == [] and reopened == []


def test_foreign_claim_with_expired_owner_heartbeat_routes_to_owner(lv, monkeypatch):
    now = time.time() + LATER
    stall = lv.liveness.stall_s()
    item, blocked, reopened = _foreign(lv, monkeypatch, now, hb_age=stall + 60)
    verdict, action, evidence = _decide(lv, item, now)
    assert (verdict, action) == ("unproven", "route_owner_host")
    assert f"owning-host heartbeat {int(stall + 60)}s old" in evidence
    assert [a["action"] for a in _sweep(lv)] == ["route_owner_host"]
    assert blocked == [] and reopened == []
    (route,) = lv.routes
    assert route["project"] == "OPS" and "GH-1" in route["title"] and "zz9" in route["title"]
    assert f"heartbeat age {int(stall + 60)}s" in route["text"]
    assert f"Observed by host {lv.q.machine_tag()}" in route["text"]   # producer named


@pytest.mark.parametrize("bad", [{"machine": "her2"}, {"wid": "becky-2"}, {"sid": SID}])
def test_foreign_claim_heartbeat_for_another_claim_is_not_proof(lv, monkeypatch, bad):
    """A stamp from another host, an earlier claimer or another session (a
    rebound claim) proves nothing about the current claim."""
    now = time.time() + LATER
    item, blocked, _ = _foreign(lv, monkeypatch, now, hb_age=30, **bad)
    assert _decide(lv, item, now)[:2] == ("unobserved", "route_owner_host")
    assert [a["action"] for a in _sweep(lv)] == ["route_owner_host"] and blocked == []


def test_foreign_claim_heartbeat_clock_skew(lv, monkeypatch):
    """A stamp slightly ahead (skew within CLOCK_SKEW_S) counts as fresh; one
    far in the future is not proof, so no host can buy immunity by its clock."""
    now = time.time() + LATER
    item, _, _ = _foreign(lv, monkeypatch, now, hb_age=-60)
    assert _decide(lv, item, now)[:2] == ("remote", "")
    item["claim_heartbeat"] = _hb(now, -(lv.liveness.CLOCK_SKEW_S + 3600))
    assert _decide(lv, item, now)[:2] == ("unobserved", "route_owner_host")


def test_local_claim_ignores_its_own_heartbeat(lv, monkeypatch):
    """On the owning host the stamp is never proof: a quiet local session
    with a fresh-looking heartbeat still escalates on local evidence."""
    now = time.time() + LATER
    item, blocked, _ = _gh_legacy(lv, monkeypatch)
    item["claim_heartbeat"] = _hb(now, 30, machine=lv.q.machine_tag())
    _hosted_thread(lv, monkeypatch, "becky-1", SID2, LATER, now)   # quiet
    assert _decide(lv, item, now)[:2] == ("unproven", "escalate")


def test_missing_claimed_machine_keeps_local_judgement(lv, monkeypatch):
    now = time.time() + LATER
    item, _, _ = _gh_legacy(lv, monkeypatch, claimed_machine=None)
    item["claim_heartbeat"] = _hb(now, 30)
    _hosted_thread(lv, monkeypatch, "becky-1", SID2, 30, now)
    assert _decide(lv, item, now)[:2] == ("alive", "")


def _stale_snapshot(lv, monkeypatch, now, fresh):
    """Sweep from a snapshot with no heartbeat while the live issue is ``fresh``."""
    item, blocked, reopened = _foreign(lv, monkeypatch, now)
    monkeypatch.setattr(lv.q, "get", fresh if callable(fresh) else (lambda ref: fresh))
    return item, blocked, reopened


def test_stale_snapshot_rechecked_against_fresh_issue_before_routing(lv, monkeypatch):
    """The 09:47:56Z shape: the sweep's list snapshot lags the shared issue.
    A fresh read showing a fresh owner heartbeat, a changed claim, a terminal
    state or an already-posted question posts nothing."""
    now = time.time() + LATER
    item, blocked, reopened = _foreign(lv, monkeypatch, now)
    cases = {
        "heartbeat": dict(item, claim_heartbeat=_hb(now, 60)),
        "rebound": dict(item, claimed_session_id=SID),
        "closed": dict(item, status="closed"),
        "asked": dict(item, needs_input=True),
    }
    for name, fresh in cases.items():
        monkeypatch.setattr(lv.q, "get", lambda ref, f=fresh: dict(f))
        acted = _sweep(lv)
        assert [a["result"].split(" ")[0] for a in acted] == ["race"], name
    assert blocked == [] and reopened == [] and lv.routes == []


def test_failed_fresh_read_posts_nothing_and_retries(lv, monkeypatch):
    now = time.time() + LATER

    def boom(ref):
        raise OSError("gh unavailable")
    item, blocked, reopened = _stale_snapshot(lv, monkeypatch, now, boom)
    acted = _sweep(lv)
    assert [a["result"] for a in acted] == ["fresh read failed: gh unavailable"]
    assert blocked == [] and reopened == [] and lv.routes == []
    monkeypatch.setattr(lv.q, "get", lambda ref: dict(item))      # read recovers
    assert [a["result"] for a in _sweep(lv)] == ["done"] and len(lv.routes) == 1
    assert blocked == []


class _HeartbeatBackend:
    def __init__(self):
        self.calls = []

    def renew_claim_heartbeat(self, ref, *, claimed_by, session_id):
        self.calls.append((ref, claimed_by, session_id))
        return True


def test_owning_host_renews_heartbeat_only_while_locally_alive(lv, monkeypatch):
    now = time.time() + LATER
    item, blocked, _ = _gh_legacy(lv, monkeypatch)
    backend = _HeartbeatBackend()
    monkeypatch.setattr(lv.q, "_github_backend_for_project",
                        lambda p: backend if str(p).upper().startswith("GH") else None)
    rollout = _hosted_thread(lv, monkeypatch, "becky-1", SID2, 30, now)
    assert _sweep(lv) == [] and backend.calls == [("GH-1", "becky-1", SID2)]
    # A recent renewal is not rewritten every tick.
    item["claim_heartbeat"] = _hb(now, 60, machine=lv.q.machine_tag())
    monkeypatch.setattr(lv.q, "list_items", lambda *a, **k: [dict(item)])
    assert _sweep(lv) == [] and len(backend.calls) == 1
    # A quiet session is not renewed: the stamp ages out for other hosts.
    item.pop("claim_heartbeat")
    os.utime(rollout, (now - LATER, now - LATER))
    monkeypatch.setattr(lv.q, "get", lambda ref: dict(item))
    assert [a["action"] for a in _sweep(lv)] == ["escalate"] and len(backend.calls) == 1


def test_foreign_host_never_renews_heartbeat(lv, monkeypatch):
    now = time.time() + LATER
    item, _, _ = _foreign(lv, monkeypatch, now, hb_age=60)
    backend = _HeartbeatBackend()
    monkeypatch.setattr(lv.q, "_github_backend_for_project",
                        lambda p: backend if str(p).upper().startswith("GH") else None)
    assert _sweep(lv) == [] and backend.calls == []


def test_orphan_requeue_leaves_foreign_github_claim_alone(lv, monkeypatch):
    """A dead builder id known here does not license this host to reopen a
    GitHub claim recorded on another host; a local one is still reopened."""
    lv.worker(wid="becky-1", sid=SID2)
    lv.kill()
    item, _, _ = _gh_legacy(lv, monkeypatch, claimed_machine=FOREIGN,
                            claimed_at="2000-01-01T00:00:00Z")
    reopened, fences = [], []
    monkeypatch.setattr(lv.q, "update_status",
                        lambda ref, st, **k: reopened.append((ref, st)) or
                        fences.append(k.get("expect")) or dict(item, status=st))
    assert lv.workers.requeue_orphaned_tickets(grace_s=0) == [] and reopened == []
    local = dict(item, claimed_machine=lv.q.machine_tag())
    monkeypatch.setattr(lv.q, "list_items", lambda *a, **k: [dict(local)])
    lv.workers.requeue_orphaned_tickets(grace_s=0)
    assert reopened == [("GH-1", "open")]
    # The reopen is fenced on the snapshot's claim identity, so a fresh read
    # naming another claimer/session/host (the backend checks it under the
    # body lock) leaves the claim alone.
    assert fences == [{k: local.get(k) for k in lv.liveness._CLAIM_KEYS}]
    assert fences[0]["claimed_by"] == "becky-1" and \
        fences[0]["claimed_machine"] == lv.q.machine_tag()


def test_foreign_claim_without_any_heartbeat_never_asks_a_human(lv, monkeypatch):
    """No stamp at all (the owning host predates WT-39, or never got a renewal
    through) is not evidence the host died: nothing at one or three stall
    windows of store idle; past UNOBSERVED_STALL_FACTOR windows it routes one
    recovery request to the owning host -- still no question on the claim,
    no reopen, and no second request on the next sweep."""
    stall = lv.liveness.stall_s()
    factor = lv.liveness.UNOBSERVED_STALL_FACTOR
    t0 = time.time()
    item, blocked, reopened = _foreign(lv, monkeypatch, t0)
    item["updated_at"] = item["claimed_at"] = _iso(t0)
    monkeypatch.setattr(lv.q, "list_items", lambda *a, **k: [dict(item)])
    monkeypatch.setattr(lv.q, "get", lambda ref: dict(item))
    for windows in (1.2, factor - 0.5):
        assert _decide(lv, item, t0 + windows * stall)[:2] == ("unobserved", "")
        assert _sweep(lv, off=windows * stall) == []
    late = (factor + 0.1) * stall
    assert _decide(lv, item, t0 + late)[:2] == ("unobserved", "route_owner_host")
    assert [a["action"] for a in _sweep(lv, off=late)] == ["route_owner_host"]
    assert blocked == [] and reopened == [] and len(lv.routes) == 1
    routed = {"ref": "OPS-1", "project": "OPS", "status": "open",
              "dedupe_key": lv.routes[0]["dedupe_key"]}
    monkeypatch.setattr(lv.q, "list_items", lambda *a, **k: [dict(item), dict(routed)])
    for off in (late, 10 * late):
        assert _sweep(lv, off=off) == []
    a = lv.liveness.assess(item, {"OPS-1": routed}, lv.liveness.ResolverContext(t0 + late))
    assert a["action"] == "" and "recovery routed to OPS-1" in a["evidence"]
    assert blocked == [] and len(lv.routes) == 1


def test_fresh_heartbeat_beats_an_old_ticket_clock(lv, monkeypatch):
    """A long-running owner never touches the issue; its host's renewal alone
    keeps a foreign observer quiet, however old the ticket's own clock."""
    now = time.time() + 10 * LATER
    item, blocked, _ = _foreign(lv, monkeypatch, now, hb_age=lv.liveness.stall_s() - 5)
    assert _decide(lv, item, now)[:2] == ("remote", "")
    assert _sweep(lv, off=10 * LATER) == [] and blocked == []


def _local_quiet(lv, monkeypatch, now):
    """A GitHub claim held HERE whose bound session went quiet: the owning
    host's own escalation, the one path that still blocks a GitHub claim."""
    item = _gh_legacy(lv, monkeypatch)[0]
    _hosted_thread(lv, monkeypatch, "becky-1", SID2, LATER, now)
    return item


def test_block_carries_the_fresh_read_as_its_write_fence(lv, monkeypatch):
    """The fresh read's claim fields go to q.block as ``expect``, so the
    backend re-checks them under its body lock right before writing."""
    now = time.time() + LATER
    _local_quiet(lv, monkeypatch, now)
    seen = []
    monkeypatch.setattr(lv.q, "block", lambda ref, sid="", **k: seen.append(k) or {"ref": ref})
    assert [a["result"] for a in _sweep(lv)] == ["done"]
    exp = seen[0]["expect"]
    assert exp["claimed_by"] == "becky-1" and exp["claimed_session_id"] == SID2
    assert exp["claimed_machine"] == lv.q.machine_tag() and exp["claim_heartbeat"] is None
    assert exp["status"] == "in_progress" and not exp.get("needs_input")


def test_block_race_at_the_write_is_reported_not_retried_as_done(lv, monkeypatch):
    now = time.time() + LATER
    _local_quiet(lv, monkeypatch, now)
    monkeypatch.setattr(lv.q, "block", lambda ref, sid="", **k: None)   # fence tripped
    assert [a["result"] for a in _sweep(lv)] == ["race"]


INCIDENT_NOW = 1793267276.0   # 2026-10-29T09:47:56Z, BECKY-2966's false escalation


def test_incident_replay_foreign_live_claim_is_not_escalated(lv, monkeypatch):
    """WT-38 replay: BECKY-2966 shape (exact worker/session, no claim_proc,
    store activity 09:36:30Z) seen from a host that is not its owner and has
    none of its session files. Neither at the incident instant nor one hour
    later (two stall windows idle) does assess or sweep ask a human."""
    item, blocked, reopened = _gh_legacy(lv, monkeypatch, claimed_machine="zz9",
                                         updated_at="2026-10-29T09:36:30Z",
                                         claimed_at="2026-10-29T06:40:10Z")
    for now in (INCIDENT_NOW, INCIDENT_NOW + 3600):
        a = lv.liveness.assess(item, {}, lv.liveness.ResolverContext(now))
        assert (a["row"], a["action"]) == ("work.claimed", "")
        assert "held on zz9" in a["evidence"]
        assert lv.liveness.sweep(now=now) == []
    assert blocked == [] and reopened == []


VERIFY_NOW = 1793271600.0   # 2026-10-29T11:00:00Z, WT-39 verifier replay


@pytest.mark.parametrize("hb_age, action", [
    (None, "route_owner_host"),     # no stamp, store idle 3h (> 4 windows)
    (2400, "route_owner_host"),     # expired stamp from 10:20Z
    (60, ""),                       # fresh stamp from 10:59Z
])
def test_verifier_replay_foreign_claim_never_blocks(lv, monkeypatch, hb_age, action):
    """WT-39 verifier replay through assess -> sweep -> _apply: a foreign
    claim idle since 08:00Z, unchanged worker/session, seen at 11:00Z. Absent
    or expired owning-host evidence routes one request to the owner; it never
    posts a question, reopens, or otherwise writes the claimed issue."""
    item, _, _ = _foreign(lv, monkeypatch, VERIFY_NOW, hb_age=hb_age)
    item.update(updated_at="2026-10-29T08:00:00Z", claimed_at="2026-10-29T08:00:00Z")
    monkeypatch.setattr(lv.q, "list_items", lambda *a, **k: [dict(item)])
    monkeypatch.setattr(lv.q, "get", lambda ref: dict(item))
    _no_claim_writes(lv, monkeypatch)
    acted = lv.liveness.sweep(now=VERIFY_NOW)
    assert [a["action"] for a in acted] == ([action] if action else [])
    assert [a["result"] for a in acted] == (["done"] if action else [])
    assert len(lv.routes) == (1 if action else 0)


def test_foreign_claim_fresh_local_files_do_not_reset_its_clock(lv, monkeypatch):
    """A same-named session file on this host is not the remote session: past
    the migration bound a foreign claim with no stamp is still routed to its
    owner, even when a local rollout for its session id was just written."""
    item, blocked, reopened = _gh_legacy(lv, monkeypatch, claimed_machine="zz9",
                                         updated_at="2026-10-29T09:36:30Z")
    later = INCIDENT_NOW + (lv.liveness.UNOBSERVED_STALL_FACTOR + 1) * lv.liveness.stall_s()
    _hosted_thread(lv, monkeypatch, "becky-1", SID2, 30, later)
    assert [a["action"] for a in lv.liveness.sweep(now=later)] == ["route_owner_host"]
    assert blocked == [] and reopened == [] and len(lv.routes) == 1


def _no_claim_writes(lv, monkeypatch):
    """Any write to the claimed ticket fails the test."""
    def write(*a, **k):
        raise AssertionError(f"foreign observer wrote the claim: {a} {k}")
    for name in ("block", "recover_claim", "update_status", "release", "answer"):
        if hasattr(lv.q, name):
            monkeypatch.setattr(lv.q, name, write)


def test_fresh_concurrent_owner_answer_routes_nothing_and_writes_nothing(lv, monkeypatch):
    """Boundary: the snapshot is old and stampless (past the bound), but the
    fresh read shows the owner answered / worked moments ago. Nothing is
    routed, and at no point does the observer write the claimed issue."""
    item, _, _ = _gh_legacy(lv, monkeypatch, claimed_machine="zz9",
                            updated_at="2026-10-29T09:36:30Z")
    later = INCIDENT_NOW + (lv.liveness.UNOBSERVED_STALL_FACTOR + 1) * lv.liveness.stall_s()
    _no_claim_writes(lv, monkeypatch)
    answered = dict(item, updated_at=_iso(later - 5), history=[
        {"ts": _iso(later - 5), "event": "answered", "by": "becky-1"}])
    monkeypatch.setattr(lv.q, "get", lambda ref: dict(answered))
    acted = lv.liveness.sweep(now=later)
    assert [a["result"].split(" ")[0] for a in acted] == ["race"] and lv.routes == []
    # The owner's question is open on the fresh read: still hands off.
    monkeypatch.setattr(lv.q, "get", lambda ref: dict(item, needs_input=True))
    acted = lv.liveness.sweep(now=later)
    assert [a["result"].split(" ")[0] for a in acted] == ["race"] and lv.routes == []


def test_owner_route_failure_is_reported_and_retried(lv, monkeypatch):
    item, blocked, _ = _gh_legacy(lv, monkeypatch, claimed_machine="zz9",
                                  updated_at="2026-10-29T09:36:30Z")
    later = INCIDENT_NOW + (lv.liveness.UNOBSERVED_STALL_FACTOR + 1) * lv.liveness.stall_s()
    _no_claim_writes(lv, monkeypatch)

    def down(**k):
        raise ValueError("dedupe keys currently require a local queue")
    monkeypatch.setattr(lv.q, "enqueue", down)
    acted = lv.liveness.sweep(now=later)
    assert [a["result"] for a in acted] == [
        "route failed: dedupe keys currently require a local queue"]
    monkeypatch.setattr(lv.q, "enqueue", lambda **k: lv.routes.append(k) or {"ref": "OPS-9"})
    assert [a["result"] for a in lv.liveness.sweep(now=later)] == ["done"]


def test_a_new_claim_gets_its_own_owner_route(lv, monkeypatch):
    """The dedupe key is the claim identity: a rebound claim is a new request."""
    item, _, _ = _gh_legacy(lv, monkeypatch, claimed_machine="zz9")
    k1 = lv.liveness._owner_route_key(item)
    assert k1 != lv.liveness._owner_route_key(dict(item, claimed_session_id=SID))
    assert k1 != lv.liveness._owner_route_key(dict(item, claimed_at="2001-01-01T00:00:00Z"))


def test_owner_route_lifecycle_closed_request_is_not_recovery_proof(lv, monkeypatch):
    """Route once -> the request closes when the heartbeat comes back -> the
    SAME claim loses its signal later -> a new bounded request, one at a
    time, never sooner than a stall window after the close, at most
    OWNER_ROUTE_MAX in all; the claimed issue is never written."""
    stall = lv.liveness.stall_s()
    t0 = time.time()
    item, _, _ = _gh_legacy(lv, monkeypatch, claimed_machine="zz9",
                            updated_at=_iso(t0 - 10 * stall))
    _no_claim_writes(lv, monkeypatch)
    store = [dict(item)]
    monkeypatch.setattr(lv.q, "list_items", lambda *a, **k: [dict(i) for i in store])

    def enqueue(**k):
        lv.routes.append(k)
        row = {"ref": f"OPS-{len(lv.routes)}", "project": "OPS", "status": "open",
               "dedupe_key": k["dedupe_key"]}
        store.append(row)
        return dict(row)
    monkeypatch.setattr(lv.q, "enqueue", enqueue)

    def close_last(at):
        store[-1].update(status="closed", closed_at=_iso(at))

    assert [a["result"] for a in lv.liveness.sweep(now=t0)] == ["done"]
    assert lv.liveness.sweep(now=t0 + 60) == [] and len(lv.routes) == 1  # open: no storm
    # Heartbeat renewed: the claim is quiet, the request is resolved.
    store[0]["claim_heartbeat"] = _hb(t0 + 120, 0)
    close_last(t0 + 120)
    assert lv.liveness.sweep(now=t0 + 180) == []
    # The same claim loses its signal again (stamp expires).
    later = t0 + 120 + stall + 60
    assert lv.liveness.sweep(now=later)[0]["result"] == "done" and len(lv.routes) == 2
    assert lv.routes[1]["dedupe_key"].endswith("#2")
    assert "OPS-1" in lv.routes[1]["text"]
    # Closed while the claim is still unresolved: re-routed, but only after
    # a stall window, and never beyond OWNER_ROUTE_MAX.
    close_last(later + 10)
    assert lv.liveness.sweep(now=later + 20) == [] and len(lv.routes) == 2
    t = later + 10 + stall + 1
    assert lv.liveness.sweep(now=t)[0]["result"] == "done" and len(lv.routes) == 3
    assert "last request" in lv.routes[2]["text"]
    close_last(t + 10)
    for off in (stall + 20, 20 * stall):
        assert lv.liveness.sweep(now=t + off) == []
    a = lv.liveness.assess(store[0], lv.q._refs_index(store),
                           lv.liveness.ResolverContext(t + 2 * stall))
    assert a["action"] == "" and "routing exhausted, still unrecovered" in a["evidence"]
    assert "`wt liveness GH-1` on zz9" in a["evidence"]
    assert len(lv.routes) == lv.liveness.OWNER_ROUTE_MAX


def test_owner_route_request_states_checks_and_safe_stop(lv, monkeypatch):
    """The auto-filed request is self-contained and grants no destructive
    authority: identity, evidence, recheck-first, explicit safe stop."""
    item, _, _ = _gh_legacy(lv, monkeypatch, claimed_machine="zz9",
                            updated_at="2026-10-29T09:36:30Z")
    later = INCIDENT_NOW + (lv.liveness.UNOBSERVED_STALL_FACTOR + 1) * lv.liveness.stall_s()
    lv.liveness.sweep(now=later)
    (r,) = lv.routes
    text = r["text"]
    for want in ("becky-1", SID2, "host zz9", f"Observed by host {lv.q.machine_tag()}",
                 "Re-read GH-1 first", "does not authorize `wt release --force`",
                 "budget", "deadline", "grant", "proves the claimed", "Done when:",
                 "an external wait, not a", "`wt defer`", "--unresolved",
                 "never for\n   reachability alone"):
        assert want in text, want
    assert "block this request on" not in text      # unreachable is not a question
    assert r["readiness"] == "ready"
    assert r["accept_line"] and r["dedupe_key"].endswith("#1")


def test_owner_route_files_a_real_local_ops_request_once(lv, monkeypatch):
    """Through the real local store: the request lands in OPS with its
    acceptance line, and the same episode key never files a second row."""
    item = {"ref": "GH-1", "project": "GH", "status": "in_progress",
            "claimed_by": "becky-1", "claimed_session_id": SID2,
            "claimed_machine": "zz9", "claimed_at": "2026-10-29T06:40:10Z"}
    a = {"ref": "GH-1", "queue": "GH", "evidence": "held on zz9; no owning-host heartbeat",
         "idle_s": 9000}
    r1 = lv.liveness._route_owner_host(item, a, {}, INCIDENT_NOW)
    r2 = lv.liveness._route_owner_host(item, a, {}, INCIDENT_NOW)
    assert r1 and r1 == r2
    row = lv.q.get(r1)
    assert row["project"] == "OPS" and row.get("accept")
    # Filed ready to work, not as a question: no needs-you state moves to OPS.
    assert (row["status"], row.get("readiness")) == ("open", "ready")
    assert not row.get("needs_input") and not row.get("wait")
    assert "`wt defer`" in row["text"] and "wt block" in row["text"]
    assert row["dedupe_key"] == lv.liveness._owner_route_key(item) + "#1"

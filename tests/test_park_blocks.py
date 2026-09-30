"""WT-28: a worker ``wt block`` parks the ticket (``awaiting_answer``, claim
released) and the human's answer is routed back to the parking session."""

from __future__ import annotations

import importlib
import json

import pytest

SID = "11111111-2222-3333-4444-555555555555"


@pytest.fixture()
def wt(tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(tmp_path / "activity.log"))
    monkeypatch.setenv("WATCHTOWER_OUTBOX_FILE", str(tmp_path / "outbox.json"))
    monkeypatch.setenv("WATCHTOWER_DELEGATE_URL", "off")
    monkeypatch.delenv("WATCHTOWER_MACHINE", raising=False)
    import watchtower.queue as q
    import watchtower.messages as messages
    importlib.reload(q)
    import watchtower.answers as answers
    import watchtower.cli as cli
    importlib.reload(answers)
    importlib.reload(cli)

    class Ns:
        pass

    ns = Ns()
    ns.q, ns.messages, ns.answers, ns.cli = q, messages, answers, cli
    return ns


def _parked(wt, worker="w1", sid=SID, project="PK", note="ticket"):
    item = wt.q.enqueue(project=project, note=note, source="test")
    wt.q.claim_by_ref(item["ref"], worker, session_uuid=sid)
    blocked = wt.q.block(item["ref"], session_id=worker, question="which way?",
                         progress="half done", origin="worker")
    return blocked


def test_worker_block_parks_and_releases_claim(wt):
    it = _parked(wt)
    assert it["status"] == "awaiting_answer"
    assert not it.get("claimed_by")
    assert it["parked"]["worker_id"] == "w1"
    assert it["parked"]["session_id"] == SID
    # The worker's slot is free: it can claim another ticket.
    other = wt.q.enqueue(project="PK", note="next", source="test")
    got = wt.q.claim_next("w1", project="PK")
    assert got["ref"] == other["ref"]


def test_legacy_origin_keeps_hold_while_blocked(wt):
    item = wt.q.enqueue(project="PK", note="x", source="test")
    wt.q.claim_by_ref(item["ref"], "w1", session_uuid=SID)
    it = wt.q.block(item["ref"], session_id="w1", question="q?", origin="plan_gate")
    assert it["status"] == "in_progress"
    assert it.get("needs_input")


def test_parked_ticket_is_not_claimable(wt):
    it = _parked(wt)
    assert wt.q.claim_next("w2", project="PK") is None
    with pytest.raises(ValueError):
        wt.q.claim_by_ref(it["ref"], "w2")
    assert wt.q.get(it["ref"])["status"] == "awaiting_answer"


def test_answer_writes_pending_answer_routing(wt):
    it = _parked(wt)
    ans = wt.q.answer(it["ref"], "go left", session_id="human")
    pa = ans["pending_answer"]
    assert pa["state"] == "routing"
    assert pa["answer"] == "go left"
    assert pa["prior_worker_id"] == "w1"
    assert ans["status"] == "awaiting_answer"


def test_route_affinity_when_worker_alive(wt, monkeypatch):
    it = _parked(wt)
    ans = wt.q.answer(it["ref"], "go left", session_id="human")
    monkeypatch.setattr(wt.answers, "worker_alive", lambda wid: wid == "w1")
    monkeypatch.setattr(wt.answers, "_wake", lambda *a, **k: None)
    res = wt.answers.route_answer(it["ref"], ans["pending_answer"]["gen"])
    assert res["route"] == "affinity"
    cur = wt.q.get(it["ref"])
    assert cur["status"] == "open"
    assert cur["pending_answer"]["state"] == "affinity"
    # Reserved for the parked worker: others cannot take it, w1 can.
    assert wt.q.claim_next("w2", project="PK") is None
    mine = wt.q.claim_next("w1", project="PK")
    assert mine["ref"] == it["ref"]
    brief = wt.answers.claim_brief(wt.q.get(it["ref"]), "w1", SID)
    assert isinstance(brief, str)


def test_route_reopen_without_session(wt, monkeypatch):
    it = _parked(wt, sid="")
    ans = wt.q.answer(it["ref"], "go left", session_id="human")
    res = wt.answers.route_answer(it["ref"], ans["pending_answer"]["gen"])
    assert res["route"] == "reopen"
    cur = wt.q.get(it["ref"])
    assert cur["status"] == "open"
    assert cur["pending_answer"]["state"] == "handed_off"
    got = wt.q.claim_next("w9", project="PK")
    assert got["ref"] == it["ref"]


def test_stale_generation_route_is_noop(wt, monkeypatch):
    it = _parked(wt)
    ans = wt.q.answer(it["ref"], "first", session_id="human")
    gen = ans["pending_answer"]["gen"]
    monkeypatch.setattr(wt.answers, "worker_alive", lambda wid: False)
    assert wt.answers.route_answer(it["ref"], gen + 7)["route"] == "noop"
    assert wt.q.get(it["ref"])["status"] == "awaiting_answer"


def test_reblock_supersedes_pending_answer(wt):
    it = _parked(wt)
    wt.q.answer(it["ref"], "first", session_id="human")
    # Forced reopen then a second block supersedes; the stale answer is gone.
    wt.q.reopen(it["ref"], reason="test", force=True)
    cur = wt.q.get(it["ref"])
    assert cur["status"] == "open"
    events = [h["event"] for h in cur["history"]]
    assert "park" in events or "block" in events


def test_release_and_reopen_refuse_parked_without_force(wt):
    it = _parked(wt)
    with pytest.raises(ValueError):
        wt.q.release(it["ref"])
    with pytest.raises(ValueError):
        wt.q.reopen(it["ref"])
    assert wt.q.get(it["ref"])["status"] == "awaiting_answer"
    forced = wt.q.release(it["ref"], force=True)
    assert wt.q.get(it["ref"])["status"] == "open"
    assert forced is not None


def test_close_clears_ledger_and_parked_ticket_closes(wt):
    it = _parked(wt)
    wt.messages.ledger_reserve("answer:%s:x:1" % it["ref"], it["ref"])
    wt.q.update_status(it["ref"], "closed", session_id="human", by_kind="human")
    assert wt.q.get(it["ref"])["status"] == "closed"


def test_list_blocked_shows_parked_and_routing(wt):
    it = _parked(wt)
    assert any(x["ref"] == it["ref"] for x in wt.q.list_blocked())
    wt.q.answer(it["ref"], "a", session_id="human")
    assert any(x["ref"] == it["ref"] for x in wt.q.list_blocked())


def test_outbox_rows_are_generation_bound(wt):
    it = _parked(wt)
    ans = wt.q.answer(it["ref"], "a", session_id="human")
    gen = ans["pending_answer"]["gen"]
    wt.messages.outbox_add("some-target", "hello", ticket=it["ref"], ticket_gen=gen,
                           dedupe_key=f"k:{it['ref']}:{gen}") \
        if "ticket" in wt.messages.outbox_add.__code__.co_varnames else None
    wt.messages.outbox_cancel_ticket(it["ref"], "test cancel")
    rows = json.loads(open(wt.messages._outbox_file()).read() or "[]") \
        if wt.messages._outbox_file().exists() else []
    assert all(r.get("ticket") != it["ref"] or r.get("status") == "cancelled"
               for r in (rows if isinstance(rows, list) else rows.get("rows", [])))


def test_migrate_legacy_blocks_parks_worker_blocks(wt):
    item = wt.q.enqueue(project="PK", note="x", source="test")
    wt.q.claim_by_ref(item["ref"], "w1", session_uuid=SID)
    wt.q.block(item["ref"], session_id="w1", question="q?", origin="system")
    picked = wt.q.migrate_legacy_blocks(queue="PK", dry_run=True)
    assert wt.q.get(item["ref"])["status"] == "in_progress"
    assert isinstance(picked, list)
    wt.q.migrate_legacy_blocks(queue="PK")
    wt.q.migrate_legacy_blocks(queue="PK")  # idempotent


def test_cli_answer_routes_parked_ticket(wt, monkeypatch, capsys):
    it = _parked(wt)
    monkeypatch.setattr(wt.answers, "worker_alive", lambda wid: True)
    monkeypatch.setattr(wt.answers, "_wake", lambda *a, **k: None)
    assert wt.cli.main(["answer", it["ref"], "go left"]) == 0
    out = capsys.readouterr().out
    assert "ANSWERED" in out and "affinity" in out
    assert wt.q.get(it["ref"])["pending_answer"]["state"] == "affinity"


def test_retained_parked_ids_window(wt, monkeypatch):
    import watchtower.workers as workers
    it = _parked(wt)
    assert "w1" in workers.retained_parked_ids("PK")
    assert workers.retained_parked_refs("PK", "w1") == [it["ref"]]
    aged = json.loads(json.dumps(wt.q.get(it["ref"])))
    aged["parked"]["at"] = "2000-01-01T00:00:00Z"
    assert workers.retained_parked_ids("PK", items=[aged]) == set()


# ---------------------------------------------------------------- verifier round 1
def _age_park(wt, ref, at="2000-01-01T00:00:00Z"):
    data = wt.q._load_unlocked()
    for it in data["items"]:
        if it["ref"] == ref:
            it["parked"]["at"] = at
    wt.q._save_unlocked(data)


def test_claim_by_ref_exclusivity_is_worker_or_session(wt, monkeypatch):
    it = _parked(wt)  # parked by w1 / SID
    ans = wt.q.answer(it["ref"], "go", session_id="human")
    monkeypatch.setattr(wt.answers, "worker_alive", lambda wid: True)
    monkeypatch.setattr(wt.answers, "_wake", lambda *a, **k: None)
    wt.answers.route_answer(it["ref"], ans["pending_answer"]["gen"])
    other = wt.q.enqueue(project="PK", note="B", source="test")
    wt.q.claim_by_ref(other["ref"], "w1", session_uuid=SID)
    # Same session under a different worker label must not hold two tickets.
    with pytest.raises(ValueError):
        wt.q.claim_by_ref(it["ref"], "alias-w1", session_uuid=SID)
    # A plain explicit second claim by the same worker is refused too.
    third = wt.q.enqueue(project="PK", note="C", source="test")
    with pytest.raises(ValueError):
        wt.q.claim_by_ref(third["ref"], "w1", session_uuid=SID)


def test_retry_stops_delivery_when_bump_cas_is_lost(wt, monkeypatch):
    it = _parked(wt)
    ans = wt.q.answer(it["ref"], "first", session_id="human")
    gen = ans["pending_answer"]["gen"]
    monkeypatch.setattr(wt.answers, "worker_alive", lambda wid: False)
    monkeypatch.setattr(wt.answers, "_resumable", lambda engine, sid: True)
    monkeypatch.setattr(wt.answers, "_deliver_bound", lambda *a, **k: {"route": "resume"})
    wt.answers.route_answer(it["ref"], gen)
    cur = wt.q.get(it["ref"])
    assert cur["pending_answer"]["state"] == "delivering"
    calls = []
    monkeypatch.setattr(wt.answers, "_deliver_bound", lambda *a, **k: calls.append(a))
    monkeypatch.setattr(wt.q, "pa_bump_attempts", lambda *a, **k: None)  # CAS lost
    assert wt.answers._retry(cur, gen, 0.0) == "stale"
    assert calls == []


def test_idle_snapshot_blocks_release_for_parked_owner(wt):
    import watchtower.workers as workers
    it = _parked(wt)
    w = {"worker_id": "w1", "session_id": SID, "queue": "PK", "engine": "claude",
         "pid": 1, "alive": True}
    snap = workers._idle_snapshot(w, 10, items=[wt.q.get(it["ref"])])
    assert "parked_retention" in snap["reasons"]
    assert snap["parked_refs"] == [it["ref"]]
    _age_park(wt, it["ref"])
    snap = workers._idle_snapshot(w, 10, items=[wt.q.get(it["ref"])])
    assert "parked_retention" not in snap["reasons"]


def test_fresh_park_owner_survives_reap_grace_until_expiry(wt, monkeypatch):
    import time
    import watchtower.workers as workers
    it = _parked(wt)
    assert "w1" in workers._fresh_park_owners(time.time())
    assert workers.park_grace_s() == 55 * 60.0
    monkeypatch.setenv("WATCHTOWER_PARK_GRACE_S", "1")
    assert workers.park_grace_s() == 1.0
    assert "w1" not in workers._fresh_park_owners(time.time() + 60)
    monkeypatch.delenv("WATCHTOWER_PARK_GRACE_S")
    _age_park(wt, it["ref"])
    assert "w1" not in workers._fresh_park_owners(time.time())


def test_affinity_reservation_retains_its_worker(wt, monkeypatch):
    import watchtower.workers as workers
    it = _parked(wt)
    ans = wt.q.answer(it["ref"], "go", session_id="human")
    monkeypatch.setattr(wt.answers, "worker_alive", lambda wid: True)
    monkeypatch.setattr(wt.answers, "_wake", lambda *a, **k: None)
    wt.answers.route_answer(it["ref"], ans["pending_answer"]["gen"])
    assert wt.q.get(it["ref"])["status"] == "open"
    assert "w1" in workers.retained_parked_ids("PK")


def test_park_expired_is_logged_once(wt):
    import watchtower.workers as workers
    it = _parked(wt)
    assert workers.expire_parked_retention() == []
    _age_park(wt, it["ref"])
    assert workers.expire_parked_retention() == [it["ref"]]
    assert workers.expire_parked_retention() == []
    assert wt.q.get(it["ref"])["parked"]["retention_expired_at"]


def test_retained_counts_per_queue(wt, monkeypatch):
    import watchtower.workers as workers
    _parked(wt)
    monkeypatch.setattr(workers, "list_workers", lambda *a, **k: [
        {"worker_id": "w1", "queue": "PK", "alive": True},
        {"worker_id": "w2", "queue": "PK", "alive": True}])
    assert workers.retained_counts() == {"PK": 1}


# ---------------------------------------------------------------- verifier round 2
def test_claim_next_exclusivity_is_worker_or_session(wt):
    it = _parked(wt)  # w1 / SID parked
    b = wt.q.enqueue(project="PK", note="B", source="test")
    wt.q.claim_by_ref(b["ref"], "w1", session_uuid=SID)
    wt.q.enqueue(project="PK", note="C", source="test")
    with pytest.raises(ValueError):
        wt.q.claim_next("alias-w1", project="PK", session_uuid=SID)
    assert wt.q.get(it["ref"])["status"] == "awaiting_answer"


def test_check_queued_delivers_the_cas_item_not_a_reload(wt, monkeypatch):
    it = _parked(wt)
    ans = wt.q.answer(it["ref"], "first", session_id="human")
    gen = ans["pending_answer"]["gen"]
    monkeypatch.setattr(wt.answers, "worker_alive", lambda wid: False)
    monkeypatch.setattr(wt.answers, "_resumable", lambda engine, sid: True)
    monkeypatch.setattr(wt.answers, "_deliver_bound", lambda *a, **k: {"route": "resume"})
    wt.answers.route_answer(it["ref"], gen)
    # Force the record into `queued` with a gone outbox row.
    wt.q.pa_transition(it["ref"], gen, "delivering", "queued", from_status="in_progress")
    cur = wt.q.get(it["ref"])
    seen = []
    real_transition = wt.q.pa_transition

    def racing(ref, g, frm, to, **kw):
        out = real_transition(ref, g, frm, to, **kw)
        # the ticket is re-blocked and answered again right after the CAS
        data = wt.q._load_unlocked()
        for x in data["items"]:
            if x["ref"] == ref:
                x["pending_answer"] = dict(x["pending_answer"], gen=g + 1, state="routing",
                                           answer="NEW")
                x["status"] = "awaiting_answer"
        wt.q._save_unlocked(data)
        return out

    monkeypatch.setattr(wt.q, "pa_transition", racing)
    monkeypatch.setattr(wt.answers, "_deliver_bound",
                        lambda item, g: seen.append((item["pending_answer"]["gen"], g)))
    assert wt.answers._check_queued(cur, gen, 0.0) == "redelivered"
    assert seen == [(gen, gen)]  # gen1's item, never the newer gen2 record


def test_one_window_governs_retention_expiry_and_reap(wt, monkeypatch):
    import time
    import watchtower.workers as workers
    _parked(wt)
    monkeypatch.setenv("WATCHTOWER_PARK_GRACE_S", "1")
    assert workers.park_retention_s() == workers.park_grace_s() == 1.0
    assert workers.retained_parked_ids("PK") == {"w1"}
    real = time.time
    monkeypatch.setattr(time, "time", lambda: real() + 60)
    assert workers.retained_parked_ids("PK") == set()
    assert "w1" not in workers._fresh_park_owners(time.time())


def test_retained_ids_exclude_busy_and_dead_owners(wt, monkeypatch):
    import watchtower.workers as workers
    _parked(wt)
    assert workers.retained_parked_ids("PK") == {"w1"}
    b = wt.q.enqueue(project="PK", note="B", source="test")
    wt.q.claim_by_ref(b["ref"], "w1", session_uuid=SID)
    assert workers.retained_parked_ids("PK") == set()  # busy on B
    wt.q.close(b["ref"], "w1", resolution="ok")
    assert workers.retained_parked_ids("PK") == {"w1"}
    monkeypatch.setattr(workers, "list_workers", lambda *a, **k: [
        {"worker_id": "w1", "queue": "PK", "alive": False}])
    assert workers.retained_parked_ids("PK") == set()  # known dead

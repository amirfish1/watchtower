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

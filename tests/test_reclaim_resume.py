"""WT-30: a verifier-rejected ticket whose builder is dead is resumed or
reopened by the orphan sweep with no human, while ambient/stage claims stay
untouched (OPS-104)."""

from __future__ import annotations

import importlib
import os
import subprocess
import time

import pytest

S = "803f761a-1111-2222-3333-444444444444"
F = "f0f0f0f0-1111-2222-3333-444444444444"


@pytest.fixture()
def wt(tmp_path, monkeypatch):
    for env, name in (
        ("WATCHTOWER_STORE", "queue.json"), ("WATCHTOWER_WORKERS_FILE", "workers.json"),
        ("WATCHTOWER_CONFIG_FILE", "config.json"),
        ("WATCHTOWER_WORKER_SESSIONS_FILE", "worker-sessions.json"),
        ("WATCHTOWER_WORKER_IDS_FILE", "worker-ids.json"),
        ("WATCHTOWER_ACTIVITY_LOG", "activity.log"),
        ("WATCHTOWER_OUTBOX_FILE", "outbox.json"),
        ("WATCHTOWER_STOP_SIGNALS_DIR", "stop-signals"),
    ):
        monkeypatch.setenv(env, str(tmp_path / name))
    monkeypatch.setenv("WATCHTOWER_DELEGATE_URL", "off")
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude-home"))
    monkeypatch.setenv("WATCHTOWER_CLAUDE_PROJECTS_DIR", str(tmp_path / "claude-home" / "projects"))
    import watchtower.queue as q
    import watchtower.config as config
    import watchtower.workers as workers
    import watchtower.messages as messages
    import watchtower.origins as origins
    importlib.reload(q)
    importlib.reload(config)
    importlib.reload(workers)
    importlib.reload(messages)
    monkeypatch.setattr(origins, "ORIGINS_FILE", tmp_path / "origins.json")
    import watchtower.cli as cli
    importlib.reload(cli)

    class Ns:
        pass

    ns = Ns()
    ns.q, ns.workers, ns.messages, ns.origins, ns.cli = q, workers, messages, origins, cli
    ns.tmp = tmp_path
    ns.base = time.time()
    ns.clock = lambda off=0: monkeypatch.setattr(time, "time", lambda: ns.base + off)
    return ns


def _dead_pid():
    p = subprocess.Popen(["true"])
    p.wait()
    return p.pid


def _builder(wt, wid="wt-x", sid=S, alive=False):
    pid = os.getpid() if alive else _dead_pid()
    log = wt.tmp / f"{wid}.log"
    log.write_text("")
    return wt.workers.record_worker(pid, "Q", "claude", wid, str(wt.tmp), str(log), session_id=sid)


def _transcript(wt, sid=S, mtime=None):
    d = wt.tmp / "claude-home" / "projects" / "-p"
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{sid}.jsonl"
    p.write_text('{"type":"user"}\n')
    if mtime is not None:
        os.utime(p, (mtime, mtime))
    return p


def _rejected(wt, claimer="wt-x", sid=S, feedback="verifier failed: missing test"):
    """A builder-claimed ticket sent back by the verifier (reject reclaim)."""
    item = wt.q.enqueue(project="Q", note="work", source="test")
    data = wt.q._load_unlocked()
    for it in data["items"]:
        if it["ref"] == item["ref"]:
            it.update(status="in_review", claimed_by=claimer, claimed_session_id=sid,
                      claimed_at="2000-01-01T00:00:00Z")
    wt.q._save_unlocked(data)
    out = wt.q.reject_with(item["ref"], feedback)
    return out["ref"]


def _sweep(wt):
    return wt.workers.requeue_orphaned_tickets()


def _status(wt, ref):
    return wt.q.get(ref)["status"]


def test_reject_keeps_builder_as_claimer_and_stamps_resume(wt):
    _builder(wt)
    ref = _rejected(wt)
    it = wt.q.get(ref)
    assert it["status"] == "in_progress"
    assert it["claimed_by"] == "wt-x"
    assert it["claimed_session_id"] == S
    assert it["resume"]["state"] == "pending"
    assert it["gate_feedback"].startswith("verifier failed")


def test_dead_builder_with_fresh_transcript_is_reopened_next_tick(wt):
    _builder(wt)
    ref = _rejected(wt)
    wt.q.set_resume_state(ref, S, "running", transport="resume")
    _transcript(wt, mtime=wt.base)  # written at reject time: really "busy" at +31 s
    wt.clock(31)
    assert wt.messages.session_state(S) == "busy"
    reopened = _sweep(wt)
    assert [i["ref"] for i in reopened] == [ref]
    assert "verify-rejected; builder wt-x dead" in reopened[0]["requeue_reason"]
    it = wt.q.get(ref)
    assert it["status"] == "open"
    assert it["gate_feedback"].startswith("verifier failed")
    assert "resume" not in it
    ev = [h for h in it["history"] if h["event"] == "reopen"][-1]
    assert ev["orphan"] is True and ev["displaced_session_id"] == S


def test_delegate_never_starts_then_busy_window(wt):
    _builder(wt)
    ref = _rejected(wt)
    wt.q.set_resume_state(ref, S, "running", transport="delegate")
    wt.clock(31)
    assert _sweep(wt) == []          # inside the 45 s start window
    wt.clock(61)
    assert [i["ref"] for i in _sweep(wt)] == [ref]   # never started -> reopened


def test_delegate_busy_transcript_holds_until_window_lapses(wt):
    _builder(wt)
    ref = _rejected(wt)
    wt.q.set_resume_state(ref, S, "running", transport="delegate")
    _transcript(wt, mtime=wt.base + 50)
    wt.clock(61)
    assert _sweep(wt) == []          # busy (11 s old) holds it
    wt.clock(50 + 121)
    assert [i["ref"] for i in _sweep(wt)] == [ref]


def test_legacy_uuid_claimer_with_worker_origin_is_reopened(wt):
    wt.origins.record(S, role="worker", worker_id="wt-gone")
    ref = _rejected(wt, claimer=S)
    wt.clock(300)
    assert [i["ref"] for i in _sweep(wt)] == [ref]


def test_live_evidence_holds_the_ticket(wt, monkeypatch):
    _builder(wt)
    ref = _rejected(wt)
    wt.q.set_resume_state(ref, S, "running", transport="resume")
    wt.clock(600)
    # (b) a live worker row owns the session
    _builder(wt, wid="wt-y", sid=S, alive=True)
    assert _sweep(wt) == []
    # (c) a live resume child holds it even with no live row
    data = wt.workers._load()
    data["workers"] = [w for w in data["workers"] if w["worker_id"] != "wt-y"]
    wt.workers._save(data)
    monkeypatch.setattr(wt.messages, "live_resume_child", lambda sid: sid == S)
    assert _sweep(wt) == []
    monkeypatch.setattr(wt.messages, "live_resume_child", lambda sid: False)
    assert [i["ref"] for i in _sweep(wt)] == [ref]


def test_queued_resume_with_pending_outbox_row_holds(wt):
    _builder(wt)
    ref = _rejected(wt)
    row = wt.messages.outbox_add(S, "x", ticket_ref=ref, ticket_session=S, delay_s=3600)
    wt.q.set_resume_state(ref, S, "queued", outbox_id=row["id"])
    wt.clock(600)
    assert _sweep(wt) == []


def test_ops104_unknown_labels_are_never_reopened(wt):
    a = wt.q.enqueue(project="Q", note="a", source="test")
    b = wt.q.enqueue(project="Q", note="b", source="test")
    data = wt.q._load_unlocked()
    for it in data["items"]:
        if it["ref"] == a["ref"]:
            it.update(status="in_progress", claimed_by="ambient-label",
                      claimed_at="2000-01-01T00:00:00Z")
        if it["ref"] == b["ref"]:
            it.update(status="in_progress", claimed_by=F, claimed_session_id=F,
                      claimed_at="2000-01-01T00:00:00Z")
    wt.q._save_unlocked(data)
    assert _sweep(wt) == []


def test_stage_provenance_is_terminal(wt):
    wt.origins.record(S, role="verifier")
    wt.workers._save_worker_session_ledger([S]) if hasattr(
        wt.workers, "_save_worker_session_ledger") else None
    ref = _rejected(wt, claimer=S)
    wt.clock(600)
    assert _sweep(wt) == []
    assert _status(wt, ref) == "in_progress"


def test_plain_claims_keep_the_spawn_grace(wt):
    _builder(wt)
    item = wt.q.enqueue(project="Q", note="plain", source="test")
    data = wt.q._load_unlocked()
    for it in data["items"]:
        if it["ref"] == item["ref"]:
            it.update(status="in_progress", claimed_by="wt-x", claimed_session_id=S,
                      claimed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(wt.base - 30)))
    wt.q._save_unlocked(data)
    assert _sweep(wt) == []          # 30 s old: inside the 120 s grace
    wt.clock(200)
    assert [i["ref"] for i in _sweep(wt)] == [item["ref"]]


def test_over_budget_builder_is_released_with_feedback(wt, monkeypatch, capsys):
    _builder(wt)
    ref = _rejected(wt)
    monkeypatch.setattr(wt.workers, "_claude_transcript_bytes", lambda sid: 10_000_000)
    sent = []
    monkeypatch.setattr(wt.messages, "deliver_message", lambda *a, **k: sent.append(a) or {"ok": True})
    assert wt.cli._resume_rejected(wt.q.get(ref), "verifier failed") == 0
    it = wt.q.get(ref)
    assert sent == []
    assert it["status"] == "open" and it["gate_feedback"]
    assert "RESUME" in open(os.environ["WATCHTOWER_ACTIVITY_LOG"]).read()


def test_incident_sized_transcript_is_not_resumed_at_default_budget(wt, monkeypatch):
    """WT-30 D6: the cutoff is the 2.5 MB recycle budget, not 2x it."""
    monkeypatch.delenv("WATCHTOWER_CONTEXT_RECYCLE_BYTES", raising=False)
    monkeypatch.delenv("WATCHTOWER_ANSWER_REQUEUE_BYTES", raising=False)
    _builder(wt)
    ref = _rejected(wt)
    monkeypatch.setattr(wt.workers, "_claude_transcript_bytes", lambda sid: 2_703_575)
    sent = []
    monkeypatch.setattr(wt.messages, "deliver_message", lambda *a, **k: sent.append(a) or {"ok": True})
    wt.cli._resume_rejected(wt.q.get(ref), "verifier failed")
    it = wt.q.get(ref)
    assert sent == [] and it["status"] == "open" and it["gate_feedback"]
    assert "skipped" in open(os.environ["WATCHTOWER_ACTIVITY_LOG"]).read() or "RESUME" in open(os.environ["WATCHTOWER_ACTIVITY_LOG"]).read()


def test_resume_success_records_running_and_total_failure_releases(wt, monkeypatch):
    _builder(wt)
    ref = _rejected(wt)
    monkeypatch.setattr(wt.workers, "_claude_transcript_bytes", lambda sid: 0)
    monkeypatch.setattr(wt.messages, "deliver_message",
                        lambda *a, **k: {"ok": True, "transport": "resume"})
    wt.cli._resume_rejected(wt.q.get(ref), "verifier failed")
    r = wt.q.get(ref)["resume"]
    assert r["state"] == "running" and r["transport"] == "resume"

    ref2 = _rejected(wt)
    monkeypatch.setattr(wt.messages, "deliver_message",
                        lambda *a, **k: {"ok": False, "error": "gone"})
    monkeypatch.setattr(wt.cli, "_resume_session_headless", lambda *a, **k: False)
    wt.cli._resume_rejected(wt.q.get(ref2), "verifier failed")
    assert _status(wt, ref2) == "open"
    assert "failed" in open(os.environ["WATCHTOWER_ACTIVITY_LOG"]).read()


def test_reopen_resume_keeps_the_builder_claimer(wt, monkeypatch):
    _builder(wt)
    item = wt.q.enqueue(project="Q", note="done", source="test")
    data = wt.q._load_unlocked()
    for it in data["items"]:
        if it["ref"] == item["ref"]:
            it.update(status="closed", claimed_by="wt-x", claimed_session_id=S)
    wt.q._save_unlocked(data)
    monkeypatch.setattr(wt.cli, "_deliver_to_blocked_session", lambda *a, **k: 0)
    assert wt.cli.main(["reopen", item["ref"], "--resume", "again", "--reason", "r"]) == 0
    it = wt.q.get(item["ref"])
    assert it["claimed_by"] == "wt-x" and it["claimed_session_id"] == S


def test_displaced_stop_survives_takeover_and_excludes_self_reclaim(wt):
    _builder(wt)
    ref = _rejected(wt)
    wt.q.set_resume_state(ref, S, "running", transport="resume")
    wt.clock(600)
    assert [i["ref"] for i in _sweep(wt)] == [ref]
    stop = wt.messages._stale_claim_stop_prefix
    assert ref in stop(S)                                   # open
    wt.q.update_status(ref, "in_progress", "wt-f", session_uuid=F)
    assert ref in stop(S) and stop(F) == ""                 # taken over
    wt.q.update_status(ref, "closed", "wt-f", session_uuid=F)
    assert ref in stop(S) and stop(F) == ""                 # closed by F


def test_self_reclaim_after_orphan_reopen_is_not_displaced(wt):
    _builder(wt)
    ref = _rejected(wt)
    wt.clock(600)
    _sweep(wt)
    wt.q.update_status(ref, "in_progress", "wt-x", session_uuid=S)
    assert wt.messages._stale_claim_stop_prefix(S) == ""
    wt.q.update_status(ref, "closed", "wt-x", session_uuid=S)
    assert wt.messages._stale_claim_stop_prefix(S) == ""


def test_release_then_fresh_claim_drops_resume_evidence(wt):
    _builder(wt)
    ref = _rejected(wt)
    assert "resume" in wt.q.get(ref)
    wt.q.release(ref, force=True)
    assert "resume" not in wt.q.get(ref)
    wt.q.update_status(ref, "in_progress", "wt-x", session_uuid=S)
    assert "resume" not in wt.q.get(ref)


def _queued_to_delegate(wt, monkeypatch, *, lose_writeback=False):
    _builder(wt)
    ref = _rejected(wt)
    row = wt.messages.outbox_add(S, "x", ticket_ref=ref, ticket_session=S,
                                 delay_s=0, now=wt.base)
    wt.q.set_resume_state(ref, S, "queued", outbox_id=row["id"])
    monkeypatch.setattr(wt.messages, "resolve_target",
                        lambda *a, **k: {"session_id": S, "engine": "claude"})
    monkeypatch.setattr(wt.messages, "deliver",
                        lambda *a, **k: {"ok": True, "transport": "delegate"})
    orig = wt.q.set_resume_state
    if lose_writeback:
        monkeypatch.setattr(wt.q, "set_resume_state",
                            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("lost")))
    wt.messages.drain_outbox(now=wt.base + 60)
    monkeypatch.setattr(wt.q, "set_resume_state", orig)
    return ref, row["id"]


def test_queued_then_delegate_delivery_uses_delivery_time(wt, monkeypatch):
    ref, oid = _queued_to_delegate(wt, monkeypatch)
    entry = wt.messages.outbox_entry(oid)
    assert entry["transport"] == "delegate"
    r = wt.q.get(ref)["resume"]
    assert r["state"] == "running" and r["transport"] == "delegate"
    wt.clock(75)                      # T0+75 > T0+45 but only 15 s after delivery
    assert _sweep(wt) == []
    wt.clock(110)                     # 50 s after delivery, nothing started
    assert [i["ref"] for i in _sweep(wt)] == [ref]


def test_lost_writeback_falls_back_to_the_outbox_row(wt, monkeypatch):
    ref, oid = _queued_to_delegate(wt, monkeypatch, lose_writeback=True)
    assert wt.q.get(ref)["resume"]["state"] == "queued"  # write-back was lost
    wt.clock(75)
    assert _sweep(wt) == []
    wt.clock(110)
    assert [i["ref"] for i in _sweep(wt)] == [ref]


def test_dead_lettered_resume_is_reopened(wt, monkeypatch):
    _builder(wt)
    ref = _rejected(wt)
    row = wt.messages.outbox_add(S, "x", ticket_ref=ref, ticket_session=S,
                                 delay_s=0, now=wt.base)
    wt.q.set_resume_state(ref, S, "queued", outbox_id=row["id"])
    monkeypatch.setattr(wt.messages, "MAX_ATTEMPTS", 1)
    monkeypatch.setattr(wt.messages, "resolve_target",
                        lambda *a, **k: {"session_id": S, "engine": "claude"})
    monkeypatch.setattr(wt.messages, "deliver",
                        lambda *a, **k: {"ok": False, "error": "nope"})
    wt.messages.drain_outbox(now=wt.base + 1)
    assert wt.q.get(ref)["resume"]["state"] == "failed"
    wt.clock(5)
    assert [i["ref"] for i in _sweep(wt)] == [ref]


def test_claim_output_prints_gate_feedback(wt, capsys):
    _builder(wt, wid="wt-z", sid=F, alive=True)
    item = wt.q.enqueue(project="Q", note="sent back", source="test")
    data = wt.q._load_unlocked()
    for it in data["items"]:
        if it["ref"] == item["ref"]:
            it["gate_feedback"] = "verifier failed: missing test"
    wt.q._save_unlocked(data)
    assert wt.cli.main(["claim", "-q", "Q", "--worker", "wt-z"]) == 0
    assert "verifier failed: missing test" in capsys.readouterr().out


def test_expired_outbox_row_writes_failed_back_to_ticket(wt, monkeypatch):
    _builder(wt)
    ref = _rejected(wt)
    row = wt.messages.outbox_add(S, "x", ticket_ref=ref, ticket_session=S,
                                 delay_s=0, ttl_s=1, now=wt.base)
    wt.q.set_resume_state(ref, S, "queued", outbox_id=row["id"])
    wt.messages.drain_outbox(now=wt.base + 5)
    assert wt.messages.outbox_entry(row["id"])["status"] == "dead"
    assert wt.q.get(ref)["resume"]["state"] == "failed"
    assert "failed (outbox dead)" in open(os.environ["WATCHTOWER_ACTIVITY_LOG"]).read()

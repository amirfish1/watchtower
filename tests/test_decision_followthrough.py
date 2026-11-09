"""CCC-392: a saved decision carries a follow-through state; delivery failure
re-delivers the same answer and never re-asks."""
from __future__ import annotations

from watchtower import autodecide

DEC = {"event": "auto_decision", "decision_id": "T-1@2026-11-08T05:32:05Z", "class": "capped-replay-spend"}


def _ft(*after):
    return autodecide.followthrough([{"event": "block"}, DEC, *after], DEC)


def test_saved_decision_not_picked_up_is_pending():
    assert _ft()["state"] == "pending"


def test_consumed_then_applied_then_verified():
    assert _ft({"event": "claim"})["state"] == "consumed"
    assert _ft({"event": "claim"}, {"event": "progress", "text": "ran"})["state"] == "applied"
    done = _ft({"event": "claim"}, {"event": "close", "resolution": {"summary": "Done.\n- ran replay"}})
    assert done["state"] == "verified"


def test_partial_close_after_auto_decision_is_failed_with_reason():
    # The BECKY-3309 shape: the approved replay never ran, ticket closed PARTIAL.
    r = _ft({"event": "claim"}, {"event": "close", "resolution": {
        "summary": "PARTIAL: tests shipped, but the 247-turn replay was not run because the case set is missing.\n- x"}})
    assert r["state"] == "failed"
    assert "replay was not run" in r["reason"]


def test_unresolved_items_make_it_failed_not_verified():
    r = _ft({"event": "close", "resolution": {"summary": "Done", "unresolved": ["CCC-1 still open"]}})
    assert r["state"] == "failed"


def test_handed_off_delivery_is_failed():
    r = _ft({"event": "reopen", "reason": "answer delivery gave up; handed off"})
    assert r["state"] == "failed" and "handed off" in r["reason"]


def test_auto_decisions_listing_includes_followthrough(tmp_path, monkeypatch):
    import importlib
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    import watchtower.queue as q
    importlib.reload(q)
    it = q.enqueue(project="T", note="n")
    with q._FileLock(q._lock_path()):
        data = q._load_unlocked()
        for x in data["items"]:
            if x["ref"] == it["ref"]:
                x["history"] = [DEC, {"event": "close", "resolution": {"summary": "PARTIAL: nope"}}]
        q._save_unlocked(data)
    rows = q.auto_decisions("T")
    assert rows[0]["followthrough"]["state"] == "failed"
    assert "nope" in rows[0]["followthrough"]["reason"]


def test_failed_inject_redelivers_same_answer_and_never_reasks(tmp_path, monkeypatch):
    from tests.test_answer_resume import _answer_args, _blocked_codex_ticket, wt as _wt  # noqa: F401
    import importlib
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_WORKERS_FILE", str(tmp_path / "workers.json"))
    monkeypatch.setenv("WATCHTOWER_WORKER_IDS_FILE", str(tmp_path / "worker-ids.json"))
    monkeypatch.setenv("WATCHTOWER_WORKER_SESSIONS_FILE", str(tmp_path / "ws.json"))
    monkeypatch.setenv("WATCHTOWER_CODEX_THREAD_REGISTRY", str(tmp_path / "ct.json"))
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(tmp_path / "a.log"))
    import watchtower.cli as cli, watchtower.queue as q, watchtower.workers as workers
    import watchtower.messages as messages
    for m in (q, workers, cli):
        importlib.reload(m)
    item = _blocked_codex_ticket(q, workers, tmp_path, worker_id="ccc-deadbeef",
                                 sid="11111111-2222-3333-4444-555555555555")
    blocks_before = sum(1 for h in q.get(item["ref"])["history"] if h.get("event") == "block")
    monkeypatch.setattr(messages, "send", lambda *a, **k: {"ok": False, "error": "unresolvable target"})
    monkeypatch.setattr(cli, "_resume_session_headless", lambda *a, **k: False)
    cli.cmd_answer(_answer_args(item["ref"], "A", engine=None))
    after = q.get(item["ref"])
    # The saved answer travels with the ticket for the next worker; no new owner question.
    assert "[ANSWERED while blocked" in after["text"]
    assert not after.get("needs_input")
    assert sum(1 for h in after["history"] if h.get("event") == "block") == blocks_before


def test_policy_decisions_lists_state_and_reason(tmp_path, monkeypatch, capsys):
    import argparse, importlib
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    import watchtower.queue as q, watchtower.cli as cli
    importlib.reload(q); importlib.reload(cli)
    it = q.enqueue(project="T", note="n")
    with q._FileLock(q._lock_path()):
        data = q._load_unlocked()
        for x in data["items"]:
            x["history"] = [DEC, {"event": "close", "resolution": {"summary": "PARTIAL: replay not run"}}]
        q._save_unlocked(data)
    assert cli.cmd_policy(argparse.Namespace(action="decisions", queue="T", settings=[])) == 0
    out = capsys.readouterr().out
    assert it["ref"] in out and "failed" in out and "replay not run" in out


def test_auto_decisions_include_github_backed_queue_history(tmp_path, monkeypatch):
    """CCC-399: GitHub queues keep auto_decision in backend history, not the local store."""
    import importlib
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    import watchtower.queue as q
    importlib.reload(q)
    gh_item = {"ref": "GH-7", "project": "GH",
               "history": [DEC, {"event": "close", "resolution": {"summary": "PARTIAL: nope"}}]}
    monkeypatch.setattr(q, "_github_projects", lambda: ["GH"])
    monkeypatch.setattr(q, "_github_backend_for_project", lambda p: object() if p == "GH" else None)
    monkeypatch.setattr(q, "list_items", lambda project=None, **kw: [gh_item] if project == "GH" else [])
    rows = q.auto_decisions()
    assert [r["ref"] for r in rows] == ["GH-7"]
    assert rows[0]["followthrough"]["state"] == "failed"
    assert [r["ref"] for r in q.auto_decisions("GH")] == ["GH-7"]
    assert q.auto_decisions("OTHER") == []

    def boom(project=None, **kw):
        raise RuntimeError("gh down")
    monkeypatch.setattr(q, "list_items", boom)
    assert q.auto_decisions() == []  # an unreachable queue must not raise


def test_unrecorded_auto_decision_is_rebuilt_from_saved_answer(tmp_path, monkeypatch):
    """CCC-409: a GitHub-backed ticket with no sidecar entry never stored the
    auto_decision event; the delivered answer must still be listed."""
    from watchtower import queue as q
    answer = ("[answer BECKY-1#7; ignore a repeat of a tag you already applied] This is an "
              "AUTO-DECISION by WatchTower, NOT a human approval.")
    hist = [
        {"event": "block", "at": "2026-11-08T05:00:00Z", "question": "?"},
        {"event": "answer", "at": "2026-11-08T05:01:00Z", "kind": "system", "entry_key": "k1", "text": answer},
        {"event": "answer", "at": "2026-11-08T05:02:00Z", "kind": "system", "entry_key": "k2", "text": answer},
        {"event": "close", "at": "2026-11-08T05:03:00Z",
         "resolution": {"summary": "PARTIAL: not run"}},
    ]
    rows = q._auto_decision_rows({"ref": "BECKY-1", "history": hist})
    assert len(rows) == 1  # re-delivery of the same tag is not a second decision
    assert rows[0]["followthrough"]["state"] == "failed"
    real = hist + [{"event": "auto_decision", "decision_id": rows[0]["event"]["decision_id"]}]
    assert len(q._auto_decision_rows({"ref": "BECKY-1", "history": real})) == 1

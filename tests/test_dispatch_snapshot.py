"""WT-99: enqueue notification reads must not deserialize the ticket archive."""
import json
from types import SimpleNamespace


def test_indexed_snapshot_reads_only_target_and_queue_claims(wt_env, monkeypatch):
    q = wt_env.queue
    target = q.enqueue(project="Q", note="new", gates=["plan"])
    busy = q.enqueue(project="Q", note="busy")
    archived = q.enqueue(project="Q", note="old")
    foreign = q.enqueue(project="OTHER", note="busy elsewhere")
    conn = q._connect(q._db_path())
    try:
        for item in (busy, foreign):
            item.update(status="in_progress", claimed_by="worker-" + item["project"])
            conn.execute("UPDATE items SET status=?, item_json=? WHERE ref=?",
                         (item["status"], json.dumps(item), item["ref"]))
        # If the reader loads irrelevant archived payloads, decoding fails.
        conn.execute("UPDATE items SET status='closed', item_json='not json' WHERE ref=?",
                     (archived["ref"],))
        conn.commit()
    finally:
        conn.close()
    monkeypatch.setattr(q, "_load_unlocked", lambda **_: (_ for _ in ()).throw(
        AssertionError("dispatch loaded the whole store")))
    page = q.dispatch_snapshot("q", target["ref"].lower())
    assert [it["ref"] for it in page] == [target["ref"], busy["ref"]]
    assert q.plan_pending(page[0])
    assert page[1]["claimed_by"] == "worker-Q"
    page[1]["claimed_by"] = "mutated"
    assert q.dispatch_snapshot("Q")[0]["claimed_by"] == "worker-Q"


def test_snapshot_uses_github_listing_without_per_issue_fetch(wt_env, monkeypatch):
    q = wt_env.queue
    rows = [{"ref": "GHI-1", "status": "open", "readiness": "needs-spec"},
            {"ref": "GHI-2", "status": "in_progress", "claimed_by": "busy"},
            {"ref": "GHI-3", "status": "closed"}]
    calls = []
    backend = SimpleNamespace(list_items=lambda **kw: calls.append(kw) or rows)
    monkeypatch.setattr(q, "_github_backend_for_project", lambda _: backend)
    assert q.dispatch_snapshot("GHI", "GHI-1") == rows[:2]
    assert calls == [{"status": None, "lane": None, "fresh": False, "strict": False}]


def test_snapshot_legacy_store_fallback(wt_env, monkeypatch):
    q = wt_env.queue
    missing = wt_env.tmp / "missing.db"
    monkeypatch.setattr(q, "_db_path", lambda: missing)
    rows = [{"ref": "Q-1", "status": "open"},
            {"ref": "Q-2", "status": "in_progress"},
            {"ref": "Q-3", "status": "closed"}]
    monkeypatch.setattr(q, "list_items", lambda **_: rows)
    assert q.dispatch_snapshot("Q", "Q-1") == rows[:2]
    assert q.dispatch_snapshot("Q") == rows[1:2]

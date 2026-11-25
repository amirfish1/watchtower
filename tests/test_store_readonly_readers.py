"""WT-113: reconcile readers share the parsed store instead of copying it.

Each unscoped private copy of the store (~95% closed archive) held the GIL
for ~1 s and stalled the daemon's HTTP threads. Readers that never modify a
ticket now read the shared normalized snapshot; this pins that they don't.
"""
import copy
import json


def _write(q, item):
    conn = q._connect(q._db_path())
    try:
        with conn:
            conn.execute(q._ITEM_UPSERT, q._item_row(item))
            conn.execute("UPDATE meta SET value = value + 1 WHERE key = 'revision'")
    finally:
        conn.close()


def _raw(q):
    conn = q._connect(q._db_path())
    try:
        return dict(conn.execute("SELECT ref, item_json FROM items").fetchall())
    finally:
        conn.close()


def test_reconcile_readers_never_modify_the_shared_snapshot(wt_env):
    q, workers, health = wt_env.queue, wt_env.workers, wt_env.health
    from watchtower import answers, liveness, stages

    old = "2026-01-01T00:00:00Z"
    base = q.enqueue(project="RO", note="open original")
    dead = q.enqueue(project="RO", note="dead claim")
    parked = q.enqueue(project="RO", note="parked")
    sent = q.enqueue(project="RO", note="sent back")
    filing = q.enqueue(project="RO", note="assessment filing")
    follower = q.enqueue(project="RO", note="duplicate follower")
    _write(q, dict(dead, status="in_progress", claimed_by="wt-dead0000", claimed_at=old,
                   claimed_session_id="00000000-dead-0000-0000-000000000000"))
    _write(q, dict(parked, status=q.PARKED_STATUS, claimed_by="wt-park0000",
                   parked={"worker_id": "wt-park0000", "at": old}))
    _write(q, dict(sent, status="in_progress", claimed_by="wt-sent0000", claimed_at=old,
                   sent_back={"at": old, "worker_id": "wt-sent0000", "reason": "redo"}))
    _write(q, dict(filing, status="closed", closed_at=old, updated_at=old,
                   assessment={"status": "filing"}))
    _write(q, dict(follower, duplicate_of=base["ref"]))

    shared = q._load_unlocked(readonly=True)["items"]
    before = {it["ref"]: copy.deepcopy(it) for it in shared}
    raw_before = _raw(q)

    readers = [
        health.all_status, workers.release_zombie_workers, workers._affinity_rows,
        workers._parked_rows, workers.retained_parked_ids, workers.expire_parked_retention,
        workers.requeue_orphaned_tickets, workers.backfill_claimed_session_ids,
        liveness.report, liveness.sweep, q.assessment_targets_for_sweep,
        answers.resume_dependency_waits, answers.route_pending_answers,
        stages.reconcile_stages,
    ]
    def list_copies():
        return {r["op_caller"]: r["count"] for r in q.store_stats(top=1000)["top_callers"]
                if r["op_caller"].startswith("mutable_load queue.list_items")}

    copies = list_copies()
    for reader in readers:
        reader()
    raw_after = _raw(q)
    for it in shared:
        if raw_after.get(it["ref"]) == raw_before.get(it["ref"]):
            assert it == before[it["ref"]], f"{it['ref']} shared snapshot modified in memory"
    # The fixture exercises real work (writes go through the queue API) ...
    assert raw_after != raw_before
    # ... and the readers themselves no longer copy the whole store.
    assert list_copies() == copies


def test_concurrent_readers_share_one_reread_after_a_write(wt_env, monkeypatch):
    """After a store write, every thread that read the store re-read every
    row at once (reconcile, status refresh, each HTTP request). One re-reads;
    the others wait and reuse its snapshot."""
    import threading
    import time

    q = wt_env.queue
    q.enqueue(project="RO", note="first")
    q._load_unlocked(readonly=True)
    item = dict(q._load_unlocked(readonly=True)["items"][0], title="changed")
    _write(q, item)

    rereads = []
    real = q._normalize_items

    def slow_normalize(items):
        rereads.append(1)
        time.sleep(0.3)  # long enough that unserialized re-reads overlap
        return real(items)

    monkeypatch.setattr(q, "_normalize_items", slow_normalize)
    seen = []
    threads = [threading.Thread(target=lambda: seen.append(q._load_unlocked(readonly=True)))
               for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)
    assert len(seen) == 4 and all(s is seen[0] for s in seen)
    assert seen[0]["items"][0]["title"] == "changed"
    assert len(rereads) == 1
    mutable = q._load_unlocked()  # a private copy, still made off the shared read
    assert mutable["items"][0]["title"] == "changed" and mutable is not seen[0]
    assert len(rereads) == 1

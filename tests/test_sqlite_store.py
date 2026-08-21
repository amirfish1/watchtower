"""SQLite queue store: the .db beside the legacy JSON path is authoritative
once it exists; JSON is a migration source and an export format only.

Design: docs/superpowers/specs/2026-08-20-sqlite-store-design.md
"""

from __future__ import annotations

import importlib
import json
import sqlite3

import pytest


@pytest.fixture()
def wt(tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(tmp_path / "activity.log"))
    monkeypatch.setenv("WATCHTOWER_CONFIG_FILE", str(tmp_path / "queue-config.json"))
    monkeypatch.setenv("WATCHTOWER_GH_LIST_CACHE_FILE", str(tmp_path / "gh-list-cache.json"))

    import watchtower.cli as cli
    import watchtower.queue as q

    importlib.reload(q)
    importlib.reload(cli)

    class Ns:
        pass

    ns = Ns()
    ns.json_store = tmp_path / "queue.json"
    ns.db = tmp_path / "queue.db"
    ns.q = q
    ns.cli = cli
    return ns


def _db_rows(db):
    conn = sqlite3.connect(str(db))
    try:
        return dict(conn.execute("SELECT number, item_json FROM items").fetchall())
    finally:
        conn.close()


def _seed_legacy_json(ns, counter=0, items=()):
    ns.json_store.write_text(json.dumps({"counter": counter, "items": list(items)}))


# ---------------------------------------------------------------- fresh stores


def test_first_mutation_creates_db_not_json(wt):
    it = wt.q.enqueue(project="NEW", note="first")
    assert wt.db.exists()
    assert not wt.json_store.exists()
    assert it["ref"] == "NEW-1"
    assert wt.q.store_path() == wt.db


def test_store_path_is_json_until_db_exists(wt):
    assert wt.q.store_path() == wt.json_store
    wt.q.enqueue(project="P", note="x")
    assert wt.q.store_path() == wt.db


def test_roundtrip_through_public_api(wt):
    it = wt.q.enqueue(project="RT", note="hello", text="full prompt")
    claimed = wt.q.claim_by_ref(it["ref"], "worker-1")
    assert claimed["status"] == "in_progress"
    closed = wt.q.close(it["ref"], session_id="worker-1", resolution="done")
    assert closed["status"] == "closed"
    again = wt.q.get(it["ref"])
    assert again["resolution"]["summary"] == "done"
    assert [e["event"] for e in again["history"]] == ["filed", "claim", "close"]


# ------------------------------------------------------------------- migration


def test_json_store_migrates_on_first_mutation(wt):
    _seed_legacy_json(
        wt,
        counter=7,
        items=[
            {
                "number": 5,
                "project": "OLD",
                "seq": 1,
                "ref": "OLD-1",
                "status": "closed",
                "note": "legacy closed",
            },
            {
                "number": 7,
                "project": "OLD",
                "seq": 2,
                "ref": "OLD-2",
                "status": "open",
                "note": "legacy open",
            },
        ],
    )
    # Readers see the JSON before any migration happens (and create no DB).
    assert {it["ref"] for it in wt.q.list_items(project="OLD")} == {"OLD-1", "OLD-2"}
    assert not wt.db.exists()

    it = wt.q.enqueue(project="OLD", note="post-migration ticket")
    assert wt.db.exists()
    # Refs and the counter survive: the new ticket continues the sequence.
    assert it["number"] == 8
    assert it["ref"] == "OLD-3"
    rows = _db_rows(wt.db)
    assert set(rows) == {5, 7, 8}
    # The JSON file is left in place, frozen: mutations no longer touch it.
    frozen = json.loads(wt.json_store.read_text())
    assert {i["ref"] for i in frozen["items"]} == {"OLD-1", "OLD-2"}


def test_json_edits_after_migration_are_ignored(wt):
    wt.q.enqueue(project="A", note="in db")
    _seed_legacy_json(wt, counter=99, items=[{"number": 50, "project": "B", "ref": "B-1", "status": "open"}])
    assert [it["project"] for it in wt.q.list_items()] == ["A"]


def test_migrate_store_cli(wt, capsys):
    _seed_legacy_json(wt, counter=3, items=[{"number": 3, "project": "M", "seq": 1, "ref": "M-1", "status": "open", "note": "x"}])
    assert wt.cli.main(["migrate-store"]) == 0
    out = capsys.readouterr().out
    assert "1 item" in out
    assert wt.db.exists()
    assert wt.q.get("M-1")["note"] == "x"
    # Second run is a no-op, not an error.
    assert wt.cli.main(["migrate-store"]) == 0


# ------------------------------------------------------------------ diff saves


def test_mutation_touches_only_its_row(wt):
    a = wt.q.enqueue(project="D", note="a")
    b = wt.q.enqueue(project="D", note="b")
    before = _db_rows(wt.db)
    wt.q.claim_by_ref(b["ref"], "w-1")
    after = _db_rows(wt.db)
    assert after[a["number"]] == before[a["number"]]
    assert after[b["number"]] != before[b["number"]]


def test_deleted_items_are_removed_from_db(wt):
    a = wt.q.enqueue(project="DEL", note="keep")
    b = wt.q.enqueue(project="DEL", note="drop")
    with wt.q._FileLock(wt.q._lock_path()):
        data = wt.q._load_unlocked()
        data["items"] = [it for it in data["items"] if it["number"] != b["number"]]
        wt.q._save_unlocked(data)
    assert set(_db_rows(wt.db)) == {a["number"]}
    assert wt.q.get(b["ref"]) is None


def test_revision_bumps_on_every_save(wt):
    assert wt.q.revision() == 0
    wt.q.enqueue(project="R", note="one")
    r1 = wt.q.revision()
    assert r1 >= 1
    wt.q.enqueue(project="R", note="two")
    assert wt.q.revision() > r1


def test_counter_guard_still_applies(wt):
    # A stored counter behind the max item number gets bumped on load (WT-2).
    _seed_legacy_json(wt, counter=1, items=[{"number": 9, "project": "G", "seq": 1, "ref": "G-1", "status": "open"}])
    it = wt.q.enqueue(project="G", note="new")
    assert it["number"] == 10


# -------------------------------------------------------------- failure modes


def test_corrupt_json_without_db_fails_closed(wt):
    wt.json_store.write_text("{not-json")
    with pytest.raises(Exception):
        wt.q._load_unlocked(strict=True)
    assert wt.q._load_unlocked() == {"counter": 0, "items": []}
    # A corrupt source must never silently become an empty authoritative DB.
    assert not wt.db.exists()


def test_corrupt_db_fails_closed(wt):
    wt.q.enqueue(project="C", note="x")
    wt.db.write_bytes(b"garbage not a database file")
    with pytest.raises(Exception):
        wt.q._load_unlocked(strict=True)
    assert wt.q._load_unlocked() == {"counter": 0, "items": []}


# ---------------------------------------------------------------- json export


def test_export_json_roundtrip(wt, tmp_path, capsys):
    wt.q.enqueue(project="EX", note="ticket one")
    wt.q.enqueue(project="EX", note="ticket two")
    out_path = tmp_path / "dump.json"
    assert wt.cli.main(["export-json", "-o", str(out_path)]) == 0
    capsys.readouterr()
    dumped = json.loads(out_path.read_text())
    assert dumped["counter"] == 2
    assert [i["note"] for i in dumped["items"]] == ["ticket one", "ticket two"]
    # No -o prints to stdout.
    assert wt.cli.main(["export-json"]) == 0
    assert json.loads(capsys.readouterr().out)["counter"] == 2

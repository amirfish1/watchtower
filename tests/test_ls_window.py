"""`wt ls` bounded time-window reads (WT-60)."""

from __future__ import annotations

import importlib
import json


def _setup(tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(tmp_path / "activity.log"))
    import watchtower.queue as q
    import watchtower.cli as cli
    importlib.reload(q)
    importlib.reload(cli)
    return q, cli


def _seed(q, tmp_path):
    items = [q.enqueue(project="DEMO", title=f"t{i}", note="n", text="") for i in range(4)]
    r = [i["ref"] for i in items]
    with q._FileLock(q._lock_path()):
        data = q._load_unlocked()
        by = {i["ref"]: i for i in data["items"]}
        by[r[0]].update(status="closed", closed_at="2026-11-01T00:00:00Z")
        by[r[1]].update(status="closed", closed_at="2026-11-09T00:00:00Z")
        by[r[2]].setdefault("history", []).append({"event": "auto_decision", "at": "2026-11-10T00:00:00Z"})
        by[r[2]]["history"].append({"event": "auto_decision", "at": "2026-10-01T00:00:00Z"})
        q._save_unlocked(data)
    return r


def test_closed_since_and_pagination(tmp_path, monkeypatch, capsys):
    q, cli = _setup(tmp_path, monkeypatch)
    r = _seed(q, tmp_path)
    assert cli.main(["ls", "-q", "DEMO", "--json", "--closed-since", "2026-11-05T00:00:00Z"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert [i["ref"] for i in out["items"]] == [r[1]]
    assert out["complete"] is True and out["total"] == 1

    assert cli.main(["ls", "-q", "DEMO", "--json", "--closed-since", "2026-10-01T00:00:00Z",
                     "--event", "auto_decision", "--event-since", "2026-11-01T00:00:00Z",
                     "--limit", "2"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["total"] == 3 and out["complete"] is False and out["next_offset"] == 2
    assert cli.main(["ls", "-q", "DEMO", "--json", "--closed-since", "2026-10-01T00:00:00Z",
                     "--event", "auto_decision", "--event-since", "2026-11-01T00:00:00Z",
                     "--limit", "2", "--offset", "2"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["complete"] is True and len(out["items"]) == 1
    ev = out["items"][0]["window_events"]
    assert len(ev) == 1 and ev[0]["at"] == "2026-11-10T00:00:00Z"


def test_ref_selector_and_bad_time(tmp_path, monkeypatch, capsys):
    q, cli = _setup(tmp_path, monkeypatch)
    r = _seed(q, tmp_path)
    assert cli.main(["ls", "-q", "DEMO", "--json", "--ref", r[3]]) == 0
    assert [i["ref"] for i in json.loads(capsys.readouterr().out)["items"]] == [r[3]]
    assert cli.main(["ls", "-q", "DEMO", "--closed-since", "garbage"]) == 2
    # no selector: legacy bare-list JSON
    assert cli.main(["ls", "-q", "DEMO", "--json"]) == 0
    assert isinstance(json.loads(capsys.readouterr().out), list)


# --- WT-60: the read is index-backed, not a post-load filter -----------------

def _no_full_load(q, monkeypatch):
    def boom(*a, **k):
        raise AssertionError("full store load during a windowed read")
    monkeypatch.setattr(q, "list_items", boom)
    monkeypatch.setattr(q, "_read_db", boom)
    monkeypatch.setattr(q, "_load_unlocked", boom)


def test_window_read_never_loads_the_store(tmp_path, monkeypatch, capsys):
    q, cli = _setup(tmp_path, monkeypatch)
    r = _seed(q, tmp_path)
    _no_full_load(q, monkeypatch)
    assert cli.main(["ls", "-q", "DEMO", "--json", "--closed-since", "2026-11-05T00:00:00Z"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["index"] == "sqlite" and [i["ref"] for i in out["items"]] == [r[1]]
    assert cli.main(["ls", "-q", "DEMO", "--json", "--event", "auto_decision",
                     "--event-since", "2026-11-01T00:00:00Z"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert [i["ref"] for i in out["items"]] == [r[2]]
    assert len(out["items"][0]["window_events"]) == 1


def test_stale_index_rows_are_repaired(tmp_path, monkeypatch, capsys):
    """Older writers and raw SQL queue dirty keys for the next indexed read."""
    import sqlite3
    q, cli = _setup(tmp_path, monkeypatch)
    r = _seed(q, tmp_path)
    db = q._db_path()
    conn = sqlite3.connect(str(db))
    row = conn.execute("SELECT item_json FROM items WHERE ref=?", (r[3],)).fetchone()
    it = json.loads(row[0])
    it.update(status="closed", closed_at="2026-11-10T12:00:00Z", updated_at="2026-11-10T12:00:00Z")
    conn.execute("UPDATE items SET item_json=?, updated_at=?, status='closed' WHERE ref=?",
                 (q._canon(it), it["updated_at"], r[3]))  # index columns left stale
    conn.commit(); conn.close()
    assert cli.main(["ls", "-q", "DEMO", "--json", "--closed-since", "2026-11-10T00:00:00Z"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert [i["ref"] for i in out["items"]] == [r[3]] and out["index"] == "sqlite"


def test_legacy_db_without_index_columns_is_upgraded(tmp_path, monkeypatch, capsys):
    import sqlite3
    q, cli = _setup(tmp_path, monkeypatch)
    r = _seed(q, tmp_path)
    conn = sqlite3.connect(str(q._db_path()))
    conn.execute("DROP INDEX idx_items_closed"); conn.execute("DROP INDEX idx_items_updated")
    for col in ("closed_ts", "updated_ts", "window_seq", "window_client", "window_blocked", "window_unresolved"):
        conn.execute(f"ALTER TABLE items DROP COLUMN {col}")
    conn.execute("DROP TABLE item_events")
    conn.execute("DELETE FROM meta WHERE key='window_index_version'")
    conn.commit(); conn.close()
    assert cli.main(["ls", "-q", "DEMO", "--json", "--closed-since", "2026-10-01T00:00:00Z"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert {i["ref"] for i in out["items"]} == {r[0], r[1]} and out["index"] == "sqlite"


def test_index_follows_reopening(tmp_path, monkeypatch, capsys):
    q, cli = _setup(tmp_path, monkeypatch)
    r = _seed(q, tmp_path)
    with q._FileLock(q._lock_path()):
        data = q._load_unlocked()
        for it in data["items"]:
            if it["ref"] == r[1]:
                it["status"] = "open"; it["closed_at"] = None; it["updated_at"] = "2026-11-12T00:00:00Z"
        q._save_unlocked(data)
    assert cli.main(["ls", "-q", "DEMO", "--json", "--closed-since", "2026-11-05T00:00:00Z"]) == 0
    assert json.loads(capsys.readouterr().out)["total"] == 0


def test_windowed_read_is_much_cheaper_than_a_full_scan(tmp_path, monkeypatch):
    """Measured against the failure: with a large history the indexed read
    parses only the page, the old path parsed every row."""
    import time
    q, cli = _setup(tmp_path, monkeypatch)
    n = 4000
    items = [{"number": i + 1, "project": "DEMO", "ref": f"DEMO-{i+1}", "seq": i + 1,
              "status": "closed", "updated_at": f"2026-0{1 + i % 9}-01T00:00:00Z",
              "closed_at": f"2026-0{1 + i % 9}-01T00:00:00Z", "title": "t" * 200,
              "history": [{"event": "x", "at": "2026-01-01T00:00:00Z", "text": "y" * 500}] * 20}
             for i in range(n)]
    with q._FileLock(q._lock_path()):
        q._save_unlocked({"counter": n, "items": items})
    spec = q.WindowSpec(closed_since=q._iso_ts("2026-09-01T00:00:00Z"))
    q.window_items("DEMO", spec)  # warm
    t0 = time.perf_counter()
    page, meta = q.window_items("DEMO", spec, limit=50)
    fast = time.perf_counter() - t0
    assert meta["index"] == "sqlite" and meta["returned"] == 50 and not meta["complete"]
    assert meta["total"] == len([i for i in items if i["closed_at"].startswith("2026-09")])
    t0 = time.perf_counter()
    import sqlite3
    conn = sqlite3.connect(str(q._db_path()))
    scanned = [json.loads(r[0]) for r in conn.execute("SELECT item_json FROM items")]
    slow = time.perf_counter() - t0
    assert len(scanned) == n
    # Timing is recorded as evidence, not asserted against scheduler noise.
    print(f"indexed={fast:.6f}s scan={slow:.6f}s rows={n}")


def test_client_id_filter_combines_with_window(tmp_path, monkeypatch, capsys):
    q, cli = _setup(tmp_path, monkeypatch)
    r = _seed(q, tmp_path)
    with q._FileLock(q._lock_path()):
        data = q._load_unlocked()
        for it in data["items"]:
            if it["ref"] == r[1]:
                it["client_id"] = "abc"
        q._save_unlocked(data)
    assert cli.main(["ls", "-q", "DEMO", "--json", "--closed-since", "2026-10-01T00:00:00Z",
                     "--client-id", "abc"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert [i["ref"] for i in out["items"]] == [r[1]] and out["total"] == 1 and out["complete"]


def test_filter_before_pagination_and_dependency_lookup(tmp_path, monkeypatch, capsys):
    q, cli = _setup(tmp_path, monkeypatch)
    refs = _seed(q, tmp_path)
    dependency = q.enqueue(project="OTHER", note="dependency")
    with q._FileLock(q._lock_path()):
        data = q._load_unlocked()
        for it in data["items"]:
            if it["ref"] == refs[2]:
                it["needs_input"] = True
                it["blocked_by"] = [dependency["ref"]]
                it["wait"] = {"on": "dependency", "ref": dependency["ref"]}
        q._save_unlocked(data)
    _no_full_load(q, monkeypatch)
    assert cli.main(["ls", "-q", "DEMO", "--json", "--updated-since", "2000-01-01",
                     "--status", "blocked", "--limit", "1"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["total"] == out["returned"] == 1 and out["complete"]
    assert out["items"][0]["ref"] == refs[2]
    assert dependency["ref"] in str(out["items"][0]["waiting_on"])


def test_same_length_same_clock_event_edits_and_deletion(tmp_path, monkeypatch):
    q, _ = _setup(tmp_path, monkeypatch)
    refs = _seed(q, tmp_path)
    conn = q.sqlite3.connect(str(q._db_path()))
    blob, = conn.execute("SELECT item_json FROM items WHERE ref=?", (refs[2],)).fetchone()
    # Neither length nor updated_at changes; the trigger must still catch this.
    edited = blob.replace("2026-11-10T00:00:00Z", "2026-11-12T00:00:00Z")
    assert len(edited) == len(blob)
    conn.execute("UPDATE items SET item_json=? WHERE ref=?", (edited, refs[2]))
    conn.commit()
    spec = q.WindowSpec(events=["auto_decision"], event_since=q._window_ts("2026-11-11T00:00:00Z"))
    rows, meta = q.window_items("DEMO", spec)
    assert [it["ref"] for it in rows] == [refs[2]] and meta["complete"]
    conn.execute("DELETE FROM items WHERE ref=?", (refs[2],))
    conn.commit()
    rows, meta = q.window_items("DEMO", spec)
    assert not rows and meta["total"] == 0
    assert conn.execute("SELECT count(*) FROM item_events WHERE number NOT IN (SELECT number FROM items)").fetchone()[0] == 0
    conn.close()


def test_epoch_and_exclusive_boundaries(tmp_path, monkeypatch):
    q, _ = _setup(tmp_path, monkeypatch)
    with q._FileLock(q._lock_path()):
        q._save_unlocked({"counter": 2, "items": [
            {"number": n + 1, "project": "DEMO", "seq": n + 1, "ref": f"DEMO-{n+1}",
             "status": "closed", "closed_at": t, "updated_at": t,
             "history": [{"event": "auto_decision_reversed", "at": t}]}
            for n, t in enumerate(["1970-01-01T00:00:00Z", "1970-01-01T01:00:00+00:00"]) ]})
    spec = q.WindowSpec(closed_since=0, closed_until=3600,
                        events=["auto_decision_reversed"], event_since=0, event_until=3600)
    rows, _ = q.window_items("DEMO", spec)
    assert [it["ref"] for it in rows] == ["DEMO-1"]
    assert len(rows[0]["window_events"]) == 1


def test_only_page_payloads_are_decoded_and_query_uses_index(tmp_path, monkeypatch):
    q, _ = _setup(tmp_path, monkeypatch)
    _seed(q, tmp_path)
    decoded = []
    loads = json.loads
    def track(blob, *a, **kw):
        decoded.append(blob)
        return loads(blob, *a, **kw)
    monkeypatch.setattr(q.json, "loads", track)
    spec = q.WindowSpec(closed_since=q._window_ts("2026-10-01T00:00:00Z"))
    page, meta = q.window_items("DEMO", spec, limit=1)
    assert len(page) == len(decoded) == 1 and meta["total"] == 2
    conn = q._connect(q._db_path())
    sql, args = q._window_query("DEMO", spec, "all", None)
    plan = " ".join(str(row) for row in conn.execute("EXPLAIN QUERY PLAN SELECT i.item_json " + sql, args))
    assert "idx_items_closed" in plan and "SCAN items" not in plan
    conn.close()


def test_legacy_db_first_write_installs_index(tmp_path, monkeypatch):
    q, _ = _setup(tmp_path, monkeypatch)
    conn = q.sqlite3.connect(str(q._db_path()))
    conn.execute("CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    conn.execute("CREATE TABLE items (number INTEGER PRIMARY KEY, project TEXT, ref TEXT, "
                 "status TEXT, updated_at TEXT, item_json TEXT)")
    conn.commit(); conn.close()
    it = q.enqueue(project="DEMO", note="first legacy write")
    page, meta = q.window_items("DEMO", q.WindowSpec(refs=[it["ref"]]))
    assert [r["ref"] for r in page] == [it["ref"]] and meta["complete"]


def test_unresolved_shorthand_and_legacy_resolution(tmp_path, monkeypatch, capsys):
    q, cli = _setup(tmp_path, monkeypatch)
    refs = _seed(q, tmp_path)
    with q._FileLock(q._lock_path()):
        data = q._load_unlocked()
        by = {it["ref"]: it for it in data["items"]}
        by[refs[0]]["resolution"] = "legacy text summary"
        by[refs[1]]["resolution"] = {"summary": "partial", "unresolved": ["OTHER-1 still open"]}
        q._save_unlocked(data)
    _no_full_load(q, monkeypatch)
    assert cli.main(["ls", "-q", "DEMO", "--json", "--closed-since", "2026-10-01", "--unresolved"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["total"] == 1 and out["complete"] and out["items"][0]["ref"] == refs[1]

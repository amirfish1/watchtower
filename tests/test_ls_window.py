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

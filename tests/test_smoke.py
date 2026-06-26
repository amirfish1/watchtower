"""WatchTower smoke test.

Imports the package and exercises the core loop — enqueue -> claim -> close ->
status — against a temp store. No network, no real engine spawn (spawn-worker
is exercised in dry-run mode only).
"""

from __future__ import annotations

import importlib
import json
from datetime import datetime, timedelta, timezone

import pytest


@pytest.fixture()
def store(tmp_path, monkeypatch):
    """Point the engine at a fresh temp store for each test."""
    path = tmp_path / "wt-test.json"
    monkeypatch.setenv("WATCHTOWER_STORE", str(path))
    monkeypatch.setenv("WATCHTOWER_WORKERS_FILE", str(tmp_path / "workers.json"))
    # Re-import so module-level paths (if any) pick up the env.
    import watchtower.queue as q
    import watchtower.health as health
    import watchtower.workers as workers
    importlib.reload(q)
    importlib.reload(health)
    importlib.reload(workers)
    return path


def test_package_imports():
    import watchtower
    import watchtower.cli  # noqa: F401
    import watchtower.queue  # noqa: F401
    import watchtower.health  # noqa: F401
    import watchtower.workers  # noqa: F401

    assert watchtower.__version__


def test_enqueue_claim_close_status(store):
    import watchtower.queue as q
    import watchtower.health as health

    # enqueue
    item = q.enqueue(project="DEMO", title="x", note="y", text="full detail")
    assert item["ref"] == "DEMO-1"
    assert item["status"] == "open"

    # status: one open, depth 1
    rows = {r["queue"]: r for r in health.all_status()}
    assert rows["DEMO"]["depth"] == 1
    # not stuck yet (just created, within window)
    assert rows["DEMO"]["stuck"] is False

    # claim
    claimed = q.claim_next("worker-1", project="DEMO")
    assert claimed["ref"] == "DEMO-1"
    assert claimed["status"] == "in_progress"
    assert claimed["claimed_by"] == "worker-1"

    # nothing left to claim
    assert q.claim_next("worker-1", project="DEMO") is None

    # close
    closed = q.close("DEMO-1", "worker-1")
    assert closed["status"] == "closed"
    assert closed["closed_by"] == "worker-1"

    # status: drained
    rows = {r["queue"]: r for r in health.all_status()}
    assert rows["DEMO"]["depth"] == 0
    assert rows["DEMO"]["stuck"] is False


def test_queues_counts(store):
    import watchtower.queue as q

    q.enqueue(project="A", note="1")
    q.enqueue(project="A", note="2")
    q.enqueue(project="B", note="3")
    q.claim_next("w", project="A")

    counts = q.queues()
    assert counts["A"]["total"] == 2
    assert counts["A"]["open"] == 1
    assert counts["A"]["in_progress"] == 1
    assert counts["B"]["open"] == 1


def test_stuck_detection(store):
    import watchtower.queue as q
    import watchtower.health as health

    q.enqueue(project="STK", note="old work")
    # Simulate a queue created 30 minutes ago with no progress.
    old = (datetime.now(timezone.utc) - timedelta(minutes=30)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    data = json.loads(store.read_text())
    for it in data["items"]:
        it["created_at"] = old
    store.write_text(json.dumps(data))

    rows = {r["queue"]: r for r in health.all_status()}
    assert rows["STK"]["depth"] == 1
    assert rows["STK"]["stuck"] is True


def test_spawn_worker_dry_run(store):
    import watchtower.workers as workers

    spawned = workers.spawn_workers("DEMO", n=2, engine="claude", dry_run=True)
    assert len(spawned) == 2
    for s in spawned:
        assert s["dry_run"] is True
        assert s["pid"] == 0
        assert s["argv"][0] == "claude"
        assert "DEMO" in s["argv"][1]


def test_cli_enqueue_and_status(store, capsys):
    from watchtower.cli import main

    assert main(["enqueue", "-q", "CLI", "--title", "t", "--note", "n"]) == 0
    out = capsys.readouterr().out
    assert "FILED: CLI-1" in out

    assert main(["status"]) == 0
    out = capsys.readouterr().out
    assert "CLI" in out

    assert main(["claim", "-q", "CLI"]) == 0
    assert main(["close", "CLI-1"]) == 0
    out = capsys.readouterr().out
    assert "CLOSED: CLI-1" in out


def test_serve_is_stub(store, capsys):
    from watchtower.cli import main

    assert main(["serve"]) == 0
    assert "phase 2" in capsys.readouterr().out

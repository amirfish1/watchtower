"""WatchTower smoke test.

Imports the package and exercises the core loop — enqueue -> claim -> close ->
status — against a temp store. No network, no real engine spawn (spawn-worker
is exercised in dry-run mode only).
"""

from __future__ import annotations

import importlib
import json
import threading
import urllib.request
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
    try:
        import watchtower.dashboard as dashboard
        importlib.reload(dashboard)
    except ImportError:
        pass
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


def test_status_includes_workers(store, capsys):
    import watchtower.queue as q
    import watchtower.workers as workers
    from watchtower.cli import main

    q.enqueue(project="WK", note="work")
    # Fake a tracked worker for this queue (our own pid is alive).
    import os

    workers.record_worker(os.getpid(), "WK", "claude", "wk-test01")

    assert main(["status"]) == 0
    out = capsys.readouterr().out
    # Per-queue WORKERS column + the workers section header.
    assert "WORKERS" in out
    assert "1 (1 live)" in out
    assert "wk-test01" in out
    assert "LIVE" in out


def test_dashboard_serves_status_json(store):
    import os

    import watchtower.queue as q
    import watchtower.workers as workers
    import watchtower.dashboard as dashboard

    # A couple of queued items + an aged, stuck one.
    q.enqueue(project="DASH", note="item one")
    q.enqueue(project="DASH", note="item two")
    workers.record_worker(os.getpid(), "DASH", "claude", "dash-w1")

    old = (datetime.now(timezone.utc) - timedelta(minutes=30)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    data = json.loads(store.read_text())
    for it in data["items"]:
        it["created_at"] = old
    store.write_text(json.dumps(data))

    # Bind an ephemeral port, serve exactly one request, hit it.
    httpd = dashboard.ThreadingHTTPServer(("127.0.0.1", 0), dashboard._Handler)
    port = httpd.server_address[1]
    t = threading.Thread(target=httpd.handle_request, daemon=True)
    t.start()
    try:
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port}/api/status", timeout=5
        ) as resp:
            assert resp.status == 200
            payload = json.loads(resp.read().decode())
    finally:
        t.join(timeout=5)
        httpd.server_close()

    assert "queues" in payload and "workers" in payload
    dash = next(r for r in payload["queues"] if r["queue"] == "DASH")
    assert dash["depth"] == 2
    assert dash["stuck"] is True
    assert dash["workers_live"] == 1
    assert any(w["worker_id"] == "dash-w1" for w in payload["workers"])


def test_dashboard_html_renders(store):
    import watchtower.queue as q
    import watchtower.dashboard as dashboard

    q.enqueue(project="HTML", note="needs work")
    old = (datetime.now(timezone.utc) - timedelta(minutes=30)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    data = json.loads(store.read_text())
    for it in data["items"]:
        it["created_at"] = old
    store.write_text(json.dumps(data))

    page = dashboard.render_html(dashboard.status_payload())
    assert "<!doctype html>" in page
    assert "viewport" in page  # mobile-first meta
    assert "HTML" in page
    assert "STUCK" in page

    empty = dashboard.render_html({"queues": [], "workers": []})
    assert "All queues clear." in empty

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
        assert "-p" in s["argv"]  # headless print mode, not interactive
        assert any("DEMO" in a for a in s["argv"])  # queue name in the goal
        assert "bypassPermissions" in s["argv"]  # autonomous drain


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
    # Night-watch redesign: a stuck queue renders the stuck card + STALLED readout.
    assert "card stuck" in page
    assert "STALLED" in page
    # Design-system markers: beacon + the night-watch palette token.
    assert "beacon" in page
    assert "--alarm" in page

    empty = dashboard.render_html({"queues": [], "workers": []})
    assert "All queues clear" in empty


def test_drain_rate_eta_fields(store):
    """A queue with recent closes reports a positive rate + an ETA; an
    untouched queue reports rate 0 and a null ('stalled') ETA."""
    import watchtower.queue as q
    import watchtower.health as health

    # Two open + two closed (closed just now => inside the drain window).
    q.enqueue(project="ETA", note="open one")
    q.enqueue(project="ETA", note="open two")
    a = q.enqueue(project="ETA", note="done a")
    b = q.enqueue(project="ETA", note="done b")
    q.close(a["ref"], "w")
    q.close(b["ref"], "w")

    row = {r["queue"]: r for r in health.all_status()}["ETA"]
    assert row["depth"] == 2
    assert row["drain_rate_per_min"] > 0
    assert row["eta_seconds"] is not None and row["eta_seconds"] > 0
    assert row["eta_human"] and row["eta_human"].startswith("~")

    # A queue with open work but no closes at all => stalled (rate 0, eta null).
    q.enqueue(project="STALL", note="nobody draining")
    stall = {r["queue"]: r for r in health.all_status()}["STALL"]
    assert stall["drain_rate_per_min"] == 0
    assert stall["eta_seconds"] is None
    assert stall["eta_human"] is None


def test_api_status_includes_drain_and_activity(store):
    """/api/status carries drain_rate/eta on queues and active_ref on workers."""
    import os

    import watchtower.queue as q
    import watchtower.workers as workers
    import watchtower.dashboard as dashboard

    q.enqueue(project="DASH", note="item one")
    item = q.enqueue(project="DASH", note="item two")
    a = q.enqueue(project="DASH", note="done a")
    q.close(a["ref"], "w")

    # A tracked worker (our own live pid) that claims one ticket => in_progress.
    workers.record_worker(os.getpid(), "DASH", "claude", "dash-live01")
    q.claim_next("dash-live01", project="DASH")

    httpd = dashboard.ThreadingHTTPServer(("127.0.0.1", 0), dashboard._Handler)
    port = httpd.server_address[1]
    t = threading.Thread(target=httpd.handle_request, daemon=True)
    t.start()
    try:
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port}/api/status", timeout=5
        ) as resp:
            payload = json.loads(resp.read().decode())
    finally:
        t.join(timeout=5)
        httpd.server_close()

    dash = next(r for r in payload["queues"] if r["queue"] == "DASH")
    assert "drain_rate_per_min" in dash
    assert "eta_seconds" in dash
    assert "eta_human" in dash
    assert dash["drain_rate_per_min"] > 0  # one recent close

    w = next(w for w in payload["workers"] if w["worker_id"] == "dash-live01")
    # The worker is joined to the in-progress ticket it claimed.
    assert w["active_ref"] == "DASH-1"
    assert w["active_since_human"] is not None


def test_dashboard_drilldown_page_and_api(store):
    """/q/<queue> renders the queue's tickets; /api/queue/<name> mirrors wt ls."""
    import watchtower.queue as q
    import watchtower.dashboard as dashboard

    q.enqueue(project="DRILL", title="first ticket", note="n1")
    q.enqueue(project="DRILL", title="second ticket", note="n2")

    # Drill-down HTML.
    payload = dashboard.status_payload()
    page = dashboard.render_queue("DRILL", payload, dashboard.queue_tickets("DRILL"))
    assert "DRILL" in page
    assert "first ticket" in page
    assert "all queues" in page  # back link
    assert "DRILL-1" in page

    # JSON for the same queue over the wire.
    httpd = dashboard.ThreadingHTTPServer(("127.0.0.1", 0), dashboard._Handler)
    port = httpd.server_address[1]
    t = threading.Thread(target=httpd.handle_request, daemon=True)
    t.start()
    try:
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port}/api/queue/DRILL", timeout=5
        ) as resp:
            data = json.loads(resp.read().decode())
    finally:
        t.join(timeout=5)
        httpd.server_close()
    assert data["queue"] == "DRILL"
    assert len(data["tickets"]) == 2
    assert data["tickets"][0]["ref"] == "DRILL-1"


def test_dashboard_launch_nonblocking_and_stop(store, tmp_path, monkeypatch, capsys):
    """`wt dashboard --no-open` returns immediately (non-blocking) and writes a
    pidfile; `wt dashboard --stop` tears the background server down."""
    import importlib
    import time

    pidfile = tmp_path / "dashboard.pid"
    monkeypatch.setenv("WATCHTOWER_DASHBOARD_PID", str(pidfile))
    import watchtower.cli as cli
    importlib.reload(cli)

    # Pick a likely-free ephemeral-ish port for the real background server.
    import socket

    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()

    try:
        # Non-blocking: this call must return without serving forever.
        rc = cli.main(["dashboard", "--no-open", "--port", str(port)])
        assert rc == 0
        out = capsys.readouterr().out
        assert "dashboard" in out.lower()
        assert pidfile.exists()
        pid = int(pidfile.read_text().strip())
        # The background server process is alive.
        import os as _os

        _os.kill(pid, 0)

        # Idempotent: a second launch does not start a second server.
        assert cli.main(["dashboard", "--no-open", "--port", str(port)]) == 0
        assert int(pidfile.read_text().strip()) == pid
    finally:
        # --stop kills the background server and removes the pidfile.
        assert cli.main(["dashboard", "--stop"]) == 0
        # Give the process a moment to die, then confirm it is gone.
        for _ in range(20):
            if not pidfile.exists():
                break
            time.sleep(0.05)
        assert not pidfile.exists()


def test_close_with_resolution_round_trips(store):
    """A resolution passed to close() persists and reloads on the item."""
    import watchtower.queue as q

    q.enqueue(project="RES", note="fix the thing")
    closed = q.close(
        "RES-1",
        "worker-9",
        resolution={
            "summary": "did X",
            "caveats": ["watch Y"],
            "follow_ups": ["do Z later"],
            "unresolved": [],
        },
    )
    assert closed["status"] == "closed"
    res = closed["resolution"]
    assert res["summary"] == "did X"
    assert res["caveats"] == ["watch Y"]
    assert res["follow_ups"] == ["do Z later"]
    # Empty list field is dropped on normalize.
    assert "unresolved" not in res

    # Reloads from disk identically.
    again = q.get("RES-1")
    assert again["resolution"] == res

    # A bare-string resolution is accepted as the summary.
    q.enqueue(project="RES", note="another")
    c2 = q.close("RES-2", "worker-9", resolution="just a summary")
    assert c2["resolution"]["summary"] == "just a summary"

    # Back-compat: no resolution -> no key.
    q.enqueue(project="RES", note="third")
    c3 = q.close("RES-3", "worker-9")
    assert "resolution" not in c3


def test_cli_close_builds_resolution(store, capsys):
    """`wt close --summary/--caveat/...` records the resolution + prints it."""
    import watchtower.queue as q
    from watchtower.cli import main

    q.enqueue(project="CLIRES", note="work")
    rc = main([
        "close", "CLIRES-1",
        "--summary", "did X",
        "--caveat", "watch Y",
        "--follow-up", "do Z later",
    ])
    assert rc == 0
    out = capsys.readouterr().out
    assert "CLOSED: CLIRES-1 — did X" in out

    it = q.get("CLIRES-1")
    assert it["resolution"]["summary"] == "did X"
    assert it["resolution"]["caveats"] == ["watch Y"]
    assert it["resolution"]["follow_ups"] == ["do Z later"]

    # The closed wt ls row shows the summary + counts.
    assert main(["ls", "-q", "CLIRES", "--status", "closed"]) == 0
    ls_out = capsys.readouterr().out
    assert "— did X" in ls_out
    assert "1 caveat" in ls_out


def test_cli_close_enqueue_follow_ups(store, capsys):
    """--enqueue-follow-ups files each follow-up/unresolved as a new ticket."""
    import watchtower.queue as q
    from watchtower.cli import main

    q.enqueue(project="CARRY", note="work")
    rc = main([
        "close", "CARRY-1",
        "--summary", "did the main bit",
        "--follow-up", "polish the edges",
        "--unresolved", "the flaky test",
        "--enqueue-follow-ups",
    ])
    assert rc == 0
    out = capsys.readouterr().out
    assert "FILED follow-up:" in out

    open_items = q.list_items(project="CARRY", status="open")
    notes = sorted(i["note"] for i in open_items)
    assert notes == ["polish the edges", "the flaky test"]


def test_dashboard_drilldown_renders_closed_resolution(store):
    """/q/<queue> renders a Closed section with the resolution summary + chips."""
    import watchtower.queue as q
    import watchtower.dashboard as dashboard

    q.enqueue(project="CDASH", title="still open", note="n1")
    q.enqueue(project="CDASH", title="will close", note="n2")
    q.close(
        "CDASH-2",
        "w",
        resolution={"summary": "patched it", "caveats": ["watch the cache"]},
    )

    payload = dashboard.status_payload()
    page = dashboard.render_queue(
        "CDASH", payload, dashboard.queue_tickets("CDASH"),
        closed=dashboard.closed_tickets("CDASH"), total_closed=1,
    )
    assert "Closed" in page
    assert "patched it" in page  # the summary, prominently
    assert "watch the cache" in page  # the caveat chip
    assert "chip caveat" in page  # palette marker for caveats


def test_api_queue_includes_closed_with_resolution(store):
    """/api/queue/<name> returns active tickets + a closed array w/ resolution."""
    import watchtower.queue as q
    import watchtower.dashboard as dashboard

    q.enqueue(project="ACLOSE", note="open one")
    q.enqueue(project="ACLOSE", note="to close")
    q.close("ACLOSE-2", "w", resolution="finished it")

    httpd = dashboard.ThreadingHTTPServer(("127.0.0.1", 0), dashboard._Handler)
    port = httpd.server_address[1]
    t = threading.Thread(target=httpd.handle_request, daemon=True)
    t.start()
    try:
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port}/api/queue/ACLOSE", timeout=5
        ) as resp:
            data = json.loads(resp.read().decode())
    finally:
        t.join(timeout=5)
        httpd.server_close()

    assert data["queue"] == "ACLOSE"
    assert len(data["tickets"]) == 1  # one still open
    assert len(data["closed"]) == 1
    assert data["closed"][0]["ref"] == "ACLOSE-2"
    assert data["closed"][0]["resolution"]["summary"] == "finished it"


def test_worker_activity_join_idle_when_unclaimed(store):
    """A worker holding no in-progress ticket reports idle (active_ref None)."""
    import watchtower.queue as q
    import watchtower.workers as workers

    q.enqueue(project="IDLEQ", note="open work, unclaimed")
    rows = [{"worker_id": "idle-w1", "queue": "IDLEQ"}]
    workers.annotate_activity(rows, q.list_items())
    assert rows[0]["active_ref"] is None
    assert rows[0]["active_since_human"] is None

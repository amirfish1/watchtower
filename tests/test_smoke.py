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
    monkeypatch.setenv("WATCHTOWER_CONFIG_FILE", str(tmp_path / "config.json"))
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(tmp_path / "activity.log"))
    monkeypatch.setenv(
        "WATCHTOWER_CCC_SPAWN_DEFAULTS_FILE", str(tmp_path / "no-ccc-spawn-defaults.json")
    )
    # Re-import so module-level paths (if any) pick up the env.
    import watchtower.queue as q
    import watchtower.health as health
    import watchtower.config as config
    import watchtower.workers as workers
    importlib.reload(q)
    importlib.reload(config)
    importlib.reload(health)
    importlib.reload(workers)
    monkeypatch.setattr(config, "_REGISTRY_FILE", tmp_path / "no-registry.json")
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


def test_help_shows_git_style_grouped_sections(capsys):
    """`wt --help` groups commands under section headers instead of
    argparse's default flat {a,b,c,...} brace listing, ordered by user
    journey: get the service running, check queue health, work tickets, talk
    to other agents, then the low-level worker protocol. "Fleet" was
    dissolved (workers -> Queues, agents/agent folded into Agent messaging)."""
    import watchtower.cli as cli

    with pytest.raises(SystemExit) as exc:
        cli.main(["--help"])
    assert exc.value.code == 0
    out = capsys.readouterr().out

    headers = ["Service:", "Queues:", "Tickets:", "Agent messaging:", "Worker protocol:"]
    for header in headers:
        assert header in out
    # Order matters: this is the intended user-journey ordering.
    positions = [out.index(h) for h in headers]
    assert positions == sorted(positions)

    assert "Fleet:" not in out
    assert "Messaging:" not in out

    # No brace-list dump of every subcommand.
    assert "{status," not in out
    assert "{add," not in out

    # Spot-check: a command still shows up with its one-line help.
    assert "status" in out
    assert "per-queue depth / age / stuck flag" in out

    # `install` is a hidden alias folded into `wt start`'s first-run
    # auto-install; it must not appear in the top-level listing.
    assert "\n    install " not in out
    assert "installs the LaunchAgent on first run" in out  # start's help text

    # Closing hint line.
    assert "wt <command> --help" in out


def test_install_hidden_but_still_works(capsys):
    """`wt install` stays registered as a hidden alias (Change 1): it doesn't
    show up in the grouped listing, but `wt install --help` still exits 0."""
    import watchtower.cli as cli

    with pytest.raises(SystemExit) as exc:
        cli.main(["install", "--help"])
    assert exc.value.code == 0


def test_agents_is_the_single_address_book_command(store, tmp_path, monkeypatch, capsys):
    """`wt agents` consolidates the old `wt agents` (list) / `wt agent`
    (manage) pair, git-remote style: bare `wt agents [--json]` lists,
    `register`/`set-name`/`rm` are nested management verbs, and `wt agent
    ...` keeps working as a hidden compat alias with the identical nested
    structure."""
    import importlib

    monkeypatch.setenv("WATCHTOWER_AGENTS_FILE", str(tmp_path / "agents.json"))
    monkeypatch.setenv(
        "WATCHTOWER_CLAUDE_PROJECTS_DIR", str(tmp_path / "claude-projects")
    )
    import watchtower.cli as cli
    importlib.reload(cli)

    sid_a = "7f72634b-b0bd-4c78-b931-3d877ed84187"
    sid_b = "c44f96bc-d720-49d3-a5e6-115426939f82"

    # Bare `wt agents --json` still lists (empty registry, no live workers).
    rc = cli.main(["agents", "--json"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {"agents": [], "workers": []}

    # `wt agents register x --session <uuid>` names a session; `wt agents
    # --json` then shows it.
    rc = cli.main(["agents", "register", "x", "--session", sid_a])
    assert rc == 0
    capsys.readouterr()
    rc = cli.main(["agents", "--json"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert [a["name"] for a in payload["agents"]] == ["x"]
    assert payload["agents"][0]["session_id"] == sid_a

    # `wt agents rm x` removes it.
    rc = cli.main(["agents", "rm", "x"])
    assert rc == 0
    capsys.readouterr()
    rc = cli.main(["agents", "--json"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["agents"] == []

    # Hidden `wt agent register ...` alias still works.
    rc = cli.main(["agent", "register", "y", "--session", sid_b])
    assert rc == 0
    capsys.readouterr()
    rc = cli.main(["agents", "--json"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert [a["name"] for a in payload["agents"]] == ["y"]

    # `agent` is folded into `agents`; it must not appear as a top-level row
    # (careful to match the row form, since "agent" appears inside other
    # help text such as "push a message to a worker/agent/session").
    with pytest.raises(SystemExit) as exc:
        cli.main(["--help"])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert "\n    agent " not in out
    assert "\n    agents " in out


def test_bare_command_prints_grouped_help(capsys):
    """A bare `wt` invocation prints the same grouped help, not a traceback."""
    import watchtower.cli as cli

    rc = cli.main([])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Service:" in out
    assert "{status," not in out


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


def test_close_unclaimed_backfills_claimed_by(store):
    """Closing a never-claimed ticket with a worker id must not drop
    attribution: claimed_by is backfilled from the closer (WT-81)."""
    import watchtower.queue as q

    item = q.enqueue(project="DEMO", note="drive-by fix")
    assert item["claimed_by"] is None

    closed = q.close(item["ref"], "worker-9")
    assert closed["closed_by"] == "worker-9"
    assert closed["claimed_by"] == "worker-9"


def test_close_keeps_original_claimant(store):
    """A different closer credits the close but never overwrites the
    original claimant."""
    import watchtower.queue as q

    q.enqueue(project="DEMO", note="claimed work")
    claimed = q.claim_next("worker-1", project="DEMO")
    closed = q.close(claimed["ref"], "worker-2")
    assert closed["closed_by"] == "worker-2"
    assert closed["claimed_by"] == "worker-1"


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
        assert "--name" not in s["argv"]  # generic names overwrite later WT titles
        assert f"{s['queue']} queue worker" not in " ".join(s["argv"])
        # claude workers run in stream-json mode; the goal is delivered on the
        # FIFO, not in argv. The queue name lives in the drain goal text.
        assert "stream-json" in s["argv"]
        assert "DEMO" in workers.drain_goal("DEMO", s["worker_id"])
        assert "bypassPermissions" in s["argv"]  # autonomous drain
        # No queue model configured -> no --model flag; the CLI's own
        # configured default applies (the pre-`wt set --model` behavior).
        assert "--model" not in s["argv"]


def test_spawn_worker_queue_model(store):
    import watchtower.config as config
    import watchtower.workers as workers

    config.set_model("DEMO", "claude-sonnet-5")
    try:
        spawned = workers.spawn_workers("DEMO", n=1, engine="claude", dry_run=True)
        argv = spawned[0]["argv"]
        assert argv[argv.index("--model") + 1] == "claude-sonnet-5"
        codex_argv = workers.build_drain_command(
            "DEMO", "codex", "demo-w1", model="gpt-5.5")
        assert codex_argv[:4] == ["codex", "exec", "--model", "gpt-5.5"]
        # Clearing restores the no-flag default.
        config.set_model("DEMO", "")
        spawned = workers.spawn_workers("DEMO", n=1, engine="claude", dry_run=True)
        assert "--model" not in spawned[0]["argv"]
    finally:
        config.set_model("DEMO", "")


def test_ccc_shared_default_model_used_when_queue_unset(store, tmp_path, monkeypatch):
    """A queue with no explicit `wt set --model` falls back to CCC's shared
    spawn-defaults.json for its engine (see config.CCC_SPAWN_DEFAULTS_FILE) --
    not to whichever ambient default the bare CLI happens to have. An
    explicit per-queue override still wins over CCC's default.

    CCC stores bare short aliases for claude (e.g. "sonnet-5") meant for its
    own UI/`/model` picker, not as a `--model` flag value -- WT must expand
    them to the full `claude-` prefixed id before spawning (WT-84), same as
    CCC's own spawn path does internally."""
    import json
    import importlib
    import watchtower.config as config
    import watchtower.workers as workers

    ccc_file = tmp_path / "ccc-spawn-defaults.json"
    ccc_file.write_text(json.dumps({"engine": "claude", "models": {"claude": "sonnet-5"}}))
    monkeypatch.setenv("WATCHTOWER_CCC_SPAWN_DEFAULTS_FILE", str(ccc_file))
    importlib.reload(config)
    importlib.reload(workers)
    try:
        assert config.model("DEMO") == "claude-sonnet-5"
        spawned = workers.spawn_workers("DEMO", n=1, engine="claude", dry_run=True)
        argv = spawned[0]["argv"]
        assert argv[argv.index("--model") + 1] == "claude-sonnet-5"

        # An explicit per-queue override still wins over CCC's default.
        config.set_model("DEMO", "claude-opus-4-8")
        assert config.model("DEMO") == "claude-opus-4-8"
    finally:
        config.set_model("DEMO", "")
        importlib.reload(config)
        importlib.reload(workers)


def test_auto_drain_config(tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_CONFIG_FILE", str(tmp_path / "qc.json"))
    import watchtower.config as config
    importlib.reload(config)
    # default-OFF: a fresh queue is a backlog until you opt in (safe default;
    # no surprise worker spawns on a parking-lot queue).
    assert config.auto_drain("FRESH") is False
    # opt in
    config.set_auto_drain("BACKLOG", True)
    assert config.auto_drain("BACKLOG") is True
    # and opt back out
    config.set_auto_drain("BACKLOG", False)
    assert config.auto_drain("BACKLOG") is False


def test_configured_empty_queue_appears_in_status(tmp_path, monkeypatch):
    """A queue with config but zero tickets should still show in wt status."""
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_CONFIG_FILE", str(tmp_path / "qc.json"))
    import watchtower.config as config
    import watchtower.health as health
    import watchtower.queue as q
    importlib.reload(config)
    importlib.reload(q)
    importlib.reload(health)

    config.set_repo_path("EMPTYCFG", "/tmp/emptycfg")

    row = {r["queue"]: r for r in health.all_status()}["EMPTYCFG"]
    assert row["depth"] == 0
    assert row["state"] == "clear"


def test_cli_enqueue_and_status(store, capsys):
    from watchtower.cli import main

    assert main(["add", "-q", "CLI", "--title", "t", "--note", "n"]) == 0
    out = capsys.readouterr().out
    assert "FILED: CLI-1" in out

    assert main(["status"]) == 0
    out = capsys.readouterr().out
    assert "CLI" in out

    assert main(["claim", "-q", "CLI"]) == 0
    assert main(["close", "CLI-1", "--summary", "done"]) == 0
    out = capsys.readouterr().out
    assert "CLOSED: CLI-1" in out


def test_cli_edit_patches_fields_without_refiling(store, capsys):
    import watchtower.queue as q
    from watchtower.cli import main

    assert main(["add", "-q", "EDT", "--title", "orig", "--type", "bug"]) == 0
    capsys.readouterr()

    assert main([
        "edit", "EDT-1",
        "--priority", "p0", "--type", "feature",
        "--title", "new title", "--readiness", "ready",
    ]) == 0
    out = capsys.readouterr().out
    assert "EDITED: EDT-1" in out

    item = q.get("EDT-1")
    assert item["priority"] == "p0"
    assert item["type"] == "feature"
    assert item["title"] == "new title"
    assert item["readiness"] == "ready"
    # Untouched fields and status/ref/number are left alone -- no refile churn.
    assert item["status"] == "open"
    assert item["ref"] == "EDT-1"


def test_cli_edit_moves_queue_without_refiling(store, capsys):
    import watchtower.queue as q
    from watchtower.cli import main

    assert main(["add", "-q", "MVSRC", "--title", "orig"]) == 0
    capsys.readouterr()

    assert main(["edit", "MVSRC-1", "--queue", "MVDST"]) == 0
    out = capsys.readouterr().out
    assert "EDITED: MVDST-1 (moved MVSRC-1 -> MVDST-1)" in out

    assert q.get("MVSRC-1") is None
    moved = q.get("MVDST-1")
    assert moved["title"] == "orig"
    assert moved["status"] == "open"

    # --queue combined with a field edit applies both in one call.
    assert main(["edit", "MVDST-1", "--queue", "MVDST2", "--priority", "p0"]) == 0
    out = capsys.readouterr().out
    assert "EDITED: MVDST2-1 (moved MVDST-1 -> MVDST2-1)" in out
    combined = q.get("MVDST2-1")
    assert combined["priority"] == "p0"


def test_cli_edit_requires_at_least_one_field(store, capsys):
    from watchtower.cli import main

    assert main(["add", "-q", "EDT2", "--title", "orig"]) == 0
    capsys.readouterr()

    rc = main(["edit", "EDT2-1"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "no fields to edit" in err


def test_cli_edit_unknown_ref(store, capsys):
    from watchtower.cli import main

    rc = main(["edit", "NOPE-1", "--priority", "p0"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "NOPE-1" in err


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


def test_dashboard_html_renders(store, monkeypatch):
    import watchtower.queue as q
    import watchtower.config as config
    import watchtower.dashboard as dashboard

    monkeypatch.setenv("WATCHTOWER_CONFIG_FILE", str(store.parent / "qcfg.json"))
    importlib.reload(config)
    q.enqueue(project="HTML", note="needs work")
    # A queue is "stuck" only when it's opted into draining but making no
    # progress. auto_drain now defaults OFF (a fresh queue is a backlog), so
    # opt HTML in to exercise the stuck-card rendering.
    config.set_auto_drain("HTML", True)
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
    # CCC-style list layout: a stuck queue renders is-stuck group + STUCK state pill.
    assert "is-stuck" in page
    assert "STUCK" in page
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


def test_block_answer_resume_flow(store):
    """WT-28 blocked-work: a claimed ticket can be blocked (stays in_progress,
    not reclaimable), answered (clears needs_input), and exposes a resumable
    session id."""
    import watchtower.queue as q

    q.enqueue(project="BLK", note="needs a human call")
    sid = "11111111-2222-3333-4444-555555555555"
    claimed = q.claim_next("worker-1", project="BLK", session_uuid=sid)
    assert claimed["status"] == "in_progress"

    blocked = q.block(claimed["ref"], session_id=sid,
                      question="ship A or B?", progress="A is safer")
    assert blocked["needs_input"] is True
    assert blocked["status"] == "in_progress"  # never bounces back to open
    assert blocked["block_question"] == "ship A or B?"
    assert blocked["claimed_session_id"] == sid  # resumable
    assert "progress_notes" not in blocked
    assert [e["event"] for e in blocked["history"]][-2:] == ["progress", "block"]
    assert blocked["history"][-2]["text"] == "A is safer"

    # A blocked ticket is NOT reclaimable by another worker.
    assert q.claim_next("worker-2", project="BLK") is None
    assert [it["ref"] for it in q.list_blocked(project="BLK")] == [blocked["ref"]]

    answered = q.answer(blocked["ref"], "go with A", session_id="human")
    assert answered["needs_input"] is False
    assert "answers" not in answered
    assert answered["history"][-1]["event"] == "answer"
    assert answered["history"][-1]["text"] == "go with A"
    assert q.list_blocked(project="BLK") == []

    # Closing clears any lingering block flag.
    closed = q.close(answered["ref"], "worker-1", resolution="shipped A")
    assert closed["status"] == "closed"
    assert closed["needs_input"] is False


def test_release_returns_claim_to_open(store, capsys):
    """WT-86: `wt release <ref>` gives up a claim without closing it, so the
    ticket is reclaimable by another worker."""
    import watchtower.queue as q
    from watchtower.cli import build_parser

    q.enqueue(project="REL", note="claimed defensively, better left for the pool")
    claimed = q.claim_next("worker-1", project="REL")
    assert claimed["status"] == "in_progress"

    parser = build_parser()
    args = parser.parse_args(["release", claimed["ref"], "--worker", "worker-1"])
    rc = args.func(args)
    out = capsys.readouterr().out
    assert rc == 0
    assert "RELEASED" in out

    item = q.get(claimed["ref"])
    assert item["status"] == "open"
    assert item["claimed_by"] is None
    assert item["claimed_at"] is None

    # Now reclaimable by another worker.
    reclaimed = q.claim_next("worker-2", project="REL")
    assert reclaimed["ref"] == claimed["ref"]
    assert reclaimed["claimed_by"] == "worker-2"

    # Releasing a ticket that isn't in_progress is a no-op error, not a crash.
    args2 = parser.parse_args(["release", reclaimed["ref"], "--worker", "worker-1"])
    q.close(reclaimed["ref"], "worker-2", resolution="done")
    rc2 = args2.func(args2)
    assert rc2 == 1
    assert "not in_progress" in capsys.readouterr().err


def test_ticket_history_records_full_lifecycle(store):
    """WT-87: each claim/reopen/close/block is appended to ``history``, not
    just overwritten as the latest snapshot — so a ticket that was claimed by
    A, released, reclaimed by B, blocked, and finally closed by B keeps every
    intermediate event, not only B's final close."""
    import watchtower.queue as q

    q.enqueue(project="HIST", note="track my whole life")
    claimed_a = q.claim_next("worker-a", project="HIST")
    ref = claimed_a["ref"]
    assert [e["event"] for e in claimed_a["history"]] == ["filed", "claim"]
    assert claimed_a["history"][1]["by"]["worker"] == "worker-a"

    released = q.release(ref, session_id="worker-a")
    events = [e["event"] for e in released["history"]]
    assert events == ["filed", "claim", "reopen"]
    assert released["history"][2]["reason"] == "released"

    claimed_b = q.claim_next("worker-b", project="HIST")
    assert claimed_b["ref"] == ref
    events = [e["event"] for e in claimed_b["history"]]
    assert events == ["filed", "claim", "reopen", "claim"]
    assert claimed_b["history"][3]["by"]["worker"] == "worker-b"

    blocked = q.block(ref, session_id="worker-b", question="ship it?")
    events = [e["event"] for e in blocked["history"]]
    assert events == ["filed", "claim", "reopen", "claim", "block"]
    assert blocked["history"][4]["question"] == "ship it?"

    q.answer(ref, "yes", session_id="human")
    closed = q.close(ref, "worker-b", resolution="shipped")
    events = [e["event"] for e in closed["history"]]
    assert events == ["filed", "claim", "reopen", "claim", "block", "answer", "close"]
    assert closed["history"][6]["by"]["worker"] == "worker-b"
    assert closed["history"][6]["resolution"]["summary"] == "shipped"

    # Every entry from worker-a's original claim is still readable off the
    # ticket itself — not just recoverable from activity.log.
    assert claimed_a["history"][1]["at"] <= closed["history"][6]["at"]


def test_discuss_command_prints_resume(store, capsys):
    """`wt discuss <ref> --print` resolves the session + repo into a
    `claude --resume` command without executing it."""
    import watchtower.queue as q
    import watchtower.cli as cli

    q.enqueue(project="DSC", note="blocked thing", repo_path="/tmp/somerepo")
    sid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    item = q.claim_next("w", project="DSC", session_uuid=sid)
    q.block(item["ref"], session_id=sid, question="which?")

    rc = cli.main(["discuss", item["ref"], "--print"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "claude" in out and "--resume" in out and sid in out
    assert "/tmp/somerepo" in out


def test_worker_activity_join_idle_when_unclaimed(store):
    """A worker holding no in-progress ticket reports idle (active_ref None)."""
    import watchtower.queue as q
    import watchtower.workers as workers

    q.enqueue(project="IDLEQ", note="open work, unclaimed")
    rows = [{"worker_id": "idle-w1", "queue": "IDLEQ"}]
    workers.annotate_activity(rows, q.list_items())
    assert rows[0]["active_ref"] is None
    assert rows[0]["active_since_human"] is None
    assert rows[0]["display_name"] == "IDLEQ Queue worker"


def test_worker_display_name_reflects_lifecycle(store):
    """WT-49: display_name updates from generic -> in-progress -> closed
    summary, since the engine's own session name (set once at spawn) can't
    be renamed after the fact."""
    import watchtower.queue as q
    import watchtower.workers as workers

    item = q.enqueue(project="NAMEQ", note="fix the thing")
    rows = [{"worker_id": "namer-1", "queue": "NAMEQ"}]

    # Never claimed anything yet: generic label.
    workers.annotate_activity(rows, q.list_items())
    assert rows[0]["display_name"] == "NAMEQ Queue worker"

    # Holding an in-progress ticket: label carries the ref.
    q.claim_next("namer-1", project="NAMEQ")
    workers.annotate_activity(rows, q.list_items())
    assert rows[0]["display_name"] == f"NAMEQ#{item['seq']}: fix the thing"

    # After close, idle again but the label now carries the outcome.
    q.close(item["ref"], "namer-1", resolution="fixed the thing")
    workers.annotate_activity(rows, q.list_items())
    assert rows[0]["active_ref"] is None
    assert rows[0]["last_closed_ref"] == item["ref"]
    assert rows[0]["last_closed_summary"] == "fixed the thing"
    assert rows[0]["display_name"] == f"NAMEQ#{item['seq']}: fixed the thing"


def test_worker_activity_join_mixed_closed_at_types(store):
    """WT-93: a legacy int-epoch ``closed_at`` must not crash the by-worker
    "most recent close" comparison against modern ISO-string closes."""
    import watchtower.queue as q
    import watchtower.workers as workers

    a = q.enqueue(project="MIXQ", note="first")
    b = q.enqueue(project="MIXQ", note="second")
    q.claim_next("mix-w", project="MIXQ")  # claims `a`
    q.close(a["ref"], "mix-w", resolution="closed a (iso)")
    q.claim_next("mix-w", project="MIXQ")  # claims `b`
    q.close(b["ref"], "mix-w", resolution="closed b (iso)")

    # Simulate an imported/legacy record whose closed_at is an int epoch.
    items = q.list_items()
    for it in items:
        if it["ref"] == a["ref"]:
            it["closed_at"] = 1782608895  # int, not ISO string

    rows = [{"worker_id": "mix-w", "queue": "MIXQ"}]
    workers.annotate_activity(rows, items)  # must not raise TypeError
    # `b` (ISO 2026) is more recent than the int-epoch `a`.
    assert rows[0]["last_closed_ref"] == b["ref"]


# WT-27: reconciler, stop-signal, and mandatory-summary tests


def test_reconcile_once_dry_run(store, tmp_path, monkeypatch):
    """reconcile_once(dry_run=True) reports that it would spawn a worker for a
    config'd queue with auto_drain=True and open tickets."""
    import importlib

    monkeypatch.setenv("WATCHTOWER_CONFIG_FILE", str(tmp_path / "qconfig.json"))
    monkeypatch.setenv("WATCHTOWER_STOP_SIGNALS_DIR", str(tmp_path / "stop-signals"))
    import watchtower.config as config
    import watchtower.workers as workers
    import watchtower.queue as q
    importlib.reload(config)
    importlib.reload(workers)
    importlib.reload(q)

    # Opt the queue into auto-drain (config is the single source of truth now).
    config.set_auto_drain("RECONCQ", True)
    # Drop a ticket into the queue so depth > 0.
    q.enqueue(project="RECONCQ", note="waiting for a worker")

    result = workers.reconcile_once(dry_run=True)

    # At least one worker should be recorded as would-be-spawned.
    assert result["spawned"], f"expected spawned entries, got {result}"
    spawned_queues = [s.get("queue") for s in result["spawned"]]
    assert "RECONCQ" in spawned_queues
    # dry_run=True: no real process was started (pid==0), file not spawned.
    for s in result["spawned"]:
        if s.get("queue") == "RECONCQ":
            assert s.get("dry_run") is True
            assert s.get("pid") == 0


def test_close_requires_summary(store, capsys):
    """wt close without --summary must exit 1 and print a helpful error."""
    import watchtower.queue as q
    from watchtower.cli import main

    q.enqueue(project="NOSUMQ", note="needs fixing")
    q.claim_next("w", project="NOSUMQ")

    rc = main(["close", "NOSUMQ-1"])
    assert rc == 1, "expected exit code 1 when --summary is missing"
    err = capsys.readouterr().err
    assert "--summary" in err


def test_enqueue_with_triage_fields(store):
    """Triage fields (item_type, readiness, priority, value, confidence) are
    stored on the item under the canonical key names."""
    import watchtower.queue as q

    item = q.enqueue(
        project="TRIAGE",
        note="ship the feature",
        item_type="feature",
        readiness="ready",
        priority="p1",
        value="H",
        confidence="M",
    )
    assert item["type"] == "feature"
    assert item["readiness"] == "ready"
    assert item["priority"] == "p1"
    assert item["value"] == "H"
    assert item["confidence"] == "M"
    # Schema completeness: block fields present from creation.
    assert item["needs_input"] is False
    assert item["block_question"] == ""

    # Reload from disk to confirm persistence.
    reloaded = q.get(item["ref"])
    assert reloaded["type"] == "feature"
    assert reloaded["priority"] == "p1"


def test_claim_skips_unready(store):
    """claim_next(shaping=False) skips needs-shaping/needs-spec items;
    claim_next(shaping=True) includes them."""
    import watchtower.queue as q

    item = q.enqueue(
        project="UNREADY", note="shape me first", readiness="needs-shaping"
    )

    # Default (shaping=False) should not claim an unready item.
    assert q.claim_next("worker-1", project="UNREADY") is None

    # Explicitly shaping=True should claim it.
    claimed = q.claim_next("worker-1", project="UNREADY", shaping=True)
    assert claimed is not None
    assert claimed["ref"] == item["ref"]
    assert claimed["status"] == "in_progress"


def test_update_patches_fields(store):
    """update() patches triage fields in place and persists them."""
    import watchtower.queue as q

    item = q.enqueue(project="UPD", note="needs priority")
    assert item.get("priority", "") == ""

    updated = q.update(item["ref"], priority="p1", value="H")
    assert updated is not None
    assert updated["priority"] == "p1"
    assert updated["value"] == "H"
    assert updated["ref"] == item["ref"]

    # Also accepts "item_type" alias (stored as "type").
    q.update(item["ref"], item_type="bug")
    reloaded = q.get(item["ref"])
    assert reloaded["type"] == "bug"

    # State-machine fields are silently ignored.
    q.update(item["ref"], status="closed", number=999)
    reloaded2 = q.get(item["ref"])
    assert reloaded2["status"] == "open"
    assert reloaded2["number"] == item["number"]

    # Returns None for unknown ident.
    assert q.update("DOES-NOT-EXIST", priority="p0") is None


def test_move_reassigns_ref_and_preserves_history(store):
    """move() moves a ticket to another queue in place (WT-83) -- no
    refile/close churn. Its ref is reassigned within the target queue
    (refs are derived from project+number) but everything else survives."""
    import watchtower.queue as q

    other = q.enqueue(project="BYM", note="already here")
    item = q.enqueue(project="BYMPROD", note="move me", title="t", priority="p1")
    assert item["ref"] == "BYMPROD-1"

    moved = q.move(item["ref"], "bym")  # lowercase input, normalized to BYM
    assert moved is not None
    assert moved["project"] == "BYM"
    assert moved["ref"] == "BYM-2"  # appended after the existing BYM-1
    assert moved["title"] == "t"
    assert moved["priority"] == "p1"
    assert moved["note"] == "move me"
    assert moved["status"] == "open"

    # Old ref is gone; ticket is now only reachable via its new ref.
    assert q.get("BYMPROD-1") is None
    assert q.get("BYM-2")["ref"] == "BYM-2"
    assert q.get(other["ref"])["ref"] == "BYM-1"  # sibling ticket untouched

    # Unknown ident / empty target queue -> None / ValueError.
    assert q.move("NOPE-1", "BYM") is None
    with pytest.raises(ValueError):
        q.move(other["ref"], "")


def test_move_rejects_github_backed_queues(store):
    import watchtower.queue as q
    import watchtower.config as config

    item = q.enqueue(project="FILEQ", note="stays local")
    config.set_backend("GHQ", "github")

    with pytest.raises(ValueError):
        q.move(item["ref"], "GHQ")


def test_priority_sort_in_claim(store):
    """claim_next returns the highest-priority (lowest p-number) item first,
    regardless of insertion order."""
    import watchtower.queue as q

    low = q.enqueue(project="PRIO", note="low prio", priority="p2")
    high = q.enqueue(project="PRIO", note="high prio", priority="p0")

    # p0 was filed second but should be claimed first.
    first = q.claim_next("worker-1", project="PRIO")
    assert first is not None
    assert first["ref"] == high["ref"]
    assert first["priority"] == "p0"

    second = q.claim_next("worker-1", project="PRIO")
    assert second is not None
    assert second["ref"] == low["ref"]
    assert second["priority"] == "p2"


def test_stop_signal_file(store, tmp_path, monkeypatch):
    """request_stop(worker_id) creates a sentinel file; claim_next for that
    worker_id consumes stale STOP only after deciding if work is claimable."""
    import importlib

    monkeypatch.setenv("WATCHTOWER_STOP_SIGNALS_DIR", str(tmp_path / "stop-signals"))
    import watchtower.workers as workers
    import watchtower.queue as q
    importlib.reload(workers)
    importlib.reload(q)

    worker_id = "test-worker-stop-01"
    signal_path = workers.request_stop(worker_id)

    # The file must exist immediately after request_stop().
    assert signal_path.exists(), "stop signal file was not created"

    # Queue has an open ticket, so a stale STOP must be consumed but ignored:
    # otherwise a ticket filed just after a drain window can be orphaned.
    q.enqueue(project="STOPQ", note="pending work")
    result = q.claim_next(worker_id, project="STOPQ")

    assert result["ref"] == "STOPQ-1"
    assert result["status"] == "in_progress"
    # File must have been consumed (deleted) by claim_next.
    assert not signal_path.exists(), "stop signal file was not cleaned up"
    item = q.get("STOPQ-1")
    assert item["status"] == "in_progress"

    # With no claimable work, STOP is honored.
    signal_path = workers.request_stop(worker_id)
    assert q.claim_next(worker_id, project="STOPQ") == {"stop": True}
    assert not signal_path.exists(), "stop signal file was not cleaned up"


# ========================================== activity-log session ID (WT-90)
def test_activity_log_includes_session_id_for_non_ticket_verbs(store, tmp_path, monkeypatch):
    """Non-ticket verbs log [sid:xxxx] when CLAUDE_CODE_SESSION_ID is set."""
    import watchtower.queue as q
    import importlib
    importlib.reload(q)
    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", "test-session-id-abc123")
    activity_log = tmp_path / "activity.log"
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(activity_log))

    q.enqueue(project="SID", note="test ticket")
    item = q.claim_next("w1", project="SID")
    q.close(item["ref"], "w1", resolution="done it")

    log_text = activity_log.read_text()
    lines = [l for l in log_text.splitlines() if l.strip()]
    enqueue_lines = [l for l in lines if "ENQUEUE" in l]
    claim_lines = [l for l in lines if "CLAIM" in l]
    close_lines = [l for l in lines if "CLOSE" in l]

    assert enqueue_lines, "Expected ENQUEUE log line"
    assert claim_lines, "Expected CLAIM log line"
    assert close_lines, "Expected CLOSE log line"

    # ENQUEUE excluded: no [sid:...] suffix
    assert "[sid:" not in enqueue_lines[0], f"ENQUEUE should NOT have sid: {enqueue_lines[0]}"
    # CLAIM excluded (already encodes session_id in detail): no [sid:...] suffix
    assert "[sid:" not in claim_lines[0], f"CLAIM should NOT have sid: {claim_lines[0]}"
    # CLOSE is a non-ticket command: should have [sid:test-ses] (first 8 chars)
    assert "[sid:test-ses]" in close_lines[0], f"CLOSE should have sid: {close_lines[0]}"


def test_activity_log_no_session_id_when_env_unset(store, tmp_path, monkeypatch):
    """When CLAUDE_CODE_SESSION_ID is absent, no [sid:...] appears in the log."""
    import watchtower.queue as q
    import importlib
    importlib.reload(q)
    monkeypatch.delenv("CLAUDE_CODE_SESSION_ID", raising=False)
    activity_log = tmp_path / "activity.log"
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(activity_log))

    q.enqueue(project="NOSID", note="ticket")
    item = q.claim_next("w1", project="NOSID")
    q.close(item["ref"], "w1", resolution="done")

    log_text = activity_log.read_text()
    assert "[sid:" not in log_text, "No session ID should appear when env is unset"

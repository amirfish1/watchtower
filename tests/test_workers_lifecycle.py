"""Comprehensive WatchTower worker-lifecycle tests.

Covers the FIFO-pushable-worker architecture and the reconcile/spawn decision
logic end to end, WITHOUT spawning a real ``claude``:

* Spawn decisions use ``reconcile_once(dry_run=True)`` — deterministic, no
  subprocess.
* "Live" workers are simulated by recording a worker whose pid is this test
  process (always alive) and holding a real FIFO reader fd open, so
  ``notify_workers`` can actually deliver and we read the message back.
* "Dead" workers are simulated with the pid of a process that has already
  exited, and a FIFO with no reader (so an O_WRONLY|O_NONBLOCK open gets ENXIO).

Everything runs against a fully isolated sandbox (store + workers.json +
queue-config.json + stop-signals dir all under tmp_path).
"""

from __future__ import annotations

import importlib
import json
import os
import select
import subprocess
import sys
import time

import pytest


@pytest.fixture()
def wt(tmp_path, monkeypatch):
    """Isolated WatchTower: fresh store, workers, config, stop-signals."""
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_WORKERS_FILE", str(tmp_path / "workers.json"))
    monkeypatch.setenv("WATCHTOWER_CONFIG_FILE", str(tmp_path / "config.json"))
    monkeypatch.setenv("WATCHTOWER_STOP_SIGNALS_DIR", str(tmp_path / "stop-signals"))
    monkeypatch.setenv(
        "WATCHTOWER_WORKER_SESSIONS_FILE", str(tmp_path / "worker-sessions.json")
    )
    monkeypatch.setenv(
        "WATCHTOWER_WORKER_IDS_FILE", str(tmp_path / "worker-ids.json")
    )
    monkeypatch.setenv(
        "WATCHTOWER_LAUNCH_FAILURES_FILE", str(tmp_path / "launch-failures.json")
    )
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(tmp_path / "activity.log"))
    monkeypatch.setenv(
        "WATCHTOWER_CCC_SPAWN_DEFAULTS_FILE", str(tmp_path / "no-ccc-spawn-defaults.json")
    )
    monkeypatch.setenv(
        "WATCHTOWER_CODEX_THREAD_REGISTRY", str(tmp_path / "codex-thread-registry.json")
    )

    import watchtower.queue as q
    import watchtower.health as health
    import watchtower.config as config
    import watchtower.workers as workers
    import watchtower.codex_registry as codex_registry
    importlib.reload(q)
    importlib.reload(config)
    importlib.reload(health)
    importlib.reload(workers)
    importlib.reload(codex_registry)
    # Keep registry-migration hermetic: point at a non-existent file.
    monkeypatch.setattr(config, "_REGISTRY_FILE", tmp_path / "no-registry.json")

    class Ns:
        pass
    ns = Ns()
    ns.q, ns.health, ns.config = q, health, config
    ns.workers = workers
    ns.codex_registry = codex_registry
    ns.tmp = tmp_path
    ns._readers = []  # open reader fds to close at teardown
    yield ns
    for fd in ns._readers:
        try:
            os.close(fd)
        except OSError:
            pass


# --------------------------------------------------------------------------- helpers
def _dead_pid():
    """A pid guaranteed not to be running (a child we just reaped)."""
    p = subprocess.Popen(["true"])
    p.wait()
    return p.pid


def _live_worker(wt, queue, *, with_fifo=True):
    """Record a worker that is alive (this pid) with a real FIFO + held reader.

    Returns the worker record. The reader fd is tracked for teardown so the
    FIFO stays openable for writing during the test."""
    workers = wt.workers
    wid = f"{queue.lower()}-live-{len(wt._readers)}"
    fifo_path = ""
    log = wt.tmp / f"{wid}.log"
    log.write_text("")  # real log file so mtime (idle clock) is resolvable
    if with_fifo:
        fifo_path, rdwr_fd = workers._make_stdin_fifo(log)
        wt._readers.append(rdwr_fd)
    return workers.record_worker(
        os.getpid(), queue, "claude", wid, str(wt.tmp), str(log),
        fifo=fifo_path or "",
    )


def _dead_worker(wt, queue):
    """Record a worker whose process is gone and whose FIFO has no reader."""
    workers = wt.workers
    wid = f"{queue.lower()}-dead"
    log = wt.tmp / f"{wid}.log"
    fifo_path, rdwr_fd = workers._make_stdin_fifo(log)
    os.close(rdwr_fd)  # drop the only reader -> writes will ENXIO
    return workers.record_worker(
        _dead_pid(), queue, "claude", wid, str(wt.tmp), str(log), fifo=fifo_path or "",
    )


# ===================================================================== reconcile
def test_reconcile_cold_drain_on_spawns(wt):
    wt.config.set_auto_drain("Q", True)
    wt.q.enqueue(project="Q", note="work")
    r = wt.workers.reconcile_once(dry_run=True)
    assert [s["queue"] for s in r["spawned"]] == ["Q"]


def test_reconcile_dry_run_spawn_is_labeled_in_activity(wt):
    wt.config.set_auto_drain("Q", True)
    wt.q.enqueue(project="Q", note="work")

    wt.workers.reconcile_once(dry_run=True)

    activity = (wt.tmp / "activity.log").read_text()
    spawn_line = next(line for line in activity.splitlines() if "SPAWN" in line)
    assert "(dry-run; no process started)" in spawn_line
    assert "— plan:" in spawn_line
    assert "(pid 0)" not in spawn_line


def test_reconcile_drain_off_skips(wt):
    wt.config.set_auto_drain("Q", False)
    wt.q.enqueue(project="Q", note="work")
    r = wt.workers.reconcile_once(dry_run=True)
    assert not r["spawned"]
    assert any(s["queue"] == "Q" and "auto_drain=off" in s["reason"] for s in r["skipped"])


def test_reconcile_empty_queue_skips(wt):
    wt.config.set_auto_drain("Q", True)  # config entry exists, but no tickets
    r = wt.workers.reconcile_once(dry_run=True)
    assert not r["spawned"]
    assert any(s["queue"] == "Q" and s["reason"] == "depth=0" for s in r["skipped"])


def test_reconcile_needs_shaping_only_skips_no_spawn(wt):
    """A queue whose only open tickets need human shaping/spec has ZERO
    claimable work -- claim_next won't hand them to a default worker, so
    spawning one just churns spawn -> idle -> reap forever (the bug this
    guards against)."""
    wt.config.set_auto_drain("Q", True)
    wt.q.enqueue(project="Q", note="needs shaping", readiness="needs-shaping")
    wt.q.enqueue(project="Q", note="needs spec", readiness="needs-spec")
    r = wt.workers.reconcile_once(dry_run=True)
    assert not r["spawned"]
    assert any(
        s["queue"] == "Q" and s["reason"].startswith("0 claimable")
        for s in r["skipped"]
    )


def test_reconcile_ready_ticket_alongside_needs_spec_still_spawns(wt):
    """A queue with one ready + one needs-spec ticket has real claimable work,
    so it should still spawn -- readiness gating must not zero out the whole
    queue, only the non-claimable tickets within it."""
    wt.config.set_auto_drain("Q", True)
    wt.q.enqueue(project="Q", note="ready work", readiness="ready")
    wt.q.enqueue(project="Q", note="needs spec", readiness="needs-spec")
    r = wt.workers.reconcile_once(dry_run=True)
    assert [s["queue"] for s in r["spawned"]] == ["Q"]


def test_reconcile_live_equals_desired_skips(wt):
    wt.config.set_auto_drain("Q", True)
    wt.q.enqueue(project="Q", note="work")
    _live_worker(wt, "Q")  # one live worker == desired (1)
    r = wt.workers.reconcile_once(dry_run=True)
    assert not r["spawned"]


def test_reconcile_desired_two_spawns_two(wt):
    wt.config.set_auto_drain("Q", True)
    wt.config.set_desired_workers("Q", 2)
    wt.q.enqueue(project="Q", note="work")
    wt.q.enqueue(project="Q", note="work")
    r = wt.workers.reconcile_once(dry_run=True)
    assert len([s for s in r["spawned"] if s["queue"] == "Q"]) == 2


def test_reconcile_caps_spawn_at_depth(wt):
    """Never spawn more workers than there are tickets. Even if desired=2 and
    there's only 1 ticket, spawn only 1 worker. (WT-98)"""
    wt.config.set_auto_drain("Q", True)
    wt.config.set_desired_workers("Q", 2)
    wt.q.enqueue(project="Q", note="work")
    r = wt.workers.reconcile_once(dry_run=True)
    # Desired is 2, but we only have 1 ticket, so spawn only 1.
    assert len([s for s in r["spawned"] if s["queue"] == "Q"]) == 1


def test_reconcile_does_not_overspawn_while_claiming(wt):
    """When 1 live worker exists and 1 ticket is open, don't spawn more workers
    while the live worker is claiming. Cap spawn at unclaimed tickets. (WT-98)"""
    wt.config.set_auto_drain("Q", True)
    wt.config.set_desired_workers("Q", 2)
    wt.q.enqueue(project="Q", note="work")
    # Simulate: 1 live worker (hasn't claimed yet), 1 open ticket, desired=2
    _live_worker(wt, "Q")
    r = wt.workers.reconcile_once(dry_run=True)
    # Even though desired=2 and actual=1, spawn 0 because 1 live can claim the 1 open.
    assert len([s for s in r["spawned"] if s["queue"] == "Q"]) == 0


def test_concurrent_reconciles_do_not_overspawn(wt, monkeypatch):
    """WT-75: the daemon tick and dispatch_after_enqueue (`wt add`) can
    reconcile the same queue concurrently. Without serialization both read the
    same live count and each spawns the full desired delta (4 spawned for
    desired=2). reconcile_once holds a cross-process file lock and
    spawn_workers registers workers before releasing it, so the racing pass
    sees them and skips — `desired` workers TOTAL across both passes."""
    import threading

    wt.config.set_auto_drain("Q", True)
    wt.config.set_desired_workers("Q", 2)
    wt.q.enqueue(project="Q", note="work")
    wt.q.enqueue(project="Q", note="work")

    real_record = wt.workers.record_worker
    spawn_calls = []

    def fake_spawn(
        queue, n=1, engine="claude", *, repo_path="", dry_run=False,
        launch_failures=None,
    ):
        # Real spawn minus the subprocess: linger inside the critical section
        # (so an unserialized second pass would count live=0 meanwhile), then
        # register n live (this-pid) workers exactly like spawn_workers does.
        time.sleep(0.2)
        recs = []
        for i in range(max(1, n)):
            wid = f"{queue.lower()}-fake-{len(spawn_calls)}-{i}"
            log = wt.tmp / f"{wid}.log"
            log.write_text("")
            recs.append(
                real_record(os.getpid(), queue, engine, wid, str(wt.tmp), str(log))
            )
        spawn_calls.append((queue, n))
        return recs

    monkeypatch.setattr(wt.workers, "spawn_workers", fake_spawn)

    results = [None, None]

    def run(i):
        results[i] = wt.workers.reconcile_once(dry_run=False)

    threads = [threading.Thread(target=run, args=(i,)) for i in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert all(r is not None for r in results), "a reconcile pass deadlocked"
    total = sum(len(r["spawned"]) for r in results)
    assert total == 2, (results, spawn_calls)
    assert wt.workers.live_worker_count("Q") == 2
    # The losing pass must have skipped with the fully-staffed reason.
    skips = [s for r in results for s in r["skipped"] if s["queue"] == "Q"]
    assert any("actual=2==desired=2" in s["reason"] for s in skips)


def test_reconcile_launch_failure_cooldown_blocks_spawn_storm(wt, monkeypatch):
    """A Codex quota failure exits after creating a session. Reconcile must not
    create a fresh cloud session every tick while the reset/cooldown is active."""
    wt.config.set_auto_drain("Q", True)
    wt.config.set_engine("Q", "codex")
    wt.q.enqueue(project="Q", note="work")
    sid = "11111111-1111-1111-1111-111111111111"
    script = (
        "print('OpenAI Codex v0.140.0')\n"
        f"print('session id: {sid}')\n"
        "print(\"ERROR: You've hit your usage limit. Visit "
        "https://chatgpt.com/codex/settings/usage to purchase more credits "
        "or try again at Jul 9th, 2099 12:09 AM.\")\n"
    )

    monkeypatch.setattr(
        wt.workers,
        "build_drain_command",
        lambda *a, **k: [sys.executable, "-c", script],
    )
    monkeypatch.setattr(wt.workers, "_LAUNCH_FAILURE_GRACE_S", 2)

    first = wt.workers.reconcile_once(dry_run=False)
    assert not first["spawned"]
    assert len(first["launch_failed"]) == 1
    assert first["launch_failed"][0]["reason"] == "engine usage limit"
    assert first["launch_failed"][0]["session_id"] == sid

    ledger = json.loads((wt.tmp / "worker-sessions.json").read_text())
    assert sid in ledger["session_ids"]

    second = wt.workers.reconcile_once(dry_run=False)
    assert not second["spawned"]
    assert not second["launch_failed"]
    assert any(
        s["queue"] == "Q" and "launch cooldown" in s["reason"]
        for s in second["skipped"]
    )


def test_spawn_workers_missing_binary_records_launch_failure(wt, monkeypatch):
    """A missing engine binary should not bubble out of Popen and kill the
    daemon; it becomes a launch failure with cooldown."""
    missing = str(wt.tmp / "missing-codex")
    monkeypatch.setattr(
        wt.workers,
        "build_drain_command",
        lambda *a, **k: [missing],
    )

    failures = []
    spawned = wt.workers.spawn_workers(
        "Q", engine="codex", repo_path=str(wt.tmp), launch_failures=failures
    )

    assert spawned == []
    assert len(failures) == 1
    assert "engine executable unavailable" in failures[0]["reason"]
    cooldown = wt.workers.active_launch_failure_cooldown("Q", "codex")
    assert cooldown and cooldown["worker_id"] == failures[0]["worker_id"]


def test_engine_available_uses_codex_env_override(wt, monkeypatch):
    script = wt.tmp / "fake-codex"
    script.write_text("#!/bin/sh\nexit 0\n")
    script.chmod(0o755)
    monkeypatch.setenv("WATCHTOWER_CODEX_BIN", str(script))
    monkeypatch.setenv("PATH", "/usr/bin:/bin")

    assert wt.workers.engine_available("codex") is True


def test_reconcile_excess_workers_not_stopped(wt):
    """New contract: the reconciler no longer STOPs surplus workers — that call
    is made at claim time (live>desired) with REAP as the safety net. It only
    records the surplus as a skip reason and spawns nothing."""
    wt.config.set_auto_drain("Q", True)  # desired defaults to 1
    wt.q.enqueue(project="Q", note="work")
    _live_worker(wt, "Q")
    _live_worker(wt, "Q")  # two live, one too many
    r = wt.workers.reconcile_once(dry_run=True)
    assert not [s for s in r["stopped"] if s["queue"] == "Q"]  # no STOP pushed
    assert not r["spawned"]
    assert any(s["queue"] == "Q" and "surplus" in s["reason"] for s in r["skipped"])


def test_reconcile_empty_queue_does_not_wind_down_idle_worker(wt):
    """New contract: a drained (0 open) queue no longer STOPs its idle worker —
    it stays warm for the next ticket; REAP kills it if it stays cold."""
    wt.config.set_auto_drain("Q", True)  # drain on, but queue empty
    _live_worker(wt, "Q")
    r = wt.workers.reconcile_once(dry_run=True)
    assert not [s for s in r["stopped"] if s["queue"] == "Q"]


def test_reconcile_nudges_live_worker_on_orphan_requeue(wt):
    """WT-50: a ticket orphaned by its dead claimer and reopened by the sweep
    must nudge any OTHER already-live worker on that queue right away.

    Without this, pickup only happens if the spawn pass separately decides
    actual<desired (it won't when a same-queue worker is already live and
    just busy elsewhere) or whenever that worker's own next unrelated poll
    happens to occur — leaving a visibly "open" ticket unworked in between."""
    wt.config.set_auto_drain("Q", True)
    wt.config.set_desired_workers("Q", 1)
    item = wt.q.enqueue(project="Q", note="work")
    ref = item["ref"]
    dead = _dead_worker(wt, "Q")  # registered worker whose pid has since exited
    # Simulate the "worker was alive, claimed, then died" scenario by writing
    # the in_progress state directly (claim_next now rejects dead workers loudly).
    data = wt.q._load_unlocked()
    for it in data["items"]:
        if it["ref"] == ref:
            it["status"] = "in_progress"
            it["claimed_by"] = dead["worker_id"]
            it["claimed_at"] = "2000-01-01T00:00:00Z"  # past the orphan grace window
    wt.q._save_unlocked(data)

    rec = _live_worker(wt, "Q")
    _age_worker_log(wt, rec, wt.workers.WARM_TTL_S - 30)  # warm: nudge, don't reap

    r = wt.workers.reconcile_once(dry_run=False)
    assert ref in r["requeued"]

    fd = wt._readers[-1]
    data = os.read(fd, 65536).decode()
    msg = json.loads(data.strip())
    assert "Q" in msg["message"]["content"][0]["text"]


def test_requeue_orphaned_tickets_wont_clobber_a_concurrent_close(wt, monkeypatch):
    """OPS-72 regression: the sweep decides which tickets are orphaned from a
    ``list_items()`` snapshot taken once up front, then writes "open" per item
    afterward. If the real worker closes its ticket for real in the gap
    between that snapshot and the sweep's write (worker finishes right as a
    reconcile tick starts), a plain reopen used to clobber the close back to
    open/in_progress -- reported live as a closed ticket briefly reappearing
    as open/in_progress right after `wt close`. Reproduce the race directly:
    the store already holds "closed" but list_items() still reports a stale
    "in_progress" snapshot, and assert the sweep's write becomes a no-op."""
    item = wt.q.enqueue(project="Q", note="work")
    ref = item["ref"]
    dead = _dead_worker(wt, "Q")  # registered worker whose pid has since exited
    # Write in_progress directly — dead workers are now rejected by claim_next.
    data = wt.q._load_unlocked()
    for it in data["items"]:
        if it["ref"] == ref:
            it["status"] = "in_progress"
            it["claimed_by"] = dead["worker_id"]
            it["claimed_at"] = "2000-01-01T00:00:00Z"
    wt.q._save_unlocked(data)
    claimed = wt.q.get(ref)
    stale_snapshot = dict(claimed, claimed_at="2000-01-01T00:00:00Z")  # past grace window

    wt.q.close(ref, session_id=dead["worker_id"], resolution="done")  # real close lands first
    monkeypatch.setattr(wt.q, "list_items", lambda *a, **k: [stale_snapshot])

    reopened = wt.workers.requeue_orphaned_tickets()
    assert ref not in [it["ref"] for it in reopened]
    assert wt.q.get(ref)["status"] == "closed"


def test_requeue_orphaned_tickets_leaves_unregistered_claimer_alone(wt):
    """OPS-104 regression: a `wt claim --worker <alias>` run from an ambient
    Claude session (not spawned via spawn_workers/spawn_run_once_worker) never
    gets a pid entry in the worker store. The old sweep read that absence the
    same as "the worker died" and reopened the ticket ~2 minutes after every
    such claim, handing it to a second worker while the original session was
    still working -- duplicate work. Such an id must be left alone; only a
    claimer that IS in the worker store (and is no longer alive) is orphaned."""
    item = wt.q.enqueue(project="Q", note="work")
    ref = item["ref"]
    wt.q.claim_next("claude-session-abc123", project="Q")  # never spawned by watchtower
    data = wt.q._load_unlocked()
    for it in data["items"]:
        if it["ref"] == ref:
            it["claimed_at"] = "2000-01-01T00:00:00Z"  # past the orphan grace window
    wt.q._save_unlocked(data)

    reopened = wt.workers.requeue_orphaned_tickets()
    assert ref not in [it["ref"] for it in reopened]
    assert wt.q.get(ref)["status"] == "in_progress"


def test_requeue_orphaned_tickets_survives_claimer_pruned_from_store(wt):
    """CCC-549: a worker's record in workers.json is pruned (list_workers'
    default prune=True runs on nearly every routine read -- `wt status`, the
    dashboard poll -- often within seconds of the worker dying) before the
    sweep ever inspects it. Before the worker_id ledger, that made a
    genuinely-dead spawned worker indistinguishable from the OPS-104
    "never spawned" case above, and the ticket was left orphaned forever.
    The ledger (populated at record_worker time, unaffected by pruning) must
    still let the sweep recognize the claimer as a known-dead worker."""
    item = wt.q.enqueue(project="Q", note="work")
    ref = item["ref"]
    dead = _dead_worker(wt, "Q")  # registered worker whose pid has since exited
    data = wt.q._load_unlocked()
    for it in data["items"]:
        if it["ref"] == ref:
            it["status"] = "in_progress"
            it["claimed_by"] = dead["worker_id"]
            it["claimed_at"] = "2000-01-01T00:00:00Z"  # past the orphan grace window
    wt.q._save_unlocked(data)

    # Simulate the eager prune a routine `wt status`/dashboard read triggers.
    wt.workers.list_workers(prune=True)
    assert dead["worker_id"] not in {
        w["worker_id"] for w in wt.workers._load()["workers"]
    }

    reopened = wt.workers.requeue_orphaned_tickets()
    assert ref in [it["ref"] for it in reopened]
    assert wt.q.get(ref)["status"] == "open"


def test_claim_next_rejects_dead_spawned_worker(wt):
    """WT-92: claim_next must fail loudly when the session_id is a registered
    spawned worker that is no longer alive, so the caller gets an immediate
    error instead of a silent requeue 2 minutes later."""
    rec = _dead_worker(wt, "Q")
    dead_id = rec["worker_id"]  # "q-dead" — _dead_worker lowercases the queue name
    wt.q.enqueue(project="Q", note="work")
    import pytest
    with pytest.raises(ValueError, match="not currently alive"):
        wt.q.claim_next(dead_id, project="Q")
    # ticket must still be open — claim was rejected
    items = [it for it in wt.q._load_unlocked()["items"] if it.get("project") == "Q"]
    assert all(it["status"] == "open" for it in items)


def test_claim_by_ref_rejects_dead_spawned_worker(wt):
    """WT-92: claim_by_ref must also fail loudly for a dead registered worker."""
    rec = _dead_worker(wt, "Q")
    dead_id = rec["worker_id"]  # "q-dead"
    item = wt.q.enqueue(project="Q", note="work")
    import pytest
    with pytest.raises(ValueError, match="not currently alive"):
        wt.q.claim_by_ref(item["ref"], dead_id)
    assert wt.q.get(item["ref"])["status"] == "open"


def test_claim_rebinds_continued_codex_worker_to_new_process(wt, monkeypatch, capsys):
    """A Codex goal continuation keeps its thread id but gets a new process.

    The matching logical session may reclaim its worker alias; an unrelated
    session must still hit the dead-worker guard.
    """
    cli = _reloaded_cli(wt)
    session_id = "11111111-1111-1111-1111-111111111111"
    worker_id = "q-codex-dead"
    wt.workers.record_worker(
        _dead_pid(),
        "Q",
        "codex",
        worker_id,
        str(wt.tmp),
        str(wt.tmp / f"{worker_id}.log"),
        session_id=session_id,
    )
    wt.workers.list_workers()  # routine read returns then prunes the dead PID
    assert wt.workers.list_workers() == []
    wt.q.enqueue(project="Q", note="continuation work")
    monkeypatch.setenv("CODEX_THREAD_ID", session_id)
    monkeypatch.setattr(
        cli.workers, "_find_engine_ancestor_pid", lambda engine: os.getpid()
    )

    rc = cli.cmd_claim(_claim_ns("Q", worker_id, json_out=True))

    assert rc == 0
    claimed = json.loads(capsys.readouterr().out)
    assert claimed["claimed_by"] == worker_id
    assert claimed["claimed_session_id"] == session_id
    rebound = next(
        worker for worker in wt.workers.list_workers(prune=False)
        if worker["worker_id"] == worker_id
    )
    assert rebound["pid"] == os.getpid()
    assert rebound["alive"] is True


def test_claim_rejects_pruned_codex_alias_from_unrelated_session(
    wt, monkeypatch, capsys
):
    cli = _reloaded_cli(wt)
    worker_id = "q-codex-dead"
    wt.workers.record_worker(
        _dead_pid(),
        "Q",
        "codex",
        worker_id,
        str(wt.tmp),
        str(wt.tmp / f"{worker_id}.log"),
        session_id="11111111-1111-1111-1111-111111111111",
    )
    wt.workers.list_workers()
    assert wt.workers.list_workers() == []
    ticket = wt.q.enqueue(project="Q", note="must remain open")
    monkeypatch.setenv("CODEX_THREAD_ID", "22222222-2222-2222-2222-222222222222")
    monkeypatch.setattr(
        cli.workers, "_find_engine_ancestor_pid", lambda engine: os.getpid()
    )

    rc = cli.cmd_claim(_claim_ns("Q", worker_id, json_out=True))

    assert rc == 1
    assert "not currently alive" in capsys.readouterr().err
    assert wt.q.get(ticket["ref"])["status"] == "open"


def test_claim_next_allows_ambient_unregistered_worker(wt):
    """WT-92: an ambient session_id not in the spawn registry must be allowed
    through — this preserves the OPS-104 fix (unregistered claimer == unknown
    liveness == don't block)."""
    wt.q.enqueue(project="Q", note="work")
    item = wt.q.claim_next("some-ambient-claude-session", project="Q")
    assert item is not None
    assert item["status"] == "in_progress"


def test_reconcile_nudges_live_worker_on_stuck_queue(wt):
    """WT-53: a queue can be fully staffed (actual==desired, no crash, no
    orphan) yet make zero progress -- e.g. a live worker's turn errored out on
    a transient API/connectivity fault and it's sitting idle mid-session. The
    reconciler must detect this via the queue's own stuck ground truth (no
    close in stuck_minutes despite claimable work) and nudge the live
    worker(s) to retry, even though actual==desired would otherwise skip
    silently."""
    wt.config.set_auto_drain("Q", True)
    wt.config.set_desired_workers("Q", 1)
    item = wt.q.enqueue(project="Q", note="work")
    data = wt.q._load_unlocked()
    for it in data["items"]:
        if it["ref"] == item["ref"]:
            it["created_at"] = "2000-01-01T00:00:00Z"  # long past stuck_minutes
    wt.q._save_unlocked(data)

    rec = _live_worker(wt, "Q")
    _age_worker_log(wt, rec, wt.workers.WARM_TTL_S - 30)  # warm: nudge, don't reap

    r = wt.workers.reconcile_once(dry_run=False)
    assert not r["spawned"]  # actual==desired -- this isn't the spawn path

    fd = wt._readers[-1]
    data = os.read(fd, 65536).decode()
    msg = json.loads(data.strip())
    assert "Q" in msg["message"]["content"][0]["text"]


def test_reconcile_nudge_preserves_queue_claim_type_filter(wt):
    """A bug-only queue's retry instruction must not invite feature claims."""
    wt.config.set_auto_drain("Q", True)
    wt.config.set_claim_types("Q", ["bug"])
    item = wt.q.enqueue(project="Q", note="bug work", item_type="bug")
    data = wt.q._load_unlocked()
    for it in data["items"]:
        if it["ref"] == item["ref"]:
            it["created_at"] = "2000-01-01T00:00:00Z"
    wt.q._save_unlocked(data)

    rec = _live_worker(wt, "Q")
    _age_worker_log(wt, rec, wt.workers.WARM_TTL_S - 30)

    wt.workers.reconcile_once(dry_run=False)

    msg = json.loads(os.read(wt._readers[-1], 65536).decode().strip())
    assert "wt claim -q Q --worker <your-id> --type bug --json" in (
        msg["message"]["content"][0]["text"]
    )


def test_reconcile_does_not_nudge_freshly_spawned_worker(wt):
    """WT-101: a ticket that sat unclaimed for a long time (no live worker to
    claim it) reads `stuck=True` the instant the queue gets staffed -- before
    the fresh worker has had any chance to start up and run its first
    `wt claim`. A reconcile tick landing in that startup window must not
    nudge a worker that's had zero time to make progress."""
    wt.config.set_auto_drain("Q", True)
    wt.config.set_desired_workers("Q", 1)
    item = wt.q.enqueue(project="Q", note="work")
    data = wt.q._load_unlocked()
    for it in data["items"]:
        if it["ref"] == item["ref"]:
            it["created_at"] = "2000-01-01T00:00:00Z"  # long past stuck_minutes
    wt.q._save_unlocked(data)

    _live_worker(wt, "Q")  # freshly recorded -- log mtime is "now", not aged

    r = wt.workers.reconcile_once(dry_run=False)
    assert not r["spawned"]  # actual==desired -- this isn't the spawn path

    fd = wt._readers[-1]
    readable, _, _ = select.select([fd], [], [], 0.2)
    assert not readable  # no nudge -- worker hasn't had a fair chance yet


def test_reconcile_stuck_nudge_has_cooldown(wt):
    """A queue that stays stuck across many reconcile ticks must not be
    re-nudged every tick -- that would spam the worker's FIFO once per
    reconciler interval for as long as it stays stuck."""
    wt.config.set_auto_drain("Q", True)
    wt.config.set_desired_workers("Q", 1)
    item = wt.q.enqueue(project="Q", note="work")
    data = wt.q._load_unlocked()
    for it in data["items"]:
        if it["ref"] == item["ref"]:
            it["created_at"] = "2000-01-01T00:00:00Z"
    wt.q._save_unlocked(data)

    rec = _live_worker(wt, "Q")
    _age_worker_log(wt, rec, wt.workers.WARM_TTL_S - 30)

    wt.workers.reconcile_once(dry_run=False)
    fd = wt._readers[-1]
    first = os.read(fd, 65536)
    assert first  # first tick nudged

    _age_worker_log(wt, rec, wt.workers.WARM_TTL_S - 30)  # stays warm
    wt.workers.reconcile_once(dry_run=False)  # second tick, still stuck
    readable, _, _ = select.select([fd], [], [], 0.2)
    assert not readable  # nothing new written -- on cooldown


# =========================================================== claim-time surplus
def _claim_ns(queue, worker, *, json_out=False):
    """Build the argparse-shaped namespace cmd_claim reads for the empty-queue path."""
    class Ns:
        pass
    ns = Ns()
    ns.queue = queue
    ns.worker = worker
    ns.ref = ""
    ns.oldest = False
    ns.type = []
    ns.readiness = []
    ns.json = json_out
    return ns


def _reloaded_cli(wt):
    """Reload cli against the sandbox so its module-level workers/queue/config
    references point at the reloaded (env-bound) modules."""
    import watchtower.cli as cli
    importlib.reload(cli)
    return cli


def test_claim_empty_queue_surplus_worker_stops(wt, capsys):
    """live>desired on an empty queue: the claiming worker is surplus -> stop."""
    cli = _reloaded_cli(wt)
    wt.config.set_auto_drain("Q", True)  # desired defaults to 1
    _live_worker(wt, "Q")
    _live_worker(wt, "Q")  # two live -> live(2) > desired(1)
    rc = cli.cmd_claim(_claim_ns("Q", "q-live-0", json_out=True))
    assert rc == 0
    out = capsys.readouterr().out.strip()
    assert json.loads(out) == {"stop": True}


def test_claim_empty_queue_surplus_worker_stops_text(wt, capsys):
    cli = _reloaded_cli(wt)
    wt.config.set_auto_drain("Q", True)
    _live_worker(wt, "Q")
    _live_worker(wt, "Q")
    rc = cli.cmd_claim(_claim_ns("Q", "q-live-0", json_out=False))
    assert rc == 0
    assert "STOP: surplus" in capsys.readouterr().out


def test_claim_empty_queue_at_desired_stays_warm(wt, capsys):
    """live<=desired: no surplus -> worker stays warm, no stop emitted."""
    cli = _reloaded_cli(wt)
    wt.config.set_auto_drain("Q", True)  # desired 1
    _live_worker(wt, "Q")  # exactly desired
    rc = cli.cmd_claim(_claim_ns("Q", "q-live-0", json_out=False))
    assert rc == 0
    out = capsys.readouterr().out
    assert "nothing open in Q" in out
    assert "STOP" not in out


def test_claim_empty_queue_at_desired_stays_warm_json(wt, capsys):
    """--json on a drained-but-warm queue must print nothing, per the
    documented claim contract (ticket JSON / empty / {"stop": true}) -- not
    the human-readable "(nothing open in Q)" sentinel (WT-73)."""
    cli = _reloaded_cli(wt)
    wt.config.set_auto_drain("Q", True)  # desired 1
    _live_worker(wt, "Q")  # exactly desired
    rc = cli.cmd_claim(_claim_ns("Q", "q-live-0", json_out=True))
    assert rc == 0
    out = capsys.readouterr().out.strip()
    assert out == ""


def test_claim_empty_queue_drain_off_stays_warm(wt, capsys):
    """auto_drain off -> desired 0, but a lone live worker is still not stopped
    unless it is actually surplus; here live(1)>desired(0) so it does stop."""
    cli = _reloaded_cli(wt)
    wt.config.set_auto_drain("Q", False)  # desired 0
    _live_worker(wt, "Q")  # live(1) > desired(0) -> surplus
    rc = cli.cmd_claim(_claim_ns("Q", "q-live-0", json_out=True))
    assert rc == 0
    assert json.loads(capsys.readouterr().out.strip()) == {"stop": True}


# ====================================================================== FIFO push
def test_notify_live_worker_delivers(wt):
    rec = _live_worker(wt, "Q")
    n = wt.workers.notify_workers("Q", "hello worker")
    assert n == 1
    # Read it back off the worker's FIFO reader.
    fd = wt._readers[-1]
    data = os.read(fd, 65536).decode()
    msg = json.loads(data.strip())
    assert msg["type"] == "user"
    assert msg["message"]["content"][0]["text"] == "hello worker"


def test_notify_dead_worker_zero(wt):
    _dead_worker(wt, "Q")
    assert wt.workers.notify_workers("Q", "nobody home") == 0


def test_notify_fans_out_to_all_live(wt):
    _live_worker(wt, "Q")
    _live_worker(wt, "Q")
    assert wt.workers.notify_workers("Q", "broadcast") == 2


def test_notify_is_queue_scoped(wt):
    _live_worker(wt, "Q")
    _live_worker(wt, "OTHER")
    assert wt.workers.notify_workers("Q", "only Q") == 1


def test_notify_mixed_live_and_dead(wt):
    _live_worker(wt, "Q")
    _dead_worker(wt, "Q")
    assert wt.workers.notify_workers("Q", "reach the living") == 1


# ============================================================ session-id handle
def test_claim_with_worker_id_leaves_session_id_empty(wt):
    """The documented gap: claiming with a non-UUID worker id does NOT populate
    claimed_session_id."""
    wt.q.enqueue(project="Q", note="work")
    item = wt.q.claim_next("q-abc12345", project="Q")
    assert item["claimed_by"] == "q-abc12345"
    assert not item.get("claimed_session_id")


def test_claim_with_real_uuid_sets_session_id(wt):
    wt.q.enqueue(project="Q", note="work")
    uuid = "7f72634b-b0bd-4c78-b931-3d877ed84187"
    item = wt.q.claim_next("q-abc12345", project="Q", session_uuid=uuid)
    assert item["claimed_session_id"] == uuid


def test_close_preserves_session_id(wt):
    wt.q.enqueue(project="Q", note="work")
    uuid = "7f72634b-b0bd-4c78-b931-3d877ed84187"
    it = wt.q.claim_next("w1", project="Q", session_uuid=uuid)
    closed = wt.q.close(it["ref"], session_id="w1", resolution="done")
    assert closed["status"] == "closed"
    assert closed["claimed_session_id"] == uuid


def test_reopen_preserves_session_id_drops_lock(wt):
    wt.q.enqueue(project="Q", note="work")
    uuid = "7f72634b-b0bd-4c78-b931-3d877ed84187"
    it = wt.q.claim_next("w1", project="Q", session_uuid=uuid)
    wt.q.close(it["ref"], session_id="w1", resolution="done")
    reopened = wt.q.update_status(it["ref"], "open")
    assert reopened["status"] == "open"
    assert reopened["claimed_session_id"] == uuid       # resume handle kept
    assert reopened.get("claimed_by") in (None, "")      # claim lock dropped


# ================================================================== stop signal
def test_request_stop_makes_claim_return_stop(wt):
    wt.q.enqueue(project="Q", note="work")
    wt.workers.request_stop("w-stopme")
    assert wt.q.claim_next("w-stopme", project="Q") == {"stop": True}
    # A release wins over a racing enqueue. The ticket stays open for the
    # replacement worker; the released session cannot claim from Q again.
    assert not (wt.workers.STOP_SIGNALS_DIR / "w-stopme").exists()
    item = wt.q.claim_next("w-replacement", project="Q")
    assert item and item.get("ref") == "Q-1"


# =========================================================== tracking & cleanup
def test_prune_drops_dead_and_unlinks_fifo(wt):
    dead = _dead_worker(wt, "Q")
    fifo = dead["fifo"]
    assert os.path.exists(fifo)
    wt.workers.list_workers(prune=True)  # prune removes dead from the store
    # A subsequent read no longer contains the dead worker, and its FIFO is gone.
    assert not any(r["worker_id"] == dead["worker_id"]
                   for r in wt.workers.list_workers())
    assert not os.path.exists(fifo)


def test_counts_accurate_mixed(wt):
    _live_worker(wt, "Q")
    _live_worker(wt, "Q")
    _dead_worker(wt, "Q")
    assert wt.workers.live_worker_count("Q") == 2
    counts = wt.workers.worker_counts(prune=False)
    assert counts["Q"]["live"] == 2


def test_record_worker_stores_fifo_and_session_id(wt):
    rec = wt.workers.record_worker(
        os.getpid(), "Q", "claude", "q-x", str(wt.tmp), "log.txt", fifo="/tmp/x.stdin",
    )
    assert rec["fifo"] == "/tmp/x.stdin"
    assert "fifo" in rec


# ================================================================= build / config
def test_build_claude_is_stream_json_no_goal(wt):
    argv = wt.workers.build_drain_command("Q", "claude", "q-1", "/repo")
    assert "stream-json" in argv
    assert "--input-format" in argv
    assert not any("Drain the Q" in a for a in argv)  # goal not in argv


def test_build_codex_has_goal_in_argv(wt):
    argv = wt.workers.build_drain_command("Q", "codex", "q-1", "/repo")
    assert argv[:2] == ["codex", "exec"]
    assert "--dangerously-bypass-approvals-and-sandbox" in argv
    goal = argv[-1]
    assert "Drain the Q" in goal
    assert "live stdin" not in goal.lower()
    assert "full warm context" not in goal
    assert "whenever you wake" not in goal
    assert "released from queue staffing" not in goal
    assert "complete this queue's drain goal" in goal.lower()
    assert "after the idle audit" in goal.lower()


def test_drain_goal_content(wt):
    goal = wt.workers.drain_goal("Q", "q-7", "/repo")
    assert "Q" in goal and "q-7" in goal and "/repo" in goal
    assert "claim" in goal.lower()
    # New FIFO model: end-turn-on-empty, no sleep-loop.
    assert "end" in goal.lower()
    # Per-queue learnings: read at spawn, update at drain-completion.
    assert "learnings/Q.md" in goal
    # WT-101: Resume Check / Idle Protocol detail moved to the shared runbook
    # -- the prompt keeps only a one-line trigger pointing at it by path.
    assert "RESUME CHECK" in goal
    assert "IDLE" in goal
    assert "stdin is a live input channel" in goal
    assert "full warm context" in goal
    assert "whenever you wake" in goal
    assert "released from queue staffing" in goal
    assert "complete this queue's drain goal" not in goal.lower()
    runbook = str(wt.workers._WORKER_RUNBOOK_PATH)
    assert goal.count(runbook) == 2  # one trigger each for Resume Check + Idle
    # Push policy must not override queue-specific ticket instructions such as
    # CHUCK's "commit and push main" workflow.
    assert "Do not push unless explicitly asked" not in goal
    assert "claimed ticket's worker instructions" in goal
    assert "leave commits local" in goal


def test_run_once_goal_uses_ticket_push_policy(wt):
    goal = wt.workers.run_once_goal("Q", "q-8", "Q-12", "/repo")
    assert "Q-12" in goal and "q-8" in goal and "/repo" in goal
    assert "Do not push unless explicitly asked" not in goal
    assert "claimed ticket's worker instructions" in goal
    assert "leave commits local" in goal


def test_worker_runbook_exists_and_covers_both_protocols(wt):
    text = wt.workers._WORKER_RUNBOOK_PATH.read_text()
    assert "## Resume Check" in text and "## Idle Protocol" in text
    assert "wt find" in text and "claimed_by" in text  # Resume Check detail
    assert "60 lines" in text or "~60" in text  # Idle Protocol detail


def test_config_is_reconcile_source_and_default_off(wt):
    # auto_drain defaults OFF (backlog until opt-in)
    assert wt.config.auto_drain("FRESH") is False
    # a queue only appears to reconcile once it has a config entry
    wt.config.set_auto_drain("Q", True)
    assert "Q" in wt.config.all_queues()


def test_peek_next_non_mutating(wt):
    wt.q.enqueue(project="Q", note="first")
    peeked = wt.q.peek_next(project="Q")
    assert peeked and peeked["status"] == "open"
    # peek must not claim — the item is still claimable.
    claimed = wt.q.claim_next("w1", project="Q")
    assert claimed["ref"] == peeked["ref"]


def test_repo_path_config_priority(wt):
    wt.config.set_auto_drain("Q", True)
    wt.config.set_repo_path("Q", "/configured/path")
    wt.q.enqueue(project="Q", note="work")
    r = wt.workers.reconcile_once(dry_run=True)
    spawned = [s for s in r["spawned"] if s["queue"] == "Q"]
    assert spawned and spawned[0]["repo_path"] == "/configured/path"


# ============================================ fable-model guard (WT-89)
def test_is_fable_model_matches_variants(wt):
    f = wt.workers._is_fable_model
    assert f("fable")
    assert f("Fable")
    assert f("fable-5")
    assert f("claude-fable-5")
    assert f("CLAUDE-FABLE-5")
    assert not f("sonnet-5")
    assert not f("claude-sonnet-5")
    assert not f("opus")
    assert not f("")


def test_spawn_workers_rejects_fable_model(wt, capsys):
    """spawn_workers strips a fable model and warns on stderr."""
    wt.config.set_auto_drain("Q", True)
    wt.config.set_model("Q", "claude-fable-5")
    wt.q.enqueue(project="Q", note="work")
    r = wt.workers.reconcile_once(dry_run=True)
    spawned = [s for s in r["spawned"] if s["queue"] == "Q"]
    assert spawned
    assert spawned[0].get("model", "") == "", "fable model should be stripped from spawned record"
    assert "--model" not in spawned[0].get("argv", []), "fable model must not appear in argv"
    err = capsys.readouterr().err
    assert "fable" in err.lower() and "refusing" in err.lower()


def test_spawn_workers_includes_engine_and_model_in_record(wt):
    """spawn_workers dry-run records carry engine and model (when non-fable)."""
    wt.config.set_auto_drain("Q", True)
    # Explicit engine so this doesn't depend on whichever bare default
    # config.engine() resolves to (WT-105: that default is now
    # availability-guarded against codex being on PATH, which varies by
    # machine) -- this test only cares that engine/model land in the record.
    wt.config.set_engine("Q", "claude")
    wt.config.set_model("Q", "claude-sonnet-5")
    wt.q.enqueue(project="Q", note="work")
    r = wt.workers.reconcile_once(dry_run=True)
    spawned = [s for s in r["spawned"] if s["queue"] == "Q"]
    assert spawned
    rec = spawned[0]
    assert rec.get("engine") == "claude"
    assert rec.get("model") == "claude-sonnet-5"


def test_spawn_run_once_worker_logs_spawn(wt, monkeypatch):
    """WT-103: the "drain once" play button's spawn must land a SPAWN row in
    the activity log, same as a reconcile-driven spawn -- otherwise the
    action leaves no trace at all."""
    wt.config.set_auto_drain("Q", False)
    item = wt.q.enqueue(project="Q", note="work")
    ref = item["ref"]

    class FakeProc:
        pid = 999999

    monkeypatch.setattr(wt.workers.subprocess, "Popen", lambda *a, **k: FakeProc())
    monkeypatch.setattr(wt.workers, "write_to_worker_fifo", lambda *a, **k: True)

    rec = wt.workers.spawn_run_once_worker("Q", ref)
    assert rec["queue"] == "Q"
    assert rec["ref"] == ref

    log_content = (wt.tmp / "activity.log").read_text()
    assert "SPAWN" in log_content
    assert f"run-once for {ref}" in log_content

    # WT-116 effort is queue-level policy, not a single-ticket override.
    with pytest.raises(TypeError):
        wt.workers.spawn_run_once_worker("Q", ref, effort="low")


# ============================================ cache-TTL staleness (warm vs cold)
def _age_worker_log(wt, rec, seconds):
    """Backdate a worker's log mtime so it reads as idle for `seconds`."""
    log = rec.get("log")
    old = time.time() - seconds
    os.utime(log, (old, old))


def test_notify_pushes_to_warm_worker(wt):
    """A worker idle WITHIN the cache TTL is warm -> gets the FIFO push."""
    rec = _live_worker(wt, "Q")
    _age_worker_log(wt, rec, wt.workers.WARM_TTL_S - 30)  # warm
    assert wt.workers.notify_workers("Q", "wake warm") == 1


def test_notify_still_pushes_worker_after_cache_ttl(wt):
    """Cache coldness does not strand new queue work before release eligibility."""
    rec = _live_worker(wt, "Q")
    _age_worker_log(wt, rec, wt.workers.WARM_TTL_S + 60)  # cold
    assert wt.workers.notify_workers("Q", "new work") == 1


def test_notify_skips_worker_already_released_from_queue(wt):
    rec = _live_worker(wt, "Q")
    wt.workers.request_stop(rec["worker_id"])

    assert wt.workers.notify_workers("Q", "new work") == 0


def test_release_floor_is_30_minutes_for_claude(wt):
    """Losing Claude's five-minute cache is not permission to release it."""
    rec = _live_worker(wt, "Q")
    _age_worker_log(wt, rec, wt.workers.WARM_TTL_S + 60)

    assert wt.workers.release_idle_workers(queue="Q") == []
    assert not (wt.workers.STOP_SIGNALS_DIR / rec["worker_id"]).exists()


def test_release_idle_claude_worker_without_killing_session(wt):
    rec = _live_worker(wt, "Q")
    _age_worker_log(wt, rec, wt.workers.RELEASE_IDLE_S + 60)

    released = wt.workers.release_idle_workers(queue="Q")

    assert [row["worker_id"] for row in released] == [rec["worker_id"]]
    assert (wt.workers.STOP_SIGNALS_DIR / rec["worker_id"]).exists()
    assert wt.workers._pid_alive(os.getpid())
    assert wt.workers.live_worker_count("Q") == 0
    assert wt.workers.worker_counts()["Q"] == {"total": 1, "live": 0}
    payload = json.loads(os.read(wt._readers[-1], 65536).decode())
    text = payload["message"]["content"][0]["text"]
    assert "no longer a WatchTower worker for Q" in text
    assert "continue any unrelated work" in text
    assert wt.q.claim_next(rec["worker_id"], project="Q") == {"stop": True}
    assert not (wt.workers.STOP_SIGNALS_DIR / rec["worker_id"]).exists()
    # The one-shot signal is gone, but durable detachment keeps this live
    # conversation out of queue staffing while unrelated work continues.
    assert wt.workers.live_worker_count("Q") == 0
    assert wt.workers.worker_counts()["Q"] == {"total": 1, "live": 0}


def test_reconcile_replaces_released_staffing_without_killing_old_session(wt, monkeypatch):
    wt.config.set_auto_drain("Q", True)
    wt.config.set_desired_workers("Q", 1)
    wt.q.enqueue(project="Q", note="new work")
    rec = _live_worker(wt, "Q")
    _age_worker_log(wt, rec, wt.workers.RELEASE_IDLE_S + 60)
    spawned = []

    def fake_spawn(queue, n=1, **kwargs):
        row = {
            "worker_id": "q-replacement", "queue": queue, "pid": 12345,
            "engine": kwargs.get("engine", "claude"),
        }
        spawned.append(row)
        return [row]

    monkeypatch.setattr(wt.workers, "spawn_workers", fake_spawn)

    result = wt.workers.reconcile_once()

    assert [row["worker_id"] for row in result["released"]] == [rec["worker_id"]]
    assert [row["worker_id"] for row in result["spawned"]] == ["q-replacement"]
    assert spawned and wt.workers._pid_alive(os.getpid())


def test_release_spares_claude_worker_with_fresh_transcript_activity(wt, monkeypatch):
    """A stale spawn log is not idle proof when Claude's transcript is live."""
    sid = "22222222-2222-2222-2222-222222222222"
    claude_home = wt.tmp / "claude-home"
    transcript_dir = claude_home / "projects" / "-tmp-project"
    transcript_dir.mkdir(parents=True)
    (transcript_dir / f"{sid}.jsonl").write_text('{"type":"user"}\n')
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(claude_home))
    log = wt.tmp / "claude-silent.log"
    log.write_text("")
    fifo, fd = wt.workers._make_stdin_fifo(log)
    wt._readers.append(fd)
    rec = wt.workers.record_worker(
        os.getpid(), "Q", "claude", "q-claude-silent", str(wt.tmp), str(log),
        fifo=fifo or "", session_id=sid,
    )
    _age_worker_log(wt, rec, wt.workers.RELEASE_IDLE_S + 60)

    assert wt.workers.release_idle_workers(queue="Q") == []
    assert not (wt.workers.STOP_SIGNALS_DIR / rec["worker_id"]).exists()


def test_release_marks_idle_worker_without_killing_process(wt):
    """Lifecycle release preserves the conversation and operating process."""
    child = subprocess.Popen(["sleep", "30"])
    log = wt.tmp / "cold.log"
    log.write_text("")
    fifo, fd = wt.workers._make_stdin_fifo(log)
    wt._readers.append(fd)
    rec = wt.workers.record_worker(
        child.pid, "Q", "claude", "q-cold", str(wt.tmp), str(log), fifo=fifo or "",
    )
    try:
        _age_worker_log(wt, rec, wt.workers.RELEASE_IDLE_S + 60)
        released = wt.workers.release_idle_workers(queue="Q")
        assert any(r["worker_id"] == "q-cold" for r in released)
        assert child.poll() is None
        assert (wt.workers.STOP_SIGNALS_DIR / "q-cold").exists()
    finally:
        child.terminate()
        child.wait(timeout=5)


def test_reap_spares_warm_worker(wt):
    rec = _live_worker(wt, "Q")
    _age_worker_log(wt, rec, wt.workers.WARM_TTL_S - 30)  # warm
    assert wt.workers.reap_stale_workers(queue="Q") == []


def test_reap_spares_codex_worker_with_fresh_rollout_activity(wt, monkeypatch):
    """A stale WT log is not idle proof when the Codex rollout is active."""
    child = subprocess.Popen(["sleep", "30"])
    log = wt.tmp / "codex-silent.log"
    log.write_text("")
    sid = "11111111-1111-1111-1111-111111111111"
    codex_home = wt.tmp / "codex-home"
    rollout_dir = codex_home / "sessions" / "2026" / "07" / "16"
    rollout_dir.mkdir(parents=True)
    rollout = rollout_dir / f"rollout-2026-07-16T00-00-00-{sid}.jsonl"
    rollout.write_text('{"type":"event_msg"}\n')
    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    rec = wt.workers.record_worker(
        child.pid, "Q", "codex", "q-codex-silent", str(wt.tmp), str(log),
        session_id=sid,
    )
    try:
        _age_worker_log(wt, rec, wt.workers.RELEASE_IDLE_S + 60)

        assert wt.workers.reap_stale_workers(queue="Q") == []
        assert child.poll() is None
    finally:
        child.terminate()
        child.wait(timeout=5)


def test_release_injects_queue_scoped_instruction_into_codex_session(wt, monkeypatch):
    import watchtower.messages as messages

    sent = []
    monkeypatch.setattr(
        messages, "send",
        lambda target, text: sent.append((target, text)) or {"ok": True},
    )
    sid = "33333333-3333-3333-3333-333333333333"
    log = wt.tmp / "codex-idle.log"
    log.write_text("")
    rec = wt.workers.record_worker(
        os.getpid(), "Q", "codex", "q-codex-idle", str(wt.tmp), str(log),
        session_id=sid,
    )
    _age_worker_log(wt, rec, wt.workers.RELEASE_IDLE_S + 60)

    released = wt.workers.release_idle_workers(queue="Q")

    assert [row["worker_id"] for row in released] == [rec["worker_id"]]
    assert sent and sent[0][0] == sid
    assert "continue any unrelated work" in sent[0][1]
    assert wt.workers._pid_alive(os.getpid())


def test_reap_spares_cold_worker_with_active_ticket(wt):
    """A stale log is not proof of idleness while the worker owns work."""
    child = subprocess.Popen(["sleep", "30"])
    log = wt.tmp / "active-cold.log"
    log.write_text("")
    rec = wt.workers.record_worker(
        child.pid, "Q", "codex", "q-active-cold", str(wt.tmp), str(log)
    )
    try:
        item = wt.q.enqueue(project="Q", note="long-running work")
        wt.q.claim_by_ref(item["ref"], rec["worker_id"])
        _age_worker_log(wt, rec, wt.workers.RELEASE_IDLE_S + 60)

        assert wt.workers.reap_stale_workers(queue="Q") == []
        assert child.poll() is None
    finally:
        child.terminate()
        child.wait(timeout=5)


def test_reap_spares_worker_owned_by_claimed_session_id(wt):
    child = subprocess.Popen(["sleep", "30"])
    log = wt.tmp / "active-session-cold.log"
    log.write_text("")
    session_id = "11111111-1111-1111-1111-111111111111"
    rec = wt.workers.record_worker(
        child.pid, "Q", "codex", "q-session-owner", str(wt.tmp), str(log),
        session_id=session_id,
    )
    try:
        item = wt.q.enqueue(project="Q", note="session-owned work")
        wt.q.claim_by_ref(item["ref"], "old-worker-alias", session_uuid=session_id)
        _age_worker_log(wt, rec, wt.workers.RELEASE_IDLE_S + 60)

        assert wt.workers.reap_stale_workers(queue="Q") == []
        assert child.poll() is None
    finally:
        child.terminate()
        child.wait(timeout=5)


def test_release_spares_worker_with_blocked_ticket(wt):
    child = subprocess.Popen(["sleep", "30"])
    log = wt.tmp / "blocked-cold.log"
    log.write_text("")
    rec = wt.workers.record_worker(
        child.pid, "Q", "codex", "q-blocked-cold", str(wt.tmp), str(log)
    )
    item = wt.q.enqueue(project="Q", note="needs a decision")
    wt.q.claim_by_ref(item["ref"], rec["worker_id"])
    wt.q.block(item["ref"], rec["worker_id"], "Which option?", "Investigated")
    try:
        _age_worker_log(wt, rec, wt.workers.RELEASE_IDLE_S + 60)

        assert wt.workers.release_idle_workers(queue="Q") == []
        assert child.poll() is None
        assert not (wt.workers.STOP_SIGNALS_DIR / rec["worker_id"]).exists()
    finally:
        child.terminate()
        child.wait(timeout=5)


def test_global_reap_fails_closed_per_queue(wt, monkeypatch):
    children = []
    records = []
    for queue in ("BROKEN", "HEALTHY"):
        child = subprocess.Popen(["sleep", "30"])
        children.append(child)
        log = wt.tmp / f"{queue.lower()}-cold.log"
        log.write_text("")
        rec = wt.workers.record_worker(
            child.pid, queue, "codex", f"{queue.lower()}-cold",
            str(wt.tmp), str(log),
        )
        records.append(rec)
        _age_worker_log(wt, rec, wt.workers.RELEASE_IDLE_S + 60)
    warm = subprocess.Popen(["sleep", "30"])
    children.append(warm)
    warm_log = wt.tmp / "warm.log"
    warm_log.write_text("")
    wt.workers.record_worker(
        warm.pid, "WARM", "codex", "warm-worker", str(wt.tmp), str(warm_log)
    )

    calls = []

    def strict_items(*, project=None, fresh=False, strict=False, **kwargs):
        calls.append((project, fresh, strict))
        if project == "BROKEN":
            raise RuntimeError("backend unavailable")
        return []

    monkeypatch.setattr(wt.q, "list_items", strict_items)
    try:
        released = wt.workers.release_idle_workers()
        assert [row["worker_id"] for row in released] == ["healthy-cold"]
        assert children[0].poll() is None
        assert calls == [
            ("BROKEN", True, True),
            ("HEALTHY", True, True),
        ]
    finally:
        for child in children:
            if child.poll() is None:
                child.terminate()
            child.wait(timeout=5)


def test_reap_fails_closed_when_file_queue_is_corrupt(wt):
    child = subprocess.Popen(["sleep", "30"])
    log = wt.tmp / "corrupt-store-cold.log"
    log.write_text("")
    rec = wt.workers.record_worker(
        child.pid, "Q", "codex", "q-corrupt-store", str(wt.tmp), str(log)
    )
    _age_worker_log(wt, rec, wt.workers.RELEASE_IDLE_S + 60)
    wt.q._resolve_store_path().write_text("{not-json")

    try:
        assert wt.workers.release_idle_workers(queue="Q") == []
        assert child.poll() is None
    finally:
        child.terminate()
        child.wait(timeout=5)


# ===================================== cloud session-id resolution (WT-38)
def test_resolve_session_id_from_log(wt):
    """The cloud UUID is parsed from the stream-json init event in the log."""
    log = wt.tmp / "w.log"
    log.write_text(
        '{"type":"system","subtype":"init",'
        '"session_id":"c44f96bc-d720-49d3-a5e6-115426939f82"}\n'
        '{"type":"assistant","message":{}}\n'
    )
    assert (wt.workers.resolve_session_id_from_log(str(log))
            == "c44f96bc-d720-49d3-a5e6-115426939f82")


def test_resolve_session_id_from_codex_exec_log(wt):
    """Codex exec logs print the session id as text, not stream JSON."""
    log = wt.tmp / "codex.log"
    log.write_text(
        "OpenAI Codex v0.142.5\n"
        "--------\n"
        "session id: 019f23e3-ba0e-7ec1-949d-d72d3f590ad2\n"
        "--------\n"
    )
    assert (wt.workers.resolve_session_id_from_log(str(log))
            == "019f23e3-ba0e-7ec1-949d-d72d3f590ad2")


def test_resolve_session_id_absent_returns_empty(wt):
    log = wt.tmp / "noinit.log"
    log.write_text('{"type":"assistant","message":{}}\n')
    assert wt.workers.resolve_session_id_from_log(str(log)) == ""


def test_list_workers_backfills_and_persists_session_id(wt):
    """list_workers parses the log, stamps session_id on the record, persists it
    so CCC can resolve worker -> session and link to its conversation."""
    log = wt.tmp / "bf.log"
    log.write_text(
        '{"type":"system","session_id":"c44f96bc-d720-49d3-a5e6-115426939f82"}\n'
    )
    rec = wt.workers.record_worker(
        os.getpid(), "Q", "claude", "q-bf", str(wt.tmp), str(log), fifo="",
    )
    assert not rec.get("session_id")  # not known at spawn
    rows = wt.workers.list_workers(prune=False)
    row = next(r for r in rows if r["worker_id"] == "q-bf")
    assert row["session_id"] == "c44f96bc-d720-49d3-a5e6-115426939f82"
    # persisted: a fresh read still has it (no re-parse needed)
    again = next(r for r in wt.workers.list_workers(prune=False)
                 if r["worker_id"] == "q-bf")
    assert again["session_id"] == "c44f96bc-d720-49d3-a5e6-115426939f82"


def test_backfill_claimed_session_id_from_codex_worker_log(wt):
    """Codex WT workers claim with worker_id, then expose the real UUID in logs."""
    sid = "019f23e3-ba0e-7ec1-949d-d72d3f590ad2"
    worker_id = "throughput-eb3f49da"
    log = wt.tmp / f"{worker_id}.log"
    log.write_text(f"session id: {sid}\n")
    wt.workers.record_worker(
        os.getpid(), "THROUGHPUT", "codex", worker_id, str(wt.tmp), str(log), fifo="",
    )
    item = wt.q.enqueue(
        project="THROUGHPUT", title="Native Codex usage", note="usage work",
    )
    wt.q.claim_next(worker_id, project="THROUGHPUT")

    wt.workers.list_workers(prune=False)
    assert wt.workers.backfill_claimed_session_ids() == [item["ref"]]
    found = wt.q.get(item["ref"])
    assert found["claimed_session_id"] == sid
    reg = wt.codex_registry.entry(sid)
    assert reg["thread_id"] == sid
    assert reg["engine"] == "codex"
    assert reg["visibility"] == "worker"
    assert reg["transport_owner"] == "wt-codex-exec"
    assert reg["transport"] == "codex-exec"
    assert reg["cwd"] == str(wt.tmp)
    assert reg["worker_id"] == worker_id
    assert reg["queue"] == "THROUGHPUT"
    assert reg["ref"] == item["ref"]
    assert reg["wt"]["worker_id"] == worker_id
    assert reg["wt"]["ref"] == item["ref"]


# ============================== persistent worker-session ledger (survives prune)
def test_ledger_records_session_id_on_backfill(wt):
    """Resolving a worker's session_id from its log appends it to the persistent
    ledger, which survives the worker being pruned from workers.json."""
    sid = "c44f96bc-d720-49d3-a5e6-115426939f82"
    log = wt.tmp / "led.log"
    log.write_text('{"type":"system","session_id":"%s"}\n' % sid)
    wt.workers.record_worker(
        os.getpid(), "Q", "claude", "q-led", str(wt.tmp), str(log), fifo="",
    )
    # Backfill resolves + ledgers the id.
    wt.workers.list_workers(prune=False)
    assert sid in wt.workers._load_worker_session_ledger()

    # Survives prune: even after the worker is gone from workers.json, the
    # ledger still holds its session_id. Simulate by clearing the live store.
    wt.workers._save({"workers": []})
    assert wt.workers.list_workers(prune=True) == []
    assert sid in wt.workers._load_worker_session_ledger()


def test_ledger_records_session_id_on_record_worker(wt):
    """record_worker with a known session_id ledgers it immediately."""
    sid = "a1b2c3d4-e5f6-7890-abcd-ef0123456789"
    wt.workers.record_worker(
        os.getpid(), "Q", "claude", "q-rec", str(wt.tmp), log="", fifo="",
        session_id=sid,
    )
    assert sid in wt.workers._load_worker_session_ledger()


def test_ledger_dedupes_and_caps(wt):
    """The ledger de-dupes and caps growth to the most recent entries."""
    sid = "a1b2c3d4-e5f6-7890-abcd-ef0123456789"
    wt.workers._add_worker_session_id(sid)
    wt.workers._add_worker_session_id(sid)  # duplicate -> no-op
    assert wt.workers._load_worker_session_ledger().count(sid) == 1
    # Push past the cap with synthetic UUIDs; oldest drops, newest kept.
    import uuid as _uuid
    last = ""
    for _ in range(wt.workers._WORKER_SESSIONS_CAP + 10):
        last = str(_uuid.uuid4())
        wt.workers._add_worker_session_id(last)
    ids = wt.workers._load_worker_session_ledger()
    assert len(ids) == wt.workers._WORKER_SESSIONS_CAP
    assert last in ids
    assert sid not in ids  # the very first id was evicted


# ==================================================== enqueue-and-claim (add --claim / take)
def _add_ns(queue, *, claim=False, worker="", note="work"):
    """Build the argparse-shaped namespace cmd_add reads."""
    class Ns:
        pass
    ns = Ns()
    ns.queue = queue
    ns.title = ""
    ns.note = note
    ns.text = ""
    ns.url = ""
    ns.lane = "normal"
    ns.type = ""
    ns.readiness = ""
    ns.priority = ""
    ns.value = ""
    ns.confidence = ""
    ns.worker = worker
    ns.claim = claim
    return ns


def _spy_dispatch(wt, monkeypatch):
    """Replace dispatch_after_enqueue with a call-counting spy; return the list of
    calls so a test can assert it was (not) invoked."""
    calls = []
    monkeypatch.setattr(
        wt.workers, "dispatch_after_enqueue",
        lambda queue, ref: calls.append((queue, ref)) or "",
    )
    return calls


def _only_item(wt, queue):
    items = wt.q.list_items(project=queue)
    assert len(items) == 1
    return items[0]


def test_add_claim_marks_in_progress_and_skips_dispatch(wt, monkeypatch):
    """`add --claim` (no --worker): item is in_progress, claimed by the default
    wt-cli-<pid> worker, and dispatch_after_enqueue is NOT called."""
    cli = _reloaded_cli(wt)
    calls = _spy_dispatch(wt, monkeypatch)
    rc = cli.cmd_add(_add_ns("Q", claim=True))
    assert rc == 0
    it = _only_item(wt, "Q")
    assert it["status"] == "in_progress"
    assert it["claimed_by"] == f"wt-cli-{os.getpid()}"
    assert calls == []  # already claimed -> no worker nudged/spawned


def test_add_claim_explicit_worker(wt, monkeypatch):
    """`add --claim --worker amir`: claimed_by is the explicit worker id."""
    cli = _reloaded_cli(wt)
    calls = _spy_dispatch(wt, monkeypatch)
    rc = cli.cmd_add(_add_ns("Q", claim=True, worker="amir"))
    assert rc == 0
    it = _only_item(wt, "Q")
    assert it["status"] == "in_progress"
    assert it["claimed_by"] == "amir"
    assert calls == []


def test_add_without_claim_stays_open_and_dispatches(wt, monkeypatch):
    """Regression: plain `add` leaves the item open and DOES dispatch."""
    cli = _reloaded_cli(wt)
    calls = _spy_dispatch(wt, monkeypatch)
    rc = cli.cmd_add(_add_ns("Q", claim=False))
    assert rc == 0
    it = _only_item(wt, "Q")
    assert it["status"] == "open"
    assert not it.get("claimed_by")
    assert len(calls) == 1  # existing contract: worker disposition runs


def test_take_is_add_with_claim(wt, monkeypatch):
    """`take` behaves exactly like `add --claim`: in_progress, claimed, no dispatch.
    The namespace has no `claim` attr (take doesn't register --claim); cmd_take
    must set it."""
    cli = _reloaded_cli(wt)
    calls = _spy_dispatch(wt, monkeypatch)
    ns = _add_ns("Q", worker="amir")
    del ns.claim  # take's subparser never registers --claim
    rc = cli.cmd_take(ns)
    assert rc == 0
    it = _only_item(wt, "Q")
    assert it["status"] == "in_progress"
    assert it["claimed_by"] == "amir"
    assert calls == []

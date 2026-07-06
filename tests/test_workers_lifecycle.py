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
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(tmp_path / "activity.log"))
    monkeypatch.setenv(
        "WATCHTOWER_CCC_SPAWN_DEFAULTS_FILE", str(tmp_path / "no-ccc-spawn-defaults.json")
    )

    import watchtower.queue as q
    import watchtower.health as health
    import watchtower.config as config
    import watchtower.workers as workers
    importlib.reload(q)
    importlib.reload(config)
    importlib.reload(health)
    importlib.reload(workers)
    # Keep registry-migration hermetic: point at a non-existent file.
    monkeypatch.setattr(config, "_REGISTRY_FILE", tmp_path / "no-registry.json")

    class Ns:
        pass
    ns = Ns()
    ns.q, ns.health, ns.config, ns.workers = q, health, config, workers
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

    def fake_spawn(queue, n=1, engine="claude", *, repo_path="", dry_run=False):
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
    wt.q.claim_next("q-dead-claimer", project="Q")  # claimer never registered as alive
    data = wt.q._load_unlocked()
    for it in data["items"]:
        if it["ref"] == ref:
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
    claimed = wt.q.claim_next("q-dead-claimer", project="Q")  # never registered as alive
    stale_snapshot = dict(claimed, claimed_at="2000-01-01T00:00:00Z")  # past grace window

    wt.q.close(ref, session_id="q-dead-claimer", resolution="done")  # real close lands first
    monkeypatch.setattr(wt.q, "list_items", lambda *a, **k: [stale_snapshot])

    reopened = wt.workers.requeue_orphaned_tickets()
    assert ref not in [it["ref"] for it in reopened]
    assert wt.q.get(ref)["status"] == "closed"


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
    item = wt.q.claim_next("w-stopme", project="Q")
    assert item and item.get("ref") == "Q-1"
    # Signal is consumed but ignored because work was claimable; this avoids
    # orphaning a ticket filed just after a drained-window STOP was dropped.
    assert not (wt.workers.STOP_SIGNALS_DIR / "w-stopme").exists()
    wt.workers.request_stop("w-stopme")
    assert wt.q.claim_next("w-stopme", project="Q") == {"stop": True}


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
    assert any("Drain the Q" in a for a in argv)


def test_drain_goal_content(wt):
    goal = wt.workers.drain_goal("Q", "q-7", "/repo")
    assert "Q" in goal and "q-7" in goal and "/repo" in goal
    assert "claim" in goal.lower()
    # New FIFO model: end-turn-on-empty, no sleep-loop.
    assert "end" in goal.lower()
    # Per-queue learnings: read at spawn, update at drain-completion.
    assert "learnings/Q.md" in goal


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
    wt.config.set_model("Q", "claude-sonnet-5")
    wt.q.enqueue(project="Q", note="work")
    r = wt.workers.reconcile_once(dry_run=True)
    spawned = [s for s in r["spawned"] if s["queue"] == "Q"]
    assert spawned
    rec = spawned[0]
    assert rec.get("engine") == "claude"
    assert rec.get("model") == "claude-sonnet-5"


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


def test_notify_skips_cold_worker(wt):
    """A worker idle PAST the cache TTL is cold -> not woken (would re-read a
    bloated context uncached); caller must reap+respawn instead."""
    rec = _live_worker(wt, "Q")
    _age_worker_log(wt, rec, wt.workers.WARM_TTL_S + 60)  # cold
    assert wt.workers.notify_workers("Q", "do not wake") == 0


def test_reap_kills_cold_idle_worker(wt):
    """reap_stale_workers SIGTERMs a cold idle worker so a fresh one can spawn.

    Uses a real short-lived child process as the 'worker' so the kill is
    observable without touching this test process."""
    child = subprocess.Popen(["sleep", "30"])
    log = wt.tmp / "cold.log"
    log.write_text("")
    fifo, fd = wt.workers._make_stdin_fifo(log)
    wt._readers.append(fd)
    rec = wt.workers.record_worker(
        child.pid, "Q", "claude", "q-cold", str(wt.tmp), str(log), fifo=fifo or "",
    )
    _age_worker_log(wt, rec, wt.workers.WARM_TTL_S + 60)  # cold
    reaped = wt.workers.reap_stale_workers(queue="Q")
    assert any(r["worker_id"] == "q-cold" for r in reaped)
    child.wait(timeout=5)  # it was terminated
    assert child.poll() is not None


def test_reap_spares_warm_worker(wt):
    rec = _live_worker(wt, "Q")
    _age_worker_log(wt, rec, wt.workers.WARM_TTL_S - 30)  # warm
    assert wt.workers.reap_stale_workers(queue="Q") == []


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

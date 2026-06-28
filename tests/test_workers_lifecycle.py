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
    r = wt.workers.reconcile_once(dry_run=True)
    assert len([s for s in r["spawned"] if s["queue"] == "Q"]) == 2


def test_reconcile_excess_workers_stopped(wt):
    wt.config.set_auto_drain("Q", True)  # desired defaults to 1
    wt.q.enqueue(project="Q", note="work")
    _live_worker(wt, "Q")
    _live_worker(wt, "Q")  # two live, one too many
    r = wt.workers.reconcile_once(dry_run=True)
    assert len([s for s in r["stopped"] if s["queue"] == "Q"]) == 1
    assert not r["spawned"]


def test_reconcile_empty_queue_winds_down_idle_worker(wt):
    wt.config.set_auto_drain("Q", True)  # drain on, but queue empty
    _live_worker(wt, "Q")
    r = wt.workers.reconcile_once(dry_run=True)
    assert len([s for s in r["stopped"] if s["queue"] == "Q"]) == 1


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
    assert item.get("stop") is True
    # Signal is consumed: a second claim now returns the real ticket.
    nxt = wt.q.claim_next("w-stopme", project="Q")
    assert nxt and nxt.get("ref")


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

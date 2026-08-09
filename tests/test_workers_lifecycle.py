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
from pathlib import Path
from types import SimpleNamespace

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
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude-home"))

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


def test_find_engine_ancestor_ignores_codex_app_server(wt, monkeypatch):
    """The shared app server is not a worker continuation process."""
    monkeypatch.setattr(wt.workers.os, "getppid", lambda: 9001)
    monkeypatch.setattr(wt.workers, "_pid_alive", lambda pid: True)
    monkeypatch.setattr(
        wt.workers.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            stdout=(
                "9000 /Users/amirfish/.local/bin/codex -c "
                "model_context_window=1000000 app-server --listen stdio://\n"
            )
        ),
    )

    assert wt.workers._find_engine_ancestor_pid("codex") == 0


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
    sid = f"00000000-0000-0000-0000-{len(wt._readers):012d}"
    transcript_dir = wt.tmp / "claude-home" / "projects" / "-test-project"
    transcript_dir.mkdir(parents=True, exist_ok=True)
    transcript = transcript_dir / f"{sid}.jsonl"
    transcript.write_text('{"type":"user"}\n')
    rec = workers.record_worker(
        os.getpid(), queue, "claude", wid, str(wt.tmp), str(log),
        fifo=fifo_path or "", session_id=sid,
    )
    rec["_test_activity_path"] = str(transcript)
    return rec


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


def test_reconcile_continues_when_one_queue_depth_lookup_fails(wt, monkeypatch):
    """A transient queue-backend failure must not stop other queues draining."""
    from watchtower.github_backend import GitHubBackendError

    wt.config.set_auto_drain("BROKEN", True)
    wt.config.set_auto_drain("HEALTHY", True)
    wt.q.enqueue(project="HEALTHY", note="work")

    real_count_claimable = wt.q.count_claimable

    def count_claimable(*, project=None, **kwargs):
        if project == "BROKEN":
            raise GitHubBackendError("GitHub backend unavailable")
        return real_count_claimable(project=project, **kwargs)

    monkeypatch.setattr(wt.q, "count_claimable", count_claimable)

    result = wt.workers.reconcile_once(dry_run=True)

    assert [s["queue"] for s in result["spawned"]] == ["HEALTHY"]
    assert any(
        skipped["queue"] == "BROKEN" and "depth lookup failed" in skipped["reason"]
        for skipped in result["skipped"]
    )


def test_reconcile_dry_run_spawn_is_labeled_in_activity(wt):
    wt.config.set_auto_drain("Q", True)
    wt.q.enqueue(project="Q", note="work")

    wt.workers.reconcile_once(dry_run=True)

    activity = (wt.tmp / "activity.log").read_text()
    spawn_line = next(
        line for line in activity.splitlines() if "  SPAWN    " in line
    )
    assert "(dry-run; no process started)" in spawn_line
    assert "— plan:" in spawn_line
    assert "(pid 0)" not in spawn_line


def test_reconcile_drain_off_skips(wt):
    wt.config.set_auto_drain("Q", False)
    wt.q.enqueue(project="Q", note="work")
    r = wt.workers.reconcile_once(dry_run=True)
    assert not r["spawned"]
    assert any(s["queue"] == "Q" and "auto_drain=off" in s["reason"] for s in r["skipped"])


def test_reconcile_drain_off_still_staffs_a_requested_run(wt):
    """auto_drain=off means "no automatic work", not "ignore the ▶ button":
    a run_requested ticket is the one thing that still gets a worker (the
    dead-end this whole eligibility change exists to fix)."""
    wt.config.set_auto_drain("Q", False)
    parked = wt.q.enqueue(project="Q", note="backlog")
    wanted = wt.q.enqueue(project="Q", note="run this one")
    wt.q.mark_runnable(wanted["ref"])

    r = wt.workers.reconcile_once(dry_run=True)

    assert [s["queue"] for s in r["spawned"]] == ["Q"]
    # Only the requested ticket counts as depth -- the parked backlog stays
    # parked, and the worker is told so.
    assert "1 requested to run" in r["spawned"][0]["spawn_reason"]
    assert wt.q.get(parked["ref"])["run_requested"] is False


def test_three_run_requests_on_a_drain_off_queue_run_one_at_a_time(wt):
    """Spec Part 3: three ▶ presses run serially, in order, respecting the
    worker budget -- not three workers at once."""
    wt.config.set_auto_drain("Q", False)
    wt.config.set_desired_workers("Q", 1)
    refs = [wt.q.enqueue(project="Q", note=f"work {i}")["ref"] for i in range(3)]
    for ref in refs:
        wt.q.mark_runnable(ref)

    first = wt.workers.reconcile_once(dry_run=True)
    assert [s["queue"] for s in first["spawned"]] == ["Q"]

    # That worker is now live: the next tick must not add a second one, however
    # many tickets are still queued to run.
    _live_worker(wt, "Q")
    assert not wt.workers.reconcile_once(dry_run=True)["spawned"]

    # And it works them oldest-first, one ticket at a time.
    assert wt.q.claim_next("w1", project="Q")["ref"] == refs[0]
    wt.q.close(refs[0], "w1", resolution={"summary": "done"})
    assert wt.q.claim_next("w1", project="Q")["ref"] == refs[1]


def test_cancelled_run_request_stops_being_staffed(wt):
    """Pressing ▶ again while still queued takes the queue back to parked."""
    wt.config.set_auto_drain("Q", False)
    item = wt.q.enqueue(project="Q", note="never mind")
    wt.q.mark_runnable(item["ref"])
    wt.q.clear_run_request(item["ref"])

    r = wt.workers.reconcile_once(dry_run=True)

    assert not r["spawned"]
    assert any(s["queue"] == "Q" and s["reason"] == "auto_drain=off" for s in r["skipped"])


# ==================================================== never-configured queue (WT-131)
def test_config_ensure_entries_is_batched_and_idempotent(wt):
    created = wt.config.ensure_entries(["A", "B", "A", "", None])
    assert created == ["A", "B"]
    assert "A" in wt.config.all_queues() and "B" in wt.config.all_queues()
    assert wt.config.ensure_entries(["A"]) == []  # already exists -> no-op


def test_enqueue_registers_a_brand_new_queue(wt):
    """A queue's very first-ever ticket must make it visible to the
    reconciler, or a later ▶ press silently no-ops forever (WT-131): dispatch
    nudges no live worker (there's never been one), reconcile_once() skips a
    queue with no config entry entirely -- not even into its own `skipped`
    list -- so the reason surfaced is the generic "no live worker accepted
    and none spawned" with no hint the real cause is "never registered"."""
    assert "NEWQ" not in wt.config.all_queues()
    wt.q.enqueue(project="NEWQ", note="first ever ticket")
    assert "NEWQ" in wt.config.all_queues()
    assert wt.config.auto_drain("NEWQ") is False  # unchanged default


def test_manual_run_spawns_worker_for_never_configured_queue(wt):
    item = wt.q.enqueue(project="NEWQ", note="first ever ticket")
    wt.q.mark_runnable(item["ref"])

    r = wt.workers.reconcile_once(dry_run=True)

    assert [s["queue"] for s in r["spawned"]] == ["NEWQ"]


def test_reconcile_backfills_config_for_queue_missing_it_by_any_other_path(wt):
    """Belt-and-suspenders for the enqueue()-side fix above: even if a
    queue's config entry is missing for some other reason (hand-edited
    config, a backend that bypasses queue.enqueue()), reconcile_once() must
    not blind itself to a queue with real open tickets and a pending run."""
    item = wt.q.enqueue(project="NEWQ2", note="first ever ticket")
    wt.q.mark_runnable(item["ref"])
    data = wt.config._load()
    data.pop("NEWQ2", None)
    wt.config._save(data)
    assert "NEWQ2" not in wt.config.all_queues()

    r = wt.workers.reconcile_once(dry_run=True)

    assert [s["queue"] for s in r["spawned"]] == ["NEWQ2"]
    assert "NEWQ2" in wt.config.all_queues()


def test_manual_run_worker_is_told_to_work_only_requested_tickets(wt, monkeypatch):
    """The file backend has no eligibility gate on claim, so the goal is what
    keeps a manual-run worker off the backlog its owner deliberately parked."""
    wt.config.set_auto_drain("Q", False)
    wt.q.enqueue(project="Q", note="backlog")
    wanted = wt.q.enqueue(project="Q", note="run this one")
    wt.q.mark_runnable(wanted["ref"])
    seen = {}

    def fake_spawn(queue, n=1, **kwargs):
        seen.update(kwargs)
        return [{"worker_id": "q-manual", "queue": queue}]

    monkeypatch.setattr(wt.workers, "spawn_workers", fake_spawn)

    wt.workers.reconcile_once(dry_run=True)

    assert "run_requested" in seen["extra_instructions"]
    assert "not auto-draining" in seen["extra_instructions"].lower()


def test_dispatch_after_enqueue_acts_on_a_requested_run_with_drain_off(wt, monkeypatch):
    """▶ must feel immediate: `wt run` dispatches instead of waiting for the
    next 30s reconciler tick."""
    wt.config.set_auto_drain("Q", False)
    item = wt.q.enqueue(project="Q", note="run me")

    parked = wt.workers.dispatch_after_enqueue("Q", item["ref"])
    assert "auto_drain off" in parked

    wt.q.mark_runnable(item["ref"])
    monkeypatch.setattr(
        wt.workers, "spawn_workers",
        lambda queue, n=1, **kwargs: [{"worker_id": "q-manual", "queue": queue}],
    )

    assert wt.workers.dispatch_after_enqueue("Q", item["ref"]) == "spawned worker q-manual"


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


def test_reconcile_blocked_worker_does_not_starve_other_claimable_work(wt):
    """A worker parked on a human question (``needs_input``) is alive but does
    no dispatch work until answered, which can take hours. Before this fix it
    still counted toward desired_workers, so one blocked ticket could occupy
    the queue's entire budget and starve every other claimable ticket
    alongside it (WT-129 blocking WT-131's dispatch)."""
    wt.config.set_auto_drain("Q", True)  # desired_workers defaults to 1
    blocked = wt.q.enqueue(project="Q", note="needs a human call")
    wt.q.enqueue(project="Q", note="unrelated claimable work")

    worker = _live_worker(wt, "Q")
    claimed = wt.q.claim_next(
        worker["worker_id"], project="Q", session_uuid=worker["session_id"],
    )
    assert claimed["ref"] == blocked["ref"]
    wt.q.block(blocked["ref"], question="fix or dismiss?", session_id=worker["worker_id"])

    r = wt.workers.reconcile_once(dry_run=True)
    assert len([s for s in r["spawned"] if s["queue"] == "Q"]) == 1


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
        launch_failures=None, **_kwargs,
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
    assert any("staffed=2==desired=2" in s["reason"] for s in skips)


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
    assert len(first["launch_failed"]) == 2
    assert first["launch_failed"][0]["reason"] == "engine usage limit"
    assert first["launch_failed"][0]["session_id"] == sid
    assert first["fallbacks"] == [{
        "queue": "Q", "from_engine": "codex", "to_engine": "claude",
        "reason": "engine usage limit",
    }]
    assert wt.config.engine("Q") == "claude"
    activity = (wt.tmp / "activity.log").read_text()
    correlated_failures = [
        line for line in activity.splitlines() if "  SPAWN_FAIL" in line
    ]
    assert len(correlated_failures) == 2
    assert all(
        f"reconcile_id={first['reconcile_id']}" in line
        and "cause=initial_staffing" in line
        for line in correlated_failures
    )

    ledger = json.loads((wt.tmp / "worker-sessions.json").read_text())
    assert sid in ledger["session_ids"]

    second = wt.workers.reconcile_once(dry_run=False)
    assert not second["spawned"]
    assert not second["launch_failed"]
    assert any(
        s["queue"] == "Q" and "launch cooldown" in s["reason"]
        for s in second["skipped"]
    )


def test_reconcile_usage_limit_falls_back_to_default_engine(wt, monkeypatch):
    """A quota-exhausted explicit engine is replaced once, not retried forever."""
    wt.config.set_auto_drain("Q", True)
    wt.config.set_engine("Q", "kimi")
    wt.config.set_model("Q", "kimi-code/k3")
    wt.q.enqueue(project="Q", note="work")
    calls = []

    def fake_spawn(
        queue, n=1, engine="claude", *, repo_path="", dry_run=False,
        launch_failures=None, **_kwargs,
    ):
        calls.append(engine)
        if engine == "kimi":
            launch_failures.append({"reason": "engine usage limit"})
            return []
        return [{"worker_id": "fallback-worker", "queue": queue, "engine": engine}]

    monkeypatch.setattr(wt.workers, "spawn_workers", fake_spawn)
    monkeypatch.setattr(wt.config, "fallback_engine", lambda failed: "codex")
    monkeypatch.setattr(wt.config, "default_model", lambda engine: "gpt-5.6-terra")

    result = wt.workers.reconcile_once(dry_run=False)

    assert calls == ["kimi", "codex"]
    assert wt.config.engine("Q") == "codex"
    assert wt.config.model("Q") == "gpt-5.6-terra"
    assert result["fallbacks"] == [{
        "queue": "Q", "from_engine": "kimi", "to_engine": "codex",
        "reason": "engine usage limit",
    }]


_KIMI_QUOTA_LOG = (
    '{"role":"meta","type":"turn.step.retrying","failed_attempt":1,'
    '"error_name":"APIConnectionError","error_message":"Connection error."}\n'
    "error: failed to run prompt: provider.api_error: 403 You've reached your "
    "usage limit for this billing cycle.\n"
)


def _dead_launch_worker(wt, **overrides):
    """Register a worker record whose pid is guaranteed dead."""
    log = wt.tmp / f"{overrides.get('worker_id', 'w')}.log"
    log.write_text(overrides.pop("log_text", _KIMI_QUOTA_LOG))
    rec = {
        "worker_id": "q-dead1234", "pid": 2 ** 31 - 1, "queue": "Q",
        "engine": "kimi", "repo_path": str(wt.tmp), "log": str(log),
        "started_at": "2099-01-01T00:00:00Z", "model": "kimi-code/k3",
    }
    rec.update(overrides)
    rec["log"] = str(log)
    (wt.tmp / "workers.json").write_text(json.dumps({"workers": [rec]}))
    return rec


def test_prune_postmortem_records_late_quota_failure(wt):
    """kimi backs off an APIConnectionError for ~2min before printing its 403,
    so the death lands well outside _LAUNCH_FAILURE_GRACE_S and used to go
    unrecorded -- leaving no cooldown and respawning into the same wall."""
    _dead_launch_worker(wt, worker_id="q-late")

    wt.workers.list_workers(prune=True)

    cooldown = wt.workers.active_launch_failure_cooldown("Q", "kimi")
    assert cooldown and cooldown["reason"] == "engine usage limit"
    assert cooldown["worker_id"] == "q-late"


def test_prune_postmortem_skips_workers_that_actually_ran(wt):
    """A worker with an engine session established did real work; a "usage
    limit" string in its log is ticket text, not a launch failure."""
    _dead_launch_worker(
        wt,
        worker_id="q-ran",
        session_id="11111111-1111-1111-1111-111111111111",
        log_text="ticket: users hit a usage limit on the pricing page\n",
    )

    wt.workers.list_workers(prune=True)

    assert wt.workers.active_launch_failure_cooldown("Q", "kimi") is None


def test_prune_postmortem_skips_long_lived_and_chatty_workers(wt, monkeypatch):
    """Age and log-size ceilings keep the classifier off logs of workers that ran."""
    _dead_launch_worker(wt, worker_id="q-old", started_at="2020-01-01T00:00:00Z")
    wt.workers.list_workers(prune=True)
    assert wt.workers.active_launch_failure_cooldown("Q", "kimi") is None

    monkeypatch.setattr(wt.workers, "_LAUNCH_POSTMORTEM_MAX_LOG_BYTES", 16)
    _dead_launch_worker(wt, worker_id="q-chatty")
    wt.workers.list_workers(prune=True)
    assert wt.workers.active_launch_failure_cooldown("Q", "kimi") is None


def test_prune_postmortem_is_not_run_when_not_pruning(wt):
    """prune=False leaves the record in place, so the post-mortem must wait --
    otherwise every reconciler tick would re-log the same LAUNCH_FAIL."""
    _dead_launch_worker(wt, worker_id="q-keep")

    wt.workers.list_workers(prune=False)

    assert wt.workers.active_launch_failure_cooldown("Q", "kimi") is None


def _fail(wt, **kw):
    log = wt.tmp / "fail.log"
    log.write_text(kw.pop("log_text", "usage limit reached\n"))
    return wt.workers._record_launch_failure(
        queue="Q", engine="kimi", worker_id="q-w", pid=1,
        log_path=log, reason="engine usage limit", **kw,
    )


def test_repeat_launch_failures_escalate_the_cooldown(wt):
    """kimi's billing-cycle 403 carries no reset timestamp, so the cooldown falls
    back to a flat 5 minutes -- a queue still on that engine would respawn into
    the same wall every 5 minutes for the rest of the cycle. Consecutive
    failures must back off instead."""
    base = wt.workers._LAUNCH_FAILURE_DEFAULT_COOLDOWN_S
    spans = []
    for _ in range(4):
        rec = _fail(wt)
        spans.append(rec["cooldown_until"] - time.time())
        # Expire the cooldown the way the passage of time would, leaving the
        # record in place so the streak survives.
        data = json.loads((wt.tmp / "launch-failures.json").read_text())
        data["Q:kimi"]["cooldown_until"] = time.time() - 1
        (wt.tmp / "launch-failures.json").write_text(json.dumps(data))
        assert wt.workers.active_launch_failure_cooldown("Q", "kimi") is None

    assert [round(s / base) for s in spans] == [1, 2, 4, 8]
    assert json.loads(
        (wt.tmp / "launch-failures.json").read_text()
    )["Q:kimi"]["consecutive"] == 4


def test_launch_failure_cooldown_is_capped(wt, monkeypatch):
    monkeypatch.setattr(wt.workers, "_LAUNCH_FAILURE_MAX_COOLDOWN_S", 900)
    for _ in range(10):
        rec = _fail(wt)
        data = json.loads((wt.tmp / "launch-failures.json").read_text())
        data["Q:kimi"]["cooldown_until"] = time.time() - 1
        (wt.tmp / "launch-failures.json").write_text(json.dumps(data))
        wt.workers.active_launch_failure_cooldown("Q", "kimi")
    assert rec["cooldown_until"] - time.time() <= 900


def test_provider_retry_at_wins_over_backoff(wt):
    """When the engine says when it will serve again, trust it over our guess."""
    _fail(wt)
    retry_at = time.time() + 30
    rec = _fail(wt, retry_at=retry_at)
    assert rec["consecutive"] == 2
    assert abs(rec["cooldown_until"] - retry_at) < 1


def test_stale_failure_record_does_not_inherit_a_streak(wt):
    """A queue that failed hard yesterday starts today at the default cooldown."""
    _fail(wt)
    _fail(wt)
    data = json.loads((wt.tmp / "launch-failures.json").read_text())
    data["Q:kimi"]["failed_at"] = time.time() - (
        wt.workers._LAUNCH_FAILURE_STREAK_WINDOW_S + 60
    )
    data["Q:kimi"]["cooldown_until"] = time.time() - 1
    (wt.tmp / "launch-failures.json").write_text(json.dumps(data))

    # Expired and outside the streak window -> dropped entirely.
    assert wt.workers.active_launch_failure_cooldown("Q", "kimi") is None
    assert "Q:kimi" not in json.loads(
        (wt.tmp / "launch-failures.json").read_text()
    )
    assert _fail(wt)["consecutive"] == 1


def test_established_session_clears_the_failure_streak(wt):
    """A worker that reaches a session proves the engine works again."""
    _fail(wt)
    _fail(wt)
    log = wt.tmp / "good.log"
    sid = "22222222-2222-2222-2222-222222222222"
    log.write_text(f'{{"type":"system","subtype":"init","session_id":"{sid}"}}\n')
    (wt.tmp / "workers.json").write_text(json.dumps({"workers": [{
        "worker_id": "q-good", "pid": os.getpid(), "queue": "Q",
        "engine": "kimi", "repo_path": str(wt.tmp), "log": str(log),
        "started_at": "2099-01-01T00:00:00Z",
    }]}))

    wt.workers.list_workers(prune=True)

    assert "Q:kimi" not in json.loads(
        (wt.tmp / "launch-failures.json").read_text()
    )
    assert _fail(wt)["consecutive"] == 1


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


def test_claim_allows_hosted_codex_thread_after_worker_pid_exits(
    wt, monkeypatch, capsys
):
    """A hosted continuation may claim after its short-lived exec parent exits."""
    cli = _reloaded_cli(wt)
    session_id = "11111111-1111-1111-1111-111111111111"
    worker_id = "q-codex-hosted"
    wt.workers.record_worker(
        _dead_pid(),
        "Q",
        "codex",
        worker_id,
        str(wt.tmp),
        str(wt.tmp / f"{worker_id}.log"),
        session_id=session_id,
    )
    wt.workers.list_workers()  # routine reads prune the dead exec process
    wt.q.enqueue(project="Q", note="continuation work")
    monkeypatch.setenv("CODEX_THREAD_ID", session_id)
    monkeypatch.setattr(cli.workers, "_find_engine_ancestor_pid", lambda engine: 0)

    rc = cli.cmd_claim(_claim_ns("Q", worker_id, json_out=True))

    assert rc == 0
    claimed = json.loads(capsys.readouterr().out)
    assert claimed["claimed_by"] == worker_id
    assert claimed["claimed_session_id"] == session_id


def test_claim_rejects_concurrent_codex_process_for_same_thread(
    wt, monkeypatch, capsys
):
    """A continuation must not replace a different Codex PID that is still live."""
    cli = _reloaded_cli(wt)
    session_id = "11111111-1111-1111-1111-111111111111"
    worker_id = "q-codex-live"
    wt.workers.record_worker(
        os.getpid(),
        "Q",
        "codex",
        worker_id,
        str(wt.tmp),
        str(wt.tmp / f"{worker_id}.log"),
        session_id=session_id,
    )
    ticket = wt.q.enqueue(project="Q", note="must stay open")
    monkeypatch.setenv("CODEX_THREAD_ID", session_id)
    monkeypatch.setattr(
        cli.workers, "_find_engine_ancestor_pid", lambda engine: os.getppid()
    )

    rc = cli.cmd_claim(_claim_ns("Q", worker_id, json_out=True))

    assert rc == 1
    assert "still owned by live pid" in capsys.readouterr().err
    assert wt.q.get(ticket["ref"])["status"] == "open"
    recorded = next(
        worker for worker in wt.workers.list_workers(prune=False)
        if worker["worker_id"] == worker_id
    )
    assert recorded["pid"] == os.getpid()


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


# ============================================ GC released-but-alive (GH issue #1)
def _spawn_sleeper():
    """A real, long-lived child process to stand in for a released worker."""
    return subprocess.Popen(
        ["sleep", "60"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def _wait_gone(pid, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            pass
        if not wt_module_pid_alive(pid):
            return True
        time.sleep(0.05)
    return False


def wt_module_pid_alive(pid):
    import watchtower.workers as workers
    return workers._pid_alive(pid)


def _released_worker(wt, queue, *, released_age_s, kill_sent_age_s=None):
    """Record a real, alive worker and backdate its released_at (and
    optionally gc_kill_sent_at) so it looks like it was released long ago."""
    proc = _spawn_sleeper()
    rec = wt.workers.record_worker(
        proc.pid, queue, "claude", f"{queue.lower()}-released-{proc.pid}",
        str(wt.tmp), str(wt.tmp / f"{proc.pid}.log"),
    )
    now = time.time()
    released_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now - released_age_s))
    with wt.workers._WorkersFileLock():
        data = wt.workers._load()
        for row in data["workers"]:
            if row["worker_id"] == rec["worker_id"]:
                row["released_at"] = released_at
                if kill_sent_age_s is not None:
                    row["gc_kill_sent_at"] = time.strftime(
                        "%Y-%m-%dT%H:%M:%SZ", time.gmtime(now - kill_sent_age_s)
                    )
        wt.workers._save(data)
    (wt.workers.STOP_SIGNALS_DIR).mkdir(parents=True, exist_ok=True)
    (wt.workers.STOP_SIGNALS_DIR / rec["worker_id"]).touch()
    return rec, proc


def test_reap_released_workers_leaves_fresh_release_alone(wt):
    rec, proc = _released_worker(wt, "Q", released_age_s=10)
    try:
        actions = wt.workers.reap_released_workers(ttl_s=3600)
        assert actions == []
        assert wt.workers._pid_alive(proc.pid)
        assert (wt.workers.STOP_SIGNALS_DIR / rec["worker_id"]).exists()
    finally:
        proc.terminate()
        proc.wait()


def test_reap_released_workers_sigterms_past_ttl(wt):
    rec, proc = _released_worker(wt, "Q", released_age_s=7200)
    try:
        actions = wt.workers.reap_released_workers(ttl_s=3600, kill_grace_s=30)
        assert len(actions) == 1
        assert actions[0]["action"] == "sigterm"
        assert actions[0]["worker_id"] == rec["worker_id"]
        assert _wait_gone(proc.pid)
        # SIGTERM only, not yet SIGKILL -- the grace window hasn't elapsed, so
        # the sentinel (only dropped once the process is confirmed dead) and
        # the row's gc_kill_sent_at marker are both still in place.
        data = wt.workers._load()
        row = next(r for r in data["workers"] if r["worker_id"] == rec["worker_id"])
        assert row.get("gc_kill_sent_at")
    finally:
        try:
            proc.terminate()
            proc.wait()
        except Exception:
            pass


def test_reap_released_workers_ignores_worker_released_gate(wt):
    """The GC pass must collect a released worker that every OTHER staffing
    path (live_worker_count, notify_workers, ...) correctly hides via
    _worker_released() -- gating the reaper the same way would mean it can
    never collect the workers it exists to collect (GH issue #1)."""
    rec, proc = _released_worker(wt, "Q", released_age_s=7200)
    try:
        assert wt.workers.live_worker_count("Q") == 0  # hidden everywhere else
        actions = wt.workers.reap_released_workers(ttl_s=3600)
        assert [a["worker_id"] for a in actions] == [rec["worker_id"]]
    finally:
        try:
            proc.terminate()
            proc.wait()
        except Exception:
            pass


def test_reap_released_workers_sigkills_after_grace(wt, monkeypatch):
    rec, proc = _released_worker(
        wt, "Q", released_age_s=7200, kill_sent_age_s=120,
    )
    # SIGTERM was "sent" (backdated) but the process ignored it (sleep does).
    try:
        actions = wt.workers.reap_released_workers(ttl_s=3600, kill_grace_s=30)
        assert len(actions) == 1
        assert actions[0]["action"] == "sigkill"
        assert _wait_gone(proc.pid)
        assert not (wt.workers.STOP_SIGNALS_DIR / rec["worker_id"]).exists()
    finally:
        try:
            proc.terminate()
            proc.wait()
        except Exception:
            pass


def test_reap_released_workers_cleans_up_already_dead(wt):
    """A released worker whose process already exited (no signal needed) still
    gets its orphaned sentinel swept -- via sweep_orphan_stop_signals(), which
    reap_released_workers() leaves to run right after it every reconcile tick."""
    dead = _dead_worker(wt, "Q")
    with wt.workers._WorkersFileLock():
        data = wt.workers._load()
        for row in data["workers"]:
            if row["worker_id"] == dead["worker_id"]:
                row["released_at"] = time.strftime(
                    "%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 7200)
                )
        wt.workers._save(data)
    (wt.workers.STOP_SIGNALS_DIR).mkdir(parents=True, exist_ok=True)
    (wt.workers.STOP_SIGNALS_DIR / dead["worker_id"]).touch()

    actions = wt.workers.reap_released_workers(ttl_s=3600)
    assert actions == []  # nothing alive to signal
    swept = wt.workers.sweep_orphan_stop_signals()
    assert dead["worker_id"] in swept
    assert not (wt.workers.STOP_SIGNALS_DIR / dead["worker_id"]).exists()


def test_sweep_orphan_stop_signals_keeps_live_worker_sentinel(wt):
    rec, proc = _released_worker(wt, "Q", released_age_s=10)
    try:
        swept = wt.workers.sweep_orphan_stop_signals()
        assert rec["worker_id"] not in swept
        assert (wt.workers.STOP_SIGNALS_DIR / rec["worker_id"]).exists()
    finally:
        proc.terminate()
        proc.wait()


def test_reconcile_once_reaps_and_sweeps(wt):
    rec, proc = _released_worker(
        wt, "Q", released_age_s=wt.workers.RELEASED_TTL_S + 60,
    )
    try:
        result = wt.workers.reconcile_once()
        assert [a["worker_id"] for a in result["reaped"]] == [rec["worker_id"]]
        assert _wait_gone(proc.pid)
    finally:
        try:
            proc.terminate()
            proc.wait()
        except Exception:
            pass


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


def test_spawn_workers_resolves_claude_opus_5_alias(wt):
    """A user-friendly alias is expanded to the canonical id before spawning."""
    wt.config.set_auto_drain("Q", True)
    wt.config.set_engine("Q", "claude")
    wt.config.set_model("Q", "opus-5")
    wt.q.enqueue(project="Q", note="work")
    r = wt.workers.reconcile_once(dry_run=True)
    spawned = [s for s in r["spawned"] if s["queue"] == "Q"]
    assert spawned
    rec = spawned[0]
    assert rec.get("engine") == "claude"
    assert rec.get("model") == "claude-opus-5"
    argv = rec.get("argv", [])
    assert "--model" in argv
    assert argv[argv.index("--model") + 1] == "claude-opus-5"


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
    assert "cause=manual_or_run_once" in log_content
    assert "reconcile_id=manual-" in log_content

    # WT-116 effort is queue-level policy, not a single-ticket override.
    with pytest.raises(TypeError):
        wt.workers.spawn_run_once_worker("Q", ref, effort="low")


def test_spawn_run_once_kimi_uses_resolved_binary_and_prompt_position(wt, monkeypatch):
    """Kimi needs its prompt immediately after ``-p`` and may not be on PATH."""
    wt.config.set_engine("Q", "kimi")
    item = wt.q.enqueue(project="Q", note="work")
    captured = []

    class FakeProc:
        pid = 999998

    def fake_popen(argv, **kwargs):
        captured.extend(argv)
        return FakeProc()

    monkeypatch.setattr(wt.workers.subprocess, "Popen", fake_popen)

    wt.workers.spawn_run_once_worker("Q", item["ref"])

    assert captured[0] == wt.workers._resolve_engine_bin("kimi")
    assert captured[captured.index("-p") + 1].startswith("Fix ticket Q-1")


# ============================================ cache-TTL staleness (warm vs cold)
def _age_worker_log(wt, rec, seconds):
    """Backdate a worker's log mtime so it reads as idle for `seconds`."""
    log = rec.get("log")
    old = time.time() - seconds
    os.utime(log, (old, old))
    test_activity = rec.get("_test_activity_path")
    if test_activity:
        os.utime(test_activity, (old, old))


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
    sid = "44444444-4444-4444-4444-444444444444"
    transcript_dir = wt.tmp / "claude-home" / "projects" / "-test-project"
    transcript_dir.mkdir(parents=True, exist_ok=True)
    transcript = transcript_dir / f"{sid}.jsonl"
    transcript.write_text('{"type":"user"}\n')
    rec = wt.workers.record_worker(
        child.pid, "Q", "claude", "q-cold", str(wt.tmp), str(log), fifo=fifo or "",
        session_id=sid,
    )
    rec["_test_activity_path"] = str(transcript)
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
    codex_home = wt.tmp / "codex-home"
    rollout_dir = codex_home / "sessions" / "2026" / "07" / "16"
    rollout_dir.mkdir(parents=True)
    rollout = rollout_dir / f"rollout-2026-07-16T00-00-00-{sid}.jsonl"
    rollout.write_text('{"type":"event_msg"}\n')
    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    log = wt.tmp / "codex-idle.log"
    log.write_text("")
    rec = wt.workers.record_worker(
        os.getpid(), "Q", "codex", "q-codex-idle", str(wt.tmp), str(log),
        session_id=sid,
    )
    _age_worker_log(wt, rec, wt.workers.RELEASE_IDLE_S + 60)
    old = time.time() - wt.workers.RELEASE_IDLE_S - 60
    os.utime(rollout, (old, old))

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
    codex_home = wt.tmp / "codex-home"
    rollout_dir = codex_home / "sessions" / "2026" / "07" / "16"
    rollout_dir.mkdir(parents=True)
    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    for index, queue in enumerate(("BROKEN", "HEALTHY"), start=1):
        child = subprocess.Popen(["sleep", "30"])
        children.append(child)
        log = wt.tmp / f"{queue.lower()}-cold.log"
        log.write_text("")
        sid = f"66666666-6666-6666-6666-{index:012d}"
        rollout = rollout_dir / f"rollout-{sid}.jsonl"
        rollout.write_text('{"type":"event_msg"}\n')
        rec = wt.workers.record_worker(
            child.pid, queue, "codex", f"{queue.lower()}-cold",
            str(wt.tmp), str(log), session_id=sid,
        )
        rec["_test_activity_path"] = str(rollout)
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


# ===================================== lifecycle audit + spawn causality (CCC-592)
def _activity_lines(wt, verb):
    path = wt.tmp / "activity.log"
    if not path.exists():
        return []
    return [line for line in path.read_text().splitlines() if f"  {verb}" in line]


def test_idle_threshold_crossing_logs_complete_correlated_bundle(wt):
    rec = _live_worker(wt, "Q")
    _age_worker_log(wt, rec, wt.workers.RELEASE_IDLE_S + 60)

    released = wt.workers.release_idle_workers(queue="Q")

    assert [row["worker_id"] for row in released] == [rec["worker_id"]]
    candidate = _activity_lines(wt, "IDLE_CANDIDATE")
    signals = _activity_lines(wt, "IDLE_SIGNAL")
    decisions = _activity_lines(wt, "IDLE_DECISION")
    releases = _activity_lines(wt, "RELEASE")
    assert len(candidate) == 1
    assert "worker_id=" + rec["worker_id"] in candidate[0]
    assert "effective_source=watchtower_stdout" in candidate[0]
    assert "floor_s=1800" in candidate[0]
    evaluation_id = candidate[0].split("evaluation_id=", 1)[1].split()[0]
    assert signals
    assert all(f"evaluation_id={evaluation_id}" in line for line in signals)
    assert any("signal=pid_alive value=true" in line for line in signals)
    assert any("signal=queue_read value=success" in line for line in signals)
    assert any("signal=pid_signal_planned value=false" in line for line in signals)
    assert len(decisions) == 1
    assert f"evaluation_id={evaluation_id}" in decisions[0]
    assert "decision=RELEASE" in decisions[0]
    assert "release_id=" in decisions[0]
    assert len(releases) == 1
    assert f"evaluation_id={evaluation_id}" in releases[0]
    assert "pid_signalled=false" in releases[0]
    assert "stop_sentinel=true" in releases[0]
    assert "released_at=" in releases[0]


def test_identical_idle_preserve_evaluation_is_suppressed(wt):
    rec = _live_worker(wt, "Q")
    item = wt.q.enqueue(project="Q", note="still working")
    wt.q.claim_by_ref(item["ref"], rec["worker_id"])
    _age_worker_log(wt, rec, wt.workers.RELEASE_IDLE_S + 60)

    assert wt.workers.release_idle_workers(queue="Q") == []
    assert wt.workers.release_idle_workers(queue="Q") == []

    assert len(_activity_lines(wt, "IDLE_CANDIDATE")) == 1
    decisions = _activity_lines(wt, "IDLE_DECISION")
    assert len(decisions) == 1
    assert "decision=PRESERVE" in decisions[0]
    assert "owned_by_worker" in decisions[0]


def test_changed_idle_evidence_emits_new_complete_bundle(wt):
    rec = _live_worker(wt, "Q")
    item = wt.q.enqueue(project="Q", note="still working")
    wt.q.claim_by_ref(item["ref"], rec["worker_id"])
    _age_worker_log(wt, rec, wt.workers.RELEASE_IDLE_S + 60)
    assert wt.workers.release_idle_workers(queue="Q") == []

    wt.q.block(item["ref"], rec["worker_id"], "Need a decision", "Investigated")
    assert wt.workers.release_idle_workers(queue="Q") == []

    assert len(_activity_lines(wt, "IDLE_CANDIDATE")) == 2
    decisions = _activity_lines(wt, "IDLE_DECISION")
    assert len(decisions) == 2
    assert "blocked_ticket" not in decisions[0]
    assert "blocked_ticket" in decisions[1]


def test_idle_candidate_with_fresh_activity_logs_active_again(wt):
    rec = _live_worker(wt, "Q")
    item = wt.q.enqueue(project="Q", note="still working")
    wt.q.claim_by_ref(item["ref"], rec["worker_id"])
    _age_worker_log(wt, rec, wt.workers.RELEASE_IDLE_S + 60)
    assert wt.workers.release_idle_workers(queue="Q") == []

    Path(rec["log"]).touch()
    assert wt.workers.release_idle_workers(queue="Q") == []

    active_again = _activity_lines(wt, "ACTIVE_AGAIN")
    assert len(active_again) == 1
    assert "worker_id=" + rec["worker_id"] in active_again[0]
    assert "effective_source=watchtower_stdout" in active_again[0]
    assert "staffing_attached=true" in active_again[0]


def test_idle_queue_read_failure_logs_all_fail_closed_evidence(wt, monkeypatch):
    rec = _live_worker(wt, "Q")
    _age_worker_log(wt, rec, wt.workers.RELEASE_IDLE_S + 60)

    def fail_read(**kwargs):
        raise RuntimeError("backend unavailable")

    monkeypatch.setattr(wt.q, "list_items", fail_read)
    assert wt.workers.release_idle_workers(queue="Q") == []

    signals = _activity_lines(wt, "IDLE_SIGNAL")
    assert any(
        "signal=queue_read value=error" in line
        and "backend unavailable" in line
        for line in signals
    )
    decision = _activity_lines(wt, "IDLE_DECISION")[0]
    assert "decision=PRESERVE" in decision
    assert "queue_read_error" in decision


def test_missing_authoritative_activity_evidence_preserves_worker(wt):
    rec = _live_worker(wt, "Q")
    Path(rec["log"]).unlink()
    Path(rec["_test_activity_path"]).unlink()

    assert wt.workers.release_idle_workers(max_idle_s=0, queue="Q") == []

    decision = _activity_lines(wt, "IDLE_DECISION")[0]
    assert "decision=PRESERVE" in decision
    assert "activity_evidence_missing" in decision
    assert not (wt.workers.STOP_SIGNALS_DIR / rec["worker_id"]).exists()


def test_missing_session_identity_preserves_stale_worker(wt):
    log = wt.tmp / "sessionless.log"
    log.write_text("")
    rec = wt.workers.record_worker(
        os.getpid(), "Q", "claude", "q-sessionless",
        str(wt.tmp), str(log),
    )
    _age_worker_log(wt, rec, wt.workers.RELEASE_IDLE_S + 60)

    assert wt.workers.release_idle_workers(queue="Q") == []

    decision = _activity_lines(wt, "IDLE_DECISION")[0]
    assert "decision=PRESERVE" in decision
    assert "session_identity_missing" in decision


def test_unknown_engine_preserves_stale_worker(wt):
    log = wt.tmp / "unknown-engine.log"
    log.write_text("")
    rec = wt.workers.record_worker(
        os.getpid(), "Q", "mystery", "q-unknown-engine",
        str(wt.tmp), str(log),
        session_id="55555555-5555-5555-5555-555555555555",
    )
    _age_worker_log(wt, rec, wt.workers.RELEASE_IDLE_S + 60)

    assert wt.workers.release_idle_workers(queue="Q") == []

    decision = _activity_lines(wt, "IDLE_DECISION")[0]
    assert "decision=PRESERVE" in decision
    assert "authoritative_activity_unknown" in decision


def test_unknown_pid_identity_preserves_worker(wt):
    rec = _live_worker(wt, "Q")
    _age_worker_log(wt, rec, wt.workers.RELEASE_IDLE_S + 60)
    data = wt.workers._load()
    data["workers"][0].pop("pid_started", None)
    wt.workers._save(data)

    assert wt.workers.release_idle_workers(queue="Q") == []

    decision = _activity_lines(wt, "IDLE_DECISION")[0]
    assert "decision=PRESERVE" in decision
    assert "pid_identity_unknown" in decision
    assert not (wt.workers.STOP_SIGNALS_DIR / rec["worker_id"]).exists()


def test_audit_bundle_write_failure_fails_closed(wt, monkeypatch):
    rec = _live_worker(wt, "Q")
    _age_worker_log(wt, rec, wt.workers.RELEASE_IDLE_S + 60)
    monkeypatch.setattr(wt.q, "_log_many", lambda events: False)

    assert wt.workers.release_idle_workers(queue="Q") == []
    assert not (wt.workers.STOP_SIGNALS_DIR / rec["worker_id"]).exists()
    stored = wt.workers._load()["workers"][0]
    assert "released_at" not in stored


def test_release_stop_failure_stays_attached_and_retries(wt, monkeypatch):
    rec = _live_worker(wt, "Q")
    _age_worker_log(wt, rec, wt.workers.RELEASE_IDLE_S + 60)
    blocked_stop_dir = wt.tmp / "not-a-directory"
    blocked_stop_dir.write_text("occupied")
    monkeypatch.setattr(wt.workers, "STOP_SIGNALS_DIR", blocked_stop_dir)

    assert wt.workers.release_idle_workers(queue="Q") == []
    stored = wt.workers._load()["workers"][0]
    assert "released_at" not in stored
    assert stored["lifecycle_audit"]["decision"] == "RELEASE_FAILED"
    assert len(_activity_lines(wt, "RELEASE_FAIL")) == 1

    blocked_stop_dir.unlink()
    released = wt.workers.release_idle_workers(queue="Q")
    assert [row["worker_id"] for row in released] == [rec["worker_id"]]
    assert len(_activity_lines(wt, "IDLE_CANDIDATE")) == 2


def test_active_again_log_failure_keeps_transition_retryable(wt, monkeypatch):
    rec = _live_worker(wt, "Q")
    item = wt.q.enqueue(project="Q", note="still working")
    wt.q.claim_by_ref(item["ref"], rec["worker_id"])
    _age_worker_log(wt, rec, wt.workers.RELEASE_IDLE_S + 60)
    assert wt.workers.release_idle_workers(queue="Q") == []
    Path(rec["log"]).touch()
    Path(rec["_test_activity_path"]).touch()
    real_log_many = wt.q._log_many

    monkeypatch.setattr(
        wt.q,
        "_log_many",
        lambda events: (
            False if events and events[0][0] == "ACTIVE_AGAIN"
            else real_log_many(events)
        ),
    )
    assert wt.workers.release_idle_workers(queue="Q") == []
    assert wt.workers._load()["workers"][0]["lifecycle_audit"]["candidate"] is True

    monkeypatch.setattr(wt.q, "_log_many", real_log_many)
    assert wt.workers.release_idle_workers(queue="Q") == []
    assert len(_activity_lines(wt, "ACTIVE_AGAIN")) == 1


def test_release_log_failure_is_persisted_and_replayed(wt, monkeypatch):
    rec = _live_worker(wt, "Q")
    _age_worker_log(wt, rec, wt.workers.RELEASE_IDLE_S + 60)
    real_log_many = wt.q._log_many

    monkeypatch.setattr(
        wt.q,
        "_log_many",
        lambda events: (
            False if events and events[0][0] == "RELEASE"
            else real_log_many(events)
        ),
    )
    released = wt.workers.release_idle_workers(queue="Q")
    assert released and released[0]["_release_log_written"] is False
    state = wt.workers._load()["workers"][0]["lifecycle_audit"]
    assert state["release_log_pending"]["detail"]
    assert _activity_lines(wt, "RELEASE") == []

    monkeypatch.setattr(wt.q, "_log_many", real_log_many)
    assert wt.workers.release_idle_workers(queue="Q") == []
    assert len(_activity_lines(wt, "RELEASE")) == 1
    state = wt.workers._load()["workers"][0]["lifecycle_audit"]
    assert "release_log_pending" not in state


def test_dead_worker_with_pending_release_log_is_not_pruned(wt, monkeypatch):
    child = subprocess.Popen(["sleep", "30"])
    log = wt.tmp / "pending-release.log"
    log.write_text("")
    fifo, fd = wt.workers._make_stdin_fifo(log)
    wt._readers.append(fd)
    sid = "77777777-7777-7777-7777-777777777777"
    transcript_dir = wt.tmp / "claude-home" / "projects" / "-test-project"
    transcript_dir.mkdir(parents=True, exist_ok=True)
    transcript = transcript_dir / f"{sid}.jsonl"
    transcript.write_text('{"type":"user"}\n')
    rec = wt.workers.record_worker(
        child.pid, "Q", "claude", "q-pending-release",
        str(wt.tmp), str(log), fifo=fifo or "", session_id=sid,
    )
    rec["_test_activity_path"] = str(transcript)
    _age_worker_log(wt, rec, wt.workers.RELEASE_IDLE_S + 60)
    real_log_many = wt.q._log_many
    monkeypatch.setattr(
        wt.q,
        "_log_many",
        lambda events: (
            False if events and events[0][0] == "RELEASE"
            else real_log_many(events)
        ),
    )
    try:
        assert wt.workers.release_idle_workers(queue="Q")
        child.terminate()
        child.wait(timeout=5)
        rows = wt.workers.list_workers(prune=True)
        assert [row["worker_id"] for row in rows] == ["q-pending-release"]
        assert wt.workers._load()["workers"]

        monkeypatch.setattr(wt.q, "_log_many", real_log_many)
        wt.workers.release_idle_workers(queue="Q")
        wt.workers.list_workers(prune=True)
        assert wt.workers._load()["workers"] == []
    finally:
        if child.poll() is None:
            child.terminate()
            child.wait(timeout=5)


def test_normal_idle_release_never_signals_worker_pid(wt, monkeypatch):
    rec = _live_worker(wt, "Q")
    _age_worker_log(wt, rec, wt.workers.RELEASE_IDLE_S + 60)
    real_kill = os.kill
    observed_signals = []

    def observing_kill(pid, sig):
        observed_signals.append(sig)
        return real_kill(pid, sig)

    monkeypatch.setattr(wt.workers.os, "kill", observing_kill)
    assert wt.workers.release_idle_workers(queue="Q")
    assert observed_signals
    assert set(observed_signals) == {0}


def test_reconcile_logs_correlated_release_replacement_plan_and_spawn(wt, monkeypatch):
    wt.config.set_auto_drain("Q", True)
    wt.config.set_desired_workers("Q", 1)
    wt.q.enqueue(project="Q", note="new work")
    rec = _live_worker(wt, "Q")
    _age_worker_log(wt, rec, wt.workers.RELEASE_IDLE_S + 60)

    monkeypatch.setattr(
        wt.workers,
        "spawn_workers",
        lambda queue, n=1, **kwargs: [
            {
                "worker_id": "q-replacement",
                "queue": queue,
                "pid": 12345,
                "engine": kwargs.get("engine", "claude"),
            }
        ],
    )

    result = wt.workers.reconcile_once()

    assert [row["worker_id"] for row in result["released"]] == [rec["worker_id"]]
    plan = _activity_lines(wt, "SPAWN_PLAN")[0]
    spawn = next(
        line for line in _activity_lines(wt, "SPAWN")
        if "worker_id=q-replacement" in line
    )
    reconcile_id = plan.split("reconcile_id=", 1)[1].split()[0]
    release_id = result["released"][0]["_release_id"]
    assert "cause=release_replacement" in plan
    assert "requested=1" in plan
    assert f"release_ids=[\"{release_id}\"]" in plan
    assert f"reconcile_id={reconcile_id}" in spawn
    assert "cause=release_replacement" in spawn
    assert f"release_ids=[\"{release_id}\"]" in spawn
    assert f"previous_worker_ids=[\"{rec['worker_id']}\"]" in spawn


def test_release_without_claimable_work_logs_zero_spawn_plan(wt):
    wt.config.set_auto_drain("Q", True)
    wt.config.set_desired_workers("Q", 1)
    rec = _live_worker(wt, "Q")
    _age_worker_log(wt, rec, wt.workers.RELEASE_IDLE_S + 60)

    result = wt.workers.reconcile_once()

    assert [row["worker_id"] for row in result["released"]] == [rec["worker_id"]]
    assert result["spawned"] == []
    plan = _activity_lines(wt, "SPAWN_PLAN")[0]
    assert "cause=release_replacement" in plan
    assert "claimable_depth=0" in plan
    assert "requested=0" in plan
    assert "zero_spawn_reason=no_claimable_work" in plan


def test_release_on_auto_drain_off_queue_logs_zero_spawn_plan(wt):
    wt.config.set_auto_drain("Q", False)
    rec = _live_worker(wt, "Q")
    _age_worker_log(wt, rec, wt.workers.RELEASE_IDLE_S + 60)

    result = wt.workers.reconcile_once()

    assert result["released"]
    plan = _activity_lines(wt, "SPAWN_PLAN")[0]
    assert "requested=0" in plan
    assert "zero_spawn_reason=auto_drain_off" in plan


def test_release_with_remaining_staffing_logs_zero_spawn_plan(wt):
    wt.config.set_auto_drain("Q", True)
    wt.config.set_desired_workers("Q", 1)
    wt.q.enqueue(project="Q", note="work one")
    wt.q.enqueue(project="Q", note="work two")
    cold = _live_worker(wt, "Q")
    _live_worker(wt, "Q")
    _age_worker_log(wt, cold, wt.workers.RELEASE_IDLE_S + 60)

    result = wt.workers.reconcile_once()

    assert [row["worker_id"] for row in result["released"]] == [cold["worker_id"]]
    assert result["spawned"] == []
    plan = _activity_lines(wt, "SPAWN_PLAN")[0]
    assert "requested=0" in plan
    assert "zero_spawn_reason=staffing_sufficient_after_release" in plan


def test_mixed_deficit_links_only_incremental_replacement_spawn(wt, monkeypatch):
    wt.config.set_auto_drain("Q", True)
    wt.config.set_desired_workers("Q", 3)
    for number in range(3):
        wt.q.enqueue(project="Q", note=f"work {number}")
    cold = _live_worker(wt, "Q")
    _age_worker_log(wt, cold, wt.workers.RELEASE_IDLE_S + 60)

    def fake_spawn(queue, n=1, **kwargs):
        return [
            {
                "worker_id": f"q-new-{index}",
                "queue": queue,
                "pid": 12000 + index,
                "engine": "claude",
                "_spawn_index": index,
            }
            for index in range(n)
        ]

    monkeypatch.setattr(wt.workers, "spawn_workers", fake_spawn)
    result = wt.workers.reconcile_once()

    assert len(result["spawned"]) == 3
    assert [row["_spawn_cause"] for row in result["spawned"]] == [
        "release_replacement",
        "scale_up",
        "scale_up",
    ]
    assert result["spawned"][0]["_related_release_ids"]
    assert result["spawned"][1]["_related_release_ids"] == []
    assert result["spawned"][2]["_related_release_ids"] == []
    plan = result["spawn_plans"][0]
    assert plan["replacement_slots"] == 1
    assert plan["base_cause"] == "scale_up"


def test_spawn_plan_log_failure_is_reported_without_dropping_work(
    wt, monkeypatch, capsys
):
    wt.config.set_auto_drain("Q", True)
    wt.q.enqueue(project="Q", note="work")
    real_log = wt.q._log

    def selective_log(verb, detail, queue=""):
        if verb == "SPAWN_PLAN":
            return False
        return real_log(verb, detail, queue)

    monkeypatch.setattr(wt.q, "_log", selective_log)
    result = wt.workers.reconcile_once(dry_run=True)

    assert result["spawned"]
    assert result["spawn_plans"][0]["logged"] is False
    assert "failed to log SPAWN_PLAN for Q" in capsys.readouterr().err


def test_release_replacement_waits_for_plan_log_and_keeps_correlation(
    wt, monkeypatch
):
    wt.config.set_auto_drain("Q", True)
    wt.config.set_desired_workers("Q", 1)
    wt.q.enqueue(project="Q", note="work")
    cold = _live_worker(wt, "Q")
    _age_worker_log(wt, cold, wt.workers.RELEASE_IDLE_S + 60)
    real_log = wt.q._log
    spawn_calls = []

    def fake_spawn(queue, n=1, **kwargs):
        spawn_calls.append(n)
        return [
            {
                "worker_id": "q-replacement",
                "queue": queue,
                "pid": 12345,
                "engine": "claude",
                "_spawn_index": 0,
            }
        ]

    monkeypatch.setattr(wt.workers, "spawn_workers", fake_spawn)
    monkeypatch.setattr(
        wt.q,
        "_log",
        lambda verb, detail, queue="": (
            False if verb == "SPAWN_PLAN" else real_log(verb, detail, queue)
        ),
    )
    first = wt.workers.reconcile_once()
    assert first["released"]
    assert first["spawned"] == []
    assert spawn_calls == []
    state = wt.workers._load()["workers"][0]["lifecycle_audit"]
    assert state["spawn_plan_pending"]["replacement"] is True

    monkeypatch.setattr(wt.q, "_log", real_log)
    second = wt.workers.reconcile_once()
    assert spawn_calls == [1]
    assert second["spawned"][0]["_spawn_cause"] == "release_replacement"
    assert second["spawned"][0]["_related_release_ids"] == [
        first["released"][0]["_release_id"]
    ]


def test_mixed_dead_recovery_still_defers_unlogged_replacement_plan(
    wt, monkeypatch
):
    wt.config.set_auto_drain("Q", True)
    wt.config.set_desired_workers("Q", 2)
    wt.q.enqueue(project="Q", note="work one")
    wt.q.enqueue(project="Q", note="work two")
    cold = _live_worker(wt, "Q")
    _dead_worker(wt, "Q")
    _age_worker_log(wt, cold, wt.workers.RELEASE_IDLE_S + 60)
    real_log = wt.q._log
    spawn_calls = []
    monkeypatch.setattr(
        wt.workers,
        "spawn_workers",
        lambda queue, n=1, **kwargs: spawn_calls.append(n) or [],
    )
    monkeypatch.setattr(
        wt.q,
        "_log",
        lambda verb, detail, queue="": (
            False if verb == "SPAWN_PLAN" else real_log(verb, detail, queue)
        ),
    )

    result = wt.workers.reconcile_once()

    assert result["spawned"] == []
    assert spawn_calls == []
    assert result["spawn_plans"][0]["base_cause"] == "dead_worker_recovery"
    assert result["spawn_plans"][0]["replacement_slots"] == 1


def test_engine_activity_lookup_error_is_logged_and_preserved(wt, monkeypatch):
    rec = _live_worker(wt, "Q")
    _age_worker_log(wt, rec, wt.workers.RELEASE_IDLE_S + 60)
    monkeypatch.setattr(
        wt.workers,
        "_newest_matching_path",
        lambda paths: (None, "PermissionError: denied"),
    )

    assert wt.workers.release_idle_workers(queue="Q") == []

    signal = next(
        line for line in _activity_lines(wt, "IDLE_SIGNAL")
        if "signal=claude_transcript" in line
    )
    assert "PermissionError: denied" in signal
    decision = _activity_lines(wt, "IDLE_DECISION")[0]
    assert "authoritative_activity_unreadable" in decision


def test_partial_usage_fallback_preserves_failed_slot_causality(
    wt, monkeypatch
):
    wt.config.set_auto_drain("Q", True)
    wt.config.set_desired_workers("Q", 2)
    wt.config.set_engine("Q", "codex")
    wt.q.enqueue(project="Q", note="work one")
    wt.q.enqueue(project="Q", note="work two")
    cold = _live_worker(wt, "Q")
    _age_worker_log(wt, cold, wt.workers.RELEASE_IDLE_S + 60)
    calls = []

    def fake_spawn(queue, n=1, engine="claude", launch_failures=None, **kwargs):
        calls.append((engine, n))
        if engine == "codex":
            launch_failures.append(
                {
                    "worker_id": "q-primary-failed",
                    "queue": queue,
                    "reason": "engine usage limit",
                    "_spawn_index": 1,
                }
            )
            return [
                {
                    "worker_id": "q-primary-ok",
                    "queue": queue,
                    "pid": 12000,
                    "engine": engine,
                    "_spawn_index": 0,
                }
            ]
        return [
            {
                "worker_id": "q-fallback-ok",
                "queue": queue,
                "pid": 12001,
                "engine": engine,
                "_spawn_index": 0,
            }
        ]

    monkeypatch.setattr(wt.workers, "spawn_workers", fake_spawn)
    result = wt.workers.reconcile_once()

    assert calls == [("codex", 2), ("claude", 1)]
    assert [row["_spawn_cause"] for row in result["spawned"]] == [
        "release_replacement",
        "scale_up",
    ]
    assert result["spawned"][0]["_related_release_ids"]
    assert result["spawned"][1]["_related_release_ids"] == []


@pytest.mark.parametrize(
    ("setup", "expected_cause"),
    [
        ("initial", "initial_staffing"),
        ("scale", "scale_up"),
        ("dead", "dead_worker_recovery"),
        ("manual", "manual_or_run_once"),
    ],
)
def test_reconcile_spawn_cause_classification(wt, setup, expected_cause):
    wt.config.set_auto_drain("Q", setup != "manual")
    wt.config.set_desired_workers("Q", 2 if setup == "scale" else 1)
    first = wt.q.enqueue(project="Q", note="work one")
    if setup == "scale":
        wt.q.enqueue(project="Q", note="work two")
        _live_worker(wt, "Q")
    elif setup == "dead":
        _dead_worker(wt, "Q")
    elif setup == "manual":
        wt.q.mark_runnable(first["ref"])

    wt.workers.reconcile_once(dry_run=True)

    plan = _activity_lines(wt, "SPAWN_PLAN")[0]
    spawn = _activity_lines(wt, "SPAWN")[0]
    assert f"cause={expected_cause}" in plan
    assert f"cause={expected_cause}" in spawn


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


def test_resolve_session_id_from_kimi_log(wt):
    """kimi -p stream-json logs close with a session.resume_hint meta line;
    the session_-prefixed id is returned as-is (CCC indexes kimi that way)."""
    log = wt.tmp / "kimi.log"
    log.write_text(
        '{"role":"assistant","content":"done"}\n'
        '{"role":"meta","type":"session.resume_hint",'
        '"session_id":"session_86aa9848-9a2d-4bf7-8aa6-ce55b6e1ff61",'
        '"command":"kimi -r session_86aa9848-9a2d-4bf7-8aa6-ce55b6e1ff61"}\n'
    )
    assert (wt.workers.resolve_session_id_from_log(str(log))
            == "session_86aa9848-9a2d-4bf7-8aa6-ce55b6e1ff61")


def test_resolve_session_id_absent_returns_empty(wt):
    log = wt.tmp / "noinit.log"
    log.write_text('{"type":"assistant","message":{}}\n')
    assert wt.workers.resolve_session_id_from_log(str(log)) == ""


def test_resolve_session_id_skips_bare_json_scalars(wt):
    """A worker can echo a bare JSON scalar (a quoted sentence) into its log.
    json.loads succeeds but yields a str -- it must be skipped, not crash the
    reconciler (which launchd then respawns into a crash loop)."""
    log = wt.tmp / "scalar.log"
    log.write_text(
        '"You have got 0 sessions left on your Private Class for now."\n'
        '42\n'
        'true\n'
        '{"type":"system","subtype":"init",'
        '"session_id":"86aa9848-9a2d-4bf7-8aa6-ce55b6e1ff61"}\n'
    )
    assert (wt.workers.resolve_session_id_from_log(str(log))
            == "86aa9848-9a2d-4bf7-8aa6-ce55b6e1ff61")


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


# --- kimi worker session-id recovery (session_<uuid> shape) -----------------


KIMI_SID = "session_019f83df-284b-7d20-9b72-4e60f3c9b535"


def _kimi_log(tmp_path, lines_before=200):
    """A kimi -p stream-json log: tool chatter, resume_hint only at the END."""
    log = tmp_path / "kimi-worker.log"
    body = "".join(
        '{"role":"assistant","content":"working step %d"}\n' % i
        for i in range(lines_before)
    )
    body += (
        '{"role":"meta","type":"session.resume_hint","session_id":"%s",'
        '"command":"kimi -r %s"}\n' % (KIMI_SID, KIMI_SID)
    )
    log.write_text(body)
    return log


def test_resolve_session_id_tail_scans_kimi_resume_hint(wt, tmp_path):
    """kimi's session id sits past the 80-line head window — the tail scan
    must still find it, in the prefixed session_<uuid> form."""
    log = _kimi_log(tmp_path)
    assert wt.workers.resolve_session_id_from_log(str(log)) == KIMI_SID


def test_resolve_session_id_ignores_quoted_resume_hint(wt, tmp_path):
    """A log that merely *quotes* a resume_hint inside another engine's
    content must not be misattributed."""
    log = tmp_path / "codex.log"
    log.write_text(
        'some preamble\nthinking: {"role":"meta","type":"session.resume_hint",'
        '"session_id":"%s"} trailing\n' % KIMI_SID
    )
    assert wt.workers.resolve_session_id_from_log(str(log)) == ""


def test_worker_session_ledger_accepts_kimi_prefixed_ids(wt):
    wt.workers._add_worker_session_id(KIMI_SID)
    wt.workers._add_worker_session_id("not-a-session-id")
    wt.workers._add_worker_session_id("session_also-not-a-uuid")
    assert wt.workers._load_worker_session_ledger() == [KIMI_SID]


def test_backfill_worker_session_ledger_recovers_kimi(wt, tmp_path, monkeypatch):
    """Log-tail path: a pruned kimi worker's id joins the ledger via the log."""
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    monkeypatch.setattr(wt.workers, "WORKERS_FILE", tmp_path / "workers.json")
    _kimi_log(log_dir)
    added = wt.workers.backfill_worker_session_ledger()
    assert added == [KIMI_SID]
    assert KIMI_SID in wt.workers._load_worker_session_ledger()


def test_kimi_worker_session_ids_from_kimi_store(wt, tmp_path, monkeypatch):
    """Kimi-store path: a worker that died before its resume_hint (quota
    failure) is recovered from the drain-goal prompt in its own wire.jsonl."""
    worker_id = "bym-deep-fixes-e4b58d49"
    wt.workers._add_worker_id(worker_id)
    wire = (
        tmp_path / "kimi-home" / "sessions" / "wd_repo" / KIMI_SID
        / "agents" / "main" / "wire.jsonl"
    )
    wire.parent.mkdir(parents=True)
    wire.write_text(
        '{"type":"metadata"}\n'
        '{"type":"turn.prompt","input":[{"type":"text","text":"Drain the '
        'BYM-DEEP-FIXES WatchTower queue. Your worker id is %s. FIRST, read '
        'the learnings file."}]}\n' % worker_id
    )
    monkeypatch.setenv("KIMI_CODE_HOME", str(tmp_path / "kimi-home"))
    assert wt.workers._kimi_worker_session_ids(0) == [KIMI_SID]
    # A session quoting the phrase with an unknown worker id is ignored.
    wire.write_text(
        '{"type":"turn.prompt","input":[{"type":"text","text":"Your worker id '
        'is not-a-real-worker."}]}\n'
    )
    assert wt.workers._kimi_worker_session_ids(0) == []


def test_coerce_session_uuid_preserves_kimi_prefix(wt):
    assert wt.q._coerce_session_uuid(KIMI_SID) == KIMI_SID
    bare = "019f83df-284b-7d20-9b72-4e60f3c9b535"
    assert wt.q._coerce_session_uuid(bare) == bare
    # Embedded bare UUID in prose still extracts (claude/codex path).
    assert wt.q._coerce_session_uuid("session is " + bare) == bare


# ===================================================== zombie-worker escape hatch
def _set_worker_started_at(wt, rec, iso):
    """Backdate a worker's started_at in workers.json."""
    with wt.workers._WorkersFileLock():
        data = wt.workers._load()
        for row in data["workers"]:
            if row.get("worker_id") == rec["worker_id"]:
                row["started_at"] = iso
        wt.workers._save(data)


def _make_queue_stuck(wt, queue):
    """Enqueue a ticket and backdate it so the queue reads stuck."""
    item = wt.q.enqueue(project=queue, note="stuck work")
    data = wt.q._load_unlocked()
    for it in data["items"]:
        if it["ref"] == item["ref"]:
            it["created_at"] = "2000-01-01T00:00:00Z"
    wt.q._save_unlocked(data)
    return item


def test_release_zombie_detects_unproductive_worker(wt):
    wt.config.set_auto_drain("Q", True)
    _make_queue_stuck(wt, "Q")
    rec = _live_worker(wt, "Q")
    _set_worker_started_at(wt, rec, "2000-01-01T00:00:00Z")

    released = wt.workers.release_zombie_workers(queue="Q")

    assert [w["worker_id"] for w in released] == [rec["worker_id"]]
    assert (wt.workers.STOP_SIGNALS_DIR / rec["worker_id"]).exists()
    assert len(_activity_lines(wt, "ZOMBIE_RELEASE")) == 1


def test_release_zombie_skips_worker_holding_ticket(wt):
    wt.config.set_auto_drain("Q", True)
    _make_queue_stuck(wt, "Q")
    rec = _live_worker(wt, "Q")
    _set_worker_started_at(wt, rec, "2000-01-01T00:00:00Z")
    wt.q.claim_by_ref(wt.q.list_items(project="Q")[0]["ref"], rec["worker_id"])

    assert wt.workers.release_zombie_workers(queue="Q") == []


def test_release_zombie_skips_unstuck_queue(wt):
    wt.config.set_auto_drain("Q", True)
    # Fresh ticket -> queue is not stuck.
    wt.q.enqueue(project="Q", note="fresh work")
    rec = _live_worker(wt, "Q")
    _set_worker_started_at(wt, rec, "2000-01-01T00:00:00Z")

    assert wt.workers.release_zombie_workers(queue="Q") == []


def test_release_zombie_skips_fresh_worker(wt):
    wt.config.set_auto_drain("Q", True)
    _make_queue_stuck(wt, "Q")
    rec = _live_worker(wt, "Q")
    # started_at is "now" by default, so the worker is too fresh to be a zombie.

    assert wt.workers.release_zombie_workers(queue="Q") == []


def test_release_zombie_records_launch_failure_on_model_errors(wt):
    wt.config.set_auto_drain("Q", True)
    _make_queue_stuck(wt, "Q")
    rec = _live_worker(wt, "Q")
    _set_worker_started_at(wt, rec, "2000-01-01T00:00:00Z")
    log = Path(rec["log"])
    log.write_text(
        "init\n"
        '{"error":"model_not_found"}\n'
        '{"error":"model_not_found"}\n'
        '{"error":"model_not_found"}\n'
    )

    released = wt.workers.release_zombie_workers(queue="Q")

    assert [w["worker_id"] for w in released] == [rec["worker_id"]]
    assert len(_activity_lines(wt, "ZOMBIE_RELEASE")) == 1
    cooldown = wt.workers.active_launch_failure_cooldown("Q", "claude")
    assert cooldown is not None
    assert "model_not_found" in cooldown["reason"]


def test_release_zombie_ignores_worker_that_closed_ticket(wt):
    wt.config.set_auto_drain("Q", True)
    _make_queue_stuck(wt, "Q")
    rec = _live_worker(wt, "Q")
    _set_worker_started_at(wt, rec, "2000-01-01T00:00:00Z")
    # Manually close a ticket by this worker after its start time.
    item = wt.q.enqueue(project="Q", note="closed work")
    wt.q.close(item["ref"], session_id=rec["worker_id"], resolution={"summary": "done"})

    assert wt.workers.release_zombie_workers(queue="Q") == []


def test_reconcile_logs_zombie_release(wt):
    wt.config.set_auto_drain("Q", True)
    wt.config.set_desired_workers("Q", 1)
    _make_queue_stuck(wt, "Q")
    rec = _live_worker(wt, "Q")
    _set_worker_started_at(wt, rec, "2000-01-01T00:00:00Z")

    r = wt.workers.reconcile_once(dry_run=False)

    assert any(w["worker_id"] == rec["worker_id"] for w in r["zombies_released"])
    zombie_logs = _activity_lines(wt, "ZOMBIE_RELEASE")
    assert len(zombie_logs) == 1
    assert rec["worker_id"] in zombie_logs[0]
    assert wt.q._coerce_session_uuid("garbage") is None

#!/usr/bin/env python3
"""Spawn and track WatchTower workers.

A *worker* is a subprocess running an agent CLI (``claude`` or ``codex``) with a
canonical drain goal: claim the oldest open ticket in a queue, do the work,
close it, repeat until the queue is empty. The worker claims under its own id so
the queue's progress signal (closes) reflects real draining.

We track the workers THIS WatchTower CLI started in ``~/.watchtower/workers.json``
so ``wt workers`` can report PID + queue. Liveness here is process-level
(``os.kill(pid, 0)``) — distinct from the queue's stuck signal, which is pure
queue ground-truth.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

WORKERS_FILE = Path(
    os.environ.get("WATCHTOWER_WORKERS_FILE")
    or (Path.home() / ".watchtower" / "workers.json")
)

STOP_SIGNALS_DIR = Path(
    os.environ.get("WATCHTOWER_STOP_SIGNALS_DIR")
    or (Path.home() / ".watchtower" / "stop-signals")
)

# Drain goal adapted from CCC's docs/ux-fixes-worker-brief.md canonical /goal.
# Generalized: no CCC paths, no shared-clone assumptions. The worker drains one
# queue via the `wt` CLI it was spawned by and idles when empty.
DRAIN_GOAL_TEMPLATE = (
    "Drain the {queue} WatchTower queue and keep it empty. "
    "Work in the git repo at {repo}. "
    "Your worker id is {worker_id}. Loop: claim the oldest open ticket with "
    "`wt claim -q {queue} --worker {worker_id} --json` (it returns the ticket "
    "JSON, nothing when the queue is drained, or {{\"stop\": true}} when the "
    "reconciler is winding you down). "
    "STOP SIGNAL: if `wt claim` returns {{\"stop\": true}}, exit immediately -- "
    "the reconciler has determined no worker is needed for this queue right now. "
    "Do not claim another ticket; just exit. "
    "Read the ticket's note/text and, if present, open its screenshot_path and "
    "resolve its selector. Make the change in the relevant repo and verify it. "
    "Commit only the paths you changed (never `git add -A`/`.`/`-a`). "
    "RESOLUTION IS MANDATORY: NEVER close a ticket without `--summary`. "
    "When you close a ticket: `wt close <ref> --worker {worker_id} --summary "
    "\"what you changed\"`. Add `--caveat \"...\"` for anything to watch out "
    "for, `--follow-up \"...\"` for notable next steps, and `--unresolved "
    "\"...\"` for anything you could not fix (each flag is repeatable). This "
    "resolution is the trust signal the dashboard surfaces; a close without "
    "--summary will be rejected with exit code 1. "
    "If a ticket genuinely cannot be resolved without a human decision, do NOT "
    "close it and do NOT guess: run `wt block <ref> --worker {worker_id} "
    "--question \"the specific decision you need\" --progress \"what you've "
    "figured out so far\"`, then move on to the next ticket. "
    "IDLE: when `wt claim` reports the queue is drained, STOP and simply end "
    "your turn. Do NOT poll, do NOT sleep-loop, do NOT exit the process on your "
    "own. Your stdin is a live input channel: when a new ticket is filed, a "
    "fresh instruction message arrives and you resume automatically with your "
    "full warm context. Ending your turn on an empty queue is correct -- the "
    "next message wakes you. Do not push unless explicitly asked."
)

_ENGINE_BIN = {"claude": "claude", "codex": "codex"}


# ── stream-json FIFO input channel ──────────────────────────────────────────
# A claude worker is spawned as `claude -p --input-format stream-json` reading
# its stdin from a named pipe (FIFO). That makes the worker a *live, pushable*
# process: `wt add` writes a stream-json user message to the FIFO and the
# running worker picks it up at its next turn boundary -- no polling, no
# sleep-loop, warm context preserved. Mirrors CCC's proven spawn-fifo path.

def _make_stdin_fifo(log_path: Path):
    """Create a named pipe next to the worker log and open it O_RDWR.

    The O_RDWR open is the keep-alive trick: the child inherits this fd as its
    stdin, so the kernel always counts >=1 writer while the child lives -- no
    EOF, no premature exit -- even after every external writer (the spawning
    CLI) closes. Returns (fifo_path, rdwr_fd) or (None, None) on failure.
    """
    try:
        fifo_path = Path(str(log_path) + ".stdin")
        if fifo_path.exists():
            try:
                fifo_path.unlink()
            except OSError:
                pass
        os.mkfifo(str(fifo_path), 0o600)
        fd = os.open(str(fifo_path), os.O_RDWR | os.O_CLOEXEC)
        return str(fifo_path), fd
    except OSError:
        return None, None


def _open_fifo_writer(fifo_path: str):
    """Open a FIFO write-only, non-blocking. Returns fd, or None.

    Fails with None when no reader is attached (ENXIO) -- which is exactly how
    we detect a worker that is no longer listening (dead/exited)."""
    if not fifo_path:
        return None
    try:
        return os.open(fifo_path, os.O_WRONLY | os.O_NONBLOCK | os.O_CLOEXEC)
    except OSError:
        return None


def _close_fd_quiet(fd) -> None:
    if fd is None:
        return
    try:
        os.close(fd)
    except OSError:
        pass


def _stream_json_user_line(text: str) -> bytes:
    msg = {
        "type": "user",
        "message": {"role": "user", "content": [{"type": "text", "text": text}]},
    }
    return (json.dumps(msg) + "\n").encode("utf-8")


def write_to_worker_fifo(fifo_path: str, text: str) -> bool:
    """Push a stream-json user message to a live worker's FIFO. Returns True on
    delivery, False if the worker isn't listening (no reader / closed)."""
    fd = _open_fifo_writer(fifo_path)
    if fd is None:
        return False
    try:
        os.write(fd, _stream_json_user_line(text))
        return True
    except OSError:
        return False
    finally:
        _close_fd_quiet(fd)


def notify_workers(queue: str, text: str) -> int:
    """Push `text` to every live worker on `queue` via its FIFO.

    Returns the number of workers that accepted the message. A return of 0 means
    no live worker is listening -- the caller should reconcile/spawn one."""
    n = 0
    for w in list_workers():
        if not w.get("alive"):
            continue
        if w.get("queue") != queue:
            continue
        fifo = w.get("fifo")
        if fifo and write_to_worker_fifo(fifo, text):
            n += 1
    return n


def _load() -> Dict[str, Any]:
    try:
        with open(WORKERS_FILE, "r") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {"workers": []}
    if not isinstance(data, dict) or not isinstance(data.get("workers"), list):
        return {"workers": []}
    return data


def _save(data: Dict[str, Any]) -> None:
    WORKERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = str(WORKERS_FILE) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, WORKERS_FILE)


def _pid_alive(pid: int) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True  # exists, owned by someone else
    except OSError:
        return False
    return True


def record_worker(
    pid: int,
    queue: str,
    engine: str,
    worker_id: str,
    repo_path: str = "",
    log: str = "",
    fifo: str = "",
) -> Dict[str, Any]:
    data = _load()
    rec = {
        "worker_id": worker_id,
        "pid": int(pid),
        "queue": queue,
        "engine": engine,
        "repo_path": repo_path,
        "log": log,
        "fifo": fifo,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    data["workers"].append(rec)
    _save(data)
    return rec


def list_workers(prune: bool = True) -> List[Dict[str, Any]]:
    """Return tracked workers, each annotated with a live flag.

    When ``prune`` is set, dead workers are dropped from the store.
    """
    data = _load()
    out: List[Dict[str, Any]] = []
    kept: List[Dict[str, Any]] = []
    for w in data["workers"]:
        alive = _pid_alive(int(w.get("pid", 0)))
        row = dict(w)
        row["alive"] = alive
        out.append(row)
        if alive:
            kept.append(w)
        else:
            # Dead worker: unlink its FIFO node so it doesn't linger on disk.
            fifo = w.get("fifo")
            if fifo:
                try:
                    Path(fifo).unlink()
                except OSError:
                    pass
    if prune and len(kept) != len(data["workers"]):
        _save({"workers": kept})
    return out


def live_worker_count(queue: Optional[str] = None) -> int:
    n = 0
    for w in list_workers():
        if not w.get("alive"):
            continue
        if queue and w.get("queue") != queue:
            continue
        n += 1
    return n


def worker_counts(prune: bool = False) -> Dict[str, Dict[str, int]]:
    """Per-queue worker tally: ``{queue: {"total": n, "live": n}}``.

    A single pass over the tracked workers so callers (``wt status``, the
    dashboard) don't fan out one liveness probe per queue.
    """
    out: Dict[str, Dict[str, int]] = {}
    for w in list_workers(prune=prune):
        row = out.setdefault(w.get("queue", ""), {"total": 0, "live": 0})
        row["total"] += 1
        if w.get("alive"):
            row["live"] += 1
    return out


def _age_human(claimed_at: Optional[str]) -> Optional[str]:
    """Compact age string for how long ago a ticket was claimed (e.g. '4m')."""
    if not claimed_at:
        return None
    try:
        dt = datetime.strptime(claimed_at, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except (ValueError, TypeError):
        return None
    secs = max(0, int((datetime.now(timezone.utc) - dt).total_seconds()))
    if secs < 60:
        return f"{secs}s"
    mins = secs // 60
    if mins < 60:
        return f"{mins}m"
    hours = mins // 60
    if hours < 24:
        return f"{hours}h{mins % 60:02d}m"
    days = hours // 24
    return f"{days}d{hours % 24:02d}h"


def annotate_activity(
    rows: List[Dict[str, Any]], items: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Join each worker row to the in-progress ticket it currently holds.

    One pass over ``items`` builds the lookup (by ``claimed_session_id`` and by
    ``claimed_by``), then one pass over ``rows`` attaches ``active_ref`` /
    ``active_since`` / ``active_since_human`` (None when the worker is idle).
    O(items + workers), consistent with ``worker_counts``.
    """
    by_key: Dict[str, Dict[str, Any]] = {}
    for it in items:
        if it.get("status") != "in_progress":
            continue
        for key in (it.get("claimed_session_id"), it.get("claimed_by")):
            if key:
                by_key.setdefault(str(key), it)
    for row in rows:
        wid = str(row.get("worker_id", ""))
        active = by_key.get(wid)
        if active:
            row["active_ref"] = active.get("ref")
            row["active_since"] = active.get("claimed_at")
            row["active_since_human"] = _age_human(active.get("claimed_at"))
        else:
            row["active_ref"] = None
            row["active_since"] = None
            row["active_since_human"] = None
    return rows


def drain_goal(queue: str, worker_id: str, repo_path: str = "") -> str:
    """The canonical drain goal text for one worker."""
    return DRAIN_GOAL_TEMPLATE.format(
        queue=queue, worker_id=worker_id, repo=repo_path or os.getcwd()
    )


def build_drain_command(
    queue: str, engine: str, worker_id: str, repo_path: str = ""
) -> List[str]:
    """Construct the argv for one worker subprocess.

    **claude**: spawned in stream-json mode -- ``claude -p --input-format
    stream-json --output-format stream-json`` -- reading its stdin from a FIFO
    (see ``spawn_workers``). The goal is NOT in argv; it is delivered as the
    first stream-json user message on the FIFO, and subsequent ``wt add``
    notifications arrive on the same channel. This is what makes a claude worker
    a live, pushable process instead of a poll-and-sleep loop.

    **codex**: one-shot ``codex exec <goal>`` (no stream-json input channel);
    the goal carries in argv and the worker drains until its turn ends.
    """
    bin_name = _ENGINE_BIN.get(engine, engine)
    if engine == "codex":
        return [bin_name, "exec", drain_goal(queue, worker_id, repo_path)]
    return [
        bin_name, "-p",
        "--input-format", "stream-json",
        "--output-format", "stream-json",
        "--verbose",
        "--permission-mode", "bypassPermissions",
    ]


def request_stop(worker_id: str) -> Path:
    """Ask a running worker to stop by dropping a sentinel file.

    The worker's next ``wt claim`` call will detect the file, delete it, and
    return ``{"stop": True}`` so the worker exits cleanly instead of being
    killed. Uses the file-system only -- does NOT touch workers.json so the
    record stays visible until the worker process dies and is pruned.
    """
    stop_dir = STOP_SIGNALS_DIR
    stop_dir.mkdir(parents=True, exist_ok=True)
    signal_path = stop_dir / worker_id
    signal_path.touch()
    return signal_path


def reconcile_once(dry_run: bool = False) -> Dict[str, Any]:
    """One reconciler tick.

    Loads the registry + live workers + queue depths and for each registered
    queue decides:
      - desired = desired_workers (default 1) if auto_drain AND depth > 0 else 0
      - if actual_live < desired: spawn the delta (or record in dry_run)
      - if actual_live > desired: call request_stop() on the excess workers

    Returns ``{"spawned": [...], "stopped": [...], "skipped": [...]}``.
    ``skipped`` entries explain why a queue was left alone (e.g. auto_drain=off
    or depth=0).  In ``dry_run`` mode no subprocesses are started and no
    stop-signal files are created; the return value shows what *would* happen.
    """
    from . import config, health

    # One-time import of legacy queue-registry.json (no-op after first run).
    try:
        config.migrate_from_registry()
    except Exception:
        pass

    result: Dict[str, Any] = {"spawned": [], "stopped": [], "skipped": []}

    all_cfg = config.all_queues()
    # Build live-worker counts keyed by queue.
    live_by_queue: Dict[str, List[Dict[str, Any]]] = {}
    for w in list_workers(prune=False):
        if w.get("alive"):
            q_name = w.get("queue", "")
            live_by_queue.setdefault(q_name, []).append(w)

    # Use health for queue depth -- one call covers all queues.
    depth_by_queue: Dict[str, int] = {}
    for row in health.all_status():
        depth_by_queue[row["queue"]] = row.get("depth", 0)

    for q_name in all_cfg:
        auto = config.auto_drain(q_name)
        desired = config.desired_workers(q_name) if auto else 0
        depth = depth_by_queue.get(q_name, 0)
        if not auto:
            result["skipped"].append({"queue": q_name, "reason": "auto_drain=off"})
            continue
        if depth == 0:
            result["skipped"].append({"queue": q_name, "reason": "depth=0"})
            # If there are surplus workers idling on an empty queue, wind them down.
            live = live_by_queue.get(q_name, [])
            for w in live:
                wid = w.get("worker_id", "")
                if not dry_run:
                    request_stop(wid)
                result["stopped"].append({"queue": q_name, "worker_id": wid,
                                          "dry_run": dry_run})
            continue
        live = live_by_queue.get(q_name, [])
        actual = len(live)
        if actual < desired:
            to_spawn = desired - actual
            from . import queue as _q
            # Peek at the next ticket to get its repo_path; fall back to queue config.
            peeked = _q.peek_next(project=q_name)
            repo_path = (
                config.repo_path(q_name)
                or (peeked or {}).get("repo_path", "")
            )
            engine = config.engine(q_name)
            spawned = spawn_workers(
                q_name, n=to_spawn, engine=engine,
                repo_path=repo_path, dry_run=dry_run,
            )
            result["spawned"].extend(spawned)
        elif actual > desired:
            # Wind down excess workers (LIFO -- stop the most recently started).
            excess = live[desired:]
            for w in excess:
                wid = w.get("worker_id", "")
                if not dry_run:
                    request_stop(wid)
                result["stopped"].append({"queue": q_name, "worker_id": wid,
                                          "dry_run": dry_run})
        else:
            result["skipped"].append(
                {"queue": q_name, "reason": f"actual={actual}==desired={desired}"}
            )

    return result


def spawn_workers(
    queue: str,
    n: int = 1,
    engine: str = "claude",
    *,
    repo_path: str = "",
    dry_run: bool = False,
) -> List[Dict[str, Any]]:
    """Launch ``n`` worker subprocesses draining ``queue``.

    ``repo_path`` is the tree the workers operate in (defaults to the current
    working directory) — it becomes the subprocess ``cwd`` and is injected into
    the drain goal. Each worker's stdout+stderr go to
    ``~/.watchtower/logs/<worker_id>.log`` so a dead worker leaves a trail
    instead of vanishing into ``/dev/null``. Returns records (with ``pid``).
    ``dry_run`` builds + records the command without spawning (tests).
    """
    repo_path = repo_path or os.getcwd()
    log_dir = WORKERS_FILE.parent / "logs"
    spawned: List[Dict[str, Any]] = []
    for _ in range(max(1, n)):
        worker_id = f"{queue.lower()}-{uuid.uuid4().hex[:8]}"
        argv = build_drain_command(queue, engine, worker_id, repo_path)
        goal = drain_goal(queue, worker_id, repo_path)
        if dry_run:
            spawned.append(
                {
                    "worker_id": worker_id,
                    "pid": 0,
                    "queue": queue,
                    "engine": engine,
                    "repo_path": repo_path,
                    "argv": argv,
                    "dry_run": True,
                }
            )
            continue
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"{worker_id}.log"
        logf = open(log_path, "ab")
        # claude workers get a stream-json FIFO stdin so they stay live and
        # pushable; codex (one-shot exec) keeps the goal in argv + DEVNULL.
        fifo_path = None
        child_stdin_fd = None
        if engine != "codex":
            fifo_path, child_stdin_fd = _make_stdin_fifo(log_path)
        stdin_arg = child_stdin_fd if child_stdin_fd is not None else subprocess.DEVNULL
        try:
            proc = subprocess.Popen(
                argv,
                stdin=stdin_arg,
                stdout=logf,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                cwd=repo_path,
            )
        finally:
            logf.close()  # the child holds its own dup'd fd
            # The child inherited a dup of the RDWR fd as its stdin; the parent's
            # copy is no longer needed (the child's keeps the FIFO from EOF).
            _close_fd_quiet(child_stdin_fd)
        # Deliver the drain goal as the first stream-json user message. The
        # child's inherited RDWR fd is already a reader, so this open + write
        # succeeds immediately and never blocks.
        if fifo_path is not None:
            if not write_to_worker_fifo(fifo_path, goal):
                # FIFO write failed -> the worker never got its task. Kill it so
                # we don't leave a stuck, goal-less process behind.
                try:
                    os.kill(proc.pid, 15)
                except OSError:
                    pass
                _close_fd_quiet(None)
        rec = record_worker(
            proc.pid, queue, engine, worker_id, repo_path, str(log_path),
            fifo=fifo_path or "",
        )
        rec["argv"] = argv
        spawned.append(rec)
    return spawned

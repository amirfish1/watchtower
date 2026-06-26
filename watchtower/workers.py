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
from pathlib import Path
from typing import Any, Dict, List, Optional

WORKERS_FILE = Path(
    os.environ.get("WATCHTOWER_WORKERS_FILE")
    or (Path.home() / ".watchtower" / "workers.json")
)

# Drain goal adapted from CCC's docs/ux-fixes-worker-brief.md canonical /goal.
# Generalized: no CCC paths, no shared-clone assumptions. The worker drains one
# queue via the `wt` CLI it was spawned by and idles when empty.
DRAIN_GOAL_TEMPLATE = (
    "/goal Drain the {queue}-* WatchTower queue and keep it empty. "
    "Your worker id is {worker_id}. Loop: claim the oldest open ticket with "
    "`wt claim -q {queue} --worker {worker_id}` (it returns the ticket JSON, "
    "or nothing when the queue is drained). Read the ticket's note/text and, "
    "if present, open its screenshot_path and resolve its selector. Make the "
    "change in the relevant repo and verify it. Commit only the paths you "
    "changed (never `git add -A`/`.`/`-a`). Close the ticket with "
    "`wt close <ref> --worker {worker_id}`, then claim the next one. When "
    "nothing is open, idle and re-poll later — never busy-wait. Do not push "
    "unless explicitly asked."
)

_ENGINE_BIN = {"claude": "claude", "codex": "codex"}


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
    pid: int, queue: str, engine: str, worker_id: str
) -> Dict[str, Any]:
    data = _load()
    rec = {
        "worker_id": worker_id,
        "pid": int(pid),
        "queue": queue,
        "engine": engine,
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


def build_drain_command(queue: str, engine: str, worker_id: str) -> List[str]:
    """Construct the argv for one worker subprocess.

    The engine CLI is invoked in non-interactive mode with the drain goal as the
    prompt. Exact flags vary per engine; we pass the goal as a single prompt
    argument, which both ``claude`` and ``codex`` accept positionally.
    """
    bin_name = _ENGINE_BIN.get(engine, engine)
    goal = DRAIN_GOAL_TEMPLATE.format(queue=queue, worker_id=worker_id)
    return [bin_name, goal]


def spawn_workers(
    queue: str,
    n: int = 1,
    engine: str = "claude",
    *,
    dry_run: bool = False,
) -> List[Dict[str, Any]]:
    """Launch ``n`` worker subprocesses draining ``queue``.

    Returns a list of records (with ``pid``). ``dry_run`` builds and records the
    command without actually spawning — used by tests so no real engine runs.
    """
    spawned: List[Dict[str, Any]] = []
    for _ in range(max(1, n)):
        worker_id = f"{queue.lower()}-{uuid.uuid4().hex[:8]}"
        argv = build_drain_command(queue, engine, worker_id)
        if dry_run:
            spawned.append(
                {
                    "worker_id": worker_id,
                    "pid": 0,
                    "queue": queue,
                    "engine": engine,
                    "argv": argv,
                    "dry_run": True,
                }
            )
            continue
        proc = subprocess.Popen(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        rec = record_worker(proc.pid, queue, engine, worker_id)
        rec["argv"] = argv
        spawned.append(rec)
    return spawned

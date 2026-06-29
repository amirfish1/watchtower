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
import re
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

# Persistent, append-only ledger of every cloud session_id a WatchTower worker
# has ever used. workers.json PRUNES dead workers, so once a worker is reaped its
# session_id is lost from that store. CCC needs the historical set to hide *past*
# worker sessions from its Current-sessions list (a reaped worker leaves a dead
# session behind). This ledger survives pruning. Shape: {"session_ids": [...]}.
WORKER_SESSIONS_FILE = Path(
    os.environ.get("WATCHTOWER_WORKER_SESSIONS_FILE")
    or (Path.home() / ".watchtower" / "worker-sessions.json")
)

# Cap to avoid unbounded growth; oldest entries are dropped first.
_WORKER_SESSIONS_CAP = 500

# Drain goal adapted from CCC's docs/ux-fixes-worker-brief.md canonical /goal.
# Generalized: no CCC paths, no shared-clone assumptions. The worker drains one
# queue via the `wt` CLI it was spawned by and idles when empty.
DRAIN_GOAL_TEMPLATE = (
    "Drain the {queue} WatchTower queue and keep it empty. "
    "Work in the git repo at {repo}. "
    "Your worker id is {worker_id}. "
    "FIRST, read the queue's learnings file at "
    "~/.watchtower/learnings/{queue}.md if it exists -- it is accumulated wisdom "
    "from prior workers on THIS queue (infra quirks, recurring ticket patterns, "
    "env gotchas, where the runbook is). Treat it as your cold-start brief. "
    "Loop: claim the oldest open ticket with "
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
    "IDLE: when `wt claim` reports the queue is drained, FIRST update the "
    "learnings file at ~/.watchtower/learnings/{queue}.md with anything the next "
    "worker should know from this session -- infra changes, recurring ticket "
    "patterns, gotchas, env quirks. EDIT it to stay concise (it is read cold by "
    "the next worker; keep it well under ~60 lines) -- do not append "
    "unboundedly. (Do this now, at drain-completion -- not later: a cold worker "
    "gets killed while idle and cannot write then.) THEN STOP and simply end "
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


# Anthropic prompt-cache TTL. A worker idle LONGER than this has lost its
# context cache, so waking it would pay a full uncached re-read of its entire
# (by-then bloated) accumulated context -- the worst case. Past this window we
# retire it and spawn a FRESH worker instead (cold but tiny context = cheaper).
WARM_TTL_S = 300


def _worker_idle_s(w: Dict[str, Any]) -> float:
    """Seconds since this worker last did anything.

    The stream-json output log is written on every turn, so its mtime is the
    worker's last-activity clock. Falls back to ``started_at`` if the log is
    missing. Returns a large number when nothing is resolvable (treat as cold)."""
    log = w.get("log")
    if log:
        try:
            return max(0.0, time.time() - os.path.getmtime(log))
        except OSError:
            pass
    started = w.get("started_at")
    if started:
        try:
            dt = datetime.strptime(started, "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=timezone.utc
            )
            return max(0.0, (datetime.now(timezone.utc) - dt).total_seconds())
        except (ValueError, TypeError):
            pass
    return float("inf")


def notify_workers(queue: str, text: str, max_idle_s: Optional[float] = None) -> int:
    """Push `text` to live workers on `queue` via their FIFO.

    ``max_idle_s`` (default ``WARM_TTL_S`` when None) skips workers idle longer
    than the prompt-cache TTL: pushing to a cold worker is the worst case, so
    the caller should reap+respawn instead. Returns the number of WARM workers
    that accepted the message; 0 means "no warm worker -- reap stale + spawn"."""
    if max_idle_s is None:
        max_idle_s = WARM_TTL_S
    n = 0
    for w in list_workers():
        if not w.get("alive"):
            continue
        if w.get("queue") != queue:
            continue
        if _worker_idle_s(w) >= max_idle_s:
            continue  # cold cache -- do not wake; let the caller spawn fresh
        fifo = w.get("fifo")
        if fifo and write_to_worker_fifo(fifo, text):
            n += 1
    return n


def reap_stale_workers(max_idle_s: float = WARM_TTL_S,
                       queue: Optional[str] = None) -> List[Dict[str, Any]]:
    """Kill live workers idle past ``max_idle_s`` (cold cache).

    An idle worker has ended its turn and is blocked reading its FIFO -- nothing
    is in flight, so SIGTERM is safe. Killing it frees the queue so the next
    add/reconcile spawns a fresh, small-context worker instead of waking a cold,
    bloated one. Returns the records reaped (after pruning + FIFO cleanup)."""
    reaped: List[Dict[str, Any]] = []
    pids: List[int] = []
    for w in list_workers(prune=False):
        if not w.get("alive"):
            continue
        if queue and w.get("queue") != queue:
            continue
        if _worker_idle_s(w) < max_idle_s:
            continue
        pid = int(w.get("pid", 0) or 0)
        if pid:
            try:
                os.kill(pid, 15)
                reaped.append(w)
                pids.append(pid)
            except OSError:
                pass
    # SIGTERM is async -- the process keeps answering os.kill(pid, 0) until it
    # actually exits. Wait for death (escalating to SIGKILL) BEFORE returning so
    # a caller that reconciles next sees the slot truly free and spawns fresh.
    # Without this wait, reconcile counts the dying worker as live and skips the
    # respawn, stranding the ticket until the next daemon tick.
    deadline = time.time() + 3.0
    while pids and time.time() < deadline:
        pids = [p for p in pids if _pid_alive(p)]
        if not pids:
            break
        time.sleep(0.05)
    for p in pids:  # stubborn ones: SIGKILL
        try:
            os.kill(p, 9)
        except OSError:
            pass
    if reaped:
        list_workers(prune=True)  # drop the dead records + unlink their FIFOs
    return reaped


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


def _load_worker_session_ledger() -> List[str]:
    """Return the de-duped, ordered list of worker session_ids from the ledger.

    Empty list on a missing/unreadable/malformed file. No exceptions escape."""
    try:
        with open(WORKER_SESSIONS_FILE, "r") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(data, dict):
        return []
    ids = data.get("session_ids")
    if not isinstance(ids, list):
        return []
    return [str(s) for s in ids if isinstance(s, str)]


def _add_worker_session_id(sid: str) -> None:
    """Append a worker session_id to the persistent ledger if not already present.

    Cheap: only writes when a genuinely new id is added. Caps the list to the
    most recent ``_WORKER_SESSIONS_CAP`` ids (drops oldest). Atomic tmp+replace,
    mirroring ``_save``. Silently no-ops on a falsy/non-UUID id or write error."""
    if not sid or not _SESSION_ID_RE.fullmatch(str(sid)):
        return
    sid = str(sid)
    ids = _load_worker_session_ledger()
    if sid in ids:
        return
    ids.append(sid)
    if len(ids) > _WORKER_SESSIONS_CAP:
        ids = ids[-_WORKER_SESSIONS_CAP:]
    try:
        WORKER_SESSIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = str(WORKER_SESSIONS_FILE) + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"session_ids": ids}, f, indent=2)
        os.replace(tmp, WORKER_SESSIONS_FILE)
    except OSError:
        pass


_SESSION_ID_RE = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)


def resolve_session_id_from_log(log_path: str) -> str:
    """Extract the cloud-assigned session UUID from a worker's stream-json log.

    A ``claude -p --output-format stream-json`` worker emits an init/system event
    carrying ``session_id`` (the UUID Claude/cloud assigns -- WatchTower does NOT
    mint it). We scan the first lines of the captured output log for it. Returns
    "" until the event has been written (the worker must have started its first
    turn). Mirrors CCC's extract_session_id."""
    if not log_path:
        return ""
    try:
        with open(log_path, "r") as f:
            for i, line in enumerate(f):
                if i >= 80:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    ev = json.loads(line)
                except (json.JSONDecodeError, ValueError):
                    continue
                sid = ev.get("session_id") or ev.get("sessionId")
                if sid and _SESSION_ID_RE.fullmatch(str(sid)):
                    return str(sid)
    except (OSError, UnicodeDecodeError):
        pass
    return ""


def _pid_alive(pid: int) -> bool:
    """Check if a process is truly alive (not a zombie).

    os.kill(pid, 0) succeeds on zombies, so we check /proc (or ps on macOS)
    to distinguish live from zombie processes.
    """
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

    # Process exists, but check if it's a zombie (defunct).
    # On macOS, use ps; on Linux, check /proc.
    try:
        import subprocess
        result = subprocess.run(
            ["ps", "-p", str(pid), "-o", "stat="],
            capture_output=True,
            text=True,
            timeout=2,
        )
        stat = result.stdout.strip()
        # Z or Z+ indicates zombie; anything else is truly alive.
        return not stat.startswith("Z")
    except Exception:
        # If ps fails, assume the process is alive (safer than killing live processes).
        return True


def record_worker(
    pid: int,
    queue: str,
    engine: str,
    worker_id: str,
    repo_path: str = "",
    log: str = "",
    fifo: str = "",
    session_id: str = "",
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
    if session_id:
        rec["session_id"] = session_id
    data["workers"].append(rec)
    _save(data)
    # If a session_id is somehow known at record time, ledger it (survives prune).
    _add_worker_session_id(rec.get("session_id", ""))
    return rec


def list_workers(prune: bool = True) -> List[Dict[str, Any]]:
    """Return tracked workers, each annotated with a live flag.

    When ``prune`` is set, dead workers are dropped from the store.
    """
    data = _load()
    out: List[Dict[str, Any]] = []
    kept: List[Dict[str, Any]] = []
    backfilled = False
    for w in data["workers"]:
        # Backfill the cloud session UUID from the worker's output log once it
        # appears (the worker has to start its first turn before the init event
        # is written). Persisted so CCC can resolve worker -> session and link
        # to its conversation. Parsed at most once per worker (skip if known).
        if not w.get("session_id") and w.get("log"):
            sid = resolve_session_id_from_log(w["log"])
            if sid:
                w["session_id"] = sid
                backfilled = True
                # Ledger it so it survives this worker being pruned later.
                _add_worker_session_id(sid)
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
        _save({"workers": kept})  # kept holds the same dicts -> backfill persists
    elif backfilled:
        _save(data)
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
        "--name", f"{queue} queue worker",
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
    import sys

    # One-time import of legacy queue-registry.json (no-op after first run).
    try:
        config.migrate_from_registry()
    except Exception:
        pass

    result: Dict[str, Any] = {"spawned": [], "stopped": [], "skipped": [], "reaped": []}

    # Reap cold idle workers first (idle past the prompt-cache TTL): waking one
    # would re-read its bloated context uncached. Killing it lets the spawn pass
    # below start a fresh, small-context worker instead. Skipped in dry_run.
    if not dry_run:
        try:
            result["reaped"] = [r.get("worker_id", "")
                                for r in reap_stale_workers()]
        except Exception:
            pass

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

    # Log the reconcile event
    import json
    import time
    try:
        log_path = WORKERS_FILE.parent / "reconcile.log"
        with open(log_path, "a") as f:
            event = {
                "ts": time.time(),
                "dry_run": dry_run,
                "spawned": len(result.get("spawned", [])),
                "stopped": len(result.get("stopped", [])),
                "reaped": len(result.get("reaped", [])),
                "skipped": result.get("skipped", []),
            }
            f.write(json.dumps(event) + "\n")
    except Exception:
        pass

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

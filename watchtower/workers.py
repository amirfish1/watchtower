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
    "`wt claim -q {queue} --worker {worker_id}{claim_filter} --json` (it returns the ticket "
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
    "patterns, gotchas, env quirks. The ~60-line cap is on the WHOLE FILE, not "
    "per-edit: read the current file before editing and prune, don't just "
    "append. Keep a 'Recent fixes' section (if any) to the last 2-3 entries, "
    "dropping the oldest when you add a new one -- old fixes are recoverable "
    "from `git log`/`wt close` summaries, so they aren't worth permanent space. "
    "Durable design reasoning (why something is structured a certain way, "
    "multi-paragraph gotchas) belongs in docs/*.md with just a one-line pointer "
    "left in the learnings file, not inlined in full. EDIT it to stay concise "
    "-- do not append unboundedly. (Do this now, at drain-completion -- not "
    "later: a cold worker gets killed while idle and cannot write then.) THEN "
    "STOP and simply end "
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


# Cooldown between stuck-queue nudges to the same queue. A queue's `stuck`
# flag stays true every reconcile tick until progress resumes, so without a
# cooldown a persistently stuck queue would get re-nudged (and re-write to
# every live worker's FIFO) once per tick. Matches WARM_TTL_S: no point
# nudging more often than a worker's prompt cache stays warm anyway.
_STUCK_NUDGE_COOLDOWN_S = WARM_TTL_S
_last_stuck_nudge: Dict[str, float] = {}


def _maybe_nudge_stuck_queue(queue: str, live_count: int) -> int:
    """Nudge live workers on a stuck-but-staffed queue to retry/continue.

    Rate-limited to once per ``_STUCK_NUDGE_COOLDOWN_S`` per queue. Returns
    the number of workers notified (0 if on cooldown or none warm)."""
    from .queue import _log
    now = time.time()
    if now - _last_stuck_nudge.get(queue, 0.0) < _STUCK_NUDGE_COOLDOWN_S:
        return 0
    _last_stuck_nudge[queue] = now
    nudge = (
        f"No ticket has closed on {queue} in a while despite {live_count} live "
        "worker(s) here. This can happen when a turn errors out on a transient "
        "API/connectivity fault and gets stuck instead of continuing. If your "
        f"last turn errored, retry now: `wt claim -q {queue} --worker <your-id> "
        "--json` and keep draining."
    )
    delivered = notify_workers(queue, nudge)
    _log("NUDGE", f"stuck queue — nudged {delivered}/{live_count} live worker(s)",
         queue=queue)
    return delivered


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
                idle_min = int(_worker_idle_s(w) / 60)
                ttl_min = int(max_idle_s / 60)
                w["_reap_reason"] = f"idle {idle_min}m (cold cache, >{ttl_min}m TTL)"
                # Drop any pending stop-signal — the worker is being killed, so the
                # sentinel would otherwise linger orphaned in the stop-signals dir.
                try:
                    (STOP_SIGNALS_DIR / str(w.get("worker_id", ""))).unlink()
                except OSError:
                    pass
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
_CODEX_SESSION_ID_LINE_RE = re.compile(
    r"^\s*session id:\s*("
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
    r")\s*$",
    re.IGNORECASE,
)


def resolve_session_id_from_log(log_path: str) -> str:
    """Extract the cloud-assigned session UUID from a worker's stream-json log.

    A ``claude -p --output-format stream-json`` worker emits an init/system event
    carrying ``session_id`` (the UUID Claude/cloud assigns -- WatchTower does NOT
    mint it). ``codex exec`` prints the same UUID as a plain ``session id:``
    startup line. We scan the first lines of the captured output log for either
    shape. Returns "" until the event has been written (the worker must have
    started its first turn)."""
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
                    m = _CODEX_SESSION_ID_LINE_RE.match(line)
                    if m:
                        return m.group(1)
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


def display_name(queue: str, ref: Optional[str] = None, summary: Optional[str] = None) -> str:
    """The canonical human-readable label for a worker at some point in its
    claim/close lifecycle, e.g. for a session title or a session-list UI.

    ``ref``/``summary`` absent -> never claimed anything: ``"<queue> worker"``.
    ``ref`` given, no ``summary`` -> holding it (or just claimed it):
    ``"<queue> worker: <ref>"``. Both given -> closed it:
    ``"<queue> worker: <ref> - <summary, clipped>"``.

    This is also fed to ``messages.set_session_title`` to actually rename the
    engine session (append a ``custom-title`` event to its transcript) -- see
    ``docs/session-naming.md`` for how that was verified against a live
    session and against CCC's own ``rename_session``."""
    queue = queue or "WT"
    if not ref:
        return f"{queue} worker"
    summary = (summary or "").strip()
    if summary:
        return f"{queue} worker: {ref} - {_clip(summary, 60)}"
    return f"{queue} worker: {ref}"


def ticket_context(item: Dict[str, Any], summary: str = "") -> str:
    """Short human context for a worker title.

    Close summaries are the best description of what happened. Before close,
    prefer the ticket title, then note, then text so the session is useful as
    soon as a worker claims work.
    """
    res = item.get("resolution") if isinstance(item, dict) else {}
    if isinstance(res, str) and not summary:
        summary = res
    elif isinstance(res, dict) and not summary:
        summary = str(res.get("summary") or "")
    for value in (summary, item.get("title"), item.get("note"), item.get("text")):
        text = str(value or "").strip()
        if text:
            return _clip(" ".join(text.split()), 60)
    return ""


def _display_name(
    row: Dict[str, Any],
    active: Optional[Dict[str, Any]],
    last_closed: Optional[Dict[str, Any]],
) -> str:
    """``display_name`` fed from a worker row + its most recent closed item."""
    if active:
        return display_name(row.get("queue", ""), active.get("ref"), ticket_context(active))
    if last_closed:
        return display_name(
            row.get("queue", ""),
            last_closed.get("ref"),
            ticket_context(last_closed),
        )
    return display_name(row.get("queue", ""))


def _clip(text: str, limit: int) -> str:
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def annotate_activity(
    rows: List[Dict[str, Any]], items: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Join each worker row to the ticket it currently (or most recently) held.

    One pass over ``items`` builds two lookups (by ``claimed_session_id`` and
    by ``claimed_by``): the current in-progress ticket, and -- for workers
    idle right now -- their most recently closed one (by ``closed_at``). A
    second pass over ``rows`` attaches ``active_ref`` / ``active_since`` /
    ``active_since_human`` (None when the worker is idle), ``last_closed_ref``
    / ``last_closed_summary`` (None when the worker has never closed a
    ticket), and ``display_name`` -- a friendlier label than the raw
    worker/session id, meant for session-list UIs (see ``_display_name``).
    O(items + workers), consistent with ``worker_counts``.
    """
    active_by_key: Dict[str, Dict[str, Any]] = {}
    last_closed_by_key: Dict[str, Dict[str, Any]] = {}
    for it in items:
        status = it.get("status")
        keys = [k for k in (it.get("claimed_session_id"), it.get("claimed_by")) if k]
        if status == "in_progress":
            for key in keys:
                active_by_key.setdefault(str(key), it)
        elif status == "closed":
            for key in keys:
                prior = last_closed_by_key.get(str(key))
                if prior is None or (it.get("closed_at") or "") > (prior.get("closed_at") or ""):
                    last_closed_by_key[str(key)] = it
    for row in rows:
        wid = str(row.get("worker_id", ""))
        active = active_by_key.get(wid)
        if active:
            row["active_ref"] = active.get("ref")
            row["active_since"] = active.get("claimed_at")
            row["active_since_human"] = _age_human(active.get("claimed_at"))
        else:
            row["active_ref"] = None
            row["active_since"] = None
            row["active_since_human"] = None
        last_closed = last_closed_by_key.get(wid)
        if last_closed:
            row["last_closed_ref"] = last_closed.get("ref")
            row["last_closed_summary"] = (last_closed.get("resolution") or {}).get("summary")
        else:
            row["last_closed_ref"] = None
            row["last_closed_summary"] = None
        row["display_name"] = _display_name(row, active, last_closed)
    return rows


def drain_goal(queue: str, worker_id: str, repo_path: str = "") -> str:
    """The canonical drain goal text for one worker."""
    from . import config
    claim_filter = "".join(f" --type {t}" for t in config.claim_types(queue))
    return DRAIN_GOAL_TEMPLATE.format(
        queue=queue, worker_id=worker_id, repo=repo_path or os.getcwd(),
        claim_filter=claim_filter,
    )


def build_drain_command(
    queue: str, engine: str, worker_id: str, repo_path: str = "", model: str = ""
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

    ``model`` (queue config, ``wt set --model``) pins the agent's model; empty
    means no ``--model`` flag, i.e. the CLI's own configured default. For
    claude, versioned ids need the full ``claude-`` prefix (``claude-sonnet-5``);
    bare family names (``sonnet``) also work.
    """
    bin_name = _ENGINE_BIN.get(engine, engine)
    if engine == "codex":
        argv = [bin_name, "exec"]
        if model:
            argv += ["--model", model]
        argv.append(drain_goal(queue, worker_id, repo_path))
        return argv
    argv = [
        bin_name, "-p",
        "--input-format", "stream-json",
        "--output-format", "stream-json",
        "--verbose",
        # No --name: a generic "<queue> queue worker" label would overwrite the
        # ticket-derived titles WT sets later via the custom-title event.
        "--permission-mode", "bypassPermissions",
    ]
    if model:
        argv += ["--model", model]
    return argv


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


def requeue_orphaned_tickets(grace_s: float = 120.0) -> List[Dict[str, Any]]:
    """Reopen in_progress tickets whose claiming worker is no longer alive.

    A worker that dies, crashes, or is reaped mid-ticket leaves its ticket
    stranded as ``in_progress`` forever — ``claim_next`` only picks ``open``, so
    nothing re-drains it and nothing closes it. The queue then reads depth=0
    ("Ready") while work is genuinely unfinished. This sweep is the durable fix:
    any in_progress ticket whose ``claimed_by`` worker_id is not in the live set
    (and that was claimed longer ago than ``grace_s``, to avoid a spawn/claim
    race) is reopened so a fresh worker re-claims it.

    Returns the list of reopened items."""
    from . import queue as _q
    import time as _time
    live_ids = {str(w.get("worker_id", "")) for w in list_workers(prune=False)
                if w.get("alive")}
    now = _time.time()
    reopened: List[Dict[str, Any]] = []
    try:
        items = _q.list_items()
    except Exception:
        return reopened
    for it in items:
        if it.get("status") != "in_progress":
            continue
        # Skip tickets parked for human input — a worker exiting after `wt block`
        # is intentional. Continuity lives in claimed_session_id; `wt answer`
        # resumes the original session. Reopening would hand it to a different
        # worker that lacks the original context.
        if it.get("needs_input"):
            continue
        claimer = str(it.get("claimed_by") or "")
        if claimer and claimer in live_ids:
            continue  # its worker is alive — leave it
        # Grace window guards against a just-claimed ticket whose worker record
        # hasn't been written yet (spawn/claim race).
        claimed_at = it.get("claimed_at")
        if claimed_at:
            try:
                from datetime import datetime, timezone
                ts = datetime.fromisoformat(str(claimed_at).replace("Z", "+00:00")).timestamp()
                if (now - ts) < grace_s:
                    continue
            except Exception:
                pass
        ref = it.get("ref")
        try:
            # quiet=True: the reconciler emits the single REQUEUE line for this
            # event, so suppress update_status's primitive REOPEN to avoid a
            # duplicate log entry at the same timestamp.
            # require_status="in_progress": the eligibility decision above was
            # made from a `list_items()` snapshot taken before this loop ran.
            # If the ticket was closed in the meantime (worker finished right
            # as this sweep started), a plain update_status would clobber that
            # close back to "open" — this ticket briefly (and wrongly)
            # reappearing as open/in_progress right after a real close was
            # reported as OPS-72. The guard makes the write a no-op instead.
            item = _q.update_status(ref, "open", quiet=True, require_status="in_progress")
            if item:
                reopened.append(item)
        except Exception:
            pass
    return reopened


def dispatch_after_enqueue(queue: str, ref: str = "") -> str:
    """Decide + act on what a newly-filed ticket needs, and log the decision.

    Called right after an enqueue (CLI `wt add` or the CCC dashboard) so a ticket
    is handled immediately instead of waiting for the next reconciler tick, and so
    the activity log explains the outcome rather than going silent after ENQUEUE.

    Disposition, logged as `DISPATCH <ref> — <reason>`:
      - auto_drain off            → queued as backlog (no worker)
      - a warm worker is live     → nudged via its FIFO (immediate pickup)
      - no warm worker            → reap cold + reconcile; spawned a fresh worker
      - reconcile spawned nothing → no action (with the reconcile skip reason)

    Returns the reason string. Best-effort: never raises."""
    from . import config, queue as _q
    from .queue import _log
    try:
        ref = ref or ""
        if not config.auto_drain(queue):
            reason = "queued — auto_drain off (backlog, no worker)"
            _log("DISPATCH", f"{ref} — {reason}", queue=queue)
            return reason
        nudge = (
            f"New ticket {ref} filed on {queue}. Claim it with "
            f"`wt claim -q {queue} --worker <your-id> --json` and drain the queue."
        )
        delivered = notify_workers(queue, nudge)
        if delivered:
            reason = f"nudged {delivered} live worker(s) — immediate pickup"
            _log("DISPATCH", f"{ref} — {reason}", queue=queue)
            return reason
        # No warm worker: reap cold ones, then reconcile to spawn a fresh worker.
        reap_stale_workers(queue=queue)
        result = reconcile_once()
        spawned = [r for r in result.get("spawned", []) if r.get("queue") == queue]
        if spawned:
            wid = spawned[0].get("worker_id", "?")
            reason = f"spawned worker {wid}"
            _log("DISPATCH", f"{ref} — {reason}", queue=queue)
            return reason
        # Nothing spawned — surface the reconcile skip reason for this queue.
        skip = next((s for s in result.get("skipped", [])
                     if s.get("queue") == queue), None)
        why = (skip or {}).get("reason", "no live worker accepted and none spawned")
        reason = f"no action — {why}"
        _log("DISPATCH", f"{ref} — {reason}", queue=queue)
        return reason
    except Exception:
        return ""


def backfill_claimed_session_ids() -> List[str]:
    """Write each live worker's cloud session UUID onto the ticket it holds.

    A worker claims with its non-UUID worker_id, so the ticket's
    ``claimed_session_id`` is empty until its engine process has started and WT
    has resolved the real UUID into workers.json. This propagates that UUID onto
    the in_progress ticket so consumers (CCC queue health) can resolve a live
    worker instead of showing WAITING/STUCK. Idempotent — a no-op once set.

    This is also the reliable trigger point for the WT-49 session rename: a
    fresh ``wt claim`` almost always fires before WT has resolved the
    worker's real UUID (the worker_id is a non-UUID label like
    ``wt-f8470ec0``), so ``cli.cmd_claim``'s own best-effort rename is
    usually a no-op — this is the moment the UUID (and therefore a
    renameable transcript) first becomes known.

    Returns the refs that were freshly backfilled this pass."""
    from . import queue as _q
    backfilled: List[str] = []
    # worker_id -> cloud session_id for live workers that have a resolved UUID.
    wid_to_sid = {str(w.get("worker_id", "")): str(w.get("session_id", ""))
                  for w in list_workers(prune=False)
                  if w.get("alive") and w.get("session_id")}
    if not wid_to_sid:
        return backfilled
    try:
        items = _q.list_items()
    except Exception:
        return backfilled
    for it in items:
        if it.get("status") != "in_progress":
            continue
        sid = wid_to_sid.get(str(it.get("claimed_by") or ""))
        if not sid or it.get("claimed_session_id") == sid:
            continue
        try:
            if _q.backfill_session_id(it.get("ref"), sid):
                backfilled.append(it.get("ref", ""))
                try:
                    from . import messages as _messages
                    name = display_name(
                        it.get("project", ""),
                        it.get("ref"),
                        ticket_context(it),
                    )
                    _messages.set_session_title(sid, name)
                except Exception:
                    pass
        except Exception:
            pass
    return backfilled


def _latest_worker_item(
    worker_id: str, session_id: str, items: List[Dict[str, Any]]
) -> Optional[Dict[str, Any]]:
    """Most useful ticket for a worker session: latest closed, else active."""
    keys = {k for k in (worker_id, session_id) if k}
    if not keys:
        return None

    def _matches(it: Dict[str, Any]) -> bool:
        return any(str(it.get(field) or "") in keys
                   for field in ("claimed_by", "claimed_session_id"))

    closed = [it for it in items if it.get("status") == "closed" and _matches(it)]
    if closed:
        return max(closed, key=lambda it: str(it.get("closed_at") or ""))
    active = [
        it for it in items
        if it.get("status") == "in_progress" and _matches(it)
    ]
    if active:
        return max(active, key=lambda it: str(it.get("claimed_at") or ""))
    return None


def _item_activity_ts(item: Dict[str, Any]) -> float:
    raw = item.get("closed_at") or item.get("claimed_at") or item.get("updated_at")
    if not raw:
        return 0.0
    try:
        return datetime.strptime(str(raw), "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        ).timestamp()
    except (ValueError, TypeError):
        return 0.0


def _session_title_backfill_row(
    *,
    worker_id: str,
    session_id: str,
    item: Dict[str, Any],
    dry_run: bool,
) -> Dict[str, Any]:
    title = display_name(
        str(item.get("project") or ""),
        str(item.get("ref") or ""),
        ticket_context(item),
    )
    updated = False
    if not dry_run:
        try:
            from . import messages as _messages
            updated = _messages.set_session_title(session_id, title)
        except Exception:
            updated = False
    return {
        "worker_id": worker_id,
        "session_id": session_id,
        "ref": item.get("ref", ""),
        "title": title,
        "updated": updated,
    }


def backfill_recent_session_titles(
    hours: float = 24.0,
    dry_run: bool = False,
) -> List[Dict[str, Any]]:
    """Rename recent WT worker transcripts from their latest ticket context.

    Scans worker logs modified within ``hours`` under ``~/.watchtower/logs``,
    resolves each log's Claude session id, finds the worker's latest closed
    ticket (or active ticket), and appends the canonical ``custom-title`` event.
    """
    from . import queue as _q
    cutoff = time.time() - max(0.0, float(hours)) * 3600.0
    log_dir = WORKERS_FILE.parent / "logs"
    try:
        logs = sorted(log_dir.glob("*.log"))
    except OSError:
        return []
    try:
        items = _q.list_items()
    except Exception:
        items = []
    out: List[Dict[str, Any]] = []
    seen: set = set()
    for path in logs:
        try:
            if path.stat().st_mtime < cutoff:
                continue
        except OSError:
            continue
        worker_id = path.stem
        sid = resolve_session_id_from_log(str(path))
        if not sid:
            continue
        item = _latest_worker_item(worker_id, sid, items)
        if not item:
            continue
        row = _session_title_backfill_row(
            worker_id=worker_id,
            session_id=sid,
            item=item,
            dry_run=dry_run,
        )
        seen.add((row["session_id"], row["ref"]))
        out.append(row)
    for item in sorted(items, key=_item_activity_ts):
        sid = str(item.get("claimed_session_id") or "")
        if not sid or _item_activity_ts(item) < cutoff:
            continue
        worker_id = str(item.get("claimed_by") or item.get("closed_by") or sid)
        key = (sid, item.get("ref", ""))
        if key in seen:
            continue
        row = _session_title_backfill_row(
            worker_id=worker_id,
            session_id=sid,
            item=item,
            dry_run=dry_run,
        )
        seen.add(key)
        out.append(row)
    return out


def reconcile_once(dry_run: bool = False) -> Dict[str, Any]:
    """One reconciler tick.

    Loads the registry + live workers + queue depths and for each registered
    queue decides:
      - desired = desired_workers (default 1) if auto_drain AND depth > 0 else 0
      - if actual_live < desired: spawn the delta (or record in dry_run)

    Wind-down (drain/surplus) is deliberately NOT decided here. Counting only
    `open` on a busy queue is a bet on the future — a worker that just claimed
    the last ticket would be STOPped prematurely. The surplus decision is made
    at claim time (``cli.cmd_claim``: nothing claimable AND live>desired), and
    REAP is the idle safety net. So ``result["stopped"]`` stays empty from the
    reconciler; the key + its log rendering remain for other callers.

    Returns ``{"spawned": [...], "stopped": [...], "skipped": [...]}``.
    ``skipped`` entries explain why a queue was left alone (e.g. auto_drain=off,
    depth=0, or surplus resolved elsewhere).  In ``dry_run`` mode no subprocesses
    are started; the return value shows what *would* happen.

    Passes are serialized under a cross-process file lock. The daemon tick and
    ``dispatch_after_enqueue`` (``wt add``) can reconcile concurrently; without
    the lock both read the same live count and each spawns the full desired
    delta, over-spawning the queue (WT-75: 4 spawned for desired=2).
    ``spawn_workers`` persists every worker via ``record_worker()`` before the
    lock is released, so a blocked pass sees the fresh workers and skips.
    """
    from .queue import _FileLock
    with _FileLock(WORKERS_FILE.parent / "reconcile.lock"):
        return _reconcile_once_locked(dry_run)


def _reconcile_once_locked(dry_run: bool = False) -> Dict[str, Any]:
    from . import config, health
    import sys

    # One-time import of legacy queue-registry.json (no-op after first run).
    try:
        config.migrate_from_registry()
    except Exception:
        pass

    result: Dict[str, Any] = {"spawned": [], "stopped": [], "skipped": [],
                              "reaped": [], "requeued": [], "backfilled": []}

    # Reap cold idle workers first (idle past the prompt-cache TTL): waking one
    # would re-read its bloated context uncached. Killing it lets the spawn pass
    # below start a fresh, small-context worker instead. Skipped in dry_run.
    if not dry_run:
        try:
            result["reaped"] = reap_stale_workers()
        except Exception:
            pass
        # Release tickets stranded in_progress by a dead/reaped/crashed worker so
        # the spawn pass below re-drains them. Without this a queue reads depth=0
        # ("Ready") while work is unfinished. Must run BEFORE the depth read.
        try:
            result["requeued"] = [it.get("ref", "")
                                  for it in requeue_orphaned_tickets()]
        except Exception:
            pass
        # Nudge any live worker already on an affected queue so the reopened
        # ticket is re-claimed right away. Without this, a requeue only gets
        # picked up when the spawn pass below decides actual<desired (it won't,
        # if a same-queue worker is already live and busy elsewhere) or when an
        # existing worker happens to poll again on its own — leaving the ticket
        # visibly "open" but unworked for however long that takes.
        try:
            requeued_queues = {ref.rsplit("-", 1)[0]
                               for ref in result["requeued"] if ref}
            for q_name in requeued_queues:
                nudge = (
                    f"A ticket on {q_name} was reopened after its previous "
                    f"worker died mid-claim. Claim it with `wt claim -q {q_name} "
                    "--worker <your-id> --json` and keep draining."
                )
                notify_workers(q_name, nudge)
        except Exception:
            pass
        # Propagate live workers' cloud session UUIDs onto the tickets they hold
        # so consumers can resolve a reachable worker (no false WAITING/STUCK).
        try:
            result["backfilled"] = backfill_claimed_session_ids()
        except Exception:
            pass

    all_cfg = config.all_queues()
    # Build live-worker counts keyed by queue.
    live_by_queue: Dict[str, List[Dict[str, Any]]] = {}
    for w in list_workers(prune=False):
        if w.get("alive"):
            q_name = w.get("queue", "")
            live_by_queue.setdefault(q_name, []).append(w)

    # Use health for queue depth + stuck ground-truth -- one call covers all queues.
    health_by_queue: Dict[str, Dict[str, Any]] = {
        row["queue"]: row for row in health.all_status()
    }

    # Raw open depth per queue, for the "N open" figure in skip/spawn messages
    # (unfiltered -- just how big the backlog is, regardless of claimability).
    from . import queue as _q
    _total_open_by_q: Dict[str, int] = {}
    try:
        for it in (_q.list_items() or []):
            if it.get("status") != "open":
                continue
            qn = str(it.get("project") or "")
            _total_open_by_q[qn] = _total_open_by_q.get(qn, 0) + 1
    except Exception:
        _total_open_by_q = {}

    def _claimable_depth(qn: str) -> tuple:
        """(claimable_open, total_open) for a queue. claimable_open comes from
        queue.count_claimable(), the exact same candidate filter claim_next()
        itself uses (claim_types restriction + readiness gating, e.g.
        needs-shaping/needs-spec) -- so the reconciler can never think a
        ticket is spawn-worthy when a worker wouldn't actually be able to
        claim it. That drift is what caused WT's SPAWN -> idle -> REAP churn:
        a hand-rolled copy of the filter here disagreed with claim_next's."""
        total = _total_open_by_q.get(qn, 0)
        types = config.claim_types(qn) or None
        claimable = _q.count_claimable(project=qn, item_types=types)
        return claimable, total

    for q_name in all_cfg:
        auto = config.auto_drain(q_name)
        desired = config.desired_workers(q_name) if auto else 0
        depth, total_open = _claimable_depth(q_name)
        if not auto:
            result["skipped"].append({"queue": q_name, "reason": "auto_drain=off"})
            continue
        if depth == 0:
            # Distinguish "truly empty" from "only non-claimable items remain"
            # (wrong claim_type, or needs-shaping/needs-spec readiness).
            filtered = total_open - depth
            reason = (f"0 claimable ({total_open} open, filtered by claim_types/readiness)"
                      if filtered > 0 else "depth=0")
            result["skipped"].append({"queue": q_name, "reason": reason})
            # Wind-down is NOT decided here. Counting only `open` on a drained
            # queue is a bet on the future: the instant a worker claims the last
            # ticket it flips open->in_progress, depth reads 0, and STOPping the
            # busy worker is premature. The surplus/idle decision is made at
            # claim time (cmd_claim, live>desired) when the real current state is
            # known, and REAP is the idle safety net.
            continue
        live = live_by_queue.get(q_name, [])
        actual = len(live)

        # A queue can be fully staffed (actual > 0) yet show zero progress --
        # e.g. every live worker's last turn errored out on a transient API or
        # connectivity fault and it's sitting idle mid-session rather than
        # crashed (a crash is caught by the reap+requeue pass above, which
        # frees the slot for a fresh spawn). Detect via the queue's own stuck
        # ground truth (no ticket closed in stuck_minutes despite claimable
        # work) and nudge the live worker(s) to retry/continue.
        if not dry_run and actual > 0 and (health_by_queue.get(q_name) or {}).get("stuck"):
            _maybe_nudge_stuck_queue(q_name, actual)

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
            # Why this spawn happened: open depth + how short of desired we were.
            spawn_reason = (
                f"{depth} open, {actual} live < {desired} desired"
            )
            for rec in spawned:
                rec["spawn_reason"] = spawn_reason
            result["spawned"].extend(spawned)
        elif actual > desired:
            # Surplus is NOT wound down here. A worker discovers it is surplus at
            # claim time (cmd_claim: nothing claimable AND live>desired) and exits
            # itself; REAP handles the persistently-idle case. The reconciler no
            # longer pushes a speculative STOP based on a momentary count.
            result["skipped"].append(
                {"queue": q_name,
                 "reason": f"surplus ({actual}>{desired}) — resolved at claim/reap"}
            )
        else:
            result["skipped"].append(
                {"queue": q_name, "reason": f"actual={actual}==desired={desired}"}
            )

    # Log the reconcile event to the unified activity log via queue._log().
    try:
        from watchtower.queue import _log
        for w in result.get("spawned", []):
            wid = w.get("worker_id", "?")
            q = w.get("queue", "?")
            pid = w.get("pid", "?")
            reason = w.get("spawn_reason", "")
            _log("SPAWN", f"{wid} (pid {pid})" + (f" — {reason}" if reason else ""), queue=q)
        for w in result.get("stopped", []):
            wid = w.get("worker_id", w) if isinstance(w, dict) else w
            q = (w.get("queue", "") if isinstance(w, dict) else "")
            reason = (w.get("reason", "") if isinstance(w, dict) else "")
            _log("STOP", str(wid) + (f" — {reason}" if reason else ""), queue=q)
        for w in result.get("reaped", []):
            wid = w.get("worker_id", w) if isinstance(w, dict) else w
            q = (w.get("queue", "") if isinstance(w, dict) else "")
            reason = (w.get("_reap_reason", "") if isinstance(w, dict) else "")
            _log("REAP", str(wid) + (f" — {reason}" if reason else ""), queue=q)
        for ref in result.get("requeued", []):
            q = ref.rsplit("-", 1)[0] if "-" in ref else ""
            _log("REQUEUE", f"{ref} — worker gone, reopened for re-drain", queue=q)
    except Exception:
        pass

    return result


def spawn_workers(
    queue: str,
    n: int = 1,
    engine: str = "claude",
    *,
    repo_path: str = "",
    model: str = "",
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
    if not model:
        from . import config
        model = config.model(queue)
    log_dir = WORKERS_FILE.parent / "logs"
    spawned: List[Dict[str, Any]] = []
    for _ in range(max(1, n)):
        worker_id = f"{queue.lower()}-{uuid.uuid4().hex[:8]}"
        argv = build_drain_command(queue, engine, worker_id, repo_path, model)
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

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
import shlex
import shutil
import subprocess
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows fallback
    fcntl = None

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

# Persistent, append-only ledger of every worker_id WatchTower has ever spawned.
# workers.json prunes a dead worker's record on the very next read (list_workers
# defaults to prune=True, and nearly every routine read -- `wt status`, the
# dashboard poll -- uses that default), often within seconds of it dying. That
# race meant requeue_orphaned_tickets, which only reopens a ticket when its
# claimant is a *known* spawned worker (to avoid double-working a still-live
# session per OPS-104), almost never saw the dead worker in time and left the
# ticket orphaned in_progress forever. This ledger survives pruning so that
# check still has the evidence it needs. Shape: {"worker_ids": [...]}.
WORKER_IDS_FILE = Path(
    os.environ.get("WATCHTOWER_WORKER_IDS_FILE")
    or (Path.home() / ".watchtower" / "worker-ids.json")
)

# Tracks short-lived worker launch failures (quota/auth/API/missing binary) so
# the reconciler does not create a new cloud session every tick while the engine
# is unavailable.
LAUNCH_FAILURES_FILE = Path(
    os.environ.get("WATCHTOWER_LAUNCH_FAILURES_FILE")
    or (Path.home() / ".watchtower" / "launch-failures.json")
)

# Cap to avoid unbounded growth; oldest entries are dropped first.
_WORKER_SESSIONS_CAP = 500
_WORKER_IDS_CAP = 500

_LAUNCH_FAILURE_GRACE_S = float(
    os.environ.get("WATCHTOWER_LAUNCH_FAILURE_GRACE_S", "6")
)
_LAUNCH_FAILURE_DEFAULT_COOLDOWN_S = float(
    os.environ.get("WATCHTOWER_LAUNCH_FAILURE_COOLDOWN_S", "300")
)

# Shared, queue-agnostic runbook (WT-101): DRAIN_GOAL_TEMPLATE keeps only a
# one-line trigger for the Resume Check / Idle Protocol steps; the how-to
# lives in docs/worker-runbook.md so the spawn prompt itself stays lean. Path
# is absolute and independent of {repo} -- the runbook ships with the wt
# package, not with whatever repo a given queue's tickets live in.
_WORKER_RUNBOOK_PATH = Path(__file__).resolve().parent.parent / "docs" / "worker-runbook.md"

# Drain goal adapted from CCC's docs/ux-fixes-worker-brief.md canonical /goal.
# Generalized: no CCC paths, no shared-clone assumptions. The worker drains one
# queue via the `wt` CLI it was spawned by. Claude waits warm on its FIFO when
# empty; Codex completes the drain goal and exits because it has no live input.
CLAUDE_IDLE_CONTRACT = (
    "An idle worker may later be released from queue staffing. "
    "Do NOT poll, do NOT sleep-loop, and do NOT exit the process on your own -- "
    "your stdin is a live input channel: when a new ticket is filed, a fresh "
    "instruction message arrives and you resume automatically with your full "
    "warm context. Ending your turn on an empty queue is correct -- the next "
    "message wakes you. "
)

CODEX_IDLE_CONTRACT = (
    "Do NOT poll or sleep-loop. After the idle audit, complete this queue's "
    "drain goal using the native goal control (or clear it if completion is "
    "unavailable), then exit immediately. This is a one-shot run; do not wait "
    "for a wake message. "
)

CLAUDE_RESUME_CONTRACT = (
    "RESUME CHECK: whenever you wake and your warm context says you were "
    "mid-work on a ticket, follow the Resume Check protocol in {runbook} "
    "before you edit, commit, or close anything -- skipping this is how the "
    "same ticket gets fixed and committed twice. "
)

CODEX_RESUME_CONTRACT = (
    "RESUME CHECK: if this Codex run starts with context indicating you were "
    "mid-work on a ticket, follow the Resume Check protocol in {runbook} before "
    "you edit, commit, or close anything -- skipping this is how the same "
    "ticket gets fixed and committed twice. "
)

KIMI_IDLE_CONTRACT = (
    "Do NOT poll or sleep-loop. After the idle audit, end your turn and exit "
    "immediately. This is a one-shot run (kimi -p); do not wait for a wake "
    "message. "
)

KIMI_RESUME_CONTRACT = (
    "RESUME CHECK: if this run starts with context indicating you were "
    "mid-work on a ticket, follow the Resume Check protocol in {runbook} before "
    "you edit, commit, or close anything -- skipping this is how the same "
    "ticket gets fixed and committed twice. "
)

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
    "IDLE: when `wt claim` reports the queue is drained, follow the Idle "
    "Protocol in {runbook} BEFORE ending your turn (it has you update the "
    "queue's learnings file). {idle_contract}{resume_contract}"
    "Push or publish exactly when the claimed ticket's worker instructions tell "
    "you to. If the claimed ticket has no explicit push/publish instruction, "
    "leave commits local unless the user explicitly asks you to push."
)

# Bounded, single-ticket variant of DRAIN_GOAL_TEMPLATE (CCC-437's per-row
# "drain once" button): claim exactly one ref, resolve it, then stop -- no
# re-poll loop, no idling on stdin for the next ticket.
RUN_ONCE_GOAL_TEMPLATE = (
    "Fix ticket {ref} on the {queue} WatchTower queue. This is a ONE-OFF run, "
    "not a drain loop -- handle exactly this one ticket and then stop. "
    "Work in the git repo at {repo}. Your worker id is {worker_id}. "
    "Claim it with `wt claim -q {queue} {ref} --worker {worker_id} --json` "
    "(if it returns nothing or an error, the ticket was already claimed or "
    "closed by someone else in the meantime -- just exit, that's fine). "
    "If you ever resume mid-work with warm context, re-verify you still own "
    "{ref} first (`wt find {ref} --json`: `claimed_by` == {worker_id}, "
    "`status` == in_progress); if not, you were reaped and it was taken over -- "
    "discard uncommitted changes and exit, do not commit or close. "
    "Read the ticket's note/text and, if present, open its screenshot_path and "
    "resolve its selector. Make the change in the relevant repo and verify it. "
    "Commit only the paths you changed (never `git add -A`/`.`/`-a`). "
    "RESOLUTION IS MANDATORY: NEVER close a ticket without `--summary`. "
    "When you close it: `wt close {ref} --worker {worker_id} --summary "
    "\"what you changed\"`. Add `--caveat \"...\"` for anything to watch out "
    "for, `--follow-up \"...\"` for notable next steps, and `--unresolved "
    "\"...\"` for anything you could not fix (each flag is repeatable). "
    "If it genuinely cannot be resolved without a human decision, do NOT "
    "close it and do NOT guess: run `wt block {ref} --worker {worker_id} "
    "--question \"the specific decision you need\" --progress \"what you've "
    "figured out so far\"`. "
    "Either way -- closed or blocked -- STOP once this one ticket is "
    "resolved. Do NOT claim another ticket, do NOT poll, do NOT wait for new "
    "work. End your turn. Push or publish exactly when the claimed ticket's "
    "worker instructions tell you to. If the claimed ticket has no explicit "
    "push/publish instruction, leave commits local unless the user explicitly "
    "asks you to push."
)

_ENGINE_BIN = {"claude": "claude", "codex": "codex", "kimi": "kimi"}

# Engines whose workers are one-shot processes (goal in argv, DEVNULL stdin,
# exit when done) rather than FIFO-fed live processes like claude.
_ONE_SHOT_ENGINES = ("codex", "kimi")


def _resolve_engine_bin(engine: str) -> str:
    """Return an executable path for an agent engine, or "" if unavailable.

    Queue workers may be spawned from a daemon/service environment whose PATH is
    narrower than an interactive shell. Resolve common user-local installs here
    so daemon-spawned Codex workers do not depend on shell startup files.
    """
    if engine == "antigravity":
        return _resolve_antigravity_bin()

    bin_name = _ENGINE_BIN.get(engine, engine)
    env_name = {
        "claude": "WATCHTOWER_CLAUDE_BIN",
        "codex": "WATCHTOWER_CODEX_BIN",
        "kimi": "WATCHTOWER_KIMI_BIN",
    }.get(engine)
    if env_name:
        env_bin = os.environ.get(env_name)
        if env_bin:
            expanded = os.path.expanduser(env_bin)
            found = shutil.which(expanded)
            if found:
                return found
            if os.path.isfile(expanded) and os.access(expanded, os.X_OK):
                return expanded
            return ""

    found = shutil.which(bin_name)
    if found:
        return found

    user_local = Path.home() / ".local" / "bin" / bin_name
    if user_local.is_file() and os.access(user_local, os.X_OK):
        return str(user_local)
    if engine == "kimi":
        # The kimi CLI self-installs here; daemon/service PATHs often miss it.
        kimi_local = Path.home() / ".kimi-code" / "bin" / bin_name
        if kimi_local.is_file() and os.access(kimi_local, os.X_OK):
            return str(kimi_local)
    return ""


def _resolve_antigravity_bin() -> str:
    """Locate the Antigravity AGY CLI. Env override first (nonstandard
    installs), then `agy`/`antigravity` on PATH. Empty string when absent."""
    env_bin = os.environ.get("WATCHTOWER_ANTIGRAVITY_BIN")
    if env_bin:
        expanded = os.path.expanduser(env_bin)
        if os.path.isfile(expanded) and os.access(expanded, os.X_OK):
            return expanded
        return ""
    for cmd in ("agy", "antigravity"):
        found = shutil.which(cmd)
        if found:
            return found
    return ""


def engine_available(engine: str) -> bool:
    """True if the engine's CLI binary is resolvable on this machine."""
    return bool(_resolve_engine_bin(engine))

# Fable is a creative/story model — not suited for code work.  Spawning a
# worker with it produces poor results; guard against accidental selection.
_FABLE_PATTERN = re.compile(r"^(claude-)?fable(-\d+)?$", re.IGNORECASE)


def _is_fable_model(m: str) -> bool:
    return bool(_FABLE_PATTERN.match(m.strip()))


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


def _unlink_path_quiet(path: Optional[str]) -> None:
    if not path:
        return
    try:
        Path(path).unlink()
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


# Cache warmth is a routing/cost hint, not permission to terminate or release a
# worker. Claude's default prompt-cache TTL is five minutes; Codex can retain a
# cached prefix longer. Keep this threshold for warm FIFO nudges only.
WARM_TTL_S = 300

# A queue worker must be inactive for at least this long before WatchTower may
# release it from queue staffing. This intentionally applies to every engine:
# provider cache policy and worker lifecycle are separate concerns.
RELEASE_IDLE_S = 30 * 60


def _codex_rollout_mtime(w: Dict[str, Any]) -> float:
    """Newest rollout mtime for a Codex worker, or 0 when unavailable.

    Codex app-server turns update the durable rollout without writing to the
    original ``codex exec`` stdout log. The rollout is therefore the only
    standalone activity clock that prevents a live resumed conversation from
    looking cold to WatchTower's reaper.
    """
    if w.get("engine") != "codex":
        return 0.0
    session_id = str(w.get("session_id") or "").strip()
    if not session_id:
        return 0.0
    codex_home = Path(os.environ.get("CODEX_HOME") or (Path.home() / ".codex"))
    sessions = codex_home / "sessions"
    newest = 0.0
    try:
        for path in sessions.glob(f"*/*/*/*{session_id}.jsonl"):
            try:
                newest = max(newest, path.stat().st_mtime)
            except OSError:
                continue
    except OSError:
        return 0.0
    return newest


def _claude_transcript_mtime(w: Dict[str, Any]) -> float:
    """Main transcript mtime for a Claude worker, or 0 when unavailable.

    A Claude conversation resumed outside the original WatchTower subprocess
    updates ``~/.claude/projects/*/<session-id>.jsonl`` without touching the
    spawn stdout log. Treating only that log as activity can therefore release
    a session that is actively doing unrelated work.
    """
    if w.get("engine") != "claude":
        return 0.0
    session_id = str(w.get("session_id") or "").strip()
    if not session_id:
        return 0.0
    claude_home = Path(
        os.environ.get("CLAUDE_CONFIG_DIR") or (Path.home() / ".claude")
    )
    newest = 0.0
    try:
        for path in (claude_home / "projects").glob(f"*/{session_id}.jsonl"):
            try:
                newest = max(newest, path.stat().st_mtime)
            except OSError:
                continue
    except OSError:
        return 0.0
    return newest


def _kimi_wire_mtime(w: Dict[str, Any]) -> float:
    """Newest wire.jsonl mtime for a kimi worker, or 0 when unavailable.

    A kimi session driven from outside its original ``kimi -p`` worker process
    (e.g. a CCC ACP attach steering it) updates
    ``~/.kimi-code/sessions/*/<sid>/agents/*/wire.jsonl`` without touching the
    spawn stdout log -- same reaper-blindness class as the codex rollout.
    """
    if w.get("engine") != "kimi":
        return 0.0
    session_id = str(w.get("session_id") or "").strip()
    if not session_id:
        return 0.0
    kimi_home = Path(os.environ.get("KIMI_CODE_HOME") or (Path.home() / ".kimi-code"))
    newest = 0.0
    try:
        for path in (kimi_home / "sessions").glob(f"*/{session_id}/agents/*/wire.jsonl"):
            try:
                newest = max(newest, path.stat().st_mtime)
            except OSError:
                continue
    except OSError:
        return 0.0
    return newest


def _worker_idle_s(w: Dict[str, Any]) -> float:
    """Seconds since this worker last did anything.

    The stream-json output log is written on every turn, so its mtime is the
    worker's last-activity clock. Falls back to ``started_at`` if the log is
    missing. Returns a large number when nothing is resolvable (treat as cold)."""
    latest_activity = max(
        _codex_rollout_mtime(w),
        _claude_transcript_mtime(w),
        _kimi_wire_mtime(w),
    )
    log = w.get("log")
    if log:
        try:
            latest_activity = max(latest_activity, os.path.getmtime(log))
        except OSError:
            pass
    if latest_activity:
        return max(0.0, time.time() - latest_activity)
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


def _worker_released(w: Dict[str, Any]) -> bool:
    if w.get("released_at"):
        return True
    worker_id = str(w.get("worker_id") or "")
    return bool(worker_id and (STOP_SIGNALS_DIR / worker_id).exists())


def notify_workers(queue: str, text: str, max_idle_s: Optional[float] = None) -> int:
    """Push `text` to live workers on `queue` via their FIFO.

    ``max_idle_s`` defaults to the lifecycle release floor. A cache-cold worker
    is still the queue's valid worker and must be woken for new work; only a
    separately verified/released worker is skipped. Callers may pass a smaller
    bound when they specifically want warm-cache-only routing.
    """
    if max_idle_s is None:
        max_idle_s = RELEASE_IDLE_S
    n = 0
    for w in list_workers():
        if not w.get("alive"):
            continue
        if w.get("queue") != queue:
            continue
        if _worker_released(w):
            continue  # already released from queue staffing
        if _worker_idle_s(w) >= max_idle_s:
            continue
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

# Grace period after a worker comes alive before it's eligible for a
# stuck-queue nudge. A ticket that sat unclaimed for a long time (no live
# worker to claim it) reads `stuck=True` the instant the queue gets staffed --
# before the fresh worker has had any chance to start up and run its first
# `wt claim`. Without this grace, a reconcile tick landing in that startup
# window nudges a worker that's had zero time to make progress (WT-101).
_NUDGE_STARTUP_GRACE_S = 45


def _all_workers_within_startup_grace(workers: List[Dict[str, Any]]) -> bool:
    """True if every live worker is too freshly started to plausibly have
    claimed a ticket yet -- a spawn/dispatch race, not real staleness."""
    return all(_worker_idle_s(w) < _NUDGE_STARTUP_GRACE_S for w in workers)


def _maybe_nudge_stuck_queue(queue: str, live_count: int) -> int:
    """Nudge live workers on a stuck-but-staffed queue to retry/continue.

    Rate-limited to once per ``_STUCK_NUDGE_COOLDOWN_S`` per queue. Returns
    the number of workers notified (0 if on cooldown or none warm)."""
    from . import config
    from .queue import _log
    now = time.time()
    if now - _last_stuck_nudge.get(queue, 0.0) < _STUCK_NUDGE_COOLDOWN_S:
        return 0
    _last_stuck_nudge[queue] = now
    claim_filter = "".join(f" --type {t}" for t in config.claim_types(queue))
    nudge = (
        f"No ticket has closed on {queue} in a while despite {live_count} live "
        "worker(s) here. This can happen when a turn errors out on a transient "
        "API/connectivity fault and gets stuck instead of continuing. If your "
        f"last turn errored, retry now: `wt claim -q {queue} --worker <your-id>"
        f"{claim_filter} "
        "--json` and keep draining."
    )
    delivered = notify_workers(queue, nudge)
    _log("NUDGE", f"stuck queue — nudged {delivered}/{live_count} live worker(s)",
         queue=queue)
    return delivered


def _release_instruction(w: Dict[str, Any]) -> str:
    queue = str(w.get("queue") or "this queue")
    return (
        "This is from the WatchTower Reconciler. You are no longer a "
        f"WatchTower worker for {queue}. Do not claim any more tickets from "
        "this queue. If the active goal is this queue's WatchTower drain goal, "
        "clear/complete that goal now; do not clear or interrupt an unrelated "
        "goal. This release is queue-scoped: continue any unrelated work "
        "already underway in this conversation."
    )


def _deliver_release_instruction(w: Dict[str, Any], text: str) -> bool:
    fifo = str(w.get("fifo") or "")
    if fifo and write_to_worker_fifo(fifo, text):
        return True
    target = str(w.get("session_id") or "").strip()
    if not target:
        return False
    try:
        from . import messages
        return bool(messages.send(target, text).get("ok"))
    except Exception:
        return False


def release_idle_workers(max_idle_s: float = RELEASE_IDLE_S,
                         queue: Optional[str] = None) -> List[Dict[str, Any]]:
    """Release verified-idle workers from queue staffing without killing them.

    Release is deliberately conservative: the process must still be tracked,
    both the engine transcript and spawn log must be stale past the floor, and
    the queue backend must affirm that the worker owns no in-progress ticket,
    including one blocked for human input. A durable stop sentinel prevents
    future claims; a live instruction explains that unrelated conversation work
    may continue. Unknown queue state fails closed. Returns newly released rows.
    """
    from . import queue as _q
    worker_rows = list_workers(prune=False)
    candidates_by_queue: Dict[str, List[Dict[str, Any]]] = {}
    for w in worker_rows:
        if not w.get("alive") or w.get("kind") == "adhoc":
            continue
        if queue and w.get("queue") != queue:
            continue
        if _worker_released(w):
            continue
        if _worker_idle_s(w) < max_idle_s:
            continue
        candidates_by_queue.setdefault(str(w.get("queue") or ""), []).append(w)

    released: List[Dict[str, Any]] = []
    for queue_name, candidates in sorted(candidates_by_queue.items()):
        if not queue_name:
            continue
        try:
            items = _q.list_items(
                project=queue_name, fresh=True, strict=True
            )
        except Exception:
            # Failing closed per queue is safer than killing a worker whose
            # active claim could not be authoritatively inspected.
            continue
        active_owners = {
            str(owner)
            for item in items
            if item.get("status") == "in_progress"
            for owner in (item.get("claimed_by"), item.get("claimed_session_id"))
            if owner
        }
        for w in candidates:
            if any(
                str(w.get(field) or "") in active_owners
                for field in ("worker_id", "session_id")
            ):
                continue
            worker_id = str(w.get("worker_id") or "")
            if not worker_id:
                continue
            request_stop(worker_id)
            delivered = _deliver_release_instruction(w, _release_instruction(w))
            idle_min = int(_worker_idle_s(w) / 60)
            floor_min = int(max_idle_s / 60)
            w["_release_reason"] = (
                f"idle {idle_min}m (verified idle, >{floor_min}m release floor)"
            )
            w["_release_delivered"] = delivered
            released.append(w)
    return released


def reap_stale_workers(max_idle_s: float = RELEASE_IDLE_S,
                       queue: Optional[str] = None) -> List[Dict[str, Any]]:
    """Compatibility alias; stale workers are now released, never killed."""
    return release_idle_workers(max_idle_s=max_idle_s, queue=queue)


def _load() -> Dict[str, Any]:
    try:
        with open(WORKERS_FILE, "r") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {"workers": []}
    if not isinstance(data, dict) or not isinstance(data.get("workers"), list):
        return {"workers": []}
    return data


class _WorkersFileLock:
    """Cross-process lock for workers.json read-modify-write operations."""

    def __init__(self):
        self._file = None

    def __enter__(self):
        if fcntl is None:
            return self
        lock_path = WORKERS_FILE.with_suffix(".lock")
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(lock_path, "w")
        fcntl.flock(self._file.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._file is not None:
            try:
                fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
            finally:
                self._file.close()


def _save(data: Dict[str, Any]) -> None:
    WORKERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = str(WORKERS_FILE) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, WORKERS_FILE)


def _mark_worker_released(worker_id: str) -> None:
    """Persist queue detachment after the one-shot stop sentinel is consumed."""
    with _WorkersFileLock():
        data = _load()
        changed = False
        for row in data["workers"]:
            if row.get("worker_id") != worker_id or row.get("released_at"):
                continue
            row["released_at"] = datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            changed = True
        if changed:
            _save(data)


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


def _load_worker_id_ledger() -> List[str]:
    """Return the de-duped, ordered list of worker_ids from the ledger.

    Empty list on a missing/unreadable/malformed file. No exceptions escape."""
    try:
        with open(WORKER_IDS_FILE, "r") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(data, dict):
        return []
    ids = data.get("worker_ids")
    if not isinstance(ids, list):
        return []
    return [str(w) for w in ids if isinstance(w, str)]


def _add_worker_id(worker_id: str) -> None:
    """Append a worker_id to the persistent ledger if not already present.

    Cheap: only writes when a genuinely new id is added. Caps the list to the
    most recent ``_WORKER_IDS_CAP`` ids (drops oldest). Atomic tmp+replace,
    mirroring ``_save``. Silently no-ops on a falsy id or write error."""
    if not worker_id:
        return
    worker_id = str(worker_id)
    ids = _load_worker_id_ledger()
    if worker_id in ids:
        return
    ids.append(worker_id)
    if len(ids) > _WORKER_IDS_CAP:
        ids = ids[-_WORKER_IDS_CAP:]
    try:
        WORKER_IDS_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = str(WORKER_IDS_FILE) + ".tmp"
        with open(tmp, "w") as f:
            json.dump({"worker_ids": ids}, f, indent=2)
        os.replace(tmp, WORKER_IDS_FILE)
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

_USAGE_RETRY_DATE_RE = re.compile(
    r"try again at\s+([A-Za-z]+ \d{1,2}(?:st|nd|rd|th)?, \d{4} "
    r"\d{1,2}:\d{2}\s*(?:AM|PM))",
    re.IGNORECASE,
)
_USAGE_RETRY_TIME_RE = re.compile(
    r"try again at\s+(\d{1,2}:\d{2}\s*(?:AM|PM))",
    re.IGNORECASE,
)


def _load_launch_failures() -> Dict[str, Any]:
    try:
        with open(LAUNCH_FAILURES_FILE, "r") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _save_launch_failures(data: Dict[str, Any]) -> None:
    try:
        LAUNCH_FAILURES_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = str(LAUNCH_FAILURES_FILE) + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, LAUNCH_FAILURES_FILE)
    except OSError:
        pass


def _launch_failure_key(queue: str, engine: str) -> str:
    return f"{queue}:{engine or 'claude'}"


def _iso_from_epoch(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_usage_retry_at(text: str, now: Optional[float] = None) -> Optional[float]:
    """Parse Codex quota reset copy like "Jul 9th, 2026 12:09 AM"."""
    now = time.time() if now is None else now

    def _strip_ordinals(s: str) -> str:
        return re.sub(r"(\d{1,2})(st|nd|rd|th)", r"\1", s, flags=re.IGNORECASE)

    m = _USAGE_RETRY_DATE_RE.search(text)
    if m:
        raw = _strip_ordinals(m.group(1))
        for fmt in ("%b %d, %Y %I:%M %p", "%B %d, %Y %I:%M %p"):
            try:
                dt = datetime.strptime(raw, fmt)
                return dt.replace(tzinfo=timezone.utc).timestamp()
            except ValueError:
                continue

    m = _USAGE_RETRY_TIME_RE.search(text)
    if not m:
        return None
    try:
        parsed = datetime.strptime(m.group(1).upper(), "%I:%M %p")
    except ValueError:
        return None
    base = datetime.fromtimestamp(now, tz=timezone.utc)
    candidate = base.replace(
        hour=parsed.hour, minute=parsed.minute, second=0, microsecond=0
    )
    if candidate.timestamp() <= now:
        candidate = candidate + timedelta(days=1)
    return candidate.timestamp()


def _classify_launch_failure_log(
    log_path: Path, now: Optional[float] = None
) -> Optional[Dict[str, Any]]:
    try:
        text = log_path.read_text(errors="replace")
    except OSError:
        return None
    lower = text.lower()
    reason = ""
    retry_at = None
    if "usage limit" in lower:
        reason = "engine usage limit"
        retry_at = _parse_usage_retry_at(text, now=now)
    elif "http 503" in lower or "upstream connect error" in lower:
        reason = "engine api unavailable"
    elif "not logged in" in lower or "please run /login" in lower:
        reason = "engine authentication required"
    elif "authentication" in lower and ("failed" in lower or "error" in lower):
        reason = "engine authentication failed"
    if not reason:
        return None
    return {"reason": reason, "retry_at": retry_at}


def _record_launch_failure(
    *,
    queue: str,
    engine: str,
    worker_id: str,
    pid: int,
    log_path: Path,
    reason: str,
    retry_at: Optional[float] = None,
    model: str = "",
    exit_code: Optional[int] = None,
) -> Dict[str, Any]:
    now = time.time()
    cooldown_until = retry_at if retry_at and retry_at > now else (
        now + _LAUNCH_FAILURE_DEFAULT_COOLDOWN_S
    )
    session_id = resolve_session_id_from_log(str(log_path))
    _add_worker_session_id(session_id)
    rec: Dict[str, Any] = {
        "queue": queue,
        "engine": engine,
        "worker_id": worker_id,
        "pid": pid,
        "log": str(log_path),
        "reason": reason,
        "cooldown_until": cooldown_until,
        "cooldown_until_human": _iso_from_epoch(cooldown_until),
        "recorded_at": _iso_from_epoch(now),
    }
    if model:
        rec["model"] = model
    if exit_code is not None:
        rec["exit_code"] = exit_code
    if session_id:
        rec["session_id"] = session_id

    data = _load_launch_failures()
    data[_launch_failure_key(queue, engine)] = rec
    _save_launch_failures(data)
    try:
        from watchtower.queue import _log
        _log(
            "LAUNCH_FAIL",
            f"{worker_id} (pid {pid}) [{engine}] — {reason}; cooldown until "
            f"{rec['cooldown_until_human']}",
            queue=queue,
        )
    except Exception:
        pass
    return rec


def active_launch_failure_cooldown(
    queue: str, engine: str
) -> Optional[Dict[str, Any]]:
    """Return active launch-failure cooldown for queue/engine, pruning expired."""
    key = _launch_failure_key(queue, engine)
    data = _load_launch_failures()
    rec = data.get(key)
    if not isinstance(rec, dict):
        return None
    try:
        until = float(rec.get("cooldown_until") or 0)
    except (TypeError, ValueError):
        until = 0
    if until > time.time():
        return rec
    data.pop(key, None)
    _save_launch_failures(data)
    return None


def _wait_for_immediate_launch_failure(
    proc: subprocess.Popen,
    *,
    queue: str,
    engine: str,
    worker_id: str,
    log_path: Path,
    model: str = "",
) -> Optional[Dict[str, Any]]:
    if _LAUNCH_FAILURE_GRACE_S <= 0:
        return None
    try:
        exit_code = proc.wait(timeout=_LAUNCH_FAILURE_GRACE_S)
    except subprocess.TimeoutExpired:
        return None
    classified = _classify_launch_failure_log(log_path)
    if not classified:
        return None
    return _record_launch_failure(
        queue=queue,
        engine=engine,
        worker_id=worker_id,
        pid=proc.pid,
        log_path=log_path,
        reason=str(classified.get("reason") or "worker launch failed"),
        retry_at=classified.get("retry_at"),
        model=model,
        exit_code=exit_code,
    )


def resolve_session_id_from_log(log_path: str) -> str:
    """Extract the engine-assigned session id from a worker's stream-json log.

    A ``claude -p --output-format stream-json`` worker emits an init/system event
    carrying ``session_id`` (the UUID Claude/cloud assigns -- WatchTower does NOT
    mint it). ``codex exec`` prints the same UUID as a plain ``session id:``
    startup line. ``kimi -p --output-format stream-json`` closes with a
    ``{"role":"meta","type":"session.resume_hint","session_id":"session_<uuid>"}``
    line; the prefixed id is returned as-is (CCC indexes kimi sessions that way).
    We scan the first lines of the captured output log for any of these shapes.
    Returns "" until the event has been written (the worker must have started
    its first turn)."""
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
                if sid:
                    sid = str(sid)
                    if _SESSION_ID_RE.fullmatch(sid):
                        return sid
                    # kimi emits {"role":"meta","type":"session.resume_hint",
                    # "session_id":"session_<uuid>"} -- keep the prefixed form,
                    # which is the id CCC knows this session by.
                    if sid.startswith("session_") and _SESSION_ID_RE.fullmatch(
                        sid[len("session_"):]
                    ):
                        return sid
    except (OSError, UnicodeDecodeError):
        pass
    return ""


def _upsert_codex_worker_registry(
    worker: Dict[str, Any],
    *,
    ref: str = "",
    title: str = "",
) -> None:
    if str((worker or {}).get("engine") or "").lower() != "codex":
        return
    sid = str((worker or {}).get("session_id") or "").strip()
    if not _SESSION_ID_RE.fullmatch(sid):
        return
    try:
        from . import codex_registry
        wt_meta = {
            "worker_id": worker.get("worker_id") or "",
            "queue": worker.get("queue") or "",
            "ref": ref,
            "log": worker.get("log") or "",
            "started_at": worker.get("started_at") or "",
        }
        codex_registry.upsert(
            sid,
            source="wt-workers",
            visibility="worker",
            transport_owner="wt-codex-exec",
            transport="codex-exec",
            cwd=worker.get("repo_path") or "",
            repo_path=worker.get("repo_path") or "",
            model=worker.get("model") or "",
            worker_id=worker.get("worker_id") or "",
            queue=worker.get("queue") or "",
            ref=ref,
            title=title,
            wt=wt_meta,
        )
    except Exception:
        pass


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


def _find_engine_ancestor_pid(engine: str, max_depth: int = 8) -> int:
    """Return the nearest live agent CLI ancestor of the current process."""
    expected = str(engine or "").strip().lower()
    if expected not in _ENGINE_BIN:
        return 0
    pid = os.getppid()
    for _ in range(max_depth):
        if pid <= 1:
            break
        try:
            result = subprocess.run(
                ["ps", "-p", str(pid), "-o", "ppid=", "-o", "command="],
                capture_output=True,
                text=True,
                timeout=2,
            )
            fields = result.stdout.strip().split(maxsplit=1)
            if len(fields) != 2:
                return 0
            parent_pid = int(fields[0])
            command = fields[1]
            executable_path = (
                command.lstrip().split(maxsplit=1)[0] if command.strip() else ""
            )
            executable = Path(executable_path).name.lower()
            command_tokens = shlex.split(command) if command.strip() else []
            is_codex_app_server = expected == "codex" and "app-server" in command_tokens
            if (
                executable == _ENGINE_BIN[expected]
                and not is_codex_app_server
                and _pid_alive(pid)
            ):
                return pid
            pid = parent_pid
        except (OSError, ValueError, subprocess.SubprocessError):
            return 0
    return 0


def rebind_continued_worker(worker_id: str, session_id: str, pid: int) -> bool:
    """Move a Codex worker record to a continuation process with the same thread.

    Codex goal continuation can replace the original ``codex exec`` process
    while preserving ``CODEX_THREAD_ID``. Rebinding requires both that stable
    thread id and a live Codex ancestor discovered by the caller; a different
    session cannot revive a dead worker alias.
    """
    if not worker_id or not _SESSION_ID_RE.fullmatch(str(session_id or "")):
        return False
    if not _pid_alive(int(pid or 0)):
        return False

    with _WorkersFileLock():
        data = _load()
        worker = next(
            (row for row in data["workers"] if row.get("worker_id") == worker_id),
            None,
        )
        if worker is None:
            try:
                from . import codex_registry
                registry_row = codex_registry.entry(session_id) or {}
            except Exception:
                registry_row = {}
            if registry_row.get("worker_id") != worker_id:
                return False
            wt_meta = (
                registry_row.get("wt")
                if isinstance(registry_row.get("wt"), dict)
                else {}
            )
            worker = {
                "worker_id": worker_id,
                "pid": int(pid),
                "queue": registry_row.get("queue") or wt_meta.get("queue") or "",
                "engine": "codex",
                "repo_path": (
                    registry_row.get("repo_path") or registry_row.get("cwd") or ""
                ),
                "log": wt_meta.get("log") or "",
                "fifo": "",
                "started_at": (
                    wt_meta.get("started_at")
                    or registry_row.get("created_at")
                    or ""
                ),
                "session_id": session_id,
            }
            if registry_row.get("model"):
                worker["model"] = registry_row["model"]
            data["workers"].append(worker)
        else:
            recorded_sid = str(worker.get("session_id") or "")
            if not recorded_sid and worker.get("log"):
                recorded_sid = resolve_session_id_from_log(str(worker["log"]))
            if (
                str(worker.get("engine") or "").lower() != "codex"
                or recorded_sid != session_id
            ):
                return False
            recorded_pid = int(worker.get("pid") or 0)
            if recorded_pid == int(pid):
                return True
            if recorded_pid and _pid_alive(recorded_pid):
                raise ValueError(
                    f"worker {worker_id!r} is still owned by live pid "
                    f"{recorded_pid}; refusing concurrent rebind to pid {int(pid)}"
                )
            worker["previous_pid"] = recorded_pid
            worker["pid"] = int(pid)

        worker["rebound_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        _save(data)
    _upsert_codex_worker_registry(worker)
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
    model: str = "",
    kind: str = "",
) -> Dict[str, Any]:
    with _WorkersFileLock():
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
        if model:
            rec["model"] = model
        if kind:
            rec["kind"] = kind
        if session_id:
            rec["session_id"] = session_id
        data["workers"].append(rec)
        _save(data)
    # Ledger the worker_id now, at spawn time, so it survives this record being
    # pruned once the worker dies (see WORKER_IDS_FILE docstring above).
    _add_worker_id(worker_id)
    # If a session_id is somehow known at record time, ledger it (survives prune).
    _add_worker_session_id(rec.get("session_id", ""))
    _upsert_codex_worker_registry(rec)
    return rec


def list_workers(prune: bool = True) -> List[Dict[str, Any]]:
    """Return tracked workers, each annotated with a live flag.

    When ``prune`` is set, dead workers are dropped from the store.
    """
    with _WorkersFileLock():
        data = _load()
        out: List[Dict[str, Any]] = []
        kept: List[Dict[str, Any]] = []
        backfilled = False
        for w in data["workers"]:
            # Backfill the cloud session UUID from the worker's output log once
            # it appears (the worker has to start its first turn before the init
            # event is written). Persisted so CCC can resolve worker -> session
            # and link to its conversation. Parsed at most once per worker.
            if not w.get("session_id") and w.get("log"):
                sid = resolve_session_id_from_log(w["log"])
                if sid:
                    w["session_id"] = sid
                    backfilled = True
                    # Ledger it so it survives this worker being pruned later.
                    _add_worker_session_id(sid)
                    _upsert_codex_worker_registry(w)
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
        if not w.get("alive") or _worker_released(w):
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
        if w.get("alive") and not _worker_released(w):
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


def _compact_ref(queue: str, ref: str) -> str:
    prefix = queue or "WT"
    raw = (ref or "").strip()
    if raw:
        head, sep, tail = raw.rpartition("-")
        if sep and tail.isdigit():
            return f"{head or prefix}#{tail}"
        return raw.replace("-", "#", 1)
    return prefix


def display_name(queue: str, ref: Optional[str] = None, summary: Optional[str] = None) -> str:
    """The canonical human-readable label for a worker at some point in its
    claim/close lifecycle, e.g. for a session title or a session-list UI.

    ``ref``/``summary`` absent -> never claimed anything:
    ``"<queue> Queue worker"``. ``ref`` given, no ``summary`` -> holding it
    (or just claimed it): ``"<queue>#<seq>"``. Both given -> closed it:
    ``"<queue>#<seq>: <summary, clipped>"``.

    This is also fed to ``messages.set_session_title`` to actually rename the
    engine session (append a ``custom-title`` event to its transcript) -- see
    ``docs/session-naming.md`` for how that was verified against a live
    session and against CCC's own ``rename_session``."""
    queue = queue or "WT"
    if not ref:
        return f"{queue} Queue worker"
    summary = (summary or "").strip()
    label = _compact_ref(queue, ref)
    if summary:
        return f"{label}: {_clip(summary, 60)}"
    return label


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


def _closed_at_key(item: Dict[str, Any]) -> str:
    """Comparable key for ``closed_at``, robust to mixed types.

    Modern items store ``closed_at`` as an ISO string, but legacy/imported
    records can carry an int epoch (observed once as CCC-236). Comparing an int
    against a str raises ``TypeError`` and used to crash ``wt status`` (WT-93).
    Normalize both to ISO strings so ordering stays chronological.
    """
    raw = item.get("closed_at")
    if isinstance(raw, (int, float)):
        try:
            return datetime.fromtimestamp(raw, tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
        except (ValueError, OverflowError, OSError):
            return ""
    return raw or ""


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
                if prior is None or _closed_at_key(it) > _closed_at_key(prior):
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


def drain_goal(
    queue: str, worker_id: str, repo_path: str = "", engine: str = "claude",
    extra_instructions: str = "",
) -> str:
    """The canonical drain goal text for one worker and engine lifecycle."""
    from . import config
    claim_filter = "".join(f" --type {t}" for t in config.claim_types(queue))
    goal = DRAIN_GOAL_TEMPLATE.format(
        queue=queue, worker_id=worker_id, repo=repo_path or os.getcwd(),
        claim_filter=claim_filter, runbook=str(_WORKER_RUNBOOK_PATH),
        idle_contract=(
            CODEX_IDLE_CONTRACT if engine == "codex"
            else KIMI_IDLE_CONTRACT if engine == "kimi"
            else CLAUDE_IDLE_CONTRACT
        ),
        resume_contract=(
            CODEX_RESUME_CONTRACT if engine == "codex"
            else KIMI_RESUME_CONTRACT if engine == "kimi"
            else CLAUDE_RESUME_CONTRACT
        ).format(runbook=str(_WORKER_RUNBOOK_PATH)),
    )
    extra = (extra_instructions or "").strip()
    if extra:
        goal += (
            "\n\nADDITIONAL INSTRUCTIONS from the dispatcher — apply them to "
            "every ticket you touch in this drain, but never let them override "
            "the claiming, resolution, or stop-signal rules above: " + extra
        )
    return goal


def build_drain_command(
    queue: str, engine: str, worker_id: str, repo_path: str = "", model: str = "",
    goal: str = "", effort: str = "",
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
    means no ``--model`` flag, i.e. the CLI's own configured default. ``effort``
    (queue config, ``wt config --effort``) sets the reasoning budget; empty
    leaves the engine's configured default intact. For
    claude, versioned ids need the full ``claude-`` prefix (``claude-sonnet-5``);
    bare family names (``sonnet``) also work.

    ``goal`` overrides the codex argv goal text (e.g. ``run_once_goal`` for a
    single-ticket spawn, CCC-437) instead of the default drain-loop goal.
    Claude workers ignore it here -- their goal always ships over the stdin
    FIFO after spawn, not in argv.
    """
    bin_name = _ENGINE_BIN.get(engine, engine)
    if engine == "codex":
        argv = [
            bin_name,
            "exec",
            # Match CCC's trusted worker mode. Some Linux hosts cannot run
            # Codex's default sandbox in a way that still lets workers reach
            # local queue tooling, so daemon-spawned queue workers bypass it.
            "--dangerously-bypass-approvals-and-sandbox",
        ]
        if model:
            argv += ["--model", model]
        if effort:
            argv += ["--config", f'model_reasoning_effort="{effort}"']
        argv.append(goal or drain_goal(queue, worker_id, repo_path, engine=engine))
        return argv
    if engine == "kimi":
        # One-shot print mode, like codex exec: the goal rides in argv, stdin
        # is DEVNULL, and the process exits when the drain loop ends. Print
        # mode forces kimi's auto permission mode internally (the CLI rejects
        # --yolo/--auto with -p), and stream-json stdout keeps the worker log
        # machine-readable. Kimi has no --effort flag; queue effort config is
        # ignored for this engine.
        # Kimi parses the value immediately following -p as its prompt.  Flags
        # placed before it make ``stream-json`` look like a subcommand.
        argv = [
            bin_name,
            "-p",
            goal or drain_goal(queue, worker_id, repo_path, engine=engine),
            "--output-format",
            "stream-json",
        ]
        if model:
            argv += ["--model", model]
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
    if effort:
        argv += ["--effort", effort]
    return argv


def request_stop(worker_id: str) -> Path:
    """Release a running worker from queue staffing via a sentinel file.

    The worker's next ``wt claim`` call will detect the file, delete it, and
    return ``{"stop": True}`` instead of claiming more queue work. The
    underlying conversation/process is preserved and may continue unrelated
    work. Uses the file-system only -- does NOT touch workers.json.
    """
    _mark_worker_released(worker_id)
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
    any in_progress ticket whose ``claimed_by`` worker_id is a KNOWN spawned
    worker (present in the tracked worker store, dead or alive) but is no
    longer alive (and that was claimed longer ago than ``grace_s``, to avoid a
    spawn/claim race) is reopened so a fresh worker re-claims it.

    A ``claimed_by`` id that never appears in the worker store at all — e.g. a
    plain ``wt claim --worker <alias>`` run from an ambient Claude session that
    was never launched via ``spawn_workers``/``spawn_run_once_worker`` — has no
    pid this sweep can check, so "not in the live set" would always be true for
    it regardless of whether the session is still working. Treating that as
    orphaned reopened the ticket ~2 minutes after every such claim, handing it
    to a second worker while the original session kept going — duplicate work
    (OPS-104). Those ids are left alone; if the claiming session really did
    die, the ticket just stays in_progress like a `wt block`ed one already does.

    Returns the list of reopened items."""
    from . import queue as _q
    import time as _time
    known_workers = list_workers(prune=False)
    live_ids = {str(w.get("worker_id", "")) for w in known_workers if w.get("alive")}
    # Union with the persistent ledger, not just the current (prunable) store:
    # workers.json drops a dead worker's record on the next routine read, often
    # within seconds, which used to make every genuinely-dead worker look
    # "unknown" here and permanently orphan its ticket (see WORKER_IDS_FILE).
    known_ids = {str(w.get("worker_id", "")) for w in known_workers} | set(_load_worker_id_ledger())
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
        if claimer and claimer not in known_ids:
            continue  # never a spawned worker — no evidence it's dead (OPS-104)
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
            item = _q.update_status(ref, "open", quiet=True, require_status="in_progress",
                                     reason="worker gone")
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
      - no eligible worker        → release verified-idle + reconcile
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
        claim_filter = "".join(f" --type {t}" for t in config.claim_types(queue))
        nudge = (
            f"New ticket {ref} filed on {queue}. Claim it with "
            f"`wt claim -q {queue} --worker <your-id>{claim_filter} --json` and drain the queue."
        )
        delivered = notify_workers(queue, nudge)
        if delivered:
            reason = f"nudged {delivered} live worker(s) — immediate pickup"
            _log("DISPATCH", f"{ref} — {reason}", queue=queue)
            return reason
        # No warm worker: only release workers past the separate lifecycle
        # floor, then reconcile. Cache coldness alone is not a release signal.
        release_idle_workers(queue=queue)
        result = reconcile_once()
        spawned = [r for r in result.get("spawned", []) if r.get("queue") == queue]
        if spawned:
            wids = ", ".join(r.get("worker_id", "?") for r in spawned)
            count = len(spawned)
            reason = (f"spawned {count} worker(s): {wids}" if count > 1
                      else f"spawned worker {wids}")
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
    live_workers = [
        w for w in list_workers(prune=False)
        if w.get("alive") and w.get("session_id")
    ]
    # worker_id -> cloud session_id for live workers that have a resolved UUID.
    wid_to_sid = {str(w.get("worker_id", "")): str(w.get("session_id", ""))
                  for w in live_workers}
    wid_to_worker = {str(w.get("worker_id", "")): w for w in live_workers}
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
                _upsert_codex_worker_registry(
                    wid_to_worker.get(str(it.get("claimed_by") or "")) or {},
                    ref=str(it.get("ref") or ""),
                    title=name,
                )
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
        return max(closed, key=_item_activity_key)
    active = [
        it for it in items
        if it.get("status") == "in_progress" and _matches(it)
    ]
    if active:
        return max(active, key=_item_activity_key)
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


def _item_activity_key(item: Dict[str, Any]) -> tuple:
    try:
        seq = int(item.get("seq") or 0)
    except (TypeError, ValueError):
        seq = 0
    return (_item_activity_ts(item), seq)


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
    candidates: Dict[str, tuple] = {}

    def _offer(worker_id: str, session_id: str, item: Dict[str, Any]) -> None:
        key = _item_activity_key(item)
        current = candidates.get(session_id)
        if current is None or key > current[0]:
            candidates[session_id] = (key, worker_id, session_id, item)

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
        _offer(worker_id, sid, item)
    for item in sorted(items, key=_item_activity_ts):
        sid = str(item.get("claimed_session_id") or "")
        if not sid or _item_activity_ts(item) < cutoff:
            continue
        worker_id = str(item.get("claimed_by") or item.get("closed_by") or sid)
        _offer(worker_id, sid, item)
    for _, worker_id, sid, item in candidates.values():
        row = _session_title_backfill_row(
            worker_id=worker_id,
            session_id=sid,
            item=item,
            dry_run=dry_run,
        )
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
    graceful release is the idle safety net. So ``result["stopped"]`` stays
    empty from the reconciler; the key + its log rendering remain for callers.

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
                              "released": [], "reaped": [], "requeued": [],
                              "backfilled": [],
                              "session_title_backfilled": [],
                              "launch_failed": []}

    # Gracefully release workers only after engine-aware activity clocks are
    # stale past the lifecycle floor. Never derive lifecycle from cache warmth.
    if not dry_run:
        try:
            result["released"] = release_idle_workers()
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
                claim_filter = "".join(
                    f" --type {t}" for t in config.claim_types(q_name)
                )
                nudge = (
                    f"A ticket on {q_name} was reopened after its previous "
                    f"worker died mid-claim. Claim it with `wt claim -q {q_name} "
                    f"--worker <your-id>{claim_filter} --json` and keep draining."
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
        # Repair transcript titles for recent workers whose ticket/session link
        # was missed before close. The title writer is idempotent, so this can
        # safely run on every reconciler tick.
        try:
            result["session_title_backfilled"] = backfill_recent_session_titles()
        except Exception:
            pass

    all_cfg = config.all_queues()
    # Build live-worker counts keyed by queue.
    live_by_queue: Dict[str, List[Dict[str, Any]]] = {}
    for w in list_workers(prune=False):
        if w.get("alive") and not _worker_released(w):
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
        try:
            depth, total_open = _claimable_depth(q_name)
        except Exception as exc:  # A remote queue must not abort every queue.
            result["skipped"].append(
                {
                    "queue": q_name,
                    "reason": (
                        "depth lookup failed: "
                        f"{type(exc).__name__}: {exc}"
                    ),
                }
            )
            continue
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
            # known, and graceful release is the idle safety net.
            continue
        live = live_by_queue.get(q_name, [])
        actual = len(live)

        # A queue can be fully staffed (actual > 0) yet show zero progress --
        # e.g. every live worker's last turn errored out on a transient API or
        # connectivity fault and it's sitting idle mid-session rather than
        # crashed (a crash is caught by the reap+requeue pass above, which
        # frees the slot for a fresh spawn). Detect via the queue's own stuck
        # ground truth (no ticket closed in stuck_minutes despite claimable
        # work) and nudge the live worker(s) to retry/continue -- unless every
        # live worker just started (startup grace, WT-101): a ticket that sat
        # unclaimed for a while reads `stuck` the moment it's staffed, before
        # the fresh worker has had any chance to run its first `wt claim`.
        if (not dry_run and actual > 0
                and (health_by_queue.get(q_name) or {}).get("stuck")
                and not _all_workers_within_startup_grace(live)):
            _maybe_nudge_stuck_queue(q_name, actual)

        if actual < desired:
            # Never spawn more workers than unclaimed tickets. Don't assume a live
            # worker will fail to claim: cap at (depth - actual) to avoid overspawning
            # while workers are still claiming. E.g., 1 ticket + 1 live worker should
            # spawn 0 more, not 1.
            to_spawn = min(desired - actual, max(0, depth - actual))
            from . import queue as _q
            # Peek at the next ticket to get its repo_path; fall back to queue config.
            peeked = _q.peek_next(project=q_name)
            repo_path = (
                config.repo_path(q_name)
                or (peeked or {}).get("repo_path", "")
            )
            engine = config.engine(q_name)
            cooldown = (
                None if dry_run else active_launch_failure_cooldown(q_name, engine)
            )
            if cooldown:
                until = cooldown.get("cooldown_until_human", "later")
                reason = cooldown.get("reason", "recent worker launch failure")
                result["skipped"].append(
                    {"queue": q_name,
                     "reason": f"launch cooldown until {until} — {reason}"}
                )
                continue
            launch_failed: List[Dict[str, Any]] = []
            spawned = spawn_workers(
                q_name, n=to_spawn, engine=engine,
                repo_path=repo_path, dry_run=dry_run,
                launch_failures=launch_failed,
            )
            # Why this spawn happened: open depth + how short of desired we were.
            unclaimed = max(0, depth - actual)
            spawn_reason = (
                f"{depth} open, {actual} live < {desired} desired, {unclaimed} unclaimed, spawn {to_spawn}"
            )
            for rec in spawned:
                rec["spawn_reason"] = spawn_reason
            for rec in launch_failed:
                rec["spawn_reason"] = spawn_reason
            result["spawned"].extend(spawned)
            result["launch_failed"].extend(launch_failed)
        elif actual > desired:
            # Surplus is NOT wound down here. A worker discovers it is surplus at
            # claim time (cmd_claim: nothing claimable AND live>desired) and exits
            # itself; graceful release handles the persistently-idle case. The
            # reconciler no longer pushes a speculative STOP based on a momentary
            # count.
            result["skipped"].append(
                {"queue": q_name,
                 "reason": f"surplus ({actual}>{desired}) — resolved at claim/release"}
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
            eng = w.get("engine", "claude")
            mdl = w.get("model", "")
            engine_label = f"{eng}:{mdl}" if mdl else eng
            if w.get("dry_run"):
                detail = f"{wid} (dry-run; no process started) [{engine_label}]"
                if reason:
                    detail += f" — plan: {reason}"
            else:
                detail = f"{wid} (pid {pid}) [{engine_label}]"
                if reason:
                    detail += f" — {reason}"
            _log("SPAWN", detail, queue=q)
        for w in result.get("stopped", []):
            wid = w.get("worker_id", w) if isinstance(w, dict) else w
            q = (w.get("queue", "") if isinstance(w, dict) else "")
            reason = (w.get("reason", "") if isinstance(w, dict) else "")
            _log("STOP", str(wid) + (f" — {reason}" if reason else ""), queue=q)
        for w in result.get("released", []):
            wid = w.get("worker_id", w) if isinstance(w, dict) else w
            q = (w.get("queue", "") if isinstance(w, dict) else "")
            reason = (w.get("_release_reason", "") if isinstance(w, dict) else "")
            delivered = (w.get("_release_delivered") if isinstance(w, dict) else False)
            detail = str(wid) + (f" — {reason}" if reason else "")
            detail += " — instruction delivered" if delivered else " — stop pending"
            _log("RELEASE", detail, queue=q)
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
    extra_instructions: str = "",
    kind: str = "",
    dry_run: bool = False,
    launch_failures: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """Launch ``n`` worker subprocesses draining ``queue``.

    ``repo_path`` is the tree the workers operate in (defaults to the current
    working directory) — it becomes the subprocess ``cwd`` and is injected into
    the drain goal. Each worker's stdout+stderr go to
    ``~/.watchtower/logs/<worker_id>.log`` so a dead worker leaves a trail
    instead of vanishing into ``/dev/null``. Returns records (with ``pid``).
    ``dry_run`` builds + records the command without spawning (tests).

    ``extra_instructions`` is appended to the drain goal (bespoke spawns that
    need dispatcher guidance beyond the standard drain contract). ``kind``
    tags the worker record (e.g. ``"bespoke"`` for one-off custom spawns) so
    dashboards can tell them from reconciler-spawned drain workers.
    """
    repo_path = repo_path or os.getcwd()
    if not model:
        from . import config
        model = config.model(queue)
    from . import config
    effort = config.effort(queue)
    if _is_fable_model(model):
        import sys
        print(
            f"[watchtower] warning: refusing fable model {model!r} for {queue} worker"
            " — not a coding model; falling back to CLI default",
            file=sys.stderr, flush=True,
        )
        model = ""
    log_dir = WORKERS_FILE.parent / "logs"
    spawned: List[Dict[str, Any]] = []
    for _ in range(n):
        worker_id = f"{queue.lower()}-{uuid.uuid4().hex[:8]}"
        goal = drain_goal(
            queue, worker_id, repo_path, engine=engine,
            extra_instructions=extra_instructions,
        )
        argv = build_drain_command(
            queue, engine, worker_id, repo_path, model, goal=goal, effort=effort,
        )
        logical_bin = _ENGINE_BIN.get(engine, engine)
        if argv and argv[0] == logical_bin:
            resolved_bin = _resolve_engine_bin(engine)
            if resolved_bin:
                argv = [resolved_bin] + argv[1:]
        if dry_run:
            rec = {
                "worker_id": worker_id,
                "pid": 0,
                "queue": queue,
                "engine": engine,
                "repo_path": repo_path,
                "argv": argv,
                "dry_run": True,
            }
            if model:
                rec["model"] = model
            if effort:
                rec["effort"] = effort
            if kind:
                rec["kind"] = kind
            spawned.append(rec)
            continue
        log_dir.mkdir(parents=True, exist_ok=True)
        log_path = log_dir / f"{worker_id}.log"
        logf = open(log_path, "ab")
        # claude workers get a stream-json FIFO stdin so they stay live and
        # pushable; one-shot engines (codex exec, kimi -p) keep the goal in
        # argv + DEVNULL.
        fifo_path = None
        child_stdin_fd = None
        if engine not in _ONE_SHOT_ENGINES:
            fifo_path, child_stdin_fd = _make_stdin_fifo(log_path)
        stdin_arg = child_stdin_fd if child_stdin_fd is not None else subprocess.DEVNULL
        proc = None
        popen_error = None
        try:
            proc = subprocess.Popen(
                argv,
                stdin=stdin_arg,
                stdout=logf,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                cwd=repo_path,
            )
        except OSError as e:
            popen_error = e
            try:
                logf.write(f"LAUNCH FAILED: {e}\n".encode("utf-8"))
                logf.flush()
            except OSError:
                pass
        finally:
            logf.close()  # the child holds its own dup'd fd
            # The child inherited a dup of the RDWR fd as its stdin; the parent's
            # copy is no longer needed (the child's keeps the FIFO from EOF).
            _close_fd_quiet(child_stdin_fd)
        if popen_error is not None or proc is None:
            _unlink_path_quiet(fifo_path)
            reason = f"engine executable unavailable: {popen_error}"
            failure = _record_launch_failure(
                queue=queue,
                engine=engine,
                worker_id=worker_id,
                pid=0,
                log_path=log_path,
                reason=reason,
                model=model,
            )
            if launch_failures is not None:
                launch_failures.append(failure)
            continue
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
        failure = _wait_for_immediate_launch_failure(
            proc,
            queue=queue,
            engine=engine,
            worker_id=worker_id,
            log_path=log_path,
            model=model,
        )
        if failure is not None:
            _unlink_path_quiet(fifo_path)
            if launch_failures is not None:
                launch_failures.append(failure)
            continue
        rec = record_worker(
            proc.pid, queue, engine, worker_id, repo_path, str(log_path),
            fifo=fifo_path or "", model=model, kind=kind,
        )
        rec["argv"] = argv
        spawned.append(rec)
    return spawned


def run_once_goal(queue: str, worker_id: str, ref: str, repo_path: str = "") -> str:
    """The bounded, single-ticket goal for a "drain once" spawn (CCC-437):
    claim exactly ``ref``, resolve it, then stop -- no re-poll drain loop."""
    return RUN_ONCE_GOAL_TEMPLATE.format(
        queue=queue, worker_id=worker_id, ref=ref, repo=repo_path or os.getcwd(),
    )


def spawn_run_once_worker(
    queue: str, ref: str, *, repo_path: str = "", engine: str = "", model: str = "",
) -> Dict[str, Any]:
    """Spawn exactly one worker scoped to a single ticket.

    CCC-437's per-row "drain once" play button on non-auto-drain queue rows.
    Unlike ``spawn_workers``/``dispatch_after_enqueue``, this deliberately
    ignores ``config.auto_drain`` -- a one-click action on a backlog queue is
    the whole point (an auto-drain queue already has, or will get, a worker
    for any open ticket, so the caller only offers this button there). The
    spawned worker gets ``run_once_goal`` instead of ``drain_goal``, so it
    exits after this one ticket instead of looping. Reuses the same
    subprocess/FIFO/log-file plumbing as ``spawn_workers`` so the worker is
    tracked in ``workers.json`` like any other.
    """
    from . import config
    repo_path = repo_path or config.repo_path(queue) or os.getcwd()
    engine = engine or config.engine(queue) or "claude"
    if not model:
        model = config.model(queue)
    effort = config.effort(queue)
    if _is_fable_model(model):
        model = ""
    worker_id = f"{queue.lower()}-{uuid.uuid4().hex[:8]}"
    goal = run_once_goal(queue, worker_id, ref, repo_path)
    argv = build_drain_command(
        queue, engine, worker_id, repo_path, model, goal=goal, effort=effort,
    )
    logical_bin = _ENGINE_BIN.get(engine, engine)
    if argv and argv[0] == logical_bin:
        resolved_bin = _resolve_engine_bin(engine)
        if resolved_bin:
            argv = [resolved_bin] + argv[1:]
    log_dir = WORKERS_FILE.parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{worker_id}.log"
    logf = open(log_path, "ab")
    fifo_path = None
    child_stdin_fd = None
    if engine not in _ONE_SHOT_ENGINES:
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
        logf.close()
        _close_fd_quiet(child_stdin_fd)
    if fifo_path is not None:
        if not write_to_worker_fifo(fifo_path, goal):
            try:
                os.kill(proc.pid, 15)
            except OSError:
                pass
            _close_fd_quiet(None)
    rec = record_worker(
        proc.pid, queue, engine, worker_id, repo_path, str(log_path),
        fifo=fifo_path or "", model=model,
    )
    rec["argv"] = argv
    rec["ref"] = ref
    if effort:
        rec["effort"] = effort
    # WT-103: run-once spawns bypass reconcile(), which is the only other
    # place that writes a SPAWN row -- without this, the "drain once" play
    # button leaves no trace in the activity log at all.
    try:
        from watchtower.queue import _log
        engine_label = f"{engine}:{model}" if model else engine
        _log("SPAWN", f"{worker_id} (pid {proc.pid}) [{engine_label}] — run-once for {ref}", queue=queue)
    except Exception:
        pass
    return rec


# --------------------------------------------------------------- ad-hoc spawns
# `wt spawn` / `wt critique` (WT-100): one-shot agents with an arbitrary goal,
# spawned natively (no CCC dependency) and tracked in workers.json like any
# other worker, but marked kind="adhoc" so queue machinery (reap, reconcile
# nudges) leaves them alone. Reply-to is WT-native: the goal gets a footer
# telling the agent to deliver its report with `wt send <report_to>` -- the
# same delivery path (and outbox fallback) every other WT message uses.

ADHOC_ENGINES = ("claude", "codex", "antigravity", "kimi")

# The footer pipes the report over stdin via a quoted heredoc: a report is
# multi-paragraph markdown full of double quotes, $variables, and `backticks`,
# and putting that inside "..." on a shell command line breaks (quote ends the
# string, $ and ` expand). <<'WT_REPORT' disables all expansion.
ADHOC_REPORT_FOOTER = (
    "\n\nWHEN DONE: deliver your full findings (the complete report text, not "
    "a pointer to a file or session) back to your dispatcher over stdin -- "
    "quote-safe, no shell escaping needed:\n\n"
    "wt send {report_to} - <<'WT_REPORT'\n"
    "<your full report>\n"
    "WT_REPORT\n\n"
    "If the send fails it parks in the WT outbox for later delivery -- do "
    "not retry in a loop. Then end your turn."
)


def build_adhoc_command(
    engine: str, prompt: str, *, model: str = "", repo_path: str = "",
    log_path: str = "",
) -> List[str]:
    """Argv for a one-shot ad-hoc agent. All engines run in print/exec mode
    with the goal in argv -- no FIFO, no drain loop; the process exits when
    the goal is done. Raises ValueError for an engine we can't build, and for
    an engine whose CLI binary isn't installed -- a clean error here beats a
    FileNotFoundError out of Popen."""
    if engine == "codex":
        if not shutil.which(_ENGINE_BIN["codex"]):
            raise ValueError("codex CLI not found on PATH")
        argv = [_ENGINE_BIN["codex"], "exec"]
        if model:
            argv += ["--model", model]
        argv.append(prompt)
        return argv
    if engine == "antigravity":
        bin_path = _resolve_antigravity_bin()
        if not bin_path:
            raise ValueError(
                "antigravity CLI not found (install agy or set "
                "WATCHTOWER_ANTIGRAVITY_BIN)"
            )
        argv = [bin_path, "--dangerously-skip-permissions"]
        if repo_path:
            argv += ["--add-dir", repo_path]
        if log_path:
            # AGY's internal CLI log goes to a sibling file: the .log itself
            # captures the agent's stdout/stderr (spawn_adhoc redirects it),
            # and appending would have produced a `.log.agy.log` orphan.
            agy_log = re.sub(r"\.log$", "", log_path) + ".agy.log"
            argv += ["--log-file", agy_log]
        if model:
            argv += ["--model", model]
        argv += ["-p", prompt]
        return argv
    if engine == "claude":
        if not shutil.which(_ENGINE_BIN["claude"]):
            raise ValueError("claude CLI not found on PATH")
        argv = [_ENGINE_BIN["claude"], "-p",
                "--permission-mode", "bypassPermissions"]
        if model:
            argv += ["--model", model]
        argv.append(prompt)
        return argv
    if engine == "kimi":
        bin_path = _resolve_engine_bin("kimi")
        if not bin_path:
            raise ValueError(
                "kimi CLI not found (install kimi-code or set WATCHTOWER_KIMI_BIN)"
            )
        # Print mode auto-approves internally; --yolo/--auto are rejected
        # with -p by the CLI, so there is no permission flag to pass.
        argv = [bin_path, "-p"]
        if model:
            argv += ["--model", model]
        argv.append(prompt)
        return argv
    raise ValueError(
        f"unsupported engine {engine!r} (supported: {', '.join(ADHOC_ENGINES)})"
    )


def spawn_adhoc(
    prompt: str,
    engine: str = "claude",
    *,
    model: str = "",
    repo_path: str = "",
    name: str = "",
    report_to: str = "",
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Spawn one one-shot ad-hoc agent on ``prompt`` and return its record.

    ``report_to`` (worker id, @agent name, or session UUID) appends the
    WT-native reply-to footer so the agent reports back via `wt send`.
    ``dry_run`` builds + returns the record without spawning (tests).

    ``report_to`` is validated up front via ``messages.resolve_target``: an
    unresolvable target (typo'd worker id, unregistered @agent name) errors
    here instead of only surfacing minutes later when the spawned agent's own
    `wt send` fails and silently drops the report -- unresolvable targets
    don't get parked in the outbox (nothing to retry against), so without
    this check the report is just lost (OPS-89)."""
    repo_path = repo_path or os.getcwd()
    if report_to:
        from . import messages
        try:
            messages.resolve_target(report_to)
        except ValueError as e:
            raise ValueError(f"--report-to {report_to!r} is unresolvable: {e}") from e
    if report_to:
        # shlex.quote: the target is interpolated into a shell command the
        # agent runs verbatim -- a resolvable but hostile target ("x; rm ...")
        # must arrive as one inert token, not as shell syntax. Normal targets
        # (UUIDs, @names, worker ids) are unchanged by quoting.
        prompt = prompt + ADHOC_REPORT_FOOTER.format(
            report_to=shlex.quote(report_to)
        )
    label = re.sub(r"[^a-z0-9]+", "-", (name or "adhoc").lower()).strip("-") or "adhoc"
    worker_id = f"{label}-{engine}-{uuid.uuid4().hex[:8]}"
    log_dir = WORKERS_FILE.parent / "logs"
    log_path = log_dir / f"{worker_id}.log"
    argv = build_adhoc_command(
        engine, prompt, model=model, repo_path=repo_path, log_path=str(log_path),
    )
    if dry_run:
        rec = {
            "worker_id": worker_id, "pid": 0, "queue": label.upper(),
            "engine": engine, "repo_path": repo_path, "argv": argv,
            "kind": "adhoc", "dry_run": True,
        }
        if model:
            rec["model"] = model
        return rec
    log_dir.mkdir(parents=True, exist_ok=True)
    logf = open(log_path, "ab")
    try:
        proc = subprocess.Popen(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=logf,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            cwd=repo_path,
        )
    finally:
        logf.close()
    rec = record_worker(
        proc.pid, label.upper(), engine, worker_id, repo_path, str(log_path),
        model=model, kind="adhoc",
    )
    rec["argv"] = argv
    return rec

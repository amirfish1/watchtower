#!/usr/bin/env python3
"""Cross-agent messaging: agents registry, delivery adapters, durable outbox, ask.

This module gives WatchTower its conversational primitives (see
``docs/messaging-design.md``):

  * ``send(target, text)``: push a message to a worker, a registered agent, or
    a raw claude session. Delivery falls through an ordered adapter chain:

      1. ``fifo``: the target is a live WT worker with a stream-json FIFO
         stdin; reuse ``workers.write_to_worker_fifo``. Cheapest path.
      2. ``tty`` (claude targets only, WT-55): the target session's TUI is
         LIVE in a terminal — a running ``claude --resume <sid>`` process on
         a real tty — so type the message into that tty via AppleScript
         (``watchtower.tty``, iTerm2/Terminal.app). Must sit BEFORE resume:
         a parallel headless resume against a live TUI would fork the
         conversation, and the resume busy-check would otherwise park these
         messages in the outbox (a live TUI keeps its transcript mtime hot).
      3. ``resume``: headless ``claude -p --resume <sid>`` with a fresh FIFO
         stdin, output logged to ``~/.watchtower/logs/msg-<sid8>-<ts>.log``.
         Guarded by a busy check: if the target session's transcript under
         ``~/.claude/projects/*/<sid>.jsonl`` was modified within the last
         ``$WATCHTOWER_BUSY_WINDOW_S`` seconds (default 120), the session is
         actively mid-turn, so we do NOT fork a parallel resume; the message
         is held in the outbox and delivered once the transcript goes quiet.
      4. Codex broker/private split (codex targets only): if a delegate is
         configured or auto-detected, WT treats CCC as the local managed Codex
         broker and delegates first. Only standalone WT starts its own private
         ``codex app-server`` JSON-RPC subprocess (see ``watchtower.codex_rpc``).
         This avoids two independent app-server owners resuming the same
         thread. The target's ``session_id`` is used as the codex thread id 1:1
         (see WT-54).
      5. ``delegate`` (optional, last): POST to a delegate HTTP endpoint
         (CCC's ``/api/inject-input``) for transports WT cannot do natively.
         ``$WATCHTOWER_DELEGATE_URL`` overrides; ``off`` disables even when
         ``~/.claude/command-center/port.txt`` exists. No delegate configured
         is a fully working standalone setup.

  * Outbox (``$WATCHTOWER_OUTBOX_FILE``, default ``~/.watchtower/outbox.json``):
    durable at-least-once store for messages that could not be delivered. The
    daemon drains it each tick with exponential backoff (30s base, 10 min
    cap); after 20 attempts or optional TTL expiry a message goes ``dead`` and
    is logged (DEADMSG).

  * ``ask(target, text)``: synchronous question. Correlation is byte-offset
    tailing: snapshot the log size before delivery, then read only the new
    bytes, accumulating assistant text until a ``{"type": "result"}`` event or
    a terminal ``stop_reason`` arrives.

  * Agents registry (``$WATCHTOWER_AGENTS_FILE``, default
    ``~/.watchtower/agents.json``): friendly names for session UUIDs, schema
    ``{name: {session_id, engine, cwd, registered_at, last_seen}}``.

  * Discovery: reachability and listing are two different questions. ANY
    session transcript on disk is reachable by UUID (the resume adapter can
    wake a dormant session), but ``list_agents`` shows the useful working
    set: the registry plus auto-discovered recent sessions, transcripts whose
    mtime falls within the last ``$WATCHTOWER_AGENTS_WINDOW_DAYS`` days
    (default 3). The window scan is stat-only (no transcript contents are
    read) and runs ONLY from ``list_agents(include_recent=True)`` and from
    UUID-prefix resolution, never from the daemon tick or delivery paths
    (``deliver`` / ``drain_outbox``), so thousands of older transcripts cost
    nothing.

All state-file paths are resolved at call time (never import-time constants)
so tests can override them via the environment. Writes are atomic
(tmp + ``os.replace``) and outbox/registry mutations are serialised with the
same ``fcntl`` file lock the queue store uses. Stdlib only.
"""

from __future__ import annotations

import json
import os
import re
import shlex
import signal
import ssl
import subprocess
import time
import urllib.request
import uuid as _uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from . import queue as queue_mod
from . import tty as tty_mod
from . import workers

# Retry policy for the durable outbox.
BACKOFF_BASE_S = 30
BACKOFF_CAP_S = 600
MAX_ATTEMPTS = 20
# How far out to schedule a message deferred by the resume busy check: the
# target is mid-turn now, so retry once it has had a chance to go idle.
BUSY_HOLD_S = 60

# A stream-json record with one of these stop reasons ends the turn.
_TERMINAL_STOP_REASONS = ("end_turn", "stop_sequence", "max_tokens")

_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
# The shape workers.spawn_workers mints: "<queue>-<8 hex>" (e.g. ccc-40546374).
# Agent names must not collide with it, or resolve_target order gets ambiguous.
_WORKER_ID_SHAPE = re.compile(r"^[a-z0-9][a-z0-9_-]*-[0-9a-f]{8}$")
# A candidate session-id prefix: hex plus dashes, at least 8 chars.
_HEX_PREFIX_RE = re.compile(r"^[0-9a-fA-F][0-9a-fA-F-]{7,}$")
_AGENT_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


# ------------------------------------------------------------------ path helpers
def _agents_file() -> Path:
    return Path(
        os.environ.get("WATCHTOWER_AGENTS_FILE")
        or (Path.home() / ".watchtower" / "agents.json")
    ).expanduser()


def _outbox_file() -> Path:
    return Path(
        os.environ.get("WATCHTOWER_OUTBOX_FILE")
        or (Path.home() / ".watchtower" / "outbox.json")
    ).expanduser()


def _agents_lock() -> Path:
    return _agents_file().with_suffix(".lock")


def _outbox_lock() -> Path:
    return _outbox_file().with_suffix(".lock")


def _logs_dir() -> Path:
    """Resume-adapter logs live next to the outbox (~/.watchtower/logs by
    default), which keeps tests fully sandboxed via $WATCHTOWER_OUTBOX_FILE."""
    return _outbox_file().parent / "logs"


def _resume_ledger_file() -> Path:
    """Ledger of resume children wt itself spawned (WT-82). Lives next to the
    outbox so tests sandbox it via $WATCHTOWER_OUTBOX_FILE."""
    return _outbox_file().parent / "resume-children.json"


def _resume_ledger_lock() -> Path:
    return _resume_ledger_file().with_suffix(".lock")


def _claude_projects_root() -> Path:
    """Where claude session transcripts live; env-overridable for tests."""
    return Path(
        os.environ.get("WATCHTOWER_CLAUDE_PROJECTS_DIR")
        or (Path.home() / ".claude" / "projects")
    ).expanduser()


def _busy_window_s() -> float:
    raw = os.environ.get("WATCHTOWER_BUSY_WINDOW_S", "")
    try:
        return float(raw) if raw else 120.0
    except ValueError:
        return 120.0


def _delegate_base() -> str:
    """Resolve the delegate base URL, or '' when no delegate is configured.

    $WATCHTOWER_DELEGATE_URL wins; the literal value 'off' disables the
    delegate even if a CCC port file exists. With no env var, a readable
    ``~/.claude/command-center/port.txt`` containing either a port number or
    a loopback base URL auto-detects a local CCC. No delegate at all is a
    fully supported configuration."""
    env = (os.environ.get("WATCHTOWER_DELEGATE_URL") or "").strip()
    if env:
        return "" if env.lower() == "off" else env.rstrip("/")
    port_file = Path.home() / ".claude" / "command-center" / "port.txt"
    try:
        value = port_file.read_text().strip()
    except OSError:
        return ""
    try:
        return f"http://127.0.0.1:{int(value)}"
    except ValueError:
        parsed = urlparse(value)
        try:
            is_loopback_url = (
                parsed.scheme in ("http", "https")
                and parsed.hostname in ("127.0.0.1", "localhost", "::1")
                and parsed.port is not None
                and not parsed.path.rstrip("/")
                and not parsed.params
                and not parsed.query
                and not parsed.fragment
            )
        except ValueError:
            is_loopback_url = False
        return value.rstrip("/") if is_loopback_url else ""


def _codex_delegate_first_enabled() -> bool:
    raw = os.environ.get("WATCHTOWER_CODEX_DELEGATE_FIRST", "1").strip().lower()
    return raw not in ("0", "false", "no", "off")


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso(s: Any) -> float:
    try:
        return (
            datetime.strptime(str(s), "%Y-%m-%dT%H:%M:%SZ")
            .replace(tzinfo=timezone.utc)
            .timestamp()
        )
    except (ValueError, TypeError):
        return 0.0  # malformed timestamps become immediately due


# --------------------------------------------------------------- agents registry
def _load_agents() -> Dict[str, Dict[str, Any]]:
    try:
        with open(_agents_file(), "r") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _save_agents(data: Dict[str, Dict[str, Any]]) -> None:
    path = _agents_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = str(path) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


def register_agent(
    name: str, session_id: str, engine: str = "claude", cwd: str = ""
) -> Dict[str, Any]:
    """Register (or re-register) a friendly name for a session UUID.

    Names must not look like a WT worker id (``<queue>-<8hex>``) or a UUID,
    since those shapes are claimed by earlier resolve_target steps. Names are
    restricted to a shell-safe charset: registered names end up interpolated
    into shell commands (e.g. the adhoc report footer's `wt send @name`), so
    a name with spaces or shell metacharacters is an injection foothold."""
    name = str(name or "").lstrip("@").strip()
    if not name:
        raise ValueError("agent name is required")
    if not _AGENT_NAME_RE.match(name):
        raise ValueError(
            f"agent name {name!r} has characters outside [A-Za-z0-9._-]; "
            "pick a shell-safe name"
        )
    if _WORKER_ID_SHAPE.match(name):
        raise ValueError(
            f"agent name {name!r} collides with the worker-id shape "
            "(<queue>-<8 hex chars>); pick a different name"
        )
    if _UUID_RE.match(name):
        raise ValueError(f"agent name {name!r} looks like a session UUID; pick a name")
    sid = str(session_id or "").strip()
    if not _UUID_RE.match(sid):
        raise ValueError(f"session_id must be a full UUID, got {sid!r}")
    rec = {
        "session_id": sid,
        "engine": str(engine or "claude"),
        "cwd": str(cwd or ""),
        "registered_at": _now_iso(),
        "last_seen": _now_iso(),
    }
    with queue_mod._FileLock(_agents_lock()):
        agents = _load_agents()
        prior = agents.get(name)
        if isinstance(prior, dict) and prior.get("registered_at"):
            rec["registered_at"] = prior["registered_at"]
        agents[name] = rec
        _save_agents(agents)
    if str(rec["engine"]).lower() == "codex":
        try:
            from . import codex_registry
            codex_registry.upsert(
                sid,
                source="wt-agents",
                visibility="registered-agent",
                cwd=rec.get("cwd") or "",
                name=name,
                title=name,
                wt={"agent_name": name, "registered_at": rec["registered_at"]},
            )
        except Exception:
            pass
    return {"name": name, **rec}


def remove_agent(name: str) -> bool:
    """Drop a name from the registry. Returns True if it existed."""
    name = str(name or "").lstrip("@").strip()
    with queue_mod._FileLock(_agents_lock()):
        agents = _load_agents()
        if name not in agents:
            return False
        del agents[name]
        _save_agents(agents)
    return True


def _recent_window_days() -> float:
    raw = os.environ.get("WATCHTOWER_AGENTS_WINDOW_DAYS", "")
    try:
        return float(raw) if raw else 3.0
    except ValueError:
        return 3.0


def recent_sessions(window_days: Optional[float] = None) -> List[Dict[str, Any]]:
    """Auto-discover recently active sessions: a stat-only pass over the
    transcript archive (``<projects dir>/*/<uuid>.jsonl``).

    Returns ``[{"session_id", "cwd_slug", "last_active_epoch"}]`` for every
    transcript whose mtime is within the last ``window_days`` days (default
    3, ``$WATCHTOWER_AGENTS_WINDOW_DAYS``), newest first. No file contents
    are ever read; this is directory listing + stat only. Callers on hot or
    daemon paths must NOT use this (see the module docstring boundary)."""
    if window_days is None:
        window_days = _recent_window_days()
    cutoff = time.time() - float(window_days) * 86400.0
    root = _claude_projects_root()
    out: List[Dict[str, Any]] = []
    try:
        project_dirs = [d for d in root.iterdir() if d.is_dir()]
    except OSError:
        return []
    for d in project_dirs:
        try:
            entries = list(d.iterdir())
        except OSError:
            continue
        for p in entries:
            if p.suffix != ".jsonl" or not _UUID_RE.match(p.stem):
                continue
            try:
                mtime = p.stat().st_mtime
            except OSError:
                continue
            if mtime >= cutoff:
                out.append({
                    "session_id": p.stem,
                    "cwd_slug": d.name,
                    "last_active_epoch": mtime,
                })
    out.sort(key=lambda r: -float(r["last_active_epoch"]))
    return out


def list_agents(include_recent: bool = True) -> List[Dict[str, Any]]:
    """The addressable working set: registry entries (kind ``agent``), plus,
    when ``include_recent`` is set, auto-discovered recent sessions (kind
    ``recent``) deduped against registry session_ids and live workers'
    session_ids. Callers merge live workers themselves (``wt agents``)."""
    agents = _load_agents()
    out: List[Dict[str, Any]] = []
    for name in sorted(agents):
        rec = agents[name]
        if isinstance(rec, dict):
            out.append({"name": name, "kind": "agent", **rec})
    if not include_recent:
        return out
    known = {str(r.get("session_id") or "") for r in out}
    for w in workers.list_workers(prune=False):
        if w.get("alive") and w.get("session_id"):
            known.add(str(w["session_id"]))
    for r in recent_sessions():
        if r["session_id"] in known:
            continue
        known.add(r["session_id"])
        out.append({"kind": "recent", **r})
    return out


# ------------------------------------------------------------- target resolution
def _worker_for_session(
    live: List[Dict[str, Any]], sid: Optional[str]
) -> Optional[Dict[str, Any]]:
    if not sid:
        return None
    for w in live:
        if str(w.get("session_id") or "") == sid:
            return w
    return None


def _known_sessions(
    live: List[Dict[str, Any]], agents: Dict[str, Dict[str, Any]]
) -> Dict[str, str]:
    """All session UUIDs we know about, mapped to their engine."""
    known: Dict[str, str] = {}
    for name, rec in agents.items():
        if isinstance(rec, dict) and rec.get("session_id"):
            known[str(rec["session_id"])] = str(rec.get("engine") or "claude")
    for w in live:  # workers win on engine when both know the sid
        if w.get("session_id"):
            known[str(w["session_id"])] = str(w.get("engine") or "claude")
    return known


def resolve_target(target: str, include_recent: bool = True) -> Dict[str, Any]:
    """Resolve a target string to a delivery descriptor.

    Returns ``{"kind": "worker"|"agent"|"session", "session_id": str|None,
    "worker": dict|None, "engine": str}``. Resolution order:

      1. exact live worker_id match,
      2. agents registry name (a leading ``@`` is allowed),
      3. raw session UUID, or a unique hex prefix (>= 8 chars) of a known
         session; an unknown value is accepted only as a full 36-char UUID.

    Known sessions for prefix matching are live workers + the registry, plus
    (when ``include_recent``, the default) the recent-transcript window from
    ``recent_sessions``. Daemon paths pass ``include_recent=False`` so the
    tick never enumerates the transcript archive.

    Raises ``ValueError`` for empty, ambiguous, or unresolvable targets."""
    t = str(target or "").strip()
    if not t:
        raise ValueError("target is required")
    live = [w for w in workers.list_workers(prune=False) if w.get("alive")]
    for w in live:
        if str(w.get("worker_id") or "") == t:
            return {
                "kind": "worker",
                "session_id": str(w.get("session_id") or "") or None,
                "worker": w,
                "engine": str(w.get("engine") or "claude"),
                "cwd": str(w.get("cwd") or ""),
            }
    agents = _load_agents()
    name = t.lstrip("@")
    rec = agents.get(name)
    if isinstance(rec, dict):
        sid = str(rec.get("session_id") or "") or None
        return {
            "kind": "agent",
            "session_id": sid,
            "worker": _worker_for_session(live, sid),
            "engine": str(rec.get("engine") or "claude"),
            "cwd": str(rec.get("cwd") or ""),
        }
    if _HEX_PREFIX_RE.match(t):
        known = _known_sessions(live, agents)
        if include_recent:
            for r in recent_sessions():
                known.setdefault(str(r["session_id"]), "claude")
        matches = sorted(
            {sid for sid in known if sid.lower().startswith(t.lower())}
        )
        if len(matches) > 1:
            short = ", ".join(m[:13] for m in matches)
            raise ValueError(f"ambiguous session prefix {t!r} (matches: {short})")
        if len(matches) == 1:
            sid = matches[0]
            return {
                "kind": "session",
                "session_id": sid,
                "worker": _worker_for_session(live, sid),
                "engine": known.get(sid, "claude"),
            }
        if _UUID_RE.match(t):
            # Nothing known matches, but a full UUID is a valid address as-is.
            # "known": False marks that the engine is an assumption (claude),
            # not a fact -- callers holding a non-claude UUID should register
            # it with its real engine instead of trusting this default.
            return {"kind": "session", "session_id": t, "worker": None,
                    "engine": "claude", "known": False}
    raise ValueError(
        f"unknown target {t!r}: not a live worker id, registered agent name, "
        "or known/full session UUID"
    )


# ------------------------------------------------------------- delivery adapters
def _deliver_fifo(resolved: Dict[str, Any], text: str) -> Dict[str, Any]:
    """Adapter 1: push a stream-json user line to a live worker's FIFO."""
    w = resolved.get("worker") or {}
    fifo = str(w.get("fifo") or "")
    if not fifo:
        return {"ok": False, "error": "no live worker fifo for target"}
    if workers.write_to_worker_fifo(fifo, text):
        return {"ok": True, "transport": "fifo"}
    return {"ok": False, "error": "fifo write failed (worker not listening)"}


def _find_transcript(sid: str) -> Optional[Path]:
    """Locate a session's transcript under the claude projects dir, if any."""
    root = _claude_projects_root()
    try:
        project_dirs = [d for d in root.iterdir() if d.is_dir()]
    except OSError:
        return None
    for d in project_dirs:
        p = d / f"{sid}.jsonl"
        if p.exists():
            return p
    return None


def _find_codex_rollout(sid: str) -> Optional[Path]:
    """Locate the newest durable Codex rollout for a thread, if any."""
    codex_home = Path(os.environ.get("CODEX_HOME") or (Path.home() / ".codex"))
    newest: Optional[Path] = None
    newest_mtime = -1.0
    try:
        paths = codex_home.joinpath("sessions").glob(f"*/*/*/*{sid}.jsonl")
        for path in paths:
            try:
                mtime = path.stat().st_mtime
            except OSError:
                continue
            if mtime > newest_mtime:
                newest = path
                newest_mtime = mtime
    except OSError:
        return None
    return newest


def _latest_session_title(path: Path) -> str:
    """Return the last explicit Claude title/agent-name in a transcript."""
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return ""
    for line in reversed(lines):
        line = line.strip()
        if not line:
            continue
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            continue
        if ev.get("type") == "custom-title":
            return str(ev.get("customTitle") or "")
        if ev.get("type") == "agent-name":
            return str(ev.get("agentName") or "")
    return ""


def set_session_title(sid: str, name: str) -> bool:
    """Rename a claude session by appending a ``custom-title`` event to its
    own transcript -- the exact event shape Claude writes for the in-session
    ``/rename`` command (verified against ``claude-command-center``'s
    ``rename_session``/``_append_custom_title``, which docstrings this as
    "the exact shape Claude writes when you run /rename" and confirms
    ``claude --resume``'s picker reads it back). No CCC dependency: this is a
    plain, atomic (``O_APPEND``) file write, safe even while the target
    session is live and writing its own turns to the same file.

    Returns False (no-op) when the session has no transcript on disk yet --
    e.g. a non-claude engine, or a session id that hasn't flushed its first
    turn -- so callers (``wt claim``/``wt close``) can treat this as
    best-effort and never fail the ticket operation over it."""
    path = _find_transcript(sid)
    if path is None or not name:
        return False
    if _latest_session_title(path) == name:
        return False
    event = {"type": "custom-title", "customTitle": name, "sessionId": sid}
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write("\n" + json.dumps(event) + "\n")
    except OSError:
        return False
    return True


def _session_busy(sid: str) -> bool:
    """True when the session's transcript was written within the busy window,
    meaning the session is actively mid-turn: forking a parallel resume would
    race it, so the caller must hold the message for a later retry."""
    return session_state(sid) == "busy"


def session_state(sid: str, now: Optional[float] = None) -> str:
    """Return busy, idle, or unknown from one targeted transcript stat."""
    p = _find_transcript(sid)
    if p is None:
        return "unknown"
    try:
        mtime = p.stat().st_mtime
    except OSError:
        return "unknown"
    now = time.time() if now is None else float(now)
    return "busy" if (now - mtime) < _busy_window_s() else "idle"


def _displaced_claim_for_session(sid: str) -> Optional[Dict[str, str]]:
    """If ``sid`` was a worker that got reaped mid-ticket and its claim was
    taken over, return ``{"ref", "reason"}`` describing the displacement; else
    None. Used to guard a headless resume so a revived worker does not silently
    resume onto -- and re-commit -- work another worker already redrained.

    Signal (false-positive-free by construction): the ticket still records this
    session as owner (``claimed_session_id == sid``) AND its history carries a
    ``reopen`` event with reason "worker gone" (the reaper's mark -- a normally
    closed ticket never has one) AND it is no longer actively in_progress under
    this session (status is ``open`` = reopened, or ``closed`` = reassigned and
    closed by another). A worker that finished its own ticket has no reopen
    event, so it is never flagged; one that was reaped-then-legitimately
    reclaimed by ITSELF is back to in_progress, so it is not flagged either.

    Best-effort: any lookup failure returns None -- the guard must never break
    delivery."""
    if not sid:
        return None
    try:
        from . import queue as _q
        items = _q.list_items()
    except Exception:
        return None
    for it in items:
        if str(it.get("claimed_session_id") or "") != sid:
            continue
        status = it.get("status")
        if status not in ("open", "closed"):
            continue  # in_progress under this session -> still legitimately theirs
        reaped = any(
            e.get("event") == "reopen"
            and "worker gone" in str(e.get("reason") or "").lower()
            for e in (it.get("history") or [])
        )
        if not reaped:
            continue  # closed/released normally, not taken away
        reason = ("reopened after you were reaped for idling"
                  if status == "open"
                  else "reassigned and closed by another worker after you were reaped")
        return {"ref": str(it.get("ref") or "?"), "reason": reason}
    return None


def _stale_claim_stop_prefix(sid: str) -> str:
    """A hard STOP directive to prepend to a resumed worker's message when its
    ticket was taken from it (see ``_displaced_claim_for_session``). Empty when
    the session holds no displaced claim."""
    d = _displaced_claim_for_session(sid)
    if not d:
        return ""
    ref = d["ref"]
    return (
        f"[WATCHTOWER] STOP before you continue. Your ticket {ref} is no longer "
        f"yours: {d['reason']}. Another worker has taken it over. Do NOT resume "
        f"that work, do NOT commit, and do NOT close {ref} (a duplicate close is "
        f"rejected). Discard any uncommitted changes for {ref}, then go back to "
        f"`wt claim` for fresh work.\n\n"
    )


def _deliver_resume(resolved: Dict[str, Any], text: str) -> Dict[str, Any]:
    """Adapter 2: wake an idle claude session headless and hand it the message.

    Spawns ``claude -p --resume <sid>`` in stream-json mode with a fresh FIFO
    stdin (same keep-alive trick as workers), logs its output to
    ``msg-<sid8>-<ts>.log``, writes the message as the first stream-json user
    line, and does not track the process further. Refuses (with ``busy``) when
    the target session looks actively mid-turn."""
    sid = str(resolved.get("session_id") or "")
    if resolved.get("engine") != "claude" or not sid:
        return {"ok": False, "error": "resume needs a claude session_id"}
    if _find_transcript(sid) is None:
        return {
            "ok": False,
            "error": f"resume target transcript not found for session {sid[:8]}",
        }
    # Reap-displacement guard: if this session was a worker reaped mid-ticket
    # and its claim was taken over, waking it back into that half-done work is
    # how the same ticket gets fixed and committed twice (the reaped session
    # resumes with warm context that predates the takeover). Prepend a hard STOP
    # so the revived worker drops the stale work and re-claims instead. We still
    # deliver -- the worker must wake to act on the STOP. Never blocks a normal
    # (non-displaced) resume, so human/chat/orchestrator messages pass through.
    _stop_prefix = _stale_claim_stop_prefix(sid)
    if _stop_prefix:
        text = _stop_prefix + text
    if _session_busy(sid):
        return {
            "ok": False,
            "busy": True,
            "error": f"session {sid[:8]} is mid-turn (transcript active); "
                     "holding for delivery on idle",
        }
    log_dir = _logs_dir()
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        return {"ok": False, "error": f"cannot create log dir: {e}"}
    ts = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    log_path = log_dir / f"msg-{sid[:8]}-{ts}.log"
    argv = [
        "claude", "-p", "--verbose",
        "--resume", sid,
        "--input-format", "stream-json",
        "--output-format", "stream-json",
        "--permission-mode", "bypassPermissions",
    ]
    # Spawn in the session's own project directory: claude scopes --resume
    # lookups to the cwd's project bucket, so any other cwd fails the resume.
    cwd = _session_cwd_from_transcript(sid) or str(resolved.get("cwd") or "")
    if cwd and not os.path.isdir(cwd):
        cwd = ""
    if cwd:
        # WT-76: resume also requires the transcript to live in the cwd's
        # project bucket — move it there if it's stranded elsewhere.
        _ensure_transcript_in_cwd_bucket(sid, cwd)
    try:
        logf = open(log_path, "ab")
        try:
            proc = subprocess.Popen(
                argv,
                stdin=subprocess.PIPE,
                stdout=logf,
                stderr=subprocess.STDOUT,
                start_new_session=True,
                cwd=cwd or None,
            )
        finally:
            logf.close()
    except OSError as e:
        return {"ok": False, "error": f"claude resume spawn failed: {e}"}
    # One-shot delivery: write the message, then CLOSE stdin so the child
    # sees EOF and exits once its turn completes. This is deliberately NOT
    # the workers' keep-alive FIFO pattern: wt never reuses a resume child,
    # and a lingering idle child squats on the session as a foreign live
    # writer — observed 2026-07-02, when a resume child from hours earlier
    # sat holding its stdin open and every subsequent CCC/wt delivery to
    # that session was parked "until it finishes" (i.e. never), swallowing
    # user text.
    try:
        proc.stdin.write(workers._stream_json_user_line(text))
        proc.stdin.flush()
        proc.stdin.close()
    except (BrokenPipeError, OSError):
        return {"ok": False, "error": "resume stdin write failed"}
    # WT-82: record the child in the wt-owned ledger so the daemon's reap
    # pass can terminate it if it ever outlives its completed turn. The EOF
    # above is the primary exit mechanism; the ledger is defense-in-depth.
    _resume_ledger_add(proc.pid, sid, str(log_path))
    # Boot verification: a resume that can't load the session (wrong cwd,
    # missing transcript) exits within a second or two. Reporting ok on a
    # dead child silently drops the message — watch the verify window and
    # surface the death so send() parks the text for retry instead.
    deadline = time.time() + _resume_verify_window_s()
    poll = getattr(proc, "poll", None)
    while callable(poll) and time.time() < deadline:
        rc = poll()
        if rc is not None:
            return {
                "ok": False,
                "error": f"claude resume exited rc={rc} at boot "
                         f"(see {log_path})",
            }
        time.sleep(0.1)
    return {"ok": True, "transport": "resume", "log": str(log_path)}


def _session_cwd_from_transcript(sid: str) -> Optional[str]:
    """Read the session's working directory out of its own transcript.

    ``claude --resume`` only finds a session when run from that session's
    project directory; spawned from an unrelated cwd it dies instantly with
    "No conversation found with session ID". Transcript events carry the
    authoritative ``cwd`` field, so scan the first lines for one."""
    p = _find_transcript(sid)
    if p is None:
        return None
    try:
        with open(p, "r", encoding="utf-8", errors="replace") as f:
            for _ in range(50):
                line = f.readline()
                if not line:
                    break
                try:
                    ev = json.loads(line)
                except json.JSONDecodeError:
                    continue
                cwd = ev.get("cwd") if isinstance(ev, dict) else None
                if cwd and os.path.isdir(str(cwd)):
                    return str(cwd)
    except OSError:
        return None
    return None


def _encode_project_slug(path: str) -> str:
    """Encode an absolute path the way claude-code names its project buckets
    under ~/.claude/projects/ (every non-alphanumeric character becomes '-')."""
    return re.sub(r"[^A-Za-z0-9]", "-", str(path))


def _ensure_transcript_in_cwd_bucket(sid: str, cwd: str) -> None:
    """Move the session transcript into the project bucket `claude --resume`
    will search from ``cwd`` (port of CCC's _ensure_session_jsonl_for_cwd).

    A session accidentally launched from "/" (or whose repo moved) keeps its
    jsonl in a bucket that doesn't match its real working directory — resume
    then fails with "No conversation found" even when spawned in the right
    cwd. Best-effort: any failure leaves the transcript where it was."""
    try:
        slug = _encode_project_slug(str(Path(cwd).expanduser().resolve()))
    except (OSError, ValueError, RuntimeError):
        return
    dest = _claude_projects_root() / slug / f"{sid}.jsonl"
    try:
        if dest.is_file():
            return
    except OSError:
        return
    src = _find_transcript(sid)
    if src is None or src == dest:
        return
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        os.rename(src, dest)
    except OSError:
        return


def _resume_verify_window_s() -> float:
    """How long to watch a fresh resume child for boot-time death."""
    try:
        return float(os.environ.get("WATCHTOWER_RESUME_VERIFY_S", "2.0"))
    except ValueError:
        return 2.0


# ------------------------------------------------------ resume-child reaper
# WT-82: post-6ab1aaf resume children exit via stdin EOF once their turn
# completes, so under normal operation this whole section is a no-op — the
# liveness check just clears dead pids from the ledger. It exists because the
# pre-fix failure mode (a lingering idle child squatting on a session as a
# foreign live writer, silently swallowing every later delivery) is exactly
# the kind of regression that would otherwise soak unnoticed again.
#
# Hard rule: NEVER signal a pid outside the ledger. CCC legitimately keeps
# ITS resume children alive for reuse and they are indistinguishable from
# ours in ps — ledger membership is the only safe ownership proof.

def _reap_idle_s() -> float:
    """How long a finished child's log must sit untouched before SIGTERM."""
    try:
        return float(os.environ.get("WATCHTOWER_REAP_IDLE_S", "600"))
    except ValueError:
        return 600.0


def _load_resume_ledger() -> List[Dict[str, Any]]:
    try:
        with open(_resume_ledger_file(), "r") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return []
    return data if isinstance(data, list) else []


def _save_resume_ledger(entries: List[Dict[str, Any]]) -> None:
    path = _resume_ledger_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = str(path) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(entries, f, indent=2)
    os.replace(tmp, path)


def _resume_ledger_add(pid: int, sid: str, log_path: str) -> None:
    """Best-effort: a ledger write failure must never fail the delivery."""
    try:
        with queue_mod._FileLock(_resume_ledger_lock()):
            entries = _load_resume_ledger()
            entries.append({
                "pid": int(pid),
                "sid": sid,
                "log": log_path,
                "spawned_at": _now_iso(),
            })
            _save_resume_ledger(entries)
    except OSError:
        pass


def _pid_command(pid: int) -> str:
    """argv of a live pid via ps, '' on any failure. Uses the import-time
    Popen binding (tty module) so tests faking subprocess.Popen module-wide
    don't intercept — and can fake this helper directly instead."""
    proc = None
    try:
        proc = tty_mod._REAL_POPEN(
            ["ps", "-o", "command=", "-p", str(pid)],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
        )
        out, _ = proc.communicate(timeout=2)
        return (out or "").strip()
    except subprocess.TimeoutExpired:
        try:
            proc.kill()
            proc.communicate(timeout=1)
        except Exception:  # noqa: BLE001 - best-effort reap
            pass
        return ""
    except (FileNotFoundError, OSError):
        return ""


def _log_shows_terminal(log_path: str) -> bool:
    """True when the child's stream-json log contains a completed turn: a
    ``result`` record or a terminal ``stop_reason``."""
    try:
        with open(log_path, "rb") as f:
            data = f.read()
    except OSError:
        return False
    for raw in data.splitlines():
        raw = raw.strip()
        if not raw:
            continue
        try:
            rec = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue
        if not isinstance(rec, dict):
            continue
        if rec.get("type") == "result":
            return True
        msg = rec.get("message") if isinstance(rec.get("message"), dict) else {}
        if msg.get("stop_reason") in _TERMINAL_STOP_REASONS:
            return True
    return False


def reap_resume_children(now: Optional[float] = None) -> Dict[str, Any]:
    """Daemon-tick backstop: SIGTERM ledgered resume children that outlived a
    completed turn (log shows a terminal record AND has been idle past the
    reap window). Every kill is triple-gated: ledger membership, pid still
    alive, and ps identity (argv still ``claude ... <sid>``) so a reused pid
    is cleared from the ledger, never signalled."""
    now = time.time() if now is None else float(now)
    idle_s = _reap_idle_s()
    reaped: List[Dict[str, Any]] = []
    cleared = 0
    with queue_mod._FileLock(_resume_ledger_lock()):
        remaining: List[Dict[str, Any]] = []
        for e in _load_resume_ledger():
            if not isinstance(e, dict):
                cleared += 1
                continue
            try:
                pid = int(e.get("pid") or 0)
            except (TypeError, ValueError):
                pid = 0
            sid = str(e.get("sid") or "")
            log = str(e.get("log") or "")
            if pid <= 0 or not sid:
                cleared += 1
                continue
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                cleared += 1  # exited on its own — the normal EOF path
                continue
            except PermissionError:
                cleared += 1  # pid reused by another user's process
                continue
            except OSError:
                remaining.append(e)
                continue
            cmd = _pid_command(pid)
            if "claude" not in cmd or sid not in cmd:
                cleared += 1  # pid reused — clear the entry, never signal
                continue
            if not _log_shows_terminal(log):
                remaining.append(e)  # mid-turn (or log unreadable): hands off
                continue
            try:
                log_idle = now - os.path.getmtime(log)
            except OSError:
                remaining.append(e)
                continue
            if log_idle < idle_s:
                remaining.append(e)
                continue
            try:
                os.kill(pid, signal.SIGTERM)
            except OSError:
                remaining.append(e)
                continue
            reaped.append({"pid": pid, "sid": sid[:8], "log": log})
        _save_resume_ledger(remaining)
    for r in reaped:
        queue_mod._log(
            "REAP", f"resume child pid={r['pid']} sid={r['sid']} "
                    "outlived its completed turn"
        )
    return {"reaped": reaped, "cleared": cleared, "kept_count": len(remaining)}


def _post_json(url: str, payload: Dict[str, Any], timeout_s: float) -> Dict[str, Any]:
    """POST JSON, return the parsed JSON response. Raises on transport/HTTP
    errors (urlopen raises HTTPError for any non-2xx status)."""
    headers = {"Content-Type": "application/json"}
    # WT-65: a remote delegate (federated WT) requires its bearer token.
    token = (os.environ.get("WATCHTOWER_DELEGATE_TOKEN") or "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        body = resp.read().decode("utf-8", "replace")
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        data = {}
    return data if isinstance(data, dict) else {}


def _deliver_codex_app_server(resolved: Dict[str, Any], text: str) -> Dict[str, Any]:
    """Standalone codex fallback: native delivery via WT's private app-server.

    Only called for targets whose resolved ``engine`` is ``codex``. Import is
    lazy and guarded so a machine with no codex binary (or a stripped-down WT
    install) never pays an import cost or hard-fails; any failure here just
    falls through to the delegate adapter like every other adapter miss.

    Session-id-to-thread-id mapping: WT treats ``resolved["session_id"]`` as
    the codex thread id directly, 1:1. This holds for how codex targets get
    into the registry today (workers.spawn_workers records a codex worker's
    own thread id as its session_id; ``agents register --engine codex``
    expects the same). If a future addressing scheme needs codex targets
    keyed by something other than the thread id, this is the seam to update.
    """
    sid = str(resolved.get("session_id") or "")
    if not sid:
        return {"ok": False, "error": "codex app-server needs a thread/session id"}
    try:
        from . import codex_rpc
    except ImportError:
        return {"ok": False, "error": "codex_rpc module unavailable"}
    if not codex_rpc.is_available():
        return {"ok": False, "error": "codex binary not found"}
    result = codex_rpc.deliver(sid, text)
    if result.get("ok"):
        try:
            from . import codex_registry
            codex_registry.upsert(
                sid,
                source="wt-delivery",
                transport_owner="wt-private-app-server",
                transport="stdio",
                cwd=resolved.get("cwd") or "",
                wt={
                    "last_delivery_transport": "codex-app-server",
                    "last_delivery_via": result.get("via"),
                },
            )
        except Exception:
            pass
        return {
            "ok": True,
            "transport": "codex-app-server",
            "turn_id": result.get("turn_id"),
            "codex_via": result.get("via"),
            "codex_app_server_warm": result.get("app_server_warm"),
            "codex_resume_ms": result.get("resume_ms"),
            "codex_turn_ms": result.get("turn_ms"),
            "codex_total_ms": result.get("latency_ms"),
        }
    return {
        "ok": False,
        "error": result.get("error") or "codex app-server delivery failed",
    }


def _gemini_bin() -> str:
    """Return the Gemini CLI binary path, or empty string if not found.

    Priority: ``WT_GEMINI_BIN`` env var (explicit path) → ``gemini`` on PATH.
    Empty string means the CLI is absent; callers must fall through."""
    import shutil
    env = os.environ.get("WT_GEMINI_BIN", "")
    if env:
        expanded = os.path.expanduser(env)
        if os.path.isfile(expanded) and os.access(expanded, os.X_OK):
            return expanded
    return shutil.which("gemini") or ""


def _deliver_gemini_resume(resolved: Dict[str, Any], text: str) -> Dict[str, Any]:
    """Adapter for Gemini sessions: ``gemini --resume <sid> -p <text>``.

    Mirrors the shape of ``_deliver_resume`` (Claude) but for Gemini CLI
    targets (engine == 'gemini').  No FIFO or stream-json stdin needed —
    Gemini resume takes the prompt via ``-p`` and runs one-shot, so this is
    simpler than the Claude path.  No busy-check either: Gemini has no local
    transcript file to probe for mid-turn state."""
    if resolved.get("engine") != "gemini":
        return {"ok": False, "error": "gemini-resume needs engine=gemini"}
    sid = str(resolved.get("session_id") or "")
    if not sid:
        return {"ok": False, "error": "gemini-resume needs a session_id"}
    bin_path = _gemini_bin()
    if not bin_path:
        return {"ok": False, "error": "Gemini CLI not found (install gemini or set WT_GEMINI_BIN)"}
    log_dir = _logs_dir()
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        return {"ok": False, "error": f"cannot create log dir: {e}"}
    ts = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    log_path = log_dir / f"msg-gemini-{sid[:8]}-{ts}.log"
    argv = [bin_path, "--approval-mode", "yolo", "--resume", sid, "-p", text]
    try:
        with open(log_path, "ab") as logf:
            subprocess.Popen(
                argv,
                stdin=subprocess.DEVNULL,
                stdout=logf,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
    except OSError as e:
        return {"ok": False, "error": f"gemini resume spawn failed: {e}"}
    return {"ok": True, "transport": "gemini-resume", "log": str(log_path)}


# ------------------------------------------------------- Antigravity LS delivery
_ANTIGRAVITY_SERVICE = "exa.language_server_pb.LanguageServerService"


def _antigravity_listening_port(pid: str) -> str:
    """Return the loopback TCP listen port owned by one LS pid, if any."""
    try:
        out = subprocess.check_output(
            ["lsof", "-a", "-nP", "-p", pid, "-iTCP", "-sTCP:LISTEN"],
            stderr=subprocess.DEVNULL, text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return ""
    for line in out.splitlines()[1:]:
        match = re.search(r"(?:127\.0\.0\.1|\[::1\]):(\d+)\s+\(LISTEN\)", line)
        if match:
            return match.group(1)
    return ""


def _antigravity_servers() -> List[Dict[str, str]]:
    """Discover local Antigravity LS processes and their CSRF credentials.

    Antigravity starts the LS with a random HTTPS port (``--https_server_port
    0``) and provides its CSRF token as a command-line flag.  Both are local
    process metadata, and the generated endpoint is always pinned to loopback.
    """
    try:
        out = subprocess.check_output(
            ["ps", "-axww", "-o", "pid=,command="],
            stderr=subprocess.DEVNULL, text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return []
    servers: List[Dict[str, str]] = []
    for line in out.splitlines():
        pid, _, command = line.strip().partition(" ")
        if not pid or "language_server" not in command:
            continue
        try:
            argv = shlex.split(command)
            token_idx = argv.index("--csrf_token")
            token = argv[token_idx + 1]
        except (ValueError, IndexError):
            continue
        port = _antigravity_listening_port(pid)
        if port:
            servers.append({
                "base_url": f"https://127.0.0.1:{port}",
                "csrf_token": token,
            })
    return servers


def _antigravity_post(
    server: Dict[str, str], method: str, payload: Dict[str, Any]
) -> Dict[str, Any]:
    """POST JSON to one local Antigravity Connect endpoint.

    The LS serves a self-signed certificate on localhost, so certificate
    verification is intentionally disabled only for this fixed 127.0.0.1 URL.
    """
    url = f"{server['base_url']}/{_ANTIGRAVITY_SERVICE}/{method}"
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "X-Codeium-Csrf-Token": server["csrf_token"],
        },
        method="POST",
    )
    context = ssl._create_unverified_context()
    with urllib.request.urlopen(req, timeout=5, context=context) as response:
        raw = response.read().decode("utf-8", "replace")
    data = json.loads(raw) if raw else {}
    return data if isinstance(data, dict) else {}


def _antigravity_requested_model(status: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Pick the LS's recommended advertised cascade model, if present."""
    configs = (
        status.get("userStatus", {}).get("cascadeModelConfigData", {})
        .get("clientModelConfigs", [])
    )
    if not isinstance(configs, list):
        return None
    for config in configs:
        if not isinstance(config, dict) or not config.get("isRecommended"):
            continue
        model = config.get("modelOrAlias")
        if isinstance(model, dict) and model:
            return model
    return None


def _deliver_antigravity_language_server(
    resolved: Dict[str, Any], text: str
) -> Dict[str, Any]:
    """Deliver to a live Antigravity cascade.

    AGY's CLI logs prove its conversation id is the language-server cascade id,
    so WatchTower uses the registered ``session_id`` directly.  It first asks
    each discovered LS for that cascade; this prevents routing a message to an
    unrelated Antigravity instance that happens to be running.
    """
    if resolved.get("engine") != "antigravity":
        return {"ok": False, "error": "antigravity adapter needs engine=antigravity"}
    sid = str(resolved.get("session_id") or "")
    if not sid:
        return {"ok": False, "error": "antigravity adapter needs a session_id"}
    servers = _antigravity_servers()
    if not servers:
        return {"ok": False, "error": "no live Antigravity language server"}
    last_error = "cascade not found on a live Antigravity language server"
    for server in servers:
        try:
            _antigravity_post(server, "GetCascadeTrajectory", {"cascadeId": sid})
            status = _antigravity_post(server, "GetUserStatus", {})
            requested_model = _antigravity_requested_model(status)
            if requested_model is None:
                return {"ok": False, "error": "Antigravity exposes no recommended cascade model"}
            config = {
                "plannerConfig": {"requestedModel": requested_model, "maxOutputTokens": 8192},
                "checkpointConfig": {"maxOutputTokens": 8192},
            }
            _antigravity_post(server, "SendUserCascadeMessage", {
                "cascadeId": sid,
                "items": [{"text": text}],
                "cascadeConfig": config,
            })
            return {"ok": True, "transport": "antigravity-language-server"}
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            last_error = str(exc)
    return {"ok": False, "error": f"antigravity: {last_error}"}


def _deliver_delegate(
    resolved: Dict[str, Any], text: str, mode: str
) -> Dict[str, Any]:
    """Adapter 3 (optional, last): hand delivery to a configured delegate."""
    base = _delegate_base()
    if not base:
        return {"ok": False, "error": "no delegate configured"}
    sid = str(resolved.get("session_id") or "")
    if not sid:
        return {"ok": False, "error": "delegate needs a session_id"}
    try:
        data = _post_json(
            base + "/api/inject-input",
            # origin=wt (WT-78): tells the delegate this request came from
            # wt's own adapter chain, so it must not call back into
            # `wt send` — the protocol-level guard against the
            # CCC -> wt -> CCC delegation loop.
            {"session_id": sid, "text": text, "mode": mode, "origin": "wt"},
            timeout_s=5,
        )
    except Exception as e:  # noqa: BLE001 - any transport failure means fall through
        return {"ok": False, "error": f"delegate: {e}"}
    if data.get("ok") is False:
        return {"ok": False, "error": "delegate rejected the message"}
    return {"ok": True, "transport": "delegate"}


def deliver(
    resolved: Dict[str, Any], text: str, mode: str = "send"
) -> Dict[str, Any]:
    """Try each adapter in order.

    For Codex targets, a configured delegate is treated as the CCC/managed
    app-server broker and is tried before WT's private app-server fallback.
    The private app-server is standalone-only, so WT does not create a second
    Codex app-server owner on a machine where CCC is already brokering Codex.

    Returns the first success (``{"ok": True, "transport": ...}``), else
    ``{"ok": False, "busy": bool, "error": "<joined adapter errors>"}``.

    Every success is recorded as a delivery receipt (WT-77) so "delivered"
    can later be verified against the target transcript — the result
    carries ``receipt_id``."""
    result = _deliver_unreceipted(resolved, text, mode)
    sid = str(resolved.get("session_id") or "")
    if result.get("ok") and sid:
        try:
            from . import receipts
            transport = str(result.get("transport") or "?")
            rec = receipts.record(
                sid,
                text,
                transport,
                engine=str(resolved.get("engine") or "claude"),
                require_path=transport == "codex-app-server",
            )
            result["receipt_id"] = rec["id"]
        except Exception as exc:  # noqa: BLE001 - surface proof failures honestly
            if result.get("transport") == "codex-app-server":
                return {
                    "ok": False,
                    "error": "codex-app-server accepted message but Codex rollout "
                             f"is unavailable for receipt verification: {exc}",
                }
    return result


def _deliver_unreceipted(
    resolved: Dict[str, Any], text: str, mode: str = "send"
) -> Dict[str, Any]:
    errors: List[str] = []
    busy = False
    r = _deliver_fifo(resolved, text)
    if r.get("ok"):
        return r
    errors.append(f"fifo: {r.get('error', 'failed')}")
    if resolved.get("engine") == "claude":
        r = tty_mod.deliver_tty(resolved, text)
        if r.get("ok"):
            return r
        errors.append(f"tty: {r.get('error', 'failed')}")
    r = _deliver_resume(resolved, text)
    if r.get("ok"):
        return r
    if r.get("busy"):
        busy = True
    errors.append(f"resume: {r.get('error', 'failed')}")
    delegate_tried = False
    if (
        resolved.get("engine") == "codex"
        and _codex_delegate_first_enabled()
        and _delegate_base()
    ):
        delegate_tried = True
        r = _deliver_delegate(resolved, text, mode)
        if r.get("ok"):
            return r
        errors.append(f"delegate: {r.get('error', 'failed')}")
        return {"ok": False, "busy": busy, "error": "; ".join(errors)}
    if resolved.get("engine") == "codex":
        r = _deliver_codex_app_server(resolved, text)
        if r.get("ok"):
            return r
        errors.append(f"codex: {r.get('error', 'failed')}")
    if resolved.get("engine") == "gemini":
        r = _deliver_gemini_resume(resolved, text)
        if r.get("ok"):
            return r
        errors.append(f"gemini: {r.get('error', 'failed')}")
    if resolved.get("engine") == "antigravity":
        r = _deliver_antigravity_language_server(resolved, text)
        if r.get("ok"):
            return r
        errors.append(f"antigravity: {r.get('error', 'failed')}")
    if not delegate_tried:
        r = _deliver_delegate(resolved, text, mode)
        if r.get("ok"):
            return r
        errors.append(f"delegate: {r.get('error', 'failed')}")
    return {"ok": False, "busy": busy, "error": "; ".join(errors)}


# ----------------------------------------------------------------------- outbox
def _empty_outbox() -> Dict[str, Any]:
    return {"messages": []}


def _load_outbox() -> Dict[str, Any]:
    try:
        with open(_outbox_file(), "r") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return _empty_outbox()
    if not isinstance(data, dict) or not isinstance(data.get("messages"), list):
        return _empty_outbox()
    return data


def _save_outbox(data: Dict[str, Any]) -> None:
    path = _outbox_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = str(path) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


def _backoff_s(attempts: int) -> float:
    """Retry delay after ``attempts`` failed deliveries: 30 * 2^n, cap 600."""
    return float(min(BACKOFF_CAP_S, BACKOFF_BASE_S * (2 ** attempts)))


def outbox_add(
    to: str,
    text: str,
    mode: str = "send",
    error: str = "",
    delay_s: float = BACKOFF_BASE_S,
    ttl_s: Optional[float] = None,
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """Append a pending message to the durable outbox. Locked + atomic."""
    now = time.time() if now is None else float(now)
    msg = {
        "id": f"msg-{_uuid.uuid4().hex[:12]}",
        "to": str(to),
        "text": str(text),
        "mode": str(mode or "send"),
        "created_at": _iso(now),
        "attempts": 0,
        "next_attempt_at": _iso(now + delay_s),
        "last_error": str(error or ""),
        "status": "pending",
    }
    if ttl_s is not None:
        msg["expires_at"] = _iso(now + float(ttl_s))
    with queue_mod._FileLock(_outbox_lock()):
        data = _load_outbox()
        data["messages"].append(msg)
        _save_outbox(data)
    return msg


def outbox_list(status: Optional[str] = None) -> List[Dict[str, Any]]:
    msgs = _load_outbox()["messages"]
    if status:
        msgs = [m for m in msgs if m.get("status") == status]
    return msgs


def _reset_outbox_message(m: Dict[str, Any], now: float) -> Dict[str, Any]:
    m["attempts"] = 0
    m["status"] = "pending"
    m["next_attempt_at"] = _iso(now)
    m["last_error"] = ""
    m.pop("delivered_at", None)
    m.pop("expires_at", None)
    return m


def outbox_retry(message_id: str, now: Optional[float] = None) -> Dict[str, Any]:
    """Reset one outbox message so the next drain tick retries it now."""
    now = time.time() if now is None else float(now)
    target = str(message_id or "")
    with queue_mod._FileLock(_outbox_lock()):
        data = _load_outbox()
        for m in data["messages"]:
            if str(m.get("id")) == target:
                out = dict(_reset_outbox_message(m, now))
                _save_outbox(data)
                return out
    raise KeyError(target)


def outbox_retry_all_dead(now: Optional[float] = None) -> List[Dict[str, Any]]:
    """Reset every dead outbox message so drain retries them now."""
    now = time.time() if now is None else float(now)
    rows: List[Dict[str, Any]] = []
    with queue_mod._FileLock(_outbox_lock()):
        data = _load_outbox()
        for m in data["messages"]:
            if m.get("status") == "dead":
                rows.append(dict(_reset_outbox_message(m, now)))
        if rows:
            _save_outbox(data)
    return rows


def outbox_remove(message_id: str) -> bool:
    """Remove one outbox message by id."""
    target = str(message_id or "")
    with queue_mod._FileLock(_outbox_lock()):
        data = _load_outbox()
        kept = [m for m in data["messages"] if str(m.get("id")) != target]
        if len(kept) == len(data["messages"]):
            return False
        data["messages"] = kept
        _save_outbox(data)
    return True


def drain_outbox(now: Optional[float] = None) -> Dict[str, List[str]]:
    """One daemon tick over the outbox: retry due pending messages.

    Delivery attempts run OUTSIDE the file lock (a delegate POST can take
    seconds), then results are folded back in under the lock. Backoff is
    30 * 2^attempts capped at 600s; after ``MAX_ATTEMPTS`` or TTL expiry a
    message goes ``dead`` and is logged as DEADMSG. Returns id lists per
    outcome."""
    now = time.time() if now is None else float(now)
    with queue_mod._FileLock(_outbox_lock()):
        data = _load_outbox()
        expired_ids = {
            str(m.get("id"))
            for m in data["messages"]
            if m.get("status") == "pending"
            and m.get("expires_at")
            and _parse_iso(m.get("expires_at")) <= now
        }
        due = [
            dict(m)
            for m in data["messages"]
            if m.get("status") == "pending"
            and str(m.get("id")) not in expired_ids
            and _parse_iso(m.get("next_attempt_at")) <= now
        ]
    result: Dict[str, List[str]] = {"delivered": [], "retried": [], "dead": []}
    if not due and not expired_ids:
        return result
    outcomes: Dict[str, Dict[str, Any]] = {}
    for m in due:
        try:
            # include_recent=False: the daemon tick must never enumerate the
            # transcript archive; outbox targets are already resolved sids
            # (or exact worker ids / names), so the window adds nothing here.
            resolved = resolve_target(str(m.get("to") or ""), include_recent=False)
            outcomes[str(m.get("id"))] = deliver(
                resolved, str(m.get("text") or ""), str(m.get("mode") or "send")
            )
        except ValueError as e:
            outcomes[str(m.get("id"))] = {"ok": False, "error": str(e)}
    with queue_mod._FileLock(_outbox_lock()):
        data = _load_outbox()
        for m in data["messages"]:
            msg_id = str(m.get("id"))
            if msg_id in expired_ids and m.get("status") == "pending":
                m["status"] = "dead"
                m["last_error"] = "expired"
                result["dead"].append(msg_id)
                queue_mod._log(
                    "DEADMSG",
                    f"{m.get('id','?')} to {m.get('to','?')}: expired",
                )
                continue
            res = outcomes.get(str(m.get("id")))
            if res is None or m.get("status") != "pending":
                continue
            attempts = int(m.get("attempts", 0)) + 1
            m["attempts"] = attempts
            if res.get("ok"):
                m["status"] = "delivered"
                m["delivered_at"] = _iso(now)
                m["last_error"] = ""
                result["delivered"].append(str(m["id"]))
                queue_mod._log(
                    "SEND",
                    f"{m.get('to','?')} via {res.get('transport','?')} "
                    f"(outbox, attempt {attempts}): {str(m.get('text',''))[:60]}",
                )
            else:
                m["last_error"] = str(res.get("error") or "delivery failed")
                if attempts >= MAX_ATTEMPTS:
                    m["status"] = "dead"
                    result["dead"].append(str(m["id"]))
                    queue_mod._log(
                        "DEADMSG",
                        f"{m.get('id','?')} to {m.get('to','?')} after "
                        f"{attempts} attempts: {m['last_error'][:80]}",
                    )
                else:
                    m["next_attempt_at"] = _iso(now + _backoff_s(attempts))
                    result["retried"].append(str(m["id"]))
        _save_outbox(data)
    return result


# ------------------------------------------------------------------------- send
def send(
    target: str,
    text: str,
    mode: str = "send",
    queue_on_fail: bool = True,
    ttl_s: Optional[float] = None,
) -> Dict[str, Any]:
    """Resolve + deliver a message; on total delivery failure, park it in the
    outbox (unless ``queue_on_fail`` is False) for the daemon to retry.

    Returns ``{"ok": True, "transport": ...}`` on delivery,
    ``{"ok": False, "queued": True, "id": ...}`` when parked, and
    ``{"ok": False, "error": ...}`` for unresolvable targets or when queueing
    is disabled."""
    try:
        resolved = resolve_target(target)
    except ValueError as e:
        return {"ok": False, "error": str(e)}
    result = deliver(resolved, text, mode)
    if result.get("ok"):
        out = {"ok": True, "transport": result.get("transport", "?")}
        if result.get("log"):
            out["log"] = result["log"]
        if result.get("turn_id"):
            out["turn_id"] = result["turn_id"]
        for key, value in result.items():
            if key.startswith("codex_") and value is not None:
                out[key] = value
        if result.get("receipt_id"):
            out["receipt_id"] = result["receipt_id"]
        queue_mod._log(
            "SEND", f"{target} via {out['transport']}: {str(text)[:60]}"
        )
        return out
    if not queue_on_fail:
        return {"ok": False, "queued": False,
                "error": str(result.get("error") or "delivery failed")}
    delay = BUSY_HOLD_S if result.get("busy") else float(BACKOFF_BASE_S)
    to = str(resolved.get("session_id") or target)
    msg = outbox_add(
        to, text, mode=mode,
        error=str(result.get("error") or ""), delay_s=delay, ttl_s=ttl_s,
    )
    return {
        "ok": False,
        "queued": True,
        "id": msg["id"],
        "busy": bool(result.get("busy")),
        "error": str(result.get("error") or "delivery failed"),
    }


# -------------------------------------------------------------------------- ask
def _feed_record(rec: Dict[str, Any], parts: List[str]) -> Tuple[bool, str]:
    """Fold one stream-json record into the accumulated answer.

    Returns ``(done, answer)``: done is True on a ``result`` event (prefer its
    non-empty ``result`` field, else the accumulated text) or on a terminal
    ``stop_reason``."""
    msg = rec.get("message") if isinstance(rec.get("message"), dict) else {}
    if rec.get("type") == "assistant":
        for block in (msg.get("content") or []):
            if isinstance(block, dict) and block.get("type") == "text":
                t = str(block.get("text") or "")
                if t:
                    parts.append(t)
    if rec.get("type") == "result":
        res = rec.get("result")
        if isinstance(res, str) and res.strip():
            return True, res
        return True, "\n".join(parts)
    if msg.get("stop_reason") in _TERMINAL_STOP_REASONS:
        return True, "\n".join(parts)
    return False, ""


def _await_reply(
    log_path: str, offset: int, deadline: float, source: str
) -> Dict[str, Any]:
    """Tail a stream-json log from ``offset``, polling every 0.2s, until a
    terminal record or the deadline. Only bytes appended after the snapshot
    are read, which is what correlates the reply to OUR message."""
    parts: List[str] = []
    buf = b""
    while True:
        try:
            size = os.path.getsize(log_path)
        except OSError:
            size = offset
        if size > offset:
            try:
                with open(log_path, "rb") as f:
                    f.seek(offset)
                    buf += f.read(size - offset)
                offset = size
            except OSError:
                pass
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                if not isinstance(rec, dict):
                    continue
                done, answer = _feed_record(rec, parts)
                if done:
                    return {"ok": True, "answer": answer, "source": source}
        if time.time() >= deadline:
            return {
                "ok": False,
                "error": "timeout",
                "partial": "\n".join(parts),
                "source": source,
            }
        time.sleep(0.2)


def ask(target: str, text: str, timeout_ms: int = 30000) -> Dict[str, Any]:
    """Send a question and wait for the answer.

    fifo path: snapshot the live worker's log size, deliver over the FIFO,
    tail only the new bytes. resume path: same, tailing the fresh msg-*.log.
    delegate path: pass through to the delegate's ``/api/ask``. On deadline:
    ``{"ok": False, "error": "timeout", "partial": <accumulated text>}``."""
    try:
        resolved = resolve_target(target)
    except ValueError as e:
        return {"ok": False, "error": str(e), "source": "resolve"}
    deadline = time.time() + max(0.1, timeout_ms / 1000.0)

    w = resolved.get("worker") or {}
    log = str(w.get("log") or "")
    if w.get("fifo") and log and os.path.exists(log):
        offset = os.path.getsize(log)
        if workers.write_to_worker_fifo(str(w["fifo"]), text):
            queue_mod._log("ASK", f"{target} via fifo: {str(text)[:60]}")
            return _await_reply(log, offset, deadline, source="fifo")

    engine = str(resolved.get("engine") or "claude")
    resume_error = "resume unavailable"
    if engine != "codex":
        r = _deliver_resume(resolved, text)
        if r.get("ok"):
            queue_mod._log("ASK", f"{target} via resume: {str(text)[:60]}")
            return _await_reply(str(r.get("log") or ""), 0, deadline, source="resume")
        resume_error = str(r.get("error") or resume_error)

    base = _delegate_base()
    sid = str(resolved.get("session_id") or "")
    if base and sid:
        try:
            data = _post_json(
                base + "/api/ask",
                {"session_id": sid, "text": text, "timeout_ms": timeout_ms},
                timeout_s=timeout_ms / 1000.0 + 5,
            )
        except Exception as e:  # noqa: BLE001 - report, do not raise out of ask
            return {"ok": False, "error": f"delegate ask failed: {e}",
                    "source": "delegate"}
        out: Dict[str, Any] = dict(data)
        out.setdefault("ok", False)
        out["source"] = "delegate"
        if out.get("ok"):
            queue_mod._log("ASK", f"{target} via delegate: {str(text)[:60]}")
        return out

    if engine == "codex":
        return {
            "ok": False,
            "error": "ask is unsupported for standalone Codex targets; "
                     "configure a delegate/broker",
            "source": "unsupported",
            "engine": "codex",
        }

    return {"ok": False, "error": resume_error, "source": "none"}

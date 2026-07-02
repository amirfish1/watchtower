#!/usr/bin/env python3
"""Cross-agent messaging: agents registry, delivery adapters, durable outbox, ask.

This module gives WatchTower its conversational primitives (see
``docs/messaging-design.md``):

  * ``send(target, text)``: push a message to a worker, a registered agent, or
    a raw claude session. Delivery falls through an ordered adapter chain:

      1. ``fifo``: the target is a live WT worker with a stream-json FIFO
         stdin; reuse ``workers.write_to_worker_fifo``. Cheapest path.
      2. ``resume``: headless ``claude -p --resume <sid>`` with a fresh FIFO
         stdin, output logged to ``~/.watchtower/logs/msg-<sid8>-<ts>.log``.
         Guarded by a busy check: if the target session's transcript under
         ``~/.claude/projects/*/<sid>.jsonl`` was modified within the last
         ``$WATCHTOWER_BUSY_WINDOW_S`` seconds (default 120), the session is
         actively mid-turn, so we do NOT fork a parallel resume; the message
         is held in the outbox and delivered once the transcript goes quiet.
      3. ``delegate`` (optional, last): POST to a delegate HTTP endpoint
         (CCC's ``/api/inject-input``) for transports WT cannot do natively.
         ``$WATCHTOWER_DELEGATE_URL`` overrides; ``off`` disables even when
         ``~/.claude/command-center/port.txt`` exists. No delegate configured
         is a fully working standalone setup.

  * Outbox (``$WATCHTOWER_OUTBOX_FILE``, default ``~/.watchtower/outbox.json``):
    durable at-least-once store for messages that could not be delivered. The
    daemon drains it each tick with exponential backoff (30s base, 10 min
    cap); after 20 attempts a message goes ``dead`` and is logged (DEADMSG).

  * ``ask(target, text)``: synchronous question. Correlation is byte-offset
    tailing: snapshot the log size before delivery, then read only the new
    bytes, accumulating assistant text until a ``{"type": "result"}`` event or
    a terminal ``stop_reason`` arrives.

  * Agents registry (``$WATCHTOWER_AGENTS_FILE``, default
    ``~/.watchtower/agents.json``): friendly names for session UUIDs, schema
    ``{name: {session_id, engine, cwd, registered_at, last_seen}}``.

All state-file paths are resolved at call time (never import-time constants)
so tests can override them via the environment. Writes are atomic
(tmp + ``os.replace``) and outbox/registry mutations are serialised with the
same ``fcntl`` file lock the queue store uses. Stdlib only.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
import urllib.request
import uuid as _uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from . import queue as queue_mod
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
    ``~/.claude/command-center/port.txt`` (single integer) auto-detects a
    local CCC. No delegate at all is a fully supported configuration."""
    env = (os.environ.get("WATCHTOWER_DELEGATE_URL") or "").strip()
    if env:
        return "" if env.lower() == "off" else env.rstrip("/")
    port_file = Path.home() / ".claude" / "command-center" / "port.txt"
    try:
        port = int(port_file.read_text().strip())
        return f"http://127.0.0.1:{port}"
    except (OSError, ValueError):
        return ""


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
    since those shapes are claimed by earlier resolve_target steps."""
    name = str(name or "").lstrip("@").strip()
    if not name:
        raise ValueError("agent name is required")
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


def list_agents() -> List[Dict[str, Any]]:
    """Registry entries as a list of rows, each with its name included."""
    agents = _load_agents()
    out: List[Dict[str, Any]] = []
    for name in sorted(agents):
        rec = agents[name]
        if isinstance(rec, dict):
            out.append({"name": name, **rec})
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


def resolve_target(target: str) -> Dict[str, Any]:
    """Resolve a target string to a delivery descriptor.

    Returns ``{"kind": "worker"|"agent"|"session", "session_id": str|None,
    "worker": dict|None, "engine": str}``. Resolution order:

      1. exact live worker_id match,
      2. agents registry name (a leading ``@`` is allowed),
      3. raw session UUID, or a unique hex prefix (>= 8 chars) of a known
         session; an unknown value is accepted only as a full 36-char UUID.

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
        }
    if _HEX_PREFIX_RE.match(t):
        known = _known_sessions(live, agents)
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
            return {"kind": "session", "session_id": t, "worker": None,
                    "engine": "claude"}
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


def _session_busy(sid: str) -> bool:
    """True when the session's transcript was written within the busy window,
    meaning the session is actively mid-turn: forking a parallel resume would
    race it, so the caller must hold the message for a later retry."""
    p = _find_transcript(sid)
    if p is None:
        return False
    try:
        return (time.time() - p.stat().st_mtime) < _busy_window_s()
    except OSError:
        return False


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
    fifo_path, rdwr_fd = workers._make_stdin_fifo(log_path)
    if fifo_path is None:
        return {"ok": False, "error": "could not create stdin fifo"}
    argv = [
        "claude", "-p", "--verbose",
        "--resume", sid,
        "--input-format", "stream-json",
        "--output-format", "stream-json",
        "--permission-mode", "bypassPermissions",
    ]
    try:
        logf = open(log_path, "ab")
        try:
            subprocess.Popen(
                argv,
                stdin=rdwr_fd,
                stdout=logf,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        finally:
            logf.close()
    except OSError as e:
        workers._close_fd_quiet(rdwr_fd)
        try:
            Path(fifo_path).unlink()
        except OSError:
            pass
        return {"ok": False, "error": f"claude resume spawn failed: {e}"}
    # Write the message while our RDWR fd still holds the FIFO open, then drop
    # our copy; the child's inherited stdin fd keeps the pipe alive.
    delivered = workers.write_to_worker_fifo(fifo_path, text)
    workers._close_fd_quiet(rdwr_fd)
    if not delivered:
        return {"ok": False, "error": "resume fifo write failed"}
    return {"ok": True, "transport": "resume", "log": str(log_path)}


def _post_json(url: str, payload: Dict[str, Any], timeout_s: float) -> Dict[str, Any]:
    """POST JSON, return the parsed JSON response. Raises on transport/HTTP
    errors (urlopen raises HTTPError for any non-2xx status)."""
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        body = resp.read().decode("utf-8", "replace")
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        data = {}
    return data if isinstance(data, dict) else {}


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
            {"session_id": sid, "text": text, "mode": mode},
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
    """Try each adapter in order: fifo, resume (with busy check), delegate.

    Returns the first success (``{"ok": True, "transport": ...}``), else
    ``{"ok": False, "busy": bool, "error": "<joined adapter errors>"}``."""
    errors: List[str] = []
    busy = False
    r = _deliver_fifo(resolved, text)
    if r.get("ok"):
        return r
    errors.append(f"fifo: {r.get('error', 'failed')}")
    r = _deliver_resume(resolved, text)
    if r.get("ok"):
        return r
    if r.get("busy"):
        busy = True
    errors.append(f"resume: {r.get('error', 'failed')}")
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


def drain_outbox(now: Optional[float] = None) -> Dict[str, List[str]]:
    """One daemon tick over the outbox: retry due pending messages.

    Delivery attempts run OUTSIDE the file lock (a delegate POST can take
    seconds), then results are folded back in under the lock. Backoff is
    30 * 2^attempts capped at 600s; after ``MAX_ATTEMPTS`` a message goes
    ``dead`` and is logged as DEADMSG. Returns id lists per outcome."""
    now = time.time() if now is None else float(now)
    with queue_mod._FileLock(_outbox_lock()):
        due = [
            dict(m)
            for m in _load_outbox()["messages"]
            if m.get("status") == "pending"
            and _parse_iso(m.get("next_attempt_at")) <= now
        ]
    result: Dict[str, List[str]] = {"delivered": [], "retried": [], "dead": []}
    if not due:
        return result
    outcomes: Dict[str, Dict[str, Any]] = {}
    for m in due:
        try:
            resolved = resolve_target(str(m.get("to") or ""))
            outcomes[str(m.get("id"))] = deliver(
                resolved, str(m.get("text") or ""), str(m.get("mode") or "send")
            )
        except ValueError as e:
            outcomes[str(m.get("id"))] = {"ok": False, "error": str(e)}
    with queue_mod._FileLock(_outbox_lock()):
        data = _load_outbox()
        for m in data["messages"]:
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
        error=str(result.get("error") or ""), delay_s=delay,
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

    r = _deliver_resume(resolved, text)
    if r.get("ok"):
        queue_mod._log("ASK", f"{target} via resume: {str(text)[:60]}")
        return _await_reply(str(r.get("log") or ""), 0, deadline, source="resume")
    resume_error = str(r.get("error") or "resume unavailable")

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

    return {"ok": False, "error": resume_error, "source": "none"}

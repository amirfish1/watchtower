"""Durable provenance for sessions a WatchTower worker (or its descendant) started.

``worker-sessions.json`` only says "this session id was once a worker"; it
carries no ticket, role or parent, and direct CLI spawns (a planner that runs
``claude -p`` probes itself) never reach it. This ledger keeps one record per
session id -- who started it, for which ticket, in what role -- so CCC can badge
the session in its Coding list long after the spawner exited, its worker
record was pruned, or the title changed.

Shape: ``{"origins": {session_id: {...}}}``. Records are append-only per
session id (the first write wins on ``role``/``ref`` unless it was blank), so a
later, less specific note never erases attribution. A session is only recorded
when its origin is *established* (a spawn we performed, or a descendant of one
detected via the spawn env markers); nothing is ever inferred from titles.
"""
from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, Optional

ORIGINS_FILE = Path(
    os.environ.get("WATCHTOWER_SESSION_ORIGINS_FILE")
    or (Path.home() / ".watchtower" / "session-origins.json")
)
_CAP = 2000
_SID_RE = re.compile(r"^[A-Za-z0-9_-]{8,80}$")

# Spawn-env markers: set by workers._spawn_env for every spawned session, so a
# process started *inside* one (a probe, a test, a helper) can name its origin.
ENV_WORKER = "WT_WORKER_ID"
ENV_SESSION = "WT_SESSION_ID"
ENV_REF = "WT_TICKET_REF"
ENV_QUEUE = "WT_TICKET_QUEUE"
ENV_ROLE = "WT_ROLE"

_FIELDS = ("role", "ref", "queue", "worker_id", "parent_worker_id",
           "parent_session_id", "engine", "via", "purpose")


def load() -> Dict[str, Dict[str, Any]]:
    try:
        with open(ORIGINS_FILE, "r") as f:
            data = json.load(f)
    except (OSError, ValueError):
        return {}
    rows = data.get("origins") if isinstance(data, dict) else None
    if not isinstance(rows, dict):
        return {}
    return {str(k): v for k, v in rows.items() if isinstance(v, dict)}


def get(session_id: str) -> Optional[Dict[str, Any]]:
    return load().get(str(session_id or ""))


def record(session_id: str, **fields: Any) -> Optional[Dict[str, Any]]:
    """Note the origin of ``session_id``. Blank fields never overwrite; an
    already-known field is kept. Never raises."""
    sid = str(session_id or "")
    if not _SID_RE.fullmatch(sid):
        return None
    clean = {k: str(fields[k]) for k in _FIELDS if fields.get(k)}
    try:
        rows = load()
        cur = rows.get(sid) or {}
        merged = dict(cur)
        for k, v in clean.items():
            if not merged.get(k):
                merged[k] = v
        merged.setdefault("at", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
        if merged == cur:
            return cur
        rows[sid] = merged
        if len(rows) > _CAP:
            for old in sorted(rows, key=lambda s: rows[s].get("at", ""))[:len(rows) - _CAP]:
                rows.pop(old, None)
        ORIGINS_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = f"{ORIGINS_FILE}.{os.getpid()}.tmp"
        with open(tmp, "w") as f:
            json.dump({"origins": rows}, f, indent=2)
        os.replace(tmp, ORIGINS_FILE)
        return merged
    except OSError:
        return None


def env_parent(env: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """Origin fields inherited from the spawn-env markers of the running
    process (empty for a human shell)."""
    env = os.environ if env is None else env
    worker = env.get(ENV_WORKER, "")
    if not worker:
        return {}
    out = {"parent_worker_id": worker}
    sid = env.get(ENV_SESSION, "") or env.get("CLAUDE_CODE_SESSION_ID", "")
    for key, field in ((ENV_REF, "ref"), (ENV_QUEUE, "queue"), (ENV_ROLE, "parent_role")):
        if env.get(key):
            out[field] = env[key]
    if sid:
        out["parent_session_id"] = sid
    return out


def note_spawn(session_id: str, *, role: str = "", ref: str = "", queue: str = "",
               worker_id: str = "", engine: str = "", via: str = "",
               purpose: str = "") -> Optional[Dict[str, Any]]:
    """Record a session this process just spawned. Explicit args win; parent,
    ticket and queue fall back to the spawner's own env markers. ``worker_id``
    is the session's own worker id (managed spawns); the spawner is always
    ``parent_worker_id``."""
    parent = env_parent()
    return record(
        session_id,
        role=role or "probe",
        ref=ref or parent.get("ref", ""),
        queue=queue or parent.get("queue", ""),
        worker_id=worker_id,
        parent_worker_id=parent.get("parent_worker_id", ""),
        parent_session_id=parent.get("parent_session_id", ""),
        engine=engine, via=via, purpose=purpose,
    )

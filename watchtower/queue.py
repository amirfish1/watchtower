#!/usr/bin/env python3
# Copyright (c) 2026 Amir Fish. All rights reserved.
# SPDX-License-Identifier: LicenseRef-WatchTower-Software-License

"""Durable, numbered, stateful work queue — the WatchTower engine.

This module is the self-contained heart of WatchTower. It replaces
fire-and-forget task injection with a single durable queue file: every request
becomes a numbered item with a status that survives processes, so:

  * nothing is silently dropped (it's a row, not a paragraph in a transcript),
  * a human can refer to work by ref ("take CCC-7"),
  * multiple workers can drain the queue in parallel by *claiming* items
    instead of stepping on each other.

Storage: a single SQLite file (stdlib ``sqlite3`` — still no pip deps).
Resolution order for the store's *base* path (kept from the JSON era):

  1. ``$WATCHTOWER_STORE`` (explicit override — used by tests and CI).
  2. The existing CCC store at ``~/.claude/command-center/ux-fixes-queue.json``
     if it (or its migrated ``.db``) already exists on this machine.
  3. ``~/.watchtower/queues.json`` (WatchTower's own default).

The authoritative file is that path with a ``.db`` suffix once it exists;
until then the legacy JSON is read directly, and the first locked save (or
``wt migrate-store``) imports it into SQLite. JSON remains the interchange
format (``wt export-json``). Design:
docs/superpowers/specs/2026-08-20-sqlite-store-design.md

Concurrency: writers from different processes are serialised with an ``fcntl``
lock file (unchanged); saves diff per-item rows so a claim/close writes one
row instead of the whole store.

Item shape::

    {
      "number": 7,                       # global monotonic id (stable, internal)
      "project": "DEMO",                 # queue / project namespace
      "seq": 2,                          # per-queue counter (derived)
      "ref": "DEMO-2",                   # human-facing id = QUEUE-seq
      "id": "...",                       # source id (if any)
      "status": "open",                  # open | in_progress | closed
      "lane": "normal",                  # normal | express
      "source": "wt",                    # which tool created it
      "note": "...",                     # short request
      "text": "...",                     # full prompt for a worker
      "url": "...", "title": "...", "selector": "...",
      "screenshot_path": "...", "repo_path": "...",
      "claimed_by": null, "claimed_at": null, "closed_at": null,
      "claimed_session_id": null,        # real worker/session id, when known
      "resolution": {                    # HOW it was fixed (set on close, optional)
        "summary": "...",                # the main one-liner
        "commit": "...",                 # verified code-change commit, if any
        "no_code": true,                  # explicit non-code completion, if any
        "caveats": [...], "follow_ups": [...], "unresolved": [...]
      },
      "history": [                       # append-only lifecycle trail (WT-87):
        {"event": "claim", "session_id": "...", "worker": "...", "at": "..."},
        {"event": "reopen", "reason": "worker gone", "at": "..."},
        {"event": "close", "session_id": "...", "resolution": {...}, "at": "..."}
      ],
      "created_at": "2026-06-25T20:05:00Z",
      "updated_at": "2026-06-25T20:05:00Z"
    }

The file holds ``{"counter": <int>, "items": [<item>, ...]}``.
"""

from __future__ import annotations

import json
import os
import re as _re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

try:  # POSIX cross-process locking; degrade gracefully if unavailable.
    import fcntl  # type: ignore
except Exception:  # pragma: no cover - non-POSIX
    fcntl = None  # type: ignore

VALID_STATUSES = ("open", "in_progress", "closed")
VALID_LANES = ("normal", "express")

VALID_ITEM_TYPES = ("bug", "feature", "")
# An item with no (or unknown) type is a bug by default. A ticket filed without
# a type must never silently vanish from a type-restricted queue (e.g. a
# bugs-only queue): the safe default is the actionable one, so untyped work
# still gets claimed. This is the single source of truth for "effective type".
DEFAULT_ITEM_TYPE = "bug"


def effective_type(it_or_value: Any) -> str:
    """The effective type of an item (or a raw type value): its declared type
    if it is a known type, else :data:`DEFAULT_ITEM_TYPE` ("bug").

    Accepts either an item dict or a bare type string so it can be used both
    on stored tickets and on values being written."""
    if isinstance(it_or_value, dict):
        raw = it_or_value.get("item_type") or it_or_value.get("type")
    else:
        raw = it_or_value
    s = str(raw or "").strip().lower()
    return s if s in ("bug", "feature") else DEFAULT_ITEM_TYPE
VALID_READINESS = ("ready", "needs-shaping", "needs-spec", "")
# Readiness values claim_next() excludes by default (a worker gets these only
# by passing shaping=True or an explicit readiness_filters whitelist). Single
# source of truth shared by claim_next/peek_next/count_claimable below, the
# GitHub backend's mirror, and health.queue_status's claimable_depth -- so
# "is this ticket claimable" can never drift between what a worker would
# actually claim and what the reconciler thinks is spawn-worthy (see WT's
# SPAWN/REAP churn bug: needs-spec tickets counted as claimable depth even
# though claim_next would never hand them to a default worker).
UNCLAIMABLE_READINESS = ("needs-shaping", "needs-spec")
VALID_PRIORITIES = ("p0", "p1", "p2", "p3", "p4", "")
VALID_VALUES = ("H", "M", "L", "")
VALID_CONFIDENCES = ("H", "M", "L", "")
# FEAT-NEXT-120 — a filer's best-guess minimum model this ticket needs. Not a
# blocker at filing time (empty is fine, filer never waits for certainty);
# checked against the claiming queue's configured model at claim time (see
# config.model_floor_met). Canonical model ids, not aliases -- keep in sync
# with config.py's MODEL_EFFORTS as new models get approved.
VALID_MODEL_FLOORS = (
    "kimi-code/k3",
    "kimi-code/kimi-for-coding",
    "kimi-code/kimi-for-coding-highspeed",
    "claude-sonnet-5",
    "claude-opus-4-8",
    "claude-opus-5",
    "",
)

# Legacy CCC store — WatchTower reads it if present so it works on this machine
# today, before any WatchTower-native queue exists.
_CCC_LEGACY_STORE = Path.home() / ".claude" / "command-center" / "ux-fixes-queue.json"
# WatchTower's own default home.
_WT_DEFAULT_STORE = Path.home() / ".watchtower" / "queues.json"
# Unified activity log — queue events (enqueue/claim/close) + reconciler (spawn/reap).
_ACTIVITY_LOG = Path.home() / ".watchtower" / "activity.log"


def _resolve_activity_log_path() -> Path:
    """Resolve the active activity-log path. Read fresh each call, mirroring
    ``_resolve_store_path``, so tests can isolate it via $WATCHTOWER_ACTIVITY_LOG
    instead of appending synthetic events to the real shared log."""
    env = os.environ.get("WATCHTOWER_ACTIVITY_LOG")
    if env:
        return Path(env).expanduser()
    return _ACTIVITY_LOG


def _log_many(events: List[tuple]) -> bool:
    """Append multiple activity events with one write.

    Lifecycle audit bundles use this strict return value to fail closed when
    their evidence cannot be recorded. Other activity producers may continue
    to use ``_log()``, whose best-effort behavior is unchanged.
    """
    try:
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        log_path = _resolve_activity_log_path()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        sid = os.environ.get("CLAUDE_CODE_SESSION_ID", "")
        lines = []
        for verb, detail, queue in events:
            # Append [sid:xxxx] for session-initiated commands so the log shows
            # WHICH worker session triggered each operation. ENQUEUE and CLAIM
            # are excluded because their existing details already identify the
            # initiating actor.
            if sid and str(verb).upper() not in ("ENQUEUE", "CLAIM"):
                detail = f"{detail} [sid:{sid[:8]}]"
            q_col = (queue or "reconciler")
            lines.append(f"{now}  {q_col:<14}  {verb:<9}{detail}\n")
        with open(log_path, "a") as f:
            f.write("".join(lines))
        return True
    except Exception:
        return False


def _log(verb: str, detail: str, queue: str = "") -> bool:
    """Append one plain-text line to the unified activity log."""
    return _log_many([(verb, detail, queue)])


def _resolve_store_path() -> Path:
    """Resolve the active store path. See module docstring for the order.

    Read fresh each call so tests can flip ``$WATCHTOWER_STORE`` between runs.
    """
    env = os.environ.get("WATCHTOWER_STORE")
    if env:
        return Path(env).expanduser()
    # The .db check keeps resolution stable if someone deletes the frozen
    # legacy JSON after its store has migrated to SQLite.
    if _CCC_LEGACY_STORE.exists() or _CCC_LEGACY_STORE.with_suffix(".db").exists():
        return _CCC_LEGACY_STORE
    return _WT_DEFAULT_STORE


def _db_path() -> Path:
    """The SQLite store beside the (possibly legacy) JSON path.

    Once this file exists it is authoritative and the JSON file is frozen;
    checked fresh on every load/save so long-running processes flip to the
    DB the moment a migration creates it, with no restart."""
    return _resolve_store_path().with_suffix(".db")


def store_path() -> Path:
    """Public accessor for the authoritative store file.

    CCC's queue-events SSE stats this path for changes, so it must track the
    live backend: the SQLite DB once it exists, else the JSON file."""
    db = _db_path()
    return db if db.exists() else _resolve_store_path()


def _lock_path() -> Path:
    return _resolve_store_path().with_suffix(".lock")


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# A reachable worker/session id is a UUID. Used to decide whether a value handed
# to us is a reachable id (worth storing as ``claimed_session_id``) or just a
# free-form attribution label.
_SESSION_ID_RE = _re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


def _coerce_session_uuid(value: Any) -> Optional[str]:
    """Return a reachable session id from ``value`` if one is present, else None.

    Kimi sessions are indexed (by both kimi itself and CCC) under their
    ``session_<uuid>`` prefixed form, so that shape is preserved verbatim --
    extracting the bare UUID would produce an id no consumer can resolve."""
    s = str(value or "").strip()
    if not s:
        return None
    if _SESSION_ID_RE.match(s):
        return s
    if _re.match(
        r"^session_[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
        r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
        s,
    ):
        return s
    m = _re.search(
        r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}",
        s,
    )
    return m.group(0) if m else None


class _FileLock:
    """Best-effort cross-process advisory lock around the queue file."""

    def __init__(self, path: Path):
        self._path = path
        self._fh = None

    def __enter__(self):
        if fcntl is None:
            return self
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._fh = open(self._path, "w")
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX)
        except OSError:
            self._fh = None
        return self

    def __exit__(self, *exc):
        if self._fh is not None:
            try:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            finally:
                self._fh.close()
                self._fh = None
        return False


def _empty_store() -> Dict[str, Any]:
    return {"counter": 0, "items": []}


def _norm_project(value: Any) -> str:
    """Uppercase, alnum-only short queue code (e.g. 'DEMO'). Empty -> ''."""
    s = "".join(ch for ch in str(value or "").upper() if ch.isalnum() or ch in "-_")
    return s.strip("-_")


def _queue_for_repo_path(repo_path: str) -> str:
    """Return the configured queue whose repo_path matches, else ''.

    Configured queues use short codes (CCC, BYM) that rarely equal the repo
    basename (claude-command-center, BYM+Finie). A client that files by
    repo_path alone must still land in the right queue, so we check the config
    for an exact repo_path match before falling back to the basename."""
    if not repo_path:
        return ""
    target = str(repo_path).rstrip("/")
    try:
        from . import config
        for name, conf in (config.all_queues() or {}).items():
            cfg_rp = str((conf or {}).get("repo_path") or "").rstrip("/")
            if cfg_rp and cfg_rp == target:
                return _norm_project(name)
    except Exception:
        pass
    return ""


def _project_for(source: str = "", repo_path: str = "", project: str = "") -> str:
    """Decide an item's queue: explicit > configured-repo match > repo basename > source > GEN."""
    explicit = _norm_project(project)
    if explicit:
        return explicit
    if repo_path:
        configured = _queue_for_repo_path(repo_path)
        if configured:
            return configured
        base = os.path.basename(str(repo_path).rstrip("/")).lower()
        if base:
            return _norm_project(base)
    src = _norm_project(source)
    return src or "GEN"


def _project_from_ident(ident: Any) -> str:
    """Extract the queue prefix from a human ref like ``WT-20``."""
    s = str(ident or "").strip()
    m = _re.match(r"^([A-Za-z0-9_-]+)-\d+$", s)
    return _norm_project(m.group(1)) if m else ""


def _github_backend_for_project(project: Any):
    """Return a GitHub backend for ``project`` when that queue is configured.

    The file-backed JSON store remains the default. A queue opts into GitHub via
    config.backend(queue) == "github"; then this module's public API delegates
    add/list/get/claim/close to GitHub Issues.
    """
    proj = _norm_project(project)
    if not proj:
        return None
    try:
        from . import config
        if config.backend(proj) != "github":
            return None
        from .github_backend import GitHubIssuesBackend
        repo = config.github_repo(proj)
        return GitHubIssuesBackend(
            proj,
            repo=repo,
            repo_path=config.repo_path(proj),
            assignee=config.github_assignee(proj),
            # Queue-level eligibility inputs, resolved here so the backend
            # judges every item against one policy snapshot per operation.
            auto_drain=config.auto_drain(proj),
            grace_s=config.grace_s(proj),
            partition_by_label=len(config.github_queues_for_repo(repo)) > 1,
        )
    except Exception:
        raise


def _github_projects() -> List[str]:
    try:
        from . import config
        return [
            _norm_project(name)
            for name in config.all_queues()
            if config.backend(name) == "github"
        ]
    except Exception:
        return []


def _normalize_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Ensure every item has project/seq/ref. Deterministic + idempotent: refs
    are assigned per-queue in global-number order, so they stay stable as long
    as items aren't reordered or removed (status changes keep them in the list).
    """
    counts: Dict[str, int] = {}
    for it in sorted(items, key=lambda x: int(x.get("number", 0))):
        proj = it.get("project") or _project_for(
            it.get("source", ""), it.get("repo_path", ""), ""
        )
        it["project"] = proj
        counts[proj] = counts.get(proj, 0) + 1
        it["seq"] = counts[proj]
        it["ref"] = f"{proj}-{counts[proj]}"
    return items


def _matches(it: Dict[str, Any], ident: Any) -> bool:
    """Match an item by global number or by ref ('DEMO-2', case-insensitive)."""
    s = str(ident).strip()
    if s.isdigit() and int(it.get("number", 0)) == int(s):
        return True
    return str(it.get("ref", "")).upper() == s.upper()


# ---------------------------------------------------------------------------
# SQLite backend (docs/superpowers/specs/2026-08-20-sqlite-store-design.md).
#
# The .db beside the JSON path is authoritative once it exists; the JSON file
# then stays frozen as a migration-time snapshot (kept so path resolution and
# pre-migration readers stay stable). ``item_json`` is the authoritative
# per-item payload; project/ref/status/updated_at are denormalized copies for
# indexes and ad-hoc ``sqlite3`` inspection.

_SCHEMA_VERSION = 1


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), timeout=5.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS items ("
        " number INTEGER PRIMARY KEY,"
        " project TEXT NOT NULL DEFAULT '',"
        " ref TEXT NOT NULL DEFAULT '',"
        " status TEXT NOT NULL DEFAULT 'open',"
        " updated_at TEXT NOT NULL DEFAULT '',"
        " item_json TEXT NOT NULL)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_items_project_status ON items(project, status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_items_ref ON items(ref)")


def _meta_get(conn: sqlite3.Connection, key: str, default: str = "") -> str:
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row[0] if row else default


def _canon(it: Dict[str, Any]) -> str:
    """Canonical per-item serialization; stable so saves can diff rows."""
    return json.dumps(it, sort_keys=True, separators=(",", ":"))


def _item_row(it: Dict[str, Any]) -> tuple:
    return (
        int(it.get("number", 0)),
        str(it.get("project", "")),
        str(it.get("ref", "")),
        str(it.get("status", "")),
        str(it.get("updated_at", "")),
        _canon(it),
    )


_ITEM_UPSERT = (
    "INSERT INTO items (number, project, ref, status, updated_at, item_json)"
    " VALUES (?, ?, ?, ?, ?, ?)"
    " ON CONFLICT(number) DO UPDATE SET project=excluded.project,"
    " ref=excluded.ref, status=excluded.status,"
    " updated_at=excluded.updated_at, item_json=excluded.item_json"
)
_META_UPSERT = (
    "INSERT INTO meta (key, value) VALUES (?, ?)"
    " ON CONFLICT(key) DO UPDATE SET value=excluded.value"
)


def _read_db(db: Path, *, strict: bool = False) -> Dict[str, Any]:
    try:
        conn = _connect(db)
        try:
            counter = int(_meta_get(conn, "counter", "0") or 0)
            items = [
                json.loads(row[0])
                for row in conn.execute("SELECT item_json FROM items ORDER BY number")
            ]
        finally:
            conn.close()
    except (sqlite3.Error, ValueError, json.JSONDecodeError):
        if strict:
            raise
        return _empty_store()
    return {"counter": counter, "items": items}


def _create_db(db: Path, data: Dict[str, Any]) -> None:
    """Build a fresh DB at a temp path and atomically move it into place.

    This IS the JSON→SQLite migration when the loaded ``data`` came from a
    legacy JSON store. Callers hold the writer flock; the temp+replace makes
    the flip atomic for lock-free readers too.

    Duplicate numbers (real stores have them from the CCC/WT dual-writer
    era: distinct tickets sharing an internal number) are renumbered, never
    collapsed — refs are the human-facing ids and stay untouched; the later
    duplicate gets a fresh number past the counter."""
    items = data.get("items", [])
    counter = int(data.get("counter", 0))
    next_num = max([counter] + [int(it.get("number", 0)) for it in items])
    seen: set = set()
    rows = []
    for it in items:
        num = int(it.get("number", 0))
        if num in seen:
            next_num += 1
            it = dict(it, number=next_num)
            num = next_num
        seen.add(num)
        rows.append(_item_row(it))
    counter = max(counter, next_num)

    db.parent.mkdir(parents=True, exist_ok=True)
    tmp = db.with_name(db.name + f".tmp{os.getpid()}")
    try:
        tmp.unlink()
    except FileNotFoundError:
        pass
    conn = sqlite3.connect(str(tmp))
    try:
        _ensure_schema(conn)
        with conn:
            conn.executemany(_ITEM_UPSERT, rows)
            conn.executemany(
                _META_UPSERT,
                [
                    ("counter", str(counter)),
                    ("revision", "1"),
                    ("schema_version", str(_SCHEMA_VERSION)),
                ],
            )
    finally:
        conn.close()
    os.replace(tmp, db)


def revision() -> int:
    """Monotonic store change-token (0 while the store is still JSON).

    Bumped by every save; lets watchers poll for change cheaply instead of
    stat()ing file mtimes."""
    db = _db_path()
    if not db.exists():
        return 0
    try:
        conn = _connect(db)
        try:
            return int(_meta_get(conn, "revision", "0") or 0)
        finally:
            conn.close()
    except (sqlite3.Error, ValueError):
        return 0


def migrate_store() -> Dict[str, Any]:
    """Ensure the SQLite store exists, importing the JSON store if present.

    Returns ``{"migrated": bool, "items": int, "db": str}``. Raises if the
    JSON source is corrupt (a bad source must never silently become an empty
    authoritative DB)."""
    db = _db_path()
    if db.exists():
        return {"migrated": False, "items": len(_load_unlocked()["items"]), "db": str(db)}
    with _FileLock(_lock_path()):
        data = _load_unlocked(strict=True)
        _save_unlocked(data)
    return {"migrated": True, "items": len(data["items"]), "db": str(db)}


def export_data() -> Dict[str, Any]:
    """The full store in the classic JSON shape — the interchange format."""
    data = _load_unlocked(strict=True)
    return {"counter": int(data.get("counter", 0)), "items": data.get("items", [])}


def _load_unlocked(*, strict: bool = False) -> Dict[str, Any]:
    db = _db_path()
    if db.exists():
        data = _read_db(db, strict=strict)
    else:
        try:
            with open(_resolve_store_path(), "r") as f:
                data = json.load(f)
        except FileNotFoundError:
            return _empty_store()
        except (OSError, json.JSONDecodeError):
            if strict:
                raise
            return _empty_store()
    if not isinstance(data, dict):
        if strict:
            raise ValueError("queue store root must be a JSON object")
        return _empty_store()
    data.setdefault("counter", 0)
    items = data.get("items")
    if not isinstance(items, list):
        if strict and items is not None:
            raise ValueError("queue store items must be a JSON list")
        items = []
    data["items"] = items
    _normalize_items(data["items"])
    # WT-2: guard against the stored counter being behind the highest item number
    # already in the file.  This happens when two systems (e.g. CCC's
    # ux_fixes_queue.py and watchtower) share the same store file and write their
    # own counter independently.  Without this bump, enqueue() assigns a number
    # that already belongs to a different item; the final
    # ``next(it for it in items if it["number"] == number)`` then returns the
    # pre-existing item, making the new ticket appear to belong to the wrong queue.
    if data["items"]:
        max_num = max(int(it.get("number", 0)) for it in data["items"])
        if max_num > int(data["counter"]):
            data["counter"] = max_num
    return data


def _save_unlocked(data: Dict[str, Any]) -> None:
    """Persist the store to SQLite, diffing so a mutation writes one row.

    First save on a machine (no .db yet) builds the DB from ``data`` — which
    the caller just loaded from the legacy JSON if one existed, so that
    build is the one-time migration. Callers hold ``_FileLock`` (same
    contract as the JSON era), so load→diff→write is race-free."""
    db = _db_path()
    if not db.exists():
        _create_db(db, data)
        return
    conn = _connect(db)
    try:
        _ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")
        existing = dict(conn.execute("SELECT number, item_json FROM items").fetchall())
        seen = set()
        for it in data.get("items", []):
            row = _item_row(it)
            seen.add(row[0])
            if existing.get(row[0]) != row[5]:
                conn.execute(_ITEM_UPSERT, row)
        gone = set(existing) - seen
        if gone:
            conn.executemany("DELETE FROM items WHERE number = ?", [(n,) for n in gone])
        rev = int(_meta_get(conn, "revision", "0") or 0) + 1
        conn.executemany(
            _META_UPSERT,
            [
                ("counter", str(int(data.get("counter", 0)))),
                ("revision", str(rev)),
                ("schema_version", str(_SCHEMA_VERSION)),
            ],
        )
        conn.commit()
    finally:
        conn.close()
    # WAL commits land in the -wal sidecar, so the DB file's mtime would go
    # stale; bump it so mtime-watchers (CCC's queue-events SSE) keep working.
    try:
        os.utime(db)
    except OSError:
        pass


def _norm_choice(value: Any, valid_values: tuple, default: str = "") -> str:
    """Coerce ``value`` to one of ``valid_values``, or return ``default``."""
    s = str(value or "").strip()
    if s in valid_values:
        return s
    return default


def _prio_rank(it: Dict[str, Any]) -> int:
    """Numeric rank for priority sorting (lower = higher priority)."""
    return {"p0": 0, "p1": 1, "p2": 2, "p3": 3, "p4": 4}.get(it.get("priority", ""), 5)


def _type_rank(it: Dict[str, Any]) -> int:
    """Bugs before features within same priority tier. Untyped == bug."""
    return {"bug": 0, "feature": 1}.get(effective_type(it), 2)


def _clip(value: Any, max_len: int) -> str:
    s = "" if value is None else str(value)
    s = " ".join(s.split()) if max_len <= 240 else s  # keep prompts multi-line
    return s if len(s) <= max_len else s[:max_len].rstrip() + "…"


def _by(kind: str = "system", worker: str = "", session_id: str = "") -> Dict[str, str]:
    kind = kind if kind in ("worker", "human", "system") else "system"
    out = {"kind": kind}
    if worker:
        out["worker"] = str(worker)
    if session_id:
        out["session_id"] = str(session_id)
    return out


def _normalize_by(value: Any = None, *, worker: Any = "", session_id: Any = "", event: str = "") -> Dict[str, str]:
    if isinstance(value, dict):
        kind = str(value.get("kind") or "").strip()
        out = _by(kind if kind else "system")
        w = value.get("worker") or worker
        sid = value.get("session_id") or session_id
        if w:
            out["worker"] = str(w)
        if sid:
            out["session_id"] = str(sid)
        return out
    if isinstance(value, str) and value in ("worker", "human", "system"):
        return _by(value, str(worker or ""), str(session_id or ""))
    if worker or session_id:
        kind = "human" if event in ("answer", "comment") else "worker"
        return _by(kind, str(worker or ""), str(session_id or ""))
    if event in ("answer", "comment"):
        return _by("human")
    return _by("system")


def _append_history(
    it: Dict[str, Any],
    event: str,
    *,
    by: Any = None,
    at: str = "",
    text: str = "",
    **fields: Any,
) -> None:
    """Append one canonical ticket event."""
    hist = it.get("history")
    if not isinstance(hist, list):
        hist = []
    entry: Dict[str, Any] = {
        "event": event,
        "at": str(at or _now_iso()),
        "by": _normalize_by(by, event=event),
    }
    if text:
        entry["text"] = text
    for key, value in fields.items():
        if value is not None and value != "":
            entry[key] = value
    hist.append(entry)
    it["history"] = hist


def _timeline_event(raw: Dict[str, Any], default_at: str = "") -> Optional[Dict[str, Any]]:
    event = str(raw.get("event") or "").strip()
    if not event:
        return None
    at = str(raw.get("at") or default_at or "")
    out: Dict[str, Any] = {
        "event": event,
        "at": at,
        "by": _normalize_by(
            raw.get("by"),
            worker=raw.get("worker") or "",
            session_id=raw.get("session_id") or "",
            event=event,
        ),
    }
    for key, value in raw.items():
        if key in ("event", "at", "by", "worker", "session_id"):
            continue
        if value is not None and value != "":
            out[key] = value
    return out


_EVENT_PRECEDENCE = {
    "filed": 0, "claim": 1, "block": 2, "progress": 2,
    "answer": 3, "comment": 4, "close": 5, "reopen": 6,
}


def _add_timeline_event(events: List[Dict[str, Any]], raw: Dict[str, Any], *, synthesized: bool = False) -> None:
    ev = _timeline_event(raw)
    if ev is None:
        return
    if synthesized and any(e.get("event") == ev.get("event") and e.get("at") == ev.get("at") for e in events):
        return
    ev["_synthesized"] = synthesized
    ev["_idx"] = len(events)
    events.append(ev)


def timeline(item: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return the canonical chronological activity stream for any ticket shape.

    New tickets store this directly in ``history``. Old tickets may still have
    append-only ``answers``/``progress_notes`` or only snapshot fields; those
    are normalized here at read time without mutating the item.
    """
    events: List[Dict[str, Any]] = []

    hist = item.get("history")
    if isinstance(hist, list):
        for raw in hist:
            if isinstance(raw, dict):
                _add_timeline_event(events, raw)

    notes = item.get("progress_notes")
    if isinstance(notes, list):
        for note in notes:
            if not isinstance(note, dict):
                continue
            by = str(note.get("by") or "")
            text = _clip(note.get("text", ""), 24000)
            if by == "human-comment":
                _add_timeline_event(events, {
                    "event": "comment",
                    "at": note.get("at") or "",
                    "by": _by("human"),
                    "text": text,
                })
            elif by == "human-reopen":
                _add_timeline_event(events, {
                    "event": "reopen",
                    "at": note.get("at") or "",
                    "by": _by("human"),
                    "reason": text,
                })
            else:
                _add_timeline_event(events, {
                    "event": "progress",
                    "at": note.get("at") or "",
                    "by": _normalize_by(note.get("by"), event="progress"),
                    "text": text,
                })

    answers = item.get("answers")
    if isinstance(answers, list):
        for ans in answers:
            if not isinstance(ans, dict):
                continue
            by_raw = ans.get("by") or ""
            _add_timeline_event(events, {
                "event": "answer",
                "at": ans.get("at") or "",
                "by": _by("human", str(by_raw or "")),
                "text": _clip(ans.get("text", ""), 24000),
            })

    created_at = str(item.get("created_at") or "")
    if created_at and not any(e.get("event") == "filed" for e in events):
        _add_timeline_event(events, {
            "event": "filed",
            "at": created_at,
            "by": _by("system"),
            "source": item.get("source") or "",
            "project": item.get("project") or "",
        }, synthesized=True)

    if item.get("claimed_at") and not any(e.get("event") == "claim" for e in events):
        _add_timeline_event(events, {
            "event": "claim",
            "at": item.get("claimed_at"),
            "by": _by("worker", str(item.get("claimed_by") or ""), str(item.get("claimed_session_id") or "")),
        }, synthesized=True)

    if item.get("block_question") and item.get("blocked_at") and not any(e.get("event") == "block" for e in events):
        _add_timeline_event(events, {
            "event": "block",
            "at": item.get("blocked_at"),
            "by": _by("worker", str(item.get("claimed_by") or ""), str(item.get("claimed_session_id") or "")),
            "question": _clip(item.get("block_question"), 4000),
        }, synthesized=True)

    if item.get("closed_at") and not any(e.get("event") == "close" for e in events):
        norm = _normalize_resolution(item.get("resolution"))
        _add_timeline_event(events, {
            "event": "close",
            "at": item.get("closed_at"),
            "by": _by("worker", str(item.get("closed_by") or item.get("claimed_by") or ""), str(item.get("claimed_session_id") or "")),
            "resolution": norm,
        }, synthesized=True)

    def _sort_key(e: Dict[str, Any]) -> tuple:
        ts = str(e.get("at") or "")
        if e.get("_synthesized"):
            # Synthesized events (from snapshot fields) sort BEFORE real history
            # events at the same timestamp, ordered by causal precedence.
            return (ts, 0, _EVENT_PRECEDENCE.get(str(e.get("event") or ""), 99), 0)
        # Real history events sort after synthesized ones at the same timestamp,
        # preserving their original insertion order (causal ground truth).
        return (ts, 1, 0, e.get("_idx", 0))

    result = sorted(events, key=_sort_key)
    for e in result:
        e.pop("_synthesized", None)
        e.pop("_idx", None)
    return result


def enqueue(
    *,
    note: str,
    text: str = "",
    source: str = "wt",
    project: str = "",
    annotation_id: str = "",
    url: str = "",
    title: str = "",
    selector: str = "",
    screenshot_path: str = "",
    repo_path: str = "",
    lane: str = "normal",
    item_type: str = "",
    readiness: str = "",
    priority: str = "",
    value: str = "",
    confidence: str = "",
    model_floor: str = "",
) -> Dict[str, Any]:
    """Append a new ``open`` item and return it (with its assigned ref)."""
    note = _clip(note, 4000)
    if not note and not text:
        raise ValueError("note or text is required")
    lane = lane if lane in VALID_LANES else "normal"
    proj = _project_for(source, repo_path, project)
    # A queue's first-ever ticket must make it visible to the reconciler
    # (workers._reconcile_once_locked() only iterates config.all_queues()),
    # or a ▶ press on it silently no-ops forever: dispatch_after_enqueue()
    # nudges no live worker (there's never been one), falls through to
    # reconcile_once(), which skips a queue with no config entry entirely --
    # not even into its own `skipped` list -- so the dispatch reason comes
    # back as the generic "no live worker accepted and none spawned" with no
    # indication the real cause is "this queue was never registered" (WT-131).
    # auto_drain stays default-off; this only makes the queue exist.
    try:
        from . import config
        config.ensure_entry(proj)
    except Exception:
        pass
    backend = _github_backend_for_project(proj)
    if backend is not None:
        saved = backend.enqueue(
            note=note,
            text=text,
            source=source,
            annotation_id=annotation_id,
            url=url,
            title=title,
            selector=selector,
            screenshot_path=screenshot_path,
            repo_path=repo_path,
            lane=lane,
            item_type=item_type,
            readiness=readiness,
            priority=priority,
            value=value,
            confidence=confidence,
        )
        _log("ENQUEUE", f"{saved.get('ref', '?')} — {saved.get('title') or saved.get('note', '')[:60]}", queue=saved.get('project', ''))
        return saved
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        data["counter"] = int(data.get("counter", 0)) + 1
        number = data["counter"]
        now = _now_iso()
        item = {
            "number": number,
            "project": proj,
            "id": str(annotation_id or ""),
            "status": "open",
            "lane": lane,
            "source": str(source or "wt"),
            "note": note,
            "text": _clip(text or note, 24000),
            "url": _clip(url, 1000),
            "title": _clip(title, 200),
            "selector": _clip(selector, 1000),
            "screenshot_path": str(screenshot_path or ""),
            "repo_path": str(repo_path or ""),
            "type": effective_type(item_type),
            "readiness": _norm_choice(readiness, VALID_READINESS),
            "priority": _norm_choice(priority, VALID_PRIORITIES),
            "value": _norm_choice(value, VALID_VALUES),
            "confidence": _norm_choice(confidence, VALID_CONFIDENCES),
            "model_floor": _norm_choice(model_floor, VALID_MODEL_FLOORS),
            "needs_input": False,
            "block_question": "",
            # The two ticket-level eligibility inputs (2026-07-26 design). The
            # GitHub backend stores these as labels; here they are plain
            # booleans, so both backends hand downstream code the same shape.
            "no_auto_drain": False,
            "run_requested": False,
            "claimed_by": None,
            "claimed_at": None,
            "closed_at": None,
            "claimed_session_id": None,
            "created_at": now,
            "updated_at": now,
        }
        _append_history(item, "filed", by=_by("system"), at=now, source=item["source"], project=proj)
        data["items"].append(item)
        _normalize_items(data["items"])  # assign this item's seq/ref
        _save_unlocked(data)
        saved = next(it for it in data["items"] if it.get("number") == number)
    _log("ENQUEUE", f"{saved.get('ref', '?')} — {saved.get('title') or saved.get('note', '')[:60]}", queue=saved.get('project', ''))
    return saved


def list_items(
    status: Optional[str] = None,
    lane: Optional[str] = None,
    project: Optional[str] = None,
    *,
    fresh: bool = False,
    strict: bool = False,
) -> List[Dict[str, Any]]:
    if project:
        backend = _github_backend_for_project(project)
        if backend is not None:
            return backend.list_items(
                status=status, lane=lane, fresh=fresh, strict=strict
            )
    data = _load_unlocked(strict=strict)
    items = data.get("items", [])
    github_projects = set(_github_projects()) if not project else set()
    if github_projects:
        items = [
            it for it in items
            if _norm_project(it.get("project") or "") not in github_projects
        ]
    if status:
        items = [it for it in items if it.get("status") == status]
    if lane:
        items = [it for it in items if it.get("lane") == lane]
    if project:
        proj = _norm_project(project)
        items = [it for it in items if it.get("project") == proj]
    if not project:
        for gh_project in github_projects:
            backend = _github_backend_for_project(gh_project)
            if backend is not None:
                try:
                    items.extend(
                        backend.list_items(
                            status=status, lane=lane, fresh=fresh, strict=strict
                        )
                    )
                except Exception as e:
                    if strict:
                        raise
                    if getattr(e, "cached", False):
                        continue
                    import sys
                    print(
                        f"Warning: failed to list items for GitHub-backed queue {gh_project}: {e}",
                        file=sys.stderr,
                    )
                    _log("ERROR", f"GitHub list failed: {e}", queue=gh_project)
    return items


def _set_run_requested(ident: Any, requested: bool, event: str) -> Optional[Dict[str, Any]]:
    """Flip a file-backed ticket's ``run_requested`` flag, recording the event."""
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        for it in data["items"]:
            if _matches(it, ident):
                if bool(it.get("run_requested", False)) == requested:
                    return it
                now = _now_iso()
                it["run_requested"] = requested
                it["updated_at"] = now
                _append_history(it, event, by=_by("system"), at=now)
                _save_unlocked(data)
                return it
    return None


def mark_runnable(ident: Any) -> Optional[Dict[str, Any]]:
    """Request a run for this ticket — the ▶ / ``wt run`` path.

    Sets ``run_requested``, the eligibility input that beats auto_drain being
    off, the no-auto-drain opt-out, and the grace period (2026-07-26 design).
    The GitHub backend stores it as a label; here it is a boolean on the item,
    so both backends leave the same observable state behind. A closed
    file-backed ticket is reopened first — asking to run a closed ticket can
    only mean "work it again", and there is nothing to run while it is closed.
    """
    backend = _github_backend_for_project(_project_from_ident(ident))
    if backend is not None:
        item = backend.mark_runnable(ident)
        if item:
            _log("RUN", f"{item.get('ref', ident)} — run requested", queue=item.get("project", ""))
        return item
    item = get(ident)
    if item is None:
        return None
    if item.get("status") == "closed":
        update_status(ident, "open", reason="marked runnable")
    item = _set_run_requested(ident, True, "run_requested")
    if item:
        _log("RUN", f"{item.get('ref', ident)} — run requested", queue=item.get("project", ""))
    return item


def clear_run_request(ident: Any) -> Optional[Dict[str, Any]]:
    """Cancel a pending run request (pressing ▶ again while still queued).

    The inverse of ``mark_runnable``. It only withdraws the request: nothing
    else about the ticket changes, and a ticket a worker has already claimed
    keeps running — the request has served its purpose by then.
    """
    backend = _github_backend_for_project(_project_from_ident(ident))
    if backend is not None:
        item = backend.clear_run_request(ident)
        if item:
            _log("RUN", f"{item.get('ref', ident)} — run request cleared", queue=item.get("project", ""))
        return item
    item = _set_run_requested(ident, False, "run_request_cleared")
    if item:
        _log("RUN", f"{item.get('ref', ident)} — run request cleared", queue=item.get("project", ""))
    return item


def queues() -> Dict[str, Dict[str, int]]:
    """Per-queue counts: ``{queue: {open, in_progress, closed, total}}``."""
    out: Dict[str, Dict[str, int]] = {}
    for it in list_items():
        proj = it.get("project") or "GEN"
        row = out.setdefault(
            proj, {"open": 0, "in_progress": 0, "closed": 0, "total": 0}
        )
        st = it.get("status", "open")
        if st in row:
            row[st] += 1
        row["total"] += 1
    return out


def get(ident: Any) -> Optional[Dict[str, Any]]:
    backend = _github_backend_for_project(_project_from_ident(ident))
    if backend is not None:
        return backend.get(ident)
    for it in _load_unlocked().get("items", []):
        if _matches(it, ident):
            return it
    return None


def update(ident: Any, **fields: Any) -> Optional[Dict[str, Any]]:
    """Patch arbitrary fields on an item. Used for triage edits (priority,
    readiness, value, etc.).

    Allowed fields: item_type, readiness, priority, value, confidence,
    model_floor, note, text, title, url, selector, screenshot_path,
    repo_path, needs_input, block_question.

    Disallowed (managed by state machine): status, claimed_by, claimed_at,
    closed_at, claimed_session_id, number, project, ref, seq, created_at.

    ``item_type`` and ``"type"`` are aliases — both are stored as ``"type"``.
    Returns the updated item, or None if not found.
    """
    backend = _github_backend_for_project(_project_from_ident(ident))
    if backend is not None:
        return backend.update(ident, **fields)
    ALLOWED = frozenset({
        "item_type", "type", "readiness", "priority", "value", "confidence",
        "note", "text", "title", "url", "selector", "screenshot_path", "repo_path",
        "needs_input", "block_question", "model_floor",
    })
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        for it in data["items"]:
            if _matches(it, ident):
                now = _now_iso()
                for k, v in fields.items():
                    if k in ALLOWED:
                        # "item_type" and "type" are aliases — store as "type"
                        key = "type" if k == "item_type" else k
                        it[key] = v
                changed = {
                    ("type" if k == "item_type" else k): v
                    for k, v in fields.items()
                    if k in ALLOWED
                }
                if changed:
                    _append_history(it, "edit", by=_by("system"), at=now, fields=changed)
                it["updated_at"] = now
                _save_unlocked(data)
                return it
    return None


def move(ident: Any, new_project: str) -> Optional[Dict[str, Any]]:
    """Move a ticket to a different queue in place (WT-83): reassigns its ref
    within the target queue (refs are derived from project+number, see
    _normalize_items) but preserves status/claim state/notes/history. Avoids
    the refile-new-ticket + close-original workaround, which churns refs and
    inflates the closed count.

    Only supported between file-backed queues -- a GitHub-backed queue's
    tickets are GitHub issues living in that queue's configured repo, so
    there's no in-place move across backends.
    """
    from . import config
    new_project = _norm_project(new_project)
    if not new_project:
        raise ValueError("new queue name is required")
    if config.backend(new_project) == "github":
        raise ValueError(
            f"{new_project} is a GitHub-backed queue; cross-backend moves aren't supported"
        )
    if _github_backend_for_project(_project_from_ident(ident)) is not None:
        raise ValueError(
            f"{ident} is in a GitHub-backed queue; cross-backend moves aren't supported"
        )
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        for it in data["items"]:
            if _matches(it, ident):
                old_ref = it.get("ref", "")
                old_project = it.get("project", "")
                it["project"] = new_project
                now = _now_iso()
                it["updated_at"] = now
                _normalize_items(data["items"])
                _append_history(
                    it,
                    "move",
                    by=_by("system"),
                    at=now,
                    from_ref=old_ref,
                    to_ref=it.get("ref", ""),
                    from_project=old_project,
                    to_project=new_project,
                )
                _save_unlocked(data)
                _log("MOVE", f"{old_ref} -> {it.get('ref', '?')}", queue=new_project)
                return it
    return None


def _claim_candidates(
    items: List[Dict[str, Any]],
    *,
    project: Optional[str] = None,
    lane: Optional[str] = None,
    shaping: bool = False,
    oldest: bool = False,
    item_types: Optional[List[str]] = None,
    readiness_filters: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Return ``items`` filtered + sorted exactly as claim_next() would pick
    from them — the single source of truth for "is this ticket claimable
    right now", shared by claim_next, peek_next, and count_claimable. Any
    caller that needs to know what a worker COULD claim (without claiming it)
    goes through here instead of re-implementing the filter, so it can never
    silently drift out of sync (``project`` is expected pre-normalized).
    """
    candidates = [it for it in items if it.get("status") == "open"]
    if project:
        candidates = [it for it in candidates if it.get("project") == project]
    if lane:
        candidates = [it for it in candidates if it.get("lane") == lane]
    if readiness_filters:
        candidates = [it for it in candidates if it.get("readiness", "") in readiness_filters]
    elif not shaping:
        candidates = [it for it in candidates if it.get("readiness", "") not in UNCLAIMABLE_READINESS]
    if item_types:
        candidates = [it for it in candidates if effective_type(it) in item_types]
    if oldest:
        candidates = sorted(candidates, key=lambda it: int(it.get("number", 0)))
    else:
        candidates = sorted(
            candidates,
            key=lambda it: (
                0 if it.get("lane") == "express" else 1,
                _prio_rank(it),
                _type_rank(it),
                int(it.get("number", 0)),
            ),
        )
    return candidates


def count_claimable(
    project: Optional[str] = None,
    lane: Optional[str] = None,
    item_types: Optional[List[str]] = None,
) -> int:
    """How many tickets claim_next() would currently pick from for ``project``,
    in default (non-shaping) mode. Used by the reconciler to decide whether a
    queue has real, claimable work before spawning a worker for it — reusing
    claim_next's exact candidate filter instead of a hand-rolled copy means it
    can never think a ticket is spawn-worthy when a worker couldn't actually
    claim it (the WT SPAWN/REAP churn bug: needs-spec tickets counted as
    claimable depth even though claim_next excludes them by default)."""
    backend = _github_backend_for_project(project)
    if backend is not None:
        return backend.count_claimable(lane=lane, item_types=item_types)
    proj = _norm_project(project) if project else None
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        return len(_claim_candidates(data["items"], project=proj, lane=lane, item_types=item_types))


def count_manual_eligible(
    project: Optional[str] = None,
    lane: Optional[str] = None,
    item_types: Optional[List[str]] = None,
) -> int:
    """How many claimable tickets a human explicitly asked to run (``▶``).

    ``count_claimable`` answers "does this queue want unattended workers", so it
    is deliberately blind to run requests. This is the other half: what the
    reconciler must still staff on a queue with auto_drain off, because
    auto_drain=off means "no *automatic* work", not "ignore what I pressed".
    Same candidate filter as claim_next, so a run request on a ticket no worker
    could claim (wrong claim_type, needs-shaping) still spawns nothing."""
    backend = _github_backend_for_project(project)
    if backend is not None:
        return backend.count_manual_eligible(lane=lane, item_types=item_types)
    proj = _norm_project(project) if project else None
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        candidates = _claim_candidates(
            data["items"], project=proj, lane=lane, item_types=item_types
        )
        return len([it for it in candidates if it.get("run_requested", False)])


def _hosted_codex_thread_owns_worker(worker_id: str, session_uuid: str) -> bool:
    """Return whether a hosted Codex thread proves its worker continuity.

    A Codex thread can outlive the short-lived ``codex exec`` process that WT
    spawned.  In that state the thread id is the durable identity; require the
    shared registry and its WT metadata to agree before bypassing the normal
    dead-worker guard.
    """
    real_sid = _coerce_session_uuid(session_uuid)
    if not real_sid:
        return False
    try:
        from . import codex_registry
        entry = codex_registry.entry(real_sid) or {}
        wt_meta = entry.get("wt") if isinstance(entry.get("wt"), dict) else {}
        return (
            entry.get("engine") == "codex"
            and str(entry.get("worker_id") or "") == worker_id
            and str(wt_meta.get("worker_id") or "") == worker_id
        )
    except Exception:
        return False


def _verify_worker_live(session_id: str, session_uuid: str = "") -> None:
    """Raise ValueError if session_id is a known-spawned worker that is dead.

    Ambient sessions (wt claim --worker <alias> from a bare Claude session,
    never launched via spawn_workers) are not in the spawn registry, so their
    liveness cannot be checked — they are left alone to match the OPS-104 fix
    in requeue_orphaned_tickets().  Only registered-but-dead workers are
    rejected: those are exactly the ones the reconciler will silently requeue
    2 minutes later, so failing loudly at claim time is strictly better UX.
    """
    try:
        from . import workers as _workers
        known = _workers.list_workers(prune=False)
        known_ids = {str(w.get("worker_id", "")) for w in known} | set(
            _workers._load_worker_id_ledger()
        )
        if session_id not in known_ids:
            return  # not a tracked spawned worker — can't verify, allow through
        live_ids = {str(w.get("worker_id", "")) for w in known if w.get("alive")}
        if session_id not in live_ids:
            if _hosted_codex_thread_owns_worker(session_id, session_uuid):
                return
            raise ValueError(
                f"worker {session_id!r} is registered as a spawned worker but is "
                "not currently alive — claim rejected to prevent a silent requeue"
            )
    except ValueError:
        raise
    except Exception:
        pass  # import/I/O failure → do not block the claim


def claim_next(
    session_id: str,
    lane: Optional[str] = None,
    project: Optional[str] = None,
    session_uuid: str = "",
    shaping: bool = False,
    oldest: bool = False,
    item_types: Optional[List[str]] = None,
    readiness_filters: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    """Atomically move the next ``open`` item to ``in_progress`` and return it.

    Scoped to ``project`` when given, so a worker only drains its own queue.
    Default sort: express lane → priority (p0 first) → bugs before features →
    oldest within tier. Pass ``oldest=True`` for pure FIFO regardless of priority.

    ``item_types``: if non-empty, only claim items whose type is in the list.
    ``readiness_filters``: if non-empty, only claim items whose readiness is in
      the list (bypasses default exclusion of unready items). If empty/None,
      excludes needs-shaping/needs-spec unless ``shaping=True``.

    Returns ``None`` when nothing matches.

    Stop signal: if a reconciler has requested this worker to stop (by placing
    a sentinel file in the stop-signals directory keyed to ``session_id``), the
    file is deleted and ``{"stop": True}`` is returned so the conversation is
    detached from this queue without claiming a new ticket. It may continue
    unrelated work.
    """
    if not session_id:
        raise ValueError("session_id is required")
    _verify_worker_live(session_id, session_uuid)
    # A reconciler stop signal is a durable, queue-scoped release. It must win
    # over a racing enqueue: the released session must never claim more work,
    # while the still-open ticket remains available for replacement staffing.
    # Resolve it before backend routing so file and GitHub queues behave alike.
    try:
        from . import workers as _workers
        stop_dir = _workers.STOP_SIGNALS_DIR
    except Exception:
        stop_dir = Path.home() / ".watchtower" / "stop-signals"
    signal_file = stop_dir / session_id
    has_stop_signal = signal_file.exists()
    if has_stop_signal:
        try:
            signal_file.unlink()
        except OSError:
            pass
        return {"stop": True}

    # Context-budget recycle: a drain worker that chain-claims tickets grows
    # its conversation without bound (observed: 800K/1M tokens ≈ 6.2MB
    # transcript). The claim boundary is the only safe recycle point — never
    # mid-ticket — so an over-budget worker is stopped here exactly like a
    # reconciler release; deficit staffing then spawns a fresh worker.
    try:
        from . import workers as _workers
        over = _workers.context_budget_exceeded(
            session_id, str(session_uuid or "")
        )
    except Exception:
        over = 0
    if over:
        # A worker only reaches claim_next() again after closing or blocking
        # its previous ticket -- by protocol it should never still hold an
        # active (non-blocked) claim here. But if it does (a protocol slip,
        # a retried close, etc.), recycling now would tell it to "exit
        # immediately" while that ticket sits in_progress and unfinished.
        # requeue_orphaned_tickets() only reopens a stranded ticket once its
        # worker's pid is dead -- a recycled worker is merely released (kept
        # alive, same as an idle release), so the ticket would sit stuck
        # until the released process is eventually reaped by
        # RELEASED_TTL_S, up to hours later. Defer the recycle to the next
        # clean boundary instead of stranding it.
        held = [
            it for it in list_active_claims(project)
            if str(it.get("claimed_by") or "") == str(session_id)
        ]
        if not held:
            # This path itself tells the worker to exit, so persist the same
            # queue detachment that a reconciler stop signal establishes.
            # Without it, the worker remains countable and receives
            # stuck-queue nudges after it has correctly honored the recycle
            # stop.
            _workers._mark_worker_released(session_id)
            _log(
                "STOP",
                f"{session_id} — context budget exceeded "
                f"({over} transcript bytes); recycling worker",
                queue=project or "",
            )
            return {"stop": True, "reason": "context_budget"}

    backend = _github_backend_for_project(project)
    if backend is not None:
        item = backend.claim_next(
            session_id,
            lane=lane,
            session_uuid=session_uuid,
            shaping=shaping,
            oldest=oldest,
            item_types=item_types,
            readiness_filters=readiness_filters,
        )
        if item and not item.get("stop"):
            _log("CLAIM", f"{item.get('ref', '?')} by {session_id[:16]} — {item.get('title') or item.get('note', '')[:60]}", queue=item.get('project', ''))
        return item

    real_sid = _coerce_session_uuid(session_uuid) or _coerce_session_uuid(session_id)
    proj = _norm_project(project) if project else None
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        candidates = _claim_candidates(
            data["items"], project=proj, lane=lane, shaping=shaping, oldest=oldest,
            item_types=item_types, readiness_filters=readiness_filters,
        )
        if not candidates:
            return None
        item = candidates[0]
        item["status"] = "in_progress"
        item["claimed_by"] = str(session_id)
        if real_sid:
            item["claimed_session_id"] = real_sid
        item["claimed_at"] = _now_iso()
        item["updated_at"] = item["claimed_at"]
        _append_history(item, "claim", by=_by("worker", str(session_id), str(real_sid or "")), at=item["claimed_at"])
        _save_unlocked(data)
    _log("CLAIM", f"{item.get('ref', '?')} by {session_id[:16]} — {item.get('title') or item.get('note', '')[:60]}", queue=item.get('project', ''))
    return item


def claim_by_ref(
    ref: str,
    session_id: str,
    session_uuid: str = "",
) -> Optional[Dict[str, Any]]:
    """Atomically claim a specific ticket by its ref (e.g. 'CCC-42').

    Returns the claimed item, or None if the ref doesn't exist or isn't open.
    Raises ValueError if the ticket is already in_progress or closed.
    """
    if not session_id:
        raise ValueError("session_id is required")
    _verify_worker_live(session_id, session_uuid)
    backend = _github_backend_for_project(_project_from_ident(ref))
    if backend is not None:
        item = backend.claim_by_ref(ref, session_id, session_uuid=session_uuid)
        if item:
            _log("CLAIM", f"{item.get('ref', '?')} by {session_id[:16]} — {item.get('title') or item.get('note', '')[:60]}", queue=item.get('project', ''))
        return item
    real_sid = _coerce_session_uuid(session_uuid) or _coerce_session_uuid(session_id)
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        item = next((it for it in data["items"] if it.get("ref") == ref), None)
        if item is None:
            return None
        status = item.get("status", "open")
        if status != "open":
            raise ValueError(f"{ref} is not open (status={status})")
        item["status"] = "in_progress"
        item["claimed_by"] = str(session_id)
        if real_sid:
            item["claimed_session_id"] = real_sid
        item["claimed_at"] = _now_iso()
        item["updated_at"] = item["claimed_at"]
        _append_history(item, "claim", by=_by("worker", str(session_id), str(real_sid or "")), at=item["claimed_at"])
        _save_unlocked(data)
    _log("CLAIM", f"{item.get('ref', '?')} by {session_id[:16]} — {item.get('title') or item.get('note', '')[:60]}", queue=item.get('project', ''))
    return item


def peek_next(
    project: Optional[str] = None,
    lane: Optional[str] = None,
    item_types: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    """Return a copy of the next claimable open item without claiming it.

    Uses the same smart sort as claim_next (priority → type → age).
    Returns None when nothing is open and claimable."""
    backend = _github_backend_for_project(project)
    if backend is not None:
        return backend.peek_next(lane=lane, item_types=item_types)
    proj = _norm_project(project) if project else None
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        candidates = _claim_candidates(data["items"], project=proj, lane=lane, item_types=item_types)
        return dict(candidates[0]) if candidates else None


def _normalize_resolution(resolution: Any) -> Optional[Dict[str, Any]]:
    """Coerce a resolution into the stored shape, or None when empty.

    Accepts a bare string (treated as the summary) or a dict with any of
    ``summary`` / ``commit`` / ``no_code`` / ``caveats`` / ``follow_ups`` /
    ``unresolved``. List fields are
    coerced to lists of clipped strings; empty fields are dropped. Returns None
    when nothing meaningful was supplied (so close stays back-compatible)."""
    if resolution is None:
        return None
    if isinstance(resolution, str):
        resolution = {"summary": resolution}
    if not isinstance(resolution, dict):
        return None
    out: Dict[str, Any] = {}
    summary = _clip(resolution.get("summary", ""), 4000)
    if summary:
        out["summary"] = summary
    commit = _clip(resolution.get("commit", ""), 128)
    if commit:
        out["commit"] = commit
    if resolution.get("no_code") is True:
        out["no_code"] = True
    for field in ("caveats", "follow_ups", "unresolved"):
        raw = resolution.get(field)
        if raw is None:
            continue
        if isinstance(raw, str):
            raw = [raw]
        vals = [_clip(v, 4000) for v in raw if str(v or "").strip()]
        if vals:
            out[field] = vals
    return out or None


def backfill_session_id(ident: Any, session_id: str) -> Optional[Dict[str, Any]]:
    """Record a worker's real cloud session UUID on the ticket it holds.

    A WatchTower worker claims with its non-UUID worker_id, so ``claimed_by`` is
    e.g. ``ccc-fbbe9e53`` and ``claimed_session_id`` starts empty. The worker's
    actual session UUID is only knowable once its engine process has started and
    written its log, so WT backfills it into workers.json later. This propagates
    that UUID onto the in_progress ticket so any consumer (e.g. CCC's queue
    health) can resolve the worker to a reachable session instead of treating it
    as unresolvable ("WAITING"/"STUCK" despite a live worker).

    Only writes when the item is in_progress and the field is empty/different —
    idempotent and a no-op once set. Returns the item, or None if not found."""
    real = _coerce_session_uuid(session_id)
    if not real:
        return None
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        for it in data["items"]:
            if _matches(it, ident):
                if it.get("status") != "in_progress":
                    return it
                if it.get("claimed_session_id") == real:
                    return it
                it["claimed_session_id"] = real
                it["updated_at"] = _now_iso()
                _save_unlocked(data)
                return it
    return None


def update_status(
    ident: Any,
    status: str,
    session_id: str = "",
    session_uuid: str = "",
    resolution: Any = None,
    quiet: bool = False,
    require_status: Optional[str] = None,
    reason: str = "",
    expect_owner: str = "",
) -> Optional[Dict[str, Any]]:
    """``require_status``, when set, makes this a compare-and-swap: the
    transition is only applied if the item's *current* status (read fresh,
    inside the lock) still matches. Without it, a caller that decided to
    transition an item based on a stale snapshot (e.g. the orphan-ticket
    reconciler, which reads ``list_items()`` before deciding) can clobber a
    legitimate concurrent transition — e.g. reopening a ticket that was
    closed a moment after the snapshot was taken (OPS-72).

    ``expect_owner``, when set (close path only), makes the close refuse to
    re-close an already-closed ticket or close a live claim owned by a
    different worker, raising ``ValueError``. This is the
    durable stop for reap-induced duplicate work: a worker reaped mid-ticket
    (idle past the prompt-cache TTL) gets its claim reopened and re-drained by
    a fresh worker; when the reaped session later resumes from checkpoint with
    stale context and tries to close, the ticket is already closed by the fresh
    worker, so this guard refuses and tells it the work is a duplicate — rather
    than silently re-closing and overwriting the real resolution (observed:
    CCC-502 closed twice, once by the reaped session after the fresh worker had
    already fixed and closed it). A worker also cannot close another worker's
    still-open claim. ``expect_owner`` is left empty by non-worker closes
    (e.g. dedup-close by ref) so those are unaffected. The same guard is
    forwarded to GitHub-backed queues.

    ``reason`` (optional) is recorded on the appended ``history`` entry, e.g.
    the orphan-ticket sweep passes "worker gone" for a reopen."""
    if status not in VALID_STATUSES:
        raise ValueError(f"status must be one of {VALID_STATUSES}")
    backend = _github_backend_for_project(_project_from_ident(ident))
    if backend is not None:
        item = backend.update_status(
            ident,
            status,
            session_id=session_id,
            session_uuid=session_uuid,
            resolution=resolution,
            reason=reason,
            expect_owner=expect_owner,
        )
        if item:
            verbs = {"open": "REOPEN", "in_progress": "CLAIM", "closed": "CLOSE"}
            verb = verbs.get(status, status.upper())
            summary = ""
            if status == "closed":
                res = item.get("resolution") or {}
                if isinstance(res, dict):
                    summary = res.get("summary", "")
                elif isinstance(res, str):
                    summary = res
            detail = f"{item.get('ref', '?')} — {summary or item.get('title') or item.get('note', '')[:60]}"
            if not quiet:
                _log(verb, detail, queue=item.get('project', ''))
        return item
    real_sid = _coerce_session_uuid(session_uuid) or _coerce_session_uuid(session_id)
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        for it in data["items"]:
            if _matches(it, ident):
                if require_status is not None and it.get("status") != require_status:
                    return None
                # Ownership guard for close (see expect_owner docstring). Runs
                # inside the lock against the fresh item, so it's race-free with
                # a concurrent close by the fresh worker that re-drained the
                # ticket after a reap. A live claim is also bound to its worker:
                # a different worker cannot replace its resolution. The
                # reap-duplicate always lands in the closed case anyway, because
                # by the time the reaped session resumes from checkpoint the
                # fresh worker has already closed the re-drained ticket.
                if status == "closed" and expect_owner and it.get("status") == "closed":
                    ref_label = it.get("ref", ident)
                    closer = str(it.get("closed_by") or it.get("claimed_by") or "?")
                    when = it.get("closed_at") or "?"
                    raise ValueError(
                        f"{ref_label} is already closed (by {closer} at {when}). "
                        f"You are {expect_owner} — you were likely reaped mid-ticket "
                        f"and it was re-drained by another worker. Your work may "
                        f"duplicate theirs: do NOT re-commit; run `wt find {ref_label} "
                        f"--json` to compare. Pass --force to close anyway."
                    )
                if (
                    status == "closed"
                    and expect_owner
                    and it.get("status") == "in_progress"
                    and it.get("claimed_by")
                    and str(it.get("claimed_by")) != expect_owner
                ):
                    raise ValueError(
                        f"{it.get('ref', ident)} is claimed by {it.get('claimed_by')}; "
                        f"you are {expect_owner}. Only the claiming worker may close "
                        "an in-progress ticket. Pass --force to override deliberately."
                    )
                it["status"] = status
                now = _now_iso()
                it["updated_at"] = now
                if status == "in_progress" and session_id:
                    it["claimed_by"] = str(session_id)
                    it["claimed_at"] = now
                    if real_sid:
                        it["claimed_session_id"] = real_sid
                    _append_history(it, "claim", by=_by("worker", str(session_id), str(real_sid or "")), at=now)
                if status == "closed":
                    it["closed_at"] = now
                    it["needs_input"] = False  # a closed ticket isn't waiting
                    # Attribute the close so a worker that closed by ref
                    # (without a prior claim) still gets credited.
                    if session_id:
                        it["closed_by"] = str(session_id)
                        # Backfill claimed_by on a never-claimed ticket so
                        # consumers that attribute by claimant (wt find, the
                        # dashboard's in-progress column) don't show a blank.
                        # Never overwrites a real claimant (WT-81).
                        if not it.get("claimed_by"):
                            it["claimed_by"] = str(session_id)
                            if real_sid and not it.get("claimed_session_id"):
                                it["claimed_session_id"] = real_sid
                    elif it.get("claimed_by"):
                        it["closed_by"] = it["claimed_by"]
                    # Record HOW it was fixed — the trust-layer signal. Optional:
                    # absent resolution leaves the item without the key.
                    norm = _normalize_resolution(resolution)
                    if norm is not None:
                        it["resolution"] = norm
                    _append_history(
                        it,
                        "close",
                        by=_by("worker", str(session_id or it.get("closed_by") or ""), str(real_sid or "")),
                        at=now,
                        resolution=norm,
                    )
                if status == "open":
                    it["claimed_by"] = None
                    it["claimed_at"] = None
                    it["closed_at"] = None
                    # Keep claimed_session_id: reopening drops the claim *lock*
                    # (so a new worker can claim) but preserves the handle to the
                    # session that last worked this ticket, so `wt discuss` can
                    # still resume that context for a follow-up. A re-claim with a
                    # real session id overwrites it in claim_next.
                    # reopening drops any block — it's back in the pool
                    it["needs_input"] = False
                    it["block_question"] = ""
                    it["blocked_at"] = None
                    _append_history(it, "reopen", by=_by("worker", str(session_id or ""), str(real_sid or "")), at=now, reason=_clip(reason, 4000))
                _save_unlocked(data)
                verbs = {"open": "REOPEN", "in_progress": "CLAIM", "closed": "CLOSE"}
                verb = verbs.get(status, status.upper())
                summary = ""
                if status == "closed":
                    res = it.get("resolution") or {}
                    if isinstance(res, dict):
                        summary = res.get("summary", "")
                    elif isinstance(res, str):
                        summary = res
                detail = f"{it.get('ref', '?')} — {summary or it.get('title') or it.get('note', '')[:60]}"
                # ``quiet`` suppresses this primitive transition line when the
                # caller emits its own higher-level log for the same event (e.g.
                # the orphan sweep logs REQUEUE and owns the single line — see
                # requeue_orphaned_tickets), avoiding a duplicate REOPEN.
                if not quiet:
                    _log(verb, detail, queue=it.get('project', ''))
                return it
    return None


def close(
    ident: Any, session_id: str = "", resolution: Any = None, force: bool = False
) -> Optional[Dict[str, Any]]:
    """Close a ticket, optionally recording HOW it was fixed.

    ``resolution`` may be a bare summary string or a dict with any of
    ``summary`` / ``caveats`` / ``follow_ups`` / ``unresolved``. Absent ->
    closes with no resolution (back-compatible).

    When ``session_id`` identifies a worker (and ``force`` is not set), the
    close is ownership-checked: a ticket already closed, or a still-open claim
    owned by a different worker, raises ``ValueError``. This blocks reap-induced
    duplicate closes and cross-worker resolution theft (see ``update_status``'s
    ``expect_owner``). Callers that close by ref without asserting ownership
    (e.g. dedup-close) pass no ``session_id`` and are unaffected. ``force=True``
    bypasses the guard for a human deliberately force-closing someone's ticket."""
    return update_status(
        ident, "closed", session_id, resolution=resolution,
        expect_owner="" if force else str(session_id or ""),
    )


def release(ident: Any, session_id: str = "", force: bool = False) -> Optional[Dict[str, Any]]:
    """Give up a claim without closing it, e.g. a ticket claimed defensively
    (to stop other workers grabbing it mid-investigation) that turns out
    better left for the normal worker pool to pick up and fix.

    Refuses a ticket that is currently ``needs_input`` unless ``force=True``:
    the reopen path this calls clears ``needs_input``/``block_question`` (see
    ``update_status``), so releasing a blocked ticket silently erases the
    open question. A fresh worker then re-claims it with no memory of what
    was asked, re-investigates from scratch, and re-blocks with the same
    question -- an endless investigate/block/release/reclaim loop that burns
    a full worker session every cycle for zero progress (observed on
    BYM-GH-FINIE-571 and 9 sibling tickets, each recycled 3 times over 6
    hours). ``requeue_orphaned_tickets`` already carries the equivalent
    guard for its own reopen path; this brings the plain release path in
    line with it. ``wt answer`` is the intentional unblock: it clears
    ``needs_input`` together with the human's answer, not just the claim.

    ``require_status="in_progress"`` is a compare-and-swap guard so a stale
    ref that's already closed or reopened by someone else is left alone
    rather than clobbered (WT-86, same pattern as the OPS-72 orphan-reopen
    guard)."""
    if not force:
        current = get(ident)
        if current is not None and current.get("needs_input"):
            raise ValueError(
                f"{current.get('ref', ident)} is blocked awaiting human input: "
                f"{current.get('block_question') or '(no question recorded)'} "
                f"-- releasing would erase that question and hand it to a "
                f"fresh worker with no memory of it. Use `wt answer "
                f"{current.get('ref', ident)} \"...\"` to resolve it, or pass "
                f"force=True (--force on the CLI) if the block is stale."
            )
    return update_status(ident, "open", session_id, require_status="in_progress",
                          reason="released")


def block(
    ident: Any,
    session_id: str = "",
    question: str = "",
    progress: str = "",
) -> Optional[Dict[str, Any]]:
    """Park a ticket that needs a human decision.

    The ticket STAYS ``in_progress`` bound to its session (so ``claim_next``,
    which only picks ``open``, can never hand it to another worker) and is flagged
    ``needs_input`` with the worker's specific ``question``. The worker process
    may then exit to save tokens — continuity is not lost, because the Claude
    session is resumable by id (``claimed_session_id``).

    ``progress`` is an optional analysis-so-far note, stored append-only as a
    backstop so a fresh worker could resume from notes if the session is ever
    truly gone. Resume-first, notes-as-fallback.
    """
    backend = _github_backend_for_project(_project_from_ident(ident))
    if backend is not None:
        return backend.block(
            ident, session_id=session_id, question=question, progress=progress,
        )
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        for it in data["items"]:
            if _matches(it, ident):
                now = _now_iso()
                it["needs_input"] = True
                it["block_question"] = _clip(question, 4000)
                it["blocked_at"] = now
                it["updated_at"] = now
                if it.get("status") == "open":
                    it["status"] = "in_progress"
                if session_id:
                    it["claimed_by"] = it.get("claimed_by") or str(session_id)
                    real = _coerce_session_uuid(session_id)
                    if real and not it.get("claimed_session_id"):
                        it["claimed_session_id"] = real
                actor = _by("worker", str(session_id), str(_coerce_session_uuid(session_id) or ""))
                if progress:
                    _append_history(it, "progress", by=actor, at=now, text=_clip(progress, 24000))
                _append_history(it, "block", by=actor, at=now, question=_clip(question, 4000))
                _save_unlocked(data)
                if progress:
                    _log(
                        "PROGRESS",
                        f"{it.get('ref', '?')} — {_clip(progress, 240)}",
                        queue=it.get("project", ""),
                    )
                _log(
                    "BLOCK",
                    f"{it.get('ref', '?')} — {_clip(question, 240)}",
                    queue=it.get("project", ""),
                )
                return it
    return None


def answer(ident: Any, text: str, session_id: str = "") -> Optional[Dict[str, Any]]:
    """Record a human answer on a blocked ticket and clear ``needs_input`` so the
    resumed session can continue. Answers are append-only, preserving a
    back-and-forth. A ticket with a resumable worker stays ``in_progress``;
    without one it reopens so the worker pool can claim it instead of leaving
    the answer stranded behind an unreclaimable claim."""
    backend = _github_backend_for_project(_project_from_ident(ident))
    if backend is not None:
        item = backend.answer(ident, text, session_id=session_id)
        if item:
            _log(
                "ANSWER",
                f"{item.get('ref', ident)} — {_clip(text, 240)}",
                queue=item.get("project", ""),
            )
        return item
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        for it in data["items"]:
            if _matches(it, ident):
                now = _now_iso()
                it["needs_input"] = False
                it["answered_at"] = now
                it["updated_at"] = now
                _append_history(
                    it,
                    "answer",
                    by=_by("human", str(session_id or "")),
                    at=now,
                    text=_clip(text, 24000),
                )
                if it.get("status") == "in_progress" and not it.get("claimed_session_id"):
                    it["status"] = "open"
                    it["claimed_by"] = None
                    it["claimed_at"] = None
                    it["block_question"] = ""
                    it["blocked_at"] = None
                    _append_history(
                        it,
                        "reopen",
                        by=_by("human", str(session_id or "")),
                        at=now,
                        reason="answered_without_resumable_session",
                    )
                _save_unlocked(data)
                _log(
                    "ANSWER",
                    f"{it.get('ref', '?')} — {_clip(text, 240)}",
                    queue=it.get("project", ""),
                )
                return it
    return None


def comment(ident: Any, text: str, by: str = "human", session_id: str = "") -> Optional[Dict[str, Any]]:
    """Append a plain ticket activity comment without changing ticket state."""
    backend = _github_backend_for_project(_project_from_ident(ident))
    if backend is not None:
        item = backend.comment(ident, text, by=by, session_id=session_id)
        if item:
            _log(
                "COMMENT",
                f"{item.get('ref', ident)} — {_clip(text, 240)}",
                queue=item.get("project", ""),
            )
        return item
    actor_kind = by if by in ("worker", "human", "system") else "human"
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        for it in data["items"]:
            if _matches(it, ident):
                now = _now_iso()
                it["updated_at"] = now
                _append_history(
                    it,
                    "comment",
                    by=_by(actor_kind, str(session_id or "")),
                    at=now,
                    text=_clip(text, 24000),
                )
                _save_unlocked(data)
                _log(
                    "COMMENT",
                    f"{it.get('ref', '?')} — {_clip(text, 240)}",
                    queue=it.get("project", ""),
                )
                return it
    return None


def list_blocked(project: Optional[str] = None) -> List[Dict[str, Any]]:
    """Tickets parked for a human (``needs_input`` truthy), optionally scoped to
    one queue. The CLI's ``wt blocked`` and CCC both read this.

    Routed through ``list_items`` (not a direct ``_load_unlocked`` scan) so a
    github-backed queue is covered too. This used to read the file store
    only, which silently returned nothing for every github-backed queue --
    every worker holding a blocked ticket there read as fully productive to
    the reconciler's staffing math (see ``list_active_claims``), so a queue
    stuck entirely behind blocked tickets never got replacement workers."""
    return [it for it in list_items(project=project) if it.get("needs_input")]


def list_active_claims(project: Optional[str] = None) -> List[Dict[str, Any]]:
    """In-progress tickets a worker is actively holding -- ``in_progress`` but
    NOT parked on ``needs_input`` (a blocked ticket stays ``in_progress``, so
    it's excluded here; use ``list_blocked`` for those). Same scope as
    ``list_blocked`` (see its docstring for why this goes through
    ``list_items`` instead of the file store directly). Lets the reconciler
    tell "worker has nothing else to do" apart from "worker blocked one
    ticket but is actively working another"."""
    return [
        it for it in list_items(status="in_progress", project=project)
        if not it.get("needs_input")
    ]


def next_item(
    session_id: str,
    close_ident: Any = None,
    lane: Optional[str] = None,
    project: Optional[str] = None,
    session_uuid: str = "",
) -> Dict[str, Any]:
    """Self-feeding loop step: optionally close the finished item, then claim
    the next open one *for the same queue*. Returns
    ``{"closed": <item|None>, "next": <item|None>}``.
    """
    closed = None
    if close_ident is not None:
        closed = close(close_ident, session_id)
    if project is None and closed:
        project = closed.get("project")
    nxt = claim_next(session_id, lane=lane, project=project, session_uuid=session_uuid)
    return {"closed": closed, "next": nxt}


def last_progress_iso(project: Optional[str] = None) -> Optional[str]:
    """Most recent ``closed_at`` across items (optionally scoped to a queue).

    This is the ground-truth "did a worker make progress" signal that drives
    the stuck-queue health check — no dependency on any external liveness."""
    backend = _github_backend_for_project(project)
    if backend is not None:
        return backend.last_progress_iso()
    proj = _norm_project(project) if project else None
    latest: Optional[str] = None
    for it in list_items(project=project):
        if proj and it.get("project") != proj:
            continue
        ca = it.get("closed_at")
        if ca and (latest is None or ca > latest):
            latest = ca
    return latest

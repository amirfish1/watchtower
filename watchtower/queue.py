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
  2. ``$WATCHTOWER_DATA_DIR/queues.json``, or
     ``$XDG_DATA_HOME/watchtower/queues.json`` (default:
     ``~/.local/share/watchtower/queues.json``).

Existing CCC and ~/.watchtower stores are imported automatically on first
access, keeping the original files. Queue data survives removal of either
application and its settings directory.

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
      "submitter": "",                    # addressable filer target (optional,
                                          # see enqueue's submitter param) --
                                          # notified on claim/close/needs-input
      "claimed_by": null, "claimed_at": null, "closed_at": null,
      "claimed_session_id": null,        # real worker/session id, when known
      "claimed_machine": null,           # ~3-letter machine tag (machine_tag()),
                                         # same for "closed_machine"
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
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:  # stdlib, but missing on Pythons built without libsqlite3-dev present
    # at build time (a common pyenv/asdf-on-fresh-Linux gotcha) -- fail with
    # an actionable message instead of a bare "No module named 'sqlite3'"
    # traceback, since this is the store's only backend (no JSON fallback).
    import sqlite3
except ImportError as _sqlite_err:  # pragma: no cover - unusual Python builds
    raise ImportError(
        "watchtower requires a Python built with sqlite3 support (stdlib "
        "module 'sqlite3' is missing). This is usually a Python built from "
        "source without libsqlite3-dev/sqlite-devel installed first (common "
        "with pyenv/asdf) -- install the sqlite dev headers and rebuild "
        "Python, or use a distribution that ships sqlite3 (python.org "
        "installer, Homebrew, or your OS package manager's python3)."
    ) from _sqlite_err

try:  # POSIX cross-process locking; degrade gracefully if unavailable.
    import fcntl  # type: ignore
except Exception:  # pragma: no cover - non-POSIX
    fcntl = None  # type: ignore

VALID_STATUSES = ("open", "in_progress", "in_review", "awaiting_answer", "closed")
# WT-28: a worker-blocked ticket on the local store leaves the worker's claim and waits
# as ``awaiting_answer`` with a ``parked`` record (who to route the answer to).
PARKED_STATUS = "awaiting_answer"
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
VALID_READINESS = ("ready", "needs-shaping", "needs-spec", "needs-rationale", "")
# Readiness values claim_next() excludes by default (a worker gets these only
# by passing shaping=True or an explicit readiness_filters whitelist). Single
# source of truth shared by claim_next/peek_next/count_claimable below, the
# GitHub backend's mirror, and health.queue_status's claimable_depth -- so
# "is this ticket claimable" can never drift between what a worker would
# actually claim and what the reconciler thinks is spawn-worthy (see WT's
# SPAWN/REAP churn bug: needs-spec tickets counted as claimable depth even
# though claim_next would never hand them to a default worker).
# needs-rationale is the product-gate icebox (a human Nacked; revival needs a new rationale — see the 2026-09-01 design).
UNCLAIMABLE_READINESS = ("needs-shaping", "needs-spec", "needs-rationale")
VALID_PRIORITIES = ("p0", "p1", "p2", "p3", "p4", "")

# --- WT-31 state vocabularies --------------------------------------------------
# The dimensions of the liveness state table (watchtower/liveness.py). A value
# added here without a table row (or an UNREACHABLE entry) and a golden fails
# tests/test_liveness_table.py; see docs/worker-lifecycle.md.
BACKENDS = ("file", "github")
PLAN_STATUSES = ("", "planning", "reviewing", "discussing", "accepted", "failed", "blocked")
DISC_STATUSES = ("none", "active", "agreed", "escalated")
DISC_AWAITING = ("none", "planner", "reviewer")
GATE_KINDS = ("none", "verify", "review")
ASSESSMENT_STATUSES = ("none", "due", "running", "filing", "done", "failed")
BLOCK_KINDS = ("input", "rationale", "awaiting-client")
DEP_VERDICTS = ("ok", "waiting", "stuck")
# ``pending_answer.state`` (WT-28). ``none`` = no record; ``affinity_expired``
# is projection-only (``affinity`` past its reservation), never stored.
ANSWER_STATES = ("none", "routing", "delivering", "queued", "delivered", "affinity",
                 "affinity_expired", "handed_off")
ANSWER_SETTLED = ("none", "delivered", "handed_off")
ANSWER_INFLIGHT = ("routing", "delivering", "queued", "affinity", "affinity_expired")
_PA_STORED = ("routing", "delivering", "queued", "delivered", "affinity", "handed_off")
VALID_VALUES = ("H", "M", "L", "")
VALID_CONFIDENCES = ("H", "M", "L", "")
# FEAT-NEXT-120 — a filer's best-guess minimum model this ticket needs. Not a
# blocker at filing time (empty is fine, filer never waits for certainty);
# checked against the claiming queue's configured model at claim time (see
# config.model_floor_met). Canonical model ids, not aliases; validity is
# config.is_valid_model_floor (any model a catalog knows), never a fixed list.

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
            # Verbs of 9+ chars (SPAWN_PLAN, IDLE_DECISION) fill the column, so
            # force one space or they glue onto the first detail field.
            sep = " " if len(verb) >= 9 else ""
            lines.append(f"{now}  {q_col:<14}  {verb:<9}{sep}{detail}\n")
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
    from . import storage
    base = storage.data_dir() / "queues.json"
    db = base.with_suffix(".db")
    if db.exists() or base.exists():
        return base
    with storage.migration_lock(base.parent):
        if not db.exists():
            # Match the previous resolver's precedence. The unused legacy
            # store is left intact, as are the files we import.
            source = next((p for p in (_CCC_LEGACY_STORE, _WT_DEFAULT_STORE)
                           if p.exists() or p.with_suffix(".db").exists()), None)
            if source == base:
                return base
            if source is not None:
                with storage.file_lock(source.with_suffix(".lock")):
                    source_db = source.with_suffix(".db")
                    if source_db.exists():
                        # SQLite backup includes committed WAL pages. A raw
                        # copy of the main file can silently lose tickets.
                        with storage.atomic_destination(db) as temporary:
                            original = sqlite3.connect(source_db.as_uri() + "?mode=ro", uri=True)
                            copied = sqlite3.connect(temporary)
                            try:
                                original.backup(copied)
                                if copied.execute("PRAGMA quick_check").fetchone() != ("ok",):
                                    raise ValueError(f"invalid queue database: {source_db}")
                            finally:
                                copied.close()
                                original.close()
                            snapshot = _read_db(temporary, strict=True)
                            _normalize_items(snapshot["items"])
                    else:
                        data = json.loads(source.read_text())
                        if not isinstance(data, dict) or not isinstance(data.get("items", []), list):
                            raise ValueError(f"invalid queue store: {source}")
                        _normalize_items(data.get("items", []))
                        with storage.atomic_destination(db) as temporary:
                            _create_db(temporary, data)
    return base


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


def _git_config_path(repo_path: str) -> str:
    """Path of the shared git config for the checkout at ``repo_path``, or ''.

    Handles plain clones (``.git/`` dir) and linked worktrees (``.git`` file
    pointing at ``<common>/worktrees/<name>``, whose ``commondir`` leads back
    to the config that holds the remotes)."""
    dot_git = os.path.join(repo_path, ".git")
    try:
        if os.path.isdir(dot_git):
            git_dir = dot_git
        elif os.path.isfile(dot_git):
            with open(dot_git, encoding="utf-8") as fh:
                line = fh.read().strip()
            if not line.startswith("gitdir:"):
                return ""
            git_dir = line[len("gitdir:"):].strip()
            if not os.path.isabs(git_dir):
                git_dir = os.path.join(repo_path, git_dir)
            commondir = os.path.join(git_dir, "commondir")
            if os.path.isfile(commondir):
                with open(commondir, encoding="utf-8") as fh:
                    common = fh.read().strip()
                git_dir = common if os.path.isabs(common) else os.path.join(git_dir, common)
        else:
            return ""
    except OSError:
        return ""
    cfg = os.path.join(git_dir, "config")
    return cfg if os.path.isfile(cfg) else ""


def _normalize_remote_url(url: str) -> str:
    """``git@github.com:o/r.git`` and ``https://u@github.com/o/r`` -> ``github.com/o/r``."""
    u = str(url or "").strip()
    if not u:
        return ""
    m = _re.match(r"^[a-z][a-z0-9+.-]*://(?:[^@/]*@)?([^/:]+)(?::\d+)?/(.*)$", u, _re.I)
    if m:
        host, path = m.group(1), m.group(2)
    else:
        m = _re.match(r"^(?:[^@/]+@)?([^/:]+):(.*)$", u)  # scp-like ssh
        if not m:
            return ""  # local path or unrecognised: no stable identity
        host, path = m.group(1), m.group(2)
    path = path.strip("/")
    if path.endswith(".git"):
        path = path[:-4]
    return f"{host}/{path}".lower() if path else ""


# config path -> (mtime_ns, identity). Keyed on mtime so a remote added or
# changed after first lookup is picked up without restarting a long-lived daemon.
_REPO_IDENTITY_CACHE: Dict[str, Any] = {}


def _repo_identity(repo_path: str) -> str:
    """Normalised ``origin`` URL of the git checkout at ``repo_path``, else ''."""
    cfg = _git_config_path(repo_path)
    if not cfg:
        return ""
    try:
        mtime = os.stat(cfg).st_mtime_ns
    except OSError:
        return ""
    hit = _REPO_IDENTITY_CACHE.get(cfg)
    if hit and hit[0] == mtime:
        return hit[1]
    ident = ""
    try:
        section = ""
        with open(cfg, encoding="utf-8", errors="replace") as fh:
            for raw in fh:
                line = raw.strip()
                if line.startswith("["):
                    section = line
                    continue
                if section.replace(" ", "") == '[remote"origin"]' and line.startswith("url"):
                    key, _, val = line.partition("=")
                    if key.strip() == "url":
                        ident = _normalize_remote_url(val)
                        break
    except OSError:
        return ""
    _REPO_IDENTITY_CACHE[cfg] = (mtime, ident)
    return ident


def _queue_for_repo_path(repo_path: str) -> str:
    """Return the configured queue whose repo_path matches, else ''.

    Configured queues use short codes (CCC, BYM) that rarely equal the repo
    basename (claude-command-center, BYM+Finie). A client that files by
    repo_path alone must still land in the right queue, so we check the config
    for an exact repo_path match before falling back to the basename.

    A second checkout of the same repository (an app's installed copy such as
    ``~/.ccc/claude-command-center``, a worktree, a symlinked path) is the same
    project, so it matches too: first by resolved path, then by git ``origin``.
    Without this, those filings fell back to the folder name and landed in an
    auto-created queue that has no workers."""
    if not repo_path:
        return ""
    target = str(repo_path).rstrip("/")
    try:
        from . import config
        configured = []
        for name, conf in (config.all_queues(include_archived=True) or {}).items():
            cfg_rp = str((conf or {}).get("repo_path") or "").rstrip("/")
            if not cfg_rp:
                continue
            if cfg_rp == target:
                return _norm_project(name)
            configured.append((name, cfg_rp))
        if not configured:
            return ""
        real_target = os.path.realpath(target)
        for name, cfg_rp in configured:
            if os.path.realpath(cfg_rp) == real_target:
                return _norm_project(name)
        ident = _repo_identity(target)
        if ident:
            for name, cfg_rp in configured:
                if _repo_identity(cfg_rp) == ident:
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
            for name in config.all_queues(include_archived=True)
            if config.backend(name) == "github"
        ]
    except Exception:
        return []


def _normalize_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Ensure every item has project/seq/ref. seq is assigned once (next free
    number in the item's queue) and then persisted, so refs stay stable when
    other tickets move in/out of a queue. An item keeps its stored seq only if
    its stored ref still belongs to its current project and the seq is unique
    there; otherwise (new item, moved item, collision) it gets max+1.
    """
    ordered = sorted(items, key=lambda x: int(x.get("number", 0)))
    used: Dict[str, set] = {}
    pending: List[Dict[str, Any]] = []
    for it in ordered:
        proj = it.get("project") or _project_for(
            it.get("source", ""), it.get("repo_path", ""), ""
        )
        it["project"] = proj
        try:
            seq = int(it.get("seq") or 0)
        except (TypeError, ValueError):
            seq = 0
        taken = used.setdefault(proj, set())
        if seq > 0 and it.get("ref", "") == f"{proj}-{seq}" and seq not in taken:
            taken.add(seq)
        else:
            pending.append(it)
    for it in pending:
        taken = used.setdefault(it["project"], set())
        seq = max(taken, default=0) + 1
        taken.add(seq)
        it["seq"] = seq
        it["ref"] = f"{it['project']}-{seq}"
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


def _norm_model_floor(value: Any) -> str:
    """A known model id, else "" (an unknown floor is dropped, never fatal)."""
    from . import config
    s = str(value or "").strip()
    return s if config.is_valid_model_floor(s) else ""


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


_MACHINE_TAG_CACHE: Optional[str] = None


def machine_tag() -> str:
    """~3-letter tag identifying the machine a worker event was recorded on.

    The queue store is shared, and its tickets are rendered on machines other
    than the one that wrote the event, so the tag is stamped at write time —
    deriving it at render time would show the *viewer's* host instead.
    ``WATCHTOWER_MACHINE`` overrides the hostname-derived default for machines
    with opaque hostnames (cloud VMs, containers); otherwise it's the first
    three alnum chars of the hostname's first label (``hermes`` -> ``her``).
    """
    global _MACHINE_TAG_CACHE
    if _MACHINE_TAG_CACHE is not None:
        return _MACHINE_TAG_CACHE
    raw = str(os.environ.get("WATCHTOWER_MACHINE") or "").strip()
    if not raw:
        import socket
        raw = socket.gethostname().split(".", 1)[0]
    tag = _re.sub(r"[^a-z0-9]", "", raw.lower())[:3]
    _MACHINE_TAG_CACHE = tag
    return tag


def with_machine(worker: Any, machine: Any) -> str:
    """``<machine>-<worker>`` for display — e.g. ``her-bym-a1b2``.

    The tag is a separate recorded field (``by.machine`` / ``claimed_machine``)
    so claim/ownership comparisons keep matching the bare worker id; a worker
    id that already carries the tag isn't double-prefixed."""
    w = str(worker or "")
    m = str(machine or "")
    if m and w and not w.startswith(m + "-"):
        return f"{m}-{w}"
    return w


def _by(kind: str = "system", worker: str = "", session_id: str = "", machine: str = "") -> Dict[str, str]:
    kind = kind if kind in ("worker", "human", "system") else "system"
    out = {"kind": kind}
    if worker:
        out["worker"] = str(worker)
    if session_id:
        out["session_id"] = str(session_id)
    if machine:
        out["machine"] = str(machine)
    return out


def _normalize_by(value: Any = None, *, worker: Any = "", session_id: Any = "", machine: Any = "", event: str = "") -> Dict[str, str]:
    if isinstance(value, dict):
        kind = str(value.get("kind") or "").strip()
        out = _by(kind if kind else "system")
        w = value.get("worker") or worker
        sid = value.get("session_id") or session_id
        m = value.get("machine") or machine
        if w:
            out["worker"] = str(w)
        if sid:
            out["session_id"] = str(sid)
        if m:
            out["machine"] = str(m)
        return out
    if isinstance(value, str) and value in ("worker", "human", "system"):
        return _by(value, str(worker or ""), str(session_id or ""), str(machine or ""))
    if worker or session_id:
        kind = "human" if event in ("answer", "comment") else "worker"
        return _by(kind, str(worker or ""), str(session_id or ""), str(machine or ""))
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
    entry_by = entry["by"]
    if (
        entry_by.get("kind") == "worker"
        and entry_by.get("worker")
        and "machine" not in entry_by
    ):
        tag = machine_tag()
        if tag:
            entry_by["machine"] = tag
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
            machine=raw.get("machine") or "",
            event=event,
        ),
    }
    for key, value in raw.items():
        if key in ("event", "at", "by", "worker", "session_id", "machine"):
            continue
        if value is not None and value != "":
            out[key] = value
    return out


_EVENT_PRECEDENCE = {
    "filed": 0, "claim": 1, "block": 2, "progress": 2,
    "answer": 3, "gate_ack": 3, "gate_nack": 3, "comment": 4, "close": 5, "reopen": 6,
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
    # The filer is a ticket-level field (``submitter``, or ``github_author``
    # for GitHub-synced issues). Fold it onto the filed event so consumers
    # rendering the stream (``wt find``, CCC's ticket detail) can show who
    # opened the ticket without a second lookup -- older tickets and the
    # GitHub backend recorded the filed event before this existed.
    filer = str(item.get("submitter") or item.get("github_author") or "")
    if filer:
        for e in result:
            if e.get("event") == "filed" and not e.get("submitter"):
                e["submitter"] = filer
    for e in result:
        e.pop("_synthesized", None)
        e.pop("_idx", None)
    return result


# Verb shown in a push-back notification for each state transition. Kept
# separate from the internal event names (see ``_append_history``'s "claim"/
# "close"/"block") since this text is user-facing, sent verbatim to whoever
# filed the ticket or subscribed to the queue.
_NOTIFY_VERBS = {
    "claimed": "claimed",
    "closed": "closed",
    "needs_input": "needs input",
    "awaits_decision": "awaits product decision",
    "in_review": "awaits review",
}


def _actor_identities(*values: Any) -> set:
    """The set of addressable strings that all mean "the actor who did this".

    A transition's actor is known by up to two names -- a worker id
    (``claimed_by``, ``wt close --worker``) and a harness session UUID
    (``claimed_session_id``) -- and either one may be the string a submitter
    or subscriber was registered under. Collecting both lets
    ``_notify_ticket_event`` recognise its own actor whichever name the
    target list happens to use.
    """
    out: set = set()
    for value in values:
        raw = str(value or "").strip()
        if not raw:
            continue
        out.add(raw)
        coerced = _coerce_session_uuid(raw)
        if coerced:
            out.add(coerced)
    return out


def _submitter_wants(item: Dict[str, Any], event: str, _config: Any) -> bool:
    """Should this ticket's ``submitter`` hear about ``event``? (WATCHTOWER-23)

    Filing a ticket makes the filing session its submitter automatically --
    nobody opted into anything -- so by default it hears only the events that
    hand it something to act on (``config.DEFAULT_NOTIFY_EVENTS``: closed,
    needs_input, awaits_decision). A claim tells the filer nothing it can use
    while costing it a turn, which is the whole point of the setting.

    Two ways to get the full stream back:

    - per queue, ``wt config <q> --notify-events claimed,closed,...``;
    - per ticket, filing it with an explicit ``--submitter`` or ``--pre-ack``.
      Both mean a human/session deliberately attached itself to THIS ticket
      (rather than being recorded as filer by ``_default_report_to``), so it
      is treated as watching and gets everything.

    Subscribers are not filtered here at all -- ``wt subscribe`` is itself the
    explicit opt-in, and ``wt unsubscribe`` is its off switch.
    """
    if item.get("submitter_explicit") or item.get("pre_ack"):
        return True
    try:
        return event in _config.notify_events(str(item.get("project") or ""))
    except Exception:
        return True  # a config hiccup must never silently mute notifications


def _notify_ticket_event(
    item: Optional[Dict[str, Any]], event: str, detail: str = "",
    actor: Any = None,
) -> None:
    """Best-effort push-back to whoever should hear about a ticket's state
    change.

    Targets = {ticket's own ``submitter`` (captured at file time, see
    ``enqueue``'s ``submitter`` param) -- but only for the events that
    submitter wants, see ``_submitter_wants``} UNION {this queue's ``subscribers``
    (``config.subscribers`` -- see ``wt subscribe``/``wt unsubscribe``)},
    MINUS ``actor`` -- the identity that performed this transition.
    Deduplicated so a target that is both gets exactly one send. Delivery
    reuses the exact path ``--report-to`` already uses: ``messages.send``
    (resolve + deliver, parking in the outbox on transient failure) -- this
    is intentionally the *only* place that talks to ``messages``/``config``
    for this purpose -- but with ``notify=True`` (WATCHTOWER-22): a status
    notice is informational and must never be worth a whole model turn, so
    it goes over live transports only (uds -> fifo -> delegate) and never
    over ``_deliver_resume``, which used to spawn a headless
    ``claude -p --resume`` per notice. Unreachable simply means the notice
    waits in the outbox for the target to come back.

    ``actor`` is one or more identity strings (worker id and/or session
    UUID; see ``_actor_identities``). A session that files a ticket is its
    own submitter, and a session that subscribes to a queue it also works
    is its own subscriber -- so without this a worker got "[watchtower]
    Q-1 claimed" for its own claim and "Q-1 closed" for its own close, i.e.
    steered mid-turn by an echo of the command it just ran (same class as
    WATCHTOWER-21's comment echo). Telling somebody what they just did is
    never news. Notification of *other* people's transitions is unchanged.
    Matching is by exact string, so an actor reached only under an unrelated
    ``@agent`` alias is not recognised and still gets the echo.

    Called from ``claim_next``/``claim_by_ref``/``close``/``block`` -- the
    four choke points every state transition already funnels through for
    both the file and the GitHub backend, so no other caller (``cli.py``,
    ``dashboard.py``, the reconciler) needs to know this exists.

    ``event`` is one of ``"claimed"`` / ``"closed"`` / ``"needs_input"``.
    Never raises: a missing/unresolvable target, an import hiccup, or a
    delivery failure must never fail the underlying claim/close/block --
    notification is strictly best-effort, exactly like the rest of this
    module's optional side effects (see ``_log``).
    """
    if not item or not event:
        return
    try:
        from . import messages
        from . import config as _config
    except Exception:
        return
    targets: List[str] = []
    seen: set = set()
    actors = _actor_identities(*(actor if isinstance(actor, (list, tuple, set)) else (actor,)))

    def _add(raw: Any) -> None:
        t = str(raw or "").strip()
        if t and t not in seen and t not in actors:
            seen.add(t)
            targets.append(t)

    project = str(item.get("project") or "")
    if _submitter_wants(item, event, _config):
        _add(item.get("submitter"))
    try:
        for target in _config.subscribers(project):
            _add(target)
    except Exception:
        pass
    if not targets:
        return
    ref = str(item.get("ref") or item.get("number") or "?")
    verb = _NOTIFY_VERBS.get(event, event)
    text = f"[watchtower] {ref} {verb}"
    if detail:
        text += f" — {_clip(detail, 200)}"
    for target in targets:
        # MEMORY-5: a submitter/subscriber that has since been manually
        # rebound or continued to a new session (`ccc rebind-report-to`,
        # `ccc spawn --continue-from`) still has its old sid written down
        # here -- ask CCC (the one source of truth for the forward map)
        # whether it should really go to a successor instead.
        try:
            deliver_target = messages.ccc_forward_target(target) or target
        except Exception:
            deliver_target = target
        try:
            messages.send(deliver_target, text, notify=True)
        except Exception:
            pass  # best-effort -- a delivery hiccup never fails the transition


def _new_item_unlocked(data: Dict[str, Any], *, note: str, text: str, source: str,
                       proj: str, annotation_id: str, url: str, title: str,
                       selector: str, screenshot_path: str, repo_path: str,
                       lane: str, item_type: str, readiness: str, priority: str,
                       value: str, confidence: str, model_floor: str,
                       planner_model: str, verifier_model: str, submitter: str,
                       submitter_explicit: bool, pre_ack: bool,
                       blocked_by: Optional[List[str]], gates: Optional[List[str]],
                       accept_line: str, assessment_origin: str = "") -> Dict[str, Any]:
    """Append a new ``open`` item to an already-loaded local store (the caller
    holds the store lock and saves). Shared by :func:`enqueue` and the
    post-fix-assessment filer so both create identical tickets."""
    blockers = _validate_blocked_by(data["items"], "", list(blocked_by or []))
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
        "model_floor": _norm_model_floor(model_floor),
        "needs_input": False,
        "block_question": "",
        # The two ticket-level eligibility inputs (2026-07-26 design). The
        # GitHub backend stores these as labels; here they are plain
        # booleans, so both backends hand downstream code the same shape.
        "no_auto_drain": False,
        "run_requested": False,
        "pre_ack": bool(pre_ack),
        "blocked_by": blockers,
        **({"planner_model": str(planner_model).strip()} if str(planner_model or "").strip() else {}),
        **({"verifier_model": str(verifier_model).strip()} if str(verifier_model or "").strip() else {}),
        **({"gates": validate_gates(gates)} if gates else {}),
        **({"accept": str(accept_line).strip()} if str(accept_line or "").strip() else {}),
        "submitter": str(submitter or ""),
        "submitter_explicit": bool(submitter_explicit),
        "claimed_by": None,
        "claimed_at": None,
        "closed_at": None,
        "claimed_session_id": None,
        "created_at": now,
        "updated_at": now,
    }
    if assessment_origin:
        item["assessment_origin"] = str(assessment_origin)
    _append_history(item, "filed", by=_by("system"), at=now, source=item["source"], project=proj,
                    submitter=str(submitter or ""))
    data["items"].append(item)
    _normalize_items(data["items"])  # assign this item's seq/ref
    return next(it for it in data["items"] if it.get("number") == number)


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
    planner_model: str = "",
    verifier_model: str = "",
    submitter: str = "",
    submitter_explicit: bool = False,
    pre_ack: bool = False,
    blocked_by: Optional[List[str]] = None,
    gates: Optional[List[str]] = None,
    accept_line: str = "",
    assessment_origin: str = "",
) -> Dict[str, Any]:
    """Append a new ``open`` item and return it (with its assigned ref).

    ``blocked_by``: refs of tickets that must close (completed) before this one
    is claimable (WT-4); validated -- unknown refs raise ValueError.

    ``submitter``: an addressable target (worker id / ``@agent`` name /
    session UUID -- the same shape ``messages.resolve_target`` already
    resolves for ``--report-to``) identifying whoever filed this ticket, so
    ``_notify_ticket_event`` can push claim/close/needs-input status back to
    them. Optional and best-effort: a caller with no addressable identity at
    filing time (a legacy caller, an anonymous annotate-widget POST) leaves it
    "" and the ticket simply gets no submitter notifications -- this never
    blocks filing.

    ``submitter_explicit``: the caller NAMED this submitter (``wt add
    --submitter``) rather than being auto-detected as the filing session, so
    it is watching this ticket deliberately and hears every event
    (``_submitter_wants``), not just the default set."""
    note = _clip(note, 4000)
    if not note and not text:
        raise ValueError("note or text is required")
    lane = lane if lane in VALID_LANES else "normal"
    proj = _project_for(source, repo_path, project)
    # Registration for the reconciler (WT-131, see mark_runnable's docstring)
    # happens lazily at run/drain time, not here -- OPS-563: registering on
    # every enqueue persisted a config row for every ephemeral queue name a
    # caller ever derived, even ones that were never run (observed: a
    # session-tracking queue re-derived per invocation left 4 dead config
    # entries behind). A queue that is never run or drain-enabled leaves no
    # trace in queue-config.json.
    backend = _github_backend_for_project(proj)
    if backend is not None:
        if blocked_by:
            raise ValueError("ticket dependencies (--after) are not supported on GitHub-backed queues")
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
            submitter=submitter,
            submitter_explicit=submitter_explicit,
        )
        _log("ENQUEUE", f"{saved.get('ref', '?')} — {saved.get('title') or saved.get('note', '')[:60]}", queue=saved.get('project', ''))
        return saved
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        saved = _new_item_unlocked(
            data, note=note, text=text, source=source, proj=proj, annotation_id=annotation_id,
            url=url, title=title, selector=selector, screenshot_path=screenshot_path,
            repo_path=repo_path, lane=lane, item_type=item_type, readiness=readiness,
            priority=priority, value=value, confidence=confidence, model_floor=model_floor,
            planner_model=planner_model, verifier_model=verifier_model, submitter=submitter,
            submitter_explicit=submitter_explicit, pre_ack=pre_ack, blocked_by=blocked_by,
            gates=gates, accept_line=accept_line, assessment_origin=assessment_origin)
        _save_unlocked(data)
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

    Registers the queue with config.ensure_entry() first (OPS-563): a queue
    with no config entry is invisible to workers._reconcile_once_locked()
    (it only iterates config.all_queues()), so running its very first ticket
    would otherwise silently no-op forever -- dispatch_after_enqueue() nudges
    no live worker (there's never been one), falls through to
    reconcile_once(), which skips an unregistered queue entirely, not even
    into its own `skipped` list (WT-131). auto_drain stays default-off; this
    only makes the queue exist for the reconciler to see.
    """
    proj = _project_from_ident(ident)
    if proj:
        try:
            from . import config
            config.ensure_entry(proj)
        except Exception:
            pass
    backend = _github_backend_for_project(proj)
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
    repo_path, needs_input, block_question, blocked_by (list of refs; validated
    -- unknown refs and cycles raise ValueError).

    Disallowed (managed by state machine): status, claimed_by, claimed_at,
    closed_at, claimed_session_id, number, project, ref, seq, created_at.

    ``item_type`` and ``"type"`` are aliases — both are stored as ``"type"``.
    Returns the updated item, or None if not found.
    """
    backend = _github_backend_for_project(_project_from_ident(ident))
    if backend is not None:
        return backend.update(ident, **fields)
    if "blocked_by" in fields and backend is not None:
        raise ValueError("ticket dependencies (--after) are not supported on GitHub-backed queues")
    ALLOWED = frozenset({
        "item_type", "type", "readiness", "priority", "value", "confidence",
        "note", "text", "title", "url", "selector", "screenshot_path", "repo_path",
        "needs_input", "block_question", "model_floor", "blocked_by", "gates", "accept",
        "planner_model", "verifier_model",
    })
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        for it in data["items"]:
            if _matches(it, ident):
                now = _now_iso()
                if "gates" in fields:
                    fields = dict(fields)
                    fields["gates"] = validate_gates(fields["gates"])
                if "blocked_by" in fields:
                    fields = dict(fields)
                    fields["blocked_by"] = _validate_blocked_by(
                        data["items"], str(it.get("ref") or ""),
                        list(fields["blocked_by"] or []),
                    )
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
    within the target queue (next free seq there, persisted; other tickets'
    refs do not shift, see _normalize_items) but preserves status/claim state/notes/history. Avoids
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


def _blocker_state(blocker: Dict[str, Any]) -> str:
    """The ONE predicate for "is this blocker satisfied" (WT-4).

    ``satisfied``: closed as completed -- not declined at the product gate,
    and its resolution has no ``unresolved`` items. ``waiting``: not closed
    yet. ``stuck``: closed, but declined or with unresolved items, so the
    dependent must NOT auto-unblock; a human decides. Extend here (e.g. with
    a review/accept state), nowhere else."""
    if blocker.get("status") != "closed":
        return "waiting"
    if blocker.get("product_nack"):
        return "stuck"
    res = blocker.get("resolution")
    if isinstance(res, dict) and any(str(x).strip() for x in (res.get("unresolved") or [])):
        return "stuck"
    return "satisfied"


def blocker_verdict(
    it: Dict[str, Any], by_ref: Dict[str, Dict[str, Any]]
) -> Tuple[str, str]:
    """``(state, blocker_ref)`` for one ticket: ``ok`` (claimable as far as
    dependencies go), ``waiting`` (a blocker is still open) or ``stuck`` (a
    blocker closed declined/unresolved). ``stuck`` wins over ``waiting``. A
    stuck blocker a human already answered for (``blocker_escalated`` and
    ``needs_input`` cleared) counts as satisfied; a blocker that no longer
    exists is ignored."""
    waiting = ""
    for ref in it.get("blocked_by") or []:
        blocker = by_ref.get(str(ref))
        if blocker is None:
            continue
        state = _blocker_state(blocker)
        if state == "stuck":
            if str(ref) in (it.get("blocker_escalated") or []) and not it.get("needs_input"):
                continue
            return "stuck", str(ref)
        if state == "waiting" and not waiting:
            waiting = str(ref)
    return ("waiting", waiting) if waiting else ("ok", "")


def waiting_on(it: Dict[str, Any], by_ref: Dict[str, Dict[str, Any]]) -> List[str]:
    """Blocker refs of ``it`` not yet completed (WT-9), in ``blocked_by`` order.

    Uses ``_blocker_state`` so display and the claim gate share one rule: a
    blocker counts as completed only when closed, not declined, and with no
    unresolved items. A stuck blocker a human already answered for counts as
    completed (same as ``blocker_verdict``); a missing blocker is ignored.
    Empty when unblocked and for tickets that are not open/in-flight."""
    if it.get("status") == "closed":
        return []
    out: List[str] = []
    for ref in it.get("blocked_by") or []:
        blocker = by_ref.get(str(ref))
        if blocker is None:
            continue
        state = _blocker_state(blocker)
        if state == "satisfied":
            continue
        if (
            state == "stuck"
            and str(ref) in (it.get("blocker_escalated") or [])
            and not it.get("needs_input")
        ):
            continue
        out.append(str(ref))
    return out


def _refs_index(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {str(it.get("ref")): it for it in items if it.get("ref")}


def _escalate_stuck_blockers(items: List[Dict[str, Any]]) -> bool:
    """Flag every open ticket whose blocker closed declined/unresolved as
    ``needs_input`` (once per blocker) so it shows under ``wt blocked``
    instead of silently never being claimed. Mutates ``items``; returns
    whether anything changed. Caller holds the lock and saves."""
    by_ref = _refs_index(items)
    changed = False
    for it in items:
        if it.get("status") != "open" or it.get("needs_input") or not it.get("blocked_by"):
            continue
        state, ref = blocker_verdict(it, by_ref)
        if state != "stuck":
            continue
        blocker = by_ref[ref]
        why = "declined" if blocker.get("product_nack") else "closed with unresolved items"
        question = (
            f"Blocker {ref} was {why}. Proceed anyway (wt answer), or change "
            f"the dependency (wt edit --clear-after)?"
        )
        now = _now_iso()
        it["needs_input"] = True
        it["block_question"] = question
        it["block_kind"] = "input"
        it["blocked_at"] = now
        it["updated_at"] = now
        it["blocker_escalated"] = list(it.get("blocker_escalated") or []) + [ref]
        _append_history(it, "block", by=_by("system"), at=now, question=question, kind="input")
        changed = True
    return changed


def _validate_blocked_by(
    items: List[Dict[str, Any]], ref: str, blocked_by: List[str]
) -> List[str]:
    """Normalise ``blocked_by`` for ticket ``ref`` (may be "" for a not-yet-
    filed ticket): every blocker must exist, none may be the ticket itself,
    and no cycle may result. Raises ValueError."""
    by_ref = _refs_index(items)
    out: List[str] = []
    for raw in blocked_by:
        found = next((it for it in items if _matches(it, raw)), None)
        if found is None:
            raise ValueError(f"blocker {raw} does not exist")
        b = str(found.get("ref"))
        if ref and b == ref:
            raise ValueError(f"{ref} cannot be blocked by itself")
        if b not in out:
            out.append(b)
    if ref:
        # A cycle exists iff ref is reachable from any of its new blockers.
        seen = set()
        stack = list(out)
        while stack:
            cur = stack.pop()
            if cur == ref:
                raise ValueError(f"dependency cycle: {ref} would (transitively) block itself")
            if cur in seen:
                continue
            seen.add(cur)
            stack.extend(str(x) for x in (by_ref.get(cur, {}).get("blocked_by") or []))
    return out


def _claim_candidates(
    items: List[Dict[str, Any]],
    *,
    project: Optional[str] = None,
    lane: Optional[str] = None,
    shaping: bool = False,
    oldest: bool = False,
    item_types: Optional[List[str]] = None,
    readiness_filters: Optional[List[str]] = None,
    all_items: Optional[List[Dict[str, Any]]] = None,
    worker_model: Optional[str] = None,
    claimer: Optional[Tuple[str, str]] = None,
) -> List[Dict[str, Any]]:
    """Return ``items`` filtered + sorted exactly as claim_next() would pick
    from them — the single source of truth for "is this ticket claimable
    right now", shared by claim_next, peek_next, and count_claimable. Any
    caller that needs to know what a worker COULD claim (without claiming it)
    goes through here instead of re-implementing the filter, so it can never
    silently drift out of sync (``project`` is expected pre-normalized).

    ``worker_model`` (canonical id, WT-10): when not None, tickets whose
    ``model_floor`` that model does not meet are skipped, so a weaker worker
    just takes the next ticket instead of claiming and parking it. None
    disables the filter.
    """
    candidates = [it for it in items if it.get("status") == "open"]
    # WT-28: a ticket whose answer is reserved for its parked worker
    # (affinity) is claimable only by that worker until the reservation
    # expires; everyone else, and every counting caller, drops it.
    _now_ts = time.time()
    candidates = [it for it in candidates
                  if not _affinity_reserved(it, _now_ts)
                  or (claimer is not None and _affinity_owner(it, claimer[0], claimer[1]))]
    # Plan gate (WT-22): a ticket is planned BEFORE a build worker claims it,
    # so one whose plan has not settled is not claimable yet.
    candidates = [it for it in candidates if not plan_pending(it)]
    if worker_model is not None:
        from . import config
        candidates = [
            it for it in candidates
            if config.model_meets_floor(worker_model, it.get("model_floor") or "")
        ]
    if any(it.get("blocked_by") for it in candidates):
        # Dependencies (WT-4): waiting/stuck tickets are not claimable. Blockers
        # may live in other queues, so resolve against ``all_items`` if given.
        by_ref = _refs_index(all_items if all_items is not None else items)
        candidates = [
            it for it in candidates
            if not it.get("blocked_by") or blocker_verdict(it, by_ref)[0] == "ok"
        ]
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
    _own = ((lambda it: 0 if _affinity_reserved(it, _now_ts) else 1)
            if claimer is not None else (lambda it: 1))
    if oldest:
        candidates = sorted(candidates, key=lambda it: (_own(it), int(it.get("number", 0))))
    else:
        candidates = sorted(
            candidates,
            key=lambda it: (
                _own(it),
                0 if it.get("lane") == "express" else 1,
                _prio_rank(it),
                0 if it.get("sent_back_released") else 1,  # WT-34
                _type_rank(it),
                int(it.get("number", 0)),
            ),
        )
    return candidates


def count_claimable(
    project: Optional[str] = None,
    lane: Optional[str] = None,
    item_types: Optional[List[str]] = None,
    worker_model: Optional[str] = None,
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
        return len(_claim_candidates(
            data["items"], project=proj, lane=lane, item_types=item_types,
            worker_model=worker_model,
        ))


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
            dead = next(
                (w for w in known if str(w.get("worker_id", "")) == session_id),
                {},
            )
            pid_hint = (
                f" (recorded pid {dead['pid']} — verify with `ps -p {dead['pid']}`)"
                if dead.get("pid")
                else ""
            )
            raise ValueError(
                f"worker {session_id!r} is registered as a spawned worker but is "
                f"not currently alive{pid_hint} — claim rejected to prevent a "
                "silent requeue. This almost always means the worker was "
                "released/reaped and its process exited; the reconciler spawns "
                "a fresh worker with a new id when staffing is needed. If you "
                "are a resumed session from that dead worker: do not retry the "
                "claim and do not file a bug about this message — end your turn."
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
    worker_model: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Atomically move the next ``open`` item to ``in_progress`` and return it.

    Scoped to ``project`` when given, so a worker only drains its own queue.
    Default sort: express lane → priority (p0 first) → bugs before features →
    oldest within tier. Pass ``oldest=True`` for pure FIFO regardless of priority.

    ``item_types``: if non-empty, only claim items whose type is in the list.
    ``worker_model``: canonical model id of the claiming worker; tickets with a
      higher ``model_floor`` are skipped (WT-10). None = no floor filtering.
    ``readiness_filters``: if non-empty, only claim items whose readiness is in
      the list (bypasses default exclusion of unready items). If empty/None,
      excludes needs-shaping/needs-spec unless ``shaping=True``.

    Returns ``None`` when nothing matches.

    Raises ``ValueError`` if ``session_id`` still holds an active (non-blocked)
    claim on this queue: a ticket ends with ``close`` or ``block``, never with
    a bare commit, and only the claim boundary can catch a worker that skipped
    that step.

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
    # One ticket at a time. Committing a fix is not the same thing as
    # finishing a ticket: a worker that commits and then claims again leaves
    # the previous ticket silently ``in_progress``, and its own end-of-turn
    # narrative ("committed, so it's done") reads as complete to the human.
    # The claim boundary is the only place that can catch it, so resolve what
    # this worker still holds before anything else decides what to hand it.
    held = worker_active_claims(session_id, project)
    if held:
        # On a GitHub-backed queue the list above can be a stale snapshot:
        # the worker's own `wt close` invalidates only ITS process's caches,
        # while the poller's in-process copy (plus fetch caps / quota guards)
        # can re-persist the pre-close listing seconds later -- the ticket
        # then reads closed in `wt find` but this guard still refuses the
        # next claim on it (OPS-854). Re-read each held ticket directly
        # (a strict single-issue read) and trust that over the snapshot.
        held = _reverify_held_claims(held)
    # WT-34: a sent-back claim is handed back first (decided in the claim lock
    # below), so it never counts as a stale "you still hold X" here.
    sb_pre = [h for h in held if h.get("sent_back")]
    held = [h for h in held if not h.get("sent_back")]
    real_sid = _coerce_session_uuid(session_uuid) or _coerce_session_uuid(session_id)
    if not sb_pre and _github_backend_for_project(project) is None:
        with _FileLock(_lock_path()):
            sb_pre = _sent_back_held_unlocked(
                _load_unlocked()["items"], str(session_id), str(real_sid or ""),
                _norm_project(project) if project else None)

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
        # clean boundary instead of stranding it -- the guard below then
        # tells the worker exactly which ticket to finish first.
        if not held and not sb_pre:
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

    if held:
        _raise_claim_refused(held, session_id, sb_pre[0] if sb_pre else None)

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
            _notify_ticket_event(
                item, "claimed",
                actor=(item.get("claimed_by"), item.get("claimed_session_id")),
            )
        return item

    proj = _norm_project(project) if project else None
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        if _escalate_stuck_blockers(data["items"]):
            _save_unlocked(data)
        # WT-28: one-ticket-at-a-time re-checked inside the lock (the pre-lock
        # read above can be stale when a resume/affinity claim lands between).
        held_now = [h for h in _held_unlocked(data["items"], str(session_id),
                                              str(real_sid or ""), proj)
                    if not h.get("sent_back")]
        # WT-34: a sent-back claim comes first -- hand it back (same lock).
        sb_now = _sent_back_held_unlocked(data["items"], str(session_id),
                                          str(real_sid or ""), proj)
        if held_now:
            _raise_claim_refused(held_now, session_id, sb_now[0] if sb_now else None)
        if sb_now:
            return _handback_unlocked(data, sb_now[0])
        candidates = _claim_candidates(
            data["items"], project=proj, lane=lane, shaping=shaping, oldest=oldest,
            item_types=item_types, readiness_filters=readiness_filters,
            worker_model=worker_model, claimer=(str(session_id), str(real_sid or "")),
        )
        if not candidates:
            return None
        item = candidates[0]
        item["status"] = "in_progress"
        item["claimed_by"] = str(session_id)
        item["claimed_machine"] = machine_tag()
        item.pop("resume", None)  # WT-30: evidence belongs to the previous claim
        _clear_sent_back_unlocked(item)
        if real_sid:
            item["claimed_session_id"] = real_sid
        item["claimed_at"] = _now_iso()
        item["updated_at"] = item["claimed_at"]
        _append_history(item, "claim", by=_by("worker", str(session_id), str(real_sid or "")), at=item["claimed_at"])
        item["claim_proc"] = _claim_proc_for(session_id, real_sid or "")
        _bind_pending_unlocked(item, str(session_id), str(real_sid or ""), item["claimed_at"])
        _save_unlocked(data)
    _log("CLAIM", f"{item.get('ref', '?')} by {session_id[:16]} — {item.get('title') or item.get('note', '')[:60]}", queue=item.get('project', ''))
    _notify_ticket_event(
        item, "claimed",
        actor=(item.get("claimed_by"), item.get("claimed_session_id")),
    )
    return item


def _handback_unlocked(data: Dict[str, Any], it: Dict[str, Any]) -> Dict[str, Any]:
    """Return the held sent-back ticket as this claim (idempotent); the release
    clock restarts once, at the first hand-back."""
    sb = it["sent_back"]
    now = _now_iso()
    if not sb.get("handed_back_at"):
        sb["handed_back_at"] = now
        it["updated_at"] = now
        _append_history(it, "handback", by=_by("system"), at=now,
                        reason="sent-back claim handed back before any new ticket")
        _save_unlocked(data)
    age = _age_min(sb.get("at"))
    _log("CLAIM", f"{it.get('ref', '?')} by {str(it.get('claimed_by') or '')[:16]} "
                  f"(handback: sent back {age}m ago)", queue=it.get("project", ""))
    out = dict(it)
    out["handed_back"] = True
    return out


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
            _notify_ticket_event(
                item, "claimed",
                actor=(item.get("claimed_by"), item.get("claimed_session_id")),
            )
        return item
    real_sid = _coerce_session_uuid(session_uuid) or _coerce_session_uuid(session_id)
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        item = next((it for it in data["items"] if it.get("ref") == ref), None)
        if item is None:
            return None
        status = item.get("status", "open")
        sb_held = _sent_back_held_unlocked(data["items"], str(session_id),
                                           str(real_sid or ""), item.get("project"))
        if sb_held:  # WT-34
            if any(item is h for h in sb_held):
                return _handback_unlocked(data, item)
            _raise_sent_back_refused(sb_held[0], session_id)
        if status != "open":
            raise ValueError(f"{ref} is not open (status={status})")
        reserved = _affinity_gate_unlocked(item, str(session_id), str(real_sid or ""), time.time())
        if reserved:
            raise ValueError(reserved)
        # WT-28: in-lock worker-or-session exclusivity on every claim path.
        held_now = _held_unlocked(data["items"], str(session_id), str(real_sid or ""),
                                  item.get("project"))
        if held_now:
            _raise_claim_refused(held_now, session_id)
        item["status"] = "in_progress"
        item["claimed_by"] = str(session_id)
        item["claimed_machine"] = machine_tag()
        item.pop("resume", None)  # WT-30: evidence belongs to the previous claim
        _clear_sent_back_unlocked(item)
        if real_sid:
            item["claimed_session_id"] = real_sid
        item["claimed_at"] = _now_iso()
        item["updated_at"] = item["claimed_at"]
        _append_history(item, "claim", by=_by("worker", str(session_id), str(real_sid or "")), at=item["claimed_at"])
        item["claim_proc"] = _claim_proc_for(session_id, real_sid or "")
        _bind_pending_unlocked(item, str(session_id), str(real_sid or ""), item["claimed_at"])
        _save_unlocked(data)
    _log("CLAIM", f"{item.get('ref', '?')} by {session_id[:16]} — {item.get('title') or item.get('note', '')[:60]}", queue=item.get('project', ''))
    _notify_ticket_event(
        item, "claimed",
        actor=(item.get("claimed_by"), item.get("claimed_session_id")),
    )
    return item


def peek_next(
    project: Optional[str] = None,
    lane: Optional[str] = None,
    item_types: Optional[List[str]] = None,
    worker_model: Optional[str] = None,
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
        candidates = _claim_candidates(
            data["items"], project=proj, lane=lane, item_types=item_types,
            worker_model=worker_model,
        )
        return dict(candidates[0]) if candidates else None


def floor_routed_candidates(
    project: str,
    queue_model: str,
    item_types: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Claimable open tickets whose ``model_floor`` ``queue_model`` does not
    meet, in claim order (WT-10). The reconciler spawns one floor-model
    run-once worker per entry. File-backed queues only; a GitHub-backed queue
    returns [] (its backend has no floor filter)."""
    if _github_backend_for_project(project) is not None:
        return []
    proj = _norm_project(project)
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        every = _claim_candidates(
            data["items"], project=proj, item_types=item_types,
            all_items=data["items"],
        )
        from . import config
        return [
            dict(it) for it in every
            if not config.model_meets_floor(queue_model, it.get("model_floor") or "")
        ]


def _normalize_resolution(resolution: Any) -> Optional[Dict[str, Any]]:
    """Coerce a resolution into the stored shape, or None when empty.

    Accepts a bare string (treated as the summary) or a dict with any of
    ``summary`` / ``commit`` / ``no_code`` / ``caveats`` / ``follow_ups`` /
    ``unresolved`` / ``session_id`` / ``engine`` / ``transcript_path`` /
    ``machine``. The last four attribute the close to the session and
    machine that did the work, so CCC's ``ccc shipped``/``ccc brief`` can
    resolve "session X on node Y" even for an ephemeral queue-worker
    sandbox whose transcript never lands on the local machine. List fields
    are coerced to lists of clipped strings; empty fields are dropped.
    Returns None when nothing meaningful was supplied (so close stays
    back-compatible)."""
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
    for field, max_len in (
        ("session_id", 128),
        ("engine", 32),
        ("transcript_path", 1024),
        ("machine", 16),
    ):
        val = _clip(resolution.get(field, ""), max_len)
        if val:
            out[field] = val
    for field in ("caveats", "follow_ups", "unresolved"):
        raw = resolution.get(field)
        if raw is None:
            continue
        if isinstance(raw, str):
            raw = [raw]
        vals = [_clip(v, 4000) for v in raw if str(v or "").strip()]
        if vals:
            out[field] = vals
    # Acknowledgements (see ``ack_resolution``) live alongside their list as
    # ``<field>_ack``; carry them through so re-normalizing a stored
    # resolution doesn't silently un-acknowledge every chip a human cleared.
    for field in ("caveats", "follow_ups", "unresolved"):
        acks = resolution.get(f"{field}_ack")
        if isinstance(acks, dict) and acks and out.get(field):
            out[f"{field}_ack"] = {
                str(k): v for k, v in acks.items()
                if str(k).isdigit() and int(k) < len(out[field])
            }
            if not out[f"{field}_ack"]:
                out.pop(f"{field}_ack")
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
    by_kind: str = "worker",
    hold_review: str = "",
    extras: Optional[Dict[str, Any]] = None,
    answer_fate: str = "handoff",
    orphan: Optional[Dict[str, str]] = None,
    close_owner: str = "",
) -> Optional[Dict[str, Any]]:
    """``close_owner`` (WT-34): run the close-ownership guard
    (``_close_owner_guard_unlocked``) in the lock for ANY target status -- the
    failed-gate reopen of a worker-attributed close passes it. ``expect_owner``
    is the same guard, applied only when ``status == "closed"``.

    ``orphan`` (WT-30): ``{"session_id", "claimed_by"}`` of the claim the
    orphan sweep is displacing; recorded on the reopen event so a later resume of
    that session can be told to STOP even after a fresh worker re-claims.

    ``answer_fate`` (WT-28): what a reopen does with a ticket's
    ``pending_answer``: ``handoff`` keeps it as ``handed_off`` for the next
    claimer, ``discard`` (forced release/reopen) drops it to history.

    ``hold_review`` (close only, WT-5): land the ticket in ``in_review``
    instead of ``closed`` -- the same close bookkeeping runs, but it does not
    count as done until ``accept``. ``extras`` are extra ticket fields
    (gate results) written in the same locked step.

    ``require_status``, when set, makes this a compare-and-swap: the
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
    the orphan-ticket sweep passes "worker gone" for a reopen.

    ``by_kind`` selects the actor recorded on the appended history entry;
    worker-driven transitions keep the default "worker", while human triage
    verbs (``reopen``) pass "human" so the timeline attributes the event
    correctly. Ignored by the GitHub backend's history mirroring."""
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
            require_status=require_status,
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
                # ticket after a reap, and with a replacement claim that lands
                # while a failing gate runs (WT-34).
                _close_owner_guard_unlocked(
                    it, ident, close_owner or (expect_owner if status == "closed" else ""),
                    str(real_sid or ""))
                it["status"] = status
                now = _now_iso()
                it["updated_at"] = now
                if status != "in_progress" or session_id:
                    it.pop("resume", None)  # WT-30: claim left/replaced
                    _clear_sent_back_unlocked(it)
                if status == "closed":
                    _clear_parked_unlocked(it)
                    it.pop("sent_back_released", None)
                if status == "in_progress" and session_id:
                    it["claimed_by"] = str(session_id)
                    it["claimed_machine"] = machine_tag()
                    it["claimed_at"] = now
                    if real_sid:
                        it["claimed_session_id"] = real_sid
                    it["claim_proc"] = _claim_proc_for(session_id, real_sid or "")
                    _append_history(it, "claim", by=_by("worker", str(session_id), str(real_sid or "")), at=now)
                if status == "closed" and not hold_review:
                    _drop_claim_proc_unlocked(it)   # hold_review keeps it (WT-30)
                if status == "closed":
                    it["closed_at"] = now
                    it["needs_input"] = False  # a closed ticket isn't waiting
                    # Attribute the close so a worker that closed by ref
                    # (without a prior claim) still gets credited.
                    if session_id:
                        it["closed_by"] = str(session_id)
                        it["closed_machine"] = machine_tag()
                        # Backfill claimed_by on a never-claimed ticket so
                        # consumers that attribute by claimant (wt find, the
                        # dashboard's in-progress column) don't show a blank.
                        # Never overwrites a real claimant (WT-81).
                        if not it.get("claimed_by"):
                            it["claimed_by"] = str(session_id)
                            it["claimed_machine"] = it["closed_machine"]
                            if real_sid and not it.get("claimed_session_id"):
                                it["claimed_session_id"] = real_sid
                    elif it.get("claimed_by"):
                        it["closed_by"] = it["claimed_by"]
                        if it.get("claimed_machine"):
                            it["closed_machine"] = it["claimed_machine"]
                    # Record HOW it was fixed — the trust-layer signal. Optional:
                    # absent resolution leaves the item without the key.
                    norm = _normalize_resolution(resolution)
                    close_machine = it.get("closed_machine") or machine_tag()
                    close_sid = real_sid or it.get("claimed_session_id") or ""
                    if norm is not None:
                        norm.setdefault("machine", close_machine)
                        if close_sid:
                            norm.setdefault("session_id", close_sid)
                        it["resolution"] = norm
                    _append_history(
                        it,
                        "close",
                        by=_by(
                            "worker",
                            str(session_id or it.get("closed_by") or ""),
                            str(real_sid or ""),
                            close_machine,
                        ),
                        at=now,
                        resolution=norm,
                    )
                    if hold_review:
                        it["status"] = "in_review"
                        it["closed_at"] = None
                        it["gate_pending"] = hold_review
                        if hold_review == "verify":
                            # WT-24: the supervision key for the verifier stage;
                            # closed_at is cleared here so it cannot serve.
                            if it.get("verifier"):
                                it["verifier_history"] = (
                                    list(it.get("verifier_history") or [])
                                    + [it["verifier"]])[-10:]
                                it.pop("verifier", None)
                            it["verify_cycle"] = int(it.get("verify_cycle") or 0) + 1
                        _append_history(it, "in_review", by=_by("system"), at=now,
                                        reviewer=hold_review,
                                        **({"verify_cycle": it["verify_cycle"]}
                                           if hold_review == "verify" else {}))
                if extras:
                    it.update(extras)
                if status == "open":
                    it["claimed_by"] = None
                    it["claimed_machine"] = None
                    it["claimed_at"] = None
                    it["closed_at"] = None
                    _drop_claim_proc_unlocked(it)
                    # Back to the pool: the stale close attribution and
                    # resolution must not survive on a claimable ticket
                    # (parity with the GitHub backend's reopen, which pops
                    # closed_by/closed_at/resolution_* from the body meta).
                    it.pop("closed_by", None)
                    it.pop("closed_machine", None)
                    it.pop("resolution", None)
                    it.pop("gate_pending", None)
                    # Keep claimed_session_id: reopening drops the claim *lock*
                    # (so a new worker can claim) but preserves the handle to the
                    # session that last worked this ticket, so `wt discuss` can
                    # still resume that context for a follow-up. A re-claim with a
                    # real session id overwrites it in claim_next.
                    # reopening drops any block — it's back in the pool
                    it["needs_input"] = False
                    it["block_question"] = ""
                    it["block_kind"] = ""
                    it["block_commit"] = ""
                    it["blocked_at"] = None
                    it.pop("parked", None)
                    _reopen_pending_unlocked(it, now, answer_fate, reason)
                    _orphan_fields = ({
                        "orphan": True,
                        "displaced_session_id": str((orphan or {}).get("session_id") or ""),
                        "displaced_claimed_by": str((orphan or {}).get("claimed_by") or ""),
                    } if orphan else {})
                    _append_history(it, "reopen", by=_by(by_kind, str(session_id or ""), str(real_sid or "")), at=now, reason=_clip(reason, 4000), **_orphan_fields)
                _save_unlocked(data)
                if status == "closed" or (status == "open" and answer_fate == "discard"):
                    _supersede_outbox(it.get("ref"))
                verbs = {"open": "REOPEN", "in_progress": "CLAIM", "closed": "CLOSE"}
                verb = "REVIEW" if it.get("status") == "in_review" else verbs.get(status, status.upper())
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


# --- Acceptance gates (WT-5) -------------------------------------------------
# A gate list is ordered; kinds: ``cmd:<command>`` (WT runs it in the ticket's
# repo at the closing commit -- the worker cannot self-report a pass),
# ``review`` / ``review:<target>`` (the submitter, or <target>, must
# ``wt accept`` / ``wt reject``), ``verify`` (an independent verifier session
# checks the ticket's ``accept`` line -- or its text -- and files its verdict
# with ``wt verdict``, never through the worker). cmd gates run first, then
# verify, then review; each later stage fires only once the earlier ones pass.
GATE_OUTPUT_TAIL = 2000
GATE_CMD_TIMEOUT_S = 900


def validate_gates(gates: Any) -> List[str]:
    """Normalise a gate list (or a comma-free list of strings); raises
    ValueError on an unknown kind or an empty ``cmd:``."""
    if isinstance(gates, str):
        gates = [gates]
    out: List[str] = []
    for raw in gates or []:
        g = str(raw or "").strip()
        if not g:
            continue
        if g.startswith("cmd:"):
            if not g[4:].strip():
                raise ValueError("gate 'cmd:' needs a command, e.g. cmd:pytest -q")
            g = "cmd:" + g[4:].strip()
        elif g.startswith("plan:"):
            if not g[5:].strip():
                raise ValueError("gate 'plan:' needs a model, e.g. plan:claude-opus-5-5")
            g = "plan:" + g[5:].strip()
        elif g not in ("review", "verify", "plan") and not (
            g.startswith("review:") and g[7:].strip()
        ):
            raise ValueError(
                f"unknown gate {g!r}: use cmd:<command>, plan, plan:<model>, "
                f"verify, review or review:<target>"
            )
        out.append(g)
    return out


def effective_gates(item: Dict[str, Any]) -> List[str]:
    """Per-ticket ``gates`` override the queue's ``wt config --gate`` list."""
    if item.get("gates") is not None:
        return list(item.get("gates") or [])
    try:
        from . import config as _config
        return list(_config.gates(str(item.get("project") or "")))
    except Exception:
        return []


# --- Plan stage (WT-11) ------------------------------------------------------
# ``plan`` / ``plan:<model>`` gate: BEFORE the build, a planner session (role
# ``planner``, WT-14) writes a plan onto the ticket, a spawned plan reviewer
# (role ``plan_reviewer``, a different family by default) accepts or rejects it,
# and the build worker gets the accepted plan. A rejected plan is revised up to
# PLAN_MAX_REVISIONS times; the ticket blocks only if they still disagree. No
# human step. State lives on ``item["plan"]``:
#   {status: planning|reviewing|discussing|accepted|blocked|failed, round, text,
#    version, accepted_version, discussion: {status, round, objections, ...},
#    reviews: [{round, accepted, reasons, by, at}], planner: {...}, reviewer: {...}}
PLAN_MAX_REVISIONS = 2
# WT-26: after the FIRST rejection the planner and reviewer talk directly
# (status ``discussing``, state in ``plan["discussion"]``); up to this many
# discussion rounds (one per reviewer rejection) before a bounded human block.
PLAN_DISCUSSION_ROUNDS = 3
PLAN_DISCUSSION_MAX_NUDGES = 3


def plan_gate(item: Dict[str, Any]) -> Optional[str]:
    """None when the ticket has no plan gate; else the gate's planner model
    ("" when the gate is a bare ``plan``)."""
    for g in effective_gates(item):
        if g == "plan":
            return ""
        if g.startswith("plan:"):
            return g[5:].strip()
    return None


def plan_pending(item: Dict[str, Any]) -> bool:
    """True while a plan-gated ticket's plan has not settled (not yet started,
    planning, reviewing, or blocked on a human): no build worker may claim it.
    ``accepted`` and ``failed`` (the build proceeds, loudly) release it."""
    if plan_gate(item) is None:
        return False
    return (item.get("plan") or {}).get("status", "") not in ("accepted", "failed")


def _plan_update(ident: Any, fn) -> Optional[Dict[str, Any]]:
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        for it in data["items"]:
            if _matches(it, ident):
                plan = dict(it.get("plan") or {})
                fn(it, plan)
                it["plan"] = plan
                it["updated_at"] = _now_iso()
                _save_unlocked(data)
                return it
    return None


def plan_start(ident: Any) -> Optional[Dict[str, Any]]:
    """Begin planning (idempotent: a ticket already past ``planning`` keeps its
    state). Returns the item with ``item["plan"]``; ``item["_plan_started"]``
    is True only when this call started a fresh round the caller must spawn a
    planner for."""
    fresh = []

    def _do(it, plan):
        if plan.get("status") in ("planning", "reviewing", "discussing", "accepted", "failed", "blocked"):
            return
        plan.update(status="planning", round=1, text="", reviews=[])
        _append_history(it, "plan_start", by=_by("system"), at=_now_iso())
        fresh.append(True)

    item = _plan_update(ident, _do)
    if item is not None:
        item = dict(item, _plan_started=bool(fresh))
    return item


def plan_set_role(ident: Any, role: str, info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Record which planner/reviewer (engine/model/source/worker) was spawned.
    During an active discussion the new session also becomes the participant."""
    def _do(it, plan):
        plan[role] = dict(info)
        disc = plan.get("discussion")
        if disc and disc.get("status") == "active" and info.get("worker_id"):
            disc["participants"] = dict(disc.get("participants") or {},
                                        **{role: info["worker_id"]})
            plan["discussion"] = disc
    return _plan_update(ident, _do)


def plan_fail(ident: Any, reason: str) -> Optional[Dict[str, Any]]:
    """The plan stage could not run (e.g. blocked model, engine missing): the
    build proceeds without a plan, loudly (history event)."""
    def _do(it, plan):
        plan.update(status="failed", reason=_clip(reason, 1000))
        _append_history(it, "plan_failed", by=_by("system"), at=_now_iso(),
                        text=_clip(reason, 500))
    return _plan_update(ident, _do)


PLAN_TEXT_MAX = 24000


def plan_submit(ident: Any, text: str, by: str = "planner") -> Optional[Dict[str, Any]]:
    """The planner files (or revises) the plan; moves to ``reviewing``. Each
    submission is a new ``version``; during a discussion (WT-26) the amended
    plan goes back to the same reviewer for an explicit verdict on it."""
    text = str(text or "").strip()
    if not text:
        raise ValueError("plan text is empty")
    if len(text) > PLAN_TEXT_MAX:
        # Clipping silently stored a plan cut mid-sentence (OPS-1300) that the
        # reviewer then judged as if complete; make the planner condense it.
        raise ValueError(f"plan text is {len(text)} chars, over the {PLAN_TEXT_MAX} "
                         f"limit; condense it (keep test and rollout sections) and resubmit")
    current = get(ident)
    if current is None:
        return None
    if (current.get("plan") or {}).get("status") not in ("planning", "discussing"):
        raise ValueError(f"{current.get('ref', ident)} is not waiting for a plan "
                         f"(plan status {(current.get('plan') or {}).get('status') or 'none'})")

    def _do(it, plan):
        disc = plan.get("discussion")
        if not (disc and disc.get("status") == "active"):
            _plan_archive_role(plan, "reviewer")
        plan.update(status="reviewing", text=_clip(text, 24000),
                    version=int(plan.get("version") or 0) + 1)
        disc = plan.get("discussion")
        if disc and disc.get("status") == "active":
            disc.update(awaiting="reviewer", nudges=0, updated_at=_now_iso())
            plan["discussion"] = disc
        _append_history(it, "plan", by=_by("system"), at=_now_iso(),
                        text=_clip(text, PLAN_TEXT_MAX), round=plan.get("round", 1),
                        version=plan["version"], planner=str(by))
    return _plan_update(ident, _do)


def _plan_archive_role(plan: Dict[str, Any], role: str) -> None:
    """Move a finished stage's role metadata into ``role_history`` so the next
    supervision key never adopts it (WT-24)."""
    info = plan.pop(role, None)
    if info:
        plan["role_history"] = (list(plan.get("role_history") or [])
                                + [dict(info, role=role)])[-10:]


def _plan_reachable(plan: Dict[str, Any]) -> bool:
    return bool((plan.get("planner") or {}).get("worker_id")
                and (plan.get("reviewer") or {}).get("worker_id"))


def plan_verdict(ident: Any, accepted: bool, reasons: str = "",
                 by: str = "plan-reviewer",
                 version_seen: Optional[int] = None) -> Optional[Dict[str, Any]]:
    """The plan reviewer's verdict on plan ``version_seen`` (default: current).
    Accept -> ``accepted`` (records ``accepted_version``). The first reject
    with both roles reachable opens a planner<->reviewer discussion
    (``discussing``, WT-26); each further reject is another discussion round
    until PLAN_DISCUSSION_ROUNDS, then ``blocked`` with a recorded reason (the
    caller blocks the ticket for a human). Without reachable roles: back to
    ``planning`` (round + 1) while revisions remain, else ``blocked``."""
    current = get(ident)
    if current is None:
        return None
    if (current.get("plan") or {}).get("status") != "reviewing":
        raise ValueError(f"{current.get('ref', ident)} has no plan under review "
                         f"(plan status {(current.get('plan') or {}).get('status') or 'none'})")

    def _do(it, plan):
        rnd = int(plan.get("round") or 1)
        version = int(plan.get("version") or rnd)
        if version_seen is not None and int(version_seen) != version:
            raise ValueError(f"verdict is for plan v{version_seen} but the ticket holds "
                             f"v{version}; review the current plan")
        plan["reviews"] = list(plan.get("reviews") or []) + [
            {"round": rnd, "version": version, "accepted": bool(accepted),
             "reasons": _clip(reasons, 3000), "by": str(by), "at": _now_iso()}]
        disc = plan.get("discussion") or None
        now = _now_iso()
        if accepted:
            plan.update(status="accepted", accepted_version=version)
            if disc:
                disc.update(status="agreed", awaiting="", updated_at=now,
                            reason=f"reviewer accepted v{version}")
        elif disc or _plan_reachable(plan):
            drnd = int((disc or {}).get("round") or 0) + 1
            disc = disc or {"started_at": now, "started_by": str(by), "messages": [],
                            "participants": {
                                "planner": plan["planner"]["worker_id"],
                                "reviewer": plan["reviewer"]["worker_id"]}}
            disc.update(round=drnd, objections=_clip(reasons, 3000), updated_at=now,
                        awaiting="planner", nudges=0)
            cap = int(plan.get("discussion_rounds", PLAN_DISCUSSION_ROUNDS))
            if drnd > cap:
                disc.update(status="escalated", awaiting="",
                            reason=f"no agreement after {cap} discussion rounds")
                plan["status"] = "blocked"
            else:
                disc["status"] = "active"
                plan["status"] = "discussing"
            plan["discussion"] = disc
        elif rnd > int(plan.get("revision_limit", PLAN_MAX_REVISIONS)):
            plan["status"] = "blocked"
        else:
            _plan_archive_role(plan, "planner")
            _plan_archive_role(plan, "reviewer")
            plan.update(status="planning", round=rnd + 1)
        _append_history(it, "plan_review", by=_by("system"), at=now,
                        passed=bool(accepted), text=_clip(reasons, 1000), round=rnd,
                        version=version)
    return _plan_update(ident, _do)


def plan_discuss(ident: Any, role: str, text: str) -> Optional[Dict[str, Any]]:
    """Record one peer message in the planner<->reviewer discussion (WT-26).
    The CLI forwards it to the counterpart; state only keeps the transcript
    (last 30) so a restart can re-deliver it. No state transition."""
    role = str(role or "").strip().lower()
    if role not in ("planner", "reviewer"):
        raise ValueError("role must be 'planner' or 'reviewer'")
    text = str(text or "").strip()
    if not text:
        raise ValueError("discussion message is empty")
    current = get(ident)
    if current is None:
        return None
    plan = current.get("plan") or {}
    if not plan.get("discussion") or plan["discussion"].get("status") != "active":
        raise ValueError(f"{current.get('ref', ident)} has no active plan discussion")

    def _do(it, plan):
        disc = plan["discussion"]
        disc["messages"] = (list(disc.get("messages") or []) + [
            {"round": disc.get("round"), "from": role, "text": _clip(text, 3000),
             "at": _now_iso()}])[-30:]
        disc["updated_at"] = _now_iso()
        plan["discussion"] = disc
        _append_history(it, "plan_discussion", by=_by("system"), at=_now_iso(),
                        text=_clip(text, 1000), role=role, round=disc.get("round"))
    return _plan_update(ident, _do)


def plan_discussion_due(ident: Any, stale_s: float = 900.0) -> Optional[str]:
    """Read-only: the role a stalled active discussion is waiting on (silent
    for ``stale_s``), else None."""
    current = get(ident)
    if current is None:
        return None
    plan = current.get("plan") or {}
    disc = plan.get("discussion") or {}
    if disc.get("status") != "active" or not disc.get("awaiting"):
        return None
    try:
        age = time.time() - datetime.fromisoformat(
            str(disc.get("updated_at") or plan.get("updated_at") or "").replace("Z", "+00:00")
        ).timestamp()
    except Exception:
        age = stale_s
    if age < stale_s:
        return None
    return str(disc["awaiting"])


def plan_discussion_record_nudge(ident: Any, delivered: bool,
                                 reason: str = "") -> Optional[Dict[str, Any]]:
    """Record the outcome of a reminder to the awaited party (WT-29). A
    delivered reminder bumps ``nudges`` and blocks the plan once it exceeds
    PLAN_DISCUSSION_MAX_NUDGES; an undelivered one blocks right away with
    ``reason`` (nobody is there to remind) and leaves ``nudges`` alone."""
    def _do(it, plan):
        disc = plan.get("discussion") or {}
        if disc.get("status") != "active":
            return
        role = disc.get("awaiting")
        disc["updated_at"] = _now_iso()
        if delivered:
            disc["nudges"] = int(disc.get("nudges") or 0) + 1
            if disc["nudges"] <= PLAN_DISCUSSION_MAX_NUDGES:
                plan["discussion"] = disc
                return
            reason_txt = (f"{role} unresponsive after "
                          f"{PLAN_DISCUSSION_MAX_NUDGES} delivered reminders")
        else:
            reason_txt = reason or f"{role} unreachable"
        disc.update(status="escalated", reason=reason_txt)
        plan["status"] = "blocked"
        _append_history(it, "plan_discussion", by=_by("system"), at=_now_iso(),
                        text=reason_txt, role="system", round=disc.get("round"))
        plan["discussion"] = disc
    return _plan_update(ident, _do)


def plan_decide(ident: Any, decision: str, text: str = "", retries: int = 1,
                by: str = "human", session_id: str = "") -> Optional[Dict[str, Any]]:
    """Human decision on a plan gate blocked by exhausted review (WT-25).

    ``accept`` settles the plan as ``accepted`` (``text``, when given, replaces
    the plan text); ``retry`` resumes planning with ``retries`` further review
    rounds. Either clears the human block (reopening a ticket that has no
    resumable session, like ``answer``) and is recorded in ``plan["decisions"]``
    and history. A free-text ``answer`` never does this implicitly."""
    decision = str(decision or "").strip().lower()
    if decision not in ("accept", "retry"):
        raise ValueError("plan decision must be 'accept' or 'retry'")
    if decision == "retry" and int(retries) < 1:
        raise ValueError("retries must be at least 1")
    current = get(ident)
    if current is None:
        return None
    cur_status = (current.get("plan") or {}).get("status")
    if cur_status != "blocked":
        raise ValueError(f"{current.get('ref', ident)} has no blocked plan "
                         f"(plan status {cur_status or 'none'})")
    text = str(text or "").strip()
    now = _now_iso()
    who = _by("human", str(by or ""), str(session_id or ""))

    def _do(it, plan):
        rnd = int(plan.get("round") or 1)
        if decision == "accept":
            if text:
                plan["text"] = _clip(text, 24000)
            plan["status"] = "accepted"
        else:
            plan.update(status="planning", round=rnd + 1,
                        revision_limit=rnd + int(retries) - 1,
                        discussion_rounds=int(retries))
            plan.pop("discussion", None)
        plan["decisions"] = list(plan.get("decisions") or []) + [
            {"decision": decision, "round": rnd, "by": str(by or "human"), "at": now,
             "text": _clip(text, 3000), **({"retries": int(retries)} if decision == "retry" else {})}]
        _append_history(it, "plan_decision", by=who, at=now, decision=decision,
                        text=_clip(text, 4000), round=rnd)
        if it.get("status") == PARKED_STATUS and it.get("parked"):
            _write_pending_answer_unlocked(it, text or decision, now)
        it["needs_input"] = False
        it["answered_at"] = now
        it["block_question"] = ""
        it["block_kind"] = ""
        it["blocked_at"] = None
        if it.get("status") == "in_progress" and not it.get("claimed_session_id"):
            it["status"] = "open"
            it["claimed_by"] = None
            it["claimed_machine"] = None
            it["claimed_at"] = None
            it["block_commit"] = ""
            _append_history(it, "reopen", by=who, at=now,
                            reason="plan_decision_without_resumable_session")
    return _plan_update(ident, _do)


def _run_cmd_gate(command: str, repo_path: str, commit: str, ref: str) -> Dict[str, Any]:
    """Run one cmd gate; at the closing commit when it differs from the repo's
    checked-out HEAD (via a throwaway detached worktree)."""
    import shutil
    import subprocess
    import tempfile
    cwd = os.path.expanduser(repo_path) if repo_path else os.getcwd()
    tmp = ""
    started = time.time()
    try:
        if commit and repo_path and os.path.isdir(cwd):
            head = subprocess.run(["git", "-C", cwd, "rev-parse", "HEAD"],
                                  capture_output=True, text=True).stdout.strip()
            full = subprocess.run(["git", "-C", cwd, "rev-parse", commit],
                                  capture_output=True, text=True).stdout.strip()
            if full and head and full != head:
                tmp = tempfile.mkdtemp(prefix="wt-gate-")
                add = subprocess.run(
                    ["git", "-C", cwd, "worktree", "add", "--detach", tmp, full],
                    capture_output=True, text=True)
                if add.returncode == 0:
                    cwd = tmp
                else:
                    shutil.rmtree(tmp, ignore_errors=True)
                    tmp = ""
        env = dict(os.environ, WT_GATE_COMMIT=commit or "", WT_TICKET_REF=ref)
        try:
            proc = subprocess.run(command, shell=True, cwd=cwd, env=env,
                                  capture_output=True, text=True,
                                  timeout=GATE_CMD_TIMEOUT_S)
            ok, out = proc.returncode == 0, (proc.stdout or "") + (proc.stderr or "")
            code = proc.returncode
        except subprocess.TimeoutExpired:
            ok, out, code = False, f"timed out after {GATE_CMD_TIMEOUT_S}s", -1
        except OSError as exc:
            ok, out, code = False, str(exc), -1
    finally:
        if tmp:
            subprocess.run(["git", "-C", os.path.expanduser(repo_path), "worktree",
                            "remove", "--force", tmp], capture_output=True)
            shutil.rmtree(tmp, ignore_errors=True)
    return {"gate": "cmd:" + command, "passed": ok, "exit_code": code,
            "output_tail": out[-GATE_OUTPUT_TAIL:], "commit": commit or "",
            "at": _now_iso(), "seconds": round(time.time() - started, 1)}


def evaluate_gates(item: Dict[str, Any], commit: str = "") -> Tuple[List[Dict[str, Any]], List[str]]:
    """Run the cmd gates in order, stopping at the first failure. Returns
    ``(results, stages)``: ``stages`` is the ordered list still owed once all
    cmd gates passed (``"verify"`` then ``"review"`` / ``"review:<target>"``),
    empty when the ticket can close outright. A failed result has ``passed``
    False (and ``stages`` is then empty)."""
    results: List[Dict[str, Any]] = []
    gates = effective_gates(item)
    for g in gates:
        if g.startswith("cmd:"):
            r = _run_cmd_gate(g[4:], str(item.get("repo_path") or ""), commit,
                              str(item.get("ref") or ""))
            results.append(r)
            if not r["passed"]:
                return results, []
    stages: List[str] = []
    if "verify" in gates:
        stages.append("verify")
    for g in gates:
        if g == "review" or g.startswith("review:"):
            stages.append(g)
            break
    return results, stages


def gate_stage_label(stage: str) -> str:
    """Who/what a pending stage waits on, for prose."""
    if stage == "verify":
        return "independent verifier"
    return stage[7:].strip() if stage.startswith("review:") else "submitter"


def verifier_target(item: Dict[str, Any]) -> Dict[str, Any]:
    """Engine/model the verify gate's verifier runs on (WT-6, WT-14): the
    ``verifier`` role from :func:`roles.effective_role_model` (ticket override,
    else the queue's ``--verifier-engine/--verifier-model``, else a different
    family than the builder). The builder is the claiming worker's recorded
    engine+model when known. Returns ``{engine, model, source, blocked}``;
    ``blocked`` is True when the resolved model is on the policy deny-list
    (the caller must then not spawn -- never substitute)."""
    return _role_target(item, "verifier")


def assessor_target(item: Dict[str, Any]) -> Dict[str, Any]:
    """Engine/model the post-fix assessor runs on (WT-21): the ``assessor``
    role, a different family than the builder unless configured."""
    return _role_target(item, "assessor")


def _role_target(item: Dict[str, Any], role: str) -> Dict[str, Any]:
    """Resolve ``role`` for a ticket against the claiming worker's recorded
    engine+model as builder. Returns ``{engine, model, source, blocked}``."""
    from . import config as _config, roles as _roles
    queue = str(item.get("project") or "")
    ticket = dict(item)
    wid = str(item.get("claimed_by") or "")
    if wid and not ticket.get("model_floor"):
        try:
            from . import workers as _workers
            rec = next((w for w in _workers._load().get("workers", [])
                        if isinstance(w, dict) and w.get("worker_id") == wid
                        and w.get("engine")), None)
        except Exception:
            rec = None
        if rec and rec.get("model"):
            # The worker that built it is the builder, whatever the queue says now.
            ticket["model_floor"] = str(rec["model"])
    engine, model, source = _roles.effective_role_model(queue, ticket, role)
    model = _config.canonical_model(engine, model) if model else ""
    return {"engine": engine, "model": model, "source": source,
            "blocked": bool(model and _config.is_blocked_model(model))}


def stage_session_update(ident: Any, fn) -> Optional[Dict[str, Any]]:
    """Run ``fn(item, stage_session_dict)`` under the store lock (WT-24); one
    save. ``fn`` returning ``"skip"`` leaves the store untouched."""
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        for it in data["items"]:
            if _matches(it, ident):
                ss = dict(it.get("stage_session") or {})
                if fn(it, ss) == "skip":
                    return it
                it["stage_session"] = ss
                it["updated_at"] = _now_iso()
                _save_unlocked(data)
                return it
    return None


def _stage_retry_reset(it: Dict[str, Any], text: str, now: str) -> bool:
    """Human ``answer`` on a stage escalation (WT-24), same store transaction:
    per-role retry transition + a fresh attempt budget. True when it applied."""
    ss = it.get("stage_session") or {}
    if not ss.get("escalated"):
        return False
    role = str(ss.get("role") or "")
    if role in ("planner", "plan_reviewer"):
        plan = dict(it.get("plan") or {})
        if plan.get("escalated") == "stage_watch":
            plan["status"] = ss.get("escalated_from") or (
                "planning" if role == "planner" else "reviewing")
            plan.pop("escalated", None)
            it["plan"] = plan
    elif role == "assessor":
        a = dict(it.get("assessment") or {})
        if a.get("status") == "failed":
            _assessment_new_cycle(a, "human retry")
            it["assessment"] = a
            _append_history(it, "assessment", by=_by("system"), at=now, outcome="retry")
    it["stage_session"] = {
        "key": ss.get("key", ""), "role": role, "attempt": 0, "escalated": False,
        "deaths": list(ss.get("deaths") or [])[-10:], "retry_at": now,
        "retry_note": _clip(text, 2000),
    }
    _append_history(it, "stage_retry", by=_by("human"), at=now, role=role,
                    text=_clip(text, 500))
    return True


def set_verifier_info(ident: Any, info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Record which verifier (engine/model/worker) was spawned for a ticket."""
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        for it in data["items"]:
            if _matches(it, ident):
                it["verifier"] = dict(info)
                _save_unlocked(data)
                return it
    return None


def checks_block(item: Dict[str, Any]) -> str:
    """Plain-words "Checks after you close" block for a ticket with gates
    (empty string when it has none). Told to the worker at claim time so it
    does not run its own independent verifier."""
    gates = effective_gates(item)
    if not gates:
        return ""
    lines = ["Checks after you close:"]
    has_verify = False
    for g in gates:
        if g.startswith("cmd:"):
            lines.append(f"- WatchTower runs: {g[4:]}")
        elif g == "plan" or g.startswith("plan:"):
            lines.append(f"- Plan first: a planner writes and a reviewer accepts a plan "
                         f"before you build; `wt plan wait {item.get('ref', '<ref>')}` "
                         f"prints it. Follow the accepted plan.")
        elif g == "verify":
            has_verify = True
            what = str(item.get("accept") or "").strip() or "the ticket text"
            try:
                vt = verifier_target(item)
                who = f" ({vt['engine']}{'/' + vt['model'] if vt['model'] else ''})"
            except Exception:
                who = ""
            lines.append(f"- An independent verifier{who} checks: {what}")
        else:
            lines.append(f"- Review by {gate_stage_label(g)} (wt accept / wt reject)")
    lines.append("Still your job: write and run tests for this change.")
    if has_verify:
        lines.append("Don't launch your own independent verifier; this ticket "
                     "already gets one.")
    return "\n".join(lines)


def _reviewer_target(item: Dict[str, Any], stage: str) -> str:
    target = gate_stage_label(stage)
    return str(item.get("submitter") or "") if target == "submitter" else target


def _dependents_of(ref: str) -> List[str]:
    try:
        return [str(i.get("ref")) for i in list_items(fresh=True)
                if ref in (i.get("blocked_by") or []) and i.get("status") != "closed"]
    except Exception:
        return []


def _notify_review(item: Dict[str, Any], reviewer: str, actor: Any) -> None:
    if reviewer == "verify":
        return  # the verifier is spawned by the CLI; nobody to notify yet
    target = _reviewer_target(item, reviewer)
    ref = str(item.get("ref") or "?")
    if not target:
        return
    res = item.get("resolution") or {}
    deps = _dependents_of(ref)
    text = (f"[watchtower] {ref} awaits your review -- "
            f"{_clip(str(res.get('summary') or item.get('title') or ''), 200)}. "
            f"Run `wt accept {ref}` or `wt reject {ref} --reason \"...\"`."
            + (f" Accepting unblocks: {', '.join(deps)}." if deps else ""))
    try:
        from . import messages
        if target not in _actor_identities(actor):
            messages.send(messages.ccc_forward_target(target) or target, text, notify=True)
    except Exception:
        pass


def accept(ident: Any, by: str = "human", force: bool = False) -> Optional[Dict[str, Any]]:
    """Accept an ``in_review`` ticket: it becomes ``closed`` (its dependents
    unblock). Raises ValueError when it is not awaiting review, or while the
    independent verifier's verdict is still pending (``force`` overrides)."""
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        for it in data["items"]:
            if _matches(it, ident):
                if it.get("status") != "in_review":
                    raise ValueError(f"{it.get('ref', ident)} is {it.get('status')}, "
                                     "not in_review -- nothing to accept")
                if it.get("gate_pending") == "verify" and not force:
                    raise ValueError(
                        f"{it.get('ref', ident)} is waiting on its independent "
                        f"verifier's verdict (wt verdict); --force overrides")
                now = _now_iso()
                it["status"] = "closed"
                it["closed_at"] = now
                it["updated_at"] = now
                it.pop("gate_pending", None)
                it.pop("gate_stages", None)
                _drop_claim_proc_unlocked(it)
                it["gate_accepted_by"] = str(by)
                _append_history(it, "accept", by=_by("human", str(by)), at=now)
                _escalate_stuck_blockers(data["items"])
                _save_unlocked(data)
                item = it
                break
        else:
            return None
    _log("ACCEPT", f"{item.get('ref', '?')}", queue=item.get("project", ""))
    res = item.get("resolution") or {}
    _notify_ticket_event(item, "closed", detail=res.get("summary", "") if isinstance(res, dict) else "",
                         actor=by)
    assessment_mark_due(item.get("ref") or ident)
    return get(item.get("ref") or ident) or item


def verdict(ident: Any, passed: bool, findings: str = "", by: str = "verifier") -> Optional[Dict[str, Any]]:
    """File the independent verifier's verdict on an ``in_review`` ticket
    whose pending stage is ``verify``. Pass: advance to the next stage (a
    review) or close. Fail: back to open with the findings and the original
    worker session re-bound (the CLI resumes it)."""
    current = get(ident)
    if current is None:
        return None
    if current.get("status") != "in_review" or current.get("gate_pending") != "verify":
        raise ValueError(f"{current.get('ref', ident)} is not waiting on a verifier "
                         f"(status {current.get('status')})")
    entry = {"gate": "verify", "passed": bool(passed), "output_tail": _clip(findings, GATE_OUTPUT_TAIL),
             "by": str(by), "at": _now_iso()}
    v = current.get("verifier") or {}
    if v.get("engine"):
        entry["engine"], entry["model"] = v.get("engine"), v.get("model", "")
    if not passed:
        why = f"independent verification failed: {_clip(findings, 3000) or '(no findings given)'}"
        item = reject_with(ident, why, by_label=str(by), results_add=entry)
        return item
    nxt = ""
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        for it in data["items"]:
            if _matches(it, ident):
                it["gate_results"] = list(it.get("gate_results") or []) + [entry]
                stages = [x for x in (it.get("gate_stages") or []) if x != "verify"]
                it["gate_stages"] = stages
                it["updated_at"] = _now_iso()
                _append_history(it, "verify", by=_by("system"), at=it["updated_at"],
                                passed=True, findings=_clip(findings, 500))
                if stages:
                    it["gate_pending"] = nxt = stages[0]
                _save_unlocked(data)
                item = it
                break
        else:
            return None
    if nxt:
        _notify_review(item, nxt, by)
        return item
    return accept(ident, by=f"verifier:{by}", force=True)


def reject_with(ident: Any, why: str, by_label: str = "human",
                results_add: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    """Shared reject path: back to open (session re-bound when known) with
    ``why`` on ``gate_feedback``."""
    current = get(ident)
    if current is None:
        return None
    sid = str(current.get("claimed_session_id") or "")
    if sid and _github_backend_for_project(_project_from_ident(ident)) is None:
        # WT-30 D1: keep the ORIGINAL claimer (worker id) so the orphan sweep
        # can tell a spawned builder from an ambient session; the session id
        # rides in claimed_session_id.
        # WT-31: the re-bind inherits the builder's process (kept through
        # hold_review), so its death is provable even after its record is pruned.
        item = reopen_and_claim(ident, str(current.get("claimed_by") or sid),
                                session_uuid=sid, reason=why,
                                sent_back_reason=why, sent_back_by=by_label,
                                claim_proc=_inheritable_proc(current, sid))
        if item is None:
            return None
        with _FileLock(_lock_path()):
            data = _load_unlocked()
            for it in data["items"]:
                if _matches(it, ident):
                    it["resume"] = {"sid": sid, "state": "pending", "at": _now_iso()}
                    _save_unlocked(data)
                    break
    else:
        item = update_status(ident, "open", reason=why, by_kind="human")
        if item is None:
            return None
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        for it in data["items"]:
            if _matches(it, ident):
                it.pop("gate_pending", None)
                it.pop("gate_stages", None)
                it["gate_feedback"] = _clip(why, 4000)
                if results_add:
                    it["gate_results"] = list(it.get("gate_results") or []) + [results_add]
                _save_unlocked(data)
                return it
    return item


RESUME_TERMINAL_STATES = ("failed", "skipped")


def set_resume_state(ident: Any, sid: str, state: str, transport: str = "",
                     error: str = "", outbox_id: str = "",
                     at: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Record resume evidence on an in_progress ticket (WT-30 D3).

    Guarded: the ticket must still be in_progress under ``sid`` and the current
    state must not be terminal (failed/skipped). A write-back from the outbox
    drain (``outbox_id`` with a non-``queued`` state) only applies when the
    ticket's ``resume.outbox_id`` is that row. Creates the record when absent
    (``wt reopen --resume`` / answers have no reject stamp)."""
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        for it in data["items"]:
            if not _matches(it, ident):
                continue
            if it.get("status") != "in_progress" or str(it.get("claimed_session_id") or "") != str(sid):
                return None
            cur = it.get("resume") if isinstance(it.get("resume"), dict) else None
            if cur and cur.get("state") in RESUME_TERMINAL_STATES:
                return None
            if outbox_id and state != "queued" and (not cur or cur.get("outbox_id") != outbox_id):
                return None
            rec = dict(cur or {"sid": str(sid)})
            rec["state"] = state
            rec["at"] = at or _now_iso()
            if transport:
                rec["transport"] = transport
            if error:
                rec["error"] = _clip(error, 500)
            if outbox_id:
                rec["outbox_id"] = outbox_id
            it["resume"] = rec
            _save_unlocked(data)
            return it
    return None


def reject(ident: Any, reason: str, by: str = "human") -> Optional[Dict[str, Any]]:
    """Reject an ``in_review`` ticket: back to ``open`` with ``reason`` kept
    on ``gate_feedback`` (the caller resumes the original session)."""
    if not str(reason or "").strip():
        raise ValueError("reject needs a --reason")
    current = get(ident)
    if current is None:
        return None
    if current.get("status") != "in_review":
        raise ValueError(f"{current.get('ref', ident)} is {current.get('status')}, "
                         "not in_review -- nothing to reject")
    return reject_with(ident, f"rejected by {by}: {reason}", by_label=str(by))


def _reopen_with_feedback(ident: Any, reason: str, results: List[Dict[str, Any]],
                          by_kind: str = "worker", close_owner: str = "",
                          session_id: str = "", session_uuid: str = "",
                          require_status: Optional[str] = None) -> Optional[Dict[str, Any]]:
    item = update_status(ident, "open", session_id, session_uuid=session_uuid,
                         reason=reason, by_kind=by_kind,
                         close_owner=close_owner, require_status=require_status,
                         extras={"gate_feedback": _clip(reason, 4000),
                                 "gate_results": results})
    return item


def close(
    ident: Any, session_id: str = "", resolution: Any = None, force: bool = False,
    declined: bool = False, session_uuid: str = "",
) -> Optional[Dict[str, Any]]:
    """Close a ticket, optionally recording HOW it was fixed.

    ``declined``: this close IS the product-gate Nack — exempt from the gate guard.

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
    if not force and not declined:
        current = get(ident)
        if current is not None:
            try:
                from . import config as _config
                gated = _config.product_gate(str(current.get("project") or ""))
            except Exception:
                gated = False
            if (
                gated
                and not current.get("product_ack")
                and not current.get("pre_ack")
            ):
                raise ValueError(
                    f"{current.get('ref', ident)}: this queue has the product "
                    f"gate on and the ticket was never Acked. Post your pitch "
                    f"and wait for the decision: `wt block "
                    f"{current.get('ref', ident)} --worker <your-id> --kind "
                    f"rationale --question \"<pitch>\"`. (--force overrides "
                    f"deliberately.)"
                )
    hold_review, extras = "", None
    if not force and _github_backend_for_project(_project_from_ident(ident)) is None:
        current = get(ident)
        if current is not None and current.get("status") != "closed" and effective_gates(current):
            norm = _normalize_resolution(resolution) or {}
            results, stages = evaluate_gates(current, str(norm.get("commit") or ""))
            failed = [r for r in results if not r["passed"]]
            if failed:
                f = failed[0]
                why = (f"gate {f['gate']} failed (exit {f['exit_code']}): "
                       f"{f['output_tail'][-600:].strip()}")
                # WT-34: the writeback re-checks ownership and the pre-gate
                # status in its own lock (gates can run for minutes).
                item = _reopen_with_feedback(
                    ident, why, results,
                    close_owner="" if force else str(session_id or ""),
                    session_id=str(session_id or ""), session_uuid=session_uuid,
                    require_status=current.get("status"))
                if item is None:
                    now_it = get(ident) or {}
                    raise ValueError(
                        f"{current.get('ref', ident)} changed while its gates ran "
                        f"(was {current.get('status')}, now {now_it.get('status')}); "
                        "nothing was written. Re-check with "
                        f"`wt find {current.get('ref', ident)}` before closing again.")
                return item
            extras = {"gate_results": results, "gate_stages": stages}
            hold_review = stages[0] if stages else ""
    item = update_status(
        ident, "closed", session_id, session_uuid=session_uuid, resolution=resolution,
        expect_owner="" if force else str(session_id or ""),
        hold_review=hold_review, extras=extras,
    )
    if item and item.get("status") == "in_review":
        _notify_review(item, hold_review, session_id)
        return item
    if item and item.get("status") == "closed":
        try:
            from . import messages
            messages.ledger_clear_ref(str(item.get("ref") or ident))
        except Exception:  # noqa: BLE001 - ledger rows are best-effort hygiene
            pass
        try:
            with _FileLock(_lock_path()):
                data = _load_unlocked()
                if _escalate_stuck_blockers(data["items"]):
                    _save_unlocked(data)
        except Exception:  # noqa: BLE001 - claim_next re-runs this sweep anyway
            pass
        res = item.get("resolution") or {}
        summary = res.get("summary", "") if isinstance(res, dict) else (
            res if isinstance(res, str) else ""
        )
        _notify_ticket_event(item, "closed", detail=summary, actor=session_id)
        if not declined:
            assessment_mark_due(item.get("ref") or ident)
            item = get(item.get("ref") or ident) or item
    return item


# ---------------------------------------------------------------------------
# Post-fix assessment (WT-21)
#
# A queue opted in with ``wt config --post-fix-assessment on`` gets, for every
# bug ticket that closes as completed, an independent assessor session that
# answers six questions about the fix and WatchTower files the follow-ups.
# State lives on ``item["assessment"]``; every transition is one store
# transaction (lock + a single _save_unlocked), and filing is one transaction
# per op so concurrent/repeat runners cannot double-write.
# ---------------------------------------------------------------------------

ASSESSMENT_POINTS = ("logging", "ui_message", "automation", "monitoring", "auditors", "other")
ASSESSMENT_QUESTIONS = {
    "logging": "Was logging adequate to diagnose this without guessing?",
    "ui_message": "Was the message the user saw adequate?",
    "automation": "Can it be fully automated (detect -> ask -> act -> recover -> notify)?",
    "monitoring": "Did internal monitoring alert the owner?",
    "auditors": "Would the auditors / self-healing have caught it?",
    "other": "Any other small improvements?",
}
_ASSESS_TYPE = {"logging": "bug", "ui_message": "bug", "automation": "feature",
                "monitoring": "feature", "auditors": "feature", "other": "feature"}
ASSESSMENT_SOURCE = "post-fix-assessment"
_ASSESS_MAX_PER_POINT = 3
_ASSESS_MAX_TOTAL = 8
# Test hook: called as hook(stage, idx) at "before_lock" / "after_commit".
_ASSESS_OP_HOOK = None


class AssessmentFenced(Exception):
    """The attempt this runner belongs to is no longer current."""


def _assessment_title_key(title: Any) -> str:
    import re as _re2
    return " ".join(_re2.sub(r"[^a-z0-9 ]", "", str(title or "").lower()).split())


def assessment_due(item: Dict[str, Any]) -> bool:
    """True when a just-closed ``item`` should get a post-fix assessment."""
    from . import config as _config
    if item.get("status") != "closed" or item.get("assessment"):
        return False
    proj = str(item.get("project") or "")
    try:
        if not _config.post_fix_assessment(proj):
            return False
    except Exception:  # noqa: BLE001
        return False
    if _github_backend_for_project(proj) is not None:
        return False
    if effective_type(item) != "bug":
        return False
    if item.get("source") == ASSESSMENT_SOURCE or item.get("assessment_origin"):
        return False
    if item.get("product_nack"):
        return False
    res = item.get("resolution") or {}
    summary = str(res.get("summary", "") if isinstance(res, dict) else res).strip().lower()
    if summary.startswith(("duplicate of", "dup of", "duplicate:")):
        return False
    return True


def assessment_mark_due(ident: Any) -> Optional[Dict[str, Any]]:
    """Best-effort: flag a freshly closed ticket ``due`` (idempotent)."""
    try:
        with _FileLock(_lock_path()):
            data = _load_unlocked()
            for it in data["items"]:
                if _matches(it, ident):
                    if not assessment_due(it):
                        return None
                    it["assessment"] = {"status": "due", "attempt": 0, "token": "",
                                        "cycle": 1}
                    it["updated_at"] = _now_iso()
                    _save_unlocked(data)
                    return it
    except Exception:  # noqa: BLE001 - never break a close
        return None
    return None


def _assessment_new_cycle(a: Dict[str, Any], reason: str = "") -> None:
    """Start a new assessment cycle inside a store transaction (WT-24): fence
    the current token, archive the assessor, and go back to ``due``. Every
    human retry / forced run goes through here so no path carries a stale
    assessor into the next cycle. ``attempt`` is kept for display only."""
    if a.get("token"):
        a["fenced_tokens"] = (list(a.get("fenced_tokens") or []) + [a["token"]])[-10:]
    if a.get("assessor"):
        a["assessor_history"] = (list(a.get("assessor_history") or []) + [a["assessor"]])[-10:]
    a.pop("assessor", None)
    a.pop("token", None)
    a.pop("reason", None)
    a.pop("reserved_at", None)
    a["cycle"] = int(a.get("cycle") or 1) + 1
    a["status"] = "due"
    if reason:
        a["cycle_reason"] = reason


def assessment_rotate(ident: Any, old_token: str) -> Optional[str]:
    """Respawn within a cycle (WT-24): while ``running`` on ``old_token``, fence
    it, mint a new token, ``attempt += 1`` and clear the assessor metadata, in
    one transaction. None when the ticket moved on (submitted / forced)."""
    import uuid as _uuid
    out: List[str] = []

    def _do(it, a, data):
        if a.get("status") != "running" or a.get("token") != old_token:
            return "skip"
        a["fenced_tokens"] = (list(a.get("fenced_tokens") or []) + [old_token])[-10:]
        if a.get("assessor"):
            a["assessor_history"] = (list(a.get("assessor_history") or []) + [a["assessor"]])[-10:]
        a.pop("assessor", None)
        a["token"] = _uuid.uuid4().hex
        a["attempt"] = int(a.get("attempt", 0)) + 1
        a["reserved_at"] = _now_iso()
        out.append(a["token"])

    _assessment_update(ident, _do)
    return out[0] if out else None


def _assessment_update(ident: Any, fn) -> Optional[Dict[str, Any]]:
    """Run ``fn(item, assessment_dict)`` under the store lock; one save. ``fn``
    returning ``"skip"`` leaves the store untouched."""
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        for it in data["items"]:
            if _matches(it, ident):
                a = dict(it.get("assessment") or {})
                if fn(it, a, data) == "skip":
                    return it
                it["assessment"] = a
                it["updated_at"] = _now_iso()
                _save_unlocked(data)
                return it
    return None


def assessment_reserve(ident: Any, force: bool = False) -> Optional[str]:
    """Reserve the next assessment attempt and return its token, or None when
    another actor already holds it (or it is finished). The only route to
    ``running``."""
    import uuid as _uuid
    out: List[str] = []

    def _do(it, a, data):
        st = a.get("status", "")
        if it.get("status") != "closed":
            return "skip"
        if not (st in ("due", "failed") or force):
            return "skip"
        if force and st == "filing":
            done = [{"attempt": a.get("attempt", 0), "ref": o.get("result_ref"), "kind": o.get("kind")}
                    for o in (a.get("pending") or {}).get("ops", []) if o.get("result_ref")
                    and o.get("outcome") == "filed"]
            a["abandoned"] = list(a.get("abandoned") or []) + done
        a.pop("pending", None)
        a.pop("assessor", None)
        a.setdefault("cycle", 1)
        a["attempt"] = int(a.get("attempt", 0)) + 1
        a["token"] = _uuid.uuid4().hex
        a["status"] = "running"
        a["reserved_at"] = _now_iso()
        a.pop("reason", None)
        out.append(a["token"])

    _assessment_update(ident, _do)
    return out[0] if out else None


def assessment_set_running(ident: Any, token: str, info: Dict[str, Any]) -> None:
    def _do(it, a, data):
        if a.get("token") != token or a.get("assessor"):
            return "skip"
        a["assessor"] = dict(info)
    _assessment_update(ident, _do)


def assessment_fail(ident: Any, token: str, reason: str) -> None:
    def _do(it, a, data):
        if token and a.get("token") != token:
            return "skip"
        a["status"] = "failed"
        a["reason"] = _clip(reason, 500)
        a["at"] = _now_iso()
        _append_history(it, "assessment", by=_by("system"), outcome="failed",
                        text=_clip(reason, 500))
    _assessment_update(ident, _do)


def assessment_release_due(ident: Any, token: str) -> None:
    """Dead assessor on its first attempt: back to ``due`` for a fresh token."""
    def _do(it, a, data):
        if a.get("token") != token or a.get("status") != "running":
            return "skip"
        a["status"] = "due"
    _assessment_update(ident, _do)


def _norm_followup(f: Any, default_queue: str) -> Dict[str, str]:
    if isinstance(f, str):
        f = {"title": f}
    if not isinstance(f, dict):
        raise ValueError("a follow-up must be an object with a title")
    title = _clip(f.get("title", ""), 200).strip()
    if not title:
        raise ValueError("a follow-up needs a title")
    return {"title": title, "note": _clip(f.get("note", "") or f.get("text", ""), 4000),
            "queue": _norm_project(f.get("queue") or default_queue) or default_queue}


def _validate_assessment_payload(data: Dict[str, Any], bug: Dict[str, Any],
                                 payload: Any) -> Dict[str, Dict[str, Any]]:
    """Normalise + validate; raises ValueError (nothing written)."""
    if not isinstance(payload, dict):
        raise ValueError("assessment payload must be a JSON object")
    pts = payload.get("points", payload)
    if not isinstance(pts, dict):
        raise ValueError("'points' must be an object")
    unknown = set(pts) - set(ASSESSMENT_POINTS)
    if unknown:
        raise ValueError(f"unknown point(s): {sorted(unknown)}; expected {list(ASSESSMENT_POINTS)}")
    missing = [p for p in ASSESSMENT_POINTS if p not in pts]
    if missing:
        raise ValueError(f"missing point(s): {missing}")
    default_queue = str(bug.get("project") or "")
    by_ref = {str(i.get("ref")): i for i in data["items"]}
    out: Dict[str, Dict[str, Any]] = {}
    total = 0
    for p in ASSESSMENT_POINTS:
        raw = pts[p]
        if isinstance(raw, str):
            raw = {"verdict": "adequate", "note": raw}
        if not isinstance(raw, dict):
            raise ValueError(f"point {p!r} must be an object")
        extra = set(raw) - {"verdict", "note", "followups", "existing"}
        if extra:
            raise ValueError(f"point {p!r}: unknown key(s) {sorted(extra)}")
        verdict = str(raw.get("verdict") or "adequate").strip().lower()
        if verdict not in ("adequate", "gap"):
            raise ValueError(f"point {p!r}: verdict must be 'adequate' or 'gap'")
        fups = [_norm_followup(f, default_queue) for f in (raw.get("followups") or [])]
        existing = [str(r).strip() for r in (raw.get("existing") or []) if str(r).strip()]
        if verdict == "gap" and not fups and not existing:
            raise ValueError(f"point {p!r} is a gap but has no follow-up and no existing ref")
        if len(fups) > _ASSESS_MAX_PER_POINT:
            raise ValueError(f"point {p!r}: at most {_ASSESS_MAX_PER_POINT} follow-ups")
        total += len(fups)
        for f in fups:
            if _github_backend_for_project(f["queue"]) is not None:
                raise ValueError(f"follow-up queue {f['queue']} is GitHub-backed; not supported")
            if f["queue"] != default_queue and not any(
                    _norm_project(i.get("project")) == f["queue"] for i in data["items"]):
                from . import config as _config
                if f["queue"] not in _config.all_queues():
                    raise ValueError(f"unknown follow-up queue {f['queue']!r}")
        for r in existing:
            t = by_ref.get(r)
            if t is None:
                raise ValueError(f"existing ref {r} is unknown")
            if t.get("status") == "closed":
                raise ValueError(f"{r} is closed; file a new follow-up instead")
        note = _clip(raw.get("note", ""), 2000).strip()
        if not note and verdict == "adequate":
            note = "nothing to add"
        out[p] = {"verdict": verdict, "note": note, "followups": fups, "existing": existing}
    if total > _ASSESS_MAX_TOTAL:
        raise ValueError(f"at most {_ASSESS_MAX_TOTAL} follow-ups in total (got {total})")
    return out


def _assessment_summary_text(ref: str, points: Dict[str, Dict[str, Any]]) -> str:
    lines = [f"Post-fix assessment of {ref}:"]
    for p in ASSESSMENT_POINTS:
        v = points[p]
        bits = [f"{f['queue']}: {f['title']}" for f in v["followups"]] + v["existing"]
        lines.append(f"- {p} [{v['verdict']}]: {v['note']}"
                     + (f" -> {'; '.join(bits)}" if bits else ""))
    return "\n".join(lines)


def assessment_accept_submission(ident: Any, token: str, payload: Any) -> List[Dict[str, Any]]:
    """Phase 1: fence, validate, and persist the op list; ``running`` ->
    ``filing``. A resubmission of the same payload on the same token is a
    resume and returns the stored ops."""
    import hashlib as _hashlib
    out: List[List[Dict[str, Any]]] = []
    phash = _hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode()).hexdigest()

    def _do(it, a, data):
        if a and token and token in (a.get("fenced_tokens") or []):
            raise AssessmentFenced("superseded by a respawn")
        if not a or a.get("token") != token:
            raise AssessmentFenced("stale or unknown assessment token")
        st = a.get("status")
        if st == "filing":
            if (a.get("pending") or {}).get("payload_hash") != phash:
                raise ValueError("an assessment submission is already being filed; "
                                 "resubmit the same payload to resume")
            out.append(a["pending"]["ops"])
            return "skip"
        if st != "running":
            raise AssessmentFenced(f"assessment is {st or 'not started'}, not running")
        points = _validate_assessment_payload(data, it, payload)
        ref, attempt = str(it.get("ref")), int(a.get("attempt", 1))
        ops: List[Dict[str, Any]] = []
        seen = set()
        for p in ASSESSMENT_POINTS:
            for i, f in enumerate(points[p]["followups"]):
                k = (f["queue"], _assessment_title_key(f["title"]))
                if k in seen:
                    continue
                seen.add(k)
                ops.append({"kind": "file", "point": p, "key": f"{ref}#a{attempt}#{p}#{i}", **f})
            for r in points[p]["existing"]:
                ops.append({"kind": "link", "point": p, "target": r,
                            "marker": f"[wt-assess {ref}#a{attempt}#{p}]",
                            "note": points[p]["note"]})
        ops.append({"kind": "summary", "target": ref,
                    "marker": f"[wt-assess {ref}#a{attempt}#summary]",
                    "text": _assessment_summary_text(ref, points)})
        a["points"] = points
        a["pending"] = {"payload_hash": phash, "submitted_at": _now_iso(), "ops": ops}
        a["status"] = "filing"
        out.append(ops)

    if _assessment_update(ident, _do) is None:
        raise ValueError(f"no item {ident}")
    return out[0]


def _has_marker(item: Dict[str, Any], marker: str) -> bool:
    return any(h.get("event") == "comment" and marker in str(h.get("text", ""))
               for h in (item.get("history") or []))


def _assessment_apply_op(ident: Any, token: str, idx: int) -> Dict[str, Any]:
    """Phase 2: fenced check + write + done-record in ONE store transaction."""
    hook = _ASSESS_OP_HOOK
    if hook:
        hook("before_lock", idx)
    logs: List[tuple] = []
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        bug = next((i for i in data["items"] if _matches(i, ident)), None)
        if bug is None:
            raise ValueError(f"no item {ident}")
        a = dict(bug.get("assessment") or {})
        if a.get("status") != "filing" or a.get("token") != token:
            raise AssessmentFenced(
                f"superseded by attempt {a.get('attempt')}" if a.get("token") != token
                else f"assessment is {a.get('status')}")
        ops = a["pending"]["ops"]
        op = ops[idx]
        if op.get("done"):
            return op
        ref = str(bug.get("ref"))
        now = _now_iso()
        if op["kind"] == "file":
            adopted = next((i for i in data["items"]
                            if i.get("assessment_origin") == op["key"]), None)
            if adopted is not None:
                op.update(result_ref=adopted.get("ref"), outcome="adopted")
            else:
                tk = _assessment_title_key(op["title"])
                match = next((i for i in data["items"]
                              if i is not bug
                              and _norm_project(i.get("project")) == op["queue"]
                              and i.get("status") in ("open", "in_progress", "in_review")
                              and _assessment_title_key(i.get("title") or i.get("note")) == tk), None)
                if match is not None:
                    marker = f"[wt-assess {op['key']}]"
                    if not _has_marker(match, marker):
                        _append_history(match, "comment", by=_by("system"), at=now,
                                        text=f"{marker} Also raised by the post-fix assessment of {ref} "
                                             f"({op['point']}): {op['note'] or op['title']}")
                        match["updated_at"] = now
                    op.update(result_ref=match.get("ref"), outcome="linked")
                else:
                    new = _new_item_unlocked(
                        data, note=op["title"],
                        text=(f"{op['note']}\n\n(Filed by the post-fix assessment of {ref}, "
                              f"point: {op['point']} -- {ASSESSMENT_QUESTIONS[op['point']]})").strip(),
                        source=ASSESSMENT_SOURCE, proj=op["queue"], annotation_id="", url="",
                        title=op["title"], selector="", screenshot_path="",
                        repo_path=str(bug.get("repo_path") or "") if op["queue"] == _norm_project(bug.get("project")) else "",
                        lane="normal", item_type=_ASSESS_TYPE[op["point"]], readiness="",
                        priority="", value="", confidence="", model_floor="", planner_model="",
                        verifier_model="", submitter="", submitter_explicit=False, pre_ack=False,
                        blocked_by=[ref], gates=None, accept_line="", assessment_origin=op["key"])
                    op.update(result_ref=new.get("ref"), outcome="filed")
                    logs.append(("ENQUEUE", f"{new.get('ref')} — {op['title']}", op["queue"]))
        else:
            target = bug if op["kind"] == "summary" else next(
                (i for i in data["items"] if str(i.get("ref")) == op["target"]), None)
            if target is None:
                op.update(outcome="skipped")
            elif _has_marker(target, op["marker"]):
                op.update(result_ref=op.get("target"), outcome="skipped")
            else:
                text = op["text"] if op["kind"] == "summary" else (
                    f"{op['marker']} Post-fix assessment of {ref} ({op['point']}): {op['note']}")
                _append_history(target, "comment", by=_by("system"), at=now, text=text)
                target["updated_at"] = now
                op.update(result_ref=op.get("target"), outcome="commented")
        op["done"] = True
        bug["assessment"] = a
        bug["updated_at"] = now
        _save_unlocked(data)
    for verb, detail, qn in logs:
        _log(verb, detail, queue=qn)
    if hook:
        hook("after_commit", idx)
    return op


def assessment_finalize(ident: Any, token: str) -> Optional[Dict[str, Any]]:
    def _do(it, a, data):
        if a.get("status") != "filing" or a.get("token") != token:
            raise AssessmentFenced("assessment is no longer filing under this token")
        ops = a["pending"]["ops"]
        if not all(o.get("done") for o in ops):
            raise ValueError("not every assessment op is done; resume")
        files = [o["result_ref"] for o in ops if o["kind"] == "file" and o.get("result_ref")]
        a["followups"] = files
        a["existing"] = [o["target"] for o in ops if o["kind"] == "link"]
        a["status"] = "done"
        a["at"] = _now_iso()
        _append_history(it, "assessment", by=_by("system"), outcome="done",
                        text=f"{len(files)} follow-up(s) filed: {', '.join(files) or 'none'}")
    return _assessment_update(ident, _do)


def assessment_run_ops(ident: Any, token: str = "") -> Optional[Dict[str, Any]]:
    """Phases 2+3 for whatever is pending; token defaults to the stored one
    (resume). Raises AssessmentFenced when superseded."""
    item = get(ident)
    if item is None:
        return None
    a = item.get("assessment") or {}
    token = token or a.get("token", "")
    if a.get("status") != "filing":
        raise ValueError(f"{item.get('ref')} assessment is {a.get('status') or 'absent'}, not filing")
    for idx in range(len(a["pending"]["ops"])):
        _assessment_apply_op(ident, token, idx)
    return assessment_finalize(ident, token)


def assessment_targets_for_sweep(now_ts: Optional[float] = None) -> Dict[str, List[Dict[str, Any]]]:
    """Items the reconcile sweep should act on: ``due``, stale ``filing`` (>5
    min) and ``running`` older than 10 min (caller checks worker liveness)."""
    import time as _time
    now_ts = now_ts if now_ts is not None else _time.time()

    def age(iso: str) -> float:
        try:
            return now_ts - datetime.strptime(
                iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()
        except Exception:  # noqa: BLE001
            return 0.0

    res: Dict[str, List[Dict[str, Any]]] = {"due": [], "filing": [], "running": []}
    for it in list_items():
        a = it.get("assessment") or {}
        st = a.get("status")
        if st == "due":
            res["due"].append(it)
        elif st == "filing" and age(it.get("updated_at", "")) > 300:
            res["filing"].append(it)
        elif st == "running" and age(a.get("reserved_at", "")) > 600:
            res["running"].append(it)
    return res


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
    current = get(ident)
    if current is not None and current.get("status") == PARKED_STATUS:
        if not force:
            raise ValueError(
                f"{current.get('ref', ident)} is parked awaiting an answer: "
                f"{current.get('block_question') or '(no question recorded)'} "
                f"-- releasing would discard it. Use `wt answer "
                f"{current.get('ref', ident)} \"...\"`, or --force if the park is stale.")
        return update_status(ident, "open", session_id, require_status=PARKED_STATUS,
                             reason="released (forced)", answer_fate="discard")
    if not force:
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


def reopen(ident: Any, reason: str = "", session_id: str = "",
           force: bool = False) -> Optional[Dict[str, Any]]:
    """Reopen a closed or in-progress ticket, returning it to the open pool.

    This is the human triage verb (``wt reopen``) — the CCC reopen button's
    CLI equivalent. Unlike ``release`` it also applies to *closed* tickets,
    and unlike ``ready`` it does not mark the ticket run_requested or
    dispatch its queue: the ticket just goes back to open for the normal
    claim pool. On GitHub-backed projects the transition is mirrored to
    ``gh issue reopen``.

    Refuses a ticket that is currently ``needs_input`` unless ``force=True``:
    the reopen path clears ``needs_input``/``block_question`` (see
    ``update_status``), so reopening a blocked ticket silently erases the
    open question — same guard and rationale as ``release``; ``wt answer``
    is the intentional unblock.
    """
    current = get(ident)
    if current is None:
        return None
    if current.get("status") == "open":
        raise ValueError(
            f"{current.get('ref', ident)} is already open — nothing to reopen"
        )
    if current.get("status") == PARKED_STATUS:
        if not force:
            raise ValueError(
                f"{current.get('ref', ident)} is parked awaiting an answer: "
                f"{current.get('block_question') or '(no question recorded)'} "
                f"-- reopening would discard it. Use `wt answer "
                f"{current.get('ref', ident)} \"...\"`, or pass --force if the park is stale.")
        return update_status(ident, "open", session_id, reason=reason or "reopened (forced)",
                             by_kind="worker" if session_id else "human",
                             answer_fate="discard")
    if not force and current.get("needs_input"):
        raise ValueError(
            f"{current.get('ref', ident)} is blocked awaiting human input: "
            f"{current.get('block_question') or '(no question recorded)'} "
            f"-- reopening would erase that question and hand it to a fresh "
            f"worker with no memory of it. Use `wt answer "
            f"{current.get('ref', ident)} \"...\"` to resolve it, or pass "
            f"--force if the block is stale."
        )
    by_kind = "worker" if session_id else "human"
    return update_status(ident, "open", session_id, reason=reason or "reopened",
                         by_kind=by_kind)


def reopen_and_claim(
    ident: Any,
    session_id: str,
    session_uuid: str = "",
    reason: str = "",
    force: bool = False,
    sent_back_reason: str = "",
    sent_back_by: str = "",
    claim_proc: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """``sent_back_reason`` / ``sent_back_by`` (WT-34): mark the re-bound claim
    as sent back (see ``_sent_back_held_unlocked``), file store only.

    ``claim_proc`` (WT-31): the prior claim's process record; the re-bind
    inherits it (``bound: inherited``). Without it the claim binds fresh.

    Reopen a closed/blocked ticket and claim it under ``session_id`` in one
    lock acquisition -- the re-entry re-bind primitive for same-topic routing
    (CLIENT-CHAT-19). ``session_id``/``session_uuid`` should be the ticket's
    OWN preserved ``claimed_session_id`` (reopen preserves it; see ``reopen``'s
    docstring) so the transition re-binds the ticket to the exact session
    that last worked it, not a fresh claim.

    Why one lock instead of ``reopen()`` then ``claim_by_ref()``: those two
    calls each take and release ``_FileLock`` independently, leaving the
    ticket sitting ``open`` -- and therefore visible to any live drain
    worker's normal claim loop -- in the gap between them. A second worker
    can steal it in that window. This function never leaves the ticket in
    an externally-claimable state: closed/blocked goes straight to
    in_progress under the given session, atomically.

    Raises ``ValueError`` for the same reasons ``reopen()`` does (already
    open, or blocked without ``force``). Returns ``None`` if the ticket
    doesn't exist. GitHub-backed projects have no equivalent atomic
    transition available server-side, so they fall back to the sequential
    reopen + claim (same residual race ``reopen`` already had there; this
    function only closes the window for the local file-backed store)."""
    backend = _github_backend_for_project(_project_from_ident(ident))
    if backend is not None:
        current = get(ident)
        if current is None:
            return None
        reopened = reopen(ident, reason=reason, session_id=session_id, force=force)
        if reopened is None:
            return None
        return claim_by_ref(reopened.get("ref", ident), session_id, session_uuid=session_uuid)

    real_sid = _coerce_session_uuid(session_uuid) or _coerce_session_uuid(session_id)
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        for it in data["items"]:
            if not _matches(it, ident):
                continue
            if it.get("status") == "open":
                raise ValueError(
                    f"{it.get('ref', ident)} is already open — nothing to reopen"
                )
            if not force and it.get("needs_input"):
                raise ValueError(
                    f"{it.get('ref', ident)} is blocked awaiting human input: "
                    f"{it.get('block_question') or '(no question recorded)'} "
                    f"-- reopening would erase that question and hand it to a "
                    f"fresh worker with no memory of it. Use `wt answer "
                    f"{it.get('ref', ident)} \"...\"` to resolve it, or pass "
                    f"--force if the block is stale."
                )
            now = _now_iso()
            # Reopen half: same field resets as update_status's "open" branch
            # (parity with reopen()/GitHub's reopen), except status lands on
            # in_progress instead of open -- this ticket is never externally
            # claimable in between.
            it.pop("closed_by", None)
            it.pop("closed_machine", None)
            it.pop("resolution", None)
            it["needs_input"] = False
            it["block_question"] = ""
            it["block_kind"] = ""
            it["block_commit"] = ""
            it["blocked_at"] = None
            it["closed_at"] = None
            _append_history(
                it, "reopen",
                by=_by("worker" if session_id else "human", str(session_id or ""), str(real_sid or "")),
                at=now, reason=_clip(reason, 4000),
            )
            # Claim half: same fields claim_by_ref sets.
            it["status"] = "in_progress"
            it["claimed_by"] = str(session_id)
            it["claimed_machine"] = machine_tag()
            it.pop("resume", None)
            _clear_sent_back_unlocked(it)
            if real_sid:
                it["claimed_session_id"] = real_sid
            it["claimed_at"] = now
            it["updated_at"] = now
            it["claim_proc"] = (dict(claim_proc, bound="inherited") if claim_proc
                                else _claim_proc_for(session_id, real_sid or ""))
            _append_history(
                it, "claim",
                by=_by("worker", str(session_id), str(real_sid or "")),
                at=now,
            )
            if sent_back_reason:
                it["sent_back"] = {
                    "at": now, "by": str(sent_back_by or "human"),
                    "worker_id": str(session_id), "session_id": str(real_sid or ""),
                    "reason": _clip(sent_back_reason, 4000),
                    "hist_from": len(it["history"]),
                    "handed_back_at": None, "progress_at": None,
                }
            _save_unlocked(data)
            _log(
                "CLAIM",
                f"{it.get('ref', '?')} by {str(session_id)[:16]} (reopen+claim) — "
                f"{it.get('title') or it.get('note', '')[:60]}",
                queue=it.get("project", ""),
            )
            _notify_ticket_event(
                it, "claimed",
                actor=(it.get("claimed_by"), it.get("claimed_session_id")),
            )
            return it
    return None


RESOLUTION_LIST_FIELDS = ("caveats", "follow_ups", "unresolved")


def ack_resolution(
    ident: Any,
    targets: Any = None,
    all_items: bool = False,
    by: str = "human",
    session_id: str = "",
    undo: bool = False,
) -> Optional[Dict[str, Any]]:
    """Acknowledge (or un-acknowledge) individual resolution warnings.

    A closed ticket's ``caveats`` / ``follow_ups`` / ``unresolved`` entries
    render as coloured chips on the dashboard, and used to have exactly one
    removal path: ``wt close --force`` with a rebuilt resolution, which
    rewrites the close record and re-fires close notifications just to clear
    visual noise. An ack is the non-destructive alternative: the text stays
    exactly as the worker wrote it, and a parallel ``<field>_ack`` map records
    who acknowledged which entry and when, so the dashboard can dim it.

    ``targets`` is an iterable of ``(field, index)`` pairs with a 0-based
    index into that field's list; ``all_items=True`` acks every entry in every
    field. ``undo=True`` removes the acks instead. Idempotent: re-acking an
    already-acked entry refreshes nothing and re-unacking is a no-op.

    Returns the updated item, None when no item matches, and raises
    ``ValueError`` for a ticket with no resolution or an out-of-range index.

    Works the same on GitHub-backed queues, where the ack maps round-trip
    through the issue's metadata block instead of the local store."""
    backend = _github_backend_for_project(_project_from_ident(ident))
    if backend is not None:
        # GitHub-backed queues keep the same index-keyed ack maps in the
        # issue's metadata block (github_backend.ack_resolution).
        item = backend.ack_resolution(
            ident,
            targets=targets,
            all_items=all_items,
            by=by,
            session_id=session_id,
            undo=undo,
        )
        if item:
            _log(
                "UNACK" if undo else "ACK",
                f"{item.get('ref', ident)}",
                queue=item.get("project", ""),
            )
        return item
    actor_kind = by if by in ("worker", "human", "system") else "human"
    pairs = [] if targets is None else [(str(f), int(i)) for f, i in targets]
    for field, _idx in pairs:
        if field not in RESOLUTION_LIST_FIELDS:
            raise ValueError(
                f"unknown resolution field {field!r}; expected one of "
                f"{', '.join(RESOLUTION_LIST_FIELDS)}"
            )
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        for it in data["items"]:
            if not _matches(it, ident):
                continue
            res = it.get("resolution")
            if not isinstance(res, dict) or not any(
                res.get(f) for f in RESOLUTION_LIST_FIELDS
            ):
                raise ValueError(
                    f"{it.get('ref', ident)} has no caveat/follow-up/unresolved "
                    "items to acknowledge"
                )
            wanted = list(pairs)
            if all_items:
                wanted = [
                    (f, i)
                    for f in RESOLUTION_LIST_FIELDS
                    for i in range(len(res.get(f) or []))
                ]
            if not wanted:
                raise ValueError(
                    "nothing selected: pass all_items=True or at least one "
                    "(field, index) target"
                )
            for field, idx in wanted:
                n = len(res.get(field) or [])
                if idx < 0 or idx >= n:
                    raise ValueError(
                        f"{it.get('ref', ident)} has {n} {field} "
                        f"item{'' if n == 1 else 's'}; no index {idx + 1}"
                    )
            now = _now_iso()
            changed = []
            for field, idx in wanted:
                key = f"{field}_ack"
                acks = res.get(key)
                if not isinstance(acks, dict):
                    acks = {}
                if undo:
                    if acks.pop(str(idx), None) is not None:
                        changed.append((field, idx))
                elif str(idx) not in acks:
                    acks[str(idx)] = {"at": now, "by": _clip(str(by or "human"), 128)}
                    changed.append((field, idx))
                if acks:
                    res[key] = acks
                else:
                    res.pop(key, None)
            if not changed:
                return it
            it["resolution"] = res
            it["updated_at"] = now
            detail = ", ".join(f"{f}#{i + 1}" for f, i in changed)
            _append_history(
                it,
                "ack" if not undo else "unack",
                by=_by(actor_kind, str(by or ""), str(session_id or "")),
                at=now,
                text=detail,
            )
            _save_unlocked(data)
            _log(
                "UNACK" if undo else "ACK",
                f"{it.get('ref', '?')} — {detail}",
                queue=it.get("project", ""),
            )
            return it
    return None


def is_acked(res: Dict[str, Any], field: str, index: int) -> bool:
    """True when entry ``index`` of ``res[field]`` has been acknowledged."""
    acks = (res or {}).get(f"{field}_ack")
    return isinstance(acks, dict) and str(index) in acks


# --- Parked blocks (WT-28) ---------------------------------------------------
# A worker `wt block` on the local store parks the ticket: it leaves the
# worker's claim (``awaiting_answer`` + ``parked`` = where the answer goes) so
# the worker can take the next ticket and a dead worker leaves no zombie claim.
# `wt answer` writes ``pending_answer`` (keyed by ``gen``); every routing step
# is a compare-and-swap on (gen, state, status) inside the store lock, so a
# stale router can never deliver or rebind a ticket whose state moved on.

def _iso_ts(value: Any) -> float:
    try:
        return datetime.fromisoformat(str(value or "").replace("Z", "+00:00")).timestamp()
    except Exception:  # noqa: BLE001
        return 0.0


def _raise_claim_refused(held: List[Dict[str, Any]], session_id: str,
                         sent_back: Optional[Dict[str, Any]] = None) -> None:
    refs = ", ".join(str(it.get("ref") or "?") for it in held)
    ref0 = str(held[0].get("ref") or "<ref>")
    sb_tail = ""
    if sent_back is not None:
        sb_tail = (f"\nthen fix {sent_back.get('ref')} (sent back "
                   f"{_age_min((sent_back.get('sent_back') or {}).get('at'))}m ago) "
                   "before anything new")
    raise ValueError(
        f"claim refused: you still hold {refs} in_progress on this queue. "
        "Committing a fix does not finish a ticket -- record its outcome "
        "first, then claim again:\n"
        f"  wt close {ref0} --worker {session_id} --summary \"...\" "
        "--commit <SHA>   (--no-code if nothing was committed)\n"
        f"  wt block {ref0} --worker {session_id} --question \"...\" "
        "--progress \"...\"   (needs a human decision)" + sb_tail
    )


def _held_unlocked(items: List[Dict[str, Any]], worker_id: str, session_id: str = "",
                   project: Optional[str] = None) -> List[Dict[str, Any]]:
    """Active (non-blocked) in_progress claims held by this worker id or
    session, evaluated on the in-lock item list."""
    out = []
    for it in items:
        if it.get("status") != "in_progress" or it.get("needs_input"):
            continue
        if project and it.get("project") != project:
            continue
        if ((worker_id and str(it.get("claimed_by") or "") == worker_id)
                or (session_id and str(it.get("claimed_session_id") or "") == session_id)):
            out.append(it)
    return out


# --- Sent-back claims (WT-34) ------------------------------------------------
# A verifier/human rejection re-binds the ticket to the session that built it
# (reopen_and_claim). ``sent_back`` marks that claim so the claim gate hands it
# back before anything new, and so a silent holder loses it after N minutes.
# State table: WT-31 row work.sent_back (sent_back in none|held|progressing).
SENT_BACK_PROGRESS_EVENTS = ("comment", "progress", "block", "park", "in_review")
SENT_BACK_FAILED_RESUME_GRACE_S = 300.0


def _clear_sent_back_unlocked(it: Dict[str, Any]) -> None:
    it.pop("sent_back", None)


def _sent_back_held_unlocked(items: List[Dict[str, Any]], worker_id: str,
                             session_id: str = "",
                             project: Optional[str] = None) -> List[Dict[str, Any]]:
    """in_progress sent-back claims this worker id OR session holds, oldest
    first (in-lock item list)."""
    out = []
    for it in items:
        sb = it.get("sent_back")
        if not isinstance(sb, dict) or it.get("status") != "in_progress":
            continue
        if project and it.get("project") != project:
            continue
        if ((worker_id and str(it.get("claimed_by") or "") == worker_id)
                or (session_id and (str(it.get("claimed_session_id") or "") == session_id
                                    or str(sb.get("session_id") or "") == session_id))):
            out.append(it)
    out.sort(key=lambda it: _iso_ts((it.get("sent_back") or {}).get("at")))
    return out


def _sent_back_progress(it: Dict[str, Any]) -> str:
    """``at`` of the first claimer event after the send-back ('' = none yet)."""
    sb = it.get("sent_back") or {}
    ids = {str(x) for x in (sb.get("worker_id"), sb.get("session_id"),
                            it.get("claimed_by"), it.get("claimed_session_id")) if x}
    hist = it.get("history") or []
    start = int(sb.get("hist_from") or 0)
    if len(hist) < start:
        floor = _iso_ts(sb.get("at"))
        cand = [h for h in hist if _iso_ts(h.get("at")) >= floor]
    else:
        cand = hist[start:]
    for h in cand:
        if h.get("event") not in SENT_BACK_PROGRESS_EVENTS:
            continue
        by = h.get("by") or {}
        if str(by.get("worker") or "") in ids or str(by.get("session_id") or "") in ids:
            return str(h.get("at") or "")
    return ""


def _sent_back_deadline(it: Dict[str, Any], minutes: int) -> float:
    """Epoch deadline after which a silent sent-back claim is released (0 = never)."""
    if minutes <= 0:
        return 0.0
    sb = it.get("sent_back") or {}
    dl = _iso_ts(sb.get("handed_back_at") or sb.get("at")) + minutes * 60.0
    r = it.get("resume")
    if isinstance(r, dict) and r.get("state") == "failed":
        dl = min(dl, _iso_ts(r.get("at")) + SENT_BACK_FAILED_RESUME_GRACE_S)
    return dl


def _sent_back_minutes(it: Dict[str, Any]) -> int:
    try:
        from . import config as _config
        return _config.sent_back_release_min(str(it.get("project") or ""))
    except Exception:  # noqa: BLE001
        return 30


def _age_min(iso: Any, now: Optional[float] = None) -> int:
    return max(0, int(((now if now is not None else time.time()) - _iso_ts(iso)) // 60))


def _hhmmz(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).strftime("%H:%MZ")


def _raise_sent_back_refused(sb_item: Dict[str, Any], session_id: str) -> None:
    sb = sb_item.get("sent_back") or {}
    ref = str(sb_item.get("ref") or "?")
    dl = _sent_back_deadline(sb_item, _sent_back_minutes(sb_item))
    tail = (f" It is released to the pool at {_hhmmz(dl)} if you show no progress "
            "(a `wt comment` counts)." if dl else "")
    raise ValueError(
        f"claim refused: {ref} was sent back to you {_age_min(sb.get('at'))}m ago by "
        f"{sb.get('by') or '?'} and still needs fixing first: "
        f"{_clip(sb.get('reason'), 300)}\n"
        f"  fix it, then `wt close {ref} --worker {session_id} --summary \"...\" "
        f"--commit <SHA>`.{tail}"
    )


def sent_back_claims(project: Optional[str] = None) -> List[Dict[str, Any]]:
    """Live sent-back claims still waiting on their holder (no progress yet).
    Stamps ``progress_at`` the first time progress is seen. File store only."""
    proj = _norm_project(project) if project else None
    out: List[Dict[str, Any]] = []
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        dirty = False
        for it in data["items"]:
            sb = it.get("sent_back")
            if (not isinstance(sb, dict) or it.get("status") != "in_progress"
                    or it.get("needs_input")):
                continue
            if proj and it.get("project") != proj:
                continue
            if sb.get("progress_at"):
                continue
            prog = _sent_back_progress(it)
            if prog:
                sb["progress_at"] = prog
                dirty = True
                continue
            out.append(dict(it))
        if dirty:
            _save_unlocked(data)
    return out


def release_stalled_sent_back(now: Optional[float] = None) -> List[Dict[str, Any]]:
    """Release sent-back claims with no progress past their deadline to the pool,
    keeping the rejection text. One lock; CAS on status + unchanged marker."""
    now_ts = time.time() if now is None else float(now)
    released: List[Dict[str, Any]] = []
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        dirty = False
        for it in data["items"]:
            sb = it.get("sent_back")
            if (not isinstance(sb, dict) or it.get("status") != "in_progress"
                    or it.get("needs_input")):
                continue
            if sb.get("progress_at"):
                continue
            prog = _sent_back_progress(it)
            if prog:
                sb["progress_at"] = prog
                dirty = True
                continue
            dl = _sent_back_deadline(it, _sent_back_minutes(it))
            if not dl or now_ts < dl:
                continue
            at = _now_iso()
            reason = (f"sent back {_age_min(sb.get('at'), now_ts)}m ago with no progress; "
                      f"released from {it.get('claimed_by') or sb.get('worker_id') or '?'}")
            it["sent_back_released"] = {
                "worker_id": str(sb.get("worker_id") or it.get("claimed_by") or ""),
                "session_id": str(sb.get("session_id") or it.get("claimed_session_id") or ""),
                "sent_back_at": sb.get("at"), "released_at": at,
                "by": sb.get("by"), "reason": sb.get("reason") or "",
            }
            it["status"] = "open"
            it["claimed_by"] = None
            it["claimed_machine"] = None
            it["claimed_at"] = None
            it.pop("claimed_session_id", None)  # identity lives in sent_back_released
            it["updated_at"] = at
            _drop_claim_proc_unlocked(it)
            it.pop("resume", None)
            it.pop("sent_back", None)
            _append_history(it, "sent_back_release", by=_by("system"), at=at, reason=reason)
            _append_history(it, "comment", by=_by("system"), at=at,
                            text=f"Released to the pool: {reason}. Rejection: "
                                 f"{_clip(it['sent_back_released']['reason'], 1500)}")
            _log("RELEASE", f"{it.get('ref', '?')} — {reason}", queue=it.get("project", ""))
            released.append(dict(it))
            dirty = True
        if dirty:
            _save_unlocked(data)
    return released


def _close_owner_guard_unlocked(it: Dict[str, Any], ident: Any, owner: str,
                                real_sid: str) -> None:
    """Close-ownership checks, run in the lock on every mutating outcome of a
    worker-attributed close (the writeback AND the failed-gate reopen)."""
    if not owner:
        return
    status = it.get("status")
    if status == "closed":
        ref_label = it.get("ref", ident)
        closer = str(it.get("closed_by") or it.get("claimed_by") or "?")
        when = it.get("closed_at") or "?"
        raise ValueError(
            f"{ref_label} is already closed (by {closer} at {when}). "
            f"You are {owner} — you were likely reaped mid-ticket "
            f"and it was re-drained by another worker. Your work may "
            f"duplicate theirs: do NOT re-commit; run `wt find {ref_label} "
            f"--json` to compare. Pass --force to close anyway."
        )
    if status == "in_progress" and it.get("claimed_by") and str(it.get("claimed_by")) != owner:
        raise ValueError(
            f"{it.get('ref', ident)} is claimed by {it.get('claimed_by')}; "
            f"you are {owner}. Only the claiming worker may close "
            "an in-progress ticket. Pass --force to override deliberately."
        )
    if (status == PARKED_STATUS and (it.get("parked") or {}).get("worker_id")
            and str(it["parked"]["worker_id"]) != owner):
        raise ValueError(
            f"{it.get('ref', ident)} is parked by {it['parked']['worker_id']}; "
            f"you are {owner}. Only that worker may close it. "
            "Pass --force to override deliberately."
        )
    rel = it.get("sent_back_released")
    if (status == "open" and not it.get("claimed_by") and isinstance(rel, dict)
            and ((rel.get("worker_id") and str(rel["worker_id"]) == owner)
                 or (real_sid and str(rel.get("session_id") or "") == real_sid))):
        ref_label = it.get("ref", ident)
        raise ValueError(
            f"{ref_label} was released to the pool at {rel.get('released_at')} "
            f"after {owner} showed no progress on the sent-back work. Run "
            f"`wt claim {ref_label} --worker {owner}` to take it back; do not "
            "close it without claiming. Pass --force to override deliberately."
        )


# --- Claim process binding (WT-31) --------------------------------------------
# ``claim_proc`` records, in the claim's own lock, which process the claim is
# bound to, so liveness.claim_owner can prove the claimant dead from the
# stored pid / start token / exit file even after workers.json prunes the
# record. ``bound``: ``record`` (a WT worker record), ``ambient`` (no record:
# never judged dead), ``inherited`` (a re-bind carrying the prior process).

def _claim_proc_from_record(rec: Dict[str, Any], session_id: str, bound: str) -> Dict[str, Any]:
    return {"worker_id": str(rec.get("worker_id") or ""),
            "session_id": str(session_id or rec.get("session_id") or ""),
            "engine": str(rec.get("engine") or ""), "pid": int(rec.get("pid") or 0),
            "pid_started": str(rec.get("pid_started") or ""),
            "exit_file": str(rec.get("exit_file") or ""),
            "record_started_at": str(rec.get("started_at") or ""), "bound": bound}


def _claim_proc_for(claimer: Any, session_id: Any = "") -> Dict[str, Any]:
    """A fresh ``claim_proc`` for ``claimer`` (worker id, or a session uuid
    mapped through the origin ledger)."""
    claimer, sid = str(claimer or ""), str(session_id or "")
    try:
        from . import workers as _workers
        rows = [w for w in _workers._load().get("workers", []) if isinstance(w, dict)]
    except Exception:  # noqa: BLE001
        rows = []
    wid = claimer
    if claimer and not any(str(w.get("worker_id") or "") == claimer for w in rows):
        try:
            from . import origins as _origins
            wid = str((_origins.get(claimer) or {}).get("worker_id") or "") or claimer
        except Exception:  # noqa: BLE001
            pass
    rec = next((w for w in reversed(rows) if wid and str(w.get("worker_id") or "") == wid), None)
    if rec is None and _coerce_session_uuid(claimer):
        rec = next((w for w in reversed(rows) if str(w.get("session_id") or "") == claimer), None)
    if rec is None:
        return {"worker_id": wid, "session_id": sid, "engine": "", "pid": 0,
                "pid_started": "", "exit_file": "", "record_started_at": "",
                "bound": "ambient"}
    return _claim_proc_from_record(rec, sid, "record")


def _inheritable_proc(it: Dict[str, Any], session_id: str) -> Optional[Dict[str, Any]]:
    """The ticket's current (else last) claim process, when it belongs to
    ``session_id`` -- what a re-bind to that session inherits."""
    for key in ("claim_proc", "prior_claim_proc"):
        cp = it.get(key)
        if isinstance(cp, dict) and (not session_id or not cp.get("session_id")
                                     or str(cp["session_id"]) == str(session_id)):
            return dict(cp)
    return None


def _drop_claim_proc_unlocked(it: Dict[str, Any]) -> None:
    """The claim ended: keep its process as ``prior_claim_proc``."""
    cp = it.pop("claim_proc", None)
    if cp:
        it["prior_claim_proc"] = cp


def _affinity_reserved(it: Dict[str, Any], now: float) -> bool:
    pa = it.get("pending_answer")
    return bool(pa and pa.get("state") == "affinity"
                and _iso_ts(pa.get("affinity_until")) > now)


def _affinity_owner(it: Dict[str, Any], worker_id: str, real_sid: str) -> bool:
    pa = it.get("pending_answer") or {}
    return bool((worker_id and worker_id == str(pa.get("prior_worker_id") or ""))
                or (real_sid and real_sid == str(pa.get("prior_session_id") or "")))


def _affinity_gate_unlocked(it: Dict[str, Any], worker_id: str, real_sid: str,
                            now: float) -> str:
    """"" when this claimer may take ``it``; else why it is reserved."""
    if not _affinity_reserved(it, now) or _affinity_owner(it, worker_id, real_sid):
        return ""
    pa = it["pending_answer"]
    until = datetime.fromtimestamp(_iso_ts(pa.get("affinity_until")), timezone.utc).strftime("%H:%M")
    return (f"{it.get('ref', '?')} is reserved for {pa.get('prior_worker_id') or 'its parked worker'}'s "
            f"answer until {until}Z")


# --- Answer transitions (WT-31 D1b) ------------------------------------------
# Every change of ``pending_answer.state`` is one declared edge: (from states)
# -> to state, with the status moves it may make and the functions allowed to
# write it. ``none`` = no record. tests/test_liveness_table.py scans the code
# (every writer and wrapper call site) against this table, and ``_pa_cas``
# checks each CAS step at runtime. Adding an edge: docs/worker-lifecycle.md.
_ANY_PA = _PA_STORED


def _same(*statuses: str) -> Tuple[Tuple[str, str], ...]:
    return tuple((s, s) for s in statuses)


ANSWER_TRANSITIONS: Tuple[Dict[str, Any], ...] = (
    {"id": "E1", "from": ("none",), "to": "routing",
     "status": _same("awaiting_answer"),
     "writers": ("_write_pending_answer_unlocked",), "callers": ("answer",)},
    {"id": "E2", "from": ("routing",), "to": "routing",
     "status": _same("awaiting_answer"), "writers": ("route_answer",)},
    {"id": "E3", "from": ("routing",), "to": "delivering",
     "status": (("awaiting_answer", "in_progress"),),
     "writers": ("resume_claim",), "callers": ("route_answer",)},
    {"id": "E4", "from": ("routing",), "to": "affinity",
     "status": (("awaiting_answer", "open"),), "writers": ("route_answer",)},
    {"id": "E5", "from": ("routing",), "to": "handed_off",
     "status": (("awaiting_answer", "open"),),
     "writers": ("route_answer", "_fallback_reopen"),
     "callers": ("route_pending_answers",)},
    {"id": "E6", "from": ("delivering",), "to": "queued",
     "status": _same("in_progress"), "writers": ("_deliver_bound",),
     "callers": ("route_answer", "_retry", "_check_queued")},
    {"id": "E7", "from": ("queued",), "to": "delivering",
     "status": _same("in_progress"), "writers": ("_check_queued",),
     "callers": ("route_pending_answers",)},
    {"id": "E8", "from": ("delivering", "queued"), "to": "=",   # attempts self-loop
     "status": _same("in_progress"), "writers": ("_retry", "_check_queued"),
     "callers": ("route_pending_answers",)},
    {"id": "E9", "from": ("delivering", "queued"), "to": "delivered",
     "status": _same("in_progress"), "writers": ("_on_answer_confirmed",)},
    {"id": "E10", "from": ("delivering", "queued"), "to": "handed_off",
     "status": (("in_progress", "open"),), "writers": ("_fallback_reopen",),
     "callers": ("route_answer", "route_pending_answers", "_deliver_bound",
                 "_retry", "_check_queued")},
    {"id": "E11", "from": ("affinity",), "to": "handed_off",
     "status": _same("open"), "writers": ("route_pending_answers",)},
    {"id": "E12", "from": ("affinity", "handed_off"), "to": "delivered",
     "status": (("open", "in_progress"),), "writers": ("_bind_pending_unlocked",),
     "callers": ("claim_next", "claim_by_ref")},
    {"id": "E13", "from": ("handed_off",), "to": "delivered",
     "status": _same("open", "in_progress"), "writers": ("_confirm_stage_answer",)},
    # Handoff reopen (update_status -> open): release, reopen, the orphan sweep,
    # a failed-gate close. ``routing`` (parked owner's failed-gate close) and
    # ``affinity`` (failed-gate close of an open ticket) reach it too.
    {"id": "E14", "from": ("routing", "delivering", "queued", "delivered", "affinity"),
     "to": "handed_off",
     "status": (("in_progress", "open"), ("awaiting_answer", "open"), ("open", "open")),
     "writers": ("_reopen_pending_unlocked",),
     "callers": ("update_status", "release", "reopen", "requeue_orphaned_tickets",
                 "_reopen_with_feedback", "close")},
    {"id": "E15", "from": _ANY_PA, "to": "none",
     "status": (("in_progress", "open"), ("awaiting_answer", "open")),
     "writers": ("_reopen_pending_unlocked",),
     "callers": ("update_status", "release", "reopen")},
    {"id": "E16", "from": _ANY_PA, "to": "none",
     "status": tuple((s, t) for s in ("open", "in_progress", "awaiting_answer")
                     for t in ("closed", "in_review")),
     "writers": ("_clear_parked_unlocked",), "callers": ("update_status", "close")},
    # Any block supersedes the record (carried to the next one); a worker block
    # parks unless the plan gate is active (then open -> in_progress, R6-1).
    {"id": "E17", "from": _ANY_PA, "to": "none",
     "status": (("open", "in_progress"), ("open", "awaiting_answer"), ("open", "open"),
                ("in_progress", "in_progress"), ("in_progress", "awaiting_answer"),
                ("awaiting_answer", "awaiting_answer")),
     "writers": ("_supersede_pending_unlocked",), "callers": ("block", "update")},
)


def answer_edges(edge_id: Optional[str] = None) -> set:
    """Expanded ``(old_state, new_state, old_status, new_status)`` tuples of one
    declared edge (or of all of them)."""
    out = set()
    for e in ANSWER_TRANSITIONS:
        if edge_id is not None and e["id"] != edge_id:
            continue
        for old in e["from"]:
            new = old if e["to"] == "=" else e["to"]
            for fs, ts in e["status"]:
                out.add((old, new, fs, ts))
    return out


_ANSWER_EDGE_SET = answer_edges()


class UndeclaredEdge(RuntimeError):
    """A ``pending_answer`` step that ANSWER_TRANSITIONS does not declare."""


def _check_answer_edge(it: Dict[str, Any], edge: Tuple[str, str, str, str]) -> None:
    """Runtime half of the transition check: strict (tests,
    ``WATCHTOWER_STRICT_EDGES=1``) raises before the write; production logs
    ANSWER_EDGE_UNDECLARED and lets the write stand."""
    if edge in _ANSWER_EDGE_SET:
        return
    detail = (f"{it.get('ref', '?')} {edge[0]}->{edge[1]} "
              f"status {edge[2]}->{edge[3]}")
    if os.environ.get("WATCHTOWER_STRICT_EDGES") == "1":
        raise UndeclaredEdge(f"undeclared answer transition: {detail}")
    _log("ANSWER_EDGE_UNDECLARED", detail, queue=str(it.get("project") or ""))


def _plan_active_unlocked(it: Dict[str, Any]) -> bool:
    """PLAN_ACTIVE (WT-31 D1a): plan-gated and the plan has not settled. The
    plan stage owns the ticket, so an answer goes to it, not to a parked
    session. Same predicate as ``plan_pending``."""
    return plan_pending(it)


def _pa_set_state(it: Dict[str, Any], pa: Dict[str, Any], state: str, now: str) -> None:
    if state not in _PA_STORED:
        raise ValueError(f"not a storable answer state: {state!r}")
    pa["state"] = state
    pa["state_at"] = now
    if state in ("delivered", "handed_off") and it.get("carried_answers"):
        pa["carried"] = list(it.pop("carried_answers"))


def _bind_pending_unlocked(it: Dict[str, Any], worker_id: str, real_sid: str, now: str) -> None:
    """A successful claim of a ticket carrying an answer: the answer is in the
    claim output itself, so the record is settled as delivered."""
    pa = it.get("pending_answer")
    if not pa or pa.get("state") == "delivered":
        return
    _pa_set_state(it, pa, "delivered", now)
    pa["route"] = "claim"
    pa["claimed_by"] = worker_id
    _append_history(it, "answer_route", by=_by("system"), at=now, gen=pa.get("gen"),
                    route="claim", reason=f"claimed by {worker_id}")


def _reopen_pending_unlocked(it: Dict[str, Any], now: str, fate: str, reason: str) -> None:
    pa = it.get("pending_answer")
    if not pa:
        return
    if fate == "discard":
        _append_history(it, "answer_discarded", by=_by("system"), at=now, gen=pa.get("gen"),
                        text=_clip(str(pa.get("answer") or ""), 4000))
        it.pop("pending_answer", None)
        it.pop("carried_answers", None)
    elif pa.get("state") != "handed_off":
        _pa_set_state(it, pa, "handed_off", now)
        pa["route"] = pa.get("route") or "reopen"
        pa["reason"] = _clip(reason or pa.get("reason") or "", 500)


def _clear_parked_unlocked(it: Dict[str, Any]) -> None:
    it.pop("parked", None)
    it.pop("pending_answer", None)   # E16
    it.pop("carried_answers", None)


def _supersede_outbox(ref: Any) -> None:
    """Cancel already-enqueued answer rows for ``ref`` (R4-4). Best effort and
    outside the store lock; the drain-time gen check is the real guard, and a
    transport already started cannot be recalled (at-least-once)."""
    if not ref:
        return
    try:
        from . import messages
        messages.outbox_cancel_ticket(str(ref), "ticket answer superseded/closed")
    except Exception:  # noqa: BLE001
        pass


def _supersede_pending_unlocked(it: Dict[str, Any], now: str) -> Optional[int]:
    """Any block on a ticket carrying ``pending_answer`` supersedes it in the
    same lock, so a router holding the old gen fails its CAS (R4-2)."""
    pa = it.get("pending_answer")
    if not pa:
        return None
    gen = pa.get("gen")
    carried = list(it.get("carried_answers") or [])
    carried += list(pa.get("carried") or [])
    carried.append({"gen": gen, "question": pa.get("question", ""),
                    "answer": pa.get("answer", "")})
    it["carried_answers"] = carried[-10:]
    it.pop("pending_answer", None)
    _append_history(it, "superseded", by=_by("system"), at=now, gen=gen,
                    state=pa.get("state"))
    return int(gen) if gen is not None else None


def answer_session(it: Dict[str, Any]) -> str:
    """The session an answer for ``it`` belongs to: the parked one when the
    ticket is parked/routing, else the claimant's."""
    parked = it.get("parked") or {}
    pa = it.get("pending_answer") or {}
    return str(parked.get("session_id") or pa.get("prior_session_id")
               or it.get("claimed_session_id") or "")


def _engine_and_transcript(worker_id: str, session_id: str) -> Tuple[str, str]:
    try:
        from . import answers
        return answers.engine_for(worker_id, session_id), answers.transcript_for(session_id)
    except Exception:  # noqa: BLE001
        return "", ""


def _park_unlocked(it: Dict[str, Any], worker_id: str, session_id: str, machine: str,
                   now: str, actor: Any, reason: str = "") -> None:
    engine, transcript = _engine_and_transcript(worker_id, session_id)
    # WT-31: the parked session's process travels with the park (resume_claim
    # inherits it when that worker is gone).
    proc = it.pop("claim_proc", None) or _claim_proc_for(worker_id, session_id)
    it["parked"] = {
        "worker_id": worker_id, "session_id": session_id, "machine": machine,
        "engine": engine, "repo_path": it.get("repo_path") or "",
        "transcript_path": transcript, "at": now, "proc": proc,
    }
    it["status"] = PARKED_STATUS
    it.pop("resume", None)
    _clear_sent_back_unlocked(it)
    it["claimed_by"] = None
    it["claimed_session_id"] = None
    it["claimed_machine"] = None
    it["claimed_at"] = None
    _append_history(it, "park", by=actor, at=now, worker=worker_id, session=session_id,
                    reason=reason)


def _write_pending_answer_unlocked(it: Dict[str, Any], text: str, now: str) -> Dict[str, Any]:
    """Answer on a parked ticket: persist ``pending_answer`` (state routing)."""
    parked = it.get("parked") or {}
    gen = int(it.get("last_answer_gen") or 0) + 1
    it["last_answer_gen"] = gen
    it["pending_answer"] = {
        "gen": gen, "state": "routing", "question": it.get("block_question", ""),
        "answer": _clip(text, 24000), "prior_worker_id": parked.get("worker_id", ""),
        "prior_session_id": parked.get("session_id", ""),
        "prior_engine": parked.get("engine", ""),
        "transcript_path": parked.get("transcript_path", ""),
        "repo_path": parked.get("repo_path", ""),
        "route": "", "reason": "", "attempts": 0, "state_at": now,
    }
    return it["pending_answer"]


def _pa_cas(ident: Any, gen: int, expect_state: Any, expect_status: Any, mutate) -> Optional[Dict[str, Any]]:
    """Compare-and-swap on a ticket's ``pending_answer``: inside the store
    lock, apply ``mutate(item, pa)`` only when the record still has this
    ``gen``, one of ``expect_state`` and one of ``expect_status``. Returns the
    item on success, None when the CAS failed (the caller stops: no delivery,
    no write)."""
    states = (expect_state,) if isinstance(expect_state, str) else tuple(expect_state)
    statuses = (expect_status,) if isinstance(expect_status, str) else tuple(expect_status)
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        for it in data["items"]:
            if not _matches(it, ident):
                continue
            pa = it.get("pending_answer")
            if (not pa or int(pa.get("gen") or -1) != int(gen)
                    or pa.get("state") not in states or it.get("status") not in statuses):
                return None
            old = (str(pa.get("state")), str(it.get("status")))
            mutate(it, pa)
            new_pa = it.get("pending_answer")
            _check_answer_edge(it, (old[0], str((new_pa or {}).get("state") or "none"),
                                    old[1], str(it.get("status"))))
            it["updated_at"] = _now_iso()
            _save_unlocked(data)
            return it
    return None


def _resume_proc(parked: Dict[str, Any]) -> Dict[str, Any]:
    """WT-31: a resume binds fresh when the parked worker's process is still
    alive, else inherits the process it parked with."""
    fresh = _claim_proc_for(parked.get("worker_id") or "", parked.get("session_id") or "")
    prior = parked.get("proc")
    if fresh.get("bound") == "record":
        try:
            from . import workers as _workers
            if _workers.record_liveness(fresh)[0] == "alive":
                return fresh
        except Exception:  # noqa: BLE001
            pass
    return dict(prior, bound="inherited") if prior else fresh


def resume_claim(ident: Any, gen: int) -> Optional[Dict[str, Any]]:
    """CAS ``awaiting_answer`` -> ``in_progress`` bound to the parked worker
    and session (state ``delivering``). Refuses, in the same lock, when that
    worker or session already holds an active claim (no double claim)."""
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        for it in data["items"]:
            if not _matches(it, ident):
                continue
            pa = it.get("pending_answer")
            parked = it.get("parked") or {}
            if (not pa or int(pa.get("gen") or -1) != int(gen) or pa.get("state") != "routing"
                    or it.get("status") != PARKED_STATUS or not parked):
                return None
            if _held_unlocked(data["items"], str(parked.get("worker_id") or ""),
                              str(parked.get("session_id") or ""), it.get("project")):
                return None
            now = _now_iso()
            it["status"] = "in_progress"
            it["claimed_by"] = parked.get("worker_id") or None
            it["claimed_session_id"] = parked.get("session_id") or None
            it["claimed_machine"] = parked.get("machine") or machine_tag()
            it["claimed_at"] = now
            it["updated_at"] = now
            it["claim_proc"] = _resume_proc(parked)
            it.pop("parked", None)
            _pa_set_state(it, pa, "delivering", now)
            pa["route"] = "resume"
            _append_history(it, "answer_route", by=_by("system"), at=now, gen=gen,
                            route="resume", reason="parked session resumed")
            _save_unlocked(data)
            return it
    return None


def _release_claim_to_open_unlocked(it: Dict[str, Any], now: str, reason: str) -> None:
    """Back to the claim pool as part of an answer handoff (keeps the
    session handle, drops the claim and the block)."""
    it["status"] = "open"
    it["claimed_by"] = None
    it["claimed_machine"] = None
    it["claimed_at"] = None
    _drop_claim_proc_unlocked(it)
    it["needs_input"] = False
    it["block_question"] = ""
    it["block_kind"] = ""
    it["block_commit"] = ""
    it["blocked_at"] = None
    it.pop("parked", None)
    _append_history(it, "reopen", by=_by("system"), at=now, reason=_clip(reason, 500))


def pa_transition(ident: Any, gen: int, from_state: Any, to_state: str, *,
                  from_status: Any, reopen: bool = False, route: str = "",
                  reason: str = "", fields: Optional[Dict[str, Any]] = None,
                  ) -> Optional[Dict[str, Any]]:
    """One routing step as a CAS (see ``_pa_cas``): move the record to
    ``to_state``; ``reopen`` also returns the ticket to the claim pool. None
    when the CAS lost (the caller stops without delivering)."""
    def _mut(it, pa):
        now = _now_iso()
        if reopen:
            _release_claim_to_open_unlocked(it, now, reason or "answer handoff")
        _pa_set_state(it, pa, to_state, now)
        if route:
            pa["route"] = route
        if reason:
            pa["reason"] = _clip(reason, 500)
        pa.update(fields or {})
        _append_history(it, "answer_route", by=_by("system"), at=now, gen=gen,
                        route=route or pa.get("route", ""), reason=_clip(reason, 500),
                        state=to_state)
    return _pa_cas(ident, gen, from_state, from_status, _mut)


def mark_park_retention_expired(ident: Any) -> bool:
    """Stamp ``parked.retention_expired_at`` once (PARK-EXPIRED is logged only
    when this returns True)."""
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        for it in data["items"]:
            if _matches(it, ident):
                pk = it.get("parked")
                if it.get("status") != PARKED_STATUS or not pk or pk.get("retention_expired_at"):
                    return False
                pk["retention_expired_at"] = _now_iso()
                _save_unlocked(data)
                return True
    return False


def pa_bump_attempts(ident: Any, gen: int, state: str) -> Optional[Dict[str, Any]]:
    """Count one delivery attempt (CAS on ``state``)."""
    def _mut(it, pa):
        pa["attempts"] = int(pa.get("attempts") or 0) + 1
        pa["state_at"] = _now_iso()
    return _pa_cas(ident, gen, state, ("in_progress",), _mut)


def migrate_legacy_blocks(queue: Optional[str] = None, dry_run: bool = False) -> List[str]:
    """Park tickets blocked the old way (``in_progress`` + ``needs_input`` held
    by a worker). Local queues only; idempotent. Returns the refs selected."""
    proj = _norm_project(queue) if queue else None
    picked: List[str] = []
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        github = set(_github_projects())
        for it in data["items"]:
            if it.get("status") != "in_progress" or not it.get("needs_input"):
                continue
            if proj and it.get("project") != proj:
                continue
            if _norm_project(it.get("project") or "") in github:
                continue
            claimant = str(it.get("claimed_by") or "")
            if not claimant or (it.get("stage_session") or {}).get("escalated"):
                continue
            if (it.get("plan") or {}).get("status") == "blocked":
                continue
            last = next((h for h in reversed(it.get("history") or [])
                         if h.get("event") == "block"), None)
            if not last or (last.get("by") or {}).get("kind") != "worker":
                continue
            picked.append(str(it.get("ref")))
            if dry_run:
                continue
            _park_unlocked(it, claimant, str(it.get("claimed_session_id") or ""),
                           str(it.get("claimed_machine") or ""),
                           str(it.get("blocked_at") or _now_iso()), _by("system"),
                           reason="migrated_legacy_block")
        if picked and not dry_run:
            _save_unlocked(data)
    return picked


def block(
    ident: Any,
    session_id: str = "",
    question: str = "",
    progress: str = "",
    kind: str = "input",
    commit: str = "",
    origin: str = "system",
) -> Optional[Dict[str, Any]]:
    """Park a ticket that needs a human decision.

    ``origin`` (WT-28): ``"worker"`` (``wt block``) on the local store PARKS
    the ticket -- status ``awaiting_answer``, claim released, ``parked``
    records the session the answer returns to -- so the worker can take the
    next ticket. Plan-gate / stage-escalation / verifier blocks and GitHub
    queues keep the legacy behaviour below.

    The legacy path: the ticket STAYS ``in_progress`` bound to its session (so ``claim_next``,
    which only picks ``open``, can never hand it to another worker) and is flagged
    ``needs_input`` with the worker's specific ``question``. The worker process
    may then exit to save tokens — continuity is not lost, because the Claude
    session is resumable by id (``claimed_session_id``).

    ``progress`` is an optional analysis-so-far note, stored append-only as a
    backstop so a fresh worker could resume from notes if the session is ever
    truly gone. Resume-first, notes-as-fallback.

    ``commit`` is an optional pre-verified commit SHA (verified by the caller,
    e.g. `wt block --commit` via the same `close_proof`/`git rev-parse` check
    `wt close --commit` uses) recording what this turn's work did, without
    requiring the ticket to close -- the point of `--kind awaiting-client`.
    """
    kind = kind if kind in ("input", "rationale", "awaiting-client") else "input"
    backend = _github_backend_for_project(_project_from_ident(ident))
    if backend is not None:
        item = backend.block(
            ident, session_id=session_id, question=question, progress=progress,
        )
        if item:
            _notify_ticket_event(
                item, "needs_input", detail=question, actor=session_id,
            )
        return item
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        for it in data["items"]:
            if _matches(it, ident):
                now = _now_iso()
                was_parked = it.get("status") == PARKED_STATUS
                _supersede_pending_unlocked(it, now)
                park_worker = str(it.get("claimed_by") or session_id or "")
                park = (origin == "worker" and not was_parked
                        and it.get("status") in ("open", "in_progress") and bool(park_worker))
                park_sid = str(it.get("claimed_session_id")
                               or _coerce_session_uuid(session_id) or "")
                park_machine = str(it.get("claimed_machine") or machine_tag())
                it["needs_input"] = True
                it["block_question"] = _clip(question, 4000)
                it["block_kind"] = kind
                it["blocked_at"] = now
                it["updated_at"] = now
                if commit:
                    it["block_commit"] = commit
                it.pop("resume", None)
                _clear_sent_back_unlocked(it)
                if it.get("status") == "open" and not park:
                    it["status"] = "in_progress"
                if session_id and not park and not was_parked:
                    real = _coerce_session_uuid(session_id)
                    if not it.get("claimed_by"):
                        it["claimed_by"] = str(session_id)
                        it["claimed_machine"] = machine_tag()
                        it["claim_proc"] = _claim_proc_for(session_id, real or "")
                    if real and not it.get("claimed_session_id"):
                        it["claimed_session_id"] = real
                actor = _by("worker", str(session_id), str(_coerce_session_uuid(session_id) or ""))
                if progress:
                    _append_history(it, "progress", by=actor, at=now, text=_clip(progress, 24000))
                _append_history(
                    it, "block", by=actor, at=now,
                    question=_clip(question, 4000), kind=kind,
                    commit=commit,
                )
                if park:
                    _park_unlocked(it, park_worker, park_sid, park_machine, now, actor)
                _save_unlocked(data)
                _supersede_outbox(it.get("ref"))
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
                _notify_ticket_event(
                    it,
                    "awaits_decision" if kind == "rationale" else "needs_input",
                    detail=question, actor=session_id,
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
                stage_retry = _stage_retry_reset(it, text, now)
                if (it.get("status") == PARKED_STATUS and it.get("parked")
                        and not stage_retry):
                    _write_pending_answer_unlocked(it, text, now)
                elif it.get("status") == "in_progress" and not it.get("claimed_session_id"):
                    it["status"] = "open"
                    it["claimed_by"] = None
                    it["claimed_machine"] = None
                    it["claimed_at"] = None
                    _drop_claim_proc_unlocked(it)
                    it["block_question"] = ""
                    it["block_kind"] = ""
                    it["block_commit"] = ""
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
                if stage_retry:
                    try:
                        from . import stages as _stages
                        _stages.request(str(it.get("ref") or ""), "answer")
                    except Exception:  # noqa: BLE001 - the tick recovers from state
                        pass
                return it
    return None


def _require_rationale_block(it: Dict[str, Any], ident: Any) -> None:
    if not it.get("needs_input") or it.get("block_kind") != "rationale":
        status = it.get("status")
        hint = (
            "it is closed — resolution-caveat acks moved to `wt unresolved-ack`"
            if status == "closed" else
            f"it is {status} with block_kind="
            f"{it.get('block_kind') or '(none)'} — the gate applies only to a "
            f"ticket a worker parked with `wt block --kind rationale`"
        )
        raise ValueError(
            f"{it.get('ref', ident)} is not awaiting a product decision: {hint}"
        )


def gate_ack(
    ident: Any, comment: str = "", by: str = "human", session_id: str = "",
) -> Optional[Dict[str, Any]]:
    """Approve a product-gate pitch: clear the block and record the decision.

    The decision survives reopen (``product_ack`` is never cleared by the
    reopen path), so a ticket approved once is never re-gated. Delivery of
    the go-signal to the parked worker is the CLI/CCC layer's job (same
    steer/resume path as ``wt answer``); this function only owns state."""
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        for it in data["items"]:
            if not _matches(it, ident):
                continue
            _require_rationale_block(it, ident)
            now = _now_iso()
            it["needs_input"] = False
            it["answered_at"] = now
            it["updated_at"] = now
            it["product_ack"] = {
                "by": _clip(str(by or "human"), 128),
                "at": now,
                "comment": _clip(comment, 4000),
            }
            if it.get("status") == PARKED_STATUS and it.get("parked"):
                _write_pending_answer_unlocked(it, comment or "approved", now)
            _append_history(
                it, "gate_ack",
                by=_by("human", str(by or ""), str(session_id or "")),
                at=now, text=_clip(comment, 24000),
            )
            _save_unlocked(data)
            _log("GATE-ACK", f"{it.get('ref', '?')} — {_clip(comment, 240)}",
                 queue=it.get("project", ""))
            return it
    return None


def gate_nack(
    ident: Any, reason: str, by: str = "human", session_id: str = "",
    close: bool = False,
) -> Optional[Dict[str, Any]]:
    """Decline a product-gate pitch.

    Default is the icebox: the claim is released and the ticket parked
    unclaimable under ``readiness: needs-rationale`` — "not now"; the value
    name records what revives it (someone brings a new rationale, via
    ``wt edit --readiness ready``). ``close=True`` is "not ever": closed via
    the normal close path with a Declined resolution (reopen stays available).
    ``reason`` is mandatory either way — the why must survive."""
    if not str(reason or "").strip():
        raise ValueError("gate_nack requires a reason (-m): record WHY this "
                         "is not being built")
    if close:
        with _FileLock(_lock_path()):
            data = _load_unlocked()
            for it in data["items"]:
                if _matches(it, ident):
                    _require_rationale_block(it, ident)
                    now = _now_iso()
                    it["product_nack"] = {
                        "by": _clip(str(by or "human"), 128), "at": now,
                        "comment": _clip(reason, 4000),
                    }
                    _append_history(
                        it, "gate_nack",
                        by=_by("human", str(by or ""), str(session_id or "")),
                        at=now, text=_clip(reason, 24000), closed=True,
                    )
                    _save_unlocked(data)
                    break
            else:
                return None
        return globals()["close"](
            ident, session_id=str(by or ""),
            resolution={"summary": f"Declined at product gate: {reason}"},
            force=True, declined=True,
        )
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        for it in data["items"]:
            if not _matches(it, ident):
                continue
            _require_rationale_block(it, ident)
            now = _now_iso()
            it["needs_input"] = False
            it["block_question"] = ""
            it["block_kind"] = ""
            it["blocked_at"] = None
            it["status"] = "open"
            it["claimed_by"] = None
            it["claimed_machine"] = None
            it["claimed_at"] = None
            it.pop("parked", None)
            it["readiness"] = "needs-rationale"
            it["updated_at"] = now
            it["product_nack"] = {
                "by": _clip(str(by or "human"), 128), "at": now,
                "comment": _clip(reason, 4000),
            }
            _append_history(
                it, "gate_nack",
                by=_by("human", str(by or ""), str(session_id or "")),
                at=now, text=_clip(reason, 24000),
            )
            _save_unlocked(data)
            _log("GATE-NACK", f"{it.get('ref', '?')} — {_clip(reason, 240)}",
                 queue=it.get("project", ""))
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
    return [it for it in list_items(project=project)
            if it.get("needs_input")
            or (it.get("status") == PARKED_STATUS
                and (it.get("pending_answer") or {}).get("state") == "routing")]


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


def worker_active_claims(
    session_id: str, project: Optional[str] = None
) -> List[Dict[str, Any]]:
    """The active (non-blocked) ``in_progress`` tickets ``session_id`` holds.

    A worker between tickets must have this empty -- that is the
    one-ticket-at-a-time invariant ``claim_next`` enforces. Blocked tickets
    are excluded (see ``list_active_claims``): parking a ticket on a human
    decision is a legitimate way to move on to the next one.
    """
    sid = str(session_id or "")
    if not sid:
        return []
    return [
        it for it in list_active_claims(project)
        if str(it.get("claimed_by") or "") == sid
    ]


def _reverify_held_claims(claims: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Re-read each allegedly-held ticket and keep only those still held.

    ``claim_next``'s one-ticket-at-a-time guard builds its ``held`` list from
    a list snapshot, which on a GitHub-backed queue can lag the worker's own
    close by up to minutes (persisted list cache re-seeded from the poller's
    stale in-process copy; OPS-854). A direct per-ticket ``get`` is a strict
    read (a single ``gh issue view``), so it always reflects the close the
    worker just made. A ticket whose fresh read fails (GitHub hiccup) is kept
    as held: refusing one claim is recoverable, double-staffing is not. For
    the file backend ``get`` reads the same store the snapshot came from, so
    this is a cheap no-op confirmation there.
    """
    verified: List[Dict[str, Any]] = []
    for it in claims:
        ref = it.get("ref")
        try:
            current = get(ref) if ref else None
        except Exception:
            current = None
        if current is None:
            verified.append(it)  # unreadable: trust the snapshot
            continue
        if current.get("status") == "in_progress" and not current.get("needs_input"):
            verified.append(current)
    return verified


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

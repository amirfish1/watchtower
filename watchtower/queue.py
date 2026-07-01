#!/usr/bin/env python3
"""Durable, numbered, stateful work queue — the WatchTower engine.

This module is the self-contained heart of WatchTower. It replaces
fire-and-forget task injection with a single durable queue file: every request
becomes a numbered item with a status that survives processes, so:

  * nothing is silently dropped (it's a row, not a paragraph in a transcript),
  * a human can refer to work by ref ("take CCC-7"),
  * multiple workers can drain the queue in parallel by *claiming* items
    instead of stepping on each other.

Storage: a single JSON file. Resolution order for its path:

  1. ``$WATCHTOWER_STORE`` (explicit override — used by tests and CI).
  2. The existing CCC store at ``~/.claude/command-center/ux-fixes-queue.json``
     if it already exists on this machine (so WatchTower drains real work today).
  3. ``~/.watchtower/queues.json`` (WatchTower's own default).

Concurrency: writers from different processes are serialised with an ``fcntl``
lock file; writes are atomic via temp-file + ``os.replace``.

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
        "caveats": [...], "follow_ups": [...], "unresolved": [...]
      },
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
VALID_PRIORITIES = ("p0", "p1", "p2", "p3", "p4", "")
VALID_VALUES = ("H", "M", "L", "")
VALID_CONFIDENCES = ("H", "M", "L", "")

# Legacy CCC store — WatchTower reads it if present so it works on this machine
# today, before any WatchTower-native queue exists.
_CCC_LEGACY_STORE = Path.home() / ".claude" / "command-center" / "ux-fixes-queue.json"
# WatchTower's own default home.
_WT_DEFAULT_STORE = Path.home() / ".watchtower" / "queues.json"
# Unified activity log — queue events (enqueue/claim/close) + reconciler (spawn/reap).
_ACTIVITY_LOG = Path.home() / ".watchtower" / "activity.log"


def _log(verb: str, detail: str, queue: str = "") -> None:
    """Append one plain-text line to the unified activity log."""
    try:
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        q_col = (queue or "reconciler")
        _ACTIVITY_LOG.parent.mkdir(parents=True, exist_ok=True)
        with open(_ACTIVITY_LOG, "a") as f:
            f.write(f"{now}  {q_col:<14}  {verb:<9}{detail}\n")
    except Exception:
        pass


def _resolve_store_path() -> Path:
    """Resolve the active store path. See module docstring for the order.

    Read fresh each call so tests can flip ``$WATCHTOWER_STORE`` between runs.
    """
    env = os.environ.get("WATCHTOWER_STORE")
    if env:
        return Path(env).expanduser()
    if _CCC_LEGACY_STORE.exists():
        return _CCC_LEGACY_STORE
    return _WT_DEFAULT_STORE


def store_path() -> Path:
    """Public accessor — handy for `wt status` to print which file it read."""
    return _resolve_store_path()


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
    """Return a bare UUID from ``value`` if one is present, else None."""
    s = str(value or "").strip()
    if not s:
        return None
    if _SESSION_ID_RE.match(s):
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
        return GitHubIssuesBackend(
            proj,
            repo=config.github_repo(proj),
            repo_path=config.repo_path(proj),
            assignee=config.github_assignee(proj),
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


def _load_unlocked() -> Dict[str, Any]:
    try:
        with open(_resolve_store_path(), "r") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return _empty_store()
    if not isinstance(data, dict):
        return _empty_store()
    data.setdefault("counter", 0)
    items = data.get("items")
    data["items"] = items if isinstance(items, list) else []
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
    path = _resolve_store_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = str(path) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


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
) -> Dict[str, Any]:
    """Append a new ``open`` item and return it (with its assigned ref)."""
    note = _clip(note, 4000)
    if not note and not text:
        raise ValueError("note or text is required")
    lane = lane if lane in VALID_LANES else "normal"
    proj = _project_for(source, repo_path, project)
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
            "needs_input": False,
            "block_question": "",
            "claimed_by": None,
            "claimed_at": None,
            "closed_at": None,
            "claimed_session_id": None,
            "created_at": now,
            "updated_at": now,
        }
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
) -> List[Dict[str, Any]]:
    if project:
        backend = _github_backend_for_project(project)
        if backend is not None:
            return backend.list_items(status=status, lane=lane)
    data = _load_unlocked()
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
                items.extend(backend.list_items(status=status, lane=lane))
    return items


def mark_runnable(ident: Any) -> Optional[Dict[str, Any]]:
    """Mark an existing ticket as eligible for WatchTower automation.

    For GitHub-backed queues this adds the queue's ``watchtower:<QUEUE>`` label
    to an existing open issue. File-backed queues are already WatchTower-native,
    so the item is returned unchanged.
    """
    backend = _github_backend_for_project(_project_from_ident(ident))
    if backend is not None:
        item = backend.mark_runnable(ident)
        if item:
            _log("RUN", f"{item.get('ref', ident)} — marked runnable", queue=item.get("project", ""))
        return item
    return get(ident)


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

    Allowed fields: item_type, readiness, priority, value, confidence, note,
    title, url, selector, screenshot_path, repo_path, needs_input,
    block_question.

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
        "note", "title", "url", "selector", "screenshot_path", "repo_path",
        "needs_input", "block_question",
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
                it["updated_at"] = now
                _save_unlocked(data)
                return it
    return None


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
    file is deleted and ``{"stop": True}`` is returned so the worker can exit
    cleanly without claiming a new ticket.
    """
    if not session_id:
        raise ValueError("session_id is required")
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

    # A reconciler stop signal means "wind down — you're not needed". But it is
    # only honored when there is genuinely nothing for this worker to claim:
    # otherwise a STOP dropped during a brief drained window races with a new
    # enqueue, and the worker — nudged awake by the new ticket — would consume
    # the stale signal and exit, orphaning the just-filed ticket. So: note the
    # signal here, but defer the decision until after we know if work exists.
    try:
        from . import workers as _workers
        stop_dir = _workers.STOP_SIGNALS_DIR
    except Exception:
        stop_dir = Path.home() / ".watchtower" / "stop-signals"
    signal_file = stop_dir / session_id
    has_stop_signal = signal_file.exists()

    real_sid = _coerce_session_uuid(session_uuid) or _coerce_session_uuid(session_id)
    proj = _norm_project(project) if project else None
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        candidates = [it for it in data["items"] if it.get("status") == "open"]
        if proj:
            candidates = [it for it in candidates if it.get("project") == proj]
        if lane:
            candidates = [it for it in candidates if it.get("lane") == lane]
        if readiness_filters:
            candidates = [it for it in candidates if it.get("readiness", "") in readiness_filters]
        elif not shaping:
            candidates = [
                it for it in candidates
                if it.get("readiness", "") not in ("needs-shaping", "needs-spec")
            ]
        if item_types:
            candidates = [
                it for it in candidates
                if effective_type(it) in item_types
            ]
        # Resolve the stop signal now that we know whether claimable work exists.
        # Either way the signal is consumed (one-shot). If nothing is claimable,
        # honor it and exit; if there IS work, the signal's premise is void —
        # discard it and claim, so a new ticket racing a STOP isn't orphaned.
        if has_stop_signal:
            try:
                signal_file.unlink()
            except OSError:
                pass
            if not candidates:
                return {"stop": True}
        if not candidates:
            return None
        if oldest:
            candidates.sort(key=lambda it: int(it.get("number", 0)))
        else:
            candidates.sort(
                key=lambda it: (
                    0 if it.get("lane") == "express" else 1,
                    _prio_rank(it),
                    _type_rank(it),
                    int(it.get("number", 0)),
                )
            )
        item = candidates[0]
        item["status"] = "in_progress"
        item["claimed_by"] = str(session_id)
        if real_sid:
            item["claimed_session_id"] = real_sid
        item["claimed_at"] = _now_iso()
        item["updated_at"] = item["claimed_at"]
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
        _save_unlocked(data)
    _log("CLAIM", f"{item.get('ref', '?')} by {session_id[:16]} — {item.get('title') or item.get('note', '')[:60]}", queue=item.get('project', ''))
    return item


def peek_next(
    project: Optional[str] = None,
    lane: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Return a copy of the next claimable open item without claiming it.

    Uses the same smart sort as claim_next (priority → type → age).
    Returns None when nothing is open and claimable."""
    backend = _github_backend_for_project(project)
    if backend is not None:
        return backend.peek_next(lane=lane)
    proj = _norm_project(project) if project else None
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        candidates = [it for it in data["items"] if it.get("status") == "open"]
        if proj:
            candidates = [it for it in candidates if it.get("project") == proj]
        if lane:
            candidates = [it for it in candidates if it.get("lane") == lane]
        candidates = [
            it for it in candidates
            if it.get("readiness", "") not in ("needs-shaping", "needs-spec")
        ]
        if not candidates:
            return None
        candidates.sort(
            key=lambda it: (
                0 if it.get("lane") == "express" else 1,
                _prio_rank(it),
                _type_rank(it),
                int(it.get("number", 0)),
            )
        )
        return dict(candidates[0])


def _normalize_resolution(resolution: Any) -> Optional[Dict[str, Any]]:
    """Coerce a resolution into the stored shape, or None when empty.

    Accepts a bare string (treated as the summary) or a dict with any of
    ``summary`` / ``caveats`` / ``follow_ups`` / ``unresolved``. List fields are
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
) -> Optional[Dict[str, Any]]:
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
                it["status"] = status
                now = _now_iso()
                it["updated_at"] = now
                if status == "in_progress" and session_id:
                    it["claimed_by"] = str(session_id)
                    it["claimed_at"] = now
                    if real_sid:
                        it["claimed_session_id"] = real_sid
                if status == "closed":
                    it["closed_at"] = now
                    it["needs_input"] = False  # a closed ticket isn't waiting
                    # Attribute the close so a worker that closed by ref
                    # (without a prior claim) still gets credited.
                    if session_id:
                        it["closed_by"] = str(session_id)
                    elif it.get("claimed_by"):
                        it["closed_by"] = it["claimed_by"]
                    # Record HOW it was fixed — the trust-layer signal. Optional:
                    # absent resolution leaves the item without the key.
                    norm = _normalize_resolution(resolution)
                    if norm is not None:
                        it["resolution"] = norm
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
    ident: Any, session_id: str = "", resolution: Any = None
) -> Optional[Dict[str, Any]]:
    """Close a ticket, optionally recording HOW it was fixed.

    ``resolution`` may be a bare summary string or a dict with any of
    ``summary`` / ``caveats`` / ``follow_ups`` / ``unresolved``. Absent ->
    closes with no resolution (back-compatible)."""
    return update_status(ident, "closed", session_id, resolution=resolution)


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
                if progress:
                    notes = it.get("progress_notes")
                    if not isinstance(notes, list):
                        notes = []
                    notes.append({"at": now, "text": _clip(progress, 24000)})
                    it["progress_notes"] = notes
                _save_unlocked(data)
                return it
    return None


def answer(ident: Any, text: str, session_id: str = "") -> Optional[Dict[str, Any]]:
    """Record a human answer on a blocked ticket and clear ``needs_input`` so the
    resumed session can continue. Answers are append-only, preserving a
    back-and-forth. Does not change ``status`` — the ticket is still
    ``in_progress`` with its worker; the human just unblocked it."""
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        for it in data["items"]:
            if _matches(it, ident):
                now = _now_iso()
                ans = it.get("answers")
                if not isinstance(ans, list):
                    ans = []
                ans.append({"at": now, "text": _clip(text, 24000),
                            "by": str(session_id or "human")})
                it["answers"] = ans
                it["needs_input"] = False
                it["answered_at"] = now
                it["updated_at"] = now
                _save_unlocked(data)
                return it
    return None


def list_blocked(project: Optional[str] = None) -> List[Dict[str, Any]]:
    """Tickets parked for a human (``needs_input`` truthy), optionally scoped to
    one queue. The CLI's ``wt blocked`` and CCC both read this."""
    proj = _norm_project(project) if project else None
    out = []
    for it in _load_unlocked().get("items", []):
        if not it.get("needs_input"):
            continue
        if proj and it.get("project") != proj:
            continue
        out.append(it)
    return out


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

"""Shared Codex thread reconciliation registry for WT and CCC.

The Codex rollout/state stores remain authoritative history. This file is a
small cross-tool index keyed by Codex thread id so WT and CCC can merge the
metadata they each learn without duplicate per-session records.
"""

from __future__ import annotations

import fcntl
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

SCHEMA_VERSION = 1
DEFAULT_PATH = Path.home() / ".claude" / "command-center" / "codex-thread-registry.json"

_VISIBILITY_RANK = {
    "unknown": 0,
    "registered-agent": 1,
    "worker": 2,
    "user-visible": 3,
}
_OWNER_RANK = {
    "codex-exec": 1,
    "wt-codex-exec": 1,
    "ccc-codex-exec": 1,
    "wt-private-app-server": 2,
    "ccc-managed-app-server": 3,
}


def registry_file() -> Path:
    raw = os.environ.get("WATCHTOWER_CODEX_THREAD_REGISTRY")
    if raw:
        return Path(os.path.expanduser(raw))
    return DEFAULT_PATH


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _empty() -> Dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "authoritative": False,
        "source": "ccc-wt-codex-reconciliation",
        "updated_at": _now_iso(),
        "threads": {},
    }


def load() -> Dict[str, Any]:
    path = registry_file()
    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return _empty()
    if not isinstance(data, dict):
        return _empty()
    threads = data.get("threads")
    if not isinstance(threads, dict):
        data["threads"] = {}
    data["schema_version"] = SCHEMA_VERSION
    data["authoritative"] = False
    data.setdefault("source", "ccc-wt-codex-reconciliation")
    return data


def save(data: Dict[str, Any]) -> None:
    path = registry_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    data["schema_version"] = SCHEMA_VERSION
    data["authoritative"] = False
    data["source"] = "ccc-wt-codex-reconciliation"
    data["updated_at"] = _now_iso()
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)
    tmp.replace(path)


class _FileLock:
    def __init__(self, path: Path):
        self.path = path
        self._fh = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("a+")
        fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, *_exc):
        if self._fh is not None:
            try:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
            finally:
                self._fh.close()


def _merge_nonempty(dst: Dict[str, Any], src: Dict[str, Any]) -> None:
    for key, value in src.items():
        if value is None or value == "":
            continue
        dst[key] = value


def _merge_record(existing: Dict[str, Any], fields: Dict[str, Any], now: str) -> Dict[str, Any]:
    rec = dict(existing or {})
    rec.setdefault("created_at", now)
    rec["updated_at"] = now
    rec["thread_id"] = fields.get("thread_id") or rec.get("thread_id")
    rec["engine"] = "codex"

    source = fields.get("source")
    sources = [str(s) for s in rec.get("sources") or [] if s]
    if source and str(source) not in sources:
        sources.append(str(source))
    if sources:
        rec["sources"] = sources

    visibility = fields.get("visibility")
    if visibility and _VISIBILITY_RANK.get(str(visibility), 0) >= _VISIBILITY_RANK.get(str(rec.get("visibility") or "unknown"), 0):
        rec["visibility"] = str(visibility)

    owner = fields.get("transport_owner")
    if owner and _OWNER_RANK.get(str(owner), 0) >= _OWNER_RANK.get(str(rec.get("transport_owner") or ""), 0):
        rec["transport_owner"] = str(owner)

    for key in (
        "cwd",
        "repo_path",
        "transport",
        "title",
        "name",
        "parent_session_id",
        "report_to",
        "model",
        "reasoning_effort",
        "worker_id",
        "queue",
        "ref",
    ):
        value = fields.get(key)
        if value is not None and value != "":
            rec[key] = value

    for key in ("ccc", "wt"):
        value = fields.get(key)
        if isinstance(value, dict):
            nested = dict(rec.get(key) or {})
            _merge_nonempty(nested, value)
            if nested:
                rec[key] = nested
    return rec


def upsert(thread_id: str, **fields: Any) -> Optional[Dict[str, Any]]:
    sid = str(thread_id or "").strip()
    if not sid:
        return None
    fields["thread_id"] = sid
    path = registry_file()
    try:
        with _FileLock(path.with_suffix(path.suffix + ".lock")):
            data = load()
            threads = data.setdefault("threads", {})
            now = _now_iso()
            rec = _merge_record(threads.get(sid) if isinstance(threads.get(sid), dict) else {}, fields, now)
            threads[sid] = rec
            save(data)
            return rec
    except OSError:
        return None


def entry(thread_id: str) -> Optional[Dict[str, Any]]:
    sid = str(thread_id or "").strip()
    if not sid:
        return None
    rec = load().get("threads", {}).get(sid)
    return dict(rec) if isinstance(rec, dict) else None

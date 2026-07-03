"""Log maintenance for ~/.watchtower/logs (WT-74).

The logs dir grows without bound: every worker and every resume delivery
captures its full stream-json output to ``logs/*.log``. For a Claude run
that exits cleanly the log is a redundant second copy of the engine
transcript — every line embeds the ``session_id``, and the durable record
lives in ``~/.claude/projects/<slug>/<sid>.jsonl``. Failures are different:
a process that dies before a session exists produces NO transcript, so its
log is the only record and must be kept longer.

Policy (oldest-first within each rule):

  1. Never touch a live worker's log, any ``*.stdin`` FIFO belonging to a
     live worker's log, or any file younger than ``min_age_s``.
  2. A log whose embedded session_id resolves to a transcript on disk is
     *redundant*: prune after ``clean_after_s`` (default 24h). The
     ``worker/log -> session_id`` mapping is persisted to
     ``logs/log-index.json`` first so the transcript stays discoverable.
  3. A log with no session_id, or whose transcript is missing, is the
     *only record*: keep for ``keep_failure_s`` (default 7 days).
  4. Backstop: if the dir still exceeds ``max_total_bytes`` (default
     500MB), delete oldest-first regardless of rule 2/3 (rule 1 still
     holds).
  5. Orphaned ``*.stdin`` FIFOs (no live owner, older than ``min_age_s``)
     are unlinked.

Entry points: ``prune()`` (used by ``wt logs prune``) and
``maybe_prune()`` (daemon tick, throttled via a stamp file).
"""

from __future__ import annotations

import json
import os
import re
import stat
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from . import messages, workers

_SESSION_ID_RE = re.compile(
    r'"session_id"\s*:\s*"([0-9a-fA-F-]{36})"'
)

INDEX_FILENAME = "log-index.json"
STAMP_FILENAME = ".last-prune"


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, "") or default)
    except ValueError:
        return default


def logs_dir() -> Path:
    return messages._logs_dir()


def _extract_session_id(path: Path) -> Optional[str]:
    """Find the embedded session_id by scanning the head and tail of the log."""
    try:
        size = path.stat().st_size
        with open(path, "rb") as f:
            head = f.read(65536)
            tail = b""
            if size > 65536:
                f.seek(max(0, size - 8192))
                tail = f.read(8192)
    except OSError:
        return None
    for chunk in (head, tail):
        m = _SESSION_ID_RE.search(chunk.decode("utf-8", "replace"))
        if m:
            return m.group(1)
    return None


def _live_protected_paths() -> set:
    """Log + FIFO paths owned by live workers — never prunable."""
    protected = set()
    try:
        rows = workers.list_workers(prune=False)
    except Exception:  # noqa: BLE001 - a workers-file hiccup must not nuke logs
        return {"__list_workers_failed__"}
    for w in rows:
        if not w.get("alive"):
            continue
        log = str(w.get("log") or "")
        if log:
            protected.add(log)
            protected.add(log + ".stdin")
        fifo = str(w.get("fifo") or "")
        if fifo:
            protected.add(fifo)
    return protected


def _load_index(d: Path) -> Dict[str, Any]:
    try:
        with open(d / INDEX_FILENAME, "r") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _save_index(d: Path, index: Dict[str, Any]) -> None:
    tmp = d / (INDEX_FILENAME + ".tmp")
    try:
        with open(tmp, "w") as f:
            json.dump(index, f, indent=1)
        os.replace(tmp, d / INDEX_FILENAME)
    except OSError:
        pass


def prune(
    now: Optional[float] = None,
    dry_run: bool = False,
    max_total_bytes: Optional[float] = None,
    clean_after_s: Optional[float] = None,
    keep_failure_s: Optional[float] = None,
    min_age_s: Optional[float] = None,
) -> Dict[str, Any]:
    """Apply the retention policy once. Returns a report dict."""
    now = time.time() if now is None else float(now)
    max_total_bytes = (
        _env_float("WATCHTOWER_LOGS_MAX_BYTES", 500 * 1024 * 1024)
        if max_total_bytes is None else float(max_total_bytes)
    )
    clean_after_s = (
        _env_float("WATCHTOWER_LOGS_CLEAN_AFTER_S", 24 * 3600.0)
        if clean_after_s is None else float(clean_after_s)
    )
    keep_failure_s = (
        _env_float("WATCHTOWER_LOGS_KEEP_FAILURE_S", 7 * 86400.0)
        if keep_failure_s is None else float(keep_failure_s)
    )
    min_age_s = (
        _env_float("WATCHTOWER_LOGS_MIN_AGE_S", 3600.0)
        if min_age_s is None else float(min_age_s)
    )

    d = logs_dir()
    report: Dict[str, Any] = {
        "pruned": [], "kept": 0, "freed_bytes": 0,
        "fifos_removed": [], "dry_run": dry_run,
    }
    if not d.is_dir():
        return report
    protected = _live_protected_paths()
    if "__list_workers_failed__" in protected:
        report["error"] = "could not list live workers; skipped prune"
        return report
    index = _load_index(d)

    entries: List[Dict[str, Any]] = []
    for p in sorted(d.iterdir()):
        if p.name in (INDEX_FILENAME, STAMP_FILENAME):
            continue
        try:
            st = p.lstat()
        except OSError:
            continue
        age = now - st.st_mtime
        if str(p) in protected or age < min_age_s:
            report["kept"] += 1
            continue
        if stat.S_ISFIFO(st.st_mode):
            # Orphaned stdin FIFO: no live owner (checked above), old enough.
            if not dry_run:
                try:
                    p.unlink()
                except OSError:
                    continue
            report["fifos_removed"].append(p.name)
            continue
        if not p.name.endswith(".log"):
            report["kept"] += 1
            continue
        entries.append({"path": p, "size": st.st_size, "age": age})

    def _delete(e: Dict[str, Any], reason: str) -> None:
        sid = _extract_session_id(e["path"])
        if not dry_run:
            index[e["path"].name] = {
                "session_id": sid,
                "size": e["size"],
                "pruned_at": now,
                "reason": reason,
            }
            try:
                e["path"].unlink()
            except OSError:
                return
        report["pruned"].append({"file": e["path"].name, "reason": reason,
                                 "session_id": sid})
        report["freed_bytes"] += e["size"]
        e["gone"] = True

    # Rules 2 + 3: redundant-after-grace vs only-record retention.
    for e in entries:
        sid = _extract_session_id(e["path"])
        transcript = messages._find_transcript(sid) if sid else None
        if sid and transcript is not None:
            if e["age"] >= clean_after_s:
                _delete(e, "transcript-redundant")
        else:
            if e["age"] >= keep_failure_s:
                _delete(e, "failure-retention-expired")

    # Rule 4 backstop: total size cap, oldest first.
    remaining = [e for e in entries if not e.get("gone")]
    total = sum(e["size"] for e in remaining)
    if total > max_total_bytes:
        for e in sorted(remaining, key=lambda x: -x["age"]):
            if total <= max_total_bytes:
                break
            _delete(e, "size-cap")
            total -= e["size"]

    report["kept"] += sum(1 for e in entries if not e.get("gone"))
    if not dry_run and (report["pruned"] or report["fifos_removed"]):
        _save_index(d, index)
    return report


def maybe_prune(min_interval_s: Optional[float] = None) -> Optional[Dict[str, Any]]:
    """Daemon-tick entry: run prune() at most once per interval (stamp file)."""
    interval = (
        _env_float("WATCHTOWER_LOGS_PRUNE_INTERVAL_S", 3600.0)
        if min_interval_s is None else float(min_interval_s)
    )
    d = logs_dir()
    stamp = d / STAMP_FILENAME
    now = time.time()
    try:
        if stamp.exists() and now - stamp.stat().st_mtime < interval:
            return None
        d.mkdir(parents=True, exist_ok=True)
        stamp.touch()
    except OSError:
        return None
    return prune(now=now)

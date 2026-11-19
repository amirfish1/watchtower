"""Local indexed projections of GitHub's shared list snapshots.

The normal poller publishes projections after updating its snapshot. Window
reads use these SQLite files and never run gh, even for dependency lookups.
A missing/legacy projection is built once under the snapshot writer lock.
Source coverage is explicit: a capped or retained GitHub list is not an export
of the entire issue history. Never call a partial snapshot complete.
"""
from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path


def _directory(cache_path: Path) -> Path:
    return cache_path.with_name(cache_path.name + ".reports")


def _path(backend, cache_path):
    key = hashlib.sha256(f"{backend.repo}:{backend.queue}".encode()).hexdigest()[:24]
    return _directory(cache_path) / (key + ".db")


def _stamp(path):
    st = path.stat()
    return f"{st.st_ino}:{st.st_mtime_ns}:{st.st_size}"


def _config_key(backend):
    from . import github_pipeline
    parts = [sorted(v) if isinstance(v, frozenset) else v for v in backend._memo_config()]
    return json.dumps([parts, github_pipeline.signature()], sort_keys=True)


def _publish(backend, data, cache_path, *, metadata_only=False):
    from . import queue as q
    db = _path(backend, cache_path)
    db.parent.mkdir(parents=True, exist_ok=True)
    conn = q._connect(db)
    try:
        q._ensure_schema(conn)
        conn.execute("CREATE TABLE IF NOT EXISTS raw_issues (number INTEGER PRIMARY KEY, blob TEXT NOT NULL)")
        conn.commit()
        conn.execute("BEGIN IMMEDIATE")
        old = {} if metadata_only else dict(conn.execute("SELECT number, blob FROM raw_issues"))
        changed_config = q._meta_get(conn, "report_config") != _config_key(backend)
        # Lists are fetched independently. Resolve overlaps before comparing
        # against old rows, or an unchanged open row could leave a stale closed
        # row indexed after processing the closed snapshot first.
        issues = {}
        for state in (() if metadata_only else ("closed", "open")):
            entry = data.get(f"{backend.repo}:{state}") or {}
            for raw in entry.get("data") or []:
                issues[int(raw["number"])] = raw
        seen = set(issues)
        for num, raw in issues.items():
            blob = q._canon(raw)
            if old.get(num) == blob and not changed_config:
                continue
            it = backend._issue_to_item(raw)
            if backend.partition_by_label and not it.get("watchtower_runnable"):
                conn.execute("DELETE FROM items WHERE number=?", (num,))
            else:
                conn.execute(q._ITEM_UPSERT, q._item_row(it))
                q._index_item(conn, it)
            conn.execute("INSERT OR REPLACE INTO raw_issues VALUES (?,?)", (num, blob))
        for num in set(old) - seen:
            conn.execute("DELETE FROM items WHERE number=?", (num,))
            conn.execute("DELETE FROM raw_issues WHERE number=?", (num,))
        # Persist only coverage metadata, never the full JSON snapshot.
        for state in ("open", "closed"):
            entry = data.get(f"{backend.repo}:{state}") or {}
            meta = {k: entry.get(k) for k in ("at", "fetched_at", "stale")}
            meta["rows"] = len(entry.get("data") or [])
            meta["present"] = isinstance(entry.get("data"), list)
            conn.execute(q._META_UPSERT, ("source:" + state, json.dumps(meta)))
        conn.executemany(q._META_UPSERT, [
            ("cache_stamp", _stamp(cache_path)), ("report_queue", backend.queue),
            ("report_repo", backend.repo), ("report_config", _config_key(backend)),
        ])
        conn.commit()
    finally:
        conn.close()
    return db


def publish_existing(data, cache_path, *, changed_repos=None, previous_stamp=None,
                     changed_data_repos=None):
    """Called by the snapshot writer while holding its lock; no network I/O.

    Only opted-in projections (created by a reporting read) pay this cost.
    A failed publication leaves its generation stale; the next reader repairs
    it under the same lock rather than silently serving incorrect coverage.
    Unchanged repositories can advance without scanning their issue bodies,
    but only when both the prior generation and configuration are proven.
    """
    from . import queue as q
    for db in _directory(cache_path).glob("*.db"):
        conn = q._connect(db)
        metadata_only = False
        try:
            project = q._meta_get(conn, "report_queue")
            repo = q._meta_get(conn, "report_repo")
            backend = q._github_backend_for_project(project)
            if backend is None or backend.repo != repo:
                continue
            current = (previous_stamp is not None
                    and q._meta_get(conn, "cache_stamp") == previous_stamp
                    and q._meta_get(conn, "report_config") == _config_key(backend))
            if current and changed_repos is not None and repo not in changed_repos:
                conn.execute(q._META_UPSERT, ("cache_stamp", _stamp(cache_path)))
                conn.commit()
                continue
            # A revalidation/invalidation changed coverage, but no issue body.
            # Keep source metadata current without reading all raw blobs in
            # each queue projection. A missed generation or config edit must
            # still take the full repair path.
            metadata_only = current and changed_data_repos is not None and repo not in changed_data_repos
        finally:
            conn.close()
        _publish(backend, data, cache_path, metadata_only=metadata_only)


def _coverage(conn, backend, spec):
    from . import queue as q
    from . import github_backend as gh
    sources = {state: json.loads(q._meta_get(conn, "source:" + state, "{}"))
               for state in ("open", "closed")}
    reasons = []
    now = time.time()
    for state, src in sources.items():
        if not src.get("present"):
            reasons.append(f"{state} snapshot missing")
        elif src.get("stale") or now - float(src.get("at") or 0) >= gh._persisted_list_ttl_s():
            reasons.append(f"{state} snapshot stale")
        if src.get("rows", 0) >= gh._list_limit():
            reasons.append(f"{state} snapshot may be capped")
    cutoff = now - gh.COMPLETED_ISSUE_RETENTION_DAYS * 86400
    # Even recent events/updates can belong to long-closed issues excluded
    # by the normal poller's retention filter. Make that gap unavoidable.
    if spec.events or spec.updated_since is not None or spec.refs \
            or spec.closed_since is None or spec.closed_since < cutoff:
        reasons.append(f"closed history outside {gh.COMPLETED_ISSUE_RETENTION_DAYS}d retention is not indexed")
    return sources, reasons


def window_items(backend, spec, *, offset=0, limit=0, status="all", client_id=None):
    from . import queue as q
    from . import github_backend as gh
    cache_path = gh._list_cache_path()
    db = _path(backend, cache_path)
    with gh._FileLock(cache_path.with_suffix(".lock")):
        if not cache_path.exists():
            meta = q._window_meta(0, offset, limit, 0, "github-sqlite")
            return [], dict(meta, complete=False, source_complete=False,
                            coverage="GitHub local snapshot missing; wait for the normal poller")
        current = False
        if db.exists():
            conn = q._connect(db)
            try:
                current = (q._meta_get(conn, "cache_stamp") == _stamp(cache_path)
                           and q._meta_get(conn, "report_config") == _config_key(backend))
            finally:
                conn.close()
        if not current:
            db = _publish(backend, gh._read_persisted_list_cache(), cache_path)
        page, total = q._window_from_index(db, backend.queue, spec, offset, limit,
                                           status=status, client_id=client_id)
        conn = q._connect(db)
        try:
            sources, reasons = _coverage(conn, backend, spec)
            # Reapply time-sensitive sidecar/usage projections to the page only.
            # Index selection uses the durable history in the cached issue body.
            for n, it in enumerate(page):
                row = conn.execute("SELECT blob FROM raw_issues WHERE number=?", (it["number"],)).fetchone()
                latest = backend._issue_to_item(json.loads(row[0]))
                if "window_events" in it:
                    latest["window_events"] = it["window_events"]
                page[n] = latest
        finally:
            conn.close()
    meta = q._window_meta(total, offset, limit, len(page), "github-sqlite")
    meta.update(source_complete=not reasons, sources=sources)
    if reasons:
        meta.update(complete=False, coverage="; ".join(reasons))
    return page, meta

"""Seed pre-WT-87 records for migration tests, never through the retired API."""
from datetime import datetime, timedelta, timezone


def seed(q, ident, worker, until, reason, progress=""):
    if until.startswith("+"):
        seconds = int(until[1:-1]) * {"m": 60, "h": 3600, "d": 86400}[until[-1]]
        until = (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()
    q.update_status(ident, "open", worker, reason="legacy fixture")
    backend = q._github_backend_for_project(q._project_from_ident(ident))
    if backend is not None:
        from watchtower.github_backend import _serialized_body_write, _split_body, _append_history

        @_serialized_body_write
        def write(backend):
            item = backend.get(ident)
            body, meta = _split_body(item["_github_body"])
            meta.update(deferred_until=until, defer_reason=reason)
            _append_history(meta, "defer", worker=worker, until=until, reason=reason)
            backend._edit_body(item, body, meta)
        write(backend)
    else:
        with q._FileLock(q._lock_path()):
            data = q._load_unlocked()
            item = next(i for i in data["items"] if q._matches(i, ident))
            item.update(deferred_until=until, defer_reason=reason)
            q._append_history(item, "defer", by=q._by("worker", worker), until=until, reason=reason)
            q._save_unlocked(data)
    return q.get(ident)

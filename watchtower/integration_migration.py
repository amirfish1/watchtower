"""Retire legacy integration timers after a worker establishes the real gate.

This does not land code, close tickets, invent reviewers or answer questions.
Use normal block/dependency/release commands first. Each cleanup is fenced by
the exact old reason and preserves a reversal receipt in ticket history.
"""
from __future__ import annotations

from . import queue as q


def _retire(item, expected_reason):
    if (item.get("status") == "closed"
            or item.get("defer_reason") != expected_reason
            or not expected_reason.startswith("[awaits integration")):
        raise ValueError("integration defer changed; inspect the ticket again")
    if not (item.get("needs_input") and (item.get("wait") or {}).get("on")
            or item.get("blocked_by")):
        raise ValueError("record the real block or dependency before retiring its timer")
    previous = {key: item[key] for key in ("defer_reason", "deferred_until") if key in item}
    item.pop("defer_reason", None)
    item.pop("deferred_until", None)
    return previous


def retire(ident, *, expected_reason, worker):
    """Clear a superseded timer, preserving the existing question/wait/claim.

    File mutations hold the store lock; GitHub mutations use its serialized
    read/edit/re-read and three-way merge path, never a raw issue body write.
    """
    backend = q._github_backend_for_project(q._project_from_ident(ident))
    if backend is not None:
        from .github_backend import _serialized_body_write, _split_body, _append_history

        @_serialized_body_write
        def write(backend):
            item = backend.get(ident)
            if item is None:
                raise ValueError(f"no item {ident}")
            body, meta = _split_body(item.get("_github_body") or item.get("text", ""))
            # Status is projected from labels, not stored in metadata.
            checked = dict(meta, status=item.get("status"))
            previous = _retire(checked, expected_reason)
            for key in ("defer_reason", "deferred_until"):
                meta.pop(key, None)
            _append_history(meta, "integration_defer_retired", worker=worker, previous_defer=previous)
            backend._edit_body(item, body, meta)
            return backend.get(ident)

        return write(backend)
    with q._FileLock(q._lock_path()):
        data = q._load_unlocked()
        for item in data["items"]:
            if q._matches(item, ident):
                previous = _retire(item, expected_reason)
                now = q._now_iso()
                q._append_history(item, "integration_defer_retired", by=q._by("worker", worker),
                                  at=now, previous_defer=previous)
                item["updated_at"] = now
                q._save_unlocked(data)
                return item
    raise ValueError(f"no item {ident}")

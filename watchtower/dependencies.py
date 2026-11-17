"""Scoped cross-backend dependency snapshots. Call before taking store locks."""
from __future__ import annotations


def resolve(items, refs, *, project="", fresh=True, transitive=False):
    from . import queue as q
    from .github_backend import GitHubBackendError

    by_ref = q._refs_index(items)
    loaded = set()
    seen = set()
    pending = list(refs)
    while pending:
        raw = str(pending.pop())
        ref = q.canonical_ref(f"{project}-{raw}" if raw.isdigit() and project else raw)
        if ref in seen:
            continue
        seen.add(ref)
        owner = q._project_from_ident(ref)
        backend = q._github_backend_for_project(owner) if owner else None
        try:
            if backend is not None:
                if fresh:
                    # Direct reads cannot mistake a truncated list/cache for absence.
                    item = backend.get(ref)
                    by_ref.pop(ref, None)
                    if item is not None:
                        by_ref[ref] = item
                elif owner not in loaded:
                    by_ref.update(q._refs_index(backend.list_items(fresh=False)))
                    loaded.add(owner)
            elif ref not in by_ref:
                item = q.get(ref)
                if item is not None:
                    by_ref[ref] = item
        except GitHubBackendError as exc:
            # An absent issue is a validation error; outages retain their cause.
            missing = ("not found" in str(exc).lower()
                       or "could not resolve to an issue" in str(exc).lower())
            if transitive and not missing:
                raise
            by_ref.pop(ref, None)
        item = by_ref.get(ref)
        if item is not None and transitive:
            pending.extend(item.get("blocked_by") or [])
    return by_ref


def local_snapshot(*, project=None, ident=None, session_id=None, refs=None, fresh=True, transitive=False):
    """Only fetch dependencies of this operation, never enumerate remote queues.

    Local records are re-read under the caller's lock. Unseen concurrent edges
    remain missing and therefore waiting (or rejected during validation).
    """
    from . import queue as q
    if refs is not None and not refs:
        return []
    items = q._load_unlocked(readonly=True).get("items", [])
    selected = [it for it in items
                if (not project or it.get("project") == project)
                and (ident is None or q._matches(it, ident))
                and (session_id is None or it.get("claimed_session_id") == session_id
                     or (it.get("supervision") or {}).get("session", {}).get("session_id") == session_id)]
    wanted = refs if refs is not None else [
        r for it in selected if ident is not None or it.get("status") != "closed"
        for r in (it.get("blocked_by") or [])]
    if not wanted:
        return []
    by_ref = resolve(items, wanted, fresh=fresh, transitive=transitive)
    owners = {it.get("project") or q._project_from_ident(it.get("ref"))
              for it in by_ref.values()}
    remote = {owner for owner in owners if q._github_backend_for_project(owner) is not None}
    return [it for it in by_ref.values()
            if (it.get("project") or q._project_from_ident(it.get("ref"))) in remote]

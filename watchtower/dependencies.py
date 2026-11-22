"""Scoped cross-backend dependency snapshots. Call before taking store locks."""
from __future__ import annotations

import json
import re
import socket
import subprocess
import time

from .refs import canonical_ref


def local_host():
    return socket.gethostname().partition(".")[0].lower()


def local_key(ref):
    host, sep, bare = ref.partition("/")
    return bare if sep and host == local_host() else ref


def local_evidence(ref, item=None):
    """Export only dependency fields, with local edges bound to this store."""
    from . import config, queue as q

    host, sep, bare = ref.partition("/")
    project = q._project_from_ident(bare)
    if not sep or host != local_host() or not project or config.backend(project) != "file":
        return None
    if item is None:
        item = q.get(bare)
    if not item:
        return None
    edges = []
    for dependency in item.get("blocked_by") or []:
        project = q._project_from_ident(dependency)
        edges.append(f"{host}/{dependency}" if project and config.backend(project) == "file"
                     else dependency)
    declined = bool(item.get("product_nack"))
    unresolved = q._blocker_state(item) == "stuck" and not declined
    return {"ref": ref, "project": item["project"], "status": item["status"],
            "resolution": {"unresolved": ["unresolved on owning host"]} if unresolved else None,
            "product_nack": declined, "blocked_by": edges}


_advisory = {}


def _remote_evidence(ref, *, fresh, deadline):
    from . import storage

    host, _, bare = ref.partition("/")
    try:
        routes = json.loads((storage.data_dir() / "dependency-hosts.json").read_text())
        destination = routes.get(host) if isinstance(routes, dict) else None
    except (OSError, ValueError):
        destination = None
    if (not isinstance(destination, str)
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.@-]*", destination)):
        raise ValueError(f"no configured SSH route for dependency host {host}")
    key = (destination, ref)
    cached = _advisory.get(key)
    if not fresh and cached and time.monotonic() < cached[0]:
        return cached[1]
    command = (f'PATH="$HOME/.local/bin:$PATH" WT_NO_SELF_UPDATE=1 WT_NO_AUTOSTART=1 '
               f'wt find {ref} --dependency-status')
    try:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError
        result = subprocess.run(
            ["ssh", "-nT", "-oBatchMode=yes", "-oStrictHostKeyChecking=yes",
             "-oClearAllForwardings=yes", "-oConnectTimeout=3", destination, command],
            capture_output=True, text=True, timeout=remaining, check=True,
        )
        item = json.loads(result.stdout)
        if item is not None and (not isinstance(item, dict) or item.get("ref") != ref
                                 or item.get("project") != bare.rsplit("-", 1)[0]
                                 or not isinstance(item.get("blocked_by"), list)
                                 or not isinstance(item.get("product_nack"), bool)
                                 or "resolution" not in item
                                 or not item.get("status")):
            raise ValueError("unexpected dependency record")
    except (OSError, subprocess.SubprocessError, ValueError) as exc:
        if fresh:
            raise ValueError(f"could not verify {ref} via {destination} ({type(exc).__name__})") from exc
        item = None
    if len(_advisory) >= 256:
        _advisory.clear()
    _advisory[key] = (time.monotonic() + 15, item)
    return item


def resolve(items, refs, *, project="", fresh=True, transitive=False):
    from . import queue as q
    from .github_backend import GitHubBackendError

    by_ref = q._refs_index(items)
    loaded = set()
    seen = set()
    pending = list(refs)
    deadline = time.monotonic() + 6
    while pending:
        raw = str(pending.pop())
        ref = q.canonical_ref(f"{project}-{raw}" if raw.isdigit() and project else raw)
        if ref in seen:
            continue
        seen.add(ref)
        if "/" in ref:
            # Ticket text is untrusted. Accept only a canonical host/ref pair
            # before interpolation into the remote command.
            host, _, bare = ref.partition("/")
            if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", host) or not q._project_from_ident(bare):
                continue
            try:
                item = (local_evidence(ref, by_ref.get(bare)) if host == local_host()
                        else _remote_evidence(ref, fresh=fresh, deadline=deadline))
            except ValueError:
                if transitive:
                    raise
                item = None
            by_ref.pop(ref, None)
            if item is not None:
                by_ref[ref] = item
                if transitive:
                    pending.extend(item["blocked_by"])
            continue
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
    return [it for ref, it in by_ref.items()
            if "/" in ref or (it.get("project") or q._project_from_ident(ref)) in remote]

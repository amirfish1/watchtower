"""Gate-pipeline state for GitHub-backed tickets (OPS-201, owner decision C).

The GitHub issue stays the canonical ticket; the pipeline fields it cannot
carry (gates, plan, stage_session, stage_retired + their history events) live
here, in a JSON sidecar beside the queue store keyed by ``<owner/repo>#<n>``.
Contract and recovery procedure: docs/github-pipeline-sidecar.md.
"""
from __future__ import annotations

import copy
import json
import os
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

# Fields the sidecar owns. Everything else on the item belongs to the issue.
PIPELINE_FIELDS = ("gates", "plan", "stage_session", "stage_retired")
_HOLD_KEYS = ("work_it", "auto_eligible", "manual_eligible", "claimable")

_CACHE: Dict[str, Any] = {"sig": None, "data": None}
_OWNER: Dict[str, str] = {}

# Stamped on every GitHub claim this code makes. Gating a ticket requires every
# machine that claimed the queue within READER_WINDOW_S to have stamped it, so
# a host whose `wt` would ignore the plan hold cannot be claiming there (OPS-203).
PIPELINE_PROTO = 1
READER_WINDOW_S = 14 * 86400


class SidecarCorrupt(RuntimeError):
    pass


def path() -> Path:
    from . import queue as q
    return q._resolve_store_path().parent / "github-pipeline.json"


def key(item: Dict[str, Any]) -> str:
    return f"{item.get('github_repo') or ''}#{item.get('number')}"


def owner_id() -> str:
    """This store's persistent pipeline owner id. ``machine_tag()`` is three
    characters and can collide across hosts, so ownership keys on a UUID made
    once per store (O_EXCL, so concurrent first calls agree)."""
    p = path().with_name("pipeline-owner.json")
    if str(p) in _OWNER:
        return _OWNER[str(p)]
    p.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    except FileExistsError:
        oid = ""
        for _ in range(50):  # a concurrent creator may not have written yet
            try:
                oid = str(json.loads(p.read_text()).get("id") or "")
            except json.JSONDecodeError:
                oid = ""
            if oid:
                break
            time.sleep(0.02)
        if not oid:
            raise SidecarCorrupt(f"{p} has no owner id; refusing to guess one")
    else:
        oid = uuid.uuid4().hex
        with os.fdopen(fd, "w") as fh:
            json.dump({"id": oid, "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}, fh)
    _OWNER[str(p)] = oid
    return oid


def owner_marker() -> str:
    """The ``pipeline_host`` value this store writes: ``<machine_tag>/<owner id>``."""
    from . import queue as q
    return f"{q.machine_tag()}/{owner_id()}"


def _mine(marker: str) -> bool:
    """A bare tag is the OPS-201 marker format; it stays ours while it equals
    our tag, and the next gate write upgrades it to the owner-id form."""
    from . import queue as q
    if "/" in marker:
        return marker == owner_marker()
    return marker == q.machine_tag()


def _owner_fields(item: Dict[str, Any], marker: str) -> None:
    host, _, oid = marker.partition("/")
    item["pipeline_owner_host"] = host
    item["pipeline_owner_id"] = oid


def stale_readers(items: List[Dict[str, Any]], now: Optional[float] = None) -> Dict[str, str]:
    """Machines (other than this one) whose latest claim in the window was made
    by a `wt` that does not stamp PIPELINE_PROTO, mapped to that claim's time."""
    from . import queue as q
    cutoff = time.strftime("%Y-%m-%dT%H:%M:%SZ",
                           time.gmtime((now or time.time()) - READER_WINDOW_S))
    latest: Dict[str, Any] = {}
    for it in items:
        for h in it.get("history") or []:
            at, machine = str(h.get("at") or ""), str(h.get("machine") or "")
            if h.get("event") != "claim" or not machine or at < cutoff:
                continue
            if machine not in latest or at > latest[machine][0]:
                latest[machine] = (at, h.get("pipeline"))
    return {m: at for m, (at, proto) in latest.items()
            if m != q.machine_tag() and int(proto or 0) < PIPELINE_PROTO}


def _read_strict() -> Dict[str, Any]:
    p = path()
    try:
        raw = p.read_text()
    except FileNotFoundError:
        return {"version": 1, "items": {}}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise SidecarCorrupt(f"{p} is not valid JSON ({exc}); refusing to overwrite it") from exc
    if not isinstance(data, dict) or not isinstance(data.get("items"), dict):
        raise SidecarCorrupt(f"{p} has no items map; refusing to overwrite it")
    return data


def signature() -> Any:
    """Changes whenever the sidecar does (list memos key on it)."""
    try:
        st = path().stat()
    except FileNotFoundError:
        return None
    return (st.st_mtime_ns, st.st_size, st.st_ino)


def _read_cached() -> Optional[Dict[str, Any]]:
    """Lock-free read for the overlay (writes are atomic replaces). None when
    the file is corrupt: every marked ticket then fails closed."""
    p = path()
    try:
        st = p.stat()
        sig = (str(p), st.st_mtime_ns, st.st_size, st.st_ino)
    except FileNotFoundError:
        return {"version": 1, "items": {}}
    if _CACHE["sig"] != sig:
        try:
            _CACHE["data"] = _read_strict()
        except SidecarCorrupt:
            _CACHE["data"] = None
        _CACHE["sig"] = sig
    return _CACHE["data"]


def _write(data: Dict[str, Any]) -> None:
    p = path()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_name(f".{p.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(data, indent=1, sort_keys=True))
    os.replace(tmp, p)


def _hold(item: Dict[str, Any]) -> None:
    for k in _HOLD_KEYS:
        item[k] = False


def overlay(item: Dict[str, Any]) -> Dict[str, Any]:
    """Apply sidecar state to a freshly built GitHub item (in place)."""
    host = str(item.get("pipeline_host") or "")
    if host:
        _owner_fields(item, host)
    if host and not _mine(host):
        item["pipeline_foreign"] = True
        _hold(item)
        return item
    data = _read_cached()
    entry = (data or {}).get("items", {}).get(key(item)) if data is not None else None
    if host and entry is None:
        item["pipeline_missing"] = True
        _hold(item)
        return item
    if entry is None:
        return item
    _apply(item, entry)
    item["pipeline_local"] = True   # this host's stage tick owns the pipeline
    from . import queue as q
    if q.plan_pending(item):
        _hold(item)
    return item


def _apply(item: Dict[str, Any], entry: Dict[str, Any]) -> None:
    for f in PIPELINE_FIELDS:
        if f in entry:
            item[f] = copy.deepcopy(entry[f])
    extra = entry.get("history") or []
    if extra:
        item["history"] = sorted(list(item.get("history") or []) + copy.deepcopy(extra),
                                 key=lambda h: str(h.get("at") or ""))


def _backend_for(ident: Any):
    from . import queue as q
    return q._github_backend_for_project(q._project_from_ident(ident))


def _fence(item: Dict[str, Any]) -> None:
    from . import queue as q
    if item.get("pipeline_foreign"):
        raise ValueError(f"{item.get('ref')}: its pipeline is owned by host "
                         f"{item.get('pipeline_owner_host')!r} (owner id "
                         f"{item.get('pipeline_owner_id') or 'legacy'}), not this store "
                         f"({owner_marker()}); inspect it there with `wt plan show {item.get('ref')}`")
    if item.get("pipeline_missing"):
        raise ValueError(f"{item.get('ref')}: marked for this host's pipeline but the "
                         f"sidecar entry is missing; re-run `wt edit {item.get('ref')} --gate ...`")


def mutate(ident: Any, fn: Callable[[Dict[str, Any]], Any],
           skip_returns_item: bool = False) -> Optional[Dict[str, Any]]:
    """Run ``fn(item)`` against the GitHub item + sidecar state under the
    store lock and persist the pipeline fields it changed. GitHub is read
    BEFORE the lock; if it is unreachable the sidecar is never touched. A
    ticket with no sidecar entry has no pipeline: returns None."""
    from . import queue as q
    backend = _backend_for(ident)
    base = backend.get(ident)
    if base is None:
        return None
    _fence(base)
    k = key(base)
    with q._FileLock(q._lock_path()):
        data = _read_strict()
        entry = data["items"].get(k)
        if entry is None:
            return None
        it = copy.deepcopy(base)
        for f in PIPELINE_FIELDS:
            it.pop(f, None)
        hist_base = [h for h in (it.get("history") or [])
                     if h not in (entry.get("history") or [])]
        it["history"] = list(hist_base)
        _apply(it, entry)
        before_len = len(it["history"])
        before_input = (bool(it.get("needs_input")), str(it.get("block_question") or ""))
        if fn(it) == "skip":
            return it if skip_returns_item else None
        new_hist = (it.get("history") or [])[before_len:]
        for f in PIPELINE_FIELDS:
            if f in it:
                entry[f] = copy.deepcopy(it[f])
            else:
                entry.pop(f, None)
        entry["history"] = list(entry.get("history") or []) + copy.deepcopy(new_hist)
        entry["rev"] = int(entry.get("rev") or 0) + 1
        entry["updated_at"] = q._now_iso()
        _write(data)
    after_input = (bool(it.get("needs_input")), str(it.get("block_question") or ""))
    if before_input[0] and not after_input[0]:
        # plan_decide clears the human block; that block lives on the issue.
        backend.update(ident, needs_input=False, block_question="")
    return it


def set_gates(ident: Any, gates: List[str]) -> Optional[Dict[str, Any]]:
    """`wt edit <ref> --gate ...` on a GitHub ref. Milestone 1 supports plan
    gates only; [] clears the pipeline (entry first, then the marker, so a
    crash in between fails closed)."""
    from . import queue as q
    gates = q.validate_gates(gates) if gates else []
    unsupported = [g for g in gates if not (g == "plan" or g.startswith("plan:"))]
    if unsupported:
        raise ValueError(f"{', '.join(unsupported)}: only plan / plan:<model> gates are "
                         f"supported on GitHub-backed queues yet (OPS-201 milestone 2)")
    backend = _backend_for(ident)
    item = backend.get(ident)
    if item is None:
        return None
    if item.get("pipeline_foreign"):
        _fence(item)
    k = key(item)
    if (gates and item.get("claimed_by") and item.get("plan") is None
            and item.get("claimed_by") != os.environ.get("WT_WORKER_ID")):
        # The claimant may gate its own build (it then waits for the plan).
        # A running builder would not pause for the planner: refuse rather
        # than plan alongside a build. The claim itself is left untouched.
        raise ValueError(f"{item.get('ref')} is claimed by {item.get('claimed_by')}; a plan "
                         f"gate cannot pause a running build. Gate it once released.")
    if gates:
        marker = owner_marker()
        if item.get("pipeline_host") != marker:
            stale = stale_readers(backend.list_items(None))
            if stale:
                raise ValueError(
                    f"{item.get('ref')}: cannot gate yet; these hosts claimed this queue "
                    f"with a wt that ignores the plan hold: "
                    + ", ".join(f"{m} (last claim {at})" for m, at in sorted(stale.items()))
                    + ". Update them; a fresh claim from each clears this.")
            backend.update(ident, pipeline_host=marker)
            # No CAS on issue edits: trust the persisted marker, not our write.
            # A racing host that wrote last owns it; we write no sidecar entry.
            persisted = backend.get(ident) or {}
            if persisted.get("pipeline_host") != marker:
                raise ValueError(f"{item.get('ref')}: pipeline marker write lost to "
                                 f"{persisted.get('pipeline_host') or 'a concurrent clear'!r}; "
                                 f"not gated")
        with q._FileLock(q._lock_path()):
            data = _read_strict()
            entry = data["items"].setdefault(k, {"plan": {}, "history": [], "rev": 0})
            entry["gates"] = list(gates)
            entry["ref"] = item.get("ref")
            entry["host"] = q.machine_tag()
            entry["owner"] = marker
            entry["rev"] = int(entry.get("rev") or 0) + 1
            entry["updated_at"] = q._now_iso()
            _write(data)
    else:
        with q._FileLock(q._lock_path()):
            data = _read_strict()
            if data["items"].pop(k, None) is not None:
                _write(data)
        if item.get("pipeline_host"):
            backend.update(ident, pipeline_host="")
    return backend.get(ident)

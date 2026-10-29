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
# The review fields (OPS-204) carry the verify/review/cmd hold: while
# ``review_hold`` is set the issue stays open and the overlay shows in_review.
REVIEW_FIELDS = ("gate_pending", "gate_stages", "gate_results", "verify_cycle",
                 "verifier", "gate_feedback", "gate_accepted_by", "review_hold")
PIPELINE_FIELDS = ("gates", "plan", "stage_session", "stage_retired") + REVIEW_FIELDS
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
    if entry is not None and entry.get("tombstone"):
        entry = None
    if host and entry is None:
        item["pipeline_missing"] = True
        _hold(item)
        return item
    if entry is None:
        return item
    _apply(item, entry)
    item["pipeline_local"] = True   # this host's stage tick owns the pipeline
    from . import queue as q
    if q.plan_pending(item) or item.get("status") == "in_review":
        _hold(item)
    return item


def _hold_live(hold: Dict[str, Any], issue_status: str) -> bool:
    """A review hold still owns the ticket until its final issue write landed."""
    final = str(hold.get("final") or "")
    if final == "close":
        return issue_status != "closed"
    if final == "open":
        return issue_status == "in_progress"
    return issue_status != "closed"   # closed on GitHub by hand: the hold is moot


def _apply(item: Dict[str, Any], entry: Dict[str, Any]) -> None:
    item.setdefault("issue_status", item.get("status"))
    for f in PIPELINE_FIELDS:
        if f in entry:
            item[f] = copy.deepcopy(entry[f])
    item["pipeline_gen"] = int(entry.get("gen") or 0)
    item["pipeline_retired_workers"] = list(entry.get("retired_workers") or [])
    hold = entry.get("review_hold")
    if hold and _hold_live(hold, str(item["issue_status"] or "")):
        item["status"] = "in_review"
        item["closed_at"] = None
        if hold.get("resolution"):   # the close's resolution, held with the issue open
            item["resolution"] = copy.deepcopy(hold["resolution"])
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
    ticket with no sidecar entry has no pipeline: returns None.

    The issue fields ``fn`` may change are written back after the lock: a
    cleared human block, and an in_progress -> open transition (plan_decide
    releasing a claimless build, #6) as a CAS reopen of the issue."""
    from . import queue as q
    backend = _backend_for(ident)
    base = backend.get(ident)
    if base is None:
        return None
    _fence(base)
    k = key(base)
    issue_status = str(base.get("issue_status") or base.get("status") or "")
    with q._FileLock(q._lock_path()):
        data = _read_strict()
        entry = data["items"].get(k)
        if entry is None or entry.get("tombstone"):
            return None
        hold = entry.get("review_hold")
        healed = bool(hold and hold.get("final") and not _hold_live(hold, issue_status))
        if healed:
            _settle(entry, hold)   # the final issue write landed; drop the hold
        it = copy.deepcopy(base)
        for f in PIPELINE_FIELDS:
            it.pop(f, None)
        it["status"] = issue_status
        it.pop("issue_status", None)
        hist_base = [h for h in (it.get("history") or [])
                     if h not in (entry.get("history") or [])]
        it["history"] = list(hist_base)
        _apply(it, entry)
        before_len = len(it["history"])
        before_status = it.get("status")
        before_input = (bool(it.get("needs_input")), str(it.get("block_question") or ""))
        if fn(it) == "skip":
            if healed:
                _write(data)
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
    if before_status == "in_progress" and it.get("status") == "open":
        if backend.update_status(ident, "open", reason="released by the plan stage",
                                 require_status="in_progress") is None:
            q._log("PIPELINE", f"{it.get('ref')}: issue moved before the plan stage's "
                   f"reopen landed; left as is", queue=str(it.get("project") or ""))
    return it


def _settle(entry: Dict[str, Any], hold: Dict[str, Any]) -> None:
    entry.pop("review_hold", None)
    for f in ("gate_pending", "gate_stages"):
        entry.pop(f, None)
    if hold.get("final") == "open" and hold.get("reason"):
        entry["gate_feedback"] = str(hold["reason"])


def fence_stale_stage(it: Dict[str, Any], by: str) -> None:
    """A stage session of a cleared pipeline generation may not write into
    the next one (OPS-204): its worker id was retired with the old entry."""
    from . import queue as q
    if by and by in (it.get("pipeline_retired_workers") or []):
        raise q.GroupFenced(f"{it.get('ref')}: {by} belongs to a cleared pipeline "
                            f"generation (now g{it.get('pipeline_gen') or 0}); write refused")


def _stage_workers(entry: Dict[str, Any]) -> List[str]:
    plan = entry.get("plan") or {}
    infos = [plan.get("planner"), plan.get("reviewer"), entry.get("verifier"),
             entry.get("stage_session")]
    infos += list(plan.get("role_history") or []) + list(entry.get("stage_retired") or [])
    return sorted({str(i.get("worker_id")) for i in infos
                   if isinstance(i, dict) and i.get("worker_id")})


def hold_review(ident: Any, resolution: Any, session_id: str, session_uuid: str,
                results: List[Dict[str, Any]], stages: List[str]) -> Optional[Dict[str, Any]]:
    """A gated close on GitHub: keep the issue open and record the review
    stage in the sidecar; the overlay then shows the ticket in_review."""
    from . import queue as q
    norm = q._normalize_resolution(resolution) or {}

    def _do(it):
        if it.get("status") not in ("open", "in_progress"):
            raise ValueError(f"{it.get('ref')} is {it.get('status')}; nothing to hold for review")
        owner = str(it.get("claimed_by") or "")
        if session_id and it.get("status") == "in_progress" and owner and owner != session_id:
            raise ValueError(f"{it.get('ref')} is claimed by {owner}; you are {session_id}. "
                             f"Only the claiming worker may close an in-progress ticket.")
        now = q._now_iso()
        it["review_hold"] = {"resolution": norm, "session_id": str(session_id or ""),
                             "session_uuid": str(session_uuid or ""), "at": now}
        it["gate_results"] = list(results)
        it["gate_stages"] = list(stages)
        it["gate_pending"] = stages[0]
        it.pop("gate_feedback", None)
        if stages[0] == "verify":
            it["verify_cycle"] = int(it.get("verify_cycle") or 0) + 1
        q._append_history(it, "in_review", by=q._by("worker", str(session_id or "")),
                          at=now, gate=stages[0])
        it["status"], it["closed_at"] = "in_review", None
    return mutate(ident, _do)


def record_results(ident: Any, results: List[Dict[str, Any]],
                   feedback: str = "") -> Optional[Dict[str, Any]]:
    """cmd-gate results of a GitHub close (pass: it closed; fail: reopened)."""
    def _do(it):
        it["gate_results"] = list(results)
        if feedback:
            it["gate_feedback"] = feedback
        else:
            it.pop("gate_feedback", None)
    return mutate(ident, _do)


def set_verifier(ident: Any, info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    def _do(it):
        it["verifier"] = dict(info)
    return mutate(ident, _do)


def _finalize(ident: Any, it: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """The ONE canonical issue write of a settled review, then drop the hold.
    The hold carries ``final`` until the write lands, so a failed write (GitHub
    down) leaves the ticket in_review and `wt accept` retries it."""
    from . import queue as q
    hold = it.get("review_hold") or {}
    final = hold.get("final")
    backend = _backend_for(ident)
    if final == "close":
        backend.update_status(ident, "closed", str(hold.get("session_id") or ""),
                              session_uuid=str(hold.get("session_uuid") or ""),
                              resolution=hold.get("resolution") or None)
    elif final == "open":
        backend.update_status(ident, "open", reason=str(hold.get("reason") or ""))
    else:
        return it

    def _clear(cur):
        h = cur.get("review_hold") or {}
        if h.get("final") != final or h.get("at") != hold.get("at"):
            return "skip"
        cur.pop("review_hold", None)
        cur.pop("gate_pending", None)
        cur.pop("gate_stages", None)
        if final == "open":
            cur["gate_feedback"] = str(hold.get("reason") or "")
    return mutate(ident, _clear, skip_returns_item=True) or q.get(ident)


def _decide(ident: Any, check: Callable[[Dict[str, Any]], Optional[str]],
            final: str, reason: str = "", result: Optional[Dict[str, Any]] = None,
            accepted_by: str = "") -> Optional[Dict[str, Any]]:
    """Mark the hold's outcome under the lock (``check`` raises to refuse,
    returns a next stage to advance instead of finishing), then finalize."""
    from . import queue as q
    nxt: List[str] = []

    def _do(it):
        hold = it.get("review_hold")
        if it.get("status") != "in_review" or not hold:
            raise ValueError(f"{it.get('ref')} is {it.get('status')}, not in_review")
        if hold.get("final"):
            return None   # already decided; finalize (retry) below
        stage = check(it)
        now = q._now_iso()
        if result:
            it["gate_results"] = list(it.get("gate_results") or []) + [result]
        if stage:
            it["gate_stages"] = [s for s in (it.get("gate_stages") or []) if s != "verify"]
            it["gate_pending"] = stage
            nxt.append(stage)
            return None
        hold = dict(hold, final=final, decided_at=now)
        if reason:
            hold["reason"] = q._clip(reason, 4000)
        if accepted_by:
            it["gate_accepted_by"] = accepted_by
            q._append_history(it, "accept", by=q._by("human", accepted_by), at=now)
        it["review_hold"] = hold
    it = mutate(ident, _do)
    if it is None:
        return None
    if nxt:
        it["_next_stage"] = nxt[0]
        return it
    return _finalize(ident, it)


def verdict(ident: Any, passed: bool, findings: str, by: str,
            expect_verify_cycle: Optional[int]) -> Optional[Dict[str, Any]]:
    """The verifier's verdict on a GitHub ticket. The cycle is required: a
    verifier of an earlier cycle (or generation) is refused, never applied."""
    from . import queue as q
    if expect_verify_cycle is None:
        raise ValueError("a GitHub-backed verdict needs --verify-cycle <n> "
                         "(the cycle in your verifier prompt)")
    entry = {"gate": "verify", "passed": bool(passed),
             "output_tail": q._clip(findings, q.GATE_OUTPUT_TAIL), "by": str(by),
             "at": q._now_iso()}

    def _check(it):
        if it.get("gate_pending") != "verify":
            raise ValueError(f"{it.get('ref')} is not waiting on a verifier")
        fence_stale_stage(it, by)
        vc = int(it.get("verify_cycle") or 0)
        if int(expect_verify_cycle) != vc:
            raise q.GroupFenced(f"{it.get('ref')}: verdict for verify cycle "
                                f"{expect_verify_cycle}, ticket is on cycle {vc}")
        v = it.get("verifier") or {}
        if v.get("engine"):
            entry["engine"], entry["model"] = v.get("engine"), v.get("model", "")
        q._append_history(it, "verify", by=q._by("system"), at=entry["at"],
                          passed=bool(passed), findings=q._clip(findings, 500))
        if passed:
            rest = [s for s in (it.get("gate_stages") or []) if s != "verify"]
            return rest[0] if rest else None
        return None
    why = ("" if passed else "independent verification failed: "
           + (q._clip(findings, 3000) or "(no findings given)"))
    return _decide(ident, _check, "close" if passed else "open", reason=why,
                   result=entry, accepted_by=f"verifier:{by}" if passed else "")


def accept(ident: Any, by: str, force: bool) -> Optional[Dict[str, Any]]:
    def _check(it):
        if it.get("gate_pending") == "verify" and not force:
            raise ValueError(f"{it.get('ref')} is waiting on its independent "
                             f"verifier's verdict (wt verdict); --force overrides")
        return None
    return _decide(ident, _check, "close", accepted_by=str(by))


def reject(ident: Any, why: str) -> Optional[Dict[str, Any]]:
    return _decide(ident, lambda it: None, "open", reason=why)


def set_gates(ident: Any, gates: List[str]) -> Optional[Dict[str, Any]]:
    """`wt edit <ref> --gate ...` / `wt add --gate` on a GitHub ref. [] clears
    the pipeline (entry first, then the marker, so a crash in between fails
    closed). A clear leaves a tombstone carrying the generation, the verify
    cycle and the retired stage workers, so a re-gate starts generation n+1
    whose stage keys, cycles and writers never collide with the old ones."""
    from . import queue as q
    gates = q.validate_gates(gates) if gates else []
    backend = _backend_for(ident)
    item = backend.get(ident)
    if item is None:
        return None
    if item.get("pipeline_foreign"):
        _fence(item)
    if item.get("status") == "in_review":
        raise ValueError(f"{item.get('ref')} is in review; settle it (wt accept / "
                         f"wt reject) before changing its gates")
    k = key(item)
    if (gates and item.get("claimed_by") and item.get("plan") is None
            and q.plan_gate({"gates": gates}) is not None
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
                    f"with a wt that ignores the pipeline hold: "
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
            entry = data["items"].get(k)
            if entry is None or entry.get("tombstone"):
                old = entry or {}
                entry = data["items"][k] = {
                    "plan": {}, "history": [], "rev": int(old.get("rev") or 0),
                    "gen": int(old.get("gen") or 0) + 1,
                    "verify_cycle": int(old.get("verify_cycle") or 0),
                    "retired_workers": list(old.get("retired_workers") or [])}
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
            entry = data["items"].get(k)
            if entry is not None and not entry.get("tombstone"):
                data["items"][k] = {
                    "tombstone": True, "ref": item.get("ref"),
                    "gen": int(entry.get("gen") or 0),
                    "verify_cycle": int(entry.get("verify_cycle") or 0),
                    "retired_workers": sorted(set(entry.get("retired_workers") or [])
                                              | set(_stage_workers(entry))),
                    "rev": int(entry.get("rev") or 0) + 1, "updated_at": q._now_iso()}
                _write(data)
        if item.get("pipeline_host"):
            backend.update(ident, pipeline_host="")
    return backend.get(ident)

"""Durable outcomes: responsibility for a result, outside the executable queue.

An outcome is a stable aim with versioned acceptance criteria, an accountable
role and one explicit next action. It is NOT a ticket: outcomes live in their
own store (``outcomes.json`` in the data dir), so no claim, dispatch, resume
or takeover path can ever pick one up. Work that serves an outcome is ordinary
queue tickets with ordinary gates, linked by a ``serves`` link.

Runtime model (no resident agents, no new timers):

* The existing daemon tick calls :func:`reconcile`. It is deterministic
  bookkeeping: it observes linked tickets and running attempts, turns changes
  into deduplicated events, derives health, and starts at most a bounded
  number of short role attempts.
* Three temporary roles: ``lead`` (diagnose, choose the next reversible step),
  ``executor`` (an ordinary ticket worker), ``verifier`` (independent check of
  an immutable artifact against a criterion revision).
* Every lead attempt must end in a concrete result: dispatched work, an
  explicit dated/triggered next action, a prepared decision, a verification
  request, or a justified stop proposal. Suggestions alone are rejected.
* Acceptance is one function (:func:`_try_accept`): every required criterion
  needs passing evidence at the current criteria revision from a trusted
  deterministic gate or an independent verifier attempt. Model assertions,
  executor scores and ticket closure never deliver an outcome.

The whole feature is default OFF (``outcomes-settings.json``). Disabled, the
reconciler starts nothing new and the queue hooks leave existing behaviour
untouched; already running attempts are not killed.

Fencing is cooperative: attempt fences guard every state write into this
store and queued ticket operations carry dedupe keys, but nothing here can
stop a stale agent holding unrestricted shell credentials from acting
externally. This is not exactly-once execution.

Lock order: queue lock -> outcomes lock. This module never takes the queue
lock (or calls queue mutators) while holding the outcomes lock.
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
import re
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple

from . import storage

SCHEMA_VERSION = 1

STATES = ("proposed", "active", "parked", "delivered", "stopped")
# Input aliases accepted on the wire; output always uses STATES.
_STATE_ALIASES = {"draft": "proposed", "paused": "parked"}
# Transitions an explicit ``update`` may make. ``delivered`` is reachable only
# through the acceptance function; a regression after delivery is a new outcome.
_TRANSITIONS = {
    "proposed": ("active", "stopped"),
    "active": ("parked", "stopped"),
    "parked": ("proposed", "active", "stopped"),
    "stopped": ("active",),
    "delivered": (),
}
ROLES = ("lead", "executor", "verifier")
LINK_KINDS = ("ticket", "session", "outcome", "artifact", "decision")
LINK_RELATIONS = ("serves", "supports", "related", "continuation")
EVIDENCE_METHODS = ("independent_verification", "ticket_gate")
EVIDENCE_RESULTS = ("pass", "fail", "inconclusive")
RESULT_KINDS = ("dispatched_work", "next_action", "decision", "verification_request", "stop")
WORK_MODES = ("new_ticket", "link_ticket", "retry_stage", "resume_supervision")
EXECUTOR_RESULTS = ("artifact_produced", "measured_improvement", "inconclusive",
                    "disproved_hypothesis", "blocked")
EVENT_TYPES = (
    "admitted", "child_result", "criterion_failed", "delivery_uncertain",
    "ticket_exhausted", "stage_exhausted", "partial_close", "assessment_unapplied",
    "deadline_due", "external_event", "finding", "dispatch_failed",
    "verification_failed", "owner_answer", "acceptance_rejected", "ownership_gap",
    "criteria_changed", "recovery_retry", "owner_review", "manual",
    "incident_observed", "incident_recovered",
)
# Events that only update state and never need a lead.
_QUIET_EVENTS = ("recovery_retry",)
FINDING_DISPOSITIONS = ("applied", "declined", "parked")
ID_RE = re.compile(r"^oc-[0-9a-f]{12}$")
_ATTEMPT_RE = re.compile(r"^at-[0-9a-f]{12}$")

DEFAULT_LIMITS: Dict[str, float] = {
    # Wall-clock ceilings, enforced by the reconciler (kill + record).
    "lead_wall_s": 480,
    "verifier_wall_s": 600,
    # Declared output ceilings; passed to the role brief. Engines expose no
    # hard output cap through the ad-hoc spawn path, so these are advisory.
    "lead_output_tokens": 12000,
    "verifier_output_tokens": 12000,
    # One technical retry per attempt; two recovery wakes per event.
    "technical_retries": 1,
    "recovery_attempts_per_event": 2,
    # Substantive lead wakes per outcome per UTC day before visible deferral.
    "lead_wakes_per_day": 3,
    "verifier_attempts_per_day": 4,
    # Concurrent outcome attempts (lead + verifier) across ALL outcomes. Only
    # outcome attempts count; ordinary queue workers are never throttled here.
    "max_concurrent_attempts": 2,
    "spawns_per_tick": 1,
    # Event coalescing: wait this long after the newest event (capped by
    # coalesce_max_s after the oldest) so a burst becomes one wake.
    "coalesce_s": 20,
    "coalesce_max_s": 120,
    "retry_backoff_s": 60,
    "recovery_backoff_s": 300,
    "spawn_receipt_grace_s": 120,
}

_KEEP = {"events": 200, "history": 100, "attempts": 100, "evidence": 200,
         "findings": 200, "decision_refs": 100, "sessions": 500, "links": 500}
_FEED_KEEP = 500
_EVENT_KEYS_KEEP = 1000


class OutcomeError(Exception):
    def __init__(self, code: str, message: str, **extra: Any):
        super().__init__(message)
        self.code = code
        self.message = message
        self.extra = extra


# ------------------------------------------------------------------ helpers

def _now() -> float:
    return time.time()


def _iso(ts: Optional[float] = None) -> str:
    return datetime.fromtimestamp(_now() if ts is None else ts, tz=timezone.utc
                                  ).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso(value: Any) -> Optional[float]:
    if not value:
        return None
    s = str(value).strip()
    for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            pass
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def _day(ts: Optional[float] = None) -> str:
    return datetime.fromtimestamp(_now() if ts is None else ts, tz=timezone.utc).strftime("%Y-%m-%d")


def _next_day_iso(ts: float) -> str:
    d = datetime.fromtimestamp(ts, tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    return (d + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _hex(n: int = 12) -> str:
    return uuid.uuid4().hex[:n]


def _digest(*parts: Any) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(json.dumps(p, sort_keys=True, default=str).encode())
        h.update(b"\0")
    return h.hexdigest()


def _clip(value: Any, n: int) -> str:
    s = "" if value is None else str(value)
    return s if len(s) <= n else s[: n - 1] + "…"


def _text(payload: Dict[str, Any], key: str, n: int, *, required: bool = False) -> str:
    v = payload.get(key)
    if v is None:
        v = ""
    if not isinstance(v, (str, int, float)):
        raise OutcomeError("invalid", f"{key} must be a string")
    s = str(v).strip()
    if required and not s:
        raise OutcomeError("invalid", f"{key} is required")
    if len(s) > n:
        raise OutcomeError("invalid", f"{key} exceeds {n} characters")
    return s


def _bounded(lst: List[Any], key: str) -> List[Any]:
    keep = _KEEP.get(key, 200)
    return lst[-keep:] if len(lst) > keep else lst


def _log(verb: str, detail: str, queue: str = "") -> None:
    try:
        from . import queue as q
        q._log(verb, detail, queue=queue)
    except Exception:  # noqa: BLE001 - logging never breaks bookkeeping
        pass


# ----------------------------------------------------------------- settings

def _settings_path() -> Path:
    return storage.data_dir() / "outcomes-settings.json"


def settings() -> Dict[str, Any]:
    """Durable runtime gate. Missing or unreadable file = disabled."""
    try:
        raw = json.loads(_settings_path().read_text())
    except (OSError, ValueError):
        raw = {}
    if not isinstance(raw, dict):
        raw = {}
    lim = dict(DEFAULT_LIMITS)
    for k, v in (raw.get("limits") or {}).items():
        if k in DEFAULT_LIMITS and isinstance(v, (int, float)) and not isinstance(v, bool) and v >= 0:
            lim[k] = v
    return {"enabled": raw.get("enabled") is True, "limits": lim,
            "updated_at": raw.get("updated_at") or "", "schema_version": SCHEMA_VERSION}


def enabled() -> bool:
    return settings()["enabled"]


def limits() -> Dict[str, float]:
    return settings()["limits"]


def set_settings(enabled_flag: Optional[bool] = None,
                 limit_overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    path = _settings_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with storage.file_lock(path.with_suffix(".lock")):
        try:
            raw = json.loads(path.read_text())
            if not isinstance(raw, dict):
                raw = {}
        except (OSError, ValueError):
            raw = {}
        if enabled_flag is not None:
            raw["enabled"] = bool(enabled_flag)
        if limit_overrides is not None:
            if not isinstance(limit_overrides, dict):
                raise OutcomeError("invalid", "limits must be an object")
            cur = dict(raw.get("limits") or {})
            for k, v in limit_overrides.items():
                if k not in DEFAULT_LIMITS:
                    raise OutcomeError("invalid", f"unknown limit {k!r}")
                if v is None:
                    cur.pop(k, None)
                    continue
                if not isinstance(v, (int, float)) or isinstance(v, bool) or v < 0:
                    raise OutcomeError("invalid", f"limit {k} must be a non-negative number")
                cur[k] = v
            raw["limits"] = cur
        raw.update(schema_version=SCHEMA_VERSION, updated_at=_iso())
        raw.setdefault("enabled", False)
        with storage.atomic_destination(path) as tmp:
            tmp.write_text(json.dumps(raw, indent=2, sort_keys=True) + "\n")
    _log("OUTCOME", f"settings enabled={raw['enabled']}")
    return settings()


# -------------------------------------------------------------------- store

def _store_path() -> Path:
    return storage.data_dir() / "outcomes.json"


def _empty() -> Dict[str, Any]:
    return {"schema_version": SCHEMA_VERSION, "outcomes": {}, "outbox": [],
            "feed": [], "feed_seq": 0}


def _load() -> Dict[str, Any]:
    """Read the store. Missing = empty. Corrupt = fail closed (never rewrite)."""
    path = _store_path()
    try:
        raw = path.read_text()
    except FileNotFoundError:
        return _empty()
    try:
        data = json.loads(raw)
    except ValueError as exc:
        raise OutcomeError("store_corrupt", f"outcome store is not valid JSON: {exc}") from exc
    if not isinstance(data, dict) or not isinstance(data.get("outcomes"), dict):
        raise OutcomeError("store_corrupt", "outcome store root is malformed")
    for k, v in _empty().items():
        data.setdefault(k, copy.deepcopy(v))
    return data


def _fingerprint(o: Dict[str, Any]) -> str:
    return json.dumps({k: v for k, v in o.items() if k not in ("revision", "updated_at")},
                      sort_keys=True, default=str)


@contextmanager
def _txn():
    """Serialized read-modify-write. Bumps ``revision`` of each changed outcome."""
    path = _store_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with storage.file_lock(path.with_name("outcomes.lock")):
        data = _load()
        before = json.dumps(data, sort_keys=True, default=str)
        prints = {oid: _fingerprint(o) for oid, o in data["outcomes"].items()}
        yield data
        now = _iso()
        for oid, o in data["outcomes"].items():
            if prints.get(oid) != _fingerprint(o):
                if oid in prints:
                    o["revision"] = int(o.get("revision") or 0) + 1
                o["updated_at"] = now
        if json.dumps(data, sort_keys=True, default=str) != before:
            data["outbox"] = [x for x in data["outbox"]
                              if x.get("state") in ("pending", "sending")
                              or (_parse_iso(x.get("updated_at")) or 0) > _now() - 7 * 86400][-1000:]
            data["feed"] = data["feed"][-_FEED_KEEP:]
            with storage.atomic_destination(path) as tmp:
                tmp.write_text(json.dumps(data, sort_keys=True, default=str))


def _get(data: Dict[str, Any], oid: Any) -> Dict[str, Any]:
    oid = str(oid or "")
    if not ID_RE.match(oid):
        raise OutcomeError("invalid", "id must look like oc-<12 hex>")
    o = data["outcomes"].get(oid)
    if o is None:
        raise OutcomeError("not_found", f"outcome {oid} not found")
    return o


def _history(o: Dict[str, Any], event: str, **fields: Any) -> None:
    o["history"] = _bounded(list(o.get("history") or []) + [dict(event=event, at=_iso(), **fields)],
                            "history")


def _feed(data: Dict[str, Any], o: Dict[str, Any], kind: str, text: str, key: str = "") -> None:
    """Notification intent, committed in the same write as the state change.

    Consumers read it with ``api feed`` (cursor = ``seq``); a stable ``key``
    deduplicates repeats."""
    key = key or f"{o['id']}:{kind}:{_digest(text)[:12]}"
    if any(f.get("key") == key for f in data["feed"][-200:]):
        return
    data["feed_seq"] = int(data.get("feed_seq") or 0) + 1
    data["feed"].append({"seq": data["feed_seq"], "key": key, "outcome_id": o["id"],
                         "kind": kind, "text": _clip(text, 500), "at": _iso()})


# --------------------------------------------------------------- validation

def _norm_queue(value: str) -> str:
    if not value:
        return ""
    try:
        from . import queue as q
        return q._norm_project(value)
    except Exception:  # noqa: BLE001
        return value.strip().upper()


def _norm_repo(value: str) -> str:
    if not value:
        return ""
    p = Path(value).expanduser()
    if not p.is_absolute():
        raise OutcomeError("invalid", "repo_path must be absolute")
    if not p.is_dir():
        raise OutcomeError("invalid", f"repo_path {value} is not a directory")
    return str(p.resolve())


def _norm_criteria(raw: Any, existing: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    if not isinstance(raw, list) or not raw:
        raise OutcomeError("invalid", "criteria must be a non-empty list")
    if len(raw) > 30:
        raise OutcomeError("invalid", "at most 30 criteria")
    used = {c["id"] for c in (existing or [])}
    out, seen = [], set()
    n = len(used)
    for c in raw:
        if isinstance(c, str):
            c = {"text": c}
        if not isinstance(c, dict):
            raise OutcomeError("invalid", "each criterion must be an object or string")
        text = _text(c, "text", 2000, required=True)
        cid = _text(c, "id", 40)
        if not cid:
            n += 1
            cid = f"c{n}"
            while cid in used or cid in seen:
                n += 1
                cid = f"c{n}"
        if not re.match(r"^[A-Za-z0-9_.-]{1,40}$", cid) or cid in seen:
            raise OutcomeError("invalid", f"criterion id {cid!r} is invalid or duplicated")
        seen.add(cid)
        accepts = c.get("accepts") or ["independent_verification"]
        if not isinstance(accepts, list) or any(a not in EVIDENCE_METHODS for a in accepts):
            raise OutcomeError("invalid", f"criterion accepts must be a subset of {EVIDENCE_METHODS}")
        crit = {"id": cid, "text": text, "required": c.get("required", True) is not False,
                "accepts": sorted(set(accepts))}
        if c.get("max_age_s") is not None:
            if not isinstance(c["max_age_s"], (int, float)) or c["max_age_s"] <= 0:
                raise OutcomeError("invalid", "max_age_s must be positive")
            crit["max_age_s"] = c["max_age_s"]
        out.append(crit)
    return out


def _norm_next_action(raw: Any, *, allow_user: bool = True) -> Optional[Dict[str, Any]]:
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise OutcomeError("invalid", "next_action must be an object")
    owner = _text(raw, "owner", 20) or "agent"
    owners = ("agent", "external", "user") if allow_user else ("agent", "external")
    if owner not in owners:
        raise OutcomeError("invalid", f"next_action.owner must be one of {owners}")
    due = _text(raw, "due_at", 40)
    if due and _parse_iso(due) is None:
        raise OutcomeError("invalid", "next_action.due_at must be an ISO timestamp")
    trigger = _text(raw, "trigger", 300)
    return {"text": _text(raw, "text", 2000, required=True), "owner": owner,
            "due_at": _iso(_parse_iso(due)) if due else "", "trigger": trigger,
            "set_at": _iso()}


def _norm_authority(raw: Any, queue: str) -> Dict[str, Any]:
    raw = raw if isinstance(raw, dict) else {}
    allowed = raw.get("allowed_queues")
    if allowed is None:
        allowed = [queue] if queue else []
    if not isinstance(allowed, list):
        raise OutcomeError("invalid", "authority.allowed_queues must be a list")
    return {
        "scope": _clip(raw.get("scope") or "Reversible work within the existing authorized task only.", 1000),
        "allowed_queues": sorted({_norm_queue(str(x)) for x in allowed if str(x).strip()}),
        "hard_stops": "Customer or external sends, money and price changes, ad spend, public posts, "
                      "and irreversible actions always remain owner decisions.",
    }


# ------------------------------------------------------------- serialization

def _public(o: Dict[str, Any]) -> Dict[str, Any]:
    out = copy.deepcopy(o)
    out["events"] = out.get("events", [])[-50:]
    out["history"] = out.get("history", [])[-50:]
    for a in out.get("attempts", []):
        a.pop("fence", None)
    out["associations"] = out.get("sessions", [])
    out["session_ids"] = sorted({s["session_id"] for s in out.get("sessions", [])})
    return out


# --------------------------------------------------------------- reads

def list_outcomes(repo_path: str = "", state: str = "", session_id: str = "") -> List[Dict[str, Any]]:
    data = _load()
    rows = list(data["outcomes"].values())
    if repo_path:
        try:
            want = str(Path(repo_path).expanduser().resolve())
        except OSError:
            want = repo_path
        rows = [o for o in rows if o.get("repo_path") == want]
    if state:
        state = _STATE_ALIASES.get(state, state)
        rows = [o for o in rows if o.get("state") == state]
    if session_id:
        rows = [o for o in rows if any(s.get("session_id") == session_id for s in o.get("sessions", []))]
    rows.sort(key=lambda o: (not o.get("commitment"), o.get("rank") if isinstance(o.get("rank"), int) else 10**6,
                             o.get("created_at", "")))
    return [_public(o) for o in rows]


def get(oid: str) -> Dict[str, Any]:
    return _public(_get(_load(), oid))


def linked_outcome_id(ref: str, *, active_only: bool = True) -> str:
    """The outcome a ticket serves (``serves`` link), or ''. Lock-free read."""
    ref = str(ref or "").strip().upper()
    if not ref:
        return ""
    try:
        data = _load()
    except OutcomeError:
        return ""
    hits = [o for o in data["outcomes"].values()
            if any(l.get("kind") == "ticket" and l.get("relation") == "serves"
                   and l.get("target") == ref for l in o.get("links", []))
            and (not active_only or o.get("state") == "active")]
    return hits[0]["id"] if len(hits) == 1 else ""


def route_exception(ref: str) -> str:
    """Queue hook: may this linked ticket's technical exhaustion route to a
    bounded lead diagnosis instead of an immediate owner question? Returns
    the outcome id, or '' (feature off, unlinked, ambiguous or inactive) so
    the caller keeps its existing behaviour. Never raises."""
    try:
        if not enabled():
            return ""
        return linked_outcome_id(ref)
    except Exception:  # noqa: BLE001 - a broken outcome store never changes queue behaviour
        return ""


# --------------------------------------------------------------- mutations

def _new_outcome(payload: Dict[str, Any]) -> Dict[str, Any]:
    queue = _norm_queue(_text(payload, "queue", 80))
    state = _STATE_ALIASES.get(_text(payload, "state", 20), _text(payload, "state", 20)) or "proposed"
    if state not in ("proposed", "active"):
        raise OutcomeError("invalid", "a new outcome starts as proposed or active")
    now = _iso()
    rank = payload.get("rank")
    if rank is not None and (not isinstance(rank, int) or isinstance(rank, bool)):
        raise OutcomeError("invalid", "rank must be an integer")
    deadline = _text(payload, "deadline", 40)
    if deadline and _parse_iso(deadline) is None:
        raise OutcomeError("invalid", "deadline must be an ISO date or timestamp")
    o = {
        "id": f"oc-{_hex()}", "schema_version": SCHEMA_VERSION, "revision": 1,
        "source_key": _text(payload, "source_key", 200),
        "title": _text(payload, "title", 200, required=True),
        "goal": _text(payload, "goal", 4000, required=True),
        "description": _text(payload, "description", 8000),
        "criteria": _norm_criteria(payload.get("criteria")),
        "criteria_revision": 1,
        "repo_path": _norm_repo(_text(payload, "repo_path", 1000)),
        "queue": queue, "state": state, "health": "proposed" if state == "proposed" else "scheduled",
        "health_reason": "", "health_since": now,
        "responsibility": {"role": "none", "attempt_id": "", "ref": "", "since": now},
        "next_action": _norm_next_action(payload.get("next_action")),
        "commitment": payload.get("commitment") is True, "rank": rank, "deadline": deadline,
        "authority": _norm_authority(payload.get("authority"), queue),
        "evidence": [], "links": [], "attempts": [], "findings": [], "decision_refs": [],
        "sessions": [], "events": [], "event_keys": [], "history": [],
        "incidents": [], "displacements": [],
        "wake": None, "verify_requests": [], "budget": {"day": "", "lead_wakes": 0, "verifier_attempts": 0},
        "briefing": "", "acceptance": None, "last_rejection": None,
        "created_at": now, "updated_at": now,
    }
    return o


def create(payload: Dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
    o = _new_outcome(payload)
    with _txn() as data:
        if o["source_key"]:
            for other in data["outcomes"].values():
                if other.get("source_key") == o["source_key"]:
                    return _public(other), False
        data["outcomes"][o["id"]] = o
        _history(o, "created", state=o["state"])
        if o["state"] == "active":
            _add_event(o, "admitted", f"admitted:{o['id']}", {"summary": "Outcome admitted"})
        _feed(data, o, "created", o["title"])
    _log("OUTCOME", f"{o['id']} created ({o['state']}): {o['title'][:60]}", queue=o["queue"])
    return _public(o), True


_UPDATABLE = ("title", "goal", "description", "criteria", "commitment", "rank", "deadline", "state",
              "next_action", "authority", "queue", "repo_path")


def update(payload: Dict[str, Any]) -> Dict[str, Any]:
    patch = payload.get("patch")
    if patch is None:
        patch = {k: v for k, v in payload.items() if k in _UPDATABLE}
    if not isinstance(patch, dict) or not patch:
        raise OutcomeError("invalid", "update needs a non-empty patch")
    unknown = sorted(set(patch) - set(_UPDATABLE))
    if unknown:
        raise OutcomeError("invalid", f"fields not updatable: {', '.join(unknown)}")
    if "expected_revision" not in payload:
        raise OutcomeError("invalid", "expected_revision is required")
    expected = payload.get("expected_revision")
    repo = _norm_repo(_text(patch, "repo_path", 1000)) if "repo_path" in patch else None
    with _txn() as data:
        o = _get(data, payload.get("id"))
        if not isinstance(expected, int) or isinstance(expected, bool) or int(o.get("revision") or 0) != expected:
            raise OutcomeError("revision_conflict", "outcome changed since it was read",
                               current_revision=o.get("revision"))
        changed = []
        if "state" in patch:
            new = _text(patch, "state", 20)
            new = _STATE_ALIASES.get(new, new)
            if new in ("delivered", "verified"):
                raise OutcomeError("forbidden", "delivery is set only by the acceptance gate")
            if new not in STATES:
                raise OutcomeError("invalid", f"state must be one of {STATES}")
            if new != o["state"]:
                if new not in _TRANSITIONS.get(o["state"], ()):
                    raise OutcomeError("invalid", f"cannot move {o['state']} -> {new}")
                o["state"] = new
                changed.append("state")
                if new == "active":
                    _add_event(o, "admitted", f"admitted:{o['id']}:{o['revision']}",
                               {"summary": "Outcome admitted"})
        if "description" in patch:
            o["description"] = _text(patch, "description", 8000)
            changed.append("description")
        for k in ("title", "goal"):
            if k in patch:
                o[k] = _text(patch, k, 200 if k == "title" else 4000, required=True)
                changed.append(k)
        if "criteria" in patch:
            crit = _norm_criteria(patch["criteria"], o["criteria"])
            if crit != o["criteria"]:
                o["criteria"] = crit
                o["criteria_revision"] = int(o["criteria_revision"]) + 1
                changed.append("criteria")
                # Pending verifier requests at the old revision are superseded.
                for r in o.get("verify_requests", []):
                    if r.get("state") == "pending":
                        r["state"] = "superseded"
                if o["state"] == "active":
                    _add_event(o, "criteria_changed", f"criteria:{o['criteria_revision']}",
                               {"criteria_revision": o["criteria_revision"]})
        if "commitment" in patch:
            o["commitment"] = patch["commitment"] is True
            changed.append("commitment")
        if "rank" in patch:
            r = patch["rank"]
            if r is not None and (not isinstance(r, int) or isinstance(r, bool)):
                raise OutcomeError("invalid", "rank must be an integer or null")
            o["rank"] = r
            changed.append("rank")
        if "deadline" in patch:
            d = _text(patch, "deadline", 40)
            if d and _parse_iso(d) is None:
                raise OutcomeError("invalid", "deadline must be an ISO date or timestamp")
            o["deadline"] = d
            changed.append("deadline")
        if "next_action" in patch:
            o["next_action"] = _norm_next_action(patch["next_action"])
            changed.append("next_action")
        if "queue" in patch:
            o["queue"] = _norm_queue(_text(patch, "queue", 80))
            changed.append("queue")
        if "authority" in patch or "queue" in patch:
            o["authority"] = _norm_authority(patch.get("authority", o.get("authority")), o["queue"])
            changed.append("authority")
        if repo is not None:
            o["repo_path"] = repo
            changed.append("repo_path")
        _history(o, "updated", fields=changed)
        _derive_health(data, o, _now())
        return _public(o)


def link(payload: Dict[str, Any]) -> Dict[str, Any]:
    if payload.get("ref") and not payload.get("target"):
        payload = dict(payload, target=payload["ref"], kind=payload.get("kind") or "ticket")
    if payload.get("source") and not isinstance(payload.get("provenance"), dict):
        payload = dict(payload, provenance={"source": payload["source"]})
    kind = _text(payload, "kind", 20, required=True)
    relation = _text(payload, "relation", 20) or "related"
    if kind not in LINK_KINDS:
        raise OutcomeError("invalid", f"kind must be one of {LINK_KINDS}")
    if relation not in LINK_RELATIONS:
        raise OutcomeError("invalid", f"relation must be one of {LINK_RELATIONS}")
    target = _text(payload, "target", 300, required=True)
    prov = payload.get("provenance") if isinstance(payload.get("provenance"), dict) else {}
    provenance = {"source": _clip(prov.get("source") or "api", 100), "by": _clip(prov.get("by") or "", 200),
                  "evidence": _clip(prov.get("evidence") or "", 1000)}
    if relation == "continuation" and not provenance["evidence"]:
        raise OutcomeError("invalid", "a continuation link needs provenance.evidence (receipt/old executor reconciled)")
    ticket = None
    if kind == "ticket":
        target = target.upper()
        from . import queue as q
        ticket = q.get(target)  # outside the outcome lock (lock order)
        if ticket is None:
            raise OutcomeError("not_found", f"ticket {target} not found")
    mutating = relation == "serves" and kind == "ticket" and payload.get("mutating", True) is not False
    with _txn() as data:
        o = _get(data, payload.get("id"))
        for l in o["links"]:
            if l["kind"] == kind and l["target"] == target and l["relation"] == relation:
                return _public(o)
        if mutating and ticket and ticket.get("status") != "closed":
            active = _active_executor_refs(o, {target: ticket})
            if active:
                raise OutcomeError("executor_active",
                                   f"outcome already has an active mutating executor: {', '.join(active)}")
        entry = {"kind": kind, "target": target, "relation": relation, "provenance": provenance,
                 "mutating": mutating, "step_id": _text(payload, "step_id", 80), "at": _iso()}
        if kind == "ticket" and relation == "serves":
            entry["observed_status"] = str((ticket or {}).get("status") or "")
            o["attempts"] = _bounded(o["attempts"] + [_executor_attempt(o, target, entry["step_id"], "")],
                                     "attempts")
        o["links"] = _bounded(o["links"] + [entry], "links")
        _history(o, "linked", kind=kind, target=target, relation=relation)
        return _public(o)


def associate(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Record an explicit session association (separate from ticket links).

    With ``id`` omitted and ``parent_session_id`` given, inherit the parent's
    explicit association only when exactly one outcome holds it; never a
    similarity or ancestry guess. Association grants no side-effect authority
    and does not touch claims."""
    sid = _text(payload, "session_id", 200, required=True)
    source = _text(payload, "source", 100) or "api"
    parent = _text(payload, "parent_session_id", 200)
    with _txn() as data:
        oid = payload.get("id")
        provenance = "explicit"
        if not oid:
            if not parent:
                raise OutcomeError("invalid", "id or parent_session_id is required")
            hits = [o for o in data["outcomes"].values()
                    if o.get("state") not in ("stopped",)
                    and any(s.get("session_id") == parent for s in o.get("sessions", []))]
            if not hits:
                raise OutcomeError("not_found", "parent session has no explicit outcome association")
            if len(hits) > 1:
                raise OutcomeError("ambiguous", "parent session is associated with several outcomes",
                                   outcome_ids=sorted(h["id"] for h in hits))
            oid = hits[0]["id"]
            provenance = "inherited"
        o = _get(data, oid)
        for s in o["sessions"]:
            if s["session_id"] == sid:
                return _public(o)
        o["sessions"] = _bounded(o["sessions"] + [{
            "session_id": sid, "source": source, "parent_session_id": parent,
            "provenance": provenance, "at": _iso()}], "sessions")
        _history(o, "associated", session_id=sid, provenance=provenance)
        return _public(o)


def _add_event(o: Dict[str, Any], etype: str, key: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Append a deduplicated event. Returns the new event, or None for a duplicate."""
    keys = o.setdefault("event_keys", [])
    if key in keys:
        return None
    ev = {"id": f"ev-{_hex()}", "key": key, "type": etype, "payload": payload,
          "at": _iso(), "ts": _now(),
          "state": "processed" if etype in _QUIET_EVENTS else "pending",
          "recovery_attempts": 0, "criteria_revision": o.get("criteria_revision")}
    o["events"] = _bounded(o.get("events", []) + [ev], "events")
    keys.append(key)
    if len(keys) > _EVENT_KEYS_KEEP:
        del keys[: len(keys) - _EVENT_KEYS_KEEP]
    return ev


def _finding(o: Dict[str, Any], text: str, source: Dict[str, Any]) -> Dict[str, Any]:
    fid = "f-" + _digest(source.get("ref") or source.get("source") or "", text.strip())[:16]
    for f in o["findings"]:
        if f["id"] == fid:
            return f
    f = {"id": fid, "text": _clip(text, 4000), "source": source, "state": "unapplied",
         "created_at": _iso(), "disposition": None}
    o["findings"] = _bounded(o["findings"] + [f], "findings")
    return f


def event(payload: Dict[str, Any]) -> Dict[str, Any]:
    etype = _text(payload, "type", 40, required=True)
    if etype not in EVENT_TYPES:
        raise OutcomeError("invalid", f"type must be one of {EVENT_TYPES}")
    body = payload.get("payload") if isinstance(payload.get("payload"), dict) else {}
    encoded = json.dumps(body, default=str)
    body = json.loads(encoded) if len(encoded) <= 20000 else {"summary": _clip(encoded, 4000)}
    key = _text(payload, "key", 300) or f"{etype}:{_digest(body)[:24]}"
    with _txn() as data:
        o = _get(data, payload.get("id"))
        if etype in ("incident_observed", "incident_recovered"):
            _incident_event(data, o, etype, key, body)
        elif etype == "finding":
            text = str(body.get("text") or body.get("summary") or "").strip()
            if not text:
                raise OutcomeError("invalid", "a finding event needs payload.text")
            _finding(o, text, {"source": _clip(body.get("source") or "event", 200),
                               "ref": _clip(body.get("ref") or "", 80)})
        elif etype == "owner_answer":
            did = str(body.get("decision_id") or "")
            rid = str(body.get("request_id") or "")
            ref = next((d for d in o["decision_refs"]
                        if (did and d["id"] == did) or (rid and d.get("request_id") == rid)), None)
            if ref is None:
                raise OutcomeError("not_found", "decision is not recorded on this outcome")
            receipt = str(body.get("receipt") or "").strip()
            if not receipt:
                raise OutcomeError("invalid", "owner_answer needs the saved-answer receipt (no answer, no resume)")
            answered_rev = str(body.get("revision") or body.get("generation") or "")
            if ref.get("revision") and answered_rev != ref["revision"]:
                raise OutcomeError("stale_revision",
                                   "answer must carry the decision's exact revision/generation")
            if ref.get("state") == "pending":
                ref.update(state="answered", answer=_clip(body.get("answer") or "", 4000),
                           receipt=_clip(receipt, 300), answered_at=_iso())
            key = key if payload.get("key") else f"answer:{ref['id']}:{receipt}"
        elif etype == "recovery_retry":
            for ev in o["events"]:
                if ev["state"] == "exhausted":
                    ev.update(state="pending", recovery_attempts=0, retry_after=0)
            o["budget"]["lead_wakes"] = min(int(o["budget"].get("lead_wakes") or 0),
                                            max(0, int(limits()["lead_wakes_per_day"]) - 1))
        ev = _add_event(o, etype, key, body)
        if ev is not None:
            _history(o, "event", type=etype, key=key)
        _derive_health(data, o, _now())
        return _public(o)


def _incident_event(data: Dict[str, Any], o: Dict[str, Any], etype: str,
                    key: str, body: Dict[str, Any]) -> None:
    """Prioritize verified incidents without silently rewriting commitments.

    This trusted CLI path can only reference evidence already admitted through
    the fenced verifier. Browser event admission excludes these event types.
    """
    if key in o.get("event_keys", []):
        return
    proof = next((e for e in o["evidence"] if e["id"] == body.get("evidence_id")
                  and e["criteria_revision"] == o["criteria_revision"]
                  and e["method"] == "independent_verification"), None)
    observed = _parse_iso((proof or {}).get("observed_at"))
    if not proof or observed is None or not 0 <= _now() - observed <= 86400:
        raise OutcomeError("evidence_rejected", "incident requires recent independent evidence at current criteria")
    if etype == "incident_observed":
        if proof["result"] != "fail" or not str(body.get("reason") or "").strip():
            raise OutcomeError("evidence_rejected", "incident requires a failed check and concrete impact reason")
        if o["state"] != "active":
            raise OutcomeError("invalid", "incident must belong to an active outcome")
        targets = body.get("displaced_outcome_ids") or []
        if not isinstance(targets, list) or len(targets) > 20:
            raise OutcomeError("invalid", "displaced_outcome_ids must be a bounded list")
        displaced = [_get(data, oid) for oid in set(targets) if oid != o["id"]]
        iid = "inc-" + _digest(o["id"], key)[:16]
        incident = {"id": iid, "verified": True, "state": "active",
                    "title": _clip(body.get("title") or o["title"], 200),
                    "reason": _clip(body["reason"], 2000), "evidence_id": proof["id"],
                    "observed_at": proof["observed_at"],
                    "displaced_outcome_ids": [x["id"] for x in displaced]}
        o.setdefault("incidents", []).append(incident)
        o["incidents"] = o["incidents"][-100:]
        for target in displaced:
            target.setdefault("displacements", []).append({
                "incident_id": iid, "outcome_id": o["id"], "title": incident["title"],
                "state": "active", "observed_at": proof["observed_at"]})
            target["displacements"] = target["displacements"][-100:]
            _feed(data, target, "incident", "Work displaced by verified incident; deadline retained", iid)
    else:
        incident = next((i for i in o.get("incidents", []) if i["id"] == body.get("incident_id")), None)
        if not incident or proof["result"] != "pass":
            raise OutcomeError("evidence_rejected", "recovery requires the incident and passing independent evidence")
        failed = next((e for e in o["evidence"] if e["id"] == incident["evidence_id"]), {})
        if proof["criterion_id"] != failed.get("criterion_id") or observed <= (_parse_iso(incident["observed_at"]) or 0):
            raise OutcomeError("evidence_rejected", "recovery must recheck the failed criterion after the incident")
        incident.update(state="recovered", recovered_at=proof["observed_at"], recovery_evidence_id=proof["id"])
        for target in data["outcomes"].values():
            for displacement in target.get("displacements", []):
                if displacement["incident_id"] == incident["id"]:
                    displacement.update(state="recovered", recovered_at=proof["observed_at"])
                    _add_event(target, "external_event", "recovered:" + incident["id"],
                               {"summary": "Incident recovered; resume dated work", "incident_id": incident["id"]})


# ----------------------------------------------------------------- evidence

def _ticket_gate_check(ref: str) -> Dict[str, Any]:
    """Trusted deterministic gate: the queue store itself says the ticket
    closed through all of its gates. Returns the immutable artifact identity."""
    from . import queue as q
    it = q.get(ref)
    if it is None:
        raise OutcomeError("not_found", f"ticket {ref} not found")
    if it.get("status") != "closed" or it.get("product_nack"):
        raise OutcomeError("evidence_rejected", f"{ref} is not closed as completed")
    gates = [g for g in q.effective_gates(it) if g != "plan" and not str(g).startswith("plan:")]
    results = it.get("gate_results") or []
    if not gates or not results:
        raise OutcomeError("evidence_rejected", f"{ref} has no recorded gate results to trust")
    latest: Dict[str, bool] = {}
    for r in results:
        latest[str(r.get("gate"))] = bool(r.get("passed"))
    names = {("verify" if g == "verify" else g) for g in gates}
    missing = [g for g in names if not any(k == g or k.startswith(g) for k in latest)]
    if missing or not all(latest.values()):
        raise OutcomeError("evidence_rejected", f"{ref} gates not all passed (missing {missing})")
    commit = str((it.get("resolution") or {}).get("commit") or "")
    if not commit:
        raise OutcomeError("evidence_rejected", f"{ref} close recorded no commit (no immutable artifact)")
    return {"artifact": {"kind": "commit", "id": commit, "ref": ref},
            "verifier": {"gate": "ticket", "ref": ref, "gates": sorted(latest)},
            "observed_at": str(it.get("closed_at") or _iso())}


def evidence(payload: Dict[str, Any]) -> Dict[str, Any]:
    method = _text(payload, "method", 40, required=True)
    if method not in EVIDENCE_METHODS:
        raise OutcomeError("invalid", f"method must be one of {EVIDENCE_METHODS}; self-reports are not evidence")
    result = _text(payload, "result", 20, required=True)
    if result not in EVIDENCE_RESULTS:
        raise OutcomeError("invalid", f"result must be one of {EVIDENCE_RESULTS}")
    cid = _text(payload, "criterion_id", 40, required=True)
    rev = payload.get("criteria_revision")
    if not isinstance(rev, int) or isinstance(rev, bool):
        raise OutcomeError("invalid", "criteria_revision (integer) is required")
    gate = _ticket_gate_check(_text(payload, "ticket_ref", 80, required=True).upper()) \
        if method == "ticket_gate" else None
    with _txn() as data:
        out = _evidence_txn(data, payload, method, result, cid, rev, gate)
        if out is not None:
            return out
    raise OutcomeError("fenced", "attempt fence is stale or revoked; result preserved as late")


def _evidence_txn(data: Dict[str, Any], payload: Dict[str, Any], method: str, result: str, cid: str,
                  rev: int, gate: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    o = _get(data, payload.get("id"))
    if method == "independent_verification":
        att = next((a for a in o["attempts"] if a["id"] == payload.get("attempt_id")), None)
        if att is not None and att.get("criteria_revision") != o["criteria_revision"]:
            # Late result for superseded criteria: keep it, never count it.
            _late(att, "evidence", payload)
            return None
    crit = next((c for c in o["criteria"] if c["id"] == cid), None)
    if crit is None:
        raise OutcomeError("not_found", f"criterion {cid} not found")
    if rev != o["criteria_revision"]:
        raise OutcomeError("stale_revision", "evidence targets an old criteria revision",
                           current_criteria_revision=o["criteria_revision"])
    if method not in crit["accepts"]:
        raise OutcomeError("evidence_rejected", f"criterion {cid} does not accept {method}")
    if method == "independent_verification":
        aid = _text(payload, "attempt_id", 40, required=True)
        att = next((a for a in o["attempts"] if a["id"] == aid), None)
        if att is None or att.get("role") != "verifier":
            raise OutcomeError("evidence_rejected", "independent verification needs a verifier attempt of this outcome")
        if not payload.get("fence") or payload.get("fence") != att.get("fence") or att.get("fence_revoked"):
            _late(att, "evidence", payload)
            return None
        if att.get("criteria_revision") != rev:
            raise OutcomeError("stale_revision", "verifier attempt was for another criteria revision")
        if cid not in att.get("criterion_ids", []):
            raise OutcomeError("evidence_rejected", f"verifier attempt was not asked to check {cid}")
        art = payload.get("artifact") if isinstance(payload.get("artifact"), dict) else {}
        want = att.get("artifact") or {}
        if not art.get("id") or str(art.get("id")) != str(want.get("id")):
            raise OutcomeError("evidence_rejected", "artifact does not match the immutable artifact under verification")
        ev = {"artifact": {"kind": str(want.get("kind") or ""), "id": str(want.get("id"))},
              "verifier": {"attempt_id": aid, "engine": att.get("engine", ""), "model": att.get("model", "")},
              "observed_at": _iso()}
    else:
        ev = gate
    for e in o["evidence"]:
        if (e["criterion_id"] == cid and e["criteria_revision"] == rev and e["method"] == method
                and e["artifact"] == ev["artifact"] and e["result"] == result
                and e.get("verifier") == ev["verifier"]):
            return dict(_public(o), _evidence_id=e["id"])
    rec = {"id": f"evd-{_hex()}", "criterion_id": cid, "criteria_revision": rev, "method": method,
           "result": result, "artifact": ev["artifact"], "verifier": ev["verifier"],
           "observed_at": ev["observed_at"], "recorded_at": _iso(),
           "observation": _clip(payload.get("observation") or "", 4000),
           "limitations": _clip(payload.get("limitations") or "", 2000)}
    o["evidence"] = _bounded(o["evidence"] + [rec], "evidence")
    _history(o, "evidence", criterion_id=cid, result=result, method=method)
    if result != "pass":
        _add_event(o, "criterion_failed", f"criterion:{rec['id']}",
                   {"criterion_id": cid, "result": result, "evidence_id": rec["id"]})
    if method == "independent_verification":
        got = {e["criterion_id"] for e in o["evidence"]
               if (e.get("verifier") or {}).get("attempt_id") == att["id"]}
        if set(att.get("criterion_ids", [])) <= got:
            att.update(state="succeeded", ended_at=_iso(), fence_revoked=True)
            req = next((r for r in o.get("verify_requests", []) if r["id"] == att.get("request_id")), None)
            if req:
                req["state"] = "done"
    _maybe_accept(data, o)
    _derive_health(data, o, _now())
    return _public(o)


def _criteria_status(o: Dict[str, Any], now: Optional[float] = None) -> Tuple[List[str], Dict[str, str]]:
    """Missing required criterion ids, and evidence id per satisfied criterion.
    The latest evidence per criterion at the current revision decides."""
    now = _now() if now is None else now
    missing, ok = [], {}
    for c in o["criteria"]:
        evs = [e for e in o["evidence"] if e["criterion_id"] == c["id"]
               and e["criteria_revision"] == o["criteria_revision"] and e["method"] in c["accepts"]]
        latest = evs[-1] if evs else None
        good = bool(latest and latest["result"] == "pass")
        if good and c.get("max_age_s"):
            seen = _parse_iso(latest.get("observed_at")) or 0
            good = now - seen <= float(c["max_age_s"])
        if good:
            ok[c["id"]] = latest["id"]
        elif c.get("required", True):
            missing.append(c["id"])
    return missing, ok


def _blockers(o: Dict[str, Any]) -> List[str]:
    out = []
    if any(i.get("verified") and i.get("state") == "active" for i in o.get("incidents", [])):
        out.append("active verified incident")
    if any(a.get("state") == "uncertain" for a in o["attempts"]):
        out.append("uncertain attempt effects")
    if any(f["state"] == "unapplied" for f in o["findings"]):
        out.append("unapplied findings")
    if any(d.get("state") == "pending" for d in o["decision_refs"]):
        out.append("pending decision")
    if any(a.get("role") in ("lead", "verifier") and a.get("state") in ("reserved", "running")
           for a in o["attempts"]):
        out.append("attempt in progress")
    if _active_executor_refs(o):
        out.append("executor work in progress")
    return out


def _try_accept(data: Dict[str, Any], o: Dict[str, Any]) -> Dict[str, Any]:
    """The single acceptance transition. Rejection keeps an owned next action."""
    if o["state"] != "active":
        raise OutcomeError("invalid", f"only an active outcome can be accepted (state {o['state']})")
    missing, ok = _criteria_status(o)
    blockers = _blockers(o)
    if missing or blockers:
        o["last_rejection"] = {"missing_criteria": missing, "blockers": blockers,
                               "criteria_revision": o["criteria_revision"], "at": _iso()}
        _add_event(o, "acceptance_rejected",
                   f"reject:{o['criteria_revision']}:{_digest(missing, blockers, sorted(ok.values()))[:16]}",
                   {"missing_criteria": missing, "blockers": blockers})
        return {"accepted": False, "missing_criteria": missing, "blockers": blockers}
    o["state"] = "delivered"
    o["acceptance"] = {"accepted_at": _iso(), "criteria_revision": o["criteria_revision"],
                       "evidence_ids": ok}
    o["next_action"] = None
    o["wake"] = None
    for ev in o["events"]:
        if ev["state"] == "pending":
            ev["state"] = "processed"
    _history(o, "delivered", criteria_revision=o["criteria_revision"])
    _feed(data, o, "delivered", f"Delivered: {o['title']}", key=f"{o['id']}:delivered")
    _log("OUTCOME", f"{o['id']} delivered (criteria r{o['criteria_revision']})", queue=o["queue"])
    return {"accepted": True, "missing_criteria": [], "blockers": []}


def accept(payload: Dict[str, Any]) -> Dict[str, Any]:
    with _txn() as data:
        o = _get(data, payload.get("id"))
        exp = payload.get("expected_revision")
        if exp is not None and exp != o.get("revision"):
            raise OutcomeError("revision_conflict", "outcome changed since it was read",
                               current_revision=o.get("revision"))
        verdict = _try_accept(data, o)
        _derive_health(data, o, _now())
        return dict(verdict, outcome=_public(o))


# --------------------------------------------------------------- attempts

def _executor_attempt(o: Dict[str, Any], ref: str, step_id: str, op_key: str) -> Dict[str, Any]:
    return {"id": f"at-{_hex()}", "role": "executor", "ref": ref, "step_id": step_id,
            "op_key": op_key, "state": "running", "criteria_revision": o["criteria_revision"],
            "input_revision": o.get("revision"), "started_at": _iso(), "result": None}


def _active_executor_refs(o: Dict[str, Any], observed: Optional[Dict[str, Dict[str, Any]]] = None) -> List[str]:
    """Serves-linked mutating tickets that are not closed (last observation)."""
    out = []
    for l in o.get("links", []):
        if l.get("kind") != "ticket" or l.get("relation") != "serves" or not l.get("mutating", True):
            continue
        st = (observed or {}).get(l["target"], {}).get("status") if observed and l["target"] in observed \
            else l.get("observed_status")
        if st and st != "closed":
            out.append(l["target"])
    # Dispatches whose ticket is not created yet still hold the executor slot.
    for a in o.get("attempts", []):
        if a.get("role") == "executor" and a.get("state") == "dispatching":
            out.append(a.get("op_key") or a["id"])
    return out


def _late(att: Dict[str, Any], kind: str, payload: Dict[str, Any]) -> None:
    att["late_results"] = (list(att.get("late_results") or []) + [
        {"kind": kind, "at": _iso(), "payload": _clip(json.dumps(payload, default=str), 4000)}])[-5:]


def _attempt(o: Dict[str, Any], aid: str) -> Dict[str, Any]:
    att = next((a for a in o["attempts"] if a["id"] == aid), None)
    if att is None:
        raise OutcomeError("not_found", f"attempt {aid} not found")
    return att


def _validate_result(o: Dict[str, Any], result: Any) -> Dict[str, Any]:
    if not isinstance(result, dict):
        raise OutcomeError("invalid_result", "result must be an object with a kind")
    kind = result.get("kind")
    if kind not in RESULT_KINDS:
        raise OutcomeError("invalid_result",
                           f"result.kind must be one of {RESULT_KINDS}; suggestions alone are not a result")
    if kind == "dispatched_work":
        w = result.get("work")
        if not isinstance(w, dict) or w.get("mode") not in WORK_MODES:
            raise OutcomeError("invalid_result", f"work.mode must be one of {WORK_MODES}")
        if w.get("hard_stop") or w.get("reversible") is not True:
            raise OutcomeError("invalid_result", "dispatched work must be reversible (reversible: true); "
                                                 "hard stops need kind=decision for the owner")
        if w["mode"] == "new_ticket":
            _text(w, "title", 200, required=True)
            _text(w, "text", 20000, required=True)
            queue = _norm_queue(_text(w, "queue", 80)) or o["queue"]
            if not queue:
                raise OutcomeError("invalid_result", "no queue: the outcome has no queue and work.queue is empty")
            allowed = set(o["authority"].get("allowed_queues") or []) | ({o["queue"]} if o["queue"] else set())
            if queue not in allowed:
                raise OutcomeError("out_of_scope", f"queue {queue} is outside this outcome's authority")
            w["queue"] = queue
        else:
            ref = _text(w, "ticket_ref", 80, required=True).upper()
            w["ticket_ref"] = ref
            if w["mode"] != "link_ticket" and not any(
                    l["kind"] == "ticket" and l["relation"] == "serves" and l["target"] == ref for l in o["links"]):
                raise OutcomeError("out_of_scope", f"{ref} does not serve this outcome")
    elif kind == "next_action":
        na = _norm_next_action(result.get("next_action"), allow_user=False)
        if na is None:
            raise OutcomeError("invalid_result", "next_action is required for an explicit wait")
        if not na["due_at"] and not na["trigger"]:
            raise OutcomeError("invalid_result", "an explicit wait needs due_at or a named trigger")
        if na["due_at"] and (_parse_iso(na["due_at"]) or 0) <= _now():
            raise OutcomeError("invalid_result", "a wait must be in the future; perform already-due work now")
        result["next_action"] = na
    elif kind == "decision":
        _text(result, "decision_id", 120, required=True)
        _text(result, "question", 4000, required=True)
        for k in ("request_id", "origin_id", "revision", "generation"):
            _text(result, k, 200)
        _validate_prepared_decision(result)
        if result.get("block_ticket"):
            ref = str(result["block_ticket"]).upper()
            if not any(l["kind"] == "ticket" and l["relation"] == "serves" and l["target"] == ref
                       for l in o["links"]):
                raise OutcomeError("out_of_scope", f"{ref} does not serve this outcome")
            result["block_ticket"] = ref
    elif kind == "verification_request":
        ids = result.get("criterion_ids")
        if not isinstance(ids, list) or not ids:
            raise OutcomeError("invalid_result", "criterion_ids must be a non-empty list")
        known = {c["id"] for c in o["criteria"]}
        bad = [i for i in ids if i not in known]
        if bad:
            raise OutcomeError("invalid_result", f"unknown criteria {bad}")
        art = result.get("artifact")
        if not isinstance(art, dict) or not str(art.get("id") or "").strip():
            raise OutcomeError("invalid_result", "verification needs an immutable artifact {kind,id}")
    elif kind == "stop":
        _text(result, "reason", 4000, required=True)
        _validate_prepared_decision(result)
    return result


def _validate_prepared_decision(result: Dict[str, Any]) -> None:
    """An owner wait must refer to an actionable, researched source request."""
    if not _text(result, "request_id", 200) or not (
            _text(result, "revision", 200) or _text(result, "generation", 200)):
        raise OutcomeError("invalid_result", "prepare the existing inbox request with exact request_id and revision/generation first")
    prep = result.get("preparation")
    fields = ("why_now", "options", "recommendation", "tradeoffs", "reversibility")
    if not isinstance(prep, dict) or any(not prep.get(k) for k in fields):
        raise OutcomeError("invalid_result", "decision preparation needs why_now, options, recommendation, tradeoffs and reversibility")


def lead_result(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Ingest a lead attempt's terminal result (fenced, durable, idempotent).

    A rejected or fenced result is persisted on the attempt (rejection count,
    late result) before the error is returned."""
    aid = _text(payload, "attempt_id", 40, required=True)
    result = payload.get("result")
    ops: List[Dict[str, Any]] = []
    err: Optional[OutcomeError] = None
    with _txn() as data:
        o = _get(data, payload.get("id"))
        att = _attempt(o, aid)
        if att.get("role") != "lead":
            raise OutcomeError("invalid", "attempt is not a lead attempt")
        if att.get("state") == "succeeded":
            if _digest(result) == att.get("result_digest"):
                return dict(_public(o), duplicate=True, work_ids=[])
            raise OutcomeError("fenced", "attempt already recorded a different result")
        if (not payload.get("fence") or payload.get("fence") != att.get("fence") or att.get("fence_revoked")
                or att.get("state") not in ("running", "reserved") or o["state"] != "active"):
            _late(att, "lead_result", payload)
            err = OutcomeError("fenced", "attempt fence is stale or revoked; result preserved as late")
        else:
            try:
                result = _validate_result(o, copy.deepcopy(result))
                if result["kind"] == "dispatched_work" and result["work"]["mode"] == "new_ticket":
                    active = _active_executor_refs(o)
                    if active:
                        raise OutcomeError("executor_active",
                                           f"one mutating executor per outcome; active: {', '.join(active)}")
            except OutcomeError as exc:
                att["rejected_results"] = int(att.get("rejected_results") or 0) + 1
                att["last_rejection"] = exc.message
                _history(o, "lead_result_rejected", attempt_id=aid, reason=exc.message)
                err = exc
        if err is None:
            att.update(state="succeeded", ended_at=_iso(), result=result,
                       result_digest=_digest(payload.get("result")), fence_revoked=True)
            wake = o.get("wake") or {}
            for ev in o["events"]:
                if ev["id"] in (wake.get("event_ids") or []) and ev["state"] in ("pending", "processing"):
                    ev["state"] = "processed"
            o["wake"] = None
            o["responsibility"] = {"role": "lead", "attempt_id": aid, "ref": "", "since": _iso()}
            if isinstance(payload.get("briefing"), str):
                o["briefing"] = _clip(payload["briefing"], 6000)
            for disp in payload.get("findings") or []:
                if not isinstance(disp, dict):
                    continue
                f = next((x for x in o["findings"] if x["id"] == disp.get("id")), None)
                if f and disp.get("disposition") in FINDING_DISPOSITIONS:
                    f["state"] = disp["disposition"]
                    f["disposition"] = {"by": aid, "note": _clip(disp.get("note") or "", 1000), "at": _iso()}
            for d in o["decision_refs"]:
                if d.get("state") == "answered":
                    d.update(state="consumed", consumed_by=aid, consumed_at=_iso())
            ops = _apply_result(data, o, att, result)
            _history(o, "lead_result", attempt_id=aid, kind=result["kind"])
            _maybe_accept(data, o)
            _derive_health(data, o, _now())
        out = _public(o)
    if err is not None:
        raise err
    if _run_outbox(only=[op["id"] for op in ops]):
        out = get(out["id"])
    return dict(out, work_ids=[op.get("id") for op in ops])


def _maybe_accept(data: Dict[str, Any], o: Dict[str, Any]) -> None:
    """Deterministic acceptance attempt once every required criterion has
    passing evidence and nothing blocks; a rejection is recorded only when
    criteria are satisfied (so missing evidence does not spam events)."""
    if o["state"] == "active" and o["criteria"] and _criteria_status(o)[0] == []:
        _try_accept(data, o)


def _op(data: Dict[str, Any], o: Dict[str, Any], kind: str, op_payload: Dict[str, Any],
        key: str) -> Dict[str, Any]:
    for existing in data["outbox"]:
        if existing["id"] == key:
            return existing
    op = {"id": key, "kind": kind, "outcome_id": o["id"], "payload": op_payload, "state": "pending",
          "attempts": 0, "created_at": _iso(), "updated_at": _iso(), "receipt": None}
    data["outbox"].append(op)
    return op


def _apply_result(data: Dict[str, Any], o: Dict[str, Any], att: Dict[str, Any],
                  result: Dict[str, Any]) -> List[Dict[str, Any]]:
    kind = result["kind"]
    ops = []
    if kind == "dispatched_work":
        w = result["work"]
        key = f"outcome:{o['id']}:{att['id']}:{w['mode']}"
        if w["mode"] == "new_ticket":
            step = f"st-{_hex(8)}"
            ex = _executor_attempt(o, "", step, key)
            ex["state"] = "dispatching"
            o["attempts"] = _bounded(o["attempts"] + [ex], "attempts")
            ops.append(_op(data, o, "create_ticket", {
                "queue": w["queue"], "title": w["title"], "text": w["text"], "step_id": step,
                "repo_path": o.get("repo_path", ""), "executor_attempt_id": ex["id"],
                "gates": w.get("gates") if isinstance(w.get("gates"), list) else None,
                "criteria_revision": o["criteria_revision"]}, key))
            o["next_action"] = {"text": f"Executor works step {step}: {w['title']}", "owner": "agent",
                                "due_at": "", "trigger": f"ticket for step {step} closes", "set_at": _iso()}
        elif w["mode"] == "link_ticket":
            ops.append(_op(data, o, "link_ticket", {"ref": w["ticket_ref"]}, key + ":" + w["ticket_ref"]))
            o["next_action"] = {"text": f"Linked work {w['ticket_ref']} continues", "owner": "agent",
                                "due_at": "", "trigger": f"{w['ticket_ref']} closes", "set_at": _iso()}
        else:
            ops.append(_op(data, o, w["mode"], {"ref": w["ticket_ref"],
                                                 "note": _clip(w.get("note") or result.get("summary") or "", 2000)},
                           key + ":" + w["ticket_ref"]))
            o["next_action"] = {"text": f"{w['mode'].replace('_', ' ')} on {w['ticket_ref']}", "owner": "agent",
                                "due_at": "", "trigger": f"{w['ticket_ref']} progresses", "set_at": _iso()}
    elif kind == "next_action":
        o["next_action"] = result["next_action"]
    elif kind == "decision":
        did = result["decision_id"]
        if not any(d["id"] == did for d in o["decision_refs"]):
            prep = result.get("preparation") or {}
            o["decision_refs"] = _bounded(o["decision_refs"] + [{
                "id": did, "kind": "decision", "question": _clip(result["question"], 4000),
                "request_id": _clip(result.get("request_id") or "", 200),
                "origin_id": _clip(result.get("origin_id") or "", 200),
                "revision": _clip(result.get("revision") or result.get("generation") or "", 200),
                "preparation": {k: _clip(prep.get(k) if isinstance(prep.get(k), str) else
                                         json.dumps(prep.get(k), default=str) if prep.get(k) is not None else "",
                                         3000)
                                for k in ("why_now", "options", "recommendation", "tradeoffs",
                                          "cost_evidence", "deadline", "reversibility")},
                "hard_stop": result.get("hard_stop") is True, "state": "pending",
                "prepared_by": att["id"], "at": _iso()}], "decision_refs")
        o["next_action"] = {"text": f"Owner decision {did}: {_clip(result['question'], 300)}", "owner": "user",
                            "due_at": "", "trigger": f"answer to {did}", "set_at": _iso()}
        if result.get("block_ticket"):
            ops.append(_op(data, o, "block_ticket", {"ref": result["block_ticket"],
                                                      "question": result["question"]},
                           f"outcome:{o['id']}:{att['id']}:block"))
        _feed(data, o, "decision", result["question"], key=f"{o['id']}:decision:{did}")
    elif kind == "verification_request":
        req = {"id": f"vr-{_hex(8)}", "criterion_ids": list(result["criterion_ids"]),
               "artifact": {"kind": _clip(result["artifact"].get("kind") or "", 40),
                            "id": _clip(result["artifact"]["id"], 300)},
               "checks": _clip(result.get("checks") or "", 4000), "state": "pending", "tries": 0,
               "criteria_revision": o["criteria_revision"], "requested_by": att["id"], "at": _iso()}
        o["verify_requests"] = (o.get("verify_requests") or []) + [req]
        o["next_action"] = {"text": f"Independent verification of {req['artifact']['id']} "
                                    f"for {', '.join(req['criterion_ids'])}", "owner": "agent",
                            "due_at": "", "trigger": f"verification {req['id']}", "set_at": _iso()}
    elif kind == "stop":
        did = f"stop-{att['id']}"
        o["decision_refs"] = _bounded(o["decision_refs"] + [{
            "id": did, "kind": "stop_proposal", "question": f"Stop outcome? {_clip(result['reason'], 2000)}",
            "request_id": _clip(result["request_id"], 200), "origin_id": _clip(result.get("origin_id") or "", 200),
            "revision": _clip(result.get("revision") or result.get("generation"), 200),
            "preparation": copy.deepcopy(result["preparation"]),
            "hard_stop": False, "state": "pending", "prepared_by": att["id"], "at": _iso()}], "decision_refs")
        o["next_action"] = {"text": f"Owner reviews stop proposal {did}", "owner": "user", "due_at": "",
                            "trigger": f"answer to {did}", "set_at": _iso()}
        _feed(data, o, "stop_proposal", result["reason"], key=f"{o['id']}:stop:{att['id']}")
    return ops


# ---------------------------------------------------------------- outbox

def _tag_ticket(ref: str, outcome_id: str, step_id: str) -> None:
    """Persist the outcome id on a local ticket (display/brief); best-effort."""
    from . import queue as q
    if q._github_backend_for_project(q._project_from_ident(ref)) is not None:
        return
    with q._FileLock(q._lock_path()):
        data = q._load_unlocked()
        for it in data["items"]:
            if q._matches(it, ref):
                if it.get("outcome_id") == outcome_id:
                    return
                it["outcome_id"] = outcome_id
                it["outcome_step"] = step_id
                q._save_unlocked(data)
                return


def _ticket_text(o: Dict[str, Any], p: Dict[str, Any]) -> str:
    crit = "\n".join(f"- {c['id']}: {c['text']}" for c in o["criteria"])
    return (f"{p['text']}\n\n---\nServes outcome {o['id']} ({o['title']}), step {p['step_id']}, "
            f"criteria revision {p['criteria_revision']}.\nOutcome criteria (context; not yours to "
            f"change):\n{crit}\n\nClose this ticket through the normal gates with an honest "
            f"resolution: what artifact you produced (commit), what you measured, or that the result "
            f"was inconclusive, disproved or blocked. Record unresolved items in the resolution. "
            f"Closing this ticket does not deliver the outcome; independent verification does. "
            f"Scope: {o['authority']['scope']} {o['authority']['hard_stops']}")


def _exec_op(op: Dict[str, Any]) -> Dict[str, Any]:
    """Perform one outbox side effect. Every operation is idempotent by key:
    a replay first finds the earlier receipt (dedupe key / stored state)."""
    from . import queue as q
    p = op["payload"]
    kind = op["kind"]
    if kind == "create_ticket":
        o = _get(_load(), op["outcome_id"])
        saved = q.enqueue(note=p["title"], title=p["title"], text=_ticket_text(o, p), source="outcome",
                          project=p["queue"], repo_path=p.get("repo_path") or "", dedupe_key=op["id"],
                          gates=p.get("gates"))
        ref = str(saved.get("ref") or "")
        _tag_ticket(ref, op["outcome_id"], p["step_id"])
        return {"ref": ref, "status": saved.get("status", "open")}
    if kind == "link_ticket":
        it = q.get(p["ref"])
        if it is None:
            raise OutcomeError("not_found", f"ticket {p['ref']} not found")
        return {"ref": p["ref"], "status": it.get("status")}
    if kind == "block_ticket":
        it = q.get(p["ref"])
        if it is None:
            raise OutcomeError("not_found", f"ticket {p['ref']} not found")
        if it.get("needs_input") or it.get("status") == "closed":
            return {"ref": p["ref"], "already": True}
        q.block(p["ref"], "", origin="outcome", question=p["question"], kind="input")
        return {"ref": p["ref"], "blocked": True}
    if kind == "retry_stage":
        it = q.get(p["ref"])
        if it is None or not (it.get("stage_session") or {}).get("escalated"):
            return {"ref": p["ref"], "already": True}
        if q._github_backend_for_project(q._project_from_ident(p["ref"])) is not None:
            raise OutcomeError("unsupported", "stage retry from an outcome needs a local queue")
        with q._FileLock(q._lock_path()):
            data = q._load_unlocked()
            row = next((i for i in data["items"] if q._matches(i, p["ref"])), None)
            applied = bool(row and q._stage_retry_reset(row, p.get("note") or "outcome lead retry", q._now_iso()))
            if applied:
                q._append_history(row, "outcome_stage_retry", by=q._by("system"), outcome_id=op["outcome_id"])
                q._save_unlocked(data)
        if applied:
            from . import stages
            stages.request(p["ref"], "outcome lead retry")
        return {"ref": p["ref"], "retried": applied}
    if kind == "resume_supervision":
        from . import takeover
        return takeover.resume_from_outcome(p["ref"], op["outcome_id"])
    raise OutcomeError("invalid", f"unknown outbox op {kind}")


def _record_receipt(op_id: str, ok: bool, receipt: Dict[str, Any], error: str = "") -> None:
    with _txn() as data:
        op = next((x for x in data["outbox"] if x["id"] == op_id), None)
        if op is None:
            return
        op.update(state="done" if ok else "failed", receipt=receipt or None, error=_clip(error, 500),
                  updated_at=_iso())
        o = data["outcomes"].get(op["outcome_id"])
        if o is None:
            return
        if op["kind"] == "create_ticket":
            ex = next((a for a in o["attempts"] if a["id"] == op["payload"].get("executor_attempt_id")), None)
            if ok:
                ref = receipt["ref"]
                if ex:
                    ex.update(ref=ref, state="running")
                if not any(l["kind"] == "ticket" and l["target"] == ref and l["relation"] == "serves"
                           for l in o["links"]):
                    o["links"] = _bounded(o["links"] + [{
                        "kind": "ticket", "target": ref, "relation": "serves", "mutating": True,
                        "step_id": op["payload"]["step_id"], "observed_status": receipt.get("status", "open"),
                        "provenance": {"source": "outcome-dispatch", "by": op["id"], "evidence": "dispatch receipt"},
                        "at": _iso()}], "links")
                _history(o, "dispatched", ref=ref, op=op["id"])
                _log("OUTCOME", f"{o['id']} dispatched {ref} (step {op['payload']['step_id']})", queue=o["queue"])
            else:
                if ex:
                    ex.update(state="failed", result={"kind": "blocked", "reason": _clip(error, 500)})
                _add_event(o, "dispatch_failed", f"dispatch:{op['id']}", {"op": op["id"], "error": _clip(error, 500)})
        elif op["kind"] == "link_ticket" and ok:
            ref = receipt["ref"]
            if not any(l["kind"] == "ticket" and l["target"] == ref and l["relation"] == "serves" for l in o["links"]):
                o["links"] = _bounded(o["links"] + [{
                    "kind": "ticket", "target": ref, "relation": "serves", "mutating": True, "step_id": "",
                    "observed_status": receipt.get("status", ""), "at": _iso(),
                    "provenance": {"source": "lead", "by": op["id"], "evidence": "lead link"}}], "links")
                o["attempts"] = _bounded(o["attempts"] + [_executor_attempt(o, ref, "", op["id"])], "attempts")
        elif not ok:
            _add_event(o, "dispatch_failed", f"dispatch:{op['id']}", {"op": op["id"], "error": _clip(error, 500)})
        _derive_health(data, o, _now())


def _run_outbox(only: Optional[List[str]] = None) -> int:
    """Execute pending outbox operations (never while disabled: no NEW dispatch)."""
    if not enabled():
        return 0
    try:
        pending = [x for x in _load()["outbox"] if x.get("state") == "pending" and x.get("kind") != "spawn"
                   and (only is None or x["id"] in only)]
    except OutcomeError:
        return 0
    n = 0
    for op in pending:
        with _txn() as data:
            cur = next((x for x in data["outbox"] if x["id"] == op["id"]), None)
            if cur is None or cur["state"] != "pending":
                continue
            cur.update(state="sending", attempts=int(cur.get("attempts") or 0) + 1, updated_at=_iso())
        try:
            receipt = _exec_op(op)
            _record_receipt(op["id"], True, receipt)
        except Exception as exc:  # noqa: BLE001 - recorded as a failed receipt
            _record_receipt(op["id"], False, {}, f"{type(exc).__name__}: {exc}")
        n += 1
    return n


def _recover_sending_ops() -> None:
    """A crash between 'sending' and the receipt: every non-spawn op is keyed,
    so re-running it finds the earlier effect (enqueue dedupe key, ticket
    state) instead of duplicating it."""
    with _txn() as data:
        for op in data["outbox"]:
            if (op.get("kind") != "spawn" and op.get("state") == "sending"
                    and (_parse_iso(op.get("updated_at")) or 0) < _now() - 60):
                op.update(state="pending", updated_at=_iso(), recovered=True)


# ------------------------------------------------------------- runtime: spawn

def _spawn(prompt: str, engine: str, **kw: Any) -> Dict[str, Any]:
    from . import workers
    return workers.spawn_adhoc(prompt, engine, **kw)


def _list_workers() -> List[Dict[str, Any]]:
    from . import workers
    try:
        return workers.list_workers(prune=False)
    except Exception:  # noqa: BLE001
        return []


def _liveness(att: Dict[str, Any], rows: List[Dict[str, Any]]) -> Tuple[str, str]:
    from . import workers
    rec = next((w for w in rows if str(w.get("worker_id") or "") == att.get("worker_id")), None)
    if rec is None:
        started = _parse_iso(att.get("spawned_at")) or 0
        return ("alive", "") if _now() - started < 30 else ("dead", "worker record gone")
    state, why, _extra = workers.record_liveness(dict(rec, alive=bool(rec.get("alive"))),
                                                 str(att.get("exit_file") or ""))
    return state, why


def _kill(pid: int) -> None:
    from . import stages
    stages._kill(int(pid or 0))


def _role_target(o: Dict[str, Any], role: str) -> Dict[str, Any]:
    """Existing approved queue role policy; never a premium substitute."""
    from . import config, roles
    policy_role = "planner" if role == "lead" else "verifier"
    eng, mdl, source = roles.effective_role_model(o.get("queue") or "", None, policy_role)
    return {"engine": eng, "model": mdl, "source": f"{policy_role}:{source}",
            "blocked": bool(mdl and config.is_blocked_model(mdl))}


def _cmd(action: str, body: Dict[str, Any]) -> str:
    return "wt outcome api " + action + " --payload-json " + "'" + json.dumps(body).replace("'", "'\\''") + "'"


def _lead_brief(o: Dict[str, Any], att: Dict[str, Any], lim: Dict[str, float]) -> str:
    wake = o.get("wake") or {}
    events = [{k: e.get(k) for k in ("type", "at", "payload", "recovery_attempts")}
              for e in o["events"] if e["id"] in (wake.get("event_ids") or [])]
    linked = [{k: l.get(k) for k in ("target", "relation", "observed_status", "step_id")}
              for l in o["links"] if l["kind"] == "ticket"][-15:]
    recent = [{k: a.get(k) for k in ("id", "role", "state", "ref", "result", "failure")}
              for a in o["attempts"] if a["id"] != att["id"]][-6:]
    missing, _ok = _criteria_status(o)
    state = {
        "outcome_id": o["id"], "title": o["title"], "goal": o["goal"],
        "criteria_revision": o["criteria_revision"], "criteria": o["criteria"],
        "missing_criteria": missing, "authority": o["authority"], "repo_path": o["repo_path"],
        "queue": o["queue"], "next_action": o["next_action"], "briefing": o.get("briefing", ""),
        "commitment": o.get("commitment"), "rank": o.get("rank"), "deadline": o.get("deadline"),
        "description": o.get("description"), "health": o.get("health"),
        "health_reason": o.get("health_reason"),
        "incidents": o.get("incidents", []), "displacements": o.get("displacements", []),
        "events": events, "linked_tickets": linked, "recent_attempts": recent,
        "unapplied_findings": [f for f in o["findings"] if f["state"] == "unapplied"][-10:],
        "pending_decisions": [d for d in o["decision_refs"] if d.get("state") == "pending"],
        "answered_decisions": [d for d in o["decision_refs"] if d.get("state") == "answered"][-5:],
        "last_rejection": o.get("last_rejection"),
    }
    base = {"id": o["id"], "attempt_id": att["id"], "fence": att["fence"]}
    ex = lambda result: _cmd("lead-result", dict(base, result=result, briefing="<updated 3-line briefing>"))  # noqa: E731
    return (
        f"You are the LEAD for a WatchTower outcome: a short, bounded attempt (wall limit "
        f"{int(lim['lead_wall_s'] // 60)} minutes, keep output under about {int(lim['lead_output_tokens'])} "
        f"tokens). Read and reconcile; do not implement, deploy, send or spend anything yourself.\n\n"
        f"STATE (JSON):\n{json.dumps(state, indent=1, default=str)[:20000]}\n\n"
        "Inspect the repository, the linked tickets (`wt find REF --json`) and receipts as needed. "
        "For outcome-intake events, inspect the bounded Unfiled suggestions and administrative_intake "
        "rows: use explicit ticket claims, runtime identity or parent evidence to associate sessions "
        "with `wt outcome api associate`. Administrative intake is review duty only, never proof that "
        "a session serves this outcome. Do not associate by title or repository similarity. Unresolved "
        "provenance stays Unfiled with its reason; do not ask the owner to file sessions. "
        "Then choose exactly ONE next step and record it with ONE command. Results that only "
        "suggest are rejected; the attempt fails without a recorded result.\n\n"
        "1. Dispatch reversible in-scope work as an ordinary ticket (one active executor per outcome):\n"
        f"   {ex({'kind': 'dispatched_work', 'work': {'mode': 'new_ticket', 'title': '...', 'text': 'scoped task and expected result', 'reversible': True}})}\n"
        "   Other work modes on tickets that already serve this outcome: retry_stage (an exhausted stage), "
        "resume_supervision (an exhausted same-session reminder budget, once), link_ticket (an existing "
        "ticket proven to be this work), each with ticket_ref and reversible true.\n"
        "2. Wait explicitly for a dated check or a named event:\n"
        f"   {ex({'kind': 'next_action', 'next_action': {'text': '...', 'owner': 'agent', 'due_at': _iso(_now() + 3600), 'trigger': 'named event'}})}\n"
        "3. Prepare an owner decision (required for sends, money/price, ad spend, public posts, "
        "irreversible steps, or a real preference). Prepare it in the existing decision path first, then:\n"
        "   Read /api/decision-inbox/requests on the existing CCC server for the actual request_id, "
        "origin_id and revision/generation. Preserve the exact source identity; never invent an ID. "
        "Research missing evidence and options before escalating. Unavailable source coverage is "
        "a technical limitation to reconcile, not permission to re-ask the owner.\n"
        f"   {ex({'kind': 'decision', 'decision_id': '<existing decision id>', 'request_id': '<source request id>', 'revision': '<exact source revision or generation>', 'question': 'prepared question', 'preparation': {'why_now': '...', 'options': ['A', 'B'], 'recommendation': '...', 'tradeoffs': '...', 'reversibility': '...'}, 'hard_stop': True})}\n"
        "   (add \"block_ticket\": \"REF\" to place the prepared question on a linked ticket).\n"
        "4. Request independent verification of an immutable artifact (commit sha, deployment id, digest):\n"
        f"   {ex({'kind': 'verification_request', 'criterion_ids': ['c1'], 'artifact': {'kind': 'commit', 'id': '<sha>'}, 'checks': 'what to observe'})}\n"
        "5. Propose stopping, with justification (the owner decides):\n"
        "   Use kind=stop, reason, request_id, revision/generation and the same preparation fields; "
        "prepare the real stop request in the existing inbox first. Never invent an owner wait.\n\n"
        "You may add \"findings\": [{\"id\": \"f-...\", \"disposition\": \"applied|declined|parked\", "
        "\"note\": \"evidence\"}] to the same payload. Never change criteria; never claim the outcome is "
        "delivered. Technical recovery stays agent work: do not ask the owner to fix technical failures. "
        "If the command errors, fix the payload and run it again."
    )


def _verifier_brief(o: Dict[str, Any], att: Dict[str, Any], lim: Dict[str, float]) -> str:
    crit = [c for c in o["criteria"] if c["id"] in att["criterion_ids"]]
    lines = []
    for c in crit:
        body = {"id": o["id"], "attempt_id": att["id"], "fence": att["fence"], "criterion_id": c["id"],
                "criteria_revision": att["criteria_revision"], "method": "independent_verification",
                "artifact": att["artifact"], "result": "pass|fail|inconclusive",
                "observation": "what you observed and how", "limitations": "what you could not check"}
        lines.append(f"- {c['id']}: {c['text']}\n  {_cmd('evidence', body)}")
    return (
        f"You are an independent VERIFIER for WatchTower outcome {o['id']} ({o['title']}). Bounded attempt: "
        f"wall limit {int(lim['verifier_wall_s'] // 60)} minutes, output under about "
        f"{int(lim['verifier_output_tokens'])} tokens.\n\nArtifact under verification (immutable): "
        f"{json.dumps(att['artifact'])}\nChecks requested: {att.get('checks') or '(observe each criterion)'}\n\n"
        "Read and test in the authorized environment only. Do not change criteria, fix code, deploy, or "
        "release anything to make a check pass. Use deterministic checks where they suffice. Record one "
        "observation per criterion (choose pass, fail or inconclusive honestly):\n" + "\n".join(lines)
    )


# ------------------------------------------------------------ runtime: reconcile

def _observe_tickets(outcomes: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    from . import queue as q
    refs = {l["target"] for o in outcomes for l in o.get("links", [])
            if l.get("kind") == "ticket" and l.get("relation") == "serves"}
    out = {}
    for ref in refs:
        try:
            it = q.get(ref)
        except Exception:  # noqa: BLE001 - one unreachable backend never stops the pass
            it = None
        if it is not None:
            out[ref] = it
    return out


def _ticket_events(o: Dict[str, Any], tickets: Dict[str, Dict[str, Any]]) -> None:
    for l in o["links"]:
        if l["kind"] != "ticket" or l["relation"] != "serves":
            continue
        it = tickets.get(l["target"])
        if it is None:
            continue
        ref = l["target"]
        status = str(it.get("status") or "")
        l["observed_status"] = status
        ex = next((a for a in reversed(o["attempts"]) if a.get("role") == "executor" and a.get("ref") == ref), None)
        if status == "closed":
            res = it.get("resolution") or {}
            unresolved = [str(x) for x in (res.get("unresolved") or []) if str(x).strip()]
            kind = ("blocked" if it.get("product_nack") else
                    "artifact_produced" if res.get("commit") else "inconclusive")
            if ex and ex.get("state") in ("running", "dispatching"):
                ex.update(state="succeeded" if kind != "blocked" else "failed", ended_at=_iso(),
                          result={"kind": kind, "summary": _clip(res.get("summary") or "", 1000),
                                  "commit": res.get("commit") or "", "unresolved": len(unresolved)})
            for text in unresolved:
                _finding(o, text, {"source": "partial_close", "ref": ref})
            _add_event(o, "partial_close" if unresolved else "child_result",
                       f"child:{ref}:{it.get('closed_at') or ''}",
                       {"ref": ref, "result": kind, "summary": _clip(res.get("summary") or "", 1000),
                        "commit": res.get("commit") or "", "unresolved": unresolved[:20]})
            a = it.get("assessment") or {}
            if a.get("status") == "failed":
                _finding(o, f"Post-fix assessment for {ref} failed to apply: {a.get('reason') or ''}",
                         {"source": "assessment", "ref": ref})
                _add_event(o, "assessment_unapplied", f"assess:{ref}:{a.get('cycle') or 1}:{a.get('at') or ''}",
                           {"ref": ref, "reason": _clip(a.get("reason") or "", 500)})
        elif ex and ex.get("state") == "dispatching":
            ex["state"] = "running"
        sup = it.get("supervision") or {}
        intent = sup.get("intent") or {}
        if sup.get("state") == "paused_unreachable" and intent.get("state") == "uncertain":
            _add_event(o, "delivery_uncertain", f"uncertain:{ref}:{intent.get('key')}",
                       {"ref": ref, "intent_key": intent.get("key"), "reason": sup.get("reason", "")})
        if sup.get("wait_source") == "outcome":
            _add_event(o, "ticket_exhausted",
                       f"exhausted:{ref}:{sup.get('generation')}:{sup.get('processed_turn')}",
                       {"ref": ref, "reason": sup.get("reason", ""), "reminders": sup.get("reminders")})
        ss = it.get("stage_session") or {}
        if ss.get("escalated") and ss.get("routed_outcome"):
            _add_event(o, "stage_exhausted", f"stage:{ref}:{ss.get('key')}:{len(ss.get('deaths') or [])}",
                       {"ref": ref, "role": ss.get("role"), "key": ss.get("key"),
                        "reason": _clip(ss.get("dead_reason") or "", 500)})


def _set_health(data: Dict[str, Any], o: Dict[str, Any], health: str, reason: str,
                role: str = "", attempt_id: str = "", ref: str = "") -> None:
    if o.get("health") != health or o.get("health_reason") != reason:
        o["health"], o["health_reason"], o["health_since"] = health, _clip(reason, 500), _iso()
        if health in ("gap", "waiting_owner", "deferred"):
            _feed(data, o, health, reason, key=f"{o['id']}:{health}:{_digest(reason)[:10]}")
    resp = o.get("responsibility") or {}
    if role and (resp.get("role"), resp.get("attempt_id"), resp.get("ref")) != (role, attempt_id, ref):
        o["responsibility"] = {"role": role, "attempt_id": attempt_id, "ref": ref, "since": _iso()}


def _derive_health(data: Dict[str, Any], o: Dict[str, Any], now: float) -> None:
    """Deterministic projection. A healthy active outcome has a running
    attempt, an eligible persisted wake, or an explicit dated/triggered wait.
    Anything else is an ownership gap (not an owner question)."""
    st = o["state"]
    if st != "active":
        _set_health(data, o, st, "", "owner" if st in ("proposed", "parked") else "none")
        return
    running = [a for a in o["attempts"] if a.get("role") in ("lead", "verifier")
               and a.get("state") in ("reserved", "running")]
    if running:
        a = running[-1]
        _set_health(data, o, "running", f"{a['role']} attempt {a['id']} in progress", a["role"], a["id"])
        return
    exhausted = [e for e in o["events"] if e["state"] == "exhausted"]
    if exhausted:
        _set_health(data, o, "gap", "Lead recovery exhausted for: " + ", ".join(
            sorted({e["type"] for e in exhausted})) + "; queue care owns technical recovery "
            "(`wt outcome api event` type recovery_retry re-arms)", "recovery")
        return
    executors = _active_executor_refs(o)
    pending = [e for e in o["events"] if e["state"] == "pending"]
    vpending = [r for r in o.get("verify_requests", []) if r.get("state") == "pending"]
    if pending or vpending:
        deferred = o.get("deferred_until")
        if deferred and (_parse_iso(deferred) or 0) > now:
            _set_health(data, o, "deferred", f"Daily lead budget used; next wake after {deferred}", "lead")
        else:
            _set_health(data, o, "scheduled", f"{len(pending) + len(vpending)} event(s) awaiting a bounded wake",
                        "lead" if pending else "verifier")
        return
    if executors:
        _set_health(data, o, "running", "Executor ticket " + ", ".join(executors) + " in progress",
                    "executor", "", executors[0])
        return
    na = o.get("next_action") or {}
    if na.get("owner") == "user":
        _set_health(data, o, "waiting_owner", na.get("text", ""), "owner")
        return
    due = _parse_iso(na.get("due_at")) if na else None
    if na and ((due and due > now) or (na.get("trigger") and not due)):
        _set_health(data, o, "waiting", na.get("text", ""), na.get("owner") or "agent")
        return
    if na and due and due <= now:
        _add_event(o, "deadline_due", f"due:{na.get('due_at')}:{_digest(na.get('text'))[:10]}",
                   {"next_action": na})
    else:
        _add_event(o, "ownership_gap", f"gap:{o['criteria_revision']}:{_digest(na)[:12]}",
                   {"reason": "No running attempt, eligible wake or explicit wait"})
    _set_health(data, o, "scheduled", "Lead wake queued to restore an owned next action", "lead")


def _budget(o: Dict[str, Any], now: float) -> Dict[str, Any]:
    b = o.setdefault("budget", {})
    if b.get("day") != _day(now):
        b.update(day=_day(now), lead_wakes=0, verifier_attempts=0)
    return b


def _fail_attempt(data: Dict[str, Any], o: Dict[str, Any], att: Dict[str, Any], reason: str,
                  now: float, lim: Dict[str, float], *, uncertain: bool = False) -> None:
    att.update(state="failed", ended_at=_iso(now), failure=_clip(reason, 500), fence_revoked=True)
    if uncertain:
        # The spawn receipt is unknown. Revoking the fence is this store's
        # capability revocation: a stray process can no longer write a result.
        att["receipt"] = "unknown"
    _history(o, "attempt_failed", attempt_id=att["id"], role=att["role"], reason=_clip(reason, 300))
    _log("OUTCOME", f"{o['id']} {att['role']} {att['id']} failed: {reason}", queue=o["queue"])
    if att["role"] == "lead":
        wake = o.get("wake") or {}
        if wake.get("id") != att.get("wake_id"):
            return
        if int(wake.get("tries") or 0) <= int(lim["technical_retries"]):
            wake["retry_after"] = now + float(lim["retry_backoff_s"])
            return
        o["wake"] = None
        for ev in o["events"]:
            if ev["id"] in (wake.get("event_ids") or []) and ev["state"] in ("pending", "processing"):
                ev["recovery_attempts"] = int(ev.get("recovery_attempts") or 0) + 1
                if ev["recovery_attempts"] >= int(lim["recovery_attempts_per_event"]):
                    ev["state"] = "exhausted"
                else:
                    ev["state"] = "pending"
                    ev["retry_after"] = now + float(lim["recovery_backoff_s"])
    else:
        req = next((r for r in o.get("verify_requests", []) if r["id"] == att.get("request_id")), None)
        if req and req.get("state") == "running":
            if int(req.get("tries") or 0) <= int(lim["technical_retries"]):
                req["state"] = "pending"
                req["retry_after"] = now + float(lim["retry_backoff_s"])
            else:
                req["state"] = "failed"
                _add_event(o, "verification_failed", f"vfail:{req['id']}",
                           {"request_id": req["id"], "reason": _clip(reason, 500)})


def _check_attempts(data: Dict[str, Any], o: Dict[str, Any], rows: List[Dict[str, Any]], now: float,
                    lim: Dict[str, float], kills: List[int]) -> None:
    for att in o["attempts"]:
        # Some engines reveal their session identity only after startup. Adopt
        # it from the exact worker receipt so later census intake can attach
        # the role session without guessing from a title or repository.
        worker = next((w for w in rows if w.get("worker_id") == att.get("worker_id")
                       and att.get("worker_id")), None)
        if worker and worker.get("session_id"):
            att["session_id"] = str(worker["session_id"])
        if att.get("role") not in ("lead", "verifier") or att.get("state") not in ("reserved", "running"):
            continue
        if att["state"] == "reserved":
            op = next((x for x in data["outbox"] if x["id"] == att.get("op_key")), None)
            if op and op["state"] == "sending" and \
                    now - (_parse_iso(op.get("updated_at")) or now) > float(lim["spawn_receipt_grace_s"]):
                label = f"outcome-{att['role']}-{att['id']}"
                rec = next((w for w in rows if str(w.get("worker_id") or "").startswith(label)), None)
                if rec is not None:
                    # Receipt found after a crash: adopt instead of respawning.
                    att.update(state="running", worker_id=rec.get("worker_id", ""), pid=rec.get("pid"),
                               pid_started=rec.get("pid_started", ""), exit_file=rec.get("exit_file", ""),
                               log=rec.get("log", ""), session_id=rec.get("session_id", ""),
                               spawned_at=str(rec.get("started_at") or _iso(now)))
                    op.update(state="done", receipt={"worker_id": rec.get("worker_id")}, updated_at=_iso(now))
                else:
                    op.update(state="uncertain", updated_at=_iso(now))
                    _fail_attempt(data, o, att, "spawn receipt unknown after interruption", now, lim, uncertain=True)
            elif op is None or op["state"] in ("failed", "done"):
                if op is not None and op["state"] == "failed":
                    _fail_attempt(data, o, att, op.get("error") or "spawn failed", now, lim)
            continue
        wall = float(lim["lead_wall_s"] if att["role"] == "lead" else lim["verifier_wall_s"])
        started = _parse_iso(att.get("spawned_at")) or now
        state, why = _liveness(att, rows)
        if state == "alive" and wall > 0 and now - started >= wall:
            kills.append(int(att.get("pid") or 0))
            _fail_attempt(data, o, att, f"wall limit {int(wall)}s exceeded", now, lim)
        elif state == "dead":
            if att["role"] == "verifier":
                req = next((r for r in o.get("verify_requests", []) if r["id"] == att.get("request_id")), None)
                got = {e["criterion_id"] for e in o["evidence"]
                       if (e.get("verifier") or {}).get("attempt_id") == att["id"]}
                if got and set(att.get("criterion_ids", [])) <= got:
                    att.update(state="succeeded", ended_at=_iso(now), fence_revoked=True)
                    if req:
                        req["state"] = "done"
                    continue
            reason = why or "exited"
            if att.get("rejected_results"):
                reason += f"; {att['rejected_results']} invalid result(s): {att.get('last_rejection', '')}"
            else:
                reason += " without a recorded result"
            _fail_attempt(data, o, att, reason, now, lim)


def _plan_spawns(data: Dict[str, Any], o: Dict[str, Any], now: float, lim: Dict[str, float],
                 slots: List[int]) -> List[Dict[str, Any]]:
    """Reserve at most one attempt for this outcome (persisted before any spawn)."""
    if slots[0] <= 0 or o["state"] != "active":
        return []
    if any(a.get("role") in ("lead", "verifier") and a.get("state") in ("reserved", "running")
           for a in o["attempts"]):
        return []
    b = _budget(o, now)
    # Verification requests first: they are cheaper and the lead asked for them.
    for req in o.get("verify_requests", []):
        if req.get("state") != "pending" or (req.get("retry_after") or 0) > now:
            continue
        if req.get("criteria_revision") != o["criteria_revision"]:
            req["state"] = "superseded"
            continue
        if b["verifier_attempts"] >= int(lim["verifier_attempts_per_day"]):
            o["deferred_until"] = _next_day_iso(now)
            return []
        req["state"] = "running"
        req["tries"] = int(req.get("tries") or 0) + 1
        b["verifier_attempts"] += 1
        att = {"id": f"at-{_hex()}", "role": "verifier", "state": "reserved", "fence": uuid.uuid4().hex,
               "request_id": req["id"], "criterion_ids": req["criterion_ids"], "artifact": req["artifact"],
               "checks": req.get("checks", ""), "criteria_revision": o["criteria_revision"],
               "input_revision": o.get("revision"), "try": req["tries"], "reserved_at": _iso(now)}
        return [_reserve(data, o, att, slots)]
    pending = [e for e in o["events"] if e["state"] == "pending" and (e.get("retry_after") or 0) <= now]
    if not pending:
        return []
    wake = o.get("wake")
    if wake and (wake.get("retry_after") or 0) > now:
        return []
    if not wake:
        newest = max(e.get("ts") or 0 for e in pending)
        oldest = min(e.get("ts") or 0 for e in pending)
        if now - newest < float(lim["coalesce_s"]) and now - oldest < float(lim["coalesce_max_s"]):
            return []
        if b["lead_wakes"] >= int(lim["lead_wakes_per_day"]):
            o["deferred_until"] = _next_day_iso(now)
            return []
        o.pop("deferred_until", None)
        b["lead_wakes"] += 1
        wake = o["wake"] = {"id": f"wk-{_hex(8)}", "event_ids": [e["id"] for e in pending], "tries": 0,
                            "started_at": _iso(now), "revision": o.get("revision")}
    else:
        # A retry coalesces anything that arrived meanwhile into the same wake.
        wake["event_ids"] = sorted(set(wake["event_ids"]) | {e["id"] for e in pending})
    wake["tries"] = int(wake.get("tries") or 0) + 1
    wake.pop("retry_after", None)
    att = {"id": f"at-{_hex()}", "role": "lead", "state": "reserved", "fence": uuid.uuid4().hex,
           "wake_id": wake["id"], "event_ids": list(wake["event_ids"]), "try": wake["tries"],
           "criteria_revision": o["criteria_revision"], "input_revision": o.get("revision"),
           "reserved_at": _iso(now)}
    return [_reserve(data, o, att, slots)]


def _reserve(data: Dict[str, Any], o: Dict[str, Any], att: Dict[str, Any], slots: List[int]) -> Dict[str, Any]:
    target = _role_target(o, att["role"])
    att.update(engine=target["engine"], model=target["model"], model_source=target["source"],
               repo_path=o.get("repo_path") or "", host=os.uname().nodename)
    att["op_key"] = f"spawn:{att['id']}"
    o["attempts"] = _bounded(o["attempts"] + [att], "attempts")
    _op(data, o, "spawn", {"attempt_id": att["id"], "role": att["role"]}, att["op_key"])
    att["model_blocked"] = target["blocked"]
    slots[0] -= 1
    slots[1] -= 1
    return {"outcome_id": o["id"], "attempt_id": att["id"], "op_key": att["op_key"]}


def _do_spawn(op_key: str, lim: Dict[str, float]) -> str:
    with _txn() as data:
        op = next((x for x in data["outbox"] if x["id"] == op_key), None)
        o = data["outcomes"].get(op["outcome_id"]) if op else None
        if o is None or op is None or op["state"] != "pending":
            return "skip"
        plan = {"outcome_id": o["id"], "attempt_id": op["payload"]["attempt_id"], "op_key": op_key}
        att = _attempt(o, plan["attempt_id"])
        if att.get("state") != "reserved":
            return "skip"
        op.update(state="sending", attempts=int(op.get("attempts") or 0) + 1, updated_at=_iso())
        prompt = _lead_brief(o, att, lim) if att["role"] == "lead" else _verifier_brief(o, att, lim)
        spawn_args = dict(engine=att["engine"], model=att["model"], repo=o.get("repo_path") or "",
                          role=att["role"], queue=o.get("queue") or "")
        blocked = bool(att.get("model_blocked"))
    try:
        if blocked:
            raise ValueError(f"model {spawn_args['model']!r} is blocked by model policy; not substituting")
        if not spawn_args["repo"]:
            raise ValueError("outcome has no repo_path")
        rec = _spawn(prompt, spawn_args["engine"], model=spawn_args["model"], repo_path=spawn_args["repo"],
                     name=f"outcome-{spawn_args['role']}-{plan['attempt_id']}", report_to="",
                     stage=f"outcome_{spawn_args['role']}", ticket_ref="", ticket_queue=spawn_args["queue"])
        error = ""
        if rec.get("launch_failure"):
            error = str((rec.get("launch_failure") or {}).get("reason") or "launch failed")
    except Exception as exc:  # noqa: BLE001 - recorded as a failed try
        rec, error = {}, f"{type(exc).__name__}: {exc}"
    lim_now = lim
    with _txn() as data:
        o = data["outcomes"].get(plan["outcome_id"])
        op = next((x for x in data["outbox"] if x["id"] == plan["op_key"]), None)
        if o is None or op is None:
            return "gone"
        att = _attempt(o, plan["attempt_id"])
        if error:
            op.update(state="failed", error=_clip(error, 500), updated_at=_iso())
            if att.get("state") == "reserved":
                _fail_attempt(data, o, att, error, _now(), lim_now)
            _derive_health(data, o, _now())
            return "failed"
        op.update(state="done", updated_at=_iso(), receipt={"worker_id": rec.get("worker_id", "")})
        if att.get("state") != "reserved":
            # Fenced meanwhile (e.g. outcome paused): the new process cannot write.
            return "fenced"
        att.update(state="running", worker_id=rec.get("worker_id", ""), pid=rec.get("pid"),
                   pid_started=rec.get("pid_started", ""), exit_file=rec.get("exit_file", ""),
                   log=rec.get("log", ""), session_id=rec.get("session_id", ""), spawned_at=_iso())
        if att.get("session_id"):
            if not any(s["session_id"] == att["session_id"] for s in o["sessions"]):
                o["sessions"] = _bounded(o["sessions"] + [{
                    "session_id": att["session_id"], "source": "outcome-runtime", "parent_session_id": "",
                    "provenance": "explicit", "role": att["role"], "attempt_id": att["id"], "at": _iso()}],
                    "sessions")
        _history(o, "attempt_started", attempt_id=att["id"], role=att["role"], engine=att.get("engine"),
                 model=att.get("model"))
        _derive_health(data, o, _now())
    _log("OUTCOME", f"{plan['outcome_id']} {spawn_args['role']} {plan['attempt_id']} spawned "
         f"[{spawn_args['engine']}{':' + spawn_args['model'] if spawn_args['model'] else ''}]",
         queue=spawn_args["queue"])
    return "spawned"


def reconcile(*, now: Optional[float] = None, dry_run: bool = False, spawn: bool = True) -> Dict[str, Any]:
    """One deterministic outcome pass (daemon tick or ``wt outcome reconcile``)."""
    now = _now() if now is None else now
    if not enabled():
        return {"enabled": False, "outcomes": 0, "spawned": [], "events": 0}
    from . import outcome_intake
    intake = outcome_intake.reconcile(now=now, dry_run=dry_run)
    lim = limits()
    try:
        snapshot = _load()
    except OutcomeError as exc:
        return {"enabled": True, "error": exc.message}
    active = [o for o in snapshot["outcomes"].values() if o.get("state") == "active"]
    if dry_run:
        return {"enabled": True, "dry_run": True, "outcomes": len(active),
                "health": {o["id"]: o.get("health") for o in active}}
    _recover_sending_ops()
    tickets = _observe_tickets(active)
    rows = _list_workers()
    kills: List[int] = []
    plans: List[str] = []
    new_events = 0
    with _txn() as data:
        running = sum(1 for o in data["outcomes"].values() for a in o.get("attempts", [])
                      if a.get("role") in ("lead", "verifier") and a.get("state") in ("reserved", "running"))
        slots = [int(lim["max_concurrent_attempts"]) - running, int(lim["spawns_per_tick"])]
        order = sorted(data["outcomes"].values(),
                       key=lambda o: (not any(i.get("verified") and i.get("state") == "active"
                                             for i in o.get("incidents", [])),
                                      not o.get("commitment"),
                                      o.get("rank") if isinstance(o.get("rank"), int) else 10**6,
                                      o.get("created_at", "")))
        for o in order:
            # Attempts of a paused/stopped outcome are fenced but not killed.
            if o["state"] != "active":
                for a in o["attempts"]:
                    if a.get("role") in ("lead", "verifier") and a.get("state") == "reserved":
                        a.update(state="cancelled", fence_revoked=True, ended_at=_iso(now))
                        for op in data["outbox"]:
                            if op["id"] == a.get("op_key") and op["state"] == "pending":
                                op.update(state="cancelled", updated_at=_iso(now))
                continue
            before = len(o["events"])
            _ticket_events(o, tickets)
            _check_attempts(data, o, rows, now, lim, kills)
            _maybe_accept(data, o)
            _derive_health(data, o, now)
            if spawn and slots[0] > 0 and slots[1] > 0:
                _plan_spawns(data, o, now, lim, slots)
            _derive_health(data, o, now)
            new_events += len(o["events"]) - before
        # Every reserved, not yet sent spawn (including one stranded by a crash
        # before it was sent) runs now; nothing was sent for it yet.
        plans = [x["id"] for x in data["outbox"] if x.get("kind") == "spawn" and x.get("state") == "pending"] \
            if spawn else []
    for pid in kills:
        _kill(pid)  # the old process is stopped before any replacement starts
    spawned = [f"{key}:{_do_spawn(key, lim)}" for key in plans]
    ran = _run_outbox()
    return {"enabled": True, "outcomes": len(active), "events": new_events, "spawned": spawned,
            "killed": len([p for p in kills if p]), "outbox": ran, "intake": intake}


def feed(after: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
    return [f for f in _load()["feed"] if int(f.get("seq") or 0) > after][:max(1, min(500, limit))]


# ------------------------------------------------------------------- API

_ADMISSION = ("create", "update", "link", "event", "associate")


def _receipt(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    """Read persisted state after the mutation's context manager has committed.

    Serialization inside a transaction precedes its revision increment. Return
    a post-commit observation instead, preserving additive operation metadata.
    A concurrent writer may have advanced it further; the returned revision and
    fields always describe the same durable snapshot.
    """
    return dict(snapshot, **get(snapshot["id"]))


def api(action: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """The wire entry point: one JSON object in, one JSON object out."""
    if not isinstance(payload, dict):
        raise OutcomeError("invalid", "payload must be a JSON object")
    if action == "settings":
        if "enabled" in payload or "limits" in payload:
            en = payload.get("enabled")
            if en is not None and not isinstance(en, bool):
                raise OutcomeError("invalid", "enabled must be a boolean")
            s = set_settings(en, payload.get("limits"))
        else:
            s = settings()
        return {"ok": True, "enabled": s["enabled"], "settings": s}
    if action in _ADMISSION and not enabled():
        raise OutcomeError("outcomes_disabled", "outcomes are disabled on this installation")
    if action == "list":
        from . import outcome_intake
        return {"ok": True, "outcomes": list_outcomes(_text(payload, "repo_path", 1000),
                                                      _text(payload, "state", 20),
                                                      _text(payload, "session_id", 200)),
                "intake": outcome_intake.view()}
    if action == "get":
        return {"ok": True, "outcome": get(payload.get("id"))}
    if action == "create":
        o, created = create(payload)
        return {"ok": True, "outcome": _receipt(o), "created": created}
    if action == "update":
        return {"ok": True, "outcome": _receipt(update(payload))}
    if action == "link":
        return {"ok": True, "outcome": _receipt(link(payload))}
    if action == "associate":
        return {"ok": True, "outcome": _receipt(associate(payload))}
    if action == "event":
        return {"ok": True, "outcome": _receipt(event(payload))}
    if action == "evidence":
        o = evidence(payload)
        return {"ok": True, "outcome": _receipt(o)}
    if action == "accept":
        r = accept(payload)
        return {"ok": True, "accepted": r["accepted"], "missing_criteria": r["missing_criteria"],
                "blockers": r["blockers"], "outcome": _receipt(r["outcome"])}
    if action == "lead-result":
        o = lead_result(payload)
        work_ids = o.pop("work_ids", [])
        return {"ok": True, "outcome": _receipt(o), "work_ids": work_ids}
    if action == "feed":
        after = payload.get("after") or 0
        if not isinstance(after, int):
            raise OutcomeError("invalid", "after must be an integer")
        return {"ok": True, "feed": feed(after, int(payload.get("limit") or 100))}
    if action == "reconcile":
        return {"ok": True, "result": reconcile(dry_run=payload.get("dry_run") is True)}
    raise OutcomeError("invalid", f"unknown action {action!r}")

"""Local conditional owner answers; durable receipts, at most one route attempt.

No producer uses this opt-in contract automatically. See docs/conditional-answers.md.
"""
from __future__ import annotations

import copy

from . import queue as q


class Conflict(ValueError):
    """The observed source or the idempotency payload changed."""


class Unsupported(ValueError):
    """The source cannot provide this atomic contract."""


def require_local(ref):
    from . import config
    project = q._project_from_ident(ref)
    if not project or config.backend(project) != "file":
        raise Unsupported("conditional answers require a local file-backed QUEUE-N ref")
    if q.fcntl is None:
        raise Unsupported("conditional answers require cross-process file locking")


def wait_kind(item):
    """Exact source classifier, including legacy blocks without current metadata."""
    wait = q.current_wait(item)
    if wait:
        return wait["on"]
    return "external" if item.get("block_kind") == "awaiting-client" else "owner"


def payload(request):
    required = ("idempotency_key", "text", "expected_blocked_at",
                "expected_block_question", "expected_wait_kind", "expected_wait")
    allowed = set(required) | {"session_id", "by_kind", "tid", "engine"}
    if not isinstance(request, dict) or set(request) - allowed:
        raise ValueError("unknown conditional answer fields or invalid request object")
    if any(not isinstance(request.get(k), str) for k in required if k != "expected_wait"):
        raise ValueError("required conditional answer fields must be strings")
    if not request["idempotency_key"].strip() or not request["expected_blocked_at"]:
        raise ValueError("idempotency_key and expected_blocked_at must be nonempty")
    if "expected_wait" not in request or not (request["expected_wait"] is None or isinstance(request["expected_wait"], dict)):
        raise ValueError("expected_wait must be the current wait record or null")
    result = dict(request)
    result.setdefault("session_id", "")
    result.setdefault("by_kind", "human")
    result.setdefault("tid", False)
    result.setdefault("engine", None)
    if (not isinstance(result["session_id"], str)
            or result["by_kind"] not in ("human", "worker", "system")
            or type(result["tid"]) is not bool
            or result["engine"] not in (None, "claude", "codex", "kimi", "devin")):
        raise ValueError("invalid answer attribution or routing options")
    return result


def prepare_unlocked(item, request):
    """Called only inside the SAME source lock as the answer write.

    Receipt and answer are committed by the ordinary answer save, together.
    """
    key = request["idempotency_key"]
    existing = item.get("answer_receipts", {}).get(key)
    if existing:
        if existing["request"] != request:
            raise Conflict("idempotency key already used with a different payload")
        return False
    if (not item.get("needs_input") or item.get("status") == "closed"
            or str(item.get("blocked_at") or "") != request["expected_blocked_at"]
            or str(item.get("block_question") or "") != request["expected_block_question"]
            or q.current_wait(item) != request["expected_wait"]
            or wait_kind(item) != request["expected_wait_kind"]
            or wait_kind(item) != "owner"):
        raise Conflict("source episode, question, or owner wait changed")
    item.setdefault("answer_receipts", {})[key] = {
        "version": 1, "ref": item["ref"], "idempotency_key": key,
        "accepted": True, "accepted_at": q._now_iso(), "work_completed": False,
        "answer_sequence": 1 + sum(h.get("event") == "answer" for h in item.get("history", [])),
        "routing": {"state": "pending"}, "request": copy.deepcopy(request),
    }
    return True


def submit(ref, request):
    """Accept/replay atomically. Does not itself deliver to a worker."""
    require_local(ref)
    request = payload(request)
    item = q.answer(ref, request["text"], session_id=request["session_id"],
                    by_kind=request["by_kind"], answer_contract=request)
    if item is None:
        raise Conflict("source ticket not found")
    return copy.deepcopy(item["answer_receipts"][request["idempotency_key"]])


def lookup(ref, key):
    """Read-only reconciliation, including after a response was lost."""
    from . import owner_blocks_export
    project = q._project_from_ident(ref)
    if not project:
        raise Unsupported("receipt lookup requires a local QUEUE-N ref")
    _, items, _, settings = owner_blocks_export.read_source()
    if str(settings.get(project, {}).get("backend") or "file").strip().lower() != "file":
        raise Unsupported("receipt lookup requires a local file-backed queue")
    for item in items:
        if q._matches(item, ref):
            return copy.deepcopy(item.get("answer_receipts", {}).get(key))
    return None


def claim_route(ref, key):
    """Consume the single route attempt BEFORE any external side effect.

    Crash/exception after this point stays uncertain; replay cannot route again.
    """
    require_local(ref)
    with q._FileLock(q._lock_path()) as lock:
        if lock._fh is None:
            raise Unsupported("source write lock unavailable")
        data = q._load_unlocked(strict=True)
        for item in data["items"]:
            if not q._matches(item, ref):
                continue
            receipt = item.get("answer_receipts", {}).get(key)
            if not receipt or receipt["routing"]["state"] != "pending":
                return None
            answer_sequence = sum(h.get("event") == "answer" for h in item.get("history", []))
            if (item.get("needs_input") or item.get("status") == "closed"
                    or answer_sequence != receipt["answer_sequence"]
                    or (item.get("blocked_at") and str(item["blocked_at"]) !=
                        receipt["request"]["expected_blocked_at"])):
                receipt["routing"] = {"state": "superseded"}
                q._save_unlocked(data)
                return None
            receipt["routing"] = {"state": "uncertain", "attempted_at": q._now_iso()}
            q._save_unlocked(data)
            return copy.deepcopy(item)
    return None


def finish_route(ref, key, exit_code):
    require_local(ref)
    with q._FileLock(q._lock_path()) as lock:
        if lock._fh is None:
            raise Unsupported("source write lock unavailable")
        data = q._load_unlocked(strict=True)
        for item in data["items"]:
            if q._matches(item, ref):
                receipt = item.get("answer_receipts", {}).get(key)
                if receipt and receipt["routing"]["state"] == "uncertain":
                    receipt["routing"].update(
                        state="completed" if exit_code == 0 else "uncertain",
                        exit_code=exit_code, returned_at=q._now_iso())
                    q._save_unlocked(data)
                return

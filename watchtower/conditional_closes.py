"""Local conditional owner closes: close the exact question the owner saw.

The close and its durable receipt are one write under the source lock. It
wakes no worker, runs no gate, verifier or assessment, and closes as Declined
so dependents are flagged instead of treated as completed. See
docs/conditional-answers.md.
"""
from __future__ import annotations

import copy

from . import queue as q
from .conditional_answers import (Conflict, Unsupported, check_store_unlocked,
                                  require_local, wait_kind)


class Replayed(Exception):
    """The same key already closed this ticket; nothing was written."""


def payload(request):
    required = ("idempotency_key", "reason", "expected_blocked_at",
                "expected_block_question", "expected_wait_kind", "expected_wait")
    allowed = set(required) | {"by", "expected_store_id"}
    if not isinstance(request, dict) or set(request) - allowed:
        raise ValueError("unknown conditional close fields or invalid request object")
    if any(not isinstance(request.get(k), str) for k in required if k != "expected_wait"):
        raise ValueError("required conditional close fields must be strings")
    if (not request["idempotency_key"].strip() or not request["expected_blocked_at"]
            or not request["reason"].strip()):
        raise ValueError("idempotency_key, reason and expected_blocked_at must be nonempty")
    if "expected_wait" not in request or not (request["expected_wait"] is None or isinstance(request["expected_wait"], dict)):
        raise ValueError("expected_wait must be the current wait record or null")
    result = dict(request)
    result.setdefault("by", "owner")
    result.setdefault("expected_store_id", None)
    if not isinstance(result["by"], str) or not result["by"].strip():
        raise ValueError("by must be a nonempty string")
    if (result["expected_store_id"] is not None and
            (not isinstance(result["expected_store_id"], str) or not result["expected_store_id"])):
        raise ValueError("expected_store_id must be a nonempty string or null")
    return result


def prepare_unlocked(item, request):
    """Called inside the SAME source lock as the close write."""
    key = request["idempotency_key"]
    existing = item.get("close_receipts", {}).get(key)
    if existing:
        if payload(existing["request"]) != request:
            raise Conflict("idempotency key already used with a different payload")
        raise Replayed(key)
    if (not item.get("needs_input") or item.get("status") == "closed"
            or str(item.get("blocked_at") or "") != request["expected_blocked_at"]
            or str(item.get("block_question") or "") != request["expected_block_question"]
            or q.current_wait(item) != request["expected_wait"]
            or wait_kind(item) != request["expected_wait_kind"]
            or wait_kind(item) != "owner"):
        raise Conflict("source episode, question, or owner wait changed")
    now = q._now_iso()
    item["product_nack"] = {"by": request["by"], "at": now, "comment": request["reason"]}
    item.setdefault("close_receipts", {})[key] = {
        "version": 1, "ref": item["ref"], "idempotency_key": key, "accepted": True,
        "accepted_at": now, "work_completed": False, "request": copy.deepcopy(request),
    }


def submit(ref, request):
    """Close or replay atomically. Never delivers anything to a worker."""
    require_local(ref)
    request = payload(request)
    from . import takeover
    try:
        # Same order as q.answer: serialize with takeover, then the store lock.
        with takeover._lock():
            item = q.close(ref, session_id=request["by"], force=True, declined=True,
                           resolution={"summary": f"Closed by owner, no more work: {request['reason']}",
                                       "no_code": True},
                           close_contract=request)
    except Replayed:
        return lookup(ref, request["idempotency_key"], request["expected_store_id"])
    if item is None:
        raise Conflict("source ticket not found")
    return copy.deepcopy(item["close_receipts"][request["idempotency_key"]])


def lookup(ref, key, expected_store_id=None):
    """Read-only reconciliation, including after a response was lost."""
    from . import owner_blocks_export
    project = q._project_from_ident(ref)
    if not project:
        raise Unsupported("receipt lookup requires a local QUEUE-N ref")
    _, items, _, settings, identity = owner_blocks_export.read_source()
    if expected_store_id is not None and identity != expected_store_id:
        raise Conflict("source store identity changed or is uninitialized")
    if str(settings.get(project, {}).get("backend") or "file").strip().lower() != "file":
        raise Unsupported("receipt lookup requires a local file-backed queue")
    for item in items:
        if q._matches(item, ref):
            return copy.deepcopy(item.get("close_receipts", {}).get(key))
    return None

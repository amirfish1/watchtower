"""Reviewed WT-89 block migration. No inference, closes, answers or sends.

Each operation carries the exact previously reviewed block fingerprint. A changed
question, wait target, dependency list or claimant refuses the write. Receipts
retain the original block and guards; a reopened task still owes all acceptance.
"""
from __future__ import annotations

import copy
import hashlib
import json

from . import queue as q

FIELDS = ("needs_input", "blocked_at", "block_question", "wait", "blocked_by", "claimed_by")


def fingerprint(item):
    fields = {k: item.get(k) for k in FIELDS}
    fields["blocked_by"] = item.get("blocked_by") or []
    fields["needs_input"] = bool(item.get("needs_input"))
    for key in ("blocked_at", "block_question", "claimed_by"):
        fields[key] = item.get(key) or ""
    return hashlib.sha256(json.dumps(fields, sort_keys=True).encode()).hexdigest()


def transform(item, operation, inventory, worker):
    if operation["action"] == "already-resolved":
        if item.get("status") != "closed" or fingerprint(item) != operation["expected"]:
            raise ValueError(f"{item.get('ref')}: terminal resolution changed since review")
        return None
    if (item.get("status") == "closed" or not item.get("needs_input")
            or fingerprint(item) != operation["expected"]):
        raise ValueError(f"{item.get('ref')}: block changed since review")
    action = operation["action"]
    if action == "retain" and item.get("status") != "in_progress":
        return None
    before = {k: copy.deepcopy(item.get(k)) for k in FIELDS}
    now = q._now_iso()
    if action in ("dependency", "reopen"):
        deps = operation.get("after") or []
        if action == "dependency" and not deps:
            raise ValueError("dependency disposition requires real --after edges")
        item["blocked_by"] = q._validate_blocked_by(inventory, item["ref"], deps)
        q._dependency_wait_unlocked(item, now, worker_done=False)
    elif action == "icebox":
        q._dependency_wait_unlocked(item, now, worker_done=False)
        item["readiness"] = "needs-rationale"
        item["product_nack"] = {"by": worker, "at": now, "comment": operation["reason"]}
    elif action in ("person", "retain"):
        on = operation.get("on") or (item.get("wait") or {}).get("on")
        actor = operation.get("actor", (item.get("wait") or {}).get("actor", ""))
        # Reclassification preserves the existing question and its Q tag. This
        # is not an answer or authorization to perform the requested action.
        q.validate_person_block(on, actor, item.get("block_question"), require_question=False)
        item["wait"] = q.wait_record(on, actor, block_at=item.get("blocked_at") or "", by=worker,
                                    note=operation["reason"],
                                    extra={k: (item.get("wait") or {})[k] for k in q.WAIT_OWNER_FIELDS
                                           if on == "owner" and (item.get("wait") or {}).get(k)})
    else:
        raise ValueError(f"unknown disposition {action}")
    if action in ("person", "retain") and item.get("status") == "in_progress":
        q._park_unlocked(item, str(item.get("claimed_by") or ""),
                         str(item.get("claimed_session_id") or ""),
                         str(item.get("claimed_machine") or q.machine_tag()), now,
                         q._by("worker", worker), reason="person decision has no active worker claim")
    item["updated_at"] = now
    if action in ("dependency", "reopen"):
        q._append_history(item, "progress", by=q._by("worker", worker), at=now,
                          text="WT-89 routing repair only; no approval or acceptance waived. "
                               + operation["reason"] + " Original gate/guards: "
                               + str(before.get("block_question") or "") + " "
                               + str((before.get("wait") or {}).get("note") or ""))
    q._append_history(item, "person_block_migrated", by=q._by("worker", worker), at=now,
                      ticket="WT-89", action=action, reason=operation["reason"], previous_block=before,
                      after=item.get("blocked_by") or [])
    return before


def apply(operation, inventory, worker):
    """Apply one reviewed operation, under the backend's normal write lock."""
    ident = operation["ref"]
    backend = q._github_backend_for_project(q._project_from_ident(ident))
    if backend is not None:
        from .github_backend import _serialized_body_write, _split_body

        @_serialized_body_write
        def write(backend):
            item = backend.get(ident)
            if item is None:
                raise ValueError(f"missing {ident}")
            body, meta = _split_body(item.get("_github_body") or item.get("text", ""))
            checked = dict(meta, ref=ident, status=item.get("issue_status") or item.get("status"))
            if operation["action"] in ("dependency", "reopen"):
                backend._validate_blockers(ident, operation.get("after") or [])
            before = transform(checked, operation, inventory, worker)
            if before is None:
                return item
            if operation["action"] in ("person", "retain") and meta.get("claimed_session_id"):
                checked["claimed_session_id"] = meta["claimed_session_id"]
            for key in set(meta) | set(checked):
                if key in ("ref", "status"):
                    continue
                if key in checked:
                    meta[key] = checked[key]
                else:
                    meta.pop(key, None)
            args = (["--remove-label", backend.in_progress_label]
                    if operation["action"] in ("dependency", "reopen", "icebox", "person", "retain") else [])
            fresh = backend.get(ident)
            fresh_meta = _split_body(fresh.get("_github_body") or fresh.get("text", ""))[1]
            if fingerprint(fresh_meta) != operation["expected"]:
                raise ValueError(f"{ident}: block changed during dependency validation")
            conflicts = backend._edit_body(item, body, meta, *args)
            result = backend.get(ident)
            result_meta = _split_body(result.get("_github_body") or result.get("text", ""))[1]
            if set(conflicts).intersection(FIELDS) or fingerprint(result_meta) != fingerprint(meta):
                raise ValueError(f"{ident}: concurrent issue edit; inspect the retained migration receipt")
            return result

        result = write(backend)
        if result and result.get("pipeline_local") and operation["action"] in ("dependency", "reopen"):
            from . import github_pipeline

            def unpark(it):
                # The issue write must have landed before dropping delivery
                # ownership. A new block generation is never touched.
                if it.get("needs_input") or it.get("blocked_at"):
                    raise ValueError(f"{ident}: reblocked during sidecar cleanup")
                it.pop("parked", None)
                it.pop("pending_answer", None)
            github_pipeline.mutate(ident, unpark)
        return backend.get(ident)
    with q._FileLock(q._lock_path()):
        data = q._load_unlocked()
        for item in data["items"]:
            if q._matches(item, ident):
                current = {i["ref"]: i for i in inventory}
                current.update({i["ref"]: i for i in data["items"]})
                if transform(item, operation, list(current.values()), worker) is not None:
                    q._save_unlocked(data)
                return item
    raise ValueError(f"missing {ident}")


def release_reason(ident, expected, worker):
    """Keep a release wait release-only, retaining owed acceptance in progress."""
    def mutate(item):
        if item.get('status') != 'awaiting_release' or item.get('release_wait') != expected:
            raise ValueError(f'{ident}: release generation changed since review')
        now = q._now_iso()
        q._append_history(item, 'progress', by=q._by('worker', worker), at=now,
                          text='WT-89: on observed adoption do one live acceptance check; keep working on a reproducible failure. Natural evidence needs a permanent watch on close, never re-parking. Original acceptance: '+expected['reason'])
        item['release_wait'] = dict(expected, reason=f"Merged {expected['commit']} on {expected['branch']}; awaiting a containing release at {expected['target']}.")
        q._append_history(item, 'release_reason_migrated', by=q._by('worker', worker), at=now,
                          previous_release_wait=expected, ticket='WT-89')
        item['updated_at'] = now

    backend = q._github_backend_for_project(q._project_from_ident(ident))
    if backend is not None:
        from .github_backend import _serialized_body_write, _split_body

        @_serialized_body_write
        def write(backend):
            item = backend.get(ident)
            body, meta = _split_body(item.get('_github_body') or item.get('text',''))
            checked = dict(meta, status=item.get('status'))
            mutate(checked)
            checked.pop('status')
            backend._edit_body(item, body, checked)
            return backend.get(ident)
        return write(backend)
    with q._FileLock(q._lock_path()):
        data = q._load_unlocked()
        item = next(i for i in data['items'] if q._matches(i, ident))
        mutate(item)
        q._save_unlocked(data)
        return item

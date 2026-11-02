"""Explicit, default-dry-run legacy owner episode initialization; no routing."""
from __future__ import annotations

import copy
from contextlib import contextmanager
from datetime import datetime, timezone
import json
import uuid

from . import queue as q, storage, store_identity
from . import owner_blocks_export as export
from .conditional_answers import Conflict, Unsupported, wait_kind

RECEIPTS = "owner_wait_initialization_receipts"
PROVENANCE = "owner_wait_initialization"
REPAIR = "repair-supervised-owner-wait-v1"


@contextmanager
def _source(apply):
    # Existing actual source lock only: no path resolver, schema/store migration,
    # missing lock creation, settings migration, duplicate synchronization or routes.
    base = export._base_path()
    if q.fcntl is None:
        raise Unsupported("source locking unavailable")
    with base.with_suffix(".lock").open("r") as lock:
        q.fcntl.flock(lock.fileno(), (q.fcntl.LOCK_EX if apply else q.fcntl.LOCK_SH)
                      | q.fcntl.LOCK_NB)
        connection = None
        try:
            settings = export._settings()
            db = base.with_suffix(".db")
            if db.exists():
                connection = q.sqlite3.connect(db.as_uri() + ("?mode=rw" if apply else "?mode=ro"), uri=True)
                connection.execute("BEGIN IMMEDIATE" if apply else "BEGIN")
                meta = dict(connection.execute("SELECT key,value FROM meta"))
                revision = int(meta["revision"])
                identity = meta.get("store_id")
                data = {"items": [json.loads(r[0]) for r in connection.execute(
                    "SELECT item_json FROM items ORDER BY number")]}
            else:
                data = json.loads(base.read_text())
                revision, identity = None, data.get("store_id")
            if identity is None:
                raise Unsupported("persisted store UUID required; identity initialization is separate")
            store_identity.validate(identity)
            items = data["items"]
            if not isinstance(items, list) or any(not isinstance(i, dict) or not i.get("project") for i in items):
                raise ValueError("invalid source items")
            normalized = copy.deepcopy(items)
            q._normalize_items(normalized)
            if normalized != items:
                raise Unsupported("source ref normalization required before initialization")
            version = {"revision": revision, "snapshot_sha256": export._digest([items, settings])}
            yield base, connection, data, settings, identity, version
        finally:
            if connection is not None:
                connection.close()  # rollback on error or dry run
            q.fcntl.flock(lock.fileno(), q.fcntl.LOCK_UN)


def _eligible(item, settings):
    backend = str(settings.get(item["project"], {}).get("backend") or "file").strip().lower()
    raw_wait = item.get("wait")
    if (backend != "file" or item.get("needs_input") is not True
            or item.get("status") in ("closed", "in_review") or item.get("needs_review")
            or item.get("duplicate_of") or item.get("block_kind") not in (None, "", "input")
            or item.get("blocked_at") not in (None, "") or wait_kind(item) != "owner"
            or not isinstance(item.get("block_question"), str) or not item["block_question"].strip()
            or item.get(PROVENANCE) or item.get("block_history")
            or any(h.get("event") == "block" for h in item.get("history", []))
            or (raw_wait is not None and (not isinstance(raw_wait, dict)
                                         or raw_wait.get("on") != "owner"
                                         or q.current_wait(item) is None))):
        raise Conflict("not an eligible missing-episode local owner input wait")


def _eligible_supervised_repair(item, settings, identity):
    """A resolved initialization followed by an unversioned supervised wait.

    This is deliberately narrower than a generic force initializer. The old
    durable receipt proves which stale wait remains; source history must show
    that it was answered, and supervision must name the exact current question.
    """
    def refused():
        raise Conflict("not an eligible stale supervised owner wait")
    original = item.get(PROVENANCE)
    original = original if isinstance(original, dict) else {}
    receipts = item.get(RECEIPTS)
    receipts = receipts if isinstance(receipts, dict) else {}
    receipt = receipts.get(original.get('idempotency_key'))
    wait = item.get('wait')
    sup = item.get('supervision') or {}
    backend = str(settings.get(item['project'], {}).get('backend') or 'file').strip().lower()
    if (backend != 'file' or item.get('status') != 'in_progress'
            or item.get('needs_input') is not True or item.get('needs_review')
            or item.get('duplicate_of') or item.get('block_kind') not in (None, '', 'input')
            or item.get('blocked_at') not in (None, '') or item.get('block_history')
            or not isinstance(item.get('block_question'), str) or not item['block_question'].strip()
            or not isinstance(wait, dict) or wait.get('on') != 'owner' or q.current_wait(item) is not None
            or not isinstance(receipt, dict) or receipt.get('wait') != wait
            or receipt.get('provenance') != original
            or receipt.get('store_id') != identity or receipt.get('ref') != item.get('ref')
            or receipt.get('idempotency_key') != original.get('idempotency_key')
            or not wait.get('revision') or wait.get('block_at') != original.get('initialized_at')
            or original.get('kind') != 'legacy-owner-wait-initialization'
            or original.get('historical_block_time') != 'unknown'
            or not isinstance(sup, dict) or sup.get('schema_version') != 1
            or sup.get('state') != 'needs_input' or sup.get('reason') != item['block_question']
            or sup.get('origin') not in ('explicit', 'legacy')):
        refused()
    try:
        uuid.UUID(sup['session']['session_id'])
        def stamp(value):
            parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
            if parsed.tzinfo is None:
                raise ValueError('timezone required')
            return parsed
        prior = stamp(original['initialized_at'])
        answered = stamp(item['answered_at'])
        history = item.get('history') or []
        before = receipt['request']['expected_item'].get('history') or []
        answers = [(i, h) for i, h in enumerate(history) if h.get('event') == 'answer']
        if (answered < prior.replace(microsecond=0) or history[:len(before)] != before
                or not answers or answers[-1][0] < len(before) or stamp(answers[-1][1]['at']) != answered
                or any(h.get('event') == 'block' for h in history[answers[-1][0] + 1:])):
            refused()
    except (KeyError, TypeError, ValueError, AttributeError):
        refused()


def _validate(request, *, repair=False):
    fields = {"version", "idempotency_key", "expected_store_id", "expected_store_version",
              "expected_item", "expected_wait"}
    if repair:
        fields.add('operation')
    if not isinstance(request, dict) or set(request) != fields:
        raise ValueError("exact initialization request fields required")
    if repair and request['operation'] != REPAIR:
        raise ValueError('exact supervised repair operation required')
    if (type(request["version"]) is not int or request["version"] != 1
            or not isinstance(request["idempotency_key"], str) or not request["idempotency_key"].strip()
            or not isinstance(request["expected_store_id"], str)
            or not isinstance(request["expected_item"], dict)
            or not isinstance(request["expected_store_version"], dict)
            or not (request["expected_wait"] is None or isinstance(request["expected_wait"], dict))):
        raise ValueError("invalid initialization request")
    store_identity.validate(request["expected_store_id"])
    return copy.deepcopy(request)


def initialize(ref, request=None, *, apply=False, _repair=False):
    """Preview or apply one frozen request; receipt replay never reinitializes."""
    if apply and request is None:
        raise ValueError("--apply requires a frozen --request-json from a dry run")
    if request is not None:
        request = _validate(request, repair=_repair)
    with _source(apply) as (base, connection, data, settings, identity, version):
        # Identity precedes receipt access, including replay.
        if request is not None and request["expected_store_id"] != identity:
            raise Conflict("source store identity changed")
        matches = [i for i in data["items"] if i.get("ref") == ref]
        if len(matches) != 1:
            raise Conflict("exact unique source ref required")
        item = matches[0]
        if request is not None:
            receipt = item.get(RECEIPTS, {}).get(request["idempotency_key"])
            if receipt:
                if q._canon(receipt["request"]) != q._canon(request):
                    raise Conflict("initialization key already used with a different payload")
                return {"dry_run": not apply, "replayed": True, "request": request,
                        "receipt": copy.deepcopy(receipt)}
            if (q._canon(version) != q._canon(request["expected_store_version"])
                    or q._canon(item) != q._canon(request["expected_item"])
                    or q._canon(q.current_wait(item)) != q._canon(request["expected_wait"])):
                raise Conflict("source version, raw question, ticket or full wait changed")
        if _repair:
            _eligible_supervised_repair(item, settings, identity)
        else:
            _eligible(item, settings)
        if request is None:
            request = {"version": 1, "idempotency_key": str(uuid.uuid4()),
                       "expected_store_id": identity, "expected_store_version": version,
                       "expected_item": copy.deepcopy(item), "expected_wait": copy.deepcopy(q.current_wait(item))}
            if _repair:
                request['operation'] = REPAIR
        if not apply:
            return {"dry_run": True, "replayed": False, "request": request, "receipt": None}
        # This is an initialization time, never a reconstruction of the old block.
        episode = datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
        if any(i.get("blocked_at") == episode for i in data["items"]):
            raise Conflict("episode timestamp collision; retry the unchanged request")
        wait = copy.deepcopy(item.get("wait") or {"on": "owner"})
        wait.update(block_at=episode, revision=uuid.uuid4().hex, at=episode)
        provenance = {"version": 1, "kind": "legacy-owner-wait-initialization",
                      "initialized_at": episode, "historical_blocked_at": None,
                      "historical_block_time": "unknown", "idempotency_key": request["idempotency_key"]}
        item.update(blocked_at=episode, wait=wait, **{PROVENANCE: provenance})
        receipt = {"version": 1, "ref": ref, "store_id": identity,
                   "idempotency_key": request["idempotency_key"], "initialized_at": episode,
                   "blocked_at": episode, "wait": copy.deepcopy(wait),
                   "provenance": copy.deepcopy(provenance), "request": copy.deepcopy(request)}
        item.setdefault(RECEIPTS, {})[request["idempotency_key"]] = receipt
        if connection is not None:
            # Only this item's JSON and the source revision change; no ordinary
            # writer side effects, updated_at rewrite, schema or identity migration.
            connection.execute("UPDATE items SET item_json=? WHERE number=?", (q._canon(item), item["number"]))
            connection.execute("UPDATE meta SET value=? WHERE key='revision'", (str(version["revision"] + 1),))
            connection.commit()
        else:
            with storage.atomic_destination(base) as temporary:
                temporary.write_text(json.dumps(data) + "\n")
        return {"dry_run": False, "replayed": False, "request": request, "receipt": copy.deepcopy(receipt)}


def repair_supervised(ref, request=None, *, apply=False):
    """Explicit, default-dry-run recovery; never answers or dispatches work."""
    return initialize(ref, request, apply=apply, _repair=True)

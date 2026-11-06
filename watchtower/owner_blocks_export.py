"""Authoritative local owner-block snapshot without migration or store writes."""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path

from . import config, queue as q, storage, store_identity
from .conditional_answers import Unsupported, wait_kind


CAPABILITIES = ["conditional-owner-answer-v1", "answer-receipt-v1", "owner-blocks-export-v1",
                "store-identity-v1", "owner-wait-initialization-v1", "conditional-owner-close-v1"]


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def _base_path():
    # The ordinary resolver may migrate a legacy store. Export must never do so.
    override = os.environ.get("WATCHTOWER_STORE")
    return (Path(override).expanduser() if override else storage.data_dir() / "queues.json").absolute()


def _settings():
    # config.config_path() can migrate settings, including creating directories.
    override = os.environ.get("WATCHTOWER_CONFIG_FILE")
    if override or config.CONFIG_FILE != config._LEGACY_CONFIG_FILE:
        path = Path(override).expanduser() if override else config.CONFIG_FILE.expanduser()
    else:
        path = storage.data_dir() / "queue-config.json"
        if not path.exists() and config._LEGACY_CONFIG_FILE.exists():
            raise Unsupported("settings migration required before authoritative export")
    try:
        settings = json.loads(path.read_text())
    except FileNotFoundError:
        settings = {}
    if not isinstance(settings, dict):
        raise ValueError("queue settings must be an object")
    # Never turn malformed ownership/backend configuration into an empty set.
    if any(not isinstance(value, dict) for value in settings.values()):
        raise ValueError("queue settings entries must be objects")
    return settings


@contextmanager
def _read_lock(base):
    if q.fcntl is None:
        raise Unsupported("authoritative export requires cross-process locking")
    # Opening an existing lock read-only avoids creating/truncating any file.
    with base.with_suffix(".lock").open("r") as lock:
        q.fcntl.flock(lock.fileno(), q.fcntl.LOCK_SH | q.fcntl.LOCK_NB)
        try:
            yield
        finally:
            q.fcntl.flock(lock.fileno(), q.fcntl.LOCK_UN)


def read_source():
    """Return a strict source copy and settings without migration or writes."""
    base = _base_path()
    with _read_lock(base):
        settings = _settings()
        db = base.with_suffix(".db")
        if db.exists():
            connection = q.sqlite3.connect(db.as_uri() + "?mode=ro", uri=True)
            try:
                connection.execute("BEGIN")
                row = connection.execute("SELECT value FROM meta WHERE key='revision'").fetchone()
                if row is None:
                    raise ValueError("source store has no revision")
                revision = int(row[0])
                identity_row = connection.execute("SELECT value FROM meta WHERE key='store_id'").fetchone()
                identity = store_identity.validate(identity_row[0]) if identity_row else None
                items = [json.loads(row[0]) for row in connection.execute(
                    "SELECT item_json FROM items ORDER BY number")]
            finally:
                connection.close()
        else:
            data = json.loads(base.read_text())
            if not isinstance(data, dict) or not isinstance(data.get("items"), list):
                raise ValueError("invalid legacy source store")
            items = data["items"]
            revision = None
            identity = data.get("store_id")
            if identity is not None:
                identity = store_identity.validate(identity)
        if any(not isinstance(item, dict) or not isinstance(item.get("project"), str)
               or not item["project"] for item in items):
            raise ValueError("source items require explicit project identities")
        q._normalize_items(items)
        return base, items, revision, settings, identity


def snapshot():
    """Return the full local owner set, authoritative only within its scope."""
    base, items, revision, settings, identity = read_source()
    if identity is None:
        raise Unsupported("store identity uninitialized; run wt answer-store-init --json")
    rows = []
    for item in items:
        project = item["project"]
        backend = str(settings.get(project, {}).get("backend") or "file").strip().lower()
        if backend not in ("file", "github"):
            raise ValueError("unknown source backend")
        if backend != "file":
            continue
        if not item.get("needs_input") or item.get("status") == "closed" or wait_kind(item) != "owner":
            continue
        episode = str(item.get("blocked_at") or "")
        question = str(item.get("block_question") or "")
        wait = q.current_wait(item)
        rows.append({
            "origin_id": item["ref"], "project": project, "generation": episode,
            "title": str(item.get("title") or ""), "status": str(item.get("status") or ""),
            "duplicate_of": str(item.get("duplicate_of") or ""),
            "body": question, "wait_kind": "owner", "expected_wait": wait,
            "expected_blocked_at": episode, "expected_block_question": question,
            "expected_wait_kind": "owner", "expected_store_id": identity, "block_kind": str(item.get("block_kind") or ""),
            "source_revision": _digest([episode, question, "owner", wait]),
            "answerable": bool(episode),
            "episode_provenance": item.get("owner_wait_initialization") if (
                isinstance(item.get("owner_wait_initialization"), dict) and
                item["owner_wait_initialization"].get("initialized_at") == episode) else None,
        })
    rows.sort(key=lambda row: row["origin_id"])
    return {
        "schema_version": 1, "adapter": "watchtower-block", "authoritative": True,
        "complete": True, "scope": {"backend": "file", "store_id": identity},
        "store_version": {"revision": revision, "snapshot_sha256": _digest([items, settings])},
        "capabilities": list(CAPABILITIES), "items": rows,
    }

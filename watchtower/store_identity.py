"""Durable random local-store identity; reads never initialize metadata."""
from __future__ import annotations

import json
import uuid

from . import queue as q, storage


def validate(value):
    if not isinstance(value, str) or str(uuid.UUID(value)) != value:
        raise ValueError("invalid source store identity")
    return value


def read_unlocked(base):
    """Caller holds the source lock. Missing identity returns None."""
    db = base.with_suffix(".db")
    if db.exists():
        connection = q.sqlite3.connect(db.absolute().as_uri() + "?mode=ro", uri=True)
        try:
            row = connection.execute("SELECT value FROM meta WHERE key='store_id'").fetchone()
            value = row[0] if row else None
        finally:
            connection.close()
    else:
        data = json.loads(base.read_text())
        if not isinstance(data, dict):
            raise ValueError("invalid source store")
        value = data.get("store_id")
    return validate(value) if value is not None else None


def initialize():
    """Explicit metadata-only initialization of an existing local source.

    Does not change items, create a missing store, migrate JSON, or rotate IDs.
    """
    from .owner_blocks_export import _base_path
    from .conditional_answers import Unsupported
    base = _base_path()
    if not (base.exists() or base.with_suffix(".db").exists()):
        raise FileNotFoundError("cannot initialize identity of a missing source store")
    if q.fcntl is None:
        raise Unsupported("store identity requires cross-process locking")
    with q._FileLock(base.with_suffix(".lock")) as lock:
        if lock._fh is None:
            raise Unsupported("source write lock unavailable")
        existing = read_unlocked(base)
        if existing is not None:
            return {"store_id": existing, "initialized": False}
        identity = str(uuid.uuid4())
        db = base.with_suffix(".db")
        if db.exists():
            connection = q.sqlite3.connect(str(db))
            try:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute("INSERT INTO meta(key,value) VALUES('store_id',?)", (identity,))
                revision = int(q._meta_get(connection, "revision", "0")) + 1
                connection.execute(q._META_UPSERT, ("revision", str(revision)))
                connection.commit()
            finally:
                connection.close()
        else:
            data = json.loads(base.read_text())
            if not isinstance(data, dict) or not isinstance(data.get("items"), list):
                raise ValueError("invalid source store")
            data["store_id"] = identity
            with storage.atomic_destination(base) as temporary:
                temporary.write_text(json.dumps(data) + "\n")
        return {"store_id": identity, "initialized": True}

# Copyright (c) 2026 Amir Fish. All rights reserved.
# SPDX-License-Identifier: LicenseRef-WatchTower-Software-License

"""Delivery receipts (WT-77): make "delivered" mean *verified landed*.

The 2026-07-02 incident showed the gap: every resume delivery died at boot
while the outbox happily recorded ``delivered`` — "the adapter said ok" is
not ground truth. A receipt captures the target transcript's state at send
time and is later verified against what actually happened:

  - ``landed``     — the sent text appears in the transcript (strong truth:
                     a delivered message is written as a user event whose
                     JSON encodes the text).
  - ``advanced``   — transcript grew after the send but the text wasn't
                     found in the tail we scan (very long messages, heavy
                     concurrent writes). Weak positive.
  - ``pending``    — nothing observable yet, still inside the wait window.
  - ``lost``       — wait window elapsed and the transcript never advanced.

``record()`` is called by messages.send on every ok delivery and snapshots
Claude transcripts or Codex rollouts according to the target engine;
``sweep()`` runs from the daemon tick; ``wt receipts`` / ``wt receipts stats``
expose the ledger. ``stats()`` is the soak-gate instrument for WT-57/WT-64
(flip wt to default only after N verified deliveries, zero lost).

Nonce receipts (WT-31 D4): a verified sender puts ``⟨wt:<delivery_id>⟩`` on
the message's last line and records the receipt *before* the send, so
``at_send.size`` is the pre-send transcript size. Only the nonce found at or
after that offset is ``landed``; the text needle is not consulted (a repeated
text or an earlier paste of the nonce proves nothing). ``advanced`` stays
pending and turns ``lost`` when the window closes. A transcript that shrank
(rewritten/rotated) resets the offset to 0; the nonce is still required.
"""

from __future__ import annotations

import json
import os
import re
import time
import uuid as _uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from . import messages
from . import queue as queue_mod

MAX_RECEIPTS = 2000
_TAIL_SCAN_BYTES = 262144
_NONCE_CHUNK = 1 << 20
NONCE_RE = re.compile(r"\u27e8wt:([A-Za-z0-9_-]{4,64})\u27e9")


def make_nonce(delivery_id: str) -> str:
    return f"\u27e8wt:{delivery_id}\u27e9"


def with_nonce(text: str, nonce: str) -> str:
    """``text`` with ``nonce`` alone on its last line."""
    return f"{str(text).rstrip()}\n{nonce}"


def nonce_of(text: str) -> str:
    """The nonce on the last line of ``text``, else ''."""
    last = str(text or "").rstrip().rsplit("\n", 1)[-1].strip()
    m = NONCE_RE.fullmatch(last)
    return m.group(0) if m else ""


def _nonce_forms(nonce: str) -> List[bytes]:
    """How the nonce can appear in a jsonl transcript: raw UTF-8 (Claude Code,
    Codex) or ASCII-escaped (a Python json.dumps writer)."""
    return [nonce.encode("utf-8"), json.dumps(nonce)[1:-1].encode("ascii")]


def _receipts_file() -> Path:
    """Lives next to the outbox so tests sandbox via $WATCHTOWER_OUTBOX_FILE."""
    return messages._outbox_file().parent / "receipts.json"


def _receipts_lock() -> Path:
    return _receipts_file().with_suffix(".lock")


def _wait_window_s() -> float:
    try:
        return float(os.environ.get("WATCHTOWER_RECEIPT_WAIT_S", "") or 600.0)
    except ValueError:
        return 600.0


def _load() -> List[Dict[str, Any]]:
    try:
        with open(_receipts_file(), "r") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return []
    rows = data.get("receipts") if isinstance(data, dict) else None
    return rows if isinstance(rows, list) else []


def _save(rows: List[Dict[str, Any]]) -> None:
    path = _receipts_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = str(path) + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"receipts": rows[-MAX_RECEIPTS:]}, f, indent=1)
    os.replace(tmp, path)


def _transcript_stat(sid: str, engine: str = "claude") -> Dict[str, Any]:
    p = (
        messages._find_codex_rollout(sid)
        if str(engine).lower() == "codex"
        else messages._find_transcript(sid)
    )
    if p is None:
        return {"path": "", "size": 0, "mtime": 0.0}
    try:
        st = p.stat()
        return {"path": str(p), "size": st.st_size, "mtime": st.st_mtime}
    except OSError:
        return {"path": str(p), "size": 0, "mtime": 0.0}


def _needle(text: str) -> str:
    """How the message text appears inside the transcript jsonl: the JSON
    string encoding, minus quotes. A 60-char prefix is distinctive enough
    and immune to the transcript splitting long content across events."""
    return json.dumps(str(text)[:60])[1:-1]


def record(
    sid: str,
    text: str,
    transport: str,
    now: Optional[float] = None,
    engine: str = "claude",
    require_path: bool = False,
    nonce: str = "",
    at_send: Optional[Dict[str, Any]] = None,
    delivery_id: str = "",
) -> Dict[str, Any]:
    """Snapshot the target transcript at send time; returns the receipt.
    With ``nonce`` (WT-31) call it before the send: ``at_send.size`` is the
    offset the nonce must appear at or after. ``at_send`` overrides the
    snapshot for a session that did not exist before the send (size 0)."""
    now = time.time() if now is None else float(now)
    engine = str(engine or "claude").lower()
    if at_send is None:
        at_send = _transcript_stat(str(sid), engine)
    if require_path and not at_send["path"]:
        raise ValueError(f"{engine} transcript path not found for {str(sid)[:8]}")
    rec = {
        "id": f"rcpt-{_uuid.uuid4().hex[:12]}",
        "sid": str(sid),
        "engine": engine,
        "transport": str(transport or "?"),
        "needle": _needle(text),
        "sent_at": now,
        "at_send": at_send,
        "status": "pending",
        "verified_at": None,
    }
    if nonce:
        rec["nonce"] = str(nonce)
        rec["offset"] = int(at_send.get("size") or 0)
    if delivery_id:
        rec["delivery_id"] = str(delivery_id)
    with queue_mod._FileLock(_receipts_lock()):
        rows = _load()
        rows.append(rec)
        _save(rows)
    return rec


def _patch(receipt_id: str, fields: Optional[Dict[str, Any]] = None,
           drop: bool = False) -> None:
    with queue_mod._FileLock(_receipts_lock()):
        rows = _load()
        kept = []
        for rec in rows:
            if rec.get("id") == receipt_id:
                if drop:
                    continue
                rec.update(fields or {})
            kept.append(rec)
        _save(kept)


def discard(receipt_id: str) -> None:
    """Drop a pre-send receipt whose send never happened."""
    _patch(receipt_id, drop=True)


def set_transport(receipt_id: str, transport: str) -> None:
    _patch(receipt_id, {"transport": str(transport or "?")})


def _scan_nonce(path: str, offset: int, nonce: str) -> bool:
    """Is ``nonce`` in ``path`` at or after byte ``offset``?"""
    forms = _nonce_forms(nonce)
    overlap = max(len(f) for f in forms)
    with open(path, "rb") as f:
        f.seek(offset)
        prev = b""
        while True:
            chunk = f.read(_NONCE_CHUNK)
            if not chunk:
                return False
            buf = prev + chunk
            if any(form in buf for form in forms):
                return True
            prev = buf[-overlap:]


def _verify_nonce(rec: Dict[str, Any], now: float) -> Dict[str, Any]:
    cur = _transcript_stat(str(rec.get("sid") or ""), str(rec.get("engine") or "claude"))
    offset = int(rec.get("offset") or 0)
    if cur["path"] and cur["size"] < offset:
        # truncated / rewritten: nothing before 0 to exclude any more
        offset = rec["offset"] = 0
        rec["truncated_at"] = now
    if cur["path"]:
        try:
            if _scan_nonce(cur["path"], offset, str(rec["nonce"])):
                rec["status"] = "landed"
                rec["verified_at"] = now
                return rec
        except OSError:
            pass
    if now - float(rec.get("sent_at") or 0) > _wait_window_s():
        rec["status"] = "lost"
        rec["verified_at"] = now
    elif cur["path"] and cur["size"] > offset:
        rec["status"] = "advanced"
        rec["verified_at"] = now
    return rec


def _verify_one(rec: Dict[str, Any], now: float) -> Dict[str, Any]:
    """Re-check one pending receipt against the transcript. Pure state-move:
    pending -> landed | advanced | lost (advanced can still become landed)."""
    if rec.get("status") not in ("pending", "advanced"):
        return rec
    if rec.get("nonce"):
        return _verify_nonce(rec, now)
    sid = str(rec.get("sid") or "")
    cur = _transcript_stat(sid, str(rec.get("engine") or "claude"))
    needle = str(rec.get("needle") or "")
    if cur["path"] and needle:
        try:
            size = os.path.getsize(cur["path"])
            with open(cur["path"], "rb") as f:
                f.seek(max(0, size - _TAIL_SCAN_BYTES))
                tail = f.read().decode("utf-8", "replace")
            if needle in tail:
                rec["status"] = "landed"
                rec["verified_at"] = now
                return rec
        except OSError:
            pass
    at_send = rec.get("at_send") or {}
    grew = (
        cur["size"] > float(at_send.get("size") or 0)
        or cur["mtime"] > float(at_send.get("mtime") or 0)
    )
    if grew:
        rec["status"] = "advanced"
        rec["verified_at"] = now
    elif now - float(rec.get("sent_at") or 0) > _wait_window_s():
        rec["status"] = "lost"
        rec["verified_at"] = now
    return rec


def sweep(now: Optional[float] = None) -> Dict[str, int]:
    """Verify every pending/advanced receipt (daemon tick + CLI refresh)."""
    now = time.time() if now is None else float(now)
    with queue_mod._FileLock(_receipts_lock()):
        rows = _load()
        for rec in rows:
            _verify_one(rec, now)
        _save(rows)
    counts: Dict[str, int] = {}
    for rec in rows:
        counts[rec.get("status", "?")] = counts.get(rec.get("status", "?"), 0) + 1
    return counts


def by_nonce(nonce: str, rows: Optional[List[Dict[str, Any]]] = None) -> str:
    """Outcome of a nonce over every receipt that carries it: ``landed`` if
    any landed, else ``pending`` while any is pending/advanced, ``lost`` when
    all are lost, '' when none was recorded."""
    seen = [r.get("status") for r in (_load() if rows is None else rows)
            if r.get("nonce") == nonce]
    if not seen:
        return ""
    if "landed" in seen:
        return "landed"
    if "pending" in seen or "advanced" in seen:
        return "pending"
    return "lost"


def list_receipts(status: Optional[str] = None) -> List[Dict[str, Any]]:
    rows = _load()
    if status:
        rows = [r for r in rows if r.get("status") == status]
    return rows


def get(receipt_id: str, refresh: bool = True) -> Optional[Dict[str, Any]]:
    if refresh:
        sweep()
    for rec in _load():
        if rec.get("id") == receipt_id:
            return rec
    return None


def stats(window_s: float = 7 * 86400.0, now: Optional[float] = None) -> Dict[str, Any]:
    """Soak-gate numbers: receipts inside the window, by outcome."""
    now = time.time() if now is None else float(now)
    counts = {"landed": 0, "advanced": 0, "pending": 0, "lost": 0, "total": 0}
    for rec in _load():
        if now - float(rec.get("sent_at") or 0) > window_s:
            continue
        counts["total"] += 1
        s = rec.get("status", "pending")
        counts[s] = counts.get(s, 0) + 1
    counts["window_days"] = round(window_s / 86400.0, 1)
    return counts

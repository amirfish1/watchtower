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

``record()`` is called by messages.send on every ok delivery; ``sweep()``
runs from the daemon tick; ``wt receipts`` / ``wt receipts stats`` expose
the ledger. ``stats()`` is the soak-gate instrument for WT-57/WT-64
(flip wt to default only after N verified deliveries, zero lost).
"""

from __future__ import annotations

import json
import os
import time
import uuid as _uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from . import messages
from . import queue as queue_mod

MAX_RECEIPTS = 500
_TAIL_SCAN_BYTES = 262144


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


def _transcript_stat(sid: str) -> Dict[str, Any]:
    p = messages._find_transcript(sid)
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
    sid: str, text: str, transport: str, now: Optional[float] = None
) -> Dict[str, Any]:
    """Snapshot the target transcript at send time; returns the receipt."""
    now = time.time() if now is None else float(now)
    rec = {
        "id": f"rcpt-{_uuid.uuid4().hex[:12]}",
        "sid": str(sid),
        "transport": str(transport or "?"),
        "needle": _needle(text),
        "sent_at": now,
        "at_send": _transcript_stat(str(sid)),
        "status": "pending",
        "verified_at": None,
    }
    with queue_mod._FileLock(_receipts_lock()):
        rows = _load()
        rows.append(rec)
        _save(rows)
    return rec


def _verify_one(rec: Dict[str, Any], now: float) -> Dict[str, Any]:
    """Re-check one pending receipt against the transcript. Pure state-move:
    pending -> landed | advanced | lost (advanced can still become landed)."""
    if rec.get("status") not in ("pending", "advanced"):
        return rec
    sid = str(rec.get("sid") or "")
    cur = _transcript_stat(sid)
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

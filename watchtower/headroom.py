"""Headroom-aware dispatch (31K B25): read CCC's per-engine quota headroom and
pick which engine a queue's next workers should launch on.

CCC serves ``GET /api/headroom`` on its dashboard port::

    {"ok": true, "generated_at": "...", "rows": [
      {"id": "claude:default", "engine": "claude", "account": "default",
       "available": true, "stale": false, "unlimited": false,
       "percent_left": 38.0, "resets_at": 1791676800, "hours_to_reset": 9.2,
       ...}]}

Numbers are null when unknown; an older CCC 404s. Everything here is
best-effort: any failure reads as "no data" and the caller keeps its normal
engine choice. Stdlib only, never raises.
"""

from __future__ import annotations

import json
import os
import time
import urllib.request
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

_HTTP_TIMEOUT_S = 1.5
_CACHE_OK_S = 60.0
_CACHE_FAIL_S = 30.0
# (expires_at, source, rows)
_cache: Optional[Tuple[float, str, Optional[List[Dict[str, Any]]]]] = None


def _headroom_url() -> str:
    env = (os.environ.get("WATCHTOWER_CCC_HEADROOM_URL") or "").strip()
    if env:
        return env
    port_file = Path.home() / ".claude" / "command-center" / "port.txt"
    try:
        value = port_file.read_text().strip()
    except OSError:
        return ""
    try:
        return f"http://127.0.0.1:{int(value)}/api/headroom"
    except ValueError:
        pass
    if value.startswith(("http://127.0.0.1", "http://localhost", "http://[::1]")):
        return value.rstrip("/") + "/api/headroom"
    return ""


def _rows_of(payload: Any) -> Optional[List[Dict[str, Any]]]:
    if not isinstance(payload, dict) or payload.get("ok") is False:
        return None
    rows = payload.get("rows")
    if not isinstance(rows, list):
        return None
    return [r for r in rows if isinstance(r, dict)]


def _fetch(source: str) -> Optional[List[Dict[str, Any]]]:
    try:
        if source.startswith("file:"):
            return _rows_of(json.loads(Path(source[5:]).read_text()))
        req = urllib.request.Request(source, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=_HTTP_TIMEOUT_S) as resp:
            return _rows_of(json.loads(resp.read().decode("utf-8", "replace")))
    except Exception:  # noqa: BLE001 - headroom is advisory; no data on any error
        return None


def read_rows() -> Optional[List[Dict[str, Any]]]:
    """CCC headroom rows, or None when unavailable.

    ``$WATCHTOWER_HEADROOM_FILE`` (a JSON file in the endpoint's shape; for
    tests/E2E) wins and is read fresh every call; otherwise HTTP GET
    ``$WATCHTOWER_CCC_HEADROOM_URL`` or the local CCC's ``/api/headroom``,
    cached in-process (60 s on success, 30 s on failure)."""
    global _cache
    try:
        path = (os.environ.get("WATCHTOWER_HEADROOM_FILE") or "").strip()
        if path:
            return _fetch("file:" + path)
        url = _headroom_url()
        if not url:
            return None
        now = time.monotonic()
        if _cache and _cache[1] == url and _cache[0] > now:
            return _cache[2]
        rows = _fetch(url)
        _cache = (now + (_CACHE_OK_S if rows is not None else _CACHE_FAIL_S), url, rows)
        return rows
    except Exception:  # noqa: BLE001
        return None


def _num(value: Any) -> Optional[float]:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _hours_to_reset(row: Dict[str, Any]) -> Optional[float]:
    hours = _num(row.get("hours_to_reset"))
    if hours is not None:
        return hours
    resets = _num(row.get("resets_at"))
    if resets is None:
        return None
    return max(0.0, (resets - time.time()) / 3600.0)


def _sort_key(row: Dict[str, Any]) -> Tuple[float, float]:
    resets = _num(row.get("resets_at"))
    return (resets if resets is not None else float("inf"),
            -(_num(row.get("percent_left")) or 0.0))


def _describe(eng: str, row: Dict[str, Any], *, with_reset: bool) -> str:
    pct = _num(row.get("percent_left")) or 0.0
    text = f"{eng} {pct:.0f}% left"
    hours = _hours_to_reset(row) if with_reset else None
    if hours is not None:
        text += f", resets in {hours:.1f}h"
    return text


def pick_engine(
    candidates: Iterable[str],
    rows: Optional[List[Dict[str, Any]]],
    min_pct: float = 10.0,
) -> Tuple[Optional[str], str]:
    """Best engine among ``candidates`` by headroom, or ``(None, reason)``.

    A row is usable when available, not stale, not unlimited and its
    ``percent_left`` is a number >= ``min_pct``; each engine takes its best
    usable account. Engines whose known headroom is below ``min_pct`` are
    avoided. Usable engines rank by ``resets_at`` ascending (spend what
    expires first), ties by more headroom left. Engines with no usable data
    are never chosen here, and a first candidate (the queue's own engine)
    with no data at all is kept -- the caller keeps its normal behavior."""
    order: List[str] = []
    for cand in candidates:
        eng = str(cand or "").strip().lower()
        if eng and eng not in order:
            order.append(eng)
    if not rows:
        return None, "headroom: no data"
    usable: Dict[str, Dict[str, Any]] = {}
    low: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        eng = str(row.get("engine") or "").strip().lower()
        if eng not in order:
            continue
        if row.get("available") is False or row.get("stale") or row.get("unlimited"):
            continue
        pct = _num(row.get("percent_left"))
        if pct is None:
            continue
        if pct >= min_pct:
            if eng not in usable or _sort_key(row) < _sort_key(usable[eng]):
                usable[eng] = row
        elif eng not in low or pct > (_num(low[eng].get("percent_left")) or 0.0):
            low[eng] = row
    # The first candidate is the queue's own engine. With no headroom data
    # for it (e.g. an engine CCC can't meter) there is nothing to say it is
    # running out, so keep it rather than move work on another engine's data.
    if order and order[0] not in usable and order[0] not in low:
        return None, f"headroom: no data for {order[0]}; keeping it"
    avoided = [e for e in order if e in low and e not in usable]
    notes = [_describe(e, low[e], with_reset=False) for e in avoided]
    if not usable:
        why = "; ".join(notes) if notes else "no usable data"
        return None, f"headroom: {why}; nothing above {min_pct:g}%"
    ranked = sorted(usable, key=lambda e: (_sort_key(usable[e]), order.index(e)))
    best = ranked[0]
    notes.append(_describe(best, usable[best], with_reset=True))
    return best, "headroom: " + "; ".join(notes)


def _reset_cache_for_tests() -> None:
    global _cache
    _cache = None

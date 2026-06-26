#!/usr/bin/env python3
"""Queue health: depth, oldest-open age, and the stuck flag.

Pure queue ground-truth. WatchTower decides a queue is *stuck* from the queue
file alone — it does NOT depend on any external liveness signal (no CCC, no
process probing). A queue is stuck when:

    open > 0  AND  no worker progress (no ticket closed) in the last N minutes.

"No progress" is measured from the most recent ``closed_at`` (or, if nothing
has ever been closed, from the oldest open item's ``created_at``). That makes a
queue that has open work and nobody draining it visibly stuck — which is the
whole point of WatchTower.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from . import queue as q

# Minutes of no progress (no ticket closed) before an open queue is "stuck".
STUCK_MINUTES = 10


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except (ValueError, TypeError):
        return None


def _age_seconds(value: Optional[str], now: datetime) -> Optional[int]:
    dt = _parse_iso(value)
    if dt is None:
        return None
    return max(0, int((now - dt).total_seconds()))


def _fmt_age(seconds: Optional[int]) -> str:
    if seconds is None:
        return "-"
    if seconds < 60:
        return f"{seconds}s"
    mins = seconds // 60
    if mins < 60:
        return f"{mins}m"
    hours = mins // 60
    if hours < 24:
        return f"{hours}h{mins % 60:02d}m"
    days = hours // 24
    return f"{days}d{hours % 24:02d}h"


def queue_status(
    project: str,
    items: List[Dict[str, Any]],
    now: Optional[datetime] = None,
    stuck_minutes: int = STUCK_MINUTES,
) -> Dict[str, Any]:
    """Compute the status row for a single queue from its items."""
    now = now or datetime.now(timezone.utc)
    open_items = [it for it in items if it.get("status") == "open"]
    in_progress = [it for it in items if it.get("status") == "in_progress"]
    closed = [it for it in items if it.get("status") == "closed"]

    depth = len(open_items)
    oldest_created = min(
        (it.get("created_at") for it in open_items if it.get("created_at")),
        default=None,
    )
    oldest_open_age = _age_seconds(oldest_created, now)

    # Most recent progress = most recent close. If never closed, fall back to
    # the oldest open item's creation time (so a never-touched queue ages).
    last_close = max(
        (it.get("closed_at") for it in closed if it.get("closed_at")),
        default=None,
    )
    progress_ref = last_close or oldest_created
    since_progress = _age_seconds(progress_ref, now)

    stuck = bool(
        depth > 0
        and since_progress is not None
        and since_progress >= stuck_minutes * 60
    )

    return {
        "queue": project,
        "depth": depth,
        "in_progress": len(in_progress),
        "closed": len(closed),
        "oldest_open_age_s": oldest_open_age,
        "oldest_open_age": _fmt_age(oldest_open_age),
        "since_progress_s": since_progress,
        "since_progress": _fmt_age(since_progress),
        "stuck": stuck,
    }


def all_status(
    project: Optional[str] = None,
    now: Optional[datetime] = None,
    stuck_minutes: int = STUCK_MINUTES,
) -> List[Dict[str, Any]]:
    """Status rows for every queue (or one, if ``project`` is given).

    Empty queues (all closed, depth 0) are still listed so a drained queue
    shows up as healthy rather than vanishing.
    """
    now = now or datetime.now(timezone.utc)
    by_queue: Dict[str, List[Dict[str, Any]]] = {}
    for it in q.list_items(project=project):
        by_queue.setdefault(it.get("project") or "GEN", []).append(it)
    rows = [
        queue_status(name, items, now=now, stuck_minutes=stuck_minutes)
        for name, items in by_queue.items()
    ]
    # Stuck first, then deepest, then name.
    rows.sort(key=lambda r: (not r["stuck"], -r["depth"], r["queue"]))
    return rows

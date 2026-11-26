"""Fleet-wide pacing for automatic worker cold starts (WT-126).

One reconcile pass once staffed 13 empty queues with 18 Claude workers in
2.5 minutes; every cold start ran tests, builds and browsers at once and
pushed hermes to load 43 on 16 cores. Automatic spawns now ask ``admit``
first. It allows at most ``WINDOW_MAX`` cold starts per ``WINDOW_S`` across
every process on the host, and none while the 1-minute load per core is at
or above ``LOAD_PER_CORE``. Under sustained load, one spawn is still allowed
every ``FLOOR_S`` so staffing can't starve. A deferred spawn is not dropped:
the reconciler recomputes the deficit and asks again on its next pass.

Explicit human launches (``wt spawn``) do not come through here. A manual
run (``wt run``) still gets one spawn per pass, as before.

Knobs (environment, read on every call): WATCHTOWER_SPAWN_PACING=off
disables pacing; WATCHTOWER_SPAWN_LOAD_PER_CORE, _WINDOW_S, _WINDOW_MAX and
_FLOOR_S override the defaults; WATCHTOWER_SPAWN_PACING_FILE moves state.
"""
from __future__ import annotations

import fcntl
import json
import os
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

LOAD_PER_CORE = 1.5
WINDOW_S = 120.0
WINDOW_MAX = 3
FLOOR_S = 600.0
WAIT_TTL_S = 300.0


def _num(name: str, default: float) -> float:
    try:
        return float(os.environ.get(f"WATCHTOWER_SPAWN_{name}", "") or default)
    except ValueError:
        return default


def enabled() -> bool:
    return os.environ.get("WATCHTOWER_SPAWN_PACING", "").strip().lower() not in {
        "off", "0", "false", "no"}


def _path() -> Path:
    return Path(os.environ.get("WATCHTOWER_SPAWN_PACING_FILE")
                or Path.home() / ".watchtower" / "spawn-pacing.json")


def load_per_core() -> Optional[float]:
    try:
        return os.getloadavg()[0] / max(1, os.cpu_count() or 1)
    except (OSError, AttributeError):
        return None


def admit(queue: str, requested: int, *, manual: bool = False,
          now: Optional[float] = None) -> Tuple[int, Dict[str, object]]:
    """Reserve up to ``requested`` cold-start slots; return (allowed, why).

    ``why`` is empty when everything requested was admitted; otherwise it
    holds the audit fields for a SPAWN_DEFERRED line, including ``reason``.
    Queues deferred earlier are served first, so the queues a pass happens
    to reach first can't take every slot (a waiter that stops asking for
    ``WAIT_TTL_S`` gives its place up).
    """
    if requested <= 0 or not enabled():
        return max(0, requested), {}
    now = time.time() if now is None else now
    window_s, window_max = _num("WINDOW_S", WINDOW_S), int(_num("WINDOW_MAX", WINDOW_MAX))
    floor_s, threshold = _num("FLOOR_S", FLOOR_S), _num("LOAD_PER_CORE", LOAD_PER_CORE)
    wait_ttl = max(WAIT_TTL_S, 2 * window_s)
    path = _path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(str(path) + ".lock", "a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            state = json.loads(path.read_text())
            starts = [s for s in state.get("starts", [])
                      if now - float(s.get("at", 0)) < max(window_s, floor_s)]
            waiting = {k: v for k, v in dict(state.get("waiting") or {}).items()
                       if now - float(v.get("asked", 0)) < wait_ttl}
        except (OSError, ValueError, AttributeError, TypeError):
            starts, waiting = [], {}
        recent = sum(1 for s in starts if now - float(s["at"]) < window_s)
        last = max((float(s["at"]) for s in starts), default=0.0)
        load = load_per_core()
        why: Dict[str, object] = dict(
            load_per_core=None if load is None else round(load, 2),
            load_threshold=threshold, recent_starts=recent,
            window_s=int(window_s), window_max=window_max)
        if load is not None and load >= threshold:
            capacity = 1 if not last or now - last >= floor_s else 0
            why["reason"] = (f"host load {load:.2f}/core >= {threshold:g}"
                             + ("; starvation floor admits one" if capacity else ""))
        else:
            capacity = max(0, window_max - recent)
            why["reason"] = f"{recent} cold starts in the last {int(window_s)}s (max {window_max})"
        since = float((waiting.get(queue) or {}).get("since", now))
        ahead = sorted(k for k, v in waiting.items() if k != queue and float(v["since"]) < since)
        allowed = min(requested, max(0, capacity - len(ahead)))
        if ahead and allowed < min(requested, capacity):
            why["reason"] += f"; {len(ahead)} queue(s) waiting longer go first"
            why["waiting_ahead"] = ahead[:10]
        if manual:
            allowed = max(allowed, 1)
        allowed = min(allowed, requested)
        starts += [dict(at=now, queue=queue)] * allowed
        if allowed < requested:
            waiting[queue] = dict(since=since, asked=now)
        else:
            waiting.pop(queue, None)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(dict(starts=starts, waiting=waiting)))
        os.replace(tmp, path)
        return allowed, ({} if allowed >= requested else why)

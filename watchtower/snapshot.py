# Copyright (c) 2026 Amir Fish. All rights reserved.
# SPDX-License-Identifier: LicenseRef-WatchTower-Software-License

"""Auto-snapshot ("token-parachute"): checkpoint an idle session before the
prompt-cache cliff, with a one-shot detached timer per armed session.

Spec: docs/superpowers/specs/2026-08-19-auto-snapshot-design.md. No daemon:
`arm` spawns one detached timer process that sleeps, re-checks true idle from
the transcript mtime, and fires at most once inside the window
[threshold, CACHE_TTL) -- or skips (never injects) when it overslept past the
TTL, e.g. after a laptop suspend."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

CACHE_TTL_MIN = 60      # measured prompt-cache cliff (99.5% hit at 59 min, 12% at 61)
DEFAULT_IDLE_MIN = 55
TIMER_CAP_S = 24 * 3600  # hard lifetime cap so a timer can never orphan


def snapshots_dir() -> Path:
    return Path(
        os.environ.get("WATCHTOWER_SNAPSHOTS_DIR")
        or (Path.home() / ".watchtower" / "snapshots")
    )


def snapshot_path(session_id: str) -> Path:
    return snapshots_dir() / f"{session_id}.md"


def timer_state_path(session_id: str) -> Path:
    return snapshots_dir() / "timers" / f"{session_id}.json"


def cwd_slug(cwd: str) -> str:
    """Match claude's project-dir slug: every '/' and '.' becomes '-'."""
    return str(cwd).replace("/", "-").replace(".", "-")


def latest_link(cwd: str) -> Path:
    return snapshots_dir() / "by-cwd" / cwd_slug(cwd) / "latest"


def load_state(session_id: str) -> Optional[Dict[str, Any]]:
    try:
        with open(timer_state_path(session_id)) as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def save_state(session_id: str, state: Dict[str, Any]) -> None:
    path = timer_state_path(session_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2, sort_keys=True)
    tmp.replace(path)


def next_action(now: float, mtime: float, armed_at: float,
                threshold_s: float, ttl_s: float,
                cap_s: float = TIMER_CAP_S) -> tuple:
    """Decide the timer's next move from the transcript's true idle time."""
    if now - armed_at >= cap_s:
        return ("expire",)
    idle = now - mtime
    if idle < threshold_s:
        return ("sleep", threshold_s - idle)
    if idle < ttl_s:
        return ("fire",)
    return ("skip",)

#!/usr/bin/env python3
"""Per-queue configuration for WatchTower.

Currently holds the ``auto_drain`` policy (WT-FEATURES #16): the watcher's
``--auto-spawn`` only starts a worker for a stuck queue when that queue is
auto-drained. A queue is auto-drained **by default**; prioritization backlogs
(e.g. BYMPROD, WT-FEATURES) opt OUT so a never-meant-to-drain backlog doesn't
keep getting workers thrown at it just for being non-empty.

Stored as ``~/.watchtower/queue-config.json`` = ``{queue: {auto_drain: bool}}``.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict

CONFIG_FILE = Path(
    os.environ.get("WATCHTOWER_CONFIG_FILE")
    or (Path.home() / ".watchtower" / "queue-config.json")
)


def _load() -> Dict[str, Any]:
    try:
        with open(CONFIG_FILE, "r") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _save(data: Dict[str, Any]) -> None:
    CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = str(CONFIG_FILE) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, CONFIG_FILE)


def get_queue_config(queue: str) -> Dict[str, Any]:
    return dict(_load().get(queue, {}))


def set_auto_drain(queue: str, enabled: bool) -> Dict[str, Any]:
    data = _load()
    q = data.setdefault(queue, {})
    q["auto_drain"] = bool(enabled)
    _save(data)
    return q


def auto_drain(queue: str) -> bool:
    """True unless the queue has explicitly opted out. Default-on so a fresh
    queue drains; a backlog opts out with ``set_auto_drain(queue, False)``."""
    return bool(_load().get(queue, {}).get("auto_drain", True))

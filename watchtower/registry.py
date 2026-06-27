#!/usr/bin/env python3
"""First-class queue registry (WT-FEATURES-13).

Lets a queue be *declared* — name, backend, owner, drain policy — instead of
springing into existence implicitly on first enqueue. This is the enabler for
alternate backends (#12 GitHub Issues) and the "anyone declares a queue"
platform move: the registry is the one place that knows a queue exists and how
it should behave, independent of whether it currently has any tickets.

Stored as ``~/.watchtower/queue-registry.json`` = ``{name: {...}}``.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict

REGISTRY_FILE = Path(
    os.environ.get("WATCHTOWER_REGISTRY_FILE")
    or (Path.home() / ".watchtower" / "queue-registry.json")
)

VALID_BACKENDS = ("store", "github")


def _load() -> Dict[str, Any]:
    try:
        with open(REGISTRY_FILE, "r") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _save(data: Dict[str, Any]) -> None:
    REGISTRY_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = str(REGISTRY_FILE) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, REGISTRY_FILE)


def register(
    name: str,
    backend: str = "store",
    owner: str = "",
    auto_drain: bool = True,
) -> Dict[str, Any]:
    if backend not in VALID_BACKENDS:
        raise ValueError(f"backend must be one of {VALID_BACKENDS}")
    data = _load()
    rec = data.get(name, {})
    rec.update(
        {
            "name": name,
            "backend": backend,
            "owner": owner,
            "auto_drain": bool(auto_drain),
            "created_at": rec.get(
                "created_at", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            ),
        }
    )
    data[name] = rec
    _save(data)
    return rec


def get(name: str) -> Dict[str, Any]:
    return dict(_load().get(name, {}))


def all_queues() -> Dict[str, Any]:
    return _load()

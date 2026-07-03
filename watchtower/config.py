#!/usr/bin/env python3
"""Per-queue configuration for WatchTower.

Currently holds the ``auto_drain`` policy (WT-FEATURES #16): the watcher's
``--auto-spawn`` only starts a worker for a stuck queue when that queue is
auto-drained. Auto-drain is **off by default** — a new queue is a backlog
until you explicitly opt in with ``wt drain on <queue>``. This prevents
surprise worker spawns on queues that are just parking lots.

Stored as ``~/.watchtower/queue-config.json`` = ``{queue: {auto_drain: bool}}``.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict

VALID_BACKENDS = ("file", "github")

CONFIG_FILE = Path(
    os.environ.get("WATCHTOWER_CONFIG_FILE")
    or (Path.home() / ".watchtower" / "queue-config.json")
)

# CCC (Claude Command Center) keeps its own per-engine default model at this
# path. WT and CCC are separate systems, but sharing this one file means a
# queue with no explicit `wt set --model` falls back to whatever CCC's own
# workers default to, instead of silently inheriting the bare CLI's ambient
# default (which drifts independently of either system's intent -- e.g. a
# machine-wide `/model` change unexpectedly re-flavoring every WT worker).
CCC_SPAWN_DEFAULTS_FILE = Path(
    os.environ.get("WATCHTOWER_CCC_SPAWN_DEFAULTS_FILE")
    or (Path.home() / ".claude" / "command-center" / "spawn-defaults.json")
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


def set_backend(queue: str, backend: str) -> Dict[str, Any]:
    backend = str(backend or "file").strip().lower()
    if backend not in VALID_BACKENDS:
        raise ValueError(f"backend must be one of {VALID_BACKENDS}")
    data = _load()
    q = data.setdefault(queue, {})
    if backend == "file":
        q.pop("backend", None)
    else:
        q["backend"] = backend
    _save(data)
    return q


def backend(queue: str) -> str:
    value = str(_load().get(queue, {}).get("backend") or "file").strip().lower()
    return value if value in VALID_BACKENDS else "file"


def set_github_repo(queue: str, repo: str) -> Dict[str, Any]:
    data = _load()
    q = data.setdefault(queue, {})
    repo = str(repo or "").strip()
    if repo:
        q["github_repo"] = repo
    else:
        q.pop("github_repo", None)
    _save(data)
    return q


def github_repo(queue: str) -> str:
    return str(_load().get(queue, {}).get("github_repo") or "")


def set_github_assignee(queue: str, assignee: str) -> Dict[str, Any]:
    data = _load()
    q = data.setdefault(queue, {})
    assignee = str(assignee or "").strip()
    if assignee:
        q["github_assignee"] = assignee
    else:
        q.pop("github_assignee", None)
    _save(data)
    return q


def github_assignee(queue: str) -> str:
    return str(_load().get(queue, {}).get("github_assignee") or "@me")


def set_auto_drain(queue: str, enabled: bool) -> Dict[str, Any]:
    data = _load()
    q = data.setdefault(queue, {})
    q["auto_drain"] = bool(enabled)
    _save(data)
    return q


def auto_drain(queue: str) -> bool:
    """False unless explicitly opted in. Default-off so a fresh queue is a
    backlog until you run ``wt drain on <queue>``."""
    return bool(_load().get(queue, {}).get("auto_drain", False))


def set_claim_types(queue: str, types: Any) -> Dict[str, Any]:
    """Restrict which ticket types an auto-drain worker claims (e.g. ['bug']).

    Empty/None means no restriction — the worker drains all types. Stored as a
    list under ``claim_types`` so ``wt drain on Q --type bug`` makes the queue's
    workers claim only bugs and leave features for a human."""
    valid = {"bug", "feature"}
    norm = [t for t in (types or []) if t in valid]
    data = _load()
    q = data.setdefault(queue, {})
    if norm:
        q["claim_types"] = norm
    else:
        q.pop("claim_types", None)
    _save(data)
    return q


def claim_types(queue: str) -> list:
    """Return the configured claim-type restriction for a queue, or [] (all)."""
    v = _load().get(queue, {}).get("claim_types", [])
    return list(v) if isinstance(v, list) else []


def set_repo_path(queue: str, path: str) -> Dict[str, Any]:
    data = _load()
    q = data.setdefault(queue, {})
    q["repo_path"] = str(path)
    _save(data)
    return q


def repo_path(queue: str) -> str:
    """Return the configured repo_path for a queue, or empty string."""
    return _load().get(queue, {}).get("repo_path", "")


def set_engine(queue: str, eng: str) -> Dict[str, Any]:
    data = _load()
    q = data.setdefault(queue, {})
    q["engine"] = eng
    _save(data)
    return q


def engine(queue: str) -> str:
    return _load().get(queue, {}).get("engine", "claude")


def set_model(queue: str, m: str) -> Dict[str, Any]:
    """Set (or clear, with "") the model workers on this queue are spawned with."""
    data = _load()
    q = data.setdefault(queue, {})
    if m:
        q["model"] = str(m)
    else:
        q.pop("model", None)
    _save(data)
    return q


def _ccc_default_model(eng: str) -> str:
    """CCC's own default model for `eng`, read from its spawn-defaults.json
    (``{"models": {"claude": "sonnet-5", ...}}``). Returns "" if the file is
    missing, unreadable, or has no entry for this engine -- a fresh install
    or a machine without CCC installed just gets the pre-existing "" (ambient
    CLI default) behavior."""
    try:
        with open(CCC_SPAWN_DEFAULTS_FILE) as f:
            data = json.load(f)
        return str((data.get("models") or {}).get(eng) or "")
    except (OSError, ValueError, AttributeError):
        return ""


def model(queue: str) -> str:
    """Return the worker model for a queue: an explicit `wt set --model`
    override if one is configured, else CCC's shared default for this
    queue's engine (see CCC_SPAWN_DEFAULTS_FILE), else "" (the engine's own
    ambient default, e.g. the bare `claude` CLI's configured default)."""
    explicit = _load().get(queue, {}).get("model", "")
    if explicit:
        return explicit
    return _ccc_default_model(engine(queue))


def set_desired_workers(queue: str, n: int) -> Dict[str, Any]:
    data = _load()
    q = data.setdefault(queue, {})
    q["desired_workers"] = int(n)
    _save(data)
    return q


def desired_workers(queue: str) -> int:
    return int(_load().get(queue, {}).get("desired_workers", 1))


def all_queues() -> Dict[str, Any]:
    """Return all configured queues (any queue with an entry in the config file)."""
    return dict(_load())


def ensure_entry(queue: str) -> Dict[str, Any]:
    """Create a config entry for queue if none exists yet."""
    data = _load()
    if queue not in data:
        data[queue] = {}
        _save(data)
    return dict(data[queue])


_REGISTRY_FILE = Path.home() / ".watchtower" / "queue-registry.json"


def migrate_from_registry() -> int:
    """One-time import of legacy queue-registry.json into queue-config.json.

    Renames the source file to ``*.migrated`` so it won't be re-processed.
    Returns the number of queues imported.
    """
    if not _REGISTRY_FILE.exists():
        return 0
    try:
        import json as _json
        with open(_REGISTRY_FILE) as f:
            reg = _json.load(f)
    except (OSError, ValueError):
        return 0
    if not isinstance(reg, dict):
        return 0
    data = _load()
    count = 0
    for name, rec in reg.items():
        entry = data.setdefault(name, {})
        for key in (
            "auto_drain", "engine", "desired_workers", "repo_path",
            "backend", "github_repo", "github_assignee",
        ):
            if key in rec and key not in entry:
                entry[key] = rec[key]
        count += 1
    if count:
        _save(data)
    try:
        _REGISTRY_FILE.rename(_REGISTRY_FILE.with_suffix(".json.migrated"))
    except OSError:
        pass
    return count

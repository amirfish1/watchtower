"""Permanent registered checks attached to CLOSED tickets, never waiting work.

Checks emit JSON: status=pass|fail|pending, real_data=true for a real pass,
and evidence. A successful empty/fixture-only run cannot clear owner attention.
The scheduler runs checks; no result reopens the original ticket.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time

from . import storage


def _path():
    # A custom queue store is a separate environment, including in tests.
    override = os.environ.get("WATCHTOWER_STORE")
    if override:
        return Path(override).expanduser().absolute().with_suffix(".watches.json")
    return storage.data_dir() / "watches.json"


def _load():
    path = _path()
    return json.loads(path.read_text()) if path.exists() else {"checks": {}, "bindings": {}}


def _save(data):
    path = _path()
    with storage.atomic_destination(path) as tmp:
        tmp.write_text(json.dumps(data, indent=2) + "\n")


def _lock():
    return storage.file_lock(_path().with_suffix(".lock"))


def inventory():
    """Read current raw metadata without usage conversion or network calls.

    The daemon's existing poller owns GitHub freshness. Missing/stale coverage
    is explicit; historical prose snapshots cannot masquerade as live state.
    """
    from . import queue as q, config, github_backend as gh
    projects = config.all_queues()
    github = {p: cfg for p, cfg in projects.items()
              if isinstance(cfg, dict) and cfg.get("backend") == "github"}
    items = {it["ref"]: it for it in q._load_unlocked(readonly=True)["items"]
             if it.get("project") not in github}
    cache = gh._read_persisted_list_cache()
    coverage = []
    for project, cfg in github.items():
        repo = cfg.get("github_repo") or ""
        labels = config.github_queues_for_repo(repo)
        for state in ("open", "closed"):
            entry = cache.get(f"{repo}:{state}") or {}
            age = time.time()-float(entry.get("at") or 0)
            coverage.append(dict(source=f"{repo}:{state}", missing="data" not in entry,
                                 stale=bool(entry.get("stale")) or age > gh._persisted_list_ttl_s()))
            for raw in entry.get("data") or []:
                names = {v.get("name", "") for v in raw.get("labels") or [] if isinstance(v, dict)}
                if len(labels) > 1:
                    if config.is_catch_all(project):
                        if names.intersection(config.sibling_queue_labels(project, repo)):
                            continue
                    elif config.queue_label(project) not in names:
                        continue
                _body, meta = gh._split_body(str(raw.get("body") or ""))
                ref = f"{project}-{raw['number']}"
                items[ref] = dict(meta, ref=ref, project=project, title=raw.get("title") or "",
                                  created_at=raw.get("createdAt"), closed_at=raw.get("closedAt"),
                                  updated_at=raw.get("updatedAt") or meta.get("updated_at"),
                                  status=("closed" if raw.get("state") == "CLOSED" else
                                          "awaiting_release" if meta.get("release_wait") else
                                          "in_progress" if meta.get("claimed_by") or "watchtower:in-progress" in names else "open"),
                                  resolution={"watches": meta.get("resolution_watches") or []})
    return list(items.values()), coverage


def register(name, *, command, cwd, where, schedule, queue, every=300):
    from . import config
    if not re.fullmatch(r"[a-zA-Z0-9][\w.-]{0,99}", name):
        raise ValueError("watch name must be 1-100 letters, digits, dots, underscores or hyphens")
    if not all(isinstance(v, str) and v.strip() for v in (command, cwd, where, schedule, queue)):
        raise ValueError("watch needs command, cwd, where, schedule and failure queue")
    if not Path(cwd).is_dir() or not 60 <= int(every) <= 86400:
        raise ValueError("watch needs an existing cwd and a cadence of 60-86400 seconds")
    if config.backend(queue) == "github":
        raise ValueError("watch failure queue must be local so filing has atomic deduplication")
    spec = dict(name=name, command=command, cwd=str(Path(cwd).resolve()), where=where,
                schedule=schedule, queue=queue, every=int(every))
    spec["revision"] = hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()
    with _lock():
        data = _load()
        old = data["checks"].get(name)
        if old and old != spec:
            raise ValueError("registered check is immutable; use a new name for a changed command")
        data["checks"][name] = spec
        _save(data)
    return spec


def snapshots(names, ref):
    """Validate before close; include runnable specification in the resolution."""
    with _lock():
        checks = _load()["checks"]
        out = []
        for name in dict.fromkeys(names):
            if name not in checks:
                raise ValueError(f"watch {name!r} is not registered; register a runnable check first")
            out.append(dict(checks[name], origin=ref))
        return out


def sync(items):
    """Recover bindings from durable closes, including after a close-time crash."""
    with _lock():
        data = _load()
        for item in items:
            if item.get("status") != "closed":
                continue
            resolution = item.get("resolution")
            for spec in (resolution.get("watches") or []) if isinstance(resolution, dict) else []:
                # Issue bodies are external data, not authority to execute
                # shell commands. Only this host's immutable registry grants it.
                registered = data["checks"].get(spec.get("name"))
                if (not registered or spec != dict(registered, origin=item["ref"])):
                    continue
                key = item["ref"] + ":" + spec["name"]
                if key not in data["bindings"]:
                    data["bindings"][key] = dict(spec=spec, attached_at=item.get("closed_at"),
                                                last={}, first_real_pass="", episode=0)
                data["bindings"][key]["origin_item"] = {
                    k: item[k] for k in ("ref", "project", "title", "created_at", "status") if k in item}
        _save(data)
        return data


def owner_rows(items, project=""):
    """Read-only projection; also show closes not yet picked up by the scheduler."""
    data = _load()
    rows = []
    for item in items:
        if item.get("status") != "closed":
            continue
        resolution = item.get("resolution")
        for spec in (resolution.get("watches") or []) if isinstance(resolution, dict) else []:
            bound = data["bindings"].get(item["ref"] + ":" + spec["name"], {})
            if not bound.get("first_real_pass"):
                rows.append(dict(ref=item["ref"], check=spec["name"], command=spec["command"],
                                 where=spec["where"], schedule=spec["schedule"],
                                 last=bound.get("last") or {"status": "not-run"}))
    # Durable registrations outlive the display list's closed-ticket retention.
    visible = {it["ref"] for it in items}
    for bound in data["bindings"].values():
        spec = bound["spec"]
        if project:
            from .queue import _project_from_ident
            if _project_from_ident(spec["origin"]) != project:
                continue
        if spec["origin"] not in visible and not bound.get("first_real_pass"):
            rows.append(dict(ref=spec["origin"], check=spec["name"], command=spec["command"],
                             where=spec["where"], schedule=spec["schedule"],
                             last=bound.get("last") or {"status": "not-run"}))
    return rows


def _execute(spec):
    try:
        proc = subprocess.Popen(spec["command"], shell=True, cwd=spec["cwd"],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True, start_new_session=True)
    except OSError as exc:
        return dict(status="error", real_data=False,
                    evidence=f"cannot start registered check: {type(exc).__name__}")
    try:
        out, err = proc.communicate(timeout=30)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        proc.communicate()
        return dict(status="error", real_data=False, evidence="check exceeded 30 seconds")
    try:
        result = json.loads(out.strip().splitlines()[-1])
        if result.get("status") not in ("pass", "fail", "pending") or not result.get("evidence"):
            raise ValueError("missing status/evidence")
    except (ValueError, IndexError, AttributeError):
        return dict(status="error", real_data=False,
                    evidence=f"invalid check receipt (exit {proc.returncode}): {(err or out)[-1500:]}")
    if proc.returncode and result["status"] == "pass":
        return dict(status="error", real_data=False, evidence="nonzero exit cannot report pass")
    if result["status"] == "pass" and result.get("real_data") is not True:
        result["status"] = "pending"
    return {k: result.get(k) for k in ("status", "real_data", "evidence")}


def run(name="", *, now=None, items=None, limit=1):
    """Run due permanent checks with a bounded per-tick budget, not ticket timers."""
    from . import queue as q, related
    now = time.time() if now is None else now
    items = inventory()[0] if items is None else items
    data = sync(items)
    if name and name not in data["checks"]:
        raise ValueError(f"watch {name!r} is not registered")
    by_ref = {i["ref"]: i for i in items}
    results = []
    # A separate nonblocking run lock prevents overlapping daemon/manual runs;
    # the registry lock is held only for short reads and receipt writes.
    import fcntl
    path = _path().with_suffix(".run.lock")
    with path.open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return []
        data = _load()
        ordered = sorted(data["bindings"].items(), key=lambda kv: kv[1].get("last", {}).get("at", 0))
        for key, bound in ordered:
            spec = bound["spec"]
            original = by_ref.get(spec["origin"], bound.get("origin_item") or
                                  dict(ref=spec["origin"], status="closed"))
            if not original or original.get("status") != "closed":
                continue
            if name and spec["name"] != name:
                continue
            if not name and now - bound.get("last", {}).get("at", 0) < spec["every"]:
                continue
            result = dict(_execute(spec), at=now)
            if result["status"] == "pass":
                bound["first_real_pass"] = bound.get("first_real_pass") or q._now_iso()
                if bound.get("failure_ref"):
                    bound["episode"] += 1
                    bound.pop("failure_ref", None)
            if result["status"] in ("fail", "error"):
                failure = q.enqueue(project=spec["queue"], source="wt-watch",
                                    dedupe_key=f"watch:{key}:{bound['episode']}",
                                    title=f"Watch {spec['name']} failed ({spec['origin']})",
                                    note=f"Original: {spec['origin']}\nCheck: {spec['name']}\n"
                                         f"Command: {spec['command']}\nWhere: {spec['where']}\n"
                                         f"Evidence: {result['evidence']}\n"
                                         "Investigate this regression; do not reopen or park the original.")
                related.confirm(failure, original, "earlier-attempt",
                                f"Registered check {spec['name']} failed: {result['evidence']}", "watchtower-watch")
                bound["failure_ref"] = failure["ref"]
                result["failure_ref"] = failure["ref"]
            bound["last"] = result
            with _lock():
                current = _load()
                current["bindings"][key] = bound
                _save(current)
            results.append(dict(origin=spec["origin"], check=spec["name"], **result))
            if len(results) >= limit:
                break
    return results


_runner = None


def schedule():
    """Never put a potentially slow check on the daemon's dispatch path."""
    global _runner
    import threading
    if _runner is not None and _runner.is_alive():
        return

    def work():
        try:
            run()
        except Exception as exc:
            from .queue import _log
            _log("WATCH_ERROR", str(exc)[:200])

    _runner = threading.Thread(target=work, name="watchtower-check", daemon=True)
    _runner.start()

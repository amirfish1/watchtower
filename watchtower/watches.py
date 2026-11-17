"""Permanent registered checks attached to CLOSED tickets, never waiting work.

Checks emit JSON: status=pass|fail|pending, real_data=true for a real pass,
and evidence. A successful empty/fixture-only run cannot clear owner attention.
The scheduler runs checks; no result reopens the original ticket.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import shlex
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


def _validate_evidence_rule(spec):
    # Cadence and evidence cohort durations are fine; a calendar is not proof.
    text = " ".join(str(spec.get(k) or "") for k in ("schedule", "command"))
    if re.search(r"pass(?:es|ing)?\s+(?:only\s+)?(?:after|on|at|not before)\s+[\"\']?\d{4}-\d{2}-\d{2}(?!\d)|"
                 r"original\s+24\s*h(?:our)?\s+window", text, re.I):
        raise ValueError("watch must pass on evidence, not a date-keyed pass condition")


def register(name, *, command, cwd, where, schedule, queue, every=300,
             replace=False, reason=""):
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
    _validate_evidence_rule(spec)
    spec["revision"] = hashlib.sha256(json.dumps(spec, sort_keys=True).encode()).hexdigest()
    with _lock():
        data = _load()
        old = data["checks"].get(name)
        if old and old != spec:
            if not replace or not reason.strip():
                raise ValueError("registered check is immutable; replacement needs --replace --reason")
            data.setdefault("revisions", {}).setdefault(name, []).append(
                dict(spec=old, reason=reason, replaced_at=time.time()))
            for bound in data["bindings"].values():
                if bound["spec"]["name"] == name:
                    bound.setdefault("receipt_history", []).append(dict(
                        spec=bound["spec"], last=bound["last"],
                        first_real_pass=bound.get("first_real_pass", "")))
                    bound.update(spec=dict(spec, origin=bound["spec"]["origin"]),
                                 last={}, first_real_pass="", episode=bound.get("episode", 0) + 1)
        data["checks"][name] = spec
        data.setdefault("registered_at", {}).setdefault(name, time.time())
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
            _validate_evidence_rule(checks[name])
            out.append(dict(checks[name], origin=ref))
        return out


def _trusted_spec(data, snapshot):
    """Only locally authorized specs execute; inherited origin is attribution."""
    name = snapshot.get("name")
    current = data["checks"].get(name)
    candidates = [current] + [r["spec"] for r in data.get("revisions", {}).get(name, [])]
    supplied = {k: v for k, v in snapshot.items() if k != "origin"}
    return current if current and supplied in candidates else None


def sync(items):
    """Recover bindings from durable closes, including inherited duplicate closes."""
    with _lock():
        data = _load()
        for item in items:
            if item.get("status") != "closed":
                continue
            resolution = item.get("resolution")
            for snapshot in (resolution.get("watches") or []) if isinstance(resolution, dict) else []:
                registered = _trusted_spec(data, snapshot)
                if not registered:
                    continue
                spec = dict(registered, origin=item["ref"])
                key = item["ref"] + ":" + spec["name"]
                if key not in data["bindings"]:
                    # The same check already ran for a sibling; inherit that
                    # receipt instead of inventing a second execution clock.
                    peers = [b for b in data["bindings"].values()
                             if b["spec"]["revision"] == spec["revision"]]
                    peer = max(peers, key=lambda b: b.get("last", {}).get("at", 0), default={})
                    data["bindings"][key] = dict(spec=spec, attached_at=item.get("closed_at"),
                                                last=peer.get("last", {}),
                                                first_real_pass=peer.get("first_real_pass", ""),
                                                episode=peer.get("episode", 0))
                data["bindings"][key]["origin_item"] = {
                    k: item[k] for k in ("ref", "project", "title", "created_at", "status") if k in item}
        _save(data)
        return data


def add_source(host, registry):
    """Authorize metadata reads only. Remote specs never enter ``checks``."""
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.@-]*", host):
        raise ValueError("source host must be an SSH host or alias")
    if not registry.startswith("/") or "\n" in registry:
        raise ValueError("source registry must be an absolute path")
    with _lock():
        data = _load()
        data.setdefault("sources", {})[host] = dict(registry=registry)
        _save(data)
    return dict(host=host, registry=registry)


def _source_data(host, source):
    """Read an explicitly authorized registry with a bounded, cached SSH read."""
    now = time.time()
    cache = source.get("cache") or {}
    if now - source.get("attempted_at", 0) >= 60:
        try:
            proc = subprocess.run(
                ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=3", host,
                 "cat -- " + shlex.quote(source["registry"])],
                capture_output=True, text=True, timeout=5)
            if proc.returncode:
                raise ValueError(f"SSH registry read exited {proc.returncode}")
            remote = json.loads(proc.stdout)
            if not isinstance(remote, dict) or not all(
                    isinstance(remote.get(k), dict) for k in ("checks", "bindings")):
                raise ValueError("invalid watch registry")
            # No source chaining; retain only metadata used in the projection.
            cache = {k: remote.get(k, {}) for k in
                     ("checks", "bindings", "revisions", "registered_at")}
            source.update(cache=cache, fetched_at=now, error="")
        except (OSError, ValueError, subprocess.TimeoutExpired) as exc:
            source["error"] = str(exc)
        source["attempted_at"] = now
        with _lock():
            current = _load()
            # A concurrent source edit must not be overwritten.
            target = current.get("sources", {}).get(host)
            if target and target["registry"] == source["registry"]:
                target.update(source)
                _save(current)
    warning = ""
    if source.get("error") or not source.get("fetched_at"):
        warning = f"Watch source {host} unavailable; cached metadata may be stale: {source.get('error', 'not fetched')}"
    return cache, warning


def _owner_candidates(data, items, *, remote=False):
    candidates = {k: dict(b) for k, b in data["bindings"].items()}
    for item in items:
        if item.get("status") != "closed":
            continue
        resolution = item.get("resolution")
        for snapshot in (resolution.get("watches") or []) if isinstance(resolution, dict) else []:
            trusted = _trusted_spec(data, snapshot)
            if remote and not trusted:
                continue
            spec = dict(trusted or snapshot, origin=item["ref"])
            key = item["ref"] + ":" + spec["name"]
            if key not in candidates:
                candidates[key] = dict(spec=spec, attached_at=item.get("closed_at"), last={})
    # Old existing duplicate bindings can predate receipt fan-out. An identical
    # registered revision has one execution clock even before the next sync.
    for bound in candidates.values():
        revision = bound["spec"].get("revision")
        peers = [b for b in data["bindings"].values()
                 if revision and b["spec"].get("revision") == revision
                 and {k: v for k, v in b["spec"].items() if k != "origin"}
                 == {k: v for k, v in bound["spec"].items() if k != "origin"}]
        peer = max(peers, key=lambda b: b.get("last", {}).get("at", 0), default={})
        if peer.get("last", {}).get("at", 0) > bound.get("last", {}).get("at", 0):
            bound.update(last=peer["last"], first_real_pass=peer.get("first_real_pass", ""))
    return candidates


def owner_rows(items, project="", *, source_warnings=None):
    """Read-only watch projection; remote metadata cannot authorize execution."""
    data = _load()
    candidates = _owner_candidates(data, items)
    for host, source in data.get("sources", {}).items():
        remote, warning = _source_data(host, source)
        if warning and source_warnings is not None:
            source_warnings.append(warning)
        if not remote:
            continue
        for key, bound in _owner_candidates(remote, items, remote=True).items():
            # A local registration owns its own clock; remote sources only
            # replace historical issue snapshots for remotely registered checks.
            if bound["spec"]["name"] in data["checks"]:
                continue
            candidates[key] = dict(bound, source_host=host, source_warning=warning,
                                   registered_at=remote.get("registered_at", {}).get(bound["spec"]["name"]))
    rows = []
    for bound in candidates.values():
        spec = bound["spec"]
        if project:
            from .queue import _project_from_ident
            if _project_from_ident(spec["origin"]) != project:
                continue
        last = bound.get("last") or {"status": "not-run"}
        if bound.get("first_real_pass") and last["status"] not in ("error", "fail") and not bound.get("source_warning"):
            continue
        registered = bound.get("registered_at") or data.get("registered_at", {}).get(spec["name"])
        if not registered:
            try:
                registered = dt.datetime.fromisoformat(bound.get("attached_at", "").replace("Z", "+00:00")).timestamp()
            except (ValueError, AttributeError, TypeError):
                registered = None
        overdue = bool(last["status"] == "not-run" and registered is not None
                       and time.time() - registered >= 2 * spec["every"])
        rows.append(dict(ref=spec["origin"], check=spec["name"], command=spec["command"],
                         where=spec["where"], schedule=spec["schedule"], last=last,
                         overdue=overdue, source_host=bound.get("source_host", ""),
                         warning="; ".join(filter(None, [
                             "OVERDUE: never ran after two intervals" if overdue else "",
                             bound.get("source_warning", "")]))))
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
        groups = {}
        for key, bound in data["bindings"].items():
            spec = bound["spec"]
            original = by_ref.get(spec["origin"], bound.get("origin_item") or
                                  dict(ref=spec["origin"], status="closed"))
            if original.get("status") == "closed" and (not name or spec["name"] == name):
                groups.setdefault((spec["name"], spec["revision"]), []).append((key, bound, original))
        ordered = sorted(groups.values(), key=lambda g: max(b.get("last", {}).get("at", 0) for _, b, _ in g))
        executed = 0
        for group in ordered:
            _, representative, _ = group[0]
            spec = representative["spec"]
            last_at = max(b.get("last", {}).get("at", 0) for _, b, _ in group)
            if not name and last_at and now - last_at < spec["every"]:
                continue
            result = dict(_execute(spec), at=now)
            episode = max(b.get("episode", 0) for _, b, _ in group)
            failure = None
            if result["status"] in ("fail", "error"):
                origins = ", ".join(b["spec"]["origin"] for _, b, _ in group)
                failure = q.enqueue(project=spec["queue"], source="wt-watch",
                                    dedupe_key=f"watch:{group[0][0]}:{episode}",
                                    title=f"Watch {spec['name']} failed ({spec['origin']})",
                                    note=f"Originals: {origins}\nCheck: {spec['name']}\n"
                                         f"Command: {spec['command']}\nWhere: {spec['where']}\n"
                                         f"Evidence: {result['evidence']}\n"
                                         "Investigate this regression; do not reopen or park the original.")
                result["failure_ref"] = failure["ref"]
            with _lock():
                current = _load()
                for key, bound, original in group:
                    # A concurrent explicit replacement invalidates this receipt.
                    target = current["bindings"].get(key)
                    if not target or target["spec"]["revision"] != spec["revision"]:
                        continue
                    if result["status"] == "pass":
                        bound["first_real_pass"] = bound.get("first_real_pass") or q._now_iso()
                        if bound.get("failure_ref"):
                            bound["episode"] += 1
                            bound.pop("failure_ref", None)
                    if failure:
                        bound["failure_ref"] = failure["ref"]
                    bound["last"] = result
                    current["bindings"][key] = bound
                    results.append(dict(origin=bound["spec"]["origin"], check=spec["name"], **result))
                _save(current)
            if failure:
                for _, _, original in group:
                    related.confirm(failure, original, "earlier-attempt",
                                    f"Registered check {spec['name']} failed: {result['evidence']}", "watchtower-watch")
            executed += 1
            if executed >= limit:
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

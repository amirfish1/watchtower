"""Bounded observations of automatic GitHub staffing decisions (WT-51).

This records the reads the reconciler already made. It neither fetches from
GitHub nor treats those separate reads as an atomic snapshot.
"""
from __future__ import annotations

import json
import os
import time
from typing import Any

_LAST: dict[tuple, tuple[str, float]] = {}
_REPEAT_S = 300.0
_SAMPLE_LIMIT = 10


def record(queue: str, *, items: list[dict], depth: int | None,
           total_open: int, health: dict, live: int, desired: int,
           reconcile_id: str, scoped: bool, reason: str,
           dry_run: bool = False) -> dict[str, Any] | None:
    """Log changes and five-minute heartbeats; failures never gate staffing."""
    from . import config, github_backend, queue as q

    if config.backend(queue) != "github" or not config.auto_drain(queue):
        return None
    key = (str(q._resolve_activity_log_path()), queue)
    previous = _LAST.get(key)
    if not total_open and not depth and previous is None:
        return None
    fields = ("ref", "created_at", "updated_at", "auto_eligible",
              "manual_eligible", "work_it", "claimable", "no_auto_drain",
              "run_requested", "readiness", "model_floor", "deferred_until",
              "pipeline_foreign", "pipeline_missing", "needs_input",
              "watchtower_runnable", "watchtower_label", "type", "placement",
              "duplicate_of")
    sample = []
    oldest = sorted(items, key=lambda it: (str(it.get("created_at") or ""),
                                           str(it.get("ref") or "")))
    for item in oldest[:_SAMPLE_LIMIT]:
        row = {k: (v[:128] if isinstance(v, str) else v)
               for k in fields if isinstance((v := item.get(k)), (str, bool))}
        row["blocked_by"] = [str(ref)[:80] for ref in (item.get("blocked_by") or [])[:10]]
        row["affinity_until"] = str((item.get("pending_answer") or {}).get("affinity_until") or "")[:128]
        sample.append(row)
    repo = config.github_repo(queue)
    observation = {
        "queue": queue, "repo": repo, "auto_drain": True,
        "grace_s": config.grace_s(queue), "claim_types": config.claim_types(queue),
        "worker_model": config.queue_model_id(queue),
        "queue_label": config.queue_label(queue),
        "partition_by_label": len(config.github_queues_for_repo(repo)) > 1,
        "desired": desired,
        "live": live, "listed_open": total_open, "counted_claimable": depth,
        "health_claimable": health.get("claimable_depth"),
        "health_state": health.get("state"), "reason": reason,
        "listed_items": sample, "omitted_items": max(0, len(items) - len(sample)),
    }
    # Fetch/validation clocks move on every poll: retain them as evidence,
    # but don't let those clocks turn the heartbeat into a per-tick log.
    fingerprint = json.dumps(observation, sort_keys=True)
    now = time.monotonic()
    cache = {}
    for state in ("open", "closed"):
        cache_key = f"{repo}:{state}"
        entry = github_backend._LIST_CACHE.get(cache_key) or {}
        cache[state] = {k: entry.get(k) for k in
                        ("at", "fetched_at", "stale", "stale_local")}
        cache[state]["error"] = bool(entry.get("error"))
        cache[state]["numbers"] = [it.get("number") for it in
                                   (entry.get("data") or [])[:_SAMPLE_LIMIT]]
        # Inspect the already-parsed shared snapshot, without opening its
        # file or refreshing it. This can expose private/shared read drift.
        shared = github_backend._PERSISTED_LIST_MEM_CACHE["data"].get(cache_key) or {}
        cache[state]["shared"] = {k: shared.get(k) for k in
                                  ("at", "fetched_at", "stale")}
        cache[state]["shared"]["numbers"] = [it.get("number") for it in
                                              (shared.get("data") or [])[:_SAMPLE_LIMIT]]
    observation.update(reconcile_id=reconcile_id, pid=os.getpid(),
                       scoped=scoped, cache=cache)
    if dry_run:
        return observation
    if previous and previous[0] == fingerprint and now - previous[1] < _REPEAT_S:
        return observation
    if q._log("ELIGIBILITY", json.dumps(observation, sort_keys=True), queue=queue):
        if len(_LAST) >= 512:
            _LAST.pop(next(iter(_LAST)))
        _LAST[key] = (fingerprint, now)
        if not total_open and not depth:
            _LAST.pop(key, None)  # record the drain once, then remain quiet
    return observation

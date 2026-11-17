"""Read natural fresh-ticket startup evidence; never launch workers or models."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import re
import time


def instant(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def check(path, *, since, lookup, now=None, threshold=30,
          queues=("WT", "CCC", "DECISIONS-FIX"), window=86400):
    """Single-worker initial staffing only; backlog/reclaims are not startups.

    The durable ticket's creation time authenticates ENQUEUE records: test runs
    on this host have leaked fixture events into the shared activity log.
    SPAWN ties the worker to its plan; DISPATCH ties it to the filed ticket.
    A worker's FIRST claim must match that ticket, or it is backlog exposure.
    """
    now = time.time() if now is None else now
    cutoff = max(since, now - window)
    path = Path(path)
    if not path.exists() or now - path.stat().st_mtime > 600:
        return dict(status="fail", real_data=False, evidence="activity log missing/stale (>10 minutes)")
    enqueues, plans, starts, spawns, claims, dispatches = {}, {}, {}, {}, {}, []
    gated = {}
    for line in path.open():
        m = re.match(r"(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d) UTC\s+(\S+)\s+(\S+)\s+(.*)", line)
        if not m:
            continue
        stamp, queue, event, text = m.groups()
        if queue not in queues:
            continue
        stamp = datetime.strptime(stamp, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
        if not cutoff <= stamp <= now:
            continue
        if event == "ENQUEUE":
            enqueues[text.split()[0]] = (stamp, queue)
        elif event == "RUN":
            gated.setdefault(text.split()[0], []).append((stamp, "explicit run, not immediate enqueue dispatch"))
        elif event == "SPAWN_PLAN":
            rid = re.search(r"reconcile_id=(\S+)", text)
            if (rid and "cause=initial_staffing " in text
                    and "eligible_after=0 " in text and "requested=1 " in text):
                plans[rid[1]] = (stamp, queue)
        elif event == "SPAWN_START":
            wid = re.search(r"worker_id=(\S+)", text)
            if wid:
                starts[wid[1]] = (stamp, queue)
        elif event == "SPAWN":
            wid = re.search(r"worker_id=(\S+)", text)
            rid = re.search(r"reconcile_id=(\S+)", text)
            if wid and rid:
                spawns[wid[1]] = rid[1]
        elif event == "CLAIM":
            claim = re.match(r"(\S+) by (\S+)", text)
            if claim:
                claims.setdefault((queue, claim[2]), (stamp, claim[1]))
        elif event == "DISPATCH":
            dispatch = re.match(r"(\S+) — spawned worker (\S+)", text)
            if dispatch:
                dispatches.append((queue, dispatch[1], dispatch[2]))
            else:
                gated.setdefault(text.split()[0], []).append((stamp, "initial dispatch did not launch (capacity/readiness/grace/warm pickup)"))

    samples, failures, gaps, excluded = [], [], [], 0
    for queue, ref, worker in dict.fromkeys(dispatches):
        enqueue = enqueues.get(ref)
        start = starts.get(worker)
        plan = plans.get(spawns.get(worker))
        if not (enqueue and start and plan and enqueue[1] == start[1] == plan[1] == queue
                and enqueue[0] <= plan[0] <= start[0]):
            excluded += 1
            continue
        if any(enqueue[0] <= stamp <= start[0] for stamp, _reason in gated.get(ref, [])):
            excluded += 1
            continue
        ticket = lookup(ref)
        if not ticket:
            gaps.append(ref + ": durable ticket missing")
            continue
        try:
            created = instant(ticket["created_at"])
        except (KeyError, ValueError, TypeError):
            gaps.append(ref + ": creation time unreadable")
            continue
        if abs(created - enqueue[0]) > 5:
            excluded += 1
            continue
        matches = [v for (q, w), v in claims.items()
                   if q == queue and worker.startswith(w)]
        if len(matches) > 1:
            gaps.append(ref + ": ambiguous worker prefix")
            continue
        if matches and matches[0][1] != ref:
            excluded += 1  # worker claimed older work first
            continue
        claimed = matches[0][0] if matches else None
        elapsed = (claimed if claimed is not None else now) - enqueue[0]
        row = dict(ref=ref, worker=worker, enqueue_to_process_s=start[0]-enqueue[0],
                   process_to_claim_s=claimed-start[0] if claimed is not None else None,
                   enqueue_to_claim_s=elapsed if claimed is not None else None,
                   age_s=now-enqueue[0])
        if elapsed > threshold:
            failures.append(row)
        elif claimed is not None:
            samples.append(row)
        else:
            gaps.append(ref + ": first claim not yet observed")
    evidence = dict(threshold_s=threshold, since=cutoff, through=now,
                    samples=samples, slow_or_unclaimed=failures, gaps=gaps,
                    excluded=excluded,
                    gated=gated,
                    scope="fresh local tickets, initial staffing from zero, one worker; not backlog or warm pickup")
    status = "fail" if failures else "pending" if gaps or not samples else "pass"
    return dict(status=status, real_data=bool(samples or failures),
                evidence=json.dumps(evidence, sort_keys=True))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--log", default=str(Path.home()/".watchtower/activity.log"))
    parser.add_argument("--since", required=True, help="ISO UTC monitoring baseline")
    parser.add_argument("--threshold", type=float, default=30)
    args = parser.parse_args(argv)
    from . import queue
    try:
        result = check(args.log, since=instant(args.since), lookup=queue.get,
                       threshold=args.threshold)
    except Exception as exc:
        result = dict(status="fail", real_data=False,
                      evidence=f"startup evidence unreadable: {type(exc).__name__}: {exc}")
    print(json.dumps(result))


if __name__ == "__main__":
    main()

"""Read-only check adapters; JSON receipts distinguish absent data from success.

These read existing production collectors. They never run the collectors (which
may send messages), generate traffic, call models or mutate production data.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import time


def production_logs(directory, route, pattern, since, success_pattern="", now=None):
    now = time.time() if now is None else now
    state = Path(directory) / "state.json"
    if not state.exists() or now - state.stat().st_mtime > 1800:
        return dict(status="fail", real_data=False, evidence="existing production log collector is missing/stale")
    meta = json.loads(state.read_text())
    if meta.get("fails"):
        return dict(status="fail", real_data=False, evidence="existing production log collector reports fetch failures")
    events = Path(directory) / "alerted.jsonl"
    matches = []
    if events.exists():
        for line in events.read_text().splitlines():
            row = json.loads(line)
            if row.get("log_ts_ms", row.get("ts", 0) * 1000) / 1000 >= since and row.get("path") == route:
                matches.append(row)
    failures = [r for r in matches if re.search(pattern, r.get("excerpt", ""), re.I)
                and r.get("level") in ("error", "fatal")]
    if failures:
        last = failures[-1]
        return dict(status="fail", real_data=True,
                    evidence=f"production log {last.get('log_id')} on {last.get('deployment')}: {last.get('excerpt')}")
    successes = [r for r in matches if success_pattern and r.get("level") == "warning"
                 and r.get("status") == 307 and re.search(success_pattern, r.get("excerpt", ""), re.I)]
    if successes:
        return dict(status="pass", real_data=True,
                    evidence=f"natural warning-level 307 with expected condition; log {successes[-1].get('log_id')}")
    return dict(status="pending", real_data=False,
                evidence="Production alert collector is current; no matching error or qualifying success. "
                         "This checks its deduplicated alert feed, not all successful requests.")


def related_feedback(since):
    from . import related
    items, _coverage = related.cached_tickets()
    report = related.feedback_report(items, since=related.instant(since))
    return dict(status="pass" if report["rated"] else "pending", real_data=bool(report["rated"]),
                evidence="Natural worker close feedback: " + json.dumps(report, sort_keys=True))


def ticket_timers(items=None):
    """Audit real ticket storage, including hidden GitHub compatibility fields."""
    from . import queue as q
    from .github_backend import _split_body
    coverage = []
    if items is None:
        from .watches import inventory
        items, coverage = inventory()
    bad = []
    for item in items:
        meta = _split_body(item["_github_body"])[1] if item.get("_github_body") else item
        if meta.get("deferred_until"):
            bad.append(item["ref"])
    if bad:
        return dict(status="fail", real_data=True,
                    evidence=f"Retired timer fields remain on {len(bad)} tickets: " + ", ".join(bad[:20]))
    gaps = sorted({c["source"] for c in coverage if c["missing"] or c["stale"]})
    if gaps:
        return dict(status="pending", real_data=False,
                    evidence=f"Zero timers in {len(items)} available records; poller coverage incomplete/stale: " + ", ".join(gaps))
    return dict(status="pass" if items else "pending", real_data=bool(items),
                evidence=f"Audited {len(items)} current raw ticket records: zero deferred_until fields, including release/pipeline fences. Older closed history is outside the live poller inventory.")


def spawn_pacing(since, path=None, now=None, window=120, limit=4):
    """WT-126: automatic cold starts stay paced during a real staffing wave.

    ``limit`` is the 3-per-window cap plus one manual-run or launch-failure
    fallback start, which pacing deliberately does not hold back. Pass needs
    a natural wave: at least one window-cap deferral, so pacing was exercised.
    """
    from datetime import datetime, timezone
    from . import queue as q
    now = time.time() if now is None else now
    since = datetime.fromisoformat(since.replace("Z", "+00:00")).timestamp()
    path = Path(path) if path else q._resolve_activity_log_path()
    if now - path.stat().st_mtime > 600:
        return dict(status="pending", real_data=False, evidence="activity log stale (>10 minutes)")
    starts, deferred, load_deferred = [], 0, 0
    with path.open(errors="replace") as fh:
        for line in fh:
            if " SPAWN_START " not in line and " SPAWN_DEFERRED " not in line:
                continue
            try:
                at = datetime.strptime(line[:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc).timestamp()
            except ValueError:
                continue
            if at < since:
                continue
            if " SPAWN_START " in line:
                starts.append(at)
            elif "cold starts in the last" in line:
                deferred += 1
            elif "host load" in line:
                load_deferred += 1
    starts.sort()
    peak, lo = 0, 0
    for hi, at in enumerate(starts):
        while at - starts[lo] >= window:
            lo += 1
        peak = max(peak, hi - lo + 1)
    evidence = (f"since {datetime.fromtimestamp(since, timezone.utc):%Y-%m-%dT%H:%M:%SZ}: "
                f"{len(starts)} SPAWN_START, peak {peak} in any {window}s (limit {limit}); "
                f"{deferred} window deferrals, {load_deferred} load deferrals")
    if peak > limit:
        return dict(status="fail", real_data=True, evidence="cold starts not paced: " + evidence)
    return dict(status="pass" if deferred else "pending", real_data=bool(deferred),
                evidence=evidence if deferred else "no staffing wave yet: " + evidence)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="kind", required=True)
    p = sub.add_parser("production-logs")
    p.add_argument("--directory", default=str(Path.home()/".local/state/bym-prod-log-monitor"))
    p.add_argument("--route", required=True)
    p.add_argument("--pattern", required=True)
    p.add_argument("--since", type=float, required=True)
    p.add_argument("--success-pattern", default="")
    p = sub.add_parser("related-feedback")
    p.add_argument("--since", required=True)
    sub.add_parser("ticket-timers")
    p = sub.add_parser("spawn-pacing")
    p.add_argument("--since", required=True)
    args = parser.parse_args(argv)
    try:
        if args.kind == "ticket-timers":
            result = ticket_timers()
        elif args.kind == "spawn-pacing":
            result = spawn_pacing(args.since)
        elif args.kind == "related-feedback":
            result = related_feedback(args.since)
        else:
            result = production_logs(args.directory, args.route, args.pattern, args.since, args.success_pattern)
    except (OSError, ValueError, TypeError) as exc:
        result = dict(status="fail", real_data=False, evidence=f"check source unreadable: {type(exc).__name__}")
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

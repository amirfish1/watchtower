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
    args = parser.parse_args(argv)
    try:
        result = (related_feedback(args.since) if args.kind == "related-feedback" else
                  production_logs(args.directory, args.route, args.pattern, args.since, args.success_pattern))
    except (OSError, ValueError, TypeError) as exc:
        result = dict(status="fail", real_data=False, evidence=f"check source unreadable: {type(exc).__name__}")
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

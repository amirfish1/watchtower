#!/usr/bin/env python3
"""Retrospective local recall evaluation, with no network or answer-key terms.

Rebuilds a disposable FTS index at each later ticket's first claim. Future
activity, resolutions and tickets are excluded. Descriptions are not versioned;
their current authored text is the explicit residual historical limitation.
"""
import argparse
from collections import Counter
import json
from pathlib import Path
import re
import sys
import tempfile
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from watchtower import related


def cases(items, cutoff):
    by_ref = {i["ref"]: i for i in items}
    out = []
    for it in items:
        if related.instant(it.get("created_at")) < cutoff:
            continue
        targets = {it.get("duplicate_of")} - {None, ""}
        targets.update(h.get("original") or h.get("of") or h.get("duplicate_of")
                       for h in it.get("history") or [] if h.get("event") == "duplicate_link")
        targets.discard(None)
        at = next((related.instant(h["at"]) for h in it.get("history") or []
                   if h.get("event") == "claim"), related.instant(it.get("created_at")))
        eligible = {r for r in targets if r in by_ref and related.instant(by_ref[r].get("created_at")) <= at}
        if eligible:
            out.append(dict(ref=it["ref"], at=at, expected=sorted(eligible), cohort="duplicate"))
    # Monitor repeats are a separate cohort: same normalized monitor title.
    groups = {}
    for it in sorted(items, key=lambda i: i.get("created_at") or ""):
        title = it.get("title") or ""
        if "[watchdog]" not in title.lower() or related.instant(it.get("created_at")) < cutoff:
            continue
        key = re.sub(r"https?://\S+", "URL", title.lower())
        key = re.sub(r"\d+", "N", key)
        earlier = groups.setdefault(key, [])
        at = related.instant(it.get("claimed_at")) or related.instant(it.get("created_at"))
        if earlier:
            out.append(dict(ref=it["ref"], at=at, expected=list(earlier), cohort="monitor"))
        earlier.append(it["ref"])
    return out


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True)
    parser.add_argument("--max-cases", type=int, default=0)
    args = parser.parse_args()
    items, coverage = related.cached_tickets()
    now = time.time()
    tests = cases(items, now-30*related.DAY)
    if args.max_cases: tests = tests[:args.max_cases]
    by_ref = {i["ref"]: i for i in items}
    results = []
    with tempfile.TemporaryDirectory(prefix="wt-related-eval-") as directory:
        with related.connect(Path(directory)/"eval.db") as conn:
            for case in sorted(tests, key=lambda c: c["at"]):
                at = case["at"]
                # Keep row identity stable while replacing prose at each instant.
                eligible = set()
                with conn:
                    for it in items:
                        created = related.instant(it.get("created_at"))
                        if not (at-30*related.DAY <= created <= at):
                            continue
                        snapshot = related.as_of(it, at)
                        eligible.add(it["ref"])
                        related.upsert(conn, it["ref"], "ticket", created, created,
                                       it.get("title") or "", related.full_text(snapshot), snapshot)
                    for row in conn.execute("SELECT id,ref FROM docs").fetchall():
                        if row["ref"] not in eligible:
                            conn.execute("DELETE FROM recall WHERE rowid=?", (row["id"],))
                            conn.execute("DELETE FROM docs WHERE id=?", (row["id"],))
                query = related.as_of(by_ref[case["ref"]], at)
                start = time.monotonic()
                hits = related.search(conn, query, at=at)
                refs = [h["ref"] for h in hits]
                rank = next((i+1 for i,r in enumerate(refs) if r in case["expected"]), None)
                results.append(dict(case, hits=refs, rank=rank, top5=rank is not None and rank<=5,
                                    query_ms=round((time.monotonic()-start)*1000,1)))
                if len(results) % 20 == 0:
                    print(f"evaluated {len(results)}/{len(tests)}", file=sys.stderr, flush=True)
    summary = {}
    for name in ("duplicate", "monitor", "all"):
        rows = [r for r in results if name=="all" or r["cohort"]==name]
        summary[name] = dict(cases=len(rows), top5_hits=sum(r["top5"] for r in rows),
                             hit_rate=sum(r["top5"] for r in rows)/len(rows) if rows else None)
    result = dict(summary=summary, coverage=coverage, results=results,
                  limitations=["Original descriptions are not versioned; current prose retained, references removed from queries.",
                               "Only locally available snapshots; missing historical GitHub tickets are not invented.",
                               "Candidate window uses created_at, excluding old tickets updated recently for retrospective consistency."])
    Path(args.out).write_text(json.dumps(result, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__": main()

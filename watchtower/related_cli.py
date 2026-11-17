"""CLI boundary for local recall and explicit semantic triage."""
import json
import sys
import urllib.error
import sqlite3
from . import related


def render(result):
    lines = ["Related work (local snapshots; verify before acting):"]
    for h in result.get("hits", []):
        judgment = h.get("judgment") or {}
        label = f"; Jev: {judgment['choice']}" if judgment else ""
        lines.append(f"  {h['ref']} [{h['status']}{label}] {h['title'] or '(untitled)'}")
        lines.append("    " + " ".join(h.get("excerpt", "").split())[:450])
    if not result.get("hits") and not result.get("error"):
        lines.append("  No matching tickets in the available snapshots.")
    if result.get("error"):
        lines.append("  Recall unavailable: " + result["error"])
    missing = [c for c in result.get("coverage", []) if c.get("missing") or
               c.get("possibly_capped") or c.get("stale") or not c.get("comments_complete")]
    if missing:
        lines.append("  Coverage is partial: cached GitHub history/comments may be missing or stale.")
    if result.get("instructions"):
        lines.append(result["instructions"])
    for c in result.get("commits", {}).get("hits", []):
        lines.append(f"  Commit {c['commit'][:12]} in {c['repo']}: {c['title']}")
    if result.get("commits", {}).get("warning"):
        lines.append(result["commits"]["warning"])
    return "\n".join(lines)


def claim_context(item, worker):
    try:
        result = related.lookup(item, worker=worker)
        # Keep JSON small; full candidate prose remains available via related.
        result["hits"] = [{k: v for k, v in h.items() if k != "item"} for h in result["hits"]]
        return result
    except (OSError, ValueError, sqlite3.Error) as exc:
        return {"hits": [], "error": str(exc),
                "instructions": "Recall failed; run wt related REF before implementing. "
                "Do not interpret this as no related work."}


def command(args):
    try:
        if args.refresh_github:
            from . import related_snapshots
            result = {"refreshed": related_snapshots.hydrate(repo=args.repo)}
            with related.connect() as conn:
                conn.execute("DELETE FROM meta WHERE key='refreshed_at'")
                conn.commit()
                related.refresh(conn)
        elif args.feedback_report:
            items, _ = related.cached_tickets()
            result = related.feedback_report(items, since=related.instant(args.since))
        else:
            item = related.get_cached(args.ref)
            if args.confirm:
                other = related.get_cached(args.confirm)
                worker = args.worker or "worker"
                message = related.confirm(item, other, args.kind, args.evidence, worker)
                result = {"confirmation": message}
            else:
                result = related.lookup(item, limit=args.limit)
                if args.triage:
                    result["triage"] = related.triage(item, result["hits"])
                result["commits"] = related.commit_hits(item)
                result["hits"] = [{k: v for k, v in h.items() if k != "item"} for h in result["hits"]]
        if args.json:
            print(json.dumps(result, indent=2))
        elif "confirmation" in result:
            print(result["confirmation"])
        elif args.feedback_report or args.refresh_github:
            print(json.dumps(result, indent=2))
        else:
            print(render(result))
        return 0
    except (ValueError, OSError, RuntimeError, sqlite3.Error, urllib.error.URLError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


def add_parser(sub):
    s = sub.add_parser("related", help="find related work using full ticket text and local snapshots (no gh calls)")
    s.add_argument("ref", nargs="?", default="")
    s.add_argument("--limit", type=int, default=10, choices=range(1, 31), metavar="N")
    s.add_argument("--triage", action="store_true", help="ask Jev to classify candidates; needs TYPESAFE_API_KEY")
    s.add_argument("--confirm", default="", metavar="REF", help="record an evidence-backed link on this ticket")
    s.add_argument("--kind", choices=related.KINDS, default="")
    s.add_argument("--evidence", default="", help="what you verified; model judgments alone are not evidence")
    s.add_argument("--worker", default="")
    s.add_argument("--feedback-report", action="store_true", help="report useful/not-useful close feedback")
    s.add_argument("--refresh-github", action="store_true", help="explicit maintenance: fetch complete recent issues/comments; never used on claim")
    s.add_argument("--repo", default="", help="limit explicit GitHub refresh to OWNER/REPO")
    s.add_argument("--since", default="", help="ISO time for feedback report")
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=command)

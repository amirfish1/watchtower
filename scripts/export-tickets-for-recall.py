#!/usr/bin/env python3
"""Export WatchTower's gh-list-cache.json ticket data into per-queue markdown files
so Total Recall (which only indexes prose, not raw JSON) can search ticket content.

Usage:
    python3 scripts/export-tickets-for-recall.py [--cache PATH] [--out DIR]

Defaults:
    --cache  ~/.watchtower/gh-list-cache.json
    --out    ~/.watchtower/knowledge-export/
"""
import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path


def slugify(bucket_key):
    return re.sub(r"[^a-zA-Z0-9._-]+", "-", bucket_key).strip("-")


def fmt_date(iso):
    if not iso:
        return "unknown"
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00")).strftime("%Y-%m-%d")
    except ValueError:
        return iso


def render_ticket(ticket):
    labels = ", ".join(l["name"] for l in ticket.get("labels") or [])
    assignees = ", ".join(a.get("login", "") for a in ticket.get("assignees") or []) or "none"
    comments = ticket.get("comments") or []

    lines = [
        f"## #{ticket['number']} — {ticket['title']}",
        "",
        f"- State: {ticket.get('state', 'unknown')}",
        f"- Labels: {labels or 'none'}",
        f"- Assignees: {assignees}",
        f"- Created: {fmt_date(ticket.get('createdAt'))}",
        f"- Updated: {fmt_date(ticket.get('updatedAt'))}",
        f"- Closed: {fmt_date(ticket.get('closedAt'))}",
        f"- URL: {ticket.get('url', '')}",
        "",
        ticket.get("body") or "_(no body)_",
    ]

    if comments:
        lines.append("")
        lines.append("### Comments")
        for c in comments:
            author = c.get("author", {}).get("login", "unknown") if isinstance(c.get("author"), dict) else c.get("author", "unknown")
            lines.append(f"\n**{author}** ({fmt_date(c.get('createdAt'))}):\n{c.get('body', '')}")

    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache", default=str(Path.home() / ".watchtower" / "gh-list-cache.json"))
    ap.add_argument("--out", default=str(Path.home() / ".watchtower" / "knowledge-export"))
    args = ap.parse_args()

    cache_path = Path(args.cache)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    with cache_path.open() as f:
        cache = json.load(f)

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    total = 0
    written = []

    for bucket_key, bucket in cache.items():
        tickets = bucket.get("data") or []
        if not tickets:
            continue
        repo, _, state = bucket_key.partition(":")
        out_path = out_dir / f"{slugify(bucket_key)}.md"
        header = f"# WatchTower tickets — {repo} ({state})\n\nGenerated {generated_at} from {cache_path.name}. {len(tickets)} tickets.\n"
        body = "\n\n---\n\n".join(render_ticket(t) for t in tickets)
        out_path.write_text(header + "\n" + body + "\n")
        written.append((out_path, len(tickets)))
        total += len(tickets)

    print(f"Wrote {len(written)} files, {total} tickets total, into {out_dir}")
    for path, count in written:
        print(f"  {path.name}: {count} tickets")


if __name__ == "__main__":
    main()

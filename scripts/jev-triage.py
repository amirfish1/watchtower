#!/usr/bin/env python3
"""Triage WatchTower tickets with TypeSafe Jev judgments.

For each in-progress ticket in the given queues, ask Jev whether the parked
block question genuinely needs the human owner (Amir), could be resolved by an
agent from evidence/defaults, or needs someone external (studio owner/customer).

Read-only: never mutates the store, never prints secret values.

Usage:
    python3 scripts/jev-triage.py [QUEUE ...]          # default: BECKY-*
    python3 scripts/jev-triage.py BECKY --json
"""

import argparse
import json
import subprocess
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor

JEV_SECRET_ID = "363f445c-1fd1-4785-80cf-b4c8011831a9"
API_URL = "https://api.typesafe.ai/v1/systemone"
DEFAULT_QUEUES = ["BECKY", "BECKY-DESIGN", "BECKY-TEACH", "BECKY-CONTEXT-DIET"]


def jev_api_key():
    out = subprocess.run(
        ["bws", "secret", "get", JEV_SECRET_ID],
        capture_output=True, text=True, check=True,
    )
    return json.loads(out.stdout)["value"]


def in_progress_tickets(queue):
    out = subprocess.run(
        ["wt", "ls", "-q", queue, "--status", "in_progress", "--json"],
        capture_output=True, text=True, check=True,
    )
    d = json.loads(out.stdout)
    items = d if isinstance(d, list) else d.get("items", d.get("tickets", []))
    return items


def judge_ticket(api_key, t):
    state = {
        "queue": t["project"],
        "type": t.get("type") or "unknown",
        "ticket": t.get("note") or t.get("text") or t.get("title") or "",
        "parked_question": t.get("block_question") or "",
        "waiting_since": t.get("updated_at") or "",
    }
    body = {
        "state": state,
        "model": "jev-latest",
        "questions": {
            "who_can_resolve": {
                "type": "choice",
                "instructions": (
                    "A coding agent is parked on this ticket awaiting a decision. "
                    "Decide who actually needs to act for it to move forward. "
                    "'amir' means it needs a product/business judgment, approval, or "
                    "privileged action that only the human owner of the product can provide. "
                    "'agent' means an agent could resolve it by investigating the repo/data "
                    "or applying a safe, conventional default — the parked question does not "
                    "truly require the human. 'external' means it needs information or a "
                    "decision from a studio owner or customer mentioned in the ticket, which "
                    "the human owner would have to go ask for."
                ),
                "criteria": {
                    "amir": "Product call, approval, or privileged action only the owner can make",
                    "agent": "Resolvable by an agent via investigation or safe defaults",
                    "external": "Needs input from a named studio owner/customer first",
                },
            },
            "question_over_cautious": {
                "type": "noul",
                "instructions": (
                    "Is the parked question one the agent could have answered itself with "
                    "a bit more investigation or a reasonable default, i.e. is the agent "
                    "being overly cautious by blocking on a human?"
                ),
                "criteria": {
                    "true": "The agent could likely have proceeded safely on its own",
                    "false": "Blocking on a human was genuinely warranted",
                },
            },
        },
    }
    req = urllib.request.Request(
        API_URL,
        data=json.dumps(body).encode(),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read())
    except Exception as e:
        return {"ref": t["ref"], "error": str(e)}
    a = data["answers"]
    return {
        "ref": t["ref"],
        "queue": t["project"],
        "type": t.get("type") or "",
        "who_can_resolve": a["who_can_resolve"]["choice"],
        "confidence": round(a["who_can_resolve"]["confidence"], 2),
        "probs": {k: round(v, 2) for k, v in a["who_can_resolve"]["probabilities"].items()},
        "over_cautious": round(a["question_over_cautious"]["noul"], 2),
        "title": (t.get("title") or "")[:100],
        "block_question": (t.get("block_question") or "")[:200],
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("queues", nargs="*", default=DEFAULT_QUEUES)
    ap.add_argument("--json", action="store_true", help="emit full JSON results")
    args = ap.parse_args()

    key = jev_api_key()
    tickets = []
    for q in args.queues:
        tickets.extend(in_progress_tickets(q))
    if not tickets:
        print("no in-progress tickets", file=sys.stderr)
        return

    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(lambda t: judge_ticket(key, t), tickets))

    if args.json:
        print(json.dumps(results, indent=1))
        return

    order = {"amir": 0, "agent": 1, "external": 2}
    results.sort(key=lambda r: (order.get(r.get("who_can_resolve"), 9), -r.get("confidence", 0)))
    print(f"{'REF':<22} {'RESOLVE':<8} {'CONF':<5} {'OVER-CAUTIOUS':<14} TITLE")
    for r in results:
        if "error" in r:
            print(f"{r['ref']:<22} ERROR: {r['error']}")
            continue
        print(f"{r['ref']:<22} {r['who_can_resolve']:<8} {r['confidence']:<5} "
              f"{r['over_cautious']:<14} {r['title']}")
    counts = {}
    for r in results:
        counts[r.get("who_can_resolve", "error")] = counts.get(r.get("who_can_resolve", "error"), 0) + 1
    print("\n== summary ==")
    for k in ("amir", "agent", "external", "error"):
        if k in counts:
            print(f"  {k}: {counts[k]}")


if __name__ == "__main__":
    main()

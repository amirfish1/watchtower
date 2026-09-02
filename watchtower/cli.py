#!/usr/bin/env python3
# Copyright (c) 2026 Amir Fish. All rights reserved.
# SPDX-License-Identifier: LicenseRef-WatchTower-Software-License

"""WatchTower CLI — the ``wt`` binary.

    wt status                 per-queue depth / age / drain / stuck flag
    wt ls -q Q [--status ..]  list the tickets in one queue
    wt unresolved [-q Q]      closed tickets flagged with unresolved work
    wt find <ref>             look up one ticket by ref, across all queues
    wt add -q Q --title..     file a ticket
    wt import FILE -q Q       preview document tasks; file them with --apply
    wt edit <ref> --priority..  patch fields on an existing ticket
    wt edit <ref> --queue Q    move ticket to a different queue in place
    wt claim -q Q             claim next ticket (smart: priority → type → age)
    wt claim -q Q CCC-42      claim a specific ticket by ref
    wt claim -q Q --oldest    claim oldest ticket (pure FIFO)
    wt claim -q Q --type bug  claim only bugs (or --type feature for ideas)
    wt claim -q Q --readiness needs-shaping  claim unspecced ideas
    wt close <ref>            close a ticket (summary + commit proof required)
    wt drain on|off Q         opt a queue in/out of auto-spawn
    wt workers                list workers the watcher started
    wt block / blocked        park a ticket needing a human / list parked
    wt answer / discuss       answer a blocked ticket / attach to its session
    wt send <target> "text"   push a message to a worker/agent/session
    wt ask <target> "q"       ask a target and wait for its reply
    wt outbox ls|retry|rm     inspect and manage undelivered messages
    wt critique "goal"        spawn 2 cross-family critique agents on a goal
    wt spawn "goal"           spawn one ad-hoc one-shot agent (WT Spawn)
    wt agents                 address book: registered agents + live workers
    wt agents register|rm     name a session UUID / drop a name (set-name is
                               an alias for register)
    wt chat new|post|read|ls  group chats: create/post/read/list
    wt chat nudge|add|leave   manual nudge / membership changes
    wt chat archive|close     lifecycle: archive or close a chat
    wt wait -q Q [--cmd ..]   block until the queue is drained, then run --cmd
    wt start / wt stop        start/stop service (watcher, reconciler, dashboard, HTTP API)
    wt dashboard              phone-first HTTP dashboard (queues + workers)
    wt skills [sync|status|remove]  sync the bundled skill into Claude/Codex
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from . import __version__
from . import health, queue as q, resume_verify, workers

DAEMON_PID_FILE = Path(
    os.environ.get("WATCHTOWER_DAEMON_PID")
    or (Path.home() / ".watchtower" / "daemon.pid")
)

DASHBOARD_PID_FILE = Path(
    os.environ.get("WATCHTOWER_DASHBOARD_PID")
    or (Path.home() / ".watchtower" / "dashboard.pid")
)


# --------------------------------------------------------------------------- fmt
def _oneline(s: str) -> str:
    """Collapse embedded newlines so a title/note can't break table rows or
    single-line output (multi-line ticket notes are common — see WT-51)."""
    return " ".join(s.split())


def _eta_note(r: dict) -> str:
    """Drain-rate + ETA readout for a queue row, e.g. '~3/min · empty in ~20m'.

    'stalled' when the rate is 0 and there is open work; '' for a clear queue."""
    rate = r.get("drain_rate_per_min") or 0
    if r.get("depth", 0) == 0:
        return ""
    if not rate:
        return "stalled"
    eta = r.get("eta_human") or "?"
    return f"~{rate}/min · empty in {eta}"


def _svc_state(pid_file: Path) -> str:
    """Return 'running (pid N)' or 'stopped' based on the pidfile."""
    if not pid_file.exists():
        return "stopped"
    try:
        pid = int(pid_file.read_text().strip())
        os.kill(pid, 0)
        return f"running  (pid {pid})"
    except (ValueError, ProcessLookupError, OSError):
        pid_file.unlink(missing_ok=True)
        return "stopped  (stale pidfile removed)"


def _print_status(rows: List[dict]) -> None:
    from . import config as _cfg
    daemon_state = _svc_state(DAEMON_PID_FILE)
    dash_state   = _svc_state(DASHBOARD_PID_FILE)
    print(f"service:  daemon={daemon_state}  dashboard={dash_state}")
    print(f"store:    {q.store_path()}")
    print()
    counts = workers.worker_counts()
    if not rows:
        print("(no queues)")
    else:
        hdr = (
            f"{'QUEUE':<14}{'OPEN':>5}{'WIP':>5}{'DONE':>6}  {'OLDEST':>8}"
            f"  {'IDLE':>8}  {'WORKERS':<12}{'DRAIN':<7}STATUS"
        )
        print(hdr)
        print("-" * len(hdr))
        for r in rows:
            flag = {"stuck": "STUCK", "backlog": "backlog",
                    "active": "draining", "clear": "ok"}.get(r.get("state"), "ok")
            wc = counts.get(r["queue"], {"total": 0, "live": 0})
            wcell = f"{wc['total']} ({wc['live']} live)"
            drain_val = _cfg.auto_drain(r["queue"])
            drain_cell = "on " if drain_val else "off"
            ctypes = _cfg.claim_types(r["queue"])
            note = _eta_note(r)
            if ctypes:
                label = f"{ctypes[0]}s only" if len(ctypes) == 1 else ",".join(ctypes)
                note = f"{note} [{label}]".strip()
            print(
                f"{r['queue']:<14}{r['depth']:>5}{r['in_progress']:>5}{r['closed']:>6}"
                f"  {r['oldest_open_age']:>8}  {r['since_progress']:>8}"
                f"  {wcell:<12}{drain_cell:<7}{flag}  {note}"
            )

    rows_w = workers.list_workers(prune=False)
    workers.annotate_activity(rows_w, q.list_items())
    print()
    print(f"workers ({sum(1 for w in rows_w if w.get('alive'))} live / {len(rows_w)})")
    if not rows_w:
        print("  (no workers tracked)")
        return
    for w in rows_w:
        state = "LIVE" if w.get("alive") else "DEAD"
        ref = w.get("active_ref")
        if ref:
            since = w.get("active_since_human")
            activity = f"-> {ref}" + (f" ({since})" if since else "")
        elif w.get("last_closed_ref"):
            activity = f"idle (last: {w['last_closed_ref']})"
        else:
            activity = "idle"
        print(
            f"  {w.get('worker_id',''):<22} q={w.get('queue',''):<12} "
            f"pid={w.get('pid',0):<8} {state}  {activity}"
        )


def _print_item(it: Optional[dict]) -> None:
    if not it:
        print("(none)")
        return
    print(json.dumps(it, indent=2))


def _with_timeline(item: dict) -> dict:
    out = dict(item)
    out["timeline"] = q.timeline(item)
    return out


def _event_summary(event: dict) -> str:
    name = str(event.get("event") or "")
    if name == "filed":
        return f"filed from {event.get('source') or 'unknown'}"
    if name == "claim":
        by = event.get("by") or {}
        return f"claimed by {by.get('worker') or by.get('session_id') or by.get('kind') or 'unknown'}"
    if name == "block":
        return f"blocked: {event.get('question') or ''}".rstrip()
    if name in ("answer", "comment", "progress"):
        return f"{name}: {event.get('text') or ''}".rstrip()
    if name == "reopen":
        return f"reopened: {event.get('reason') or ''}".rstrip()
    if name == "close":
        res = event.get("resolution") or {}
        summary = res.get("summary") if isinstance(res, dict) else ""
        return f"closed: {summary or ''}".rstrip()
    if name in ("ack", "unack"):
        verb = "acknowledged" if name == "ack" else "un-acknowledged"
        return f"{verb}: {event.get('text') or ''}".rstrip()
    if name == "edit":
        fields = event.get("fields") or {}
        return "edited: " + ", ".join(sorted(fields)) if fields else "edited"
    if name == "move":
        return f"moved {event.get('from_ref') or ''} -> {event.get('to_ref') or ''}".strip()
    return name or "event"


def _default_worker_id() -> str:
    """Stable default worker id for bare CLI use (WATCHTOWER-9).

    Derived from the parent process (the interactive shell), NOT this process's
    pid. Every ``wt`` invocation is a fresh pid, so a pid-based default made a
    bare ``wt claim`` then bare ``wt close`` un-composable: the close ran under
    a different id than the claim and was refused as another worker's ticket
    (the README's own claim->close example dead-ended without --force). The
    parent shell is stable across invocations typed in the same terminal, and
    differs between terminals, so claim/close/release/find compose within one
    session without leaking identity across sessions.
    """
    return f"wt-cli-{os.getppid()}"


def _caller_identity(args: argparse.Namespace) -> Tuple[str, str]:
    """``(worker_id, session_id)`` identifying the CALLER, for self-attribution.

    Ticket records read in the third person: "closed by ccc-0906ba75" looks
    like somebody else even when it was the reader ten seconds earlier, and
    the safe-side reaction to a phantom "duplicate process" is a worker
    discarding its own correct work (CCC-675/676, 2026-07-28). Worker id
    comes from ``--worker`` or a ``WT_WORKER`` env a spawner may export;
    session id from the same harness env vars the claim path records into
    ``claimed_session_id``, so hosted workers get marks with no flag at all.
    Falling back to the same stable default the write commands use means a bare
    ``wt find`` from the terminal that claimed the ticket marks it "(you)" too,
    instead of the claim/close and find disagreeing about who you are.
    """
    worker = str(getattr(args, "worker", "") or os.environ.get("WT_WORKER", "")).strip()
    if not worker:
        worker = _default_worker_id()
    session = (
        os.environ.get("CODEX_THREAD_ID", "").strip()
        or os.environ.get("CLAUDE_CODE_SESSION_ID", "").strip()
    )
    return worker, session


def _mark_self(item: dict, worker: str, session: str) -> dict:
    """Annotate a ticket dict with "this was YOU" marks for the caller.

    Adds ``claimed_by_you`` / ``closed_by_you`` at the top level and
    ``"you": true`` on matching timeline/history events. Matching is by
    worker id or session id. Close events record only a worker id, so once
    the claim is established as yours (e.g. via session id), any event by
    that same claimed worker id is also yours — worker ids are per-run,
    never shared between live processes.
    """
    if not worker and not session:
        return item

    def mine(w: Any, s: Any = None) -> bool:
        return bool(
            (worker and w and str(w) == worker)
            or (session and s and str(s) == session)
        )

    out = dict(item)
    claimed_by = str(out.get("claimed_by") or "")
    if mine(claimed_by, out.get("claimed_session_id")):
        out["claimed_by_you"] = True
    closed_by = str(out.get("closed_by") or "")
    if closed_by and (
        mine(closed_by) or (out.get("claimed_by_you") and closed_by == claimed_by)
    ):
        out["closed_by_you"] = True
    for key in ("timeline", "history"):
        events = out.get(key)
        if not isinstance(events, list):
            continue
        marked = []
        for event in events:
            if isinstance(event, dict):
                by = event.get("by") if isinstance(event.get("by"), dict) else {}
                actor_w = by.get("worker")
                if mine(actor_w, by.get("session_id")) or (
                    out.get("claimed_by_you")
                    and actor_w
                    and str(actor_w) == claimed_by
                ):
                    event = dict(event)
                    event["you"] = True
            marked.append(event)
        out[key] = marked
    return out


# ----------------------------------------------------------------------- commands
def cmd_status(args: argparse.Namespace) -> int:
    # fresh=True: a human asking for status gets current state, never a cached
    # snapshot. On a GitHub queue that is an ETag revalidation, so the usual
    # answer is a ~0.5s 304 that costs no rate limit.
    rows = health.all_status(
        project=args.queue, stuck_minutes=args.stuck_minutes, fresh=True
    )
    if args.json:
        print(json.dumps(rows, indent=2))
    else:
        gh = health.github_connectivity()
        if gh.get("alert"):
            msg = f"⚠ GitHub unreachable for {gh.get('outage_duration') or '?'}"
            if gh.get("last_error"):
                msg += f" — {gh['last_error']}"
            print(msg)
        _print_status(rows)
    return 0



def _unresolved_entries(item: dict) -> List[str]:
    """The ``resolution.unresolved`` entries of a CLOSED ticket, else [].

    Only closed tickets carry a resolution, so an open ticket is never
    "unresolved" in this sense — the word means "the worker closed this and
    flagged something it could not fix", which is exactly what the dashboard's
    UNRESOLVED badge shows and what the CLI had no way to list (WATCHTOWER-17).
    """
    if item.get("status") != "closed":
        return []
    # Legacy rows store `resolution` as a bare summary string, never a dict.
    res = item.get("resolution")
    if not isinstance(res, dict):
        return []
    return [str(x) for x in (res.get("unresolved") or []) if str(x).strip()]


def _unresolved_items(items: List[dict]) -> List[dict]:
    return [it for it in items if _unresolved_entries(it)]


def _ack_counts(item: dict) -> Tuple[int, int]:
    """``(total, acked)`` unresolved entries for one closed ticket.

    An acked entry still counts -- the record is never rewritten -- but says
    so, so a triage pass can tell deliberate closes from live work.
    """
    entries = _unresolved_entries(item)
    res = item.get("resolution") if isinstance(item.get("resolution"), dict) else {}
    acked = sum(1 for i in range(len(entries)) if q.is_acked(res, "unresolved", i))
    return len(entries), acked


def cmd_ls(args: argparse.Namespace) -> int:
    """List the tickets in a single queue (the actual items, not just counts)."""
    # fresh=True for the same reason as `wt status`: a CLI read always
    # revalidates, so `wt ls` is never behind the repo.
    items = q.list_items(project=args.queue, fresh=True)
    # Counted over the WHOLE queue, before the status filter, so the default
    # (active) view still surfaces that closed-but-unresolved work exists.
    unresolved_total = len(_unresolved_items(items))
    want = args.status
    if getattr(args, "unresolved", False):
        want = "unresolved"
    if want == "active":
        items = [i for i in items if i.get("status") in ("open", "in_progress")]
    elif want == "blocked":
        items = [i for i in items if i.get("needs_input")]
    elif want == "unresolved":
        items = _unresolved_items(items)
    elif want != "all":
        items = [i for i in items if i.get("status") == want]
    if args.json:
        print(json.dumps(items, indent=2))
        return 0
    if not items:
        print(f"(no {('' if want=='all' else want+' ')}items in {args.queue})")
        return 0
    limit = args.limit or len(items)
    if unresolved_total and want != "unresolved":
        print(f"{args.queue}: {unresolved_total} closed "
              f"{'ticket' if unresolved_total == 1 else 'tickets'} with unresolved "
              f"items (wt unresolved -q {args.queue})")
    print(f"{'REF':<14}{'STATUS':<12}{'WORKER':<22}TITLE")
    print("-" * 72)
    for it in items[:limit]:
        worker = str(it.get("claimed_by") or it.get("claimed_session_id") or "")[:20]
        title = _oneline(it.get("title") or it.get("note") or "")[:56]
        line = f"{str(it.get('ref','')):<14}{str(it.get('status','')):<12}{worker:<22}{title}"
        res = it.get("resolution") if it.get("status") == "closed" else None
        if res and res.get("summary"):
            line += f"  — {res['summary']}"
            extras = []
            for key, label in (("caveats", "caveat"), ("follow_ups", "follow-up"),
                               ("unresolved", "unresolved")):
                n = len(res.get(key) or [])
                if n:
                    # An acknowledged item still counts -- the record is never
                    # rewritten -- but says so, so a line of stale warnings
                    # reads as handled rather than outstanding (`wt unresolved-ack`).
                    acked = sum(1 for i in range(n) if q.is_acked(res, key, i))
                    suffix = f", {acked} acked" if acked else ""
                    extras.append(f"{n} {label}{'s' if n != 1 else ''}{suffix}")
            if extras:
                line += f" [{', '.join(extras)}]"
        print(line)
    if len(items) > limit:
        print(f"... and {len(items) - limit} more (raise --limit)")
    return 0


def cmd_unresolved(args: argparse.Namespace) -> int:
    """Summarise closed tickets that were flagged with unresolved items.

    The dashboard has always shown an UNRESOLVED badge on these, but the CLI
    had no query for them, so owners either eyeballed the web UI or piped
    `wt ls --status closed --json` through their own filter (WATCHTOWER-17).
    With no -q this scans every queue, because the question ("is there
    anything I closed but did not actually fix?") is rarely per-queue.
    """
    items = q.list_items(project=args.queue or None, fresh=True)
    rows = _unresolved_items(items)
    rows.sort(key=lambda i: (str(i.get("project") or ""), i.get("seq") or 0))
    if args.json:
        print(json.dumps(rows, indent=2))
        return 0
    if not rows:
        where = f" in {args.queue}" if args.queue else ""
        print(f"(no closed tickets with unresolved items{where})")
        return 0
    total = sum(_ack_counts(it)[0] for it in rows)
    acked = sum(_ack_counts(it)[1] for it in rows)
    scope = args.queue or "all queues"
    tail = f", {acked} acked" if acked else ""
    print(f"{scope}: {len(rows)} closed "
          f"{'ticket' if len(rows) == 1 else 'tickets'} with {total} unresolved "
          f"{'item' if total == 1 else 'items'}{tail}")
    print()
    limit = args.limit or len(rows)
    for it in rows[:limit]:
        n, n_acked = _ack_counts(it)
        title = _oneline(it.get("title") or it.get("note") or "")[:56]
        mark = f" [{n} unresolved{f', {n_acked} acked' if n_acked else ''}]"
        print(f"{str(it.get('ref','')):<14}{title}{mark}")
        res = it.get("resolution") if isinstance(it.get("resolution"), dict) else {}
        summary = _oneline(str(res.get("summary") or ""))
        if summary:
            print(f"  resolution: {summary[:100]}")
        for i, entry in enumerate(_unresolved_entries(it)):
            flag = " (acked)" if q.is_acked(res, "unresolved", i) else ""
            print(f"  - {_oneline(entry)[:100]}{flag}")
    if len(rows) > limit:
        print(f"... and {len(rows) - limit} more (raise --limit)")
    return 0


def cmd_find(args: argparse.Namespace) -> int:
    """Look up one ticket by ref or number, searching every queue -- the CLI
    surface for queue.get(), which already matches globally. No -q needed,
    so an agent (or a skill) that only has a bare ref like 'HERMES-20' can
    resolve it without knowing which queue it lives in."""
    item = q.get(args.ref)
    if not item:
        print(f"not found: {args.ref}", file=sys.stderr)
        return 1
    caller_worker, caller_session = _caller_identity(args)
    item_with_timeline = _mark_self(
        _with_timeline(item), caller_worker, caller_session
    )
    if args.json:
        print(json.dumps(item_with_timeline, indent=2))
        return 0
    worker = str(item.get("claimed_by") or item.get("claimed_session_id") or "")
    title = _oneline(item.get("title") or item.get("note") or "")
    print(f"{item.get('ref',''):<14}[{item.get('status',''):<11}] {title}")
    if worker:
        you = " (you)" if item_with_timeline.get("claimed_by_you") else ""
        print(f"  claimed_by: {worker}{you}")
    closed_by = str(item.get("closed_by") or "")
    if closed_by:
        you = " (you)" if item_with_timeline.get("closed_by_you") else ""
        print(f"  closed_by: {closed_by}{you}")
    res = item.get("resolution") if item.get("status") == "closed" else None
    if res and res.get("summary"):
        print(f"  resolution: {res['summary']}")
    timeline = item_with_timeline.get("timeline") or []
    if timeline:
        print("  activity:")
        for event in timeline:
            you = "  (you)" if event.get("you") else ""
            print(f"    {event.get('at') or '-'}  {_event_summary(event)}{you}")
    return 0


def cmd_edit(args: argparse.Namespace) -> int:
    """Patch fields (title/priority/type/readiness/...) on an existing
    ticket, in place -- no refile/close churn (WT-71). Only flags the
    caller actually passed are touched; everything else is left as-is.
    --queue moves the ticket to a different queue in place (WT-83), also
    without refile/close churn -- its ref is reassigned within the target
    queue since refs are derived from project+number."""
    fields = {}
    for name in (
        "title", "note", "text", "url", "type", "readiness", "priority",
        "value", "confidence", "model_floor", "selector", "screenshot_path",
        "repo_path",
    ):
        value = getattr(args, name, None)
        if value is not None:
            fields[name] = value
    new_queue = getattr(args, "queue", None)
    if not fields and new_queue is None:
        print(
            "error: no fields to edit -- pass at least one of "
            "--title/--note/--text/--url/--type/--readiness/--priority/"
            "--value/--confidence/--model-floor/--selector/"
            "--screenshot-path/--repo-path/--queue",
            file=sys.stderr,
        )
        return 1
    ident = args.ref
    old_ref = None
    if new_queue is not None:
        try:
            item = q.move(ident, new_queue)
        except ValueError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        if not item:
            print(f"(no item {ident})", file=sys.stderr)
            return 1
        old_ref, ident = ident, item["ref"]
    if fields:
        try:
            item = q.update(ident, **fields)
        except ValueError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        if not item:
            print(f"(no item {ident})", file=sys.stderr)
            return 1
    if args.json:
        _print_item(item)
    else:
        moved = f" (moved {old_ref} -> {item['ref']})" if old_ref else ""
        print(f"EDITED: {item['ref']}{moved}  {item.get('title') or item.get('note','')}")
    return 0


def cmd_add(args: argparse.Namespace) -> int:
    submitter = (getattr(args, "submitter", "") or "").strip()
    if not submitter:
        # Same auto-detection --report-to already relies on: a Claude session
        # UUID is directly addressable; a Codex thread id gets registered as
        # an addressable @name (see _default_report_to's docstring). Silent by
        # design here -- a submitter is best-effort metadata, not something
        # worth a print for every `wt add`.
        submitter, _rnote = _default_report_to()
    elif submitter:
        from . import messages
        try:
            messages.resolve_target(submitter)
        except ValueError as e:
            print(
                f"warning: --submitter {submitter!r} does not resolve yet "
                f"({e}) -- it will still be recorded on the ticket, but "
                "notifications sent before it's known/registered will fail",
                file=sys.stderr,
            )
    item = q.enqueue(
        project=args.queue,
        title=args.title or "",
        note=args.note or (args.title or ""),
        text=args.text or "",
        url=args.url or "",
        lane=args.lane,
        source="wt",
        item_type=getattr(args, "type", "") or "",
        readiness=getattr(args, "readiness", "") or "",
        priority=getattr(args, "priority", "") or "",
        value=getattr(args, "value", "") or "",
        confidence=getattr(args, "confidence", "") or "",
        model_floor=getattr(args, "model_floor", "") or "",
        submitter=submitter,
    )
    print(f"FILED: {item['ref']}  {item.get('title') or item.get('note','')}")
    # Enqueue-and-claim: file the ticket, then immediately mark it in_progress so
    # the reconciler (which only spawns for OPEN tickets) leaves it alone. For the
    # user who's already working the bug they're documenting. Skip the dispatch
    # entirely -- an already-claimed ticket is in_progress, not open, so nudging
    # or spawning a worker would be a no-op at best.
    if getattr(args, "claim", False):
        worker = args.worker or _default_worker_id()
        try:
            q.claim_by_ref(item["ref"], worker)
            print(f"CLAIMED: {item['ref']} -> {worker}")
        except Exception as e:
            # Enqueue already succeeded; a claim hiccup shouldn't fail the file.
            print(f"[watchtower] could not claim {item['ref']}: {e}", file=sys.stderr)
        return 0
    # Decide + act on the new ticket NOW (nudge current staffing via FIFO, else
    # gracefully release verified-idle staffing and spawn) and log the
    # decision. Centralized in
    # workers.dispatch_after_enqueue so the CLI and the CCC dashboard share one
    # disposition path. Best-effort -- a hiccup here never fails the enqueue.
    try:
        from . import workers
        reason = workers.dispatch_after_enqueue(args.queue, item.get("ref", ""))
        if reason:
            print(f"[watchtower] {reason}")
    except Exception:
        pass
    return 0


def cmd_import(args: argparse.Namespace) -> int:
    """Reason about a document once, then preview or apply its ticket graph."""
    from .document_import import ReasoningError, extract_document, plan_import

    try:
        candidates = extract_document(args.file)
    except (OSError, UnicodeError, ValueError, ReasoningError) as exc:
        print(f"error: cannot import {args.file}: {exc}", file=sys.stderr)
        return 1

    existing_items = q.list_items(project=args.queue)
    plan = plan_import(candidates, existing_items)
    new_ids = {candidate.import_key for candidate in plan.new}
    for candidate in plan.candidates:
        item_type = args.type or candidate.item_type
        if candidate.import_key in new_ids:
            verb = "FILE" if args.apply else "WOULD FILE"
            print(f"{verb}: [{item_type}] {candidate.title}")
        else:
            print(f"EXISTS: [{item_type}] {candidate.title}")
        print(f"  source: {candidate.source_ref}")
        dependencies = ", ".join(candidate.depends_on) or "none"
        print(f"  depends_on: {dependencies}")
        for line in candidate.body.splitlines():
            print(f"  {line}")

    if not args.apply:
        print(
            "IMPORT dry-run: "
            f"candidates={len(plan.candidates)} new={len(plan.new)} "
            f"existing={len(plan.existing)}; pass --apply to file"
        )
        return 0

    existing_by_id = {
        str(item.get("id") or ""): item
        for item in existing_items
        if item.get("id")
    }
    refs_by_title = {}
    for candidate in plan.existing:
        stored = existing_by_id.get(candidate.import_key)
        if stored and stored.get("ref"):
            refs_by_title[candidate.title] = str(stored["ref"])

    created = 0
    for candidate in plan.new:
        dependency_lines = []
        for title in candidate.depends_on:
            ref = refs_by_title.get(title)
            if not ref:
                print(
                    f"error: validated dependency {title!r} has no queue ref; "
                    "no further tickets were filed",
                    file=sys.stderr,
                )
                return 1
            dependency_lines.append(f"- {ref}: {title}")
        ticket_body = candidate.body
        if dependency_lines:
            ticket_body += "\n\nDepends on:\n" + "\n".join(dependency_lines)
        try:
            item = q.enqueue(
                project=args.queue,
                title=candidate.title,
                note=candidate.title,
                text=ticket_body,
                source="doc-import",
                annotation_id=candidate.import_key,
                url=candidate.source_ref,
                repo_path=str(Path(candidate.source_path).parent),
                item_type=args.type or candidate.item_type,
            )
        except Exception as exc:
            print(
                f"error: import stopped after creating {created} ticket(s): {exc}",
                file=sys.stderr,
            )
            return 1
        print(f"FILED: {item['ref']}  {item.get('title') or item.get('note', '')}")
        refs_by_title[candidate.title] = str(item["ref"])
        created += 1
    print(
        "IMPORT applied: "
        f"candidates={len(plan.candidates)} created={created} "
        f"existing={len(plan.existing)}"
    )
    return 0


def cmd_take(args: argparse.Namespace) -> int:
    """Shorthand for `add --claim`: file a ticket and immediately claim it, for
    documenting a bug you're already working on. Delegates to cmd_add so the two
    share one code path and can't drift."""
    args.claim = True
    return cmd_add(args)


def cmd_claim(args: argparse.Namespace) -> int:
    worker = args.worker or _default_worker_id()
    ref = getattr(args, "ref", None) or None
    session_uuid = (
        os.environ.get("CODEX_THREAD_ID", "").strip()
        or os.environ.get("CLAUDE_CODE_SESSION_ID", "").strip()
    )
    codex_thread_id = os.environ.get("CODEX_THREAD_ID", "").strip()
    claude_session_id = os.environ.get("CLAUDE_CODE_SESSION_ID", "").strip()
    continuation = (
        ("codex", codex_thread_id)
        if codex_thread_id
        else ("claude", claude_session_id)
    )
    if continuation[1]:
        continuation_pid = workers._find_engine_ancestor_pid(continuation[0])
        if continuation_pid:
            try:
                workers.rebind_continued_worker(
                    worker, continuation[1], continuation_pid,
                    engine=continuation[0],
                )
            except ValueError as exc:
                print(f"error: {exc}", file=sys.stderr)
                return 1

    if ref:
        try:
            item = q.claim_by_ref(ref, worker, session_uuid=session_uuid)
        except ValueError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        if not item:
            print(f"error: {ref} not found", file=sys.stderr)
            return 1
    else:
        try:
            item = q.claim_next(
                worker,
                project=args.queue,
                session_uuid=session_uuid,
                oldest=getattr(args, "oldest", False),
                item_types=getattr(args, "type", None) or [],
                readiness_filters=getattr(args, "readiness", None) or [],
            )
        except ValueError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        if not item:
            # Nothing claimable. Decide surplus HERE, at claim time, when the real
            # current state is known — not on the reconciler's future-guessing
            # count. A worker is surplus only if more workers are live than the
            # queue wants; then it exits itself. Otherwise it stays available
            # (its next `wt add` nudge wakes it), with queue-scoped release as the
            # persistently-idle safety net.
            from . import config
            desired = config.desired_workers(args.queue) if config.auto_drain(args.queue) else 0
            live = workers.live_worker_count(args.queue)
            if live > desired:
                from watchtower.queue import _log
                _log("STOP", f"{worker} — surplus at claim ({live}>{desired} desired)",
                     queue=args.queue)
                if args.json:
                    print(json.dumps({"stop": True}))
                else:
                    print("STOP: surplus worker (live>desired); exiting")
                return 0
            if not args.json:
                print(f"(nothing open in {args.queue})")
            return 0
        # Stop signal: reconciler asked this worker to wind down.
        if item.get("stop"):
            if args.json:
                print(json.dumps({"stop": True}))
            else:
                print("STOP: reconciler requested shutdown; exiting")
            return 0

    # FEAT-NEXT-120 — per-ticket model floor. Checked here, after a genuine
    # claim (not the nothing-open/stop early-returns above), so an
    # under-tiered queue never silently works a ticket that named a higher
    # floor. Auto-parks it via the existing block() path -- "worker parks
    # the ticket blocked with a note" per the design -- rather than
    # bouncing it back to open, so a human sees exactly why and can either
    # reconfigure the queue's model or hand it to a stronger one.
    model_floor = str(item.get("model_floor") or "").strip()
    if model_floor:
        from . import config
        if not config.model_floor_met(args.queue, model_floor):
            queue_model = config.canonical_model(config.engine(args.queue), config.model(args.queue))
            q.block(
                item["ref"],
                session_uuid,
                question=(
                    # Built from the shared prefix so the reconciler's SIDE-39
                    # timebox watchdog can recognize a floor-park (and only a
                    # floor-park) from the question text alone.
                    f"{config.MODEL_FLOOR_BLOCK_PREFIX} {model_floor!r}, but queue "
                    f"{args.queue!r} is configured for {queue_model or '(unset)'!r}. "
                    "Reassign to a queue running at least that model, or bump this "
                    "queue's --model, then answer to resume."
                ),
                progress=f"Auto-parked at claim time by {worker} — floor not met.",
            )
            print(
                f"error: {item['ref']} requires model floor {model_floor!r}; "
                f"queue {args.queue!r} runs {queue_model or '(unset)'!r} — "
                "parked blocked instead of claimed",
                file=sys.stderr,
            )
            return 1

    _rename_claiming_session(item)

    if args.json:
        # The claim just made is by definition the caller's; marking history
        # events under the same identity keeps a re-claimed ticket's earlier
        # activity from reading as another worker's (CCC-675).
        _print_item(_mark_self(item, worker, session_uuid))
    else:
        print(f"CLAIMED: {item['ref']} -> {worker}")
        print(item.get("text") or item.get("note") or "")
    return 0


def _rename_claiming_session(item: dict, summary: str = "") -> None:
    """Best-effort: rename the claiming session's engine transcript to
    reflect the ticket it now holds, or -- once ``summary`` is given --
    what it closed with (WT-49). No-ops silently when there's no real
    session id or no transcript on disk yet; never blocks a claim/close
    over cosmetics. See ``docs/session-naming.md``."""
    sid = item.get("claimed_session_id")
    if not sid:
        return
    try:
        from . import messages
        name = workers.display_name(
            item.get("project", ""),
            item.get("ref"),
            workers.ticket_context(item, summary),
        )
        messages.set_session_title(str(sid), name)
    except Exception:
        pass


def cmd_run(args: argparse.Namespace) -> int:
    """Mark an existing ticket runnable and dispatch its queue.

    Alias: ``wt ready`` (more descriptive name — 'run' implies 'execute now'
    but the command actually marks a ticket drainable by workers).

    ``--cancel`` withdraws a run request that has not started yet. The dashboards
    can already do this (a second press of the play button), so without a flag
    here the CLI is the only surface that can queue a run but not un-queue it."""
    if getattr(args, "cancel", False):
        try:
            item = q.clear_run_request(args.ref)
        except ValueError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        if not item:
            print(f"error: {args.ref} not found", file=sys.stderr)
            return 1
        print(f"CANCELLED: {item['ref']}  {item.get('title') or item.get('note','')}")
        return 0
    try:
        item = q.mark_runnable(args.ref)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    if not item:
        print(f"error: {args.ref} not found", file=sys.stderr)
        return 1
    print(f"RUNNABLE: {item['ref']}  {item.get('title') or item.get('note','')}")
    if not getattr(args, "no_dispatch", False):
        try:
            reason = workers.dispatch_after_enqueue(item.get("project", ""), item.get("ref", ""))
            if reason:
                print(f"[watchtower] {reason}")
        except Exception:
            pass
    return 0


cmd_ready = cmd_run  # preferred alias: 'wt ready' reads as "mark this ticket ready for workers"


def _resolution_from_args(args: argparse.Namespace) -> Optional[dict]:
    """Build a resolution dict from the close-resolution flags.

    Returns None when no flag was given (so close stays back-compatible)."""
    res = {
        "summary": args.summary or "",
        "caveats": list(args.caveat or []),
        "follow_ups": list(args.follow_up or []),
        "unresolved": list(args.unresolved or []),
    }
    if getattr(args, "commit", ""):
        res["commit"] = args.commit
    if getattr(args, "no_code", False):
        res["no_code"] = True
    if not any(res.values()):
        return None
    return res


_COMMIT_SHA_RE = re.compile(r"[0-9a-fA-F]{7,64}")


def _verify_close_commit(ref: str, sha: str) -> Tuple[str, str]:
    """Return a canonical commit SHA or an actionable validation error.

    A ticket may point at its repository directly, while an older ticket uses
    its queue's configured repository.  Falling back to the current directory
    keeps ad-hoc local queues usable.  The commit must resolve in that repo;
    accepting a merely SHA-shaped string would let dirty work masquerade as a
    committed resolution.
    """
    candidate = str(sha or "").strip()
    if not _COMMIT_SHA_RE.fullmatch(candidate):
        return "", "error: --commit must be a 7- to 64-character hexadecimal commit SHA"
    item = q.get(ref)
    if not item:
        return "", f"error: {ref} not found"
    from . import close_proof, config
    repo = str(item.get("repo_path") or config.repo_path(item.get("project", "")) or os.getcwd())
    # A queue has one repo_path, but its tickets need not. Look in the expected
    # repo first, then every other configured one (including `host:/path`
    # remotes, verified over ssh). The commit must still genuinely resolve --
    # see close_proof for why widening the search beats loosening the check.
    verified, _found_in, errors = close_proof.verify_with_errors(candidate, repo)
    if not verified:
        if errors:
            detail = "; ".join(errors[:3])
            return "", (
                f"error: git refused to verify {candidate}: {detail}. "
                "Fix the git access issue, then retry, or use "
                "`wt block <ref> --progress \"...\"` instead of closing"
            )
        return "", (
            f"error: {candidate} is not a commit in {repo} or any other configured "
            "repo; commit the verified work or use "
            "`wt block <ref> --progress \"...\"` instead of closing"
        )
    return verified, ""


def cmd_close(args: argparse.Namespace) -> int:
    if not (args.summary or "").strip():
        print(
            "error: --summary is required when closing a ticket\n"
            "  example: wt close <ref> --summary \"what you changed\"",
            file=sys.stderr,
        )
        return 1
    if bool(args.commit) == bool(args.no_code):
        print(
            "error: closing requires exactly one completion proof: --commit <SHA> "
            "for code changes or --no-code for work that changed no code",
            file=sys.stderr,
        )
        return 1
    if args.commit:
        verified, error = _verify_close_commit(args.ref, args.commit)
        if error:
            print(error, file=sys.stderr)
            return 1
        args.commit = verified
    worker = args.worker or _default_worker_id()
    resolution = _resolution_from_args(args)
    try:
        item = q.close(args.ref, worker, resolution=resolution,
                       force=getattr(args, "force", False))
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    if not item:
        print(f"(no item {args.ref})", file=sys.stderr)
        return 1
    res = item.get("resolution") or {}
    summary = res.get("summary", "")
    print(f"CLOSED: {item['ref']}" + (f" — {summary}" if summary else ""))

    _rename_claiming_session(item, summary)

    # STRETCH (opt-in): file each follow-up / unresolved item as a new open
    # ticket in the same queue so nothing falls through the cracks.
    if getattr(args, "enqueue_follow_ups", False):
        carry = (res.get("follow_ups") or []) + (res.get("unresolved") or [])
        for note in carry:
            new = q.enqueue(
                project=item.get("project", ""),
                note=note,
                source="wt-followup",
            )
            print(f"  FILED follow-up: {new['ref']}  {note}")
    return 0


_ACK_FIELDS = (
    ("caveats", "caveat", "--caveat"),
    ("follow_ups", "follow_up", "--follow-up"),
    ("unresolved", "unresolved", "--unresolved"),
)


def _print_resolution_items(item: dict) -> None:
    """Numbered listing of a closed ticket's caveat/follow-up/unresolved
    entries, with their ack state -- what `wt unresolved-ack <ref>` prints when no
    selector was given, so the caller can see which index to ack."""
    res = item.get("resolution") or {}
    print(f"{item.get('ref', '')}  {res.get('summary') or item.get('title') or ''}")
    for field, _dest, flag in _ACK_FIELDS:
        vals = res.get(field) or []
        for i, val in enumerate(vals):
            mark = "[acked]" if q.is_acked(res, field, i) else "[     ]"
            print(f"  {mark} {flag} {i + 1}  {_oneline(str(val))[:100]}")
    print("\n  ack one with: wt unresolved-ack <ref> --unresolved N   (or --all)")


def _resolution_haystack(item: dict) -> str:
    """All of a closed ticket's resolution prose, lowercased, for --matching.

    Includes the summary as well as the individual entries: verdicts like
    "closed as not-applicable" are usually written in the summary, not
    repeated in each unresolved bullet.
    """
    res = item.get("resolution")
    if not isinstance(res, dict):
        return ""
    parts = [str(res.get("summary") or "")]
    for field in q.RESOLUTION_LIST_FIELDS:
        parts.extend(str(x) for x in (res.get(field) or []))
    return "\n".join(parts).lower()


def _has_resolution_items(item: dict) -> bool:
    res = item.get("resolution")
    if not isinstance(res, dict):
        return False
    return any(res.get(f) for f in q.RESOLUTION_LIST_FIELDS)


def _cmd_unresolved_ack_bulk(args: argparse.Namespace) -> int:
    """`wt unresolved-ack -q QUEUE --all [--matching TEXT]` — ack a whole queue at once.

    Per-ref ack is already scriptable, but the case that motivated this
    (WATCHTOWER-18) is a backlog of closed tickets whose unresolved entries
    are terminal verdicts -- "not-applicable", "guide-only by policy",
    "false positive" -- each of which keeps its row amber forever. Acking
    them one ref at a time is the chore, not the decision.

    Bulk mode is deliberately --all only: a 1-based index into one ticket's
    caveat list means nothing applied across a hundred different tickets.
    """
    queue = getattr(args, "_ignored_queue", None)
    if not queue:
        print("error: bulk ack needs -q QUEUE (or pass a ticket ref)",
              file=sys.stderr)
        return 1
    if not args.all:
        print("error: bulk ack needs --all; per-index flags (--caveat/"
              "--follow-up/--unresolved) only make sense for a single ticket",
              file=sys.stderr)
        return 1
    items = [
        it for it in q.list_items(project=queue, fresh=True)
        if it.get("status") == "closed" and _has_resolution_items(it)
    ]
    needle = args.matching.strip().lower()
    if needle:
        items = [it for it in items if needle in _resolution_haystack(it)]
    if not items:
        why = f" matching {args.matching!r}" if needle else ""
        print(f"(no closed tickets in {queue} with resolution items{why})")
        return 0
    verb = "UNACK" if args.undo else "ACK"
    if args.dry_run:
        print(f"would {verb.lower()} {len(items)} "
              f"{'ticket' if len(items) == 1 else 'tickets'} in {queue}:")
        for it in items:
            print(f"  {str(it.get('ref','')):<14}"
                  f"{_oneline(it.get('title') or it.get('note') or '')[:56]}")
        return 0
    by = args.by or _default_worker_id()
    updated, failed = [], []
    for it in items:
        try:
            res = q.ack_resolution(
                it.get("ref"), all_items=True, by=by, undo=bool(args.undo)
            )
        except ValueError as exc:
            # e.g. a GitHub-backed queue that has no ack representation --
            # report it rather than aborting a half-applied bulk run.
            failed.append((it.get("ref"), str(exc)))
            continue
        if res:
            updated.append(res)
    if args.json:
        print(json.dumps(updated, indent=2))
        return 1 if failed and not updated else 0
    print(f"{verb}ED: {len(updated)} "
          f"{'ticket' if len(updated) == 1 else 'tickets'} in {queue}")
    for it in updated:
        print(f"  {str(it.get('ref','')):<14}"
          f"{_oneline(it.get('title') or it.get('note') or '')[:56]}")
    for ref, err in failed:
        print(f"  {str(ref):<14}SKIPPED — {err}", file=sys.stderr)
    return 1 if failed and not updated else 0


def cmd_unresolved_ack(args: argparse.Namespace) -> int:
    """Acknowledge resolution warnings without rewriting the close record.

    The dashboard renders a closed ticket's caveats / follow-ups / unresolved
    entries as chips; before this the only way to clear a stale one was
    `wt close --force` with a rebuilt resolution, which rewrites history and
    re-fires close notifications. `wt unresolved-ack` marks the entry seen instead: the
    text is preserved verbatim and the chip renders dimmed.

    With no ref this dispatches to the bulk form over a whole queue."""
    if not args.ref:
        return _cmd_unresolved_ack_bulk(args)
    if args.matching:
        print("error: --matching is bulk mode; drop the ref (and pass -q QUEUE)",
              file=sys.stderr)
        return 1
    targets = []
    for field, dest, _flag in _ACK_FIELDS:
        for n in (getattr(args, dest, None) or []):
            if n < 1:
                print(f"error: indexes are 1-based; got {n}", file=sys.stderr)
                return 1
            targets.append((field, n - 1))
    if not targets and not args.all:
        item = q.get(args.ref)
        if not item:
            print(f"not found: {args.ref}", file=sys.stderr)
            return 1
        _print_resolution_items(item)
        return 0
    try:
        item = q.ack_resolution(
            args.ref,
            targets=targets,
            all_items=bool(args.all),
            by=args.by or _default_worker_id(),
            undo=bool(args.undo),
        )
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    if not item:
        print(f"(no item {args.ref})", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(item, indent=2))
        return 0
    print(f"{'UNACKED' if args.undo else 'ACKED'}: {item['ref']}")
    _print_resolution_items(item)
    return 0


def cmd_release(args: argparse.Namespace) -> int:
    """Give up a claim without closing it, returning the ticket to the open
    pool (WT-86) -- e.g. it was claimed defensively to stop other workers
    grabbing it mid-investigation, and turns out better left for the normal
    pool to pick up."""
    worker = args.worker or _default_worker_id()
    try:
        item = q.release(args.ref, session_id=worker, force=args.force)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    if not item:
        existing = q.get(args.ref)
        if existing is None:
            print(f"(no item {args.ref})", file=sys.stderr)
        else:
            print(
                f"error: {args.ref} is {existing.get('status')}, not in_progress "
                "-- nothing to release",
                file=sys.stderr,
            )
        return 1
    print(f"RELEASED: {item['ref']} -> open")
    return 0


def cmd_block(args: argparse.Namespace) -> int:
    """A worker parks a ticket that needs a human decision (WT-28). Stays
    in_progress, bound to its session; flagged needs_input with a question."""
    item = q.block(
        args.ref, session_id=args.worker,
        question=args.question, progress=args.progress,
    )
    if not item:
        print(f"(no item {args.ref})", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(item, indent=2))
        return 0
    print(f"BLOCKED: {item['ref']} — {item.get('block_question') or '(no question)'}")
    sid = item.get("claimed_session_id")
    if sid:
        print(f"  session {sid} — resume with: wt discuss {item['ref']}")
    else:
        print("  (no resumable session id recorded; a human can still read progress notes)")
    return 0


def cmd_blocked(args: argparse.Namespace) -> int:
    """List tickets parked for a human (WT-28)."""
    rows = q.list_blocked(project=args.queue)
    if args.json:
        print(json.dumps(rows, indent=2))
        return 0
    if not rows:
        print("(nothing blocked)")
        return 0
    for it in rows:
        print(f"{it['ref']:<12} {it.get('block_question') or '(no question)'}")
        print(f"             session={it.get('claimed_session_id') or '-'}  "
              f"repo={it.get('repo_path') or '-'}")
    return 0


def _resume_session_headless(
    sid: str,
    repo: str,
    prompt: str,
    engine: str,
    *,
    queue: str = "",
    worker_id: str = "",
) -> bool:
    """Wake a blocked worker's session non-interactively and hand it the answer.

    Spawns `claude --resume <sid> -p <prompt>` (or the headless Codex
    equivalent) detached, in the ticket's repo, logging to
    ~/.watchtower/logs. The resumed process is registered under the original
    worker id so the orphan sweep cannot reopen its ticket and spawn a second
    owner while the answer is being applied. Returns True if the resume process
    started."""
    if engine == "codex":
        argv = [
            "codex", "exec", "resume",
            "--dangerously-bypass-approvals-and-sandbox",
            sid, prompt,
        ]
    elif engine == "kimi":
        # Print mode resumes the session, applies the prompt, exits; kimi
        # auto-approves internally in this mode (no permission flag exists).
        argv = ["kimi", "--session", sid, "-p", prompt,
                "--output-format", "stream-json"]
    else:
        argv = ["claude", "--resume", sid, "-p", prompt,
                "--permission-mode", "bypassPermissions"]
    log_dir = Path.home() / ".watchtower" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"resume-{sid}.log"
    try:
        logf = open(log_path, "ab")
        try:
            proc = subprocess.Popen(
                argv, stdin=subprocess.DEVNULL, stdout=logf,
                stderr=subprocess.STDOUT, start_new_session=True,
                cwd=repo or os.getcwd(),
            )
        finally:
            logf.close()
        died, _ = resume_verify.verify_resume_child(proc)
        if died:
            return False
        if queue and worker_id:
            workers.record_worker(
                proc.pid,
                queue,
                engine,
                worker_id,
                repo_path=repo,
                log=str(log_path),
                session_id=sid,
                kind="resume",
            )
        return True
    except (OSError, FileNotFoundError):
        return False


def _answer_engine(item: Dict[str, object], requested: Optional[str]) -> str:
    """Resolve the blocked session's engine unless the caller overrode it."""
    if requested:
        return requested
    sid = str(item.get("claimed_session_id") or "")
    if sid.startswith("session_"):
        return "kimi"
    worker_id = str(item.get("claimed_by") or "")
    try:
        known = workers.list_workers(prune=False)
        for field, value in (("session_id", sid), ("worker_id", worker_id)):
            if not value:
                continue
            for worker in reversed(known):
                if str(worker.get(field) or "") == value:
                    engine = str(worker.get("engine") or "")
                    if engine in ("claude", "codex", "kimi"):
                        return engine
    except (OSError, ValueError):
        pass
    if sid:
        try:
            from . import codex_registry
            if (codex_registry.entry(sid) or {}).get("engine") == "codex":
                return "codex"
        except (OSError, ValueError):
            pass
    return "claude"


def cmd_answer(args: argparse.Namespace) -> int:
    """Inject a human answer onto a blocked ticket and hand it to the session.

    Clears needs_input, then delivers the answer through the one liveness-aware
    delivery primitive (``messages.deliver_message`` with ``verb="steer"``,
    which prefers the peer socket and so cannot truncate a live turn): steer the
    worker if its turn is live, resume it if idle, hold-and-retry via the
    durable outbox if it is busy or momentarily unreachable. This replaces a
    blind ``--resume`` fork, which for a Codex worker becomes a second
    app-server owner on a session CCC may already be driving (WT-28, WT-90).
    Only CCC knows a Codex session's liveness, so ``messages.send`` delegates
    there; a genuinely unresolvable target still falls back to the headless
    resume fork so the answer is never silently dropped."""
    item = q.answer(args.ref, args.text, session_id=args.worker)
    if not item:
        print(f"(no item {args.ref})", file=sys.stderr)
        return 1
    sid = item.get("claimed_session_id")
    if not sid:
        print(f"ANSWERED: {item['ref']} — needs_input cleared. "
              f"(no resumable session recorded; a worker will pick it up on "
              f"next claim)")
        return 0
    # Context-budget escalation for the answer-resume path. Resuming the
    # original session is normally right (its investigation context makes the
    # answer land), so the threshold here is intentionally HIGHER than the
    # claim-time recycle budget (default 2x). Past it, hours of post-answer
    # work in an already-huge conversation costs more than the lost context:
    # embed the Q&A on the ticket, release the claim, and let a fresh worker
    # take it. 0 disables.
    try:
        _requeue_at = int(os.environ.get(
            "WATCHTOWER_ANSWER_REQUEUE_BYTES",
            str(2 * int(os.environ.get(
                "WATCHTOWER_CONTEXT_RECYCLE_BYTES", "2500000") or 0)),
        ) or 0)
    except (TypeError, ValueError):
        _requeue_at = 5_000_000
    if _requeue_at > 0:
        _tsize = workers._claude_transcript_bytes(str(sid))
        if _tsize >= _requeue_at:
            from datetime import datetime as _dt, timezone as _tz
            stamp = _dt.now(_tz.utc).strftime("%Y-%m-%d %H:%M UTC")
            qa_note = (
                f"{item.get('text') or item.get('note') or ''}\n\n"
                f"[ANSWERED while blocked, {stamp}] Q: "
                f"{item.get('block_question') or '(see history)'}\n"
                f"A: {args.text}\n"
                f"(Original session {str(sid)[:8]} was over the context "
                f"budget — apply this answer fresh.)"
            )
            q.update(item["ref"], text=qa_note)
            q.release(item["ref"], session_id=str(
                item.get("claimed_by") or args.worker or ""))
            print(
                f"ANSWERED: {item['ref']} — answer embedded on the ticket and "
                f"claim released: original session {str(sid)[:8]} is over the "
                f"answer context budget ({_tsize} transcript bytes >= "
                f"{_requeue_at}); a fresh worker will apply it."
            )
            return 0
    repo = item.get("repo_path") or os.getcwd()
    prompt = (
        f"A human answered your blocked question on ticket {item['ref']}. "
        f"Their answer: {args.text}. Apply it, finish the ticket, and close it "
        f"with `wt close {item['ref']} --worker <your-id> --summary \"...\" "
        "--commit <SHA>` (or `--no-code` if no code changed). "
        f"If it still cannot be resolved, run `wt block` again with the new "
        f"open question."
    )
    from . import messages
    target = item.get("claimed_session_id") or item.get("claimed_by")
    delivery_engine = _answer_engine(item, args.engine)
    # Kimi has no local messages adapter. Without a delegate, parking this in
    # the outbox would retry the same unsupported adapter chain until dead.
    # Let the existing headless Kimi resume fallback run immediately instead.
    queue_on_fail = not (
        delivery_engine == "kimi" and not messages._delegate_base()
    )
    try:
        sent = messages.deliver_message(
            str(target),
            prompt,
            verb="steer",
            engine=delivery_engine,
            on_busy="hold" if queue_on_fail else "reject",
        )
    except Exception as e:  # never lose the answer to a delivery-layer crash
        sent = {"ok": False, "error": str(e)}
    if sent.get("ok"):
        print(f"ANSWERED: {item['ref']} — delivered to session {sid} via "
              f"{sent.get('transport', '?')} to apply your answer and close.")
        return 0
    if sent.get("queued"):
        # Busy or momentarily unreachable: the durable outbox will deliver once
        # the session goes idle. The answer_grace in requeue_orphaned_tickets
        # keeps the sweep from reopening the ticket in the meantime.
        held = "session is mid-turn" if sent.get("busy") else "delivery deferred"
        print(f"ANSWERED: {item['ref']} — {held}; answer queued for delivery "
              f"({sent.get('id', '?')}).")
        return 0
    # Unresolvable target (nothing queued): fall back to the headless resume
    # fork, which registers the resume child under the worker id so the orphan
    # sweep cannot reopen the ticket while the answer is applied.
    started = _resume_session_headless(
        sid,
        repo,
        prompt,
        delivery_engine,
        queue=item.get("project", ""),
        worker_id=item.get("claimed_by", ""),
    )
    if started:
        print(f"ANSWERED: {item['ref']} — resuming session {sid} in {repo} "
              f"to apply your answer and close.")
    else:
        print(f"ANSWERED: {item['ref']} — needs_input cleared, but delivery "
              f"failed ({sent.get('error', 'unknown')}); {delivery_engine} "
              "resume also failed to stay running. Resume manually: "
              f"wt discuss {item['ref']} --engine {delivery_engine}")
    return 0


def _comment_author_is_claimant(item: Dict[str, Any], args: argparse.Namespace) -> bool:
    """True when the caller of ``wt comment`` IS the ticket's claimant.

    Claimant notify exists to tell a working session that somebody ELSE said
    something on its ticket. Injecting a session's own comment back at it is
    pure noise: the worker gets steered mid-turn by a message it just wrote
    (WATCHTOWER-21, seen on BYMPURCH-14, where a Codex worker ran
    ``wt comment`` and was immediately interrupted with its own text).

    Identity comes from ``_caller_identity``, which reads the same
    ``--worker`` flag and ``CODEX_THREAD_ID``/``CLAUDE_CODE_SESSION_ID`` env
    the claim path records into ``claimed_by``/``claimed_session_id`` -- so a
    hosted worker matches with no extra flag. Either half matching is enough:
    a worker id is per-run and never shared between live processes, and the
    session id is the harness's own. The ``_default_worker_id`` fallback
    (``wt-cli-<ppid>``) can never collide with a real claimant, so a bare
    terminal ``wt comment`` on somebody else's ticket still notifies.
    """
    worker, session = _caller_identity(args)
    claimed_by = str(item.get("claimed_by") or "").strip()
    claimed_session = str(item.get("claimed_session_id") or "").strip()
    return bool(
        (worker and worker == claimed_by)
        or (session and session == claimed_session)
    )


def cmd_comment(args: argparse.Namespace) -> int:
    item = q.comment(args.ref, args.text, by=args.by, session_id=args.worker)
    if not item:
        print(f"(no item {args.ref})", file=sys.stderr)
        return 1
    delivery = ""
    target = item.get("claimed_session_id") or item.get("claimed_by")
    live = item.get("status") == "in_progress" and target
    if live and _comment_author_is_claimant(item, args):
        # Recorded on the ticket, but not echoed back at its own author.
        target = ""
        delivery = " — you are the claimant; not injected back at you"
    if item.get("status") == "in_progress" and target:
        from . import messages

        prompt = (
            f"[WATCHTOWER] A new comment was added to your claimed ticket "
            f"{item['ref']}:\n\n{args.text}"
        )
        try:
            sent = messages.deliver_message(str(target), prompt, verb="steer")
        except Exception as e:  # durable ticket comment already succeeded
            delivery = f" — live injection failed ({e})"
        else:
            if sent.get("ok"):
                delivery = (
                    " — injected into claimed worker via "
                    f"{sent.get('transport', '?')}"
                )
            elif sent.get("queued"):
                delivery = f" — live injection queued as {sent.get('id', '?')}"
            else:
                delivery = (
                    " — live injection unavailable"
                    + (f" ({sent.get('error')})" if sent.get("error") else "")
                )
    print(f"COMMENTED: {item['ref']}{delivery}")
    return 0


def cmd_discuss(args: argparse.Namespace) -> int:
    """Attach to a blocked ticket's worker session for a real discussion (WT-28).
    Resolves the ticket's session id + repo and runs `claude --resume` there
    (engine-aware). With --print, shows the command instead of running it."""
    item = q.get(args.ref)
    if not item:
        print(f"(no item {args.ref})", file=sys.stderr)
        return 1
    sid = item.get("claimed_session_id")
    if not sid:
        print(f"(no resumable session on {args.ref} — it was never claimed with a "
              f"real session id)", file=sys.stderr)
        return 1
    repo = item.get("repo_path") or os.getcwd()
    if args.engine == "codex":
        inner = ["codex", "resume", sid]
    elif args.engine == "kimi":
        inner = ["kimi", "--session", sid]
    else:
        inner = ["claude", "--resume", sid]
    cmd = "cd " + shlex.quote(repo) + " && " + " ".join(shlex.quote(c) for c in inner)
    if args.print:
        print(cmd)
        return 0
    print(f"Resuming {item['ref']} (session {sid}) in {repo} …")
    return os.system(cmd) >> 8


def cmd_workers(args: argparse.Namespace) -> int:
    rows = workers.list_workers()
    if args.json:
        print(json.dumps(rows, indent=2))
        return 0
    if not rows:
        print("(no workers tracked)")
        return 0
    print(f"{'WORKER':<22}{'PID':>8}  {'QUEUE':<12}{'ENGINE':<8}{'ALIVE':<6}STARTED")
    for w in rows:
        print(
            f"{w.get('worker_id',''):<22}{w.get('pid',0):>8}  "
            f"{w.get('queue',''):<12}{w.get('engine',''):<8}"
            f"{'yes' if w.get('alive') else 'no':<6}{w.get('started_at','')}"
        )
    return 0


def cmd_workers_release(args: argparse.Namespace) -> int:
    """Gracefully retire selected workers before their next claim."""
    released = workers.release_workers(engine=args.engine, queue=args.queue or "")
    if args.json:
        print(json.dumps({"released": released}, indent=2))
        return 0
    if not released:
        print("no matching live workers")
        return 0
    print("gracefully retiring: " + ", ".join(
        str(worker.get("worker_id") or "") for worker in released
    ))
    return 0


def cmd_session_names(args: argparse.Namespace) -> int:
    """Maintenance helpers for worker session display names."""
    if args.session_names_command != "backfill":
        print("error: session-names requires a subcommand", file=sys.stderr)
        return 1
    rows = workers.backfill_recent_session_titles(
        hours=args.hours,
        dry_run=args.dry_run,
    )
    print(json.dumps(rows, indent=2))
    return 0



def cmd_send(args: argparse.Namespace) -> int:
    """Push a message to a worker/agent/session via the adapter chain; on
    delivery failure the message is parked in the durable outbox (unless
    --no-queue) for the daemon to retry.

    A text of ``-`` reads the message from stdin: the quote-safe path for
    multi-paragraph bodies (agent reports full of quotes/$/backticks that
    would break as a shell argument)."""
    from . import messages
    text = args.text
    if text == "-":
        raw = sys.stdin.read()
        if not raw.strip():
            print("error: empty message on stdin", file=sys.stderr)
            return 1
        # Preserve the body verbatim (leading blank lines, indentation --
        # "the complete report text" means text-for-text). Only the single
        # newline every heredoc/echo appends is dropped.
        text = raw.removesuffix("\n")
    res = messages.send(
        args.target, text, mode=args.mode,
        queue_on_fail=not args.no_queue,
        ttl_s=args.ttl,
    )
    if args.json:
        print(json.dumps(res, indent=2))
        return 0 if (res.get("ok") or res.get("queued")) else 1
    if res.get("ok"):
        extra = f"  (log: {res['log']})" if res.get("log") else ""
        print(f"SENT: {args.target} via {res.get('transport', '?')}{extra}")
        return 0
    if res.get("queued"):
        why = res.get("error", "")
        print(f"QUEUED: {res.get('id', '?')} for {args.target}"
              + (f"  ({why})" if why else ""))
        return 0
    print(f"error: {res.get('error', 'send failed')}", file=sys.stderr)
    return 1


def cmd_ask(args: argparse.Namespace) -> int:
    """Ask a target a question and wait for the reply. Prints the answer text;
    exits 1 on timeout (partial text, if any, goes to stdout after the error).

    With --notify-webhook (WT-59, the async half — mirrors wt wait's): the
    CLI returns immediately and a detached child does the blocking ask, then
    POSTs {event, target, ok, answer|error} to the webhook."""
    from . import messages
    webhook = getattr(args, "notify_webhook", "") or ""
    if webhook and not getattr(args, "_notify_child", False):
        cmd = [
            sys.executable, "-m", "watchtower.cli", "ask",
            args.target, args.text,
            "--timeout", str(args.timeout),
            "--notify-webhook", webhook, "--_notify-child",
        ]
        subprocess.Popen(
            cmd, start_new_session=True,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        print(f"accepted: answer will POST to {webhook}")
        return 0
    res = messages.ask(
        args.target, args.text, timeout_ms=int(args.timeout * 1000)
    )
    if webhook:
        payload = {
            "event": "ask-answered" if res.get("ok") else "ask-failed",
            "target": args.target,
            "ok": bool(res.get("ok")),
            "answer": res.get("answer") or "",
        }
        if not res.get("ok"):
            payload["error"] = res.get("error", "ask failed")
            if res.get("partial"):
                payload["partial"] = res["partial"]
        _post_webhook(webhook, payload)
        return 0 if res.get("ok") else 1
    if args.json:
        print(json.dumps(res, indent=2))
        return 0 if res.get("ok") else 1
    if res.get("ok"):
        print(res.get("answer") or "")
        return 0
    print(f"error: {res.get('error', 'ask failed')}", file=sys.stderr)
    if res.get("partial"):
        print(res["partial"])
    return 1


# WT-100: the three agent families /critique picks its two spawns from.
# Fixed and small on purpose -- CCC supports more engines (gemini, cursor,
# kilo, hermes) but the spec asks for "the other 2 of 3", not "every engine
# but mine".
_CRITIQUE_FAMILIES: Tuple[str, ...] = ("claude", "codex", "antigravity")


def _detect_family() -> str:
    """Best-effort detection of the calling agent's family from harness env.
    Claude Code sets CLAUDE_CODE_SESSION_ID; Codex sets CODEX_THREAD_ID
    (verified against codex 0.142). Antigravity sets neither that we know of;
    an AGY caller must pass --family. Empty string when nothing matches."""
    if os.environ.get("CLAUDE_CODE_SESSION_ID"):
        return "claude"
    if os.environ.get("CODEX_THREAD_ID"):
        return "codex"
    return ""


def _default_report_to() -> Tuple[str, str]:
    """Default reply target from harness env: ``(target, note)``.

    A Claude session UUID is directly addressable. A Codex thread id is not:
    resolve_target treats an unknown bare UUID as engine=claude, so delivery
    would try the wrong transport and never land. Registering it in the
    agents registry (engine=codex, deterministic per-thread name, idempotent)
    makes ``wt send @name`` route via the codex app-server transport."""
    sid = os.environ.get("CLAUDE_CODE_SESSION_ID", "").strip()
    if sid:
        return sid, ""
    tid = os.environ.get("CODEX_THREAD_ID", "").strip()
    if tid:
        from . import messages
        name = f"codex-thread-{tid.replace('-', '')[:12]}"
        try:
            messages.register_agent(name, tid, engine="codex", cwd=os.getcwd())
        except ValueError:
            return "", ""
        return f"@{name}", (
            f"note: registered @{name} -> codex thread {tid[:13]}... so "
            "replies route via the codex transport"
        )
    return "", ""


def _select_critique_engines(
    family: str,
    model1: str = "",
    model2: str = "",
    *,
    available,
) -> Tuple[List[str], List[str]]:
    """Pick the critique engines for a spawner in ``family``.

    Returns ``(engines, notes)``; raises ValueError for anything unusable.
    The whole selection is preflighted here -- supported AND installed --
    before the caller spawns anything, so a bad second engine can never
    strand a half-spawned critique (and --dry-run reflects reality).

    Rules, in order:
      - explicit --engine1/--engine2 picks are honored exactly: unsupported
        or not-installed errors out, never falls back (the user asked for
        that engine by name);
      - two identical explicit picks are rejected (duplicate critics);
      - a default slot (the two families other than ``family``) whose CLI is
        missing -- or which an explicit pick already took -- falls back to
        the first installed, not-yet-picked family (own family first);
      - with nothing distinct left to fall back to, the slot is dropped (one
        good critic beats a duplicate); zero engines is an error."""
    if family not in _CRITIQUE_FAMILIES:
        raise ValueError(
            f"unsupported family {family!r} "
            f"(supported: {', '.join(_CRITIQUE_FAMILIES)})"
        )
    others = [f for f in _CRITIQUE_FAMILIES if f != family]
    supported = ", ".join(_CRITIQUE_FAMILIES)
    notes: List[str] = []
    picks: List[Optional[str]] = [
        (model1 or "").strip().lower() or None,
        (model2 or "").strip().lower() or None,
    ]
    if picks[0] and picks[0] == picks[1]:
        raise ValueError(
            f"--engine1 and --engine2 are both {picks[0]!r}; two identical "
            "critics duplicate each other -- pick two different engines "
            "(or run `wt spawn` twice if you really want that)"
        )
    for i, eng in enumerate(picks):
        if eng is None:
            continue
        if eng not in _CRITIQUE_FAMILIES:
            raise ValueError(
                f"unsupported engine {eng!r} (supported: {supported})"
            )
        if not available(eng):
            raise ValueError(
                f"{eng} CLI not found -- an explicitly requested engine "
                f"never falls back; install it or drop --engine{i + 1}"
            )
    for i, eng in enumerate(picks):
        if eng is not None:
            continue
        want = others[i]
        if want not in picks and available(want):
            picks[i] = want
            continue
        fallback = next(
            (c for c in (family, *_CRITIQUE_FAMILIES)
             if c not in picks and available(c)),
            None,
        )
        why = (
            f"{want} already picked for the other critic"
            if want in picks else f"{want} CLI not found"
        )
        if fallback is None:
            notes.append(
                f"note: {why} and no distinct installed engine is left -- "
                "spawning fewer critics"
            )
            continue
        suffix = " (your own family)" if fallback == family else ""
        notes.append(
            f"note: {why} -- falling back to {fallback}{suffix} "
            "for this critic"
        )
        picks[i] = fallback
    engines = [e for e in picks if e]
    if not engines:
        raise ValueError(
            "no critique engine is installed (need at least one of: "
            f"{supported})"
        )
    return engines, notes


def _critique_prompt(goal: str) -> str:
    """Wrap a user-supplied goal with the baked-in critique ground rules from
    WT-100's answered spec: contrarian, no priors, comprehensive, scored,
    concrete resolutions with the score delta each would buy."""
    return (
        f"{goal.strip()}\n\n"
        "Critique the above. Ground rules:\n"
        "- Be contrarian: look for reasons this is wrong, not reasons to agree.\n"
        "- Ignore any priors from your own past work on this -- assess it fresh.\n"
        "- Be comprehensive: don't stop at the first issue you find.\n"
        "- Give an overall score out of 10 for the current state.\n"
        "- For each issue, give a concrete resolution and the score improvement "
        "it would buy (e.g. \"fixes X: 6 -> 8\").\n"
    )


def cmd_critique(args: argparse.Namespace) -> int:
    """Spawn two independent critique agents on a goal (WT-100).

    Spawns natively via workers.spawn_adhoc -- no CCC dependency. The two
    engines default to the families OTHER than yours (--family, auto-detected
    from harness env when omitted); --engine1/--engine2 override them
    explicitly. Selection is preflighted in _select_critique_engines before
    anything spawns. Each critic's goal carries a WT-native reply-to footer
    (`wt send <report_to> -`), so the report comes back over the same
    delivery path (and outbox fallback) every other WT message uses.
    """
    from . import workers

    family = (args.family or "").strip().lower() or _detect_family() or "claude"
    try:
        engines, notes = _select_critique_engines(
            family, args.model1, args.model2,
            available=workers.engine_available,
        )
    except ValueError as e:
        if args.json:
            print(json.dumps([{"ok": False, "error": str(e)}], indent=2))
        else:
            print(f"error: {e}", file=sys.stderr)
        return 1
    for note in notes:
        print(note, file=sys.stderr)

    report_to = args.report_to
    if not report_to:
        report_to, rnote = _default_report_to()
        if rnote:
            print(rnote, file=sys.stderr)
    elif family != "claude":
        # An unknown bare UUID resolves with engine=claude *by assumption*
        # (messages.resolve_target). A non-claude caller passing their own
        # session UUID would get replies routed down the claude transports,
        # which can never deliver to them -- say so before it happens.
        from . import messages
        try:
            resolved = messages.resolve_target(report_to)
        except ValueError:
            pass  # spawn_adhoc fail-fasts on this with its own error
        else:
            if resolved.get("known") is False:
                print(
                    f"warning: --report-to {report_to} is an unknown session "
                    "UUID, so replies will be delivered via the *claude* "
                    f"transport. If that UUID is a {family} session, register "
                    f"it first (`wt agents register <name> --session "
                    f"{report_to} --engine {family}`) and pass @<name> "
                    "instead.",
                    file=sys.stderr,
                )
    if not report_to and not args.dry_run:
        print(
            "warning: no --report-to and no session detected in the "
            "environment ($CLAUDE_CODE_SESSION_ID / $CODEX_THREAD_ID) -- "
            "the critics will run, but their reports will only land in "
            "their log files below. Pass --report-to <you> to receive them.",
            file=sys.stderr,
        )
    prompt = _critique_prompt(args.goal)

    results = []
    for engine in engines:
        try:
            rec = workers.spawn_adhoc(
                prompt, engine, repo_path=args.cwd, name="critique",
                report_to=report_to, dry_run=args.dry_run,
            )
            data: Dict[str, object] = {"ok": True, **rec}
        except (ValueError, OSError) as e:
            data = {"ok": False, "engine": engine, "error": str(e)}
        results.append(data)

    ok_all = all(r.get("ok") for r in results)
    if args.json:
        print(json.dumps(results, indent=2))
        return 0 if ok_all else 1
    for r in results:
        if r.get("ok"):
            what = "would spawn" if r.get("dry_run") else "spawned"
            print(f"{what} {r['engine']}: {r.get('worker_id', '?')} "
                  f"(log {r.get('log', '-')})")
        else:
            print(f"error spawning {r['engine']}: {r.get('error', 'spawn failed')}", file=sys.stderr)
    if report_to and not args.dry_run and ok_all:
        print(f"critics will report back to {report_to} via `wt send` when done")
    return 0 if ok_all else 1


def cmd_spawn(args: argparse.Namespace) -> int:
    """Spawn one ad-hoc one-shot agent on an arbitrary goal (WT Spawn).

    The native primitive `wt critique` builds on: any of claude / codex /
    antigravity, tracked in workers.json (kind=adhoc, exempt from reap), with
    an optional WT-native reply-to footer."""
    from . import workers

    try:
        rec = workers.spawn_adhoc(
            args.goal, (args.engine or "claude").strip().lower(),
            model=args.model, repo_path=args.repo, name=args.name,
            report_to=args.report_to, dry_run=args.dry_run,
        )
    except (ValueError, OSError) as e:
        if args.json:
            print(json.dumps({"ok": False, "error": str(e)}))
        else:
            print(f"error: {e}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps({"ok": True, **rec}, indent=2))
        return 0
    what = "would spawn" if rec.get("dry_run") else "spawned"
    print(f"{what} {rec['engine']} agent {rec['worker_id']} "
          f"(pid {rec.get('pid', 0)}, log {rec.get('log', '-')})")
    if args.report_to and not args.dry_run:
        print(f"it will report back to {args.report_to} via `wt send` when done")
    return 0


def cmd_receipts(args: argparse.Namespace) -> int:
    """Delivery receipts (WT-77): ledger of verified deliveries.

    ``wt receipts``           list (sweeps pending first)
    ``wt receipts get <id>``  one receipt
    ``wt receipts stats``     soak-gate counts (landed/advanced/pending/lost)
    """
    from . import receipts

    sub = getattr(args, "receipts_command", None)
    if sub == "get":
        rec = receipts.get(args.id)
        if rec is None:
            print(f"not found: {args.id}", file=sys.stderr)
            return 1
        print(json.dumps(rec, indent=2))
        return 0
    if sub == "stats":
        s = receipts.stats(window_s=float(args.window_days) * 86400.0)
        if getattr(args, "json", False):
            print(json.dumps(s, indent=2))
        else:
            print(
                f"last {s['window_days']}d: {s['landed']} landed, "
                f"{s['advanced']} advanced, {s['pending']} pending, "
                f"{s['lost']} LOST of {s['total']}"
            )
            if s["lost"]:
                print("inspect lost receipts: wt receipts --status lost --json")
        return 1 if s["lost"] else 0
    receipts.sweep()
    rows = receipts.list_receipts(status=getattr(args, "status", None) or None)
    if getattr(args, "json", False):
        print(json.dumps(rows, indent=2))
        return 0
    for r in rows[-50:]:
        print(
            f"{r['id']}  {r.get('status','?'):9}  {r.get('transport','?'):9}  "
            f"{str(r.get('sid',''))[:8]}  {time.strftime('%m-%d %H:%M', time.localtime(r.get('sent_at') or 0))}"
        )
    if not rows:
        print("no receipts")
    return 0


def cmd_logs(args: argparse.Namespace) -> int:
    """Log-dir maintenance (WT-74): `wt logs prune [--dry-run] [--json]`."""
    from . import logmaint

    sub = getattr(args, "logs_command", None)
    if sub != "prune":
        print("usage: wt logs prune [--dry-run] [--json]", file=sys.stderr)
        return 2
    report = logmaint.prune(dry_run=getattr(args, "dry_run", False))
    if getattr(args, "json", False):
        print(json.dumps(report, indent=2))
        return 0
    tag = " (dry-run)" if report.get("dry_run") else ""
    print(
        f"pruned {len(report.get('pruned', []))} logs, "
        f"{len(report.get('fifos_removed', []))} orphan fifos, "
        f"freed {report.get('freed_bytes', 0) / 1048576:.1f} MB, "
        f"kept {report.get('kept', 0)}{tag}"
    )
    if report.get("error"):
        print(f"warning: {report['error']}", file=sys.stderr)
    return 0


def cmd_gh(args: argparse.Namespace) -> int:
    """GitHub-backend diagnostics: `wt gh recheck [--json]`.

    Forces a live `gh issue list` for every GitHub-backed queue, bypassing
    the persisted connectivity backoff -- the explicit "I fixed it, check
    now" action instead of waiting out the escalated retry window.
    """
    sub = getattr(args, "gh_command", None)
    if sub != "recheck":
        print("usage: wt gh recheck [--json]", file=sys.stderr)
        return 2
    results = []
    for name in q._github_projects():
        backend = q._github_backend_for_project(name)
        if backend is None:
            continue
        try:
            backend.list_items(fresh=True, strict=True)
            results.append({"queue": name, "ok": True, "error": ""})
        except Exception as e:
            results.append({"queue": name, "ok": False, "error": str(e)})
    gh = health.github_connectivity()
    if args.json:
        print(json.dumps({"queues": results, "github": gh}, indent=2))
        return 0
    if not results:
        print("no GitHub-backed queues configured")
    for r in results:
        status = "ok" if r["ok"] else f"FAIL — {r['error']}"
        print(f"{r['queue']}: {status}")
    if gh.get("alert"):
        print(f"still unreachable — {gh.get('outage_duration')} — {gh.get('last_error')}")
    else:
        print("GitHub connectivity: healthy")
    return 0


def cmd_outbox(args: argparse.Namespace) -> int:
    """Inspect and manage messages parked in the durable outbox."""
    from . import messages

    sub = getattr(args, "outbox_command", None)
    if sub == "ls":
        rows = messages.outbox_list(status=None if args.all else "pending")
        if args.json:
            print(json.dumps({"messages": rows}, indent=2))
            return 0
        if not rows:
            print("(no outbox messages)")
            return 0
        print(f"{'ID':<17}{'STATUS':<10}{'ATTEMPTS':>8}  {'NEXT':<20}{'TO':<38}ERROR")
        print("-" * 105)
        for m in rows:
            print(
                f"{str(m.get('id','')):<17}{str(m.get('status','')):<10}"
                f"{int(m.get('attempts', 0)):>8}  "
                f"{str(m.get('next_attempt_at','')):<20}"
                f"{str(m.get('to','')):<38}"
                f"{str(m.get('last_error',''))}"
            )
        return 0

    if sub == "retry":
        if args.all_dead:
            rows = messages.outbox_retry_all_dead()
            print(f"RETRY: {len(rows)} dead message(s)")
            return 0
        if not args.id:
            print("error: retry requires <id> or --all-dead", file=sys.stderr)
            return 1
        try:
            row = messages.outbox_retry(args.id)
        except KeyError:
            print(f"error: no outbox message {args.id}", file=sys.stderr)
            return 1
        print(f"RETRY: {row.get('id')}")
        return 0

    if sub == "rm":
        if messages.outbox_remove(args.id):
            print(f"REMOVED: {args.id}")
            return 0
        print(f"error: no outbox message {args.id}", file=sys.stderr)
        return 1

    print("usage: wt outbox ls|retry|rm", file=sys.stderr)
    return 1


def cmd_agents(args: argparse.Namespace) -> int:
    """Merged view: registered agent names plus live WT workers."""
    from . import messages
    now = time.time()

    def with_state(row: dict) -> dict:
        out = dict(row)
        sid = str(out.get("session_id") or "")
        out["state"] = messages.session_state(sid, now=now) if sid else "unknown"
        return out

    agents = [with_state(a) for a in messages.list_agents()]
    live = [with_state(w) for w in workers.list_workers() if w.get("alive")]
    if args.json:
        print(json.dumps({"agents": agents, "workers": live}, indent=2))
        return 0
    if not agents and not live:
        print("(no agents registered, no live workers)")
        return 0
    print(f"{'NAME':<24}{'KIND':<8}{'STATE':<8}{'ENGINE':<8}{'SESSION':<38}CWD/QUEUE")
    print("-" * 98)
    for a in agents:
        kind = a.get("kind") or "agent"
        if kind == "recent":
            name = str(a.get("session_id") or "")[:8]
            cwd = a.get("cwd_slug", "")
        else:
            name = f"@{a.get('name','')}"
            cwd = a.get("cwd", "")
        print(
            f"{name:<24}{kind:<8}{a.get('state',''):<8}"
            f"{a.get('engine',''):<8}"
            f"{a.get('session_id',''):<38}{cwd}"
        )
    for w in live:
        print(
            f"{w.get('worker_id',''):<24}{'worker':<8}{w.get('state',''):<8}"
            f"{w.get('engine',''):<8}"
            f"{w.get('session_id','') or '-':<38}{w.get('queue','')}"
        )
    return 0


def cmd_agent(args: argparse.Namespace) -> int:
    """Manage the agents registry: register/set-name a session UUID, rm a name.

    Reached via `wt agents register|set-name|rm` (the address-book pattern,
    git-remote style) or the hidden `wt agent ...` compat alias."""
    from . import messages
    sub = getattr(args, "agent_command", None)
    if sub in ("register", "set-name"):
        try:
            rec = messages.register_agent(
                args.name, args.session, engine=args.engine, cwd=args.cwd,
            )
        except ValueError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        print(f"REGISTERED: @{rec['name']} -> {rec['session_id']} "
              f"({rec['engine']})")
        return 0
    if sub == "rm":
        if messages.remove_agent(args.name):
            print(f"REMOVED: @{str(args.name).lstrip('@')}")
            return 0
        print(f"(no agent {args.name})", file=sys.stderr)
        return 1
    print("usage: wt agents register|set-name <name> --session <uuid> | "
          "wt agents rm <name>", file=sys.stderr)
    return 1


def _resolve_chat_participant(target: str) -> dict:
    """Resolve a `wt chat new --with` / `wt chat add` target to a
    participant dict (``{"session_id", "name"}``) for chats.create_chat /
    chats.add_participant.

    Name preference (docs/messaging-design.md addressing rules): the
    registered agent name, else the live worker id, else an 8-char short
    session id. Raises ``ValueError`` when the target cannot be resolved."""
    from . import messages
    resolved = messages.resolve_target(target)
    sid = str(resolved.get("session_id") or target)
    kind = resolved.get("kind")
    if kind == "worker":
        worker = resolved.get("worker") or {}
        name = str(worker.get("worker_id") or target)
    elif kind == "agent":
        name = str(target).lstrip("@")
    else:
        name = sid[:8]
    return {"session_id": sid, "name": name}


def _resolve_chat_author(ref: str, value: str) -> tuple:
    """Match a `wt chat post --as` / `nudge --target` / `leave` value
    against a chat's existing participants: session id, sid8 prefix, or
    display name (case-insensitive). Returns ``(session_id, name)``."""
    from . import chats
    _, sidecar = chats.find_chat(ref)
    session_ids = [str(s) for s in (sidecar.get("session_ids") or [])]
    name_map = {str(k): str(v) for k, v in (sidecar.get("name_map") or {}).items()}
    v = str(value).lstrip("@")
    for sid in session_ids:
        if sid == value or sid[:8].lower() == v.lower():
            return sid, name_map.get(sid, sid[:8])
    for sid, name in name_map.items():
        if name.lower() == v.lower():
            return sid, name
    raise ValueError(f"{value!r} is not a participant in chat {ref!r}")


def cmd_chat_new(args: argparse.Namespace) -> int:
    """Create a chat and send each `--with` target an initial check-in.

    Resolves every target via messages.resolve_target, creates the chat
    (chats.create_chat), then delivers one check-in message per participant
    through messages.send, using chats.build_nudge_text for the body."""
    from . import chats, messages
    targets = [t.strip() for t in (args.with_targets or "").split(",") if t.strip()]
    if not targets:
        print("error: --with requires at least one target", file=sys.stderr)
        return 1
    participants = []
    for t in targets:
        try:
            participants.append(_resolve_chat_participant(t))
        except ValueError as e:
            print(f"error: could not resolve {t!r}: {e}", file=sys.stderr)
            return 1
    info = chats.create_chat(args.topic, participants, include_human=args.include_human)
    sent = []
    for part in participants:
        text = chats.build_nudge_text(info["path"], args.topic, "topic", part["session_id"])
        res = messages.send(part["session_id"], text)
        sent.append({"target": part["session_id"], "name": part["name"],
                      "ok": bool(res.get("ok")), "queued": bool(res.get("queued"))})
    if args.json:
        print(json.dumps({**info, "sent": sent}, indent=2))
        return 0
    print(f"CHAT CREATED: {info['path']}")
    print(f"  ref: {info['uuid']}")
    for s in sent:
        status = "sent" if s["ok"] else ("queued" if s["queued"] else "failed")
        print(f"  check-in -> {s['name']} ({s['target'][:8]}): {status}")
    return 0


def cmd_chat_post(args: argparse.Namespace) -> int:
    """Post a message; --as resolves to a participant, default author Human."""
    from . import chats
    author_sid = None
    author_name = "Human"
    if args.as_target:
        try:
            author_sid, author_name = _resolve_chat_author(args.ref, args.as_target)
        except ValueError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
    try:
        res = chats.post(args.ref, args.message, author_sid=author_sid, author_name=author_name)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    print(f"POSTED: {res['heading']}")
    return 0


def cmd_chat_read(args: argparse.Namespace) -> int:
    """Print a chat transcript (speaker + message), or --json for the parsed dict."""
    from . import chats
    try:
        data = chats.read_chat(args.ref, tail=(args.tail or None))
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(data, indent=2))
        return 0
    print(f"# {data['topic']}  (mode={data['mode']})"
          + ("  [archived]" if data.get("archived") else "")
          + ("  [closed]" if data.get("closed_at") else ""))
    if not data["messages"]:
        print("(no messages yet)")
        return 0
    for m in data["messages"]:
        speaker = m.get("author_name") or "Human"
        print(f"[{m.get('ts', '')}] {speaker}: {m.get('body', '')}")
    return 0


def cmd_chat_ls(args: argparse.Namespace) -> int:
    """List chats; --archived includes archived ones (matches chats.list_chats)."""
    from . import chats
    rows = chats.list_chats(include_archived=args.archived)
    if args.json:
        print(json.dumps(rows, indent=2))
        return 0
    if not rows:
        print("(no chats)")
        return 0
    for r in rows:
        state = "archived" if r.get("archived") else ("closed" if r.get("closed_at") else "open")
        print(f"{r['path']}  [{state}]  {r.get('topic', '')}")
    return 0


def cmd_chat_nudge(args: argparse.Namespace) -> int:
    """Manual nudge: --target picks one participant, else the same
    deterministic targeting the daemon uses (chats.pick_nudge_targets)."""
    from . import chats, messages
    try:
        md_path, sidecar = chats.find_chat(args.ref)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    if args.target:
        try:
            sid, _name = _resolve_chat_author(args.ref, args.target)
        except ValueError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        targets = [sid]
    else:
        try:
            md_text = md_path.read_text()
        except OSError:
            md_text = ""
        targets = chats.pick_nudge_targets(md_text, sidecar)
    if not targets:
        print("(no targets to nudge)")
        return 0
    ok = 0
    for sid in targets:
        text = chats.build_nudge_text(
            str(md_path), sidecar.get("topic", ""), sidecar.get("mode", "topic"), sid
        )
        res = messages.send(sid, text)
        ok += 1 if res.get("ok") else 0
        status = "sent" if res.get("ok") else ("queued" if res.get("queued") else "failed")
        print(f"  nudge -> {sid[:8]}: {status}")
    print(f"NUDGED: {ok}/{len(targets)}")
    return 0


def cmd_chat_add(args: argparse.Namespace) -> int:
    """Add a participant to a chat (wraps chats.add_participant)."""
    from . import chats
    try:
        part = _resolve_chat_participant(args.target)
        chats.add_participant(args.ref, part["session_id"], part["name"])
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    print(f"ADDED: {part['name']} ({part['session_id'][:8]}) -> {args.ref}")
    return 0


def cmd_chat_leave(args: argparse.Namespace) -> int:
    """Remove a participant from a chat (wraps chats.remove_participant)."""
    from . import chats
    try:
        sid, name = _resolve_chat_author(args.ref, args.target)
        chats.remove_participant(args.ref, sid)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    print(f"REMOVED: {name} ({sid[:8]}) from {args.ref}")
    return 0


def cmd_chat_set(args: argparse.Namespace) -> int:
    """Get/set per-chat nudge-policy knobs (WT-61).

    With no knob flags, prints the current effective policy
    (chats.get_chat_policy: sidecar override or module default). With any
    knob flag, persists the override via chats.set_chat_policy and prints
    the new effective policy."""
    from . import chats
    knobs = {
        "nudge_interval_s": args.nudge_interval_s,
        "idle_close_s": args.idle_close_s,
        "max_auto_nudges_per_hour": args.max_auto_nudges_per_hour,
    }
    try:
        if any(v is not None for v in knobs.values()):
            policy = chats.set_chat_policy(args.ref, **knobs)
        else:
            policy = chats.get_chat_policy(args.ref)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(policy, indent=2))
        return 0
    for k, v in policy.items():
        print(f"  {k} = {v}")
    return 0


def cmd_chat_archive(args: argparse.Namespace) -> int:
    """Archive a chat (wraps chats.set_archived)."""
    from . import chats
    try:
        chats.set_archived(args.ref, True)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    print(f"ARCHIVED: {args.ref}")
    return 0


def cmd_chat_close(args: argparse.Namespace) -> int:
    """Close a chat (wraps chats.close_chat)."""
    from . import chats
    try:
        chats.close_chat(args.ref)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    print(f"CLOSED: {args.ref}")
    return 0


def cmd_chat(args: argparse.Namespace) -> int:
    """Dispatch `wt chat <subcommand>` (same pattern as cmd_agent)."""
    handlers = {
        "new": cmd_chat_new, "post": cmd_chat_post, "read": cmd_chat_read,
        "ls": cmd_chat_ls, "nudge": cmd_chat_nudge, "add": cmd_chat_add,
        "leave": cmd_chat_leave, "archive": cmd_chat_archive, "close": cmd_chat_close,
        "set": cmd_chat_set,
    }
    fn = handlers.get(getattr(args, "chat_command", None))
    if fn is None:
        print("usage: wt chat new|post|read|ls|nudge|add|leave|archive|close|set ...",
              file=sys.stderr)
        return 1
    return fn(args)


def cmd_monitor(args: argparse.Namespace) -> int:
    """Monitor-as-a-job (WT-FEATURES-20): run a check command; if it fails
    (non-zero exit), file a ticket into the queue so a worker drains it. Pair
    with cron/launchd for scheduled sanity checks (e.g. a landing page)."""
    from . import queue as q
    rc = os.system(args.cmd) >> 8
    if rc == 0:
        print(f"OK: `{args.cmd}` passed (rc=0); no ticket filed")
        return 0
    note = args.note or f"Monitor failed: `{args.cmd}` exited {rc}"
    item = q.enqueue(note=note, title=(args.title or "monitor failure"),
                     project=args.queue)
    print(f"FAIL (rc={rc}) -> filed {item.get('ref')} in {args.queue}")
    return 0


def cmd_migrate_store(args: argparse.Namespace) -> int:
    """One-time JSON→SQLite store migration (idempotent; see queue docstring)."""
    from . import queue as q

    try:
        result = q.migrate_store()
    except Exception as e:  # corrupt JSON source — refuse to shadow it
        print(f"migrate-store: refusing to migrate: {e}", file=sys.stderr)
        return 1
    n = result["items"]
    if result["migrated"]:
        print(f"migrated {n} item(s) into {result['db']}")
    else:
        print(f"store is already SQLite ({n} item(s)) at {result['db']}")
    return 0


def cmd_export_json(args: argparse.Namespace) -> int:
    """Dump the store in the classic {counter, items} JSON interchange shape."""
    from . import queue as q

    try:
        blob = json.dumps(q.export_data(), indent=2)
    except Exception as e:
        print(f"export-json: {e}", file=sys.stderr)
        return 1
    if args.out:
        out = Path(args.out).expanduser()
        out.parent.mkdir(parents=True, exist_ok=True)
        tmp = str(out) + ".tmp"
        with open(tmp, "w") as f:
            f.write(blob + "\n")
        os.replace(tmp, out)
        print(f"wrote {out}")
    else:
        print(blob)
    return 0


def cmd_dedup(args: argparse.Namespace) -> int:
    """Exact-key dedup pass (WT-FEATURES-14, first cut): group open tickets by
    normalized title+note, keep the oldest in each group, and (with --apply)
    close the rest as duplicates. The semantic merge+rank pass is a follow-up."""
    import re
    from . import queue as q

    def norm(it: dict) -> str:
        s = (str(it.get("title", "")) + " " + str(it.get("note", ""))).lower()
        return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", "", s)).strip()

    items = [
        i for i in q.list_items(status="open")
        if not args.queue or i.get("project") == args.queue
    ]
    groups: dict = {}
    for it in items:
        key = norm(it)
        if key:
            groups.setdefault(key, []).append(it)
    dups = {k: v for k, v in groups.items() if len(v) > 1}
    if not dups:
        print("no exact duplicates")
        return 0
    closed = 0
    for v in dups.values():
        v.sort(key=lambda x: int(x.get("number", 0)))
        keep, rest = v[0], v[1:]
        print(f"dup group: keep {keep['ref']} | dupes {[x['ref'] for x in rest]}")
        if args.apply:
            for x in rest:
                q.close(x["ref"], resolution=f"duplicate of {keep['ref']}")
                closed += 1
    print(f"closed {closed} duplicate(s)" if args.apply
          else "(dry-run; pass --apply to close duplicates)")
    return 0


def cmd_set(args: argparse.Namespace) -> int:
    """Compatibility command for the queue settings that predate ``config``."""
    from . import config
    if not _validate_queue_worker_settings(args, config):
        return 1
    changed = []
    if args.backend is not None:
        try:
            config.set_backend(args.queue, args.backend)
        except ValueError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        changed.append(f"backend={config.backend(args.queue)}")
    if args.github_repo is not None:
        try:
            config.set_github_repo(args.queue, args.github_repo)
        except ValueError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        changed.append(f"github_repo={args.github_repo}")
    if args.github_assignee is not None:
        config.set_github_assignee(args.queue, args.github_assignee)
        changed.append(f"github_assignee={config.github_assignee(args.queue)}")
    if args.repo_path is not None:
        config.set_repo_path(args.queue, args.repo_path)
        changed.append(f"repo_path={args.repo_path}")
    if args.engine is not None:
        config.set_engine(args.queue, args.engine)
        changed.append(f"engine={args.engine}")
    if args.model is not None:
        config.set_model(args.queue, args.model)
        changed.append(f"model={str(args.model or '').strip() or '(engine default)'}")
    if args.engine is not None or args.model is not None:
        released = workers.release_workers(queue=args.queue, mismatched=True)
        if released:
            changed.append(
                "retiring=" + ",".join(
                    str(worker.get("worker_id") or "") for worker in released
                )
            )
    if args.effort is not None:
        config.set_effort(args.queue, args.effort)
        changed.append(f"effort={args.effort or '(engine default)'}")
    if args.desired_workers is not None:
        try:
            config.set_desired_workers(args.queue, args.desired_workers)
        except ValueError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        changed.append(f"desired_workers={args.desired_workers}")
    if not changed:
        cfg = config.get_queue_config(args.queue)
        print(f"{args.queue}: {cfg if cfg else '(no config)'}")
    else:
        print(f"{args.queue}: {', '.join(changed)}")
    return 0


def _warn_if_public_repo(queue: str, config) -> None:
    """Print the public-repo warning before auto-drain is switched on.

    A warning, not a refusal: draining a public repo is a real (if bold)
    choice. But it is the moment the fleet stops working only your issues and
    starts working strangers', so it must be said at the point of decision.
    Never fatal — an unreachable/unknown repo just says nothing.
    """
    if config.backend(queue) != "github":
        return
    try:
        from .github_backend import public_repo_warning
        warning = public_repo_warning(queue, config.github_repo(queue))
    except Exception:
        return
    if warning:
        print(warning, file=sys.stderr)


def cmd_drain(args: argparse.Namespace) -> int:
    """Enable or disable auto-drain for a queue (wt drain on|off <queue>)."""
    from . import config
    enabled = args.onoff == "on"
    if enabled:
        _warn_if_public_repo(args.queue, config)
    config.set_auto_drain(args.queue, enabled)
    # Claim-type restriction: set on `on`, cleared on `off` (off = no policy).
    types = (getattr(args, "type", None) or []) if enabled else []
    config.set_claim_types(args.queue, types)
    state = "on" if enabled else "off"
    restriction = (
        f"claiming only: {', '.join(types)}" if types else "claiming: all types"
    )
    print(f"{args.queue}: drain {state} — reconciler will {'spawn workers automatically' if enabled else 'leave this queue alone'} — {restriction}")
    if enabled:
        # Load the LaunchAgent if installed but not yet active.
        if _LAUNCHAGENT_PLIST.exists():
            rc = os.system(f"launchctl load '{_LAUNCHAGENT_PLIST}' 2>/dev/null")
            if rc == 0:
                print(f"LaunchAgent activated (survives reboots)")
        # Also start the service right now if daemon isn't running.
        daemon_live = False
        if DAEMON_PID_FILE.exists():
            try:
                pid = int(DAEMON_PID_FILE.read_text().strip())
                os.kill(pid, 0)
                daemon_live = True
            except (ValueError, ProcessLookupError, OSError):
                pass
        if not daemon_live:
            import subprocess
            log_path = Path.home() / ".watchtower" / "watcher.log"
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(log_path, "a") as log_f:
                subprocess.Popen(
                    [sys.executable, "-m", "watchtower.cli", "start", "--auto-spawn"],
                    stdin=subprocess.DEVNULL, stdout=log_f, stderr=log_f,
                    start_new_session=True,
                )
            print(f"service auto-started (log: {log_path})")
    return 0


def cmd_subscribe(args: argparse.Namespace) -> int:
    """Register a target to hear about every enqueue/claim/close/needs-input
    event on a queue (`wt subscribe <queue> <target>`), or list current
    subscribers when ``target`` is omitted.

    Delivery reuses the exact ``messages.send`` path a ticket's own
    ``submitter`` (see ``cmd_add``'s ``--submitter``) is notified through --
    see ``queue._notify_ticket_event``, which unions the two so a target
    that's both gets one send, not two."""
    from . import config
    queue = q._norm_project(args.queue)
    target = (args.target or "").strip()
    if not target:
        subs = config.subscribers(queue)
        if args.json:
            print(json.dumps(subs, indent=2))
        elif subs:
            print(f"{queue} subscribers:")
            for sub in subs:
                print(f"  {sub}")
        else:
            print(f"{queue} has no subscribers")
        return 0
    from . import messages
    try:
        messages.resolve_target(target)
    except ValueError as e:
        print(
            f"warning: {target!r} does not resolve yet ({e}) -- it will "
            "still be subscribed, but notifications sent before it's "
            "known/registered will fail",
            file=sys.stderr,
        )
    config.add_subscriber(queue, target)
    print(f"SUBSCRIBED: {target} -> {queue}")
    return 0


def cmd_unsubscribe(args: argparse.Namespace) -> int:
    """Remove a target's subscription to a queue (`wt unsubscribe <queue> <target>`)."""
    from . import config
    queue = q._norm_project(args.queue)
    target = (args.target or "").strip()
    if not target:
        print("error: target is required", file=sys.stderr)
        return 1
    config.remove_subscriber(queue, target)
    print(f"UNSUBSCRIBED: {target} -> {queue}")
    return 0


def cmd_config(args: argparse.Namespace) -> int:
    """Unified queue configuration — combines ``wt set`` + ``wt drain`` (WT-97).

    ``wt set`` and ``wt drain`` are kept as-is; this command is the single-stop
    alternative for the common case of configuring a queue from scratch."""
    from . import config
    if not _validate_queue_worker_settings(args, config):
        return 1
    changed = []
    # Delegate drain-related flags first so enabling auto-drain also auto-starts.
    auto_drain = getattr(args, "auto_drain", None)
    if auto_drain is not None:
        enabled = auto_drain == "on"
        if enabled:
            _warn_if_public_repo(args.queue, config)
        config.set_auto_drain(args.queue, enabled)
        types = (getattr(args, "type", None) or []) if enabled else []
        config.set_claim_types(args.queue, types)
        state = "on" if enabled else "off"
        changed.append(f"auto_drain={state}")
        if enabled:
            if _LAUNCHAGENT_PLIST.exists():
                rc = os.system(f"launchctl load '{_LAUNCHAGENT_PLIST}' 2>/dev/null")
                if rc == 0:
                    print("LaunchAgent activated (survives reboots)")
            daemon_live = False
            if DAEMON_PID_FILE.exists():
                try:
                    pid = int(DAEMON_PID_FILE.read_text().strip())
                    os.kill(pid, 0)
                    daemon_live = True
                except (ValueError, ProcessLookupError, OSError):
                    pass
            if not daemon_live:
                import subprocess
                log_path = Path.home() / ".watchtower" / "watcher.log"
                log_path.parent.mkdir(parents=True, exist_ok=True)
                with open(log_path, "a") as log_f:
                    subprocess.Popen(
                        [sys.executable, "-m", "watchtower.cli", "start", "--auto-spawn"],
                        stdin=subprocess.DEVNULL, stdout=log_f, stderr=log_f,
                        start_new_session=True,
                    )
                print(f"service auto-started (log: {log_path})")
    # Set-type flags (all optional).
    if getattr(args, "backend", None) is not None:
        try:
            config.set_backend(args.queue, args.backend)
        except ValueError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        changed.append(f"backend={config.backend(args.queue)}")
    if getattr(args, "github_repo", None) is not None:
        try:
            config.set_github_repo(args.queue, args.github_repo)
        except ValueError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        changed.append(f"github_repo={args.github_repo}")
    if getattr(args, "github_assignee", None) is not None:
        config.set_github_assignee(args.queue, args.github_assignee)
        changed.append(f"github_assignee={config.github_assignee(args.queue)}")
    if getattr(args, "workers_local_path", None) is not None:
        expanded_path = os.path.expanduser(args.workers_local_path)
        if not os.path.isdir(expanded_path):
            print(
                f"error: workers_local_path {args.workers_local_path!r} is not a directory",
                file=sys.stderr,
            )
            return 1
        config.set_repo_path(args.queue, expanded_path)
        changed.append(f"workers_local_path={expanded_path}")
    if getattr(args, "grace_s", None) is not None:
        try:
            config.set_grace_s(args.queue, args.grace_s)
        except ValueError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        changed.append(f"grace_s={config.grace_s(args.queue)}")
    if getattr(args, "product_gate", None) is not None:
        enabled = args.product_gate == "on"
        if enabled and config.backend(args.queue) == "github":
            print("warning: product_gate is not enforced on GitHub-backed "
                  "queues yet (v1 gates file-backed queues only)",
                  file=sys.stderr)
        config.set_product_gate(args.queue, enabled)
        changed.append(f"product_gate={'on' if enabled else 'off'}")
    if getattr(args, "engine", None) is not None:
        config.set_engine(args.queue, args.engine)
        changed.append(f"engine={args.engine}")
    if getattr(args, "model", None) is not None:
        config.set_model(args.queue, args.model)
        changed.append(f"model={str(args.model or '').strip() or '(engine default)'}")
    if getattr(args, "engine", None) is not None or getattr(args, "model", None) is not None:
        released = workers.release_workers(queue=args.queue, mismatched=True)
        if released:
            changed.append(
                "retiring=" + ",".join(
                    str(worker.get("worker_id") or "") for worker in released
                )
            )
    if getattr(args, "effort", None) is not None:
        config.set_effort(args.queue, args.effort)
        changed.append(f"effort={args.effort or '(engine default)'}")
    if getattr(args, "workers", None) is not None:
        try:
            config.set_desired_workers(args.queue, args.workers)
        except ValueError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        changed.append(f"workers={args.workers}")
    if not changed:
        cfg = config.get_queue_config(args.queue)
        # grace_s is shown even when unset: it silently gates auto-drain, so
        # "why did nothing pick up my new ticket for 3 minutes" has to be
        # answerable from the queue's own config output.
        cfg.setdefault("grace_s", config.grace_s(args.queue))
        print(f"{args.queue}: {cfg}")
    else:
        print(f"{args.queue}: {', '.join(changed)}")
    return 0


def _validate_queue_worker_settings(args: argparse.Namespace, config: Any) -> bool:
    """Reject incompatible model/effort combinations before queue mutation."""
    engine = getattr(args, "engine", None) or config.engine(args.queue)
    existing = config.get_queue_config(args.queue)
    model_arg = getattr(args, "model", None)
    model = existing.get("model", "") if model_arg is None else model_arg
    if not config.is_approved_model(engine, model):
        choices = ", ".join(config.approved_models(engine))
        print(
            f"error: {model!r} is not approved for {engine}; "
            f"run `wt models --engine {engine}` (approved: {choices})",
            file=sys.stderr,
        )
        return False

    effort_arg = getattr(args, "effort", None)
    effort = existing.get("effort", "") if effort_arg is None else effort_arg
    if config.is_approved_effort(engine, model, effort):
        return True
    choices = ", ".join(config.approved_efforts(engine, model))
    print(
        f"error: {model or f'{engine} default model'} does not support effort "
        f"{effort!r}; supported: {choices}",
        file=sys.stderr,
    )
    return False


def cmd_models(args: argparse.Namespace) -> int:
    """List model identifiers WatchTower intentionally supports per engine."""
    from . import config

    models = list(config.approved_models(args.engine))
    payload = {"engine": args.engine, "models": models}
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(f"{args.engine} approved models:")
        for model in models:
            print(f"  {model}")
    return 0


# NOTE: there is intentionally no user-facing `wt spawn-worker` command. Workers
# are a function of policy (per-queue auto_drain) + queue depth, spawned by the
# watcher/reconciler (`wt start`) via workers.spawn_workers(), not by hand. See
# docs/worker-lifecycle.md. The spawn primitive lives in workers.py.


def _post_webhook(url: str, payload: dict) -> None:
    """Best-effort async reply: POST JSON to a webhook when a queue drains
    (WT-FEATURES-19, the async half of spawn-and-reply; `wt wait` is the sync
    half). Never raises — a failed notify must not fail the wait."""
    import json as _json
    import urllib.request
    try:
        req = urllib.request.Request(
            url, data=_json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"}, method="POST",
        )
        urllib.request.urlopen(req, timeout=10).close()
        print(f"notified: {url}")
    except Exception as e:  # noqa: BLE001 - best-effort, report and move on
        print(f"notify failed ({url}): {e}", file=sys.stderr)


def cmd_wait(args: argparse.Namespace) -> int:
    """Block until the queue has 0 open items, then exit 0 (run --cmd if set)."""
    deadline = time.time() + args.timeout if args.timeout else None
    interval = max(1, args.interval)
    while True:
        rows = health.all_status(project=args.queue)
        row = rows[0] if rows else {"depth": 0, "stuck": False}
        depth = row.get("depth", 0)
        if depth == 0:
            print(f"DRAINED: {args.queue} has 0 open tickets")
            if getattr(args, "notify_webhook", ""):
                _post_webhook(args.notify_webhook, {
                    "event": "drained", "queue": args.queue, "open": 0,
                })
            if args.cmd:
                print(f"running: {args.cmd}")
                return os.system(args.cmd) >> 8
            return 0
        stuck = " STUCK" if row.get("stuck") else ""
        print(f"waiting: {args.queue} open={depth}{stuck} (re-check in {interval}s)")
        if deadline and time.time() >= deadline:
            print(f"TIMEOUT: {args.queue} still has {depth} open", file=sys.stderr)
            return 2
        time.sleep(interval)


def _maybe_self_update() -> None:
    """Best-effort ``git pull --ff-only`` on the source checkout, then re-exec.

    The failure this prevents: a daemon that runs for weeks never picks up
    reconciler fixes because nothing in the start path updates the code (a
    production VM ran 44 commits behind with a dead daemon for exactly this
    reason). CCC's run.sh already self-updates on every restart; this gives
    the watcher the same property no matter how it was launched (launchd,
    systemd, manual).

    Only fires when wt executes from a real git checkout (editable/source
    install); pipx/venv installs have no .git and are skipped — their update
    path is ``pipx upgrade``. A failed or non-fast-forward pull never blocks
    the daemon: it logs and runs the code it has. When the pull does move
    HEAD, the process re-execs itself immediately so the fresh code is what
    actually runs the loop (execvp preserves the pid, so pidfiles stay valid).

    Set WT_NO_SELF_UPDATE=1 to opt out (e.g. dev checkouts with uncommitted
    work you don't want pulled over).
    """
    if os.environ.get("WT_NO_SELF_UPDATE"):
        return
    try:
        pkg_dir = Path(__file__).resolve().parent
        top = subprocess.run(
            ["git", "-C", str(pkg_dir), "rev-parse", "--show-toplevel"],
            capture_output=True, text=True, timeout=10,
        )
        if top.returncode != 0:
            return  # not a git checkout (pipx/venv install) — nothing to pull
        repo = top.stdout.strip()

        def _head() -> str:
            r = subprocess.run(
                ["git", "-C", repo, "rev-parse", "HEAD"],
                capture_output=True, text=True, timeout=10,
            )
            return r.stdout.strip() if r.returncode == 0 else ""

        before = _head()
        pull = subprocess.run(
            ["git", "-C", repo, "pull", "--ff-only"],
            capture_output=True, text=True, timeout=60,
        )
        after = _head()
        try:
            q._log(
                "SELF_UPDATE",
                f"pull --ff-only rc={pull.returncode} head {before[:8]} -> {after[:8]}",
            )
        except Exception:
            pass
        if pull.returncode != 0 or not after or after == before:
            return
        print(
            f"[watchtower] self-update moved HEAD {before[:8]} -> {after[:8]}; re-exec",
            flush=True,
        )
        os.execvp(sys.executable, [sys.executable, "-m", "watchtower.cli"] + sys.argv[1:])
    except Exception as e:  # noqa: BLE001 - an update hiccup must never block the daemon
        print(f"[watchtower] self-update skipped: {e}", flush=True)


def _daemon_loop(args: argparse.Namespace) -> None:
    """Own the GitHub-list-cache poller thread's lifetime, then run the tick
    loop. A ``try/finally`` here (rather than inside the loop itself) is
    what lets ``_daemon_loop_ticks`` stay exactly the plain ``while True``
    it always was -- including a test driving it straight into an exception
    via a monkeypatched ``time.sleep`` still reliably signals the poller
    thread to stop instead of leaking it into every later test in the
    process (it is a ``daemon=True`` thread: nothing else ever joins it)."""
    import threading
    from . import github_backend

    gh_poller_stop = threading.Event()
    threading.Thread(
        target=github_backend.poll_list_caches_forever,
        args=(5.0,),
        kwargs={"stop_event": gh_poller_stop},
        daemon=True,
    ).start()
    try:
        _daemon_loop_ticks(args)
    finally:
        gh_poller_stop.set()


def _daemon_loop_ticks(args: argparse.Namespace) -> None:
    _maybe_self_update()  # pick up reconciler fixes on every (re)start; re-execs if HEAD moved
    interval = max(5, args.interval)
    dry_run = getattr(args, "dry_run", False)
    # Always host the HTTP server alongside the watcher.
    import threading

    from . import dashboard, queue as _q

    host = getattr(args, "host", "127.0.0.1")
    port = getattr(args, "port", 8787)
    # Only bind HTTP if the dashboard isn't already running on this port.
    dashboard_already_up = _pid_from_file(DASHBOARD_PID_FILE) is not None
    if not dashboard_already_up:
        httpd = dashboard.ThreadingHTTPServer((host, port), dashboard._Handler)
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        print(f"[watchtower] HTTP server on http://{host}:{port}", flush=True)
    else:
        print(f"[watchtower] dashboard already running; skipping HTTP bind", flush=True)
    # Log daemon start to activity log.
    try:
        auto_spawn_status = "auto-spawn on" if getattr(args, "auto_spawn", False) else "auto-spawn off"
        _q._log("DAEMON_START", f"(pid {os.getpid()}) {auto_spawn_status}")
    except Exception:
        pass
    while True:
        result = workers.reconcile_once(dry_run=dry_run)
        # Drain queued cross-agent messages each tick. Best-effort: a messaging
        # hiccup must never kill the reconcile loop.
        try:
            from . import messages
            messages.drain_outbox()
        except Exception as e:  # noqa: BLE001 - log and keep the loop alive
            print(f"[watchtower] drain_outbox failed: {e}", flush=True)
        # Receipt verification (WT-77): move pending receipts to
        # landed/advanced/lost against transcript ground truth.
        try:
            from . import receipts
            receipts.sweep()
        except Exception as e:  # noqa: BLE001 - log and keep the loop alive
            print(f"[watchtower] receipts sweep failed: {e}", flush=True)
        # Resume-child reaper (WT-82): SIGTERM wt-spawned resume children that
        # outlived a completed turn. Ledger-scoped — never touches pids wt did
        # not spawn (CCC keeps its own resume children alive on purpose).
        try:
            from . import messages as _msgs
            reap = _msgs.reap_resume_children()
            if reap.get("reaped"):
                print(
                    f"[watchtower] reaped {len(reap['reaped'])} stale "
                    "resume children",
                    flush=True,
                )
        except Exception as e:  # noqa: BLE001 - log and keep the loop alive
            print(f"[watchtower] resume reap failed: {e}", flush=True)
        # Log retention (WT-74): throttled internally to ~1/hour via a stamp
        # file; same never-kill-the-loop contract as the outbox drain.
        try:
            from . import logmaint
            pruned = logmaint.maybe_prune()
            if pruned and (pruned.get("pruned") or pruned.get("fifos_removed")):
                print(
                    f"[watchtower] logs pruned: {len(pruned['pruned'])} files, "
                    f"{pruned.get('freed_bytes', 0) / 1048576:.1f} MB freed",
                    flush=True,
                )
        except Exception as e:  # noqa: BLE001 - log and keep the loop alive
            print(f"[watchtower] logs prune failed: {e}", flush=True)
        # Group-chat nudge scheduler: same never-kill-the-loop contract as the
        # outbox drain above. deliver() wraps messages.send so chats.py never
        # touches transports directly; a chats.py bug must not take down
        # message draining or reconciliation.
        try:
            from . import chats

            def _chat_deliver(sid: str, text: str) -> bool:
                from . import messages as _messages
                return bool(_messages.send(sid, text).get("ok"))

            chats.nudge_tick(deliver=_chat_deliver)
        except Exception as e:  # noqa: BLE001 - log and keep the loop alive
            print(f"[watchtower] nudge_tick failed: {e}", flush=True)
        for rec in result.get("spawned", []):
            tag = " (dry-run)" if rec.get("dry_run") else ""
            eng = rec.get("engine", "claude")
            mdl = rec.get("model", "")
            engine_label = f"{eng}:{mdl}" if mdl else eng
            print(
                f"[watchtower] spawned worker {rec.get('worker_id','')} "
                f"for {rec.get('queue','')} [{engine_label}]{tag}",
                flush=True,
            )
        for rec in result.get("stopped", []):
            tag = " (dry-run)" if rec.get("dry_run") else ""
            print(
                f"[watchtower] requested stop for {rec.get('worker_id','')} "
                f"on {rec.get('queue','')}{tag}",
                flush=True,
            )
        for rec in result.get("reaped", []):
            print(
                f"[watchtower] {rec.get('action','')} released worker "
                f"{rec.get('worker_id','')} pid {rec.get('pid','')} on "
                f"{rec.get('queue','')} (released {rec.get('released_age_s','')}s ago)",
                flush=True,
            )
        swept = result.get("stop_signals_swept") or []
        if swept:
            print(
                f"[watchtower] swept {len(swept)} orphaned stop-signal "
                f"sentinel(s): {', '.join(swept)}",
                flush=True,
            )
        # Handle stuck-queue auto-spawn for queues not handled by reconcile_once
        # (queues with auto_drain=True that appeared stuck but had depth=0 at
        # reconcile time, or queues only known via health scan).
        if args.auto_spawn:
            from . import config
            rows = health.all_status(stuck_minutes=args.stuck_minutes)
            managed = set(config.all_queues().keys())
            for r in rows:
                if r["queue"] in managed:
                    continue  # already handled by reconcile_once
                if not r["stuck"]:
                    continue
                live = workers.live_worker_count(r["queue"])
                if live == 0 and config.auto_drain(r["queue"]):
                    print(
                        f"[watchtower] STUCK {r['queue']} open={r['depth']} "
                        f"no live workers -> auto-spawn",
                        flush=True,
                    )
                    # A queue can become configured between the registry
                    # snapshot above and this health scan. Re-enter the locked
                    # reconciler so the spawn receives the same SPAWN_PLAN,
                    # cause classification, and capacity cap as every other
                    # worker launch.
                    workers.reconcile_once(dry_run=dry_run)
        time.sleep(interval)


def cmd_start(args: argparse.Namespace) -> int:
    dry_run = getattr(args, "dry_run", False)
    # First-time auto-install: `wt start` is the normal user entry point, so a
    # user should never have to run a separate `wt install` first. If the
    # LaunchAgent has never been written, write it now (same plist + skill
    # sync as `wt install`) and fall through to the launchd-start branch
    # below, which always loads it -- this is an explicit `wt start`, so the
    # cmd_install "only load if some queue has auto_drain" gate does not
    # apply here. Guard: --foreground is what the plist itself execs (no user
    # session, must never recurse into installing itself); --dry-run must not
    # write anything either.
    if not args.foreground and not dry_run and not _LAUNCHAGENT_PLIST.exists():
        _write_launchagent_plist()
        print(f"first start: installed LaunchAgent {_LAUNCHAGENT_LABEL} (auto-starts on login)")
    # Prefer launchd supervision: if a plist exists, start THROUGH launchd so
    # there is exactly ONE supervised daemon (KeepAlive relaunches it on crash).
    # A manual background `wt start` would create a second, unsupervised daemon,
    # which is exactly the bug that made the live service unreliable. Guard: the
    # --foreground path is what the plist itself invokes, so it must run the loop
    # directly and NOT re-enter launchctl (that would recurse forever); likewise
    # --dry-run stays a pure in-process run.
    if not args.foreground and not dry_run and _LAUNCHAGENT_PLIST.exists():
        target = _launchd_domain_target()
        if _launchagent_loaded():
            # Already bootstrapped: (re)start the existing service in place.
            rc = os.system(f"launchctl kickstart -k '{target}' 2>/dev/null") >> 8
            action = "restarted"
        else:
            rc = os.system(
                f"launchctl bootstrap gui/{os.getuid()} '{_LAUNCHAGENT_PLIST}' 2>/dev/null"
            ) >> 8
            action = "started"
        if rc == 0:
            print(f"{action} LaunchAgent {_LAUNCHAGENT_LABEL} (launchd-supervised)")
            return 0
        print(f"warning: launchctl exited {rc}; falling back to manual start")
    if not dry_run and DAEMON_PID_FILE.exists():
        try:
            pid = int(DAEMON_PID_FILE.read_text().strip())
            os.kill(pid, 0)
            print(f"watcher already running (pid {pid})")
            return 0
        except (ValueError, ProcessLookupError, OSError):
            pass  # stale pidfile
    if args.foreground or dry_run:
        if not dry_run:
            DAEMON_PID_FILE.parent.mkdir(parents=True, exist_ok=True)
            DAEMON_PID_FILE.write_text(str(os.getpid()))
        try:
            _daemon_loop(args)
        except KeyboardInterrupt:
            print("\n[watchtower] interrupted, stopping", file=sys.stderr)
        finally:
            # Log daemon stop to activity log.
            if not dry_run:
                try:
                    from . import queue as _q
                    _q._log("DAEMON_STOP", f"(pid {os.getpid()})")
                except Exception:
                    pass
                DAEMON_PID_FILE.unlink(missing_ok=True)
        return 0
    # Re-exec ourselves in the background in foreground-mode. This is the only
    # supervision path on hosts without launchd, so use the same hardened PATH
    # as the LaunchAgent plist; otherwise the daemon may later fail to find
    # git/gh/claude/codex while spawning workers.
    import subprocess

    cmd = [
        sys.executable,
        "-m",
        "watchtower.cli",
        "start",
        "--foreground",
        "--interval",
        str(args.interval),
        "--stuck-minutes",
        str(args.stuck_minutes),
        "--engine",
        args.engine,
    ]
    if args.auto_spawn:
        cmd.append("--auto-spawn")
    cmd += ["--host", args.host, "--port", str(args.port)]
    env = dict(os.environ, PATH=_launchd_path())
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        env=env,
    )
    print(
        f"watcher started (pid {proc.pid}); auto-spawn={'on' if args.auto_spawn else 'off'}"
        f"; HTTP on http://{args.host}:{args.port}"
    )
    return 0


def cmd_stop(args: argparse.Namespace) -> int:
    # With KeepAlive=true, a raw SIGTERM to the pid is immediately undone by
    # launchd (it relaunches the daemon). So an INTENTIONAL stop of a launchd-
    # supervised daemon must tell launchd to stop-and-stay-stopped via `bootout`.
    # Only fall back to the pidfile+SIGTERM path for a manually-started daemon
    # (dev machines that never ran `wt install`).
    if _LAUNCHAGENT_PLIST.exists() and _launchagent_loaded():
        rc = _launchctl_bootout()
        if rc == 0:
            print(f"stopped LaunchAgent {_LAUNCHAGENT_LABEL} (launchd will not relaunch)")
            # Log stop to activity log before clearing pidfile.
            try:
                from . import queue as _q
                _q._log("DAEMON_STOP", "via launchctl bootout")
            except Exception:
                pass
            # The launchd-owned daemon owns the pidfile; clear it so a later
            # `wt start`/status doesn't see a stale pid.
            DAEMON_PID_FILE.unlink(missing_ok=True)
            return 0
        print(f"warning: launchctl bootout exited {rc}; falling back to signal")
    if not DAEMON_PID_FILE.exists():
        print("watcher not running")
        return 0
    try:
        pid = int(DAEMON_PID_FILE.read_text().strip())
    except ValueError:
        DAEMON_PID_FILE.unlink(missing_ok=True)
        print("removed stale pidfile")
        return 0
    try:
        os.kill(pid, signal.SIGTERM)
        print(f"stopped watcher (pid {pid})")
        # Log stop to activity log.
        try:
            from . import queue as _q
            _q._log("DAEMON_STOP", f"via SIGTERM (pid {pid})")
        except Exception:
            pass
    except ProcessLookupError:
        print("watcher process already gone")
    finally:
        DAEMON_PID_FILE.unlink(missing_ok=True)
    return 0



_LAUNCHAGENT_LABEL = "ai.watchtower.watcher"
_LAUNCHAGENT_PLIST = Path.home() / "Library" / "LaunchAgents" / f"{_LAUNCHAGENT_LABEL}.plist"


def _launchd_path() -> str:
    """Build a PATH the launchd-spawned daemon can actually use.

    launchd starts LaunchAgents with a minimal PATH (roughly /usr/bin:/bin:
    /usr/sbin:/sbin). The daemon shells out to gh/git/claude/codex (e.g. the
    GitHub backend runs `gh issue list`), so with the minimal PATH those tools
    are not found and the worker crashes. We capture the INSTALLING shell's real
    PATH (which already contains the user's tool locations) and additionally
    guarantee the usual Homebrew and user-local bins are present, then ensure the
    system dirs are on the tail. De-duped, order preserved."""
    prepend = [
        os.path.expanduser("~/.local/bin"),
        "/opt/homebrew/bin",
        "/usr/local/bin",
    ]
    current = [p for p in os.environ.get("PATH", "").split(os.pathsep) if p]
    system = ["/usr/bin", "/bin", "/usr/sbin", "/sbin"]
    ordered: List[str] = []
    seen = set()
    for p in prepend + current + system:
        if p and p not in seen:
            seen.add(p)
            ordered.append(p)
    return os.pathsep.join(ordered)


def _launchd_domain_target() -> str:
    """The modern launchctl service target: gui/<uid>/<label>."""
    return f"gui/{os.getuid()}/{_LAUNCHAGENT_LABEL}"


def _launchagent_loaded() -> bool:
    """Best-effort check for whether the LaunchAgent is bootstrapped.

    `launchctl print gui/<uid>/<label>` exits 0 when the service is known to the
    domain and nonzero otherwise. Never raises: if launchctl is absent we treat
    the agent as not loaded so callers fall back to the manual path."""
    rc = os.system(f"launchctl print '{_launchd_domain_target()}' >/dev/null 2>&1")
    return rc == 0


def _launchctl_bootout() -> int:
    """Stop-and-stay-stopped: remove the service from the gui domain.

    Returns the launchctl exit status (0 = success). Uses `bootout` rather than
    the deprecated `unload` so it composes with `bootstrap`/`kickstart`."""
    return os.system(
        f"launchctl bootout 'gui/{os.getuid()}/{_LAUNCHAGENT_LABEL}' 2>/dev/null"
    ) >> 8


def _write_launchagent_plist() -> None:
    """Write the LaunchAgent plist (creating/refreshing it) and sync the
    bundled skill into every installed agent harness. Shared by `wt install`
    and the first-run auto-install inside `wt start` (see cmd_start); callers
    decide whether/how to load the result into launchctl.

    The generated plist is HARDENED against three production failures we hit:
      1. ProgramArguments used a bare `wt` shim, but launchd's minimal PATH could
         not resolve it, so the spawn failed (exit 78) and launchd's copy of the
         daemon never ran. We now use `sys.executable -m watchtower.cli`, i.e. an
         absolute interpreter path that has watchtower installed, no shim needed.
      2. KeepAlive was false, so launchd never relaunched a dead or killed
         daemon. It is now true; launchd supervises and restarts on crash/kill.
      3. No PATH env, so once running the daemon could not find gh/git/claude/
         codex. We now inject a real PATH via EnvironmentVariables (see
         _launchd_path)."""
    # Robust invocation: an absolute interpreter path plus the module form means
    # there is no dependence on a `wt` shim being on launchd's minimal PATH.
    program_args = [sys.executable, "-m", "watchtower.cli",
                    "start", "--foreground", "--auto-spawn"]
    launchd_path = _launchd_path()
    plist = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{_LAUNCHAGENT_LABEL}</string>
  <key>ProgramArguments</key>
  <array>
    {''.join(f'<string>{a}</string>' + chr(10) + '    ' for a in program_args).rstrip()}
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>{launchd_path}</string>
  </dict>
  <key>StandardOutPath</key>
  <string>{Path.home()}/.watchtower/watcher.log</string>
  <key>StandardErrorPath</key>
  <string>{Path.home()}/.watchtower/watcher.log</string>
</dict>
</plist>
"""
    _LAUNCHAGENT_PLIST.parent.mkdir(parents=True, exist_ok=True)
    _LAUNCHAGENT_PLIST.write_text(plist)
    print(f"wrote {_LAUNCHAGENT_PLIST}")
    # Keep the bundled watchtower skill in sync with every installed agent
    # harness on every install/auto-install, independent of LaunchAgent
    # activation -- so re-running always refreshes it.
    from . import skills_sync
    for r in skills_sync.sync():
        print(skills_sync.format_result(r))


def cmd_install(args: argparse.Namespace) -> int:
    """Write a LaunchAgent plist so the WT service starts automatically on login.

    Writes the plist unconditionally (so it's ready), but only loads it into
    launchctl if at least one queue has auto-drain enabled — otherwise the
    service would start for no reason.

    Hidden alias: the normal user path is `wt start`, which auto-installs on
    first run (see cmd_start) and always loads, since starting the service is
    an explicit user action there. `wt install` stays registered for anyone
    who wants to install without starting, but it's no longer in the help
    listing (see COMMAND_SECTIONS)."""
    from . import config as _cfg
    _write_launchagent_plist()
    # Only activate if some queue has auto-drain on — no point starting the
    # service when there's nothing to drain.
    drain_queues = [q for q in (_cfg._load().keys()) if _cfg.auto_drain(q)]
    if not drain_queues:
        print("no queues have drain=on yet — plist written, will activate on first 'wt drain on <queue>'")
        return 0
    rc = os.system(f"launchctl load '{_LAUNCHAGENT_PLIST}'")
    if rc == 0:
        print(f"loaded: {_LAUNCHAGENT_LABEL} — service starts on every login")
        print(f"  drain-on queues: {', '.join(drain_queues)}")
    else:
        print(f"warning: launchctl load exited {rc} — plist written but not loaded")
    return 0


def cmd_uninstall(args: argparse.Namespace) -> int:
    """Remove the LaunchAgent so WT no longer starts on login."""
    if _LAUNCHAGENT_PLIST.exists():
        os.system(f"launchctl unload '{_LAUNCHAGENT_PLIST}'")
        _LAUNCHAGENT_PLIST.unlink(missing_ok=True)
        print(f"removed {_LAUNCHAGENT_PLIST} and unloaded from launchctl")
    else:
        print("not installed")
    from . import skills_sync
    for r in skills_sync.remove():
        print(skills_sync.format_result(r))
    return 0


def cmd_skills(args: argparse.Namespace) -> int:
    """Sync/check/remove the bundled `watchtower` skill in every installed
    agent harness's skills dir (~/.claude/skills, ~/.codex/skills, ...).
    Symlinked, not copied, so once synced it never goes stale; `wt install`
    also calls this on every run."""
    from . import skills_sync
    sub = getattr(args, "skills_command", None) or "sync"
    if sub == "remove":
        results = skills_sync.remove()
    else:
        results = skills_sync.sync(dry_run=(sub == "status"))
    for r in results:
        print(skills_sync.format_result(r))
    return 0


def cmd_snapshot(args: argparse.Namespace) -> int:
    """Manage session snapshots ("token-sitter") and detached timers.

    Verbs: arm, disarm, status, fire, timer-run, path, record, latest, consume, sessions.
    """
    from . import snapshot as snap

    cmd = getattr(args, "snapshot_command", None)
    if cmd == "arm":
        r = snap.arm(
            args.session,
            args.engine,
            args.cwd,
            idle_min=args.idle if args.idle is not None else snap.DEFAULT_IDLE_MIN,
            mode=args.mode,
        )
        if r.get("ok"):
            st = r["state"]
            print(
                f"armed ({st.get('mode', 'mdfile')}): fires after {st['idle_min']:g} "
                f"idle minutes; window closes at {snap.CACHE_TTL_MIN} "
                f"(timer pid {st['pid']})"
            )
            return 0
        print(r.get("error"), file=sys.stderr)
        return 1
    if cmd == "disarm":
        r = snap.disarm(args.session)
        print(
            "disarmed" if r.get("ok") else r.get("error"),
            file=sys.stdout if r.get("ok") else sys.stderr,
        )
        return 0 if r.get("ok") else 1
    if cmd == "status":
        rows = snap.status(args.session)
        for s_ in rows:
            print(
                f"{s_['session_id'][:8]}  {s_.get('engine','?'):7} "
                f"{s_.get('mode','mdfile'):7} "
                f"{s_.get('outcome','?'):18} idle_min={s_.get('idle_min','?')} "
                f"alive={s_.get('timer_alive')}"
            )
        if not rows:
            print("no snapshot timers")
        return 0
    if cmd == "fire":
        r = snap.fire(args.session)
        print(
            r if r.get("ok") else r.get("error"),
            file=sys.stdout if r.get("ok") else sys.stderr,
        )
        return 0 if r.get("ok") else 1
    if cmd == "timer-run":
        outcome = snap.run_timer(args.session_id)
        print(f"timer outcome: {outcome}")
        return 0
    if cmd == "path":
        print(snap.snapshot_path(args.session))
        return 0
    if cmd == "record":
        r = snap.record(args.session, args.cwd)
        print(
            r.get("path") if r.get("ok") else r.get("error"),
            file=sys.stdout if r.get("ok") else sys.stderr,
        )
        return 0 if r.get("ok") else 1
    if cmd == "latest":
        p = snap.find_latest(args.cwd)
        if p is None:
            print("no snapshot for this directory", file=sys.stderr)
            return 1
        print(p)
        return 0
    if cmd == "consume":
        from pathlib import Path as _P

        print(snap.consume(_P(args.path)))
        return 0
    if cmd == "sessions":
        rows = snap.list_sessions(args.cwd, limit=args.limit, exclude=args.exclude)
        if not rows:
            print("no sessions found for this directory")
            return 0
        now = time.time()
        for i, r_ in enumerate(rows, 1):
            print(
                f"{i}) {r_['session_id']}  {snap._age_str(now - r_['mtime'])}  "
                f"{r_['first_message']}"
            )
        return 0
    print(
        "usage: wt snapshot arm|disarm|status|fire|path|record|latest|consume|sessions ...",
        file=sys.stderr,
    )
    return 1


def _pid_from_file(path: Path) -> Optional[int]:
    """Return the live pid recorded in ``path``, or None (cleaning up stale)."""
    if not path.exists():
        return None
    try:
        pid = int(path.read_text().strip())
    except (ValueError, OSError):
        path.unlink(missing_ok=True)
        return None
    try:
        os.kill(pid, 0)
        return pid
    except (ProcessLookupError, OSError):
        path.unlink(missing_ok=True)
        return None


def _ensure_dashboard(host: str, port: int) -> int:
    """Start the dashboard server detached if not already running. Idempotent.

    Returns the pid of the (new or existing) background server.
    """
    existing = _pid_from_file(DASHBOARD_PID_FILE)
    if existing is not None:
        return existing
    import subprocess

    cmd = [
        sys.executable,
        "-m",
        "watchtower.cli",
        "dashboard",
        "--foreground",
        "--host",
        host,
        "--port",
        str(port),
    ]
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    DASHBOARD_PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    DASHBOARD_PID_FILE.write_text(str(proc.pid))
    return proc.pid


def cmd_dashboard(args: argparse.Namespace) -> int:
    from . import dashboard

    # --stop: kill the background dashboard via its pidfile.
    if getattr(args, "stop", False):
        pid = _pid_from_file(DASHBOARD_PID_FILE)
        if pid is None:
            print("dashboard not running")
            return 0
        try:
            os.kill(pid, signal.SIGTERM)
            print(f"stopped dashboard (pid {pid})")
        except ProcessLookupError:
            print("dashboard process already gone")
        finally:
            DASHBOARD_PID_FILE.unlink(missing_ok=True)
        return 0

    # --foreground (or --once): the old blocking server. Used for debugging and
    # as the body of the detached background process we spawn below.
    if getattr(args, "foreground", False) or args.once:
        DASHBOARD_PID_FILE.parent.mkdir(parents=True, exist_ok=True)
        if not args.once:
            DASHBOARD_PID_FILE.write_text(str(os.getpid()))
        try:
            return dashboard.serve(host=args.host, port=args.port, once=args.once)
        finally:
            if not args.once:
                DASHBOARD_PID_FILE.unlink(missing_ok=True)

    # Default: ensure the server runs in the background, open a browser, return.
    pid = _ensure_dashboard(args.host, args.port)
    url = f"http://{args.host}:{args.port}/"
    started = pid is not None
    print(f"WatchTower dashboard: {url} (pid {pid})")
    if args.no_open:
        print("  (browser not opened: --no-open)")
    else:
        import webbrowser

        if webbrowser.open(url):
            print("  opened in your browser")
        else:
            print("  open it in your browser")
    print("  wt dashboard --stop   to stop the background server")
    return 0 if started else 0


# --------------------------------------------------------------------------- help sections
# Single source of truth for the top-level command listing: (name, section,
# one-line help). Used to build the grouped --help epilog below; the
# individual add_parser() calls in build_parser() no longer pass help= so
# argparse doesn't also render its own flat {a,b,c,...} listing.
COMMAND_SECTIONS: List[Tuple[str, str]] = [
    ("Service", "start"),
    ("Service", "stop"),
    ("Service", "dashboard"),
    ("Service", "skills"),
    ("Service", "uninstall"),
    ("Queues", "status"),
    ("Queues", "models"),
    ("Queues", "config"),
    ("Queues", "set"),
    ("Queues", "drain"),
    ("Queues", "subscribe"),
    ("Queues", "unsubscribe"),
    ("Queues", "wait"),
    ("Queues", "monitor"),
    ("Queues", "workers"),
    ("Queues", "migrate-store"),
    ("Queues", "export-json"),
    ("Tickets", "add"),
    ("Tickets", "import"),
    ("Tickets", "take"),
    ("Tickets", "edit"),
    ("Tickets", "ready"),
    ("Tickets", "find"),
    ("Tickets", "ls"),
    ("Tickets", "blocked"),
    ("Tickets", "answer"),
    ("Tickets", "comment"),
    ("Tickets", "discuss"),
    ("Tickets", "dedup"),
    ("Agent messaging", "send"),
    ("Agent messaging", "ask"),
    ("Agent messaging", "critique"),
    ("Agent messaging", "spawn"),
    ("Agent messaging", "outbox"),
    ("Agent messaging", "agents"),
    ("Agent messaging", "chat"),
    ("Agent messaging", "snapshot"),
    ("Worker protocol", "claim"),
    ("Worker protocol", "release"),
    ("Worker protocol", "close"),
    ("Worker protocol", "unresolved-ack"),
    ("Worker protocol", "block"),
]
# `install` is intentionally absent: it's a hidden alias folded into `wt start`
# (see cmd_start's first-time auto-install), not a command users need to type.
# `agent` is intentionally absent too: it's a hidden compat alias folded into
# `wt agents` (register/set-name/rm now live there; see _add_agent_subcommands).

COMMAND_HELP: Dict[str, str] = {
    "add": "file a ticket",
    "import": "turn a document into queue tickets (default: dry-run)",
    "take": "file a ticket and immediately claim it (= add --claim)",
    "edit": "patch fields (title/priority/type/readiness/...) on an existing ticket",
    "claim": "claim next open ticket (smart sort: priority + type + age)",
    "release": "give up a claim without closing it; returns the ticket to open",
    "close": "close a ticket (record how you fixed it)",
    "unresolved-ack": "acknowledge a closed ticket's caveat/unresolved chips (no history rewrite)",
    "block": "park a ticket that needs a human decision",
    "blocked": "list tickets parked for a human",
    "answer": "answer a blocked ticket; auto-resumes its session",
    "comment": "append a ticket activity comment",
    "discuss": "attach to a blocked ticket's session (claude --resume)",
    "ready": "mark a ticket ready for workers (dispatch its queue); 'wt run' is an alias",
    "run": "mark an existing GitHub issue runnable and dispatch its queue (alias: wt ready)",
    "find": "look up one ticket by ref across all queues (no -q needed)",
    "ls": "list the tickets in one queue",
    "dedup": "close exact-duplicate open tickets",
    "migrate-store": "one-time JSON -> SQLite store migration (idempotent)",
    "export-json": "dump the store as classic {counter, items} JSON",
    "status": "per-queue depth / age / stuck flag",
    "models": "list WatchTower-approved model identifiers per engine",
    "config": "recommended queue configuration: settings plus auto-drain policy",
    "set": "compatibility alias for basic queue settings; prefer `wt config`",
    "drain": "enable or disable auto-drain for a queue",
    "subscribe": "register a target for every event on a queue (or list subscribers)",
    "unsubscribe": "remove a target's subscription to a queue",
    "wait": "block until the queue is drained",
    "monitor": "run a check; file a ticket if it fails",
    "workers": "list workers this CLI started",
    "agents": "address book: list reachable agents; register/set-name/rm to name them",
    "send": "push a message to a worker/agent/session",
    "ask": "ask a target and wait for its reply",
    "critique": "spawn 2 cross-family critique agents on a goal",
    "spawn": "spawn one ad-hoc one-shot agent on a goal (WT Spawn)",
    "outbox": "inspect and manage undelivered messages",
    "chat": "group chats: multi-agent conversations",
    "start": "start the service (installs the LaunchAgent on first run)",
    "stop": "stop service (watcher, reconciler, dashboard, HTTP API)",
    "uninstall": "remove LaunchAgent (stop auto-start on login)",
    "dashboard": "open the night-watch dashboard (background server + browser)",
    "skills": "sync the bundled skills into installed agent harnesses",
    "snapshot": "manage session auto-snapshots before prompt cache expiration",
}

# "Worker protocol" = the claim/close/block loop agent workers run; humans
# rarely type these (a human closing a ticket by hand still can). Ordered by
# user journey: get the service running, look at queue health, work tickets,
# talk to other agents, and only then the low-level worker protocol.
_SECTION_ORDER = [
    "Service", "Queues", "Tickets", "Agent messaging", "Worker protocol",
]


def _build_command_epilog() -> str:
    """Git-style grouped command listing for the top-level --help epilog."""
    name_width = max(len(name) for _, name in COMMAND_SECTIONS)
    lines = ["commands:"]
    for section in _SECTION_ORDER:
        lines.append(f"\n  {section}:")
        for sec, name in COMMAND_SECTIONS:
            if sec != section:
                continue
            helptext = COMMAND_HELP[name]
            lines.append(f"    {name:<{name_width}}  {helptext}")
    lines.append("\nRun 'wt <command> --help' for details on any command.")
    return "\n".join(lines)


def _add_agent_subcommands(parser: argparse.ArgumentParser) -> None:
    """Wire the register/set-name/rm management verbs onto `parser`.

    Shared by `wt agents` (the address-book command) and the hidden
    `wt agent` compat alias, so both expose the identical nested structure.
    Each leaf sets its own `func=cmd_agent`, overriding whatever the parent
    parser defaulted `func` to (e.g. `cmd_agents` for bare `wt agents`)."""
    asub = parser.add_subparsers(dest="agent_command")
    for alias in ("register", "set-name"):
        sa = asub.add_parser(
            alias,
            help="name a session UUID (re-registering a name repoints it)",
        )
        sa.add_argument("name", help="agent name (a leading @ is allowed)")
        sa.add_argument("--session", required=True, help="the session UUID")
        sa.add_argument("--engine", default="claude", help="engine (default claude)")
        sa.add_argument("--cwd", default="", help="working directory hint")
        sa.set_defaults(func=cmd_agent)
    sa = asub.add_parser("rm", help="remove a name from the registry")
    sa.add_argument("name")
    sa.set_defaults(func=cmd_agent)


def _install_provenance() -> str:
    """Describe where this ``wt`` actually loads code from.

    A ``pipx install`` (without ``--editable``) copies the repo into the
    venv's site-packages instead of pointing at it live — the CLI still
    runs fine, but every commit after that snapshot is invisible to it
    until someone happens to reinstall (see VM-NEXT-18: a Mac install went
    stale for two days with no symptom other than "the fix didn't take").
    Surfacing the source path and its git SHA in ``--version`` makes that
    staleness checkable instead of silent.
    """
    src = Path(__file__).resolve().parent
    try:
        result = subprocess.run(
            ["git", "-C", str(src), "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=2,
        )
        sha = result.stdout.strip() if result.returncode == 0 else ""
    except Exception:  # noqa: BLE001 - best-effort label; must never break the CLI
        # OSError / SubprocessError in the wild, but also anything a test
        # harness does to subprocess.Popen (a non-context-manager fake raises
        # TypeError from inside subprocess.run). --version is cosmetic.
        sha = ""
    if sha:
        return f"{src} @ {sha}"
    return f"{src} (no git history — likely a frozen pipx snapshot, not a live checkout)"


# --------------------------------------------------------------------------- main
class _WtArgumentParser(argparse.ArgumentParser):
    """argparse parser that keeps error output legible (WATCHTOWER-3).

    A rejected `wt comment -q <QUEUE> <REF> "<800-char text>"` used to echo the
    entire argument payload back through stderr as part of argparse's
    "unrecognized arguments: ..." message. Piped through `head`/`tail` -- normal
    when you expect one confirmation line -- the tail of your own comment reads
    exactly like a success confirmation, so a *failed* command looks *succeeded*
    and the write is silently lost.

    Fix: cap the error message length so a rejected multi-kilobyte value can
    never bury (or masquerade as) the actual diagnostic. Subparsers created by
    add_subparsers inherit this class automatically (parser_class defaults to
    type(self)), so a subcommand error also prints that subcommand's own usage.
    """

    _MAX_ERROR_LEN = 200

    def error(self, message: str):  # type: ignore[override]
        if len(message) > self._MAX_ERROR_LEN:
            elided = len(message) - self._MAX_ERROR_LEN
            message = (
                message[: self._MAX_ERROR_LEN]
                + f" …[+{elided} chars elided]"
            )
        super().error(message)


def _add_redundant_queue_flag(subparser: argparse.ArgumentParser) -> None:
    """Accept -q/--queue on ref-based commands and ignore it (WATCHTOWER-3).

    `wt add`/`wt claim` require -q, but ref-based commands (`comment`, `find`,
    `close`, ...) resolve a globally-unique ref and never needed it. Passing -q
    out of habit used to be rejected as an unrecognized argument; accepting and
    ignoring it removes the three-different-conventions footgun the ticket
    flagged. `edit` is the exception: there --queue *moves* the ticket, so it is
    a real flag, not a redundant one."""
    subparser.add_argument(
        "-q", "--queue", dest="_ignored_queue", default=None, metavar="QUEUE",
        help="accepted but ignored; a ref is globally unique so no queue is needed",
    )


class _VersionAction(argparse.Action):
    """``--version`` that resolves the install provenance lazily.

    ``_install_provenance`` forks ``git``; doing that eagerly at parser-build
    time charged every ``wt`` invocation for a label only ``--version``
    prints, and made any test that stubs ``subprocess.Popen`` see a stray
    git call. Compute it only when the flag is actually hit."""

    def __init__(self, option_strings, dest=argparse.SUPPRESS,
                 default=argparse.SUPPRESS, help="show version and exit"):
        super().__init__(option_strings=option_strings, dest=dest,
                         default=default, nargs=0, help=help)

    def __call__(self, parser, namespace, values, option_string=None):
        parser._print_message(
            f"wt {__version__}\nsource: {_install_provenance()}\n", sys.stdout
        )
        parser.exit()


def build_parser() -> argparse.ArgumentParser:
    p = _WtArgumentParser(
        prog="wt",
        usage="wt <command> [options]",
        description="WatchTower queue CLI",
        epilog=_build_command_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--version", action=_VersionAction)
    sub = p.add_subparsers(dest="command", metavar="<command>", help=argparse.SUPPRESS)

    s = sub.add_parser("status")
    s.add_argument("-q", "--queue", default=None)
    s.add_argument("--stuck-minutes", type=int, default=health.STUCK_MINUTES)
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_status)

    s = sub.add_parser("models")
    s.add_argument("--engine", required=True,
                   choices=["claude", "codex", "kimi", "antigravity"],
                   help="engine whose supported worker model identifiers to list")
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_models)

    s = sub.add_parser("ls")
    s.add_argument("-q", "--queue", required=True)
    s.add_argument(
        "--status",
        default="active",
        choices=["active", "open", "in_progress", "blocked", "closed",
                 "unresolved", "all"],
        help="which tickets to show (default: active = open + in_progress; "
             "blocked = parked for human input; unresolved = closed with "
             "unresolved items flagged in the resolution)",
    )
    s.add_argument("--limit", type=int, default=0, help="max rows (0 = all)")
    s.add_argument("--unresolved", action="store_true",
                   help="shorthand for --status unresolved")
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_ls)

    s = sub.add_parser("unresolved",
                       help="closed tickets whose resolution flagged unresolved work")
    s.add_argument("-q", "--queue", default="",
                   help="one queue (default: every queue)")
    s.add_argument("--limit", type=int, default=0, help="max rows (0 = all)")
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_unresolved)

    s = sub.add_parser("find")
    s.add_argument("ref", help="ticket ref (e.g. WT-48) or bare number")
    s.add_argument(
        "--worker", default="",
        help="your worker id: events you performed are marked claimed_by_you/"
             "closed_by_you/\"you\" so your own past actions never read as "
             "another worker's (also honors $WT_WORKER and harness session env)",
    )
    s.add_argument("--json", action="store_true")
    _add_redundant_queue_flag(s)
    s.set_defaults(func=cmd_find)

    # Shared arg registration so `add` and its `take` shorthand can't drift.
    def _add_common_ticket_args(subparser: argparse.ArgumentParser) -> None:
        subparser.add_argument("-q", "--queue", required=True)
        subparser.add_argument("--title", default="")
        subparser.add_argument("--note", default="")
        subparser.add_argument("--text", default="")
        subparser.add_argument("--url", default="")
        subparser.add_argument("--lane", default="normal", choices=list(q.VALID_LANES))
        subparser.add_argument("--type", default="", choices=["bug", "feature", ""],
                               help="item type: bug or feature")
        subparser.add_argument("--readiness", default="",
                               choices=["ready", "needs-shaping", "needs-spec", ""],
                               help="readiness level")
        subparser.add_argument("--priority", default="",
                               choices=["p0", "p1", "p2", "p3", "p4", ""],
                               help="priority: p0 (highest) through p4 (lowest)")
        subparser.add_argument("--value", default="", choices=["H", "M", "L", ""],
                               help="business value: H, M, or L")
        subparser.add_argument("--confidence", default="", choices=["H", "M", "L", ""],
                               help="confidence: H, M, or L")
        subparser.add_argument("--model-floor", default="", dest="model_floor",
                               choices=list(q.VALID_MODEL_FLOORS),
                               help="filer's best-guess minimum model this ticket "
                                    "needs (FEAT-NEXT-120); empty is fine, never a "
                                    "blocker at filing time")
        subparser.add_argument("--worker", default="",
                               help="worker/owner id to claim under when --claim is "
                                    "set; defaults to wt-cli-<shell> (stable per "
                                    "terminal so a later bare close composes)")
        subparser.add_argument(
            "--submitter", default="",
            help="addressable target (worker id / @agent name / session UUID) "
                 "to notify when this ticket is claimed/closed/needs input; "
                 "defaults to your own session ($CLAUDE_CODE_SESSION_ID / "
                 "$CODEX_THREAD_ID) like --report-to does. Omit entirely (pass "
                 "no flag and run with neither env var set) to file with no "
                 "submitter -- notifications are then skipped silently.",
        )

    s = sub.add_parser("add")
    _add_common_ticket_args(s)
    s.add_argument("--claim", action="store_true",
                   help="immediately claim the new ticket (mark in_progress) so no "
                        "auto-drain worker picks it up; use when you're already working it")
    s.set_defaults(func=cmd_add)

    s = sub.add_parser(
        "import",
        description=(
            "Use one Claude reasoning call over the whole document to infer a "
            "validated ticket graph. Preview is the default; --apply files it."
        ),
    )
    s.add_argument("file", help="Markdown or text document to extract tasks from")
    s.add_argument("-q", "--queue", required=True)
    s.add_argument(
        "--apply",
        action="store_true",
        help="file new tickets (default: dry-run)",
    )
    s.add_argument(
        "--type",
        default="",
        choices=["bug", "feature"],
        help="override the inferred type for every newly filed ticket",
    )
    s.set_defaults(func=cmd_import)

    s = sub.add_parser("take")
    _add_common_ticket_args(s)
    s.set_defaults(func=cmd_take)

    s = sub.add_parser("edit")
    s.add_argument("ref", help="ticket ref (e.g. WT-48) or bare number")
    s.add_argument("--title", default=None)
    s.add_argument("--note", default=None)
    s.add_argument("--text", default=None)
    s.add_argument("--url", default=None)
    s.add_argument("--type", default=None, choices=["bug", "feature"],
                   help="item type: bug or feature")
    s.add_argument("--readiness", default=None,
                   choices=["ready", "needs-shaping", "needs-spec"],
                   help="readiness level")
    s.add_argument("--priority", default=None,
                   choices=["p0", "p1", "p2", "p3", "p4"],
                   help="priority: p0 (highest) through p4 (lowest)")
    s.add_argument("--value", default=None, choices=["H", "M", "L"],
                   help="business value: H, M, or L")
    s.add_argument("--confidence", default=None, choices=["H", "M", "L"],
                   help="confidence: H, M, or L")
    s.add_argument("--model-floor", default=None, dest="model_floor",
                   choices=[m for m in q.VALID_MODEL_FLOORS if m],
                   help="filer's best-guess minimum model this ticket needs "
                        "(FEAT-NEXT-120)")
    s.add_argument("--selector", default=None)
    s.add_argument("--screenshot-path", default=None, dest="screenshot_path")
    s.add_argument("--repo-path", default=None, dest="repo_path")
    s.add_argument("--queue", default=None,
                   help="move the ticket to a different queue in place, "
                        "reassigning its ref (WT-83); file-backed queues only")
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_edit)

    s = sub.add_parser("claim")
    s.add_argument("-q", "--queue", required=True)
    s.add_argument("ref", nargs="?", default="",
                   help="claim a specific ticket by ref (e.g. CCC-42); omit to claim next")
    s.add_argument("--worker", default="")
    s.add_argument("--oldest", action="store_true",
                   help="FIFO: claim oldest ticket regardless of priority")
    s.add_argument("--type", action="append", default=None,
                   choices=["bug", "feature"],
                   help="only claim this type (repeatable: --type bug --type feature)")
    s.add_argument("--readiness", action="append", default=None,
                   choices=["ready", "needs-shaping", "needs-spec"],
                   help="only claim items with this readiness (repeatable)")
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_claim)

    def _add_ready_args(p: argparse.ArgumentParser) -> None:
        p.add_argument("ref", help="ticket ref / GitHub issue ref, e.g. BYM-GH-FINIE-402")
        p.add_argument("--no-dispatch", action="store_true",
                       help="only record the run request; do not nudge/spawn workers")
        p.add_argument("--cancel", action="store_true",
                       help="withdraw a run request that has not started yet")

    s = sub.add_parser("ready", help="mark an existing ticket ready for workers")
    _add_ready_args(s)
    s.set_defaults(func=cmd_ready)

    s = sub.add_parser("run")  # backward-compat alias for 'wt ready'
    _add_ready_args(s)
    s.set_defaults(func=cmd_run)

    s = sub.add_parser("close")
    s.add_argument("ref")
    s.add_argument("--worker", default="")
    s.add_argument("--summary", default="",
                   help="one-line description of what you changed")
    proof = s.add_mutually_exclusive_group()
    proof.add_argument("--commit", default="", metavar="SHA",
                       help="verified commit SHA containing code changes")
    proof.add_argument("--no-code", action="store_true",
                       help="explicitly declare that this ticket changed no code")
    s.add_argument("--caveat", action="append",
                   help="something to watch out for (repeatable)")
    s.add_argument("--follow-up", action="append", dest="follow_up",
                   help="a notable follow-up task (repeatable)")
    s.add_argument("--unresolved", action="append",
                   help="something you could not fix (repeatable)")
    s.add_argument("--enqueue-follow-ups", action="store_true",
                   dest="enqueue_follow_ups",
                   help="also file each follow-up/unresolved as a new open ticket")
    s.add_argument("--force", action="store_true",
                   help="close even if the ticket is already closed or claimed by "
                        "another worker (bypasses the reap-induced duplicate-close guard)")
    _add_redundant_queue_flag(s)
    s.set_defaults(func=cmd_close)

    s = sub.add_parser("unresolved-ack", help=COMMAND_HELP.get("unresolved-ack", ""))
    s.add_argument("ref", nargs="?", default="",
                   help="ticket ref; omit it (with -q and --all) to bulk-ack a queue")
    s.add_argument("--all", action="store_true",
                   help="acknowledge every caveat/follow-up/unresolved item")
    s.add_argument("--caveat", action="append", type=int, metavar="N",
                   help="acknowledge caveat N (1-based, repeatable)")
    s.add_argument("--follow-up", action="append", type=int, dest="follow_up",
                   metavar="N", help="acknowledge follow-up N (1-based, repeatable)")
    s.add_argument("--unresolved", action="append", type=int, metavar="N",
                   help="acknowledge unresolved item N (1-based, repeatable)")
    s.add_argument("--undo", action="store_true",
                   help="remove the acknowledgement instead of adding it")
    s.add_argument("--by", default="", help="who is acknowledging (default: your worker id)")
    s.add_argument("--json", action="store_true", help="print the updated ticket as JSON")
    s.add_argument("--matching", default="", metavar="TEXT",
                   help="bulk mode: only tickets whose resolution text contains "
                        "TEXT (case-insensitive substring, e.g. not-applicable)")
    s.add_argument("--dry-run", action="store_true", dest="dry_run",
                   help="bulk mode: list what would be acked, change nothing")
    # Unlike other ref-based commands, -q is NOT purely decorative here: with
    # no ref it selects the queue to bulk-ack. It stays ignored in ref mode.
    s.add_argument("-q", "--queue", dest="_ignored_queue", default=None,
                   metavar="QUEUE",
                   help="with a ref: accepted but ignored (refs are globally "
                        "unique). With no ref: the queue to bulk-ack.")
    s.set_defaults(func=cmd_unresolved_ack)

    s = sub.add_parser("release")
    s.add_argument("ref")
    s.add_argument("--worker", default="", help="your session/worker id")
    s.add_argument("--force", action="store_true",
                   help="release even if the ticket is blocked (needs_input) -- "
                        "normally refused because it erases the open question; "
                        "prefer `wt answer` to resolve a block")
    _add_redundant_queue_flag(s)
    s.set_defaults(func=cmd_release)

    s = sub.add_parser("block")
    s.add_argument("ref")
    s.add_argument("--worker", default="", help="your session/worker id")
    s.add_argument("--question", default="", help="the specific decision you need")
    s.add_argument("--progress", default="",
                   help="analysis-so-far note (backstop if the session is lost)")
    s.add_argument("--json", action="store_true")
    _add_redundant_queue_flag(s)
    s.set_defaults(func=cmd_block)

    s = sub.add_parser("blocked")
    s.add_argument("-q", "--queue", default=None)
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_blocked)

    s = sub.add_parser("answer")
    s.add_argument("ref")
    s.add_argument("text", help="your answer")
    s.add_argument("--worker", default="")
    s.add_argument("--engine", choices=["claude", "codex", "kimi"],
                   help="override the blocked session engine")
    _add_redundant_queue_flag(s)
    s.set_defaults(func=cmd_answer)

    s = sub.add_parser("comment")
    s.add_argument("ref")
    s.add_argument("text", help="comment text")
    s.add_argument("--worker", default="")
    s.add_argument("--by", default="human", choices=["human", "worker", "system"])
    _add_redundant_queue_flag(s)
    s.set_defaults(func=cmd_comment)

    s = sub.add_parser("discuss")
    s.add_argument("ref")
    s.add_argument("--engine", default="claude", choices=["claude", "codex", "kimi"])
    s.add_argument("--print", action="store_true", dest="print",
                   help="print the resume command instead of running it")
    _add_redundant_queue_flag(s)
    s.set_defaults(func=cmd_discuss)

    s = sub.add_parser("workers")
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_workers)
    workers_sub = s.add_subparsers(dest="workers_command")
    release_workers_parser = workers_sub.add_parser(
        "release", help="gracefully stop selected workers before their next claim"
    )
    release_workers_parser.add_argument(
        "--engine", required=True, choices=["claude", "codex", "kimi"],
        help="release live workers running this engine",
    )
    release_workers_parser.add_argument(
        "-q", "--queue", default="", help="limit release to one queue"
    )
    release_workers_parser.add_argument("--json", action="store_true")
    release_workers_parser.set_defaults(func=cmd_workers_release)

    s = sub.add_parser("session-names", help=argparse.SUPPRESS)
    s.set_defaults(func=cmd_session_names)
    snub = s.add_subparsers(dest="session_names_command")
    b = snub.add_parser("backfill")
    b.add_argument("--hours", type=float, default=24.0)
    b.add_argument("--dry-run", action="store_true")
    b.set_defaults(func=cmd_session_names)

    s = sub.add_parser("send")
    s.add_argument("target", help="worker id, @agent name, or session UUID/prefix")
    s.add_argument("text", help="the message, or '-' to read it from stdin "
                                "(quote-safe for long/multi-line bodies)")
    s.add_argument("--mode", default="send", choices=["send", "steer"],
                   help="delivery mode hint (delegate transports honor steer)")
    s.add_argument("--no-queue", action="store_true", dest="no_queue",
                   help="fail immediately instead of parking in the outbox")
    s.add_argument("--ttl", type=float, default=None,
                   help="seconds before a queued outbox message expires")
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_send)

    s = sub.add_parser("ask")
    s.add_argument("target", help="worker id, @agent name, or session UUID/prefix")
    s.add_argument("text", help="the question")
    s.add_argument("--timeout", type=float, default=30.0,
                   help="seconds to wait for the reply (default 30)")
    s.add_argument("--json", action="store_true")
    s.add_argument("--notify-webhook", default="", dest="notify_webhook",
                   help="don't block: POST the answer to this URL when it "
                        "arrives (mirrors wt wait --notify-webhook)")
    s.add_argument("--_notify-child", action="store_true",
                   dest="_notify_child", help=argparse.SUPPRESS)
    s.set_defaults(func=cmd_ask)

    s = sub.add_parser("critique")
    s.add_argument("goal", help="what to critique / the context to critique it against")
    s.add_argument("--family", default="", choices=list(_CRITIQUE_FAMILIES),
                   help="your own agent family -- excluded from the 2 picked "
                        "to critique (default: auto-detected from harness "
                        "env, else claude)")
    s.add_argument("--engine1", "--model1", default="", dest="model1",
                   help="override the first critique engine "
                        "(--model1 is a deprecated alias)")
    s.add_argument("--engine2", "--model2", default="", dest="model2",
                   help="override the second critique engine "
                        "(--model2 is a deprecated alias)")
    s.add_argument("--report-to", default="", dest="report_to",
                   help="worker id, @agent, or session UUID the critique agents "
                        "report back to via `wt send` (default: auto-detected "
                        "from $CLAUDE_CODE_SESSION_ID / $CODEX_THREAD_ID)")
    s.add_argument("--cwd", default="", help="repo/dir the critique agents start in")
    s.add_argument("--dry-run", action="store_true", dest="dry_run",
                   help="build the spawn commands without launching anything")
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_critique)

    s = sub.add_parser("spawn")
    s.add_argument("goal", help="the task for the ad-hoc agent")
    s.add_argument("--engine", default="claude",
                   help="claude (default) | codex | antigravity")
    s.add_argument("--model", default="",
                   help="model override, passed through to the engine CLI")
    s.add_argument("--repo", default="", help="repo/dir the agent works in "
                                              "(default: cwd)")
    s.add_argument("--name", default="", help="label for the worker id/log")
    s.add_argument("--report-to", default="", dest="report_to",
                   help="worker id, @agent, or session UUID to report back to "
                        "via `wt send` when done")
    s.add_argument("--dry-run", action="store_true", dest="dry_run",
                   help="build the spawn command without launching")
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_spawn)

    s = sub.add_parser("outbox")
    s.set_defaults(func=cmd_outbox, outbox_command=None)
    osub = s.add_subparsers(dest="outbox_command")

    so = osub.add_parser("ls", help="list pending outbox messages")
    so.add_argument("--json", action="store_true")
    so.add_argument("--all", action="store_true",
                    help="include delivered and dead messages")
    so.set_defaults(func=cmd_outbox)

    so = osub.add_parser("retry", help="retry one message or all dead messages")
    so.add_argument("id", nargs="?", help="outbox message id")
    so.add_argument("--all-dead", action="store_true",
                    help="retry every dead message")
    so.set_defaults(func=cmd_outbox)

    so = osub.add_parser("rm", help="remove an outbox message")
    so.add_argument("id", help="outbox message id")
    so.set_defaults(func=cmd_outbox)

    s = sub.add_parser("receipts")
    s.add_argument("--json", action="store_true")
    s.add_argument("--status", choices=["pending", "landed", "advanced", "lost"])
    s.set_defaults(func=cmd_receipts, receipts_command=None)
    rsub = s.add_subparsers(dest="receipts_command")
    sr = rsub.add_parser("get", help="show one receipt (verifies it first)")
    sr.add_argument("id")
    sr.set_defaults(func=cmd_receipts)
    sr = rsub.add_parser("stats", help="soak-gate delivery counts")
    sr.add_argument("--window-days", default=7.0, type=float)
    sr.add_argument("--json", action="store_true")
    sr.set_defaults(func=cmd_receipts)

    s = sub.add_parser("logs")
    s.set_defaults(func=cmd_logs, logs_command=None)
    lsub = s.add_subparsers(dest="logs_command")
    sl = lsub.add_parser(
        "prune", help="apply the log retention policy to ~/.watchtower/logs"
    )
    sl.add_argument("--dry-run", action="store_true", dest="dry_run")
    sl.add_argument("--json", action="store_true")
    sl.set_defaults(func=cmd_logs)

    s = sub.add_parser("gh")
    s.set_defaults(func=cmd_gh, gh_command=None)
    ghsub = s.add_subparsers(dest="gh_command")
    sg = ghsub.add_parser(
        "recheck", help="force a live GitHub connectivity check now, bypassing backoff"
    )
    sg.add_argument("--json", action="store_true")
    sg.set_defaults(func=cmd_gh)

    # `wt agents` is the single address-book command (git-remote pattern):
    # bare `wt agents [--json]` lists; `register`/`set-name`/`rm` are nested
    # management verbs. `wt agent ...` stays wired below as a hidden compat
    # alias with the identical nested structure via _add_agent_subcommands.
    s = sub.add_parser("agents")
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_agents, agent_command=None)
    _add_agent_subcommands(s)

    s = sub.add_parser("agent")  # hidden alias, not in COMMAND_SECTIONS
    s.set_defaults(func=cmd_agent, agent_command=None)
    _add_agent_subcommands(s)

    s = sub.add_parser("chat")
    s.set_defaults(func=cmd_chat, chat_command=None)
    csub = s.add_subparsers(dest="chat_command")

    sc = csub.add_parser("new", help="create a chat and check in with participants")
    sc.add_argument("topic")
    sc.add_argument("--with", dest="with_targets", required=True,
                    help="comma-separated targets (worker id, @agent, session UUID/prefix)")
    sc.add_argument("--include-human", action="store_true", dest="include_human",
                    help="list a human participant in the header/participants list")
    sc.add_argument("--json", action="store_true")

    sc = csub.add_parser("post", help="post a message to a chat")
    sc.add_argument("ref", help="chat path, filename, slug prefix, or sidecar uuid prefix")
    sc.add_argument("message")
    sc.add_argument("--as", dest="as_target", default="",
                    help="post as this participant (name or sid8); default Human")

    sc = csub.add_parser("read", help="print a chat transcript")
    sc.add_argument("ref")
    sc.add_argument("--tail", type=int, default=0, help="only the last N messages")
    sc.add_argument("--json", action="store_true")

    sc = csub.add_parser("ls", help="list chats")
    sc.add_argument("--archived", action="store_true", help="include archived chats")
    sc.add_argument("--json", action="store_true")

    sc = csub.add_parser("nudge", help="manually nudge a chat's targets")
    sc.add_argument("ref")
    sc.add_argument("--target", default="",
                    help="nudge only this participant (name or sid8); default: "
                         "the same deterministic targeting the daemon uses")

    sc = csub.add_parser("add", help="add a participant to a chat")
    sc.add_argument("ref")
    sc.add_argument("target", help="worker id, @agent, or session UUID/prefix")

    sc = csub.add_parser("leave", help="remove a participant from a chat")
    sc.add_argument("ref")
    sc.add_argument("target", help="existing participant (name or sid8)")

    sc = csub.add_parser("archive", help="archive a chat")
    sc.add_argument("ref")

    sc = csub.add_parser("close", help="close a chat")
    sc.add_argument("ref")

    sc = csub.add_parser("set", help="get/set per-chat nudge-policy knobs")
    sc.add_argument("ref")
    sc.add_argument("--nudge-interval-s", type=int, default=None, dest="nudge_interval_s",
                    help="seconds between auto-nudges for this chat")
    sc.add_argument("--idle-close-s", type=int, default=None, dest="idle_close_s",
                    help="seconds of inactivity before this chat auto-closes")
    sc.add_argument("--max-auto-nudges-per-hour", type=int, default=None,
                    dest="max_auto_nudges_per_hour",
                    help="per-chat auto-nudge rate cap")
    sc.add_argument("--json", action="store_true")

    s = sub.add_parser(
        "set",
        help="compatibility alias for basic queue settings; prefer `wt config`",
        description=(
            "Compatibility alias for the basic queue settings that predate "
            "`wt config`. Prefer `wt config` for new scripts and interactive "
            "use: it includes auto-drain policy and uses the current flag names."
        ),
    )
    s.add_argument("-q", "--queue", required=True)
    s.add_argument("--backend", default=None, choices=["file", "github"],
                   help="queue backing store: file (default) or github")
    s.add_argument("--github-repo", default=None, dest="github_repo",
                   help="GitHub repo for --backend github, as OWNER/REPO")
    s.add_argument("--github-assignee", default=None, dest="github_assignee",
                   help="assignee used by GitHub-backed claims (default: @me)")
    s.add_argument("--repo-path", default=None, dest="repo_path",
                   help="default cwd for workers spawned on this queue")
    s.add_argument("--engine", default=None, choices=["claude", "codex", "kimi"],
                   help=(
                       "agent engine for workers on this queue (default: claude). "
                       "claude: stream-json mode over a FIFO stdin — live, pushable, "
                       "prompt-cache warm for ~5 min; requires the Claude Code CLI. "
                       "codex: one-shot `codex exec <goal>` — no FIFO, no live push; "
                       "requires the OpenAI Codex CLI. "
                       "kimi: one-shot `kimi -p <goal>` — no FIFO, no live push; "
                       "auto-approves internally in print mode; requires the "
                       "Kimi Code CLI."
                   ))
    s.add_argument("--model", default=None, dest="model",
                   help=(
                       "model workers on this queue are spawned with (passed as the "
                       "engine's --model flag), e.g. claude-sonnet-5 or gpt-5.5. "
                       "Empty string clears it (workers use the CLI's own default)."
                   ))
    s.add_argument("--effort", default=None,
                   choices=["low", "medium", "high", "xhigh", "max", ""],
                   help=(
                       "reasoning effort for queue workers; omit for the engine "
                       "default. Empty string clears an override, like --model."
                   ))
    s.add_argument("--desired-workers", default=None, type=int, dest="desired_workers",
                   help="number of concurrent workers the reconciler should maintain")
    s.set_defaults(func=cmd_set)

    s = sub.add_parser("drain")
    s.add_argument("onoff", choices=["on", "off"], help="on = auto-spawn workers; off = backlog mode")
    s.add_argument("queue", metavar="QUEUE", help="queue name (e.g. CCC, WT)")
    s.add_argument("--type", action="append", default=None, choices=["bug", "feature"],
                   help="restrict auto-drain workers to these ticket types (repeatable); omit to clear")
    s.set_defaults(func=cmd_drain)

    s = sub.add_parser(
        "subscribe",
        help="register a target for every event on a queue",
        description=(
            "Register a target (worker id / @agent name / session UUID) to "
            "hear about every enqueue/claim/close/needs-input event on a "
            "queue -- not just tickets it filed itself. Omit the target to "
            "list current subscribers."
        ),
    )
    s.add_argument("queue", metavar="QUEUE", help="queue name (e.g. CCC, WT)")
    s.add_argument("target", nargs="?", default="",
                   help="worker id / @agent name / session UUID; omit to list")
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_subscribe)

    s = sub.add_parser(
        "unsubscribe",
        help="remove a target's subscription to a queue",
    )
    s.add_argument("queue", metavar="QUEUE", help="queue name (e.g. CCC, WT)")
    s.add_argument("target", help="target previously passed to `wt subscribe`")
    s.set_defaults(func=cmd_unsubscribe)

    s = sub.add_parser(
        "config",
        help="recommended queue configuration command (includes auto-drain)",
        description=(
            "The recommended queue configuration command. It combines the "
            "settings available through legacy `wt set` with auto-drain policy "
            "from `wt drain`."
        ),
    )
    s.add_argument("-q", "--queue", required=True)
    s.add_argument("--auto-drain", default=None, choices=["on", "off"],
                   dest="auto_drain",
                   help="on = auto-spawn workers; off = backlog mode")
    s.add_argument("--workers", default=None, type=int,
                   help="number of concurrent workers the reconciler should maintain")
    s.add_argument("--workers-local-path", default=None, dest="workers_local_path",
                   help="local repo path workers operate in (cwd for spawned workers)")
    s.add_argument("--backend", default=None, choices=["file", "github"],
                   help="queue backing store: file (default) or github")
    s.add_argument("--github-repo", default=None, dest="github_repo",
                   help="GitHub repo for --backend github, as OWNER/REPO")
    s.add_argument("--github-assignee", default=None, dest="github_assignee",
                   help="assignee used by GitHub-backed claims (default: @me)")
    s.add_argument("--engine", default=None, choices=["claude", "codex", "kimi"],
                   help="agent engine for workers on this queue")
    s.add_argument("--model", default=None,
                   help="model workers are spawned with (e.g. claude-sonnet-5)")
    s.add_argument("--effort", default=None,
                   choices=["low", "medium", "high", "xhigh", "max", ""],
                   help=(
                       "reasoning effort for queue workers; omit for the engine "
                       "default. Empty string clears an override, like --model."
                   ))
    s.add_argument("--type", action="append", default=None, choices=["bug", "feature"],
                   help="restrict auto-drain to these ticket types (requires --auto-drain on)")
    s.add_argument("--grace-s", default=None, type=int, dest="grace_s",
                   help=(
                       "seconds a new ticket is left alone before auto-drain may "
                       "claim it (default 180); 0 drains "
                       "immediately. Gives a human time to label a ticket "
                       "watchtower:no-auto-drain; pressing run ignores it."
                   ))
    s.add_argument("--product-gate", default=None, choices=["on", "off"],
                   dest="product_gate",
                   help="on = workers must get a human Ack (wt ack) after a "
                        "minimal-diagnosis pitch before implementing")
    s.set_defaults(func=cmd_config)

    s = sub.add_parser("monitor")
    s.add_argument("-q", "--queue", required=True)
    s.add_argument("--cmd", required=True, help="shell command; non-zero exit = fail")
    s.add_argument("--title", default="", help="ticket title on failure")
    s.add_argument("--note", default="", help="ticket note on failure")
    s.set_defaults(func=cmd_monitor)

    s = sub.add_parser("dedup")
    s.add_argument("-q", "--queue", default=None)
    s.add_argument("--apply", action="store_true", help="close dupes (default: dry-run)")
    s.set_defaults(func=cmd_dedup)

    s = sub.add_parser("migrate-store")
    s.set_defaults(func=cmd_migrate_store)

    s = sub.add_parser("export-json")
    s.add_argument("-o", "--out", default="", help="write to this path (default: stdout)")
    s.set_defaults(func=cmd_export_json)

    # No `wt spawn-worker`: workers are spawned by the watcher (`wt start`) from
    # per-queue auto_drain policy + depth, not by hand. See workers.spawn_workers.

    s = sub.add_parser("wait")
    s.add_argument("-q", "--queue", required=True)
    s.add_argument("--timeout", type=float, default=0.0, help="seconds; 0 = forever")
    s.add_argument("--interval", type=float, default=5.0)
    s.add_argument("--cmd", default="", help="shell command to run once drained")
    s.add_argument("--notify-webhook", default="", dest="notify_webhook",
                   help="POST JSON to this URL when the queue drains (async reply)")
    s.set_defaults(func=cmd_wait)

    s = sub.add_parser("start")
    s.add_argument("--interval", type=int, default=30,
                   help="reconciler tick interval in seconds (default 30)")
    s.add_argument("--stuck-minutes", type=int, default=health.STUCK_MINUTES)
    s.add_argument("--engine", default="claude", choices=["claude", "codex", "kimi"])
    s.add_argument("--auto-spawn", action="store_true",
                   help="auto spawn-worker on a stuck queue with no live workers")
    s.add_argument("--dry-run", action="store_true", dest="dry_run",
                   help="reconciler tick: log what would happen but don't spawn/stop")
    s.add_argument("--dashboard", action="store_true",
                   help=argparse.SUPPRESS)  # deprecated: HTTP is now always-on
    s.add_argument("--host", default="127.0.0.1",
                   help="HTTP server bind host (default 127.0.0.1)")
    s.add_argument("--port", type=int, default=8787,
                   help="HTTP server bind port (default 8787)")
    s.add_argument("--foreground", action="store_true", help=argparse.SUPPRESS)
    s.set_defaults(func=cmd_start)

    s = sub.add_parser("stop")
    s.set_defaults(func=cmd_stop)

    s = sub.add_parser("install")
    s.set_defaults(func=cmd_install)

    s = sub.add_parser("uninstall")
    s.set_defaults(func=cmd_uninstall)

    s = sub.add_parser("skills")
    s.set_defaults(func=cmd_skills, skills_command=None)
    ssub = s.add_subparsers(dest="skills_command")
    ssub.add_parser("sync", help="symlink into every present harness (default; also runs on `wt install`)")
    ssub.add_parser("status", help="show sync state without changing anything")
    ssub.add_parser("remove", help="remove the managed symlinks")

    s = sub.add_parser("snapshot")
    s.set_defaults(func=cmd_snapshot, snapshot_command=None)
    ssub = s.add_subparsers(dest="snapshot_command", metavar="<verb>")
    sa = ssub.add_parser("arm")
    sa.add_argument("--session", required=True)
    sa.add_argument("--engine", required=True)
    sa.add_argument("--cwd", required=True)
    sa.add_argument("--idle", type=float, default=None)
    sa.add_argument("--mode", default="mdfile", choices=["mdfile", "compact", "both"])
    sa.set_defaults(func=cmd_snapshot)

    sd = ssub.add_parser("disarm")
    sd.add_argument("--session", required=True)
    sd.set_defaults(func=cmd_snapshot)

    st = ssub.add_parser("status")
    st.add_argument("--session", default=None)
    st.set_defaults(func=cmd_snapshot)

    sf = ssub.add_parser("fire")
    sf.add_argument("--session", required=True)
    sf.set_defaults(func=cmd_snapshot)

    tr = ssub.add_parser("timer-run")  # internal: spawned detached by snapshot.arm
    tr.add_argument("session_id")
    tr.set_defaults(func=cmd_snapshot)

    sp = ssub.add_parser("path")
    sp.add_argument("--session", required=True)
    sp.set_defaults(func=cmd_snapshot)

    sr = ssub.add_parser("record")
    sr.add_argument("--session", required=True)
    sr.add_argument("--cwd", required=True)
    sr.set_defaults(func=cmd_snapshot)

    sl = ssub.add_parser("latest")
    sl.add_argument("--cwd", required=True)
    sl.set_defaults(func=cmd_snapshot)

    sc = ssub.add_parser("consume")
    sc.add_argument("--path", required=True)
    sc.set_defaults(func=cmd_snapshot)

    se = ssub.add_parser("sessions")
    se.add_argument("--cwd", required=True)
    se.add_argument("-n", type=int, default=10, dest="limit")
    se.add_argument("--exclude", default="")
    se.set_defaults(func=cmd_snapshot)

    s = sub.add_parser("dashboard", aliases=["serve"])
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8787)
    s.add_argument("--no-open", action="store_true",
                   help="ensure the server is up but don't open a browser")
    s.add_argument("--stop", action="store_true",
                   help="stop the background dashboard server")
    s.add_argument("--foreground", action="store_true",
                   help="run the server in the foreground (blocking; for debugging)")
    s.add_argument("--once", action="store_true",
                   help="handle one request then exit (for tests)")
    s.set_defaults(func=cmd_dashboard)

    return p


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not getattr(args, "command", None):
        parser.print_help()
        return 0
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

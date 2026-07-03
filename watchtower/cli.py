#!/usr/bin/env python3
"""WatchTower CLI — the ``wt`` binary.

    wt status                 per-queue depth / age / drain / stuck flag
    wt ls -q Q [--status ..]  list the tickets in one queue
    wt find <ref>             look up one ticket by ref, across all queues
    wt add -q Q --title..     file a ticket
    wt edit <ref> --priority..  patch fields on an existing ticket
    wt claim -q Q             claim next ticket (smart: priority → type → age)
    wt claim -q Q CCC-42      claim a specific ticket by ref
    wt claim -q Q --oldest    claim oldest ticket (pure FIFO)
    wt claim -q Q --type bug  claim only bugs (or --type feature for ideas)
    wt claim -q Q --readiness needs-shaping  claim unspecced ideas
    wt close <ref>            close a ticket (--summary required)
    wt drain on|off Q         opt a queue in/out of auto-spawn
    wt workers                list workers the watcher started
    wt block / blocked        park a ticket needing a human / list parked
    wt answer / discuss       answer a blocked ticket / attach to its session
    wt send <target> "text"   push a message to a worker/agent/session
    wt ask <target> "q"       ask a target and wait for its reply
    wt outbox ls|retry|rm     inspect and manage undelivered messages
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
import shlex
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from . import __version__
from . import health, queue as q, workers

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


# ----------------------------------------------------------------------- commands
def cmd_status(args: argparse.Namespace) -> int:
    rows = health.all_status(project=args.queue, stuck_minutes=args.stuck_minutes)
    if args.json:
        print(json.dumps(rows, indent=2))
    else:
        _print_status(rows)
    return 0



def cmd_ls(args: argparse.Namespace) -> int:
    """List the tickets in a single queue (the actual items, not just counts)."""
    items = q.list_items(project=args.queue)
    want = args.status
    if want == "active":
        items = [i for i in items if i.get("status") in ("open", "in_progress")]
    elif want != "all":
        items = [i for i in items if i.get("status") == want]
    if args.json:
        print(json.dumps(items, indent=2))
        return 0
    if not items:
        print(f"(no {('' if want=='all' else want+' ')}items in {args.queue})")
        return 0
    limit = args.limit or len(items)
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
                    extras.append(f"{n} {label}{'s' if n != 1 else ''}")
            if extras:
                line += f" [{', '.join(extras)}]"
        print(line)
    if len(items) > limit:
        print(f"... and {len(items) - limit} more (raise --limit)")
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
    if args.json:
        print(json.dumps(item, indent=2))
        return 0
    worker = str(item.get("claimed_by") or item.get("claimed_session_id") or "")
    title = _oneline(item.get("title") or item.get("note") or "")
    print(f"{item.get('ref',''):<14}[{item.get('status',''):<11}] {title}")
    if worker:
        print(f"  claimed_by: {worker}")
    res = item.get("resolution") if item.get("status") == "closed" else None
    if res and res.get("summary"):
        print(f"  resolution: {res['summary']}")
    return 0


def cmd_edit(args: argparse.Namespace) -> int:
    """Patch fields (title/priority/type/readiness/...) on an existing
    ticket, in place -- no refile/close churn (WT-71). Only flags the
    caller actually passed are touched; everything else is left as-is."""
    fields = {}
    for name in (
        "title", "note", "text", "url", "type", "readiness", "priority",
        "value", "confidence", "selector", "screenshot_path", "repo_path",
    ):
        value = getattr(args, name, None)
        if value is not None:
            fields[name] = value
    if not fields:
        print(
            "error: no fields to edit -- pass at least one of "
            "--title/--note/--text/--url/--type/--readiness/--priority/"
            "--value/--confidence/--selector/--screenshot-path/--repo-path",
            file=sys.stderr,
        )
        return 1
    try:
        item = q.update(args.ref, **fields)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    if not item:
        print(f"(no item {args.ref})", file=sys.stderr)
        return 1
    if args.json:
        _print_item(item)
    else:
        print(f"EDITED: {item['ref']}  {item.get('title') or item.get('note','')}")
    return 0


def cmd_add(args: argparse.Namespace) -> int:
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
    )
    print(f"FILED: {item['ref']}  {item.get('title') or item.get('note','')}")
    # Enqueue-and-claim: file the ticket, then immediately mark it in_progress so
    # the reconciler (which only spawns for OPEN tickets) leaves it alone. For the
    # user who's already working the bug they're documenting. Skip the dispatch
    # entirely -- an already-claimed ticket is in_progress, not open, so nudging
    # or spawning a worker would be a no-op at best.
    if getattr(args, "claim", False):
        worker = args.worker or f"wt-cli-{os.getpid()}"
        try:
            q.claim_by_ref(item["ref"], worker)
            print(f"CLAIMED: {item['ref']} -> {worker}")
        except Exception as e:
            # Enqueue already succeeded; a claim hiccup shouldn't fail the file.
            print(f"[watchtower] could not claim {item['ref']}: {e}", file=sys.stderr)
        return 0
    # Decide + act on the new ticket NOW (nudge a live worker via FIFO, else
    # reap+spawn) and log the decision to the activity log. Centralized in
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


def cmd_take(args: argparse.Namespace) -> int:
    """Shorthand for `add --claim`: file a ticket and immediately claim it, for
    documenting a bug you're already working on. Delegates to cmd_add so the two
    share one code path and can't drift."""
    args.claim = True
    return cmd_add(args)


def cmd_claim(args: argparse.Namespace) -> int:
    worker = args.worker or f"wt-cli-{os.getpid()}"
    ref = getattr(args, "ref", None) or None

    if ref:
        try:
            item = q.claim_by_ref(ref, worker)
        except ValueError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        if not item:
            print(f"error: {ref} not found", file=sys.stderr)
            return 1
    else:
        item = q.claim_next(
            worker,
            project=args.queue,
            oldest=getattr(args, "oldest", False),
            item_types=getattr(args, "type", None) or [],
            readiness_filters=getattr(args, "readiness", None) or [],
        )
        if not item:
            # Nothing claimable. Decide surplus HERE, at claim time, when the real
            # current state is known — not on the reconciler's future-guessing
            # count. A worker is surplus only if more workers are live than the
            # queue wants; then it exits itself. Otherwise it stays warm (its next
            # `wt add` nudge wakes it) and REAP handles a persistently-idle one.
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

    _rename_claiming_session(item)

    if args.json:
        _print_item(item)
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
    """Mark an existing ticket runnable and dispatch its queue."""
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


def _resolution_from_args(args: argparse.Namespace) -> Optional[dict]:
    """Build a resolution dict from --summary/--caveat/--follow-up/--unresolved.

    Returns None when no flag was given (so close stays back-compatible)."""
    res = {
        "summary": args.summary or "",
        "caveats": list(args.caveat or []),
        "follow_ups": list(args.follow_up or []),
        "unresolved": list(args.unresolved or []),
    }
    if not any(res.values()):
        return None
    return res


def cmd_close(args: argparse.Namespace) -> int:
    if not (args.summary or "").strip():
        print(
            "error: --summary is required when closing a ticket\n"
            "  example: wt close <ref> --summary \"what you changed\"",
            file=sys.stderr,
        )
        return 1
    worker = args.worker or f"wt-cli-{os.getpid()}"
    resolution = _resolution_from_args(args)
    try:
        item = q.close(args.ref, worker, resolution=resolution)
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


def _resume_session_headless(sid: str, repo: str, prompt: str, engine: str) -> bool:
    """Wake a blocked worker's session non-interactively and hand it the answer.

    Spawns `claude --resume <sid> -p <prompt>` (or the codex equivalent)
    detached, in the ticket's repo, logging to ~/.watchtower/logs. The resumed
    session has its full original context, applies the answer, finishes the
    ticket, and closes it. Returns True if the resume process started."""
    import subprocess
    if engine == "codex":
        argv = ["codex", "resume", sid, prompt]
    else:
        argv = ["claude", "--resume", sid, "-p", prompt,
                "--permission-mode", "bypassPermissions"]
    log_dir = Path.home() / ".watchtower" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"resume-{sid}.log"
    try:
        logf = open(log_path, "ab")
        try:
            subprocess.Popen(
                argv, stdin=subprocess.DEVNULL, stdout=logf,
                stderr=subprocess.STDOUT, start_new_session=True,
                cwd=repo or os.getcwd(),
            )
        finally:
            logf.close()
        return True
    except (OSError, FileNotFoundError):
        return False


def cmd_answer(args: argparse.Namespace) -> int:
    """Inject a human answer onto a blocked ticket and auto-resume its session.

    Clears needs_input, then wakes the blocked worker's session headless with
    the answer so it finishes and closes the ticket — no manual `wt discuss`
    step (WT-28)."""
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
    repo = item.get("repo_path") or os.getcwd()
    prompt = (
        f"A human answered your blocked question on ticket {item['ref']}. "
        f"Their answer: {args.text}. Apply it, finish the ticket, and close it "
        f"with `wt close {item['ref']} --worker <your-id> --summary \"...\"`. "
        f"If it still cannot be resolved, run `wt block` again with the new "
        f"open question."
    )
    started = _resume_session_headless(sid, repo, prompt, args.engine)
    if started:
        print(f"ANSWERED: {item['ref']} — resuming session {sid} in {repo} "
              f"to apply your answer and close.")
    else:
        print(f"ANSWERED: {item['ref']} — needs_input cleared, but auto-resume "
              f"failed to start. Resume manually: wt discuss {item['ref']}")
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
    --no-queue) for the daemon to retry."""
    from . import messages
    res = messages.send(
        args.target, args.text, mode=args.mode,
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
    }
    fn = handlers.get(getattr(args, "chat_command", None))
    if fn is None:
        print("usage: wt chat new|post|read|ls|nudge|add|leave|archive|close ...",
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
    """Set queue-level config (repo_path, engine, desired_workers, etc.)."""
    from . import config
    changed = []
    if args.backend is not None:
        try:
            config.set_backend(args.queue, args.backend)
        except ValueError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        changed.append(f"backend={config.backend(args.queue)}")
    if args.github_repo is not None:
        config.set_github_repo(args.queue, args.github_repo)
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
    if args.desired_workers is not None:
        config.set_desired_workers(args.queue, args.desired_workers)
        changed.append(f"desired_workers={args.desired_workers}")
    if not changed:
        cfg = config.get_queue_config(args.queue)
        print(f"{args.queue}: {cfg if cfg else '(no config)'}")
    else:
        print(f"{args.queue}: {', '.join(changed)}")
    return 0


def cmd_drain(args: argparse.Namespace) -> int:
    """Enable or disable auto-drain for a queue (wt drain on|off <queue>)."""
    from . import config
    enabled = args.onoff == "on"
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


def _daemon_loop(args: argparse.Namespace) -> None:
    interval = max(5, args.interval)
    dry_run = getattr(args, "dry_run", False)
    # Always host the HTTP server alongside the watcher.
    import threading

    from . import dashboard

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
            print(
                f"[watchtower] spawned worker {rec.get('worker_id','')} "
                f"for {rec.get('queue','')}{tag}",
                flush=True,
            )
        for rec in result.get("stopped", []):
            tag = " (dry-run)" if rec.get("dry_run") else ""
            print(
                f"[watchtower] requested stop for {rec.get('worker_id','')} "
                f"on {rec.get('queue','')}{tag}",
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
                    if not dry_run:
                        workers.spawn_workers(r["queue"], n=1, engine=args.engine)
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
            if not dry_run:
                DAEMON_PID_FILE.unlink(missing_ok=True)
        return 0
    # Re-exec ourselves in the background in foreground-mode.
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
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
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
    ("Queues", "set"),
    ("Queues", "drain"),
    ("Queues", "wait"),
    ("Queues", "monitor"),
    ("Queues", "workers"),
    ("Tickets", "add"),
    ("Tickets", "take"),
    ("Tickets", "edit"),
    ("Tickets", "run"),
    ("Tickets", "find"),
    ("Tickets", "ls"),
    ("Tickets", "blocked"),
    ("Tickets", "answer"),
    ("Tickets", "discuss"),
    ("Tickets", "dedup"),
    ("Agent messaging", "send"),
    ("Agent messaging", "ask"),
    ("Agent messaging", "outbox"),
    ("Agent messaging", "agents"),
    ("Agent messaging", "chat"),
    ("Worker protocol", "claim"),
    ("Worker protocol", "close"),
    ("Worker protocol", "block"),
]
# `install` is intentionally absent: it's a hidden alias folded into `wt start`
# (see cmd_start's first-time auto-install), not a command users need to type.
# `agent` is intentionally absent too: it's a hidden compat alias folded into
# `wt agents` (register/set-name/rm now live there; see _add_agent_subcommands).

COMMAND_HELP: Dict[str, str] = {
    "add": "file a ticket",
    "take": "file a ticket and immediately claim it (= add --claim)",
    "edit": "patch fields (title/priority/type/readiness/...) on an existing ticket",
    "claim": "claim next open ticket (smart sort: priority + type + age)",
    "close": "close a ticket (record how you fixed it)",
    "block": "park a ticket that needs a human decision",
    "blocked": "list tickets parked for a human",
    "answer": "answer a blocked ticket; auto-resumes its session",
    "discuss": "attach to a blocked ticket's session (claude --resume)",
    "run": "mark an existing GitHub issue runnable and dispatch its queue",
    "find": "look up one ticket by ref across all queues (no -q needed)",
    "ls": "list the tickets in one queue",
    "dedup": "close exact-duplicate open tickets",
    "status": "per-queue depth / age / stuck flag",
    "set": "set queue-level config (repo_path, engine, workers)",
    "drain": "enable or disable auto-drain for a queue",
    "wait": "block until the queue is drained",
    "monitor": "run a check; file a ticket if it fails",
    "workers": "list workers this CLI started",
    "agents": "address book: list reachable agents; register/set-name/rm to name them",
    "send": "push a message to a worker/agent/session",
    "ask": "ask a target and wait for its reply",
    "outbox": "inspect and manage undelivered messages",
    "chat": "group chats: multi-agent conversations",
    "start": "start the service (installs the LaunchAgent on first run)",
    "stop": "stop service (watcher, reconciler, dashboard, HTTP API)",
    "uninstall": "remove LaunchAgent (stop auto-start on login)",
    "dashboard": "open the night-watch dashboard (background server + browser)",
    "skills": "sync the bundled watchtower skill into installed agent harnesses",
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


# --------------------------------------------------------------------------- main
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="wt",
        usage="wt <command> [options]",
        description="WatchTower queue CLI",
        epilog=_build_command_epilog(),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--version", action="version", version=f"wt {__version__}")
    sub = p.add_subparsers(dest="command", metavar="<command>", help=argparse.SUPPRESS)

    s = sub.add_parser("status")
    s.add_argument("-q", "--queue", default=None)
    s.add_argument("--stuck-minutes", type=int, default=health.STUCK_MINUTES)
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_status)

    s = sub.add_parser("ls")
    s.add_argument("-q", "--queue", required=True)
    s.add_argument(
        "--status",
        default="active",
        choices=["active", "open", "in_progress", "closed", "all"],
        help="which tickets to show (default: active = open + in_progress)",
    )
    s.add_argument("--limit", type=int, default=0, help="max rows (0 = all)")
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_ls)

    s = sub.add_parser("find")
    s.add_argument("ref", help="ticket ref (e.g. WT-48) or bare number")
    s.add_argument("--json", action="store_true")
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
        subparser.add_argument("--worker", default="",
                               help="worker/owner id to claim under when --claim is "
                                    "set; defaults to wt-cli-<pid>")

    s = sub.add_parser("add")
    _add_common_ticket_args(s)
    s.add_argument("--claim", action="store_true",
                   help="immediately claim the new ticket (mark in_progress) so no "
                        "auto-drain worker picks it up; use when you're already working it")
    s.set_defaults(func=cmd_add)

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
    s.add_argument("--selector", default=None)
    s.add_argument("--screenshot-path", default=None, dest="screenshot_path")
    s.add_argument("--repo-path", default=None, dest="repo_path")
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

    s = sub.add_parser("run")
    s.add_argument("ref", help="ticket ref / GitHub issue ref, e.g. BYM-GH-FINIE-402")
    s.add_argument("--no-dispatch", action="store_true",
                   help="only add the WatchTower label; do not nudge/spawn workers")
    s.set_defaults(func=cmd_run)

    s = sub.add_parser("close")
    s.add_argument("ref")
    s.add_argument("--worker", default="")
    s.add_argument("--summary", default="",
                   help="one-line description of what you changed")
    s.add_argument("--caveat", action="append",
                   help="something to watch out for (repeatable)")
    s.add_argument("--follow-up", action="append", dest="follow_up",
                   help="a notable follow-up task (repeatable)")
    s.add_argument("--unresolved", action="append",
                   help="something you could not fix (repeatable)")
    s.add_argument("--enqueue-follow-ups", action="store_true",
                   dest="enqueue_follow_ups",
                   help="also file each follow-up/unresolved as a new open ticket")
    s.set_defaults(func=cmd_close)

    s = sub.add_parser("block")
    s.add_argument("ref")
    s.add_argument("--worker", default="", help="your session/worker id")
    s.add_argument("--question", default="", help="the specific decision you need")
    s.add_argument("--progress", default="",
                   help="analysis-so-far note (backstop if the session is lost)")
    s.set_defaults(func=cmd_block)

    s = sub.add_parser("blocked")
    s.add_argument("-q", "--queue", default=None)
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_blocked)

    s = sub.add_parser("answer")
    s.add_argument("ref")
    s.add_argument("text", help="your answer")
    s.add_argument("--worker", default="")
    s.add_argument("--engine", default="claude", choices=["claude", "codex"],
                   help="engine to resume the blocked session with")
    s.set_defaults(func=cmd_answer)

    s = sub.add_parser("discuss")
    s.add_argument("ref")
    s.add_argument("--engine", default="claude", choices=["claude", "codex"])
    s.add_argument("--print", action="store_true", dest="print",
                   help="print the resume command instead of running it")
    s.set_defaults(func=cmd_discuss)

    s = sub.add_parser("workers")
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_workers)

    s = sub.add_parser("session-names", help=argparse.SUPPRESS)
    s.set_defaults(func=cmd_session_names)
    snub = s.add_subparsers(dest="session_names_command")
    b = snub.add_parser("backfill")
    b.add_argument("--hours", type=float, default=24.0)
    b.add_argument("--dry-run", action="store_true")
    b.set_defaults(func=cmd_session_names)

    s = sub.add_parser("send")
    s.add_argument("target", help="worker id, @agent name, or session UUID/prefix")
    s.add_argument("text", help="the message")
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

    s = sub.add_parser("set")
    s.add_argument("-q", "--queue", required=True)
    s.add_argument("--backend", default=None, choices=["file", "github"],
                   help="queue backing store: file (default) or github")
    s.add_argument("--github-repo", default=None, dest="github_repo",
                   help="GitHub repo for --backend github, as OWNER/REPO")
    s.add_argument("--github-assignee", default=None, dest="github_assignee",
                   help="assignee used by GitHub-backed claims (default: @me)")
    s.add_argument("--repo-path", default=None, dest="repo_path",
                   help="default cwd for workers spawned on this queue")
    s.add_argument("--engine", default=None, choices=["claude", "codex"],
                   help=(
                       "agent engine for workers on this queue (default: claude). "
                       "claude: stream-json mode over a FIFO stdin — live, pushable, "
                       "prompt-cache warm for ~5 min; requires the Claude Code CLI. "
                       "codex: one-shot `codex exec <goal>` — no FIFO, no live push; "
                       "requires the OpenAI Codex CLI."
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
    s.add_argument("--engine", default="claude", choices=["claude", "codex"])
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

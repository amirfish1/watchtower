#!/usr/bin/env python3
"""WatchTower CLI — the ``wt`` binary.

    wt status                 per-queue depth / age / drain / stuck flag
    wt ls -q Q [--status ..]  list the tickets in one queue
    wt add -q Q --title..     file a ticket
    wt claim -q Q             claim next ticket (smart: priority → type → age)
    wt claim -q Q --oldest    claim oldest ticket (pure FIFO)
    wt claim -q Q --type bug  claim only bugs (or --type feature for ideas)
    wt claim -q Q --readiness needs-shaping  claim unspecced ideas
    wt close <ref>            close a ticket (--summary required)
    wt drain on|off Q         opt a queue in/out of auto-spawn
    wt workers                list workers the watcher started
    wt block / blocked        park a ticket needing a human / list parked
    wt answer / discuss       answer a blocked ticket / attach to its session
    wt wait -q Q [--cmd ..]   block until the queue is drained, then run --cmd
    wt start / wt stop        start/stop service (watcher, reconciler, dashboard, HTTP API)
    wt dashboard              phone-first HTTP dashboard (queues + workers)
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import signal
import sys
import time
from pathlib import Path
from typing import List, Optional

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
            print(
                f"{r['queue']:<14}{r['depth']:>5}{r['in_progress']:>5}{r['closed']:>6}"
                f"  {r['oldest_open_age']:>8}  {r['since_progress']:>8}"
                f"  {wcell:<12}{drain_cell:<7}{flag}  {_eta_note(r)}"
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
        title = (it.get("title") or it.get("note") or "")[:56]
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
    # Auto-drain queues: push the new work to a live worker NOW via its
    # stream-json FIFO (immediate pickup, warm context, no polling). Only if no
    # live worker accepts do we reconcile to spawn a fresh one. Idempotent and
    # best-effort -- a hiccup here never fails the enqueue.
    try:
        from . import config, workers
        if config.auto_drain(args.queue):
            nudge = (
                f"New ticket {item['ref']} filed on {args.queue}. "
                f"Claim it with `wt claim -q {args.queue} --worker <your-id> "
                f"--json` and drain the queue."
            )
            delivered = workers.notify_workers(args.queue, nudge)
            if delivered:
                print(f"[watchtower] nudged {delivered} live worker(s) on "
                      f"{args.queue}")
            else:
                result = workers.reconcile_once()
                for rec in result.get("spawned", []):
                    print(f"[watchtower] spawned worker {rec.get('worker_id','')} "
                          f"for {rec.get('queue','')}")
    except Exception:
        pass
    return 0


def cmd_claim(args: argparse.Namespace) -> int:
    worker = args.worker or f"wt-cli-{os.getpid()}"
    item = q.claim_next(
        worker,
        project=args.queue,
        oldest=getattr(args, "oldest", False),
        item_types=getattr(args, "type", None) or [],
        readiness_filters=getattr(args, "readiness", None) or [],
    )
    if not item:
        print(f"(nothing open in {args.queue})")
        return 0
    # Stop signal: reconciler asked this worker to wind down.
    if item.get("stop"):
        if args.json:
            print(json.dumps({"stop": True}))
        else:
            print("STOP: reconciler requested shutdown; exiting")
        return 0
    if args.json:
        _print_item(item)
    else:
        print(f"CLAIMED: {item['ref']} -> {worker}")
        print(item.get("text") or item.get("note") or "")
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
    item = q.close(args.ref, worker, resolution=resolution)
    if not item:
        print(f"(no item {args.ref})", file=sys.stderr)
        return 1
    res = item.get("resolution") or {}
    summary = res.get("summary", "")
    print(f"CLOSED: {item['ref']}" + (f" — {summary}" if summary else ""))

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


def cmd_answer(args: argparse.Namespace) -> int:
    """Quick-inject a human answer onto a blocked ticket; clears needs_input so
    the resumed session continues (WT-28)."""
    item = q.answer(args.ref, args.text, session_id=args.worker)
    if not item:
        print(f"(no item {args.ref})", file=sys.stderr)
        return 1
    print(f"ANSWERED: {item['ref']} — needs_input cleared. "
          f"Resume the session to continue: wt discuss {item['ref']}")
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
    state = "on" if enabled else "off"
    print(f"{args.queue}: drain {state} — reconciler will {'spawn workers automatically' if enabled else 'leave this queue alone'}")
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


def cmd_install(args: argparse.Namespace) -> int:
    """Write a LaunchAgent plist so the WT service starts automatically on login.

    Writes the plist unconditionally (so it's ready), but only loads it into
    launchctl if at least one queue has auto-drain enabled — otherwise the
    service would start for no reason."""
    from . import config as _cfg
    import shutil
    python = shutil.which("wt") or sys.executable
    if shutil.which("wt"):
        program_args = ["wt", "start", "--foreground", "--auto-spawn"]
    else:
        program_args = [sys.executable, "-m", "watchtower.cli", "start",
                        "--foreground", "--auto-spawn"]
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
  <false/>
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


# --------------------------------------------------------------------------- main
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="wt", description="WatchTower queue CLI")
    p.add_argument("--version", action="version", version=f"wt {__version__}")
    sub = p.add_subparsers(dest="command")

    s = sub.add_parser("status", help="per-queue depth / age / stuck flag")
    s.add_argument("-q", "--queue", default=None)
    s.add_argument("--stuck-minutes", type=int, default=health.STUCK_MINUTES)
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_status)

    s = sub.add_parser("ls", help="list the tickets in one queue")
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

    s = sub.add_parser("add", help="file a ticket")
    s.add_argument("-q", "--queue", required=True)
    s.add_argument("--title", default="")
    s.add_argument("--note", default="")
    s.add_argument("--text", default="")
    s.add_argument("--url", default="")
    s.add_argument("--lane", default="normal", choices=list(q.VALID_LANES))
    s.add_argument("--type", default="", choices=["bug", "feature", ""],
                   help="item type: bug or feature")
    s.add_argument("--readiness", default="",
                   choices=["ready", "needs-shaping", "needs-spec", ""],
                   help="readiness level")
    s.add_argument("--priority", default="",
                   choices=["p0", "p1", "p2", "p3", "p4", ""],
                   help="priority: p0 (highest) through p4 (lowest)")
    s.add_argument("--value", default="", choices=["H", "M", "L", ""],
                   help="business value: H, M, or L")
    s.add_argument("--confidence", default="", choices=["H", "M", "L", ""],
                   help="confidence: H, M, or L")
    s.set_defaults(func=cmd_add)

    s = sub.add_parser("claim", help="claim next open ticket (smart sort: priority + type + age)")
    s.add_argument("-q", "--queue", required=True)
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

    s = sub.add_parser("close", help="close a ticket (record how you fixed it)")
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

    s = sub.add_parser("block", help="park a ticket that needs a human decision")
    s.add_argument("ref")
    s.add_argument("--worker", default="", help="your session/worker id")
    s.add_argument("--question", default="", help="the specific decision you need")
    s.add_argument("--progress", default="",
                   help="analysis-so-far note (backstop if the session is lost)")
    s.set_defaults(func=cmd_block)

    s = sub.add_parser("blocked", help="list tickets parked for a human")
    s.add_argument("-q", "--queue", default=None)
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_blocked)

    s = sub.add_parser("answer", help="answer a blocked ticket (clears needs_input)")
    s.add_argument("ref")
    s.add_argument("text", help="your answer")
    s.add_argument("--worker", default="")
    s.set_defaults(func=cmd_answer)

    s = sub.add_parser("discuss",
                       help="attach to a blocked ticket's session (claude --resume)")
    s.add_argument("ref")
    s.add_argument("--engine", default="claude", choices=["claude", "codex"])
    s.add_argument("--print", action="store_true", dest="print",
                   help="print the resume command instead of running it")
    s.set_defaults(func=cmd_discuss)

    s = sub.add_parser("workers", help="list workers this CLI started")
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_workers)

    s = sub.add_parser("set", help="set queue-level config (repo_path, engine, workers)")
    s.add_argument("-q", "--queue", required=True)
    s.add_argument("--repo-path", default=None, dest="repo_path",
                   help="default cwd for workers spawned on this queue")
    s.add_argument("--engine", default=None, choices=["claude", "codex"],
                   help="agent engine for workers on this queue (default: claude)")
    s.add_argument("--desired-workers", default=None, type=int, dest="desired_workers",
                   help="number of concurrent workers the reconciler should maintain")
    s.set_defaults(func=cmd_set)

    s = sub.add_parser("drain", help="enable or disable auto-drain for a queue")
    s.add_argument("onoff", choices=["on", "off"], help="on = auto-spawn workers; off = backlog mode")
    s.add_argument("queue", metavar="QUEUE", help="queue name (e.g. CCC, WT)")
    s.set_defaults(func=cmd_drain)

    s = sub.add_parser("monitor", help="run a check; file a ticket if it fails")
    s.add_argument("-q", "--queue", required=True)
    s.add_argument("--cmd", required=True, help="shell command; non-zero exit = fail")
    s.add_argument("--title", default="", help="ticket title on failure")
    s.add_argument("--note", default="", help="ticket note on failure")
    s.set_defaults(func=cmd_monitor)

    s = sub.add_parser("dedup", help="close exact-duplicate open tickets")
    s.add_argument("-q", "--queue", default=None)
    s.add_argument("--apply", action="store_true", help="close dupes (default: dry-run)")
    s.set_defaults(func=cmd_dedup)

    # No `wt spawn-worker`: workers are spawned by the watcher (`wt start`) from
    # per-queue auto_drain policy + depth, not by hand. See workers.spawn_workers.

    s = sub.add_parser("wait", help="block until the queue is drained")
    s.add_argument("-q", "--queue", required=True)
    s.add_argument("--timeout", type=float, default=0.0, help="seconds; 0 = forever")
    s.add_argument("--interval", type=float, default=5.0)
    s.add_argument("--cmd", default="", help="shell command to run once drained")
    s.add_argument("--notify-webhook", default="", dest="notify_webhook",
                   help="POST JSON to this URL when the queue drains (async reply)")
    s.set_defaults(func=cmd_wait)

    s = sub.add_parser("start", help="start service (watcher, reconciler, dashboard, HTTP API)")
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

    s = sub.add_parser("stop", help="stop service (watcher, reconciler, dashboard, HTTP API)")
    s.set_defaults(func=cmd_stop)

    s = sub.add_parser("install", help="install LaunchAgent so service starts on login")
    s.set_defaults(func=cmd_install)

    s = sub.add_parser("uninstall", help="remove LaunchAgent (stop auto-start on login)")
    s.set_defaults(func=cmd_uninstall)

    s = sub.add_parser(
        "dashboard",
        aliases=["serve"],
        help="open the night-watch dashboard (background server + browser)",
    )
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

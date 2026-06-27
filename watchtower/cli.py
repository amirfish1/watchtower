#!/usr/bin/env python3
"""WatchTower CLI — the ``wt`` binary.

Phase-1 commands:

    wt status                 per-queue depth / oldest-open age / stuck flag
    wt queues                 list queues + counts
    wt ls -q Q [--status ..]  list the tickets in one queue
    wt enqueue -q Q --title.. file a ticket
    wt claim -q Q             claim the oldest open ticket (atomic)
    wt next -q Q              alias for claim
    wt close <ref>            close a ticket
    wt workers                list workers this CLI started
    wt spawn-worker -q Q      launch N draining worker subprocess(es)
    wt wait -q Q [--cmd ..]   block until the queue is drained, then run --cmd
    wt start / wt stop        start/stop the background watcher daemon
    wt dashboard              phone-first HTTP dashboard (queues + workers)
"""

from __future__ import annotations

import argparse
import json
import os
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


def _print_status(rows: List[dict]) -> None:
    print(f"store: {q.store_path()}")
    counts = workers.worker_counts()
    if not rows:
        print("(no queues)")
    else:
        hdr = (
            f"{'QUEUE':<14}{'OPEN':>5}{'WIP':>5}{'DONE':>6}  {'OLDEST':>8}"
            f"  {'IDLE':>8}  {'WORKERS':<12}STATUS"
        )
        print(hdr)
        print("-" * len(hdr))
        for r in rows:
            flag = "STUCK" if r["stuck"] else ("ok" if r["depth"] == 0 else "draining")
            wc = counts.get(r["queue"], {"total": 0, "live": 0})
            wcell = f"{wc['total']} ({wc['live']} live)"
            print(
                f"{r['queue']:<14}{r['depth']:>5}{r['in_progress']:>5}{r['closed']:>6}"
                f"  {r['oldest_open_age']:>8}  {r['since_progress']:>8}"
                f"  {wcell:<12}{flag}  {_eta_note(r)}"
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


def cmd_queues(args: argparse.Namespace) -> int:
    data = q.queues()
    if args.json:
        print(json.dumps(data, indent=2))
        return 0
    print(f"store: {q.store_path()}")
    if not data:
        print("(no queues)")
        return 0
    print(f"{'QUEUE':<14}{'OPEN':>5}{'WIP':>5}{'DONE':>6}{'TOTAL':>7}")
    for name in sorted(data):
        c = data[name]
        print(
            f"{name:<14}{c['open']:>5}{c['in_progress']:>5}"
            f"{c['closed']:>6}{c['total']:>7}"
        )
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


def cmd_enqueue(args: argparse.Namespace) -> int:
    item = q.enqueue(
        project=args.queue,
        title=args.title or "",
        note=args.note or (args.title or ""),
        text=args.text or "",
        url=args.url or "",
        lane=args.lane,
        source="wt",
    )
    print(f"FILED: {item['ref']}  {item.get('title') or item.get('note','')}")
    return 0


def cmd_claim(args: argparse.Namespace) -> int:
    worker = args.worker or f"wt-cli-{os.getpid()}"
    item = q.claim_next(worker, project=args.queue)
    if not item:
        print(f"(nothing open in {args.queue})")
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


def cmd_config(args: argparse.Namespace) -> int:
    """Get/set per-queue config. Today: the auto_drain opt-out (WT-FEATURES #16)."""
    from . import config
    if args.auto_drain is not None:
        cfg = config.set_auto_drain(args.queue, args.auto_drain == "true")
        print(f"{args.queue}: auto_drain={cfg['auto_drain']}")
    else:
        val = config.auto_drain(args.queue)
        explicit = "auto_drain" in config.get_queue_config(args.queue)
        print(f"{args.queue}: auto_drain={val}" + ("" if explicit else " (default)"))
    return 0


def cmd_spawn_worker(args: argparse.Namespace) -> int:
    spawned = workers.spawn_workers(
        args.queue, n=args.n, engine=args.engine,
        repo_path=args.repo, dry_run=args.dry_run,
    )
    for s in spawned:
        tag = " (dry-run)" if s.get("dry_run") else f" pid={s['pid']}"
        print(f"SPAWNED worker {s['worker_id']} engine={s['engine']}{tag}")
        print(f"  repo: {s.get('repo_path','')}")
        if s.get("log"):
            print(f"  log:  {s['log']}")
        if args.dry_run:
            print(f"  argv: {s['argv']}")
    return 0


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
    if getattr(args, "dashboard", False):
        # Host the dashboard alongside the watcher in a background thread, so
        # `wt start --dashboard` brings both up in one process.
        import threading

        from . import dashboard

        host = getattr(args, "host", "127.0.0.1")
        port = getattr(args, "port", 8787)
        httpd = dashboard.ThreadingHTTPServer((host, port), dashboard._Handler)
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        print(
            f"[watchtower] dashboard on http://{host}:{port}",
            flush=True,
        )
    while True:
        from . import config
        rows = health.all_status(stuck_minutes=args.stuck_minutes)
        for r in rows:
            if not r["stuck"]:
                continue
            live = workers.live_worker_count(r["queue"])
            if live == 0 and args.auto_spawn and config.auto_drain(r["queue"]):
                print(
                    f"[watchtower] STUCK {r['queue']} open={r['depth']} "
                    f"no live workers -> auto-spawn",
                    flush=True,
                )
                workers.spawn_workers(r["queue"], n=1, engine=args.engine)
            elif live == 0 and args.auto_spawn and not config.auto_drain(r["queue"]):
                print(
                    f"[watchtower] STUCK {r['queue']} open={r['depth']} "
                    f"auto_drain=off (backlog, opted out)",
                    flush=True,
                )
            else:
                print(
                    f"[watchtower] STUCK {r['queue']} open={r['depth']} "
                    f"live_workers={live} (auto-spawn off)",
                    flush=True,
                )
        time.sleep(interval)


def cmd_start(args: argparse.Namespace) -> int:
    if DAEMON_PID_FILE.exists():
        try:
            pid = int(DAEMON_PID_FILE.read_text().strip())
            os.kill(pid, 0)
            print(f"watcher already running (pid {pid})")
            return 0
        except (ValueError, ProcessLookupError, OSError):
            pass  # stale pidfile
    if args.foreground:
        DAEMON_PID_FILE.parent.mkdir(parents=True, exist_ok=True)
        DAEMON_PID_FILE.write_text(str(os.getpid()))
        try:
            _daemon_loop(args)
        finally:
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
    if getattr(args, "dashboard", False):
        cmd += ["--dashboard", "--host", args.host, "--port", str(args.port)]
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    print(f"watcher started (pid {proc.pid}); auto-spawn={'on' if args.auto_spawn else 'off'}")
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

    s = sub.add_parser("queues", help="list queues + counts")
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_queues)

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

    s = sub.add_parser("enqueue", help="file a ticket")
    s.add_argument("-q", "--queue", required=True)
    s.add_argument("--title", default="")
    s.add_argument("--note", default="")
    s.add_argument("--text", default="")
    s.add_argument("--url", default="")
    s.add_argument("--lane", default="normal", choices=list(q.VALID_LANES))
    s.set_defaults(func=cmd_enqueue)

    s = sub.add_parser("claim", help="claim the oldest open ticket")
    s.add_argument("-q", "--queue", required=True)
    s.add_argument("--worker", default="")
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_claim)

    s = sub.add_parser("next", help="alias for claim")
    s.add_argument("-q", "--queue", required=True)
    s.add_argument("--worker", default="")
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

    s = sub.add_parser("workers", help="list workers this CLI started")
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_workers)

    s = sub.add_parser("config", help="per-queue config (auto-drain opt-out)")
    s.add_argument("-q", "--queue", required=True)
    s.add_argument("--auto-drain", choices=["true", "false"], default=None,
                   dest="auto_drain", help="opt this queue in/out of auto-spawn")
    s.set_defaults(func=cmd_config)

    s = sub.add_parser("spawn-worker", help="launch draining worker subprocess(es)")
    s.add_argument("-q", "--queue", required=True)
    s.add_argument("--n", type=int, default=1)
    s.add_argument("--engine", default="claude", choices=["claude", "codex"])
    s.add_argument("--repo", default="", help="repo the worker drains in (default: cwd)")
    s.add_argument("--dry-run", action="store_true")
    s.set_defaults(func=cmd_spawn_worker)

    s = sub.add_parser("wait", help="block until the queue is drained")
    s.add_argument("-q", "--queue", required=True)
    s.add_argument("--timeout", type=float, default=0.0, help="seconds; 0 = forever")
    s.add_argument("--interval", type=float, default=5.0)
    s.add_argument("--cmd", default="", help="shell command to run once drained")
    s.add_argument("--notify-webhook", default="", dest="notify_webhook",
                   help="POST JSON to this URL when the queue drains (async reply)")
    s.set_defaults(func=cmd_wait)

    s = sub.add_parser("start", help="start the background watcher daemon")
    s.add_argument("--interval", type=int, default=30)
    s.add_argument("--stuck-minutes", type=int, default=health.STUCK_MINUTES)
    s.add_argument("--engine", default="claude", choices=["claude", "codex"])
    s.add_argument("--auto-spawn", action="store_true",
                   help="auto spawn-worker on a stuck queue with no live workers")
    s.add_argument("--dashboard", action="store_true",
                   help="also host the dashboard alongside the watcher")
    s.add_argument("--host", default="127.0.0.1",
                   help="dashboard bind host (with --dashboard)")
    s.add_argument("--port", type=int, default=8787,
                   help="dashboard bind port (with --dashboard)")
    s.add_argument("--foreground", action="store_true", help=argparse.SUPPRESS)
    s.set_defaults(func=cmd_start)

    s = sub.add_parser("stop", help="stop the background watcher daemon")
    s.set_defaults(func=cmd_stop)

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

# WatchTower

[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

WatchTower is a queue-focused, CLI-first tool for running fleets of AI
coding-agent workers unattended and knowing, at a glance, which queues are
stuck. You file tickets into named queues, point workers at them to drain the
work, and `wt status` tells you which queues have open work that nobody is
making progress on. The engine is a single durable JSON queue (append-only,
file-locked) — stdlib-only Python, no runtime dependencies.

## Install

**Requirements:** Python 3.11+, macOS or Linux.

```bash
# 1. Clone
git clone https://github.com/amirfish1/watchtower.git
cd watchtower

# 2. Install the `wt` command
pip install -e .

# 3. Verify
wt --version
```

### Start the service

```bash
wt start        # start the background watcher + reconciler (one-shot, survives the shell)
wt status       # confirm it's running
```

### Auto-start on login (macOS LaunchAgent)

```bash
wt install      # writes ~/Library/LaunchAgents/ai.amirfish.watchtower.plist and loads it
wt uninstall    # unload and remove the LaunchAgent
```

After `wt install`, the watcher starts automatically on every login and restarts
if it crashes. No manual `wt start` needed after that.

### Make a queue and enable auto-drain

```bash
wt add -q MYAPP --title "First ticket" --text "Fix the login page"
wt set -q MYAPP --repo-path /path/to/your/repo --engine claude
wt drain on MYAPP      # auto-spawn workers; installs the LaunchAgent if not present
```

That's it. When a ticket lands in `MYAPP`, the reconciler spawns a Claude
worker in `/path/to/your/repo` to drain it. `wt status` shows progress.

### Claude Code skill (optional)

If you use [Claude Code](https://claude.ai/code), drop the bundled skill into
your Claude config so agents can file tickets directly from within a session:

```bash
cp contrib/annotate-widget.js your-project/static/dev/  # browser annotation widget
```

See [`contrib/annotate-widget.md`](contrib/annotate-widget.md) for the full
widget setup.

## Usage

```bash
wt status                              # per-queue depth, oldest-open age, stuck flag
wt ls -q DEMO                          # list tickets in a queue

wt add -q DEMO --title "Fix nav" \
       --text "navbar overlaps on mobile" --type bug

wt claim  -q DEMO                      # claim the next open ticket (smart sort)
wt claim  -q DEMO DEMO-1               # claim a specific ticket by ref
wt close  DEMO-1                       # close a ticket by ref
wt close  DEMO-1 --summary "fixed the overlap" \
          --caveat "only tested on iOS" --follow-up "add a regression test"

wt drain on DEMO                       # enable auto-drain for a queue
wt drain off DEMO                      # disable auto-drain
wt set -q DEMO --repo-path /path/to/repo --engine claude

wt workers                             # list workers the watcher started
wt wait   -q DEMO --timeout 600 --cmd "say done" # block until drained, then run cmd

wt start                               # start the background watcher + reconciler
wt stop                                # stop the watcher
wt install                             # install the watcher as a LaunchAgent (auto-start on login)
wt uninstall                           # remove the LaunchAgent
wt dashboard                           # open the night-watch dashboard (non-blocking)
wt dashboard --no-open                 # ensure the server is up, don't open a browser
```

`wt status` shows, per queue, depth (open) / WIP / done, oldest-open age, idle
time, a `WORKERS` column (`total (n live)`), and a STUCK/draining/ok flag,
followed by a **drain readout** (`~3/min · empty in ~20m`, or `stalled` when no
ticket has closed recently). Then a workers section listing each tracked
worker's id, queue, pid, LIVE/DEAD state (process liveness via
`os.kill(pid, 0)`), and what it is doing right now — `-> DASH-3 (4m)` (the
in-progress ticket it holds and how long ago it claimed it) or `idle`.

### Drain rate + ETA

WatchTower turns the queue's own close timestamps into a live estimate, so
`wt status` and the dashboard read like a forecast rather than a static count:

- **drain rate** — tickets closed in the last 30 minutes (a fixed window) over
  the window, i.e. closes/min.
- **ETA** — `depth / drain_rate`, rendered as `~20m` / `~2h`; `stalled` (null in
  JSON) when nothing has closed in the window.

These are computed from data already in the queue (`closed_at` timestamps) — no
new persistent state, no transcript parsing. `/api/status` carries
`drain_rate_per_min`, `eta_seconds`, and `eta_human` on each queue, and
`active_ref` / `active_since_human` on each worker.

### The dashboard: `wt dashboard`

```bash
wt dashboard [--port 8787] [--host 127.0.0.1]
             [--no-open] [--stop] [--foreground] [--once]
```

`wt dashboard` is a **night-watch operations console** — a calm, dark instrument
panel over your agent fleet that lights up like a beacon when a queue stalls.
It does **not** block your terminal:

1. Starts the HTTP server **detached** in the background if it isn't already
   running (idempotent — a second `wt dashboard` won't start a second server),
   recording its pid in `~/.watchtower/dashboard.pid` (override with
   `$WATCHTOWER_DASHBOARD_PID`).
2. Opens your browser to the URL (`webbrowser.open`).
3. Prints the URL and returns immediately.

Flags:

- `--no-open` — ensure the server is up, but don't open a browser.
- `--stop` — stop the background dashboard server (via the pidfile).
- `--foreground` — run the server in the foreground (blocking), for debugging.
- `--once` — handle a single request then exit (used by the test suite).
- `--host` / `--port` — bind address (default `127.0.0.1:8787`, local-first).

The watcher can host it too: `wt start --dashboard` brings the dashboard up
alongside the watcher in one process (`--host`/`--port` apply there as well).

**The page** (stdlib-only `http.server`, no dependencies, auto-refreshes ~5s):

- A **tower** header — the `WatchTower` wordmark with a beacon dot (calm green
  normally, amber when any queue is stuck) and a mono fleet summary
  (`3 queues · 1 stuck · 2 workers live`).
- A responsive **instrument grid** — one card per queue with a big mono readout
  (`7 open · empty in ~14m`, `STALLED`, or `clear`), a slim drain bar, and a
  live-worker count. A **stuck** card gets an amber accent border and a slow
  pulsing beacon glow (respecting `prefers-reduced-motion`).
- A **workers** section — worker id (mono), queue, activity (`→ SHIP-3 (4m)` or
  `idle`), and a LIVE / DEAD pill.
- **Drill-down** — clicking a queue card opens `/q/<queue>`, listing that
  queue's active tickets (ref / status / worker / title) and, below them, a
  **Closed** section where each row shows its resolution summary + chips.
- An **empty state** that reads as an invitation, not an error: a dim beacon and
  "All queues clear".

It also exposes read-only JSON:

- `GET /api/status` — health rows (each annotated with worker counts) + the
  worker roster.
- `GET /api/queues` — per-queue counts (mirrors `wt queues`).
- `GET /api/queue/<name>` — the active tickets in one queue plus a `closed`
  array (each closed item carries its `resolution`).

### Resolutions: record HOW a ticket was fixed

Closing a ticket is also where a worker records *what it did* — the trust-layer
signal that turns a drained queue into an auditable log:

```bash
wt close DEMO-1 --summary "rewrote the flex container" \
         --caveat "watch the sticky footer on Safari" \
         --follow-up "add a visual regression test" \
         --unresolved "the print stylesheet still clips"
```

`--summary` is the one-liner; `--caveat`, `--follow-up`, and `--unresolved` are
repeatable. All are optional — `wt close DEMO-1` with no flags still works. The
resolution is stored on the item (`item["resolution"]`) and surfaced in the
dashboard's per-queue **Closed** section and on closed `wt ls` rows. Spawned
workers are instructed to always pass `--summary` so nothing closes silently.

Pass `--enqueue-follow-ups` to also file each follow-up / unresolved item as a
new open ticket in the same queue (opt-in), so loose ends don't get lost.

The drill-down page (`/q/<queue>`) lists active tickets, then a **Closed**
section (most-recent first) where each row shows its summary with small chips for
caveats / follow-ups / unresolved. `GET /api/queue/<name>` returns both the
active `tickets` and the `closed` array (closed items carry their `resolution`).

### The signature feature: `wt wait`

`wt wait -q DEMO` blocks (polling) until the queue has zero open tickets, then
exits 0 — optionally running a `--cmd` afterward. Drop it at the end of a script
to gate on a fleet of agents finishing their work.

## Where the queue lives

WatchTower resolves its store in this order:

1. `$WATCHTOWER_STORE` — explicit override (used by tests/CI).
2. The existing CCC store at `~/.claude/command-center/ux-fixes-queue.json`
   **if it already exists** on this machine — so WatchTower drains real work
   today without migration.
3. `~/.watchtower/queues.json` — WatchTower's own default.

Tracked workers live in `~/.watchtower/workers.json`; the watcher daemon's
pidfile is `~/.watchtower/daemon.pid`, and the background dashboard server's is
`~/.watchtower/dashboard.pid`.

## Stuck detection

A queue is **stuck** when it has open tickets AND no ticket has been closed in
the last 10 minutes. This is pure queue ground-truth — WatchTower decides from
the queue file alone, with no dependency on any external liveness signal.

## Roadmap

- **Tap-to-act / buttons** — claim, close, or requeue tickets from the dashboard UI.
- **Cost / token tracking** — per-queue and per-worker spend.
- **Push notifications** — alert on a queue going stuck or draining.

## License

MIT — see [LICENSE](LICENSE).

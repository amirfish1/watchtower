# WatchTower

WatchTower is a queue-focused, CLI-first tool for running fleets of AI
coding-agent workers unattended and knowing, at a glance, which queues are
stuck. You file tickets into named queues, point workers at them to drain the
work, and `wt status` tells you which queues have open work that nobody is
making progress on. The engine is a single durable JSON queue (append-only,
file-locked) — stdlib-only Python, no runtime dependencies.

## Install

```bash
pip install -e .        # installs the `wt` console command (package: wt-agent)
```

Requires Python 3.11+.

## Usage

```bash
wt status                              # per-queue depth, oldest-open age, stuck flag
wt queues                              # list queues + open/wip/done counts

wt enqueue -q DEMO --title "Fix nav" --note "navbar overlaps on mobile" \
           --text "Full detail: where it shows up, suggested fix."

wt claim  -q DEMO                      # claim the oldest open ticket (atomic)
wt next   -q DEMO                      # alias for claim
wt close  DEMO-1                       # close a ticket by ref

wt workers                             # list worker subprocesses this CLI started
wt spawn-worker -q DEMO --n 2 --engine claude   # launch 2 draining workers
wt wait   -q DEMO --timeout 600 --cmd "say done" # block until drained, then run cmd

wt start  --auto-spawn                 # background watcher: log/auto-handle stuck queues
wt stop                                # stop the watcher
wt dashboard --port 8787               # phone-first HTTP dashboard (queues + workers)
```

`wt status` shows, per queue, depth (open) / WIP / done, oldest-open age, idle
time, a `WORKERS` column (`total (n live)`), and a STUCK/draining/ok flag — then
a workers section listing each tracked worker's id, queue, pid, and LIVE/DEAD
state (process liveness via `os.kill(pid, 0)`).

### The dashboard: `wt dashboard`

```bash
wt dashboard [--port 8787] [--host 127.0.0.1] [--once]
```

A read-only, mobile-first HTTP view over the same queue engine — stdlib-only
(`http.server` + `json`), no dependencies. The page auto-refreshes (~5s) and
shows one card per queue (depth, oldest-open age, live workers, and a
STUCK/LIVE/clear badge), plus a workers section. Empty state reads
"All queues clear." It also exposes read-only JSON:

- `GET /api/status` — health rows (each annotated with worker counts) + the
  worker roster.
- `GET /api/queues` — per-queue counts (mirrors `wt queues`).

Binds `127.0.0.1` by default (local-first). `--once` handles a single request
then exits (used by the test suite).

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
pidfile is `~/.watchtower/daemon.pid`.

## Stuck detection

A queue is **stuck** when it has open tickets AND no ticket has been closed in
the last 10 minutes. This is pure queue ground-truth — WatchTower decides from
the queue file alone, with no dependency on any external liveness signal.

## Roadmap

- Phase 1: the `wt` CLI and queue engine.
- Phase 2 (now): `wt dashboard` — a read-only HTTP viewer over the same queue.
- Later: CCC becomes just one WatchTower client.

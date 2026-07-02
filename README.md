# WatchTower

[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**Run fleets of AI coding-agent workers, unattended.** File tickets into named queues. Workers drain them automatically. `wt status` shows which queues are stuck — nothing more, nothing less.

The engine is a single durable JSON queue (append-only, file-locked) — stdlib-only Python, zero runtime dependencies.

## Quick start (3 steps)

**Requirements:** Python 3.11+, macOS or Linux, [Claude Code](https://claude.ai/code) CLI.

```bash
# 1. Install
git clone https://github.com/amirfish1/watchtower.git
cd watchtower && pip install -e .

# 2. Create a queue, point it at your repo
wt set -q MYAPP --repo-path /path/to/your/repo --engine claude
wt drain on MYAPP      # enable auto-drain + install the service

# 3. File your first ticket
wt add -q MYAPP --title "Fix the login page" --type bug
```

That's it. A Claude worker spawns automatically in `/path/to/your/repo` and starts draining. Open the dashboard to watch:

```bash
wt dashboard    # opens http://127.0.0.1:8787 in your browser
```

## Install

```bash
git clone https://github.com/amirfish1/watchtower.git
cd watchtower
pip install -e .     # installs the `wt` CLI
wt --version
```

### Service (background watcher + reconciler)

```bash
wt install      # install as a macOS LaunchAgent — auto-starts on login
wt uninstall    # remove it
```

After `wt install`, the watcher starts on every login and restarts on crash. No need to run `wt start` manually.

Manual start (without LaunchAgent):
```bash
wt start    # foreground-detached; survives the shell
wt stop     # stop it
wt status   # check service + queue health
```

### Browser annotation widget (optional)

Drop `contrib/annotate-widget.js` into your project to let users file tickets directly from the browser with a single click. See [`contrib/annotate-widget.md`](contrib/annotate-widget.md).

## Usage

```bash
wt status                              # per-queue depth, oldest-open age, stuck flag
wt ls -q DEMO                          # list tickets in a queue

wt add -q DEMO --title "Fix nav" \
       --text "navbar overlaps on mobile" --type bug

wt claim  -q DEMO                      # claim the next open ticket (smart sort)
wt claim  -q DEMO DEMO-1               # claim a specific ticket by ref
wt run    DEMO-1                       # mark an existing GitHub issue runnable
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

### Agent engines

WatchTower supports two agent engines. Set the engine per queue with `wt set`:

```bash
wt set -q MYAPP --engine claude    # default — Claude Code (stream-json, FIFO, live)
wt set -q MYAPP --engine codex     # OpenAI Codex (one-shot exec)
```

The engine is stored in `~/.watchtower/queue-config.json` and picked up by the
reconciler on the next spawn. Existing live workers are unaffected until they
are reaped and a fresh one is started.

#### `claude` (default)

Requires the [Claude Code CLI](https://claude.ai/code) (`claude` on `$PATH`).

Workers are spawned as:

```
claude -p --input-format stream-json --output-format stream-json \
       --verbose --name "<QUEUE> queue worker" --permission-mode bypassPermissions
```

The drain goal is delivered as the **first stream-json message** on a named pipe
(FIFO) that is the worker's stdin. This makes the worker a **live, pushable
process**: when `wt add` files a new ticket while the worker is idle, the
message arrives on the FIFO immediately — no polling, no sleep loop, warm context
preserved. The worker's prompt cache stays hot for ~5 minutes after the last
claim (Anthropic's cache TTL), so tickets that arrive in that window are
cheaper and faster to handle.

The reconciler reaps workers idle longer than 5 minutes (cold cache) and spawns
a fresh one on the next tick rather than waking a worker whose context would be
re-read uncached.

#### `codex`

Requires the [OpenAI Codex CLI](https://github.com/openai/codex) (`codex` on
`$PATH`).

Workers are spawned as:

```
codex exec <drain-goal>
```

The drain goal is passed directly as the command-line argument (one-shot exec).
There is no FIFO stdin channel and no live push notification: the worker drains
until the queue is empty, then exits. New tickets filed while it is running are
picked up on the next `wt claim` loop iteration. `wt add` notifications are not
delivered to a running codex worker.

#### Comparison

| | `claude` | `codex` |
|---|---|---|
| Spawn mode | stream-json over FIFO | `exec <goal>` |
| Live push (`wt add`) | yes — via FIFO | no |
| Prompt cache warm wake | yes (~5 min) | no |
| Multi-ticket session | yes — one process, warm ctx | one process, warm ctx |
| Requires | Claude Code CLI | OpenAI Codex CLI |

### GitHub Issues backend (optional)

A queue can use GitHub Issues instead of the local JSON queue file. WatchTower
still exposes the same commands; behind the scenes `wt add` creates an issue,
`wt claim` assigns it, and `wt close` closes it with a resolution comment.

```bash
gh auth login
wt set -q MYAPP --backend github --github-repo owner/repo

wt add -q MYAPP --title "Fix checkout" --text "Steps to reproduce..."
wt run MYAPP-123                       # opt an existing issue into automation
wt claim -q MYAPP --worker worker-1
wt close MYAPP-123 --worker worker-1 --summary "fixed the null state"
```

GitHub-backed refs use the issue number (`MYAPP-123` is issue `#123`). A
GitHub-backed queue lists open issues from the configured repository so GitHub is
the visible storage layer. The `watchtower:<QUEUE>` label is the automation gate:
unlabeled open issues are visible in `wt ls`, `wt status`, and the dashboard, but
workers will not claim them. Use `wt run MYAPP-123` (or the dashboard Run action)
to add that label and dispatch the queue. Claims assign the issue to `@me` by
default; override with `wt set -q MYAPP --github-assignee USERNAME`.

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

## Cross-agent messaging

Queues move work; messaging moves conversation. WatchTower can push a message
to any agent session, ask one a question and wait for the answer, and run
multi-agent group chats where the daemon (deterministically, no LLM
moderator) nudges the right participants to respond.

```bash
wt send @planner "heads up: schema changed, re-read models.py"
wt ask  @reviewer "is PR 42 safe to merge?" --timeout 60
wt agents                        # named registry + workers + last-3-days sessions
wt agent register planner --session <uuid>   # set-name works as an alias

wt chat new "release plan" --with @planner,@reviewer --include-human
wt chat post <ref> "let's cut v0.2 tonight"
wt chat read <ref> --tail 20
wt chat ls / add / leave / nudge / archive / close
```

Targets are a worker id, a registered `@name`, or any session UUID (or unique
prefix). Any session on disk is reachable; liveness is not required.

Delivery falls through three adapters:

1. **fifo**: the target is a live WatchTower worker, message goes straight
   down its stdin pipe.
2. **resume**: `claude --resume` headless, with a busy-hold: if the target's
   transcript changed in the last 120s the message waits in a durable outbox
   and is delivered the moment the session goes quiet (never forks a parallel
   turn).
3. **delegate** (optional): an HTTP endpoint (`$WATCHTOWER_DELEGATE_URL`, or
   auto-detected from a local Claude Command Center) for instant injection
   into live terminal tabs and for non-Claude engines. Unset and undetected
   means fully standalone; `off` disables it explicitly.

Undeliverable messages persist in `~/.watchtower/outbox.json` and are retried
by the daemon with backoff (dead-lettered after 20 attempts, visible in the
activity log).

Group chats are file-compatible with CCC group chats (same markdown + JSON
sidecar under `~/.claude/group-chats` when present), so both tools can serve
the same conversations. Per-engine feasibility ground truth lives in
[`docs/engine-capability-matrix.md`](docs/engine-capability-matrix.md).

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
- **Cross-queue ticket lookup** (`wt find <ref>`) plus a `watchtower` skill
  for Claude Code and Codex (both harnesses, one source), so any agent
  session can resolve a ref like `HERMES-20` — regardless of which repo
  it's rooted in — without first knowing which queue it lives in. An MCP
  server is the longer-term structured-API follow-up. See
  [`docs/agent-discovery.md`](docs/agent-discovery.md) for the full option
  comparison.

## More docs

- [`docs/worker-lifecycle.md`](docs/worker-lifecycle.md) — vocabulary, service
  lifecycle, worker lifecycle.
- [`docs/agent-discovery.md`](docs/agent-discovery.md) — how an agent session
  (Claude Code, Codex, etc.) outside this repo can discover and query
  WatchTower; 4 options compared with a recommendation.

## License

MIT — see [LICENSE](LICENSE).

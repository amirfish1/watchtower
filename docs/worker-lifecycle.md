# WatchTower — design reference

Single source of truth for vocabulary, service lifecycle, worker lifecycle, and
internal implementation notes. README covers the user-facing CLI; this doc
covers the why and the how.

---

## Vocabulary

### Things you work with

| Term | Definition |
|------|-----------|
| **Queue** | A named collection of tickets, declared in the registry. Each queue has a policy (`auto_drain`, `desired_workers`, backend). |
| **Ticket** | One unit of work. Lives in the queue store. Referred to as `item` in the raw JSON; `ticket` in docs and CLI output. |
| **Ref** | Unique ticket identifier: `<PROJECT>-<N>` (e.g. `WT-27`, `CCC-338`). Stable once assigned; never reused. |
| **Registry** | Declared queue metadata: name, backend, owner, `auto_drain`, `desired_workers`. Stored at `~/.watchtower/queue-registry.json`. Queues exist independently of whether they have tickets. |
| **Resolution** | What a worker reports when a ticket is done: a required `summary` plus optional `caveats`, `follow_ups`, `unresolved` items. Stored on the ticket; surfaced in the dashboard and `wt ls`. |

### Ticket states

```
open  →  in_progress  →  closed
                ↕
           (needs_input flag — not a state, a flag on in_progress)
```

| State | Meaning |
|-------|---------|
| `open` | Unclaimed. Available for the next `wt claim`. |
| `in_progress` | Claimed by a worker. Has a `claimed_session_id`. Not reclaimable by another worker. |
| `closed` | Done. Has a resolution. Immutable. |

`needs_input` is a flag on an `in_progress` ticket — NOT a fourth state. A
ticket stays `in_progress` while blocked; the flag signals that it is waiting
for human input before the worker can continue. Keeping it a flag (not a state)
prevents agents from using it as a comfortable parking lot for hard tickets.

### Ticket operations (user-facing)

| Verb | Command | Who does it | Meaning |
|------|---------|-------------|---------|
| Enqueue | `wt enqueue` | Human / CI | File a new ticket. |
| Claim | `wt claim` | Worker | Atomically take the oldest open ticket. |
| Release | `wt release <ref>` | Worker | Give up a claim without closing it -- back to `open` for the pool. No-op if the ticket isn't `in_progress`. |
| Block | `wt block <ref>` | Worker | Park a ticket that needs a human decision. Sets `needs_input` + `block_question`. |
| Answer | `wt answer <ref> "..."` | Human | Provide input to unblock a blocked ticket. Clears `needs_input`. |
| Discuss | `wt discuss <ref>` | Human | Resume the blocked ticket's worker session (`claude --resume <sid>`). |
| Close | `wt close <ref>` | Worker | Mark a ticket done. `--summary` is required; `--caveat/--follow-up/--unresolved` optional. |

> **Naming note (open):** `close` for tickets vs `stop` for the service — two
> different nouns, but the words are close. Candidate rename: `wt resolve <ref>`
> for tickets, reserving `stop` purely for the daemon/service. Not yet decided.

---

## Service lifecycle

The **WatchTower service** is the reconciler daemon. It has nothing to do with
tickets — it manages the fleet of workers.

| Command | Effect |
|---------|--------|
| `wt start` | Start the reconciler daemon (loops `reconcile_once()` every 30 s). |
| `wt start --dashboard` | Start daemon + dashboard server together. |
| `wt stop` | Stop the reconciler daemon. *(not yet built)* |
| `wt dashboard` | Start the dashboard HTTP server (detached). |
| `wt dashboard --stop` | Stop the dashboard server. |

The daemon is optional. Without it, queues accumulate tickets and workers must
be spawned manually (or via the watcher's simpler auto-spawn). With the
reconciler running, `auto_drain` queues drain automatically.

---

## Worker lifecycle

A **worker** is a subprocess running a headless agent CLI (`claude -p ...` or
`codex exec ...`). It is not a user — it is a tool the daemon uses to drain a
queue. Worker processes are ephemeral, while queue staffing state is durable in
`workers.json`: a released conversation
remains recorded so it cannot silently rejoin the queue after consuming its
one-shot stop sentinel.

### Engines

Each queue has an **engine** setting (default `claude`) that controls how
workers are spawned. Set it with `wt set -q <QUEUE> --engine <ENGINE>`.

| Engine | Spawn command | Live push | Prompt cache |
|--------|--------------|-----------|--------------|
| `claude` | `claude -p --input-format stream-json ...` | yes (FIFO) | ~5 min warm |
| `codex` | `codex exec <goal>` | no | n/a |

**`claude`** (default) — requires the Claude Code CLI.

The worker's stdin is a named pipe (FIFO). The drain goal arrives as the first
stream-json user message; subsequent `wt add` notifications push new messages
on the same channel. The worker stays alive between tickets, so its prompt cache
(Anthropic's 5-minute TTL) covers tickets filed within that window: they are
cheaper and faster than a cold start. Cache warmth is separate from staffing:
after 30 minutes of verified inactivity, the reconciler gracefully releases the
conversation from this queue without killing it.

**`codex`** — requires the OpenAI Codex CLI.

Workers are spawned as `codex exec <drain-goal>`. The goal text is in argv;
there is no FIFO and no live push channel. The worker drains until the queue is
empty and then exits. New tickets filed while it is running are picked up on the
next `wt claim` iteration inside the same process.

### Normal cycles

```
reconciler spawns Claude worker
  └─ FIFO-backed worker loop:
       wt claim → ticket → do work → wt close --summary "..."
       wt claim → ticket → ...
       wt claim → empty  → idle audit → end turn (no polling)
  └─ a new FIFO message wakes the warm conversation
  └─ after 30m verified idle, reconciler releases it from queue staffing

reconciler spawns Codex worker
  └─ one-shot worker loop:
       wt claim → ticket → do work → wt close --summary "..."
       wt claim → ticket → ...
       wt claim → empty  → idle audit → complete drain goal → exit immediately
  └─ reconciler spawns a new process when later work needs staffing
```

### Blocked cycle (needs human input)

```
worker reaches a decision it can't make alone
  └─ wt block <ref> --question "..." --progress "analysis so far"
       ticket: still in_progress, still bound to this session
       needs_input = true, block_question set
  └─ worker moves to next ticket, then follows its engine's empty-queue lifecycle
  └─ human sees blocked ticket in CCC or `wt blocked`
  └─ human answers: `wt answer <ref> "decision"` OR `wt discuss <ref>`
       answer appended to ticket, needs_input cleared
  └─ worker's session is resumable; it picks up where it left off
```

The blocked ticket stays `in_progress` and is NOT reclaimable. Continuity lives
in the resumable session, not in a running process.

### Resolution is mandatory

`wt close` rejects a close with no `--summary` or completion proof (exit 1).
Code-changing work must provide `--commit <SHA>`, which is verified in the
ticket's repository; non-code work must explicitly provide `--no-code`. Workers
are instructed to block work with progress when a verified change cannot be
committed, rather than closing it with a follow-up. The resolution is the trust
signal that turns a drained queue into an auditable log.

```bash
wt close REF --summary "what changed" --commit <SHA>
             --caveat "watch X"          # repeatable
             --follow-up "do Y next"     # repeatable
             --unresolved "Z still open" # repeatable
```

### Queue-scoped release

```
worker crosses the 30-minute inactivity floor
  └─ reconciler snapshots PID, WT stdout, engine transcript/rollout, and ownership
  └─ strict queue read confirms no ticket is owned by worker ID or session ID
  └─ complete IDLE_CANDIDATE / IDLE_SIGNAL / IDLE_DECISION bundle is logged
  └─ released_at is persisted and a one-shot stop sentinel is written
  └─ queue-scoped release instruction is attempted over FIFO/session delivery
  └─ worker's next wt claim returns {"stop": true}
  └─ process and unrelated conversation work continue untouched
```

Unknown or unreadable evidence fails closed. A worker is preserved when it owns
in-progress or blocked work, the strict queue read fails, its PID identity is
not attributable, or required activity evidence is unavailable. WatchTower
never sends `SIGTERM` or `SIGKILL` as part of normal queue release.

### Engine-specific idle behavior

When `wt claim` returns empty, neither engine polls or sleep-loops. A Claude
worker ends its turn and remains blocked on its live FIFO, so a later ticket can
wake the same conversation. Its prompt cache is typically warm for about five
minutes, but cache warmth does not control staffing; the reconciler may release
the conversation after 30 minutes of verified inactivity. A Codex exec worker
has no FIFO, so after its idle audit it completes the active queue-drain goal and
exits immediately. A wind-down STOP makes either engine exit between tickets.

---

## Auditable idle decisions

The unified activity log at `~/.watchtower/activity.log` contains the evidence,
decision, release, and any replacement spawn. Stable `key=value` fields allow
an operator to reconstruct the transaction without reading a worker transcript.

| Verb | Meaning |
|------|---------|
| `IDLE_CANDIDATE` | Identity, release floor, and newest effective activity clock for a worker that crossed the floor. |
| `IDLE_SIGNAL` | One line per safety signal: PID, WT stdout, Claude transcript/Codex rollout/Kimi wire log, queue-read result, owned and blocked refs, and `pid_signal_planned=false`. |
| `IDLE_DECISION` | Exactly one `PRESERVE` (with every reason) or `RELEASE` result for an evaluation. |
| `ACTIVE_AGAIN` | A prior idle candidate received newer authoritative activity and fell below the floor. |
| `RELEASE` | Durable detachment, delivery outcome, sentinel state, and `pid_signalled=false`. |
| `SPAWN_PLAN` | Claimable depth, before/after staffing, releases, deficit, requested count, and cause for one reconcile pass. |
| `SPAWN` / `SPAWN_FAIL` | Launch result correlated to its reconcile pass and any replacement release. |

Every evaluation has an `evaluation_id`, every release a `release_id`, and every
reconcile pass a `reconcile_id`. The worker record stores the last evidence
fingerprint and decision, so identical 30-second ticks are silent. Changed
evidence emits a fresh complete bundle; daemon restart may emit one new
snapshot.

Spawn causes are `initial_staffing`, `scale_up`, `release_replacement`,
`dead_worker_recovery`, and `manual_or_run_once`. Only
`release_replacement` spawns carry related release and previous-worker IDs. In
a mixed deficit, only the incremental slot created by a same-pass release gets
that cause; pre-existing deficit slots retain their initial, scale-up, or
dead-worker-recovery cause.

Failed final `RELEASE` appends and failed release-correlated `SPAWN_PLAN`
appends are retained in the worker's lifecycle audit state and retried on later
passes. Replacement spawning waits until its causal plan is durably logged.

## Activity log — SPAWN vs DISPATCH

The activity log at `~/.watchtower/activity.log` records two related but distinct
events when a new ticket is filed:

| Verb | Who emits it | What it means |
|------|-------------|---------------|
| **SPAWN_PLAN** | `reconcile_once()` / reconciler | The staffing calculation, including cause and release correlation. A release with no claimable work records `requested=0`. |
| **SPAWN** | `reconcile_once()` / reconciler | A new worker process was created, labeled with cause and reconcile ID. |
| **DISPATCH** | `dispatch_after_enqueue()` | The routing decision for *this ticket* — what happened to ensure it gets worked. One line per ticket, one of: nudged an existing worker, spawned new worker(s), or queued as backlog. |

**Why do I see two SPAWN lines for one ticket?**  
The queue's `desired_workers` setting controls how many workers the reconciler
may run concurrently, but launches are capped by unclaimed claimable tickets.
With `desired_workers=2`, zero live workers, and one claimable ticket, only one
worker starts. Two SPAWN lines require at least two unclaimed tickets. The
DISPATCH line records which workers the enqueue was routed to.

To reduce to 1 worker per queue: `wt set -q <QUEUE> --desired-workers 1`.

**Example sequence** for a queue with `desired_workers=2`, 0 live workers, 1 ticket:
```
ENQUEUE     CCC-461   Command Center ticket
SPAWN_PLAN  reconcile_id=reconcile-a1 claimable_depth=1 requested=1 cause=initial_staffing
SPAWN       worker_id=ccc-abc1 reconcile_id=reconcile-a1 cause=initial_staffing
DISPATCH    CCC-461   spawned 1 worker: ccc-abc1
```

An extra Claude worker waits on its FIFO until another ticket arrives or it is
gracefully released after 30 minutes of verified inactivity. An extra Codex
worker exits immediately after its empty-queue idle audit.

---

## Logs

`~/.watchtower/logs/` is one shared directory for every queue's process
output — not just WT's. Three kinds of file land there, all raw stdout+stderr
(stream-json for `claude` engine workers, plain text for `codex`):

| Pattern | Written by | What it is |
|---------|-----------|------------|
| `<queue>-<worker8>.log` | `spawn_workers()` (`workers.py`) | A drain worker's full session output — every claim, tool call, and message from spawn to exit. Named `<queue-lower>-<uuid8>`, e.g. `wt-1dcf03a0.log`, `ccc-4b9bd8cf.log`. |
| `<queue>-<worker8>.log.stdin` | `_make_stdin_fifo()` (`workers.py`) | The paired FIFO used to push follow-up messages into a live `claude` worker's stdin (keeps it resumable instead of one-shot). Not a real log — a named pipe, size 0. |
| `msg-<sid8>-<ts>.log` | `send_message()` (`messages.py`) | Output from a resume-adapter message delivered to an existing session (`wt agents`/message routing). |
| `resume-<sid>.log` | `_resume_session_headless()` (`cli.py`) | Output from waking a blocked session with `claude --resume` after `wt answer`. |

There is **no rotation, size cap, or pruning** for any of these today — files
accumulate for as long as the queue has been in use. As of 2026-07-02 the
directory was ~403MB across 147 files; the bulk (~340MB) was `ccc-*` worker
logs, since `claude` engine workers emit full stream-json (every tool-call
payload, not just prose) and CCC has run the most worker-sessions historically.
Safe to delete individual `<queue>-<worker8>.log` files for workers that are no
longer live (check `wt workers` / `~/.watchtower/workers.json` for liveness
first) — nothing reads old logs except a human debugging a dead worker.

---

## INTERNAL — implementation details (ignore unless debugging)

These are not user commands. They are Python functions and file conventions.

### Stop-signal files

`~/.watchtower/stop-signals/<worker_id>` — a sentinel file created by
`request_stop(worker_id)` in `workers.py`. `claim_next()` in `queue.py` checks
for this file before touching the queue; if present, it deletes the file and
returns `{"stop": True}`. The worker reads this and exits. The directory is
overridable via `$WATCHTOWER_STOP_SIGNALS_DIR` for test isolation.

### `reconcile_once(dry_run=False)`

One tick of the reconciler. Called by the `wt start` daemon loop. Per queue:
snapshots eligible staffing, evaluates and persists idle releases, recomputes
eligible staffing, reads claimable depth, records a `SPAWN_PLAN`, and starts
only the capped deficit. A same-pass release plus claimable work is labeled
`release_replacement`; a release with no claimable work records a zero-worker
plan. Returns `spawned`, `released`, `spawn_plans`, `launch_failed`, `skipped`,
and related maintenance results. In `dry_run` mode, no subprocesses or releases
occur; staffing plans and synthetic spawn records are still returned and
logged for tests.

### `request_stop(worker_id)`

Creates the stop-signal sentinel, then persists `released_at` under the
workers-file lock. If durable persistence fails, the new sentinel is removed so
staffing remains attached and the next evaluation can retry. The sentinel is
only the next-claim transport; durable detachment in `workers.json` remains
after the worker consumes it.

### `claim_next(queue, worker_id, ...)`

Checks for a stop-signal file first (before acquiring the queue lock). If
found: deletes file, returns `{"stop": True}`. Otherwise: acquires lock, finds
oldest open ticket matching the queue, stamps it `in_progress`, returns it. All
atomic under `_FileLock`.

### Workers file

`~/.watchtower/workers.json` — PID + metadata for workers THIS CLI spawned.
Liveness is process-level (`os.kill(pid, 0)`). Dead workers are pruned on reads
with `prune=True`. Live records may carry `released_at` plus a
`lifecycle_audit` fingerprint, previous decision/evaluation ID, effective
activity timestamp, and log timestamp. This state suppresses duplicate audit
bundles and keeps a released conversation out of eligible staffing.

### `_CCC_LEGACY_STORE`

WatchTower resolves its queue store base path in order: `$WATCHTOWER_STORE` →
`~/.claude/command-center/ux-fixes-queue.json` (if it or its `.db` exists) →
`~/.watchtower/queues.json`. The middle path is the CCC legacy store — this
lets WatchTower drain real CCC work without migration (WT-26 Phase 0). The
authoritative file is the base path with a `.db` suffix once the SQLite
migration has run (2026-08-20 spec).

---

## Open questions

- **`wt close` vs `wt resolve`** — should ticket close be renamed to `resolve`
  to avoid collision with service `stop`? Not yet decided.
- **`item` vs `ticket`** — the store says `items`; the CLI and docs say
  `tickets`. Should standardize on `ticket` everywhere.
- **`desired_workers > 1`** — registry field for parallel drain. Exists in
  design; reconciler defaults to 1. Not specced beyond that.
- **Reclaim/abuse thresholds** — how long is `needs_input` "too long"; how many
  bounces is "lazy." Needs real numbers from production data.
- **Per-queue engine override** — today the engine (claude/codex) is a spawn-time
  flag; it could live in the registry instead.

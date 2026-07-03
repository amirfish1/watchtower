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
queue. Workers are ephemeral and stateless outside the queue.

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
cheaper and faster than a cold start. Workers idle past 5 minutes are reaped and
replaced with a fresh process on the next reconciler tick.

**`codex`** — requires the OpenAI Codex CLI.

Workers are spawned as `codex exec <drain-goal>`. The goal text is in argv;
there is no FIFO and no live push channel. The worker drains until the queue is
empty and then exits. New tickets filed while it is running are picked up on the
next `wt claim` iteration inside the same process.

### Normal cycle

```
reconciler spawns worker
  └─ worker loop:
       wt claim → ticket → do work → wt close --summary "..."
       wt claim → ticket → ...
       wt claim → empty  → idle 2 min, re-poll
       wt claim → empty  → idle 2 min, re-poll (up to 5 min total)
       wt claim → empty  → self-exit (warm-idle TTL reached)
  └─ reconciler sees actual < desired → respawn if work returns
```

### Blocked cycle (needs human input)

```
worker reaches a decision it can't make alone
  └─ wt block <ref> --question "..." --progress "analysis so far"
       ticket: still in_progress, still bound to this session
       needs_input = true, block_question set
  └─ worker moves to next ticket (or self-exits if nothing else)
  └─ human sees blocked ticket in CCC or `wt blocked`
  └─ human answers: `wt answer <ref> "decision"` OR `wt discuss <ref>`
       answer appended to ticket, needs_input cleared
  └─ worker's session is resumable; it picks up where it left off
```

The blocked ticket stays `in_progress` and is NOT reclaimable. Continuity lives
in the resumable session, not in a running process.

### Resolution is mandatory

`wt close` rejects a close with no `--summary` (exit 1). Workers are instructed
in their goal prompt to never close silently. The resolution is the trust signal
that turns a drained queue into an auditable log.

```bash
wt close REF --summary "what changed"
             --caveat "watch X"          # repeatable
             --follow-up "do Y next"     # repeatable
             --unresolved "Z still open" # repeatable
```

### Wind-down (reconciler asked the worker to stop)

```
reconciler: actual > desired
  └─ request_stop(worker_id)          # writes a sentinel file (INTERNAL)
  └─ worker's next wt claim returns {"stop": true}
  └─ worker exits cleanly — no ticket abandoned, it only saw STOP between tickets
```

Cost: wind-down waits up to one ticket's duration (the worker finishes what it
started). That is intentional.

### Warm-idle window

When `wt claim` returns empty (nothing open), a worker does NOT exit
immediately. It re-polls every 2 minutes for up to 5 minutes total, then
self-exits. Rationale: Anthropic's prompt cache has a 5-minute TTL — a ticket
arriving in that window reuses the warm cache (cheaper, faster) rather than
paying a cold start. A wind-down STOP bypasses the warm window.

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
computes `desired = desired_workers if (auto_drain and depth > 0) else 0`,
compares to `live_worker_count(queue)`, calls `spawn_workers()` or
`request_stop()` to match. Returns `{spawned: [...], stopped: [...], skipped:
[...]}`. In `dry_run` mode, skips all I/O — used by tests.

### `request_stop(worker_id)`

Creates the stop-signal sentinel file. Does NOT touch `workers.json` (avoids
write races between the reconciler and the claim path).

### `claim_next(queue, worker_id, ...)`

Checks for a stop-signal file first (before acquiring the queue lock). If
found: deletes file, returns `{"stop": True}`. Otherwise: acquires lock, finds
oldest open ticket matching the queue, stamps it `in_progress`, returns it. All
atomic under `_FileLock`.

### Workers file

`~/.watchtower/workers.json` — PID + metadata for workers THIS CLI spawned.
Liveness is process-level (`os.kill(pid, 0)`). Pruned on read when `prune=True`
(dead workers removed). Distinct from queue state — a worker can be alive in the
workers file but idle (no in-progress ticket), or have an in-progress ticket but
no tracked worker (if spawned by a different CLI invocation).

### `_CCC_LEGACY_STORE`

WatchTower resolves its queue store in order: `$WATCHTOWER_STORE` →
`~/.claude/command-center/ux-fixes-queue.json` (if it exists) →
`~/.watchtower/queues.json`. The middle path is the CCC legacy store — this
lets WatchTower drain real CCC work without migration (WT-26 Phase 0).

---

## Open questions

- **`wt close` vs `wt resolve`** — should ticket close be renamed to `resolve`
  to avoid collision with service `stop`? Not yet decided.
- **`item` vs `ticket`** — the JSON store says `items`; the CLI and docs say
  `tickets`. Should standardize on `ticket` everywhere.
- **`desired_workers > 1`** — registry field for parallel drain. Exists in
  design; reconciler defaults to 1. Not specced beyond that.
- **Reclaim/abuse thresholds** — how long is `needs_input` "too long"; how many
  bounces is "lazy." Needs real numbers from production data.
- **Per-queue engine override** — today the engine (claude/codex) is a spawn-time
  flag; it could live in the registry instead.

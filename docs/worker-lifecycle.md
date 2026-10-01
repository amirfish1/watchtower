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
| `awaiting_answer` | Parked by `wt block` (WT-28): claim released, waiting for a human answer. Not claimable. |
| `closed` | Done. Has a resolution. Immutable. |

`needs_input` is a flag on an `in_progress` ticket (the legacy hold-while-blocked
form, still used by plan-gate / stage-escalation blocks and GitHub-backed queues
until WT-28b) — NOT a separate state. A
ticket stays `in_progress` while blocked; the flag signals that it is waiting
for human input before the worker can continue. Keeping it a flag (not a state)
prevents agents from using it as a comfortable parking lot for hard tickets.

### Ticket operations (user-facing)

| Verb | Command | Who does it | Meaning |
|------|---------|-------------|---------|
| Enqueue | `wt enqueue` | Human / CI | File a new ticket. |
| Claim | `wt claim` | Worker | Atomically take the oldest open ticket. |
| Release | `wt release <ref>` | Worker | Give up a claim without closing it -- back to `open` for the pool. No-op if the ticket isn't `in_progress`. |
| Reopen | `wt reopen <ref> --reason "..."` | Human | Reopen a closed (or in-progress) ticket -- back to `open` for the pool, without marking it run_requested or dispatching. Refused on blocked tickets unless `--force` (same guard as release). |
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
       wt claim → empty  → idle audit → exit immediately
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

### Parking (WT-28)

A worker `wt block` on a local queue now **parks** the ticket: status
`awaiting_answer`, claim released, `parked` = `{worker_id, session_id, machine,
engine, repo_path, transcript_path, at}`. The worker may claim the next ticket
immediately. A retained parked worker is never STOPped for ~55 minutes and does
not consume the spawn budget while idle.

`wt answer` / `wt ack` / `wt plan decide` on a parked ticket write a
`pending_answer` (`gen`-keyed; states `routing -> affinity | delivering | queued
| delivered | handed_off`) and route it through compare-and-swap transitions so
a stale router is a no-op:

| Route | When | Effect |
|---|---|---|
| `affinity` | parked worker still alive | ticket reopens reserved for that worker, which is woken; nothing is bound |
| `resume` | worker gone, session resumable | `resume_claim` rebinds the ticket to the parked session and the answer is delivered to it |
| `reopen` | no session / over the context budget / session busy elsewhere | ticket opens; the next claimer gets question + answer + transcript pointer (`answer_brief`) |

Answer outbox rows are generation-bound (R4-4): the drain re-checks the ticket's
`gen` and cancels on re-block, forced release/reopen and close. Delivery is
at-least-once; the `[answer REF#gen]` tag lets a session ignore a repeat.
`wt migrate-blocks [--dry-run] [-q Q]` (and the reconciler) parks legacy blocks.

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
  └─ no message is sent: the worker is idle and cache-cold, so a message
     would buy a full-price uncached turn just to say "stop" (WATCHTOWER-31)
  └─ worker's next wt claim returns {"stop": true}
  └─ process and unrelated conversation work continue untouched
```

Unknown or unreadable evidence fails closed. A worker is preserved when it owns
in-progress or blocked work, the strict queue read fails, its PID identity is
not attributable, or required activity evidence is unavailable. WatchTower
never sends `SIGTERM` or `SIGKILL` as part of normal queue release.

### Context recycle (claim-time)

`wt claim` answers `{"stop": true, "reason": "context_budget"}` to a
**registered** drain worker (never a human) that holds no active claim and has
hit any limit below; the STOP log line names the limit and value. Each knob is
read per call and `0` disables it.

| Env var | Default | Applies to | Measures |
| --- | --- | --- | --- |
| `WATCHTOWER_CONTEXT_RECYCLE_BYTES` | `2500000` | claude | session transcript bytes (`claude_bytes`) |
| `WATCHTOWER_CODEX_RECYCLE_INPUT_TOKENS` | `30000000` | codex | cumulative `input_tokens` from the rollout's last `token_count` event, cached tokens included (`codex_input_tokens`) |
| `WATCHTOWER_RECYCLE_TICKETS` | `10` | every engine | tickets closed or blocked this run, counted as `tickets_done` on the worker record (`tickets`) |

### Engine-specific idle behavior

When `wt claim` returns empty, neither engine polls or sleep-loops. A Claude
worker ends its turn and remains blocked on its live FIFO, so a later ticket can
wake the same conversation. Its prompt cache is typically warm for about five
minutes, but cache warmth does not control staffing; the reconciler may release
the conversation after 30 minutes of verified inactivity. A Codex exec worker
has no FIFO, so after its idle audit it exits immediately. No native Codex
thread goal is used, and spawns pass `-c model_auto_compact_token_limit=<worker_auto_compact_k*1000>`
(CCC spawn-defaults.json, default 250000). A wind-down STOP makes either engine exit between tickets.

---

## Stage sessions (planner, plan reviewer, verifier, assessor)

Stage sessions are owned by the reconciler, not by whichever build worker ran
`wt claim` / `wt close`. Ticket state is the intent; the CLI only calls
`stages.request()`, which appends to `~/.watchtower/stage-wake` and logs
`STAGE_QUEUED`. Only the one real daemon (`wt start --auto-spawn`, never
`--dry-run`, always under `reconcile.lock`) or a manual `wt stages tick` spawns
or respawns. The daemon sleeps in 0.5 s chunks and runs a stage pass as soon as
the wake file's mtime advances; a lost wake is caught by the next full tick.

Supervision key per stage: `plan:r<round>`, `review:r<round>`,
`verify:<verify_cycle>`, `assess:<assessment.cycle>`. Two attempts per key: the
first death respawns, the second escalates to `needs_input` (`wt answer REF
"retry"` grants a fresh budget). Classified launch failures (auth, quota, engine
missing, rejected model, untrusted cwd) set the shared cooldown and are refunded
(max 3 per key); any other immediate death consumes an attempt and sets no
cooldown. Sessions run under `_exitwrap.py`, which records exit code/signal.
Idle is judged by the claude transcript mtime (claude -p writes nothing to its
log until it exits), other engines by log mtime. Dead stage records are kept
30 min (`WATCHTOWER_ADHOC_DEAD_KEEP_S`). Engine starts are staggered host-wide
(`WATCHTOWER_SPAWN_STAGGER_S`, claude 3 s, others 1 s). The cwd is the ticket's
`repo_path`, else the queue's; never the daemon's.

| Verb | Meaning |
|------|---------|
| `STAGE_QUEUED` | A CLI transition requested a stage; wake file touched. |
| `STAGE_SPAWN` | A stage session started (`attempt n/2`, `cause=initial|respawn|retry|adopted`). |
| `STAGE_DEAD` | A session died or could not start, with the reason and exit forensics. |
| `STAGE_LOST` | A verified plan message to the live stage session never landed; the next pass kills it and respawns (one attempt). |
| `STAGE_WAIT` | Spawn deferred by an engine launch cooldown (logged once). |
| `STAGE_DEFER` | Per-tick spawn cap (`WATCHTOWER_STAGE_SPAWNS_PER_TICK`) reached. |
| `STAGE_BLOCK` | Out of attempts; the ticket is waiting for a human. |
| `ASSESS_RESUME` | An assessor was respawned with a rotated (fenced) token. |
| `SPAWN_GATE` | The host-wide spawn stagger timed out and proceeded. |

### Plan discussion turns (WT-29)

After a plan rejection, the planner<->reviewer discussion runs as stage
sessions, not as messages to the original one-shot sessions (they have
usually exited by then). Each turn has its own key and its own 2-attempt budget:

- `discuss:r<round>:d<n>:planner` while `plan.status == discussing` and the
  discussion awaits the planner (amend the plan, answer the objections).
- `discuss:r<round>:v<version>:reviewer` while `reviewing` and the discussion
  awaits the reviewer (a verdict on that exact version).

`wt plan verdict` / `wt plan submit` only call `stages.request`. The supervisor
adopts the participant if its session is still running (attempt 0, no message);
otherwise it spawns a fresh session that same tick, seeded with the ticket, the
plan, the objections and the last discussion messages. Dying twice escalates
like any stage (`wt answer REF retry` restarts the turn).

Plan messages (`wt plan discuss`, fallback reminders) use the live-only
transport (`messages.send(..., live_only=True)`: UDS, then WT stdin FIFO; never
the delegate, a resume, or the outbox). A send result is never taken as
liveness: `wt plan discuss` to a peer that is not running is recorded in the
transcript and reported as "recorded"; a live send is reported as "sent live;
awaiting its receipt". The reminder/nudge fallback (`recover_plan_discussions`)
only covers tickets outside the supervisor (github-backed or otherwise
unsupervised): a dead peer blocks the plan for a human at once, and a send no
transport took blocks without counting. A reminder a live transport took is
**not** counted yet (WT-31 D4): it is noted as `discussion.reminder =
{delivery_id, role, at}` (no second reminder while it is pending), and the
delivery ledger settles it. Only its confirmed nonce bumps `nudges` toward
`PLAN_DISCUSSION_MAX_NUDGES` (escalating past it); a lost or unverified one
blocks the plan for a human. Both are compare-and-swaps on the pending
`delivery_id`, so a late outcome after the discussion moved on is a no-op.

A lost plan message to the ticket's **live stage session** (`wt plan discuss`
to the participant the supervisor adopted) means that session is not working
the turn: `stages.mark_delivery_lost` sets `stage_session.lost`, and the next
`_supervise` pass kills the session, records the death (`delivery lost: ...`,
one attempt of the 2-attempt budget) and respawns it, or escalates once the
budget is spent. A lost message to anyone else (not the current stage session)
is only recorded; the next turn reads the transcript.

## Auditable idle decisions

The unified activity log at `~/.watchtower/activity.log` contains the evidence,
decision, release, and any replacement spawn. Stable `key=value` fields allow
an operator to reconstruct the transaction without reading a worker transcript.

| Verb | Meaning |
|------|---------|
| `IDLE_CANDIDATE` | Identity, release floor, and newest effective activity clock for a worker that crossed the floor. |
| `IDLE_SIGNAL` | One line per safety signal: PID, WT stdout, Claude transcript/Codex rollout/Kimi wire log/Antigravity conversation db, queue-read result, owned and blocked refs, and `pid_signal_planned=false`. |
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
logged for tests. With `supervise_stages` (and not `dry_run`) the stage pass is
followed by the liveness backstop (`result["backstop"]`, see "Liveness
backstop").

### Launch-failure escalation (spawn-then-die)

A worker that dies before it claims anything is invisible to every other
mechanism here: idle release, zombie release and reaping all reason about
workers that are *alive*. The only thing standing between a broken engine and
an endless respawn loop is the launch-failure ladder in `workers.py`, and it
escalates in four steps as the same failure repeats:

1. **Record + cooldown.** `_wait_for_immediate_launch_failure` (spawn-time,
   `_LAUNCH_FAILURE_GRACE_S`) and `_postmortem_launch_failure` (prune-time, for
   engines that burn minutes before printing the real error) both funnel into
   `_record_launch_failure`, which sets a cooldown the next tick honours.
   Consecutive failures double it up to `_LAUNCH_FAILURE_MAX_COOLDOWN_S`.
2. **Name it if we can.** `_classify_launch_failure_log` maps the log to a
   reason an operator can act on — usage limit, auth, API down, broken binary.
   It can only ever name failures we have already seen, so an unrecognised
   non-zero exit inside the grace window is still recorded, under a generic
   `engine exited immediately (exit N): <last log line>`. That fallback is the load-bearing
   part: WATCHTOWER-29 was an *unclassified* failure (a half-installed codex
   npm package), and "no phrase matched" used to mean "no cooldown at all".
3. **Say so.** At `_LAUNCH_FAILURE_ALERT_STREAK` consecutive failures,
   `_alert_repeated_launch_failure` files a ticket about the outage. A cooldown
   is correct but silent, and silence is what let hermes INTAKE grind for five
   days. The ref is written back onto the failure record, so one outage files
   one ticket however long it lasts; the streak (and the alert) clear when a
   worker finally establishes a session.
4. **Substitute or park.** Only a queue that opted in with
   `wt config -q <queue> --fallback-to-default-worker on` ("Revert to CCC
   default worker if current model is exhausted"; default **off**) gets a
   substitute. `_warrants_engine_swap` decides when: immediately for a usage
   limit, only on a repeat for anything else. `_launch_substitute` then
   launches that tick's workers on `config.fallback_engine()` with
   `config.fallback_model()`, and keeps doing so while the preferred engine's
   cooldown runs. It is a **launch-time substitution only**: the queue's stored
   engine/model are never rewritten (WATCHTOWER-30 — the reconciler used to
   `set_engine`/`set_model`, silently overwriting a user's choice), so the next
   tick after the cooldown tries the preferred engine again. The `FALLBACK`
   log line names the real failure reason. A queue that did not opt in, or has
   no fallback installed, is parked on a repeated failure instead: `auto_drain`
   goes off with the reason logged, and `wt drain on <queue>` resumes it.

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

## Orphan sweep and verifier-rejection reclaim (WT-30)

`requeue_orphaned_tickets` (reconciler tick) reopens an `in_progress` ticket
whose claimer is gone. A verifier/reviewer rejection re-binds the ticket to the
builder's session but **keeps `claimed_by` = the builder's worker id** (the
session id rides in `claimed_session_id`), stamps `resume = {sid, state, at}` and
delivers the rejection to that session.

`workers._resolve_claimer` classifies `claimed_by` as `builder | stage |
unknown`: a known worker id, a `list_workers` row with that session id, the
session-origin ledger (`worker`/`adhoc` = builder, any other role = stage,
terminal), then `worker-sessions.json`. `unknown` (ambient sessions, OPS-104) and
`stage` (WT-24) are never reopened by the sweep.

A builder ticket is **held** while any evidence of life exists: (a) its worker
is alive, (b) a live worker row owns the session, (c) a live resume child in the
resume ledger, (d) the resume is still queued in the outbox, (e) **delegate
transport only**: within `WATCHTOWER_RESUME_START_S` (45 s) of the real delivery
time, or the transcript is busy. For local transports process identity is the
only liveness evidence: a transcript written just before the process died does
**not** hold the ticket (busy protects against forking a parallel resume, not
against reclaiming from a dead process). Otherwise the ticket reopens on that
tick (`REQUEUE <ref> — worker gone (verify-rejected; builder <id> dead)`), with
`gate_feedback` kept for the next claimer (`wt claim` prints it). The reopen
event records `orphan`, `displaced_session_id` and `displaced_claimed_by`, so a
later resume of the displaced session gets the STOP preamble even after a fresh
worker re-claims; a legitimate self-reclaim clears it.

Every resume attempt logs `RESUME <ref> — <transport|queued|headless|failed|skipped>`.
A builder over the context budget is skipped (released with the rejection text);
a total delivery failure releases the claim instead of asking a human to resume
manually. `drain_outbox` writes delivery / dead-letter outcomes back to the
ticket's `resume` state, and the sweep also reads the outbox row itself.

## Sent-back claims (WT-34)

A rejection (`wt reject`, verifier FAIL, `wt reopen --resume`) re-binds the
ticket to its builder and stamps `sent_back` (`at`, `by`, `worker_id`,
`session_id`, `reason`, `hist_from`, `handed_back_at`, `progress_at`). While the
marker is live:

- **Hand-back first.** `wt claim` by that worker id or session returns the
  sent-back ticket (`HANDED BACK:` / `handed_back: true`, idempotent) instead of
  a new one; `wt claim <other-ref>` is refused with the rejection text. The
  decision is made in the same lock as the claim.
- **Age bound.** With no claimer `comment`/`progress`/`block`/`park`/`in_review`
  event after the send-back, `reconcile_once` releases it to the pool after
  `sent_back_release_min` minutes (default 30, `wt config -q Q
  --sent-back-release-min N`, 0 disables; the clock restarts once at the first
  hand-back; a `failed` resume shortens it to 5 min). The release keeps
  `gate_feedback`, records `sent_back_released` (the next claimer sees the
  rejection text; the former holder may `wt claim` it back) and sorts the ticket
  ahead of same-priority peers. The orphan sweep still owns dead sessions.
- **Nudge.** The stuck-queue nudge names each held sent-back ticket, its age and
  the release time; those holders are excluded from the generic broadcast.
- `wt block`/park, close, release and any new claim clear the marker.

Close ownership (already-closed, foreign in_progress, foreign parked, and a
holder whose claim was released) is enforced in the store lock on every mutating
outcome of a worker-attributed `wt close`, including the failed-gate reopen,
which also compare-and-swaps on the pre-gate status.

## Liveness state table (WT-31)

`watchtower/liveness.py` is the single state table: `project(item)` maps a
ticket to a `State` over the dims `status, gated, backend, plan, disc, awaiting,
gate_pending, assessment, block, readiness, dep, claimed, parked, answer`, and
`classify(state)` returns the one `Row` it is in (`id`, `owner`, `proof`,
`recover`), or raises when the state is in `UNREACHABLE` or in no row.
`prove(item, row)` names what proves the owner is at work (for stage rows, the
exact `stages.desired()` role and key).

Answer states (`pending_answer.state`, vocab `queue.ANSWER_STATES`) move only
along `queue.ANSWER_TRANSITIONS` (E1–E17). Each edge declares `from`, `to`
(`"="` = self-loop), the `(old_status, new_status)` pairs and its writers.
`_pa_cas` checks every move at runtime (strict under
`WATCHTOWER_STRICT_EDGES=1`, which the test suite sets; production logs
`ANSWER_EDGE_UNDECLARED`), as do `block()` and `update(needs_input=True)` for
their in-lock supersession (E17).

Plan gate vs answers (D1a): while `PLAN_ACTIVE` (plan-gated, plan not
`accepted`/`failed`) the plan stage owns the ticket. A worker `wt block` stays
legacy `needs_input` instead of parking, `resume_claim` refuses, and
`route_answer` / the reconciler hand a pending answer to the plan stage
(`plan gate active; answer handed to the plan stage`). `desired()` spawns no
stage while an answer is in flight (`ANSWER_INFLIGHT`).

Every claim write records `claim_proc` (`worker_id, session_id, engine, pid,
pid_started, exit_file, record_started_at, bound`); `bound` is `record` (a WT
worker record), `ambient` (no record: never provably dead) or `inherited` (a
re-bind that kept the builder's process: reject, `reopen --resume`, a resume
after the parked worker exited). Parking moves it to `parked.proc`; ending a
claim keeps it as `prior_claim_proc`. `liveness.claim_owner(item)` answers
`alive | dead | unproven` from it.

The claim-time guard (`queue._verify_worker_live`, run by `claim_next` /
`claim_by_ref`) asks the same resolver about the `claim_proc` the claim would
write, and rejects (`... not currently alive ... claim rejected`) only on
`dead`. A dead worker record whose session is alive in the Claude registry (a
resumed session) is `alive` and claims normally; a claimer with no record
(never spawned, or pruned) is `ambient`, and an unprovable death (registry
unreadable, codex rollout missing, a process still naming the session) is
`unproven` — both go through.

### Adding a state value, a dim or an edge

1. Add the value to its vocabulary tuple in `queue.py` (`VALID_STATUSES`,
   `PLAN_STATUSES`, `ANSWER_STATES`, ...). `tests/test_liveness_table.py`
   now fails: the frozen copy in `liveness._FROZEN` differs (D2.4) and the
   value is in no row (D2.1).
2. Add it to `liveness._FROZEN`, then either widen a row's boxes, add a row
   (with owner, proof and recover), or add an `UNREACHABLE` entry citing the
   code that makes it impossible. Rows must stay disjoint on reachable states.
3. A stage row needs its key rule in `prove()`, and `stages.desired()` must
   agree over the whole product (D2.3).
4. A new dim: add it to `DIMS`, `_FROZEN`, `project()` and `synth()` in
   `tests/liveness_golden.py`; boxes built with `_box()` admit every value of a
   dim they omit.
5. A new answer move: declare it in `ANSWER_TRANSITIONS` with its writer. Write
   it only through `pa_transition` / `pa_bump_attempts` with literal
   arguments, or in a function listed in `liveness.PA_INLOCK_WRITERS`; the AST
   scan (D2.6) rejects anything else. Only `RECEIPT_CONFIRMED_WRITERS` may move
   an answer to `delivered`. Add a golden in `tests/test_liveness_goldens.py`
   that drives the real function through the edge (`Golden.step(..., edge=)`).

## Liveness backstop (WT-31 phase B)

`liveness.sweep()` runs every reconciler tick after `reconcile_stages`
(`reconcile_once` with `supervise_stages` and not `dry_run`) and in `wt stages
tick [--ref REF]`. It walks every live ticket (closed ones only with a
due/running/filing assessment), calls `assess()`, and acts on at most one step
per ticket. `wt liveness [-q Q] [REF] [--json]` prints the same assessment
read-only (row, owner, proof, verdict, evidence, idle, the action it would take).

**Idle gate.** Stage, claim and staffing rows act only after `STALL_S` of no
activity (`WATCHTOWER_STALL_S`, default 1800 s). Activity is the newest store
stamp (`updated_at`, `claimed_at`, `stage_session.spawned_at`, answer
`state_at`, `parked.at`, recent history) and then the claim transcript / stage
worker file mtimes. System-op rows (`dep.stuck`, `answer.parked_bare`,
`answer.plan_conflict`, `answer.affinity_expired`) act on the first sweep;
`plan.blocked` waits one tick (120 s), because `plan_verdict` sets `blocked`
before its caller blocks the ticket.

| Row kind | Evidence | Action |
|---|---|---|
| stage (`plan.*`, `review.verify`, `assess.*`) | `stage_session` worker alive, or a launch cooldown | `stage`: `reconcile_stages(only_ref)` |
| `assess.filing` | idle | `assessment_run_ops` (the idempotent op replay) |
| claim (`work.claimed`, sent-back) | `claim_owner` | dead: `resume` if resume-first applies, else `reopen`; unproven or GitHub: `escalate`; alive: none |
| `work.unowned` (in_progress, no claimer, e.g. `update_status(in_progress, worker='')`) | owner `reconciler`; only a bound session `claim_owner` proves alive vetoes | `reopen` to the pool (no claimer to protect); GitHub: `escalate` |
| `work.open` (staffing) | a live, unreleased queue worker | `spawn` one worker (auto_drain on, no launch-failure cooldown; one per queue per sweep) |
| `dep.stuck` | stuck blocker not yet escalated | `escalate_stuck_blockers`; `BACKSTOP_NO_ESCALATION` if it still is not flagged |
| `plan.blocked` | idle one tick | `BACKSTOP_UNBLOCKED_PLAN`, escalate to a human |
| human / terminal / dependency rows | - | none |

A live sent-back claim inside its WT-34 release window is never touched;
`release_stalled_sent_back` owns it. A stage row with no `desired()` entry
escalates (`no supervisor for state <id>`).

**Answer floors.** Each floor is an edge through `pa_transition`, declared with
its writer: `answer.routing` older than 2 x `ROUTE_LEASE_S` (the router gets its
lease first) hands off by E5 (`_floor_routing`); `queued` older than
`ANSWER_QUEUE_TTL_S` + lease, and `delivering` older than lease x
(`MAX_DELIVERY_ATTEMPTS`+1) or with a dead owner after one lease, hand off by
E10 (`_floor_bound`); `affinity_expired` (and an `affinity` whose prior worker
is not alive after one lease) hands off by E11 (`_floor_affinity`);
`plan_conflict` hands the answer to the plan stage (E5); `parked_bare` reopens.

**`queue.recover_claim(ref, expect=, action=, reason=, ...)`** is the only
backstop writer of claim state. It compares `expect` (status, `claimed_by`,
`claimed_session_id`, `claim_proc`, and `liveness.fingerprint()`) inside the
store lock; any race (reclaim, block, answer, comment/progress, re-bind) makes
it a no-op with `BACKSTOP_SKIP <ref> race: ...`. Actions: `reopen` (the same
field reset as `update_status(open)`, keeping `gate_feedback`, handing off a
settled answer by E14, recording `orphan`/`displaced_*`), `resume` (stamps
`claim_proc.resumed_at`), `escalate` (a system `needs_input` block, E17
supersession) and `mark` (before a stage/spawn/assessment action). Every write
appends a `backstop` history event and sets `backstop = {state, action, count,
at, fingerprint}`. GitHub tickets are refused (the sweep escalates them with
`q.block`).

**Loop guard.** If a ticket is still in the same row with the fingerprint the
backstop recorded (nothing moved since its last action), a prior `resume`
becomes `reopen` and anything else becomes `escalate` (`second stall ...`), so
the backstop never repeats one action forever.

**Resume first (WT-30).** A dead builder holding a verifier rejection
(`gate_feedback`, `resume.state == "pending"`, a resumable session) is resumed
before it is reopened, both in the sweep and in `requeue_orphaned_tickets`. The
orphan sweep stays the fast path: `claim_owner` alive vetoes a reopen, and a
local reopen goes through `recover_claim` (GitHub still uses `update_status`).

**Log lines.** `BACKSTOP <ref> state=<row> owner= proof= verdict= evidence=
idle=Ns action=<a> [result]`, plus `BACKSTOP_SKIP`, `BACKSTOP_ANSWER_HANDOFF`,
`BACKSTOP_ANSWER_PLAN_CONFLICT`, `BACKSTOP_NO_ESCALATION`,
`BACKSTOP_UNBLOCKED_PLAN`.

**Health (D5).** `claimable_depth` excludes plan-active tickets, `dep.stuck` and
a reserved answer affinity. `queue_status` adds `stage_owned` (the `desired()`
count), `stage_stuck` (no stage event for max(stuck minutes, stage idle limit +
120 s)) and `stuck_reason` (`claimable` or `stage`). A stalled stage sets
`state = "stuck"` even at depth 0, but the `stuck` bool keeps its old meaning
(claimable work with no progress), since workers use it for nudges and spawns.
There is no CCC alarm banner.

## Verified delivery (WT-31 phase C)

A send is not a delivery. `liveness.deliver(target, text, purpose=,
dedupe_key=, ...)` is the one way a verified sender reaches a session. It tries
UDS, then the FIFO, then `messages.deliver_message` / `send`, then a headless
resume, in the order the caller lists. Every send:

1. gets a delivery id and a nonce `⟨wt:<delivery_id>⟩` alone on the last line
   of the text (a slash command gets none: it would become the command's
   argument, so it is sent `unverified`);
2. writes a `sending` row to the ledger, `~/.watchtower/deliveries.json`
   (next to the outbox; `$WATCHTOWER_DELIVERIES_FILE` or
   `liveness.DELIVERIES_FILE` override it);
3. records a receipt (`receipts.record(nonce=)`) **before** the send, with the
   transcript size as its offset. `messages.deliver` does the same for nonce'd
   text it sends, and drops the receipt if every adapter fails.

A receipt is `landed` only when its nonce appears in the transcript at or
after the offset (raw or JSON-escaped). Earlier bytes, the same text sent
before, and a nonce pasted mid-message never count. `advanced` (the transcript
grew) stays pending. After `WATCHTOWER_RECEIPT_WAIT_S` (600 s) the receipt is
`lost`. A transcript smaller than the offset (compacted or rewritten) resets
the offset to 0.

`deliver()` returns `state`:

| State | Meaning |
|---|---|
| `pending` | sent; the row waits for the receipt |
| `unverified` | sent; there is no receipt source (no session id, kimi/devin, slash command), and the sweep reads it lost |
| `queued` | the outbox holds it (`msg_id`) |
| `deferred` | `fifo_busy="defer"` and the turn is open; there is no row |
| `in_flight` / `duplicate` | the messages ledger already holds the key; there is no row |
| `failed` | no transport took it |

A headless resume is refused for an engine with no receipt source unless the
caller passes `unverified_ok`. Only the legacy `_deliver_to_blocked_session` does,
to keep kimi resume working.

**Ledger.** There is one live row per `dedupe_key`: a new send supersedes the
older `sending`/`pending` rows. Every retry has a new nonce, so an old send's
nonce never confirms the new one. Rows are pruned after 24 h, and at most 2000
are kept.

**Sweep.** `liveness.sweep_deliveries()` runs on every daemon tick, after
`receipts.sweep()`, and settles each pending row under the ledger lock:

- **`confirmed`**: the nonce landed.
- **`lost`**: any one of these:
  - the receipt was lost or is missing;
  - the row was pending for more than 2 x the window;
  - the row was unverified;
  - a queued row was still waiting after `expire + 2 x window`;
  - a `sending` row is older than 2 x the window.

It then runs the purpose's handler and logs
`DELIVERY <ref|worker> <purpose> <key> <id> <state> (<reason>) -> <result>`.

| Purpose | Key | Confirmed | Lost |
|---|---|---|---|
| `answer` | `answer:<ref>:<sid>:<gen>` | E9 `_on_answer_confirmed` (`delivering`/`queued` -> `delivered`) | E10 `_fallback_reopen` (hand off) |
| `stage_answer` | `stage_answer:<ref>:<gen>:<key>` | E13 `stages._confirm_stage_answer` | stays `handed_off` |
| `nudge` | `nudge:<worker_id>` | clears `undeliverable_since` | resend once, then set `undeliverable_since` on the worker record |
| `release` | `release:<worker_id>` | - | resend once, then `RELEASE_UNDELIVERED` |
| `plan` | `plan:<ref>:<role>:<kind>` | a `reminder` bumps `nudges` (escalates past the max) | a `reminder` blocks the plan for a human; any other message to the live stage session marks it lost (killed, death recorded, respawned within the attempt budget) |
| `review` | `review:<ref>:<n>` | - | renotify once, then `wt blocked` (system block) |
| `resume` | `resume:<ref>:<sid>` | - | `set_resume_state(failed)` |

E9, E10 and E13 are compare-and-swap moves on `gen` and the state. A confirm that
arrives after E14 or E17 (release, reopen, re-block) is therefore a no-op.
`delivered` is written only by `RECEIPT_CONFIRMED_WRITERS`. The answer router
treats a ledger row that is still pending as in flight, so it does not resend,
and the E10 floors wait for it.

A stage prompt that carries a handed-off answer ends with the nonce. The
session did not exist before the spawn, so its receipt offset is 0
(`register_spawn_delivery`). A codex stage has no session id at spawn, so its row
is unverified and the answer stays `handed_off` until the next claim (E12).

**Sender lists (D2.8).** Every function that calls a raw transport
(`deliver_via_uds`, `write_to_worker_fifo`, `_write_fifo_frame`, `send_lines`,
`messages.send`, `deliver_message`, `_resume_session_headless`), or calls
`_deliver_to_blocked_session` or `liveness.deliver`, is in exactly one list:

- **`liveness.VERIFIED_SENDERS`**: reaches a session only through `deliver`.
- **`ADVISORY_SENDERS`**: best effort. The result of the send never feeds a
  ticket-state write, either as an argument or as the test of an `if`.
- **`TRANSPORT_LAYER`**: the plumbing itself.

`tests/test_liveness_delivery.py` checks this with an AST scan, and also fails
on stale list entries.

### Adding a verified sender

1. Call `liveness.deliver(...)` with one of the `DELIVERY_PURPOSES`, a stable
   `dedupe_key`, and `ref` / `worker_id` / `meta` for the handler. Do not call a
   raw transport.
2. Treat `ok` as "sent", not as "delivered". Any state change that depends on
   the session reading the message belongs in the purpose's handler
   (`liveness.HANDLERS`).
3. Add `module.function` to `VERIFIED_SENDERS`. A new purpose needs a handler,
   and if it moves an answer, a declared edge with `sweep_deliveries` as its
   caller.

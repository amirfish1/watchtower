# Worker Idle Audit and Spawn Causality

Date: 2026-07-16

## Problem

WatchTower now releases a verified-idle conversation from queue staffing rather
than terminating its process. The safety policy is conservative, but the
activity log currently records only the final `RELEASE` and `SPAWN` outcomes.
It does not expose the evidence that produced a preserve/release decision, and
it does not identify whether a spawn is an initial worker, scale-up, crash
replacement, or a direct replacement for a released worker.

That missing evidence is operationally unsafe. A previous worker was reaped
after its WatchTower stdout log looked idle for 2,504 minutes even though its
Codex rollout showed live conversation activity. The activity log recorded the
kill but not the clocks and assumptions that authorized it. Future lifecycle
decisions must be reconstructable from the activity log alone.

## Goals

1. Log every signal used to classify a worker as active, an idle candidate,
   preserved, or released.
2. Emit detailed logs only at meaningful transitions: first threshold crossing,
   changed evidence/decision, release, and return to activity.
3. Correlate a released staffing slot with any replacement spawned in the same
   reconciliation pass.
4. Preserve healthy, working, or ticket-owning conversations and their cache.
5. Keep release queue-scoped: never terminate or interrupt the underlying
   conversation merely to adjust WatchTower staffing.
6. Distinguish release replacements from valid initial, scale-up, and
   dead-worker recovery spawns.

## Non-goals

- This design does not bulk-recover CCC sessions labeled `Stuck`.
- It does not treat CCC's stale-transcript heuristic as proof of a hung process.
- It does not change queue ordering or allow replacement workers to bypass the
  ordinary oldest-claimable-ticket policy.
- It does not add a second diagnostic file; the unified activity log remains the
  operator-facing source of truth.

At design time CCC reports 24 stuck sessions from 26 coarse candidates among
134 recent Codex sessions. None overlaps the three currently registered
WatchTower workers. Those sessions require a separate CCC recovery audit using
app-server turn state, writer ownership, pending-tool state, rollout tail, and
queued-wake state. WatchTower must not mutate them merely because they appear in
that counter.

## Vocabulary

- **Eligible staffing:** a tracked, live, non-adhoc worker that has not already
  been released from its queue.
- **Idle candidate:** eligible staffing whose newest available activity clock
  is at least the 30-minute release floor.
- **Verified idle:** an idle candidate for which the queue backend was read
  authoritatively and no ticket is owned by worker ID or session ID.
- **Preserved:** a worker WatchTower deliberately leaves attached to the queue
  because at least one safety gate forbids release.
- **Released:** a conversation durably detached from queue staffing. Its process
  and unrelated work remain untouched.
- **Replacement spawn:** a spawn caused by claimable work plus a staffing
  deficit created by one or more releases in the same locked reconciliation
  pass.

## Activity evidence snapshot

Worker evaluation will produce a structured snapshot before deciding. The
snapshot is also returned internally so decision code and logging cannot drift.

Required identity fields:

- queue
- worker ID
- session ID, or `missing`
- PID
- engine and model
- worker kind
- already-released state

Required process/activity fields:

- PID alive result
- WatchTower stdout log path, existence, mtime, and age
- authoritative engine activity source:
  - Codex rollout path, existence, mtime, and age
  - Claude transcript path, existence, mtime, and age
- newest effective activity source and timestamp
- calculated idle seconds/minutes
- configured release floor
- whether the worker crossed the release threshold

Required ownership fields for threshold-crossed workers:

- strict queue read success/failure and backend error when present
- number and refs of tickets owned by worker ID
- number and refs of tickets owned by session ID
- whether any owned ticket is blocked for human input

Missing or unreadable authoritative state is logged explicitly. Unknown state
never silently becomes affirmative evidence for release.

## Event model

The existing plain-text activity log gains correlated, stable `key=value`
details. Values that can contain spaces are JSON quoted. Each evaluation bundle
uses an `evaluation_id`; each release uses a `release_id`.

### `IDLE_CANDIDATE`

Emitted when a worker first crosses the 30-minute floor or when a prior idle
evaluation's evidence fingerprint changes. It includes the complete identity,
clock, and threshold summary.

Example:

```text
IDLE_CANDIDATE worker=ccc-123 engine=codex session=019f... pid=1234 evaluation=idle-a1 floor_s=1800 effective_source=codex_rollout effective_age_s=1902 wt_log_age_s=5200 rollout_age_s=1902
```

### `IDLE_SIGNAL`

One line per safety gate, sharing the evaluation ID. This makes each fact
searchable without hiding everything inside a single prose paragraph.

Examples include:

```text
IDLE_SIGNAL evaluation=idle-a1 signal=pid_alive value=true source=tracked_pid
IDLE_SIGNAL evaluation=idle-a1 signal=queue_read value=success backend=file
IDLE_SIGNAL evaluation=idle-a1 signal=owned_tickets value=0 refs=[]
IDLE_SIGNAL evaluation=idle-a1 signal=pid_signal_planned value=false reason="queue-scoped release never terminates the conversation"
```

### `IDLE_DECISION`

Exactly one terminal decision per evaluation bundle:

- `decision=PRESERVE` with every blocking reason
- `decision=RELEASE` with `release_id`

The term `IDLE` is reserved for a fully evaluated result. Merely crossing the
clock threshold is an idle candidate, not proof of safe release.

### `ACTIVE_AGAIN`

Emitted when a previously logged idle candidate receives newer authoritative
activity and falls below the threshold. It includes the previous evaluation ID,
new activity source/age, and the fact that queue staffing remains attached.

### `RELEASE`

Emitted after durable queue detachment and delivery attempt. It includes:

- release and evaluation IDs
- worker/session/queue
- durable `released_at`
- instruction transport (`fifo`, native session delivery, or unavailable)
- delivery outcome/error
- explicit `pid_signalled=false`
- whether a claim stop sentinel was written
- the final release reason

The event verb is `RELEASE`; normal worker lifecycle no longer emits `REAP`.
The compatibility function name may remain temporarily, but logs and returned
result fields use release terminology.

### `SPAWN_PLAN`

Emitted whenever a spawn is requested, a release occurred in the pass, or the
spawn cause/decision changed. A release with no claimable work therefore emits
an explicit zero-worker plan; unchanged routine `actual == desired` ticks stay
silent.

- queue and reconcile pass ID
- total open and claimable depth
- desired workers
- eligible live staffing before releases
- workers released in this pass
- eligible live staffing after releases
- unclaimed work and calculated staffing deficit
- requested spawn count
- spawn cause
- preserve/no-spawn reason when count is zero

### `SPAWN`

Each successfully created worker retains the existing process details and adds:

- reconcile pass ID
- classified cause
- related release IDs and previous worker IDs when applicable

Spawn causes are:

- `initial_staffing`: claimable work and no prior eligible staffing
- `scale_up`: desired capacity increased or backlog requires another configured
  slot while existing eligible staffing remains
- `release_replacement`: releases in this pass created the staffing deficit
- `dead_worker_recovery`: a tracked worker died and claimable work remains
- `manual_or_run_once`: existing explicit one-off paths

## Transition suppression

Logging every worker every 30-second tick would bury useful evidence. Each
tracked worker therefore persists a compact lifecycle-audit record in
`workers.json`:

- last evidence fingerprint
- last decision
- last evaluation ID
- last logged timestamp
- last effective activity timestamp

The fingerprint covers every input that can change the decision, including
clock sources, threshold state, backend-read outcome, ownership refs, alive
state, and released state. An identical evaluation is silent. A changed signal
emits a complete new bundle rather than a partial delta so each bundle remains
self-contained.

Daemon restart may emit one fresh audit bundle even when evidence is unchanged.
That deliberate restart boundary is more useful than suppressing the only
post-restart explanation of current state.

Workers below the threshold do not generate routine logs. A worker previously
seen as an idle candidate emits `ACTIVE_AGAIN` when new activity arrives.

## Release and spawn transaction

`reconcile_once()` remains serialized by the existing cross-process reconcile
lock. Within one pass it performs these phases:

1. Assign a reconcile pass ID and snapshot eligible staffing.
2. Evaluate release candidates and emit any transition bundles.
3. Persist releases and attempt queue-scoped instructions.
4. Recompute eligible staffing, excluding durably released workers.
5. Read claimable depth and desired staffing.
6. Emit `SPAWN_PLAN`.
7. Spawn only the required deficit, capped by unclaimed claimable tickets.
8. Emit correlated `SPAWN` or launch-failure events.

If a release creates a deficit and claimable work exists, replacement spawning
occurs in that same locked pass and is tagged `release_replacement`. If no work
is claimable, no replacement is needed. A future enqueue will calculate a new
deficit and may spawn initial staffing; it must not pretend that later spawn was
part of the old release transaction.

Queue order is unchanged. Replacement workers call the same atomic
`claim_next()` path and receive the oldest claimable ticket under current queue
policy.

## Preserve policy

A worker is preserved when any of these conditions applies:

- PID is not confidently attributable to the tracked worker
- worker is adhoc
- effective activity is newer than 30 minutes
- Claude transcript or Codex rollout reports newer activity than the spawn log
- queue state cannot be read strictly
- worker ID owns an in-progress ticket
- any owned ticket is blocked for human input
- required identity or authoritative evidence is unknown in a way that prevents
  a confident release

Every applicable reason appears in the `IDLE_DECISION decision=PRESERVE` event.
Preservation is not an error; it is the intended fail-closed behavior.

## Error handling

- Logging failure is never treated as affirmative idle evidence. A release
  fails closed if its audit bundle cannot be written. Initial or recovery
  spawning may still service claimable work and reports the logging failure to
  daemon stderr.
- Queue backend failure produces a preserve decision with the backend error.
- Missing transcript/rollout evidence is recorded explicitly and handled
  conservatively.
- Release-instruction delivery failure leaves durable queue detachment and stop
  sentinel intact, logs the transport error, and does not signal the PID.
- Spawn launch failure logs the cause, release correlation, executable error,
  and cooldown state.
- Audit-state persistence uses the existing workers-file lock and atomic write
  pattern.

## Testing

Tests must prove:

1. A threshold crossing emits a full candidate/signal/decision bundle.
2. Identical subsequent ticks emit no duplicate bundle.
3. Any changed evidence emits a new complete bundle.
4. Fresh Claude transcript activity and fresh Codex rollout activity produce
   `ACTIVE_AGAIN`/preserve behavior.
5. Queue-read failure and ticket ownership log their specific preserve reasons.
6. Blocked ticket ownership prevents release.
7. Release logs delivery transport, durable detachment, stop sentinel, and
   `pid_signalled=false`.
8. No normal release path calls `os.kill`.
9. Claimable work plus a same-pass release emits correlated `SPAWN_PLAN` and
   `SPAWN cause=release_replacement`.
10. No claimable work after release emits `SPAWN_PLAN requested=0` and no spawn.
11. Initial, scale-up, and dead-worker recovery spawns receive distinct causes.
12. Replacement workers retain normal oldest-claimable ordering.
13. Existing CCC stuck-summary sessions are never mutated by WatchTower merely
    because CCC labels them stuck.

## Documentation migration

`docs/worker-lifecycle.md` still describes five-minute killing/reaping and an
older polling worker loop. Implementation must update that reference to the
30-minute queue-scoped release model, authoritative Claude/Codex activity
clocks, durable released staffing, transition audit events, and classified
spawn causes.

# Worker lifecycle & the reconciler

Design notes for how WatchTower runs, stops, and recovers workers. Captures the
decisions from the 2026-06-26 design discussion. This is the spec for the
"policy-driven workers" feature (WT-FEATURES) — not yet fully built; what exists
today is noted inline.

## Philosophy: declare intent, reconcile to it

You don't manage workers. You declare **policy** (per queue), and a controller
makes reality match — the Kubernetes/thermostat model. Workers are a *function
of (policy, queue depth)*, a side effect the system maintains, not something you
spawn by hand.

This is the same thesis as the rest of WatchTower: trust the declared state and
the queue's ground truth, not manual fiddling or an agent's say-so.

## The three layers

| Layer | Role | Where |
|-------|------|-------|
| **Registry** | desired state — what queues exist, their backend/owner, `auto_drain`, repo, engine | `~/.watchtower/queue-registry.json` (`registry.py`) — *exists, thin* |
| **Reconciler** | the brain — spawns / stops workers so reality matches the registry | the `wt start` daemon loop — *to build* |
| **Workers** | the muscle — claim → fix → close, draining a queue | `workers.py` (`spawn_workers`) — *exists; real headless `claude -p` since WT-BUGS-2* |

## The reconciler loop

Every tick, for each queue:

```
desired = (auto_drain AND open_tickets > 0) ? N : 0
actual  = live workers on this queue
if actual < desired: spawn
if actual > desired: signal-stop (never hard-kill — see below)
```

Properties this buys for free:
- **Self-healing:** worker crashes → next tick `actual < desired` → respawn.
- **Opt-out winds down:** flip `auto_drain` off → `desired` drops to 0 → the
  queue's worker is told to stand down.
- **No drift / no orphans:** the loop is the single source of worker truth;
  manual `spawn-worker` becomes daemon-**internal** (demoted from a user command).

`auto_drain` is the one knob: `wt config -q Q --auto-drain true|false`. On =
"keep it drained," off = "this is a backlog, hands off." (Exists today; the
watcher already honors it for auto-spawn. The reconciler generalizes it to also
stand workers *down*.)

## Stopping a worker: cooperative, never mid-ticket

A worker only touches shared state at one moment — **between tickets, when it
calls `wt claim`**. That is the only safe interruption point, so that's where we
signal. We never `kill` a worker mid-ticket.

`wt claim` returns one of three things:
- **a ticket** — work it.
- **empty** — nothing open; idle and re-poll (see warm window).
- **STOP** — wind down: finish nothing new, exit.

When the reconciler wants a worker gone, it does **not** touch the process. It
sets state so the next `claim` returns STOP. The worker — which already finished
and closed its current ticket before calling claim — reads STOP and exits
cleanly on its own. Cost: winding down waits up to **one ticket's duration**.
That's the "let them finish what they started" behavior, by construction.

This deletes the orphaned-`in_progress` problem entirely: a worker never
abandons a ticket, because it only sees STOP between tickets.

## Warm-idle window (cost optimization)

On **empty** (queue active, nothing open right now), a worker does **not** exit
immediately. It idles and re-polls for **~5 minutes**, then exits. Rationale:
Anthropic's prompt cache has a 5-minute TTL — a ticket arriving inside that
window reuses the warm cache (cheaper, faster) instead of paying a cold start.
Past 5 min idle → self-exit; the reconciler respawns when work returns.

(STOP is different: exit right after the current ticket, no warm window — you
asked it to stand down.)

So the reconciler barely manages lifecycle: workers self-retire on a 5-min idle
TTL or on STOP.

## Blocked work: when a ticket needs a human

Some tickets can't reach `closed` without a human decision. Handling this is the
subtle part. Decisions:

### No new lifecycle state
A `needs-input` *state* is a slippery slope — give agents a comfortable park and
they dump hard tickets there to avoid closing. We keep `open → in_progress →
closed` and use a **flag**, not a state.

### Continuity comes from resumable sessions
A worker that's 95% to a solution holds that analysis in its **session context**.
Throwing the ticket to a fresh worker discards it. So:

- A blocked ticket **stays `in_progress`, bound to its session**
  (`claimed_session_id`), flagged `needs_input` + the worker's specific question.
  It is **not** reclaimable by another worker.
- The worker process may **exit** (saves tokens) — the context is not lost,
  because Claude sessions are **resumable by id**. Continuity is free; no need to
  hold a process alive.
- **Backstop:** before parking, the worker dumps its analysis-so-far into the
  ticket (a progress/notes field), so even if the session is ever truly gone, the
  next worker resumes from rich notes rather than scratch. Resume-first,
  notes-as-fallback.

### Resume the *same* session, never a fresh worker
The whole point is to not lose the 95%. A discussion-needing ticket is **the same
resumable session you converse with** — not a newly spawned worker.

### Guard against abuse (on-thesis)
The watcher flags a worker/queue that bounces too many tickets to `needs_input`,
or a ticket that sits blocked too long — the queue's ground truth catching a lazy
or neglected agent. Same trust thesis, applied to the agent's own honesty.

## Human-in-the-loop: where you give input

Two depths, same mechanism:
- **Quick answer:** inject into the session (CCC inject/steer) or
  `wt answer <ref> "..."`; the resumed session reads it and continues.
- **Real discussion:** open the session and talk to it. The agent that's deepest
  in the problem is the best partner for the last 5% — getting to that
  conversation is a feature, not a failure.

### CCC (premium surface)
The blocked session shows up live; click in, read full context, converse or
inject, it continues. You run the fleet headless via WatchTower; you **talk to
the stuck one** in CCC. This is CCC's role: the rich client over WatchTower's API.

### CLI (minimal surface)
- `wt blocked` — list blocked tickets with question + `session_id`.
- `wt discuss <ref>` — resolve the ticket's `claimed_session_id` + repo, then
  `cd <repo> && claude --resume <session_id>` (engine-aware: `codex resume` for
  codex). Drops you into the worker's session with full context. `--print` to
  show the command instead of exec'ing it.
- `wt answer <ref> "..."` — quick inject without attaching.

CLI makes the conversation *capable* (a terminal session with the agent); CCC
makes it *comfortable* (transcript history, screenshots, parallel sessions).
Same destination, different doorway.

## Command surface (target)

| Command | Purpose | Status |
|---------|---------|--------|
| `wt config -q Q --auto-drain true\|false` | the one policy knob | built |
| `wt register` / `wt registry` | declare queues (desired state) | built |
| `wt start` (reconciler) | spawn/stop workers to match policy | to build |
| `wt block <ref> --question "..."` | worker parks a ticket needing a human | to build |
| `wt blocked` | list blocked tickets | to build |
| `wt discuss <ref>` | attach to a blocked ticket's session (`claude --resume`) | to build |
| `wt answer <ref> "..."` | quick inject into a blocked session | to build |
| `wt spawn-worker` | **demoted to daemon-internal** (no longer user-facing) | to change |

## Open questions (not yet decided)

- **Resume-on-demand vs park-alive** for blocked workers. Lean: resume-on-demand
  (cheap, crash-safe, lossless via session persistence); add park-alive only if
  rapid back-and-forth latency hurts.
- **Per-queue `desired_workers > 1`** (parallel drain) — registry field, reconciler
  honors it. Not specced.
- **Reclaim/abuse thresholds** — how long blocked is "too long"; how many bounces
  is "lazy." Needs real numbers.

## What ships first

The **bulk drain** half is solid and shippable: reconciler loop + STOP-at-claim +
warm-idle TTL + demote `spawn-worker`. The **blocked-session** half
(`block`/`blocked`/`discuss`/`answer` + `needs_input` + resume) is the
sophisticated second slice — build after the first is proven.

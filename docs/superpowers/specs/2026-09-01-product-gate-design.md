# Product Gate — design

**Date:** 2026-09-01
**Status:** Approved design, pending implementation plan
**Scope:** WatchTower core (`queue.py`, `config.py`, `cli.py`, `workers.py`, `dashboard.py`) + CCC surfaces (`server.py`, `static/app.js`, `static/q2.js`)

## Problem

Workers currently go straight from claiming a ticket to implementing it. For some
queues the user (Amir) wants a product decision *before* implementation effort is
spent: maybe we don't want to fix/build this right now, or at all. Today the only
human-in-the-loop primitives are `wt block` (worker-discretionary, free-text,
usually about implementation) and pre-claim knobs (`auto_drain`, `run_requested`,
`readiness`) that fire before anyone has analyzed the ticket at all.

The Product Gate is a **mandatory early checkpoint inside the worker's run** on
opted-in queues: claim → minimal diagnosis → post a decision-grade pitch → stop
and wait for the user's Ack/Nack.

Key distinction settled during design: the gate is *typed and protocol-mandated*
(response is Ack / Nack / Ack-with-comment; the queue setting forces it), unlike
`wt block` which is free-text and at the worker's discretion.

## Decisions (with rationale)

1. **Gate fires after minimal diagnosis, not at claim time.** The pitch requires
   a worker to have understood the problem; a pre-claim gate would ask the user
   to decide on raw ticket text. Worker budget: *understand the problem, not the
   solution* — the goal template says this explicitly.
2. **Pending state = a block kind, not a new status.** Rides the existing
   block/notify/answer machinery (`needs_input`, keep-warm grace, release,
   steer-on-answer). New field `block_kind: "input" | "rationale"`, default
   `"input"` so all existing call sites and blocked tickets are unchanged.
   This also gives the user visibility they asked for: every blocked ticket is
   distinguishably "implementation question" vs "product decision".
3. **Icebox = readiness value `needs-rationale`.** Nack (park flavor) sets
   `readiness: needs-rationale`, added to `UNCLAIMABLE_READINESS`
   (`queue.py:134`). The name records what revives it: someone must bring a new
   rationale (it wasn't a priority, or wasn't right for the product). Nack
   comment is required and stored on the ticket.
4. **"Not ever" is just Close.** No parallel machinery: closing with resolution
   `declined` via the existing close path. Reopen remains available.
5. **Ack is the phase-2 go signal.** ▶/`run_requested` keeps today's meaning
   ("start work") — on a gated queue that work is the diagnosis phase. The gate
   always fires; Ack continues the run (no second ▶ press). ▶ is NOT a pre-ack:
   the diagnosis itself needs a run, so the two signals must stay separate.
6. **Explicit pre-ack for "just do it" tickets.** `pre_ack: true` settable at
   filing (`wt add --pre-ack`, UI checkbox). The user filing a ticket is *not*
   an implicit ack — cheap diagnosis may reveal the ticket is bigger/different
   than assumed at filing.
7. **Ack persists across reopen.** The product decision was made once
   (`product_ack: {by, at, comment}` stays on the ticket).
8. **Rename existing `wt ack` → `wt unresolved-ack` first.** Today's `wt ack`
   means "human acknowledged a caveat on a *closed* ticket"
   (`queue.ack_resolution()`, `queue.py:2114`) and pairs with `wt unresolved`.
   The rename frees "ack" to mean the gate decision in every surface. New
   `wt ack` on a closed ticket errors with "did you mean wt unresolved-ack?".
9. **Hard enforcement, not just prompting.** On a gated queue, `wt close` with
   an implemented/fixed-type resolution refuses when the ticket has neither
   `product_ack` nor `pre_ack`; error text tells the worker to gate first.
   `declined` closes are exempt (that's the Nack path).

## Data model

- **Queue setting** `product_gate` (bool, default **off**): `config.py`
  getter/setter following the `auto_drain` pattern (`config.py:231-254`);
  `wt config --product-gate on|off`; CCC queue-settings write-through
  (`claude-command-center/server.py:~27719` — must be added explicitly there or
  the UI silently drops it).
- **Ticket fields:**
  - `block_kind: "input" | "rationale"` (only meaningful while `needs_input`).
  - `product_ack: {by, at, comment}` — set on Ack; persists across reopen.
  - `pre_ack: bool` — set at filing.
  - `readiness` enum gains `needs-rationale` (`queue.py:125`,
    `UNCLAIMABLE_READINESS` at `queue.py:134`).
- **History events:** `gate_pitch`, `gate_ack`, `gate_nack` join
  `_EVENT_PRECEDENCE` (`queue.py:796-799`) and `timeline()`.

## Lifecycle on a gated queue

```
open --claim--> in_progress
  --[worker: minimal diagnosis]-->
  wt block <ref> --kind rationale --question "<pitch>"   (gate_pitch event)
  --[user notified: "awaits product decision"]-->
    Ack   -> product_ack recorded; answer path steers/resumes worker; implement
    Nack  -> claim released; readiness=needs-rationale; comment stored (icebox)
    Close -> existing close path, resolution "declined"
```

Existing keep-warm grace (`answer_grace_s`, `workers.py:3625-3745`) and
release/resume machinery apply unchanged; gate decisions taking days is fine
because release already handles a cold resume or re-dispatch.

If `product_ack` or `pre_ack` is already present at claim time, the goal
template instructs the worker to skip the gate and implement directly.

### Pitch contract (enforced by goal templates)

The gate question must be decision-grade:

1. **Problem** — 2–3 sentences, product terms not code terms.
2. **Evidence** — links: originating conversation, source ticket, failing surface.
3. **Magnitude** — for inefficiency/tech-debt claims, numbers (tokens, seconds,
   $/day), each labeled measured vs. estimated.
4. **Rough size** — S/M/L gut call, explicitly *not* a design.

Templates to update: `DRAIN_GOAL_TEMPLATE` (`workers.py:269-322`) and
`RUN_ONCE_GOAL_TEMPLATE` (`workers.py:327-360`) — gate phase inserted only when
the queue has `product_gate` on.

## CLI

| Command | Behavior |
|---|---|
| `wt unresolved-ack <ref> ...` | Renamed from today's `wt ack` (resolution-caveat ack). Same semantics. |
| `wt ack <ref> [-m comment]` | Gate ack. Valid only on a `rationale`-blocked ticket; comment delivered via the `wt answer` steer/resume path; records `product_ack`. On a closed ticket: error suggesting `wt unresolved-ack`. |
| `wt nack <ref> -m reason [--close]` | Default: release claim + `readiness=needs-rationale` + store comment (`-m` required). `--close`: close with resolution `declined`. |
| `wt block <ref> --kind rationale\|input ...` | New `--kind` flag, default `input`. |
| `wt add ... --pre-ack` | Sets `pre_ack` at filing. |
| `wt blocked` | Output split by kind; `wt gated` filter/alias lists rationale-blocked tickets. |
| `wt config -q Q --product-gate on\|off` | Queue setting. |

## UIs

All ticket surfaces get:
- A distinct chip: **"Awaiting product decision"** (rationale) vs today's
  "Waiting for your answer" (input), pitch text displayed.
- Three actions: **Ack**, **Ack with comment**, **Nack** (Nack prompts for the
  required comment plus an icebox/close choice).
- Queue settings panel: `product_gate` toggle.

Surfaces: `dashboard.py` queue page (`render_queue()` ~:1078) + POST routes;
CCC `static/app.js` ticket detail (~:43883 answer box area, chips ~:44499);
`static/q2.js` console (status model :207, chips, timeline); CCC
`server.py` `/api/ux-fixes/*` gains ack/nack endpoints and the rename of the
existing ack endpoint.

Notifications: reuse `_notify_ticket_event()` (`queue.py:954-1027`) with a new
verb, "awaits product decision".

## Enforcement backstop

`queue.close()` (`queue.py:2045-2072`) / `wt close`: on a `product_gate` queue,
a non-`declined` close requires `product_ack` or `pre_ack` on the ticket;
otherwise error instructing the worker to run the gate. This is the hard stop
for a worker that ignores its prompt.

Note: `claim_by_ref()` (`queue.py:1725`) and `_claim_candidates()`
(`queue.py:1395`) are intentionally untouched — the gate is not a claim filter.
`needs-rationale` iceboxing is enforced by the existing
`UNCLAIMABLE_READINESS` exclusion, which both paths already respect (verify
`claim_by_ref` honors readiness; if it doesn't, add the check there).

## Testing

- `product_gate` joins the settings round-trip tables in
  `tests/test_queue_settings.py` (`test_wt_config_round_trips_every_setting`,
  `test_settings_survive_a_process_restart`).
- New `tests/test_product_gate.py`: block-kind default + explicit; ack path
  (steer delivery, `product_ack` recorded); nack path (release + readiness +
  required comment); nack `--close` as declined; close guard (refuses ungated
  implemented close, allows declined, allows with `pre_ack`); pre-ack skip in
  goal template rendering; ack persists across reopen; `wt gated` listing;
  notifications verb.
- Rename coverage: existing `tests/test_resolution_ack.py` updated to the new
  command name; a test that old-style gate-less `wt ack` on a closed ticket
  errors helpfully.

## Out of scope (v1)

- **GitHub-backed queues**: gate label mapping (`github_backend.py` eligibility
  formula :1078, label constants :32-39) is a follow-up; v1 gates SQLite-backed
  queues only. `product_gate on` for a GitHub-backed queue should error or warn.
- History-graph changes beyond the new event names.
- Any change to ▶/`run_requested` semantics.

## Implementation notes

- Implementation will be delegated to cheap agents (Flash 3.7 High) via CCC CLI
  spawn, per standing practice (spawn through CCC, not bare engine exec).
- Suggested commit sequence: (1) `wt ack` → `wt unresolved-ack` rename across
  CLI/dashboard/CCC, (2) data model + config setting, (3) block kind + gate
  verbs + enforcement, (4) goal templates, (5) UIs, (6) tests alongside each.

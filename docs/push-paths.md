# Every way WatchTower pushes text into a session

Four independent paths, three of them added or narrowed on 2026-09-04
(WATCHTOWER-22/23/25). They share no code, so a rule applied to one is NOT
applied to the others — check this list before adding a fifth.

## 1. Ticket-event notices — `queue._notify_ticket_event`

The only push emitter for ticket transitions (`claimed` / `closed` /
`needs_input` / `awaits_decision`). Every other `[watchtower] ...` string in
the codebase is stdout, not a push.

- **Who hears it.** The ticket's `submitter` UNION the queue's `subscribers`,
  MINUS the `actor` who performed the transition (`_actor_identities`
  matches both worker id and session UUID).
- **Which events.** The submitter is filtered by `config.notify_events`
  (default: everything but `claimed` — a claim tells the filer nothing it can
  act on and costs it a turn). Two carve-outs get the full stream:
  `submitter_explicit` (the filer named a submitter with `wt add --submitter`)
  and `pre_ack`. Subscribers are never filtered — `wt subscribe` IS the
  explicit opt-in, `wt unsubscribe` is its off switch.
- **How it travels.** `messages.send(..., notify=True)`, i.e. the
  `_deliver_notify` chain: **uds → fifo → delegate, and nothing else**. A
  notice must never be worth a model turn, so `_deliver_resume`
  (`claude -p --resume`, a full-context turn nobody is watching), tty,
  codex-app-server and gemini-resume are all excluded. Unreachable means the
  target is idle: the notice waits in the outbox, and the `notify` flag
  persists on the outbox row so `drain_outbox` keeps the same guarantee.

## 2. Dispatch nudges — `workers.dispatch_after_enqueue` → `notify_workers`

Fired after `wt add` and behind ▶ (`wt run`). Broadcasts "claim this" to every
live worker on the queue.

- A ticket somebody **already holds** gets no nudge at all — telling a worker
  to claim what it just claimed is the self-echo bug (WATCHTOWER-25).
- `notify_workers(..., exclude=...)` skips given worker/session ids; dispatch
  passes its own (`_self_identities()`), so a session is never nudged about a
  ticket it filed.
- Reads the ticket from the **cached list**, never `github_backend.get()` —
  that is a live uncapped `gh issue view` per call (docs/github-read-caps.md).

## 3. Stuck-queue nudges — `_maybe_nudge_stuck_queue` (reconciler)

Same `notify_workers` transport, different trigger: a staffed queue closing
nothing. It has no ticket context and so no exclusion; the startup-grace check
is what keeps it from hounding a fresh worker.

## 4. Comments — `cli.cmd_comment`

Injects a new comment into the claimant, except one the claimant itself wrote
(`_comment_author_is_claimant`, WATCHTOWER-21). The comment is still recorded.

## The standing rule

Never tell a session about something that session just did. It arrives as a
steer mid-turn, and the recipient cannot tell it apart from new instructions.
Four bugs of this exact shape have been filed on the WATCHTOWER queue; every
new push path needs an answer for "who caused this, and are they the one being
told about it?"

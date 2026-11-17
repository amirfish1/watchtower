# Every way WatchTower pushes text into a session

Four independent paths, three of them added or narrowed on 2026-09-04
(WATCHTOWER-22/23/25). They share no code, so a rule applied to one is NOT
applied to the others — check this list before adding a fifth.

## 1. Ticket-event notices — `queue._notify_ticket_event`

The only push emitter for ticket transitions (`claimed` / `closed` /
`needs_input` / `awaits_decision`). Every other `[watchtower] ...` string in
the codebase is stdout, not a push.

- **Who hears it.** A ticket filed with `wt add --subscribe` includes its
  `submitter`, UNION explicit ticket subscribers, UNION the queue's
  `subscribers`, MINUS the actor. Opt in later with `wt subscribe REF [target]`
  or `wt edit REF --subscribe`; opt out with `wt unsubscribe REF [target]` or
  `wt edit REF --no-subscribe`. The default target is the caller session.
  Filer attribution stays intact. `wt find REF` lists ticket subscribers;
  JSON includes `subscribed_targets`. Queue subscriptions are independent: a
  ticket opt-out does not stop events requested by a queue subscription.
  `--submitter` records identity; `--pre-ack` records a product decision.
  Neither opts into notices. Tickets without either form of explicit
  ticket opt-in stay silent unless their queue has subscribers. Successor following uses the same CCC forward map as before.
- **Which events.** Subscribed filers use `config.notify_events` (default:
  `closed`, `needs_input`, `awaits_decision`). Queue subscribers keep their
  existing event stream. Queue/steer delivery is unchanged.
- **Dependencies.** File with `wt add --after DEP` or release current work
  with `wt edit REF --after DEP` (repeatable). The edit preserves history and
  answers, clears the old dependency question/park/claim, and leaves an open
  ticket gated by `blocked_by`. The dependency must close cleanly before
  dispatch can claim it. Starting or satisfying this wait emits no notice.
  `wt block --wait-on dependency` and recognizable wait-only questions are
  refused with the edit command; real human questions can mention refs.
  Legacy dependency blocks remain readable/resumable and emit no question
  notices. Existing human questions and active plans cannot be silently
  replaced by the dependency edit.
- **How it travels.** `messages.send(..., notify=True)`, i.e. the
  `_deliver_notify` chain: **uds → fifo → delegate, and nothing else**. A
  notice must never be worth a model turn, so `_deliver_resume`
  (`claude -p --resume`, a full-context turn nobody is watching), tty,
  codex-app-server and gemini-resume are all excluded. Unreachable means the
  target is idle: the notice waits in the outbox, and the `notify` flag
  persists on the outbox row so `drain_outbox` keeps the same guarantee.

Actionable notice detail is preserved in full; only claim previews may be
clipped, with a `wt find <ref>` pointer. See [length investigation](ticket-notice-length.md)
for history, measurements, transport limits, and a received long-question replay.

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

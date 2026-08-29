# Ticket-event push-back: submitter notifications + queue subscribers

Date: 2026-08-29
Status: implemented

## Problem

Filing a ticket with `wt add` is fire-and-forget. The agent that filed it has
no way to learn that the ticket got claimed, closed, or blocked on a question
short of polling `wt ls`/`wt show`. Separately, there was no way for a target
to hear about *every* event on a queue without filing every ticket itself.

## Design

**Submitter (Feature A).** `enqueue()` gains an optional `submitter: str`
field — the same addressable-target shape `messages.resolve_target()` already
resolves for `--report-to` (worker id / `@agent` name / session UUID). `wt add`
computes it from `--submitter` if given, else falls back to
`_default_report_to()` (the existing `$CLAUDE_CODE_SESSION_ID` /
`$CODEX_THREAD_ID` auto-detection). If neither is present the field is `""`
and notifications are silently skipped — never a hard requirement.

**Subscribers (Feature B).** `config.py` gains a per-queue `subscribers: [str]`
list, persisted in `queue-config.json` alongside the existing `claim_types`
entry, with `subscribers()`/`add_subscriber()`/`remove_subscriber()`/
`set_subscribers()` CRUD to match that precedent. Surfaced as `wt subscribe
<queue> [target] [--json]` (omit target to list) and `wt unsubscribe <queue>
<target>`.

**Single choke point.** `queue._notify_ticket_event(item, event, detail="")`
is the only place either feature calls `messages.send()`. It is invoked from
exactly four functions — `claim_next`, `claim_by_ref`, `close`, `block` — since
those are the only paths that drive a ticket into `claimed`/`closed`/
`needs_input` (verified: every other caller of the generic `update_status()`
only reopens tickets to `status="open"`). Targets are `{item.submitter} ∪
config.subscribers(item.project)`, deduplicated via an order-preserving
seen-set so a target that is both submitter and subscriber gets one message,
not two. Any failure — unresolvable target, missing module, adapter exception
— is swallowed; a notification hiccup must never block the underlying state
transition.

**GitHub-backed queues are fully supported, not a gap.** `submitter` has no
native GitHub field, so it rides in the existing metadata-block mechanism
(`_meta_block`/`_split_body`/`_body_with_metadata` in `github_backend.py`) the
same way `note`/`resolution` already do. It round-trips through every
meta-mutating operation (claim, close, block, reopen) because those only
`.update()`/`.pop()` specific keys out of the full meta dict — `submitter`
isn't one of the keys reopen pops, so it survives. Covered by
`test_github_backend_submitter_round_trips_and_notifies_on_claim_close_block`
in `tests/test_ticket_notifications.py`.

## Non-goals

No new delivery transport — this reuses `messages.send()` verbatim. No
broadcast/pub-sub primitive in the codebase already existed (checked); this is
intentionally the smallest structure that satisfies "notify submitter ∪
subscribers, no dupes."

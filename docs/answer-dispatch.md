# Answer acceptance and delivery

`wt answer REF TEXT` records the answer, clears `needs_input`, and returns an
`ANSWERED ... answer recorded; delivery queued for the daemon` acknowledgment.
It does not wait for session lookup, steering, a headless resume, or delivery
confirmation. The acknowledgment means the decision was saved, not that the
worker has applied it or finished the ticket.

For ordinary local tickets, `answer_dispatch` is saved with the answer in the
same store transaction. GitHub tickets save it in the same issue-body edit
that clears the question; the full answer remains in its durable issue comment,
referenced by URL/id and SHA-256 instead of copied into the body. GitHub API
persistence is still synchronous and may fail; failure is not acknowledged as
successful acceptance. Independent local answers take only short source locks,
never the takeover dispatch lock that spans native observation and transport.

The daemon's existing `answers.route_pending_answers` pass consumes these
intents before routing/retrying parked answers. Existing routing, affinity,
context-budget handoff, receipt verification and stage retry rules still apply.
`--tid`, engine overrides and attribution survive CLI exit. Tracker decisions
need no delivery; same-session takeover answers use their existing durable
supervision intent. The opt-in `answer-conditional` receipt protocol retains
its existing explicit single-attempt delivery contract.

The accepting host owns dispatch, so a second host reading the same GitHub
issue cannot also resume its worker. An intent is fenced by its ID, question
episode, claimant and session. Reblocking,
closing or replacing ownership cancels a stale request. Dispatch commits a
five-minute lease before transport, so an interrupted daemon can retry through
the existing liveness deduplication after restart. Transport exceptions are
logged as `ANSWER_DISPATCH_ERROR` and leave the request recoverable; acceptance
never depends on a detached CLI child or a background thread surviving exit.

Regression evidence is in `tests/test_answer_async.py`: ten real CLI processes
complete while the takeover dispatch lock is held, then a later daemon pass
recovers their answers. It also covers held claims, stale requests, restart
leases and full GitHub comment delivery. `tests/test_takeover.py` verifies an
answer replaces an intent during a slow transport without losing cancellation
serialization or letting the old transport outcome overwrite the new answer.

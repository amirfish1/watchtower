# Outcomes (opt-in preview)

Outcomes give a durable result an owner and a next action across ticket and
session boundaries. They live outside the claimable queue. The feature is off
by default; `wt outcome settings --enable` enables this installation.
Use `wt outcome --help` for the CLI and `wt outcome api ACTION --payload-json
'{...}'` for the additive JSON interface. Disabling preserves records and
prevents new outcome dispatch; it does not kill existing workers.

The existing supervisor reconciles outcomes. There is no additional daemon or
resident manager. A temporary lead must record dispatched work, a future dated
or event wait, a prepared existing decision, an independent verification request,
or a stop proposal. Ordinary tickets remain the executors, with one active
mutating executor per outcome. Role models come from the existing queue policy.
Wall limits, retries, wake budgets, concurrent attempts and per-tick spawns are
bounded in the persisted settings. Output-token ceilings are advisory.

Acceptance requires current-criteria evidence from a fenced independent
verifier or an explicitly accepted ticket gate. Ticket closure, suggestions,
model assertions and owner answers cannot directly establish delivery. Late
results are retained without applying them. Unknown delivery requires receipt
reconciliation rather than blind replay. Fences protect this store, not every
external action available to an agent with shell access.

Census intake reads a bounded response and scans at most 200 sessions hourly,
rotating a cursor. It associates only explicit claim, runtime, step or parent
evidence. Other sessions remain Unfiled with reasons and an agent-owned next
check; one existing lead takes bounded administrative intake for rows without
candidate outcomes. Repository suggestions are review material, never proof of
contribution. Archived, trashed and empty polling sessions are excluded.

Saved decisions are pulled from the existing inbox and matched by exact request,
origin and revision or generation. Only an accepted answer with a matching
receipt resumes the outcome. Source acceptance and session consumption are
recorded separately. Source outages preserve previous observations and expose
unavailable coverage. Intake never starts a model or sends a message itself.

Verified incidents require recent failed independent evidence. The
`incident_observed` event can record displaced outcomes without changing their
commitments or deadlines. Recovery requires a later passing check of the failed
criterion and an `incident_recovered` event. Active verified incidents receive
dispatch priority and block acceptance until recovery is recorded.

The JSON mutation receipt includes the post-commit revision. Pass that revision
as `expected_revision` for the next edit; conflicts must be reconciled rather
than overwritten. Browser adapters should admit only ordinary edits and review
requests, never trusted evidence, arbitrary incident declarations or acceptance.

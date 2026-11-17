# Event-driven release waits (WT-82)

`awaiting_release` means the fix is already integrated, with acceptance evidence
or an explicit live-check instruction. Timed ticket parking is retired by WT-87. The operational commands and targets are in
[the worker runbook](worker-runbook.md#code-complete-awaiting-release-wt-82).

## Event sources and completion

The existing BYM stamper (`scripts/deploy-stamper/{lib,wt}.mjs` in BYM-Finie)
adds deployment labels/comments. Its WT adapter only lists closed tickets and
subject-matched refs; it neither wakes nor closes tickets. Reusing those stamps
would miss an open wait whose commit subject did not name the ref. WatchTower
therefore observes GitHub deployment success directly. BYM currently reports
`Production – bookyourmat-app`; querying bare `production` finds no deployment.

The daemon coalesces reads by target, at most once per minute, and compares the
observed SHA against the recorded patch. It refreshes remote refs first and
fetches a new deployment SHA if necessary. This reads release evidence; it
never deploys, promotes, or restarts a service. Service adoption uses a frozen
process revision, not the moving source checkout: CCC `/api/version` `code_rev`
(discovered through `CCC_SERVER` or its published `port.txt`, not a fixed port),
WatchTower daemon boot revision (also exposed at its `/api/version`), or an
explicit HTTP JSON build endpoint. An explicit `release-observe` report supports
other service reporters. CCC dashboard and execution-worker adoption are separate;
a dashboard report cannot prove the worker's revision.

Default completion reopens once, with a run request and the release evidence,
for the worker's live check. Pre-proven acceptance may opt into automatic close
with stored summary, commit and verification evidence. Required gates always
route through a worker and the ordinary close path. Status plus the complete
wait record fence an event against newer claims, closes and replacement waits.
GitHub writes also re-read the event before reopening/closing; GitHub has no
server-side compare-and-swap, so its existing cross-host write limitation remains.

GitHub stores an explicit `release_wait` gate, without a synthetic expiry.
All active hosts must run WT-87 or later; older readers are unsupported.
A release event or a manual reopen removes the explicit gate.

## Migration and reversal

`release-migrate` previews typed release defers. Apply only after fetching their
release branch. It validates reachability and target, fences the old defer
reason, and keeps `previous_defer` for reversal. It never migrates integration
holds, untyped data waits, or active claims. Unknown paths/targets are reported
and accept explicit `--repo`/`--target` overrides. Legacy typed release defers can only be imported by the explicit migration
command; `wt defer` always refuses, including the former release spelling.

To reverse a migration, use `wt reopen REF --reason 'release wait correction'`
and record the genuine dependency/person gate or correct `wt await-release`.
The originals remain in `release_wait.previous_defer` and history for audit;
never restore a ticket timer.

Owner inventory is separate (`wt ls --status awaiting_release`, owner-view's
`awaiting_release`, health/queue counts). Default active lists omit it. Owner
attention surfaces waits after three days, missing branch reachability, or an
unknown branch check. It does not claim, close or manufacture an owner question.

## Measurement checkpoint

Before implementation, this host's activity log contained these DEFER counts
(UTC calendar days, not a cross-host total):

| UTC day | DEFER events |
| --- | ---: |
| 2026-11-14 | 73 |
| 2026-11-15 | 116 |
| 2026-11-16 | 87 as of the initial investigation |

The ticket's filing analysis counted 270 defers across 152 tickets since Nov14,
including 120 repeats; these are historical filing figures, not a new sample.
The live migration preview found ten currently open typed release waits: nine
BECKY waits with a BYM repo path and BYM-UX-78 without a stored path. All ten
commits resolved on fresh `origin/next` once the latter used an explicit BYM
checkout. Migration/adoption results and the post-change daily counts must be
recorded in ticket progress. A full after-day cannot be inferred from passing
tests or a partial-day counter; retain WT-82 for that bounded measurement.

## Dependency completion (WT-88)

An `awaiting_release` ticket counts as completed for `--after` dependents;
those tickets can proceed before the build ships. The release wait itself
remains unclaimable and keeps its build/adoption check. Declined or unresolved
completion records still hold dependents, and missing prerequisites fail closed.
Entering the wait dispatches dependent queues immediately through the ordinary
eligibility gates. Existing waits need no data migration: claim and display
projections use the same completion predicate.

Group integration uses the parked release commit as the child's commit proof;
it still requires the containing integration commit and the group's gates.
The dashboard completion fraction includes parked release work, while actual
closed-ticket history and release-wait inventory keep their distinct statuses.

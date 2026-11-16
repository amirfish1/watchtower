# Event-driven release waits (WT-82)

`awaiting_release` means the fix is already integrated, with acceptance evidence
or an explicit live-check instruction. It is independent of `deferred`, whose
clock is for accumulating data. The operational commands and targets are in
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
process revision, not the moving source checkout: CCC `/api/version` `code_rev`,
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

An old GitHub reader would otherwise interpret an unclaimed open issue as ready.
A wire-level year-9999 defer sentinel prevents that. Current readers suppress
this compatibility field and expose the distinct, untimed release state; the
event or a manual reopen removes it. There is no scheduled worker check.

## Migration and reversal

`release-migrate` previews typed release defers. Apply only after fetching their
release branch. It validates reachability and target, fences the old defer
reason, and keeps `previous_defer` for reversal. It never migrates integration
holds, untyped data waits, or active claims. Unknown paths/targets are reported
and accept explicit `--repo`/`--target` overrides. Legacy typed release defers
with a known target now enter the new state directly, avoiding new timer churn
from warm workers using an older prompt.

To reverse a migration, `wt reopen REF --reason 'restore legacy release wait'`,
then `wt defer REF --until ORIGINAL_UNTIL --reason ORIGINAL_REASON` **without**
`--awaits release` (which redirects to the new state). The originals live in
`release_wait.previous_defer` and the `await_release` history entry. Restore
an elapsed window using a newly bounded data/check window rather than a past date.

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

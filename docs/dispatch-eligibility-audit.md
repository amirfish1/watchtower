# Automatic GitHub eligibility evidence

`ELIGIBILITY` lines in `~/.watchtower/activity.log` explain what an automatic
reconcile observed before deciding whether a GitHub queue needs staffing.
They complement `SPAWN_PLAN` and `SPAWN`; they do not change dispatch policy.

Each process emits a record when an open queue's observed state changes and
at most one unchanged heartbeat per five minutes. A queue emits one drained transition,
then stays quiet until work returns. Dry runs do not write or suppress later
real records. Failed appends retry on the next pass; audit failures never
block staffing. At most 512 process-local dedupe entries are retained.

Each JSON record carries the daemon PID, reconcile ID, `scoped` flag, queue
policy, live/desired workers, independent depth and health calculations, and
up to ten listed ticket projections, oldest first. Ticket text, bodies, credentials, and
error messages are excluded; string and dependency samples are capped.
The snapshot includes the private list-cache timestamps, stale markers and
issue numbers, plus the already-parsed shared snapshot's equivalent fields.
Logging performs no GitHub requests and does not refresh any cache.

These reads are **separate observations**, not a transaction. `listed_items`
come from the reconcile's fleet listing; `counted_claimable` comes from its
later candidate count. A mismatch is evidence to investigate, not proof of a
particular cache defect. Shared cache fields can predate the private cache
because the logger intentionally does not read the shared file again.

For a missed dispatch, retain the matching `ELIGIBILITY`, `SPAWN_PLAN`, and
`SPAWN` lines and service journal. Compare `scoped=false` daemon decisions
with a fresh process's read-only observations, including PID and timestamps.
Keep existing workers alive. A scoped reconcile can recover staffing, but it
does not verify automatic dispatch and may erase the original condition.

WT-51's original Nov 1 ADS incident lacks raw eligibility evidence. WT-52
fixed a separate dashboard count error; the subsequent restart and Nov 2–6
automatic ADS dispatches do not establish the original staffing cause.
The regression in `tests/test_dispatch_audit.py` drives ordinary daemon
ticks in one process across a GitHub grace boundary with a warm listing
cache. Only external reads and the engine-launch boundary are substituted.

# Worker startup latency (WT-68)

The November 12 DECISIONS-FIX-15 report measured 36 seconds from enqueue to
claim. In America/Los_Angeles time: enqueue 08:10:20, spawn plan 08:10:37,
Codex thread started 08:10:39.300, claim command started 08:10:50.869,
claim completed 08:10:56.504. The SPAWN audit appeared at 08:10:57, after
the actual launch. The exact split of the 17 seconds before the plan was
not instrumented. Two CCC launches preceded that plan, each with a six-second
launch-failure observation window. That is evidence of serial scheduling,
not a complete measured attribution of the original delay.

Commit 8bd3cac moved fleet maintenance off the dispatch lock. It retained
one exclusive staffing lock across every queue's depth reads and serial
launch observations. A newly filed ticket could therefore still wait behind
many unrelated launches before starting its engine.

## Lock boundaries

Ordinary dispatch now takes `reconcile.lock` **shared**, then the exclusive
`reconcile-queues/<sha256(uppercase queue)>.lock`. Both the scoped and periodic
paths hold the same queue lock through fresh capacity reads, launch observation
and worker registration. The periodic pass acquires/releases it per queue,
re-reading capacity when it reaches each one. This prevents a stale fleet
snapshot from duplicating an intervening scoped launch.

Stage-only ticks and stage recovery continue taking `reconcile.lock`
**exclusive**, excluding every staffing pass. Old processes using an exclusive
global staffing lock also exclude the new shared holders during transition.
Fleet maintenance retains `reconcile-maintenance.lock`. Worker registry writes
retain their separate existing exclusive lock.

No worker limit, release rule, eligibility filter, model, fallback, or
launch-failure observation duration is changed. Different queues can observe
their launches concurrently; a single queue's launches retain their existing
order and observation windows.

## Measurement and remaining verification

`test_enqueue_to_claim_during_concurrent_launch_observation` launches five
harmless Python children on queue A, each with the real six-second observation
window, while enqueueing and dispatching B. Its child uses the actual local
queue claim function and records a monotonic claim timestamp. All stores and
logs are temporary; no model/provider or real ticket is used. Run with `-s`
to see enqueue-to-claim and dispatch-return timings. Dispatch returns after
its own six-second observation; this is distinct from the earlier durable claim.

Measured on November 15 against a frozen checkout of df6126c and the same
regression test: enqueue-to-claim **30.311s**, dispatch return **36.185s**.
With queue-scoped staffing locks: enqueue-to-claim **0.172s**, dispatch return
**6.058s**. The baseline fails the reported 30-second symptom check; the
patched implementation passes without shortening observation. These are
isolated local-worker timings, not model inference or production percentiles.

The event-controlled regression also overlaps a periodic pass with a scoped
dispatch and checks that the periodic pass does not launch B again. Separate
checks cover same-queue overspawn and the exclusive stage/recovery barrier
across processes and threads.

`SPAWN_START` records successful process creation immediately, including worker
ID, PID, engine, model and an ISO UTC timestamp. `SPAWN` still records the
completed staffing outcome after observation. Compare `ENQUEUE`, `SPAWN_PLAN`,
`SPAWN_START`, engine log thread/turn timestamps and `CLAIM` to separate queue
wait, engine startup, initial agent briefing and claim execution. An immediate
launch failure may have `SPAWN_START` followed by `SPAWN_FAIL` without `SPAWN`.

The isolated measurement proves the scheduling regression, not a live model's
startup performance. Before closing WT-68, retain natural live enqueue-to-claim
samples from the updated dispatcher under concurrent fleet activity. Attribute
remaining long samples to their actual evidence, including engine failures,
intentional grace/placement gates and occupied worker capacity; do not silently
exclude them or claim the original symptom fixed from a code inspection.

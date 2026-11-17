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

## November 16 live checkpoint: still open

The activity log records adoption of d1c2343c at November 15 18:07:42 UTC
(`DEPLOY_PICKUP` and `DAEMON_START`). Natural new-ticket samples after adoption
include the following. These are individual timelines, not fleet percentiles;
they do not establish that capacity was idle throughout every interval.

| Ticket | Enqueue (UTC, Nov 16) | Process start | Claim | Enqueue to claim |
| --- | --- | --- | --- | --- |
| WT-75 | 14:34:59 | 14:35:04 | 14:35:24 | 25s |
| CCC-556 | 14:35:19 | 14:35:25 | 14:35:48 | 29s |
| DECISIONS-FIX-23 | 14:52:03 | 14:52:28 | 14:52:44 | 41s |
| WT-77 | 15:16:41 | 15:16:44 | 15:17:00 | 19s |
| WT-78 | 15:58:06 | 15:58:12 | 15:58:25 | 19s |
| CCC-563 | 16:47:37 | 16:47:53 | 16:48:17 | 40s |
| WT-79 | 16:53:24 | 16:53:29 | 16:53:52 | 28s |
| CCC-564 | 16:57:31 | 16:58:05 | 16:58:25 | 54s |

WT-75 was already running when CCC-556 started, so real cross-queue activity
is present. Slow samples remain: DECISIONS-FIX-23 spent 25s before process
creation, CCC-564 spent 34s. Their DISPATCH outcomes say a worker was spawned;
neither timeline alone attributes the pre-plan delay to a particular function.
Do not close WT-68 from the faster WT samples. Other tickets can wait behind
occupied capacity, launch failures, grace or dependencies; second and later
claims by an existing worker measure work backlog, not engine startup.

Read-only scoped reconcile profiles on the current host took 1.317s for WT
and 2.121s/1.356s for consecutive CCC passes. Local SQLite loading dominated;
these profiles did not reproduce the 25–34s delay and exclude the non-dry-run
idle-release path. They are not evidence that historical delays were DB time.

`STARTUP_TIMING` now logs JSON with `phase`, `ref`, `elapsed_s` (monotonic),
`outcome` and `pid` for ticket reads, busy-claim reads, worker notification,
scoped staffing-lock acquisition, scoped idle release and total reconcile.
Lock and idle-release records have an empty ref; join them by queue/PID/time
to the enclosing dispatch. Reconcile includes its nested phases and launch
observation, so durations must not be summed. Logging failures cannot stop
dispatch. Dry runs do not emit lock or idle-release timings.

Next bounded check: after natural adoption of this instrumentation, inspect
the next day's natural fresh-ticket timelines, including all slow samples.
Use phase records to isolate any remaining >30s eligible idle-capacity delay
before choosing another repair. Do not create real work or extra model
workers just to obtain samples. If none occurs, record the missing exposure
and defer again; the original acceptance remains WT-68.

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

The November 17 checkpoint below replaces the former timed data wait under
the owner's WT-87 permanent-monitor policy. Do not park WT-68 for another
day of samples or infer permission to create real work/model workers.

## November 17: measured explanation and permanent check

The queue-lock regression is repaired (d1c2343); its isolated 30.311s ->
0.172s claim benchmark remains relevant. Instrumentation 0375420 was adopted;
natural records now separate process creation from agent briefing and claim.
These subsequent **fresh, first-worker** cases retain slow observations:

| Ticket | Enqueue UTC | Process | First claim | Total / pre-process / after-process |
| --- | --- | --- | --- | --- |
| WT-80 | Nov16 18:30:08 | 18:30:12 | 18:30:32 | 24 / 4 / 20s |
| CCC-567 | Nov16 18:35:24 | 18:35:31 | 18:35:54 | 30 / 7 / 23s |
| WT-81 | Nov16 19:42:57 | 19:43:01 | 19:43:21 | 24 / 4 / 20s |
| WT-82 | Nov16 21:08:11 | 21:08:15 | 21:08:29 | 18 / 4 / 14s |
| WT-83 | Nov17 01:44:50 | 01:44:53 | 01:45:22 | 32 / 3 / 29s |
| CCC-575 | Nov17 02:30:01 | 02:30:20 | 02:30:46 | 45 / 19 / 26s |
| CCC-576 | Nov17 08:07:03 | 08:07:16 | 08:07:34 | 31 / 13 / 18s |
| WT-85 | Nov17 08:29:53 | 08:29:58 | 08:30:16 | 23 / 5 / 18s |

CCC-575 STARTUP_TIMING: ticket_read 2.890s, busy_read 0.553s,
notify_workers 0.016s, staffing_lock 0.000431s, idle_release 0.007s,
reconcile 22.450s. Reconcile includes its six-second launch observation;
about 14s preceded SPAWN_PLAN after idle_release. That is **not lock wait**;
existing instrumentation does not identify its individual staffing function.
A current read-only scoped CCC cProfile took 2.270s, including 1.524s in
nine local DB reads. This does not reproduce the historical slow staffing.
WT-83 spent 29s after process creation, so further dispatcher lock changes
alone cannot establish a universal sub-30-second enqueue-to-claim result.
WT-84's eight-hour enqueue-to-claim interval is a later explicit RUN, not
fresh dispatch latency. Reclaims, backlog, capacity/readiness gates, and warm
pickup are also separate exposures, not silently counted as fast startups.

Options considered: speculative scheduler rewriting without causal evidence;
prewarming or model changes (outside current fleet-policy authorization);
or permanent natural measurement using the already-shipped repair. The last
follows WT-87's explicit owner instruction to finish verifiable work, close
with a caveat and install a permanent check instead of waiting for time/data.
It does **not** certify the remaining >30s observations resolved.

`python3 -m watchtower.startup_check --since ISO_UTC` reads existing activity
logs and durable tickets; it never launches workers, calls models, creates
traffic or changes fleet settings. Scope is WT/CCC/DECISIONS-FIX local queues,
one initial worker from zero, with a completed successful launch observation
and direct enqueue dispatch. It authenticates ENQUEUE against durable ticket
creation (within five seconds), since shared logs contain leaked test events.
It joins SPAWN_PLAN -> SPAWN -> SPAWN_START -> DISPATCH -> first CLAIM,
rejects ambiguous worker prefixes, and excludes explicit RUN/backlog/gated
exposures. Launch failures before SPAWN are outside this check's exposure;
existing launch-failure handling owns them. Timestamp precision is one second.

The watch uses a rolling 24h window, never older than its registered baseline.
Any qualifying >30s sample or still-unclaimed launch older than 30s fails;
missing exposure/creation evidence is pending, not a real pass. A missing/stale
activity log (>10min) fails without claiming real data. Faster later samples
cannot hide a slow first claim. A normal qualifying <=30s claim passes on real
data. The registered command runs every five minutes on `her`, links failures
back to WT-68, and stays visible in owner view until its first real pass.
The original historical window intentionally returns **fail**, retaining
WT-83/CCC-575/CCC-576; the new monitoring baseline starts at installation,
with the historical limits preserved here and on the ticket's caveat.

## WT-97: terminal socket timeout after successful process creation

The first permanent watch failure on November 17 was a different path from
queue contention: WT-93 enqueued at 18:53:41 UTC; worker `wt-83c1490a`
started at 18:53:46 and passed the six-second launch observation. Its log
`~/.watchtower/logs/wt-83c1490a.log` then ended with the wrapper's structured
`app-server did not open ... within 60s` error. No thread or claim existed.
The original monitor correctly retained this failed attempt even after a
replacement claimed WT-93 at 19:20:18; do not erase it with the replacement.
There is no evidence establishing why that particular Codex server stalled.
The nearby missing-bubblewrap message explicitly says a bundled fallback is
used; it does not prove a missing prerequisite caused the timeout.

Dead-worker postmortem previously returned no classification for this terminal
error, losing its launch-failure audit, bounded backoff and repeated-failure
alerting. It now recognizes the wrapper's exact structured terminal error.
Plain text, warning events and quoted ticket text do not qualify. Established
sessions retain the existing postmortem exclusion; auth/quota errors keep their
more specific classification. No timeout, engine, worker count or fleet policy
has changed. Reversal is the WT-97 commit's `git revert`.

Options considered: rewriting scheduler locks cannot address a five-second
dispatch followed by a server stall; raising the socket timeout increases the
wait; prewarming/changing engines is a fleet product decision. The chosen
change puts this transport failure through existing recovery and alerting.
Twelve focused failure/postmortem tests pass, including the actual terminal
error, false-positive controls and established-session exclusion. A separate
real installed Codex app-server probe opened its socket in 0.906s and completed
initialization in 0.936s, with zero threads, turns or model calls; evidence is
`/home/hermes/wt97-transport-rzp3l55x/result.json`. This verifies present transport
health, not the cause of the historical stall or universal sub-30s startup.
Future recurrence remains subject to permanent natural startup measurement;
the original watch and failed historical evidence remain intact.

## WT-99: archive reads on the immediate dispatch path

The November 18 failure was CCC-598: enqueue 20:02:58 UTC, process creation
20:03:08.053, thread start 20:03:10.961, claim command start 20:03:25.988,
durable CLAIM 20:03:29. Total enqueue-to-claim was **31s**: 10s before the
process and 21s afterward. Its worker log shows normal initialization and
briefing (including repeated skill/runbook reads), not WT-97's socket failure.
Claim command output completed at 20:03:38.759; that later return is distinct
from the durable claim measured by the watch.

Dispatch's ticket_read took 2.210s and busy_read another 1.483s. Both loaded
the entire local archive to inspect a single ticket and busy claims, before
staffing did its own fresh capacity read. Merely sharing that snapshot saved
only 0.159s in a paired cold-memo benchmark (1.547s -> 1.388s), insufficient
to account for the reported threshold breach. The implemented read instead
uses the existing project/status/ref SQLite indexes to deserialize only the
target ticket and in-progress tickets in its queue, once per notification.
Staffing and claims retain their own fresh reads and locks. Legacy JSON
falls back to the existing listing; GitHub retains its cached listing and
never adds a per-issue network fetch.

Five alternating pairs against the same real-store SQLite backup, clearing
the memo before every dispatch, measured median notification-read overhead
**1.604s -> 0.0045s**. Notification, staffing and audit delivery were stubbed:
no workers, models or synthetic tickets were launched. This measures the
archive-read repair, not total live model startup. Script and raw results:
`~/.watchtower/evidence/WT-99/dispatch_bench.py` and
`paired-benchmark-indexed.json`. Tests cover actual notification exclusions,
a claim racing the snapshot before staffing, plan gates, archive payloads
that must never be decoded, and GitHub/legacy fallbacks.

Options considered before implementation: reuse the full snapshot (measured
above), indexed hot reads (chosen after the first measurement), a broad claim
transaction rewrite (larger regression surface), or prewarming/changing the
startup product experience (a separate fleet-policy choice). Historical
CCC-598 evidence and the 30-second limit remain intact. Future natural cold
startup is the remaining end-to-end evidence and stays under a permanent
check; these measurements do not certify universal sub-30-second startup.

## WT-102: repeated archive copies during staffing and claim

WT-101 breached the same limit on November 19: enqueue 02:49:48 UTC,
process creation 02:49:58.837, claim command start 02:50:12.188, durable
CLAIM 02:50:20. The 32-second total comprises 10 seconds before process
creation and 22 afterward. WT-99's indexed dispatch reads took 0.001934s;
the staffing lock took 0.000291s. Seven seconds elapsed between idle release
and SPAWN_PLAN, but historical instrumentation does not attribute each part.
The worker initialized normally and read its brief before claiming; this
was not WT-97's terminal socket failure.

A cold copied-store claim profile identified repeated private copies of the
whole archive, including preliminary ownership and sent-back reads. Scoped
listings now filter the normalized read-only snapshot before making private
nested copies of their results. Read-only snapshots defer pickle generation
until a mutable caller actually needs a private store. The preliminary
sent-back check reads only the queue's indexed in-progress rows under its
existing lock. Candidate filtering excludes other queues before evaluating
model and eligibility predicates, while retaining the complete dependency
and group-parent index. The final locked mutation still reloads the full
store, rechecks ownership and sent-back claims, escalates stuck blockers,
and saves duplicate/group effects through the existing transaction.

Five alternating before/after pairs on the same real-store SQLite backup,
clearing the memo each time, measured median staffing reads (queue listing
plus claimable count) **1.570s -> 0.608s**, and complete local claim operation
**2.732s -> 1.965s**. Dependencies and side effects were stubbed equally for
both versions; no workers, models, customer messages or original-store writes
were made. These phase measurements are not an end-to-end latency promise.
Script, raw results and original ticket are in
`~/.watchtower/evidence/WT-102/`.

Options considered: narrow preliminary ownership reads; defer unused archive
serialization and filter before copying (chosen together); rewrite the claim
write transaction around hot rows (larger correctness risk); or prewarm/change
the startup experience (fleet product policy). Tests exercise nested result
isolation, mutable-store isolation, refreshed state after writes, exclusion of
unrelated archive copying, and cross-queue dependency retention. Historical
slow samples and the 30-second target remain unchanged. Future natural startup
remains subject to a new permanent check after adoption, under WT-87; a
fixture benchmark cannot certify universal sub-30-second model startup.

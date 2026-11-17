# No ticket waits on a clock (WT-87)

The November 17 owner note and later comment supersede the ticket's original
event-wait draft. There is no `await event` replacement. A ticket is being
worked, done, dependent on another ticket, blocked on a named person, or code
complete awaiting release. Future natural evidence belongs to checks attached
to closed tickets, with a caveat, never a ticket timer.

## Evidence and options

Initial fresh all-queue census: 68 nonclosed records have `deferred_until`,
including expired stale fields on existing owner/dependency blocks. Existing
admission reads wall-clock expiry in local queue, GitHub conversion and
liveness. GitHub also writes year-2099/year-9999 compatibility sentinels for
pipeline/release gates; those must become explicit metadata gates, never be
silently removed with their protection. `wt monitor` currently runs an ad hoc
command with no registry, history or deduplication. Owner-view warnings already
feed the decision desk's text/HTML morning digest.

| Option | Expected result | Risk/decision |
|---|---|---|
| Rename defer to event wait | Keeps tickets parked and adds polling machinery | Rejected by explicit owner decision |
| Delete timers and bulk reopen everything | Stops clock admission | Dispatch storm, loses evidence and existing gates; rejected |
| Product: close verified work, surface named watches until a first real pass | Removes dormant tickets while keeping future checks visible | Selected by owner; requires honest pending vs real-data pass and linked failures |
| Keep only the existing person/dependency/release gates | Removes arbitrary waits without broadening authorization | Selected; preserve all genuine gates during migration |

Implementation uses a durable registered-check registry and `close --watch`
snapshots stored in each resolution. A check defines a command, working
directory, execution location and schedule. Daemon execution schedules checks,
not tickets. A pass needs explicit real-data evidence; empty/no-data results
remain pending. Failures create deduplicated linked tickets. First real pass
removes owner attention; permanent checking continues for later regressions.
Check results never reopen the closed original.

Measured goal: 68 legacy waits -> zero active `deferred_until` fields; zero
wall-clock ticket-admission reads; installed CLI rejects defer; real-data watch
pass/failure/pending and deduplication tested on both ticket stores; owner view
and digest show pending watches; daemon frozen build reports the integrated fix.

At each active-work checkpoint, record measured coverage, remaining migrations
and the next check. Two attempts without progress require reassessing the
approach. No new event-wait status, sends, paid calls or bypass of approval
gates is authorized. Per-ticket migration mapping and final evidence follow.

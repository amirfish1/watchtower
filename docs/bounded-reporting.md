# Bounded queue reporting

Use `wt ls -q WT --closed-since 7d --json --limit 2000` for recent closures,
including old tickets closed this week. Selectors are a union: add
`--event auto_decision --event auto_decision_reversed --event-since 7d` and
repeat `--ref WT-60` to include events and referenced active tickets. Event
time is independent of ticket creation, closure and update time. `--*-since`
is inclusive; `--*-until` is exclusive. ISO offsets are honored; naive times
mean UTC. Status, unresolved and client filters apply before pagination.

Windowed JSON has `items`, `total`, `returned`, `offset`, `limit`, `complete`,
`next_offset` and `index`. Follow `next_offset` until it is null. `complete`
means that no later page or known source gap remains; check source coverage
as well as pagination before treating a result as an export. Offset pages
are live reads, not a pinned multi-page snapshot; use a fixed `--closed-until`
or `--event-until` and avoid concurrent changes when a stable export matters.
Legacy JSON without selectors keeps its bare-list shape.

`queue.window_items(project, WindowSpec(...), offset=0, limit=2000,
status="all", client_id=None)` is the Python API; timestamps are epoch seconds.
It returns `(items, metadata)`. `limit=0` reads the remaining matches.

## SQLite queues

A one-time migration derives closure/update timestamps and an event index
from authoritative `item_json`. Indexes select candidate primary keys before
counting, filtering and pagination; only the returned page is JSON decoded.
SQLite triggers queue changed primary keys, including updates from older
binaries that do not know the indexes exist. Reads repair only dirty keys.
Deletes remove event rows. Counts and pages share one transaction. Named
dependencies are fetched by indexed ref lookups, not a second full-store load.

## GitHub queues

Reporting reads never invoke `gh` or fetch tickets individually. On first use,
a queue-specific SQLite projection is built from the existing local list cache.
The normal snapshot writer maintains opted-in projections, reindexing changed
issue contents. Steady reads decode the page and its named dependencies only.
Sidecar/configuration changes or an older writer that does not update the
projection trigger a local rebuild under the cache writer lock.

The normal GitHub poller limits its closed snapshot to 14 days and caps each
state's rows. This API does not silently turn that snapshot into a complete
archive: `source_complete=false`, `complete=false` and `coverage` explain
missing, stale, possibly capped or retained history. Event/update/ref unions
can include long-closed issues outside the snapshot, so coverage is conservative
for those selectors. Older rows already available locally remain searchable.
When all available pages have been read, `next_offset=null` even if source
coverage is incomplete. No additional GitHub traffic or retention policy change
is introduced by reporting.

## WT-60 validation

Focused tests cover migration, old writers, same-clock/same-length changes,
reopening/deletion, selector unions, UTC/offset boundaries, filters before
pagination, cross-queue dependencies, indexed query plans, page-only decoding,
GitHub writer adoption and explicit source coverage.

A consistent SQLite backup of the live store on 2026-11-15 held 3,466 tickets,
including 71 WT tickets. The weekly closure/event union matched the previous
full-scan reference exactly (11 rows): first index construction took 0.857 s,
the old full-load selection took 1.009 s, and the indexed read took 0.00236 s.
A synthetic 4,000-row history read took 0.0124 s versus 0.375 s for full JSON
decoding. Timings are observations; regression assertions test decoded row
counts and query plans, avoiding wall-clock thresholds on a shared host.

# GitHub-backed queue read caps

Why the caps on `gh` reads are shaped the way they are, and what breaks when
a new read path bypasses them. Background: OPS-838/839 (a fleet of pollers
burning ~300 GraphQL points/min), fixed in WATCHTOWER-16.

## The caps only fire on a warm cache

Both guards live in `github_backend._list_issues`:

- the **ETag probe** — a free conditional REST request that turns a repeat
  read into a 304, and
- **`_LIST_FETCH_MIN_INTERVAL_S`** (60s) — a floor on how often a live fetch
  may happen at all.

Both require a populated `_LIST_CACHE` entry. That makes them invisible to
any *new* read path that starts cold: a fresh CLI process has an empty
in-memory cache, so it silently pays a full uncapped `gh issue list` per
state before either guard can engage. This was the actual shape of the
OPS-838 burn — not one hot loop, but many cold one-shot `wt` processes.

The fix is that non-strict reads seed `_LIST_CACHE` from
`~/.watchtower/gh-list-cache.json`, which carries both `etag` and
`fetched_at`. A cold process therefore starts warm enough to revalidate.

`strict=True` deliberately still pays for a live fetch. Its callers — claim,
close verification, `wt gh recheck`, `release_idle_workers` — are the paths
where acting on a stale list corrupts state (double-claiming a ticket, for
instance), so correctness outranks quota there.

## The two constants are coupled

`_LIST_FETCH_MIN_INTERVAL_S` (60s) and `_PERSISTED_LIST_STALE_S` (300s) must
move together. Raising the fetch interval alone ages persisted entries past
the staleness bound: seeded entries are then rejected as stale, soft readers
fall back to live fetches, and the change makes quota use *worse* than
before. Move both or neither.

## Adding a read path

If you add a code path that lists issues, route it through `_list_issues`
rather than calling `gh` directly, and decide explicitly whether it is
strict. A direct `gh issue list` anywhere else re-opens the OPS-838 hole.

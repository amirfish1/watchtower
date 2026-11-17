# GitHub history archive (WT-47)

A GitHub-backed ticket keeps its history in the issue body, which GitHub caps at
262,144 bytes. With archival on, the oldest entries move into hash-chained
comment segments and the body keeps a bounded tail plus a small index.

## What changes

- `history_archive` in the body metadata records how many entries are archived,
  the chain head and a digest. The tail stays at or under 300 entries and 96 KB.
- Readers rebuild the full history from the segments, so `wt` output looks the
  same. Comments are posted first; one body edit lands the tail and the index.
- Segments are immutable. A retry derives the same keys and adopts what an
  earlier attempt posted.

## Rollout

Archival is off by default. Readers ship first and work with the flag off.

```bash
wt history-archive status -q QUEUE
wt history-archive on -q QUEUE --attest-by NAME --hosts host1,host2
wt history-archive off -q QUEUE
```

Turn it on only after every host with write access to the repo runs a build that
stamps `hist_proto`; check each with `wt deploy --json`. `--attest-by` and `--hosts` record who confirmed that. A hand
edit to the config is not an attestation and leaves archival off. A claim from a
host without `hist_proto` inside the reader window also stops archival.

## When the chain cannot be verified

A missing or edited segment leaves the ticket degraded: `history_resolved` is
false and `history_archive_state` carries the error. Nothing is invented, and
each consumer fails closed:

- writers do not extend the chain; a write that fits still lands, and the byte
  budget refuses the rest
- `stale_readers` refuses unless a valid digest proves the last archived claim
  predates the window
- displacement guards hold, stage adoption stops, and stage health alerts
- `deep` labels its counts as lower bounds

The digest is read only while the chain is unresolved, and only with a valid
`dsha`, version and classifier version.

## Limits

- `dsha` detects damage, not forgery: a writer who recomputes it is trusted. Any
  account with write access to the issue is inside that boundary.
- GitHub has no compare-and-swap, so a write after our re-read can still be lost,
  as it could before archival.
- A pre-archival snapshot written back by an old host is logged as
  `GH-ARCHIVE-REVERTED`; the next write adopts the same segments when the
  boundary matches. Otherwise it posts a new chain and the old one is ignored.

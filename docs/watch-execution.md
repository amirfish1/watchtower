# Permanent watch execution (WT-90)

The scheduler executes each locally registered revision once per cadence, then
fans the receipt out to all closed originals using it. Duplicate closes may
inherit a snapshot with the parent's origin: origin is normalized to the closing
ref, while every executable field must exactly match a locally authorized spec.
External ticket bodies cannot introduce or change a command. A later duplicate
inherits the most recent receipt immediately. One execution failure creates one
failure ticket, linked to all originals, without reopening them.

`wt owner-view` marks a never-run check OVERDUE after two registered intervals
(legacy registrations fall back to attachment time). Failed/error receipts are
visible even after a prior real pass. Missing entrypoint or working directory
produces an error receipt and deduplicated failure ticket.

Registration and `close --watch` reject fixed-calendar pass rules such as
`pass after 2026-11-18T15:45:47Z` and `original 24h window`. Cadence, rolling
observation durations, sample minima and evidence cutoffs remain legitimate;
the check must decide from evidence rather than a calendar deadline.

To repair a command without abandoning its original bindings:

```
wt monitor --register NAME --cmd 'NEW COMMAND' --cwd /stable/repo \
  --where HOST --schedule 'hourly; evidence-based natural acceptance' \
  --every 3600 -q OPS --replace --reason 'why the original spec was unsafe'
```

Without explicit replacement and a reason, registrations remain immutable.
Replacement archives the old authorized spec and receipts, updates existing
bindings, and clears old acceptance. Historical issue snapshots of that exact
old spec resolve to the new locally authorized revision on future syncs; modified
or unknown specs still cannot execute. Roll back by replacing with the old spec
from `watches.json`'s `revisions` audit list using another explicit reason.

WT-90 live repairs:

- `becky-3461-portal-identity`, `becky-3502-ended-session`, and
  `becky-3531-archive-guidance`: replace
  `/home/hermes/Apps/BYM-wt3502-a6426748/scripts/monitoring/` with the already
  committed `/home/hermes/Apps/BYM+Finie/scripts/monitoring/`. The cwd stays BYM+Finie.
- `ccc550-sidebar-gates`: shared by CCC-550 and CCC-551. Original snapshot's
  CCC-550 origin previously prevented binding CCC-551. The existing check keeps
  its measured rolling cohort/budgets; the snapshot is now attributed per ticket.
- Mac `becky-3041-babysit-window`: same command and evidence directory; replace
  schedule `hourly; pass after 2026-11-18T15:45:47Z` with ongoing evidence-based
  observation. BYM commit 5e255f750 removes the fixed END and pass deadline,
  continues bounded capture of the latest day using unchanged bucket filenames,
  and retains original inbox, phase/row, long-lived RSS and error acceptance.
  Raw prior captures remain intact. No synthetic production request or send.

This infrastructure repair does not assert that pending natural customer
acceptance passed. Those checks continue permanently and report actual gaps.

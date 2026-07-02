# Friendlier worker session names (WT-49)

**Status: partially implemented.** WT now computes a friendly `display_name`
per worker (`watchtower/workers.py:annotate_activity`); wiring it into a
session-list UI (e.g. CCC's) is a follow-up in a different repo.

**Problem:** every spawned worker gets the same static engine session name —
`"<queue> queue worker"`, set once via `--name` at spawn
(`workers.py:build_drain_command`). That makes a session list (CCC's, or
`claude agents`/`claude --resume`'s own picker) show a wall of identical
labels, especially unhelpful *after* a worker closes its ticket, when a
human scanning the list wants to know "what did this one actually do"
(e.g. `WT worker: CCC-123 - CPU load fixed`).

**Constraint, confirmed by inspection:** `claude --help` has no rename
subcommand and no way to update `--name` on a running or finished session —
the engine name is fixed for the process's lifetime. So this can't be fixed
by mutating the underlying session name after the fact; it has to be a
presentation-layer overlay computed from WT's own ticket data, which is the
one place that already joins worker ↔ ticket (`annotate_activity`, used by
both `wt status` and the dashboard's `/api/status`).

**Who should own the join — WT or CCC?** WT, not CCC:
- WT already does this join once per status call (worker ↔ in-progress
  ticket, for `active_ref`); adding "worker ↔ most-recently-closed ticket"
  is the same shape of query against data WT already owns (`resolution`,
  `closed_at`, `claimed_by` on closed items — see `queue.py`'s item shape).
- Doing it in WT means every consumer of `/api/status` (CCC, `wt status`,
  any future client) gets the friendly label for free, instead of each
  consumer re-implementing "find this worker's last closed ticket."

**What shipped here:** `annotate_activity` now also attaches, per worker
row:
- `last_closed_ref` / `last_closed_summary` — the most recent closed
  ticket's ref and `resolution.summary`, for workers currently idle.
- `display_name` — a ready-to-render label:
  - never claimed anything: `"<queue> worker"`
  - holding an in-progress ticket: `"<queue> worker: <ref>"`
  - idle after closing one: `"<queue> worker: <ref> - <summary, clipped to 60 chars>"`

These fields ride the existing `/api/status` payload and `wt status`'s
worker rows/dashboard HTML for free (both already call `annotate_activity`).

**Follow-up (different repo):** CCC's session-list rendering
(`claude-command-center/static/app.js`, wherever it surfaces `claude
agents`-style session names) should prefer `display_name` from
`/api/wt/workers` or `/api/status` over the raw engine session name when a
row is a known WT worker. That change lives in CCC, not here.

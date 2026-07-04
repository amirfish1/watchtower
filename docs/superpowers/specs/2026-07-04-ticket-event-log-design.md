# Ticket event log: canonical history + timeline (design)

**Date:** 2026-07-04 · **Status:** approved (design v2) · **Owner:** amir
**Repos touched:** `watchtower` (engine), `claude-command-center` (thin UI)

## Problem

A ticket's activity is scattered across four stores that don't agree:

| Store | What lands there | Append-only? |
|---|---|---|
| `history` (WT-87) | claim, close, reopen, block | yes |
| `answers` | human answers — `queue.answer()` never writes `history` | yes |
| `progress_notes` | worker progress + CCC comments/reopen notes smuggled in via sentinel `by` values (`human-comment`, `human-reopen`) | yes |
| snapshot fields | `block_question`, `claimed_*`, `closed_*`, `resolution` | no — overwritten |

The CCC ticket modal ignores `history` and reconstructs Activity from the
snapshot fields, so anything overwritten is lost from the UI: WT-64 had a
block→answer round that vanished when a second `block()` overwrote
`block_question`. Separately, CCC's server pokes WT's *private* internals
(`_FileLock`/`_load_unlocked`/`_save_unlocked` in its `/api/ux-fixes/comment`
and `/reopen` handlers) because WT has no `comment()` primitive.

## Constraints (from owner)

1. **WT is the engine; CCC is a thin UI layer.** People will use WT without
   CCC / integrate WT into their own systems, so the timeline must be a WT
   API + CLI surface, not CCC render logic.
2. **Scale is small (zero external customers).** No data migration; a
   read-time helper that normalizes old-format tickets is enough. Only the
   last ~1–2 days of tickets truly matter.

## Design

### 1. One canonical event log (write side)

`history` becomes *the* event log. Every mutation appends one uniform entry:

```json
{
  "event": "answer",
  "at": "2026-07-04T08:12:00Z",
  "by": {"kind": "human", "worker": "wt-d9715912", "session_id": "<uuid>"},
  "text": "…",
  "…extra": "per-event fields below"
}
```

- `by.kind` is `"worker" | "human" | "system"`; `worker` / `session_id`
  included when known. `_append_history` gains the standardized `by` param;
  existing ad-hoc top-level `session_id`/`worker` keys remain readable by the
  normalizer but new writes use `by`.
- Event vocabulary and extras:
  - `filed` — from `enqueue()`; extras: `source`, `project`.
  - `claim` — extras: none beyond `by`.
  - `block` — extras: `question`.
  - `progress` — worker analysis-so-far (from `block(progress=…)` and any
    future progress API); `text` holds the note.
  - `answer` — human answer to a block; `text` holds it.
  - `comment` — plain status update (new public primitive, see §2).
  - `close` — extras: `resolution` (normalized dict).
  - `reopen` — extras: `reason`.
  - `edit` — triage field changes from `update()`; extras:
    `fields: {name: new_value}` (old values not required).
  - `move` — queue move; extras: `from_ref`, `to_ref`, `from_project`,
    `to_project`.
- Text values clipped with the existing `_clip` (4000 for questions/reasons,
  24000 for progress/answers — match current limits).
- **Stop writing the legacy lists.** `answer()` stops appending to
  `answers`; `block()` stops appending to `progress_notes`. The fields stay
  untouched on old tickets and are absorbed at read time (§3). Verified: no
  code inside watchtower reads either list; the only readers are CCC's modal
  (moving to `timeline`) and CCC's server writers (moving to the new
  primitives).
- Snapshot fields are untouched: they remain the **state machine**
  (`status`, `claimed_by`, `needs_input`, `block_question`, `resolution`
  answer "what is true now"); `history` answers "what happened". No
  event-sourcing; both written under the same `_FileLock`.

### 2. New public primitive: `comment()`

`queue.comment(ident, text, by="human", session_id="") -> item` — appends a
`comment` event, bumps `updated_at`, no status change. Exposed as
`wt comment <ref> "text"` in the CLI. CCC's `/api/ux-fixes/comment` and
`/reopen` handlers switch to `comment()` / `update_status(reason=…)` and drop
all use of WT private internals.

### 3. Read side: `timeline()` normalizer

`queue.timeline(item) -> list[event]` — the single place that understands
old formats. It:

1. Starts from `history` (normalizing legacy entries: hoisting top-level
   `session_id`/`worker` into `by`, mapping missing `kind`).
2. Merges in legacy `answers` (→ `answer` events) and `progress_notes`
   (→ `progress`, or `comment`/`reopen` for the `human-comment` /
   `human-reopen` sentinels).
3. Synthesizes `filed` (from `created_at`/`source`), and — only when absent
   from history — `claim` (from `claimed_at`/`claimed_by`), `block` (from
   `block_question`/`blocked_at`), `close` (from `closed_at`/`closed_by`/
   `resolution`).
4. Dedupes (an event synthesized from a snapshot is dropped when a real
   history event of the same type exists within the same timestamp) and
   sorts by `at` ascending.

Deterministic, pure (no I/O beyond the passed item), and safe on any ticket
shape ever produced.

### 4. Integration surface (WT-first)

- `wt find <ref>` prints the timeline as an "activity" section;
  `wt find <ref> --json` includes a `timeline` array (computed via §3).
- CCC imports `watchtower.queue`, so the modal calls the same data: the
  queue list/detail payloads CCC serves gain `timeline` per item (computed
  on detail fetch, not on bulk list, to keep `wt ls`-scale calls cheap).

### 5. CCC modal becomes a thin renderer

`_uxqOpenItemModal` (static/app.js) deletes the hand-built
Filed→Claimed→Blocked→Closed reconstruction and renders one node per
`timeline` event, chronological:

- `block` renders its own question; every block/answer round is a separate
  node — nothing collapses or overwrites.
- `answer`, `comment`, `progress`, `reopen`, `close` (with resolution
  rows), `filed`, `claim`, `move` each get a node style (reuse existing
  `uxq-tl-*` CSS classes; add small ones as needed).
- `edit` events render collapsed behind a "show edits" toggle so triage
  churn doesn't drown the timeline.
- The current-state footer node ("Open" / "In progress" / "Waiting for
  answer") stays, derived from snapshot fields as today.
- Answer/comment/reopen/close input sections are unchanged UX; they now hit
  endpoints that write canonical events.

### 6. GitHub backend parity — phase 2

`github_backend.py` mirrors the same event appends into its issue-body
metadata history (clipped text; issue bodies have size limits) and its items
flow through the same `timeline()`. Not blocking phase 1: all active queues
are file-backed.

## Non-goals

- No event-sourcing (state derived from events).
- No data migration of existing tickets.
- No changes to claim/locking semantics or the activity *log file*
  (`_log()` lines), which remains the cross-ticket operational feed.

## Testing

- `tests/test_queue.py` additions: every mutation appends its event with
  correct `by`/extras; `answer()`/`block()` no longer grow legacy lists;
  `comment()` primitive; `timeline()` on (a) a new-format ticket, (b) an
  old-format ticket with `answers`+`progress_notes`+sentinels, (c) a
  snapshot-only ticket (pre-WT-87), (d) a multi-round block/answer ticket —
  asserting order and no loss.
- CLI: `wt find <ref> --json` exposes `timeline`.
- CCC: comment/reopen endpoints no longer reference `_wt_q._load_unlocked`
  (grep-level assertion in review); modal renders multi-round block/answer
  fixtures.

## Implementation plan (for the Codex worker)

Phase 1 — `watchtower` repo:
1. `queue.py`: standardize `_append_history` (`by` dict), add events to
   `enqueue`, `update`, `move`, `answer`, `block` (progress→event), add
   `comment()`, add `timeline()`. Stop legacy-list writes.
2. `cli.py`: `wt comment`; extend `wt find` output + `--json` with timeline.
3. Tests as above; run the existing suite.

Phase 2 — `claude-command-center` repo:
4. `server.py`: switch `/api/ux-fixes/comment` + `/reopen` to public
   primitives; include `timeline` in the item payload the modal fetches.
5. `static/app.js`: render Activity from `timeline`; edits toggle.

Phase 3 (optional, later) — `github_backend.py` parity.

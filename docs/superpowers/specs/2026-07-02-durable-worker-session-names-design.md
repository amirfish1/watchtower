# Durable Worker Session Names Design

## Goal

WatchTower-created worker sessions should show meaningful names in session lists:
the queue, ticket ref, and a few words describing the ticket or outcome. The
last 24 hours of worker sessions should be backfilled so recent history is
scannable.

## Problem

WT-49 added transcript renames via Claude `custom-title` events, but live
transcripts show those titles are later overwritten by Claude's own spawn-time
metadata. A worker launched with `--name "WT queue worker"` receives later
`custom-title` and `agent-name` events with that generic name, after WT has
already written a useful title such as `WT worker: WT-71`.

The previous fix was therefore real but not durable. The final title in the
transcript can drift back to the generic spawn name.

## Design

1. Stop passing the static Claude `--name "<QUEUE> queue worker"` argument when
   spawning WT workers. That removes the source of later generic title resets.
2. Extend the canonical worker title helper so active tickets use ticket words:
   `WT worker: WT-71 - Add ticket editing`.
3. Keep close-time renames, using the resolution summary when present:
   `WT worker: WT-71 - Added wt edit command`.
4. Add a backfill primitive that inspects recent WT worker logs, maps worker id
   to session id, looks up the most recent ticket claimed or closed by that
   worker, and appends the canonical final title to the transcript.
5. Expose backfill through a hidden/maintenance CLI command so the current
   machine can repair the last 24 hours without hand-editing transcripts.

## Data Sources

- `~/.watchtower/logs/<worker-id>.log`: worker output containing session ids.
- WT queue state: current and historical ticket records with `claimed_by`,
  `claimed_session_id`, `title`, `note`, and `resolution.summary`.
- Claude transcripts under `~/.claude/projects`: renamed by appending a
  `custom-title` event via `messages.set_session_title`.

## Rules

- Runtime remains stdlib-only.
- Title formatting is deterministic and centralized in `workers.display_name`.
- Ticket title is preferred over note/text for claim-time names.
- Resolution summary is preferred for close/backfill names.
- Missing logs, missing transcripts, and non-Claude sessions are no-ops, not
  ticket-operation failures.
- Backfill defaults to 24 hours and supports dry-run output for verification.

## Verification

- Tests prove Claude spawn argv no longer includes `--name`.
- Tests prove active worker display names include ticket title words.
- Tests prove claim/backfill rename writes `ref - ticket title`.
- Tests prove close rename writes `ref - summary`.
- Tests prove recent-worker backfill maps logs and queue records to transcript
  title writes without touching stale workers outside the window.
- Live verification runs the backfill for 24 hours and inspects representative
  recent transcripts for final meaningful `custom-title` events.

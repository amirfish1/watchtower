# Retiring integration deferrals (WT-84)

`wt defer --awaits integration` now fails before reading or writing the
ticket. It explains the three available paths: the author lands the fix under
repo rules and uses `await-release`; a source-complete child closes only after
its own acceptance and the composing parent uses `--after`; an actual approval
or named-reviewer gate stays a classified block. Ordinary data windows remain
unchanged. Legacy integration-owner/path flags are parsed only so old callers
receive that same error; they cannot create a new integration deferral.

Both the worker spawn contract and worker runbook carry the replacement
instructions. No change authorizes a release, customer send, additional model
request, personal-browser access, or bypass of an existing gate.

## Live migration, November 17, 2026

The fresh all-queue scan found five nonclosed tickets carrying an integration
tag in their current defer reason. Their histories establish these dispositions:

| Ticket | Replacement | Preserved acceptance/gate |
|---|---|---|
| BECKY-3057 | Untimed production adoption wait; diagnostic commit `5523d73b2af444b619bae02990ff8d53f4a0de31` verified on freshly fetched `origin/next` | One live check after normal production adoption, followed by the original 24h natural portal-recovery evidence window; no manual cron or expedited release |
| BECKY-3470 | Worker wait on named BYM reform reviewer/coordinator `aa1f8c14-5a7b-4c13-b793-97d7ed246022` | Existing branch review and original attendance acceptance; Q989 terminal replay stop (5/6 requests, $0) remains in force |
| BECKY-3525 | Existing owner wait retained exactly; stale timer removed | Existing Q1152 first-answer/list evidence, Q960 restrictions and normal mobile-fix adoption remain open; question, claimant and wait unchanged |
| BECKY-3532 | Worker wait on the same named reviewer/coordinator | Whole-branch review and exact operational PROVIDER-READY receipt; non-book provider/wiring and original Anne acceptance stay open |
| BECKY-3540 | Worker wait on the same named reviewer/coordinator | Draft PR3545 review/checks, normal adoption and actual stored-template rendering; consent/channel gates retained |

These are migrations of wait records, not resolutions of the underlying
customer reports. None of the five tickets was closed or its acceptance moved
to a new ticket. The three review holds are classified worker waits, not new
owner questions. The production target is
`github:amirfish1/BYM-Finie:Production – bookyourmat-app`.

The final fresh scan covered 338 open tickets and found zero current integration
defer tags. The initial snapshots, applied mapping and final scan are recorded at
`/home/hermes/wt84-integration-before.json`,
`/home/hermes/wt84-migration-report.json` and
`/home/hermes/wt84-post-scan.json`. Each cleaned gate retains its exact previous
defer fields in an `integration_defer_retired` history event; the release wait
retains `release_wait.previous_defer` for reversal.

`watchtower.integration_migration.retire(ref, expected_reason=old_reason,
worker=worker_id)` is deliberately narrow: establish the real block/dependency
using normal commands first, then remove only the matching legacy timer. It
refuses closed, changed or ungated tickets. File writes hold the store lock;
GitHub writes use the backend's serialized body edit and merge path. As with
other GitHub writes, this is not a cross-host compare-and-swap guarantee.
Inspect the resulting record after applying it.

Focused tests cover actionable CLI rejection without mutation, valid release
proof and ordinary data windows, both migration backends, stale-snapshot
refusal, existing owner/worker gate preservation and exact reversal receipts.

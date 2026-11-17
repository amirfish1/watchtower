# Person decisions, actionable work and release adoption (WT-89)

`wt block` is for a concrete decision or fact a person can supply now. Use
`--wait-on owner`, or `--wait-on external --wait-actor NAME` for an identified
human. Worker/coordinator handoffs and text-only dependencies are rejected by
both the CLI and both backend write APIs, including `wt wait-on`. A missing
external name, placeholder question or future-event wait is also refused.
Historical wait records remain readable so migration never destroys evidence.

Do the ticket's work, or express genuinely separate prerequisite work with
`wt edit REF --after DEP`. This creates validated edges, rejects cycles and
releases the claim. Already-integrated `awaiting_release` prerequisites use
WT-88 completion semantics. An edge neither waives an explicit approval nor
certifies a source branch integrated or a customer incident repaired.

Future natural evidence belongs to a permanent registered check on a close,
with its evidence gap stated. It is not a person question. Explicit owner
pauses are iceboxed (`readiness=needs-rationale`) with the original reason;
revival needs a new rationale, rather than time passing. A routing repair
never authorizes a customer send, money change, replay, source publication,
security-policy change, live activation or destructive branch retirement.

Local claims with no proven live worker expire at the existing liveness idle
threshold (default 30 minutes). Live process identity, bound resumed-session
proof and recent session activity still veto recovery. Recovery uses the
existing fingerprint/claim CAS and preserves answers and review feedback;
it does not manufacture an owner question. Cancelled/completed supervision
also expires when its worker is absent. Its cancellation tombstone and
authorization scope survive, while reply routing follows the new worker;
a concurrent supervision generation change refuses recovery. GitHub claims
recorded on this host use the cross-host body lock: re-read the exact claim,
re-prove local liveness and idle/heartbeat age, then release under that same
lock. A fresh claim, answer in flight, recent session or failed process scan
refuses recovery. Ordinary work reopens; a legacy held human block releases
only its claim and keeps the question, wait generation and reply context.
A live human-block worker keeps publishing its owning-host heartbeat. Foreign/unknown hosts still need
owning-host proof; missing local processes do not establish remote death.

`awaiting_release` means only that an integrated commit needs a containing
release. The observer verifies branch reachability and the actual running
build, releases the ticket for one live acceptance check and records the
observed generation. The same commit/target cannot be parked again. Keep
working on a reproducible failure; close with a registered watch for natural
evidence. Existing required review gates and customer/money authorization
remain necessary. Release reasons contain only release adoption; original
post-release acceptance is retained as ticket progress.

## Reviewed migration

The baseline was **260 active blocks** on host `her`: 119 dependency, 44
worker, 70 owner, 16 external and 11 unclassified. A raw extra SBQ fixture
record was archived and excluded. Reviewed operations are recorded in
[person-block-migration.json](person-block-migration.json); the final mapping
is listed below; runtime verification accompanies the completed migration.

Six older immediate human actions also receive explicit wording repairs in
[person-block-wording-migration.json](person-block-wording-migration.json),
retaining their original Q tags and full question receipts. New convenience
parking is refused; real sign-in, credential and subjective acceptance
requests remain human gates.

The migration is explicit, not a prose classifier. `scripts/migrate-person-blocks.py`
is dry-run by default. Every operation fences the reviewed question, block
generation, wait, claimant and edges; changed records are refused. Writes use
normal local locks or GitHub serialized body edits and retain complete prior
block receipts. Guards are copied to progress for the next worker. Human
blocks release idle claims while retaining their question and reply context.
No ticket is closed, no question answered and no held branch deleted by this
migration. The two business-dependent PROJECTS tasks are iceboxed per the
owner's explicit instruction; their acceptance is not claimed complete.

`scripts/audit-no-parking.py` checks real inventory for forbidden waits,
release-evidence parking and stale local claims. It makes no network calls or
sends. Stale/missing open-source coverage is pending, never a real pass.

## Baseline dispositions

| Ticket | Previous wait | Disposition | Prerequisites |
|---|---|---|---|
| OPS-2 | external | person |  |
| IDEAS-6 | owner | retain |  |
| IDEAS-48 | owner | reopen |  |
| IDEAS-59 | dependency | dependency | OPS-545 |
| REPO-FIXER-210 | external | retain |  |
| REPO-FIXER-213 | dependency | dependency | STRAMP-242 |
| CHUCK-307 | external | person |  |
| IDEAS-96 | external | reopen |  |
| WT-32 | dependency | dependency | OPS-264 |
| IOS_US-13 | worker | icebox |  |
| CCC-251 | owner | retain |  |
| WT-41 | worker | person |  |
| VM-NEXT-37 | dependency | dependency | OPS-264 |
| VM-NEXT-39 | dependency | dependency | OPS-264 |
| OPS-264 | external | person |  |
| WT-45 | dependency | dependency | WT-47, WT-57 |
| IDEAS-111 | owner | retain |  |
| OPS-279 | owner | retain |  |
| OPS-285 | external | person |  |
| OPS-289 | owner | retain |  |
| REPO-FIXER-326 | dependency | dependency | BECKY-2995 |
| OPS-301 | owner | retain |  |
| REPO-FIXER-333 | dependency | dependency | BECKY-2995 |
| OPS-310 | worker | reopen |  |
| REPO-FIXER-349 | dependency | dependency | BECKY-2995 |
| REPO-FIXER-354 | dependency | dependency | BECKY-3000 |
| OPS-361 | external | person |  |
| IDEAS-164 | owner | retain |  |
| IDEAS-165 | owner | retain |  |
| IDEAS-167 | dependency | dependency | OPS-543 |
| OPS-380 | owner | retain |  |
| CCC-343 | owner | retain |  |
| OPS-395 | owner | retain |  |
| OPS-396 | external | person |  |
| SMALL-BUGS-87 | external | person |  |
| WT-57 | owner | retain |  |
| OPS-404 | unset | person |  |
| REPO-FIXER-471 | owner | retain |  |
| CCC-390 | dependency | dependency | CCC-251 |
| WT-61 | owner | retain |  |
| BYM-UX-2 | worker | reopen |  |
| BYM-UX-4 | unset | person |  |
| BYM-UX-7 | worker | reopen |  |
| BYM-UX-22 | unset | person |  |
| BYM-UX-27 | worker | reopen |  |
| BYM-UX-28 | worker | reopen |  |
| BYM-UX-29 | worker | reopen |  |
| BYM-UX-32 | unset | person |  |
| BYM-UX-34 | worker | reopen |  |
| BYM-UX-37 | worker | reopen |  |
| BYM-UX-38 | worker | reopen |  |
| BYM-UX-44 | worker | reopen |  |
| BYM-UX-47 | unset | reopen |  |
| BYM-UX-53 | worker | reopen |  |
| BYM-UX-58 | worker | reopen |  |
| BYM-UX-64 | owner | retain |  |
| OPS-435 | dependency | dependency | OPS-285 |
| REPO-FIXER-513 | worker | reopen |  |
| REPO-FIXER-514 | dependency | dependency | WT-57 |
| REPO-FIXER-515 | worker | dependency | BYM-UX-7, BECKY-3480 |
| REPO-FIXER-518 | dependency | dependency | BYM-UX-7 |
| REPO-FIXER-522 | worker | reopen |  |
| REPO-FIXER-525 | worker | reopen |  |
| REPO-FIXER-527 | dependency | dependency | BYM-UX-2 |
| REPO-FIXER-528 | worker | dependency | PR-SWEEP-6 |
| REPO-FIXER-551 | dependency | dependency | REPO-FIXER-513 |
| REPO-FIXER-552 | dependency | dependency | REPO-FIXER-513 |
| REPO-FIXER-553 | dependency | dependency | REPO-FIXER-513 |
| REPO-FIXER-554 | dependency | dependency | REPO-FIXER-513 |
| REPO-FIXER-557 | dependency | dependency | REPO-FIXER-513 |
| REPO-FIXER-561 | dependency | dependency | REPO-FIXER-513 |
| BYM-UX-71 | unset | person |  |
| OPS-447 | dependency | dependency | OPS-547 |
| IDEAS-208 | dependency | dependency | CCC-144 |
| REPO-FIXER-565 | dependency | dependency | REPO-FIXER-513 |
| REPO-FIXER-566 | dependency | dependency | WT-57 |
| REPO-FIXER-567 | dependency | dependency | PR-SWEEP-6 |
| REPO-FIXER-577 | dependency | dependency | BYM-UX-71 |
| REPO-FIXER-588 | dependency | dependency | REPO-FIXER-513 |
| REPO-FIXER-589 | dependency | dependency | REPO-FIXER-513 |
| REPO-FIXER-590 | dependency | dependency | REPO-FIXER-513 |
| OPS-452 | unset | person |  |
| BYM-UX-84 | dependency | dependency | OPS-547 |
| OPS-463 | dependency | dependency | OPS-538 |
| REPO-FIXER-591 | dependency | dependency | REPO-FIXER-513 |
| REPO-FIXER-593 | owner | retain |  |
| REPO-FIXER-594 | dependency | dependency | BECKY-3480 |
| REPO-FIXER-597 | dependency | dependency | BECKY-2995 |
| REPO-FIXER-598 | dependency | dependency | BECKY-2995 |
| REPO-FIXER-599 | owner | retain |  |
| REPO-FIXER-600 | owner | retain |  |
| REPO-FIXER-601 | dependency | dependency | BECKY-3480 |
| REPO-FIXER-604 | dependency | dependency | BECKY-3480 |
| REPO-FIXER-606 | worker | reopen |  |
| OPS-469 | owner | retain |  |
| CCC-463 | dependency | dependency | CCC-465, CCC-462 |
| REPO-FIXER-607 | dependency | dependency | REPO-FIXER-513 |
| REPO-FIXER-608 | dependency | dependency | WT-57 |
| REPO-FIXER-619 | dependency | dependency | WT-57 |
| OPS-496 | owner | retain |  |
| OPS-507 | owner | retain |  |
| IDEAS-331 | owner | retain |  |
| CCC-538 | owner | retain |  |
| OPS-517 | worker | dependency | BYM-UX-93, BYM-UX-94, BYM-UX-95, BYM-UX-96 |
| REPO-FIXER-621 | dependency | dependency | REPO-FIXER-513 |
| REPO-FIXER-622 | worker | dependency | CCC-251 |
| REPO-FIXER-623 | worker | dependency | PR-SWEEP-7, PR-SWEEP-8 |
| REPO-FIXER-635 | dependency | dependency | BECKY-3548 |
| REPO-FIXER-637 | dependency | dependency | REPO-FIXER-515 |
| REPO-FIXER-638 | dependency | dependency | REPO-FIXER-600 |
| REPO-FIXER-642 | dependency | dependency | BECKY-3483 |
| REPO-FIXER-643 | dependency | dependency | BECKY-3514 |
| REPO-FIXER-651 | dependency | dependency | BECKY-3532 |
| REPO-FIXER-652 | dependency | dependency | BECKY-3532 |
| REPO-FIXER-653 | dependency | dependency | PR-SWEEP-7 |
| REPO-FIXER-658 | dependency | dependency | PR-SWEEP-8 |
| REPO-FIXER-659 | dependency | dependency | PR-SWEEP-8 |
| REPO-FIXER-664 | dependency | dependency | CCC-251 |
| REPO-FIXER-671 | dependency | dependency | REPO-FIXER-513 |
| REPO-FIXER-672 | dependency | dependency | REPO-FIXER-513 |
| REPO-FIXER-673 | dependency | dependency | REPO-FIXER-513 |
| REPO-FIXER-674 | dependency | dependency | REPO-FIXER-513 |
| REPO-FIXER-675 | dependency | dependency | REPO-FIXER-513 |
| REPO-FIXER-676 | dependency | dependency | REPO-FIXER-513 |
| OPS-523 | dependency | dependency | OPS-452 |
| CHUCK-320 | owner | retain |  |
| VM-NEXT-48 | dependency | dependency | CHUCK-320 |
| OPS-530 | dependency | dependency | OPS-452 |
| OPS-532 | owner | retain |  |
| OPS-534 | dependency | dependency | WT-45 |
| OPS-536 | owner | retain |  |
| OPS-537 | dependency | dependency | OPS-452 |
| OPS-538 | dependency | dependency | OPS-285 |
| OPS-541 | dependency | dependency | OPS-452 |
| OPS-542 | dependency | dependency | OPS-538 |
| OPS-543 | dependency | dependency | OPS-538 |
| OPS-545 | dependency | dependency | OPS-538 |
| OPS-547 | owner | retain |  |
| OPS-549 | dependency | dependency | OPS-285 |
| OPS-550 | dependency | dependency | OPS-452 |
| OPS-551 | dependency | dependency | OPS-452 |
| OPS-553 | dependency | dependency | OPS-452 |
| OPS-558 | dependency | dependency | OPS-452 |
| OPS-559 | dependency | dependency | OPS-553 |
| OPS-562 | dependency | dependency | OPS-553 |
| OPS-567 | dependency | dependency | OPS-553 |
| OPS-568 | dependency | dependency | OPS-553 |
| OPS-569 | worker | reopen |  |
| CCC-547 | dependency | dependency | CCC-251 |
| OPS-575 | dependency | dependency | OPS-553 |
| OPS-582 | dependency | dependency | OPS-553 |
| OPS-583 | dependency | dependency | OPS-517 |
| OPS-584 | dependency | dependency | OPS-517 |
| OPS-585 | dependency | dependency | OPS-553 |
| REPO-FIXER-682 | dependency | dependency | REPO-FIXER-513 |
| REPO-FIXER-683 | dependency | dependency | WT-61 |
| REPO-FIXER-684 | worker | reopen |  |
| OPS-588 | dependency | dependency | OPS-517 |
| REPO-FIXER-689 | dependency | dependency | BECKY-3480 |
| REPO-FIXER-692 | worker | dependency | BECKY-3565 |
| REPO-FIXER-693 | worker | dependency | BECKY-3566 |
| REPO-FIXER-700 | dependency | dependency | BECKY-3470 |
| REPO-FIXER-702 | dependency | dependency | BECKY-3532 |
| REPO-FIXER-703 | dependency | dependency | BYM-UX-78 |
| REPO-FIXER-705 | dependency | dependency | OPS-536 |
| REPO-FIXER-707 | dependency | dependency | CCC-251 |
| REPO-FIXER-709 | dependency | dependency | BYM-UX-84 |
| REPO-FIXER-712 | dependency | dependency | CCC-251 |
| REPO-FIXER-713 | dependency | dependency | CCC-251 |
| REPO-FIXER-714 | dependency | dependency | CCC-251 |
| REPO-FIXER-715 | dependency | dependency | CCC-251 |
| REPO-FIXER-716 | dependency | dependency | CCC-251 |
| REPO-FIXER-717 | dependency | dependency | CCC-251 |
| REPO-FIXER-718 | dependency | dependency | CCC-462 |
| REPO-FIXER-719 | dependency | dependency | WT-61 |
| REPO-FIXER-720 | dependency | dependency | OPS-463 |
| OPS-590 | dependency | dependency | OPS-553 |
| OPS-593 | dependency | dependency | OPS-553 |
| BYM-UX-97 | dependency | dependency | OPS-594 |
| OPS-594 | dependency | dependency | OPS-517 |
| OPS-596 | dependency | dependency | OPS-553 |
| OPS-599 | worker | reopen |  |
| OPS-611 | owner | retain |  |
| OPS-627 | owner | retain |  |
| OPS-633 | owner | retain |  |
| BE-86 | owner | retain |  |
| IDEAS-355 | owner | retain |  |
| REPO-FIXER-724 | worker | dependency | OPS-641 |
| REPO-FIXER-730 | worker | dependency | BECKY-3569 |
| BECKY-3613 | owner | retain |  |
| BECKY-3608 | owner | retain |  |
| BECKY-3607 | owner | retain |  |
| BECKY-3595 | owner | retain |  |
| BECKY-3579 | owner | retain |  |
| BECKY-3577 | owner | retain |  |
| BECKY-3576 | owner | retain |  |
| BECKY-3575 | owner | retain |  |
| BECKY-3553 | owner | retain |  |
| BECKY-3548 | owner | retain |  |
| BECKY-3541 | external | retain |  |
| BECKY-3540 | worker | dependency | PR-SWEEP-8 |
| BECKY-3537 | external | retain |  |
| BECKY-3532 | worker | reopen |  |
| BECKY-3525 | owner | retain |  |
| BECKY-3524 | owner | retain |  |
| BECKY-3523 | owner | retain |  |
| BECKY-3516 | dependency | dependency | BECKY-3510 |
| BECKY-3514 | worker | reopen |  |
| BECKY-3513 | worker | reopen |  |
| BECKY-3510 | owner | retain |  |
| BECKY-3509 | owner | retain |  |
| BECKY-3492 | worker | person |  |
| BECKY-3487 | dependency | dependency | OPS-496 |
| BECKY-3483 | owner | retain |  |
| BECKY-3480 | owner | retain |  |
| BECKY-3477 | owner | retain |  |
| BECKY-3473 | owner | retain |  |
| BECKY-3470 | worker | reopen |  |
| BECKY-3462 | external | retain |  |
| BECKY-3448 | dependency | dependency | OPS-599 |
| BECKY-3447 | owner | retain |  |
| BECKY-3444 | owner | retain |  |
| BECKY-3422 | owner | retain |  |
| BECKY-3409 | owner | retain |  |
| BECKY-3398 | owner | retain |  |
| BECKY-3395 | worker | person |  |
| BECKY-3369 | dependency | dependency | OPS-536 |
| BECKY-3361 | owner | retain |  |
| BECKY-3328 | owner | retain |  |
| BECKY-3322 | owner | retain |  |
| BECKY-3298 | dependency | dependency | BECKY-2966 |
| BECKY-3296 | dependency | dependency | BECKY-3328 |
| BECKY-3277 | owner | retain |  |
| BECKY-3233 | worker | person |  |
| BECKY-3192 | owner | retain |  |
| BECKY-3000 | worker | dependency | BECKY-2995 |
| BECKY-2995 | worker | person |  |
| BECKY-2968 | dependency | dependency | BECKY-2886, BECKY-2995 |
| BECKY-2966 | worker | person |  |
| BECKY-2884 | worker | icebox |  |
| BECKY-2331 | worker | reopen |  |
| BYM-FUTURE-3330 | unset | person |  |
| BYM-FUTURE-3326 | unset | person |  |
| BYM-FUTURE-3324 | owner | retain |  |
| BYM-FUTURE-3299 | owner | retain |  |
| BYM-FUTURE-3297 | owner | retain |  |
| BYM-FUTURE-3289 | unset | person |  |
| BYM-FUTURE-2868 | owner | retain |  |
| BYMOPS-3054 | external | already-resolved |  |
| BECKY-DESIGN-3494 | owner | retain |  |
| BECKY-DESIGN-3249 | owner | retain |  |
| BECKY-DESIGN-3246 | dependency | dependency | BECKY-2995 |
| BECKY-DESIGN-3088 | owner | retain |  |
| BECKY-DESIGN-2844 | dependency | dependency | BECKY-2884 |
| STRAMP-315 | worker | reopen |  |
| STRAMP-265 | unset | person |  |
| STRAMP-211 | owner | retain |  |
| STRAMP-199 | external | person |  |
| PROJECTS-101 | external | icebox |  |
| PROJECTS-100 | external | icebox |  |


## Execution evidence

- Initial reviewed dispositions: 130 dependency dispositions, 73 existing human
  gates retained, 28 returned to work, 24 human routes corrected, four explicit
  owner pauses iceboxed, one already-resolved record verified untouched.
- Six immediate owner-action questions clarified with their original Q tags:
  OPS-2, OPS-264, OPS-285, OPS-361, OPS-396 and SMALL-BUGS-87. No question
  answered and no new send, sign-in or spend authority granted.
- Eighteen existing release waits normalized to release-only reasons; their
  full original acceptance survives in progress/history. Observed commit/target
  generations cannot re-park. This migration does not claim those releases or
  their live acceptance have occurred.
- SMALL-BUGS-130: expired cancelled supervision released; cancellation,
  authorization scope and original transcript provenance retained.
- STRAMP-242: stale owning-host lease explicitly released after a fresh local
  process/session/activity check. The old Q636 was already answered and was
  not restored as unanswered. Later Q760–Q764/Q762 billing/source/release holds
  remain in the original record and recovery receipt; no production or Stripe
  action occurred.
- Passing evidence: required push gate 425; takeover/liveness/reclaim 175;
  inventory/watch 23; person writer/migration 44; expanded GitHub lease/liveness
  132 (eight backend-specific skips). Additional final edge checks cover raw
  answer protection, fresh heartbeat, failed process scan, post-write conflict
  and foreign-host audit coverage. Passing evidence is reused for unchanged
  source; every pushed commit also runs the repository's required hook.
- Permanent registered check: `wt89-no-parking-v1`, every 300 seconds on `her`,
  runs `scripts/audit-no-parking.py` against real local and GitHub metadata.
  Forbidden blocks, stale local claims and missing remote heartbeat evidence
  fail; stale/missing open-source coverage is pending. Failure files a new
  linked OPS ticket rather than reopening or parking WT-89.

The migration initially stopped at BECKY-2966 because its existing body was
262,030 bytes and the new receipt exceeded GitHub's 262,144-byte hard cap.
The storage writer now removes optional JSON separator spaces only as a
last capacity fallback. The original human text and every metadata value
round-trip identically; a still-oversized payload is refused without a write.
Body-capacity/transport/concurrency compatibility: 122 passed, plus two new
lossless-fallback/hard-limit checks. The resumed operations retained their
original reviewed fingerprints; already-applied records are not replayed.


## What the routing changes mean

IDEAS-96 originally waited for a colleague's UI draft PR to appear. It is
reopened for the remaining authorized preparation, source identification and
review work. The missing PR and outreach/review-posting limits are retained;
no review is claimed complete and no message was sent. If work reaches a real
missing-person-information or send-approval gate, the worker must ask that
concrete question rather than park on the PR appearing.

IDEAS-48 originally asked whether to wait for a natural VM reboot. It is
reopened for finishing the authorized verification/check work. Reboot recovery
is still unproven; its worker must implement and run a permanent check for
future evidence. A deliberate VM reboot remains a separately authorized
operation. Neither routing change closes the original ticket or certifies
its original acceptance complete.


Final migration receipts: **260/260 verified/applied**, no outstanding
refusals: 130 dependency dispositions (138 named edges), 73 unchanged human
gates verified, 28 reopened work items, 24 corrected human routes, four owner
pauses iceboxed, one already-resolved record preserved. Six wording repairs
and 18 release-reason repairs also applied. BECKY-2966 live re-read verified
all 279 prior history entries and the human text unchanged at 259,766 bytes.

Live real-data audit after all mutations: **PASS**, zero violations and zero
stale/missing open-source cache coverage. The deployed owner-view command
returned 106 person blocks: 100 owner and six named external; zero worker,
dependency, unclassified or system blocks. These counts are a point-in-time
census; the permanent check enforces the invariant as other workers act.

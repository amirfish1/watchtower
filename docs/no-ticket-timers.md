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

## Migration results

The initial census contained 68 nonclosed human data timers. One more
(BECKY-3057) appeared during migration: 69 unique actionable records in this
mapping. A separate raw-body audit found hidden year-9999 release and year-2099
duplicate sentinels plus historical fields on already closed tickets. Across
these snapshots, **284 unique records on her/shared GitHub**, plus **22 local
records on ami**, carried obsolete timer fields. Host-local refs are distinct.

Every cleanup records `timer_retired` / `integration_defer_retired` with the
previous fields in append-only history. Questions, resolutions, explicit
release/pipeline gates, claims and source acceptance are preserved. An exact
reason/timestamp fence refuses a changed record; the sole local batch race
(OPS-225) was already cleaned under the store lock, confirmed by final audit.

Actionable mapping counts: **2 closed + registered watch**, **40 existing gate/dependency**, **27 working**.

“Working” is the already-authorized active state in the owner note, not a new
hold: these records are immediately available for actual repair or permanent
check implementation. Known unfixed symptoms and missing prevention were not
falsely closed. No future-data trigger, synthetic person block, or dependency
on this migration was invented. Subsequent workers can finish that work normally.

| Ticket | Migration disposition | Preserved gate or next actual work |
|---|---|---|
| BE-88 | working | Original acceptance remains open; actionable repair or runnable-check implementation |
| BECKY-2884 | existing gate/dependency | worker |
| BECKY-3041 | working | Preserved active worker claim; original diagnosis/approval remains open |
| BECKY-3051 | existing gate/dependency | BECKY-3041 |
| BECKY-3055 | existing gate/dependency | dependency BECKY-3041 |
| BECKY-3057 | working | Original acceptance remains open; actionable repair or runnable-check implementation |
| BECKY-3156 | existing gate/dependency | BECKY-3041 |
| BECKY-3251 | working | Original acceptance remains open; actionable repair or runnable-check implementation |
| BECKY-3283 | existing gate/dependency | OPS-445 |
| BECKY-3407 | working | Original acceptance remains open; actionable repair or runnable-check implementation |
| BECKY-3408 | existing gate/dependency | BECKY-3448, OPS-599 |
| BECKY-3443 | existing gate/dependency | BECKY-3526 |
| BECKY-3448 | existing gate/dependency | dependency OPS-1635 |
| BECKY-3456 | closed + registered watch | becky3456-callback-errors |
| BECKY-3461 | working | Original acceptance remains open; actionable repair or runnable-check implementation |
| BECKY-3466 | working | Original acceptance remains open; actionable repair or runnable-check implementation |
| BECKY-3477 | working | Original acceptance remains open; actionable repair or runnable-check implementation |
| BECKY-3480 | working | Original acceptance remains open; actionable repair or runnable-check implementation |
| BECKY-3501 | working | Original acceptance remains open; actionable repair or runnable-check implementation |
| BECKY-3502 | working | Original acceptance remains open; actionable repair or runnable-check implementation |
| BECKY-3512 | existing gate/dependency | BECKY-3526 |
| BECKY-3518 | working | Original acceptance remains open; actionable repair or runnable-check implementation |
| BECKY-3523 | existing gate/dependency | owner |
| BECKY-3524 | existing gate/dependency | owner |
| BECKY-3526 | working | Original acceptance remains open; actionable repair or runnable-check implementation |
| BECKY-3531 | working | Original acceptance remains open; actionable repair or runnable-check implementation |
| BECKY-3537 | existing gate/dependency | external |
| BECKY-3565 | existing gate/dependency | OPS-445 |
| BECKY-3566 | existing gate/dependency | OPS-445 |
| BECKY-3568 | existing gate/dependency | BECKY-3569 |
| BECKY-3590 | existing gate/dependency | BECKY-3448 |
| BECKY-3596 | existing gate/dependency | OPS-445 |
| BECKY-3602 | existing gate/dependency | BECKY-3448 |
| BECKY-DESIGN-3246 | existing gate/dependency | dependency BECKY-2995 |
| BYM-FUTURE-2868 | existing gate/dependency | owner |
| BYM-FUTURE-2989 | working | Original acceptance remains open; actionable repair or runnable-check implementation |
| BYM-FUTURE-2998 | working | Original acceptance remains open; actionable repair or runnable-check implementation |
| BYM-FUTURE-3216 | existing gate/dependency | dependency BYM-FUTURE-2989 |
| BYM-UX-84 | existing gate/dependency | dependency OPS-547 |
| BYMOPS-3054 | working | Original acceptance remains open; actionable repair or runnable-check implementation |
| CCC-144 | existing gate/dependency | CCC-459 |
| CCC-229 | existing gate/dependency | Preserved existing gate; see timer_retired history |
| CCC-343 | working | Original acceptance remains open; actionable repair or runnable-check implementation |
| CCC-390 | existing gate/dependency | dependency CCC-251 |
| CCC-459 | existing gate/dependency | CCC-537, CCC-538 |
| CCC-463 | existing gate/dependency | CCC-465 |
| CCC-550 | working | Original acceptance remains open; actionable repair or runnable-check implementation |
| IDEAS-86 | working | Original acceptance remains open; actionable repair or runnable-check implementation |
| IDEAS-90 | working | Original acceptance remains open; actionable repair or runnable-check implementation |
| OPS-2 | existing gate/dependency | OPS-3 |
| OPS-301 | existing gate/dependency | owner |
| OPS-310 | existing gate/dependency | dependency CCC-550 |
| OPS-447 | existing gate/dependency | dependency OPS-547 |
| OPS-517 | existing gate/dependency | worker |
| OPS-530 | existing gate/dependency | dependency OPS-452 |
| OPS-533 | working | Original acceptance remains open; actionable repair or runnable-check implementation |
| OPS-538 | existing gate/dependency | dependency OPS-285 |
| OPS-542 | existing gate/dependency | dependency OPS-538 |
| OPS-607 | working | Original acceptance remains open; actionable repair or runnable-check implementation |
| OPS-633 | existing gate/dependency | owner |
| REPO-FIXER-471 | existing gate/dependency | owner |
| REPO-FIXER-730 | existing gate/dependency | BECKY-3569 |
| REPO-FIXER-739 | working | Original acceptance remains open; actionable repair or runnable-check implementation |
| STRAMP-199 | existing gate/dependency | external |
| STRAMP-230 | working | Original acceptance remains open; actionable repair or runnable-check implementation |
| STRAMP-315 | working | Original acceptance remains open; actionable repair or runnable-check implementation |
| WT-61 | existing gate/dependency | owner |
| WT-68 | working | Original acceptance remains open; actionable repair or runnable-check implementation |
| WT-83 | closed + registered watch | wt83-related-feedback |

Hidden active gates retained without sentinels:

- BECKY-3365: explicit `release_wait`, awaiting_release.
- BECKY-3511: existing duplicate/pipeline gate.
- BECKY-3557: explicit `release_wait`, awaiting_release.
- BECKY-3569: explicit `release_wait`, awaiting_release.
- BECKY-3609: explicit `release_wait`, awaiting_release.

Additional **210 already-closed records** received field-only cleanup; no close or resolution was rewritten:

- BE-76, BECKY-2150, BECKY-3056, BECKY-3159, BECKY-3204, BECKY-3274, BECKY-3308, BECKY-3453, BECKY-3454, BECKY-3465, BECKY-3476, BECKY-3479.
- BECKY-3491, BECKY-3496, BECKY-3497, BECKY-3500, BECKY-3505, BECKY-3506, BECKY-3507, BECKY-3508, BECKY-3515, BECKY-3527, BECKY-3528, BECKY-3529.
- BECKY-3530, BECKY-3534, BECKY-3535, BECKY-3536, BECKY-3539, BECKY-3544, BECKY-3546, BECKY-3556, BECKY-3558, BECKY-3559, BECKY-3560, BECKY-3574.
- BYM-FUTURE-3323, BYM-UX-67, BYM-UX-91, BYM-UX-93, BYM-UX-94, BYM-UX-95, BYMOPS-3567, BYMOPS-3586, CCC-142, CCC-143, CCC-152, CCC-168.
- CCC-201, CCC-206, CCC-264, CCC-270, CCC-319, CCC-329, CCC-338, CCC-339, CCC-347, CCC-370, CCC-412, CCC-465.
- CCC-483, CCC-505, CCC-511, CCC-512, CCC-514, CCC-532, CCC-540, CCC-541, CCC-542, CCC-543, CCC-553, CCC-557.
- CCC-559, CCC-569, CCC-578, CHUCK-319, DECISIONS-FIX-13, DECISIONS-FIX-19, IDEAS-201, IDEAS-42, INTAKE-79, ONCALL-17, OPS-104, OPS-109.
- OPS-111, OPS-112, OPS-121, OPS-124, OPS-130, OPS-134, OPS-140, OPS-142, OPS-147, OPS-155, OPS-156, OPS-166.
- OPS-168, OPS-170, OPS-175, OPS-176, OPS-180, OPS-185, OPS-186, OPS-188, OPS-190, OPS-191, OPS-192, OPS-194.
- OPS-197, OPS-200, OPS-204, OPS-212, OPS-213, OPS-219, OPS-221, OPS-222, OPS-225, OPS-226, OPS-235, OPS-236.
- OPS-238, OPS-245, OPS-247, OPS-254, OPS-255, OPS-272, OPS-276, OPS-277, OPS-282, OPS-286, OPS-291, OPS-297.
- OPS-3, OPS-316, OPS-325, OPS-326, OPS-327, OPS-334, OPS-343, OPS-35, OPS-363, OPS-366, OPS-372, OPS-376.
- OPS-381, OPS-383, OPS-388, OPS-398, OPS-405, OPS-406, OPS-416, OPS-419, OPS-423, OPS-426, OPS-438, OPS-441.
- OPS-454, OPS-461, OPS-462, OPS-471, OPS-472, OPS-474, OPS-477, OPS-479, OPS-482, OPS-483, OPS-486, OPS-489.
- OPS-490, OPS-492, OPS-494, OPS-495, OPS-498, OPS-501, OPS-502, OPS-505, OPS-506, OPS-511, OPS-512, OPS-516.
- OPS-544, OPS-561, OPS-572, OPS-578, OPS-597, OPS-602, OPS-608, OPS-618, OPS-640, OPS-645, OPS-83, OPS-84.
- OPS-85, OPS-87, OPS-92, OPS-97, OPS-98, OPS-99, PROJECTS-9, REPO-FIXER-224, REPO-FIXER-227, REPO-FIXER-235, REPO-FIXER-625, SMALL-BUGS-136.
- SMALL-BUGS-53, SMALL-BUGS-59, STRAMP-258, VM-NEXT-46, WT-80, WT-82.

ami local mapping (21 historical closed fields; one existing owner money gate):

| Ticket on ami | Disposition |
|---|---|
| OPS-1490 | historical-field-cleanup |
| OPS-1570 | historical-field-cleanup |
| OPS-1571 | historical-field-cleanup |
| OPS-1572 | historical-field-cleanup |
| OPS-1573 | historical-field-cleanup |
| OPS-1574 | historical-field-cleanup |
| OPS-1575 | historical-field-cleanup |
| OPS-1576 | historical-field-cleanup |
| OPS-1577 | historical-field-cleanup |
| OPS-1578 | historical-field-cleanup |
| OPS-1600 | preserve-existing-owner-money-gate |
| OPS-1606 | historical-field-cleanup |
| OPS-1613 | historical-field-cleanup |
| OPS-1628 | historical-field-cleanup |
| OPS-1645 | historical-field-cleanup |
| OPS-1666 | historical-field-cleanup |
| OPS-1668 | historical-field-cleanup |
| OPS-1671 | historical-field-cleanup |
| OPS-1672 | historical-field-cleanup |
| OPS-1674 | historical-field-cleanup |
| OPS-1680 | historical-field-cleanup |
| OPS-1685 | historical-field-cleanup |

## Verification and operation

- `wt defer` (bare or legacy arguments), local API and GitHub API refuse without
  mutation. Admission/liveness/health no longer read expiry; legacy metadata is
  read only by explicit migration/audit code. There is no event-wait replacement.
- Focused broad suite: 221 passed / one intentional skip before six affected
  assertions exposed removed projection fields; corrected tests 13 passed.
  Final watch/release/migration suite 72 passed / one skip; pipeline 53 passed;
  owner-view/watch/digest 47 passed; admission/health/deep 44 passed.
- Required pre-push gate: 425 passed. Exact registered commands only may execute;
  changing an external issue-body command cannot introduce an executable check.
- WT-83's real close feedback ran: 36 useful, five not useful, two unavailable,
  useful ratio 87.8% (41 rated). This is current evidence, not a full-week claim.
  Its first real pass clears owner attention while permanent checks continue.
- BECKY-3456's existing production alert feed is current, with no qualifying
  natural callback receipt. Its watch honestly remains pending; absence of an
  alert is not a production acceptance pass. No traffic or sends were generated.
- `watchtower-no-ticket-timers` audits actual raw ticket metadata every 15 minutes.
  A reintroduced field files a deduplicated ticket linked to WT-87. Watch commands
  use the local registry (or an isolated companion for WATCHTOWER_STORE), and the
  scheduler is independent of release-observer errors.

Final zero-field census and frozen daemon revision are recorded in the ticket's
close evidence after final source adoption; checkout HEAD alone is not proof.

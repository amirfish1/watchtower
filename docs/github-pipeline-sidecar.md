# GitHub pipeline sidecar (OPS-201, owner decision Q477 = C)

GitHub-backed queues (BECKY, …) keep the GitHub issue as the **canonical
ticket**. The gate pipeline (plan → plan review → build, later verify) has no
storage on GitHub, so its state lives in a **local sidecar** keyed by the
issue's canonical GitHub identity. This doc is the ownership and
state-transition contract; tests in `tests/test_github_pipeline_sidecar.py`
pin it.

## Ownership split

| Owned by the GitHub issue (unchanged)                  | Owned by the sidecar                         |
|---------------------------------------------------------|----------------------------------------------|
| title, body, note, status (open/closed), claim fields, close + resolution, defer, history of ticket events, labels | `gates`, `plan` (status/round/text/reviews/discussion/planner/reviewer), `stage_session`, the review hold (`review_hold`, `gate_pending`, `gate_stages`, `gate_results`, `verify_cycle`, `verifier`, `gate_feedback`, `gate_accepted_by`), `gen`, pipeline history events |

- The sidecar never stores ticket state; GitHub never stores pipeline state.
- **One GitHub write** belongs to the pipeline: the meta key
  `pipeline_host: <machine_tag>/<owner id>`, written when gates are set (and
  removed when they are cleared). It is the federation fence (below). The
  owner id is a UUID made once per store in `pipeline-owner.json` beside the
  sidecar (OPS-203): `machine_tag()` is 3 chars and can collide, the UUID
  cannot. A bare `<machine_tag>` marker (OPS-201 format) still counts as this
  host's; the next `wt edit --gate` upgrades it.

## Identity and storage

- Key: `"<owner>/<repo>#<number>"` — survives a queue rename; the ref
  (`BECKY-2332`) is derived for display only.
- File: `github-pipeline.json` next to the queue DB, JSON
  `{"version": 1, "items": {key: entry}}`, written atomically (tmp + `os.replace`).
- Every mutation runs under the queue store's `_FileLock(_lock_path())` — the
  same single lock local pipeline writes use, so no new lock ordering exists.
  Network I/O (the GitHub base read) happens **before** taking the lock.
- Readers (overlay) read without the lock; atomic replace makes that safe.

## Overlay (read path)

`GitHubIssueBackend._issue_to_item` applies the sidecar on every item it
returns (memo hit or miss, after the memo so the memo stays pure GitHub):
copies `gates`/`plan`/`stage_session`, merges pipeline history into
`history` (sorted by `at`), then:

- `plan_pending(item)` ⇒ `work_it = auto_eligible = manual_eligible =
  claimable = False`. This one point covers `claim_next`, `claim_by_ref`,
  `peek_next`, `count_claimable`, `count_manual_eligible` (all read work_it).
- **Foreign host**: meta `pipeline_host` ≠ this store's marker ⇒
  `pipeline_foreign = True`, not claimable, `stages.desired()` skips it,
  `wt edit --gate` refuses. A host without the state never builds a gated
  ticket unplanned. Every marked item carries `pipeline_owner_host` /
  `pipeline_owner_id` (`wt find --json`), and the fence error names the
  owning host to run `wt plan show` on.
- **Missing state**: meta `pipeline_host` == this host but no sidecar entry
  (lost/corrupt file) ⇒ `pipeline_missing = True`, not claimable, fail-closed
  (surfaced by `wt find`); recovery = re-run `wt edit <ref> --gate …`.

## Mutation path

`queue._plan_update` and `queue.stage_session_update` route GitHub refs to
`github_pipeline.update(ident, attr, fn)`:

1. `base = backend.get(ident)` (fresh, outside the lock). GitHub unreachable ⇒
   exception propagates, **sidecar untouched**; the stage tick retries.
2. Under `_FileLock`: load sidecar, overlay entry onto a copy of `base`, run
   `fn(it, state)` — unchanged functions, same `"skip"` CAS contract.
3. Persist `state` into the entry plus any history events `fn` appended;
   bump `rev`; atomic write.

Plan transitions are serialized by the lock and keyed on local state, so two
concurrent `plan_start` calls yield one fresh round. `fn` may read a
GitHub-derived field (status, claim) that is seconds stale; the only
consequence is one extra tick (the next tick re-reads, `desired()` stops on a
closed/answered ticket).

## Write path for gates

`wt edit <ref> --gate plan[:model]` on a GitHub ref:
validate → refuse if foreign host → **reader check** → write `pipeline_host`
marker via `backend.update` → **re-read the issue and require our marker**
(GitHub has no CAS on issue edits: a host that wrote last owns it, and the
loser fails with no sidecar entry; GitHub down ⇒ edit fails, nothing local
written) → write sidecar entry.

Reader check (OPS-203, relaxed OPS-221): every GitHub claim stamps
`pipeline: <PIPELINE_PROTO>` (2 since OPS-215) on its history event. Gating is
refused while other machines have claims in the last 14 days below the required
protocol: **READER_MIN_PROTO** (1: OPS-203 onward) **for plan gates** (protected
by issue-level `deferred_until`), and **PIPELINE_PROTO** (2) **for non-plan
gates** (verify/review/cmd only; no issue-level protection). Without the
sidecar, a proto-1 host could otherwise claim and skip verify on a non-plan gate.
Every protocol 1+ host honours the `deferred_until` hold (OPS-215), so only an
unstamped (protocol 0) pre-OPS-203 claim blocks plan gates. The error names the
host and its last claim. Once that host runs the required version, its next
claim clears the check. A retired host ages out after 14 days. Crash between
the two ⇒ marker without entry ⇒ fail-closed `pipeline_missing`, never an
unplanned build. Gating a ticket a builder already holds is **refused** (the
claim is left untouched): the hold only excludes new claims, so a running
builder would not pause for the planner. Gate it once it is released.

## Scope

Milestone 1 (OPS-201): `plan`, `plan:<model>` gates via `wt edit`; the full
plan lifecycle (start, submit, review verdict, discussion, decide, fail);
claim exclusion; federation fence; restart.

Milestone 2 (OPS-204): `verify`, `review*`, `cmd:` gates and `wt add --gate`
on GitHub refs (`--accept`, `--planner-model`, `--verifier-model` stay
refused: the verifier checks the ticket text).

## Review hold (OPS-204)

`wt close` on a ticket with this host's sidecar gates (`pipeline_local`;
queue-config gates never gate a GitHub close) runs the `cmd:` gates first:

- a failed cmd gate reopens the issue (CAS on the pre-gate status, owner
  checked) and records `gate_results` + `gate_feedback` in the sidecar;
- with `verify` / `review*` owed, the issue is **not closed**: the sidecar
  gets `review_hold = {resolution, session_id, session_uuid, at}`,
  `gate_pending`, `gate_stages`, `gate_results` and, for verify,
  `verify_cycle + 1`. The overlay shows `status = in_review`, the held
  resolution, and holds the ticket (not claimable). `wt close` on it is
  refused (it settles through `wt verdict` / `wt accept` / `wt reject`).
- cmd gates only, all passed: an ordinary issue close; results recorded.

Settling (`verdict` / `accept` / `reject`) is two steps:

1. Under the store lock: check status, stage, generation and cycle, then
   either advance to the next stage (verify pass with a review owed: no issue
   write) or record the outcome as `review_hold.final = close|open`.
2. The ONE canonical issue write: close with the held resolution (attributed
   to the original worker), or reopen with the reason. Then the hold is
   dropped (`gate_feedback` kept on a reopen).

A failed step 2 (GitHub down) leaves `final` set and the ticket in_review;
`wt accept` retries the write. If step 2 landed but the drop did not, the
next mutation sees the issue already reflects `final` and drops the hold.

**Stale verdicts:** a GitHub `wt verdict` requires `--verify-cycle <n>` (the
verifier prompt carries it); any other cycle raises the `SUPERSEDED` fence
(`GroupFenced`), and nothing is written. After a settle the ticket is no
longer waiting on a verifier, so a late verdict is refused too.

**Generations:** clearing gates (`--gate ''`) leaves a tombstone
`{gen, verify_cycle, retired_workers}` (every planner/reviewer/verifier/stage
worker id of the entry) instead of deleting it. A re-gate starts generation
`gen + 1`: verify cycles keep counting (never reused), stage keys get a
`:g<gen>` suffix (no adoption of the old generation's sessions), and
`plan_submit` / `plan_verdict` / `verdict` from a retired worker id are
refused. Gates cannot change while the ticket is in review.

**Prechecks in the lock (#9):** plan_submit / plan_verdict / plan_decide
re-check the plan status inside the locked callback (local and GitHub).

**Reserve before spawn (#10/#11):** before `spawn_adhoc`, the stage tick
reserves `{key, attempt}` in `stage_session` under the lock after re-running
`desired()` on the fresh item. A stage that settled since the tick read it,
an attempt already spawned, or a live reservation (< 300 s) makes the spawn
a no-op. A spawn error releases the reservation; a crash lets it expire.

**plan_decide (#6):** a callback that moves the item `in_progress → open`
(releasing a build with no resumable session) is written to the issue as a
CAS reopen after the sidecar write.

## Cross-host limits (honest remainder)

- **Issue-level hold (OPS-215).** While a ticket is plan-pending or held in
  review, the owner also writes `pipeline_hold: {why, prev_defer, prev_reason}`
  and `deferred_until: 2099-12-31T00:00:00Z` into the issue meta. The hold
  rides on the marker write at gate time, and every pipeline mutation resyncs
  it afterwards. Every `wt` since OPS-100 keeps a deferred ticket out of
  `claim_next`, `claim_by_ref` and `wt run`, so a host that cannot read the
  sidecar is refused from the issue alone. That includes a host older than
  OPS-201, a downgraded host, or one first claiming after the ticket was gated.
  A held in_review issue that is reopened stays deferred too. A `wt` at
  `PIPELINE_PROTO` 2 or later sees through the sentinel to the saved human
  defer and lets the overlay decide. Lifting the hold restores that defer;
  a `wt defer` made during the hold is kept. Limits:
  - a `wt` older than OPS-100 (2026-10-25) is not fenced;
  - a crash between the sidecar write and the issue write leaves the issue
    fence one step behind until the next mutation.
- A current `wt` on another host fails closed: the marker makes the ticket
  `pipeline_foreign` (no claim, no stage). Its orphan sweep may still reopen
  a held issue whose builder is gone; the owning host keeps showing it
  in_review (the hold stays live until `final` lands), so the review still
  settles it there.
- Ownership never moves between hosts (no `wt pipeline adopt` yet).

## Independent review (wt critique, 2026-10-29) and disposition

Codex review scored the doc-only contract 3/10. Disposition:

- Fixed in this slice: implementation + tests exist (#1); every pipeline field
  a callback touches is persisted in one transaction, not just one attr (#7);
  GitHub `wt answer` on a stage escalation runs `_stage_retry_reset` in the
  sidecar (#8); the list-items memo keys on the sidecar signature (#5);
  `pipeline_host` is an explicit backend.update key (#3, partly); gating a
  running build is refused (#13); queue-config gates never start a GitHub
  planner or hold a GitHub ticket (#14); clear deletes the entry before the
  marker, so a crash fails closed (#12, partly); `plan_decide` clears the
  canonical block on the issue after the sidecar write (#6, partly).
- Parity with local queues, not new risk: `plan_fail` releases the ticket
  ("failed" builds loudly) (#2).
- Fixed in OPS-204: in-lock status prechecks (#9); reserve-before-spawn with
  a `desired()` recheck (#10/#11); `plan_decide` reopens the issue (#6);
  generation tombstone + retired-worker fence (#12).
- Closed by OPS-203 (#3/#4): per-store owner UUID in the marker, verified
  marker write (lost race = not gated), and the reader check above. The
  claim-time remainder (a host downgraded, or claiming for the first time,
  after gating) is closed by OPS-215's issue-level hold. GitHub duplicate links (Mac OPS-1456) need verify/review
  proof on GitHub first, so they follow OPS-204.

## Activation / resume procedure

0. Preconditions: every host that claims the queue runs a `wt` at or after
   the OPS-203 commit (git -C <its wt checkout> merge-base --is-ancestor; on
   the Mac that checkout is `~/.ccc/watchtower`, see the `~/.local/bin/wt`
   shim). `wt edit --gate` enforces this from claim stamps; the ticket is
   unclaimed, or gated by its own claimant (`WT_WORKER_ID` == claimed_by), which
   then waits for the plan. Another worker's live claim is refused.
0b. The stage tick runs in the reconciler: it plans GitHub tickets only once
   the reconciler process has been restarted onto this change (until then
   `wt plan show` stays `none`). That restart is the release step; it is not
   automatic.
1. On the owning host (`her`): `wt edit BECKY-2332 --gate plan`.
2. `wt find BECKY-2332 --json` shows `gates: ["plan"]`, `plan: {}` and the
   ticket is not claimable for a new builder.
3. The reconciler's stage tick spawns the planner (`STAGE_SPAWN … planner`);
   follow with `wt plan show BECKY-2332`.
4. After a restart nothing is needed: the sidecar is on disk and the stage
   tick resumes from `plan.status` / `stage_session`.
5. To abandon: `wt edit BECKY-2332 --gate ''` (tombstones the entry, clears
   the marker).
6. Verify/review (OPS-204): `wt edit <ref> --gate plan --gate verify` (or
   `wt add ... --gate verify`). After the builder's `wt close` the issue
   stays open and `wt find` shows `in_review`; the reconciler spawns the
   verifier (`verify:<cycle>`), whose `wt verdict <ref> --verify-cycle <n>`
   closes or reopens the issue. A review gate settles with `wt accept` /
   `wt reject`. Requires the reconciler restart of step 0b onto OPS-204.

## Parking and answer routing (WT-32)

A worker `wt block` on a sidecar-owned GitHub ticket (one with a sidecar
entry, i.e. gated) parks it instead of holding it `in_progress`:

- The sidecar entry carries `parked`, `pending_answer` and `last_answer_gen`
  (`github_pipeline.PARK_FIELDS`). The overlay shows `status=awaiting_answer`
  with the claim fields cleared, and the ticket is not claimable. The GitHub
  issue stays canonical: it keeps its `in_progress` label and the parked
  worker's `claimed_by`, and the question is mirrored as an issue comment.
- Writers: `queue.block` -> `_github_park` (after the issue write; a failure
  leaves the legacy hold-while-blocked state), `github_backend.answer`
  (writes `pending_answer` in the sidecar BEFORE the issue edit, so a crash
  re-routes instead of losing the answer), `queue._pa_cas`/`pa_transition`
  and `queue.resume_claim` -> `github_pipeline.pa_cas`/`resume_claim`, and the
  claim path (`_github_bind_answer`, edge E12).
- Every step is a compare-and-swap under the store lock behind the owner
  fence. A foreign host, a missing or corrupt sidecar, a stale gen/state/
  status all return None and nothing is delivered. `resume_claim` also
  refuses when the issue's claim is no longer the parked worker's or that
  worker already holds another ticket.
- A handoff (`reopen`) reopens the issue from `awaiting_answer`; an `affinity`
  reservation keeps the ticket for the parked worker until it expires.
- The owner-comment answer sweep includes parked tickets.
- Tickets with no sidecar entry keep the legacy hold-while-blocked behaviour.
- Liveness: `awaiting_answer` x github is covered by the `answer.*` rows;
  `github_parked` / `github_answer` are gone from `UNREACHABLE`.

Activation: same preconditions as the gate (every claiming host at or after
OPS-203) plus the reconciler restart of step 0b, so the router that drives
`answers.route_answer` runs this code. A host on an older `wt` reads a parked
ticket as `in_progress` + `needs_input` (the issue is unchanged), so it cannot
claim it; it just does not route the answer, which the owning host does.

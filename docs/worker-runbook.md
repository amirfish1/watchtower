# WatchTower worker runbook

Shared, queue-agnostic procedures for a `wt`-spawned worker. This file is
**not** a generic doc a human reads for context — it is referenced by
absolute path from `DRAIN_GOAL_TEMPLATE` (`watchtower/workers.py`), the
literal prompt every worker across every queue is spawned with. The prompt
keeps only a one-line trigger for each protocol below ("follow the Idle
Protocol in `<path>") plus the engine-specific action to take afterward; the
*how* lives here so the prompt itself stays lean. If you're a worker reading
this because your prompt pointed you here, you're in the right place — do what
the matching section says, then return to your prompt's next instruction.

This is distinct from `~/.watchtower/learnings/{queue}.md`: that file is
per-queue accumulated wisdom (infra quirks, recurring ticket patterns) that
*this* runbook's Idle Protocol tells you to update. This runbook itself
never changes per-queue and workers never edit it.

## Resume Check

Do this FIRST whenever you wake and your warm context says you were
mid-work on a ticket (i.e. this is a resumed session, not a fresh spawn).

Before you edit, commit, or close anything: re-verify you still own that
ticket. Run `wt find <ref> --json --worker <your-id>` and read the
self-attribution marks: `claimed_by_you`, `closed_by_you`, and `"you": true`
on timeline events (matched against your worker id and your harness session
id). Then decide by WHO, not just by status:

- `status == in_progress` and `claimed_by_you`: still yours — continue.
- `status == closed` and `closed_by_you`: **you already closed it**, earlier
  in this same session or turn. This is NOT a duplicate process and NOT a
  "concurrent resolution" — do not discard anything on that theory. If you
  still hold uncommitted hunks for it, judge them on their merits: if the
  close missed part of the requested surface, file a corrective ticket and
  commit them under it; if the close covered everything, drop them.
- claimed or closed by a **different** worker id: you were reaped for idling
  while you worked and another worker took it over: STOP — discard any
  uncommitted changes for that ticket, do NOT commit and do NOT close (a
  duplicate close is rejected anyway), and go back to `wt claim` for fresh
  work. Skipping this check is how the same ticket gets fixed and committed
  twice.

Identity is trustworthy: worker ids are per-run and never shared between
live processes (WT-125 tracks hardening continuations; its interim rule —
a fresh worker id per continuation — is what keeps "same id = same agent"
true). Do NOT reason from "duplicate processes can share my worker id", and
never write that theory into a learnings file: it is unfalsifiable (once
your own id stops proving it's you, ALL evidence of your own work reads as
someone else's) and it caused a worker to revert its own correct fix and
report a phantom "concurrent resolution" (CCC-675, 2026-07-28). If you
genuinely suspect an identity collision, compare the event timestamps
against commands you actually ran this session, and file a WT ticket with
that evidence instead of silently discarding work.

Note: WT also enforces this automatically at the transport layer for
headless resumes (`messages._deliver_resume`'s reap-displacement guard,
WT-99/WT-101-adjacent) — a resumed session whose ticket was taken over gets
a `[WATCHTOWER] STOP before you continue...` directive prepended to its
wake message before it even reads this file. This section is the manual
fallback: it still applies to any resume path the automatic guard doesn't
cover (e.g. it's best-effort and falls through to normal delivery on any
lookup error), and reinforces the same rule as prose in case the automatic
prefix is ever missing.

## Idle Protocol

Learnings writes are tiered (same-topic-routing plan §8) so a chatty queue
doesn't pay a full file rewrite every turn:

- **Tier 1 — per turn, append-only.** After each `block`/`close`, append at
  most one dated line to `~/.watchtower/learnings/{queue}.pending.md`, and
  only when the turn produced a genuinely new hard-won fact. Append-only —
  never read-modify-write this file, so concurrent workers can't clobber it.
- **Tier 2 — consolidate on a forced boundary.** Fold `pending.md` into
  `{queue}.md` (the audit below) only when `wt claim` returns
  `{"stop": true}` (recycle or release), or when you have been idle on a
  drained queue past the floor (~20 min) and `pending.md` is non-empty.
  After folding, clear `pending.md` of the lines you merged.
- **Skip rule:** on a merely-drained queue, if `pending.md` is empty and
  `{queue}.md` was modified within the floor, do nothing — end your turn.

A stop with `reason: context_budget` means a recycle limit tripped (Claude transcript
bytes, Codex cumulative input tokens, or tickets closed/blocked this run); see
`docs/worker-lifecycle.md` (Context recycle) for the knobs.

The stop path is where the audit matters most: a recycled worker is torn
down and its successor starts cold, so skipping the learnings hand-off on a
stop throws away the whole session's wisdom. Either way, the audit is not
optional and not deferrable to "next time" — run it before you exit on a
stop, too.

The audit (Tier 2):

1. Read the queue's learnings file at `~/.watchtower/learnings/{queue}.md`
   (create it if it doesn't exist) and `{queue}.pending.md`.
2. Fold the pending lines plus anything else the next worker should know
   from this session into `{queue}.md`: infra changes, recurring ticket
   patterns, gotchas, env quirks. Record VERIFIED facts only. Never record
   rules that tell future workers to distrust their own worker id or to
   treat their own commits, closes, or working-tree hunks as another
   process's — that class of entry is self-amplifying paranoia and has
   caused workers to destroy their own correct work (CCC-675/676; see
   Resume Check above).
3. Keep the WHOLE FILE under ~60 lines. This cap is on the whole file, not
   per-edit — read the current file before editing and prune, don't just
   append unboundedly.
4. Keep a "Recent fixes" section (if any) to the last 2-3 entries, dropping
   the oldest when you add a new one. Old fixes are recoverable from
   `git log` / `wt close` summaries, so they aren't worth permanent space.
5. Durable design reasoning (why something is structured a certain way,
   multi-paragraph gotchas) belongs in `docs/*.md` with just a one-line
   pointer left in the learnings file — not inlined in full there.
6. THEN follow the lifecycle for your engine:
   - **Claude (stream-json over FIFO):** end your turn. Do NOT poll, do NOT
     sleep-loop, and do NOT exit the process yourself. Stdin is a live input
     channel; on a *drained* queue a new ticket arrives as a fresh instruction
     and resumes the conversation with full warm context. On a *stop/recycle*
     you have been released — end your turn after the audit; deficit staffing
     spawns a fresh worker to take over, so do not expect to be re-engaged.
   - **Codex (`codex exec`):** end your turn and exit immediately after the idle audit.
     Never set, complete, or clear a native Codex thread goal (a goal makes
     Codex auto-continue after the process dies). There
     is no live stdin channel and no wake message to wait for.

## Definition of done

### Read related work before implementing (WT-83)

Every successful `wt claim` includes `related` in JSON, or a related-work list
in plain text. This searches full prose across all queues' recent local
snapshots; candidates are suggestions, never proof that a feature shipped.
An error or partial coverage is an evidence gap, not "nothing related".

1. Run `wt related REF --triage --json` to have Jev classify candidates as
   `already-shipped`, `earlier-attempt`, `in-progress`, or `unrelated`.
   Set `TYPESAFE_API_KEY` (or `JEV_API_KEY`) from the authorized secret store;
   never print it. Retrieval itself needs no key and makes no network calls.
   If triage is unavailable, inspect the candidates yourself and record the gap.
2. Read useful candidates and verify their evidence. Record confirmed links:
   `wt related REF --confirm OTHER --kind earlier-attempt --worker ID
   --evidence "specific facts checked"`. Jev advice alone is not confirmation.
   Do not confirm a merely similar error in a different job.
3. Already shipped: verify current integration/deployment and the reported
   behavior, then resolve instead of rebuilding. Same bug recurring after a
   closed repair: say "fix in OTHER did not hold", read that attempt first,
   and treat the ticket as deep (confirmation sets its depth). Related active
   work: coordinate or use a dependency; do not duplicate it.
4. Three confirmed tickets for the same job within seven days mean "this job
   is fragile; fix the design, not the latest error". The owner view supplies
   one line per confirmed job to the decision desk morning digest.
5. At close, pass `--related-feedback useful` or `not-useful`; use
   `unavailable` for a recall failure. `wt related --feedback-report --since
   ISO --json` reports the useful ratio and unavailable counts separately.

`wt related --refresh-github` is an explicit maintenance/backfill command:
it fetches full recent issues and comments outside the claim path. Ordinary
GitHub detail reads retain prose they already fetched; claims never hydrate
missing history. `wt related` also searches CCC's existing recent commit FTS
under a bounded query deadline. An indexed commit is not deployment evidence.

Applies to every ticket, quick or deep.

- **Done means the reason the ticket was filed has gone away**: the symptom it
  reports no longer reproduces. Fixing one plausible cause is progress, not done.
- **Verify the way the problem was seen** on every ticket. Cover the reported
  failure, its consequences and relevant regression risks. A focused test can
  be sufficient when it establishes that outcome; choose broader verification
  when the evidence requires it. For measured problems, re-measure under the
  conditions that produced them (cold paths, cache misses, real load).
- **A follow-up carries separate work, never the ticket's own goal.** If the
  symptom still reproduces, continue working or preserve a genuine person/ticket
  gate. For future natural evidence, use the permanent-watch policy below.
- **Partial is labelled partial**: a close that leaves part of the ask undone
  starts its `--summary` with `PARTIAL:` and cites the follow-up ref.

## Code complete, awaiting release (WT-82)

Use `wt await-release`, not a timed defer, when the code is merged and only
release/adoption remains. Fetch the release branch first. The commit (or its
identical cherry-picked patch) must be on that branch; a feature branch is
still an integration wait. A release wait has distinct status `awaiting_release`,
no timer, no worker slot, and no periodic reclaims. GitHub issues carry an
explicit release_wait gate. Every active host must run WT-87 or later;
the former year-9999 compatibility timer is retired.

```bash
wt await-release REF --worker ID --repo /path/to/repo --commit SHA \
  --release-branch origin/next --target 'github:OWNER/REPO:Production – bookyourmat-app' \
  --reason "merged; waiting for production adoption" --live-check \
  --progress "Tests passed. After adoption check the reported booking flow."
```

Targets: `github:OWNER/REPO:EXACT_ENVIRONMENT` observes the latest successful
GitHub deployment; BYM's environment is `Production – bookyourmat-app`.
`ccc:dashboard` reads the running local dashboard's `/api/version` `code_rev`;
`watchtower:local` uses the daemon's frozen boot revision. Other services/hosts
can use an HTTP(S) JSON endpoint returning a frozen `code_rev` or `sha`.
Do not use the checkout HEAD or a version string as running-build proof.
A service reporter can explicitly submit a verified adoption event:
`wt release-observe --target TARGET --build RUNNING_SHA --evidence "source and time"`.
For CCC execution-worker adoption, require its own actual running commit
report; dashboard adoption never proves the worker restarted.

Default `--live-check`: the observed containing build reopens the ticket once
for a worker to perform the progress note's live acceptance check, then close
normally with its verified fix commit. Repeated build reports cannot reopen it
again. If acceptance is fully proven before release, use `--close-on-release
--summary "outcome and bullets" --verified "specific passing evidence"` instead;
the event closes it with that stored summary and commit. Required gates force
a worker check and ordinary `wt close`; automatic close never bypasses gates.

Observer failures, unknown SHAs, unrelated builds and commits no longer on the
release branch leave the ticket parked. `wt ls --status awaiting_release` lists
them separately; default active lists omit them. `wt owner-view` counts them
separately and surfaces those older than three days or with unproven branch
reachability. These warnings never close a ticket or dispatch a timer check.
Use `wt reopen REF --reason ...` to return an incorrectly parked wait to work.

`wt release-migrate` previews typed release defers; `--apply` migrates verifiable
BYM/WatchTower waits conservatively to one live check, preserving the old window
in `release_wait.previous_defer`. It reports missing commits/paths and ambiguous
targets rather than guessing; use `--repo`/`--target` to resolve those explicitly.
`wt defer` is retired, including its former release spelling. Use
`wt await-release` explicitly; historical fields exist only in migration receipts.

## Proportional verification and waits

Choose the smallest check that proves the reported fix and its meaningful
regression risks. A focused test or direct behavior check is enough when it
establishes the outcome. A copy change does not automatically need a separate
browser session; a backend fix does not need visual checks. Use a browser when
rendering or interaction is the failure, and an independent verifier when an
explicit gate requires one or a material uncertainty warrants it.

Reuse relevant passing evidence for the unchanged code. Repeat checks only
after a relevant change, failure, or concrete gap; do not repeat a passed suite
or add desktop/phone/theme matrices just to accumulate proof. When WatchTower
will run a required verification gate after close, do not spawn a second
verifier for the same acceptance criteria. Required gates still apply.

Related fixes sharing a deployment use one restart owner and one shared
adoption check. Communicate using actual session IDs, not reply aliases that
the chosen transport cannot resolve. Once adoption evidence covers your
commit, use it and move on. Do not hold a worker slot for peer watches or
future data: if necessary evidence is pending, record the exact trigger and
success check as a registered permanent watch on the close, then claim the
next ready ticket. Never create a ticket timer or replacement event-wait state.
Do not mark an unresolved acceptance criterion complete to free a slot.

## Keeping a claim alive through a long silent step (OPS-1744)

The backstop reopens a claim whose owner is not proven at work once the ticket
has had no store activity for `STALL_S` (30 min). A session blocked in one long
foreground command (a full test run, a build) writes no transcript, so after
`RECENT_SESSION_S` (10 min) its local proof can read `unproven`, and a manual
claim that never reaches a proven state is reopened and reassigned while the
work is still running (BYMOPS-3629, 2026-11-17).

The visible heartbeat is `wt comment REF "still running <what>" --worker ID`.
It appends a history event and bumps `updated_at`, which resets the store-idle
clock. Before any step that may run longer than ~20 minutes, run it, then run
the long step backgrounded and poll it with short commands (each poll writes
the transcript too). A comment is not proof of life to a foreign observer: the
GitHub `claim_heartbeat` is renewed only by the owning host's sweep while its
local proof reads alive.

## Customer reports: sentence coverage and action coverage

Keep evidence proportional to the report. For a single symptom or simple
interaction, a concise record of the report, remedy, relevant check and any
remaining gap is sufficient; separate tables and a new evidence file are not
mandatory. Do not turn greetings or incidental wording into acceptance work.

For multi-step incidents with distinct claims, contradictions, record changes
or consequential customer actions, use sentence coverage and action coverage
in the same linked evidence file. Reconstruct actions from
logs, screenshots and reports; label inferred goals/beliefs and unknown ordering.
For record changes, use the production audit log, later corrections and current
records; identify actors and present timestamps in the customer's local timezone.

| Step / customer action and intended outcome | Immediate customer repair | Long-term prevention for others | Possible confusion and help at this step | Evidence and status |
|---|---|---|---|---|
| Number and local time if known; distinguish fact from inferred intent | Verified remedy or justified N/A | Safeguard at THIS action; verified or explicitly OPEN | How to expose the correct reality before a mistake or quickly enable recovery | Sources, verification, delivered vs proposed vs unknown, remaining work |

At each step ask: **if the initial bug or misunderstanding already happened,
how can the product prevent the next mistake or quickly help the customer
understand the correct reality here?** Examine subsequent edits, cancellations,
rebooking, assistant requests, error/conflict feedback and recovery separately.
Fixing the first trigger is not prevention evidence for every later action.
Correct backend behavior or an accurate reply may still leave the customer's
contradiction unexplained; assess the actual information and next action offered.

Example: a calendar jumps to Monday while a customer intends Friday. After
fixing that jump, still inspect whether cancelling Monday's booking makes the
actual date clear, and whether a duplicate warning helps locate the existing
Friday booking. An audit timeline alone does not establish those safeguards.
Do not invent mandatory UX changes: record a verified safeguard, an evidenced
N/A, or a concrete open gap for each step. Distinguish a proposed improvement
from delivered prevention; never claim the former is verified or close away
required prevention under an upstream fix. Cite both tables when resolving the
report and retain unresolved original work under the definition-of-done rules.

## Ticket depth

Every ticket requires thorough investigation, a complete solution and verified
outcomes. The filer sets depth; a worker never guesses it. `quick` remains the
default value for compatibility. It is not a speed target or a lower quality
standard, and it does not prohibit design work or excuse relying on the owner
to discover regressions. Choose investigation, design and verification based
on the actual failure, its consequences and relevant regression risks.

Continue authorized structural work when necessary. Complexity or duration
alone does not require an owner decision or promotion to deep. Existing access,
approval and spending limits still apply. Several related tickets can share
verification as a plan group (`wt add --group-parent --children ...`) when that
verification covers each ticket's acceptance criteria.

**Deep** (`wt add --depth deep [--budget-min N]`) adds structure for sustained
work across hours or days, including performance, chronic regressions and
multi-step optimisation. It adds planning and progress checkpoints to the same
quality standard required on every ticket. Set by owners and system filers
that know what they file, not by workers filing follow-ups. A deep claim's JSON
carries `deep_brief`; follow it:

1. **Evidence first.** Build logging and permanent checks when future natural
   evidence is needed. Finish what can be verified now and use the permanent-watch
   close policy below; never park a ticket for a later data window.
2. **Options memo** with 3-5 candidates, at least one a *product* option (cut
   scope, tier the data, change the UX). Read the queue learnings' *Product
   constraints* first; if the best option needs an owner decision, `wt block`
   with the memo. Record the owner's answer as a Product constraints line.
3. **Checkpoint budget** (default 90 min of active work per checkpoint, not a
   total ticket time limit). Honour explicit ticket/owner time and spending
   caps. Log each attempt:
   `wt comment REF "attempt N: <change> -- metric before -> after"`.
4. **Stop rule**: at each checkpoint, record the measured trajectory, remaining
   work and next checkpoint plan. Continue authorized work while evidence
   supports progress, including across sessions or days. After two attempts
   in a row with no measured gain, stop repeating that approach and reassess
   evidence/options before changing approach. Watch future evidence; block only for
   a genuine decision, access or authorization gap. Leave resumable progress
   before ending a session. A checkpoint/session ending never makes incomplete
   work done; exceeding an explicit cap still requires approval.
5. **Close** only when the goal is met, with the measurement in the summary.

### Future evidence belongs to permanent checks

`wt defer` and ticket `--until` windows are retired. There is no replacement
`await event` mechanism. Tickets are being worked, closed, dependent on another
ticket (`--after`), blocked on a named person, or code complete awaiting release.
A short wait for an already-running check belongs inside the worker's turn.

For natural customer behavior, scheduled QA, recurrence or other future evidence:
finish what can be verified now, install a permanent check, and close with
`--caveat "what remains unverified" --watch NAME` (repeatable). The registered
check specifies its command, working directory, where/how often it runs and the
original ticket. Use `wt monitor --register NAME --cmd CMD --cwd PATH --where HOST
--schedule TEXT --every SECONDS -q OPS`, then `wt close REF ... --watch NAME`.
The daemon schedules checks, never tickets. A check emits JSON with `status`
(`pass`, `fail`, `pending`), `real_data` and `evidence`. Only an explicit real-data
pass clears the owner's watching list. Fixture-only success or no qualifying
records stays pending. The check continues after its first pass; failures file a
new deduplicated ticket linked to the closed original, which is never reopened.

Owner view and the morning digest list closed tickets whose watch has not yet
passed on real data, including check, location, schedule and last result. Never
register an invented or nonrunnable check just to close. If no runnable check
can be installed, close with an honest unresolved item under the owner's WT-87
policy, preserving any genuine remaining action and its tracking ref. Never
waive customer-send, money, privacy, model-spend or other approval gates.

### "Waiting for release" must be true

A commit pushed to a feature branch is not integrated, and a release never
carries it. Before closing a ticket as fixed, or parking it until a release,
check the commit is on the branch releases are cut from (`git fetch`, then
`git merge-base --is-ancestor SHA origin/BRANCH`). Say which case you are in:

```bash
# Merged, waiting to ship: no timer. The release target must report the build.
wt await-release REF --worker ID --reason "ships with the next release" \
  --commit SHA --release-branch origin/next --target TARGET --live-check

# Authoring worker lands the fix in its own worktree, under repo rules.
git fetch origin
# Review/merge or cherry-pick the fix onto the fresh release branch worktree;
# run that repo's required checks and push next/main as its rules require.
git merge-base --is-ancestor SHA origin/BRANCH
wt await-release REF --worker ID --commit SHA --release-branch origin/BRANCH \
  --target TARGET --reason "integrated; waiting for adoption" --live-check
```

- `--awaits integration` is retired: authoring workers land their own fixes.
- If a parent must compose branches, close the source-complete child only when
  its own acceptance is met, and make the parent depend on it with
  `wt edit PARENT --after CHILD`. Never close away the child's original symptom.
- A real human gate (owner approval, customer/money hard stop, named reviewer)
  stays `wt block --wait-on owner` or
  `wt block --wait-on external --wait-actor WHO`. Preserve required review;
  this policy grants no new access or approval.
- Never switch the branch of a shared checkout to get onto the release branch.
  Work in your own worktree off a fresh copy of it.
- Preserve actual approval and named-person holds with `wt block`; use
  `--after` for ticket dependencies. Future natural evidence uses permanent
  watches on a close, never a source/data timer.

### Say who a block waits on

The dashboard paints a blocked ticket red ("needs you") unless the block says
otherwise, so name the actor whenever the owner is not the one who must act:

- `wt block REF --worker ID --question "..." --wait-on external --wait-actor "Sonia"`:
  waiting on a client, studio owner or vendor (blue).
- `--wait-on owner`: a decision only the owner can make (red).

Always pass `--wait-on owner|external`; these are the supported person waits.
Worker waits and text-only dependency blocks are retired. Do authorized work
yourself, or use `wt edit REF --after DEP` for unfinished work tracked by another
ticket. This releases the claim without creating a human question. Verify that
DEP actually owns the missing work and does not already depend on REF; never
add a cycle or an already-completed prerequisite just to park a ticket.

**Reserved actions are an authority boundary, not a worker wait.** If an explicit
instruction reserves a shared build, landing, restart or other action for a
coordinator, retain that restriction. A real unfinished prerequisite can use
`--after`; otherwise ask the owner to delegate that specific reserved action or
retain the restriction, using the decision desk and an owner block. State the
instruction and the exact scope requiring delegation. Do not infer delegation
from a closed child, a missing coordinator or a dependency cycle; do not relabel
an agent as an external human. Ordinary reversible implementation choices that
are already authorized still belong to the worker, not an owner question.

`wt wait-on REF --on external --actor "Sonia" --note "..."` corrects the
current block in place (no new question, history kept). A later `wt block`
without `--wait-on` drops the wait record again.

**Owner waits carry three more facts**, so the owner can tell urgent from held:

```bash
wt block REF --worker ID --question "..." --wait-on owner \
  --decide-by 2026-11-17T09:00:00-08:00 \
  --if-unanswered "Client arrives for a class booked as the wrong type" \
  --root BECKY-3041
```

- `--decide-by`: the latest safe time to act, ISO 8601 **with a UTC offset**
  (a time without a zone is refused), or `none` when there is no deadline.
- `--if-unanswered`: the named real-world consequence, or `none`. "Pending
  permission" is not a consequence.
- `--root`: the ticket whose decision this one really hangs on, when the same
  answer settles several tickets. Omit it when this ticket is its own root.

When `--decide-by` passes, the ticket is flagged for a re-check of the facts.
It stays blocked and open: a passed time never means handled, expired or
safe to close. None of these fields grants or removes authority.

**Another host's local queue is a separate identity.** Inspect the dependency
on its owning host and record the host with the ref in progress. The retired
dependency-wait flags are not a fallback when `--after` cannot resolve that
ticket. Never substitute a same-number local ticket or invent an edge; report
the concrete tooling/access gap and preserve the unfinished acceptance.

`wt owner-view [-q QUEUE] [--json]` shows every blocked ticket by who it
really waits on, grouped by root, on file and GitHub queues alike. It reads
the current wait record, not the question text, and changes nothing.

## Minor blocks: proposing an auto-decision (CCC-370)

Before creating a block, check whether the choice is already within your
authorized work. For a routine implementation detail that is cheap to undo,
does not reach customers or money, and fits the owner's instructions and past
decisions, choose it yourself. Record the reason and exact undo step in ticket
progress (use `desk check` and `desk log` where available), then continue without
`wt block`. This applies before a commit exists and when queue auto-approval is
disabled. It grants no new authority: missing information, consequential
preferences, customer sends, money, public posts, ad spend and irreversible
actions still require the owner.

The formal gate below handles proposals that still require approval. It runs
when a worker submits proposal flags, not as a background sweep of old blocks.
An old block without a proposal is not retroactively approved.

**Check this every time you are about to `wt block`** (CCC-413: 18 blocks in 3 days
carried no flags, so the gate never ran). If a block is a small, reversible code change in the ticket's own repo, you may
propose that WatchTower approve it for you instead of making the owner read a card:

```
wt block REF --worker ID --question "..." --commit SHA --class code-change-revertable \
  --weight minor --confidence 0.9 --recommend "the exact choice" --undo "git revert SHA" \
  --attest money=no,customer-sends=no,public-posts=no,ad-spend=no,irreversible=no
```

WatchTower, not you, decides: the queue must have auto-approval enabled
(`wt policy -q QUEUE show`), the class must be enabled, every attestation `no`,
confidence over the threshold, the undo `git revert <that same --commit SHA>`, and
nothing in your words may match the hard-stop floor (customers, money, prices, ads,
public posts, production, irreversible). Any miss is an ordinary owner card that
shows the reason. A pass resumes you with an AUTO-DECISION (not a human approval)
that authorizes only your recommendation. To say it needs the owner, use `--weight major --major-because
<reason> --major-detail "<one concrete sentence>"`. Reasons: money, customer-sends,
public-posts, ad-spend, irreversible, live-product (changes what real users get),
safety-guard (weakens a privacy/security/safety control), owner-only-action (only
the owner can do it), needs-information (name the fact only the owner has). The
detail must be at least ~40 characters and say what concretely goes wrong or what
exactly only the owner can do; "preference", "unknown", "needs you", "not sure",
"tbd", "your call" are rejected and nothing is recorded, so restate and retry. Owner commands:
`wt undo REF` (reverts and verifies), `wt policy -q QUEUE set enabled=true
enabled_classes=code-change-revertable`, kill switch = create `auto-decision.off`
in the WatchTower data dir. Workers cannot write policy.

The hard-stop floor is built in, so auto-approval works on a fresh install with
nothing extra to set up. It always sends these to a human: sending to customers, a
production change, which reaches customers, money or prices, ad spend or a public
post, and anything that cannot be undone. To add rules of your own, set
`WT_HARD_STOPS_FILE` (in the environment `wt` runs in) to a Python file that defines
`hard_stops(text)` and returns the names of the rules the text matches. Your rules
are added to the built-in ones and cannot remove them. If that file is set but
missing or broken, nothing is auto-approved.

### Replay approvals (`capped-replay-spend`, CCC-376)

A model replay/evaluation that writes only to a file, sends nothing, writes
nothing to production and costs $0 on the subscription proxy can be proposed the
same way. Example (the BECKY-3309 shape: a 247-case blind replay):

```
wt block BECKY-3309 --worker ID --question "Run the 247-case blind replay on the subscription proxy?" \
  --class capped-replay-spend --weight minor --confidence 0.9 \
  --recommend "run the 247-case replay capped at 300 requests, output to a file" \
  --replay-cap-requests 300 --replay-cap-usd 0 --replay-backend subscription-proxy \
  --undo "rm /path/to/replay-output.json" --prev-value "none (no limit raised)" \
  --attest money=no,customer-sends=no,public-posts=no,ad-spend=no,irreversible=no,production-writes=no \
  --gate-dry-run
```

- `--replay-cap-usd` above 0, or any backend other than `subscription-proxy`, means
  real money: block it as `--weight major --major-because money` instead.
- `--undo` must be a concrete command or file edit (`rm <file>`, `git checkout -- <file>`,
  `wt policy ...`, `sed ...`), not prose. If you raised a ledger or limit, put its previous
  value in `--prev-value` and make the undo restore exactly that.
- Keep the question, recommendation, undo and progress free of hard-stop words (production,
  stripe, refund, customer sends, `$5`). The words "subscription" and "$0" are fine here.
- `--gate-dry-run` (or a queue `dry_run_classes` entry) runs the whole gate and marks the
  card "Eligible for auto-approval (test, not approved)"; nothing is answered. Check
  yourself with `wt gate-check REF`: it prints every check and the fix for each failure.

### Own capped spend (`capped-own-spend`, Q856)

The owner's OWN small capped spend (a hosting memory bump, a model-token replay with a
dollar cap) may be minor. Customer money, Stripe, prices and refunds are still major and
the hard-stop floor still vetoes them. The class is off everywhere: the owner enables it
per queue, first `dry_run_classes=capped-own-spend`, later `enabled_classes`. A queue can
only LOWER `own_spend_max_usd` (default 25), never raise it.

```
wt block REF --worker ID --question "Raise babysit-queue function memory from 1024 MB to 2048 MB?" \
  --class capped-own-spend --weight minor --confidence 0.9 \
  --recommend "raise function memory to 2048 MB, about $1.50 a week" \
  --spend-cap-usd 5 --spend-category hosting --prev-value 1024 \
  --undo "set /path/vercel-memory.json memory back to 1024" \
  --attest money=own-capped,customer-money=no,customer-sends=no,public-posts=no,ad-spend=no,irreversible=no \
  --gate-dry-run
```

- `--spend-cap-usd` must be above 0 and at most the queue's `own_spend_max_usd`;
  `--spend-category` is `hosting` or `model-usage`.
- `--prev-value` is required and the `--undo` must contain it (the exact edit that restores it).
- Only a dollar figure at or under the cap is exempt from the floor; stripe, refund,
  invoice, price or charge words still veto. Check with `wt gate-check REF`.

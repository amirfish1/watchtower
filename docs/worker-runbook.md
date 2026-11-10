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

Applies to every ticket, quick or deep.

- **Done means the reason the ticket was filed has gone away**: the symptom it
  reports no longer reproduces. Fixing one plausible cause is progress, not done.
- **Verify the way the problem was seen**: for a quick ticket, a focused check
  proportionate to the change is enough; for a deep ticket, re-measure under the
  conditions that produced the problem (cold paths, cache misses, real load).
- **A follow-up carries separate work, never the ticket's own goal.** If the
  symptom still reproduces, keep the ticket open (`wt block`, or `wt defer` while
  data accumulates) instead of closing with the remainder in a follow-up.
- **Partial is labelled partial**: a close that leaves part of the ask undone
  starts its `--summary` with `PARTIAL:` and cites the follow-up ref.

## Ticket depth

The filer sets depth; a worker never guesses it. `quick` is the default.

**Quick** is fix-and-look work. The owner uses the product and sees within
minutes whether a fix is right, so: no design step, verification proportionate
to the change (no full browser reproduction for a small UI tweak). Several
related quick tickets can share one verification as a plan group
(`wt add --group-parent --children ...`). If a quick ticket turns out to be
structural (no contained fix removes the symptom), `wt block` it asking the
owner to promote it to deep rather than shipping a token fix.

**Deep** (`wt add --depth deep [--budget-min N]`) is for work whose outcome you
cannot judge by using the product: performance, chronic regressions,
multi-step optimisation. Set by owners (the filing UI) and by system filers
that know what they file (e.g. a perf watchdog), not by workers filing
follow-ups. A deep claim's JSON carries `deep_brief`; follow it. In short:

1. **Evidence first.** If you lack the data to know the cause, build the
   logging, ship it, decide how much data and how long, then
   `wt defer REF --until +24h --reason "..." --progress "..."`. The ticket leaves
   your claim and the pool, and returns when the window ends.
2. **Options memo** with 3-5 candidates, at least one a *product* option (cut
   scope, tier the data, change the UX). Read the queue learnings' *Product
   constraints* first; if the best option needs an owner decision, `wt block`
   with the memo. Record the owner's answer as a Product constraints line.
3. **Budget** (default 90 min). Log each attempt:
   `wt comment REF "attempt N: <change> -- metric before -> after"`.
4. **Stop rule**: budget spent, two attempts in a row with no gain, or four
   attempts without converging. On stop, block with the trajectory and best
   remaining options; don't close.
5. **Close** only when the goal is met, with the measurement in the summary.

`wt defer` is not `wt block`: defer waits for *data* (no human involved, the
ticket returns on its own); block waits for a *decision*.

### Say who a block waits on

The dashboard paints a blocked ticket red ("needs you") unless the block says
otherwise, so name the actor whenever the owner is not the one who must act:

- `wt block REF --worker ID --question "..." --wait-on external --wait-actor "Sonia"`:
  waiting on a client, studio owner or vendor (blue).
- `--wait-on dependency --wait-ref OPS-167`: waiting on another ticket or
  handoff (neutral).
- `--wait-on worker`: the decision is already made and only worker time is
  missing (amber).
- `--wait-on owner` (or no flag): a decision only the owner can make (red).

`wt wait-on REF --on external --actor "Sonia" --note "..."` corrects the
current block in place (no new question, history kept). A later `wt block`
without `--wait-on` resets the ticket to an owner wait.

## Minor blocks: proposing an auto-decision (CCC-370)

If a block is a small, reversible code change in the ticket's own repo, you may
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

---
name: wt-queue-review
description: Use to review WatchTower queues end to end - "triage", "sweep", "queue care", "queue review", "is queue X stuck", "what needs my input", or an hourly routine that lists flagged queues/tickets. One pass covers operational health (stuck, orphaned, dead claims, duplicates) with safe mechanical repairs, then content (tickets blocked on a human, closes with unresolved items). Replaces wt-triage-queue, wt-queue-sweep and wt-queue-care.
---

# Queue review: health first, then content

One workflow. Two levels, always in this order, because a stuck queue can't
deliver the answers a content review produces:

- **Stuck** is a queue property (open work, nothing closing, or a claim/stage
  with nobody working it).
- **Blocked** is a ticket property (a worker parked it with a question; status
  stays `in_progress`, `needs_input: true`).

**REQUIRED BACKGROUND:** the `watchtower` skill (lookup, filing, claiming, closing).

## Scope

Review exactly what the caller names: one queue, several queues, a ticket
list, or an explicit wildcard (e.g. `BYM-*`: expand it with `wt status --json`
and review every match; a wildcard the user typed is the scope). A routine
ticket listing flagged queues/tickets is the named scope even across queues.
Work queue by queue. Do not widen beyond it; something urgent elsewhere gets
one line of mention, no action.

Boundaries that always hold: no sends to customers, no money/Stripe/price
changes, no ad spend or public posts, and no opening the owner's Stripe
Dashboard or logged-in browser (use API/CLI/DB/audit evidence; if that cannot
answer, report the gap and stop that check). Never mark a still-unresolved
problem complete: a ticket closes only when its work is verified done.

**What needs the owner vs not.** Routine reversible agent work (verified
mechanical answers, releasing dead claims, exact-duplicate dedup, acking items
proven moot, filing a ticket you are Auto-GO on) is done and logged in the
digest, no approval. Ask only for: product/policy design approval, information
only the owner has, and hard stops above. Run `desk check "<question>"` first;
if past decisions answer it and it's cheap to undo, decide and `desk log`.

## Part 1: Health (per queue)

```bash
wt status -q <Q> --json                      # depth, oldest-open age, stuck flag
wt ls -q <Q> --status all --json --limit 0   # every ticket, any state
wt ls -q <Q> --status in_progress --json     # claimed, not closed: usual culprit
wt blocked -q <Q> --json                     # parked for a human
```

Don't act on a "stuck" flag alone; read `wt ls` to see what is stuck
(`--stuck-minutes N` tunes the window; default 10).

Safe mechanical repairs:
- **Claimed, worker dead** (no recent activity and `wt workers --json` shows no
  live worker): `wt release <ref> --worker <id>`. Never fabricate a close.
- **Exact-duplicate open tickets**: `wt dedup -q <Q>` (dry run), then
  `wt dedup -q <Q> --apply`.
- **Parked for a human**: classify in Part 2 first; only a verified
  mechanical blocker is answered by you (`wt answer`).
- **Nothing draining**: check auto-drain. Turning it ON spends money and backlog
  mode is often deliberate: raise a decision card (depth, oldest ticket,
  recommendation); `wt drain on|off <Q>` only on the owner's say-so.
- Looks like a WatchTower bug: file it in WT with evidence; don't work around it.

Confirm with `wt wait -q <Q> --timeout 600` (always bounded). Recurring
condition: `wt monitor -q <Q> --cmd "<check>" --title "<title>"`. If a queue is
still stuck after repairs, say so in the digest; still review its content, but
state that answers may not be delivered.

## Part 2: Content

Collect:

```bash
wt ls -q <Q> --status blocked --json --limit 0      # parked for a human
wt ls -q <Q> --status unresolved --json --limit 0   # closed with unresolved items
```

Read each ticket's `block_question`, `progress` history, and for closed ones
`resolution.unresolved` / `follow_ups` / `caveats`; never judge from the
one-line question. Scratch output goes under `~/dev/scratch/`, not `/tmp`.

## 2. Classify each blocked ticket

**A. Mechanical blocker** (ownership of uncommitted work, git index lock,
changelog snippet, stale repo entry, commit/close-proof plumbing): resolve it
automatically after verifying. Never escalate to the user.

**B. Product / policy decision** (the worker proposes a design or policy and
asks for approval): present to the user in the format below. Approving a
worker's proposed design is NEVER automatic, whatever the score.

**C. Needs information only the user has** (a repro, a preference): ask the
user directly, briefly.

## 3. Rule A: verify, then answer

Before any takeover or answer, verify against the actual repo (find its path in
`~/.watchtower/queue-config.json` or the ticket):

1. **Target files still exist.** A refactor may have deleted or renamed them
   (`git log -- <path>`); then the leftover diff is obsolete. Do not authorize a
   takeover; tell the worker to re-check the issue against the current code and
   close `--no-code` citing the deleting commit if it is gone.
2. **Cited SHAs exist** (`git cat-file -t <sha>`). Workers mistype SHAs; resolve
   the real one from the short SHA or subject line.
3. **Already landed?** `git log -S'<symbol>'` and `git status --short -- <paths>`.
   Often the code is in HEAD and only a test or changelog is uncommitted.
4. **Stand-down guard (hard).** If a sibling session is live-editing the same
   files (recent mtimes, `git diff` hunks belonging to other tickets, active
   sessions on that repo), do NOT authorize a takeover of those files.
   Authorize commits of the worker's own non-overlapping files only, with
   `git commit --only <paths>`. Never `-A`, never checkout/restore to undo.
5. **Locks.** Confirm `.git/index.lock` is actually gone (or no live git
   process holds it) before telling a worker to retry.

Then answer with the verified facts, so the worker doesn't rely on its own
(possibly wrong) premise:

```bash
wt answer <ref> "Rule-1 auto-answer. Verified: <facts>. Do: <exact scoped action>. Close with the SHA."
```

`wt answer` resumes the worker's session; it commits and closes. Report to the
user what you verified and what you told each worker; do not claim tickets are
done until they close.

Check the `delivered to session <id>` line `wt answer` prints. If `<id>` equals
your own `$CLAUDE_CODE_SESSION_ID`, the answer came back to you, not to the
worker (a devin worker once inherited its spawner's id and CCC-1153's answer
was misrouted). Do not act on the echoed message as if you were the worker:
`wt release <ref> --worker <claimed_by>` if that worker's process is gone, and
put the steps in the ticket note so the next claimant sees them.

## 4. Rule B: the decision card

Show a small number: ONE if the topic is meaty, two or three if each is small.
Never flood. Each card, short enough that the user never opens the ticket:

- **What happened** — the situation in plain words.
- **Why the worker didn't complete it** — what it lacked authority/info for.
- **Proposal** — concrete, with thresholds/criteria.
- **The opposing view** — the real trade-off and why not to do it.
- **Score 0–1** — 1 = no-brainer, go; 0 = controversial, think more. Say what
  drives the score; split into parts if the parts differ (e.g. 0.95 for the
  source fix, 0.75 for the risky piece).
- **Other repos are fair game.** If the root-cause fix lives in a different repo
  (e.g. an external tool's repo), that is fine: file the ticket in the current
  queue, name the target repo path at the top of the ticket, and have the worker
  do the work there. Check the queue's auto-drain type filter first (`wt add`
  prints "wrong type" if it won't drain); file as `bug` when the queue only
  drains bugs, or the ticket will sit unclaimed.
- **Scoring principles.** (1) A fix at the root cause scores high; a workaround
  that leaves the cause in place scores low. (2) Someone opened the ticket, so
  it matters: never lower a score because "the user may not care anymore" or
  "it's rare". Score only on whether the proposal is right, cheap and safe.
- **Blast radius: L/M/H** — how much of the product or how many users the
  bug/feature touches (one button vs a whole surface vs everything).
- **Risk: L/M/H** — chance that making the change breaks something or is hard
  to undo. Say what drives it in a few words.
- **Auto-GO rule:** score above 0.8 AND blast radius L AND risk L means GO with
  no question: do the proposal (e.g. file the ticket with `wt add ... --pre-ack`;
  omit `--submitter`, a made-up name makes the close notification undeliverable)
  and list it in the end-of-sweep digest. Applies to proposals the sweep itself
  makes. It does NOT override "approving a worker's proposed design is never
  automatic". Anything else: ask.
- **Your call** — what to reply. Then `wt answer` exactly the user's decision,
  including any refinements they add.

## 5. Unresolved closes

Closed tickets whose resolution lists `unresolved` / `follow_ups` / `caveats`.
Read the full resolution. **First check the feature still exists as shipped**:
`git log` for later commits that disabled, gated, or removed it (grep the
feature's keywords). If it was killed or removed, its caveats are moot: propose
acking, nothing else. Never infer that a feature is live from a data file
(e.g. a `fired: true` flag); read the code path that sets it. Group tickets
that share a root cause. Classify each
item by nature, then apply that category's rule. Reopening, filing a product-decision ticket or acking an item you cannot
justify from verified facts needs the user's say-so; the auto-acks named below
do not.

**Acknowledge** with `wt unresolved-ack <ref> --all` (or `--unresolved N`,
`--caveat N`, `--follow-up N`; `--undo` reverses; no history rewrite). NOT
`wt ack` -- that approves product-gate pitches.

**Presenting owner-action items.** Any item routed to a human (studio owner
must text a client, fix a contact record, apply a credit) must show (a) how
old the ticket is — days since `created_at` — (b) when it references a
specific appointment, the APPOINTMENT's date, not just the ticket's, and
(c) the ticket's ORIGIN: system (auditor autofile, digest, delivery-log
alert, cron) vs owner (in-app "Report an issue", a direct ask). Owners skim:
"The owner should text Dana (system, 10d old; class was Sep 15)" is
actionable, a bare ticket ref is not. Auditor-filed issues carry a
`Auto-filed by the Becky Auditor sweeper` note in the body; otherwise infer
from phrasing and mark the inference — tickets have no explicit origin field.

Origin and staleness rules:
- **A past appointment date is NOT proof the communication was handled.**
  Never ack or close a send/notify item on date alone. For BYM (and any
  customer-comms item) read the audit/delivery evidence and current facts
  first: the message/delivery log for that client and appointment, the
  current record (contact still valid, appointment status, credit applied).
  Ack as moot only when that evidence shows it was sent/handled, or the
  condition no longer exists (cite the evidence). If the evidence is missing
  or unreachable, report the gap and present the item; do not ack. If a fresher
  ticket in the cluster is still live, say so.
- System-originated items may be acked/closed on verified evidence (above)
  without asking. Owner-originated items go in front of the user even when
  they look stale.
- Sending to customers is never done by this skill: draft/propose only.

1. **Couldn't verify** (worker lacked the means to test it). Assess (a) feature
   size S/M/L and (b) probability 0-1 that the fix is right unverified, with the
   evidence. Small AND high probability: let it go and `wt unresolved-ack` it.
   Important OR low probability: bring it to the user with a proposal to reopen
   or file a dedicated verification ticket.
2. **Real bug noticed, never filed**: a decision card (Rule B) whose proposal is
   "file this bug", with confidence it is worth fixing and the case for NOT
   opening it.
3. **Design question the worker didn't guess at**: did it implement nothing, or
   something? If something: score 0-1 how reasonable that implementation is. If
   nothing: should it have implemented? If yes, it becomes a product decision
   card (Rule B).
4. **Can't reproduce**: an opened issue has a reason, so never dismiss. Give 1-3
   hypotheses of what the reporter meant and why it doesn't reproduce, and a
   0-1 score for "no longer happens" vs "worker misread it". Bring it to the
   user to clarify; the goal is to get to the bottom of it and solve it.
5. **Recurring alerts / shared root cause** (e.g. repeated perf tickets): treat
   as one big item under 6.
7. **Feature deliberately killed later** (checked at the top of this section):
   ack its leftover caveats automatically (`wt unresolved-ack <ref> --all`),
   no question to the user. Only when a commit provably disabled, removed,
   reverted or superseded it on purpose (cite the SHA). If it merely broke as a
   side effect, or no clear killing commit exists, it is NOT moot: make it a
   normal decision card. List every auto-ack in a one-line "closed as moot"
   digest at the end of the sweep (ticket, killing SHA, `--undo` hint).
6. **Caveats, tech debt, fragile assumptions**: propose a remedy. Small: suggest
   a new ticket. Big: suggest opening a `CCC-DESIGN-*` ticket for a proper
   design assessment and a go/no-go decision.

## Digest (one per run)

Per queue: what was wrong, what you did, whether it now drains. Then:
auto-answered (with verified facts), auto-acked/"closed as moot" (ticket,
evidence or killing SHA, `--undo` hint), Auto-GO actions, decision cards
(max three per run, one per root cause), evidence gaps, deferred refs.

## Don't fabricate

Verify facts before asserting them; say which checks were not run. Every
command here is real (`wt <command> --help`).

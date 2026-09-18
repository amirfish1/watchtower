---
name: wt-queue-sweep
description: Use when asked to sweep, review, or "see what needs my input" in a healthy WatchTower queue — tickets blocked on a human question (needs_input) or closed with unresolved items. Content-level review, NOT for a queue that is stuck or not draining (use wt-triage-queue for that).
---

# Sweep a healthy queue for tickets that need a decision

**Scope.** The queue itself works. This skill is about the *substance* of
tickets inside it: blocked-on-human tickets and closes that left loose ends.
If `wt status -q <Q>` shows the queue stuck (open tickets, nothing closing),
stop and use `wt-triage-queue` instead.

Two levels, don't mix them: **stuck** is a queue property (open work, no
closes in the window). **Blocked** is a ticket property (a worker parked it
with a question; status stays `in_progress`, `needs_input: true`).

**A sweep covers exactly one queue: the one the user names.** Tickets waiting
in other queues are out of scope; don't go looking, and don't widen the sweep on
your own. (If you happen to notice something urgent elsewhere, mention it in one
line, don't act on it.)

## 1. Collect

```bash
wt ls -q <Q> --status blocked --json --limit 0      # parked for a human
wt ls -q <Q> --status unresolved --json --limit 0   # closed, unresolved items in resolution
```

Read each ticket's `block_question`, its `progress` history entries, and for
closed ones `resolution.unresolved` / `follow_ups` / `caveats`. Don't judge
from the one-line question alone. Write scratch output under
`~/dev/scratch/`, not `/tmp`.

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
- **Blast radius: L/M/H** — how much of the product or how many users the
  bug/feature touches (one button vs a whole surface vs everything).
- **Risk: L/M/H** — chance that making the change breaks something or is hard
  to undo. Say what drives it in a few words.
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
item by nature, then apply that category's rule. Never file, reopen or ack
without the user's say-so, except the auto-ack in category 1.

**Acknowledge** with `wt unresolved-ack <ref> --all` (or `--unresolved N`,
`--caveat N`, `--follow-up N`; `--undo` reverses; no history rewrite). NOT
`wt ack` -- that approves product-gate pitches.

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

## Don't fabricate

Verify facts before asserting them; if a check wasn't run, say so. Every
command here is real (`wt <command> --help`).

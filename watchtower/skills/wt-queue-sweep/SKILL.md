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
- **Your call** — what to reply. Then `wt answer` exactly the user's decision,
  including any refinements they add.

## 5. Unresolved closes

Closed tickets whose resolution lists `unresolved` items. Group by root cause
(several perf tickets often share one architectural fix). Present as decision
cards (Rule B). Propose per item: file a follow-up ticket, `wt reopen`, or
dismiss. Do not reopen or file without the user's say-so. *(Rule for
auto-handling these is not yet defined; ask the user.)*

## Don't fabricate

Verify facts before asserting them; if a check wasn't run, say so. Every
command here is real (`wt <command> --help`).

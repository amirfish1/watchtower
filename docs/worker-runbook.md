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

Do this when `wt claim` reports the queue is drained (returns nothing), FIRST —
before ending your turn. A Claude conversation may later be released from queue
staffing, while a Codex worker exits immediately after this audit. Either way,
the audit is not optional and not deferrable to "next time."

1. Read the queue's learnings file at `~/.watchtower/learnings/{queue}.md`
   (create it if it doesn't exist).
2. Update it with anything the next worker should know from this session:
   infra changes, recurring ticket patterns, gotchas, env quirks. Record
   VERIFIED facts only. Never record rules that tell future workers to
   distrust their own worker id or to treat their own commits, closes, or
   working-tree hunks as another process's — that class of entry is
   self-amplifying paranoia and has caused workers to destroy their own
   correct work (CCC-675/676; see Resume Check above).
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
     channel; a new ticket arrives as a fresh instruction and resumes the
     conversation with full warm context.
   - **Codex (`codex exec`):** complete this queue's active drain goal (or clear
     it if the harness has no completion state), then exit immediately. There
     is no live stdin channel and no wake message to wait for.

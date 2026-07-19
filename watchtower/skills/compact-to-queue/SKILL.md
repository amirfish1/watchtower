---
name: compact-to-queue
description: Use when a session's context is filling up, before running /compact, or when the user says "remember this for later" — distill the remaining work into WatchTower queue tickets instead of relying on compaction or memory.
---

# Compact to queue

A compaction summary is lossy memory; a queue ticket is durable state. When a
long session is about to compact — or is ending with work still open — file
the *remaining* work as WatchTower tickets so any future session (human,
agent, or drain worker) can pick it up cold.

**REQUIRED BACKGROUND:** the `watchtower` skill covers filing, claiming, and
closing tickets. This skill is the discipline of *what to extract and when*.

## When to fire

- Context is ~70%+ full, or the user asks "when should we compact?"
- The user says "don't forget X", "put this on the list", "file it".
- A session ends with known open items, limitations, or deferred work.

## 1. Pick the queue

- Prefer the repo's existing queue (`wt ls` / `wt status` to see what's in use).
- For a workstream that spans sessions, a dedicated queue reads better
  (e.g. `KIMI-FIXES` for the kimi-engine workstream). Ask if unclear.
- Check for dupes first: `wt ls -q <QUEUE> --status all --json`.

## 2. Extract only what's NOT done

File: remaining work, known limitations, deferred fixes, ideas the user
approved but aren't built. Never file completed work — the ticket is a
promise of future effort, not a log.

One item per ticket. If a note needs "and also", that's two tickets.

## 3. File each item self-contained

The reader has **zero** context. Every note must stand alone: the why, any
measured numbers, and file:line pointers to where the work lives.

```bash
wt add -q <QUEUE> \
  --type feature \
  --priority p2 \
  --readiness needs-shaping \
  --title "Short imperative title" \
  --note "Why this matters + measured evidence + file:line pointers. \
Must be fully understandable with zero prior context."
```

Field limits (hard clips, so don't fight them): title 200 chars, note 4000,
text 24000, comments 24000. Title = preview, note = brief, text = full body.

Honest priorities: p1 = next thing to do, p2 = should do, p3 = icebox.
`--readiness ready` only when the work is fully specified; otherwise
`needs-shaping`.

## 4. Verify and report

```bash
wt ls -q <QUEUE>
```

Show the user the refs you filed. Those refs — not the session, not the
compact summary — are now the source of truth for what's next.

## After filing

Compacting is safe: everything of value now lives in the queue. The session
can end. The next session resumes from `wt ls -q <QUEUE>`, not from memory.

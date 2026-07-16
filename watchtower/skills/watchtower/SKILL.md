---
name: watchtower
description: Use when asked about a WatchTower ticket ref (e.g. WT-48, HERMES-20, any <PROJECT>-<N>), asked "is queue X stuck / drained", or asked to file/claim/close a ticket. WatchTower (`wt`) is a local CLI that tracks fleets of AI coding-agent workers across named queues.
---

# WatchTower

WatchTower is a local, stdlib-only CLI (`wt`) that tracks tickets in named
queues and the agent workers draining them. Tickets are refs like `WT-48` or
`HERMES-20` (`<PROJECT>-<N>`). If `wt` isn't on `$PATH`, WatchTower isn't
installed here — say so rather than guessing at ticket state.

## Scope: what WatchTower is (and isn't)

WatchTower is a **ticket + worker tracker**. It answers "what's in queue X",
"is a queue stuck", and files/claims/closes tickets. That's all `wt` does.

It does **not** message, ping, spawn, or inject into a running session. A bare
session UUID like `e1588ce4-…` is a CCC conversation, not a WatchTower object,
and `wt` has no command that reaches it. If asked to "ping / message / ask /
inject into session `<uuid>`", that's Claude Command Center — use the
`ccc-orchestration` skill instead, not this one.

## Look up a ticket by ref

You don't need to know which queue a ref belongs to:

```bash
wt find HERMES-20 --json     # searches every queue; exits 1 if not found
```

Use this whenever asked to "check", "look up", or "refer to" a specific ref,
even from a session working in an unrelated repo.

## Check queue health

```bash
wt status --json             # every queue: depth, oldest-open age, stuck flag
wt status -q WT --json       # one queue
```

A queue is "stuck" when it has open tickets and none has closed in the last
10 minutes.

## List / file / work tickets

```bash
wt ls -q WT --json                          # tickets in one queue (needs -q)
wt add -q WT --title "..." --type bug       # file a ticket
wt claim -q WT --worker <id> --type bug --json   # claim the next open one
wt close <ref> --worker <id> --summary "..."      # close (summary required)
```

## Import a document as tickets

Preview extracted tasks first, then apply them to a queue:

```bash
wt import plan.md -q PLAN
wt import plan.md -q PLAN --apply --type feature
```

Preview is the default. The importer makes one tool-free Claude reasoning call
over the complete document, infers explicit and implicit work, and chooses
coherent one-worker-session ticket granularity. Structural Markdown is a hint,
not the extraction mechanism. Each filed ticket retains its source path and
line anchor, and dependencies become Watchtower refs in ticket bodies. Stable
source keys make repeat imports idempotent, so a later import files only newly
inferred tasks. If Claude is unavailable or returns a malformed ticket graph,
the command fails before filing anything.

`wt close` rejects a close with no `--summary` (exit code 1) — that
resolution text is the trust signal surfaced on the dashboard, so always
supply one when closing a ticket you worked.

## Don't fabricate

If `wt find <ref>` returns "not found" or `wt` isn't installed, say so
plainly. Don't infer a ticket's status from memory or guess at a queue name.

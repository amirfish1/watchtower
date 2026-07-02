---
name: watchtower
description: Use when asked about a WatchTower ticket ref (e.g. WT-48, HERMES-20, any <PROJECT>-<N>), asked "is queue X stuck / drained", or asked to file/claim/close a ticket. WatchTower (`wt`) is a local CLI that tracks fleets of AI coding-agent workers across named queues.
---

# WatchTower

WatchTower is a local, stdlib-only CLI (`wt`) that tracks tickets in named
queues and the agent workers draining them. Tickets are refs like `WT-48` or
`HERMES-20` (`<PROJECT>-<N>`). If `wt` isn't on `$PATH`, WatchTower isn't
installed here — say so rather than guessing at ticket state.

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

`wt close` rejects a close with no `--summary` (exit code 1) — that
resolution text is the trust signal surfaced on the dashboard, so always
supply one when closing a ticket you worked.

## Don't fabricate

If `wt find <ref>` returns "not found" or `wt` isn't installed, say so
plainly. Don't infer a ticket's status from memory or guess at a queue name.

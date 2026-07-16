---
name: wt-triage-queue
description: Use when a WatchTower queue is reported stuck, has aging open tickets, has claims that look abandoned (claimed but nothing closing), or otherwise needs draining/cleanup triage before it can proceed.
---

# Triage a stuck WatchTower queue

**REQUIRED BACKGROUND:** the `watchtower` skill covers ticket lookup, filing,
claiming, and closing. This skill is the next step: what to do when a queue
*isn't moving* and you need to find out why and unstick it.

## 1. Confirm it's actually stuck

```bash
wt status -q <QUEUE> --json          # depth, oldest-open age, stuck flag
wt status -q <QUEUE> --stuck-minutes 30 --json   # tune the stuck threshold
```

A queue is flagged stuck when it has open tickets and none has closed within
the window (10 minutes by default, per the `watchtower` skill). Don't act on
a "stuck" flag alone — read `wt ls` next to see *what's* stuck.

## 2. See what's actually sitting there

```bash
wt ls -q <QUEUE> --status all --json          # every ticket, any state
wt ls -q <QUEUE> --status in_progress --json  # claimed but not closed — the usual culprit
wt blocked -q <QUEUE> --json                  # parked for a human decision
```

`--status` defaults to `active` (open + in_progress) if you omit it. Use
`--status all` when triaging so closed/duplicate tickets don't hide in the
noise.

## 3. Unstick what you find

- **Ticket parked for a human** (`wt blocked` shows it): answer it — this
  auto-resumes the worker's session:
  ```bash
  wt answer <ref> "<your answer>"
  ```
- **Claimed but the worker is dead/gone** (in_progress with no recent
  activity, and `wt workers --json` doesn't show a live worker for it):
  release the claim so it goes back to `open` for another worker to pick up.
  Don't fabricate a `close` for work you didn't verify happened:
  ```bash
  wt release <ref> --worker <id>
  ```
- **Exact-duplicate open tickets** clogging the queue: dry-run first, then
  apply:
  ```bash
  wt dedup -q <QUEUE>            # shows what would close, changes nothing
  wt dedup -q <QUEUE> --apply    # actually closes the dupes
  ```
- **No workers are draining the queue at all**: check whether auto-drain is
  on, and turn it on if not:
  ```bash
  wt drain on <QUEUE>                        # auto-spawn workers
  wt drain on <QUEUE> --type bug             # restrict auto-drain to one type
  wt drain off <QUEUE>                       # back to backlog mode (manual claim only)
  ```

## 4. Confirm it drains

```bash
wt wait -q <QUEUE> --timeout 0     # block until drained (0 = forever; use a real timeout in scripts)
wt wait -q <QUEUE> --timeout 600 --cmd "echo drained"
```

Prefer a bounded `--timeout` over `0` unless you're a long-running watcher —
an unbounded wait in a one-shot triage session just hangs.

## 5. Build a standing health check (optional)

If the same stuck condition recurs, file a monitor instead of re-triaging by
hand each time — it auto-files a ticket when the check fails:

```bash
wt monitor -q <QUEUE> --cmd "<healthcheck-shell-command>" --title "<ticket title on failure>"
```

## Don't fabricate

Every command above is real (`wt <command> --help`). Don't invent flags or
guess at a queue's state — if `wt status` or `wt ls` doesn't show what you
expect, say so rather than assuming the queue is fine.

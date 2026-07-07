---
name: critique
description: Use when asked to "/critique" something, get a second opinion, or have two other agents independently review a plan/design/diff/decision. Spawns two cross-family agents (via `wt critique`) that each critique the same goal fresh and report back.
---

# Critique

`wt critique` spawns two independent agents to critique a goal — a plan,
design, diff, decision, or anything else you describe. Each spawned agent
gets the same goal text plus baked-in ground rules (contrarian, no priors,
comprehensive, scored, concrete resolutions), and reports back to your
session when done.

## Requirements

`wt critique` needs `wt` on `$PATH` plus the engine CLIs it spawns:
`claude` (Claude Code), `codex` (Codex), and `antigravity` — whose CLI
binary is named `agy` (override its location with
`$WATCHTOWER_ANTIGRAVITY_BIN`). Spawning is WT-native — no CCC required:
each critic runs as a local one-shot process tracked in `wt workers` (kind
`adhoc`), and the report comes back via `wt send` (parking in the WT outbox
if the target is unreachable).

Engine selection is preflighted before anything spawns: if a *default*
engine's CLI is missing, that critic falls back to another installed family
with a printed note (never spawning two identical critics — with only one
family installed you get one critic, not a duplicate pair). An *explicitly
requested* engine that is missing or unsupported errors out instead, before
any critic is spawned.

## Usage

```bash
wt critique "<goal — what to critique and any context the critics need>"
```

By default this picks the two agent families other than your own (of
`claude` / `codex` / `antigravity`). Your family is auto-detected from the
harness environment — `$CLAUDE_CODE_SESSION_ID` means Claude Code,
`$CODEX_THREAD_ID` means Codex. **Antigravity sets neither: if you are
driving from Antigravity you MUST pass `--family antigravity`** (also pass
`--family` anywhere detection would guess wrong):

```bash
wt critique "review the retry-backoff design in docs/retry.md" --family antigravity
```

Override which engines get spawned explicitly:

```bash
wt critique "review this diff" --engine1 codex --engine2 antigravity
```

Other flags:
- `--report-to <target>` — who the critics report back to via `wt send`: a
  worker id, `@agent` name, or session UUID. Defaults to your own session,
  auto-detected: `$CLAUDE_CODE_SESSION_ID` directly, or `$CODEX_THREAD_ID`
  auto-registered in the WT agents registry so replies route over the codex
  transport. **From Antigravity or any other undetected harness, pass
  `--report-to` explicitly — without it the reports only land in the
  critics' log files** (a loud warning is printed if that's about to
  happen).
- `--cwd <path>` — repo/dir the critique agents work in (default: the
  current directory). Applied at spawn time; it won't show inside the
  `--dry-run` argv, but the record's `repo_path` field reflects it.
- `--dry-run` — show what would be spawned without launching anything
  (availability preflight still runs, so a dry-run that passes will spawn).
- `--json` — machine-readable `[{ok, worker_id, engine, argv, error}, ...]`.

`--model1`/`--model2` still work as deprecated aliases for
`--engine1`/`--engine2`.

For a single ad-hoc agent on any goal (the primitive critique builds on),
use `wt spawn "<goal>" [--engine claude|codex|antigravity] [--report-to t]`.

## Waiting for the reports

The critics run asynchronously (minutes, not seconds). After spawning:
tell your user the critics are running and end your turn — each report
arrives in your session as a `wt send` message when its critic finishes
(quote-safe, delivered over stdin: `wt send <you> - <<'WT_REPORT'`). Don't
poll or sleep-loop; if a report hasn't arrived, `wt workers` shows whether
the critic is still alive, and its log file has the raw output.

## What the goal text should contain

`wt critique` only adds the *how to critique* rules (be contrarian, ignore
priors, be comprehensive, score it, give concrete resolutions with the score
delta each buys). It does not add *what* to critique — put the full context
in the goal string yourself: what the thing is, where to find it (file
paths, a diff, a doc), and what "good" looks like if that's not obvious.
Vague goals like "review this" get vague critiques back.

## What this is not

`wt critique` spawns *new* sessions to critique something — it is not for
messaging or resuming an existing session (that's `wt send`/`wt ask`, or the
`ccc-orchestration` skill), and it is not a substitute for `/code-review`
(which reviews the working diff in-process rather than spawning external
critics).

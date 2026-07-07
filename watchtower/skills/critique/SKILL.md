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

`wt critique` needs `wt` on `$PATH` plus the engine CLIs it spawns (`claude`,
`codex`, `agy`). Spawning is WT-native — no CCC required: each critic runs as
a local one-shot process tracked in `wt workers` (kind `adhoc`), and the
report comes back via `wt send` (parking in the WT outbox if the target is
unreachable). If a default engine's CLI is missing, that critic falls back to
your own family with a printed note; an explicitly requested engine that is
missing errors instead.

## Usage

```bash
wt critique "<goal — what to critique and any context the critics need>"
```

By default this picks the two agent families other than your own (of
`claude` / `codex` / `antigravity` — pass `--family` if auto-detection would
guess wrong, e.g. you're driving from a family other than Claude):

```bash
wt critique "review the retry-backoff design in docs/retry.md" --family claude
```

Override which engines get spawned explicitly:

```bash
wt critique "review this diff" --model1 codex --model2 antigravity
```

Other flags:
- `--report-to <target>` — who the critics report back to via `wt send`: a
  worker id, `@agent` name, or session UUID (default
  `$CLAUDE_CODE_SESSION_ID`, i.e. you).
- `--cwd <path>` — repo/dir the critique agents work in (default: the
  current directory).
- `--dry-run` — show what would be spawned without launching anything.
- `--json` — machine-readable `[{ok, worker_id, engine, argv, error}, ...]`.

For a single ad-hoc agent on any goal (the primitive critique builds on),
use `wt spawn "<goal>" [--engine claude|codex|antigravity] [--report-to t]`.

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

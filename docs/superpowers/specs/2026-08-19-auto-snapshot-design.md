# Auto-Snapshot ("token-sitter") — Design Spec

Date: 2026-08-19
Status: approved design, pre-implementation
Owner: Amir Fish
Implementation: delegated to Gemini 3.7 agents via CCC spawns (antigravity engine)

## Problem

A long agent session (e.g. 300K tokens of context) that goes idle loses its
prompt cache at the ~60-minute cliff (measured: 99.5% cache hit at 59 min,
12% at 61). If the user stepped away and returns later, continuing the
session re-reads the entire context at cold price — the "300K penalty."

CCC solves this today with an "auto-handover" button: a watchdog loop inside
the CCC server injects a prompt at 55 idle minutes telling the session to
write a checkpoint (`wt add` ticket). This works but requires the CCC UI and
its always-running server.

This feature extracts the capability into a standalone, installable command
set that works without CCC.

## Decisions (settled during brainstorm)

1. **Code lives in the watchtower repo; brand lives at the distribution
   layer.** Shipped as a Claude Code marketplace plugin with its own
   feature-named brand (final name `token-sitter`, chosen 2026-08-20 in a dedicated naming session). Optional later: a thin landing
   repo (README + installer only, no code).
2. **No daemon.** The trigger is a one-shot detached timer process per armed
   session — not a persistent server, not launchd, and NOT folded into the
   watchtower orchestrator loop even when that daemon happens to run (one
   firing mechanism, zero coupling, works for daemon-less installs).
3. **Snapshot storage is a plain file keyed by session id**, not a wt ticket.
   A wt ticket is an optional *pointer* for dashboard visibility when queues
   are configured.
4. **Late fire = no fire.** If the timer oversleeps past the cache-TTL window
   (laptop sleep), it must NOT inject; the user decides what to do with a
   cold session.
5. **Tiered engine support.** v1 MUST ship Tier 1 (Claude, full) and Tier 2
   (Codex, headless auto-fire). Tier 3 (Kimi, Grok): install + manual
   commands only; auto-fire unsupported with a clear message.

## Commands (skills / prompt files)

| Command | Effect |
|---|---|
| `/auto-snapshot-on [minutes]` | Arm; default 55 idle minutes |
| `/auto-snapshot-off` | Disarm |
| `/snapshot-now` | Write a snapshot immediately, no timer (works on every engine) |
| `/resume-from-snapshot [id\|path]` | After `/clear`: load the newest snapshot for this cwd (or the one named) and continue |

Skills are thin prompts; all real logic lives in the `wt` CLI so behavior is
testable and uniform across engines. Claude installs via the plugin
marketplace. Codex/Kimi/Grok get the same commands as prompt files via
`wt snapshot install --engine codex|kimi|grok`. The plugin requires the `wt`
CLI; setup installs it from git until PyPI publishing is unblocked (PyPI
still serves 0.1.0; repo is at 0.4.0 — no creds).

## Components

### `wt snapshot` subcommand family

- `wt snapshot arm --session <sid> --engine <e> --idle <min>` — spawn the
  one-shot timer (detached). Re-arm replaces the existing timer.
- `wt snapshot disarm --session <sid>` — kill via pidfile, clean state.
- `wt snapshot status [--session <sid>]` — armed timers, last outcomes.
- `wt snapshot fire --session <sid>` — the delivery step, callable directly
  for testing.
- `wt snapshot install --engine <e>` — drop prompt files for non-Claude
  engines.

### One-shot timer process

State under `~/.watchtower/snapshots/timers/<session-id>.{pid,json}`.

Loop: sleep until deadline → wake → read transcript mtime → compute true
idle:

- idle < threshold (user was active): resleep exactly the remainder.
- threshold ≤ idle < cache-TTL (default window 55–60 min; the upper bound is
  always the cache TTL, 60, regardless of a custom `--idle` threshold):
  recheck mtime
  immediately before delivery (user may have just returned → resleep), then
  deliver the snapshot prompt, record `fired`, **exit**. One-shot per arm —
  never re-fire on the fire's own transcript footprint (CCC observed a 10×
  overnight refire loop before making handover one-shot).
- idle ≥ cache-TTL (overslept): write a `skipped-overslept` marker, exit.
  No injection.

Hard-capped lifetime (24h) so nothing can orphan. Timer dies with fire,
disarm, oversleep-skip, or cap — whichever comes first.

### Delivery (per engine, always to an *idle* session by construction)

- **Claude**: watchtower's existing chain — keystroke into a live idle TUI
  (`watchtower/tty.py`, Terminal.app/iTerm2), else headless
  `claude -p --resume <sid>`. Terminal-closed case: deliver via headless
  resume, within the fire window only (the window rule prevents the
  wasteful cold-resurrect CCC avoided).
- **Codex**: new adapter using `codex exec resume <thread-id> "<prompt>"`.
  No keystroke path in v1; headless resume is Codex's only mode.
- **Kimi/Grok**: none in v1. `/auto-snapshot-on` on these engines explains
  auto-fire is unsupported and points to `/snapshot-now`.

The delivered prompt instructs the model to: write the snapshot file at the
exact path given, optionally file one wt ticket pointing at it, and take no
other action.

### Snapshot storage

- `~/.watchtower/snapshots/<session-id>.md` — frontmatter: session id,
  engine, cwd, git branch + commit, trigger (`auto|manual`), created-at.
  Body: what's done, what's in flight, next concrete step, key files,
  gotchas.
- `~/.watchtower/snapshots/by-cwd/<slug>/latest` symlink → newest snapshot
  for that project dir, so `/resume-from-snapshot` needs no arguments.
- Optional wt ticket (when queues configured) pointing at the file; the file
  is the source of truth.
- On resume, the snapshot is archived as consumed (moved to `archive/`).

## Flows

**Arm**: skill resolves its own session id + engine (Claude: newest transcript
jsonl for the cwd slug / `$CLAUDE_SESSION_ID`; Codex: thread id), runs
`wt snapshot arm`, confirms with the fire window ("armed; snapshots after 55
idle minutes; window closes at 60"). Warns if CCC's auto-handover flag is
already set for the same session (double-snapshot is harmless but noisy).

**Fire**: as specified under the timer process.

**Resume**: after `/clear`, skill loads `by-cwd/<slug>/latest` (explicit
arg overrides), warns if the git branch/commit drifted since capture, states
a one-paragraph "here's where we were," archives the snapshot, continues.

## Edge cases

- **User returns during fire**: final mtime recheck immediately before
  delivery; recent activity → resleep, not fire.
- **Laptop sleep**: `sleep` slides; the wake-side idle computation plus the
  fire window makes oversleep a skip, never a late inject.
- **Double-arm**: re-arm replaces the timer and its deadline.
- **Two sessions, same cwd**: `latest` symlink points at the newest; explicit
  id/path argument disambiguates.
- **Timer killed (reboot, kill -9)**: nothing fires; `wt snapshot status`
  shows the armed-but-dead state. Orchestrator-as-backstop is explicitly
  deferred to v2.

## Testing

- Timer logic (window math, resleep remainders, oversleep skip, one-shot
  marking) as pure functions unit-tested against fake transcript mtimes — no
  sleeping in tests.
- Delivery adapters tested in the style of the existing `tty.py` tests
  (parse/build/dry-run, no real keystrokes in CI).
- One end-to-end smoke per Tier-1/2 engine with a short idle threshold
  against a real session.

## Out of scope (v1)

- Kimi/Grok auto-fire (needs a keystroke transport nobody has mapped).
- Orchestrator-daemon backstop for dead timers.
- Codex TUI keystroke injection (extend `tty.py` later).
- Thin landing repo (marketing shell; can be added any time, no code moves).

## Open items

- Exact `codex exec resume` prompt-size/escaping limits — verify during
  implementation.

## Addendum 2026-08-24: fire mode (`mdfile` / `compact` / `both`)

v1 always fired the mdfile flow above. `wt snapshot arm` now takes
`--mode {mdfile,compact,both}` (default `mdfile`, unchanged behavior):

- `mdfile` — the flow described in this spec, unchanged.
- `compact` — delivers a literal `/compact` instead of the snapshot prompt.
  Claude only (rejected for any other engine at `arm` time): keeps the SAME
  session cheap to resume in place, by running the harness's own compaction
  before the cache TTL lapses (so the compaction's own big input read is
  still cache-priced, not a cold re-read). Does nothing for a process that
  no longer exists when the user returns — this mode trades that durability
  for staying in-session.
- `both` — delivers `/compact`, waits (fixed step schedule, ~100s total,
  covering the 0:25–1:07 compaction durations observed in practice) for it
  to land, then delivers the ordinary mdfile prompt as a second message.
  Belt-and-suspenders: cheap in-place resume from `compact`, plus the
  process-independent durability from `mdfile` if the session doesn't
  survive the gap.

Mode is stored on the timer's state file (`mode` key) and read back by
`fire()`; old state files without the key default to `mdfile`. CCC's
status-bar toggle (`toggleAutoHandoverForPane` → renamed
`cycleAutoHandoverModeForPane` in `static/app.js`) now cycles the pill
through compact → md → both → off per click instead of a plain on/off
toggle, debounced ~450ms so a fast click-through settles once.

---
name: auto-snapshot-on
description: Arm auto-snapshot for this session — if you go idle near the prompt-cache cliff (default 55 min), the session writes a durable state snapshot (and/or runs /compact) so it stays cheap to pick back up. Use when the user says "/auto-snapshot-on", "auto snapshot on", or "snapshot me if I step away".
---

# Auto-snapshot: arm

Arm a one-shot idle timer for THIS session. Accepts two optional arguments:
idle minutes (default 55; must be below 60, the cache TTL) and a mode.

**Mode** — what the timer does when it fires:
- `mdfile` (default) — write a durable markdown snapshot; works on every
  supported engine; a NEW session reads it back via /resume-from-snapshot.
  Best when the process might not survive the gap (crash, closed terminal,
  different engine next time).
- `compact` — deliver a literal `/compact` into the SAME live session.
  Claude Code only. Keeps the session itself alive and cheap to resume in
  place; does nothing for you if that process is gone when you return.
- `both` — `/compact` first, then (after waiting for it to land) also write
  the markdown snapshot. Belt-and-suspenders: cheap in-place resume AND a
  durable fallback if the process doesn't survive.

1. Determine your session id and engine:
   - Claude Code: session id = `$CLAUDE_SESSION_ID` if set; otherwise the
     basename (without `.jsonl`) of the newest `*.jsonl` under
     `~/.claude/projects/<slugified-cwd>/` (slug: `/` and `.` become `-`).
     Engine is `claude`.
   - Codex: your thread id; engine is `codex`.
   - Any other engine (kimi, grok, devin, ...): auto-fire is not supported —
     tell the user to run /snapshot-now before stepping away, and stop.
2. Run (network sandbox not required, but run outside any restricted shell):
   `wt snapshot arm --session <SID> --engine <ENGINE> --cwd "$PWD" --idle <MIN> --mode <MODE>`
   (omit `--mode` for the `mdfile` default). `compact`/`both` are rejected
   for any engine other than `claude` — fall back to `mdfile` there.
3. Relay the confirmation, including the mode and fire window. If the
   command errors, show the error verbatim — do not retry with different
   numbers unless the user asks.

The timer is one-shot: after it fires (or skips because you were idle past
the 60-min TTL), it will not fire again until re-armed.

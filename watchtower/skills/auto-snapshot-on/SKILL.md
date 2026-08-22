---
name: auto-snapshot-on
description: Arm auto-snapshot for this session — if you go idle near the prompt-cache cliff (default 55 min), the session writes a durable state snapshot so a fresh session can resume without re-paying the full context cost. Use when the user says "/auto-snapshot-on", "auto snapshot on", or "snapshot me if I step away".
---

# Auto-snapshot: arm

Arm a one-shot idle timer for THIS session. Accepts an optional argument:
idle minutes (default 55; must be below 60, the cache TTL).

1. Determine your session id and engine:
   - Claude Code: session id = `$CLAUDE_SESSION_ID` if set; otherwise the
     basename (without `.jsonl`) of the newest `*.jsonl` under
     `~/.claude/projects/<slugified-cwd>/` (slug: `/` and `.` become `-`).
     Engine is `claude`.
   - Codex: your thread id; engine is `codex`.
   - Any other engine (kimi, grok, devin, ...): auto-fire is not supported —
     tell the user to run /snapshot-now before stepping away, and stop.
2. Run (network sandbox not required, but run outside any restricted shell):
   `wt snapshot arm --session <SID> --engine <ENGINE> --cwd "$PWD" --idle <MIN>`
3. Relay the confirmation (including the fire window) and any warning about
   CCC auto-handover being armed too. If the command errors, show the error
   verbatim — do not retry with different numbers unless the user asks.

The timer is one-shot: after it fires (or skips because you were idle past
the 60-min TTL), it will not fire again until re-armed.

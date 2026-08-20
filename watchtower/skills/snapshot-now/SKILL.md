---
name: snapshot-now
description: Write a durable state snapshot of this session immediately (no timer) so a fresh session can resume it cheaply after /clear. Works on every engine. Use when the user says "/snapshot-now" or is about to step away on an engine without auto-fire.
---

# Snapshot now

1. Determine your session id and engine (same as auto-snapshot-on step 1;
   any engine is fine here).
2. Get the canonical path: `wt snapshot path --session <SID>`
3. Write that file with YAML frontmatter — `session_id`, `engine`, `cwd`
   (absolute), `git_branch`, `git_commit`, `trigger: manual`, `created_at`
   (ISO) — and a body with sections: What's done; What's in flight; Next
   concrete step; Key files; Gotchas & decisions. Write for a reader with
   ZERO context: no session-local shorthand.
4. Run: `wt snapshot record --session <SID> --cwd "$PWD"`
5. Tell the user the snapshot is saved and that after /clear they can run
   /resume-from-snapshot in this directory.

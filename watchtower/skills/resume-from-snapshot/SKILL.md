---
name: resume-from-snapshot
description: Resume work from the most recent auto/manual snapshot for this project directory — run after /clear (or in a brand-new session) to continue without reloading the old session's full context. Use when the user says "/resume-from-snapshot" or "restore from snapshot".
---

# Resume from snapshot

Accepts an optional argument: an explicit snapshot file path or session id.

1. Locate the snapshot:
   - No argument: `wt snapshot latest --cwd "$PWD"` (if it exits 1, tell the
     user no snapshot exists for this directory and stop).
   - Session-id argument: `wt snapshot path --session <ARG>`.
   - Path argument: use it directly.
2. Read the file. Compare its `git_branch`/`git_commit` frontmatter to the
   current repo state; if they differ, warn the user the tree has moved
   since the snapshot and summarize the drift (branch name, commits between).
3. State in one short paragraph: what was done, what was in flight, and the
   next concrete step. Then continue that work (or await the user's go-ahead
   if the next step is destructive/outward-facing).
4. Archive it so `latest` stops pointing at consumed state:
   `wt snapshot consume --path <FILE>`

---
name: auto-snapshot-off
description: Disarm auto-snapshot for this session (the user is back and no checkpoint is needed). Use when the user says "/auto-snapshot-off" or "auto snapshot off".
---

# Auto-snapshot: disarm

1. Determine your session id (same procedure as auto-snapshot-on step 1).
2. Run: `wt snapshot disarm --session <SID>`
3. Confirm to the user, or show the error verbatim ("no timer state" just
   means nothing was armed — say so plainly).

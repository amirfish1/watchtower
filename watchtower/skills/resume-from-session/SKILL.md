---
name: resume-from-session
description: Resume work from a recent prior session WITHOUT a snapshot — lists the last 10 sessions for this project, lets the user pick one, and briefs this fresh session from that transcript. Use when the user says "/resume-from-session", "continue from an old session", or wants to pick up prior work but no snapshot exists.
---

# Resume from a prior session (no snapshot needed)

1. Determine your own session id (same procedure as auto-snapshot-on step 1)
   so you can exclude yourself from the list.
2. Run: `wt snapshot sessions --cwd "$PWD" -n 10 --exclude <YOUR-SID>`
   If it prints "no sessions found", tell the user and stop.
3. Show the numbered list (id shortened to 8 chars, age, first-message
   snippet) and ask the user to pick one. In Claude Code, use the
   AskUserQuestion tool with the top choices; elsewhere ask for a number.
4. The chosen session's transcript is
   `~/.claude/projects/<slugified-cwd>/<SID>.jsonl` (slug: `/` and `.`
   become `-`). Read its TAIL (roughly the last 150-300 lines) and, if
   needed for orientation, the first few user messages. Do NOT ingest the
   whole file — these transcripts can be enormous; the point of this
   command is briefing, not full replay.
5. Brief the user in one short paragraph: what that session was doing, what
   it finished, and what appears to have been left open. Mention the
   transcript path so deeper digs are one command away.
6. Only if a Total Recall install is detected (the `/recall` skill is
   available, or `command -v total-recall` succeeds): recommend running
   `/recall <topic of that session>` to pull richer cross-session context,
   and offer to do it. If Total Recall is not installed, do not mention it
   at all.
7. Continue the open work (or await the user's go-ahead if the next step is
   destructive/outward-facing).

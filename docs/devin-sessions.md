# Addressing a devin session

Everything WatchTower knows about reaching a Devin CLI session. Devin breaks
three assumptions the other engines share, so the exceptions are collected
here rather than rediscovered per ticket (WATCHTOWER-32, -33, -34).

## Ids are word slugs, never UUIDs

A devin session id is `glorious-sturgeon`, not hex. Every shape-based branch
in `messages.resolve_target` (`_UUID_RE`, `_HEX_PREFIX_RE`) therefore rejects
it, and so do `workers._SESSION_ID_RE` / `_is_worker_session_id`. Anything
that validates a session id by shape needs an explicit devin case.

`recent_sessions()` can never help: it lists `<projects>/*/<uuid>.jsonl` and
matches `_UUID_RE` on the stem, so it only ever yields Claude transcript
UUIDs. A devin id is never "in the recent set", no matter how active it is.

## Two spellings, for two different consumers

| Form | Who wants it | Where it comes from |
|---|---|---|
| `glorious-sturgeon` (bare) | `devin --resume <sid>` | `workers.resolve_devin_session_id` |
| `devincli-glorious-sturgeon` | CCC, and so `wt send` | `workers.current_devin_session_id` |

CCC prefixes devin-CLI sessions to keep them from colliding with cloud Devin
sessions (`ccc_server/devin.py`, `DEVIN_CLI_SESSION_PREFIX`). Its
`/api/inject-input` strips the prefix itself, so WT passes the prefixed form
through verbatim — see `_deliver_delegate`. WT has no native devin transport
(no FIFO, and `_deliver_resume` is claude-only), so every send to a devin
target that is not a live WT worker ends at the delegate.

A devin WORKER's recorded `session_id` is the BARE slug, because that is what
`devin --resume` takes. It resolves only through `resolve_target`'s
live-worker branch, so it stops resolving once the worker is pruned.

## Finding a session id at all

`devin -p` never prints one. Two recovery paths, for two different questions:

- **"Which session is worker X?"** — `workers.resolve_devin_session_id`. Devin
  records every prompt, print mode included, in `sessions.db`
  `prompt_history`; the drain goal's `Your worker id is <id>` line links them.
- **"Which session am I running inside?"** — `workers.current_devin_session_id`.
  Devin sets no session env var (neither `CLAUDE_CODE_SESSION_ID` nor
  `CODEX_THREAD_ID`), which is why a `wt add` from a devin shell used to file
  with `submitter=""` and silently skip notifying its own filer. Devin does
  write `session_locks/<slug>.lock` holding the session's pid, so walking our
  own parent chain names the session we are in.

  **Devin never prunes those locks** — 199 of 201 on the dev machine held dead
  pids — and macOS recycles pids, so a bare pid match would eventually name a
  stranger's session as a ticket's filer. Only match a pid that is still a
  live `devin`. A wrong filer is worse than none.

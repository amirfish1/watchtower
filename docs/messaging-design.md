# WatchTower Messaging: send, ask, chat

Status: approved design, 2026-07-01. Implements the next WatchTower layer:
cross-agent conversation (async messages, synchronous asks, group chats with
nudge orchestration) as standalone `wt` capabilities. CCC becomes a client.

## Why

WatchTower today is queues + workers. Queue orchestration is becoming a
commodity; the durable layer is fleet coordination. The missing primitives are
conversational: push a message to an agent, ask an agent and wait for the
answer, and run a multi-agent group chat where the orchestrator (not an LLM)
decides who gets nudged to respond. CCC has all three today, but locked inside
its server process (threads die with the server, macOS-only transports mixed
with portable ones, no CLI). WT extracts the portable core.

## Division of labor (the one-sentence architecture)

WatchTower owns the data plane and the brain: message outbox, chat files,
delivery retries, and nudge policy, all durable on disk and driven by the
launchd-supervised daemon. CCC keeps what only CCC can do: the browser UI and
the macOS TTY transport (AppleScript keystroke injection), reachable by WT as
a delegate. Everything CCC currently duplicates (chat watcher thread, pending
inputs queue, targeting logic) migrates to WT behind a flag.

## New modules

- `watchtower/messages.py`: outbox store + delivery adapters + ask.
- `watchtower/chats.py`: group-chat store (CCC-compatible format) + nudge
  targeting + scheduler tick.

Both stdlib-only, both env-var overridable for tests, matching existing
patterns (`$WATCHTOWER_*` overrides, `_FileLock`, tmp+`os.replace`).

## Addressing

Targets are resolved in order:

1. WT worker id (`ccc-40546374`): live worker, has FIFO.
2. Registered agent name (`@planner`): `~/.watchtower/agents.json`
   (`$WATCHTOWER_AGENTS_FILE`), schema
   `{name: {session_id, engine, cwd, registered_at, last_seen}}`.
   WT workers auto-register under their worker id; anyone can
   `wt agent register <name> --session <uuid> [--engine claude] [--cwd path]`.
3. Raw session UUID (or unique prefix >= 8 chars).

`wt agents` lists the registry with liveness where known.

## Delivery adapters (ordered fall-through)

1. `fifo`: target maps to a live WT worker; reuse `write_to_worker_fifo`
   (existing stream-json user line). Cheapest, zero new code.
2. `resume`: `claude -p --resume <sid> --input-format stream-json
   --output-format stream-json` headless, FIFO stdin, log to
   `~/.watchtower/logs/msg-<sid8>-<ts>.log`. Port of the `wt answer`
   machinery, generalized.
3. `delegate`: POST to a delegate HTTP endpoint for sessions WT cannot reach
   natively (live TTY sessions needing AppleScript, codex/gemini/cursor
   engines). Default delegate: CCC at `http://127.0.0.1:<port>/api/inject-input`
   with the port read from `~/.claude/command-center/port.txt` when present.
   Override via `$WATCHTOWER_DELEGATE_URL`. The delegate seam is also the
   future federation point (remote WT instances).

If every adapter fails, the message lands in the outbox.

## Outbox (durable, at-least-once)

`~/.watchtower/outbox.json` (`$WATCHTOWER_OUTBOX_FILE`):

```json
{"messages": [{"id": "msg-...", "to": "<resolved sid or name>", "text": "...",
  "mode": "send", "created_at": "...", "attempts": 3,
  "next_attempt_at": "...", "last_error": "...",
  "status": "pending|delivered|dead"}]}
```

The daemon drains pending messages each tick with exponential backoff
(30s base, cap 10 min); after 20 attempts a message goes `dead` and is logged.
Activity log gains verbs: `SEND`, `ASK`, `POST`, `NUDGE`, `DEADMSG`.

## CLI surface

```
wt send <target> "text" [--mode send|steer] [--now|--queue]
wt ask  <target> "question" [--timeout 30] [--json]
wt agents / wt agent register|rm
wt chat new "topic" --with <target>[,<target>...] [--include-human]
wt chat post <chat> "message" [--as <name>]
wt chat read <chat> [--tail N] [--json]
wt chat ls [--archived]
wt chat nudge <chat> [--target <sid>]
wt chat add <chat> <target> / wt chat leave <chat> <target>
wt chat close <chat> / wt chat archive <chat>
```

`wt send` answers the "/inject is also regular messages" concern: send IS the
primitive; inject/steer/answer are modes of it.

`wt ask` correlation is byte-offset tailing (snapshot transcript or log size
before delivery, read only bytes after), same proven mechanism as CCC.
Reply detection: fifo/resume paths tail the worker or resume log for a
`{"type":"result"}` event or assistant text followed by end of turn; delegate
path passes through to CCC `/api/ask`. Timeout returns
`{"ok": false, "error": "timeout", "partial": "..."}`. Optional
`--notify-webhook` mirrors `wt wait`.

## Group chats: CCC-compatible on disk

Chat dir resolution (same pattern as the queue store):
`$WATCHTOWER_CHATS_DIR` -> `~/.claude/group-chats` if it exists ->
`~/.watchtower/chats`. File format is byte-compatible with CCC:

- `<slug>-<ts>.md`: header (topic, started, mode, participants) plus
  append-only `## <ts> — <8hex|Human>: <display name>` entries.
- `<slug>-<ts>.json` sidecar: `{uuid, session_ids, topic, mode, name_map,
  include_human, started_at, archived, closed_at, ...}`.

Consequence: CCC's group-chat UI keeps working unchanged, both systems
interoperate on the same files from day one, and rollback is deleting a flag.

## Nudge orchestration (the daemon owns it)

Port CCC's deterministic targeting into pure functions in `chats.py`:

- Parse last `##` heading author from the md tail.
- Last author is an agent: nudge everyone else.
- Last author is Human with @mentions or 8-hex ids: nudge only those.
- Last author is Human, no mentions: nudge the previous agent writer, else all.
- Dedup via `last_reminder_key` (post count + heading) in the sidecar.

Scheduler: `chats.nudge_tick()` called from `_daemon_loop` each tick. Policy
knobs (sidecar, with defaults): `nudge_interval_s` 60, `idle_close_s` 2700
(45 min), done markers close the chat. Loop prevention: hard cap
`max_auto_nudges_per_hour` (default 30) per chat; over cap, log and pause.
Nudge text is engine-agnostic prose (invoke your group-chat-checkin skill, or
read the chat file), same wording contract as CCC; delivery goes through the
adapter chain above, so a chat can mix WT workers, TTY sessions (via
delegate), and codex sessions (via delegate).

## HTTP API (dashboard server, 8787)

- `POST /api/send` `{to, text, mode}`
- `POST /api/ask` `{to, text, timeout_ms}`
- `GET/POST /api/chats`, `POST /api/chat/<id>/post`, `GET /api/chat/<id>`
- All POSTs get a same-origin/localhost check (port CCC's `_check_same_origin`
  posture) before this ships.

## CCC as client (staged, behind flags)

1. Stage 1 (this build): CCC env flag `CCC_CHAT_ORCHESTRATOR=wt` disables
   CCC's `_coordination_watcher`; WT's daemon does all auto-nudging on the
   same chat files. Default off.
2. Stage 2: CCC `/api/ask` and `/api/inject-input` optionally proxy to `wt`
   for headless-reachable sessions; CCC remains the delegate for TTY.
3. Stage 3: delete CCC's duplicated watcher/queue code once Stage 1+2 soak.

## Engine matrix (honest, v1)

| Engine | send/ask | chat member |
|---|---|---|
| claude (WT worker) | native fifo | yes |
| claude (any session) | native resume | yes |
| claude (live TTY) | delegate (CCC) | yes |
| codex / gemini / cursor / antigravity / hermes | delegate (CCC) | yes via delegate |

Codex one-shot workers cannot receive mid-run messages; sends to them queue in
the outbox until a live channel exists or go through the delegate.

## Explicitly out of scope (v1)

Cross-machine federation (delegate URL is the seam, nothing more), an MCP
server (skill-first per docs/agent-discovery.md), LLM-driven chat moderation
(targeting stays deterministic), porting the AppleScript transport into WT.

## Test plan

Same isolation pattern as existing tests (env overrides for every new state
file). New test modules: `tests/test_messages.py` (resolve/adapters/outbox/
backoff/dead-letter, fake delegate via local HTTP handler, ask timeout and
correlation on synthetic logs), `tests/test_chats.py` (format round-trip
against a fixture copied from a real CCC chat file, targeting matrix, nudge
tick cadence/cap/close), plus plist and daemon ticks untouched assertions.

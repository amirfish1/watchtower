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
launchd-supervised daemon. WT is fully standalone: without CCC installed,
every claude session is reachable natively (fifo for WT workers, headless
resume for everything else, with a busy-hold so a session that is actively
mid-turn gets its message when it goes idle). CCC, when present, is an
optional accelerator delegate: instant injection into live TTY tabs via its
AppleScript transport, and reach into foreign engines (codex, gemini, cursor)
whose resume machinery lives in CCC today. Everything CCC currently
duplicates (chat watcher thread, pending inputs queue, targeting logic)
migrates to WT behind a flag.

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

`wt agents` lists the registry plus live WT workers, with liveness where
known. It is an address book, not a session browser: it never scans the
transcript archive, so the thousands of dormant sessions on disk stay out of
it unless explicitly registered (browsing history remains CCC / Total Recall
territory). `wt agent set-name` is accepted as an alias for `register`, and
re-registering an existing name just repoints it.

## Delivery adapters (ordered fall-through)

1. `fifo`: target maps to a live WT worker; reuse `write_to_worker_fifo`
   (existing stream-json user line). Cheapest, zero new code.
2. `resume`: `claude -p --resume <sid> --input-format stream-json
   --output-format stream-json` headless, FIFO stdin, log to
   `~/.watchtower/logs/msg-<sid8>-<ts>.log`. Port of the `wt answer`
   machinery, generalized. Guarded by a busy check: if the target session
   looks actively mid-turn (its transcript `.jsonl` under
   `~/.claude/projects/*/<sid>.jsonl` was modified within the last 120s,
   `$WATCHTOWER_BUSY_WINDOW_S`), do NOT fork a parallel resume; hold the
   message in the outbox and let the daemon deliver it once the transcript
   goes quiet. This is the native, CCC-free path for live TTY sessions:
   delivery on idle instead of keystroke injection.
3. `delegate` (optional, only when configured or auto-detected): POST to a
   delegate HTTP endpoint for instant delivery WT cannot do natively
   (keystroke injection into a live TTY tab, codex/gemini/cursor engines).
   Default delegate: CCC at `http://127.0.0.1:<port>/api/inject-input` with
   the port read from `~/.claude/command-center/port.txt` when present.
   Override via `$WATCHTOWER_DELEGATE_URL`; disable via
   `$WATCHTOWER_DELEGATE_URL=off`. WT must behave correctly with no delegate
   at all; the delegate only upgrades latency and engine reach. The seam is
   also the future federation point (remote WT instances).

If every adapter fails or is deferred by the busy check, the message lands in
the outbox.

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
| claude (idle session) | native resume | yes |
| claude (live TTY, no CCC) | native busy-hold, resume on idle | yes (nudge lands on idle) |
| claude (live TTY, CCC present) | delegate upgrade: instant keystroke inject | yes, instant |
| codex / gemini / cursor / antigravity / hermes | delegate only (v1); native adapters later | yes via delegate |

Codex nuance (per the canonical engine study, see below): `codex exec`
one-shots, which is how WT spawns codex workers today, run with stdin closed
and cannot receive mid-run input; sends to them hold in the outbox or go via
delegate. But codex the engine IS steerable mid-run through its app-server
(JSON-RPC over stdio: `thread/resume`, `turn/start`, `turn/steer`, and an
experimental native `thread/inject`). A WT-owned `codex app-server` subprocess
is therefore the designated future native codex adapter, removing the delegate
dependency for codex entirely. Without any delegate, WT covers 100% of claude
sessions with at-most-idle-lag delivery.

Shared knowledge: the cross-engine feasibility ground truth (headless vs
terminal vs app-server, inject/steer/reap per engine) is maintained as
`docs/engine-capability-matrix.md` in this repo, distilled from the CCC
session-states study. Update it when an engine's surface changes; design
decisions here defer to that matrix.

## Explicitly out of scope (v1)

Cross-machine federation (delegate URL is the seam, nothing more), an MCP
server (skill-first per docs/agent-discovery.md), LLM-driven chat moderation
(targeting stays deterministic), and a native macOS keystroke (AppleScript)
adapter. That last one is deferred, not rejected: there is no architectural
reason WT cannot inject into a live terminal the way CCC does; the hard part
is session-to-tty discovery, and busy-hold covers the need in v1. When built,
it slots into the adapter chain between resume and delegate.

## Test plan

Same isolation pattern as existing tests (env overrides for every new state
file). New test modules: `tests/test_messages.py` (resolve/adapters/outbox/
backoff/dead-letter, fake delegate via local HTTP handler, ask timeout and
correlation on synthetic logs), `tests/test_chats.py` (format round-trip
against a fixture copied from a real CCC chat file, targeting matrix, nudge
tick cadence/cap/close), plus plist and daemon ticks untouched assertions.

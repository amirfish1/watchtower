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

## Division of labor

The canonical WT / CCC / Codex integration contract lives in
`docs/ccc-watchtower-boundary.md`.

In one sentence: WT owns durable control-plane semantics (send/ask, outbox,
receipts, chats, queue workers, retry policy), while CCC owns local desktop
transports, the user-facing UI, transcript rendering, and the managed Codex
app-server broker when CCC is running.

WT is fully standalone: without CCC installed, claude sessions are reachable
natively (fifo for WT workers, tty where supported, headless resume for
dormant sessions, with busy-hold so a session that is actively mid-turn gets
its message when it goes idle), and messages WT cannot deliver are parked in
the outbox. CCC, when present, is a delegate/broker for local desktop-only
paths and for managed Codex app-server access. WT -> CCC delegate requests are
stamped `origin=wt`; CCC must not route those requests back into `wt send`.

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
   `wt agents register <name> --session <uuid> [--engine claude] [--cwd path]`.
3. Raw session UUID (or unique prefix >= 8 chars).

Reachability vs listing are two different questions:

- Reachable (send/ask targets): ANY session on disk, by UUID or unique
  prefix. Process liveness is irrelevant; the resume adapter can wake any
  dormant transcript, and busy-hold handles the mid-turn case.
- Listed (`wt agents`): the useful working set, not all of history. Three
  sources merged: (1) the named registry, always; (2) WT workers; (3)
  auto-discovered recent sessions, transcripts under ~/.claude/projects whose
  mtime is within the last 3 days ($WATCHTOWER_AGENTS_WINDOW_DAYS). The
  window scan is a stat-only pass, run only inside the `wt agents` command
  (and UUID-prefix resolution), never in the daemon tick, so the
  thousands of older transcripts cost nothing. Full-history browsing remains
  CCC / Total Recall territory.

`wt agents set-name` is accepted as an alias for `register`, and re-registering
an existing name just repoints it (`wt agent ...` survives as a hidden alias).

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
   delegate HTTP endpoint for instant delivery WT cannot do natively or for
   brokered local resources (for example CCC's managed Codex app-server).
   Default delegate: CCC at `http://127.0.0.1:<port>/api/inject-input` with
   the port read from `~/.claude/command-center/port.txt` when present.
   Override via `$WATCHTOWER_DELEGATE_URL`; disable via
   `$WATCHTOWER_DELEGATE_URL=off`. WT must behave correctly with no delegate
   at all; the delegate only upgrades latency and engine reach. Every WT
   delegate request includes `origin=wt`, which is the loop-prevention marker
   CCC honors by avoiding a callback into `wt send` for the same request. The
   seam is also the future federation point (remote WT instances).

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
wt agents [--json] / wt agents register|set-name|rm <name>
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
`--notify-webhook` mirrors `wt wait`. Standalone Codex targets return an
explicit unsupported result for `ask`; synchronous Codex questions require a
CCC/delegate broker that owns response correlation.

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
adapter chain above, so a chat can mix WT workers, TTY sessions, Codex sessions
(CCC managed broker first, WT private fallback only when standalone), and
delegate-only engines.

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
3. Stage 3: delete CCC's duplicated watcher/queue code once Stage 1+2 soak —
   gated by the soak gate below. Not before.

### Stage 3 soak gate (WT-57)

Stage 2 once soaked *broken* for a full day (2026-07-02): wt's resume adapter
spawned `claude --resume` without the session cwd, every delegated delivery
died at boot with "No conversation found" while the outbox reported ok, and
CCC's native path was the only recovery (fixed in watchtower `13a2de6` +
CCC `58ca751`). "It ran for a while with no complaints" is therefore not
soak. The deletion is authorized only when ALL of:

1. **Receipts are the ground truth (WT-77 — shipped).** "Delivered" means
   *verified landed* against the target transcript, not "the adapter said
   ok". Instrument: `wt receipts stats` (7-day window by default).
2. **7 consecutive days of verified wt-mediated delivery**: CCC running with
   `CCC_MESSAGING_BACKEND=wt` the whole time, and `wt receipts stats` for
   that window showing `landed >= 50` and `lost == 0` (silent loss is the
   failure mode that motivated this gate).
3. **Explicit owner sign-off** recorded on the Stage 3 ticket. Meeting the
   numbers does not self-authorize the deletion.

Prerequisites before the clock can even start: WT-76 (rebucket) and WT-78
(delegate origin-marker) — wt must own delivery end-to-end for the window to
measure the right thing.

What Stage 3 deletes when the gate opens (CCC `server.py`, by name — line
numbers drift): `_register_coordination`, `_coordination_watcher`,
`_start_coordination_watcher` (the `CCC_CHAT_ORCHESTRATOR=wt` gate),
`_group_chat_nudge` targeting logic, the pending-inputs drain
(`PENDING_INPUTS_FILE`, `_load_pending_inputs`/`_save_pending_inputs`,
`_get_queued_events_for_session`), leaving CCC pure client + TTY delegate.

## Engine matrix (honest, v1)

| Engine | send/ask | chat member |
|---|---|---|
| claude (WT worker) | native fifo | yes |
| claude (idle session) | native resume | yes |
| claude (live TTY, no CCC) | native busy-hold, resume on idle | yes (nudge lands on idle) |
| claude (live TTY, CCC present) | delegate upgrade: instant keystroke inject | yes, instant |
| codex (CCC present) | send + ask through CCC delegate/broker | yes |
| codex (standalone WT) | send through private `codex app-server`; ask unsupported | yes |
| gemini / cursor / antigravity / hermes | delegate or engine-specific CCC path | yes via delegate/broker |

Codex nuance (per the canonical engine study, see below): `codex exec`
one-shots, which is how WT spawns codex workers today, run with stdin closed
and cannot receive mid-run input. User-visible Codex threads should be driven
through the managed app-server broker when CCC is running; WT starts its own
private stdio app-server only as the standalone fallback. Codex the engine is
steerable mid-run through app-server JSON-RPC (`thread/resume`, `turn/start`,
`turn/steer`, and experimental `thread/inject`), but the ownership rule matters
more than the method list: avoid two app-server owners for one thread.
Without any delegate, WT covers claude sessions with at-most-idle-lag delivery
and Codex sends through its private fallback. A successful private app-server
send requires a receipt backed by the thread's durable Codex rollout; an
app-server acknowledgement without that proof path is reported as failure.

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

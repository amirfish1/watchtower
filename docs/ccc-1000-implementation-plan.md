# CCC-1000 — implementation plan

Delivery-semantics rework across CCC + WatchTower. Design rationale and full
evidence live in `docs/Pulled every mode branch and interrupt c.md`; this file is
the build order. Written 2026-08-30.

## The model

Three delivery verbs, one control verb:

| Verb | Kind | Lands | In-flight work |
|---|---|---|---|
| `engine_default` | delivery | whatever the engine natively does | engine-defined |
| `queue` | delivery | after the turn ends | untouched |
| `steer` | delivery | at the next safe seam | untouched |
| `abort` | **control** | nothing delivered | turn ends early |

Orthogonal fields (today smeared into `mode`):

```
on_busy:                  hold | drop | reject     (+ expire_after: <duration>)
position:                 back | front             (replaces force_queue / send_queue)
abort_first:              bool                     (replaces the fused destructive "steer")
answers_pending_question: bool                     (today's mode="answer", server.py:60626)
await_reply:              bool                     (what makes messages.ask() a special case)
```

**Non-goals.** Not renaming `mode` on the wire in phase 1. Not touching Codex/ACP
native steer. Not building a tool-call seam for headless Claude (cosmetic — see
"Why #6 aborts" in the design doc).

## Open decisions — RESOLVED 2026-08-30

| # | Decision | Resolution |
|---|---|---|
| D1 | `steer` on a transport with no seam (#9 ACP) | **degrade + report.** A *seam* is a point where input can be inserted without cutting anything off. Codex has one (end of tool call); Claude's stream-json exposes none; ACP exposes only `session/cancel`, i.e. wait or destroy, nothing between. So ACP can only emulate `steer`, and the result contract must say so. |
| D2 | Slash commands over UDS | **execute them; intercept only what mutates CCC-tracked state.** Per `server.py:60494`, a slash command written to the FIFO as user text *does* execute — "Claude executes it, but CCC then has no idea the session was reset". `/compact` and `/clear` are intercepted because they change session identity. `/model`, `/cost` etc. are harmless and should just run. |
| D3 | Does WT get its own `abort`? | **yes, support it.** |
| D4 | Is Grok `queue` like Kimi? | **yes — settled by code, not testing.** `_terminal_queue_waits_for_active_acp` (`server.py:44362`) gates on `status["kind"] == "acp"`, generic across harnesses. Kimi and Grok share one path and cannot differ. |

## The real slash-command bug (revised)

Not "commands silently become text" — it is narrower and stranger:

**The same slash command behaves three different ways depending on transport.**
Taking `/compact` as the worked example:

| Path | Outcome |
|---|---|
| CCC API (`/api/inject-input`) | intercepted at `server.py:60475`, returns `compact_session_context()` **before** the UDS attempt at `:60553` — works, and CCC re-keys its own state |
| Direct FIFO write (WT worker with a fifo) | lands as stream-json user text with a leading slash, so **Claude executes it** — but CCC never observes the change and the dashboard goes stale (`server.py:60494`) |
| UDS peer socket (native SendMessage, or WT `peer_uds`) | wrapped by `peer_uds.wrap()` into `<cross-session-message>`, no leading slash, **does not execute** — inert text with a `delivered` receipt |

So `/compact` never reaches UDS *through CCC's API* — the intercept fires first.
It only gets there when something dials the socket directly.

Three transports, three outcomes: correct, correct-but-unobserved, and silent
no-op. Phase 4 must make all three agree, not two.

## Phase 1 — the result contract (CCC, no behaviour change)

Highest value, lowest risk. Most of the day's confusion was callers not being
told what happened.

Add to every `/api/inject-input` response:

```json
{"requested": "steer", "effect": "delivered", "aborted": false,
 "landed": "next_seam", "transport": "uds",
 "reason": "target was idle; nothing to interrupt"}
```

- Touch: `_inject_text_into_session` (`server.py` ~60470-60900), every `return`
  path, plus `/api/inject-input` at `:75775`.
- **Prerequisite:** inventory every consumer of the current response shape
  before adding fields — `static/app.js` (~20 `fetch('/api/inject-input')` sites),
  `watchtower/messages.py::_deliver_delegate` (`:1348`), the curl report footer,
  the ccc-orchestration skill. Additive only; do not rename existing keys.
- Verify: existing callers keep working unchanged; new fields present on all paths.

## Phase 2 — verb vocabulary at the API (CCC)

- Accept the new verbs at `:75964` alongside today's `answer`/`send`/`steer`.
- Map legacy: `send` → `engine_default`, `steer` → `steer` + `abort_first: true`
  (preserves today's exact behaviour), `answer` → `engine_default` +
  `answers_pending_question: true`, `send_queue`/`force_queue` → `queue`.
- **#6 and #18 default to `engine_default`.** #6 behaves correctly today and must
  not change.
- Resolve D4 first: confirm ACP/Kimi/Grok defaults empirically, not by assumption.

## Phase 3 — WT consolidation (the real prize)

WT has four delivery implementations; only `messages.send()` uses the adapter
chain. Collapse to one:

```python
messages.deliver(target, text, verb, *, on_busy="hold", expire=None,
                 position="back", await_reply=False, abort_first=False)
```

| Collapse | From | Notes |
|---|---|---|
| `ask()` (`messages.py:1805`) | own fifo→resume→delegate chain | becomes `await_reply=True`; gains outbox + tty/codex/gemini/antigravity adapters |
| `_deliver_release_instruction()` (`workers.py:1092`) | own uds→fifo→send chain | disappears; its UDS-first preference *is* `verb="steer"` |
| `notify_workers()` (`workers.py:767`) | raw fifo, no fallback | becomes a loop over `deliver(verb="steer", expire=30)` — **fixes WATCHTOWER-14 for free** |
| `write_to_worker_fifo()` (`:593`) | raw | **keep as-is** — spawn-time (#17) has no other channel yet |

Prefer UDS for `verb="steer"`: it is steer by construction and cannot interrupt.
WT already vendors `peer_uds.py`.

**Sender identity.** `wt send` is invoked by both humans and agents and the
process looks identical. Use `$CLAUDE_CODE_SESSION_ID` / `$CODEX_THREAD_ID` —
present for agents, absent in a human shell — the same signal `--submitter`
already defaults from. Needed for the UDS eligibility rule (agent-origin only).

## Phase 4 — slash-command guard (CCC)

`/compact` and `/clear` return unconditionally at `server.py:60487` / `:60498`,
above the UDS attempt at `:60553`, so they are safe. **Every other slash command
is not**: `slash_command` (`:60472`) is only consulted for Codex (`:60562`) and
only after UDS has run. `/model`, `/cost`, `/resume`, `/code-review` and custom
skills arrive as literal text wrapped in `<cross-session-message>`, with a
transcript-confirmed `delivered` receipt and no error either side.

Fix per D2. Whatever is chosen, the sender must stop receiving a success receipt
for something that did not execute.

## Phase 5 — sync warnings

`worker_turn_open()` (WT, `workers.py:543`) and `_headless_log_turn_open()` (CCC,
`server.py:53372`) are character-identical and maintained in parallel with **no**
sync note — unlike `peer_uds.py` / `ccc_peer_uds.py`, which carry an explicit
"update together" header. Add the same warning to both.

## Test plan

- Phase 1: response-shape assertions on every return path; existing consumer
  tests unchanged.
- Phase 2: legacy `mode` values map to identical observable behaviour (this is
  the regression risk — #6 especially).
- Phase 3: WT suite is 998 tests today; `notify_workers` fallback for a
  fifo-less worker is the new case (WATCHTOWER-14).
- Phase 4: a slash command from an eligible source must not report success.

## Risks

1. **Phase 2 regressions on #6.** It works today; any change is a downgrade.
   Mitigate by mapping `steer` → `steer` + `abort_first: true` exactly.
2. **Response-shape breakage.** Additive only, and inventory consumers first.
3. **Two-repo drift.** Phases 2 and 3 must land together or WT's delegate sends
   verbs CCC rejects. Ship CCC first (it accepts both vocabularies), WT second.
4. **The watcher daemon caches code.** `ai.watchtower.watcher` must be
   kickstarted after any WT change or the fix sits inert in a running process.

Pulled every mode branch and interrupt call site instead of guessing. Full picture, achi — the whole confusion this session came from "steer" meaning two different things.

**The one real interrupt primitive:** `_write_stream_json_interrupt` (server.py:52744). It aborts an in-flight tool, ends the turn, keeps the process alive. It fires from exactly ONE place: `_inject_text_into_session`. Anything that reaches that function CAN interrupt. Anything that doesn't, can't — no matter what "priority" label you slap on the JSON.

## Use-case map

| # | Use case | From → To | Path today | Reaches `_inject_text_into_session`? | Real interrupt today? | Should it interrupt? |
|---|---|---|---|---|---|---|
| 1 | WT ticket notify (claim/close/block, FYI) | WT queue → ticket submitter/subscriber session | `queue.py:_notify_ticket_event` → `messages.send` | No (WT's own fifo/tty/resume adapters) | No | **No** — pure notification |
| 2 | `wt send` (ad-hoc chat) | User at CLI → any target session | `messages.deliver`, mode=`send` | No | No | **No** — normal chat semantics |
| 3 | `wt steer` (user says "redirect NOW") | User at CLI → live WT worker session | `messages.deliver`, mode=`steer` → WT's own fifo/tty write | **No, unless** target is a CCC-owned session and delegate falls through to `/api/inject-input` | **Only sometimes, by accident of routing** | **Yes** — this is the one case the whole feature name promises and mostly doesn't deliver |
| 4 | CCC ticket-comment / answered-blocked-question (server.py:2515,2543) | CCC server → idle/blocked worker session | `wt_messages.send(mode="steer")` | Same as #3 — depends on delegate fallthrough | Irrelevant — target is already idle/blocked | **No** — nothing running to abort, "steer" here really means "jump the queue," not "abort" |
| 5 | `wt ask` | Caller (CLI/agent) → target session, reply back to caller | `ask_session_via_resume` (UDS→keystroke-tail→headless resume) | No | No | **No** — you want an answer without derailing current work |
| 6 | CCC dashboard textbox while session is actively running (headless, no tty, mode=steer) | User in CCC dashboard → live CCC-owned headless session | `_inject_text_into_session` direct | **Yes** | **Yes — real, already shipped** | **Yes**, and it works |
| 7 | Esc/Kill button | User in CCC dashboard → live CCC-owned headless session | `_interrupt_claude_headless_local` | Yes | Yes | **Yes**, already shipped |
| 8 | Codex steer | User in CCC dashboard → live Codex session | `resume_session_codex(steer=True)` | N/A — Codex-native | Yes, but coarser (cancels turn, doesn't resume mid-tool) | **Yes**, already shipped |
| 9 | ACP (Grok/Kimi) steer while busy | User in CCC dashboard → live ACP session | `session/cancel` + resend | N/A — ACP-native | Yes, but coarsest (kills whole turn, fresh turn after) | **Yes**, already shipped |
| 10 | CCC→foreign session outbound (`_try_uds_peer_delivery`, used by ask + cross-model bridge) | CCC → foreign (non-CCC-owned) Claude session | sets `priority: "now"/"next"` in JSON body, delivered via receiver's native `SendMessage` inbox | No — foreign process, no FIFO CCC controls | **Impossible**, structurally — confirmed by Anthropic's own docs, no such primitive exists on that transport | N/A — can't build what the receiving harness doesn't expose |
| 11 | CCC inbound peer socket (Slice 3, another session → CCC) | Foreign peer session → CCC-owned session | `_ccc_peer_handle_connection` — only routes ask-replies + report envelopes | No — general chat frames get logged `CCC-PEER-UNROUTED` and dropped | No, not even attempted | **No, by default** — a peer message hijacking your live dashboard session mid-tool would be a bad surprise; should stay queued unless explicitly tagged urgent |
| 12 | Worker child → parent completion report | Spawned worker child → parent session | curl footer or `SendMessage` | Sometimes (report envelope path) | N/A | **No** — parent's idle/polling anyway |

## The actual bug in the model

**"steer" is overloaded.** It means two unrelated things depending on caller:
- "abort the running tool, right now" (#6, #7, #8, #9 — genuinely wired)
- "put my message ahead of the queue, but the target's idle anyway" (#4 — doesn't need real interrupt, just got the same mode string)

And #3 (`wt steer`, the flagship use case) only gets real interrupt **by accident**, when routing happens to fall through WT's delegate adapter into CCC's `/api/inject-input`. Any WT worker not spawned by CCC's dashboard gets zero interrupt from `wt steer` — it's silently just `send` with a label.

## Flag proposal

Split `mode` into two independent axes instead of one overloaded string:

```
priority: queue | jump      # position in the delivery queue (today's real meaning for #4)
interrupt: none | abort     # actually abort a running tool, where the engine supports it
```

`interrupt: abort` should return an explicit `unsupported` result code (not silently downgrade to queued) when the target transport can't do it — UDS-to-foreign-session (#10) and WT's non-delegate adapters (#3 today) are exactly the cases that currently fail silent instead of failing loud.

This is where I'd stop and ask: want me to spec this as the actual rework doc (replacing "steer" call sites one by one with the two-axis flag), or do you want the Opus-5/Codex outside review to weigh in on the flag design first before anything gets written?

<session-state>
DID: Mapped every cross-session-messaging use case (12 total) against whether it reaches the one real interrupt primitive (_write_stream_json_interrupt) and whether it should.
INSIGHT: "steer" is overloaded — it means "abort running tool" in 4 places (already correctly wired) and "jump the queue, target's idle anyway" in 1 place (#4), and the flagship `wt steer` case only gets real interrupt by accident of routing, never guaranteed.
NEXT_STEP_USER: Decide whether to spec the two-axis (priority/interrupt) flag as the rework doc now, or send it to Opus-5/Codex review first.
</session-state>
# Engine capability matrix

Status: living reference, distilled from a canonical cross-engine session-state
study (originally written for Claude Command Center, generalized here). This is
the ground truth `docs/messaging-design.md` defers to for what each AI
coding-agent engine can and cannot do while a session is running: inject text
mid-turn, steer a live turn, or be torn down safely. WT's delivery adapters
(fifo, resume, delegate) and any future native adapter should agree with this
matrix, not the other way around.

Cells marked **unverified** are not settled by reading the engine's own code
or docs and need an empirical test before you build against them.

## 1. The two-axis taxonomy

Engines are not four unrelated buckets. Two independent axes decide what
"inject", "steer", and "reap" even mean for a given session:

- **Process model**: *CLI process* (its own OS process, therefore reapable as
  a process tree) vs. *GUI / app-server* (a conversation hosted inside one
  long-lived server process, so there is no per-session process to kill; the
  only verbs are steer/stop-turn via RPC, or graceful-quit of the whole app).
- **Attachment** (CLI only): *headless* (no TTY, driven programmatically) vs.
  *terminal* (a TTY, a human is actively in it).

So "CLI" is headless union terminal, two sub-modes of one process model, and
"GUI / app-server" is the alternative process model entirely. That gives three
practical groups, not four:

## 2. Group 1: Headless CLI (no TTY, programmatic, reapable as a tree)

| Engine | How it runs | Inject while running? | Reap (SIGTERM the tree)? |
|---|---|---|---|
| Claude | `claude -p` stream-json, stdin FIFO | yes, write to the FIFO | yes, clean |
| Codex | `codex exec` one-shot | no (queues for the next turn) | yes (aborts the turn) |
| Cursor | `cursor --print` one-shot | no (queues) | yes |
| Antigravity | `agy --conversation <id> -p` one-shot | no (queues) | yes |

This is WT's `fifo` and `resume` adapter territory for Claude: a live worker's
FIFO takes text mid-turn, and a dormant session can be woken headless via
`claude -p --resume <sid>`. The other engines' one-shot CLIs cannot receive
mid-run input at all, so a WT `send` to one of those, while it is actively
executing, has nowhere headless-native to land and needs the delegate adapter
or must wait for the process to exit.

## 3. Group 2: Terminal CLI / TUI (a human is in it, protect, don't kill)

| Engine | How it runs | Inject while running? | Reap? |
|---|---|---|---|
| Claude | `claude` / `claude --resume` in a real TTY | keystroke injection into the terminal app (platform-specific, e.g. AppleScript on macOS) | no, it's a human |
| Codex | interactive `codex` TUI | a keystroke path could exist in principle; not wired anywhere | no |
| Antigravity | `agy ui` TUI (same surface a GUI "launch" action opens) | a keystroke path could exist in principle; not wired | no |
| Cursor | unverified whether an interactive TUI exists at all | unverified | no if it does |

A generic terminal keystroke injector is not inherently Claude-specific, it
could in principle drive any terminal CLI/TUI, but today nothing in this
group has a native WT delivery path. WT's `resume` adapter never targets a
terminal session directly (that would race the human); this whole group is
"protect, don't kill" and, for WT v1, reachable only through an optional
delegate that owns a real keystroke-injection transport.

## 4. Group 3: App-server back end (hosted, no per-session process, graceful quit)

| Engine | Where sessions live (survive quitting the app) | Inject while running? | Stop verb |
|---|---|---|---|
| Codex (an externally-owned `codex app-server`) | on-disk session/rollout files | yes, JSON-RPC over stdio (`thread/resume` + `turn/start`) | app-server-managed; no confirmed "stop this turn without quitting" verb |
| Cursor.app | app-local state store | no RPC path found | graceful quit only (must flush state, never SIGKILL) |
| Antigravity (`agy ui` + its language server) | app-local session directory | yes, HTTPS RPC to a localhost port the app's language server listens on | graceful quit |

(Claude is CLI-only, it has no app-server form.)

### The codex app-server in detail

Codex's app-server is a persistent subprocess speaking JSON-RPC 2.0 over its
own stdin/stdout: `{"jsonrpc":"2.0","id":N,"method":...,"params":...}\n` in,
matching responses read back by id. The methods relevant to messaging:

- `thread/resume`: attach to an existing on-disk thread by id.
- `turn/start`: start a new turn with fresh input.
- `turn/steer`: inject text into the *currently running* turn. This is the
  closest Codex analog to Claude's FIFO write: genuine mid-run injection, not
  a queue-for-next-turn.
- `thread/inject` (**experimental**): a native "inject items" call, cleaner
  in principle than spawning a fresh `codex exec` per message. Exact params
  are not nailed down; the surface is negotiated via an `experimentalApi`
  capability flag at connection init.
- `thread/compact` (**experimental**): real compaction over RPC, unlike
  Claude where compaction requires driving an interactive TUI because `/compact`
  is a client-side slash command, not message content a headless process can
  execute.
- Also present on the schema, not yet exercised anywhere: `thread/rollback`,
  `thread/fork`.

Implication for WT: **a WT-owned `codex app-server` subprocess is the
designated future native codex adapter.** It removes the need to delegate
codex sends anywhere else, because `turn/steer` gives WT the one thing the
one-shot `codex exec` path structurally cannot: injection into an
already-running turn. Building this adapter slots it into the fall-through
chain between `resume` and `delegate` (or replaces `delegate` for codex
entirely), the same seam the deferred native-keystroke adapter is reserved
for Claude terminals.

### Codex process zoo caution

A codex app-server subprocess can be owned by up to three different parties
at once on one machine: a caller-owned instance (spawned lazily to hold a
private stdio pipe), the GUI app's own instance (dies when the GUI quits),
and a standalone long-lived daemon started by the OS-level service manager
(survives quitting the GUI). These do not share state automatically. If WT
and a GUI app both `thread/resume` the same on-disk thread id, each holds its
own in-memory copy from the moment of resume; concurrent `turn/start` calls
from both sides can interleave, clobber, or diverge (**unverified**, but
treat two live resumers of one thread id as a hazard the same way a Claude
session with two concurrent live processes is a hazard, see below), and
never assume the daemon that quitting a GUI app kills is the only app-server
running.

## 5. The concurrency hazard

Any engine's transcript/rollout file can, in principle, be resumed by two
independent processes at once (a WT-spawned headless resume plus a human's
live terminal, or two independent app-servers on one thread id). For Claude
specifically, a controlled test resuming the same session from two
concurrent headless writers found the on-disk transcript stays strictly
append-only under that condition: line count only grew, nothing was
clobbered or truncated by starting, restarting, or killing either writer.
That result does not extend to the terminal-attached case (a real TTY's
write/exit path is unverified) or to any other engine. WT's busy-hold check
(hold a message in the outbox rather than forking a parallel `resume` while a
session's transcript was written within the last
`$WATCHTOWER_BUSY_WINDOW_S`) exists specifically to avoid creating this
hazard in the first place, not to recover from it after the fact.

## 6. WT adapter roadmap

| Adapter | Status | Covers |
|---|---|---|
| `fifo` | implemented | live WT workers (Claude, stream-json stdin) |
| `resume` with busy-hold | implemented | any dormant/idle Claude session by UUID; holds in the outbox instead of racing a session that looks mid-turn |
| `delegate` (optional) | implemented | anything WT cannot do natively today: live-terminal keystroke injection, and codex/cursor/antigravity engines, via a configured HTTP delegate |
| native codex app-server adapter (`turn/steer`, `thread/inject`) | future | true mid-run codex injection without a delegate |
| native tty keystroke adapter | future, deferred not rejected | live-terminal Claude sessions without a delegate; the hard part is session-to-tty discovery, which the busy-hold `resume` path already covers for the common case |

Building either future row removes that engine's dependency on the delegate
without changing the adapter chain's shape: both slot in as an additional
adapter, in the same ordered fall-through `deliver()` already uses.

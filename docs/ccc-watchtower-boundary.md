# WT / CCC / Codex ownership boundary

Status: canonical policy, 2026-07-09.

This document is the integration contract between WatchTower (WT), Claude
Command Center (CCC), and Codex. It exists to prevent two classes of bugs:
duplicate writers against one session, and delivery paths that report success
before a message has either landed or been durably queued.

## Ownership

WT owns durable control-plane semantics:

- addressing: worker ids, registered agents, raw session ids, and queue refs;
- message delivery intent: `wt send`, `wt ask`, chats, nudges, outbox retry,
  TTL, dead-letter state, and receipts;
- queue-worker lifecycle: queue config, worker records, claim/close/reap, and
  worker-spawn policy;
- durable ledgers under `~/.watchtower`, including agents, workers, outbox,
  receipts, and queue state.

CCC owns local desktop and UI surfaces:

- HTTP/UI entry points such as `/api/inject-input`, conversation panes,
  status pills, transcript rendering, and user-visible spawn controls;
- local desktop transports that WT cannot own portably, such as terminal
  keystroke injection through a running desktop app;
- the managed Codex app-server connection when CCC is running, including live
  turn status, notifications, interrupt/compact controls, and Codex thread UI
  visibility;
- local conversation archives and rendering caches. These are presentation
  state, not the durable WT delivery ledger.

Codex durable truth is its own on-disk thread/rollout store. App-server
notifications are useful for fast UI and receipts, but they are not canonical
unless explicitly persisted and reconciled with the rollout.

CCC persists a compact last-known notification snapshot at
`~/.claude/command-center/codex-app-server-state.json`. Schema version 1 stores
`{authoritative:false, source, transport, updated_at, threads:{...}}`, where
each thread entry may include status, active/completed turn ids, last event,
activity timestamps, and token usage. This powers live UI, active-turn hints,
startup/turn confirmation, and telemetry. WT may read it as a hint, but WT must
not treat it as delivery proof or durable history; receipts and rollout growth
remain the verification path.

CCC and WT also share a reconciliation index at
`~/.claude/command-center/codex-thread-registry.json` (override:
`CCC_CODEX_THREAD_REGISTRY` / `WATCHTOWER_CODEX_THREAD_REGISTRY`). Schema
version 1 is `{authoritative:false, threads:{thread_id:{...}}}`. The key is
always the Codex thread id, and each record may merge:

- identity: `thread_id`, `engine:"codex"`, `cwd`, `repo_path`, `model`,
  `reasoning_effort`;
- ownership: `transport_owner` (`ccc-managed-app-server`,
  `wt-private-app-server`, `ccc-codex-exec`, `wt-codex-exec`) and `transport`;
- visibility: `user-visible`, `worker`, `registered-agent`, or `unknown`;
- labels and routing: `title`, `name`, `parent_session_id`, `report_to`;
- WT facts: `worker_id`, `queue`, `ref`, plus a nested `wt` map;
- CCC facts: `spawn_id`, spawn log, prompt preview, worktree metadata, plus a
  nested `ccc` map.

This registry is authoritative for reconciliation keys and merge policy, not
for transcript content or delivery proof. Codex rollout/state remains history;
WT receipts/outbox remain delivery truth; CCC notification snapshots remain
live hints. Upserts merge by thread id and preserve higher-precedence
visibility/transport-owner facts, preventing duplicate thread records when a
CCC-spawned Codex thread later appears as a WT target or queue worker.

## Call Direction

CCC calls WT when it wants durable delivery semantics:

- dormant or unknown sessions where CCC has no safe direct live transport;
- queue-worker messages and inter-agent reports;
- any flow that should survive CCC restart via outbox retry;
- any flow that needs a WT receipt or outbox/dead-letter state.

CCC does not call WT for a request that arrived from WT, or for an explicit
local-only operation. Use `origin=wt` and `skip_wt=true` as loop guards.

WT calls CCC only as a delegate/broker:

- desktop-only transports, such as a live terminal tab that WT cannot type into
  directly on the current platform;
- engines whose native WT adapter is absent or deliberately delegated;
- managed Codex app-server access when CCC is the local broker and the target
  is a user-visible Codex thread.

WT remains standalone. If CCC is absent or delegation is disabled, WT must
either deliver through a native adapter or park the message in the outbox.

## Loop Prevention

`origin=wt` is mandatory on WT -> CCC delegate requests. CCC must treat it as
an instruction to avoid calling back into `wt send` for the same delivery.

`skip_wt=true` is the CCC-side local-delivery guard. It means "do not proxy this
request to WT"; CCC may still use its own local transports.

`no_queue` only controls durable queueing. It must not bypass the loop guard.
If delivery cannot be proven and queueing is disabled, return a failed result
instead of reporting delivered.

The route matrix is:

| Caller | Target path | Required guard | On failure |
|---|---|---|---|
| CCC direct local transport | CCC only | `skip_wt=true` internally | return direct failure |
| CCC durable delivery | `wt send` | no `origin=wt` | WT outbox/receipt rules |
| WT native adapter | WT only | none | try next adapter or outbox |
| WT delegate to CCC | CCC `/api/inject-input` | `origin=wt` | fail back to WT outbox |
| CCC handling WT delegate | CCC only | received `origin=wt` | return success/failure; no WT callback |

## Codex Policy

Codex has two distinct lifecycles.

User-visible Codex conversations should be hosted through the managed
app-server path when available. The preferred fresh-session primitive is:

1. `thread/start` through the managed app-server with cwd, workspace roots,
   model, approval policy, sandbox policy, and optional config;
2. `thread/name/set` for the user-visible title;
3. `turn/start` with the initial prompt and any attached images.

CCC owns that path because it already owns the local managed app-server
transport, Codex thread visibility in the sidebar, transcript rendering,
spawn-card reconciliation, and parent/report-to UI metadata. If the app-server
is unavailable before a thread is created, CCC may fall back to `codex exec`.
After a thread is created, do not launch a second fallback worker for the same
prompt; return the error against the created thread.

That gives CCC and WT one broker for `thread/start`, `thread/resume`,
`turn/start`, `turn/steer`, status, and notifications. A private WT stdio
app-server is a standalone fallback, not the preferred path when a managed
broker is reachable.

WT queue workers are separate. They may continue to use `codex exec` for
bounded one-shot worker jobs because they are process-tracked queue runners,
not ongoing user-visible app-server threads.

WT ad-hoc bounded work, such as critique workers that report back with
`wt send`, also remains `codex exec` unless it needs a long-lived user-visible
thread. That keeps worker tracking process-based and avoids giving WT two
responsibilities: queue-runner lifecycle and Codex conversation hosting.

Never start a second writer against a Codex thread simply because another
transport is available. If the managed broker reports an active turn and the
turn is not steerable, queue the message for the next turn or return an
explicit queued/failure state.

## Reporting Success

CCC UI may say "accepted" when WT accepted a message for durable handling. It
may say "delivered" only when a direct local transport succeeded or WT returned
a receipt-backed delivery result. If WT queued a message, CCC should surface
queued/outbox state, not a delivered toast.

WT receipts are the durable delivery ledger. CCC may render them, but WT owns
their state transitions.

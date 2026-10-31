# Supervise an existing session

`wt takeover` retains an existing Claude or Codex conversation and binds it to
one local WatchTower ticket. It does not spawn a replacement worker, change its
model, or grant additional authority. Queue staffing and automatic draining
remain independent. A running `wt daemon` reconciles enrolled sessions even
without auto-spawn, a browser, or CCC list requests.

```sh
wt takeover SESSION --ticket PROJECT-42 --dry-run --json
wt takeover SESSION --ticket PROJECT-42 --engine codex --json
wt takeover SESSION --queue PROJECT --title "Finish existing work" --json
wt takeover status SESSION --json
wt takeover status --json
wt takeover cancel SESSION --json
wt takeover observe SESSION --engine claude --json
```

The session must have a readable native transcript with repository identity.
When the engine is omitted, an ambiguous identity is rejected. Registered
`@agent` names are accepted for enrollment. An existing unique binding can be
reused without `--ticket`; otherwise specify a ticket or both queue and title.
There is no default queue. GitHub mutation backends are not supported in v1.

Enrollment atomically creates/adopts a ticket, records an ambient claim and
persists the first instruction. Repeated identical enrollment returns the same
generation. **Accepted means durable intent, not delivery.** Busy sessions wait;
unknown observations pause intervention. Only a receipt proving the message
landed advances delivery to confirmed. Intents are stored on the ticket and
use the existing delivery ledger, not an additional daemon or service.

After 20 idle minutes, the supervisor can remind the same session. Three
confirmed work reminders exhaust the budget and record an owner escalation.
Delivery failures use bounded exponential backoff and do not consume work
reminders. A lost/ambiguous receipt pauses instead of blindly replaying the
instruction. Inspect `wt receipts`, the ticket and takeover status before
explicitly cancelling/re-enrolling such a session. This is not an exactly-once
transport guarantee across a crash during a native send.

`wt block` records owner questions or named dependencies. `wt answer REF TEXT`
persists an answer for the same session; missing transcripts and failed delivery
never reopen it for another worker. Cancelled supervision retains answers for
explicit re-enrollment. Cancel increments the generation, fences future sends,
preserves the ticket and its question, and suppresses automatic re-enrollment.
It cannot retract an instruction already delivered or interrupt a running turn.
The dispatch lock means cancel may wait for an in-flight transport to return.

The agent closes with normal `wt close` and all existing ownership, product,
proof and verification gates. Footer text never closes a ticket. Failed closure
or open ticket state does not make a supervised ticket available to fleet
workers. V1 does not automatically archive sessions.

## Stable integration boundary (schema 1)

All CLI responses are JSON; `--json` is accepted explicitly for adapters.
`watchtower.takeover` exposes:

- `takeover(session_id, ticket_ref='', *, queue='', title='', engine='', dry_run=False, legacy=None)`
- `status(session_id)`, `status_all()`, `cancel(session_id)`
- `reconcile(*, now=None, dry_run=False)` (called by the existing serialized reconciler)
- `replace_candidates(source, policy)` and `enroll_candidates(*, now=None)`

Enrollment/status/cancel return `schema_version`, `supervision_id`, `generation`,
`ticket`, `session: {engine, session_id, host, repo_path}`, `state`,
`delivery_state`, `reason`, `question`, `wait_identity`, and a `supervision`
record. Batch status returns `{schema_version, capabilities, supervisions}`.
States include `pending_delivery`, `running`, `waiting`, `needs_input`,
`paused_unreachable`, `cancelled`, and `completed`.

`watchtower.observations.observe(session_id, engine='', *, persist=True)` returns
schema version, canonical identity, native transcript path, byte cursor,
completed-turn identity, activity timestamp, busy/idle/unknown state, parsed
footer, provenance, observation timestamp and error. `discover` provides a
bounded non-authoritative inventory or accepts explicit `session_ids`.
Missing/unreadable stores return errors, not an authoritative empty inventory.
The durable incremental cache lives under the WT data directory. Initial scans
read at most 8 MiB per observation and pause intervention while incomplete;
unchanged completed files are not reread. Discovery does not enroll sessions.
This stdlib-only optional interface does not import CCC.

### CCC transport and migration

CCC integration is optional. Existing native adapters support standalone
operation. A managed Codex broker remains the only writer when configured;
unavailability does not start a competing private writer. To delegate takeover
messages, deploy a broker that enforces `same_session:true`, `no_queue:true`,
`skip_wt:true`, `origin:wt` and acknowledges `same_session:true`. WT first checks
`GET /api/takeover/capabilities` for `{schema_version:1,same_session:true,no_queue:true}`
and caches the result for 30 seconds. Old/unavailable brokers fail closed.
No broker configuration or service restart is performed by installation.

After fencing legacy CCC effects and settling in-flight sends:

```sh
wt takeover SESSION --ticket PROJECT-42 --legacy-json '{"migration_id":"ccc:record:epoch","owner_fenced":true,"state":"waiting","reminders":2,"next_deadline":1790000000,"processed_turn":"native-turn-id","wait_identity":"dependency"}' --json
```

Legacy import accepts `running`, `waiting`, `needs_input`, `cancelled`, or
`paused_unreachable`; also accepts `enrolled_at` and a question in `reason`.
It preserves the supplied budget/deadline/watermark and sends no enrollment
instruction. Repeating a migration id returns the stored generation. The
caller owns the cross-application fence; WT cannot prove a caller's assertion.
For rollback, cancel WT and fence its dispatch **before** restoring CCC effects.

### Automatic candidates (off by default)

Automatic policy never scans CCC's private state or guesses a task. A scope
must explicitly name repository, queue and authorized ticket bindings. It
requires an eligible unfinished-work footer with no human decision, agent-owned
in-scope remaining actions, native idle for 120 seconds, activity within two
days, and idle related sessions. Existing and cancelled generations suppress
re-enrollment. The caller must supply all relevant related session IDs.

CCC publishes through WT rather than writing WT-owned files:

```sh
wt takeover candidates --source ccc --candidates-json '{"schema_version":1,"enabled":true,"delegation_fenced":true,"scopes":[{"repo_path":"/work/project","queue":"PROJECT","candidates":[{"session_id":"SESSION","engine":"codex","ticket":"PROJECT-42","task_brief":"Finish authorized work","authorized":true,"related_session_ids":[]}]}]}' --json
```

The command only replaces CCC's policy under a lock; it does not send or enroll
inline. Enabling CCC candidates requires `delegation_fenced:true`.
`WATCHTOWER_DATA_DIR/takeover-policy-ccc.json` stores that source; an independently
configured `takeover-policy.json` supports the same shape for native candidates.
Eligibility reasons are persisted to `takeover-candidates.json`. Neither file
exists by default. Publishing a disabled policy stops future automatic
adoptions; cancel separately to stop already-enrolled sessions.

## Operational limits

The local queue store is the lifecycle authority; machine/engine/repository
identity changes pause rather than migrate ownership. There is no distributed
cross-host lease or automatic archival. Only Claude and Codex native JSONL
formats are implemented. A malformed footer cannot authorize automatic work.
Unknown native turn state deliberately pauses. External waits without a ticket
trigger need an explicit answer/wake. Context-budget failures preserve the
session and require operator recovery instead of a replacement conversation.

Native Codex takeover snapshots the rollout's model, reasoning effort, approval
policy and sandbox policy at enrollment. The private app-server receives these
explicitly, including writable roots; its ambient model must not replace the
session model. Unsupported/missing native permission settings pause delivery.
A managed CCC connection retains its current thread settings instead. Native
Claude resume retains the session model and does not add a permission-bypass
flag for takeover.

Validation includes isolated concurrent adoption/cancel races, incremental
native readers, receipt-ledger restart recovery, retained answers, dependency
wakeups, legacy budgets, footer waits, review gates, scoped candidates and
native policy retention. Controlled real Claude and Codex sessions were also
resumed without CCC, with receipt confirmation after supervisor reload and
normal no-code closure; Claude additionally exercised an owner question/answer.
Production deployment, legacy cutover and managed CCC transport verification
remain separate integration steps.

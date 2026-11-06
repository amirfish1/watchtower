# Conditional owner answers (opt-in, local source only)

This additive API supports a Decisions outbox without changing existing
`wt answer`, dependency-answer, or block-kind behavior. No producer migration
is enabled. Remote/GitHub queues are unsupported: they fail before any source
write, without a read-before-write approximation.

## Interface

`wt answer-conditional DEMO-1 --request-json '<request>'` always emits JSON
on stdout (delivery messages go to stderr). Required request fields:

```json
{
  "idempotency_key": "decision-attempt-unique-key",
  "text": "Blue",
  "expected_blocked_at": "2026-10-01T12:00:00Z",
  "expected_block_question": "Choose a color?",
  "expected_wait_kind": "owner",
  "expected_wait": null
}
```

Optional fields: `session_id` (default `""`), `by_kind` (default `"human"`,
choices human/worker/system), `tid` (default false), `engine` (default null;
otherwise claude/codex/kimi/devin), and `expected_store_id` (default null).
When nonnull, the store ID is checked under the actual source write lock
**before any receipt replay or answer mutation**; a different or uninitialized
store conflicts. Unknown fields are rejected. Defaults are
expanded before comparing payloads. All answer and routing options, including `expected_store_id`, participate
in idempotency; JSON object key order does not matter. Keys are scoped to the
source ticket and retained on it, including after close/reblock.

`expected_block_question` is the **exact source text**, with no whitespace
stripping or hashing. `expected_blocked_at` is a nonempty episode string.
`expected_wait_kind` uses this exact source classifier:

1. Use `queue.current_wait(item)["on"]` if a current wait record exists.
2. Otherwise `block_kind == "awaiting-client"` means `external`.
3. Otherwise the legacy block is `owner`.

`expected_wait` must equal the entire current wait record, or be null if
`current_wait(item)` is null. A record is current only when `needs_input`
is true, `wait.on` is recognized, and `wait.block_at` matches `blocked_at`.
Preserve all record fields, including actor, ref, at, by, note, and host. This
extra comparison catches ownership revisions even if the kind returns to owner.
New wait records carry a random `revision` token, so same-second changes also
differ. New local blocks use microsecond ISO timestamps to avoid same-second
episode aliases; existing episode strings remain valid.
Do not substitute a digest computed from stripped question text and wait kind.

Acceptance requires a blocked, nonclosed ticket, exact episode/question/wait
matches, and the source classifier to still be `owner`. Validation, replay
lookup, receipt insertion, and the ordinary answer mutation occur under the
same actual source write lock. Receipt and answer are saved atomically in the
same item transaction. Failure to acquire the source lock refuses the request.
New answers use all the ordinary source handling (including group integration,
plan/stage retries, takeover, parked answers, and legacy claims).

## Receipts and reconciliation

`wt answer-receipt DEMO-1 decision-attempt-unique-key --json` performs a
read-only source lookup. It returns the durable receipt, including the
canonical request, or `{"error":"not_found"}` (exit 1). It does not acquire a
write lock, migrate the store, send a message, or trigger a worker.

Example successful receipt (request abbreviated here):

```json
{
  "version": 1,
  "ref": "DEMO-1",
  "idempotency_key": "decision-attempt-unique-key",
  "accepted": true,
  "accepted_at": "2026-10-01T12:01:00Z",
  "work_completed": false,
  "routing": {"state": "pending"},
  "request": {}
}
```

`accepted` means **the source recorded the answer**, never that worker work
completed. `work_completed` remains false; this API makes no completion claim.
Identical-key/payload submission returns the retained receipt even after the
source changes. Reusing a key with a different payload conflicts (exit 2).
Stale source conflicts also exit 2 with `error: conflict`; invalid requests
use `invalid_request` and unsupported sources use `unsupported`.

Routing state:

| State | Meaning |
| --- | --- |
| pending | Answer saved; no CLI delivery attempt claimed yet. A replay may claim it. |
| uncertain | Attempt claimed durably **before** delivery; its outcome may be unknown. |
| completed | Existing delivery helper returned zero; delivery may be queued, held, or handed off. This is not worker completion. |
| superseded | A newer block, answer, or close was observed before the attempt; no delivery made. |

The CLI claims at most one attempt under the source lock. Concurrent duplicate
submissions cannot answer twice or call the delivery helper twice. An exception,
nonzero delivery result, or crash after claiming leaves `uncertain`. A replay
never reroutes `uncertain`, `completed`, or `superseded`. Consult existing
pending-answer/message transport state to investigate uncertainty; do not use a
new key to bypass it. Transport retries already owned by those systems remain
their responsibility. This contract provides at-most-one entry into the route
helper, not distributed exactly-once message delivery.

If the response is lost, first use `answer-receipt`. If no receipt exists, the
same key and exact payload can be safely submitted again; source validation
still applies. A durable pending receipt can also be submitted again to claim
the first route attempt. If source persistence/delivery infrastructure fails
outside the typed errors, the CLI may fail without JSON; reconcile by lookup.

Python API: `watchtower.conditional_answers.submit(ref, request)` records or
replays and returns a receipt without CLI delivery; `lookup(ref, key)` is
read-only. The CLI uses `claim_route` and `finish_route` around the existing
block-kind-aware `_deliver_answer` helper. These capabilities are local-only;
callers must not fall back to an unguarded legacy answer after a refusal.

## Conditional owner closes

`wt close-conditional DEMO-1 --request-json '<request>'` closes a ticket because
the owner decided the question no longer matters. It always emits JSON on stdout.
The request takes the same `idempotency_key`, `expected_*` fields and optional
`expected_store_id` as a conditional answer, with a required `reason` in place of
`text` and an optional `by` (default `owner`). Acceptance rules are the same: an
exact, still-current owner question under the actual source write lock. The close
and its receipt (`close_receipts` on the ticket) are one write.

The close is final work-wise: it wakes no worker, runs no gate, verifier or
post-fix assessment, and clears any park. It closes as Declined, so a ticket
waiting on this one is flagged for a human instead of being treated as unblocked
by finished work. A replay of the same key returns the original receipt and
writes nothing. Exit 2 carries `conflict`, `unsupported` or `invalid_request`.
`wt reopen` returns a wrongly closed ticket to the pool; it does not restore the
original block.

## Authoritative local owner-block export

`wt owner-blocks-export --json` exports the **full local owner-block set**, with
no pagination or display truncation. The JSON envelope has:

- `schema_version: 1`, `adapter: "watchtower-block"`.
- `authoritative: true`, `complete: true` on successful source reads only.
- `scope: {backend: "file", store_id: "<durable random UUID>"}`.
- `store_version: {revision: <SQLite revision>, snapshot_sha256: "<content hash>"}`.
  Legacy JSON stores have a null revision; the content hash remains available.
- `capabilities: ["conditional-owner-answer-v1", "answer-receipt-v1", "owner-blocks-export-v1", "store-identity-v1"]`.
- `items`: every local nonclosed `needs_input` record whose exact source wait
  classifier is owner. GitHub-backed queues and other wait kinds are excluded.
  Review-only requests are outside this set and cannot be retired using it.

Each item contains `origin_id` (ticket ref), `project`, `generation`
(`blocked_at`), `title`, `status`, `duplicate_of`, `body` (exact question),
`wait_kind: "owner"`, `block_kind`,
`expected_blocked_at`, `expected_block_question`, `expected_wait_kind`,
`expected_store_id` (the scope UUID), and `expected_wait` (the complete current record or null). These expected fields
map directly to the conditional-answer request. `answerable` is false when an
old record has no episode identity; do not submit such a record through CAS.

New owner questions detected by same-session supervision, including its final
reminder escalation, receive a microsecond block episode, an explicit owner wait
with a random revision, and a block history event in the same source transaction.
Repeated reads preserve that episode. An identical question after an answer is
a new episode, so a frozen answer for the previous question fails the source
check. This does not enable answering or send a message.

Supervision preserves existing source waits and does not silently initialize
legacy records with missing episodes. Those still require explicit, reviewed
source repair; their historical block time cannot be inferred from a footer.

`source_revision` is SHA-256 of
`json.dumps([generation, body, "owner", expected_wait], sort_keys=True).encode()`.
Freeze these source fields unchanged in the outbox; do not recompute them from
a trimmed display body. The store content hash includes all source items and
backend settings, so it also changes when an item disappears or leaves scope.

`title`, `status`, and `duplicate_of` preserve source values as strings (empty
when absent); title is not inferred from the question or note. The export does
not add a duplicate-follower or `in_review` filter to WatchTower's owner-wait
classifier. `block_kind: "rationale"` is retained as a structured distinction.
Consumers such as CCC may apply stricter owner-question admission, excluding
duplicate followers and review status and treating rationale as a separate
read-only approval until its own adapter exists. Exported owner membership and
`answerable` do not grant that consumer admission or approve a rationale.

Completeness is scoped to **this store ID, adapter, and local owner-block
class**, not all WatchTower requests or federated nodes. CCC may retire a
previously imported row missing from a later successful complete snapshot of
the **same scope**, including a truly empty set. A changed store ID is a new
source scope, never evidence that the previous store's requests disappeared.

Export holds an existing source flock shared/read lock and uses a SQLite
`mode=ro` read transaction (including committed WAL data). It neither calls
the migrating source/settings resolvers nor writes answers, receipts, worker
state, migrations, or activity logs. It does not create a missing lock file.
Legacy JSON can be read in place without creating a database. A busy writer,
missing/unreadable source or lock, invalid configuration, or required settings
migration fails closed: exit 2 with
`{"error":"unavailable","authoritative":false,"complete":false,"message":"..."}`.
Such a response, a timeout, or an infrastructure failure provides **no absence
evidence**. Keep prior requests and retry a later read; never withdraw them
from a partial/failed snapshot. This command does not enable any producer
migration or review accept/reject operations.

The single-ref `answer-receipt` lookup uses this same strict, shared-lock,
nonmigrating reader. Missing/unreadable stores and busy writers yield exit 2
(`unavailable`, or `unsupported` for unsupported sources), never exit 1
`not_found`. Only exit 1 after a successful source read is authoritative absence.

## Durable store identity

Store identity is persisted random metadata, not a hostname/path/content hash.
New SQLite stores receive a UUID at creation (`meta.store_id`); ordinary source
saves initialize missing identity on older stores. Moves and ordinary edits do
not change it. A legacy JSON source stores its UUID in top-level `store_id`,
which is preserved when the normal writer migrates it to SQLite.

Read-only export never invents an identity. If an older store has no ID, export
returns unavailable rather than an authoritative anonymous scope. The explicit
`wt answer-store-init --json` command initializes **only identity metadata** of
an existing local store and returns `{store_id, initialized}`. It does not
change ticket items, migrate JSON, create a missing source, rotate an existing
ID, or contact workers. Running it again returns the same UUID with
`initialized:false`. Initialization has not been run against a live store by
this implementation lane.

Freeze exported `expected_store_id` with the answer payload. Conditional
submission, route attempt claiming, and route result recording verify it under
the source lock; lookup accepts
`wt answer-receipt REF KEY --expected-store-id UUID --json` and checks it in the
shared read snapshot before returning any receipt. Mismatch is exit 2
`conflict`, not authoritative absence. The Python `lookup`, `claim_route`, and
`finish_route` helpers take the optional expected ID as their last argument.

Omitting `expected_store_id` is canonically equivalent to explicit null and
preserves older caller behavior, including replay of receipts created before
this field existed. Null opts out of store binding; CCC should require the
exported UUID for its authoritative workflow. Adding a nonnull store ID to an
already used null-bound key changes the payload and conflicts; preserve the
original request rather than silently upgrading a replay. Consumers of the
previous experimental path-hash scope must import the new UUID scope explicitly;
they must not reinterpret a path hash as this durable identity.

## Explicit legacy owner episode initialization (inert, opt-in)

An old owner input wait can have `blocked_at: null` and no block history. Its
historical episode and block date are **unknown**. Never derive a generation
from a hash, title, question number, or guessed historical timestamp.

The separate `owner-wait-initialization-v1` capability provides:

```sh
wt owner-wait-init EXAMPLE-1 --json
# Review and retain the exact response.request; no source metadata is changed.
wt owner-wait-init EXAMPLE-1 --request-json '<exact response.request JSON>' --apply --json
```

The first command defaults to **DRY RUN**. `--apply` requires the frozen request;
there is no implicit preview-and-apply mode. Both emit an envelope:

```json
{"dry_run":true,"replayed":false,"request":{"version":1,"idempotency_key":"random-attempt-UUID","expected_store_id":"persisted-store-UUID","expected_store_version":{"revision":1,"snapshot_sha256":"full-source-content-hash"},"expected_item":{},"expected_wait":null},"receipt":null}
```

`expected_item` in a real preview is the **complete raw source item**, not the
empty documentation placeholder. It freezes exact raw question, status,
`needs_input`, raw wait, ref, owner, answer/routing fields and all other fields;
`expected_wait` separately freezes the entire current wait or null. The source
version is exactly the export's `store_version`, including its full content
hash and SQLite revision (null revision for legacy JSON). All request fields
are required; unknown fields are rejected. JSON object key order is irrelevant;
JSON scalar types and whitespace within strings are significant. Persist the
entire request rather than constructing a reduced payload from display text.

Only exact unique refs in an existing local store are supported. Admission
requires a nonclosed, nonreview `needs_input: true` owner input wait with a
nonblank raw question, missing episode (null, empty or absent), no block history
and no previous initialization provenance. Duplicate followers, review requests,
rationale, external, dependency, worker and unknown waits are refused. Any raw
wait must itself be a current owner record; stale owner metadata is not revived.
Known block history is refused rather than interpreted by this migration.

A persisted random store UUID is mandatory; this command never initializes one.
It opens the **existing actual source lock**, shared for dry run and exclusive
for apply, nonblocking and fail closed. The store UUID is checked before receipt
replay. First application requires exact full store version, raw item and current
wait CAS under that lock. SQLite also holds `BEGIN IMMEDIATE` through validation
and commit. Receipt and metadata are atomic in the same item transaction; JSON
is atomically replaced in place without conversion to SQLite. Missing stores,
locks, identity or required ref/settings migrations are refused. No ordinary
save/duplicate synchronization, schema migration, answer delivery, ticket
history event, worker contact, producer activation or service refresh occurs.

Apply creates a new microsecond UTC episode at actual initialization time,
a random wait `revision`, and a full owner wait. Existing actor/ref/by/note/host
and extra wait fields are retained; `block_at`, `at` and `revision` describe the
new initialization. All other existing source fields, including exact question,
status, title, ticket ref, owner, answers, routing and `updated_at`, are preserved.
Only `blocked_at`, `wait`, `owner_wait_initialization` provenance, the receipt
map and SQLite source revision change. The content hash is a CAS token only,
never an episode. A timestamp collision with a current episode refuses apply.

Provenance is additive, persisted and exported as `episode_provenance`:

```json
{"version":1,"kind":"legacy-owner-wait-initialization","initialized_at":"2026-10-01T12:00:00.123456Z","historical_blocked_at":null,"historical_block_time":"unknown","idempotency_key":"random-attempt-UUID"}
```

Consumers must label this **initialized at**, with historical block time unknown,
not "blocked since" that date. Export returns null provenance for ordinary
records or when a later episode supersedes initialization. Export's schema stays
v1; its capabilities add `owner-wait-initialization-v1`. Existing conditional
answer fields, source revision calculation and answer semantics are unchanged.
After application, consumers must re-export and explicitly freeze the new
full wait/episode before any separately authorized answer.

Successful apply returns `dry_run:false`, `replayed:false` and an immutable
receipt retained in `owner_wait_initialization_receipts[idempotency_key]`.
It contains version, ref, store_id, key, initialized_at, blocked_at, full wait,
provenance and the exact request. Repeating the same ref/key/payload returns that
receipt with `replayed:true` even after later edits/close; it never changes the
source again. A same-key changed payload, wrong store or stale first-application
snapshot yields exit 2 `conflict`. Invalid envelopes yield exit 2
`invalid_request`, unsupported prerequisites `unsupported`, and I/O/SQLite/lock
failures `unavailable`. No refusal permits an unguarded fallback. Dry runs with
a retained request also support read-only receipt reconciliation and replay.

The full-store fence deliberately invalidates preview after unrelated source
edits, favoring safety over throughput; a fresh preview needs review again.
Source settings are read without migration under the source lock; operators
must not independently edit settings during a controlled apply. Existing
persistent writers must be refreshed by their owner before controlled use so
an old in-memory writer cannot discard additive metadata. This implementation
does not automatically refresh them or enable migration, answers, or producers.

### Recovering an answered initialization followed by an unversioned wait

`wt owner-wait-repair EXAMPLE-1 --json` previews one narrowly supported recovery
without changing the source. It requires an open supervised owner question
whose episode is missing, with an exact stale owner wait from a retained
initialization receipt. That receipt must belong to this ticket and store.
Source history must preserve the pre-initialization history, show a subsequent
answer, and contain no later block. The active supervision must name the exact
current question. Duplicates, review gates, rationale approvals, nonowner waits,
changed receipt evidence, and other malformed records are refused.

The frozen request adds `operation: "repair-supervised-owner-wait-v1"` to the
initialization envelope. Apply only that exact reviewed request:

```sh
wt owner-wait-repair EXAMPLE-1 --request-json '<exact frozen request>' --apply --json
```

The original initializer refuses repair requests and still refuses stale waits.
Repair uses the same actual source lock, complete source version/item/wait CAS,
store binding, transaction and durable replay contract. It replaces only the
episode, wait revision/time, current initialization provenance, and receipt map;
the old receipt remains. Question text, status, supervision, claims, history and
answers remain unchanged. The new timestamp is the recovery time, explicitly
labelled with unknown historical block time. No answer, transport, worker wake,
producer activation or automatic recovery occurs. Re-export before considering
any separately authorized answer.

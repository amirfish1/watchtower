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
(`blocked_at`), `body` (exact question), `wait_kind: "owner"`, `block_kind`,
`expected_blocked_at`, `expected_block_question`, `expected_wait_kind`,
`expected_store_id` (the scope UUID), and `expected_wait` (the complete current record or null). These expected fields
map directly to the conditional-answer request. `answerable` is false when an
old record has no episode identity; do not submit such a record through CAS.
`source_revision` is SHA-256 of
`json.dumps([generation, body, "owner", expected_wait], sort_keys=True).encode()`.
Freeze these source fields unchanged in the outbox; do not recompute them from
a trimmed display body. The store content hash includes all source items and
backend settings, so it also changes when an item disappears or leaves scope.

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

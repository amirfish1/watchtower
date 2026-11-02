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
otherwise claude/codex/kimi/devin). Unknown fields are rejected. Defaults are
expanded before comparing payloads. All answer and routing options participate
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

# External CLI clients

Clients can use the public CLI without importing WatchTower internals. Ticket
snapshots come from `wt find REF --json`. The snapshot includes
`effective_gates`, the resolved acceptance gates (including queue defaults),
and any `stage_session` or `supervision` record. Queue settings and resolved
role targets come from `wt config -q QUEUE --json`: `config` contains queue
settings, and `roles` maps role names to engine, model, and source.

## Opaque correlation

```sh
wt add -q QUEUE --title 'Task' --client-id 'external:any/string' --json
wt edit REF --client-id 'external:any/string' --json
wt ls -q QUEUE --client-id 'external:any/string' --status all --json
```

`client_id` is an optional opaque string, stored and returned verbatim. It
never changes ticket authority, gates, routing, or worker behavior. Local and
GitHub-backed queues both support it. Pass an empty string to clear it; the
empty filter includes older tickets with no identifier. Filters apply before
window selection and pagination. Existing list output shapes are preserved.

## Fenced stage retry

```sh
wt stages retry REF --expected-cycle N --json
```

Read N from `stage_session.retry_cycle` in a fresh snapshot; absent means zero.
Success is one JSON object:
`{"ok":true,"ref":"Q-1","retried":true,"cycle":1,"stage_session":{...}}`.
The retry cycle advances atomically on retry, and when the stage key changes.
Human answers that retry an escalation advance the same fence. A duplicate
call with the old cycle fails. The command resets an escalated stage's attempt
budget and clears only its own stage block. It does not spawn or send inline.
The next ordinary stage-supervision pass performs the retry.

Retries require local storage and the same eligible stage. They refuse a stale
cycle/key, a live stage, an unrelated wait, pending or unknown answer delivery,
a duplicate, parked claim, unresolved dependency, or active same-session
supervision. Normal builder claim ownership is preserved. Assessment retries
start a fresh assessment cycle; they do not reopen the ticket.

## Fenced supervision resume

```sh
wt takeover resume REF --expected-generation N --json
```

Read N from `supervision.generation`. Success is one JSON object:
`{"ok":true,"ref":"Q-1","resumed":true,"generation":1,"supervision":{...}}`.
This resumes waiting supervision once per generation, resets its reminder
budget and deadline, and preserves the canonical session and claim. No inline
message is sent. The next supervision pass observes the retained session.

Resume requires local storage, settled intent delivery, and an active claim
still owned uniquely by the same session. Stale generations, repeated resumes,
pending or unknown deliveries, ticket waits, parked claims, duplicates, active
planning, and unresolved dependency gates are refused. A cancelled or completed
supervision requires explicit adoption, not resume. Recorded external waits
must first be resolved through their normal public ticket operations.

Both recovery commands emit one JSON object on success or runtime rejection;
rejections contain `ok:false` and `error` and exit 1. `--json` is accepted for
consistency; recovery output is always JSON. These commands do not start or
restart a daemon or enable automatic queue draining.

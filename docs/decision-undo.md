# Decision outcomes and replay cancellation

`wt undo REF --decision 'REF@BLOCK_TIMESTAMP'` selects one actual persisted
approval. With more than one approval the decision ID is required. A code
decision still produces a verified Git revert. No command ever executes the
approval's free-text `undo` string.

For completed capped replays, requests already sent cannot be unspent:

```sh
wt undo REF --decision 'REF@BLOCK_TIMESTAMP' --consumed \
  --evidence /absolute/path/ledger-or-result.json \
  --note 'Explain how this preserved evidence belongs to this decision'
```

This records `auto_decision_consumed`, never `auto_decision_reversed`. It
preserves evidence and terminal budgets, hashes the selected evidence and
records consumed requests/cap. Historical results must identify their ticket;
modern controls must also match the exact decision. The association of older
unlabelled evidence with a particular approval is an explicit operator claim,
not an inferred runner acknowledgement. For reused output paths select the
preserved result for that attempt, not a later overwrite. Consumed recording
does **not** certify cancellation or grant a new replay budget.

All outcomes persist on the ticket's own backend, including GitHub tickets
without pipeline sidecars. Success requires readback. Identical retries retain
one event; a different proof for an existing event fails visibly. If remote
persistence fails after stopping a runner, it stays stopped: retry the same
command to persist the receipt, never reset its ledger.

## Supported cancellation contract

Legacy one-off runners cannot be cancelled by a history marker. Their old
`rm ledger.json` undo text must never be used: deleting it may permit reruns.
`--cancel` refuses these ledgers without modifying them.

Runners can use `watchtower.replay_control.ReplayControl` on POSIX hosts:

```python
control = ReplayControl(path, exact_decision_id)
control.create(already_approved_cap)  # exclusive: existing ledgers are refused
try:
    with control.request():  # must enclose the ENTIRE bounded upstream call
        send_one_already_approved_request()
finally:
    control.finish()
```

This API coordinates cancellation; it grants no approval and does not contact
any model. The caller must independently obtain and respect the existing
decision, backend, dollar cap and request cap. Never convert a historical
ledger into this schema or reuse a terminal path for another approval.

`wt undo REF --decision ID --cancel --evidence CONTROL_JSON` takes the same
cross-process lock as `request()`. It waits for an in-flight call to finish,
then seals the ledger before acknowledging cancellation. After acknowledgement,
no later request can start. A request still in flight after 30 seconds produces
a visible timeout with no cancellation receipt; retry when it has finished.
Runners must bound their upstream call duration. Previously forwarded attempts, including failed
ones, stay charged. The durable `auto_decision_cancelled` receipt means no
future request can enter this cooperating guard; it does not reverse prior
effects or claim to kill an arbitrary process. A naturally finished runner
returns the consumed limitation instead of claiming cancellation. Once terminal,
its accounting is never rewritten, and exclusive creation prevents restart.

Cancellation is tested separately with a real isolated cooperating subprocess
and no model calls. Historical BECKY gates remain legacy, not silently migrated.
No policy/class expansion or additional replay execution is implied.

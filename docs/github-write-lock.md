# Cross-host GitHub write exclusion (OPS-463)

GitHub issue edits have no compare-and-swap. A local flock and an ownership
readback cannot exclude a later writer on another host. The isolated-host
reproduction returned two successful claims and lost the earlier claim history.

## Mechanism and tradeoffs

The backend retains its per-repository, reentrant critical section. Inside the
local flock it acquires the unique repository label `watchtower:body-lock:v1` by
REST POST (never `label create --force`). A uniqueness conflict waits up to 30s;
other errors fail closed. Every existing serialized body writer participates,
including claim, close, release, heartbeat, comment, answer and gate metadata.
The lock stays held through the last issue side effect, including reopen/close.
It is an unattached repository label, not a label applied to user issues.

Cleanup uses GraphQL `deleteLabel` with the captured immutable node ID. Deleting
by the reusable name would let delayed cleanup erase a replacement lock. Both
successful deletion and the absence of GraphQL errors must be confirmed. The
credentials need Issues-write access and usable REST **and GraphQL** quota.
There is no contents permission, code-branch write, or new credential grant.

Alternatives considered:

| Option | Benefit | Cost / reason not selected |
|---|---|---|
| Extra readback or delay | Small patch | Already present; cannot exclude a later stale write |
| Git ref CAS and authoritative state | Atomic state transitions | Contents-write permission and a separate claim-state/projection protocol |
| Shared service | Can serialize transactions | Adds a new authority and availability/security dependency |
| Pin each queue to one host | Simpler coordination | Changes fleet placement policy and removes cross-host failover |
| Atomic repository-label mutex | Preserves current writer contract and host placement | Two extra API writes, bounded remote contention, owning-host crash recovery |

This is internal writer serialization. It does not change queue policy, model
routing, gates, customer actions or who may operate on a ticket.

## Crash recovery and failure handling

The description contains `<host/store-id> <pid> <UTC-start-hash> <nonce>` (under
GitHub's 100-character limit). The store identity is the existing pipeline owner
UUID; a short machine tag alone is insufficient. Never copy a live store's owner
identity to another host.

A POSIX flock on `<store-dir>/gh-write-fences/<nonce>` is inherited by every gh
request process, including the single-issue REST transport. If the parent dies
while gh is still writing, that child retains the fence. A later writer on the
**owning store/host** may recover only when a nonblocking exclusive fence lock
proves inherited request processes have drained, AND either the PID is absent/
provably reused or the local fence explicitly records `ended`. The writer records
`ended` only after leaving its transaction, including when cleanup failed; this
lets a still-running daemon retry cleanup safely after an API outage. Missing
identity/fence, an active/unproven transaction or a foreign store never authorizes
recovery. Recovered fence files are kept so
concurrent recoverers cannot lock different inodes for the same acquisition.

There is no time-based stealing: issue API side effects have no server-enforced
fencing token. The 30s wait limit ends a caller's attempt, not the holder's lock.
A failed cleanup reports an error and retains its ended-transaction record. A
later owning-host write retries safe cleanup; a live PID alone is no longer a
reason to require a daemon restart. Do not restart a daemon merely because
another host is busy. A locked foreign repo needs an owning-host write after
that transaction and its request children have ended. If the host is unavailable, preserve the lock and track that dependency.

Never manually delete the reserved label **by name**, expire it, clear a
foreign/unproven holder, or fall back to a local-only lock. A failed or ambiguous
write is not a verified resolution: re-read the ticket before retrying a close.
Normal host restarts preserve enough on-disk identity/fence evidence to recover;
a lost store/fence requires investigation, not automatic stealing.

A worker-attributed release also refuses a foreign claimant and carries its
original claim snapshot into the locked write. A replacement claim arriving
while it waits cannot be released. Explicit `--force` recovery remains deliberate;
it still cannot overwrite a claim that changed after its snapshot. Closed
resolutions reject stale worker closes rather than overwriting attribution.

## Rollout and rollback

All processes that write this repository's issue bodies must run this version.
An older binary ignores the repository mutex; a rolling mix is **not** protected.
Keep the incident open until each claiming/writing host has synced the exact
commit and its daemon has adopted it. CLI source sync alone does not reload an
already-running daemon. Do not change fleet placement or weaken gates to hide a
host that has not adopted. `wt deploy --json` locates each installed source;
`wt deploy --sync` is the guarded update path. Coordinate one restart per host,
preserve workers, and prove adoption with private-fixture contention.

Rollback: revert all OPS-463 source commits, newest first, then push and
sync/restart the same hosts. Reverting only the cleanup-recovery follow-up does
not remove cross-host exclusion.
First let current mutex holders and their request children finish. Do not erase
an active lock to accelerate a rollback. Rollback restores the original
cross-host race; it is not a resolution of OPS-463.

Tests: `python3 -m pytest tests/test_github_write_lock.py` uses separate client
processes, independent homes/stores and local locks, and a shared fake GitHub
server whose label creation is atomic. It covers one winning claim, foreign
and stale release/close refusal, crash recovery, an orphan request fence,
immutable-ID cleanup, failed-cleanup recovery in a live daemon and API permission
failure without an issue write.

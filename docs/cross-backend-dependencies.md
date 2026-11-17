# Cross-backend ticket dependencies (WT-85)

Local and GitHub tickets use the same dependency resolver in
`watchtower/dependencies.py`. Add/edit validation traverses the named parents
and their dependency graph; unknown references, self-edges and transitive
cycles are refused. An incomplete graph (including a local edge added after
the snapshot) is refused rather than accepted without cycle validation.

Local operations gather remote evidence **before** taking the local store
lock. The transaction re-reads local records and combines them with the remote
snapshot. Claims, explicit claims, peeks, stage retries and validation use
single-issue fresh reads. Depth/manual/floor checks reuse the owning GitHub
backend's bounded list cache. Only queues reached through named dependencies
are contacted; an unrelated local operation performs no GitHub request.
GitHub-dependent tickets use this same resolver in the opposite direction.
There is no distributed transaction across GitHub and SQLite; a remote change
after a fresh read can only be observed by the next operation.

`blocker_verdict` and `waiting_on` fail closed on missing parents. Open and
owner-held parents wait; declined or unacknowledged unresolved completions
stay stuck. Clean completion wakes the child without an owner answer. Existing
acknowledgements of stuck blockers retain their established behavior. The
stuck-blocker escalation fetches its evidence before locking too. Scoped health
and ordinary CLI listings resolve named parents without listing all queues;
windowed listings retain their existing bounded mirror lookup.

Regression proof: `tests/test_local_remote_dependencies.py` combines fake
GitHub issues with actual isolated SQLite persistence, exercising add/edit,
owner holds, wait-to-wake, explicit and automatic claims, manual/depth/floor
checks, cycle rejection, missing/outage safety and request/lock scoping.
`tests/test_github_backend.py` retains the reverse-direction regressions.

Operational migration: preserve the upstream owner's acceptance hold. A
previous worker wait must first be classified as a dependency wait before
`wt edit CHILD --after PARENT` can replace it; the existing guard refuses to
erase a human question. This source fix does not grant approval to answer,
close or otherwise release the parent. Validate the installed CLI in a fully
isolated fake-GitHub environment before converting an existing wait, then
verify the actual child still cannot be claimed while its parent is held.

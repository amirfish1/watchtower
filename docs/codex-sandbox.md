# Codex sandbox queue transport — WT-61 review checkpoint

Status: **experimental source, not accepted, not enabled, not ready to merge**.
The workspace-write launcher previously shipped in `dd3c3dc`; it redirects
workers to an isolated empty store and therefore does not preserve real queue
operations. This branch adds a candidate bridge. WT-61 stays open until its
original workflow and security acceptance pass. Do not enable a queue policy
or integrate this checkpoint as a finished fix.

## Implemented transport

Each worker receives a distinct directory beneath the queue's configured
isolated store. Its only writable roots remain that directory and the assigned
workspace; tool network stays disabled. Both exec and app-server receive the
same environment and roots. `WT_SANDBOX_BRIDGE` routes supported CLI operations
to request/response files instead of mutating a local shadow queue. `tell` is
an alias for `send`. Other CLI verbs fail explicitly.

The trusted launcher records worker, queue, source identity, workspace and
optional single-ticket assignment under the daemon's data directory, outside
both writable roots. Requests cannot choose those fields, a cwd, executable,
environment or destination authority. Claims use the real queue APIs. Later
requests must carry the assigned ref, worker/session identity, claim timestamp,
claim-event count and answer generation. Close rechecks that fence and required
gates inside the source write lock, retaining normal product/ownership guards.

The daemon opens every spool component without following symlinks; request
files must be regular, single-link files under 128 KiB. Responses use an
exclusive temporary file followed by rename, so a planted response symlink
cannot redirect a privileged write. A source-side receipt is reserved before
effects and bound to a payload hash. Matching replays return the same result;
different content under the same ID is refused. An interrupted operation is
indeterminate and is never blindly executed again. A worker has a 1,024-receipt
budget. Disabled policies and dead worker registrations revoke dispatch.

Messages can reach only the assigned ticket's submitter, resolved at claim
time, with its identity rechecked before sending. Reports carry an untrusted
sandbox label and attribution evidence; at most ten messages are accepted per
claim. Tells cannot park in the outbox and later resolve a renamed recipient.
Transport receipts and ask results are returned without treating them as
delivery confirmation or permission to close anything.

Commit proof disables Git transports, lazy fetch, replacement objects,
filesystem monitors and hooks. A missing commit fails rather than asking an
untrusted partial-clone configuration to fetch it. This addresses the concrete
review exploit in which `remote.origin.uploadpack` executes a daemon-side shell
command even during `rev-parse`.

Command gates currently fail closed. Review/verify requirements retain their
ordinary state and verify requests wake the stage supervisor, but full-access
stage launches on sandbox queues are refused. Generic headless/RPC/delegate
resumes cannot promote a sandbox worker; only steering its existing hosted
turn is allowed. These refusals are safety boundaries, **not completed support**
for those workflows.

## Verification performed

`tests/test_sandbox_bridge.py` exercises real isolated queue effects, replay,
cross-ticket/identity fields, claim/generation changes including an in-lock
close race, link/FIFO/size attacks, destination changes, message receipts,
policy revocation, single-ticket assignment and actual CLI subprocesses for
both launch environments. Existing sandbox, app-server, RPC, messages and
liveness-delivery suites cover adjacent behavior. The focused run of these
six suites passed 251 tests on 2026-11-15 before the final documentation edits.

A model-free Codex 0.169.0 app-server `command/exec` probe on this host showed
ordinary workspace writes succeed and `.git` writes fail read-only under the
legacy workspace-write policy. Explicitly adding `.git` as a nested root made
it writable; that experiment was temporary and **is not in this patch**. The
probe script is `/tmp/wt61-sandbox-probe.py` (temporary evidence, not a runtime
dependency). Never broaden the roots to an external worktree Git directory.

An independent Claude review found the Git uploadpack escape and wrapper import
escape, plus unsafe stage/resume paths. The corresponding focused fixes and
regressions are included here. A second cross-family review was requested;
its result and a review of the final revised diff remain to be evaluated.

## Remaining original acceptance — keep WT-61 open

1. **Local test/commit workflow.** Codex protects `.git`; ordinary commits fail.
   Shared worktrees additionally reference metadata outside the assigned roots.
   Select and verify a design that preserves roots, such as separate Git
   metadata within the isolated store, bootstrapped inside the sandbox. Keep
   proof verification unable to execute hostile Git configuration. A writable
   nested `.git` root is not a safe blanket fix: replacing it with a symlink or
   poisoning later privileged Git operations must be considered. Network-free
   workers also cannot follow the generic drain prompt's push instruction;
   their integration handoff needs an explicit safe contract.
2. **Stages and handoff.** Implement scoped block/progress and safe command,
   verification and assessment transports. They must neither run hostile tests
   with full access nor skip a required gate. Current refusals are intentional
   interim behavior. Generic resumes must retain policy or remain refused.
3. **Actual engine acceptance.** The current subprocess test proves CLI bridge
   routing, and argv tests prove construction. They do not prove a live exec
   turn and app-server turn each enforce filesystem/network constraints while
   editing, testing, committing and round-tripping claim/close. Run both in
   isolated fixtures, including forbidden writes and outbound network probes.
   Check interactions with inherited Codex execpolicy rules as well.
4. **Crash/size recovery.** Reservation prevents duplicate effects, but a crash
   between real claim and scope persistence can strand a live worker. Add
   fenced adoption of its own outstanding claim and a recoverable current-claim
   read. Bound/project oversized ticket responses explicitly; an item larger
   than the response cap must not become an unreachable claim. Add metadata
   and receipt retirement with bounded work per daemon tick.
5. **Review remaining authority edges.** Finish the revised security review,
   cover source-store replacement races and protected control/runtime paths,
   destination re-resolution inside messaging adapters, and legitimate trusted
   prefix symlinks on macOS. The workspace-write policy is write/network
   isolation: it still permits broad filesystem reads, including credentials.
   Scoped messaging is an egress channel; do not describe this as credential
   read isolation. Evaluate a read-restricted policy separately without silently
   changing this ticket's roots or approval requirements.

No live queue settings, send switches or service restarts were changed for
this checkpoint. Source is deliberately held off the release branch pending
completion and review; a pushed review branch is not integrated or deployed.

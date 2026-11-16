# Codex sandbox queue transport — WT-61 review checkpoint

Status: **experimental source, not accepted, not enabled, not ready to merge**.
WT-61 remains open. Do not enable a live queue policy or describe this review
branch as integrated. The complete candidate combines the earlier bridge,
lifecycle fences, recovery and native-policy work with the integration fixes.

## Enforcement and authority

Each registered worker gets two writable roots: its assigned workspace and a
per-worker directory beneath the configured isolated store. Native Codex
permissions start from `:minimal` read access, add explicit runtime paths and
shared Git objects read-only, deny `/dev/shm`, and disable tool network.
Workspace `.git`, `.codex` and `.agents` and store `.codex` remain read-only.
The launcher runs a model-free kernel preflight before spawning. Both exec and
app-server use this profile; sending the legacy thread `sandbox` field would
replace it and is forbidden. App-server checks the returned policy before a
model turn. Its Unix socket lives outside the writable roots, in a private
directory. The host model process still uses existing Codex authentication;
tool processes receive a scrubbed environment and private HOME/TMPDIR.

All workspace ancestors are pinned untrusted; populated or symlinked `.codex`
entries refuse launch/proof and stop hosted turns. Empty directory mount
targets are allowed: Codex temporarily creates them during tool execution,
and rejecting those broke concurrent proof/turn guards. MCP, hooks, apps,
plugins, web search, browser/computer tools and subagent tools are disabled by
session overrides. Keep testing the actual runtime, not just constructed argv.

The spool broker's trusted scope lives outside both writable roots. It records
worker, queue, source identity, workspace, store, optional ticket assignment
and the current claim fence. The worker cannot choose those authorities.
Claim/close/find/current/comment/block/ask/tell/send operate through bounded
request/response files; other commands are refused. Tell aliases send.
Close/comment/block recheck source identity and claim ownership/generation at
the source mutation lock. Worker/session restriction tombstones survive registry
pruning and later queue-policy changes, including sessions backfilled during
the same pass that prunes a dead worker. A released or dead worker is refused.

Every effect reserves a source-side receipt first. Matching replays return the
same result; changed content under the same ID is refused. Interrupted effects
remain indeterminate. Claim recovery adopts only that worker's outstanding
claim and never repeats a claim event. Oversized tickets are explicitly
projected; `wt find REF --field note --page N` retrieves the original text.
Nonregular, linked, oversized or deeply nested spool entries are isolated and
retired with bounded work. Responses, temporaries and dead-worker receipts
have retention periods; permanent restriction tombstones are not retired.

Messages pin the submitter's exact resolved session/engine/worker descriptor,
label text as untrusted sandbox evidence and deliver only to the current live
destination. They never resume/delegate or park in the outbox. Per-ticket
message/comment budgets survive fence refresh. Minimal receipts/results avoid
returning arbitrary adapter output or privileged logs.

Git metadata for local commits lives in the isolated store (`GIT_DIR`), with
read-only alternates to launch-time workspace objects. Bootstrap and proof
run inside native Codex sandboxes, never host Git against worker config,
`.git` pointers or alternates. Proof roots and base come from a trusted launch
record and must match the bridge scope. Full SHAs must strictly descend from
the assigned base. A read-only proof sandbox disables lazy fetch, replacements,
hooks and transports. Trusted Git operations must use a system executable,
never the worker-writable PATH (the earlier checkpoint failed this condition).

## Patch handoff and restricted worker contract

A code close request exports a bounded patch and its digest to trusted
`sandbox-bridge/handoffs/`, then parks the original ticket with a worker wait.
It does **not** close the ticket or claim the store-only commit integrated.
The request still requires a valid, strict-descendant commit proof, source
identity and the original claim fence. Non-code closes retain required gates.

The integration worker reviews the exported patch and supplies its own clean,
trusted worktree at the recorded base to:

```sh
python3 scripts/adopt_sandbox_patch.py --worker WORKER --commit FULL_SHA --destination TRUSTED_WORKTREE
```

The script rejects sandbox destinations, changed digests, dirty worktrees and
project control metadata. It applies data with system Git, disables hooks,
external transports, replacements and fsmonitor, then creates a local commit.
It does not run proposed code, push, or close the ticket. After source review,
the integration worker tests, lands and pushes this commit, verifies ancestry
on the freshly fetched release branch, and resolves the original ticket with
that integrated commit through its required gates. Conflicts/base drift remain
open integration work; a new handoff requires the current claim fence.

Restricted workers receive a self-contained prompt matching the available
bridge verbs. They do not receive host-home learnings, generic resume recipes,
or instructions to push over a disabled network. They post learnings through
progress for the trusted coordinator. Ask may return a correlated transport
reply or explicit `answer_pending`; the broker never tails recipient host logs.
Worker stores default to `~/.watchtower-sandbox/QUEUE/WORKER`, outside the
entire trusted worker-control directory. Other workers' stores are protected.

## Verification on 2026-11-16 — worker wt-039caa56

Both earlier actual-model **honest-path** disposable-queue runs passed. They
did not exercise a planted `store/bin/git`, empty base or the integration
handoff and are not security acceptance for the revised candidate. The runner uses
production launch builders **and** `codex_sandbox_prepare`, an isolated source
store/registry and a real spool broker; it never edits live queue settings.
Each worker claims a fixture, fixes addition, runs unittest, checks boundaries,
commits only its fix in the store, and closes the actual fixture ticket.

| Transport | Retained evidence | Worker commit |
|---|---|---|
| exec | `/home/hermes/wt-sandbox-acceptance-4d1jpezv/controller/verdict.json` | `4bd4edad240f83721926de1dd8e74be8cf348ce8` |
| app-server | `/home/hermes/wt-sandbox-acceptance-mvdzr14b/controller/verdict.json` | `95c914d3f41637c8265248e0193a53b328362040` |

Both report closed fixture status, passing tests, changed source, denied
AF_INET/AF_UNIX probes, denied shared-memory writes, and no host file written
outside the roots. Unlisted ancestors may be private tmpfs overlays, so a
successful write there inside the sandbox is not evidence of a host write;
the runner checks the host view as well. The unit/kernel suites exercise
IPv6, Unix bind/connect, credential canaries, shared worktrees, hostile Git
configuration/alternates, project MCP configuration, claim races, replay,
crash recovery, oversize paging and restriction persistence.

Earlier review reports are in `~/.watchtower/logs/critique-claude-9ae3e712.log`
and `critique-antigravity-9563be6a.log`. The concrete host Git uploadpack and
Python import escapes were addressed. The alleged unrestricted AF_UNIX escape
did not reproduce on this host; native socket tests remain required. Revised
combined-source security review is still required before integration.

## Remaining acceptance — keep WT-61 open

1. **Safe gated workflows.** At the earlier checkpoint, command gates and full-access stage/assessment
   launches currently refuse sandbox-origin work, including group-parent
   gates after policy changes. This is a safety boundary, not completed
   stage support. Implement isolated command/verification/assessment
   transports with scoped verdicts; never skip a required gate or run hostile
   tests with daemon authority. Generic resumed/delegated turns remain refused
   unless the exact restricted policy is retained.
2. **Revised handoff acceptance.** Exercise the new restricted prompt,
   adversarial proof rejection, code handoff, trusted adoption and final gate
   behavior on both real transports. A local fixture release ref is test
   evidence only; real tickets require fetched remote release ancestry.
3. **Combined security review.** Review final root/path/config races,
   source replacement and launch/proof records; verify destination pinning
   and protected runtime/control paths. Credentials must remain outside
   explicit runtime read roots; operator-provided extra read roots require
   review. Restriction tombstones and launch records still grow, and receipt
   scans are capped. Mac trusted-prefix handling has unit coverage but no
   native macOS acceptance; fail closed if enforcement cannot be established.

Review branch/worktree: `wt-61-wt-039caa56`,
`/home/hermes/Apps/watchtower-wt61-039caa56`. No live policies, customer sends,
credentials, send switches or services were changed. Source remains deliberately
held off main until the remaining workflows and final review are complete.

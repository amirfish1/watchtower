# Codex sandbox queue transport — WT-61 review checkpoint

Status: **experimental source, not accepted, not enabled, not ready to merge**.
WT-61 remains open. Do not enable a live queue policy or describe this review
branch as integrated. The complete candidate combines the earlier bridge,
lifecycle fences, recovery and native-policy work with the integration fixes.

## Security review continuation — 2026-11-17, wt-c670215d

Claude's final review of cba8ce3 is blocking; full report:
`/home/hermes/wt61-final-review-claude-cba8ce3.md`. The independent Antigravity
report is still pending. Keep the candidate off main and live policies off.

- F1: e66b700 replaces stage spool verdict/assessment authority with the final
  model response captured by the trusted parent outside both tool roots.
  Exec uses `--output-last-message` plus a strict output schema; app-server
  captures only the assigned thread/turn's final agent message. Tool output,
  commentary, forged spool requests and writable shims cannot submit a verdict.
  Ticket context and the materialization manifest are under the read-only
  store `.agents` subtree. Real gated exec and app-server runs passed on this
  revision: `/home/hermes/wt61-trusted-capture-{exec,app-server}.log`.
- F2: 83a0763 (adopted as 68ae0a7 on this worker's branch) stamps sticky
  `sandbox_origin` under the claim lock and checks legacy history tombstones.
  Trusted reclaim and a disabled policy cannot promote command gates,
  verifier/assessor launch or group integration to host authority.
  `tests/test_sandbox_origin.py` exercises these consumers and ledger failures.
- F5: tool environment overrides exclude `WATCHTOWER_*` and `CODEX_HOME` while
  retaining the scoped `WT_SANDBOX_*` bridge transport. The acceptance runner
  checks this from a real tool process, alongside filesystem/network probes.
- F6: stage root validation now precedes context creation (included in e66b700).
- F7: peer workspaces can overlap: shared queue checkouts are the current
  topology. Their source bytes are mutually untrusted; a local commit is not
  an independent source attestation. Handoff, restricted gates and independent
  model review must still validate the pinned adopted commit.

F3/F4 source from the implementation lane is now combined: b6c9cd6 adopted as
3b46a97 adds protected patch paths, bounded no-follow ledger scans, restricted
adoption testing and a sandbox-aware pre-push hook. This worker additionally
keeps the complete four-suite gate for every pushed adopted ref, avoids
pipefail/SIGPIPE trailer misses, and stamps sticky provenance in the common Git
directory before applying bytes; history rewriting and linked worktrees retain
the sandbox requirement. Adoption/hook regressions: 73 passed in
`/home/hermes/wt61-c670215d-adoption-v2.log`. Required gate runtime is blocked
by missing system pytest (OPS-643); no host fallback or gate override is allowed.
Assess both security reports and remaining gaps, and obtain final revised
security review before integration. Existing honest-builder acceptance does not prove
that a malicious builder cannot forge a verifier result; retain the hostile
spool/native-boundary regressions and test an adversarial gated run.

This worker's real honest-builder gated runs passed on both transports, with
`controller_env_hidden=true`: `/home/hermes/wt61-c670215d-{exec,app}.log`.
Real adversarial runs also passed their expected-rejection check on both
transports: `/home/hermes/wt61-c670215d-hostile-{exec,app}-v4.log`. Each records
an actual forged PASS request and replaced shim inside the real verifier
store, followed by the model's own FAIL and the ticket reopening. No assessor
runs after the failed verification gate. Earlier fixture runs are retained:
v2 encountered the newly enforced global-hook refusal; v3 verifiers mocked
the writes, so those runs intentionally failed the attack-attempt acceptance.
The final fixture isolates only its own local hook lookup and explicitly
authorizes adversarial writes inside its disposable roots. It never pushes.

## Required-gate portability continuation — 2026-11-17, wt-9aafe8b3

WT-86 candidate `14688d5d22b73f8019db376a7223f7747b1c9cb9` supplies the
pinned offline runtime. Its normal native four-suite gate ran all 425 tests:
415 passed, 10 failed. Eight failures were socket-dependent tests; two relied
on ambient engine availability. See `~/.watchtower/evidence/WT-86/full-gate.log`.

The continuation preserves the complete four required suites and every
behavior assertion. `tests/portable_transports.py` uses real TCP/UDS sockets
when allowed, and chooses memory transport only on EPERM/EACCES. Unexpected
socket errors still fail. The smoke tests feed serialized HTTP through the
real `BaseHTTPRequestHandler` parser, routes and response serializer. The
peer tests exercise the real frame writer, endpoint lookup, connect/write/
half-close protocol, receipt check and fallback. The dashboard lifecycle test
still starts/stops a real child and checks idempotent PID reuse; when sockets
are denied it asserts the exact foreground-server argv and detached launch
options before substituting a harmless sleeper. This is process-management
evidence, not evidence that a denied HTTP listener started.

Each socket-dependent case is also parameterized to force memory transport
on unrestricted hosts, so those branches cannot silently rot. The existing
native boundary tests independently require TCP/IPv6 denial and Unix bind/
connect denial. No production launcher, policy, roots, gate command or suite
list changes. Engine assumptions are pinned inside the two affected tests.
Host smoke+lifecycle: **387 passed**, including eight additional memory
cases, `/home/hermes/wt61-9aafe8b3-portable.log`. The unchanged full native gate
is run through `scripts/sandbox_test_commit.py`; its receipt is retained at
`/home/hermes/wt61-9aafe8b3-native-gate.log` (inspect the result, not just the
existence of this path).

The Antigravity baseline review has not produced its report after repeated
provider stream timeouts, HTTP 503 and EOF retries; **OPS-645** records that
infrastructure gap. Both baseline report assessment and revised combined
security review remain required before integration. WT-61 stays open,
source-only and default-off; neither portability nor the runtime alone clears
that hold. Decision D557 records this reversible test implementation choice.

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

1. **Safe gated workflows.** Isolated command gates, Codex verifiers and
   assessments are implemented and both real transports passed the disposable
   gated fixture below. Review the combined implementation and race checks;
   never skip required gates or run hostile tests with daemon authority.
   Planner/plan-reviewer and non-Codex stage targets explicitly fail closed;
   they have no isolated transport and are never silently replaced. A required
   human review remains pending after a verifier pass. Generic resumed/delegated
   turns remain refused unless the exact restricted policy is retained.
2. **Integration.** A local fixture release ref is test evidence only; real
   tickets require fetched remote release ancestry. The source hold remains
   until the combined security review is resolved. No live policy enablement
   is authorized by these fixture results.
3. **Combined security review.** Review final root/path/config races,
   source replacement and launch/proof records; verify destination pinning
   and protected runtime/control paths. Credentials must remain outside
   explicit runtime read roots; operator-provided extra read roots require
   review. Restriction tombstones and launch records still grow, and receipt
   scans are capped. Mac trusted-prefix handling has unit coverage but no
   native macOS acceptance; fail closed if enforcement cannot be established.

## Combined checkpoint — worker wt-65ecd04d, 2026-11-16

Private branch `wt-61-wt-65ecd04d`, worktree
`/home/hermes/Apps/watchtower-wt61-65ecd04d`, combines parent `a7e2c2d`,
policy lane `1ae673c` and host-fence lane `e7517e7`. Integration now preserves
patch bytes through export and adoption (including binary payloads and
non-UTF-8 filenames), passes the worker id to Git preparation on relaunch,
and checks store project configuration in both launchers. Fence fixtures
keep their workspaces outside the entire trusted worker-control directory.

Actual-model acceptance first exposed `code-mode host is disabled` in both
transports: the selected model routes shell commands through this dispatcher.
The candidate now retains Codex's default Code Mode host configuration,
while nested tools retain the native profile and external tools stay disabled
(decision D448). No writable/readable roots or network permission were added.
Revised runs both passed forged-proof rejection, boundary probes, local test
and commit, real broker handoff, trusted adoption and final fixture close:

| Transport | Retained evidence | Adopted fixture commit |
|---|---|---|
| exec | `/home/hermes/wt-sandbox-acceptance-ifyiea71/controller/verdict.json` | `4029a91ae759a741a73f33de03d69e0e7032d2d1` |
| app-server | `/home/hermes/wt-sandbox-acceptance-sq4ardcq/controller/verdict.json` | `ff0a2cc8523db513becb2eb8fa42774d0ab0739a` |

These are disposable local-release fixtures, not evidence of integration on
the real release branch. The complete gated workflow and final combined
security review remain open; retain the default-off source hold.

The earlier stages lane (`80bde4e0-af13-418b-9885-8c7bb6befe44`) produced no
code or commit. Its transcript records a Bash prototype denial at
`2026-11-16T02:06:50.303Z` (tool `toolu_01Wk3y9jm8rYe8r8ZFaAvkrM`):
"The user doesn't want to take this action right now. STOP what you are doing
and wait for the user to tell you how to proceed." The record does not identify
the denial's source. That command included a scratch-directory `rm -rf *`;
resumed implementation must use fresh disposable directories and bounded
cleanup. Do not treat the earlier lane as pending completed work or retry its
denied prototype. On 2026-11-17 Amir answered **ok** in the original session;
the worker recorded **[Q1121:wt61-stage-hold] answered** on WT-61. This resumes
the remaining source-only work with those cleanup limits, keeps live policy
disabled, and retains the final combined security review before integration.

## Resumed source work — 2026-11-17

The private candidate was refreshed against `origin/main` at merge checkpoint
`9d94cad`; both the sandbox contract and upstream release expectation arguments
are preserved. The source materializer is `27a61ec`, stage/result transport is
`502070f`, and command gate integration is `7869d43` (child `72bf494`). Required
push checks passed for each checkpoint (425 tests). The stage lane
`ce5712aa-033c-4afc-861c-d89d0f17d4c7` also pushed gate snapshot hardening
`db06309`, adopted as `71014c8`. Later hardening pins source directory identities
for gates and stages and rejects edited ticket requirements during slow gates.

Command gates materialize a full commit from the trusted integration checkout's
object stores inside a fresh native sandbox, never the worker's writable Git
metadata. Gates get system PATH, masked `.git`/`.codex`/`.agents`, no network and
only fresh workspace/store writes. A shared 1500-second gate budget is below
the broker close request's 1800-second timeout. A worker tool timeout can still
leave an indeterminate receipt; inspect that receipt rather than replay blindly.

Supervised verifier/assessor launches retain their configured Codex model and
transport. Their trusted scope binds source store, ticket, queue, stage key,
attempt, full commit, request/clarification/accepted-plan context and cycle/token.
Results recheck the scope under the source write lock; assessment operations
repeat that check at every write. Follow-ups can only create new icebox items
in the assigned queue, never comment on an existing ticket selected by the
stage. A rejected verifier releases the ticket to the restricted builder pool
with feedback and clears the rejected resolution. Group completion also
requires the current child commit map to match the integration proof.

The acceptance runner's opt-in `--gated` mode configures a disposable bug
ticket with a command gate, an explicit Codex verifier and a post-fix assessor.
After reviewed patch adoption, the controller selects the trusted integration
checkout and drives only that fixture's stage supervision until a bounded
deadline. It requires both recorded gate passes and a completed assessment.
The role choices are fixture configuration, not permission to replace live
non-Codex targets. Earlier ungated verdicts do not establish stage support.

The revised runner's exec smoke passed at
`/home/hermes/wt-sandbox-acceptance-9laa0ym7/controller/verdict.json`, adopted
fixture commit `39335d96bf6c5994f6ffc90640124debc9c636f2`. It checks the existing
boundary/proof/handoff/adoption flow after the runner changes. Seven focused
runner tests cover assessment completion, failed/blocked stage termination,
the bounded deadline, and cleanup rejecting peer or reused PID records.

Real gated runs passed command and independent verifier gates, then completed
a separate post-fix assessment after reviewed patch adoption:

| Transport | Retained evidence | Adopted fixture commit |
|---|---|---|
| exec | `/home/hermes/wt-sandbox-acceptance-3qvpdch8/controller/verdict.json` | `baaba8c0279241da03bccce4c28bdb55d16a7c2b` |
| app-server | `/home/hermes/wt-sandbox-acceptance-3a0t9fn4/controller/verdict.json` | `b8b4b45b74ea79fa315795a5ca718ebf8d48f20f` |

The first exec gate run exposed Codex's checkout check; isolated stage launch
now explicitly uses `--skip-git-repo-check`. A second run attempted to inspect
the bootstrap Git alternate, which is intentionally unavailable to the model.
The prompt now explains the controller-verified full-commit materialization
and manifest; neither read roots nor network were widened. The verifier checks
the snapshot's behavior and local tests. Controller provenance checks and
native pinned-source tests supply the separate commit-binding evidence.
Later context/rejection regressions passed 135 focused tests, including changed
clarifications, inherited accepted plans, group proof and follow-up boundaries.

Review branch/worktree: `wt-61-wt-65ecd04d`,
`/home/hermes/Apps/watchtower-wt61-65ecd04d`. No live policies, customer sends,
credentials, send switches or services were changed. Source remains deliberately
held off main until the remaining workflows and final review are complete.

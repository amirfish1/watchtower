# Sandbox patch adoption safety (WT-61 F2/F3/F4/F7)

Status: **experimental source, WT-61 acceptance still open, security review
still required.** This note is the supported workflow for turning a sandbox
builder's handoff into an integrated commit without running its bytes with the
daemon's authority. It adds to `docs/codex-sandbox.md`; it does not replace it.

## F2 — sandbox origin is sticky

The first claim of a ticket by a restricted claimant stamps an immutable
`sandbox_origin` on the ticket, inside the same source write lock as the claim
(`queue._stamp_sandbox_origin_unlocked`, called from every claim path through
`_stamp_claim_unlocked`: `claim_next`, `claim_by_ref`, `update_status`,
`reopen`). Nothing clears it. `queue._sandbox_restricted` is true when any of
these holds, so a trusted reclaim or a disabled queue policy no longer turns
command gates, the adoption close, verifier/assessor launch, assessment start or
group integration into host-authority work:

1. the queue policy is `workspace-write` today;
2. the ticket carries `sandbox_origin`;
3. legacy backstop for tickets claimed before the stamp: any worker or session
   named in the ticket's history, claimant or parked worker carries a write-once
   restriction tombstone;
4. the current claimant is restricted (live-policy fallback of
   `sandbox_bridge.restricted_session`);
5. a group parent with any member meeting 1–4.

An unreadable tombstone ledger, a ledger directory replaced by a file, or any
lookup error answers *restricted*. Tickets never touched by a restricted worker
in an unrestricted queue stay full-access. Regression tests:
`tests/test_sandbox_origin.py` (each consumer, plus the review's exact
exploit).

Not covered: a ticket first claimed by a restricted worker *before* its
tombstone existed and whose history names no restricted principal. No such
evidence can exist to find; the sticky stamp prevents new instances.

## F3 — what adoption refuses and how to test and push safely

`scripts/adopt_sandbox_patch.py` applies bytes as data and creates one local
commit. It never runs proposed code. It now also:

- **Refuses** any path (at any depth, case-, Unicode- and trailing-dot/space
  insensitive) naming agent instructions or config (`AGENTS.md`, `CLAUDE.md`,
  `.claude/`, `.codex/`, `.agents/`, `.mcp.json`, `.cursor*`, …), hook runners
  (`.githooks/`, `.husky/`, `.pre-commit-config.yaml`, lefthook), CI
  (`.github/`, `.gitlab-ci.yml`, Jenkinsfile, …), environment/editor/tool
  loaders (`.envrc`, `.vscode/`, `.devcontainer/`, `*.pth`,
  `sitecustomize.py`, `conftest.py`, `pytest.ini`, `tox.ini`, `setup.cfg`),
  the hook chain and adoption machinery (`scripts/pre-push.sh`,
  `scripts/install-hooks.sh`, `scripts/adopt_sandbox_patch.py`,
  `scripts/sandbox_test_commit.py`) and `watchtower/sandbox_*.py`. Symlinks and
  submodules are refused too. The authoritative list is
  `sandbox_handoff.PROTECTED_*`.
  A refused change is not lost: read the exported patch and, if the edit is
  wanted, make it by hand in your own reviewed commit.
- **Fails closed on the push hook chain** (`push_hook_problem`): adoption is
  refused when an executable `pre-push` hook (effective `core.hooksPath` or the
  common hooks dir) would run adopted tests on the host. A hook is acceptable if
  it carries the marker `wt-sandbox-aware-prepush` or delegates to a committed
  `scripts/pre-push.sh` that does.
- Writes a trailer to the adoption commit:
  `WT-Sandbox-Adopted: REF worker=W patch-sha256=…`. Before applying bytes,
  it also writes a sticky `watchtower-sandbox-origin` marker into the trusted
  common Git directory. This is outside commit history and shared by linked
  worktrees, so rewriting/dropping the trailer cannot remove the restriction
  within that destination repository.

### Supported workflow

1. `python3 scripts/adopt_sandbox_patch.py --worker W --commit SHA --destination CLEAN_WORKTREE`.
2. Read the commit's bytes. Reading is not running.
3. **Never run its tests, scripts, hooks or build tools on the host** (pytest,
   `make`, `npm`, `python setup.py`, an IDE opening the tree). Test it only with
   `python3 scripts/sandbox_test_commit.py --repo CLEAN_WORKTREE --commit FULL_SHA --command 'CMD'`
   run from a trusted WatchTower checkout (not from the adopted worktree).
   It materializes the commit from trusted objects into a fresh sandbox
   (no network, only scratch writable) and never falls back to the host.
4. Push with the repository's real hooks (never `--no-verify`). The sandbox-aware
   `scripts/pre-push.sh` checks every pushed ref. A sticky repository origin marker, a range containing
   a `WT-Sandbox-Adopted:` trailer, or an unreadable range runs the full required
   workers lifecycle, smoke, queue and argparse UX gate
   through the deployed WatchTower (`wt deploy --json`) in the sandbox. A missing
   deployment, an unusable sandbox or a failing gate blocks the push.
5. Resolve the original ticket with the integrated commit; its required gates
   run in the sandbox because of F2.

### Remaining acceptance gap (not closed by this change)

- **Host agent discipline is policy, not mechanism.** An integration agent
  with a host shell can still choose to run adopted bytes. Adoption output,
  this document and the refusal list reduce that; nothing prevents it. The
  hook chain covers pushes only.
- **Sandbox cannot run this repo's pytest suite on a host whose system
  `python3` has no pytest** (observed on this host). The adopted-push gate then
  fails closed. Fixes: install pytest system-wide outside the sandbox roots or provide a reviewed offline runtime
  within the existing disposable roots. Tracked as OPS-643; native evidence is
  `/home/hermes/wt61-c670215d-pytest-runtime.log`. No roots were widened and no
  packages installed. An environment override cannot replace required suites.
- **Cross-repository manual copying.** The sticky Git metadata marker survives
  history rewriting and linked worktrees in the supported destination. An
  unrestricted operator manually copying the bytes into a different repository
  is outside that boundary; it still requires the same sandboxed test workflow.
  A marker must not be removed while adopted bytes remain.
- **Semantic review remains required.** Adopted source that changes
  application logic runs with host authority once integrated and deployed. The
  protected-path list only keeps it from auto-loading into privileged contexts
  before review.
- **Generic external repositories** whose pre-push hook is not marker-aware are
  refused at adoption. Supported alternative: a human applies the reviewed
  change by hand in a checkout whose hook chain is understood.

## F4 — adoption ledger scan

The overlap scan now opens the trusted authority directory without following
links, caps entries at `sandbox_bridge.LEDGER_SCAN`, reads each `*.json` scope
through `sandbox_bridge._read` (regular file, one link, bounded size and
depth), and turns any unreadable, malformed, oversized, linked or incomplete
entry into a `ValueError` naming it. The CLI prints `adoption refused: …` and
exits 1; no traceback. Tests: `tests/test_sandbox_adoption_safety.py`.

## F7 — peer workspaces may overlap (assessment, not changed here)

`sandbox_bridge._check_roots` allows two restricted workers to share or nest a
workspace. Queue drains register every worker with the queue's configured
`repo_path`, so shared workspaces are the normal topology today; refusing
overlap in `register()` would break multi-worker queues and relaunch (a worker's
own prior scope matches itself). It is not part of first-claim scope. Impact is
bounded: one worker can alter bytes another later commits, but those bytes reach
the host only through the handoff, which is path-checked, adopted as data and
tested in the sandbox. Proposed hunk for `sandbox_bridge._check_roots`, opt-in
until per-worker worktrees are the default:

```python
        if (os.environ.get("WATCHTOWER_SANDBOX_EXCLUSIVE_WORKSPACES") == "1"
                and peer.get("worker") != worker and
                (_under(workspace, peer_repo) or _under(peer_repo, workspace))):
            raise ValueError("sandbox workspace overlaps another worker's workspace")
```
(`_check_roots` would need the registering worker's id as a parameter.)
Documentation of the shared-workspace trust model belongs in
`docs/codex-sandbox.md`.

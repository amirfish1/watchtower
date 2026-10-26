# WatchTower

Run fleets of AI coding-agent workers, unattended: tickets go into named
queues on a durable append-only JSON store; workers drain them automatically.
Stdlib-only Python, zero runtime dependencies — keep it that way.

## Shared clone rules

This checkout is a shared clone on `main`; several agent sessions work in it
concurrently.

- Never create or switch branches here. Need a branch? `git worktree`.
- Stage by explicit path: `git commit --only <paths> -m "type(scope): msg"`.
  Never `git add -A`, `git add .`, or `git commit -a`. New files: `git add`
  the exact paths first, then `git commit --only`.
- Never `git checkout --`, `git restore .`, `git clean -f`, or
  `git reset --hard` without asking.
- Conventional commits; subject under ~70 chars.

## Commit means push

Every commit is pushed immediately: `git push origin main`. Deployed hosts
sync from origin, so an unpushed commit is undeployed. The pre-push hook
(`scripts/pre-push.sh`, installed via `scripts/install-hooks.sh`) runs the
test gate — if it fails, fix it or don't commit. Never `--no-verify`, never
force-push `main`.

## Deployment and hooks (don't guess)

- Use `wt deploy --json` to locate and inspect the checkout imported by the
  running `wt`, then `wt deploy --sync` to update it. The guarded sync refuses
  novel tracked edits or local commits and fast-forwards clean installations;
  it can restore recognized stale files. Do not bypass it with a raw pull.
  Install paths vary by host; `~/.ccc/watchtower` is not universal.
- Hooks are not in the worktree. In a `git worktree`, `.git` is a file, so
  `.git/hooks/` does not exist there and `scripts/install-hooks.sh` (which
  writes `$REPO_ROOT/.git/hooks/pre-push`) is for the main clone only. Hooks
  are shared via `git rev-parse --git-common-dir`; run
  `scripts/install-hooks.sh` once from the main clone, not from a worktree.

## Tests

```bash
python3 -m pytest tests -q              # full suite (~100 s)
python3 -m pytest tests/test_queue.py   # one file
```

There is no `tests/test_stages.py` (a verifier collected nothing running it,
OPS-1298). Pick the file by topic:

| Topic | File |
|---|---|
| Stage sessions (planner/verifier/assessor session lifecycle) | `tests/test_stage_sessions.py` |
| Gates (plan / verify / review stages, `--gate`) | `tests/test_gates.py` |
| Product gate | `tests/test_product_gate.py` |
| Post-fix assessment (WT-21, `wt assess`) | `tests/test_assessment.py` |
| Workers (spawn, release, retirement, `wt workers`) | `tests/test_workers_lifecycle.py` (there is no `tests/test_workers.py`) |
| Claim ownership / liveness | `tests/test_claim_owner.py`, `tests/test_liveness_*.py` |
| Outbox / reclaim-resume | `tests/test_messages.py`, `tests/test_reclaim_resume.py` |

Unsure? `ls tests | rg <topic>` or `python3 -m pytest --collect-only -q tests -k <word>`.

Python 3.12+, pytest 7+. No other dependencies.

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

## Tests

```bash
python3 -m pytest tests -q              # full suite (~100 s)
python3 -m pytest tests/test_queue.py   # one file
```

Python 3.12+, pytest 7+. No other dependencies.

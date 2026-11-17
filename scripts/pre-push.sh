#!/usr/bin/env bash
# Pre-push test gate. Runs the fast slice of the suite before any push:
# workers lifecycle + queue + CLI smoke + argparse UX (339 tests, ~7s).
# The full suite (~100 s) is too slow to gate every push on; CI / a manual
# `pytest tests -q` covers the rest.
#
# Install pytest before pushing; an unavailable gate must stop the push.
#
# wt-sandbox-aware-prepush (marker read by watchtower.sandbox_handoff): commits
# adopted from a sandbox builder carry a "WT-Sandbox-Adopted:" trailer. Their
# bytes are untrusted, so they are tested ONLY in the restricted sandbox via the
# deployed WatchTower (never this worktree's code), and a sandbox that cannot
# run them blocks the push. There is no host fallback and no --no-verify path.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

ZERO="0000000000000000000000000000000000000000"
adopted_shas=()
common="$(git rev-parse --git-common-dir)"
[[ "$common" == /* ]] || common="$REPO_ROOT/$common"
sandbox_origin=false
# Sticky metadata survives amend/squash and applies to every linked worktree.
# An unreadable or linked marker still means restricted.
if [ -e "$common/watchtower-sandbox-origin" ] || [ -L "$common/watchtower-sandbox-origin" ]; then
  sandbox_origin=true
fi
while read -r _lref local_sha _rref remote_sha || [ -n "${local_sha:-}" ]; do
  [ -n "${local_sha:-}" ] && [ "$local_sha" != "$ZERO" ] || continue
  if [ "${remote_sha:-$ZERO}" = "$ZERO" ]; then
    range=("$local_sha" --not --remotes)
  else
    range=("${remote_sha}..${local_sha}")
  fi
  # An unreadable range is treated as adopted: fail toward the sandbox.
  if [ "$sandbox_origin" = true ] \
     || ! msgs="$(git log --format=%B "${range[@]}" 2>/dev/null)" \
     || [[ $'\n'"$msgs" == *$'\nWT-Sandbox-Adopted: '* ]]; then
    adopted_shas+=("$local_sha")
  fi
done

if [ "${#adopted_shas[@]}" -gt 0 ]; then
  echo "pre-push: adopted sandbox commit in range; testing in the restricted sandbox…"
  TRUSTED="$(wt deploy --json 2>/dev/null | python3 -c 'import json,sys; print(json.load(sys.stdin)["path"])' 2>/dev/null || true)"
  if [ -z "$TRUSTED" ] || [ ! -f "$TRUSTED/scripts/sandbox_test_commit.py" ]; then
    echo "pre-push: no trusted WatchTower deployment found (wt deploy --json); push blocked." >&2
    exit 1
  fi
  for adopted_sha in "${adopted_shas[@]}"; do
  [ -n "$(git rev-parse --verify -q "$adopted_sha^{commit}")" ] || { echo "pre-push: cannot resolve $adopted_sha; push blocked." >&2; exit 1; }
  if ! (cd "$TRUSTED" && python3 -I scripts/sandbox_test_commit.py --repo "$REPO_ROOT" \
        --commit "$(git -C "$REPO_ROOT" rev-parse "$adopted_sha^{commit}")" \
        --command 'python3 -m pytest tests/test_workers_lifecycle.py tests/test_smoke.py tests/test_queue.py tests/test_argparse_ux.py -q --tb=short'); then
    echo "❌ pre-push sandbox gate FAILED or could not run — push blocked (no host fallback)." >&2
    echo "   If the sandbox's system python3 lacks pytest, install it system-wide (outside the" >&2
    echo "   sandbox roots). The required four-suite gate cannot be replaced by an environment override." >&2
    echo "   Never --no-verify; see docs/sandbox-adoption-safety.md." >&2
    exit 1
  fi
  done
  echo "pre-push: sandbox gate passed ✓"
  exit 0
fi

if [ ! -d tests ]; then
  exit 0  # nothing to gate on this checkout
fi

PY=""
for candidate in "$REPO_ROOT/.venv/bin/python3" $(type -aP python3 2>/dev/null); do
  [ -x "$candidate" ] || continue
  if "$candidate" -c "import pytest" >/dev/null 2>&1; then
    PY="$candidate"
    break
  fi
done
if [ -z "$PY" ]; then
  echo "pre-push: no python3 with pytest found (checked .venv and PATH); push blocked." >&2
  echo "Install pytest in .venv or a python3 on PATH, then retry the push." >&2
  exit 1
fi

echo "pre-push: running test gate…"
if ! "$PY" -m pytest \
     tests/test_workers_lifecycle.py \
     tests/test_smoke.py \
     tests/test_queue.py \
     tests/test_argparse_ux.py \
     -q --tb=short; then
  echo ""
  echo "❌ pre-push test gate FAILED — fix the tests or don't commit."
  exit 1
fi
echo "pre-push: test gate passed ✓"

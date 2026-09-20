#!/usr/bin/env bash
# Pre-push test gate. Runs the fast slice of the suite before any push:
# workers lifecycle + queue + CLI smoke + argparse UX (339 tests, ~7s).
# The full suite (~100 s) is too slow to gate every push on; CI / a manual
# `pytest tests -q` covers the rest.
#
# Bypass for a genuine emergency: git push --no-verify
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

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
[ -z "$PY" ] && { echo "pre-push: no python3 with pytest found (checked .venv and PATH), skipping test gate"; exit 0; }

echo "pre-push: running test gate…"
if ! "$PY" -m pytest \
     tests/test_workers_lifecycle.py \
     tests/test_smoke.py \
     tests/test_queue.py \
     tests/test_argparse_ux.py \
     -q --tb=short; then
  echo ""
  echo "❌ pre-push test gate FAILED — fix the tests or don't commit."
  echo "   (Emergency bypass: git push --no-verify)"
  exit 1
fi
echo "pre-push: test gate passed ✓"

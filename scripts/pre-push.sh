#!/usr/bin/env bash
# Pre-push test gate. Runs the fast slice of the suite before any push:
# workers lifecycle + queue + CLI smoke + argparse UX (339 tests, ~7s).
# The full suite (~100 s) is too slow to gate every push on; CI / a manual
# `pytest tests -q` covers the rest.
#
# Install pytest before pushing; an unavailable gate must stop the push.
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

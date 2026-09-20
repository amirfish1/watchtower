#!/usr/bin/env bash
# Install the pre-push hook shim into .git/hooks/pre-push.
# Idempotent: only writes when content differs.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HOOK="$REPO_ROOT/.git/hooks/pre-push"
SHIM='#!/usr/bin/env bash
# Shim → committed scripts/pre-push.sh (shared test gate). See that file.
DIR="$(git rev-parse --show-toplevel 2>/dev/null)"
[ -x "$DIR/scripts/pre-push.sh" ] && exec "$DIR/scripts/pre-push.sh" "$@"
exit 0'

mkdir -p "$(dirname "$HOOK")"
if [ -f "$HOOK" ] && cmp -s <(printf '%s\n' "$SHIM") "$HOOK"; then
  echo "pre-push hook already up to date"
  exit 0
fi
printf '%s\n' "$SHIM" > "$HOOK"
chmod +x "$HOOK"
echo "installed $HOOK"

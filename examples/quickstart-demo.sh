#!/usr/bin/env bash
# WatchTower quickstart demo: enqueue -> claim -> close -> `wt wait` unblocks.
#
# Runs against a throwaway queue store in a temp directory, so it never
# touches your real ~/.watchtower state. No CCC, no Claude/Codex CLI, and no
# network access required: this demonstrates the queue mechanic itself, the
# same primitive a real agent worker uses via `wt claim` / `wt close`.
#
# Usage:
#   ./examples/quickstart-demo.sh

set -euo pipefail

if ! command -v wt >/dev/null 2>&1; then
    echo "error: 'wt' is not on PATH. Install it first:" >&2
    echo "  pip install -e ." >&2
    exit 1
fi

DEMO_DIR="$(mktemp -d)"
export WATCHTOWER_STORE="$DEMO_DIR/queues.json"
export WATCHTOWER_WORKERS_FILE="$DEMO_DIR/workers.json"
export WATCHTOWER_CONFIG_FILE="$DEMO_DIR/queue-config.json"
export WATCHTOWER_ACTIVITY_LOG="$DEMO_DIR/activity.log"
trap 'rm -rf "$DEMO_DIR"' EXIT

QUEUE="DEMO"

section() { printf '\n=== %s ===\n' "$1"; }

section "1. File a ticket into a fresh queue"
wt add -q "$QUEUE" --title "Fix the login page" --type bug

section "2. wt wait blocks until the queue drains (running in the background)"
wt wait -q "$QUEUE" --timeout 30 &
WAIT_PID=$!
sleep 1
echo "wt wait is now blocked (pid $WAIT_PID), waiting for $QUEUE to empty..."

section "3. A worker claims the ticket"
sleep 1
wt claim -q "$QUEUE" --worker demo-worker

section "4. The worker closes it, recording how it was fixed"
sleep 1
wt close "${QUEUE}-1" --worker demo-worker --summary "fixed the login redirect bug"

section "5. wt wait unblocks now that the queue is empty"
wait "$WAIT_PID"
echo "wt wait exited $?: queue drained."

echo
echo "That's the whole loop: enqueue, a worker claims and closes, wt wait"
echo "unblocks. Swap the manual claim/close above for a real agent worker"
echo "(wt drain on $QUEUE) and the same mechanic runs unattended."

#!/usr/bin/env python3
"""Test one commit in the restricted sandbox, never on the host.

Used by operators and by the sandbox-aware pre-push gate for commits adopted
from a sandbox builder. The command runs in a fresh materialization of the
commit (no network, only fresh scratch writable). A setup problem is a failure;
there is no host fallback. Run this from a TRUSTED WatchTower checkout, never
from the worktree that holds the adopted bytes.
"""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from watchtower.sandbox_handoff import sandbox_test  # noqa: E402

if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--repo", required=True, help="repository or worktree holding the commit")
    p.add_argument("--commit", required=True, help="full commit id")
    p.add_argument("--command", required=True, help="shell command run inside the sandbox")
    p.add_argument("--timeout", type=float, default=None)
    args = p.parse_args()
    try:
        result = sandbox_test(args.repo, args.commit, args.command, args.timeout)
    except (ValueError, OSError) as exc:
        p.exit(2, "sandbox test refused: " + str(exc) + "\n")
    print(json.dumps({k: result.get(k) for k in
                      ("gate", "passed", "exit_code", "commit", "seconds", "sandboxed")}, indent=2))
    sys.stderr.write(str(result.get("output_tail") or "") + "\n")
    sys.exit(0 if result.get("passed") else 1)

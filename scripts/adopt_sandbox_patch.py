#!/usr/bin/env python3
"""Adopt a reviewed sandbox handoff into an operator-supplied clean worktree.

This creates a local commit only. Review the patch first. Never run the
adopted tests, scripts or hooks on the host: use scripts/sandbox_test_commit.py.
Integration, push and the original ticket's resolution remain explicit.
Patches touching agent/tool/hook/CI paths are refused; see
docs/sandbox-adoption-safety.md for the supported workflow.
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from watchtower.sandbox_handoff import adopt

if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--worker", required=True)
    p.add_argument("--commit", required=True)
    p.add_argument("--destination", required=True)
    args = p.parse_args()
    try:
        print(json.dumps(adopt(args.worker, args.commit, args.destination), indent=2))
    except (ValueError, OSError, KeyError, TypeError, subprocess.SubprocessError) as exc:
        p.exit(1, f"adoption refused: {type(exc).__name__}: {exc}\n")

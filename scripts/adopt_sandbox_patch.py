#!/usr/bin/env python3
"""Adopt a reviewed sandbox handoff into an operator-supplied clean worktree.

This creates a local commit only. Review the patch first; run tests only after
review. Integration, push and the original ticket's resolution remain explicit.
"""
import argparse
import json
from pathlib import Path
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
    except (ValueError, OSError) as exc:
        p.exit(1, str(exc) + "\n")

#!/usr/bin/env python3
"""WT-24 step 0: try to reproduce the 2026-09-30 silent stage-session deaths.

Hypothesis: a burst of concurrent ``claude -p`` starts (three spawned within
~10 s at 16:04:49Z) makes some of them exit at once with a 0-byte log.

    scripts/repro_concurrent_spawn.py [--n 4] [--rounds 3] [--cmd 'claude -p hi']

Starts N copies of the command at the same instant (burst) and then staggered
by 3 s (what ``workers._spawn_gate`` now enforces), and prints how many died
within 20 s with an empty log. Needs a logged-in engine, so it is a manual
tool, not part of the test suite. Stdlib only.
"""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def run_round(cmd: list, n: int, gap: float, watch_s: float) -> list:
    tmp = Path(tempfile.mkdtemp(prefix="wt-repro-"))
    procs = []
    for i in range(n):
        log = tmp / f"{i}.log"
        f = open(log, "wb")
        procs.append((subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT,
                                       start_new_session=True), log))
        if gap:
            time.sleep(gap)
    end = time.time() + watch_s
    while time.time() < end and any(p.poll() is None for p, _ in procs):
        time.sleep(0.5)
    out = []
    for p, log in procs:
        rc = p.poll()
        size = log.stat().st_size
        if rc is None:
            p.terminate()
        out.append({"rc": rc, "log_bytes": size, "silent_death": rc not in (None, 0) and size == 0})
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=4)
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--cmd", default="claude -p 'reply with OK'")
    ap.add_argument("--watch", type=float, default=20.0)
    a = ap.parse_args()
    cmd = shlex.split(a.cmd)
    for label, gap in (("burst", 0.0), ("staggered", 3.0)):
        silent = total = 0
        for _ in range(a.rounds):
            for r in run_round(cmd, a.n, gap, a.watch):
                total += 1
                silent += r["silent_death"]
        print(f"{label:10s} {silent}/{total} silent deaths (non-zero exit, empty log)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

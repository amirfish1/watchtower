"""Exit-forensics wrapper for stage sessions (WT-24).

    python _exitwrap.py --exit-file <path> -- argv...

Writes ``{wrapper_pid, started_at}`` to the exit file, runs ``argv`` on the
same stdio, forwards SIGTERM/SIGINT to the child, then writes
``{rc, signal, ended_at, child_pid}``. The recorded worker pid is this
wrapper's, and the spawn keeps ``start_new_session``, so ``killpg`` covers both
processes. A wrapper killed together with its child leaves no ``ended_at``,
which the stage watcher reads as "killed along with the wrapper".

Stdlib only; imports nothing from watchtower so it runs from any cwd.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time


def _iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _write(path: str, payload: dict) -> None:
    tmp = f"{path}.tmp{os.getpid()}"
    try:
        with open(tmp, "w") as f:
            json.dump(payload, f)
        os.replace(tmp, path)
    except OSError:
        pass


def main(argv: list) -> int:
    if "--" not in argv or "--exit-file" not in argv:
        print("usage: _exitwrap.py --exit-file PATH -- argv...", file=sys.stderr)
        return 2
    sep = argv.index("--")
    exit_file = argv[argv.index("--exit-file") + 1]
    cmd = argv[sep + 1:]
    if not cmd:
        return 2
    state = {"wrapper_pid": os.getpid(), "started_at": _iso()}
    _write(exit_file, state)
    try:
        child = subprocess.Popen(cmd)
    except OSError as exc:
        _write(exit_file, dict(state, rc=127, signal=0, ended_at=_iso(), child_pid=0,
                               error=str(exc)))
        return 127

    def _forward(signum, _frame):
        try:
            child.send_signal(signum)
        except OSError:
            pass

    signal.signal(signal.SIGTERM, _forward)
    signal.signal(signal.SIGINT, _forward)
    rc = child.wait()
    sig = -rc if rc < 0 else 0
    _write(exit_file, dict(state, rc=rc, signal=sig, ended_at=_iso(), child_pid=child.pid))
    return 128 + sig if sig else rc


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

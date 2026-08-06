"""Tests for _maybe_self_update: the daemon's start-time ``git pull --ff-only``.

Locks in the behavior that keeps long-running daemons from rotting (a
production VM ran 44 commits behind because nothing in the start path ever
pulled):
  1. WT_NO_SELF_UPDATE=1 -> no git calls at all.
  2. Non-git install (pipx/venv, no .git) -> quietly skipped.
  3. Pull that leaves HEAD unchanged -> no re-exec.
  4. Pull that moves HEAD -> re-exec with the same interpreter + argv.
  5. Failed pull -> daemon starts anyway with the code it has.

No real git is invoked: subprocess.run is faked, os.execvp is recorded.
"""

from __future__ import annotations

import sys

import pytest


class _Result:
    def __init__(self, returncode=0, stdout=""):
        self.returncode = returncode
        self.stdout = stdout


@pytest.fixture()
def harness(monkeypatch):
    """Patch cli.subprocess.run / os.execvp / q._log; return a control object."""
    from watchtower import cli

    state = {"calls": [], "exec": None, "heads": []}

    def fake_run(cmd, **kwargs):
        state["calls"].append(cmd)
        joined = " ".join(cmd)
        if "--show-toplevel" in joined:
            if state.get("toplevel") is None:
                return _Result(1, "")
            return _Result(0, state["toplevel"] + "\n")
        if "rev-parse HEAD" in joined:
            return _Result(0, state["heads"].pop(0) + "\n")
        if "pull --ff-only" in joined:
            return _Result(state.get("pull_rc", 0), "")
        raise AssertionError(f"unexpected git call: {cmd}")

    def fake_execvp(exe, argv):
        state["exec"] = (exe, argv)

    monkeypatch.setattr(cli.subprocess, "run", fake_run)
    monkeypatch.setattr(cli.os, "execvp", fake_execvp)
    monkeypatch.setattr(cli.q, "_log", lambda *a, **k: True)
    monkeypatch.delenv("WT_NO_SELF_UPDATE", raising=False)
    return cli, state


def test_opt_out_env_skips_git_entirely(harness, monkeypatch):
    cli, state = harness
    monkeypatch.setenv("WT_NO_SELF_UPDATE", "1")
    cli._maybe_self_update()
    assert state["calls"] == []
    assert state["exec"] is None


def test_non_git_install_is_skipped(harness):
    cli, state = harness
    state["toplevel"] = None  # rev-parse fails: pipx/venv install
    cli._maybe_self_update()
    assert len(state["calls"]) == 1  # only the toplevel probe
    assert state["exec"] is None


def test_unchanged_head_does_not_reexec(harness):
    cli, state = harness
    state["toplevel"] = "/repo"
    state["heads"] = ["aaa111", "aaa111"]
    cli._maybe_self_update()
    assert state["exec"] is None


def test_moved_head_reexecs_with_same_argv(harness, monkeypatch):
    cli, state = harness
    state["toplevel"] = "/repo"
    state["heads"] = ["aaa111", "bbb222"]
    monkeypatch.setattr(sys, "argv", ["wt", "start", "--foreground", "--auto-spawn"])
    cli._maybe_self_update()
    exe, argv = state["exec"]
    assert exe == sys.executable
    assert argv == [sys.executable, "-m", "watchtower.cli", "start", "--foreground", "--auto-spawn"]


def test_failed_pull_still_starts_daemon(harness):
    cli, state = harness
    state["toplevel"] = "/repo"
    state["heads"] = ["aaa111", "aaa111"]
    state["pull_rc"] = 1  # diverged / offline / dirty
    cli._maybe_self_update()  # must not raise
    assert state["exec"] is None

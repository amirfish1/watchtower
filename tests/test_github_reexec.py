"""OPS-555: same-PID daemon updates must finish GitHub writes before exec."""
import json
import threading

import pytest

from watchtower import cli, deploy, github_write_lock as lock
from test_github_body_writes import _setup
from test_github_backend import restore_watchtower_modules  # noqa: F401


@pytest.mark.parametrize("update", ["moved", "stale", "pull"])
def test_daemon_update_drains_real_body_transaction(tmp_path, monkeypatch, update):
    state, _, _, backend = _setup(tmp_path, monkeypatch)
    entered, release = threading.Event(), threading.Event()
    errors, snapshots = [], []

    def writer():
        try:
            with backend._body_lock():
                entered.set()
                assert release.wait(10), "test did not release transaction"
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=writer)
    thread.start()
    assert entered.wait(10)
    # Keep a real writer alive across the update request. Baseline execs
    # immediately; guarded exec waits until the transaction cleans up.
    timer = threading.Timer(2, release.set)
    timer.start()
    monkeypatch.setattr(cli.os, "execvp", lambda *a: snapshots.append(
        json.loads(state.read_text())["mutex_labels"]))
    monkeypatch.setattr(cli.q, "_log", lambda *a, **k: None)
    monkeypatch.delenv("WT_NO_SELF_UPDATE", raising=False)
    monkeypatch.setattr(deploy, "sync", lambda: {
        "action": "moved" if update == "moved" else "up-to-date",
        "was": "old", "head": "new", "dirty": [],
    })
    try:
        if update == "pull":
            heads = iter(["old", "new"])
            real_run = cli.subprocess.run

            def git(args, **kw):
                if args[0] != "git":
                    return real_run(args, **kw)
                from subprocess import CompletedProcess
                output = next(heads) if args[-2:] == ["rev-parse", "HEAD"] else "/repo"
                return CompletedProcess(args, 0, output, "")

            monkeypatch.setattr(cli.subprocess, "run", git)
            cli._maybe_self_update()
        else:
            cli._deploy_sync_tick("old")
        assert snapshots == [{}], "exec would abandon an active GitHub transaction"
    finally:
        release.set()
        timer.cancel()
        thread.join(10)
        timer.join(10)
    assert not thread.is_alive() and not errors


def test_busy_update_postpones_exec_and_retries_after_drain(tmp_path, monkeypatch):
    state, _, _, backend = _setup(tmp_path, monkeypatch)
    entered, release = threading.Event(), threading.Event()
    errors, execs = [], []

    def writer():
        try:
            with backend._body_lock():
                entered.set()
                assert release.wait(10)
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=writer)
    thread.start()
    assert entered.wait(10)
    monkeypatch.setattr(lock, "REEXEC_WAIT_SECONDS", 0)
    monkeypatch.setattr(cli.os, "execvp", lambda *a: execs.append(a))
    monkeypatch.setattr(cli.q, "_log", lambda *a, **k: None)
    monkeypatch.setattr(deploy, "sync", lambda: {
        "action": "up-to-date", "head": "new", "dirty": [],
    })
    try:
        cli._deploy_sync_tick("old")
        assert not execs
        assert lock.LABEL in json.loads(state.read_text())["mutex_labels"]
    finally:
        release.set()
        thread.join(10)
    assert not thread.is_alive() and not errors
    cli._deploy_sync_tick("old")
    assert len(execs) == 1
    # A returning/failed exec must also reopen admission for later writes.
    with backend._body_lock():
        assert lock.LABEL in json.loads(state.read_text())["mutex_labels"]

"""WT-59: `wt config --auto-drain on` autostart is opt-out, forwards host/port,
and reports a watcher that never came up."""

from __future__ import annotations

import importlib
import subprocess

import pytest


@pytest.fixture()
def cli(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(tmp_path / "activity.log"))
    monkeypatch.delenv("WT_NO_AUTOSTART", raising=False)
    import watchtower.queue as q
    import watchtower.cli as cli_mod
    importlib.reload(q)
    importlib.reload(cli_mod)
    monkeypatch.setattr(cli_mod, "DAEMON_PID_FILE", tmp_path / "daemon.pid")
    monkeypatch.setattr(cli_mod, "_service_installed", lambda: False)
    return cli_mod


class _Spy:
    def __init__(self, rc=0):
        self.calls = []
        self.rc = rc

    def __call__(self, cmd, **kw):
        self.calls.append(cmd)
        return type("P", (), {"poll": lambda s: self.rc, "pid": 1})()


def _run(cli, *argv):
    return cli.cmd_config(cli.build_parser().parse_args(["config", "-q", "AS", *argv]))


def test_no_autostart_flag_starts_nothing(cli, monkeypatch):
    spy = _Spy()
    monkeypatch.setattr(subprocess, "Popen", spy)
    assert _run(cli, "--auto-drain", "on", "--no-autostart") == 0
    assert spy.calls == []


def test_env_optout_starts_nothing(cli, monkeypatch):
    spy = _Spy()
    monkeypatch.setattr(subprocess, "Popen", spy)
    monkeypatch.setenv("WT_NO_AUTOSTART", "1")
    assert _run(cli, "--auto-drain", "on") == 0
    assert spy.calls == []


def test_host_port_forwarded_and_ready(cli, monkeypatch, capsys):
    spy = _Spy()
    monkeypatch.setattr(subprocess, "Popen", spy)
    monkeypatch.setattr(cli, "_daemon_pid_live", lambda: bool(spy.calls))
    assert _run(cli, "--auto-drain", "on", "--host", "127.0.0.1", "--port", "0") == 0
    assert spy.calls[0][-4:] == ["--host", "127.0.0.1", "--port", "0"]
    assert "service auto-started" in capsys.readouterr().out


def test_failed_start_is_reported(cli, monkeypatch, capsys):
    monkeypatch.setattr(subprocess, "Popen", _Spy(rc=1))
    assert _run(cli, "--auto-drain", "on") == 1
    assert "autostart failed" in capsys.readouterr().err

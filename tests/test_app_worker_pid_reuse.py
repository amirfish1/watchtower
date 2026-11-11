"""A reused PID after a container restart is not a live hosted worker (WT-65)."""

from __future__ import annotations

import json
import os

from watchtower import codex_app_worker as cw


def _write(tmp_path, monkeypatch, **rec):
    monkeypatch.setattr(cw, "registry_dir", lambda: tmp_path)
    sock = tmp_path / "w.sock"
    sock.write_text("")
    base = {"thread_id": "t1", "sock": str(sock), "pid": os.getpid(), "server_pid": os.getppid()}
    base.update(rec)
    (tmp_path / "t1.json").write_text(json.dumps(base))
    return tmp_path / "t1.json"


def test_reused_pid_whose_cmdline_is_not_our_server_is_dead(tmp_path, monkeypatch):
    path = _write(tmp_path, monkeypatch)  # pids alive, but not an app-server on our sock
    assert cw.find_worker("t1") is None
    assert not path.exists()


def test_start_token_mismatch_is_dead(tmp_path, monkeypatch):
    path = _write(tmp_path, monkeypatch, server_pid=0, pid_started="proc:1")
    assert cw.find_worker("t1") is None
    assert not path.exists()


def test_matching_record_is_alive(tmp_path, monkeypatch):
    from watchtower.stages import proc_start_token
    monkeypatch.setattr(cw, "_is_our_server", lambda pid, sock: True)
    _write(tmp_path, monkeypatch, pid_started=proc_start_token(os.getpid()))
    assert cw.find_worker("t1") is not None

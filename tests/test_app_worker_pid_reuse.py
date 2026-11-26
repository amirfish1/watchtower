"""A reused PID after a container restart is not a live hosted worker (WT-65)."""

from __future__ import annotations

import json
import os

from watchtower import codex_app_worker as cw, stages


def _write(tmp_path, monkeypatch, **rec):
    monkeypatch.setattr(cw, "registry_dir", lambda: tmp_path)
    sock = tmp_path / "w.sock"
    sock.write_text("")
    base = {"thread_id": "t1", "sock": str(sock), "pid": os.getpid(), "server_pid": os.getppid()}
    base.update(rec)
    (tmp_path / "t1.json").write_text(json.dumps(base))
    return tmp_path / "t1.json"


def _mock_proc_cmdline(monkeypatch, server_cmdline):
    # Exercise the platform probe and argv parser using synthetic /proc entries.
    probe = f"/proc/{os.getpid()}/cmdline"
    server_entry = cw.Path(f"/proc/{os.getppid()}/cmdline")
    exists = os.path.exists
    read_bytes = cw.Path.read_bytes

    def exists_with_proc(path):
        if path == probe:
            return server_cmdline is not None
        return exists(path)

    def read_with_proc(path):
        if path == server_entry:
            assert server_cmdline is not None, "cmdline probe ran without /proc"
            return server_cmdline
        return read_bytes(path)

    monkeypatch.setattr(cw.os.path, "exists", exists_with_proc)
    monkeypatch.setattr(cw.Path, "read_bytes", read_with_proc)


def test_reused_pid_whose_cmdline_is_not_our_server_is_dead(tmp_path, monkeypatch):
    _mock_proc_cmdline(monkeypatch, b"codex\0app-server\0--listen\0unix:///other.sock\0")
    path = _write(tmp_path, monkeypatch)
    assert cw.find_worker("t1") is None
    assert not path.exists()


def test_start_token_mismatch_is_dead(tmp_path, monkeypatch):
    monkeypatch.setattr(stages, "proc_start_token", lambda pid: "proc:2")
    path = _write(tmp_path, monkeypatch, server_pid=0, pid_started="proc:1")
    assert cw.find_worker("t1") is None
    assert not path.exists()


def test_matching_record_is_alive(tmp_path, monkeypatch):
    sock = tmp_path / "w.sock"
    _mock_proc_cmdline(monkeypatch, os.fsencode(f"codex\0app-server\0--listen\0unix://{sock}\0"))
    _write(tmp_path, monkeypatch)
    assert cw.find_worker("t1") is not None


def test_matching_start_token_is_alive_without_proc(tmp_path, monkeypatch):
    _mock_proc_cmdline(monkeypatch, None)
    monkeypatch.setattr(stages, "proc_start_token", lambda pid: "ps:start-time")
    path = _write(tmp_path, monkeypatch, pid_started="ps:start-time")
    assert cw.find_worker("t1") is not None
    assert path.exists()

"""A reused PID must not read as a live daemon (WT-62)."""

from __future__ import annotations

import os

from watchtower import stages


def test_own_pid_in_stale_record_is_not_live(tmp_path):
    pf = tmp_path / "daemon.pid"
    pf.write_text(str(os.getpid()))  # legacy record naming this very process
    assert stages.live_pid_from_record(pf) == 0
    assert stages.live_pid_from_record(pf, exclude_self=False) == os.getpid()


def test_reused_pid_with_different_start_token(tmp_path):
    pf = tmp_path / "daemon.pid"
    stages.write_pid_record(pf, os.getpid())
    assert stages.live_pid_from_record(pf, exclude_self=False) == os.getpid()
    (tmp_path / "daemon.pid.start").write_text("proc:1")  # old container's daemon
    assert stages.live_pid_from_record(pf, exclude_self=False) == 0


def test_dead_pid_and_legacy_record(tmp_path):
    pf = tmp_path / "daemon.pid"
    pf.write_text("999999999")
    assert stages.live_pid_from_record(pf) == 0
    pf.write_text(str(os.getppid()))  # legacy: no token file, alive -> live
    assert stages.live_pid_from_record(pf) == os.getppid()


def test_clear_removes_both(tmp_path):
    pf = tmp_path / "daemon.pid"
    stages.write_pid_record(pf, os.getpid())
    stages.clear_pid_record(pf)
    assert not pf.exists() and not (tmp_path / "daemon.pid.start").exists()

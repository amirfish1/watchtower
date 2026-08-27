# Copyright (c) 2026 Amir Fish. All rights reserved.
# SPDX-License-Identifier: LicenseRef-WatchTower-Software-License

"""wt snapshot CLI surface, via the in-process run_cli fixture."""

from pathlib import Path
from watchtower import snapshot


def test_arm_status_disarm_roundtrip(run_cli, monkeypatch):
    monkeypatch.setattr(snapshot, "_spawn_timer", lambda sid: 4242)
    r = run_cli("snapshot", "arm", "--session", "s1", "--engine", "claude",
                "--cwd", "/tmp/proj", "--idle", "55")
    assert r.code == 0, r.err
    r = run_cli("snapshot", "status", "--session", "s1")
    assert r.code == 0 and "armed" in r.out
    assert run_cli("snapshot", "disarm", "--session", "s1").code == 0
    assert "disarmed" in run_cli("snapshot", "status").out


def test_arm_accepts_mode_flag(run_cli, monkeypatch):
    monkeypatch.setattr(snapshot, "_spawn_timer", lambda sid: 4242)
    r = run_cli("snapshot", "arm", "--session", "s1", "--engine", "claude",
                "--cwd", "/tmp/proj", "--mode", "both")
    assert r.code == 0 and "armed (both)" in r.out
    r2 = run_cli("snapshot", "status", "--session", "s1")
    assert r2.code == 0 and "both" in r2.out


def test_arm_rejects_compact_mode_on_unsupported_engine(run_cli, monkeypatch):
    monkeypatch.setattr(snapshot, "_spawn_timer", lambda sid: 4242)
    r = run_cli("snapshot", "arm", "--session", "s1", "--engine", "codex",
                "--cwd", "/tmp/proj", "--mode", "compact")
    assert r.code == 1 and "/compact" in r.err


def test_arm_rejects_bad_engine_and_threshold(run_cli):
    r = run_cli("snapshot", "arm", "--session", "s1", "--engine", "kimi",
                "--cwd", "/tmp/x")
    assert r.code == 1 and "snapshot-now" in r.err
    r = run_cli("snapshot", "arm", "--session", "s1", "--engine", "claude",
                "--cwd", "/tmp/x", "--idle", "75")
    assert r.code == 1 and "TTL" in r.err


def test_path_record_latest_consume_flow(run_cli):
    p_out = run_cli("snapshot", "path", "--session", "s1")
    assert p_out.code == 0
    path = p_out.out.strip()
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("---\nsession_id: s1\n---\nbody\n")
    assert run_cli("snapshot", "record", "--session", "s1",
                   "--cwd", "/tmp/proj").code == 0
    latest = run_cli("snapshot", "latest", "--cwd", "/tmp/proj")
    assert latest.code == 0 and latest.out.strip() == path
    assert run_cli("snapshot", "consume", "--path", path).code == 0
    assert run_cli("snapshot", "latest", "--cwd", "/tmp/proj").code == 1


def test_fire_and_timer_run_cli(run_cli, monkeypatch):
    monkeypatch.setattr(snapshot, "fire", lambda sid: {"ok": True, "transport": "tty"})
    r = run_cli("snapshot", "fire", "--session", "s1")
    assert r.code == 0

    monkeypatch.setattr(snapshot, "run_timer", lambda sid: "fired")
    r2 = run_cli("snapshot", "timer-run", "s1")
    assert r2.code == 0 and "fired" in r2.out


def test_status_empty_cli(run_cli):
    r = run_cli("snapshot", "status")
    assert r.code == 0
    assert "no snapshot timers" in r.out


def test_sessions_verb_lists_and_handles_empty(run_cli, tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_CLAUDE_PROJECTS_DIR", str(tmp_path))
    r = run_cli("snapshot", "sessions", "--cwd", "/tmp/proj")
    assert r.code == 0 and "no sessions found" in r.out
    # tests/ is not a package (no __init__.py); pytest puts it on sys.path,
    # so sibling modules import by bare name. `from tests.test_snapshot ...`
    # only works when the repo root happens to be on sys.path (e.g. under
    # `python -m pytest`) and fails under the `pytest` console script.
    from test_snapshot import _write_transcript
    from watchtower import snapshot as snap
    _write_transcript(tmp_path, snap.cwd_slug("/tmp/proj"), "abc12345-full-id",
                      5000.0, "build the widget")
    r = run_cli("snapshot", "sessions", "--cwd", "/tmp/proj", "-n", "5")
    assert r.code == 0
    assert "abc12345-full-id" in r.out and "build the widget" in r.out

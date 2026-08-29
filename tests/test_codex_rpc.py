"""WatchTower codex_rpc tests: a fake stdio JSON-RPC app-server exercises the
handshake, deliver round-trip, request timeout, crash-then-restart, and
shutdown paths without ever spawning a real ``codex`` binary.

The fake app-server is a tiny Python script written to ``tmp_path`` per test
and pointed to via ``$WATCHTOWER_CODEX_BIN``. Its behavior is steered by a
handful of env vars read fresh on each request, which is why every test sets
them *before* the first RPC call in that test (the child's environment is a
one-time copy taken at spawn, so later parent-side env changes only take
effect on the NEXT spawn — exactly what the crash-then-restart test relies
on).
"""

from __future__ import annotations

import importlib
import os
import stat
import sys
import time

import pytest

FAKE_CODEX_SRC = '''#!/usr/bin/env python3
import json
import os
import sys


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except json.JSONDecodeError:
            continue
        method = req.get("method")
        req_id = req.get("id")

        if os.environ.get("FAKE_CODEX_NO_REPLY_METHOD") == method:
            continue  # simulate a hung app-server: never answer this one

        if method == "initialize":
            result = {}
        elif method == "thread/resume":
            if os.environ.get("FAKE_CODEX_ACTIVE_TURN") == "1":
                result = {
                    "thread": {
                        "status": {"type": "active"},
                        "turns": [{"id": "turn-1", "status": "inProgress"}],
                    }
                }
            else:
                result = {"thread": {"status": {"type": "idle"}, "turns": []}}
        elif method == "turn/steer":
            expected = (req.get("params") or {}).get("expectedTurnId", "turn-1")
            result = {"turnId": expected}
        elif method == "turn/start":
            result = {"turn": {"id": "turn-new"}}
        elif method == "thread/compact/start":
            result = {}
        else:
            result = {}

        if os.environ.get("FAKE_CODEX_FAIL_METHOD") == method:
            resp = {"jsonrpc": "2.0", "id": req_id, "error": {"message": "fake failure"}}
        elif (req.get("params") or {}).get("threadId") == os.environ.get(
            "FAKE_CODEX_NOT_FOUND_THREAD"
        ) and os.environ.get("FAKE_CODEX_NOT_FOUND_THREAD"):
            tid = (req.get("params") or {}).get("threadId")
            resp = {"jsonrpc": "2.0", "id": req_id, "error": {"message": f"thread not found: {tid}"}}
        else:
            resp = {"jsonrpc": "2.0", "id": req_id, "result": result}

        sys.stdout.write(json.dumps(resp) + "\\n")
        sys.stdout.flush()

        if os.environ.get("FAKE_CODEX_EXIT_AFTER") == method:
            sys.exit(0)


if __name__ == "__main__":
    main()
'''


@pytest.fixture()
def rpc(tmp_path, monkeypatch):
    """A codex_rpc module reloaded fresh, pointed at the fake app-server."""
    script = tmp_path / "fake_codex.py"
    script.write_text(f"#!{sys.executable}\n" + FAKE_CODEX_SRC)
    script.chmod(script.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
    monkeypatch.setenv("WATCHTOWER_CODEX_BIN", str(script))
    for var in (
        "FAKE_CODEX_ACTIVE_TURN",
        "FAKE_CODEX_NO_REPLY_METHOD",
        "FAKE_CODEX_EXIT_AFTER",
        "FAKE_CODEX_FAIL_METHOD",
        "FAKE_CODEX_NOT_FOUND_THREAD",
    ):
        monkeypatch.delenv(var, raising=False)

    import watchtower.codex_rpc as codex_rpc
    importlib.reload(codex_rpc)
    yield codex_rpc
    codex_rpc.shutdown()


# -------------------------------------------------------------------- is_available
def test_is_available_true_for_fake_binary(rpc):
    assert rpc.is_available() is True


def test_is_available_false_when_unset(rpc, monkeypatch):
    monkeypatch.delenv("WATCHTOWER_CODEX_BIN", raising=False)
    monkeypatch.setenv("PATH", "/nonexistent-bin-dir")
    assert rpc.is_available() is False


def test_is_available_false_for_missing_file(rpc, monkeypatch, tmp_path):
    monkeypatch.setenv("WATCHTOWER_CODEX_BIN", str(tmp_path / "does-not-exist"))
    assert rpc.is_available() is False


# ---------------------------------------------------------------------- handshake
def test_handshake_starts_and_initializes(rpc):
    assert rpc.is_running() is False
    proc = rpc.ensure_server()
    assert proc is not None
    assert rpc.is_running() is True
    # Idempotent: a second call reuses the same live, initialized process.
    assert rpc.ensure_server() is proc


def test_handshake_returns_none_without_a_binary(rpc, monkeypatch, tmp_path):
    monkeypatch.setenv("WATCHTOWER_CODEX_BIN", str(tmp_path / "missing"))
    assert rpc.ensure_server() is None
    assert rpc.is_running() is False


# --------------------------------------------------------------------- deliver
def test_deliver_starts_a_fresh_turn_when_idle(rpc):
    result = rpc.deliver("thread-abc", "hello codex")
    assert result["ok"] is True
    assert result["via"] == "start"
    assert result["turn_id"] == "turn-new"
    assert result["app_server_warm"] is False
    assert result["resume_ms"] >= 0
    assert result["turn_ms"] >= 0
    assert result["latency_ms"] >= result["resume_ms"]


def test_deliver_steers_an_active_turn(rpc, monkeypatch):
    monkeypatch.setenv("FAKE_CODEX_ACTIVE_TURN", "1")
    result = rpc.deliver("thread-abc", "steer this in")
    assert result["ok"] is True
    assert result["via"] == "steer"
    assert result["turn_id"] == "turn-1"
    assert result["app_server_warm"] is False
    assert result["resume_ms"] >= 0
    assert result["turn_ms"] >= 0
    assert result["latency_ms"] >= result["resume_ms"]


def test_deliver_reports_warm_state_after_first_delivery(rpc):
    first = rpc.deliver("thread-abc", "first")
    second = rpc.deliver("thread-abc", "second")
    assert first["ok"] is True
    assert second["ok"] is True
    assert first["app_server_warm"] is False
    assert second["app_server_warm"] is True


def test_deliver_reports_resume_failure(rpc, monkeypatch):
    monkeypatch.setenv("FAKE_CODEX_FAIL_METHOD", "thread/resume")
    result = rpc.deliver("thread-abc", "hi")
    assert result["ok"] is False
    assert "fake failure" in result["error"]


def test_deliver_without_a_binary_fails_fast(rpc, monkeypatch, tmp_path):
    monkeypatch.setenv("WATCHTOWER_CODEX_BIN", str(tmp_path / "missing"))
    result = rpc.deliver("thread-abc", "hi")
    assert result["ok"] is False
    assert result["error"] == "codex binary not found"
    assert result["stage"] == "resolve"
    assert result["latency_ms"] >= 0


# ---------------------------------------------------------------------- compact
def test_compact_resumes_then_starts_compaction(rpc):
    result = rpc.compact("thread-abc")
    assert result["ok"] is True
    assert result["app_server_warm"] is False
    assert result["resume_ms"] >= 0
    assert result["compact_ms"] >= 0
    assert result["latency_ms"] >= result["resume_ms"]


def test_compact_without_a_binary_fails_fast(rpc, monkeypatch, tmp_path):
    monkeypatch.setenv("WATCHTOWER_CODEX_BIN", str(tmp_path / "missing"))
    result = rpc.compact("thread-abc")
    assert result["ok"] is False
    assert result["error"] == "codex binary not found"
    assert result["stage"] == "resolve"


def test_compact_reports_resume_failure(rpc, monkeypatch):
    monkeypatch.setenv("FAKE_CODEX_FAIL_METHOD", "thread/resume")
    result = rpc.compact("thread-abc")
    assert result["ok"] is False
    assert "fake failure" in result["error"]
    assert result["stage"] == "thread/resume"


def test_compact_reports_compact_start_failure(rpc, monkeypatch):
    monkeypatch.setenv("FAKE_CODEX_FAIL_METHOD", "thread/compact/start")
    result = rpc.compact("thread-abc")
    assert result["ok"] is False
    assert "fake failure" in result["error"]
    assert result["stage"] == "thread/compact/start"


# ---------------------------------------------------------------------- timeout
def test_request_timeout_never_hangs_the_caller(rpc, monkeypatch):
    monkeypatch.setenv("FAKE_CODEX_NO_REPLY_METHOD", "thread/resume")
    started = time.monotonic()
    result = rpc.deliver("thread-abc", "hi", timeout=0.3)
    elapsed = time.monotonic() - started
    assert result["ok"] is False
    assert "timed out" in result["error"]
    # Bounded by the timeout, not the test's own patience — proves the
    # caller never blocks past the requested deadline.
    assert elapsed < 3.0


# ------------------------------------------------------------- crash + restart
def test_server_crash_then_restart_on_next_deliver(rpc, monkeypatch):
    monkeypatch.setenv("FAKE_CODEX_EXIT_AFTER", "turn/start")
    first = rpc.deliver("thread-abc", "first message")
    assert first["ok"] is True
    assert first["via"] == "start"
    assert first["turn_id"] == "turn-new"

    dead_proc = rpc._PROC
    # Give the reader thread a moment to notice EOF, though ensure_server()
    # detects a dead process itself via proc.poll() regardless of timing.
    deadline = time.monotonic() + 2.0
    while dead_proc is not None and dead_proc.poll() is None and time.monotonic() < deadline:
        time.sleep(0.05)
    assert dead_proc is None or dead_proc.poll() is not None

    second = rpc.deliver("thread-abc", "second message, after respawn")
    assert second["ok"] is True
    assert second["via"] == "start"
    assert second["turn_id"] == "turn-new"
    assert rpc._PROC is not dead_proc


# ------------------------------------------------------------- wedge recycle
def test_wedged_server_recycled_after_verified_false_misses(rpc, monkeypatch):
    """'thread not found' for a thread whose rollout exists on disk is a
    false miss — after _FALSE_MISS_LIMIT consecutive ones the child must be
    recycled, and the next deliver must succeed on a fresh process."""
    monkeypatch.setenv("FAKE_CODEX_NOT_FOUND_THREAD", "thread-lost")
    monkeypatch.setattr(rpc, "_rollout_exists_on_disk", lambda tid: True)
    first_proc = rpc.ensure_server()

    for _ in range(rpc._FALSE_MISS_LIMIT):
        result = rpc.deliver("thread-lost", "hi")
        assert result["ok"] is False
        assert "thread not found" in result["error"]

    assert rpc._PROC is None  # shutdown() ran: wedged child dropped

    # The respawned child gets a fresh env copy, so clearing the failure
    # mode now makes the next deliver succeed on a NEW process.
    monkeypatch.delenv("FAKE_CODEX_NOT_FOUND_THREAD")
    result = rpc.deliver("thread-lost", "hi again")
    assert result["ok"] is True
    assert rpc._PROC is not first_proc


def test_genuine_thread_miss_does_not_recycle(rpc, monkeypatch):
    """No rollout on disk = genuine miss (deleted/archived thread), which
    must never recycle a healthy child."""
    monkeypatch.setenv("FAKE_CODEX_NOT_FOUND_THREAD", "thread-gone")
    monkeypatch.setattr(rpc, "_rollout_exists_on_disk", lambda tid: False)
    proc = rpc.ensure_server()

    for _ in range(rpc._FALSE_MISS_LIMIT + 2):
        result = rpc.deliver("thread-gone", "hi")
        assert result["ok"] is False

    assert rpc._PROC is proc
    assert rpc.is_running() is True
    assert rpc._FALSE_MISSES == 0


def test_false_miss_counter_resets_on_success(rpc, monkeypatch):
    """A successful thread-level RPC between false misses resets the strike
    counter, so intermittent failures against a healthy child never
    accumulate into a recycle."""
    monkeypatch.setenv("FAKE_CODEX_NOT_FOUND_THREAD", "thread-lost")
    monkeypatch.setattr(rpc, "_rollout_exists_on_disk", lambda tid: True)
    proc = rpc.ensure_server()

    rpc.deliver("thread-lost", "hi")
    rpc.deliver("thread-lost", "hi")
    assert rpc._FALSE_MISSES == 2
    ok = rpc.deliver("thread-healthy", "hi")
    assert ok["ok"] is True
    assert rpc._FALSE_MISSES == 0
    rpc.deliver("thread-lost", "hi")
    rpc.deliver("thread-lost", "hi")

    assert rpc._PROC is proc  # 2 + 2 non-consecutive strikes: no recycle
    assert rpc._FALSE_MISSES == 2


# -------------------------------------------------------------------- shutdown
def test_shutdown_terminates_the_process(rpc):
    rpc.ensure_server()
    proc = rpc._PROC
    assert proc is not None
    assert proc.poll() is None

    rpc.shutdown()

    assert proc.poll() is not None
    assert rpc.is_running() is False
    assert rpc._PROC is None


def test_shutdown_is_idempotent_with_no_server_started(rpc):
    assert rpc.is_running() is False
    rpc.shutdown()  # must not raise
    assert rpc.is_running() is False

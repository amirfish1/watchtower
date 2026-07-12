"""WatchTower messaging tests: registry, resolve, adapters, outbox, ask.

Everything runs against a fully isolated sandbox (agents.json, outbox.json,
workers.json, queue store, and a fake claude-projects dir all under tmp_path),
mirroring tests/test_workers_lifecycle.py. No real ``claude`` is ever spawned:
the resume adapter is exercised with a monkeypatched subprocess.Popen, and the
delegate adapter against a local in-thread http.server.
"""

from __future__ import annotations

import importlib
import json
import os
import stat
import sys
import threading
import time
import uuid as _uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

SID_A = "7f72634b-b0bd-4c78-b931-3d877ed84187"
SID_B = "c44f96bc-d720-49d3-a5e6-115426939f82"
SID_C = "12345678-aaaa-4bbb-8ccc-1234567890ab"
# A codex thread id, addressed the same way as a claude session_id (WT-54:
# session_id maps 1:1 onto the codex thread id, see messages.py docstring).
SID_D = "9c1f5a3e-6b2d-4e11-9a77-2f6c8b0d4e55"
# Two UUIDs sharing the same first 8 hex chars, for ambiguity tests.
SID_AMB1 = "aaaaaaaa-1111-4111-8111-111111111111"
SID_AMB2 = "aaaaaaaa-2222-4222-8222-222222222222"


@pytest.fixture()
def wt(tmp_path, monkeypatch):
    """Isolated WatchTower: fresh store, workers, agents, outbox, no delegate."""
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_WORKERS_FILE", str(tmp_path / "workers.json"))
    monkeypatch.setenv("WATCHTOWER_CONFIG_FILE", str(tmp_path / "config.json"))
    monkeypatch.setenv("WATCHTOWER_STOP_SIGNALS_DIR", str(tmp_path / "stop-signals"))
    monkeypatch.setenv(
        "WATCHTOWER_WORKER_SESSIONS_FILE", str(tmp_path / "worker-sessions.json")
    )
    monkeypatch.setenv("WATCHTOWER_AGENTS_FILE", str(tmp_path / "agents.json"))
    monkeypatch.setenv("WATCHTOWER_OUTBOX_FILE", str(tmp_path / "outbox.json"))
    # No delegate by default: WT standalone must be a working configuration,
    # and tests must never hit a real CCC via ~/.claude/command-center/port.txt.
    monkeypatch.setenv("WATCHTOWER_DELEGATE_URL", "off")
    # Keep the resume busy check away from any real transcript archive.
    monkeypatch.setenv(
        "WATCHTOWER_CLAUDE_PROJECTS_DIR", str(tmp_path / "claude-projects")
    )
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(tmp_path / "activity.log"))
    monkeypatch.setenv(
        "WATCHTOWER_CCC_SPAWN_DEFAULTS_FILE", str(tmp_path / "no-ccc-spawn-defaults.json")
    )
    monkeypatch.setenv(
        "WATCHTOWER_CODEX_THREAD_REGISTRY", str(tmp_path / "codex-thread-registry.json")
    )

    # No codex binary by default: is_available() must read False so the
    # codex-app-server adapter is a clean no-op miss in every test that
    # doesn't explicitly opt in (see test_deliver_codex_target below).
    monkeypatch.delenv("WATCHTOWER_CODEX_BIN", raising=False)

    import watchtower.queue as q
    import watchtower.config as config
    import watchtower.workers as workers
    import watchtower.messages as messages
    import watchtower.codex_rpc as codex_rpc
    import watchtower.codex_registry as codex_registry
    importlib.reload(q)
    importlib.reload(config)
    importlib.reload(workers)
    importlib.reload(messages)
    importlib.reload(codex_rpc)
    importlib.reload(codex_registry)
    monkeypatch.setattr(config, "_REGISTRY_FILE", tmp_path / "no-registry.json")

    class Ns:
        pass
    ns = Ns()
    ns.q, ns.config, ns.workers, ns.messages = q, config, workers, messages
    ns.codex_rpc = codex_rpc
    ns.codex_registry = codex_registry
    ns.tmp = tmp_path
    ns._readers = []
    yield ns
    codex_rpc.shutdown()
    for fd in ns._readers:
        try:
            os.close(fd)
        except OSError:
            pass


# --------------------------------------------------------------------- helpers
def _live_worker(wt, queue, *, session_id=""):
    """Record a live worker (this pid) with a real FIFO whose reader fd we hold."""
    workers = wt.workers
    wid = f"{queue.lower()}-live{len(wt._readers)}-{_uuid.uuid4().hex[:4]}"
    log = wt.tmp / f"{wid}.log"
    log.write_text("")
    fifo_path, rdwr_fd = workers._make_stdin_fifo(log)
    wt._readers.append(rdwr_fd)
    return workers.record_worker(
        os.getpid(), queue, "claude", wid, str(wt.tmp), str(log),
        fifo=fifo_path or "", session_id=session_id,
    )


def _read_fifo_msg(wt):
    """Read one stream-json user line back off the most recent worker FIFO."""
    data = os.read(wt._readers[-1], 65536).decode()
    return json.loads(data.strip().splitlines()[-1])


def _disable_resume(wt, monkeypatch):
    monkeypatch.setattr(
        wt.messages, "_deliver_resume",
        lambda *a, **k: {"ok": False, "error": "resume disabled in test"},
    )


def _write_transcript(wt, sid, age_s=0.0):
    """Drop a session transcript into the fake claude-projects dir."""
    d = Path(os.environ["WATCHTOWER_CLAUDE_PROJECTS_DIR"]) / "some-project"
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{sid}.jsonl"
    p.write_text('{"type":"user"}\n')
    if age_s:
        old = time.time() - age_s
        os.utime(p, (old, old))
    return p


def _write_worker_log(wt, worker_id, sid, age_s=0.0):
    """Drop a worker stream-json log where WT's log scanner expects it."""
    d = wt.workers.WORKERS_FILE.parent / "logs"
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{worker_id}.log"
    p.write_text(json.dumps({"type": "system", "session_id": sid}) + "\n")
    if age_s:
        old = time.time() - age_s
        os.utime(p, (old, old))
    return p


class _DelegateHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0) or 0)
        body = json.loads(self.rfile.read(n) or b"{}")
        self.server.requests.append((self.path, body))
        status, payload = (
            self.server.responses.pop(0)
            if self.server.responses else (200, {"ok": True})
        )
        data = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *a):  # keep pytest output clean
        pass


@pytest.fixture()
def delegate():
    """A local fake delegate HTTP server. Yields (server, base_url)."""
    srv = HTTPServer(("127.0.0.1", 0), _DelegateHandler)
    srv.requests = []
    srv.responses = []
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    yield srv, f"http://127.0.0.1:{srv.server_address[1]}"
    srv.shutdown()
    srv.server_close()


# ================================================================ agents registry
def test_registry_roundtrip(wt):
    rec = wt.messages.register_agent("planner", SID_A, engine="claude", cwd="/repo")
    assert rec["name"] == "planner" and rec["session_id"] == SID_A
    rows = wt.messages.list_agents()
    assert [r["name"] for r in rows] == ["planner"]
    assert rows[0]["cwd"] == "/repo"
    assert wt.messages.remove_agent("planner") is True
    assert wt.messages.list_agents() == []
    assert wt.messages.remove_agent("planner") is False


def test_register_codex_agent_updates_thread_registry(wt):
    wt.messages.register_agent("codexer", SID_D, engine="codex", cwd="/repo")

    rec = wt.codex_registry.entry(SID_D)

    assert rec["thread_id"] == SID_D
    assert rec["engine"] == "codex"
    assert rec["visibility"] == "registered-agent"
    assert rec["cwd"] == "/repo"
    assert rec["name"] == "codexer"
    assert rec["title"] == "codexer"
    assert rec["wt"]["agent_name"] == "codexer"
    assert "wt-agents" in rec["sources"]


def test_register_strips_leading_at_and_repoints(wt):
    wt.messages.register_agent("@planner", SID_A)
    # set-name semantics: re-registering an existing name repoints it, no error.
    rec = wt.messages.register_agent("planner", SID_B)
    assert rec["session_id"] == SID_B
    rows = wt.messages.list_agents()
    assert len(rows) == 1 and rows[0]["session_id"] == SID_B


def test_register_rejects_worker_id_shape_and_uuid_names(wt):
    with pytest.raises(ValueError):
        wt.messages.register_agent("ccc-40546374", SID_A)  # worker-id shape
    with pytest.raises(ValueError):
        wt.messages.register_agent(SID_B, SID_A)  # UUID as a name
    with pytest.raises(ValueError):
        wt.messages.register_agent("planner", "not-a-uuid")


# ================================================================= resolve_target
def test_resolve_exact_worker_id(wt):
    rec = _live_worker(wt, "Q", session_id=SID_A)
    r = wt.messages.resolve_target(rec["worker_id"])
    assert r["kind"] == "worker"
    assert r["worker"]["worker_id"] == rec["worker_id"]
    assert r["session_id"] == SID_A
    assert r["engine"] == "claude"


def test_resolve_agent_name_with_at(wt):
    wt.messages.register_agent("planner", SID_A, engine="claude")
    r = wt.messages.resolve_target("@planner")
    assert r["kind"] == "agent" and r["session_id"] == SID_A
    assert r["worker"] is None


def test_resolve_agent_attaches_matching_live_worker(wt):
    _live_worker(wt, "Q", session_id=SID_A)
    wt.messages.register_agent("planner", SID_A)
    r = wt.messages.resolve_target("planner")
    assert r["kind"] == "agent"
    assert r["worker"] is not None and r["worker"]["session_id"] == SID_A


def test_resolve_full_uuid_unknown_accepted(wt):
    r = wt.messages.resolve_target(SID_B)
    assert r["kind"] == "session" and r["session_id"] == SID_B
    assert r["worker"] is None and r["engine"] == "claude"


def test_resolve_unique_prefix(wt):
    wt.messages.register_agent("planner", SID_A)
    r = wt.messages.resolve_target(SID_A[:8])
    assert r["kind"] == "session" and r["session_id"] == SID_A


def test_resolve_ambiguous_prefix_raises(wt):
    wt.messages.register_agent("one", SID_AMB1)
    wt.messages.register_agent("two", SID_AMB2)
    with pytest.raises(ValueError, match="ambiguous"):
        wt.messages.resolve_target("aaaaaaaa")


def test_resolve_short_unknown_prefix_raises(wt):
    with pytest.raises(ValueError):
        wt.messages.resolve_target("deadbeef")  # 8 hex, but matches nothing
    with pytest.raises(ValueError):
        wt.messages.resolve_target("not/a/target")


def test_exact_resolution_and_plain_listing_never_touch_projects_dir(wt, monkeypatch):
    """Exact worker-id / agent-name resolution and list_agents(include_recent=
    False) never touch the claude projects dir at all."""
    def boom():
        raise AssertionError("projects dir must not be touched here")
    monkeypatch.setattr(wt.messages, "_claude_projects_root", boom)
    rec = _live_worker(wt, "Q", session_id=SID_A)
    wt.messages.register_agent("planner", SID_B)
    assert [r["name"] for r in wt.messages.list_agents(include_recent=False)] \
        == ["planner"]
    assert wt.messages.resolve_target(rec["worker_id"])["kind"] == "worker"
    assert wt.messages.resolve_target("@planner")["kind"] == "agent"


def test_deliver_and_drain_never_enumerate_projects_dir(wt, monkeypatch):
    """Boundary: deliver() and drain_outbox() must never run the recent-session
    scan. The resume busy check's targeted <sid>.jsonl lookup stays the only
    transcript touch on those paths."""
    def boom(*a, **k):
        raise AssertionError("recent-session scan is not allowed on this path")
    monkeypatch.setattr(wt.messages, "recent_sessions", boom)
    # Busy transcript so the resume adapter defers instead of spawning claude;
    # a Popen here would mean the busy check was skipped, so make it fatal too.
    _write_transcript(wt, SID_C, age_s=0)
    def no_spawn(*a, **k):
        raise AssertionError("no spawn expected on a busy session")
    monkeypatch.setattr(wt.messages.subprocess, "Popen", no_spawn)
    resolved = wt.messages.resolve_target(SID_C, include_recent=False)
    r = wt.messages.deliver(resolved, "hi")
    assert r["ok"] is False and r["busy"] is True
    wt.messages.outbox_add(SID_C, "queued", now=time.time() - 60)
    out = wt.messages.drain_outbox()  # resolves with include_recent=False
    assert out["retried"]  # attempted (busy again), and no scan happened


# ========================================================= recent-session window
def test_recent_sessions_window_and_fields(wt):
    _write_transcript(wt, SID_A, age_s=60)            # fresh: in window
    _write_transcript(wt, SID_B, age_s=5 * 86400)     # 5 days old: out (3d default)
    rows = wt.messages.recent_sessions()
    assert [r["session_id"] for r in rows] == [SID_A]
    assert rows[0]["cwd_slug"] == "some-project"
    assert abs(rows[0]["last_active_epoch"] - (time.time() - 60)) < 5


def test_recent_sessions_env_override(wt, monkeypatch):
    _write_transcript(wt, SID_B, age_s=5 * 86400)
    assert wt.messages.recent_sessions() == []        # 5d old, default 3d window
    monkeypatch.setenv("WATCHTOWER_AGENTS_WINDOW_DAYS", "10")
    assert [r["session_id"] for r in wt.messages.recent_sessions()] == [SID_B]
    assert wt.messages.recent_sessions(window_days=1) == []  # arg beats env


def test_recent_sessions_ignores_non_uuid_files(wt):
    d = Path(os.environ["WATCHTOWER_CLAUDE_PROJECTS_DIR"]) / "some-project"
    d.mkdir(parents=True, exist_ok=True)
    (d / "notes.jsonl").write_text("{}\n")
    (d / f"{SID_A}.txt").write_text("{}\n")
    assert wt.messages.recent_sessions() == []


def test_list_agents_merges_and_dedupes_recent(wt):
    wt.messages.register_agent("planner", SID_A)
    _live_worker(wt, "Q", session_id=SID_B)
    _write_transcript(wt, SID_A, age_s=60)   # deduped: registered
    _write_transcript(wt, SID_B, age_s=60)   # deduped: live worker session
    _write_transcript(wt, SID_C, age_s=60)   # genuinely new: listed as recent
    rows = wt.messages.list_agents()
    assert [(r.get("kind"), r.get("name") or r.get("session_id")) for r in rows] \
        == [("agent", "planner"), ("recent", SID_C)]


def test_resolve_prefix_of_recent_session(wt):
    _write_transcript(wt, SID_C, age_s=60)
    r = wt.messages.resolve_target(SID_C[:8])
    assert r["kind"] == "session" and r["session_id"] == SID_C
    assert r["engine"] == "claude" and r["worker"] is None
    # The daemon-path variant does not see the recent window.
    with pytest.raises(ValueError):
        wt.messages.resolve_target(SID_C[:8], include_recent=False)


# ================================================================== fifo delivery
def test_send_fifo_delivery_to_live_worker(wt):
    rec = _live_worker(wt, "Q")
    res = wt.messages.send(rec["worker_id"], "hello via fifo")
    assert res["ok"] is True and res["transport"] == "fifo"
    msg = _read_fifo_msg(wt)
    assert msg["type"] == "user"
    assert msg["message"]["content"][0]["text"] == "hello via fifo"
    assert wt.messages.outbox_list() == []  # nothing queued on success


# ============================================================== delegate delivery
def test_delegate_delivery_payload(wt, delegate, monkeypatch):
    srv, url = delegate
    monkeypatch.setenv("WATCHTOWER_DELEGATE_URL", url)
    _disable_resume(wt, monkeypatch)
    res = wt.messages.send(SID_B, "over http", mode="steer")
    assert res["ok"] is True and res["transport"] == "delegate"
    path, body = srv.requests[0]
    assert path == "/api/inject-input"
    assert body == {"session_id": SID_B, "text": "over http", "mode": "steer",
                    "origin": "wt"}


def test_delegate_500_falls_to_outbox(wt, delegate, monkeypatch):
    srv, url = delegate
    srv.responses.append((500, {"ok": False}))
    monkeypatch.setenv("WATCHTOWER_DELEGATE_URL", url)
    _disable_resume(wt, monkeypatch)
    res = wt.messages.send(SID_B, "will queue")
    assert res["ok"] is False and res["queued"] is True
    pending = wt.messages.outbox_list(status="pending")
    assert len(pending) == 1
    assert pending[0]["to"] == SID_B and pending[0]["text"] == "will queue"
    assert pending[0]["attempts"] == 0


def test_delegate_ok_false_body_is_failure(wt, delegate, monkeypatch):
    srv, url = delegate
    srv.responses.append((200, {"ok": False}))
    monkeypatch.setenv("WATCHTOWER_DELEGATE_URL", url)
    _disable_resume(wt, monkeypatch)
    res = wt.messages.send(SID_B, "rejected", queue_on_fail=False)
    assert res["ok"] is False and res.get("queued") is False


def test_no_delegate_is_a_working_configuration(wt, monkeypatch):
    """WT standalone: DELEGATE_URL=off means no delegate, and send still works
    end to end via the native fifo path."""
    rec = _live_worker(wt, "Q")
    res = wt.messages.send(rec["worker_id"], "native only")
    assert res["ok"] is True and res["transport"] == "fifo"
    assert wt.messages._delegate_base() == ""


# ======================================================= codex app-server delivery
def _write_fake_codex_bin(tmp_path):
    """A minimal fake codex app-server: answers initialize, thread/resume
    (always idle, no active turn), and turn/start. Enough to prove
    messages.deliver reaches watchtower.codex_rpc end to end without a real
    codex binary anywhere in this test."""
    script = tmp_path / "fake_codex.py"
    script.write_text(
        f"#!{sys.executable}\n"
        + '''
import json
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    req = json.loads(line)
    method = req.get("method")
    req_id = req.get("id")
    if method == "initialize":
        result = {}
    elif method == "thread/resume":
        result = {"thread": {"status": {"type": "idle"}, "turns": []}}
    elif method == "turn/start":
        result = {"turn": {"id": "turn-integration"}}
    else:
        result = {}
    sys.stdout.write(json.dumps({"jsonrpc": "2.0", "id": req_id, "result": result}) + "\\n")
    sys.stdout.flush()
'''
    )
    script.chmod(script.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
    return script


def test_deliver_codex_target_via_app_server(wt, monkeypatch):
    """A registered codex agent with no live worker and no delegate still
    gets delivered natively through WT's own codex app-server subprocess
    (WT-54), never touching the delegate adapter."""
    script = _write_fake_codex_bin(wt.tmp)
    monkeypatch.setenv("WATCHTOWER_CODEX_BIN", str(script))
    _disable_resume(wt, monkeypatch)
    wt.messages.register_agent("codexer", SID_D, engine="codex", cwd="/repo")

    res = wt.messages.send("codexer", "hello from wt")

    assert res["ok"] is True and res["transport"] == "codex-app-server"
    assert res["turn_id"] == "turn-integration"
    assert res["codex_via"] == "start"
    assert res["codex_app_server_warm"] is False
    assert res["codex_resume_ms"] >= 0
    assert res["codex_turn_ms"] >= 0
    assert res["codex_total_ms"] >= res["codex_resume_ms"]
    # WT-77: every ok delivery also carries a verification receipt.
    assert res.get("receipt_id", "").startswith("rcpt-")
    reg = wt.codex_registry.entry(SID_D)
    assert reg["transport_owner"] == "wt-private-app-server"
    assert reg["transport"] == "stdio"
    assert reg["wt"]["last_delivery_transport"] == "codex-app-server"
    assert reg["wt"]["last_delivery_via"] == "start"


def test_deliver_codex_prefers_delegate_broker_over_private_app_server(wt, delegate, monkeypatch):
    """When CCC/delegate is available, WT must not start its private codex
    app-server for the same thread."""
    srv, url = delegate
    monkeypatch.setenv("WATCHTOWER_DELEGATE_URL", url)
    monkeypatch.setenv("WATCHTOWER_CODEX_BIN", str(_write_fake_codex_bin(wt.tmp)))
    _disable_resume(wt, monkeypatch)
    wt.messages.register_agent("codexer", SID_D, engine="codex", cwd="/repo")

    def _private_should_not_run(*_args, **_kwargs):
        raise AssertionError("private codex app-server should not be used")

    monkeypatch.setattr(wt.codex_rpc, "deliver", _private_should_not_run)

    res = wt.messages.send("codexer", "hello through broker", mode="steer")

    assert res["ok"] is True
    assert res["transport"] == "delegate"
    path, body = srv.requests[0]
    assert path == "/api/inject-input"
    assert body == {
        "session_id": SID_D,
        "text": "hello through broker",
        "mode": "steer",
        "origin": "wt",
    }


def test_deliver_codex_delegate_failure_queues_without_private_app_server(wt, delegate, monkeypatch):
    """A reachable CCC broker failure queues; WT does not create a second
    private app-server owner as fallback."""
    srv, url = delegate
    srv.responses.append((500, {"ok": False}))
    monkeypatch.setenv("WATCHTOWER_DELEGATE_URL", url)
    monkeypatch.setenv("WATCHTOWER_CODEX_BIN", str(_write_fake_codex_bin(wt.tmp)))
    _disable_resume(wt, monkeypatch)
    wt.messages.register_agent("codexer", SID_D, engine="codex", cwd="/repo")

    def _private_should_not_run(*_args, **_kwargs):
        raise AssertionError("private codex app-server should not be used")

    monkeypatch.setattr(wt.codex_rpc, "deliver", _private_should_not_run)

    res = wt.messages.send("codexer", "queue if broker fails")

    assert res["ok"] is False and res["queued"] is True
    assert "delegate:" in res["error"]
    pending = wt.messages.outbox_list(status="pending")
    assert len(pending) == 1
    assert pending[0]["to"] == SID_D
    assert pending[0]["text"] == "queue if broker fails"


def test_deliver_codex_falls_through_when_binary_missing(wt, monkeypatch):
    """No codex binary on this machine: the codex adapter misses cleanly and
    the message still lands in the outbox (no delegate configured either)."""
    monkeypatch.delenv("WATCHTOWER_CODEX_BIN", raising=False)
    monkeypatch.setenv("PATH", "/nonexistent-bin-dir")
    _disable_resume(wt, monkeypatch)
    wt.messages.register_agent("codexer", SID_D, engine="codex", cwd="/repo")

    res = wt.messages.send("codexer", "hello from wt")

    assert res["ok"] is False and res["queued"] is True
    assert "codex" in res["error"]


# ========================================================= gemini delivery (WT-80)
def _write_fake_gemini_bin(tmp_path):
    """A minimal fake gemini binary: exits 0 immediately (accepts any argv)."""
    import stat as _stat
    script = tmp_path / "fake_gemini.sh"
    script.write_text("#!/bin/sh\nexit 0\n")
    script.chmod(script.stat().st_mode | _stat.S_IEXEC | _stat.S_IXGRP | _stat.S_IXOTH)
    return script


def test_deliver_gemini_target_via_resume(wt, monkeypatch):
    """A registered gemini agent with no live FIFO gets delivered natively
    via _deliver_gemini_resume (WT-80): gemini --resume <sid> -p <text>."""
    bin_path = _write_fake_gemini_bin(wt.tmp)
    monkeypatch.setenv("WT_GEMINI_BIN", str(bin_path))
    _disable_resume(wt, monkeypatch)
    wt.messages.register_agent("gemineer", SID_D, engine="gemini", cwd="/repo")

    res = wt.messages.send("gemineer", "hello from wt")

    assert res["ok"] is True and res["transport"] == "gemini-resume"
    assert res.get("receipt_id", "").startswith("rcpt-")


def test_deliver_gemini_falls_through_when_binary_missing(wt, monkeypatch):
    """No gemini binary: adapter misses cleanly and the message parks in outbox."""
    monkeypatch.delenv("WT_GEMINI_BIN", raising=False)
    monkeypatch.setenv("PATH", "/nonexistent-bin-dir")
    _disable_resume(wt, monkeypatch)
    wt.messages.register_agent("gemineer", SID_D, engine="gemini", cwd="/repo")

    res = wt.messages.send("gemineer", "hello from wt")

    assert res["ok"] is False and res["queued"] is True
    assert "gemini" in res["error"]


# ===================================================== antigravity delivery (WT-79)
def test_deliver_antigravity_target_via_local_language_server(wt, monkeypatch):
    """An Antigravity conversation UUID is its cascade UUID, so a live local
    language server can receive the next user message directly."""
    _disable_resume(wt, monkeypatch)
    wt.messages.register_agent("antigrav", SID_D, engine="antigravity")
    calls = []

    monkeypatch.setattr(wt.messages, "_antigravity_servers", lambda: [{
        "base_url": "https://127.0.0.1:4444", "csrf_token": "csrf",
    }])

    def post(server, method, payload):
        calls.append((method, payload))
        if method == "GetCascadeTrajectory":
            return {"status": "RUNNING"}
        if method == "GetUserStatus":
            return {"userStatus": {"cascadeModelConfigData": {
                "clientModelConfigs": [{
                    "isRecommended": True,
                    "modelOrAlias": {"model": "MODEL_TEST"},
                }],
            }}}
        assert method == "SendUserCascadeMessage"
        return {}

    monkeypatch.setattr(wt.messages, "_antigravity_post", post)

    res = wt.messages.send("antigrav", "hello from wt")

    assert res["ok"] is True
    assert res["transport"] == "antigravity-language-server"
    assert calls[-1] == ("SendUserCascadeMessage", {
        "cascadeId": SID_D,
        "items": [{"text": "hello from wt"}],
        "cascadeConfig": {
            "plannerConfig": {
                "requestedModel": {"model": "MODEL_TEST"},
                "maxOutputTokens": 8192,
            },
            "checkpointConfig": {"maxOutputTokens": 8192},
        },
    })


def test_antigravity_delivery_requires_a_live_matching_cascade(wt, monkeypatch):
    _disable_resume(wt, monkeypatch)
    wt.messages.register_agent("antigrav", SID_D, engine="antigravity")
    monkeypatch.setattr(wt.messages, "_antigravity_servers", lambda: [{
        "base_url": "https://127.0.0.1:4444", "csrf_token": "csrf",
    }])

    def missing(*_args):
        raise OSError("HTTP Error 404: cascade not found")

    monkeypatch.setattr(wt.messages, "_antigravity_post", missing)

    res = wt.messages.send("antigrav", "hello from wt")

    assert res["ok"] is False and res["queued"] is True
    assert "antigravity" in res["error"]


def test_antigravity_server_discovery_reads_random_loopback_port(wt, monkeypatch):
    def check_output(argv, **_kwargs):
        if argv[0] == "ps":
            return "123 /Applications/Antigravity.app/Contents/Resources/bin/language_server --csrf_token csrf-value\n"
        assert argv[:2] == ["lsof", "-a"]
        return "COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME\nlanguage_ 123 me 6u IPv4 0x0 0t0 TCP 127.0.0.1:45678 (LISTEN)\n"

    monkeypatch.setattr(wt.messages.subprocess, "check_output", check_output)

    assert wt.messages._antigravity_servers() == [{
        "base_url": "https://127.0.0.1:45678", "csrf_token": "csrf-value",
    }]


# ===================================================== outbox backoff/dead-letter
def test_outbox_backoff_schedule_and_dead_letter(wt, monkeypatch):
    _disable_resume(wt, monkeypatch)
    t0 = 1_000_000.0
    msg = wt.messages.outbox_add(SID_B, "undeliverable", now=t0)
    assert msg["status"] == "pending" and msg["attempts"] == 0
    assert wt.messages._parse_iso(msg["next_attempt_at"]) == t0 + 30

    for attempt in range(1, wt.messages.MAX_ATTEMPTS + 1):
        m = wt.messages.outbox_list()[0]
        due_at = wt.messages._parse_iso(m["next_attempt_at"])
        # One tick BEFORE it is due: nothing happens.
        out = wt.messages.drain_outbox(now=due_at - 1)
        assert not out["retried"] and not out["dead"] and not out["delivered"]
        # At the due time: one attempt is made and fails (no transport works).
        wt.messages.drain_outbox(now=due_at)
        m = wt.messages.outbox_list()[0]
        assert m["attempts"] == attempt
        if attempt < wt.messages.MAX_ATTEMPTS:
            assert m["status"] == "pending"
            delay = wt.messages._parse_iso(m["next_attempt_at"]) - due_at
            assert delay == min(600, 30 * 2 ** attempt)
        else:
            assert m["status"] == "dead"
    # A dead message is never retried again.
    out = wt.messages.drain_outbox(now=t0 + 10 ** 9)
    assert not out["retried"] and not out["dead"] and not out["delivered"]


def test_drain_outbox_delivers_once_delegate_comes_up(wt, delegate, monkeypatch):
    _disable_resume(wt, monkeypatch)
    # Delegate down (off): the send parks in the outbox.
    res = wt.messages.send(SID_B, "queued hello")
    assert res["queued"] is True
    # Delegate comes up: the next drain tick delivers it.
    srv, url = delegate
    monkeypatch.setenv("WATCHTOWER_DELEGATE_URL", url)
    out = wt.messages.drain_outbox(now=time.time() + 60)
    assert out["delivered"]
    m = wt.messages.outbox_list()[0]
    assert m["status"] == "delivered" and m["last_error"] == ""
    assert srv.requests[-1][1]["text"] == "queued hello"


def test_send_ttl_is_stored_on_queued_outbox_entry(wt, monkeypatch):
    _disable_resume(wt, monkeypatch)
    t0 = 1_200_000.0
    monkeypatch.setattr(wt.messages.time, "time", lambda: t0)

    res = wt.messages.send(SID_B, "expires later", ttl_s=45)

    assert res["ok"] is False and res["queued"] is True
    m = wt.messages.outbox_list(status="pending")[0]
    assert wt.messages._parse_iso(m["expires_at"]) == t0 + 45


def test_drain_outbox_marks_expired_pending_message_dead(wt, monkeypatch):
    t0 = 1_300_000.0
    msg = wt.messages.outbox_add(SID_B, "too old", delay_s=0, ttl_s=5, now=t0)

    def no_delivery(*a, **k):
        raise AssertionError("expired message must not be delivered")

    monkeypatch.setattr(wt.messages, "deliver", no_delivery)
    out = wt.messages.drain_outbox(now=t0 + 6)

    assert out["dead"] == [msg["id"]]
    m = wt.messages.outbox_list()[0]
    assert m["status"] == "dead"
    assert m["last_error"] == "expired"
    assert m["attempts"] == 0


def test_outbox_retry_and_remove_helpers(wt):
    t0 = 1_400_000.0
    msg = wt.messages.outbox_add(SID_B, "retry me", now=t0)
    data = wt.messages._load_outbox()
    data["messages"][0].update({
        "status": "dead",
        "attempts": 7,
        "next_attempt_at": wt.messages._iso(t0 + 600),
        "last_error": "failed",
    })
    wt.messages._save_outbox(data)

    retried = wt.messages.outbox_retry(msg["id"], now=t0 + 10)

    assert retried["id"] == msg["id"]
    assert retried["status"] == "pending"
    assert retried["attempts"] == 0
    assert wt.messages._parse_iso(retried["next_attempt_at"]) == t0 + 10
    assert wt.messages.outbox_remove(msg["id"]) is True
    assert wt.messages.outbox_remove("missing") is False


def test_outbox_retry_all_dead_only_resets_dead_messages(wt):
    t0 = 1_500_000.0
    dead = wt.messages.outbox_add(SID_A, "dead", now=t0)
    pending = wt.messages.outbox_add(SID_B, "pending", now=t0)
    data = wt.messages._load_outbox()
    data["messages"][0].update({
        "status": "dead",
        "attempts": 9,
        "last_error": "failed",
    })
    data["messages"][1]["attempts"] = 3
    wt.messages._save_outbox(data)

    rows = wt.messages.outbox_retry_all_dead(now=t0 + 20)

    assert [r["id"] for r in rows] == [dead["id"]]
    by_id = {m["id"]: m for m in wt.messages.outbox_list()}
    assert by_id[dead["id"]]["status"] == "pending"
    assert by_id[dead["id"]]["attempts"] == 0
    assert wt.messages._parse_iso(by_id[dead["id"]]["next_attempt_at"]) == t0 + 20
    assert by_id[pending["id"]]["status"] == "pending"
    assert by_id[pending["id"]]["attempts"] == 3


# ======================================================== resume adapter + busy check
def _fake_popen(calls):
    class FakeStdin:
        """Captures the one-shot resume delivery: the adapter must write the
        stream-json line and then CLOSE stdin (EOF) so the child exits after
        its turn instead of squatting on the session as a foreign writer."""
        def __init__(self):
            self.data = b""
            self.closed = False

        def write(self, b):
            self.data += b

        def flush(self):
            pass

        def close(self):
            self.closed = True

    class FakeProc:
        pid = 424242

        def __init__(self):
            self.stdin = FakeStdin()

    def popen(argv, **kw):
        proc = FakeProc()
        calls.append((argv, kw, proc))
        return proc
    return popen


def test_stale_transcript_proceeds_to_resume(wt, monkeypatch):
    _write_transcript(wt, SID_B, age_s=600)  # quiet for 10 min: not busy
    calls = []
    monkeypatch.setattr(wt.messages.subprocess, "Popen", _fake_popen(calls))
    res = wt.messages.send(SID_B, "resume me")
    assert res["ok"] is True and res["transport"] == "resume"
    assert res["log"].endswith(".log") and f"msg-{SID_B[:8]}-" in res["log"]
    argv = calls[0][0]
    assert argv[0] == "claude" and "-p" in argv and "--verbose" in argv
    assert "--resume" in argv and SID_B in argv
    assert "--permission-mode" in argv and "bypassPermissions" in argv
    kw = calls[0][1]
    assert kw.get("start_new_session") is True
    # The message went down the fresh FIFO as the first stream-json user line
    # and the outbox stayed empty (delivery succeeded).
    assert wt.messages.outbox_list() == []


def test_busy_transcript_defers_to_outbox(wt, monkeypatch):
    _write_transcript(wt, SID_B, age_s=0)  # fresh mtime: actively mid-turn
    calls = []
    monkeypatch.setattr(wt.messages.subprocess, "Popen", _fake_popen(calls))
    t0 = time.time()
    res = wt.messages.send(SID_B, "hold this")
    assert res["ok"] is False and res["queued"] is True and res["busy"] is True
    assert calls == []  # no parallel resume forked against a busy session
    m = wt.messages.outbox_list(status="pending")[0]
    delay = wt.messages._parse_iso(m["next_attempt_at"]) - t0
    assert 50 <= delay <= 70  # held ~60s out for retry on idle


def test_busy_window_env_override(wt, monkeypatch):
    _write_transcript(wt, SID_B, age_s=30)
    assert wt.messages._session_busy(SID_B) is True  # default window 120s
    monkeypatch.setenv("WATCHTOWER_BUSY_WINDOW_S", "10")
    assert wt.messages._session_busy(SID_B) is False  # 30s old > 10s window


def test_session_state_reports_busy_idle_unknown(wt, monkeypatch):
    _write_transcript(wt, SID_A, age_s=30)
    _write_transcript(wt, SID_B, age_s=600)
    monkeypatch.setenv("WATCHTOWER_BUSY_WINDOW_S", "120")

    assert wt.messages.session_state(SID_A) == "busy"
    assert wt.messages.session_state(SID_B) == "idle"
    assert wt.messages.session_state(SID_C) == "unknown"


def test_drain_delivers_after_transcript_goes_quiet(wt, monkeypatch):
    p = _write_transcript(wt, SID_B, age_s=0)
    calls = []
    monkeypatch.setattr(wt.messages.subprocess, "Popen", _fake_popen(calls))
    res = wt.messages.send(SID_B, "on idle please")
    assert res["queued"] is True and calls == []
    # Transcript goes quiet; the daemon's drain tick now resumes + delivers.
    old = time.time() - 600
    os.utime(p, (old, old))
    out = wt.messages.drain_outbox(now=time.time() + 120)
    assert out["delivered"] and len(calls) == 1
    assert wt.messages.outbox_list()[0]["status"] == "delivered"


# ============================== reap-displacement guard on resume (dup-work fix)
def _claim(wt, ref, *, worker, sid=""):
    """Force a ticket into in_progress under (worker, sid) without a real spawn."""
    wt.q.update_status(ref, "in_progress", session_id=worker, session_uuid=sid)


def test_displaced_claim_detected_when_reopened(wt):
    ref = wt.q.enqueue(project="Q", note="fix a thing", source="test")["ref"]
    _claim(wt, ref, worker="worker-a", sid=SID_A)
    wt.q.update_status(ref, "open", reason="worker gone")  # reaper takes it back
    d = wt.messages._displaced_claim_for_session(SID_A)
    assert d and d["ref"] == ref and "reaped" in d["reason"]
    assert "[WATCHTOWER] STOP" in wt.messages._stale_claim_stop_prefix(SID_A)
    assert ref in wt.messages._stale_claim_stop_prefix(SID_A)


def test_displaced_claim_detected_when_reassigned_and_closed(wt):
    # The exact CCC-502 shape: reaped, reopened, then a DIFFERENT worker closed
    # it while claimed_session_id still points at the reaped session.
    ref = wt.q.enqueue(project="Q", note="fix a thing", source="test")["ref"]
    _claim(wt, ref, worker="worker-a", sid=SID_A)
    wt.q.update_status(ref, "open", reason="worker gone")
    wt.q.close(ref, "worker-b", resolution={"summary": "done by b"})
    d = wt.messages._displaced_claim_for_session(SID_A)
    assert d and d["ref"] == ref and "another worker" in d["reason"]


def test_no_guard_for_normally_finished_worker(wt):
    # Claimed and closed by the same session, no reap: must NOT be flagged, or
    # every worker resumed for its NEXT ticket would be falsely nagged.
    ref = wt.q.enqueue(project="Q", note="fix a thing", source="test")["ref"]
    _claim(wt, ref, worker="worker-a", sid=SID_A)
    wt.q.close(ref, "worker-a", resolution={"summary": "done"})
    assert wt.messages._displaced_claim_for_session(SID_A) is None
    assert wt.messages._stale_claim_stop_prefix(SID_A) == ""


def test_no_guard_while_still_in_progress(wt):
    ref = wt.q.enqueue(project="Q", note="fix a thing", source="test")["ref"]
    _claim(wt, ref, worker="worker-a", sid=SID_A)
    assert wt.messages._displaced_claim_for_session(SID_A) is None


def test_no_guard_for_self_reclaim_after_reap(wt):
    # Reaped, reopened, then the SAME session legitimately reclaimed it: it is
    # back in_progress under this session, so no STOP.
    ref = wt.q.enqueue(project="Q", note="fix a thing", source="test")["ref"]
    _claim(wt, ref, worker="worker-a", sid=SID_A)
    wt.q.update_status(ref, "open", reason="worker gone")
    _claim(wt, ref, worker="worker-a", sid=SID_A)
    assert wt.messages._displaced_claim_for_session(SID_A) is None


def test_no_guard_for_non_claimant_session(wt):
    # A plain chat/orchestrator target that never held a claim passes through.
    wt.q.enqueue(project="Q", note="fix a thing", source="test")
    assert wt.messages._displaced_claim_for_session(SID_B) is None


def test_resume_prepends_stop_prefix_for_displaced_worker(wt, monkeypatch):
    _write_transcript(wt, SID_B, age_s=600)  # idle -> resume adapter runs
    ref = wt.q.enqueue(project="Q", note="fix a thing", source="test")["ref"]
    _claim(wt, ref, worker="worker-a", sid=SID_B)
    wt.q.update_status(ref, "open", reason="worker gone")
    calls = []
    monkeypatch.setattr(wt.messages.subprocess, "Popen", _fake_popen(calls))
    res = wt.messages.send(SID_B, "here is your next ticket")
    assert res["ok"] is True and res["transport"] == "resume"
    delivered = calls[0][2].stdin.data.decode()
    assert "[WATCHTOWER] STOP" in delivered and ref in delivered
    assert "here is your next ticket" in delivered  # original text preserved


def test_resume_no_prefix_for_normal_session(wt, monkeypatch):
    _write_transcript(wt, SID_B, age_s=600)
    calls = []
    monkeypatch.setattr(wt.messages.subprocess, "Popen", _fake_popen(calls))
    res = wt.messages.send(SID_B, "just a normal message")
    assert res["ok"] is True and res["transport"] == "resume"
    delivered = calls[0][2].stdin.data.decode()
    assert "[WATCHTOWER] STOP" not in delivered
    assert "just a normal message" in delivered


# ============================================================ ask (byte-offset)
def test_ask_correlation_reads_only_new_bytes(wt):
    rec = _live_worker(wt, "Q")
    log = Path(rec["log"])
    # Pre-existing bytes from an earlier turn must NOT leak into the answer.
    log.write_text(
        '{"type":"assistant","message":{"content":'
        '[{"type":"text","text":"OLD TURN"}]}}\n'
    )

    def append():
        with open(log, "a") as f:
            f.write(
                '{"type":"assistant","message":{"content":'
                '[{"type":"text","text":"NEW ANSWER"}]}}\n'
            )
            f.write('{"type":"result","result":""}\n')

    t = threading.Timer(0.4, append)
    t.start()
    try:
        res = wt.messages.ask(rec["worker_id"], "question?", timeout_ms=5000)
    finally:
        t.join()
    assert res["ok"] is True and res["source"] == "fifo"
    assert res["answer"] == "NEW ANSWER"
    # The question itself went down the worker FIFO.
    msg = _read_fifo_msg(wt)
    assert msg["message"]["content"][0]["text"] == "question?"


def test_ask_prefers_nonempty_result_field(wt):
    rec = _live_worker(wt, "Q")
    log = Path(rec["log"])

    def append():
        with open(log, "a") as f:
            f.write(
                '{"type":"assistant","message":{"content":'
                '[{"type":"text","text":"chatter"}]}}\n'
            )
            f.write('{"type":"result","result":"FINAL ANSWER"}\n')

    t = threading.Timer(0.3, append)
    t.start()
    try:
        res = wt.messages.ask(rec["worker_id"], "q", timeout_ms=5000)
    finally:
        t.join()
    assert res["ok"] is True and res["answer"] == "FINAL ANSWER"


def test_ask_stop_reason_ends_turn(wt):
    rec = _live_worker(wt, "Q")
    log = Path(rec["log"])

    def append():
        with open(log, "a") as f:
            f.write(
                '{"type":"assistant","message":{"stop_reason":"end_turn",'
                '"content":[{"type":"text","text":"done here"}]}}\n'
            )

    t = threading.Timer(0.3, append)
    t.start()
    try:
        res = wt.messages.ask(rec["worker_id"], "q", timeout_ms=5000)
    finally:
        t.join()
    assert res["ok"] is True and res["answer"] == "done here"


def test_ask_timeout_returns_partial(wt):
    rec = _live_worker(wt, "Q")
    log = Path(rec["log"])

    def append():
        with open(log, "a") as f:
            f.write(
                '{"type":"assistant","message":{"content":'
                '[{"type":"text","text":"part one"}]}}\n'
            )

    t = threading.Timer(0.2, append)
    t.start()
    try:
        res = wt.messages.ask(rec["worker_id"], "q", timeout_ms=1200)
    finally:
        t.join()
    assert res["ok"] is False and res["error"] == "timeout"
    assert res["partial"] == "part one"


# ===================================================================== CLI wiring
def test_cli_parsers_wired(wt):
    import watchtower.cli as cli
    importlib.reload(cli)
    p = cli.build_parser()
    a = p.parse_args([
        "send", "@x", "hi", "--mode", "steer", "--no-queue", "--ttl", "45",
    ])
    assert a.func is cli.cmd_send and a.no_queue is True and a.mode == "steer"
    assert a.ttl == 45.0
    a = p.parse_args(["ask", "@x", "hi", "--timeout", "5"])
    assert a.func is cli.cmd_ask and a.timeout == 5.0
    a = p.parse_args(["outbox", "ls", "--all", "--json"])
    assert a.func is cli.cmd_outbox and a.outbox_command == "ls"
    assert a.all is True and a.json is True
    a = p.parse_args(["outbox", "retry", "msg-123"])
    assert a.func is cli.cmd_outbox and a.outbox_command == "retry"
    assert a.id == "msg-123" and a.all_dead is False
    a = p.parse_args(["outbox", "retry", "--all-dead"])
    assert a.id is None and a.all_dead is True
    a = p.parse_args(["outbox", "rm", "msg-123"])
    assert a.func is cli.cmd_outbox and a.outbox_command == "rm"
    a = p.parse_args(["agents", "--json"])
    assert a.func is cli.cmd_agents
    a = p.parse_args(["agent", "register", "planner", "--session", SID_A])
    assert a.func is cli.cmd_agent and a.agent_command == "register"
    a = p.parse_args(["agent", "set-name", "planner", "--session", SID_A])
    assert a.agent_command == "set-name"
    a = p.parse_args(["agent", "rm", "planner"])
    assert a.agent_command == "rm"


def test_cli_agent_register_and_agents_view(wt, capsys):
    import watchtower.cli as cli
    importlib.reload(cli)
    rc = cli.main(["agent", "set-name", "planner", "--session", SID_A])
    assert rc == 0
    _live_worker(wt, "Q", session_id=SID_B)
    rc = cli.main(["agents", "--json"])
    assert rc == 0
    out = capsys.readouterr().out
    payload = json.loads(out[out.index("{"):])
    assert payload["agents"][0]["name"] == "planner"
    assert len(payload["workers"]) == 1


def test_cli_agents_json_includes_busy_idle_unknown_state(wt, capsys):
    import watchtower.cli as cli
    importlib.reload(cli)
    wt.messages.register_agent("busy", SID_A)
    wt.messages.register_agent("idle", SID_B)
    _write_transcript(wt, SID_A, age_s=0)
    _write_transcript(wt, SID_B, age_s=600)
    worker = _live_worker(wt, "Q", session_id=SID_C)

    rc = cli.main(["agents", "--json"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    states = {a["name"]: a["state"] for a in payload["agents"]}
    assert states == {"busy": "busy", "idle": "idle"}
    assert payload["workers"][0]["worker_id"] == worker["worker_id"]
    assert payload["workers"][0]["state"] == "unknown"


def test_cli_agents_human_output_has_state_column(wt, capsys):
    import watchtower.cli as cli
    importlib.reload(cli)
    wt.messages.register_agent("planner", SID_A)
    _write_transcript(wt, SID_A, age_s=0)

    rc = cli.main(["agents"])

    assert rc == 0
    out = capsys.readouterr().out
    assert "STATE" in out.splitlines()[0]
    assert "busy" in out


def test_cli_agents_human_output_labels_recent_rows(wt, capsys):
    import watchtower.cli as cli
    importlib.reload(cli)
    _write_transcript(wt, SID_A, age_s=60)  # no registry entry: auto-discovered "recent"

    rc = cli.main(["agents"])

    assert rc == 0
    lines = capsys.readouterr().out.splitlines()
    row = next(l for l in lines if SID_A in l)
    assert row.split()[0] == SID_A[:8]
    assert "recent" in row
    assert "@" not in row


def test_cli_outbox_ls_retry_and_rm(wt, capsys):
    import watchtower.cli as cli
    importlib.reload(cli)
    t0 = 1_600_000.0
    pending = wt.messages.outbox_add(SID_A, "pending", now=t0)
    dead = wt.messages.outbox_add(SID_B, "dead", now=t0)
    data = wt.messages._load_outbox()
    data["messages"][1].update({
        "status": "dead",
        "attempts": 4,
        "last_error": "failed",
    })
    wt.messages._save_outbox(data)

    assert cli.main(["outbox", "ls", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert [m["id"] for m in payload["messages"]] == [pending["id"]]

    assert cli.main(["outbox", "ls", "--all", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert {m["id"] for m in payload["messages"]} == {pending["id"], dead["id"]}

    assert cli.main(["outbox", "retry", "--all-dead"]) == 0
    capsys.readouterr()
    by_id = {m["id"]: m for m in wt.messages.outbox_list()}
    assert by_id[dead["id"]]["status"] == "pending"
    assert by_id[dead["id"]]["attempts"] == 0

    assert cli.main(["outbox", "rm", pending["id"]]) == 0
    capsys.readouterr()
    assert pending["id"] not in {m["id"] for m in wt.messages.outbox_list()}


# ------------------------------------------------------------- WT-49: renaming
def test_set_session_title_appends_custom_title_event(wt):
    """set_session_title writes the exact event shape Claude Code's own
    /rename command writes, verified against claude-command-center's
    rename_session/_append_custom_title (see docs/session-naming.md)."""
    p = _write_transcript(wt, SID_A)
    assert wt.messages.set_session_title(SID_A, "WT#1: repair title") is True
    lines = [l for l in p.read_text().splitlines() if l.strip()]
    assert json.loads(lines[-1]) == {
        "type": "custom-title", "customTitle": "WT#1: repair title", "sessionId": SID_A,
    }


def test_set_session_title_is_idempotent(wt):
    """A reconciler tick may retry the same repair many times; exact repeats
    must not append unbounded duplicate custom-title events."""
    p = _write_transcript(wt, SID_A)

    assert wt.messages.set_session_title(SID_A, "WT#1: repair title") is True
    assert wt.messages.set_session_title(SID_A, "WT#1: repair title") is False

    lines = [json.loads(l) for l in p.read_text().splitlines() if l.strip()]
    title_events = [ev for ev in lines if ev.get("type") == "custom-title"]
    assert title_events == [
        {"type": "custom-title", "customTitle": "WT#1: repair title", "sessionId": SID_A}
    ]


def test_set_session_title_no_transcript_is_noop(wt):
    """No transcript on disk yet (e.g. non-claude engine) -> False, no crash."""
    assert wt.messages.set_session_title(SID_B, "anything") is False


def test_backfill_renames_session_once_uuid_resolves(wt):
    """A fresh `wt claim` fires before WT knows the worker's real session
    UUID (claimed_by is the non-UUID worker_id), so cli.cmd_claim's own
    rename is usually a no-op. backfill_claimed_session_ids is the moment
    the UUID becomes known -- confirm it renames the transcript there."""
    p = _write_transcript(wt, SID_A)
    rec = _live_worker(wt, "NAMEQ", session_id=SID_A)
    item = wt.q.enqueue(project="NAMEQ", note="do a thing")
    wt.q.claim_next(rec["worker_id"], project="NAMEQ")

    backfilled = wt.workers.backfill_claimed_session_ids()
    assert backfilled == [item["ref"]]

    lines = [l for l in p.read_text().splitlines() if l.strip()]
    assert json.loads(lines[-1]) == {
        "type": "custom-title",
        "customTitle": f"NAMEQ#{item['seq']}: do a thing",
        "sessionId": SID_A,
    }


def test_cli_close_renames_session_with_summary(wt):
    """wt close renames the claiming session's transcript to carry the
    outcome, when its claimed_session_id is already known (e.g. it was
    backfilled earlier in the ticket's lifetime, as in practice it always
    is by close time)."""
    import watchtower.cli as cli
    importlib.reload(cli)

    p = _write_transcript(wt, SID_A)
    item = wt.q.enqueue(project="NAMEQ", note="do a thing")
    wt.q.claim_next("w1", project="NAMEQ")
    wt.q.backfill_session_id(item["ref"], SID_A)

    assert cli.main(["close", item["ref"], "--summary", "fixed the thing"]) == 0

    lines = [l for l in p.read_text().splitlines() if l.strip()]
    assert json.loads(lines[-1]) == {
        "type": "custom-title",
        "customTitle": f"NAMEQ#{item['seq']}: fixed the thing",
        "sessionId": SID_A,
    }


def test_recent_session_title_backfill_dry_run_uses_latest_ticket_context(wt):
    recent_worker = "nameq-recent"
    stale_worker = "nameq-stale"
    _write_worker_log(wt, recent_worker, SID_A)
    _write_worker_log(wt, stale_worker, SID_B, age_s=25 * 3600)

    recent = wt.q.enqueue(project="NAMEQ", title="Add useful titles", note="fallback")
    wt.q.claim_next(recent_worker, project="NAMEQ")
    wt.q.close(recent["ref"], recent_worker, resolution="added durable titles")

    stale = wt.q.enqueue(project="NAMEQ", title="Old work", note="body")
    wt.q.claim_next(stale_worker, project="NAMEQ")
    wt.q.close(stale["ref"], stale_worker, resolution="old summary")

    result = wt.workers.backfill_recent_session_titles(hours=24, dry_run=True)

    assert result == [
        {
            "worker_id": recent_worker,
            "session_id": SID_A,
            "ref": recent["ref"],
            "title": f"NAMEQ#{recent['seq']}: added durable titles",
            "updated": False,
        }
    ]


def test_recent_session_title_backfill_appends_custom_title(wt):
    worker_id = "nameq-recent"
    _write_worker_log(wt, worker_id, SID_A)
    p = _write_transcript(wt, SID_A)

    item = wt.q.enqueue(project="NAMEQ", title="Add useful titles", note="body")
    wt.q.claim_next(worker_id, project="NAMEQ")

    result = wt.workers.backfill_recent_session_titles(hours=24)

    assert result == [
        {
            "worker_id": worker_id,
            "session_id": SID_A,
            "ref": item["ref"],
            "title": f"NAMEQ#{item['seq']}: Add useful titles",
            "updated": True,
        }
    ]
    lines = [l for l in p.read_text().splitlines() if l.strip()]
    assert json.loads(lines[-1]) == {
        "type": "custom-title",
        "customTitle": f"NAMEQ#{item['seq']}: Add useful titles",
        "sessionId": SID_A,
    }


def test_recent_session_title_backfill_uses_queue_session_when_log_is_gone(wt):
    p = _write_transcript(wt, SID_A)
    item = wt.q.enqueue(project="NAMEQ", title="Log already pruned", note="body")
    wt.q.claim_next("nameq-pruned", project="NAMEQ")
    wt.q.backfill_session_id(item["ref"], SID_A)
    wt.q.close(item["ref"], "nameq-pruned", resolution="renamed without log")

    result = wt.workers.backfill_recent_session_titles(hours=24)

    assert result == [
        {
            "worker_id": "nameq-pruned",
            "session_id": SID_A,
            "ref": item["ref"],
            "title": f"NAMEQ#{item['seq']}: renamed without log",
            "updated": True,
        }
    ]
    lines = [l for l in p.read_text().splitlines() if l.strip()]
    assert json.loads(lines[-1]) == {
        "type": "custom-title",
        "customTitle": f"NAMEQ#{item['seq']}: renamed without log",
        "sessionId": SID_A,
    }


def test_recent_session_title_backfill_keeps_one_latest_title_per_session(wt):
    worker_id = "nameq-recent"
    _write_worker_log(wt, worker_id, SID_A)
    p = _write_transcript(wt, SID_A)

    old = wt.q.enqueue(project="NAMEQ", title="Old work", note="body")
    wt.q.claim_next(worker_id, project="NAMEQ")
    wt.q.backfill_session_id(old["ref"], SID_A)
    wt.q.close(old["ref"], worker_id, resolution="older title")

    new = wt.q.enqueue(project="NAMEQ", title="New work", note="body")
    wt.q.claim_next(worker_id, project="NAMEQ")
    wt.q.backfill_session_id(new["ref"], SID_A)
    wt.q.close(new["ref"], worker_id, resolution="newest title")

    result = wt.workers.backfill_recent_session_titles(hours=24)

    assert result == [
        {
            "worker_id": worker_id,
            "session_id": SID_A,
            "ref": new["ref"],
            "title": f"NAMEQ#{new['seq']}: newest title",
            "updated": True,
        }
    ]
    lines = [json.loads(l) for l in p.read_text().splitlines() if l.strip()]
    title_events = [ev for ev in lines if ev.get("type") == "custom-title"]
    assert title_events == [
        {
            "type": "custom-title",
            "customTitle": f"NAMEQ#{new['seq']}: newest title",
            "sessionId": SID_A,
        }
    ]


def test_reconcile_backfills_recent_session_titles(wt):
    """Closed tickets can miss claimed_session_id if the worker UUID was never
    propagated before close. Reconcile should repair the transcript from logs
    instead of waiting for a hidden manual command."""
    worker_id = "nameq-recent"
    _write_worker_log(wt, worker_id, SID_A)
    p = _write_transcript(wt, SID_A)

    item = wt.q.enqueue(project="NAMEQ", title="Log-known close", note="body")
    wt.q.claim_next(worker_id, project="NAMEQ")
    wt.q.close(item["ref"], worker_id, resolution="renamed from reconcile")

    result = wt.workers.reconcile_once(dry_run=False)

    assert result["session_title_backfilled"] == [
        {
            "worker_id": worker_id,
            "session_id": SID_A,
            "ref": item["ref"],
            "title": f"NAMEQ#{item['seq']}: renamed from reconcile",
            "updated": True,
        }
    ]
    lines = [l for l in p.read_text().splitlines() if l.strip()]
    assert json.loads(lines[-1]) == {
        "type": "custom-title",
        "customTitle": f"NAMEQ#{item['seq']}: renamed from reconcile",
        "sessionId": SID_A,
    }


def test_session_names_backfill_cli_dry_run(wt, capsys):
    import watchtower.cli as cli
    importlib.reload(cli)

    worker_id = "nameq-recent"
    _write_worker_log(wt, worker_id, SID_A)
    item = wt.q.enqueue(project="NAMEQ", title="Add useful titles", note="body")
    wt.q.claim_next(worker_id, project="NAMEQ")

    assert cli.main(["session-names", "backfill", "--hours", "24", "--dry-run"]) == 0
    rows = json.loads(capsys.readouterr().out)

    assert rows == [
        {
            "worker_id": worker_id,
            "session_id": SID_A,
            "ref": item["ref"],
            "title": f"NAMEQ#{item['seq']}: Add useful titles",
            "updated": False,
        }
    ]

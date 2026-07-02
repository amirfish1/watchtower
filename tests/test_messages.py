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

    # No codex binary by default: is_available() must read False so the
    # codex-app-server adapter is a clean no-op miss in every test that
    # doesn't explicitly opt in (see test_deliver_codex_target below).
    monkeypatch.delenv("WATCHTOWER_CODEX_BIN", raising=False)

    import watchtower.queue as q
    import watchtower.config as config
    import watchtower.workers as workers
    import watchtower.messages as messages
    import watchtower.codex_rpc as codex_rpc
    importlib.reload(q)
    importlib.reload(config)
    importlib.reload(workers)
    importlib.reload(messages)
    importlib.reload(codex_rpc)
    monkeypatch.setattr(config, "_REGISTRY_FILE", tmp_path / "no-registry.json")

    class Ns:
        pass
    ns = Ns()
    ns.q, ns.config, ns.workers, ns.messages = q, config, workers, messages
    ns.codex_rpc = codex_rpc
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
    assert body == {"session_id": SID_B, "text": "over http", "mode": "steer"}


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

    assert res == {"ok": True, "transport": "codex-app-server"}


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
    class FakeProc:
        pid = 424242

    def popen(argv, **kw):
        calls.append((argv, kw))
        return FakeProc()
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

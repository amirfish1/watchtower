"""`wt chat` CLI, daemon nudge-tick wiring, and dashboard messaging API.

Everything runs against a fully isolated sandbox (queue store, workers,
config, agents registry, outbox, and chats dir all under tmp_path), same
env-override pattern as tests/test_messages.py and tests/test_chats.py.
``messages.send`` is monkeypatched wherever a test would otherwise trigger
real delivery (fifo/resume/delegate): no real ``claude`` is ever spawned and
no real HTTP call leaves the process except against our own in-thread test
server for the dashboard endpoint tests.
"""

from __future__ import annotations

import importlib
import json
import os
import threading
import urllib.request

import pytest

SID_A = "aaaa1111-0000-4000-8000-000000000001"
SID_B = "bbbb2222-0000-4000-8000-000000000002"


@pytest.fixture()
def wt(tmp_path, monkeypatch):
    """Isolated WatchTower: fresh store, workers, agents, outbox, chats dir."""
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_WORKERS_FILE", str(tmp_path / "workers.json"))
    monkeypatch.setenv("WATCHTOWER_CONFIG_FILE", str(tmp_path / "config.json"))
    monkeypatch.setenv("WATCHTOWER_STOP_SIGNALS_DIR", str(tmp_path / "stop-signals"))
    monkeypatch.setenv(
        "WATCHTOWER_WORKER_SESSIONS_FILE", str(tmp_path / "worker-sessions.json")
    )
    monkeypatch.setenv("WATCHTOWER_AGENTS_FILE", str(tmp_path / "agents.json"))
    monkeypatch.setenv("WATCHTOWER_OUTBOX_FILE", str(tmp_path / "outbox.json"))
    monkeypatch.setenv("WATCHTOWER_CHATS_DIR", str(tmp_path / "chats"))
    monkeypatch.setenv("WATCHTOWER_DASHBOARD_PID", str(tmp_path / "dashboard.pid"))
    monkeypatch.setenv("WATCHTOWER_DAEMON_PID", str(tmp_path / "daemon.pid"))
    # No delegate: WT standalone must be exercised, and tests must never hit a
    # real CCC via ~/.claude/command-center/port.txt.
    monkeypatch.setenv("WATCHTOWER_DELEGATE_URL", "off")
    monkeypatch.setenv(
        "WATCHTOWER_CLAUDE_PROJECTS_DIR", str(tmp_path / "claude-projects")
    )

    import watchtower.queue as q
    import watchtower.config as config
    import watchtower.workers as workers
    import watchtower.messages as messages
    import watchtower.chats as chats
    import watchtower.cli as cli
    import watchtower.dashboard as dashboard

    importlib.reload(q)
    importlib.reload(config)
    importlib.reload(workers)
    importlib.reload(messages)
    importlib.reload(chats)
    importlib.reload(cli)
    importlib.reload(dashboard)
    monkeypatch.setattr(config, "_REGISTRY_FILE", tmp_path / "no-registry.json")
    monkeypatch.setattr(q, "_ACTIVITY_LOG", tmp_path / "activity.log")

    class Ns:
        pass

    ns = Ns()
    ns.q = q
    ns.config = config
    ns.workers = workers
    ns.messages = messages
    ns.chats = chats
    ns.cli = cli
    ns.dashboard = dashboard
    ns.tmp_path = tmp_path
    return ns


@pytest.fixture()
def stub_send(wt, monkeypatch):
    """Replace messages.send with an in-memory recorder; never hits real
    delivery. Returns the list of {"target", "text", "mode"} calls."""
    calls = []

    def _fake_send(target, text, mode="send", queue_on_fail=True):
        calls.append({"target": target, "text": text, "mode": mode})
        return {"ok": True, "transport": "stub"}

    monkeypatch.setattr(wt.messages, "send", _fake_send)
    return calls


# --------------------------------------------------------------------- new
def test_chat_new_creates_chat_and_checks_in_every_participant(wt, stub_send):
    wt.messages.register_agent("planner", SID_A, engine="claude")
    wt.messages.register_agent("builder", SID_B, engine="claude")

    parser = wt.cli.build_parser()
    args = parser.parse_args([
        "chat", "new", "Ship the feature",
        "--with", "@planner,@builder", "--include-human", "--json",
    ])
    assert args.func(args) == 0

    assert len(stub_send) == 2
    targets = {c["target"] for c in stub_send}
    assert targets == {SID_A, SID_B}
    for c in stub_send:
        assert "group-chat-checkin" in c["text"]

    rows = wt.chats.list_chats()
    assert len(rows) == 1
    assert rows[0]["topic"] == "Ship the feature"
    names = {p["name"] for p in rows[0]["participants"]}
    assert names == {"planner", "builder"}


def test_chat_new_reports_unresolvable_target(wt, stub_send):
    parser = wt.cli.build_parser()
    args = parser.parse_args(
        ["chat", "new", "topic", "--with", "@nobody-registered"]
    )
    assert args.func(args) == 1
    assert not stub_send
    assert not wt.chats.list_chats()


# -------------------------------------------------------------------- post
def test_chat_post_defaults_to_human_and_as_resolves_participant(wt, capsys):
    info = wt.chats.create_chat(
        "Topic", [{"session_id": SID_A, "name": "planner"}]
    )
    parser = wt.cli.build_parser()

    args = parser.parse_args(["chat", "post", info["path"], "hello team"])
    assert args.func(args) == 0
    data = wt.chats.read_chat(info["path"])
    assert data["messages"][-1]["author_name"] == "Human"

    args2 = parser.parse_args(
        ["chat", "post", info["path"], "on it", "--as", "planner"]
    )
    assert args2.func(args2) == 0
    data2 = wt.chats.read_chat(info["path"])
    last = data2["messages"][-1]
    assert last["author_sid8"] == SID_A[:8]
    assert last["body"] == "on it"


def test_chat_post_unknown_as_target_errors(wt):
    info = wt.chats.create_chat(
        "Topic", [{"session_id": SID_A, "name": "planner"}]
    )
    parser = wt.cli.build_parser()
    args = parser.parse_args(
        ["chat", "post", info["path"], "hi", "--as", "not-a-participant"]
    )
    assert args.func(args) == 1


# -------------------------------------------------------------------- read
def test_chat_read_tail_and_json(wt, capsys):
    info = wt.chats.create_chat(
        "Topic", [{"session_id": SID_A, "name": "planner"}]
    )
    wt.chats.post(info["path"], "first")
    wt.chats.post(info["path"], "second")
    wt.chats.post(info["path"], "third")

    parser = wt.cli.build_parser()
    args = parser.parse_args(["chat", "read", info["path"], "--tail", "1", "--json"])
    assert args.func(args) == 0
    out = json.loads(capsys.readouterr().out)
    assert len(out["messages"]) == 1
    assert out["messages"][0]["body"] == "third"

    args2 = parser.parse_args(["chat", "read", info["path"]])
    assert args2.func(args2) == 0
    text_out = capsys.readouterr().out
    assert "first" in text_out and "second" in text_out and "third" in text_out


# ---------------------------------------------------------------------- ls
def test_chat_ls_json_and_archived_filter(wt, capsys):
    a = wt.chats.create_chat("Alpha", [{"session_id": SID_A, "name": "planner"}])
    b = wt.chats.create_chat("Beta", [{"session_id": SID_B, "name": "builder"}])
    wt.chats.set_archived(b["path"], True)

    parser = wt.cli.build_parser()
    args = parser.parse_args(["chat", "ls", "--json"])
    assert args.func(args) == 0
    rows = json.loads(capsys.readouterr().out)
    assert len(rows) == 1
    assert rows[0]["topic"] == "Alpha"

    args2 = parser.parse_args(["chat", "ls", "--archived", "--json"])
    assert args2.func(args2) == 0
    rows2 = json.loads(capsys.readouterr().out)
    assert {r["topic"] for r in rows2} == {"Alpha", "Beta"}


# ------------------------------------------------------------------- nudge
def test_chat_nudge_explicit_target(wt, stub_send):
    info = wt.chats.create_chat(
        "Topic",
        [
            {"session_id": SID_A, "name": "planner"},
            {"session_id": SID_B, "name": "builder"},
        ],
    )
    parser = wt.cli.build_parser()
    args = parser.parse_args(["chat", "nudge", info["path"], "--target", "planner"])
    assert args.func(args) == 0
    assert len(stub_send) == 1
    assert stub_send[0]["target"] == SID_A


def test_chat_nudge_deterministic_targeting_when_no_target_given(wt, stub_send):
    info = wt.chats.create_chat(
        "Topic",
        [
            {"session_id": SID_A, "name": "planner"},
            {"session_id": SID_B, "name": "builder"},
        ],
    )
    # Last entry authored by planner (SID_A) -> nudge everyone else (builder).
    wt.chats.post(info["path"], "status update", author_sid=SID_A, author_name="planner")

    parser = wt.cli.build_parser()
    args = parser.parse_args(["chat", "nudge", info["path"]])
    assert args.func(args) == 0
    assert len(stub_send) == 1
    assert stub_send[0]["target"] == SID_B


# ------------------------------------------------------- add/leave/archive/close
def test_chat_add_leave_archive_close(wt):
    info = wt.chats.create_chat("Topic", [{"session_id": SID_A, "name": "planner"}])
    wt.messages.register_agent("builder", SID_B, engine="claude")

    parser = wt.cli.build_parser()
    add_args = parser.parse_args(["chat", "add", info["path"], "@builder"])
    assert add_args.func(add_args) == 0
    _, sidecar = wt.chats.find_chat(info["path"])
    assert SID_B in sidecar["session_ids"]

    leave_args = parser.parse_args(["chat", "leave", info["path"], "planner"])
    assert leave_args.func(leave_args) == 0
    _, sidecar2 = wt.chats.find_chat(info["path"])
    assert SID_A not in sidecar2["session_ids"]

    archive_args = parser.parse_args(["chat", "archive", info["path"]])
    assert archive_args.func(archive_args) == 0
    _, sidecar3 = wt.chats.find_chat(info["path"])
    assert sidecar3["archived"] is True

    close_args = parser.parse_args(["chat", "close", info["path"]])
    assert close_args.func(close_args) == 0
    _, sidecar4 = wt.chats.find_chat(info["path"])
    assert sidecar4["closed_at"]


def test_chat_command_with_no_subcommand_errors(wt):
    parser = wt.cli.build_parser()
    args = parser.parse_args(["chat"])
    assert args.func(args) == 1


# ---------------------------------------------------- daemon tick wiring
def test_daemon_loop_calls_nudge_tick_and_survives_its_exception(wt, monkeypatch):
    """chats.nudge_tick is invoked from _daemon_loop every tick, and an
    exception it raises must not kill the reconcile/drain loop (same
    never-die contract as messages.drain_outbox above it)."""
    calls = {"nudge_tick": 0}

    def _boom(deliver=None, now=None):
        calls["nudge_tick"] += 1
        raise RuntimeError("boom from chats.nudge_tick")

    monkeypatch.setattr(wt.chats, "nudge_tick", _boom)
    monkeypatch.setattr(
        wt.workers, "reconcile_once", lambda dry_run=False: {"spawned": [], "stopped": []}
    )
    monkeypatch.setattr(
        wt.messages, "drain_outbox",
        lambda: {"delivered": [], "retried": [], "dead": []},
    )

    # Pretend the dashboard is already up so _daemon_loop skips the HTTP bind
    # (we don't need a real server for this test, just the tick logic).
    wt.cli.DASHBOARD_PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    wt.cli.DASHBOARD_PID_FILE.write_text(str(os.getpid()))

    class _StopLoop(Exception):
        pass

    def _fake_sleep(_seconds):
        raise _StopLoop()

    monkeypatch.setattr(wt.cli.time, "sleep", _fake_sleep)

    class Args:
        interval = 5
        dry_run = False
        host = "127.0.0.1"
        port = 8787
        auto_spawn = False
        stuck_minutes = 20
        engine = "claude"

    with pytest.raises(_StopLoop):
        wt.cli._daemon_loop(Args())

    # nudge_tick ran (and raised) but the loop reached time.sleep anyway,
    # proving the exception was swallowed rather than propagating.
    assert calls["nudge_tick"] == 1


# --------------------------------------------------------- dashboard: HTTP
def _post_json(port, path, payload, origin=None):
    headers = {"Content-Type": "application/json"}
    if origin is not None:
        headers["Origin"] = origin
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=json.dumps(payload).encode(),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


def _serve_once(dashboard):
    httpd = dashboard.ThreadingHTTPServer(("127.0.0.1", 0), dashboard._Handler)
    port = httpd.server_address[1]
    t = threading.Thread(target=httpd.handle_request, daemon=True)
    t.start()
    return httpd, port, t


def test_dashboard_api_send_round_trip(wt, stub_send):
    dashboard = wt.dashboard
    httpd, port, t = _serve_once(dashboard)
    try:
        status, body = _post_json(
            port, "/api/send", {"to": SID_A, "text": "hi", "mode": "send"}
        )
    finally:
        t.join(timeout=5)
        httpd.server_close()
    assert status == 200
    assert body["ok"] is True
    assert stub_send and stub_send[0]["target"] == SID_A


def test_dashboard_api_ask_round_trip(wt, monkeypatch):
    def _fake_ask(target, text, timeout_ms=30000):
        return {"ok": True, "answer": "42", "source": "stub"}

    monkeypatch.setattr(wt.messages, "ask", _fake_ask)
    dashboard = wt.dashboard
    httpd, port, t = _serve_once(dashboard)
    try:
        status, body = _post_json(
            port, "/api/ask", {"to": SID_A, "text": "what is it?", "timeout_ms": 1000}
        )
    finally:
        t.join(timeout=5)
        httpd.server_close()
    assert status == 200
    assert body["answer"] == "42"


def test_dashboard_api_chat_create_and_post_and_get(wt):
    dashboard = wt.dashboard

    httpd, port, t = _serve_once(dashboard)
    try:
        status, body = _post_json(port, "/api/chat/create", {
            "topic": "Roadmap",
            "participants": [{"session_id": SID_A, "name": "planner"}],
            "include_human": True,
        })
    finally:
        t.join(timeout=5)
        httpd.server_close()
    assert status == 200
    assert body["ok"] is True
    ref = body["path"]

    httpd2, port2, t2 = _serve_once(dashboard)
    try:
        status2, body2 = _post_json(
            port2, "/api/chat/post", {"ref": ref, "body": "hello", "author": "Human"}
        )
    finally:
        t2.join(timeout=5)
        httpd2.server_close()
    assert status2 == 200
    assert body2["ok"] is True

    httpd3 = dashboard.ThreadingHTTPServer(("127.0.0.1", 0), dashboard._Handler)
    port3 = httpd3.server_address[1]
    t3 = threading.Thread(target=httpd3.handle_request, daemon=True)
    t3.start()
    try:
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port3}/api/chats", timeout=5
        ) as resp:
            listed = json.loads(resp.read().decode())
    finally:
        t3.join(timeout=5)
        httpd3.server_close()
    assert any(c["topic"] == "Roadmap" for c in listed["chats"])

    httpd4 = dashboard.ThreadingHTTPServer(("127.0.0.1", 0), dashboard._Handler)
    port4 = httpd4.server_address[1]
    t4 = threading.Thread(target=httpd4.handle_request, daemon=True)
    t4.start()
    try:
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port4}/api/chat/{ref}", timeout=5
        ) as resp:
            read_back = json.loads(resp.read().decode())
    finally:
        t4.join(timeout=5)
        httpd4.server_close()
    assert read_back["topic"] == "Roadmap"
    assert read_back["messages"][-1]["body"] == "hello"


# ---------------------------------------------------- same-origin asymmetry
def test_dashboard_messaging_endpoints_reject_foreign_origin(wt, stub_send):
    dashboard = wt.dashboard
    httpd, port, t = _serve_once(dashboard)
    try:
        status, body = _post_json(
            port, "/api/send", {"to": SID_A, "text": "hi"},
            origin="https://evil.example.com",
        )
    finally:
        t.join(timeout=5)
        httpd.server_close()
    assert status == 403
    assert not stub_send  # rejected before messages.send ever ran


def test_dashboard_queue_add_allows_foreign_origin_on_purpose(wt):
    """The SAME foreign Origin that gets 403'd on /api/send must NOT be
    rejected on /api/queue/<name>/add -- the annotate widget deliberately
    posts cross-origin there. This proves the asymmetry from
    dashboard._check_same_origin's docstring is real, not accidental."""
    dashboard = wt.dashboard
    httpd, port, t = _serve_once(dashboard)
    try:
        status, body = _post_json(
            port, "/api/queue/WT/add", {"title": "from a widget"},
            origin="https://evil.example.com",
        )
    finally:
        t.join(timeout=5)
        httpd.server_close()
    assert status == 200
    assert body["ok"] is True

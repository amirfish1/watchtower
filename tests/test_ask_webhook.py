"""`wt ask --notify-webhook` (WT-59): fire-and-forget ask for scripts.

Mirrors wt wait's webhook. Parent invocation must return immediately after
spawning a detached child; the child does the blocking ask and POSTs the
answer. No real claude, no real HTTP: everything is monkeypatched."""

from __future__ import annotations

import sys

from test_messages import wt  # noqa: F401


def _cli(wt):
    import importlib
    import watchtower.cli as cli
    importlib.reload(cli)
    return cli


def test_parent_spawns_detached_child_and_returns_immediately(wt, monkeypatch):
    cli = _cli(wt)
    spawned = []

    def fake_popen(cmd, **kw):
        spawned.append((cmd, kw))
        class P:
            pid = 999
        return P()

    def boom(*a, **k):
        raise AssertionError("parent must not block on messages.ask")

    monkeypatch.setattr(cli.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(cli.messages if hasattr(cli, "messages") else
                        __import__("watchtower.messages", fromlist=["ask"]),
                        "ask", boom)
    rc = cli.main([
        "ask", "some-target", "what is status?",
        "--notify-webhook", "http://127.0.0.1:9/hook", "--timeout", "5",
    ])
    assert rc == 0
    assert len(spawned) == 1
    cmd, kw = spawned[0]
    assert cmd[0] == sys.executable
    assert cmd[1:3] == ["-m", "watchtower.cli"]
    assert "--_notify-child" in cmd and "--notify-webhook" in cmd
    assert kw.get("start_new_session") is True


def test_child_asks_and_posts_answer(wt, monkeypatch):
    cli = _cli(wt)
    import watchtower.messages as messages
    posted = []
    monkeypatch.setattr(
        messages, "ask", lambda *a, **k: {"ok": True, "answer": "42"}
    )
    monkeypatch.setattr(cli, "_post_webhook", lambda url, p: posted.append((url, p)))
    rc = cli.main([
        "ask", "some-target", "meaning of life?",
        "--notify-webhook", "http://127.0.0.1:9/hook", "--_notify-child",
    ])
    assert rc == 0
    url, payload = posted[0]
    assert url == "http://127.0.0.1:9/hook"
    assert payload == {
        "event": "ask-answered", "target": "some-target",
        "ok": True, "answer": "42",
    }


def test_child_posts_failure_with_partial(wt, monkeypatch):
    cli = _cli(wt)
    import watchtower.messages as messages
    posted = []
    monkeypatch.setattr(
        messages, "ask",
        lambda *a, **k: {"ok": False, "error": "timeout", "partial": "half"},
    )
    monkeypatch.setattr(cli, "_post_webhook", lambda url, p: posted.append((url, p)))
    rc = cli.main([
        "ask", "some-target", "q?",
        "--notify-webhook", "http://127.0.0.1:9/hook", "--_notify-child",
    ])
    assert rc == 1
    _, payload = posted[0]
    assert payload["event"] == "ask-failed"
    assert payload["error"] == "timeout" and payload["partial"] == "half"

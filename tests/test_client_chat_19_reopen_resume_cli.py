"""CLIENT-CHAT-19: `wt reopen --resume` CLI wiring.

Verifies cmd_reopen's --resume branch: reopen_and_claim then delivery
through the same liveness-aware path `wt answer` uses (mocked here since
the delivery mechanics themselves are covered by test_answer_resume.py).
"""
from __future__ import annotations

import argparse
import importlib

import pytest


@pytest.fixture()
def wt(tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_WORKERS_FILE", str(tmp_path / "workers.json"))
    monkeypatch.setenv("WATCHTOWER_WORKER_IDS_FILE", str(tmp_path / "worker-ids.json"))
    monkeypatch.setenv(
        "WATCHTOWER_WORKER_SESSIONS_FILE", str(tmp_path / "worker-sessions.json")
    )
    monkeypatch.setenv(
        "WATCHTOWER_CODEX_THREAD_REGISTRY", str(tmp_path / "codex-threads.json")
    )
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(tmp_path / "activity.log"))

    import watchtower.cli as cli
    import watchtower.queue as q
    import watchtower.workers as workers

    importlib.reload(q)
    importlib.reload(workers)
    importlib.reload(cli)
    return cli, q, workers


def test_reopen_resume_rebinds_and_delivers(wt, tmp_path, monkeypatch):
    cli, q, workers = wt
    worker_id = "client-chat-19-cli-worker"
    sid = "77777777-8888-9999-aaaa-bbbbbbbbbbbb"
    item = q.enqueue(project="CLIENTCHAT19CLITEST", note="topic work")
    claimed = q.claim_next(worker_id, project="CLIENTCHAT19CLITEST", session_uuid=sid)
    q.update_status(claimed["ref"], "closed", worker_id, resolution={"summary": "turn done"})

    delivered = {}

    def fake_deliver(item_arg, text, prompt, engine, worker):
        delivered["item"] = item_arg
        delivered["text"] = text
        delivered["prompt"] = prompt
        return 0

    monkeypatch.setattr(cli, "_deliver_to_blocked_session", fake_deliver)

    args = argparse.Namespace(
        ref=claimed["ref"], reason="", worker="", force=False,
        resume="the client is back", engine=None, json=False,
    )
    assert cli.cmd_reopen(args) == 0
    assert delivered["item"]["status"] == "in_progress"
    assert delivered["item"]["claimed_session_id"] == sid
    assert delivered["text"] == "the client is back"
    assert "re-engaged" in delivered["prompt"]

    final = q.get(claimed["ref"])
    assert final["status"] == "in_progress"
    assert final["claimed_session_id"] == sid


def test_reopen_resume_falls_back_to_plain_reopen_without_session(wt, tmp_path, monkeypatch):
    """A ticket that was never claimed with a real session id -- --resume
    must degrade to a plain reopen, not crash or silently drop the text."""
    cli, q, workers = wt
    item = q.enqueue(project="CLIENTCHAT19CLITEST", note="never claimed")
    # Close it directly (no claim_next -> no claimed_session_id).
    q.update_status(item["ref"], "closed", "some-human", resolution={"summary": "done"})

    called = {"deliver": False}
    monkeypatch.setattr(
        cli, "_deliver_to_blocked_session",
        lambda *a, **k: called.__setitem__("deliver", True) or 0,
    )

    args = argparse.Namespace(
        ref=item["ref"], reason="", worker="", force=False,
        resume="hello again", engine=None, json=False,
    )
    assert cli.cmd_reopen(args) == 0
    assert called["deliver"] is False  # no session -> no delivery attempted
    final = q.get(item["ref"])
    assert final["status"] == "open"


def test_reopen_without_resume_flag_uses_plain_path_unchanged(wt, tmp_path):
    """--resume defaults to None -- omitting it entirely must be
    byte-for-byte today's existing plain reopen behavior."""
    cli, q, workers = wt
    item = q.enqueue(project="CLIENTCHAT19CLITEST", note="topic work")
    worker_id = "client-chat-19-plain-worker"
    q.claim_next(worker_id, project="CLIENTCHAT19CLITEST")
    q.update_status(item["ref"], "closed", worker_id, resolution={"summary": "done"})

    args = argparse.Namespace(
        ref=item["ref"], reason="triage", worker="", force=False,
        resume=None, engine=None, json=False,
    )
    assert cli.cmd_reopen(args) == 0
    final = q.get(item["ref"])
    assert final["status"] == "open"

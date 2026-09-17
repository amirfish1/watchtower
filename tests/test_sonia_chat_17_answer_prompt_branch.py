"""SONIA-CHAT-17: cmd_answer's delivered prompt branches on block_kind.

Three variants:
1. Default (no block_kind / not awaiting-client) -- today's unchanged
   behavior: "apply it, finish the ticket, and close it."
2. awaiting-client, ordinary continuation -- "do this turn, then park
   again with wt block --kind awaiting-client unless the topic is
   finished."
3. awaiting-client, TID-flavored answer (--tid flag) -- "push anything
   local, close with the last SHA (or --no-code), append your learnings
   line, message the client only if you actually pushed something."
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


def _capturing_deliver(captured):
    def _deliver(item, text, prompt, engine, worker):
        captured["prompt"] = prompt
        return 0
    return _deliver


def _make_blocked_ticket(q, workers, tmp_path, *, kind: str = "") -> tuple[dict, str]:
    worker_id = "sonia-chat-17-worker"
    sid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    item = q.enqueue(project="SONIACHAT17TEST", note="topic work")
    claimed = q.claim_next(worker_id, project="SONIACHAT17TEST", session_uuid=sid)
    kwargs = {"session_id": worker_id, "question": "what next?"}
    if kind:
        kwargs["kind"] = kind
    q.block(claimed["ref"], **kwargs)
    workers.record_worker(
        1,  # arbitrary; not checked for liveness in these prompt-shape tests
        "SONIACHAT17TEST", "claude", worker_id,
        repo_path=str(tmp_path), session_id=sid,
    )
    return claimed, worker_id


def test_default_block_kind_prompt_unchanged(wt, tmp_path, monkeypatch):
    """No block_kind (or a non-awaiting-client kind) -- today's exact
    'apply it, finish the ticket, and close it' instruction, verbatim."""
    cli, q, workers = wt
    claimed, worker_id = _make_blocked_ticket(q, workers, tmp_path, kind="rationale")

    captured: dict = {}
    monkeypatch.setattr(cli, "_deliver_to_blocked_session", _capturing_deliver(captured))

    args = argparse.Namespace(ref=claimed["ref"], text="B", worker="", engine=None, tid=False)
    assert cli.cmd_answer(args) == 0
    assert "Apply it, finish the ticket, and close it" in captured["prompt"]
    assert "park again" not in captured["prompt"]
    assert "done with this topic" not in captured["prompt"]


def test_awaiting_client_ordinary_answer_says_park_again(wt, tmp_path, monkeypatch):
    """block_kind=awaiting-client, no --tid: the client replied mid-topic --
    prompt must say to do this turn then park again, not to close."""
    cli, q, workers = wt
    claimed, worker_id = _make_blocked_ticket(q, workers, tmp_path, kind="awaiting-client")

    captured: dict = {}
    monkeypatch.setattr(cli, "_deliver_to_blocked_session", _capturing_deliver(captured))

    args = argparse.Namespace(ref=claimed["ref"], text="make it lighter", worker="", engine=None, tid=False)
    assert cli.cmd_answer(args) == 0
    assert "park again" in captured["prompt"]
    assert "wt block" in captured["prompt"]
    assert "awaiting-client" in captured["prompt"]
    # Must NOT unconditionally say close -- that's the whole bug this fixes.
    assert "finish the ticket, and close it" not in captured["prompt"]


def test_awaiting_client_tid_answer_says_push_and_close(wt, tmp_path, monkeypatch):
    """block_kind=awaiting-client with --tid: the client said the topic is
    done -- prompt must say push local work, close with last SHA (or
    --no-code), append learnings, message only if something pushed."""
    cli, q, workers = wt
    claimed, worker_id = _make_blocked_ticket(q, workers, tmp_path, kind="awaiting-client")

    captured: dict = {}
    monkeypatch.setattr(cli, "_deliver_to_blocked_session", _capturing_deliver(captured))

    args = argparse.Namespace(ref=claimed["ref"], text="that's everything, thanks", worker="", engine=None, tid=True)
    assert cli.cmd_answer(args) == 0
    prompt = captured["prompt"]
    assert "done with this topic" in prompt or "topic is done" in prompt.lower()
    assert "push" in prompt.lower()
    assert "--no-code" in prompt
    assert "learnings" in prompt.lower()
    assert "only if" in prompt.lower()
    # Must not tell it to just park again -- the topic is over.
    assert "park again" not in prompt


def test_tid_flag_without_awaiting_client_kind_falls_back_to_default(wt, tmp_path, monkeypatch):
    """--tid on a ticket that isn't awaiting-client blocked is meaningless --
    the default (unconditional close) prompt still applies. TID is a
    same-topic-routing concept and only makes sense on an awaiting-client
    parked ticket."""
    cli, q, workers = wt
    claimed, worker_id = _make_blocked_ticket(q, workers, tmp_path, kind="input")

    captured: dict = {}
    monkeypatch.setattr(cli, "_deliver_to_blocked_session", _capturing_deliver(captured))

    args = argparse.Namespace(ref=claimed["ref"], text="A", worker="", engine=None, tid=True)
    assert cli.cmd_answer(args) == 0
    assert "Apply it, finish the ticket, and close it" in captured["prompt"]

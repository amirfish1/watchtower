from __future__ import annotations

import argparse
import importlib
import os
import subprocess

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


def _dead_pid() -> int:
    proc = subprocess.Popen(["true"])
    proc.wait()
    return proc.pid


def test_codex_answer_resume_is_headless_and_keeps_claim_owned(
    wt, tmp_path, monkeypatch
):
    cli, q, workers = wt
    worker_id = "throughput-deadbeef"
    sid = "11111111-2222-3333-4444-555555555555"
    item = q.enqueue(project="THROUGHPUT", note="blocked work")
    claimed = q.claim_next(worker_id, project="THROUGHPUT", session_uuid=sid)
    q.block(claimed["ref"], session_id=worker_id, question="A or B?")
    workers.record_worker(
        _dead_pid(), "THROUGHPUT", "codex", worker_id,
        repo_path=str(tmp_path), session_id=sid,
    )
    q.answer(item["ref"], "A", session_id="human")

    calls = []

    class Proc:
        pid = os.getpid()

    def fake_popen(argv, **kwargs):
        calls.append((argv, kwargs))
        return Proc()

    monkeypatch.setattr(cli.subprocess, "Popen", fake_popen, raising=False)
    monkeypatch.setattr(cli.Path, "home", classmethod(lambda cls: tmp_path))

    assert cli._resume_session_headless(
        sid,
        str(tmp_path),
        "apply the answer",
        "codex",
        queue="THROUGHPUT",
        worker_id=worker_id,
    )

    assert calls[0][0][:4] == [
        "codex", "exec", "resume", "--dangerously-bypass-approvals-and-sandbox",
    ]
    live = [w for w in workers.list_workers(prune=False) if w["alive"]]
    assert [(w["worker_id"], w["session_id"]) for w in live] == [(worker_id, sid)]
    assert workers.requeue_orphaned_tickets(grace_s=0) == []
    assert q.get(item["ref"])["status"] == "in_progress"


def _answer_args(ref, text, worker="human", engine="codex"):
    return argparse.Namespace(ref=ref, text=text, worker=worker, engine=engine)


def _blocked_codex_ticket(q, workers, tmp_path, *, worker_id, sid, alive=False):
    item = q.enqueue(project="THROUGHPUT", note="blocked work")
    q.claim_next(worker_id, project="THROUGHPUT", session_uuid=sid)
    q.block(item["ref"], session_id=worker_id, question="A or B?")
    pid = os.getpid() if alive else _dead_pid()
    workers.record_worker(
        pid, "THROUGHPUT", "codex", worker_id,
        repo_path=str(tmp_path), session_id=sid,
    )
    return item


def test_answer_delivers_via_messages_send_not_a_blind_fork(wt, tmp_path, monkeypatch):
    cli, q, workers = wt
    import watchtower.messages as messages
    sid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    item = _blocked_codex_ticket(q, workers, tmp_path, worker_id="w-1", sid=sid)

    calls = {}

    def fake_send(target, text, mode="send", **kw):
        calls["send"] = {
            "target": target,
            "mode": mode,
            "engine": kw.get("engine"),
        }
        return {"ok": True, "transport": "delegate"}

    forked = []
    monkeypatch.setattr(messages, "send", fake_send)
    monkeypatch.setattr(
        cli, "_resume_session_headless",
        lambda *a, **k: forked.append(a) or True,
    )

    assert cli.cmd_answer(_answer_args(item["ref"], "A")) == 0
    # Delivered through the one liveness-aware primitive, steering the session —
    # never a blind `codex exec resume` fork.
    assert calls["send"] == {
        "target": sid,
        "mode": "steer",
        "engine": "codex",
    }
    assert forked == []
    assert q.get(item["ref"])["needs_input"] is False


def test_answer_explicit_engine_overrides_primary_delivery(
    wt, tmp_path, monkeypatch
):
    cli, q, workers = wt
    import watchtower.messages as messages
    sid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    item = _blocked_codex_ticket(q, workers, tmp_path, worker_id="w-1", sid=sid)
    calls = []

    def fake_send(target, text, mode="send", **kwargs):
        calls.append(kwargs.get("engine"))
        return {"ok": True, "transport": "delegate"}

    monkeypatch.setattr(messages, "send", fake_send)

    assert cli.cmd_answer(_answer_args(item["ref"], "A", engine="claude")) == 0
    assert calls == ["claude"]


def test_answer_queued_delivery_does_not_fork(wt, tmp_path, monkeypatch):
    cli, q, workers = wt
    import watchtower.messages as messages
    sid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    item = _blocked_codex_ticket(q, workers, tmp_path, worker_id="w-1", sid=sid)

    forked = []
    monkeypatch.setattr(
        messages, "send",
        lambda *a, **k: {"ok": False, "queued": True, "id": "msg-x", "busy": True},
    )
    monkeypatch.setattr(
        cli, "_resume_session_headless",
        lambda *a, **k: forked.append(a) or True,
    )

    assert cli.cmd_answer(_answer_args(item["ref"], "A")) == 0
    # Busy/held is delivered by the durable outbox, not a parallel fork.
    assert forked == []


def test_answer_falls_back_to_resume_when_target_unresolvable(wt, tmp_path, monkeypatch):
    cli, q, workers = wt
    import watchtower.messages as messages
    sid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    item = _blocked_codex_ticket(q, workers, tmp_path, worker_id="w-1", sid=sid)

    forked = []
    monkeypatch.setattr(
        messages, "send",
        lambda *a, **k: {"ok": False, "error": "unresolvable target"},
    )
    monkeypatch.setattr(
        cli, "_resume_session_headless",
        lambda *a, **k: forked.append((a, k)) or True,
    )

    assert cli.cmd_answer(_answer_args(item["ref"], "A")) == 0
    # Nothing queued -> preserve delivery via the headless resume fallback.
    assert len(forked) == 1


def test_answer_fallback_infers_codex_engine_from_blocked_session(
    wt, tmp_path, monkeypatch
):
    cli, q, workers = wt
    import watchtower.messages as messages
    worker_id = "ccc-deadbeef"
    sid = "11111111-2222-3333-4444-555555555555"
    item = _blocked_codex_ticket(
        q, workers, tmp_path, worker_id=worker_id, sid=sid,
    )
    assert workers.list_workers()[-1]["alive"] is False

    monkeypatch.setattr(
        messages, "send",
        lambda *a, **k: {"ok": False, "error": "unresolvable target"},
    )
    calls = []

    def fake_resume(session_id, repo, prompt, engine, **kwargs):
        calls.append((session_id, engine, kwargs))
        return True

    monkeypatch.setattr(cli, "_resume_session_headless", fake_resume)

    assert cli.cmd_answer(_answer_args(item["ref"], "A", engine=None)) == 0
    assert calls == [
        (
            sid,
            "codex",
            {"queue": "THROUGHPUT", "worker_id": worker_id},
        )
    ]


def test_answer_infers_kimi_engine_after_worker_exit(wt, tmp_path, monkeypatch):
    cli, q, workers = wt
    monkeypatch.setenv("WATCHTOWER_DELEGATE_URL", "off")
    worker_id = "throughput-deadbeef"
    sid = "session_11111111-2222-3333-4444-555555555555"
    item = q.enqueue(project="THROUGHPUT", note="blocked work")
    q.claim_next(worker_id, project="THROUGHPUT", session_uuid=sid)
    q.block(item["ref"], session_id=worker_id, question="A or B?")
    workers.record_worker(
        _dead_pid(), "THROUGHPUT", "kimi", worker_id,
        repo_path=str(tmp_path), session_id=sid,
    )
    assert workers.list_workers()[-1]["alive"] is False

    calls = []
    monkeypatch.setattr(
        cli,
        "_resume_session_headless",
        lambda *args, **kwargs: calls.append((args, kwargs)) or True,
    )

    assert cli.cmd_answer(_answer_args(item["ref"], "A", engine=None)) == 0
    assert calls[0][0][3] == "kimi"
    assert calls[0][1] == {
        "queue": "THROUGHPUT",
        "worker_id": worker_id,
    }


def test_answer_resume_reports_immediate_process_exit(wt, tmp_path, monkeypatch):
    cli, _, _ = wt

    class Proc:
        pid = os.getpid()

        def __init__(self):
            self.polls = 0

        def poll(self):
            self.polls += 1
            return None if self.polls == 1 else 2

    monkeypatch.setattr(cli.subprocess, "Popen", lambda *args, **kwargs: Proc())
    monkeypatch.setattr(cli.Path, "home", classmethod(lambda cls: tmp_path))
    monkeypatch.setenv("WATCHTOWER_RESUME_VERIFY_S", "0.01")

    assert not cli._resume_session_headless(
        "11111111-2222-3333-4444-555555555555",
        str(tmp_path),
        "apply the answer",
        "codex",
    )


def test_answer_resume_nonfinite_verify_window_uses_default(
    wt, tmp_path, monkeypatch
):
    cli, _, _ = wt

    class Proc:
        pid = os.getpid()

        def __init__(self):
            self.polls = 0

        def poll(self):
            self.polls += 1
            return None if self.polls == 1 else 2

    monkeypatch.setattr(cli.subprocess, "Popen", lambda *args, **kwargs: Proc())
    monkeypatch.setattr(cli.Path, "home", classmethod(lambda cls: tmp_path))
    monkeypatch.setattr(cli.time, "sleep", lambda seconds: None)
    monkeypatch.setenv("WATCHTOWER_RESUME_VERIFY_S", "nan")

    assert not cli._resume_session_headless(
        "11111111-2222-3333-4444-555555555555",
        str(tmp_path),
        "apply the answer",
        "codex",
    )


def test_answer_resume_polls_after_deadline_sleep(wt, tmp_path, monkeypatch):
    cli, _, _ = wt
    clock = [0.0]
    poll_results = iter([None, 2])

    class Proc:
        pid = os.getpid()

        def poll(self):
            return next(poll_results)

    monkeypatch.setattr(cli.subprocess, "Popen", lambda *args, **kwargs: Proc())
    monkeypatch.setattr(cli.Path, "home", classmethod(lambda cls: tmp_path))
    monkeypatch.setattr(cli.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(
        cli.time,
        "sleep",
        lambda seconds: clock.__setitem__(0, clock[0] + seconds + 0.1),
    )
    monkeypatch.setenv("WATCHTOWER_RESUME_VERIFY_S", "0.2")

    assert not cli._resume_session_headless(
        "11111111-2222-3333-4444-555555555555",
        str(tmp_path),
        "apply the answer",
        "codex",
    )


def test_answer_fallback_failure_names_engine_for_manual_resume(
    wt, tmp_path, monkeypatch, capsys
):
    cli, q, workers = wt
    import watchtower.messages as messages
    sid = "11111111-2222-3333-4444-555555555555"
    item = _blocked_codex_ticket(
        q, workers, tmp_path, worker_id="ccc-deadbeef", sid=sid,
    )
    assert workers.list_workers()[-1]["alive"] is False
    monkeypatch.setattr(
        messages, "send",
        lambda *a, **k: {"ok": False, "error": "unresolvable target"},
    )
    monkeypatch.setattr(cli, "_resume_session_headless", lambda *a, **k: False)

    assert cli.cmd_answer(_answer_args(item["ref"], "A", engine=None)) == 0

    output = capsys.readouterr().out
    assert "codex resume also failed to stay running" in output
    assert f"wt discuss {item['ref']} --engine codex" in output


def test_answered_ticket_not_reopened_while_answer_in_flight(wt, tmp_path):
    cli, q, workers = wt
    sid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    item = _blocked_codex_ticket(q, workers, tmp_path, worker_id="w-1", sid=sid)
    q.answer(item["ref"], "A", session_id="human")

    # Worker is dead and needs_input is now cleared, but the answer is fresh:
    # the sweep must not hand the ticket to a second worker mid-answer.
    assert workers.requeue_orphaned_tickets(grace_s=0, answer_grace_s=300) == []
    assert q.get(item["ref"])["status"] == "in_progress"

    # Once the answer grace lapses (and the codex session shows no live
    # transcript), the ticket reopens so it is not stranded forever.
    reopened = workers.requeue_orphaned_tickets(grace_s=0, answer_grace_s=0)
    assert [r["ref"] for r in reopened] == [item["ref"]]
    assert q.get(item["ref"])["status"] == "open"

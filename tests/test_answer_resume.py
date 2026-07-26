from __future__ import annotations

import importlib
import os
import subprocess
from types import SimpleNamespace

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


def test_answer_infers_codex_engine_after_worker_process_exits(
    wt, tmp_path, monkeypatch
):
    cli, q, workers = wt
    worker_id = "ccc-deadbeef"
    sid = "11111111-2222-3333-4444-555555555555"
    item = q.enqueue(project="CCC", note="blocked work")
    claimed = q.claim_next(worker_id, project="CCC", session_uuid=sid)
    q.block(claimed["ref"], session_id=worker_id, question="A or B?")
    workers.record_worker(
        _dead_pid(), "CCC", "codex", worker_id,
        repo_path=str(tmp_path), session_id=sid,
    )
    assert workers.list_workers()[-1]["alive"] is False

    calls = []

    def fake_resume(session_id, repo, prompt, engine, **kwargs):
        calls.append((session_id, engine, kwargs))
        return True

    monkeypatch.setattr(cli, "_resume_session_headless", fake_resume)

    rc = cli.cmd_answer(
        SimpleNamespace(
            ref=item["ref"],
            text="A",
            worker="human",
            engine=None,
        )
    )

    assert rc == 0
    assert calls == [
        (
            sid,
            "codex",
            {"queue": "CCC", "worker_id": worker_id},
        )
    ]


def test_answer_resume_reports_immediate_process_exit(wt, tmp_path, monkeypatch):
    cli, _, _ = wt

    class Proc:
        pid = os.getpid()

        def poll(self):
            return 2

    monkeypatch.setattr(cli.subprocess, "Popen", lambda *args, **kwargs: Proc())
    monkeypatch.setattr(cli.Path, "home", classmethod(lambda cls: tmp_path))
    monkeypatch.setenv("WATCHTOWER_RESUME_VERIFY_S", "0.2")

    assert not cli._resume_session_headless(
        "11111111-2222-3333-4444-555555555555",
        str(tmp_path),
        "apply the answer",
        "codex",
    )

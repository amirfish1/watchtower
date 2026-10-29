"""WT-23: a session CCC forwarded an answer-resume to (a lineage successor)
never holds the claim, so it must be told when the ticket is taken away and
must be able to check ownership itself (BECKY-2970)."""
from __future__ import annotations

import argparse
import importlib
import subprocess

import pytest

SID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
SUCCESSOR = "ffffffff-0000-1111-2222-333333333333"


@pytest.fixture()
def wt(tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_WORKERS_FILE", str(tmp_path / "workers.json"))
    monkeypatch.setenv("WATCHTOWER_WORKER_IDS_FILE", str(tmp_path / "worker-ids.json"))
    monkeypatch.setenv(
        "WATCHTOWER_WORKER_SESSIONS_FILE", str(tmp_path / "worker-sessions.json"))
    monkeypatch.setenv(
        "WATCHTOWER_CODEX_THREAD_REGISTRY", str(tmp_path / "codex-threads.json"))
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(tmp_path / "activity.log"))

    import watchtower.cli as cli
    import watchtower.queue as q
    import watchtower.workers as workers

    importlib.reload(q)
    importlib.reload(workers)
    importlib.reload(cli)
    return cli, q, workers


def _answered_orphan(q, workers, tmp_path):
    item = q.enqueue(project="THROUGHPUT", note="blocked work")
    q.claim_next("w-1", project="THROUGHPUT", session_uuid=SID)
    q.block(item["ref"], session_id="w-1", question="A or B?")
    dead = subprocess.Popen(["true"])
    dead.wait()
    workers.record_worker(dead.pid, "THROUGHPUT", "codex", "w-1",
                          repo_path=str(tmp_path), session_id=SID)
    q.answer(item["ref"], "A", session_id="human")
    return item


def test_sweep_reopen_sends_stop_to_the_lineage_successor(wt, tmp_path, monkeypatch):
    cli, q, workers = wt
    import watchtower.messages as messages
    item = _answered_orphan(q, workers, tmp_path)
    monkeypatch.setattr(messages, "ccc_forward_target",
                        lambda s, **k: SUCCESSOR if s == SID else None)
    sent = []
    monkeypatch.setattr(messages, "deliver_message",
                        lambda target, text, **kw: sent.append((target, text, kw))
                        or {"ok": True})

    reopened = workers.requeue_orphaned_tickets(grace_s=0, answer_grace_s=0)

    assert [r["ref"] for r in reopened] == [item["ref"]]
    assert len(sent) == 1
    target, text, kw = sent[0]
    assert target == SUCCESSOR
    assert text.startswith(f"[WATCHTOWER] STOP work on ticket {item['ref']}")
    assert "do NOT edit, commit, push, reset, or close".lower() in text.lower()
    assert kw["verb"] == "steer" and kw["on_busy"] == "hold"
    # Not bound to the ticket: the outbox would cancel it on the claim change.
    assert "ticket_ref" not in kw
    assert kw["dedupe_key"] == f"stop-displaced:{item['ref']}:{SID}:{SUCCESSOR}"


def test_sweep_reopen_without_a_forward_sends_nothing(wt, tmp_path, monkeypatch):
    cli, q, workers = wt
    import watchtower.messages as messages
    item = _answered_orphan(q, workers, tmp_path)
    monkeypatch.setattr(messages, "ccc_forward_target", lambda s, **k: None)
    sent = []
    monkeypatch.setattr(messages, "deliver_message",
                        lambda *a, **kw: sent.append(a) or {"ok": True})

    reopened = workers.requeue_orphaned_tickets(grace_s=0, answer_grace_s=0)

    assert [r["ref"] for r in reopened] == [item["ref"]]
    assert sent == []


def test_answer_prompt_carries_an_ownership_check_by_worker_id(wt):
    from watchtower import answers
    item = {"ref": "WT-9", "block_kind": ""}
    prompt = answers.answer_prompt(item, {"gen": 1, "answer": "A",
                                          "prior_worker_id": "w-1"})
    assert "`wt find WT-9 --json --worker w-1`" in prompt
    assert "claimed_by_you: true" in prompt
    # No worker id: a session-id check would fail a legitimate successor.
    assert answers.ownership_check("WT-9", "") == ""


def test_cmd_answer_prompt_carries_the_ownership_check(wt, tmp_path, monkeypatch):
    cli, q, workers = wt
    from watchtower import liveness
    item = q.enqueue(project="THROUGHPUT", note="blocked work")
    q.claim_next("w-1", project="THROUGHPUT", session_uuid=SID)
    q.block(item["ref"], session_id="w-1", question="A or B?")
    prompts = []

    def fake_deliver(target, text, **kw):
        prompts.append(text)
        return {"ok": True, "transport": "delegate"}

    monkeypatch.setattr(liveness, "deliver", fake_deliver)
    monkeypatch.setattr(cli, "_answer_engine", lambda *a: "claude")
    args = argparse.Namespace(ref=item["ref"], text="A", worker="human", engine="claude")

    assert cli.cmd_answer(args) == 0
    assert prompts and f"`wt find {item['ref']} --json --worker w-1`" in prompts[0]


def test_find_with_the_named_worker_marks_ownership_until_reassigned(wt):
    cli, q, workers = wt
    item = q.enqueue(project="THROUGHPUT", note="work")
    q.claim_next("w-1", project="THROUGHPUT", session_uuid=SID)
    mine = cli._mark_self(q.get(item["ref"]), "w-1", SUCCESSOR)
    assert mine.get("claimed_by_you") is True

    q.release(item["ref"], session_id="w-1")
    q.claim_next("w-2", project="THROUGHPUT", session_uuid="other")
    theirs = cli._mark_self(q.get(item["ref"]), "w-1", SUCCESSOR)
    assert not theirs.get("claimed_by_you")

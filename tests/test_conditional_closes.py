"""Source acceptance tests use only generic, isolated local records."""
import json

import pytest

from watchtower import cli, conditional_answers as ca, conditional_closes as cc, config, queue as q


@pytest.fixture
def source(tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setattr(config, "backend", lambda project: "file")
    item = {"number": 1, "project": "DEMO", "seq": 1, "ref": "DEMO-1", "status": q.PARKED_STATUS,
            "needs_input": True, "blocked_at": "episode-1", "block_question": "Choose a color?",
            "block_kind": "input", "claimed_by": "worker-1", "parked": {"worker": "worker-1"},
            "gates": ["cmd:false"], "history": []}
    with q._FileLock(q._lock_path()):
        q._save_unlocked({"counter": 1, "items": [item]})
    return {"idempotency_key": "close-1", "reason": "No longer relevant",
            "expected_blocked_at": "episode-1", "expected_block_question": "Choose a color?",
            "expected_wait_kind": "owner", "expected_wait": None}


def change(**fields):
    with q._FileLock(q._lock_path()):
        data = q._load_unlocked()
        data["items"][0].update(fields)
        q._save_unlocked(data)


def test_closes_with_no_more_work(source, monkeypatch):
    calls = []
    monkeypatch.setattr(q, "evaluate_gates", lambda *a, **k: calls.append("gates"))
    monkeypatch.setattr(q, "assessment_mark_due", lambda *a, **k: calls.append("assess"))
    from watchtower import messages
    monkeypatch.setattr(messages, "deliver_message", lambda *a, **k: calls.append("deliver"))
    receipt = cc.submit("DEMO-1", source)
    item = q.get("DEMO-1")
    assert receipt["accepted"] and receipt["work_completed"] is False
    assert item["status"] == "closed" and item["needs_input"] is False and not item.get("parked")
    assert item["closed_by"] == "owner" and item["resolution"]["no_code"] is True
    assert "No longer relevant" in item["resolution"]["summary"]
    assert [h for h in item["history"] if h["event"] == "close"][0]["by"]["kind"] == "human"
    assert not any(h["event"] == "answer" for h in item["history"])
    assert calls == []


@pytest.mark.parametrize("fields", [
    {"blocked_at": "episode-2"}, {"block_question": "Choose a shape?"},
    {"block_question": "Choose a color? "}, {"needs_input": False}, {"status": "closed"},
    {"wait": {"on": "external", "block_at": "episode-1"}}, {"block_kind": "awaiting-client"},
])
def test_stale_question_changes_nothing(source, fields):
    change(**fields)
    before = q._db_path().read_bytes()
    with pytest.raises(ca.Conflict):
        cc.submit("DEMO-1", source)
    assert q._db_path().read_bytes() == before
    assert cc.lookup("DEMO-1", "close-1") is None


def test_replay_and_lost_response(source, monkeypatch):
    first = cc.submit("DEMO-1", source)
    notified = []
    monkeypatch.setattr(q, "_notify_ticket_event", lambda *a, **k: notified.append(a))
    assert cc.submit("DEMO-1", source) == first
    assert notified == []
    assert sum(h["event"] == "close" for h in q.get("DEMO-1")["history"]) == 1
    assert cc.lookup("DEMO-1", "close-1") == first
    assert cc.lookup("DEMO-1", "missing") is None


def test_key_reuse_with_other_payload_conflicts(source):
    cc.submit("DEMO-1", source)
    with pytest.raises(ca.Conflict):
        cc.submit("DEMO-1", {**source, "reason": "Different"})


def test_wrong_store_refused_without_write(source):
    before = q.get("DEMO-1")
    with pytest.raises(ca.Conflict):
        cc.submit("DEMO-1", {**source, "expected_store_id": "another-store"})
    assert q.get("DEMO-1") == before


def test_remote_queue_refused(source, monkeypatch):
    monkeypatch.setattr(config, "backend", lambda project: "github")
    with pytest.raises(ca.Unsupported):
        cc.submit("DEMO-1", source)


@pytest.mark.parametrize("bad", [{"reason": " "}, {"idempotency_key": ""}, {"extra": 1}, {"by": ""}])
def test_invalid_requests(source, bad):
    with pytest.raises(ValueError):
        cc.payload({**source, **bad})


def test_dependents_are_not_treated_as_completed(source):
    cc.submit("DEMO-1", source)
    assert q._blocker_state(q.get("DEMO-1")) == "stuck"


def test_closed_ticket_can_be_reopened(source):
    cc.submit("DEMO-1", source)
    q.reopen("DEMO-1", reason="wrong click")
    assert q.get("DEMO-1")["status"] == "open"


def run_cli(capsys, request):
    code = cli.main(["close-conditional", "DEMO-1", "--request-json", json.dumps(request)])
    return code, json.loads(capsys.readouterr().out)


def test_cli_is_json_only(source, capsys):
    code, out = run_cli(capsys, source)
    assert code == 0 and out["accepted"] is True and out["ref"] == "DEMO-1"
    code, out = run_cli(capsys, {**source, "idempotency_key": "close-2"})
    assert code == 2 and out["error"] == "conflict"

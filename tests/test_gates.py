"""Acceptance gates (WT-5): in_review state, cmd gates, review gate."""
from __future__ import annotations

import importlib

import pytest


@pytest.fixture()
def wt(tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(tmp_path / "activity.log"))
    monkeypatch.setenv("WATCHTOWER_DELEGATE_URL", "off")
    monkeypatch.delenv("WATCHTOWER_MACHINE", raising=False)
    import watchtower.queue as q
    importlib.reload(q)

    class Ns:
        pass

    ns = Ns()
    ns.q = q
    monkeypatch.setattr(q, "_notify_review", lambda *a, **k: None)
    monkeypatch.setattr(q, "_notify_ticket_event", lambda *a, **k: None)
    return ns


def _file(q, title, **kw):
    return q.enqueue(project="GT", title=title, note=title, **kw)


def _claimed(q, **kw):
    a = _file(q, "t", **kw)
    q.claim_next("w1", project="GT")
    return a


def test_validate_gates_rejects_unknown_kinds(wt):
    assert wt.q.validate_gates(["cmd: true ", "review", "review:@bob"]) == [
        "cmd:true", "review", "review:@bob"]
    for bad in (["nope"], ["cmd:"], ["review:"]):
        with pytest.raises(ValueError):
            wt.q.validate_gates(bad)


def test_close_without_gates_is_unchanged(wt):
    q = wt.q
    a = _claimed(q)
    assert q.close(a["ref"], session_id="w1", resolution="ok")["status"] == "closed"


def test_cmd_gate_pass_closes_and_records_result(wt):
    q = wt.q
    a = _claimed(q, gates=["cmd:echo hi"])
    item = q.close(a["ref"], session_id="w1", resolution="ok")
    assert item["status"] == "closed"
    res = item["gate_results"]
    assert res[0]["passed"] and "hi" in res[0]["output_tail"]


def test_cmd_gate_failure_reopens_with_reason(wt):
    q = wt.q
    a = _claimed(q, gates=["cmd:echo boom; exit 3"])
    item = q.close(a["ref"], session_id="w1", resolution="ok")
    assert item["status"] == "open"
    assert "exit 3" in item["gate_feedback"] and "boom" in item["gate_feedback"]
    assert item["gate_results"][0]["passed"] is False


def test_review_gate_holds_in_review_until_accept(wt):
    q = wt.q
    a = _file(q, "first", gates=["cmd:true", "review"])
    b = _file(q, "second", blocked_by=[a["ref"]])
    q.claim_next("w1", project="GT")
    item = q.close(a["ref"], session_id="w1", resolution="ok")
    assert item["status"] == "in_review" and item["closed_at"] is None
    assert q.claim_next("w2", project="GT") is None  # dependent still waiting
    q.accept(a["ref"], by="boss")
    assert q.get(a["ref"])["status"] == "closed"
    assert q.claim_next("w2", project="GT")["ref"] == b["ref"]


def test_review_only_fires_after_cmd_gates_pass(wt):
    q = wt.q
    a = _claimed(q, gates=["review", "cmd:exit 1"])
    assert q.close(a["ref"], session_id="w1", resolution="ok")["status"] == "open"


def test_reject_reopens_and_rebinds_session(wt):
    q = wt.q
    a = _claimed(q, gates=["review"])
    q.close(a["ref"], session_id="w1", resolution="ok")
    with pytest.raises(ValueError):
        q.reject(a["ref"], "")
    item = q.reject(a["ref"], "add tests", by="boss")
    assert item["status"] in ("open", "in_progress")
    assert "add tests" in item["gate_feedback"]
    assert "gate_pending" not in item


def test_accept_requires_in_review(wt):
    q = wt.q
    a = _claimed(q)
    with pytest.raises(ValueError):
        q.accept(a["ref"])


def test_queue_default_gates_and_ticket_override(wt, tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_CONFIG_FILE", str(tmp_path / "config.json"))
    import watchtower.config as config
    importlib.reload(config)
    config.set_gates("GT", ["cmd:exit 1"])
    q = wt.q
    a = _claimed(q)
    assert q.close(a["ref"], session_id="w1", resolution="ok")["status"] == "open"
    q.update(a["ref"], gates=["cmd:true"])
    q.claim_next("w1", project="GT")
    assert q.close(a["ref"], session_id="w1", resolution="ok")["status"] == "closed"

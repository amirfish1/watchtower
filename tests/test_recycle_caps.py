"""Claim-time worker recycle limits: claude bytes, codex tokens, ticket cap."""
import json
import os

import pytest


@pytest.fixture
def env(wt_env, monkeypatch):
    tmp_path = wt_env.tmp
    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "codex"))
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude"))
    for k in ("WATCHTOWER_CONTEXT_RECYCLE_BYTES", "WATCHTOWER_CODEX_RECYCLE_INPUT_TOKENS",
              "WATCHTOWER_RECYCLE_TICKETS"):
        monkeypatch.delenv(k, raising=False)
    return tmp_path, wt_env.workers, wt_env.queue


def _register(tmp_path, **rec):
    base = {"pid": os.getpid(), "queue": "RQ"}
    base.update(rec)
    (tmp_path / "workers.json").write_text(json.dumps({"workers": [base]}))


def _rollout(tmp_path, sid, totals):
    d = tmp_path / "codex" / "sessions" / "2026" / "09" / "30"
    d.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps({"type": "session_meta", "payload": {"id": sid}})]
    for t in totals:
        lines.append(json.dumps({"timestamp": "x", "type": "event_msg", "payload": {
            "type": "token_count",
            "info": {"total_token_usage": {"input_tokens": t, "cached_input_tokens": t // 2,
                                           "output_tokens": 5, "total_tokens": t + 5},
                     "last_token_usage": {"input_tokens": 10}},
            "rate_limits": {}}}))
        lines.append(json.dumps({"type": "response_item", "payload": {"type": "message"}}))
    (d / f"rollout-2026-09-30T01-02-03-{sid}.jsonl").write_text("\n".join(lines) + "\n")


SID = "019a0000-1111-2222-3333-444455556666"


def test_codex_tokens_trip_and_no_trip(env, monkeypatch):
    tmp_path, workers, _ = env
    _register(tmp_path, worker_id="cx-1", engine="codex", session_id=SID)
    _rollout(tmp_path, SID, [1_000_000, 31_000_000])
    assert workers.context_budget_exceeded("cx-1") == ("codex_input_tokens", 31_000_000)
    monkeypatch.setenv("WATCHTOWER_CODEX_RECYCLE_INPUT_TOKENS", "40000000")
    assert not workers.context_budget_exceeded("cx-1")
    monkeypatch.setenv("WATCHTOWER_CODEX_RECYCLE_INPUT_TOKENS", "0")
    assert not workers.context_budget_exceeded("cx-1")


def test_codex_reads_tail_of_large_rollout(env):
    tmp_path, workers, _ = env
    _register(tmp_path, worker_id="cx-1", engine="codex", session_id=SID)
    _rollout(tmp_path, SID, [5_000_000])
    p = next((tmp_path / "codex").rglob("rollout-*.jsonl"))
    pad = json.dumps({"type": "response_item", "payload": {"text": "y" * 2000}})
    p.write_text(p.read_text() + "\n".join([pad] * 400) + "\n")  # >256KB after the count
    assert workers._codex_input_tokens(SID) == 5_000_000


def test_ticket_cap_codex_and_claude(env, monkeypatch):
    tmp_path, workers, _ = env
    monkeypatch.setenv("WATCHTOWER_RECYCLE_TICKETS", "3")
    for eng in ("codex", "claude"):
        _register(tmp_path, worker_id="w-1", engine=eng, session_id=SID, tickets_done=2)
        assert not workers.context_budget_exceeded("w-1")
        workers.note_ticket_done("w-1")
        assert workers.context_budget_exceeded("w-1") == ("tickets", 3)
    monkeypatch.setenv("WATCHTOWER_RECYCLE_TICKETS", "0")
    assert not workers.context_budget_exceeded("w-1")


def test_unregistered_never_recycled(env, monkeypatch):
    tmp_path, workers, _ = env
    monkeypatch.setenv("WATCHTOWER_RECYCLE_TICKETS", "1")
    _register(tmp_path, worker_id="w-1", engine="codex", session_id=SID, tickets_done=5)
    assert not workers.context_budget_exceeded("human-cli")
    workers.note_ticket_done("human-cli")  # no-op


def test_claim_stops_at_cap_and_defers_while_holding_claim(env, monkeypatch):
    tmp_path, workers, q = env
    monkeypatch.setenv("WATCHTOWER_RECYCLE_TICKETS", "1")
    _register(tmp_path, worker_id="w-1", engine="codex", session_id=SID)
    q.enqueue(project="RQ", note="one")
    q.enqueue(project="RQ", note="two")
    first = q.claim_next("w-1", project="RQ")
    assert first["ref"] == "RQ-1"
    # still holding RQ-1: recycle is deferred (claim refused, not stop)
    workers.note_ticket_done("w-1")
    with pytest.raises(ValueError):
        q.claim_next("w-1", project="RQ")
    q.close("RQ-1", "w-1")  # counts a second ticket
    assert q.claim_next("w-1", project="RQ") == {"stop": True, "reason": "context_budget"}
    assert q.get("RQ-2")["status"] == "open"

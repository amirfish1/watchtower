"""Acceptance gates (WT-5): in_review state, cmd gates, review gate."""
from __future__ import annotations

import importlib
import json

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
    import watchtower.config as config
    monkeypatch.setattr(config, "CONFIG_FILE", tmp_path / "config.json")
    config.set_gates("GT", ["cmd:exit 1"])
    q = wt.q
    a = _claimed(q)
    assert q.close(a["ref"], session_id="w1", resolution="ok")["status"] == "open"
    q.update(a["ref"], gates=["cmd:true"])
    q.claim_next("w1", project="GT")
    assert q.close(a["ref"], session_id="w1", resolution="ok")["status"] == "closed"


def test_accept_field_and_checks_block(wt):
    q = wt.q
    a = _file(q, "t", gates=["cmd:pytest -q", "verify", "review"],
              accept_line="Rows show the short ticket ID at 390px.")
    assert a["accept"].startswith("Rows show")
    block = q.checks_block(a)
    assert "WatchTower runs: pytest -q" in block
    assert "An independent verifier" in block and "checks: Rows show the short ticket ID" in block
    assert "Don't launch your own independent verifier" in block
    assert q.checks_block(_file(q, "plain")) == ""
    b = _file(q, "no accept", gates=["verify"])
    assert "checks: the ticket text" in q.checks_block(b)


def test_verify_gate_pass_then_review_then_accept(wt):
    q = wt.q
    a = _claimed(q, gates=["verify", "review"])
    item = q.close(a["ref"], session_id="w1", resolution="ok")
    assert item["status"] == "in_review" and item["gate_pending"] == "verify"
    with pytest.raises(ValueError):
        q.accept(a["ref"])  # verifier verdict still owed
    item = q.verdict(a["ref"], True, "looks right")
    assert item["status"] == "in_review" and item["gate_pending"] == "review"
    assert q.accept(a["ref"])["status"] == "closed"


def test_verify_gate_pass_alone_closes(wt):
    q = wt.q
    a = _claimed(q, gates=["verify"])
    q.close(a["ref"], session_id="w1", resolution="ok")
    item = q.verdict(a["ref"], True, "ok")
    assert item["status"] == "closed"
    assert item["gate_results"][-1]["gate"] == "verify"


def test_verify_gate_fail_reopens_with_findings_and_rebinds(wt):
    q = wt.q
    a = _claimed(q, gates=["verify"])
    q.close(a["ref"], session_id="w1", resolution="ok")
    item = q.verdict(a["ref"], False, "ID not shown")
    assert item["status"] in ("open", "in_progress")
    assert "ID not shown" in item["gate_feedback"]
    assert item["gate_results"][-1]["passed"] is False


def test_verdict_requires_pending_verify(wt):
    q = wt.q
    a = _claimed(q, gates=["review"])
    q.close(a["ref"], session_id="w1", resolution="ok")
    with pytest.raises(ValueError):
        q.verdict(a["ref"], True)


def test_import_accepts_optional_accept_line(tmp_path):
    from watchtower import document_import as di
    src = tmp_path / "plan.md"
    src.write_text("# Plan\nMake rows short.\n")
    payload = {"tickets": [{"title": "Shorten rows", "body": "do it", "type": "feature",
                            "depends_on": [], "source_anchor": "L2",
                            "accept": "Rows show short ID."}]}
    got = di.extract_document(src, reasoner=lambda p, s: payload)
    assert got[0].accept == "Rows show short ID."
    payload["tickets"][0].pop("accept")
    assert di.extract_document(src, reasoner=lambda p, s: payload)[0].accept == ""


@pytest.fixture()
def vcfg(wt, tmp_path, monkeypatch):
    import watchtower.config as config
    import watchtower.workers as workers
    monkeypatch.setattr(config, "CONFIG_FILE", tmp_path / "config.json")
    monkeypatch.setattr(config, "CCC_MODEL_POLICY_FILE", tmp_path / "policy.json")
    monkeypatch.setattr(config, "CCC_SPAWN_DEFAULTS_FILE", tmp_path / "spawn.json")
    monkeypatch.setattr(workers, "WORKERS_FILE", tmp_path / "workers.json")
    monkeypatch.setattr(workers, "WORKER_IDS_FILE", tmp_path / "worker-ids.json")
    monkeypatch.setattr(workers, "WORKER_SESSIONS_FILE", tmp_path / "worker-sessions.json")
    config.set_engine("GT", "codex")
    config.set_model("GT", "gpt-5.5")
    wt.config, wt.workers = config, workers
    return wt


def _worker_rec(v, engine, model):
    (v.workers.WORKERS_FILE).write_text(json.dumps({"workers": [
        {"worker_id": "w1", "queue": "GT", "engine": engine, "model": model}]}))


def _verify_item(q):
    a = _claimed(q, gates=["verify"])
    return q.get(a["ref"])


def test_verifier_inherits_queue_engine_model(vcfg):
    t = vcfg.q.verifier_target(_verify_item(vcfg.q))
    assert (t["engine"], t["model"], t["source"]) == ("codex", "gpt-5.5", "queue")


def test_claimed_worker_model_beats_queue_config(vcfg):
    q = vcfg.q
    item = _verify_item(q)
    _worker_rec(vcfg, "claude", "claude-sonnet-5")
    t = q.verifier_target(item)
    assert (t["engine"], t["model"], t["source"]) == ("claude", "claude-sonnet-5", "worker")


def test_verifier_override_beats_both(vcfg):
    q = vcfg.q
    item = _verify_item(q)
    _worker_rec(vcfg, "claude", "claude-sonnet-5")
    vcfg.config.set_verifier("GT", "codex", "gpt-5.5")
    t = q.verifier_target(item)
    assert (t["engine"], t["model"], t["source"]) == ("codex", "gpt-5.5", "override")
    assert "codex/gpt-5.5" in q.checks_block(item)


def test_blocked_verifier_model_is_flagged_not_substituted(vcfg, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_BLOCKED_MODELS", "gpt-5.5")
    t = vcfg.q.verifier_target(_verify_item(vcfg.q))
    assert t["model"] == "gpt-5.5" and t["blocked"] is True


def test_spawn_verifier_passes_engine_model_and_refuses_blocked(vcfg, monkeypatch, capsys):
    import watchtower.cli as cli
    importlib.reload(cli)
    q = vcfg.q
    calls = []
    monkeypatch.setattr(cli.workers, "spawn_adhoc",
                        lambda goal, eng, **kw: calls.append((eng, kw)) or {"worker_id": "v1"})
    cli.q = q
    item = _verify_item(q)
    cli._spawn_verifier(item)
    assert calls[0][0] == "codex" and calls[0][1]["model"] == "gpt-5.5"
    assert q.get(item["ref"])["verifier"]["engine"] == "codex"
    monkeypatch.setenv("WATCHTOWER_BLOCKED_MODELS", "gpt-5.5")
    calls.clear()
    cli._spawn_verifier(item)
    assert calls == [] and "blocked" in capsys.readouterr().err

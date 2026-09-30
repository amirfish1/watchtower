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
    monkeypatch.setattr(workers, "engine_available", lambda e: e in ("codex", "claude"))
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


def test_verifier_defaults_to_a_different_family_than_the_builder(vcfg):
    t = vcfg.q.verifier_target(_verify_item(vcfg.q))
    # builder is codex; strongest ranked (priced) claude model in the fixture catalog
    assert (t["engine"], t["model"], t["source"]) == ("claude", "claude-opus-5-5", "default")


def test_claimed_worker_is_the_builder_for_the_default(vcfg):
    q = vcfg.q
    item = _verify_item(q)
    _worker_rec(vcfg, "claude", "claude-sonnet-5")
    t = q.verifier_target(item)
    assert t["engine"] == "codex" and t["source"] == "default"


def test_queue_verifier_override_beats_default(vcfg):
    q = vcfg.q
    item = _verify_item(q)
    _worker_rec(vcfg, "claude", "claude-sonnet-5")
    vcfg.config.set_verifier("GT", "codex", "gpt-5.5")
    t = q.verifier_target(item)
    assert (t["engine"], t["model"], t["source"]) == ("codex", "gpt-5.5", "queue")
    assert "codex/gpt-5.5" in q.checks_block(item)


def test_ticket_verifier_model_beats_queue_override(vcfg):
    q = vcfg.q
    vcfg.config.set_verifier("GT", "codex", "gpt-5.5")
    item = dict(_verify_item(q), verifier_model="claude-sonnet-5-5")
    t = q.verifier_target(item)
    assert (t["engine"], t["model"], t["source"]) == ("claude", "claude-sonnet-5-5", "ticket")


def test_blocked_verifier_model_is_flagged_not_substituted(vcfg, monkeypatch):
    vcfg.config.set_verifier("GT", "codex", "gpt-5.5")
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
    vcfg.config.set_verifier("GT", "codex", "gpt-5.5")
    item = _verify_item(q)
    cli._spawn_verifier(item)
    assert calls[0][0] == "codex" and calls[0][1]["model"] == "gpt-5.5"
    assert q.get(item["ref"])["verifier"]["engine"] == "codex"
    monkeypatch.setenv("WATCHTOWER_BLOCKED_MODELS", "gpt-5.5")
    calls.clear()
    cli._spawn_verifier(item)
    assert calls == [] and "blocked" in capsys.readouterr().err


# --- Plan stage (WT-11) ------------------------------------------------------

@pytest.fixture()
def plan_cli(vcfg, monkeypatch):
    import watchtower.cli as cli
    importlib.reload(cli)
    cli.q = vcfg.q
    calls = []

    def fake_spawn(goal, eng, **kw):
        calls.append({"goal": goal, "engine": eng, **kw})
        return {"worker_id": f"sp{len(calls)}"}

    monkeypatch.setattr(cli.workers, "spawn_adhoc", fake_spawn)
    vcfg.cli, vcfg.calls = cli, calls
    return vcfg


def _ns(**kw):
    import argparse
    base = dict(json=False, by="t", text="", file="", reasons="",
                accept=False, reject=False, timeout=1)
    return argparse.Namespace(**{**base, **kw})


def test_plan_gate_validates_and_shows_in_checks(vcfg):
    q = vcfg.q
    assert q.validate_gates(["plan", "plan:claude-opus-5-5"]) == ["plan", "plan:claude-opus-5-5"]
    with pytest.raises(ValueError):
        q.validate_gates(["plan:"])
    item = _claimed(q, gates=["plan:claude-opus-5-5"])
    assert q.plan_gate(q.get(item["ref"])) == "claude-opus-5-5"
    assert "Plan first" in q.checks_block(q.get(item["ref"]))
    assert q.evaluate_gates(q.get(item["ref"]))[1] == []  # plan is not a close-time stage


def test_plan_flow_accept_then_builder_gets_plan(plan_cli):
    q, cli, calls = plan_cli.q, plan_cli.cli, plan_cli.calls
    a = _claimed(q, gates=["plan:claude-opus-5-5"])
    item = cli._start_plan_stage(q.get(a["ref"]))
    assert item["plan"]["status"] == "planning"
    assert calls[0]["engine"] == "claude" and calls[0]["model"] == "claude-opus-5-5"
    assert "PLAN PENDING" in cli._plan_note(item)
    cli._start_plan_stage(q.get(a["ref"]))          # re-claim: no second planner
    assert len(calls) == 1
    assert cli.cmd_plan(_ns(plan_cmd="submit", ref=a["ref"], text="do X then Y")) == 0
    assert q.get(a["ref"])["plan"]["status"] == "reviewing"
    assert calls[1]["engine"] == "claude"           # builder is codex, so a different family reviews
    assert cli.cmd_plan(_ns(plan_cmd="verdict", ref=a["ref"], accept=True, reasons="sound")) == 0
    done = q.get(a["ref"])
    assert done["plan"]["status"] == "accepted"
    assert "do X then Y" in cli._plan_note(done)
    assert "do X then Y" in cli._verifier_goal(done)
    assert [h["event"] for h in done["history"] if h["event"].startswith("plan")] == [
        "plan_start", "plan", "plan_review"]


def test_plan_reject_revises_then_blocks(plan_cli):
    q, cli, calls = plan_cli.q, plan_cli.cli, plan_cli.calls
    a = _claimed(q, gates=["plan"])
    cli._start_plan_stage(q.get(a["ref"]))
    for n in range(q.PLAN_MAX_REVISIONS + 1):
        cli.cmd_plan(_ns(plan_cmd="submit", ref=a["ref"], text=f"plan v{n}"))
        cli.cmd_plan(_ns(plan_cmd="verdict", ref=a["ref"], reject=True, reasons=f"bad {n}"))
    final = q.get(a["ref"])
    assert final["plan"]["status"] == "blocked"
    assert final["needs_input"] is True
    # planner respawned with feedback after each non-final rejection
    assert any("REJECTED it: bad 0" in c["goal"] for c in calls)


def test_plan_stage_fails_open_when_model_blocked(plan_cli, monkeypatch):
    q, cli = plan_cli.q, plan_cli.cli
    monkeypatch.setenv("WATCHTOWER_BLOCKED_MODELS", "claude-opus-5-5")
    a = _claimed(q, gates=["plan:claude-opus-5-5"])
    item = cli._start_plan_stage(q.get(a["ref"]))
    assert item["plan"]["status"] == "failed"
    assert "plan the work yourself" in cli._plan_note(item)


def test_plan_submit_out_of_turn_is_refused(plan_cli):
    q = plan_cli.q
    a = _claimed(q, gates=["plan"])
    with pytest.raises(ValueError):
        q.plan_submit(a["ref"], "x")

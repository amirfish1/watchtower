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


def _to_review(q, ref, fresh=False):
    """Put a claimed verify-gated ticket in in_review/verify (what `wt close` does)."""
    with q._FileLock(q._lock_path()):
        data = q._load_unlocked()
        for it in data["items"]:
            if it["ref"] == ref:
                it.update(status="in_review", gate_pending="verify", needs_input=False)
                if fresh:
                    it.pop("stage_session", None)
                    it.pop("verifier", None)
        q._save_unlocked(data)


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


def test_spawn_verifier_passes_engine_model_and_refuses_blocked(vcfg, monkeypatch, capsys,
                                                                 instant_daemon):
    import watchtower.cli as cli
    importlib.reload(cli)
    q = vcfg.q
    calls = []
    monkeypatch.setattr(cli.workers, "spawn_adhoc",
                        lambda goal, eng, **kw: calls.append((eng, kw)) or {"worker_id": "v1"})
    cli.q = q
    vcfg.config.set_verifier("GT", "codex", "gpt-5.5")
    item = _verify_item(q)
    _to_review(q, item["ref"])
    cli._spawn_verifier(item)
    assert calls[0][0] == "codex" and calls[0][1]["model"] == "gpt-5.5"
    assert calls[0][1]["verify"] is True and calls[0][1]["stage"] == "verifier"
    v = q.get(item["ref"])["verifier"]
    assert v["engine"] == "codex" and v["stage_key"] == "verify:0"
    # a blocked model is never substituted: no spawn, the ticket needs a human
    monkeypatch.setenv("WATCHTOWER_BLOCKED_MODELS", "gpt-5.5")
    calls.clear()
    _to_review(q, item["ref"], fresh=True)
    cli._spawn_verifier(q.get(item["ref"]))
    assert calls == []
    blocked = q.get(item["ref"])
    assert blocked["needs_input"] is True and "blocked" in blocked["block_question"]


# --- Plan stage (WT-11) ------------------------------------------------------

@pytest.fixture()
def plan_cli(vcfg, monkeypatch, instant_daemon):
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


def test_plan_reject_revises_then_blocks(plan_cli, monkeypatch):
    # roles without worker ids cannot be messaged: legacy respawn-with-feedback path
    q, cli, calls = plan_cli.q, plan_cli.cli, plan_cli.calls
    monkeypatch.setattr(cli.workers, "spawn_adhoc",
                        lambda goal, eng, **kw: calls.append({"goal": goal, "engine": eng, **kw}) or {})
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


def test_plan_gated_ticket_unclaimable_until_plan_accepted(plan_cli):
    """WT-22: planning happens before a build worker claims."""
    q, cli, calls = plan_cli.q, plan_cli.cli, plan_cli.calls
    a = _file(q, "t", gates=["plan"])
    assert q.claim_next("w1", project="GT") is None     # plan not started
    assert cli.start_pending_plans("GT") == 1           # planner spawned, unclaimed
    assert q.get(a["ref"])["status"] == "open" and len(calls) == 1
    assert cli.start_pending_plans("GT") == 0           # idempotent
    assert q.claim_next("w1", project="GT") is None     # planning
    cli.cmd_plan(_ns(plan_cmd="submit", ref=a["ref"], text="plan"))
    assert q.claim_next("w1", project="GT") is None     # reviewing
    cli.cmd_plan(_ns(plan_cmd="verdict", ref=a["ref"], accept=True, reasons="ok"))
    got = q.claim_next("w1", project="GT")
    assert got and got["ref"] == a["ref"]
    assert "ACCEPTED PLAN" in cli._plan_note(q.get(a["ref"]))


def _set_session(q, ref, sid):
    with q._FileLock(q._lock_path()):
        data = q._load_unlocked()
        for it in data["items"]:
            if it["ref"] == ref:
                it["claimed_session_id"] = sid
        q._save_unlocked(data)


def _block_plan(plan_cli, session=False):
    q, cli = plan_cli.q, plan_cli.cli
    a = _claimed(q, gates=["plan"])
    if session:
        _set_session(q, a["ref"], "sess-1")
    cli._start_plan_stage(q.get(a["ref"]))
    for n in range(20):
        if q.get(a["ref"])["plan"]["status"] == "blocked":
            break
        cli.cmd_plan(_ns(plan_cmd="submit", ref=a["ref"], text=f"plan v{n}"))
        cli.cmd_plan(_ns(plan_cmd="verdict", ref=a["ref"], reject=True, reasons=f"bad {n}"))
    return a["ref"]


def test_plan_decide_accept_clears_block_and_audits(plan_cli):
    q, cli = plan_cli.q, plan_cli.cli
    ref = _block_plan(plan_cli)
    # a generic answer must not silently accept the plan
    q.answer(ref, "looks fine, go")
    assert q.get(ref)["plan"]["status"] == "blocked"
    assert q.plan_pending(q.get(ref))
    assert cli.cmd_plan(_ns(plan_cmd="decide", ref=ref, accept=True, text="amended plan",
                            retries=1)) == 0
    it = q.get(ref)
    assert it["plan"]["status"] == "accepted" and it["plan"]["text"] == "amended plan"
    assert it["needs_input"] is False and not q.plan_pending(it)
    assert it["plan"]["decisions"][0]["decision"] == "accept"
    assert it["status"] == "open"  # no retained session: reopened, now claimable
    assert "plan_decision" in [h["event"] for h in it["history"]]


def test_plan_decide_retry_grants_budget(plan_cli):
    q, cli = plan_cli.q, plan_cli.cli
    ref = _block_plan(plan_cli, session=True)
    assert cli.cmd_plan(_ns(plan_cmd="decide", ref=ref, retry=True, accept=False,
                            retries=1)) == 0
    it = q.get(ref)
    assert it["plan"]["status"] == "planning"
    assert it["needs_input"] is False and it["status"] == "in_progress"  # session kept
    cli.cmd_plan(_ns(plan_cmd="submit", ref=ref, text="v4"))
    cli.cmd_plan(_ns(plan_cmd="verdict", ref=ref, reject=True, reasons="still bad"))
    assert q.get(ref)["plan"]["status"] == "discussing"
    cli.cmd_plan(_ns(plan_cmd="submit", ref=ref, text="v5"))
    cli.cmd_plan(_ns(plan_cmd="verdict", ref=ref, reject=True, reasons="still bad"))
    assert q.get(ref)["plan"]["status"] == "blocked"  # the granted budget is spent


def test_plan_decide_requires_blocked_plan(plan_cli):
    q = plan_cli.q
    a = _claimed(q, gates=["plan"])
    with pytest.raises(ValueError):
        q.plan_decide(a["ref"], "accept")


def test_answer_on_blocked_plan_warns(plan_cli, capsys):
    import argparse
    q, cli = plan_cli.q, plan_cli.cli
    ref = _block_plan(plan_cli)
    cli.cmd_answer(argparse.Namespace(ref=ref, text="ok", worker="", engine="", tid=False))
    assert "plan gate is STILL BLOCKED" in capsys.readouterr().out


# --- WT-26: planner <-> reviewer discussion after the first rejection --------

def _planned(plan_cli, monkeypatch=None):
    q, cli = plan_cli.q, plan_cli.cli
    a = _claimed(q, gates=["plan"])
    cli._start_plan_stage(q.get(a["ref"]))
    cli.cmd_plan(_ns(plan_cmd="submit", ref=a["ref"], text="plan v1"))
    return a["ref"]


def _sent(cli, monkeypatch):
    sent = []
    from watchtower import messages
    monkeypatch.setattr(messages, "send",
                        lambda target, text, **kw: sent.append((target, text)) or {"ok": True})
    return sent


def test_first_rejection_starts_one_discussion_and_reviewer_accepts_version(plan_cli, monkeypatch):
    """WT-29: each turn is a supervised spawn seeded from the ticket; no messages."""
    q, cli, calls = plan_cli.q, plan_cli.cli, plan_cli.calls
    sent = _sent(cli, monkeypatch)
    ref = _planned(plan_cli)
    n_spawns = len(calls)
    cli.cmd_plan(_ns(plan_cmd="verdict", ref=ref, reject=True, reasons="missing tests"))
    it = q.get(ref)
    assert it["plan"]["status"] == "discussing" and it["needs_input"] is False
    assert it["plan"]["discussion"]["round"] == 1
    assert len(calls) == n_spawns + 1                  # one fresh planner turn
    assert "plan v1" in calls[-1]["goal"] and "missing tests" in calls[-1]["goal"]
    assert it["stage_session"]["key"] == "discuss:r1:d1:planner"
    assert sent == []                                  # nothing sent to a dead peer
    assert q.plan_pending(it)
    cli.cmd_plan(_ns(plan_cmd="discuss", ref=ref, sender="reviewer", text="add a restart test"))
    assert sent == []                                  # peer not running: recorded only
    cli.cmd_plan(_ns(plan_cmd="submit", ref=ref, text="plan v2 with tests"))
    assert len(calls) == n_spawns + 2                  # one fresh reviewer turn
    assert "plan v2 with tests" in calls[-1]["goal"] and "--version 2" in calls[-1]["goal"]
    assert "add a restart test" in calls[-1]["goal"]
    assert sent == []
    with pytest.raises(ValueError):
        q.plan_verdict(ref, True, version_seen=1)      # stale version refused
    assert q.get(ref)["plan"]["status"] == "reviewing"
    assert cli.cmd_plan(_ns(plan_cmd="verdict", ref=ref, accept=True, version=2,
                            reasons="ok")) == 0
    done = q.get(ref)
    assert done["plan"]["status"] == "accepted" and done["plan"]["accepted_version"] == 2
    assert done["plan"]["discussion"]["status"] == "agreed"
    assert "plan v2 with tests" in cli._plan_note(done)
    assert "plan_discussion" in [h["event"] for h in done["history"]]


def test_discussion_bounded_then_explained_human_block(plan_cli, monkeypatch):
    q, cli = plan_cli.q, plan_cli.cli
    _sent(cli, monkeypatch)
    ref = _planned(plan_cli)
    for n in range(q.PLAN_DISCUSSION_ROUNDS + 1):
        cli.cmd_plan(_ns(plan_cmd="verdict", ref=ref, reject=True, reasons=f"no {n}"))
        if q.get(ref)["plan"]["status"] == "discussing":
            cli.cmd_plan(_ns(plan_cmd="submit", ref=ref, text=f"v{n + 2}"))
    it = q.get(ref)
    assert it["plan"]["status"] == "blocked" and it["needs_input"] is True
    assert "discussion rounds" in it["block_question"]
    assert it["plan"]["discussion"]["status"] == "escalated"


def _live_only_spies(monkeypatch, fail_test):
    """UDS/FIFO fail; every adapter that could start an unsupervised turn fails
    the test if reached."""
    from watchtower import messages
    monkeypatch.setattr(messages, "resolve_target",
                        lambda t: {"session_id": "s-" + str(t), "engine": "claude"})
    monkeypatch.setattr(messages, "_deliver_uds", lambda r, t: {"ok": False, "error": "no uds"})
    monkeypatch.setattr(messages, "_deliver_fifo", lambda r, t: {"ok": False, "error": "no fifo"})
    for name in ("_deliver_resume", "_deliver_gemini_resume", "_deliver_codex_app_server",
                 "_deliver_antigravity_language_server", "_deliver_delegate"):
        monkeypatch.setattr(messages, name, lambda *a, _n=name, **k: fail_test(_n))


def test_plan_send_is_live_only(plan_cli, monkeypatch):
    cli = plan_cli.cli
    from watchtower import messages
    seen = []
    monkeypatch.setattr(messages, "send", lambda t, x, **kw: seen.append(kw) or
                        {"ok": False, "queued": True})
    item = {"plan": {"planner": {"worker_id": "w1"}}}
    assert cli._plan_send(item, "planner", "hi") is False
    assert seen == [{"live_only": True}]
    assert cli._plan_send(item, "reviewer", "hi") is False      # no worker id


def test_discuss_peer_exits_after_precheck_is_recorded_not_delivered(plan_cli, monkeypatch, capsys):
    q, cli = plan_cli.q, plan_cli.cli
    ref = _planned(plan_cli)
    cli.cmd_plan(_ns(plan_cmd="verdict", ref=ref, reject=True, reasons="bad"))
    from watchtower import messages
    monkeypatch.setattr(cli, "_plan_role_alive", lambda item, role: True)   # died after the check
    _live_only_spies(monkeypatch, lambda n: pytest.fail(f"{n} must not run"))
    capsys.readouterr()
    assert cli.cmd_plan(_ns(plan_cmd="discuss", ref=ref, sender="reviewer", text="hello")) == 0
    assert "recorded" in capsys.readouterr().out
    assert [m["text"] for m in q.get(ref)["plan"]["discussion"]["messages"]] == ["hello"]
    assert messages.outbox_list() == []


def test_discuss_to_dead_peer_is_recorded_not_sent(plan_cli, monkeypatch):
    q, cli = plan_cli.q, plan_cli.cli
    ref = _planned(plan_cli)
    cli.cmd_plan(_ns(plan_cmd="verdict", ref=ref, reject=True, reasons="bad"))
    monkeypatch.setattr(cli, "_plan_role_alive", lambda item, role: False)
    monkeypatch.setattr(cli, "_plan_send", lambda *a: pytest.fail("must not send"))
    assert cli.cmd_plan(_ns(plan_cmd="discuss", ref=ref, sender="reviewer", text="yo")) == 0


def _github_plan(plan_cli, monkeypatch):
    """A discussing ticket the stage supervisor does not own."""
    from watchtower import stages
    ref = _planned(plan_cli)
    plan_cli.cli.cmd_plan(_ns(plan_cmd="verdict", ref=ref, reject=True, reasons="bad"))
    monkeypatch.setattr(stages, "_github_backed", lambda it: True)
    return ref


def test_supervised_discussion_is_skipped_by_fallback_nudge(plan_cli, monkeypatch):
    q, cli = plan_cli.q, plan_cli.cli
    ref = _planned(plan_cli)
    cli.cmd_plan(_ns(plan_cmd="verdict", ref=ref, reject=True, reasons="bad"))
    monkeypatch.setattr(cli, "_plan_send", lambda *a: pytest.fail("must not send"))
    assert cli.recover_plan_discussions("GT", stale_s=0) == 0
    assert q.get(ref)["plan"]["discussion"]["nudges"] == 0


def test_fallback_nudge_to_dead_peer_blocks_immediately(plan_cli, monkeypatch):
    q, cli = plan_cli.q, plan_cli.cli
    ref = _github_plan(plan_cli, monkeypatch)
    monkeypatch.setattr(cli, "_plan_role_alive", lambda item, role: False)
    assert cli.recover_plan_discussions("GT", stale_s=0) == 0
    it = q.get(ref)
    assert it["plan"]["status"] == "blocked" and it["plan"]["discussion"]["nudges"] == 0
    assert "not running" in it["block_question"] and "wt plan decide" in it["block_question"]
    assert it["needs_input"] is True


def test_fallback_nudge_undelivered_blocks_without_resume_or_outbox(plan_cli, monkeypatch):
    q, cli = plan_cli.q, plan_cli.cli
    from watchtower import messages
    ref = _github_plan(plan_cli, monkeypatch)
    monkeypatch.setattr(cli, "_plan_role_alive", lambda item, role: True)
    _live_only_spies(monkeypatch, lambda n: pytest.fail(f"{n} must not run"))
    assert cli.recover_plan_discussions("GT", stale_s=0) == 0
    it = q.get(ref)
    assert it["plan"]["status"] == "blocked" and it["plan"]["discussion"]["nudges"] == 0
    assert "undelivered" in it["block_question"]
    assert messages.outbox_list() == []


def test_fallback_nudge_delivered_counts_then_escalates(plan_cli, monkeypatch):
    q, cli = plan_cli.q, plan_cli.cli
    from watchtower import messages
    ref = _github_plan(plan_cli, monkeypatch)
    sent = []
    monkeypatch.setattr(cli, "_plan_role_alive", lambda item, role: True)
    monkeypatch.setattr(messages, "send",
                        lambda t, x, **kw: sent.append(kw) or {"ok": True, "transport": "uds"})
    assert cli.recover_plan_discussions("GT", stale_s=3600) == 0   # fresh: nothing due
    for n in range(q.PLAN_DISCUSSION_MAX_NUDGES):
        assert cli.recover_plan_discussions("GT", stale_s=0) == 1
        assert q.get(ref)["plan"]["discussion"]["nudges"] == n + 1
    assert all(kw == {"live_only": True} for kw in sent)
    cli.recover_plan_discussions("GT", stale_s=0)
    it = q.get(ref)
    assert it["plan"]["status"] == "blocked"
    assert "unresponsive after" in it["block_question"] and "delivered reminders" in it["block_question"]

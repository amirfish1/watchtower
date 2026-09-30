"""Post-fix assessment (WT-21): close hook, reservation, filing, dedup, config guard."""
from __future__ import annotations

import importlib
import json
import subprocess
import threading

import pytest

ALL_ADEQUATE = {p: {"verdict": "adequate", "note": ""} for p in
                ("logging", "ui_message", "automation", "monitoring", "auditors", "other")}


@pytest.fixture()
def wt(tmp_path, monkeypatch, instant_daemon):
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(tmp_path / "activity.log"))
    monkeypatch.setenv("WATCHTOWER_DELEGATE_URL", "off")
    monkeypatch.delenv("WATCHTOWER_MACHINE", raising=False)
    import watchtower.config as config
    import watchtower.queue as q
    import watchtower.workers as workers
    importlib.reload(q)
    import watchtower.cli as cli
    importlib.reload(cli)
    monkeypatch.setattr(config, "CONFIG_FILE", tmp_path / "config.json")
    monkeypatch.setattr(q, "_notify_review", lambda *a, **k: None)
    monkeypatch.setattr(q, "_notify_ticket_event", lambda *a, **k: None)
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", str(repo)], check=True)

    class Ns:
        pass

    ns = Ns()
    ns.q, ns.config, ns.cli, ns.workers, ns.repo = q, config, cli, workers, repo
    ns.spawns = []

    def fake_spawn(prompt, engine="claude", **kw):
        ns.spawns.append(dict(prompt=prompt, engine=engine, **kw))
        return {"worker_id": f"assess-{len(ns.spawns)}", "pid": 0}

    monkeypatch.setattr(cli.workers, "spawn_adhoc", fake_spawn)
    monkeypatch.setattr(workers, "spawn_adhoc", fake_spawn)
    config.set_repo_path("AS", str(repo))
    config.set_post_fix_assessment("AS", True)
    return ns


def _bug(q, title="boom", **kw):
    it = q.enqueue(project="AS", title=title, note=title, item_type="bug", **kw)
    q.claim_next("w1", project="AS")
    return it


def _close(q, ref, summary="fixed"):
    return q.close(ref, session_id="w1", resolution={"summary": summary, "no_code": True})


def _running(wt, title="boom"):
    b = _bug(wt.q, title)
    _close(wt.q, b["ref"])
    token = wt.q.assessment_reserve(b["ref"])
    assert token
    return b["ref"], token


def test_due_only_for_completed_bugs_on_opted_in_queues(wt):
    q = wt.q
    b = _bug(q)
    assert _close(q, b["ref"])["assessment"]["status"] == "due"
    f = q.enqueue(project="AS", title="feat", note="feat", item_type="feature")
    q.claim_next("w1", project="AS")
    assert "assessment" not in _close(q, f["ref"])
    d = _bug(q, "dupe")
    assert "assessment" not in _close(q, d["ref"], "Duplicate of AS-1")
    wt.config.set_post_fix_assessment("AS", False)
    o = _bug(q, "off")
    assert "assessment" not in _close(q, o["ref"])


def test_recursion_guard_followup_bugs_never_assessed(wt):
    q = wt.q
    ref, token = _running(wt)
    q.assessment_accept_submission(ref, token, {**ALL_ADEQUATE, "logging": {
        "verdict": "gap", "followups": [{"title": "add request id to log"}]}})
    q.assessment_run_ops(ref, token)
    f = next(i for i in q.list_items() if i.get("source") == "post-fix-assessment")
    q.claim_next("w1", project="AS")
    assert "assessment" not in _close(q, f["ref"])


def test_verify_gated_bug_is_due_only_after_accept(wt):
    q = wt.q
    b = _bug(q, gates=["verify"])
    it = _close(q, b["ref"])
    assert it["status"] == "in_review" and "assessment" not in it
    it = q.accept(b["ref"], force=True)
    assert it["assessment"]["status"] == "due"


def test_cmd_close_spawns_cross_family_assessor_in_queue_repo(wt, monkeypatch, capsys):
    b = _bug(wt.q)
    rc = wt.cli.main(["close", b["ref"], "--worker", "w1", "--summary", "fixed", "--no-code"])
    assert rc == 0
    assert len(wt.spawns) == 1
    sp = wt.spawns[0]
    assert sp["repo_path"] == str(wt.repo) and sp["name"] == "assess-" + b["ref"]
    a = wt.q.get(b["ref"])["assessment"]
    assert a["status"] == "running" and a["assessor"]["worker_id"] == "assess-1"
    assert a["token"] in sp["prompt"]


def test_spawn_failure_never_breaks_close(wt, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("no engine")
    monkeypatch.setattr(wt.workers, "spawn_adhoc", boom)
    b = _bug(wt.q)
    assert wt.cli.main(["close", b["ref"], "--worker", "w1", "--summary", "s", "--no-code"]) == 0
    it = wt.q.get(b["ref"])
    assert it["status"] == "closed" and it["assessment"]["status"] == "failed"
    assert "no engine" in it["assessment"]["reason"]


def test_blocked_assessor_model_fails_without_spawn(wt, monkeypatch):
    monkeypatch.setattr(wt.config, "is_blocked_model", lambda m: True)
    wt.config.set_role("AS", "assessor", "codex", None)
    monkeypatch.setattr(wt.q, "assessor_target",
                        lambda item: {"engine": "codex", "model": "m", "source": "queue", "blocked": True})
    b = _bug(wt.q)
    _close(wt.q, b["ref"])
    res = wt.workers.start_assessment(wt.q.get(b["ref"]))
    assert res["status"] == "failed" and not wt.spawns
    assert "policy" in wt.q.get(b["ref"])["assessment"]["reason"]


def test_no_repo_fails_and_never_uses_cwd(wt, monkeypatch):
    monkeypatch.setattr(wt.workers, "assessment_repo", wt.workers.assessment_repo.real)
    wt.config.set_repo_path("AS", "")
    b = _bug(wt.q)
    _close(wt.q, b["ref"])
    res = wt.workers.start_assessment(wt.q.get(b["ref"]))
    assert res["status"] == "failed" and "no local repo" in res["reason"] and not wt.spawns
    wt.config.set_repo_path("AS", "host:vm:/x")
    with pytest.raises(ValueError):
        wt.workers.assessment_repo({"project": "AS", "repo_path": ""})


def test_reserve_is_single_winner_and_force_mints_new_token(wt):
    q = wt.q
    b = _bug(q)
    _close(q, b["ref"])
    results = []
    ts = [threading.Thread(target=lambda: results.append(q.assessment_reserve(b["ref"])))
          for _ in range(8)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    assert len([r for r in results if r]) == 1
    tok = next(r for r in results if r)
    new = q.assessment_reserve(b["ref"], force=True)
    assert new and new != tok
    with pytest.raises(q.AssessmentFenced):
        q.assessment_accept_submission(b["ref"], tok, ALL_ADEQUATE)
    assert q.get(b["ref"])["assessment"]["status"] == "running"


def test_submission_files_followups_and_summary(wt):
    q = wt.q
    ref, token = _running(wt)
    payload = {**ALL_ADEQUATE,
               "logging": {"verdict": "gap", "note": "no request id",
                           "followups": [{"title": "Log request id", "note": "details"}]},
               "monitoring": {"verdict": "gap", "followups": [{"title": "Alert on 5xx"}]}}
    q.assessment_accept_submission(ref, token, payload)
    item = q.assessment_run_ops(ref, token)
    assert item["assessment"]["status"] == "done"
    kids = [i for i in q.list_items() if i.get("source") == "post-fix-assessment"]
    assert len(kids) == 2 and all(k["blocked_by"] == [ref] for k in kids)
    assert {k["type"] for k in kids} == {"bug", "feature"}
    bug = q.get(ref)
    summaries = [h for h in bug["history"] if h["event"] == "comment"]
    assert len(summaries) == 1 and "logging [gap]" in summaries[0]["text"]
    assert any(h["event"] == "assessment" for h in bug["history"])


def test_all_adequate_files_nothing(wt):
    q = wt.q
    ref, token = _running(wt)
    q.assessment_accept_submission(ref, token, ALL_ADEQUATE)
    q.assessment_run_ops(ref, token)
    assert not [i for i in q.list_items() if i.get("source") == "post-fix-assessment"]
    a = q.get(ref)["assessment"]
    assert a["status"] == "done" and all(v["note"] == "nothing to add" for v in a["points"].values())


def test_dedup_links_active_match_and_ignores_note(wt):
    q = wt.q
    existing = q.enqueue(project="AS", title="Alert on 5xx!", note="different note")
    ref, token = _running(wt)
    payload = {**ALL_ADEQUATE, "monitoring": {"verdict": "gap", "followups": [
        {"title": "alert on 5xx", "note": "other"}, {"title": "Alert on 5xx"}]}}
    q.assessment_accept_submission(ref, token, payload)
    q.assessment_run_ops(ref, token)
    assert not [i for i in q.list_items() if i.get("source") == "post-fix-assessment"]
    m = q.get(existing["ref"])
    assert sum(1 for h in m["history"] if h["event"] == "comment") == 1
    assert q.get(ref)["assessment"]["followups"] == [existing["ref"]]


def test_closed_match_is_not_a_dedup_hit(wt):
    q = wt.q
    old = q.enqueue(project="AS", title="Alert on 5xx", note="x")
    q.claim_next("w2", project="AS")  # claims the older ticket
    q.close(old["ref"], session_id="w2", resolution={"summary": "s", "no_code": True})
    ref, token = _running(wt, "second")
    q.assessment_accept_submission(ref, token, {**ALL_ADEQUATE, "monitoring": {
        "verdict": "gap", "followups": [{"title": "Alert on 5xx"}]}})
    q.assessment_run_ops(ref, token)
    assert len([i for i in q.list_items() if i.get("source") == "post-fix-assessment"]) == 1


@pytest.mark.parametrize("mutate", [
    lambda p: p.pop("other"),
    lambda p: p.update(bogus={"verdict": "adequate"}),
    lambda p: p.update(logging={"verdict": "gap"}),
    lambda p: p.update(logging={"verdict": "gap", "existing": ["AS-999"]}),
    lambda p: p.update(logging={"verdict": "gap", "followups": [{"title": "x", "queue": "NOPE"}]}),
    lambda p: p.update(logging={"verdict": "maybe"}),
    lambda p: p.update(logging={"verdict": "gap", "followups": [{"title": f"t{i}"} for i in range(4)]}),
])
def test_validation_rejects_without_mutating(wt, mutate):
    q = wt.q
    ref, token = _running(wt)
    payload = json.loads(json.dumps(ALL_ADEQUATE))
    mutate(payload)
    before = json.dumps(q.list_items(), sort_keys=True)
    with pytest.raises(ValueError):
        q.assessment_accept_submission(ref, token, payload)
    assert json.dumps(q.list_items(), sort_keys=True) == before
    assert q.get(ref)["assessment"]["status"] == "running"


def test_closed_existing_ref_rejected(wt):
    q = wt.q
    other = _bug(q, "other")
    _close(q, other["ref"])
    ref, token = _running(wt)
    with pytest.raises(ValueError, match="closed"):
        q.assessment_accept_submission(ref, token, {**ALL_ADEQUATE, "logging": {
            "verdict": "gap", "existing": [other["ref"]]}})


def test_link_op_comments_existing_ticket_once(wt):
    q = wt.q
    cover = q.enqueue(project="AS", title="covers it", note="x")
    ref, token = _running(wt)
    q.assessment_accept_submission(ref, token, {**ALL_ADEQUATE, "auditors": {
        "verdict": "gap", "existing": [cover["ref"]], "note": "same root"}})
    q.assessment_run_ops(ref, token)
    assert sum(1 for h in q.get(cover["ref"])["history"] if h["event"] == "comment") == 1


def test_concurrent_runners_write_each_op_once(wt):
    q = wt.q
    ref, token = _running(wt)
    payload = {**ALL_ADEQUATE, "logging": {"verdict": "gap", "followups": [
        {"title": "one"}, {"title": "two"}, {"title": "three"}]}}
    q.assessment_accept_submission(ref, token, payload)
    barrier = threading.Barrier(2)
    q._ASSESS_OP_HOOK = lambda stage, idx: barrier.wait(5) if (stage == "before_lock" and idx == 0) else None
    errs = []

    def run():
        try:
            q.assessment_run_ops(ref, token)
        except Exception as exc:  # noqa: BLE001
            errs.append(exc)

    ts = [threading.Thread(target=run) for _ in range(2)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    q._ASSESS_OP_HOOK = None
    kids = [i for i in q.list_items() if i.get("source") == "post-fix-assessment"]
    assert len(kids) == 3
    # the loser may find the assessment already done and be fenced; never double-write
    assert all(isinstance(e, (q.AssessmentFenced, ValueError)) for e in errs)
    assert sum(1 for h in q.get(ref)["history"] if h["event"] == "comment") == 1


def test_force_during_filing_fences_old_runner_and_dedups(wt):
    q = wt.q
    ref, token = _running(wt)
    payload = {**ALL_ADEQUATE, "logging": {"verdict": "gap", "followups": [
        {"title": "alpha"}, {"title": "beta"}]}}
    q.assessment_accept_submission(ref, token, payload)

    def hook(stage, idx):
        if stage == "after_commit" and idx == 0:
            raise RuntimeError("runner died after op 0")
    q._ASSESS_OP_HOOK = hook
    with pytest.raises(RuntimeError):
        q.assessment_run_ops(ref, token)
    q._ASSESS_OP_HOOK = None
    new = q.assessment_reserve(ref, force=True)
    a = q.get(ref)["assessment"]
    assert new and a["abandoned"] and a["status"] == "running"
    with pytest.raises(q.AssessmentFenced):
        q._assessment_apply_op(ref, token, 1)
    q.assessment_accept_submission(ref, new, payload)
    q.assessment_run_ops(ref, new)
    kids = [i for i in q.list_items() if i.get("source") == "post-fix-assessment"]
    assert sorted(k["title"] for k in kids) == ["alpha", "beta"]  # alpha linked, not duplicated


def test_crash_before_commit_then_resume(wt, monkeypatch):
    q = wt.q
    ref, token = _running(wt)
    q.assessment_accept_submission(ref, token, {**ALL_ADEQUATE, "logging": {
        "verdict": "gap", "followups": [{"title": "alpha"}, {"title": "beta"}]}})
    real = q._save_unlocked
    calls = {"n": 0}

    def flaky(data):
        calls["n"] += 1
        if calls["n"] == 2:
            raise OSError("disk")
        return real(data)
    monkeypatch.setattr(q, "_save_unlocked", flaky)
    with pytest.raises(OSError):
        q.assessment_run_ops(ref, token)
    monkeypatch.setattr(q, "_save_unlocked", real)
    a = q.get(ref)["assessment"]
    assert a["status"] == "filing" and [o.get("done") for o in a["pending"]["ops"]][:2] == [True, None]
    q.assessment_run_ops(ref)
    assert len([i for i in q.list_items() if i.get("source") == "post-fix-assessment"]) == 2
    assert q.get(ref)["assessment"]["status"] == "done"


def test_resubmit_same_payload_resumes_different_payload_refused(wt):
    q = wt.q
    ref, token = _running(wt)
    payload = {**ALL_ADEQUATE, "logging": {"verdict": "gap", "followups": [{"title": "a"}]}}
    ops = q.assessment_accept_submission(ref, token, payload)
    assert q.assessment_accept_submission(ref, token, payload) == ops
    with pytest.raises(ValueError, match="already being filed"):
        q.assessment_accept_submission(ref, token, ALL_ADEQUATE)


def _age_stage(q, ref, seconds=120):
    """Backdate the stage session so its (recordless, fake) worker reads as dead."""
    import datetime as _dt
    old = (_dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(seconds=seconds)
           ).strftime("%Y-%m-%dT%H:%M:%SZ")
    q.stage_session_update(ref, lambda it, ss: ss.update(spawned_at=old))


def test_sweep_spawns_due_respawns_dead_then_escalates(wt):
    q = wt.q
    b = _bug(q)
    _close(q, b["ref"])                      # library close: nothing spawns inline
    assert q.get(b["ref"])["assessment"]["status"] == "due"
    assert wt.workers.spawn_due_assessments() == [f"{b['ref']}:spawned"]
    a = q.get(b["ref"])["assessment"]
    assert a["status"] == "running" and a["attempt"] == 1 and a["cycle"] == 1
    t1 = a["token"]
    assert wt.workers.spawn_due_assessments() == []          # alive (just spawned)
    _age_stage(q, b["ref"])
    assert wt.workers.spawn_due_assessments() == [f"{b['ref']}:spawned"]   # respawn
    a = q.get(b["ref"])["assessment"]
    assert a["attempt"] == 2 and a["token"] != t1 and t1 in a["fenced_tokens"]
    assert a["assessor"]["worker_id"] == "assess-2"
    with pytest.raises(q.AssessmentFenced, match="superseded"):
        q.assessment_accept_submission(b["ref"], t1, ALL_ADEQUATE)
    _age_stage(q, b["ref"])
    assert wt.workers.spawn_due_assessments() == [f"{b['ref']}:settled"]
    it = q.get(b["ref"])
    assert it["assessment"]["status"] == "failed" and it["needs_input"] is True
    assert "died twice" in it["assessment"]["reason"]


def test_enqueue_persists_assessment_origin_and_validates_blockers(wt):
    q = wt.q
    it = q.enqueue(project="AS", title="t", note="t", assessment_origin="AS-1#a1#logging#0")
    assert it["assessment_origin"] == "AS-1#a1#logging#0"
    with pytest.raises(ValueError):
        q.enqueue(project="AS", title="t", note="t", blocked_by=["AS-999"])


def test_cli_submit_show_and_stale_token(wt, capsys):
    ref, token = _running(wt)
    payload = json.dumps({**ALL_ADEQUATE, "ui_message": {"verdict": "gap", "followups": [
        {"title": "Show the real error"}]}})
    assert wt.cli.main(["assess", "submit", ref, "--token", "nope", "--json", payload]) == 1
    assert wt.cli.main(["assess", "submit", ref, "--token", token, "--json", payload]) == 0
    capsys.readouterr()
    assert wt.cli.main(["assess", "show", ref, "--json"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["status"] == "done" and len(out["followups"]) == 1
    assert wt.cli.main(["find", ref]) == 0
    assert "assessment: done" in capsys.readouterr().out


def test_cli_invalid_submit_exits_2(wt):
    ref, token = _running(wt)
    assert wt.cli.main(["assess", "submit", ref, "--token", token, "--json", "{}"]) == 2


def test_cli_run_dry_run_reserves_nothing(wt, capsys):
    b = _bug(wt.q)
    _close(wt.q, b["ref"])
    assert wt.cli.main(["assess", "run", b["ref"], "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert str(wt.repo) in out and "assessor:" in out
    assert wt.q.get(b["ref"])["assessment"]["status"] == "due" and not wt.spawns


def test_assess_new_files_closes_and_spawns(wt, capsys):
    rc = wt.cli.main(["assess", "new", "-q", "AS", "--title", "auditor healed X",
                      "--no-code", "--json"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["status"] == "closed" and out["assessment"] == "running" and len(wt.spawns) == 1


def test_assess_new_verify_gated_waits(wt, capsys):
    wt.config.set_gates("AS", ["review"])
    rc = wt.cli.main(["assess", "new", "-q", "AS", "--title", "healed", "--no-code", "--json"])
    out = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert out["status"] == "in_review" and out["assessment"] == "" and not wt.spawns
    assert wt.cli.main(["accept", out["ref"]]) == 0
    assert len(wt.spawns) == 1


def test_config_flag_needs_local_git_repo(wt, tmp_path, capsys):
    cli = wt.cli
    assert cli.main(["config", "-q", "NEW", "--post-fix-assessment", "on"]) == 2
    assert "--repo-path" in capsys.readouterr().err
    assert not wt.config.post_fix_assessment("NEW")
    plain = tmp_path / "plain"
    plain.mkdir()
    assert cli.main(["config", "-q", "NEW", "--repo-path", str(plain),
                     "--post-fix-assessment", "on"]) == 2
    assert cli.main(["config", "-q", "NEW", "--repo-path", str(wt.repo),
                     "--post-fix-assessment", "on"]) == 0
    assert wt.config.post_fix_assessment("NEW") and wt.config.repo_path("NEW") == str(wt.repo)
    assert cli.main(["config", "-q", "NEW", "--post-fix-assessment", "off"]) == 0
    assert not wt.config.post_fix_assessment("NEW")


def test_assessor_role_config_and_default_family(wt):
    assert "assessor" in wt.config.ROLE_KEYS
    wt.config.set_role("AS", "assessor", "codex", None)
    assert wt.config.role_override("AS", "assessor")[0] == "codex"
    import watchtower.roles as roles
    assert "assessor" in roles.ROLES

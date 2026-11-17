"""Stage results affect only the daemon's pinned ticket/attempt/generation."""
import uuid

import pytest

from watchtower import sandbox_bridge as bridge, sandbox_stage_transport as stage
from tests.test_sandbox_bridge import env, claim, submit  # noqa: F401
from tests.test_sandbox_policy import base  # noqa: F401


def change(w, ref, **fields):
    with w.queue._FileLock(w.queue._lock_path()):
        data = w.queue._load_unlocked()
        item = next(i for i in data["items"] if i["ref"] == ref)
        item.update(fields)
        w.queue._save_unlocked(data)
    return w.queue.get(ref)


def scoped(w, role="verifier", stages=None):
    item = claim(w)
    fields = dict(status="in_review" if role == "verifier" else "closed",
                  resolution={"commit": "a" * 40, "summary": "Fixed fixture."},
                  gate_pending="verify" if role == "verifier" else "",
                  gate_stages=stages or ["verify"], verify_cycle=1,
                  stage_session={"key": role + ":1", "attempt": 1, "worker_id": w.worker})
    if role == "assessor":
        fields["assessment"] = {"status": "running", "token": "token1", "cycle": 1, "attempt": 1}
    item = change(w, item["ref"], **fields)
    with bridge.directory(bridge.authority_dir()) as fd:
        scope = bridge._read(fd, w.worker + ".json")
        scope["claim"] = None
        scope["stage"] = {"ref": item["ref"], "role": role, "key": role + ":1", "attempt": 1,
                          "epoch": stage.epoch(item), "commit": "a" * 40,
                          "context_digest": stage._digest(stage.context(item, {"items": w.queue.list_items()})),
                          "token": "token1" if role == "assessor" else "",
                          "assessment_cycle": 1, "assessment_attempt": 1}
        bridge._write(fd, w.worker + ".json", scope)
    return item, scope


def verdict(item, passed=True):
    return {"ref": item["ref"], "passed": passed, "findings": "Fixture tests and outcome checked."}


def payload(w):
    return {"points": {point: {"verdict": "adequate", "note": "Fixture needs no change here."}
                        for point in w.queue.ASSESSMENT_POINTS}}


def test_scoped_verdict_closes_once_and_receipt_replays(env):
    w = env
    item, _ = scoped(w)
    key = uuid.uuid4().hex
    response = submit(w, "verdict", verdict(item), key=key)
    assert response["ok"], response
    assert w.queue.get(item["ref"])["status"] == "closed"
    assert submit(w, "verdict", verdict(item), key=key) == response
    assert not submit(w, "verdict", verdict(item))["ok"]
    results = w.queue.get(item["ref"])["gate_results"]
    assert len(results) == 1 and results[0]["sandboxed"]


def test_scoped_verdict_preserves_required_review_and_failure_reopens(env):
    w = env
    item, _ = scoped(w, stages=["verify", "review"])
    assert submit(w, "verdict", verdict(item))["ok"]
    current = w.queue.get(item["ref"])
    assert current["status"] == "in_review" and current["gate_pending"] == "review"


def test_failed_verdict_returns_to_restricted_builder_pool(env):
    w = env
    item, _ = scoped(w)
    assert submit(w, "verdict", verdict(item, False))["ok"]
    current = w.queue.get(item["ref"])
    assert current["status"] == "open" and not current.get("claimed_by")
    assert not current.get("resolution") and not current.get("closed_by")
    assert "Fixture tests" in current["gate_feedback"]
    assert w.queue._sandbox_restricted(current)


@pytest.mark.parametrize("mutation", [
    {"verify_cycle": 2}, {"last_answer_gen": 2}, {"text": "Changed request"},
    {"repo_path": "/different"}, {"resolution": {"commit": "b" * 40}},
    {"stage_session": {"key": "verifier:1", "attempt": 2, "worker_id": "other"}},
])
def test_stage_stale_result_never_mutates_current_ticket(env, mutation):
    w = env
    item, _ = scoped(w)
    current = change(w, item["ref"], **mutation)
    assert not submit(w, "verdict", verdict(item))["ok"]
    assert w.queue.get(item["ref"]) == current


def test_stage_cannot_claim_close_or_select_another_ref(env):
    w = env
    item, _ = scoped(w)
    other = w.queue.enqueue(project="SB", note="peer")
    assert not submit(w, "claim", {})["ok"]
    assert not submit(w, "close", {})["ok"]
    assert not submit(w, "verdict", dict(verdict(item), ref=other["ref"]))["ok"]
    assert w.queue.get(other["ref"])["status"] == "open"
    assert w.queue.get(item["ref"])["status"] == "in_review"


def test_assessment_submits_six_points_and_cannot_write_existing_tickets(env):
    w = env
    item, _ = scoped(w, "assessor")
    other = w.queue.enqueue(project="SB", note="peer")
    bad = payload(w)
    bad["points"]["other"]["existing"] = [other["ref"]]
    request = {"ref": item["ref"], "token": "token1", "payload": bad}
    assert not submit(w, "assess", request)["ok"]
    assert w.queue.get(other["ref"]) == other
    result = submit(w, "assess", dict(request, payload=payload(w)))
    assert result["ok"], result
    assert w.queue.get(item["ref"])["assessment"]["status"] == "done"


def test_assessment_rechecks_scope_at_each_op(env, monkeypatch):
    w = env
    item, scope = scoped(w, "assessor")
    w.queue.assessment_accept_submission(item["ref"], "token1", payload(w), sandbox_scope=scope)
    before = change(w, item["ref"], last_answer_gen=2)
    with pytest.raises(ValueError, match="generation changed"):
        w.queue.assessment_run_ops(item["ref"], "token1")
    assert w.queue.get(item["ref"]) == before


def test_assessment_followups_are_new_icebox_items_in_assigned_queue(env):
    w = env
    item, _ = scoped(w, "assessor")
    peer = w.queue.enqueue(project="SB", note="Regression coverage")
    bad = payload(w)
    bad["points"]["other"] = {"verdict": "gap", "note": "Missing check", "followups": [
        {"title": "Regression coverage", "queue": "OTHER"}]}
    request = {"ref": item["ref"], "token": "token1", "payload": bad}
    assert not submit(w, "assess", request)["ok"]
    bad["points"]["other"]["followups"][0]["queue"] = "SB"
    result = submit(w, "assess", request)
    assert result["ok"], result
    assert w.queue.get(peer["ref"]) == peer
    new = [i for i in w.queue.list_items() if i.get("assessment_origin")]
    assert len(new) == 1 and new[0]["project"] == "SB"
    assert new[0]["readiness"] == w.queue.ASSESSMENT_FOLLOWUP_READINESS


@pytest.mark.parametrize("stale", [False, True])
def test_group_verdict_requires_current_child_integration_proof(env, stale):
    w = env
    item, scope = scoped(w)
    child = w.queue.enqueue(project="SB", note="group member")
    child = change(w, child["ref"], status="closed", resolution={"commit": "b" * 40})
    item = change(w, item["ref"], group={"role": "parent", "members": [child["ref"]],
        "integration": {"state": "verifying", "sha": "a" * 40,
                        "proven": {"sha": "a" * 40, "commits": {child["ref"]: "b" * 40}}}})
    scope["stage"]["epoch"] = stage.epoch(item)
    scope["stage"]["context_digest"] = stage._digest(stage.context(item, {"items": [item, child]}))
    with bridge.directory(bridge.authority_dir()) as fd:
        bridge._write(fd, w.worker + ".json", scope)
    if stale:
        change(w, child["ref"], resolution={"commit": "c" * 40})
    result = submit(w, "verdict", verdict(item))
    if stale:
        assert not result["ok"]
        assert w.queue.get(item["ref"]) == item
    else:
        assert result["ok"], result
        done = w.queue.get(item["ref"])
        assert done["status"] == "closed" and done["group"]["integration"]["state"] == "done"


def test_clarification_delivery_is_not_a_new_answer_but_text_change_is(env):
    w = env
    item, scope = scoped(w)
    # Bind a real clarification, then simulate its delivery bookkeeping.
    item = change(w, item["ref"], pending_answer={
        "gen": 1, "question": "Scope?", "answer": "Only the fixture.", "state": "routing"})
    scope["stage"]["epoch"] = stage.epoch(item)
    scope["stage"]["context_digest"] = stage._digest(stage.context(item, {"items": [item]}))
    with bridge.directory(bridge.authority_dir()) as fd:
        bridge._write(fd, w.worker + ".json", scope)
    item = change(w, item["ref"], pending_answer=dict(item["pending_answer"], state="delivered"))
    stage.guard(scope, item, data={"items": [item]})
    before = change(w, item["ref"], pending_answer=dict(item["pending_answer"], answer="New scope"))
    assert not submit(w, "verdict", verdict(item))["ok"]
    assert w.queue.get(item["ref"]) == before


def test_parent_accepted_plan_is_in_child_context_and_invalidates_old_view(env):
    w = env
    item, scope = scoped(w)
    parent = {"ref": "SB-999", "group": {"role": "parent", "members": [item["ref"]]},
              "plan": {"status": "accepted", "text": "Shared accepted requirements.",
                       "sections": {item["ref"]: "Specific child requirement."}}}
    item["group"] = {"role": "child", "parent": parent["ref"]}
    data = {"items": [item, parent]}
    view = stage.context(item, data)
    assert "Shared accepted requirements." in view["accepted_plan"]
    assert "Specific child requirement." in view["accepted_plan"]
    scope["stage"]["epoch"] = stage.epoch(item)
    scope["stage"]["context_digest"] = stage._digest(view)
    stage.guard(scope, item, data=data)
    parent["plan"]["sections"][item["ref"]] = "Revised child requirement."
    with pytest.raises(ValueError, match="generation changed"):
        stage.guard(scope, item, data=data)


def test_non_codex_stage_target_is_refused_without_allocating(env, monkeypatch):
    def unexpected(*a, **kw):
        pytest.fail("unsupported stage must not allocate or launch")
    monkeypatch.setattr(stage.source, "prepared_source", unexpected)
    with pytest.raises(ValueError, match="not substituting claude"):
        stage.launch({}, "verifier", "claude", "configured-model")


def test_scope_survives_live_policy_disable_without_granting_builder_verbs(env):
    w = env
    item, _ = scoped(w)
    w.config.set_codex_sandbox("SB", "full-access")
    assert not submit(w, "claim", {})["ok"]
    assert submit(w, "verdict", verdict(item))["ok"]
    assert w.queue.get(item["ref"])["status"] == "closed"


def test_replaced_stage_source_never_accepts_a_result(env, monkeypatch):
    w = env
    item, scope = scoped(w)
    scope["stage"]["source_pin"] = {"commit": "a" * 40}
    with bridge.directory(bridge.authority_dir()) as fd:
        bridge._write(fd, w.worker + ".json", scope)
    monkeypatch.setattr(stage.source, "source_unchanged", lambda *a: "repository was replaced")
    result = submit(w, "verdict", verdict(item))
    assert not result["ok"] and "replaced" in result["error"]
    assert w.queue.get(item["ref"]) == item


def test_registration_writes_full_clarification_context_before_launch(env):
    import json
    w = env
    item, _ = scoped(w)
    item = change(w, item["ref"], pending_answer={"gen": 7, "question": "Scope?",
        "answer": "Preserve all original requirements."},
        stage_session={"reserved": {"key": "verifier:1", "attempt": 1}})
    scope = stage._register(item, "verifier", w.worker,
        {"workspace": w.repo, "store": w.store, "commit": "a" * 40})
    stored = json.loads((w.store / "ticket-context.json").read_text())
    assert stored["pending_answer"]["answer"] == "Preserve all original requirements."
    assert stored["resolution"] == item["resolution"]
    assert scope["stage"]["context_digest"] == stage._digest(stored)
    assert w.queue.get(item["ref"])["stage_session"]["worker_id"] == w.worker


@pytest.mark.parametrize("transport", ["exec", "app-server"])
def test_stage_launch_always_uses_native_profile_and_scoped_store(env, base, monkeypatch, transport):
    from types import SimpleNamespace
    w = env
    monkeypatch.setenv("WATCHTOWER_DATA_DIR", str(w.tmp / "persistent-data"))
    item, _ = scoped(w)
    roots = {"dir": base / "fresh", "workspace": base / "stage-ws", "store": base / "stage-store",
             "commit": "a" * 40, "manifest": {"commit": "a" * 40}}
    roots["workspace"].mkdir()
    roots["store"].mkdir()
    monkeypatch.setattr(stage.source, "prepared_source", lambda *a: roots)
    monkeypatch.setattr(stage.source, "pin_source", lambda *a: {"commit": "a" * 40})
    monkeypatch.setattr(stage.policy, "write_wt_shim", lambda *a: None)
    monkeypatch.setattr(stage, "_register", lambda *a: {
        "stage": {"ref": item["ref"], "role": "verifier", "commit": "a" * 40},
        "repo": str(roots["workspace"]), "store": str(roots["store"]), "worker": w.worker})
    monkeypatch.setattr(bridge, "worker_store", lambda *a: roots["store"])
    monkeypatch.setattr(w.workers, "_resolve_engine_bin", lambda *a: "/usr/bin/codex")
    monkeypatch.setattr(w.workers, "_spawn_gate", lambda *a: None)
    monkeypatch.setattr(w.workers, "record_worker", lambda *a, **kw: dict(kw, pid=999))
    calls = []
    monkeypatch.setattr(stage, "subprocess", SimpleNamespace(
        DEVNULL=stage.subprocess.DEVNULL, STDOUT=stage.subprocess.STDOUT,
        Popen=lambda args, **kw: calls.append((args, kw)) or SimpleNamespace(pid=999)))
    w.config.set_codex_transport("SB", transport)
    w.config.set_codex_sandbox("SB", "full-access")
    result = stage.launch(item, "verifier", "codex", "configured-model")
    argv, spawn = calls[0]
    assert result["sandboxed"] and "configured-model" in argv
    assert not any("dangerously" in arg or "danger-full-access" in arg for arg in argv)
    assert any("default_permissions=" in arg for arg in argv)
    assert spawn["cwd"] == "/"
    assert spawn["env"]["WT_SANDBOX_BRIDGE"] == str(roots["store"])
    assert spawn["env"]["WATCHTOWER_STORE"] == str(roots["store"] / "queue.db")

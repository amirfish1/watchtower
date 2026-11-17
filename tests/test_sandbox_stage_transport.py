"""Stage results affect only the daemon's pinned ticket/attempt/generation."""
import uuid

import pytest

from watchtower import sandbox_bridge as bridge, sandbox_stage_transport as stage
from tests.test_sandbox_bridge import env, claim, submit  # noqa: F401


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


def test_non_codex_stage_target_is_refused_without_allocating(env, monkeypatch):
    def unexpected(*a, **kw):
        pytest.fail("unsupported stage must not allocate or launch")
    monkeypatch.setattr(stage.source, "prepared_source", unexpected)
    with pytest.raises(ValueError, match="not substituting claude"):
        stage.launch({}, "verifier", "claude", "configured-model")

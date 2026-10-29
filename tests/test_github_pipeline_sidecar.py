"""OPS-201 (owner decision C): the plan gate on a GitHub-backed queue. The issue
stays canonical; pipeline state lives in the local sidecar
(docs/github-pipeline-sidecar.md). These pin the contract, not the storage."""
import importlib
import json
import threading

import pytest

from tests.test_github_backend import (  # noqa: F401  (autouse fixture)
    _drainable,
    _install_fake_gh,
    _reload_isolated,
    restore_watchtower_modules,
)


def _setup(tmp_path, monkeypatch, host="her"):
    monkeypatch.setenv("WATCHTOWER_MACHINE", host)
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    q._MACHINE_TAG_CACHE = None
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _drainable(config)
    item = q.enqueue(project="GHI", note="gated work", text="body")
    return state, config, q, item["ref"]


def _body(state):
    return json.loads(state.read_text())["issues"][0]["body"]


def _sidecar(q):
    from watchtower import github_pipeline
    return github_pipeline.path()


def test_plan_gate_holds_github_ticket_until_plan_accepted(tmp_path, monkeypatch):
    state, _config, q, ref = _setup(tmp_path, monkeypatch)
    q.update(ref, gates=["plan"])

    item = q.get(ref)
    assert item["gates"] == ["plan"] and item["pipeline_host"] == "her"
    assert "pipeline_host" in _body(state)
    assert q.count_claimable(project="GHI") == 0
    assert q.claim_next("builder-1", project="GHI") is None

    started = q.plan_start(ref)
    assert started["_plan_started"] and q.get(ref)["plan"]["status"] == "planning"
    q.plan_submit(ref, "1. do it")
    assert q.claim_next("builder-1", project="GHI") is None
    q.plan_verdict(ref, True, "fine")

    assert q.get(ref)["plan"]["status"] == "accepted"
    events = [h["event"] for h in q.get(ref)["history"]]
    assert {"plan_start", "plan", "plan_review"} <= set(events)
    claimed = q.claim_next("builder-1", project="GHI")
    assert claimed and claimed["ref"] == ref


def test_pipeline_state_survives_restart(tmp_path, monkeypatch):
    _state, _config, q, ref = _setup(tmp_path, monkeypatch)
    q.update(ref, gates=["plan"])
    q.plan_start(ref)

    import watchtower.queue as fresh_q
    importlib.reload(fresh_q)
    fresh_q._MACHINE_TAG_CACHE = None
    assert fresh_q.get(ref)["plan"]["status"] == "planning"
    assert fresh_q.claim_next("builder-1", project="GHI") is None
    assert fresh_q.plan_start(ref)["_plan_started"] is False


def test_foreign_host_pipeline_fails_closed(tmp_path, monkeypatch):
    _state, _config, q, ref = _setup(tmp_path, monkeypatch)
    q._github_backend_for_project("GHI").update(ref, pipeline_host="mac")

    item = q.get(ref)
    assert item["pipeline_foreign"] and not item["work_it"]
    assert q.claim_next("builder-1", project="GHI") is None
    with pytest.raises(ValueError, match="owned by host"):
        q.update(ref, gates=["plan"])
    from watchtower import stages
    assert stages.desired([item]) == []


def test_lost_sidecar_fails_closed_not_open(tmp_path, monkeypatch):
    _state, _config, q, ref = _setup(tmp_path, monkeypatch)
    q.update(ref, gates=["plan"])
    _sidecar(q).unlink()

    item = q.get(ref)
    assert item["pipeline_missing"] and not item["work_it"]
    assert q.claim_next("builder-1", project="GHI") is None
    q.update(ref, gates=["plan"])   # documented recovery
    assert q.get(ref)["gates"] == ["plan"] and not q.get(ref).get("pipeline_missing")


def test_github_unreachable_leaves_sidecar_untouched(tmp_path, monkeypatch):
    _state, _config, q, ref = _setup(tmp_path, monkeypatch)
    q.update(ref, gates=["plan"])
    before = _sidecar(q).read_bytes()
    from watchtower.github_backend import GitHubBackendError
    backend_cls = type(q._github_backend_for_project("GHI"))

    def _down(self, ident):
        raise GitHubBackendError("gh: connection refused")
    monkeypatch.setattr(backend_cls, "get", _down)

    with pytest.raises(GitHubBackendError):
        q.plan_start(ref)
    assert _sidecar(q).read_bytes() == before


def test_concurrent_plan_start_starts_one_round(tmp_path, monkeypatch):
    _state, _config, q, ref = _setup(tmp_path, monkeypatch)
    q.update(ref, gates=["plan"])
    results = []
    threads = [threading.Thread(target=lambda: results.append(q.plan_start(ref)))
               for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sum(1 for r in results if r and r["_plan_started"]) == 1
    assert q.get(ref)["plan"]["round"] == 1


def test_gating_a_running_build_is_refused_and_the_claim_kept(tmp_path, monkeypatch):
    _state, _config, q, ref = _setup(tmp_path, monkeypatch)
    assert q.claim_by_ref(ref, "becky-worker")
    with pytest.raises(ValueError, match="running build"):
        q.update(ref, gates=["plan"])

    item = q.get(ref)
    assert item["claimed_by"] == "becky-worker" and item["status"] == "in_progress"
    assert item.get("gates") is None and not _sidecar(q).exists()


def test_only_sidecar_gates_start_a_github_planner(tmp_path, monkeypatch):
    _state, config, q, ref = _setup(tmp_path, monkeypatch)
    config.set_gates("GHI", ["plan"])
    from watchtower import stages
    assert stages.desired([q.get(ref)]) == []


def test_unsupported_github_gates_are_refused_without_writes(tmp_path, monkeypatch):
    state, _config, q, ref = _setup(tmp_path, monkeypatch)
    before = _body(state)
    with pytest.raises(ValueError, match="milestone 2"):
        q.update(ref, gates=["plan", "verify"])
    assert _body(state) == before and not _sidecar(q).exists()


def test_clearing_gates_releases_the_ticket(tmp_path, monkeypatch):
    state, _config, q, ref = _setup(tmp_path, monkeypatch)
    q.update(ref, gates=["plan"])
    q.update(ref, gates=[])
    item = q.get(ref)
    assert item.get("gates") is None and item["work_it"]
    assert "pipeline_host" not in _body(state)


def test_warm_list_memo_sees_sidecar_only_transitions(tmp_path, monkeypatch):
    _state, _config, q, ref = _setup(tmp_path, monkeypatch)
    from watchtower import usage
    monkeypatch.setattr(usage, "ledger_dir_stamp", lambda: "warm")
    monkeypatch.setattr("watchtower.github_backend._LIST_CACHE_TTL", 3600.0)
    q.update(ref, gates=["plan"])
    assert [it["work_it"] for it in q.list_items(project="GHI")] == [False]
    q.plan_start(ref)
    q.plan_submit(ref, "1. do it")
    q.plan_verdict(ref, True, "fine")
    assert [it["work_it"] for it in q.list_items(project="GHI")] == [True]


def test_answer_on_escalated_planner_resets_the_stage(tmp_path, monkeypatch):
    _state, _config, q, ref = _setup(tmp_path, monkeypatch)
    q.update(ref, gates=["plan"])
    q.plan_start(ref)

    def _escalate(it, ss):
        ss.update(role="planner", key="plan:r1", attempt=2, escalated=True,
                  escalated_from="planning")
        it["plan"] = dict(it["plan"], status="blocked", escalated="stage_watch")
    q.stage_session_update(ref, _escalate)
    q.block(ref, "", origin="stage", question="planner died twice", kind="input")
    assert q.get(ref)["plan"]["status"] == "blocked"

    q.answer(ref, "retry")
    item = q.get(ref)
    assert not item["needs_input"]
    assert item["plan"]["status"] == "planning"
    assert item["stage_session"]["escalated"] is False
    from watchtower import stages
    assert [d["role"] for d in stages.desired([item])] == ["planner"]


def test_the_claimant_may_gate_its_own_build(tmp_path, monkeypatch):
    _state, _config, q, ref = _setup(tmp_path, monkeypatch)
    assert q.claim_by_ref(ref, "becky-worker")
    monkeypatch.setenv("WT_WORKER_ID", "becky-worker")
    q.update(ref, gates=["plan"])

    item = q.get(ref)
    assert item["claimed_by"] == "becky-worker" and item["gates"] == ["plan"]
    from watchtower import stages
    assert [d["role"] for d in stages.desired([item])] == ["planner"]

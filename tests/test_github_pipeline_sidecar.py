"""OPS-201 (owner decision C): the plan gate on a GitHub-backed queue. The issue
stays canonical; pipeline state lives in the local sidecar
(docs/github-pipeline-sidecar.md). These pin the contract, not the storage."""
import contextlib
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
    assert item["gates"] == ["plan"] and item["pipeline_owner_host"] == "her"
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


def _issue_state(state):
    return json.loads(state.read_text())["issues"][0]["state"]


def _closed_by_verify(q, ref, worker="builder-1"):
    assert q.claim_by_ref(ref, worker)
    return q.close(ref, worker, resolution={"summary": "did it", "commit": "abc123"})


def test_verify_gate_holds_the_issue_open_until_a_current_verdict(tmp_path, monkeypatch):
    """OPS-204: a gated GitHub close is held for review with the issue open;
    only a verdict for the CURRENT cycle settles it, in one issue write."""
    state, _config, q, ref = _setup(tmp_path, monkeypatch)
    q.update(ref, gates=["verify"])
    held = _closed_by_verify(q, ref)

    assert held["status"] == "in_review" and _issue_state(state) == "OPEN"
    item = q.get(ref)
    assert item["status"] == "in_review" and not item["work_it"]
    assert item["resolution"]["summary"] == "did it"
    assert q.claim_next("builder-2", project="GHI") is None
    from watchtower import stages
    assert [(d["role"], d["key"]) for d in stages.desired([item])] == [("verifier", "verify:1")]
    with pytest.raises(ValueError, match="wt verdict"):
        q.close(ref, "builder-1", resolution="bypass")
    with pytest.raises(ValueError, match="--verify-cycle"):
        q.verdict(ref, True, "ok", by="v1")
    with pytest.raises(q.GroupFenced, match="cycle 0"):
        q.verdict(ref, True, "ok", by="v1", expect_verify_cycle=0)
    assert _issue_state(state) == "OPEN"

    done = q.verdict(ref, True, "checked", by="v1", expect_verify_cycle=1)
    assert done["status"] == "closed" and _issue_state(state) == "CLOSED"
    final = q.get(ref)
    assert final["resolution"]["summary"] == "did it" and not final.get("review_hold")
    assert [r["passed"] for r in final["gate_results"]] == [True]
    with pytest.raises(ValueError):
        q.verdict(ref, False, "late", by="v0", expect_verify_cycle=1)
    assert _issue_state(state) == "CLOSED"


def test_failed_verdict_reopens_with_feedback_and_review_gates_settle(tmp_path, monkeypatch):
    state, _config, q, ref = _setup(tmp_path, monkeypatch)
    q.update(ref, gates=["verify", "review"])
    _closed_by_verify(q, ref)
    out = q.verdict(ref, False, "test X fails", by="v1", expect_verify_cycle=1)

    assert out["status"] == "open" and _issue_state(state) == "OPEN"
    item = q.get(ref)
    assert "test X fails" in item["gate_feedback"] and item["work_it"]

    held = _closed_by_verify(q, ref, "builder-2")
    assert held["verify_cycle"] == 2
    with pytest.raises(q.GroupFenced):
        q.verdict(ref, True, "stale", by="v1", expect_verify_cycle=1)
    nxt = q.verdict(ref, True, "ok", by="v2", expect_verify_cycle=2)
    assert nxt["status"] == "in_review" and nxt["gate_pending"] == "review"
    assert _issue_state(state) == "OPEN"
    q.reject(ref, "not what I asked", by="amir")
    assert q.get(ref)["status"] == "open" and "not what I asked" in q.get(ref)["gate_feedback"]

    _closed_by_verify(q, ref, "builder-3")
    q.verdict(ref, True, "ok", by="v3", expect_verify_cycle=3)
    assert q.accept(ref, by="amir")["status"] == "closed" and _issue_state(state) == "CLOSED"


def test_a_failed_final_issue_write_stays_in_review_and_accept_retries(tmp_path, monkeypatch):
    state, _config, q, ref = _setup(tmp_path, monkeypatch)
    q.update(ref, gates=["review"])
    _closed_by_verify(q, ref)
    from watchtower.github_backend import GitHubBackendError
    backend_cls = type(q._github_backend_for_project("GHI"))
    real = backend_cls.update_status

    def _down(self, ident, status, *a, **kw):
        if status == "closed":
            raise GitHubBackendError("gh: connection refused")
        return real(self, ident, status, *a, **kw)
    monkeypatch.setattr(backend_cls, "update_status", _down)
    with pytest.raises(GitHubBackendError):
        q.accept(ref, by="amir")
    assert q.get(ref)["status"] == "in_review" and _issue_state(state) == "OPEN"

    monkeypatch.setattr(backend_cls, "update_status", real)
    assert q.accept(ref, by="amir")["status"] == "closed" and _issue_state(state) == "CLOSED"


def test_a_regate_is_a_new_generation_old_stage_writers_are_fenced(tmp_path, monkeypatch):
    """Clear + re-gate keeps a tombstone: cycles keep counting, stage keys
    change, and the cleared generation's planner/verifier cannot write."""
    _state, _config, q, ref = _setup(tmp_path, monkeypatch)
    q.update(ref, gates=["plan"])
    q.plan_start(ref)
    q.plan_set_role(ref, "planner", {"worker_id": "old-planner"})
    q.update(ref, gates=[])
    assert q.get(ref).get("gates") is None and q.get(ref)["work_it"]

    q.update(ref, gates=["plan", "verify"])
    q.plan_start(ref)
    from watchtower import stages
    item = q.get(ref)
    assert [d["key"] for d in stages.desired([item])] == ["plan:r1:g2"]
    with pytest.raises(q.GroupFenced, match="cleared pipeline generation"):
        q.plan_submit(ref, "1. stale plan", by="old-planner")
    q.plan_submit(ref, "1. fresh plan", by="new-planner")
    assert q.get(ref)["plan"]["text"] == "1. fresh plan"


def test_reservation_rechecks_the_stage_before_a_spawn(tmp_path, monkeypatch):
    """#10/#11: a spawn reserves its attempt in the store first; a stage that
    settled since the tick read it, or a second tick, never spawns."""
    _state, _config, q, ref = _setup(tmp_path, monkeypatch)
    q.update(ref, gates=["review", "verify"])
    _closed_by_verify(q, ref)
    from watchtower import stages
    assert stages._reserve(ref, "verifier", "verify:1", 1)
    assert not stages._reserve(ref, "verifier", "verify:1", 1)
    q.verdict(ref, True, "ok", by="v1", expect_verify_cycle=1)
    assert not stages._reserve(ref, "verifier", "verify:1", 2)


def test_add_with_gates_gates_the_new_issue(tmp_path, monkeypatch):
    _state, _config, q, _ref = _setup(tmp_path, monkeypatch)
    ref = q.enqueue(project="GHI", note="gated at birth", text="b", gates=["verify"])["ref"]
    assert q.get(ref)["gates"] == ["verify"] and q.get(ref)["pipeline_local"]


def test_plan_decide_release_reopens_the_issue(tmp_path, monkeypatch):
    """#6: plan_decide releasing a claim with no resumable session must
    release it on the issue, not only in the sidecar view."""
    state, _config, q, ref = _setup(tmp_path, monkeypatch)
    assert q.claim_by_ref(ref, "builder-1")
    monkeypatch.setenv("WT_WORKER_ID", "builder-1")
    q.update(ref, gates=["plan"])
    monkeypatch.delenv("WT_WORKER_ID")
    q.plan_start(ref)
    q._plan_update(ref, lambda it, plan: plan.update(status="blocked"))
    q.plan_decide(ref, "accept", text="1. go")
    item = q.get(ref)
    assert item["status"] == "open" and not item.get("claimed_by")
    assert "in-progress" not in json.dumps(json.loads(state.read_text())["issues"][0].get("labels"))


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


def test_ownership_keys_on_the_store_not_the_machine_tag(tmp_path, monkeypatch):
    """OPS-203: two stores whose hosts share a tag must not both own an issue,
    and a marker write that a racing host overwrote must not gate."""
    _state, _config, q, ref = _setup(tmp_path, monkeypatch)
    backend = q._github_backend_for_project("GHI")
    backend.update(ref, pipeline_host="her/0123456789abcdef0123456789abcdef")

    item = q.get(ref)
    assert item["pipeline_foreign"] and not item["work_it"]
    assert item["pipeline_owner_host"] == "her"
    with pytest.raises(ValueError, match="owned by host 'her'"):
        q.update(ref, gates=["plan"])

    backend.update(ref, pipeline_host="")
    real_update = type(backend).update

    def racing_update(self, ident, **fields):
        out = real_update(self, ident, **fields)
        if fields.get("pipeline_host"):
            real_update(self, ident, pipeline_host="ami/fedcba9876543210fedcba9876543210")
        return out

    monkeypatch.setattr(type(backend), "update", racing_update)
    with pytest.raises(ValueError, match="marker write lost"):
        q.update(ref, gates=["plan"])
    from watchtower import github_pipeline
    assert not _sidecar(q).exists() or github_pipeline.key(item) not in json.loads(
        _sidecar(q).read_text())["items"]
    assert q.get(ref)["pipeline_foreign"]


def test_gating_waits_until_every_claiming_host_honors_the_hold(tmp_path, monkeypatch):
    """OPS-203: a host whose wt predates the plan hold would claim a gated
    ticket, so gating is refused while one claimed the queue recently."""
    state, _config, q, ref = _setup(tmp_path, monkeypatch)
    other = q.enqueue(project="GHI", note="mac work", text="body")["ref"]
    q._MACHINE_TAG_CACHE = "ami"
    assert q.claim_by_ref(other, "ami-worker")
    q._MACHINE_TAG_CACHE = None
    data = json.loads(state.read_text())
    for issue in data["issues"]:
        issue["body"] = issue["body"].replace('"pipeline": 2', '"pipeline": 0')
    state.write_text(json.dumps(data))

    with pytest.raises(ValueError, match="ami"):
        q.update(ref, gates=["plan"])
    assert "pipeline_host" not in json.loads(state.read_text())["issues"][0]["body"]

    q.close(other)
    third = q.enqueue(project="GHI", note="more mac work", text="body")["ref"]
    q._MACHINE_TAG_CACHE = "ami"
    assert q.claim_by_ref(third, "ami-worker")
    q._MACHINE_TAG_CACHE = None
    q.update(ref, gates=["plan"])
    assert q.get(ref)["gates"] == ["plan"]


@contextlib.contextmanager
def _other_host(monkeypatch, q, sidecar):
    """Another machine running a wt from before OPS-215: it ignores
    ``pipeline_hold`` and honors a defer literally (OPS-100 on). Without the
    sidecar it predates OPS-201 too and ignores ``pipeline_host``."""
    from watchtower import github_backend, github_pipeline
    with monkeypatch.context() as m:
        m.setattr(github_backend, "_effective_defer",
                  lambda meta: str(meta.get("deferred_until") or ""), raising=False)
        if not sidecar:
            m.setattr(github_pipeline, "overlay", lambda item: item)
        q._MACHINE_TAG_CACHE = "ami"
        try:
            yield
        finally:
            q._MACHINE_TAG_CACHE = None


def _fenced(monkeypatch, q, ref):
    for sidecar in (False, True):
        with _other_host(monkeypatch, q, sidecar):
            backend = q._github_backend_for_project("GHI")
            if q.get(ref)["status"] == "in_progress":   # a manual release on that host
                backend.update_status(ref, "open", reason="released")
            assert q.claim_next("ami-worker", project="GHI") is None
            with pytest.raises(ValueError):
                q.claim_by_ref(ref, "ami-worker")


def test_a_host_that_cannot_read_the_pipeline_cannot_claim_a_held_ticket(tmp_path, monkeypatch):
    """OPS-215: the hold is on the issue itself, so a host without the sidecar
    (or one older than OPS-201) cannot claim a plan-pending ticket or a held
    in_review one, even reopened, and the owning host still settles both."""
    state, _config, q, ref = _setup(tmp_path, monkeypatch)
    q.update(ref, gates=["plan", "verify"])
    _fenced(monkeypatch, q, ref)

    q.plan_start(ref)
    q.plan_submit(ref, "1. do it")
    q.plan_verdict(ref, True, "fine")
    assert "pipeline_hold" not in _body(state) and q.get(ref)["work_it"]
    held = _closed_by_verify(q, ref)
    assert held["status"] == "in_review" and "pipeline_hold" in _body(state)
    _fenced(monkeypatch, q, ref)

    assert q.get(ref)["status"] == "in_review"
    done = q.verdict(ref, True, "checked", by="v1", expect_verify_cycle=1)
    assert done["status"] == "closed" and _issue_state(state) == "CLOSED"
    assert "pipeline_hold" not in _body(state) and "deferred_until" not in _body(state)


def _park_in_sidecar(q, ref, **extra):
    """Write park state straight into the sidecar entry (nothing writes it yet, WT-32a)."""
    p = _sidecar(q)
    data = json.loads(p.read_text())
    (entry,) = data["items"].values()
    entry["parked"] = {"worker_id": "w1", "session_id": "s1", "at": "2026-01-01T00:00:00Z"}
    entry.update(extra)
    p.write_text(json.dumps(data))


def _claimed_gated(tmp_path, monkeypatch):
    _state, _config, q, ref = _setup(tmp_path, monkeypatch)
    q.update(ref, gates=["plan"])
    q.plan_start(ref)
    q.plan_submit(ref, "1. do it")
    q.plan_verdict(ref, True, "fine")
    assert q.claim_next("w1", project="GHI")["ref"] == ref
    assert q.get(ref)["status"] == "in_progress"
    return q, ref


def test_parked_sidecar_overlays_awaiting_answer_and_clears_the_claim(tmp_path, monkeypatch):
    q, ref = _claimed_gated(tmp_path, monkeypatch)
    _park_in_sidecar(q, ref, pending_answer={"state": "routing"}, last_answer_gen=3)

    item = q.get(ref)
    assert item["status"] == "awaiting_answer" and item["issue_status"] == "in_progress"
    assert item["parked"]["worker_id"] == "w1" and item["last_answer_gen"] == 3
    assert item["pending_answer"] == {"state": "routing"}
    for f in ("claimed_by", "claimed_session_id", "claimed_machine", "claimed_at", "claim_proc"):
        assert item.get(f) is None
    assert not item["work_it"]
    assert q.claim_next("w2", project="GHI") is None


def test_no_park_state_changes_nothing(tmp_path, monkeypatch):
    q, ref = _claimed_gated(tmp_path, monkeypatch)
    item = q.get(ref)
    assert item["status"] == "in_progress" and item["claimed_by"] == "w1"
    assert "parked" not in item


def test_park_on_a_closed_issue_is_moot(tmp_path, monkeypatch):
    q, ref = _claimed_gated(tmp_path, monkeypatch)
    _park_in_sidecar(q, ref)
    backend = q._github_backend_for_project("GHI")
    backend.update_status(ref, "closed", reason="x")
    assert q.get(ref)["status"] == "closed"


def test_parked_foreign_or_missing_sidecar_fails_closed(tmp_path, monkeypatch):
    q, ref = _claimed_gated(tmp_path, monkeypatch)
    _park_in_sidecar(q, ref)
    q._github_backend_for_project("GHI").update(ref, pipeline_host="mac")
    item = q.get(ref)
    assert item["pipeline_foreign"] and item["status"] != "awaiting_answer"
    assert not item["work_it"] and "parked" not in item

    q._github_backend_for_project("GHI").update(ref, pipeline_host="her")
    _sidecar(q).unlink()
    item = q.get(ref)
    assert item["pipeline_missing"] and not item["work_it"]


def test_parked_with_corrupt_sidecar_fails_closed(tmp_path, monkeypatch):
    q, ref = _claimed_gated(tmp_path, monkeypatch)
    _park_in_sidecar(q, ref)
    _sidecar(q).write_text("{not json")
    item = q.get(ref)
    assert item["pipeline_missing"] and not item["work_it"]
    assert q.claim_next("w2", project="GHI") is None


def _routing(gen=1):
    return {"gen": gen, "state": "routing", "question": "q?", "answer": "a", "attempts": 0,
            "prior_worker_id": "w1", "prior_session_id": "s1", "state_at": "2026-01-01T00:00:00Z"}


def _parked_routing(tmp_path, monkeypatch):
    q, ref = _claimed_gated(tmp_path, monkeypatch)
    _park_in_sidecar(q, ref, pending_answer=_routing(), last_answer_gen=1)
    return q, ref


def _entry(q):
    (entry,) = json.loads(_sidecar(q).read_text())["items"].values()
    return entry


def test_resume_claim_binds_the_parked_worker_in_the_sidecar(tmp_path, monkeypatch):
    q, ref = _parked_routing(tmp_path, monkeypatch)
    assert q.get(ref)["status"] == "awaiting_answer"
    got = q.resume_claim(ref, 1)
    assert got and got["status"] == "in_progress" and got["claimed_by"] == "w1"
    assert "parked" not in _entry(q) and _entry(q)["pending_answer"]["state"] == "delivering"
    item = q.get(ref)
    assert item["status"] == "in_progress" and item["claimed_by"] == "w1"
    assert item["pending_answer"]["route"] == "resume"


def test_concurrent_resume_has_one_winner(tmp_path, monkeypatch):
    q, ref = _parked_routing(tmp_path, monkeypatch)
    out, barrier = [], threading.Barrier(2)

    def _go():
        barrier.wait()
        out.append(q.resume_claim(ref, 1))
    threads = [threading.Thread(target=_go) for _ in range(2)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    assert sorted(bool(r) for r in out) == [False, True]
    assert _entry(q)["pending_answer"]["state"] == "delivering"


def test_restart_mid_transition_keeps_the_cas_state(tmp_path, monkeypatch):
    q, ref = _parked_routing(tmp_path, monkeypatch)
    assert q.resume_claim(ref, 1)

    import watchtower.queue as fresh_q
    importlib.reload(fresh_q)
    fresh_q._MACHINE_TAG_CACHE = None
    assert fresh_q.resume_claim(ref, 1) is None   # already moved on
    done = fresh_q.pa_transition(ref, 1, "delivering", "delivered", from_status="in_progress")
    assert done and done["pending_answer"]["state"] == "delivered"
    assert fresh_q.pa_transition(ref, 1, "delivering", "delivered",
                                 from_status="in_progress") is None


def test_lost_cas_stops_without_writing(tmp_path, monkeypatch):
    q, ref = _parked_routing(tmp_path, monkeypatch)
    before = _sidecar(q).read_bytes()
    assert q.resume_claim(ref, 2) is None                      # stale gen
    assert q.pa_transition(ref, 1, "queued", "delivered", from_status="awaiting_answer") is None
    assert q.pa_transition(ref, 1, "routing", "handed_off", from_status="in_progress") is None
    assert _sidecar(q).read_bytes() == before


def test_cas_fences_foreign_and_corrupt_sidecar(tmp_path, monkeypatch):
    q, ref = _parked_routing(tmp_path, monkeypatch)
    before = _sidecar(q).read_bytes()
    q._github_backend_for_project("GHI").update(ref, pipeline_host="mac")
    assert q.resume_claim(ref, 1) is None
    q._github_backend_for_project("GHI").update(ref, pipeline_host="her")
    _sidecar(q).write_text("{not json")
    assert q.resume_claim(ref, 1) is None
    assert _sidecar(q).read_text() == "{not json"
    _sidecar(q).write_bytes(before)


def test_handoff_reopens_the_issue_from_awaiting_answer(tmp_path, monkeypatch):
    q, ref = _parked_routing(tmp_path, monkeypatch)
    done = q.pa_transition(ref, 1, "routing", "handed_off", from_status="awaiting_answer",
                           reopen=True, route="pool", reason="worker gone")
    assert done and done["status"] == "open"
    item = q.get(ref)
    assert item["status"] == "open" and not item.get("claimed_by")
    assert item["pending_answer"]["state"] == "handed_off" and "parked" not in item


def _block_parks(q, ref, who="w1"):
    return q.block(ref, session_id=who, question="which db?", origin="worker")


def test_worker_block_parks_a_sidecar_ticket_and_frees_the_worker(tmp_path, monkeypatch):
    q, ref = _claimed_gated(tmp_path, monkeypatch)
    assert _block_parks(q, ref)["status"] == "awaiting_answer"
    item = q.get(ref)
    assert item["status"] == "awaiting_answer" and item["parked"]["worker_id"] == "w1"
    assert item["claimed_by"] is None and item["needs_input"]
    assert q.claim_next("w2", project="GHI") is None
    assert [h["event"] for h in item["history"] if h["event"] == "park"] == ["park"]


def test_answer_routes_through_the_sidecar_and_resumes_the_parked_worker(tmp_path, monkeypatch):
    q, ref = _claimed_gated(tmp_path, monkeypatch)
    _block_parks(q, ref)
    q.answer(ref, "use postgres")
    pa = q.get(ref)["pending_answer"]
    assert pa["state"] == "routing" and pa["answer"] == "use postgres" and pa["gen"] == 1
    assert pa["question"] == "which db?" and pa["prior_worker_id"] == "w1"
    assert q.get(ref)["status"] == "awaiting_answer"

    q.answer(ref, "actually mysql")   # a second answer supersedes: newest gen wins
    assert q.get(ref)["pending_answer"]["gen"] == 2
    assert q.resume_claim(ref, 1) is None
    got = q.resume_claim(ref, 2)
    assert got["status"] == "in_progress" and got["claimed_by"] == "w1"
    assert q.get(ref)["pending_answer"]["state"] == "delivering"


def test_handoff_then_a_fresh_claim_settles_the_answer_as_delivered(tmp_path, monkeypatch):
    q, ref = _claimed_gated(tmp_path, monkeypatch)
    _block_parks(q, ref)
    q.answer(ref, "use postgres")
    assert q.pa_transition(ref, 1, "routing", "handed_off", from_status="awaiting_answer",
                           reopen=True, route="reopen", reason="gone")
    assert q.get(ref)["status"] == "open"
    claimed = q.claim_next("w2", project="GHI")
    assert claimed["ref"] == ref and claimed["claimed_by"] == "w2"
    pa = q.get(ref)["pending_answer"]
    assert pa["state"] == "delivered" and pa["claimed_by"] == "w2" and pa["route"] == "claim"


def test_affinity_reservation_holds_the_ticket_for_the_parked_worker(tmp_path, monkeypatch):
    q, ref = _claimed_gated(tmp_path, monkeypatch)
    _block_parks(q, ref)
    q.answer(ref, "use postgres")
    until = "2999-01-01T00:00:00Z"
    assert q.pa_transition(ref, 1, "routing", "affinity", from_status="awaiting_answer",
                           reopen=True, route="affinity", fields={"affinity_until": until})
    assert q.claim_next("w2", project="GHI") is None
    assert q.count_claimable(project="GHI") == 0
    assert q.claim_next("w1", project="GHI")["ref"] == ref
    assert q.get(ref)["pending_answer"]["state"] == "delivered"


def test_stale_worker_cannot_block_another_workers_claim(tmp_path, monkeypatch):
    q, ref = _claimed_gated(tmp_path, monkeypatch)
    with pytest.raises(ValueError, match="claimed by w1"):
        q.block(ref, session_id="w2", question="mine?", origin="worker")
    assert q.get(ref)["status"] == "in_progress" and not q.get(ref)["needs_input"]


def test_a_block_without_a_pipeline_keeps_the_legacy_hold(tmp_path, monkeypatch):
    _state, _config, q, ref = _setup(tmp_path, monkeypatch)
    assert q.claim_next("w1", project="GHI")["ref"] == ref
    _block_parks(q, ref)
    item = q.get(ref)
    assert item["status"] == "in_progress" and item["needs_input"] and not item.get("parked")


def test_liveness_classifies_the_parked_github_lifecycle(tmp_path, monkeypatch):
    """WT-41: awaiting_answer x github is a table row, not UNREACHABLE."""
    from watchtower import liveness as lv
    q, ref = _claimed_gated(tmp_path, monkeypatch)

    def row():
        item = q.get(ref)
        st = lv.project(item)
        assert st.backend == "github"
        return st, lv.classify(st).id
    assert row()[1] == "work.claimed"
    _block_parks(q, ref)
    st, rid = row()
    assert st.status == "awaiting_answer" and st.parked and rid == "answer.await_human"
    q.answer(ref, "use postgres")
    st, rid = row()
    assert st.answer == "routing" and not st.claimed and rid == "answer.routing"
    assert lv.prove(q.get(ref), lv.classify(st))["kind"] == "lease"
    assert q.resume_claim(ref, 1)
    st, rid = row()
    assert st.answer == "delivering" and rid == "answer.delivering"
    ids = {u.id for u in lv.UNREACHABLE}
    assert not ids & {"github_parked", "github_answer"}

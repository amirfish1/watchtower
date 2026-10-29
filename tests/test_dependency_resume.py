"""WT-37: a ticket blocked ``--wait-on dependency --wait-ref B`` is answered
by the reconciler (attributed to the system) once B closes cleanly."""

from __future__ import annotations


import importlib

import pytest

from tests.test_park_blocks import SID, wt  # noqa: F401 - fixture


def _close(wt, ref, unresolved=None):
    wt.q.claim_by_ref(ref, "wdep")
    res = {"summary": "dep landed\n- x.py"}
    if unresolved:
        res["unresolved"] = unresolved
    return wt.q.update_status(ref, "closed", session_id="wdep", resolution=res)


def _waiting(wt, wait_on="dependency", wait_ref="", origin="worker"):
    dep = wt.q.enqueue(project="PK", note="dependency", source="test")
    a = wt.q.enqueue(project="PK", note="dependent", source="test")
    wt.q.claim_by_ref(a["ref"], "w1", session_uuid=SID)
    kw = {"wait_on": wait_on}
    if wait_on == "dependency":
        kw["wait_ref"] = wait_ref or dep["ref"]
    elif wait_on == "owner":
        kw["wait_actor"] = "amir"
    elif wait_on == "external":
        kw["wait_actor"] = "vendor"
    wt.q.block(a["ref"], session_id="w1", question="waiting on dep",
               progress="half", origin=origin, **kw)
    return a["ref"], dep["ref"]


@pytest.fixture()
def quiet(wt, monkeypatch):
    monkeypatch.setattr(wt.answers, "worker_alive", lambda wid: wid == "w1")
    monkeypatch.setattr(wt.answers, "_wake", lambda *a, **k: None)
    return wt


def test_dependency_close_answers_and_routes(quiet):
    wt = quiet
    a, dep = _waiting(wt)
    assert wt.answers.resume_dependency_waits() == []  # dep still open
    _close(wt, dep)
    assert wt.answers.resume_dependency_waits() == [f"{a}:{dep}"]
    cur = wt.q.get(a)
    assert not cur.get("needs_input")
    pa = cur["pending_answer"]
    assert f"dependency {dep} closed" in pa["answer"]
    assert "dep landed" in pa["answer"]
    assert pa["state"] != "routing"  # routed to the parked session
    ans = [h for h in cur["history"] if h.get("event") == "answer"]
    assert ans and ans[-1]["by"]["kind"] == "system"


def test_second_tick_is_noop(quiet):
    wt = quiet
    a, dep = _waiting(wt)
    _close(wt, dep)
    assert wt.answers.resume_dependency_waits()
    before = len(wt.q.get(a)["history"])
    assert wt.answers.resume_dependency_waits() == []
    assert len(wt.q.get(a)["history"]) == before


@pytest.mark.parametrize("wait_on", ["owner", "external"])
def test_owner_and_external_waits_untouched(quiet, wait_on):
    wt = quiet
    a, dep = _waiting(wt, wait_on=wait_on)
    _close(wt, dep)
    assert wt.answers.resume_dependency_waits() == []
    assert wt.q.get(a).get("needs_input")


def test_stuck_dependency_stays_human(quiet):
    wt = quiet
    a, dep = _waiting(wt)
    _close(wt, dep, unresolved=["OPS-1 left over"])
    assert wt.answers.resume_dependency_waits() == []
    assert wt.q.get(a).get("needs_input")


def test_legacy_in_progress_block_is_answered(quiet):
    """The WT-37 incident shape: a block that kept the ticket in_progress."""
    wt = quiet
    a, dep = _waiting(wt, origin="plan_gate")
    assert wt.q.get(a)["status"] == "in_progress"
    _close(wt, dep)
    assert wt.answers.resume_dependency_waits() == [f"{a}:{dep}"]
    assert not wt.q.get(a).get("needs_input")


def test_cas_on_block_generation(quiet):
    wt = quiet
    a, dep = _waiting(wt)
    _close(wt, dep)
    assert wt.q.answer(a, "x", expect_wait_block_at="1999-01-01T00:00:00Z") is None
    assert wt.q.get(a).get("needs_input")
    assert wt.cli.deliver_dependency_answer(a, "x", "stale") is None
    assert wt.q.get(a).get("needs_input")


def test_reconcile_runs_the_pass(quiet):
    wt = quiet
    a, dep = _waiting(wt)
    _close(wt, dep)
    import watchtower.workers as workers
    res = workers.reconcile_once(supervise_stages=False)
    assert res.get("dependency_resumed") == [f"{a}:{dep}"]


def test_github_dependency_wait_is_answered(tmp_path, monkeypatch):
    """The incident queue was GitHub-backed (BECKY-2968 on BECKY-2984)."""
    from tests.test_github_backend import (
        _drainable, _install_fake_gh, _reload_isolated,
    )
    _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _drainable(config)
    import watchtower.answers as answers
    import watchtower.cli as cli
    importlib.reload(answers)
    importlib.reload(cli)
    dep = q.enqueue(project="GHI", note="dependency")
    a = q.enqueue(project="GHI", note="dependent")
    q.claim_by_ref(a["ref"], "worker-1")
    q.block(a["ref"], session_id="worker-1", question="wait",
            wait_on="dependency", wait_ref=dep["ref"])
    assert answers.resume_dependency_waits() == []
    q.claim_by_ref(dep["ref"], "worker-2")
    q.update_status(dep["ref"], "closed", session_id="worker-2",
                    resolution={"summary": "landed"})
    assert answers.resume_dependency_waits() == [f"{a['ref']}:{dep['ref']}"]
    cur = q.get(a["ref"])
    assert not cur.get("needs_input")
    ans = [h for h in cur["history"] if h.get("event") == "answer"]
    assert ans and ans[-1]["kind"] == "system"  # GitHub rows are flat
    assert answers.resume_dependency_waits() == []


def test_local_ref_resolves_only_on_the_host_that_recorded_the_wait(tmp_path, monkeypatch):
    """OPS-230: local queues are per host, so a shared GitHub ticket waiting on
    local OPS-202 must not be answered by another host's unrelated OPS-202 --
    with the host stamped on the wait, or only on the block event (legacy)."""
    from tests.test_github_backend import (
        _drainable, _install_fake_gh, _reload_isolated,
    )
    _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _drainable(config)
    import watchtower.answers as answers
    import watchtower.cli as cli
    importlib.reload(answers)
    importlib.reload(cli)
    dep = q.enqueue(project="PK", note="local dependency on her", source="test")
    q.claim_by_ref(dep["ref"], "worker-2")
    q.update_status(dep["ref"], "closed", session_id="worker-2",
                    resolution={"summary": "landed"})
    for legacy in (False, True):
        a = q.enqueue(project="GHI", note="shared dependent")
        monkeypatch.setattr(q, "_MACHINE_TAG_CACHE", "her")
        q.claim_by_ref(a["ref"], "worker-1")
        q.block(a["ref"], session_id="worker-1", question="wait",
                wait_on="dependency", wait_ref=dep["ref"])
        if legacy:
            q._github_backend_for_project("GHI").update(
                a["ref"], wait={k: v for k, v in q.get(a["ref"])["wait"].items() if k != "host"})
        monkeypatch.setattr(q, "_MACHINE_TAG_CACHE", "ami")   # another host, same ref
        assert answers.resume_dependency_waits() == []
        assert q.get(a["ref"]).get("needs_input")
        monkeypatch.setattr(q, "_MACHINE_TAG_CACHE", "her")
        assert answers.resume_dependency_waits() == [f"{a['ref']}:{dep['ref']}"]
        assert not q.get(a["ref"]).get("needs_input")

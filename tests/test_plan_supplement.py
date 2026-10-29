"""OPS-232: an independent review of a supplement filed after the plan was
accepted. The accepted plan, its review history, the claim and the stage
gating stay exactly as they were; one contract on a local queue and on a
GitHub-backed queue (sidecar)."""
import pytest

from tests.test_github_backend import (  # noqa: F401  (autouse fixture)
    _drainable,
    _install_fake_gh,
    _reload_isolated,
    restore_watchtower_modules,
)


def _local(tmp_path, monkeypatch):
    _config, q = _reload_isolated(tmp_path, monkeypatch)
    return q, q.enqueue(project="LOC", note="gated work", text="body", gates=["plan"])["ref"]


def _github(tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_MACHINE", "her")
    _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    q._MACHINE_TAG_CACHE = None
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _drainable(config)
    ref = q.enqueue(project="GHI", note="gated work", text="body")["ref"]
    q.update(ref, gates=["plan"])
    return q, ref


@pytest.fixture(params=["local", "github"])
def accepted(request, tmp_path, monkeypatch):
    q, ref = (_local if request.param == "local" else _github)(tmp_path, monkeypatch)
    q.plan_start(ref)
    q.plan_submit(ref, "1. the accepted plan")
    q.plan_verdict(ref, True, "sound", by="reviewer-v1")
    assert q.claim_by_ref(ref, "builder-1")
    return q, ref


def test_supplement_is_reviewed_independently_and_never_reopens_the_plan(accepted):
    q, ref = accepted
    from watchtower import stages
    before = q.get(ref)
    q.plan_supplement_submit(ref, "2. addendum", by="builder-1")
    q.plan_supplement_submit(ref, "2. addendum", by="builder-1")   # idempotent
    it = q.get(ref)
    plan = it["plan"]
    assert len(plan["supplements"]) == 1 and plan["supplements"][0]["status"] == "reviewing"
    assert plan["status"] == "accepted" and plan["accepted_version"] == 1
    assert plan["reviews"] == before["plan"]["reviews"]             # v1 audit kept
    assert it["status"] == "in_progress" and it["claimed_by"] == "builder-1"
    assert "2. addendum" not in q.render_plan(it)                   # not binding yet
    # A human block on the build does not hold the review-only stage.
    q.block(ref, "", question="waiting", kind="input")
    assert [(d["role"], d["key"]) for d in stages.desired([q.get(ref)])] == \
        [("plan_reviewer", "supplement:s1")]

    for who in ("builder-1", ""):                                  # no self-verdict
        with pytest.raises(ValueError):
            q.plan_supplement_verdict(ref, True, "ok", by=who)
    with pytest.raises(ValueError):
        q.plan_supplement_verdict(ref, True, "ok", by="rev-2", n=2)
    q.plan_supplement_verdict(ref, False, "wrong lock order", by="rev-2", n=1)
    it = q.get(ref)
    assert it["plan"]["supplements"][0]["status"] == "rejected"
    assert it["plan"]["status"] == "accepted" and "2. addendum" not in q.render_plan(it)
    assert stages.desired([it]) == []

    q.plan_supplement_submit(ref, "2. addendum, revised", by="builder-1")   # revision
    q.plan_supplement_verdict(ref, True, "sound now", by="rev-2")
    it = q.get(ref)
    assert [s["status"] for s in it["plan"]["supplements"]] == ["rejected", "accepted"]
    assert "2. addendum, revised" in q.render_plan(it)
    assert it["plan"]["accepted_version"] == 1 and it["claimed_by"] == "builder-1"
    events = [h["event"] for h in it["history"]]
    assert events.count("plan_supplement") == 2 and events.count("plan_supplement_review") == 2


def test_supplement_refused_without_accepted_plan_or_from_a_non_claimant(accepted):
    q, ref = accepted
    with pytest.raises(ValueError, match="claimant"):
        q.plan_supplement_submit(ref, "x", by="someone-else")
    with pytest.raises(ValueError):
        q.plan_supplement_verdict(ref, True, "nothing open", by="rev-2")
    q.plan_supplement_submit(ref, "a", by="builder-1")
    with pytest.raises(ValueError, match="under review"):
        q.plan_supplement_submit(ref, "b", by="builder-1")


def test_supplement_needs_an_accepted_plan(tmp_path, monkeypatch):
    q, ref = _local(tmp_path, monkeypatch)
    q.plan_start(ref)
    with pytest.raises(ValueError, match="no accepted plan"):
        q.plan_supplement_submit(ref, "x", by="w")

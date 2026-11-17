"""WT-84 migration preserves real gates and refuses stale or ungated edits."""
import pytest

from tests.test_owner_view import _load, _ticket
from watchtower import integration_migration as migration


@pytest.fixture(params=["file", "github"])
def env(request, tmp_path, monkeypatch):
    loaded = _load(tmp_path, monkeypatch, request.param)
    monkeypatch.setattr(migration, "q", loaded[0])
    return loaded


def legacy(env):
    q, _cli, _a, _ov, name = env
    ref = _ticket(q, name)
    reason = "[awaits integration abcdef123: owner reviewer; path review PR] original symptom open"
    return ref, q.defer(ref, "w1", "+1d", reason)


def test_migration_requires_a_real_gate_and_exact_snapshot(env):
    q, *_ = env
    ref, before = legacy(env)
    with pytest.raises(ValueError, match="real block or dependency"):
        migration.retire(ref, expected_reason=before["defer_reason"], worker="migration")
    assert q.get(ref) == before
    q.block(ref, "reviewer", "Review existing source", wait_on="worker", wait_actor="reviewer")
    blocked = q.get(ref)
    with pytest.raises(ValueError, match="changed"):
        migration.retire(ref, expected_reason=before["defer_reason"] + " stale", worker="migration")
    assert q.get(ref) == blocked


@pytest.mark.parametrize("wait_on", ["owner", "worker"])
def test_migration_clears_only_timer_preserving_question_claim_and_gate(env, wait_on):
    q, *_ = env
    ref, before = legacy(env)
    extra = {"decide_by": "none", "consequence": "Original acceptance remains open"} if wait_on == "owner" else {}
    q.block(ref, "reviewer", "[Q1:existing-check] Existing acceptance question",
            wait_on=wait_on, wait_actor="reviewer", wait_extra=extra)
    blocked = q.get(ref)
    after = migration.retire(ref, expected_reason=before["defer_reason"], worker="migration")
    assert not after.get("defer_reason") and not after.get("deferred_until")
    for key in ("status", "needs_input", "block_question", "wait", "claimed_by", "claimed_session_id", "blocked_by"):
        assert after.get(key) == blocked.get(key)
    receipt = next(h for h in after["history"] if h["event"] == "integration_defer_retired")
    assert receipt["previous_defer"] == {k: before[k] for k in ("defer_reason", "deferred_until")}
    with pytest.raises(ValueError, match="changed"):
        migration.retire(ref, expected_reason=before["defer_reason"], worker="migration")

"""CCC-owned fallback/profile config stays dynamic and respects queue pins."""
import json

import pytest


def save_policy(wt_env, **settings):
    wt_env.config.CCC_SPAWN_DEFAULTS_FILE.write_text(json.dumps(settings))


CODEX = {"engine": "codex", "model": "gpt-6.1-sol", "effort": "medium"}
CLAUDE = {"engine": "claude", "model": "claude-sonnet-5", "effort": "high"}


def test_queue_default_inheritance_and_explicit_overrides(wt_env):
    c = wt_env.config
    assert c.worker_fallback_policy() is None
    assert not c.fallback_to_default_worker("Q")
    save_policy(wt_env, worker_fallback={"enabled": True, "models": [CODEX]})
    assert c.fallback_to_default_worker("Q")
    c.set_fallback_to_default_worker("Q", False)
    assert not c.fallback_to_default_worker("Q")
    c.set_fallback_to_default_worker("Q", None)
    assert c.fallback_to_default_worker("Q")
    assert "fallback_to_default_worker" not in c._queue_entry("Q")
    save_policy(wt_env, worker_fallback={"enabled": False, "models": [CODEX]})
    assert not c.fallback_to_default_worker("Q")
    c.set_fallback_to_default_worker("Q", True)
    assert c.fallback_to_default_worker("Q")


def test_configured_order_and_effort_are_used_without_rewriting_pins(wt_env, monkeypatch):
    c = wt_env.config
    monkeypatch.setattr(wt_env.workers, "engine_available", lambda e: True)
    c.set_engine("Q", "kimi")
    c.set_model("Q", "kimi-code/k3")
    c.set_fallback_to_default_worker("Q", True)
    save_policy(wt_env, worker_fallback={"enabled": True, "models": [CODEX, CLAUDE]})
    assert c.fallback_engine("kimi") == "codex"
    assert c.fallback_model("codex") == "gpt-6.1-sol"
    assert c.fallback_effort("Q", "codex") == "medium"
    c.set_effort("Q", "high")
    assert c.fallback_effort("Q", "codex") == "high"
    save_policy(wt_env, worker_fallback={"enabled": True, "models": [CLAUDE, CODEX]})
    assert c.fallback_engine("kimi") == "claude"
    assert c.fallback_engine("kimi", excluded={"claude"}) == "codex"
    assert c.engine("Q") == "kimi"
    assert c.model("Q") == "kimi-code/k3"


def test_explicit_empty_or_invalid_routes_do_not_restore_legacy_candidates(wt_env, monkeypatch):
    c = wt_env.config
    monkeypatch.setattr(wt_env.workers, "engine_available", lambda e: True)
    for routes in [[], [CODEX, CODEX], [{"engine": "codex", "model": "made-up", "effort": "medium"}]]:
        save_policy(wt_env, worker_fallback={"enabled": True, "models": routes})
        assert c.fallback_engine("claude") == ""


def test_profile_resolution_is_dynamic_and_fails_closed(wt_env):
    c = wt_env.config
    with pytest.raises(ValueError, match="configured"):
        c.model_profile("deep")
    save_policy(wt_env, model_profiles={"deep": {"models": [CLAUDE, CODEX]}})
    assert c.model_profile("deep")["models"] == [CLAUDE, CODEX]
    save_policy(wt_env, model_profiles={"deep": {"models": [CODEX]}})
    assert c.model_profile("deep")["models"] == [CODEX]
    with pytest.raises(ValueError, match="unknown"):
        c.model_profile("other")
    save_policy(wt_env, model_profiles={"deep": {"models": []}})
    with pytest.raises(ValueError, match="no configured"):
        c.model_profile("deep")


def test_reconcile_uses_configured_fallback_model_and_effort(wt_env, monkeypatch):
    c = wt_env.config
    c.set_engine("Q", "claude")
    c.set_auto_drain("Q", True)
    wt_env.queue.enqueue(project="Q", note="work")
    save_policy(wt_env, worker_fallback={"enabled": True, "models": [CODEX]})
    monkeypatch.setattr(wt_env.workers, "engine_available", lambda e: True)
    calls = []
    def spawn(queue, n=1, engine="claude", launch_failures=None, **kwargs):
        calls.append((engine, kwargs.get("model"), kwargs.get("effort")))
        if engine == "claude":
            launch_failures.append({"reason": "engine usage limit", "_spawn_index": 0})
            return []
        return [{"worker_id": "fallback", "queue": queue, "engine": engine}]
    monkeypatch.setattr(wt_env.workers, "spawn_workers", spawn)
    wt_env.workers.reconcile_once()
    assert calls == [("claude", "", None), ("codex", "gpt-6.1-sol", "medium")]


def test_profiles_reject_engines_without_required_output_and_tool_contract(wt_env):
    save_policy(wt_env, model_profiles={"deep": {"models": [
        {"engine": "kimi", "model": "kimi-code/k3", "effort": ""}
    ]}})
    with pytest.raises(ValueError, match="tool-free structured"):
        wt_env.config.model_profile("deep")

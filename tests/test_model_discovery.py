import json

import pytest

from watchtower import config


@pytest.fixture
def sources(tmp_path, monkeypatch):
    codex = tmp_path / "codex.json"
    codex.write_text(json.dumps({"models": [
        {"slug": "gpt-6.1-sol", "visibility": "list", "priority": 1,
         "supported_reasoning_levels": [{"effort": "low"}, {"effort": "ultra"}]},
        {"slug": "gpt-6-astra", "visibility": "list", "priority": 2,
         "supported_reasoning_levels": [{"effort": "low"}]},
        {"slug": "hidden-one", "visibility": "hide", "priority": 3},
    ]}))
    claude = tmp_path / "claude.json"
    claude.write_text(json.dumps({"records": [{"id": "fable-5-1", "released_at": "2026-09-01"}]}))
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({"blocked_models": ["gpt-6-astra"]}))
    monkeypatch.setattr(config, "CODEX_MODELS_CACHE", codex)
    monkeypatch.setattr(config, "CLAUDE_MODELS_FILE", claude)
    monkeypatch.setattr(config, "CCC_MODEL_POLICY_FILE", policy)


def test_discovered_models_approved(sources):
    assert config.is_approved_model("codex", "gpt-6.1-sol")
    assert config.approved_efforts("codex", "gpt-6.1-sol") == ("low",)
    assert not config.is_approved_model("codex", "hidden-one")
    assert config.is_approved_model("claude", "claude-fable-5-1")
    assert config.is_approved_model("codex", "gpt-5.5")  # fallback entry


def test_blocked_model_still_refused(sources):
    assert not config.is_approved_model("codex", "gpt-6-astra")


def test_new_models_ranked(sources):
    tiers = config.model_floor_tiers()
    assert "claude-fable-5-1" in tiers
    assert "gpt-6.1-sol" not in tiers  # codex ids stay unranked


def test_missing_sources_fall_back(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CODEX_MODELS_CACHE", tmp_path / "nope.json")
    assert config.is_approved_model("codex", "gpt-5.6-sol")

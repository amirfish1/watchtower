"""Model policy: the deny-list shared with CCC that queue workers must honor.

Regression for 2026-09-05: a 2.5x-priced Codex model was pinned on a queue
through CCC's queue editor (which calls config.set_model directly and so
skipped the CLI's approved-list check) and was also inherited by every
codex queue without a pin, via CCC's spawn defaults. Three layers here:
set_model refuses a blocked pin, model() substitutes an allowed fallback for
an inherited or pre-existing blocked value, and build_drain_command never
emits a blocked --model.
"""

import json

import pytest

import watchtower.config as config
import watchtower.workers as workers


@pytest.fixture
def policy_env(tmp_path, monkeypatch):
    policy = tmp_path / "model-policy.json"
    defaults = tmp_path / "spawn-defaults.json"
    monkeypatch.setattr(config, "CONFIG_FILE", tmp_path / "queue-config.json")
    monkeypatch.setattr(config, "CCC_MODEL_POLICY_FILE", policy)
    monkeypatch.setattr(config, "CCC_SPAWN_DEFAULTS_FILE", defaults)
    monkeypatch.delenv("WATCHTOWER_BLOCKED_MODELS", raising=False)
    defaults.write_text(json.dumps({
        "models": {"codex": "gpt-6-astra", "claude": "sonnet-5"},
        "worker_engine": "kimi",
        "worker_model": "kimi-code/kimi-for-coding",
    }))

    def block(*models):
        policy.write_text(json.dumps({"blocked_models": list(models)}))

    return block


def test_no_policy_file_blocks_nothing(policy_env):
    assert not config.is_blocked_model("gpt-6-astra")
    config.set_engine("Q", "codex")
    config.set_model("Q", "gpt-6-astra")
    assert config.model("Q") == "gpt-6-astra"


def test_set_model_refuses_a_blocked_pin(policy_env):
    policy_env("gpt-6-astra")
    config.set_engine("Q", "codex")
    with pytest.raises(ValueError, match="blocked by model policy"):
        config.set_model("Q", "GPT-6-Astra")
    assert "model" not in config._queue_entry("Q")


def test_set_model_confirm_blocked_allows_a_deliberate_pin(policy_env):
    # 2026-09-06: a human confirming the pick in CCC's queue-config dialog
    # (or `wt config -q ... --confirm-blocked`) can still pin it -- the
    # automatic worker-dispatch path (build_drain_command / model()) never
    # passes this, so an unattended queue still can't silently spawn it.
    policy_env("gpt-6-astra")
    config.set_engine("Q", "codex")
    config.set_model("Q", "gpt-6-astra", confirm_blocked=True)
    assert config._queue_entry("Q")["model"] == "gpt-6-astra"


def test_env_var_unions_with_policy_file(policy_env, monkeypatch):
    policy_env("gpt-6-astra")
    monkeypatch.setenv("WATCHTOWER_BLOCKED_MODELS", "gpt-5.5, Claude-Opus-5")
    assert config.is_blocked_model("gpt-5.5")
    assert config.is_blocked_model("claude-opus-5")
    assert config.is_blocked_model("gpt-6-astra")
    assert not config.is_blocked_model("gpt-5.6-sol")


def test_inherited_ccc_default_is_substituted(policy_env):
    config.set_engine("Q", "codex")
    assert config.model("Q") == "gpt-6-astra"
    policy_env("gpt-6-astra")
    resolved = config.model("Q")
    assert resolved and resolved != "gpt-6-astra"
    assert resolved in config.approved_models("codex")


def test_pre_existing_blocked_pin_is_substituted_at_read_time(policy_env):
    config.set_engine("Q", "codex")
    config.set_model("Q", "gpt-6-astra")
    policy_env("gpt-6-astra")
    assert config.model("Q") != "gpt-6-astra"


def test_is_approved_model_rejects_blocked_even_if_in_catalog(policy_env):
    assert config.is_approved_model("codex", "gpt-5.6-sol")
    policy_env("gpt-5.6-sol")
    assert not config.is_approved_model("codex", "gpt-5.6-sol")
    assert config.is_approved_model("codex", "")


def test_build_drain_command_never_emits_a_blocked_model(policy_env, capsys):
    policy_env("gpt-6-astra")
    argv = workers.build_drain_command("Q", "codex", "w1", model="gpt-6-astra", goal="x")
    assert "gpt-6-astra" not in argv
    if "--model" in argv:
        assert argv[argv.index("--model") + 1] in config.approved_models("codex")
    assert "blocked by model policy" in capsys.readouterr().err

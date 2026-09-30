"""WT-14: per-queue role models resolved in one place."""
import pytest

from watchtower import config, roles


@pytest.fixture
def q(tmp_path, monkeypatch):
    from watchtower import workers
    monkeypatch.setattr(config, "CONFIG_FILE", tmp_path / "config.json")
    monkeypatch.setattr(config, "CCC_MODEL_POLICY_FILE", tmp_path / "policy.json")
    monkeypatch.setattr(config, "CCC_SPAWN_DEFAULTS_FILE", tmp_path / "spawn.json")
    monkeypatch.setattr(workers, "engine_available", lambda e: e in ("codex", "claude"))
    config.set_engine("RQ", "claude")
    config.set_model("RQ", "claude-sonnet-5-5")
    return "RQ"


def test_defaults(q):
    t = roles.role_table(q)
    assert t["builder"] == {"engine": "claude", "model": "claude-sonnet-5-5", "source": "queue"}
    assert t["planner"] == {"engine": "claude", "model": "claude-opus-5-5", "source": "default"}
    assert t["plan_reviewer"]["engine"] == "codex" and t["plan_reviewer"]["source"] == "default"
    assert t["verifier"]["engine"] == "codex"


def test_queue_settings_and_ticket_override_precedence(q):
    config.set_role(q, "planner", "codex", "gpt-5.5")
    config.set_role(q, "plan_reviewer", "claude", "claude-opus-5")
    assert roles.effective_role_model(q, None, "planner") == ("codex", "gpt-5.5", "queue")
    assert roles.effective_role_model(q, None, "plan_reviewer") == ("claude", "claude-opus-5", "queue")
    ticket = {"planner_model": "claude-opus-5-5", "model_floor": "gpt-5.6"}
    assert roles.effective_role_model(q, ticket, "planner") == ("claude", "claude-opus-5-5", "ticket")
    assert roles.effective_role_model(q, ticket, "builder") == ("codex", "gpt-5.6", "ticket")


def test_set_role_validates_against_catalog(q):
    with pytest.raises(ValueError):
        config.set_role(q, "planner", "codex", "not-a-model")
    with pytest.raises(ValueError):
        config.set_role(q, "planner", "nope", None)
    with pytest.raises(ValueError):
        config.set_role(q, "builder", "codex", None)


def test_policy_fallback_source(q, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_BLOCKED_MODELS", "claude-sonnet-5-5")
    assert roles.effective_role_model(q, None, "builder")[2] == "policy-fallback"


def test_cli_config_shows_four_roles_and_status_json(q, capsys):
    import json
    import watchtower.cli as cli
    assert cli.main(["config", "-q", q, "--planner-engine", "codex", "--planner-model", "gpt-5.5"]) == 0
    capsys.readouterr()
    assert cli.main(["config", "-q", q]) == 0
    out = capsys.readouterr().out
    for role in roles.ROLES:
        assert role in out
    assert "[queue]" in out and "[default]" in out


def test_ticket_role_flags_on_add_and_edit(wt_env, run_cli):
    res = run_cli("add", "-q", "RQ2", "--title", "t", "--note", "n",
                  "--planner-model", "claude-opus-5-5", "--builder-model", "gpt-5.6")
    assert res.code == 0, res.err
    ref = res.out.split()[1]
    it = wt_env.queue.get(ref)
    assert it["planner_model"] == "claude-opus-5-5" and it["model_floor"] == "gpt-5.6"
    res = run_cli("edit", ref, "--verifier-model", "gpt-5.5", "--planner-model", "")
    assert res.code == 0, res.err
    it = wt_env.queue.get(ref)
    assert it["verifier_model"] == "gpt-5.5" and not it.get("planner_model")
    assert run_cli("edit", ref, "--verifier-model", "bogus").code == 1

"""`wt models migrate` / `unpin` (WT-7): resolution, dry-run, approval refusal,
floor warning, default rewrite."""
import argparse
import json

import pytest

import watchtower.cli as cli
import watchtower.config as config
import watchtower.models_migrate as mm


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CONFIG_FILE", tmp_path / "queue-config.json")
    defaults = tmp_path / "spawn-defaults.json"
    defaults.write_text(json.dumps({"worker_engine": "claude", "worker_model": "sonnet-5",
                                    "models": {"claude": "sonnet-5"}}))
    monkeypatch.setattr(config, "CCC_SPAWN_DEFAULTS_FILE", defaults)
    monkeypatch.setattr(config, "CCC_MODEL_POLICY_FILE", tmp_path / "policy.json")
    monkeypatch.delenv("WATCHTOWER_BLOCKED_MODELS", raising=False)
    items = []
    monkeypatch.setattr(mm, "_counts_and_items", lambda: {"B": items})
    for q, m in (("A", "claude-sonnet-5"), ("B", "claude-opus-5-5"), ("C", "")):
        config.set_engine(q, "claude")
        config.set_model(q, m)
    return type("E", (), {"defaults": defaults, "items": items, "tmp": tmp_path})


def _ns(**kw):
    base = dict(from_model="claude-sonnet-5", to_model="claude-sonnet-5-5", queues=None,
                to_engine=None, include_default=False, apply=False, json=False)
    base.update(kw)
    return argparse.Namespace(**base)


def test_plan_lists_pinned_and_inherited_queues(env):
    plan = mm.plan_migrate("sonnet-5", "sonnet-5-5")
    rows = {r["queue"]: r for r in plan["rows"]}
    assert set(rows) == {"A", "C"}            # B is on opus-5-5
    assert rows["A"]["source"] == "pinned" and rows["C"]["source"] == "default"


def test_dry_run_writes_nothing(env, capsys):
    assert cli.cmd_models_migrate(_ns()) == 0
    out = capsys.readouterr().out
    assert "dry run" in out and "A " in out
    assert config.raw_model("A") == "claude-sonnet-5"


def test_apply_changes_only_pinned_matches(env):
    assert cli.cmd_models_migrate(_ns(apply=True)) == 0
    assert config.raw_model("A") == "claude-sonnet-5-5"
    assert config.raw_model("B") == "claude-opus-5-5"
    assert "model" not in config.get_queue_config("C")     # inherited stays unpinned
    assert json.loads(env.defaults.read_text())["worker_model"] == "sonnet-5"


def test_include_default_rewrites_keys_keeping_short_form(env, capsys):
    assert cli.cmd_models_migrate(_ns(apply=True, include_default=True)) == 0
    data = json.loads(env.defaults.read_text())
    assert data["worker_model"] == "sonnet-5-5" and data["models"]["claude"] == "sonnet-5-5"
    assert config.raw_model("C") == "claude-sonnet-5-5"
    assert "worker_model" in capsys.readouterr().out


def test_unapproved_target_refused(env, capsys):
    assert cli.cmd_models_migrate(_ns(to_model="claude-nonsense-9", apply=True)) == 1
    assert "not an approved model" in capsys.readouterr().err
    assert config.raw_model("A") == "claude-sonnet-5"


def test_cross_engine_needs_engine_flag(env, capsys):
    assert cli.cmd_models_migrate(_ns(to_model="gpt-5.5", apply=True)) == 1
    assert config.raw_model("A") == "claude-sonnet-5"
    assert "cross-engine" in capsys.readouterr().err


def test_floor_warning_for_open_tickets(env):
    env.items.append({"ref": "B-9", "status": "open", "model_floor": "claude-opus-5-5"})
    plan = mm.plan_migrate("claude-opus-5-5", "claude-sonnet-5")
    assert any("B-9" in w and "BLOCK" in w for w in plan["warnings"])


def test_blocked_queue_model_warns(env):
    (env.tmp / "policy.json").write_text(json.dumps({"blocked_models": ["claude-opus-5-5"]}))
    plan = mm.plan_migrate("claude-sonnet-5", "claude-sonnet-5-5")
    assert any(w.startswith("B:") and "blocked" in w for w in plan["warnings"])


def test_unpin_clears_explicit_model(env):
    ns = argparse.Namespace(queues=None, all_matching="claude-opus-5-5", apply=True, json=False)
    assert cli.cmd_models_unpin(ns) == 0
    assert "model" not in config.get_queue_config("B")
    assert config.raw_model("B") == "claude-sonnet-5"       # follows the default now
    assert config.get_queue_config("A")["model"] == "claude-sonnet-5"


def test_unpin_requires_selector(env):
    assert cli.cmd_models_unpin(argparse.Namespace(queues=None, all_matching=None,
                                                   apply=False, json=False)) == 2

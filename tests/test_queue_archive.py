"""`wt queue archive|unarchive|ls` (WT-8): refusal with live tickets, hiding
from status / models migrate, history kept."""
import argparse
import json

import pytest

import watchtower.cli as cli
import watchtower.config as config
import watchtower.health as health
import watchtower.models_migrate as mm


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setattr(config, "CONFIG_FILE", tmp_path / "queue-config.json")
    monkeypatch.setattr(config, "CCC_SPAWN_DEFAULTS_FILE", tmp_path / "spawn-defaults.json")
    monkeypatch.setattr(config, "CCC_MODEL_POLICY_FILE", tmp_path / "policy.json")
    items = []
    monkeypatch.setattr(cli.q, "list_items", lambda **kw: [
        i for i in items if not kw.get("project") or i["project"] == kw["project"]])
    monkeypatch.setattr(mm, "_counts_and_items", lambda: {})
    for name in ("LIVE", "DEAD"):
        config.set_engine(name, "claude")
        config.set_model(name, "claude-sonnet-5")
    config.set_auto_drain("DEAD", True)
    return items


def _ns(**kw):
    base = dict(name="DEAD", apply=False, force=False, json=False, archived=False, all=False)
    base.update(kw)
    return argparse.Namespace(**base)


def _item(ref, status, project="DEAD"):
    return {"ref": ref, "project": project, "status": status, "title": "t",
            "created_at": "2026-01-01T00:00:00Z"}


def test_dry_run_changes_nothing(env, capsys):
    assert cli.cmd_queue_archive(_ns()) == 0
    assert "would archive" in capsys.readouterr().out
    assert not config.is_archived("DEAD") and config.auto_drain("DEAD")


def test_apply_archives_and_disables_drain(env):
    assert cli.cmd_queue_archive(_ns(apply=True)) == 0
    assert config.is_archived("DEAD") and not config.auto_drain("DEAD")
    assert "DEAD" not in config.all_queues()
    assert "DEAD" in config.all_queues(include_archived=True)


def test_refuses_with_open_tickets_unless_force(env, capsys):
    env.append(_item("DEAD-1", "open"))
    assert cli.cmd_queue_archive(_ns(apply=True)) == 1
    assert "DEAD-1" in capsys.readouterr().err and not config.is_archived("DEAD")
    assert cli.cmd_queue_archive(_ns(apply=True, force=True)) == 0
    assert config.is_archived("DEAD")


def test_closed_tickets_do_not_block(env):
    env.append(_item("DEAD-1", "closed"))
    assert cli.cmd_queue_archive(_ns(apply=True)) == 0


def test_unknown_queue(env):
    assert cli.cmd_queue_archive(_ns(name="NOPE", apply=True)) == 1


def test_status_hides_archived_but_all_shows_it(env):
    env.extend([_item("DEAD-1", "closed"), _item("LIVE-1", "open", "LIVE")])
    cli.cmd_queue_archive(_ns(apply=True))
    names = lambda **kw: {r["queue"] for r in health.all_status(items=list(env), **kw)}
    assert names() == {"LIVE"}
    assert names(include_archived=True) == {"LIVE", "DEAD"}
    assert "DEAD" in {r["queue"] for r in health.all_status(project="DEAD", items=list(env))}


def test_models_migrate_skips_archived(env):
    assert {r["queue"] for r in mm.plan_migrate("sonnet-5", "sonnet-5-5")["rows"]} == {"LIVE", "DEAD"}
    cli.cmd_queue_archive(_ns(apply=True))
    assert {r["queue"] for r in mm.plan_migrate("sonnet-5", "sonnet-5-5")["rows"]} == {"LIVE"}
    assert {r["queue"] for r in mm.plan_unpin()["rows"]} == {"LIVE"}


def test_unarchive_restores_but_leaves_drain_off(env):
    cli.cmd_queue_archive(_ns(apply=True))
    assert cli.cmd_queue_unarchive(_ns()) == 0
    assert not config.is_archived("DEAD") and "DEAD" in config.all_queues()
    assert not config.auto_drain("DEAD")
    assert cli.cmd_queue_unarchive(_ns()) == 1


def test_queue_ls_filters(env, capsys):
    cli.cmd_queue_archive(_ns(apply=True))
    capsys.readouterr()
    cli.cmd_queue_ls(_ns(json=True))
    assert [r["queue"] for r in json.loads(capsys.readouterr().out)] == ["LIVE"]
    cli.cmd_queue_ls(_ns(json=True, archived=True))
    assert [r["queue"] for r in json.loads(capsys.readouterr().out)] == ["DEAD"]
    cli.cmd_queue_ls(_ns(json=True, all=True))
    assert len(json.loads(capsys.readouterr().out)) == 2

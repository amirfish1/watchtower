"""WT-3: CONFIG events carry an actor; outside edits are detected."""
import json

import pytest

from watchtower import config


@pytest.fixture
def env(tmp_path, monkeypatch):
    cfg = tmp_path / "queue-config.json"
    log = tmp_path / "activity.log"
    monkeypatch.setattr(config, "CONFIG_FILE", cfg)
    monkeypatch.setenv("WATCHTOWER_CONFIG_FILE", str(cfg))
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(log))
    monkeypatch.setenv("CLAUDE_SESSION_ID", "sess-1234")
    return cfg, log


def test_save_logs_actor_and_diff(env):
    cfg, log = env
    config.set_auto_drain("Q", True)
    line = [l for l in log.read_text().splitlines() if "CONFIG" in l][-1]
    assert "auto_drain: unset" in line and "true" in line
    assert "session=sess-1234" in line and "host=" in line and "pid=" in line


def test_outside_edit_is_logged(env):
    cfg, log = env
    config.set_auto_drain("Q", True)
    data = json.loads(cfg.read_text())
    data["Q"]["auto_drain"] = False
    cfg.write_text(json.dumps(data))
    diff = config.check_outside_changes()
    assert "Q" in diff
    assert "changed outside wt" in log.read_text()
    assert config.check_outside_changes() == {}


def test_config_without_queue_prints_path(env, capsys):
    from watchtower import cli
    import argparse
    assert cli.cmd_config(argparse.Namespace(queue=None)) == 0
    assert str(env[0]) in capsys.readouterr().out

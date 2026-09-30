"""config.sanitize_worker_settings -- self-heal for invalid queue efforts.

2026-09-17: a queue pinned to an effort-less model (kimi's pinned models
accept no explicit effort) with ``"effort": "high"`` set made every
is_approved_effort validator flag the queue config invalid permanently.
The daemon now drops exactly that key at start and hourly; these tests pin
what it drops and -- just as hard -- what it must leave alone.
"""

import json

import pytest

import watchtower.config as config


@pytest.fixture
def cfg(tmp_path, monkeypatch):
    """Redirect the queue-config file into tmp_path (same pattern as
    test_config_model_aliases)."""
    monkeypatch.setattr(config, "CONFIG_FILE", tmp_path / "queue-config.json")
    return config


def _raw(cfg):
    return json.loads(cfg.CONFIG_FILE.read_text())


def test_invalid_explicit_effort_is_kept_and_reported(cfg):
    """The incident shape: kimi's pinned models accept no explicit effort."""
    cfg.set_engine("CCX", "kimi")
    cfg.set_model("CCX", "kimi-code/kimi-for-coding")
    cfg.set_effort("CCX", "high")

    changes = cfg.sanitize_worker_settings()

    assert changes == [
        {
            "queue": "CCX",
            "unapproved_effort": "high",
            "engine": "kimi",
            "model": "kimi-code/kimi-for-coding",
        }
    ]
    entry = _raw(cfg)["CCX"]
    # WT-19: a user pin is never deleted by a migration.
    assert entry["effort"] == "high"
    assert "not listed" in cfg.effort_pin_warning("CCX")
    assert entry["engine"] == "kimi"
    assert entry["model"] == "kimi-code/kimi-for-coding"


def test_valid_configs_are_left_untouched(cfg):
    cfg.set_engine("CLD", "claude")
    cfg.set_model("CLD", "claude-opus-5")
    cfg.set_effort("CLD", "high")
    cfg.set_engine("CDX", "codex")
    cfg.set_model("CDX", "gpt-5.6")
    cfg.set_effort("CDX", "max")
    before = _raw(cfg)

    assert cfg.sanitize_worker_settings() == []
    assert _raw(cfg) == before


def test_effort_without_an_explicit_model_stays(cfg):
    """An unpinned model leaves effort to the engine default, so the full
    effort vocabulary is approved -- nothing to heal."""
    cfg.set_engine("ENG", "kimi")
    cfg.set_effort("ENG", "medium")

    assert cfg.sanitize_worker_settings() == []
    assert _raw(cfg)["ENG"]["effort"] == "medium"


def test_malformed_entries_are_skipped_never_fatal(cfg):
    cfg.CONFIG_FILE.write_text(
        json.dumps(
            {
                "BROKEN": "oops",
                "ALSO_BROKEN": 42,
                "CCX": {
                    "engine": "kimi",
                    "model": "kimi-code/kimi-for-coding",
                    "effort": "high",
                },
            }
        )
    )

    changes = cfg.sanitize_worker_settings()

    assert [c["queue"] for c in changes] == ["CCX"]
    data = _raw(cfg)
    assert data["BROKEN"] == "oops"
    assert data["ALSO_BROKEN"] == 42
    assert data["CCX"]["effort"] == "high"


def test_log_callable_fires_once_per_healed_queue(cfg):
    cfg.set_engine("CCX", "kimi")
    cfg.set_model("CCX", "kimi-code/kimi-for-coding")
    cfg.set_effort("CCX", "high")
    messages = []

    cfg.sanitize_worker_settings(log=messages.append)

    assert len(messages) == 1
    assert "CCX" in messages[0]
    assert "high" in messages[0]


def test_nothing_to_heal_does_not_rewrite_the_file(cfg):
    cfg.set_engine("CLD", "claude")
    cfg.set_model("CLD", "claude-opus-5")
    before = cfg.CONFIG_FILE.stat().st_mtime_ns

    assert cfg.sanitize_worker_settings() == []
    assert cfg.CONFIG_FILE.stat().st_mtime_ns == before


def test_unlisted_model_or_missing_catalog_never_flags(cfg, monkeypatch):
    """Stale/missing catalog -> pins untouched and no warning (WT-19)."""
    cfg.set_engine("VM", "codex")
    cfg.set_model("VM", "gpt-6.1-sol")  # not in any catalog
    cfg.set_effort("VM", "medium")
    assert cfg.sanitize_worker_settings() == []
    assert cfg.effort_pin_warning("VM") == ""
    monkeypatch.setattr(config_models(), "catalog", lambda eng: None)
    assert cfg.sanitize_worker_settings() == []
    assert _raw(cfg)["VM"]["effort"] == "medium"


def test_spawn_drops_rejected_effort_for_that_launch_only(cfg, capsys):
    from watchtower import workers
    cfg.set_engine("CCX", "kimi")
    cfg.set_model("CCX", "kimi-code/kimi-for-coding")
    cfg.set_effort("CCX", "high")
    assert workers._launch_effort("CCX", "kimi", "kimi-code/kimi-for-coding", "high") == ""
    assert "launching without it" in capsys.readouterr().out
    assert _raw(cfg)["CCX"]["effort"] == "high"
    assert workers._launch_effort("CCX", "claude", "claude-opus-5", "high") == "high"


def config_models():
    from watchtower import models
    return models

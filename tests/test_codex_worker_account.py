"""Codex workers on a chosen account (CCC worker_codex_home)."""

import json

from watchtower import codex_rpc, config, messages, workers


def _setup(monkeypatch, tmp_path, worker_home=True):
    default = tmp_path / "default-codex"
    second = tmp_path / "second-codex"
    for home in (default, second):
        (home / "sessions" / "2026" / "11" / "04").mkdir(parents=True)
    monkeypatch.setenv("CODEX_HOME", str(default))
    defaults = tmp_path / "spawn-defaults.json"
    data = {"codex_account_homes": [str(second)]}
    if worker_home:
        data["worker_codex_home"] = str(second)
    defaults.write_text(json.dumps(data))
    monkeypatch.setattr(config, "CCC_SPAWN_DEFAULTS_FILE", defaults)
    workers._CODEX_ROLLOUT_CACHE.clear()
    return default, second


def _rollout(home, sid):
    path = home / "sessions" / "2026" / "11" / "04" / f"rollout-2026-11-04T00-00-00-{sid}.jsonl"
    path.write_text("{}\n")
    return path


def test_worker_home_comes_from_ccc_defaults(monkeypatch, tmp_path):
    default, second = _setup(monkeypatch, tmp_path)
    assert config.codex_worker_home() == second
    assert config.codex_homes() == [default, second]
    assert workers._codex_spawn_home("codex") == str(second)
    assert workers._codex_spawn_home("claude") == ""
    env = workers._spawn_env("w1", role="worker", codex_home=workers._codex_spawn_home("codex"))
    assert env["CODEX_HOME"] == str(second)


def test_missing_or_default_worker_home_means_default(monkeypatch, tmp_path):
    default, second = _setup(monkeypatch, tmp_path, worker_home=False)
    assert config.codex_worker_home() is None
    assert workers._codex_spawn_home("codex") == ""
    config.CCC_SPAWN_DEFAULTS_FILE.write_text(json.dumps({"worker_codex_home": str(tmp_path / "gone")}))
    assert config.codex_worker_home() is None
    config.CCC_SPAWN_DEFAULTS_FILE.write_text(json.dumps({"worker_codex_home": str(default)}))
    assert config.codex_worker_home() is None


def test_resumed_session_stays_in_its_home(monkeypatch, tmp_path):
    default, second = _setup(monkeypatch, tmp_path, worker_home=False)
    _rollout(second, "sid-second")
    _rollout(default, "sid-default")
    assert workers._codex_spawn_home("codex", "sid-second") == str(second)
    assert workers._codex_spawn_home("codex", "sid-default") == ""


def test_liveness_sees_rollouts_in_other_homes(monkeypatch, tmp_path):
    default, second = _setup(monkeypatch, tmp_path)
    path = _rollout(second, "sid-x")
    w = {"engine": "codex", "session_id": "sid-x", "codex_home": str(second)}
    assert workers._codex_rollout_mtime(w) == path.stat().st_mtime
    assert workers._codex_rollout_path("sid-x") == path
    assert workers._engine_activity(w)["path"] == str(path)
    assert messages._find_codex_rollout("sid-x") == path


def test_private_app_server_skips_other_account_threads(monkeypatch, tmp_path):
    default, second = _setup(monkeypatch, tmp_path)
    _rollout(second, "sid-y")
    result = codex_rpc.deliver("sid-y", "hello")
    assert result["ok"] is False and result["stage"] == "account"


def test_execpolicy_written_into_given_home(tmp_path):
    home = tmp_path / "h"
    home.mkdir()
    assert workers.ensure_codex_wt_execpolicy(home) is True
    assert "wt" in (home / "rules" / "watchtower.rules").read_text()


def test_shared_sessions_dir_uses_thread_index_for_ownership(monkeypatch, tmp_path):
    import shutil
    import sqlite3
    default, second = _setup(monkeypatch, tmp_path)
    # Account homes usually symlink sessions to the default home's.
    shutil.rmtree(second / "sessions")
    (second / "sessions").symlink_to(default / "sessions")
    _rollout(default, "sid-owned")
    _rollout(default, "sid-primary")
    con = sqlite3.connect(second / "state_5.sqlite")
    con.execute("CREATE TABLE threads (id TEXT PRIMARY KEY)")
    con.execute("INSERT INTO threads VALUES ('sid-owned')")
    con.commit()
    con.close()
    assert config.codex_home_for_session("sid-owned") == second
    assert config.codex_home_for_session("sid-primary") == default
    assert workers._codex_spawn_home("codex", "sid-primary") == ""
    # One shared dir is scanned once, not once per home.
    assert config.codex_session_roots() == [default / "sessions"]

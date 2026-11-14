"""Account routing tests never launch a real model or change credentials."""
import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from watchtower import antigravity_accounts as accounts, cli, config, workers


@pytest.fixture
def setup_accounts(tmp_path, monkeypatch):
    defaults = tmp_path / "spawn-defaults.json"
    defaults.write_text(json.dumps({"worker_antigravity_account_id": "secondary"}))
    monkeypatch.setattr(config, "CCC_SPAWN_DEFAULTS_FILE", defaults)
    monkeypatch.setattr(workers, "WORKERS_FILE", tmp_path / "workers.json")
    monkeypatch.setattr(workers, "WORKER_IDS_FILE", tmp_path / "worker-ids.json")
    monkeypatch.setattr(workers, "WORKER_SESSIONS_FILE", tmp_path / "sessions.json")
    monkeypatch.setattr(workers, "_resolve_antigravity_bin", lambda: "/fake/agy")
    monkeypatch.setattr(workers, "_resolve_engine_bin", lambda eng: "/fake/agy")
    monkeypatch.setattr(workers, "_pid_start_token", lambda pid: "test-start")
    monkeypatch.setattr(workers.origins, "note_spawn", lambda *a, **kw: None)
    monkeypatch.setattr(workers.origins, "caller_session_id", lambda: "")
    monkeypatch.setattr(workers, "_wait_for_immediate_launch_failure", lambda *a, **kw: None)
    monkeypatch.setattr(workers, "_make_stdin_fifo", lambda *a: (None, None))
    monkeypatch.setattr(config, "model", lambda q: "")
    monkeypatch.setattr(config, "effort", lambda q: "")
    monkeypatch.setattr(config, "product_gate", lambda q: False)
    return defaults


def overlay(argv):
    return json.loads(argv[argv.index("--settings") + 1])


def test_worker_default_validation(setup_accounts):
    assert accounts.worker_account() == "secondary"
    for value in ("", "primary"):
        setup_accounts.write_text(json.dumps({"worker_antigravity_account_id": value}))
        assert accounts.worker_account() == "primary"
    setup_accounts.write_text('{"worker_antigravity_account_id":"wrong"}')
    with pytest.raises(ValueError):
        accounts.worker_account()


def test_secondary_environment_is_child_only(setup_accounts, monkeypatch):
    for key in accounts._CLOUD_ENV:
        monkeypatch.setenv(key, "test-private-value")
    monkeypatch.setenv("KEEP_ME", "yes")
    env = workers._spawn_env("w", engine="antigravity", antigravity_account_id="secondary")
    assert env["AGY_ADC_AUTH"] == "false"
    assert all(k not in env for k in accounts._CLOUD_ENV - {"AGY_ADC_AUTH"})
    assert env["KEEP_ME"] == "yes"
    assert os.environ["GOOGLE_APPLICATION_CREDENTIALS"] == "test-private-value"
    assert accounts.launch_env("primary")["GOOGLE_APPLICATION_CREDENTIALS"] == "test-private-value"


def test_pending_binding_is_durable_and_conflicts_fail(setup_accounts):
    accounts.bind_pending("worker.log", "secondary")
    accounts.finalize_pending("worker.log", "session-x")
    accounts.finalize_pending("worker.log", "session-x")
    assert accounts.session_account("session-x") == "secondary"
    assert accounts.session_account("legacy") == "primary"
    setup_accounts.write_text('{"worker_antigravity_account_id":"primary"}')
    assert accounts.session_account("session-x") == "secondary"
    with pytest.raises(ValueError):
        accounts.bind("session-x", "primary")


@pytest.mark.parametrize("kind", ["drain", "once", "adhoc", "stage"])
def test_all_launches_select_env_and_persist_account(setup_accounts, monkeypatch, tmp_path, kind):
    captured = []
    def popen(argv, **kwargs):
        captured.append((argv, kwargs))
        # Change defaults during launch: account selection must remain atomic.
        setup_accounts.write_text('{"worker_antigravity_account_id":"primary"}')
        return SimpleNamespace(pid=987654)
    monkeypatch.setattr(workers.subprocess, "Popen", popen)
    monkeypatch.setattr(workers, "_spawn_gate", lambda *a: None)
    if kind == "drain":
        rec = workers.spawn_workers("Q", engine="antigravity", repo_path=str(tmp_path))[0]
    elif kind == "once":
        rec = workers.spawn_run_once_worker("Q", "Q-1", engine="antigravity", repo_path=str(tmp_path))
    else:
        rec = workers.spawn_adhoc("check", "antigravity", repo_path=str(tmp_path),
                                  stage="verifier" if kind == "stage" else "")
    argv, kwargs = captured[0]
    assert kwargs["env"]["AGY_ADC_AUTH"] == "false"
    assert rec["antigravity_account_id"] == "secondary"
    assert "test-private-value" not in str(argv)
    disk = json.loads(workers.WORKERS_FILE.read_text())["workers"][0]
    assert disk["antigravity_account_id"] == "secondary"
    Path(rec["log"]).write_text('{"event":"init","conversation_id":"11111111-2222-4333-8444-555555555555"}\n')
    monkeypatch.setattr(workers, "_pid_alive", lambda pid: True)
    workers.list_workers(prune=False)
    assert accounts.session_account("11111111-2222-4333-8444-555555555555") == "secondary"


def test_resume_retains_account_after_defaults_change(setup_accounts, monkeypatch, tmp_path):
    accounts.bind("session-second", "secondary")
    setup_accounts.write_text('{"worker_antigravity_account_id":"primary"}')
    captured = []
    monkeypatch.setattr(cli.subprocess, "Popen", lambda argv, **kw: captured.append((argv, kw)) or SimpleNamespace(pid=987654))
    monkeypatch.setattr(cli.resume_verify, "verify_resume_child", lambda proc: (False, ""))
    monkeypatch.setattr(cli.Path, "home", lambda: tmp_path)
    assert cli._resume_session_headless("session-second", str(tmp_path), "continue", "antigravity")
    argv, kwargs = captured[0]
    assert argv[0] == "/fake/agy" and "--resume" in argv
    assert kwargs["env"]["AGY_ADC_AUTH"] == "false"
    assert cli._answer_engine({"claimed_session_id": "session-second"}, "antigravity") == "antigravity"


def test_primary_overlay_copies_only_safe_settings(setup_accounts, monkeypatch):
    home = Path(os.environ["WATCHTOWER_ANTIGRAVITY_HOME"])
    home.mkdir()
    settings = {"gcp": {"project": "test-project", "location": "test-location", "key": "test-secret"},
                "apiKey": "test-secret", "auth": {"type": "oauth-business", "token": "test-secret"}}
    (home / "settings.json").write_text(json.dumps(settings))
    for key in accounts._CLOUD_ENV:
        monkeypatch.delenv(key, raising=False)
    primary = accounts.configure_command(["agy", "-p", "check"], "primary", "test-model")
    assert overlay(primary)["gcp"] == {"project": "test-project", "location": "test-location"}
    assert "test-secret" not in str(primary)
    assert (home / "settings.json").read_text() == json.dumps(settings)
    with pytest.raises(ValueError):
        accounts.configure_command(["agy", "--settings", "{}"], "secondary")


def test_existing_worker_account_is_preserved(setup_accounts):
    first = workers.record_worker(987654, "Q", "antigravity", "same-worker", session_id="sid-a",
                                  antigravity_account_id="secondary")
    setup_accounts.write_text('{"worker_antigravity_account_id":"primary"}')
    second = workers.record_worker(987655, "Q", "antigravity", "same-worker", session_id="sid-a")
    assert first["antigravity_account_id"] == second["antigravity_account_id"] == "secondary"


def test_dry_run_has_account_without_creating_bindings(setup_accounts):
    rec = workers.spawn_workers("Q", engine="antigravity", dry_run=True)[0]
    assert rec["antigravity_account_id"] == "secondary"
    assert not Path(os.environ["WATCHTOWER_ANTIGRAVITY_STATE_DIR"]).exists()
    assert not workers.WORKERS_FILE.exists()


def test_engine_detection_and_discuss_keep_account(setup_accounts, monkeypatch, tmp_path):
    accounts.bind("sid-secondary", "secondary")
    monkeypatch.setattr(workers, "list_workers", lambda **kw: [
        {"session_id": "sid-secondary", "engine": "antigravity"}])
    assert cli._answer_engine({"claimed_session_id": "sid-secondary"}, None) == "antigravity"
    monkeypatch.setattr(cli.q, "get", lambda ref: {"ref": ref, "repo_path": str(tmp_path)})
    monkeypatch.setattr(cli.q, "answer_session", lambda item: "sid-secondary")
    captured = []
    monkeypatch.setattr(cli.subprocess, "call", lambda argv, **kw: captured.append((argv, kw)) or 0)
    assert cli.cmd_discuss(SimpleNamespace(ref="Q-1", engine="antigravity", print=False)) == 0
    argv, kw = captured[0]
    assert argv[:3] == ["/fake/agy", "--resume", "sid-secondary"]
    assert kw["env"]["AGY_ADC_AUTH"] == "false"


def test_discuss_print_includes_secondary_env_scrub(setup_accounts, monkeypatch, capsys):
    accounts.bind("sid-print", "secondary")
    monkeypatch.setattr(cli.q, "get", lambda ref: {"ref": ref})
    monkeypatch.setattr(cli.q, "answer_session", lambda item: "sid-print")
    assert cli.cmd_discuss(SimpleNamespace(ref="Q-1", engine="antigravity", print=True)) == 0
    printed = capsys.readouterr().out
    assert "-u GOOGLE_APPLICATION_CREDENTIALS" in printed
    assert "AGY_ADC_AUTH=false" in printed


@pytest.mark.parametrize("argv", [
    ["config", "-q", "Q", "--engine", "antigravity"],
    ["set", "-q", "Q", "--engine", "antigravity"],
    ["start", "--engine", "antigravity"],
    ["answer", "Q-1", "continue", "--engine", "antigravity"],
    ["discuss", "Q-1", "--engine", "antigravity", "--print"],
])
def test_worker_and_resume_cli_accept_antigravity(argv):
    assert cli.build_parser().parse_args(argv).engine == "antigravity"


def test_secondary_isolates_profiles_and_preserves_operational_context(setup_accounts, monkeypatch, tmp_path):
    native = Path(os.environ["WATCHTOWER_ANTIGRAVITY_HOME"])
    native.mkdir()
    (native / "settings.json").write_text('{"private":"test-private-value"}')
    (tmp_path / "port.txt").write_text("http://127.0.0.1:12345")
    monkeypatch.delenv("CCC_SERVER", raising=False)
    monkeypatch.delenv("WATCHTOWER_DELEGATE_URL", raising=False)
    monkeypatch.setenv("ANTIGRAVITY_GEMINI_DIR", "test-ambient-directory")
    monkeypatch.setenv("GEMINI_DIR", "test-ambient-alias")
    monkeypatch.setenv("XDG_CONFIG_HOME", "test-ambient-config")
    env = accounts.launch_env("secondary")
    home = Path(env["HOME"])
    assert home == Path(os.environ["WATCHTOWER_ANTIGRAVITY_STATE_DIR"]) / "secondary-home"
    assert env["ANTIGRAVITY_CREDENTIAL_SCOPE"] == "ccc-antigravity-secondary"
    assert all(key not in env for key in ("ANTIGRAVITY_GEMINI_DIR", "GEMINI_DIR", "XDG_CONFIG_HOME"))
    assert env["WATCHTOWER_DATA_DIR"] == os.environ["WATCHTOWER_DATA_DIR"]
    assert env["WATCHTOWER_CCC_SPAWN_DEFAULTS_FILE"] == str(setup_accounts)
    assert env["WATCHTOWER_MODEL_POLICY_FILE"] == str(config.CCC_MODEL_POLICY_FILE)
    assert env["CCC_SERVER"] == "http://127.0.0.1:12345"
    assert env["WATCHTOWER_DELEGATE_URL"] == env["CCC_SERVER"]
    isolated_cli = home / ".gemini" / "antigravity-cli"
    for name in ("brain", "conversations"):
        assert (isolated_cli / name).is_symlink()
        assert (isolated_cli / name).resolve() == (native / name).resolve()
    assert not (isolated_cli / "settings.json").exists()
    assert not (home / ".gemini" / "config").exists()
    assert (home / ".watchtower").is_symlink()
    assert accounts.launch_settings("secondary") == {"gcp": {"project": "", "location": ""}, "modelProvider": ""}
    assert accounts.launch_env("primary", {"HOME": "test-original"})["HOME"] == "test-original"
    # Refuse a conflicting profile layout instead of overwriting it.
    (isolated_cli / "brain").unlink()
    (isolated_cli / "brain").mkdir()
    with pytest.raises(ValueError, match="storage conflicts"):
        accounts.launch_env("secondary")


def test_pending_adhoc_account_resolves_native_log_before_discovery(setup_accounts, tmp_path):
    log = tmp_path / "adhoc.log"
    log.write_text("model output without init event")
    native_log = log.with_suffix(".agy.log")
    sid = "11111111-2222-4333-8444-555555555555"
    native_log.write_text(f"[INFO] Print mode: conversation={sid}\n")
    accounts.bind_pending(str(log), "secondary")
    assert accounts.session_account(sid) == "secondary"
    assert workers.resolve_antigravity_session_id(str(log)) == sid


def test_nested_secondary_keeps_original_home_and_ls_registration(setup_accounts, monkeypatch, tmp_path):
    original = tmp_path / "original-home"
    first = accounts.launch_env("secondary", {"CCC_ANTIGRAVITY_PRIMARY_HOME": str(original)})
    monkeypatch.setenv("HOME", first["HOME"])
    monkeypatch.setenv("CCC_ANTIGRAVITY_PRIMARY_HOME", str(original))
    second = accounts.launch_env("secondary")
    assert second["CCC_ANTIGRAVITY_PRIMARY_HOME"] == str(original)
    assert second["CCC_ANTIGRAVITY_ACCOUNT_ID"] == "secondary"
    assert second["CCC_ANTIGRAVITY_LS_DIR"] == str(tmp_path / "agy-ls")
    assert (Path(second["HOME"]) / ".watchtower").resolve() == original / ".watchtower"
    for key in ("AGY_LLM_GATEWAY_API_KEY", "AGY_LLM_GATEWAY_FUTURE_OPTION"):
        monkeypatch.setenv(key, "test-private-value")
    assert all(not key.startswith("AGY_LLM_GATEWAY_") for key in accounts.launch_env("secondary"))


def test_nested_primary_restores_only_safe_selectors(setup_accounts, tmp_path):
    original = {
        "HOME": str(tmp_path / "native-home"),
        "CCC_ANTIGRAVITY_PRIMARY_HOME": str(tmp_path / "native-home"),
        "ANTIGRAVITY_CREDENTIAL_SCOPE": "custom-primary-scope",
        "ANTIGRAVITY_CONFIG_REL_DIR": "primary-config",
        "XDG_CONFIG_HOME": str(tmp_path / "original-config"),
        "GEMINI_DIR": str(tmp_path / "original-gemini-dir"),
        "AGY_ADC_AUTH": "true",
        "GOOGLE_APPLICATION_CREDENTIALS": str(tmp_path / "credentials-reference.json"),
        "GOOGLE_GENAI_USE_VERTEXAI": "true",
        "GOOGLE_CLOUD_PROJECT": "test-primary-project",
    }
    secondary = accounts.launch_env("secondary", original)
    snapshot = secondary[accounts._PRIMARY_SELECTOR_MARKER]
    assert "test-secret-never-copy" not in snapshot
    assert "GEMINI_API_KEY" not in snapshot and "GOOGLE_CREDENTIALS" not in snapshot
    nested_secondary = accounts.launch_env("secondary", secondary)
    assert nested_secondary[accounts._PRIMARY_SELECTOR_MARKER] == snapshot
    primary = accounts.launch_env("primary", nested_secondary)
    for key in accounts._PRIMARY_SELECTORS | {"HOME"}:
        assert primary.get(key) == original.get(key), key
    assert primary["CCC_ANTIGRAVITY_ACCOUNT_ID"] == "primary"
    assert "GEMINI_API_KEY" not in primary and "GOOGLE_CREDENTIALS" not in primary
    assert accounts._PRIMARY_SELECTOR_MARKER not in primary
    secrets = {**original, "GEMINI_API_KEY": "test-secret-never-copy"}
    secret_secondary = accounts.launch_env("secondary", secrets)
    assert "test-secret-never-copy" not in str(secret_secondary)
    with pytest.raises(ValueError, match="original launcher environment"):
        accounts.launch_env("primary", secret_secondary)
    assert secrets["GEMINI_API_KEY"] == "test-secret-never-copy"


def test_legacy_secondary_restores_native_defaults_without_snapshot(setup_accounts, tmp_path):
    env = accounts.launch_env("secondary", {"CCC_ANTIGRAVITY_PRIMARY_HOME": str(tmp_path / "native")})
    env.pop(accounts._PRIMARY_SELECTOR_MARKER)
    restored = accounts.launch_env("primary", env)
    assert restored["HOME"] == str(tmp_path / "native")
    assert "AGY_ADC_AUTH" not in restored
    assert "ANTIGRAVITY_CREDENTIAL_SCOPE" not in restored
    assert "ANTIGRAVITY_CONFIG_REL_DIR" not in restored


def test_primary_never_rewrites_unrelated_environment(setup_accounts, tmp_path):
    custom = {"HOME": str(tmp_path / "custom-home"), "AGY_ADC_AUTH": "false",
              "ANTIGRAVITY_CREDENTIAL_SCOPE": "custom-scope",
              "CCC_ANTIGRAVITY_PRIMARY_HOME": str(tmp_path / "native-home"),
              "CCC_ANTIGRAVITY_ACCOUNT_ID": "secondary",
              accounts._PRIMARY_SELECTOR_MARKER: '{"AGY_ADC_AUTH":"true"}'}
    routed = accounts.launch_env("primary", custom)
    for key in ("HOME", "AGY_ADC_AUTH", "ANTIGRAVITY_CREDENTIAL_SCOPE"):
        assert routed[key] == custom[key]
    # A namespace-like scope alone is insufficient: HOME must also match.
    custom["ANTIGRAVITY_CREDENTIAL_SCOPE"] = "ccc-antigravity-secondary"
    assert accounts.launch_env("primary", custom)["HOME"] == custom["HOME"]


def test_selector_restore_ignores_credential_material(setup_accounts, tmp_path):
    secondary = accounts.launch_env("secondary", {"CCC_ANTIGRAVITY_PRIMARY_HOME": str(tmp_path / "native")})
    secondary[accounts._PRIMARY_SELECTOR_MARKER] = json.dumps({
        "GEMINI_API_KEY": "test-secret", "GOOGLE_CREDENTIALS": "test-secret",
        "GOOGLE_APPLICATION_CREDENTIALS": "not-a-file-reference",
        "AGY_ADC_AUTH": "invalid-boolean"})
    restored = accounts.launch_env("primary", secondary)
    assert all(key not in restored for key in ("GEMINI_API_KEY", "GOOGLE_CREDENTIALS", "GOOGLE_APPLICATION_CREDENTIALS", "AGY_ADC_AUTH"))

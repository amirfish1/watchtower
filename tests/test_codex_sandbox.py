"""Codex workspace-write sandbox launch policy (WT-61): argv and environment.

Kernel enforcement is proven in tests/test_sandbox_policy.py; this file
checks what the launcher builds, without running Codex.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path

import pytest

from watchtower import config, sandbox_policy, workers


def _overrides(argv):
    return [argv[i + 1] for i, a in enumerate(argv[:-1]) if a == "-c"]


@pytest.fixture
def cfg(tmp_path, monkeypatch):
    # The profile denies /dev/shm, and some hosts put pytest's tmp there.
    base = tmp_path
    made = None
    if Path("/dev/shm") in Path(os.path.realpath(tmp_path)).parents:
        made = tempfile.mkdtemp(prefix="wt-sandbox-test-", dir=str(Path.home()))
        base = Path(made)
    monkeypatch.setenv("HOME", str(base))
    monkeypatch.setenv("WATCHTOWER_QUEUE_CONFIG", str(base / "queue-config.json"))
    monkeypatch.setenv("WATCHTOWER_DATA_DIR", str(base / "data"))
    monkeypatch.delenv("CODEX_HOME", raising=False)
    monkeypatch.setattr(config, "codex_worker_home", lambda: None)
    yield base
    if made:
        shutil.rmtree(made, ignore_errors=True)


def test_default_keeps_full_access(cfg, monkeypatch):
    repo = cfg / "repo"; repo.mkdir()
    monkeypatch.setattr(workers, "_resolve_engine_bin", lambda e: None)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-live")
    argv = workers.build_drain_command("DEMO", "codex", "w1", str(repo), goal="g")
    assert "--dangerously-bypass-approvals-and-sandbox" in argv
    assert not any("default_permissions" in a for a in argv)
    # Sandbox off: the environment is inherited exactly as before.
    env = workers._spawn_env("w1", queue="DEMO", engine="codex")
    assert env["OPENAI_API_KEY"] == "sk-live"
    assert "WT_SANDBOX_BRIDGE" not in env


def test_default_app_server_keeps_legacy_thread_sandbox(cfg, monkeypatch):
    repo = cfg / "repo"; repo.mkdir()
    monkeypatch.setattr(workers, "_resolve_engine_bin", lambda e: "/bin/codex")
    argv = workers.build_drain_command("DEMO", "codex", "w1", str(repo), goal="g")
    assert "watchtower.codex_app_worker" in " ".join(argv)
    assert "--permission-profile" not in argv and "--server-cwd" not in argv


def test_exec_uses_native_profile(cfg, monkeypatch):
    repo = cfg / "repo"; repo.mkdir()
    store = cfg / "store"
    config.set_codex_sandbox("DEMO", "workspace-write", str(store))
    config.set_codex_transport("DEMO", "exec")
    monkeypatch.setattr(workers, "_resolve_engine_bin", lambda e: None)
    argv = workers.build_drain_command("DEMO", "codex", "w1", str(repo), goal="g")
    assert "--dangerously-bypass-approvals-and-sandbox" not in argv
    # Native profile only: the legacy mode would drop the profile's rules.
    assert "--sandbox" not in argv and "--ignore-rules" in argv
    ov = _overrides(argv)
    assert not any(o.startswith(("sandbox_mode", "sandbox_workspace_write")) for o in ov)
    assert f'default_permissions="{sandbox_policy.PROFILE}"' in ov
    assert 'approval_policy="never"' in ov
    assert "notify=[]" in ov and 'web_search="disabled"' in ov
    for feature in ("hooks", "plugins", "apps", "goals", "browser_use", "computer_use"):
        assert f"features.{feature}=false" in ov
    worker_store = store / "w1"
    rules = sandbox_policy.filesystem_rules(repo, worker_store)
    assert rules[str(repo)] == "write" and rules[str(worker_store)] == "write"
    assert rules[":minimal"] == "read" and ":root" not in rules
    assert rules["/dev/shm"] == "deny"
    for protected in (".git", ".codex", ".agents"):
        assert rules[str(repo / protected)] == "read"
    assert sorted(k for k, v in rules.items() if v == "write") == sorted([str(repo), str(worker_store)])
    profile = next(o for o in ov if o.startswith(f"permissions.{sandbox_policy.PROFILE}="))
    assert json.dumps(str(repo)) + '="write"' in profile
    assert '"network"={"enabled"=false}' in profile
    trust = next(o for o in ov if o.startswith("projects="))
    assert json.dumps(str(repo)) + '={"trust_level"="untrusted"}' in trust
    assert json.dumps(str(cfg)) + '={"trust_level"="untrusted"}' in trust  # ancestors too
    # Building the command creates nothing (dry runs stay side-effect free).
    assert not store.exists()


def test_sandbox_env_is_scrubbed(cfg, monkeypatch):
    repo = cfg / "repo"; repo.mkdir()
    store = cfg / "store"
    config.set_codex_sandbox("DEMO", "workspace-write", str(store))
    for key in ("OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GITHUB_TOKEN", "AWS_SECRET_ACCESS_KEY",
                "SSH_AUTH_SOCK", "HTTPS_PROXY", "https_proxy", "ALL_PROXY", "NO_PROXY",
                "GIT_SSH_COMMAND", "GIT_ASKPASS", "DATABASE_URL", "XDG_RUNTIME_DIR",
                "WATCHTOWER_QUEUE_CONFIG_EXTRA", "WT_MESSAGE_ENVELOPE", "TMPDIR"):
        monkeypatch.setenv(key, "leak")
    env = workers._spawn_env("w1", queue="DEMO", engine="codex")
    assert not [k for k, v in env.items() if v == "leak"]
    worker_store = str(store / "w1")
    assert env["TMPDIR"] == worker_store + "/tmp"
    assert env["WATCHTOWER_STORE"].startswith(worker_store)
    assert env["WT_SANDBOX_BRIDGE"] == worker_store and env["WT_WORKER_ID"] == "w1"
    assert env["PATH"] and env["HOME"]
    # Worker-writable Git metadata must never be named in the host process env.
    assert "GIT_DIR" not in env and "GIT_WORK_TREE" not in env
    # Other engines on the queue are untouched.
    assert workers._spawn_env("w2", queue="DEMO", engine="claude")["OPENAI_API_KEY"] == "leak"


def test_app_server_preserves_profile(cfg, monkeypatch):
    repo = cfg / "repo"; repo.mkdir()
    store = cfg / "store"
    config.set_codex_sandbox("DEMO", "workspace-write", str(store))
    config.set_codex_transport("DEMO", "app-server")
    monkeypatch.setattr(workers, "_resolve_engine_bin", lambda e: "/bin/codex")
    argv = workers.build_drain_command("DEMO", "codex", "w1", str(repo), goal="g")
    assert "watchtower.codex_app_worker" in " ".join(argv)
    assert "--sandbox" not in argv
    assert argv[argv.index("--permission-profile") + 1] == sandbox_policy.PROFILE
    assert argv[argv.index("--sandbox-store") + 1] == str(store / "w1")
    # The server starts outside the untrusted workspace.
    assert argv[argv.index("--server-cwd") + 1] == "/"
    assert argv[argv.index("--cwd") + 1] == str(repo)
    ov = _overrides(argv)
    assert f'default_permissions="{sandbox_policy.PROFILE}"' in ov
    assert any(o.startswith("shell_environment_policy.set=") for o in ov)
    assert argv[-1] == "g"


def test_fails_closed(cfg):
    config.set_codex_sandbox("DEMO", "workspace-write", str(cfg / "store"))
    with pytest.raises(workers.CodexSandboxError):
        workers.build_drain_command("DEMO", "codex", "w1", str(cfg / "missing"), goal="g")
    (cfg / "repo").mkdir()
    config.set_codex_sandbox("DEMO", "workspace-write", "/dev/shm/wt-nope/store")
    with pytest.raises(workers.CodexSandboxError, match="/dev/shm"):
        workers.build_drain_command("DEMO", "codex", "w1", str(cfg / "repo"), goal="g")
    # Store inside the workspace is not a separate root.
    config.set_codex_sandbox("DEMO", "workspace-write", str(cfg / "repo" / "store"))
    with pytest.raises(workers.CodexSandboxError, match="separate"):
        workers.build_drain_command("DEMO", "codex", "w1", str(cfg / "repo"), goal="g")
    with pytest.raises(workers.CodexSandboxError):
        workers.build_drain_command("DEMO", "codex", "../evil", str(cfg / "repo"), goal="g")


@pytest.mark.parametrize("where", ["repo", "ancestor"])
def test_refuses_project_codex_config(cfg, where):
    """A workspace `.codex` ran its MCP command on the host in testing."""
    repo = cfg / "nest" / "repo"; repo.mkdir(parents=True)
    config.set_codex_sandbox("DEMO", "workspace-write", str(cfg / "store"))
    target = (repo if where == "repo" else repo.parent) / ".codex"
    target.mkdir()
    (target / "config.toml").write_text("[mcp_servers.evil]\ncommand = 'x'\n")
    with pytest.raises(workers.CodexSandboxError, match="untrusted Codex project config"):
        workers.build_drain_command("DEMO", "codex", "w1", str(repo), goal="g")
    (target / "config.toml").unlink()
    target.rmdir()
    target.symlink_to(cfg)  # a symlink is refused too, never followed
    with pytest.raises(workers.CodexSandboxError, match="untrusted Codex project config"):
        workers.build_drain_command("DEMO", "codex", "w1", str(repo), goal="g")


def test_owner_codex_home_is_not_a_hazard(cfg):
    repo = cfg / "repo"; repo.mkdir()
    (cfg / ".codex").mkdir()  # $HOME/.codex: the owner's own Codex home
    assert sandbox_policy.project_config_hazards(repo) == []


def test_sandboxed_launch_leaves_execpolicy_alone(cfg, monkeypatch):
    repo = cfg / "repo"; repo.mkdir()
    config.set_codex_sandbox("DEMO", "workspace-write", str(cfg / "store"))
    config.set_codex_transport("DEMO", "exec")
    called = []
    monkeypatch.setattr(workers, "ensure_codex_wt_execpolicy", lambda *a, **k: called.append(a))
    workers.build_drain_command("DEMO", "codex", "w1", str(repo), goal="g")
    assert called == []
    config.set_codex_sandbox("DEMO", "")
    workers.build_drain_command("DEMO", "codex", "w1", str(repo), goal="g")
    assert called  # default path unchanged

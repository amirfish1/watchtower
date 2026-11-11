"""Codex workspace-write sandbox policy (WT-61)."""

from __future__ import annotations

import json
import pytest

from watchtower import config, workers


@pytest.fixture
def cfg(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("WATCHTOWER_QUEUE_CONFIG", str(tmp_path / "queue-config.json"))
    return tmp_path


def test_default_keeps_full_access(cfg, monkeypatch):
    repo = cfg / "repo"; repo.mkdir()
    monkeypatch.setattr(workers, "_resolve_engine_bin", lambda e: None)
    argv = workers.build_drain_command("DEMO", "codex", "w1", str(repo), goal="g")
    assert "--dangerously-bypass-approvals-and-sandbox" in argv
    assert "--sandbox" not in argv


def test_exec_workspace_write(cfg, monkeypatch):
    repo = cfg / "repo"; repo.mkdir()
    store = cfg / "store"
    config.set_codex_sandbox("DEMO", "workspace-write", str(store))
    config.set_codex_transport("DEMO", "exec")
    argv = workers.build_drain_command("DEMO", "codex", "w1", str(repo), goal="g")
    assert "--dangerously-bypass-approvals-and-sandbox" not in argv
    assert argv[argv.index("--sandbox") + 1] == "workspace-write"
    assert "sandbox_workspace_write.network_access=false" in argv
    roots = next(a for a in argv if a.startswith("sandbox_workspace_write.writable_roots="))
    assert json.loads(roots.split("=", 1)[1]) == [str(repo.resolve()), str(store.resolve())]
    env = workers._spawn_env("w1", queue="DEMO", engine="codex")
    assert env["WATCHTOWER_STORE"].startswith(str(store))
    # other engines on the queue are not redirected
    assert "WATCHTOWER_STORE" not in workers._spawn_env("w2", queue="DEMO", engine="claude") \
        or "WATCHTOWER_STORE" not in __import__("os").environ


def test_app_server_carries_sandbox(cfg, monkeypatch):
    repo = cfg / "repo"; repo.mkdir()
    config.set_codex_sandbox("DEMO", "workspace-write", str(cfg / "store"))
    config.set_codex_transport("DEMO", "app-server")
    monkeypatch.setattr(workers, "_resolve_engine_bin", lambda e: "/bin/codex")
    argv = workers.build_drain_command("DEMO", "codex", "w1", str(repo), goal="g")
    assert "watchtower.codex_app_worker" in " ".join(argv)
    assert argv[argv.index("--sandbox") + 1] == "workspace-write"
    assert any(a == "sandbox_workspace_write.network_access=false" for a in argv)


def test_fails_closed(cfg):
    config.set_codex_sandbox("DEMO", "workspace-write", str(cfg / "store"))
    with pytest.raises(workers.CodexSandboxError):
        workers.build_drain_command("DEMO", "codex", "w1", str(cfg / "missing"), goal="g")
    config.set_codex_sandbox("DEMO", "workspace-write", "/proc/nope/store")
    (cfg / "repo").mkdir()
    with pytest.raises(workers.CodexSandboxError):
        workers.build_drain_command("DEMO", "codex", "w1", str(cfg / "repo"), goal="g")

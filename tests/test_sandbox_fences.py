"""WT-61 host fencing: host code must never run git or resume/compact/ask a
sandbox-origin repository or session.

Every fixture is disposable (tmp_path). The hostile "helper" is a marker
script wired as a partial-clone promisor ``uploadpack`` in the sandbox
workspace's own git config: if host git ever needs an object it lacks in that
repository, the helper runs with the daemon's authority and leaves a marker.
No engine, delegate or network is ever contacted.
"""
import json
import os
import stat
import subprocess
import sys

import pytest

from watchtower import sandbox_bridge as bridge

SID = "aaaaaaaa-1111-4222-8333-444444444444"
MISSING = "deadbeef" * 5


def git(repo, *args, check=True):
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True,
                          check=check).stdout.strip()


def make_repo(path):
    path.mkdir(parents=True)
    git(path, "init", "-q", "-b", "main")
    git(path, "config", "user.email", "t@example.com")
    git(path, "config", "user.name", "t")
    (path / "f.txt").write_text("x\n")
    git(path, "add", "f.txt")
    git(path, "commit", "-q", "-m", "base")
    return git(path, "rev-parse", "HEAD")


def arm_helper(repo, marker):
    """Config that would run a host command on a missing-object lookup."""
    helper = repo.parent / (repo.name + "-helper.sh")
    helper.write_text(f"#!/bin/sh\necho ran >> {marker}\nexit 1\n")
    helper.chmod(helper.stat().st_mode | stat.S_IXUSR)
    git(repo, "config", "extensions.partialClone", "evil")
    git(repo, "config", "remote.evil.url", str(repo))
    git(repo, "config", "remote.evil.promisor", "true")
    git(repo, "config", "remote.evil.partialclonefilter", "blob:none")
    git(repo, "config", "remote.evil.uploadpack", str(helper))
    git(repo, "config", "core.fsmonitor", str(helper))
    git(repo, "config", "core.repositoryformatversion", "1")


@pytest.fixture
def env(wt_env, monkeypatch):
    w = wt_env
    w.marker = w.tmp / "marker"
    w.sb_repo = w.tmp / "sandbox-ws"
    make_repo(w.sb_repo)
    arm_helper(w.sb_repo, w.marker)
    w.main_repo = w.tmp / "main-ws"
    w.main_sha = make_repo(w.main_repo)
    w.config.set_codex_sandbox("SB", "workspace-write", str(w.tmp / "isolated"))
    w.config._save({**w.config._load(),
                    "SB": {**w.config._load()["SB"], "repo_path": str(w.sb_repo)},
                    "MAIN": {"repo_path": str(w.main_repo)}})
    with w.queue._FileLock(w.queue._lock_path()):
        w.queue._save_unlocked(w.queue._load_unlocked())
    w.store = bridge.register("SB", "sb-w1", str(w.sb_repo))
    assert bridge.bind_session("sb-w1", SID)
    monkeypatch.setattr(w.queue, "_notify_ticket_event", lambda *a, **k: None)
    monkeypatch.setattr(w.queue, "_notify_review", lambda *a, **k: None)
    return w


def unsandbox(w):
    """Turn the queue policy off: durable origin must still restrict."""
    data = w.config._load()
    data["SB"].pop("codex_sandbox", None)
    w.config._save(data)
    assert w.config.codex_sandbox("SB") != "workspace-write"


# ------------------------------------------------------ host git: close proof
def test_configured_repos_excludes_sandbox_workspaces(env):
    from watchtower import close_proof
    repos = close_proof.configured_repos()
    assert str(env.main_repo) in repos and str(env.sb_repo) not in repos
    unsandbox(env)      # the scope outlives a policy edit
    assert str(env.sb_repo) not in close_proof.configured_repos()


def test_cross_queue_proof_never_runs_git_in_sandbox_repo(env):
    from watchtower import close_proof
    verified, found, errors = close_proof.verify_with_errors(MISSING, str(env.main_repo))
    assert (verified, found) == ("", "")
    assert not env.marker.exists()
    unsandbox(env)
    close_proof.verify_with_errors(MISSING, str(env.main_repo))
    assert not env.marker.exists()


@pytest.mark.parametrize("via_symlink", [False, True])
def test_direct_proof_in_sandbox_repo_is_refused(env, via_symlink):
    from watchtower import close_proof
    repo = env.sb_repo
    if via_symlink:
        repo = env.tmp / "alias"
        repo.symlink_to(env.sb_repo)
    verified, err = close_proof.resolve_in_repo(str(repo), MISSING)
    assert verified == "" and "sandbox" in err
    verified, err = close_proof.resolve_in_repo(str(repo / ".git" / ".."), MISSING)
    assert verified == "" and "sandbox" in err
    assert not env.marker.exists()
    # A real commit that exists there is not accepted either: no host git.
    assert close_proof.resolve_in_repo(str(repo), git(env.sb_repo, "rev-parse", "HEAD"))[0] == ""


def test_subdirectory_and_gitdir_pointer_into_sandbox_are_restricted(env):
    from watchtower import close_proof
    sub = env.sb_repo / "pkg"
    sub.mkdir()
    assert close_proof.restricted_repo(str(sub))
    # A "trusted" checkout whose .git file points at the sandbox's metadata.
    other = env.tmp / "other-queue-repo"
    other.mkdir()
    (other / ".git").write_text(f"gitdir: {env.sb_repo / '.git'}\n")
    assert close_proof.restricted_repo(str(other))
    assert close_proof.resolve_in_repo(str(other), MISSING)[0] == ""
    assert not env.marker.exists()
    assert close_proof.restricted_repo(str(env.main_repo)) == ""


def test_unreadable_restriction_ledger_fails_closed(env, monkeypatch):
    from watchtower import close_proof
    monkeypatch.setattr(bridge, "authority_dir", lambda: env.tmp / "no" / "such" / "dir" / "x")
    # A missing authority dir means no sandbox was ever registered...
    assert close_proof.restricted_repo(str(env.main_repo)) == ""
    # ...but a ledger we cannot parse never grants host git.
    (env.tmp / "badauth").mkdir()
    (env.tmp / "badauth" / "w.json").write_text("{not json")
    monkeypatch.setattr(bridge, "authority_dir", lambda: env.tmp / "badauth")
    assert close_proof.restricted_repo(str(env.main_repo))
    assert close_proof.configured_repos() == []


def test_host_git_is_hardened_for_trusted_repos(env, monkeypatch):
    """Defence in depth for repos that ARE allowed: no lazy fetch, transports,
    fsmonitor or inherited GIT_DIR."""
    from watchtower import close_proof
    seen = {}
    real = subprocess.run

    def spy(argv, **kw):
        seen["argv"], seen["env"] = list(argv), kw.get("env")
        return real(argv, **kw)

    monkeypatch.setenv("GIT_DIR", str(env.sb_repo / ".git"))
    monkeypatch.setattr(close_proof.subprocess, "run", spy)
    sha, err = close_proof.resolve_in_repo(str(env.main_repo), env.main_sha)
    assert sha == env.main_sha and not err
    argv = seen["argv"]
    for pair in (("-c", "protocol.allow=never"), ("-c", "core.fsmonitor=false")):
        assert any(argv[i:i + 2] == list(pair) for i in range(len(argv) - 1))
    assert seen["env"]["GIT_NO_LAZY_FETCH"] == "1"
    assert seen["env"]["GIT_NO_REPLACE_OBJECTS"] == "1"
    assert "GIT_DIR" not in seen["env"]


# ------------------------------------------------------- host git: integration
def test_integration_git_refuses_sandbox_repo_before_any_git(env, monkeypatch):
    from watchtower import integration_git
    calls = []
    monkeypatch.setattr(integration_git.subprocess, "run",
                        lambda *a, **k: calls.append(a) or pytest.fail("git ran"))
    sha, source, problem = integration_git.containing_sha(str(env.sb_repo), {"SB-1": MISSING})
    assert (sha, source) == ("", "") and problem.startswith("sandbox_restricted")
    # Also the no-commits branch, which would otherwise resolve HEAD.
    assert integration_git.containing_sha(str(env.sb_repo), {})[2].startswith("sandbox_restricted")
    assert not calls and not env.marker.exists()


def _plan_group(w, repo, project, n=2, session=SID, commits=None, worker="sb-w1"):
    kids = [w.queue.enqueue(project=project, note=f"m{i}", repo_path=str(repo), source="t")["ref"]
            for i in range(n)]
    parent = w.queue.enqueue(project=project, note="group", source="t", repo_path=str(repo),
                             group_parent=True, children=kids)["ref"]
    w.queue.plan_start(parent)
    mv = w.queue.group_mv(w.queue.get(parent))
    text = "## Shared\nshared\n\n" + "\n\n".join(f"## Section: {k}\nbuild {k}" for k in kids)
    w.queue.plan_submit(parent, text, by="planner", expect_mv=mv)
    w.queue.plan_verdict(parent, True, "sound", by="reviewer", expect_mv=mv)
    for i, k in enumerate(kids):
        w.queue.claim_by_ref(k, worker, session_uuid=session)
        sha = commits[i] if commits else MISSING
        w.queue.close(k, force=True, resolution={"summary": "done", "commit": sha})
    return parent, kids


def test_group_sweep_checks_restriction_before_host_git(env, monkeypatch):
    """The reported escape: group reconciliation resolved member commits with
    host git before evaluate_gates' restricted check ever ran."""
    from watchtower import integration_git
    parent, kids = _plan_group(env, env.sb_repo, "SB")
    seen = []
    real = integration_git._git
    monkeypatch.setattr(integration_git, "_git", lambda *a: seen.append(a) or real(*a))
    unsandbox(env)       # even after the policy is switched off
    acted = env.queue.group_sweep(only_ref=parent)
    assert acted and acted[0][0] == parent
    assert not seen and not env.marker.exists()
    p = env.queue.get(parent)
    integ = p["group"]["integration"]
    assert integ["blocked"] == "gate_setup" and integ["state"] == "idle"
    assert not p["group"]["fixes"]       # not misfiled as a divergence fix
    assert "refusing host git" in json.dumps(p["history"])


def test_group_sweep_in_trusted_queue_with_sandbox_child_never_uses_host_git(env, monkeypatch):
    """A group on a normal queue whose member was built by a sandbox worker
    (tombstoned session) is also refused, whatever repo it names."""
    from watchtower import integration_git
    calls = []
    monkeypatch.setattr(integration_git, "containing_sha",
                        lambda *a: calls.append(a) or ("x", "head", ""))
    parent, kids = _plan_group(env, env.main_repo, "MAIN")
    acted = env.queue.group_sweep(only_ref=parent)
    assert acted and not calls


def test_group_sweep_still_integrates_ordinary_groups(env):
    other = "bbbbbbbb-1111-4222-8333-444444444444"
    shas = []
    for name in ("a", "b"):
        (env.main_repo / f"{name}.txt").write_text(name)
        git(env.main_repo, "add", f"{name}.txt")
        git(env.main_repo, "commit", "-q", "-m", name)
        shas.append(git(env.main_repo, "rev-parse", "HEAD"))
    parent, kids = _plan_group(env, env.main_repo, "MAIN", session=other, commits=shas,
                              worker="plain-w1")
    assert env.queue.group_sweep(only_ref=parent) == [(parent, "verifying")]
    integ = env.queue.get(parent)["group"]["integration"]
    assert integ["sha"] == shas[1]


# --------------------------------------------------- restricted session sends
@pytest.fixture
def traps(env, monkeypatch):
    """Every unsandboxed route a restricted session could be promoted through."""
    from watchtower import codex_rpc, messages
    calls = []

    def trap(name):
        def _t(*a, **k):
            calls.append(name)
            raise AssertionError(f"{name} must not be reached for a restricted session")
        return _t

    monkeypatch.setattr(messages, "_post_json", trap("_post_json"))
    monkeypatch.setattr(messages, "_get_json", trap("_get_json"))
    monkeypatch.setattr(messages, "_delegate_base", lambda: "http://127.0.0.1:9")
    monkeypatch.setattr(messages.subprocess, "Popen", trap("Popen"))
    monkeypatch.setattr(codex_rpc, "request", trap("codex_rpc.request"))
    monkeypatch.setattr(codex_rpc, "is_available", lambda: True)
    monkeypatch.setattr(messages.tty_mod, "deliver_tty", trap("tty"))
    monkeypatch.setattr(messages, "_outbox_file", lambda: env.tmp / "outbox.json")
    monkeypatch.setattr(messages, "_outbox_lock", lambda: env.tmp / "outbox.lock")
    env.calls = calls
    return env


def restricted_resolutions():
    # The registry forgot the worker (pruned) and no codex thread is known, so
    # resolve_target would label the bare UUID "claude"; callers may also
    # force any engine label, a federated prefix or an agent name.
    return [
        {"kind": "session", "session_id": SID, "worker": None, "engine": "claude"},
        {"kind": "session", "session_id": SID, "worker": None, "engine": "codex"},
        {"kind": "session", "session_id": SID, "worker": None, "engine": "gemini"},
        {"kind": "session", "session_id": SID, "worker": None, "engine": "antigravity"},
        {"kind": "session", "session_id": SID, "worker": None, "engine": "claude",
         "same_session": True},
        {"kind": "session", "session_id": SID, "worker": None, "engine": "claude",
         "remote_node": "peer"},
        {"kind": "agent", "name": SID, "session_id": "other-id", "worker": None, "engine": "kimi"},
        {"kind": "worker", "session_id": "", "engine": "claude",
         "worker": {"worker_id": "sb-w1"}},
    ]


@pytest.mark.parametrize("resolved", restricted_resolutions())
@pytest.mark.parametrize("notify,live_only", [(False, False), (True, False)])
def test_deliver_refuses_every_engine_label(traps, resolved, notify, live_only):
    from watchtower import messages
    out = messages.deliver(dict(resolved), "hello", notify=notify)
    assert out["ok"] is False and out.get("terminal") is True
    assert traps.calls == []


def test_unknown_uuid_default_engine_is_still_restricted(traps, monkeypatch):
    from watchtower import messages
    monkeypatch.setattr(messages, "resolve_target",
                        lambda t, **k: {"kind": "session", "session_id": t, "worker": None,
                                        "engine": "claude", "known": False})
    out = messages.send(SID, "hello", queue_on_fail=True, start_new_if_needed=True)
    assert out["ok"] is False and out["queued"] is False
    assert not (traps.tmp / "outbox.json").exists()
    assert traps.calls == []


def test_ask_refuses_before_delegate_or_resume(traps, monkeypatch):
    from watchtower import messages
    for engine in ("claude", "codex"):
        monkeypatch.setattr(messages, "resolve_target",
                            lambda t, e=engine, **k: {"kind": "session", "session_id": SID,
                                                      "worker": None, "engine": e})
        out = messages.ask(SID, "question?", timeout_ms=200)
        assert out["ok"] is False and out["code"] == "sandbox_restricted"
    assert traps.calls == []


def test_continue_if_cold_never_spawns_a_continuation(traps):
    from watchtower import messages
    out = messages._continue_if_cold(
        {"kind": "session", "session_id": SID, "worker": None, "engine": "claude"}, "go")
    assert out is not None and out["ok"] is False and out["terminal"] is True
    assert "/api/sessions/spawn-continue-from" not in traps.calls and traps.calls == []


def test_compact_never_reaches_ccc_or_shared_app_server(traps):
    from watchtower import messages
    for engine in ("claude", "codex"):
        out = messages.compact_codex({"session_id": SID, "engine": engine, "cwd": str(traps.sb_repo)})
        assert out["ok"] is False and out["code"] == "sandbox_restricted"
    assert traps.calls == []


def test_codex_rpc_compact_and_deliver_refuse_directly(traps, monkeypatch):
    from watchtower import codex_rpc
    monkeypatch.setattr(codex_rpc, "_ensure_server", lambda *a, **k: pytest.fail("spawned"),
                        raising=False)
    for fn in (lambda: codex_rpc.compact(SID, cwd=str(traps.sb_repo)),
               lambda: codex_rpc.deliver(SID, "hi", cwd=str(traps.sb_repo))):
        out = fn()
        assert out["ok"] is False and out["stage"] == "sandbox-policy"
    assert traps.calls == []


def test_unreadable_session_ledger_never_grants_promotion(traps, monkeypatch):
    from watchtower import messages
    monkeypatch.setattr(bridge, "restricted_session",
                        lambda *a, **k: (_ for _ in ()).throw(OSError("ledger")))
    out = messages.deliver({"kind": "session", "session_id": "cccccccc-1111-4222-8333-444444444444",
                            "worker": None, "engine": "claude"}, "hi")
    assert out["ok"] is False and out["terminal"] is True


def test_live_pinned_worker_may_still_be_steered(traps, monkeypatch):
    from watchtower import codex_app_worker, messages
    got = []
    monkeypatch.setattr(codex_app_worker, "steer",
                        lambda sid, text, **k: got.append((sid, text)) or {"ok": True, "via": "steer"})
    out = messages.deliver({"kind": "session", "session_id": SID, "worker": None,
                            "engine": "claude"}, "nudge")
    assert out["ok"] is True and got and got[0][0] == SID
    assert traps.calls == []


def test_unrestricted_session_is_unaffected(traps, monkeypatch):
    from watchtower import messages
    sid = "dddddddd-1111-4222-8333-444444444444"
    assert messages._is_restricted({"session_id": sid, "worker": None}) is False


def test_dead_restricted_outbox_row_is_terminal_not_retried(traps, monkeypatch):
    from watchtower import messages
    messages.outbox_add(SID, "late", mode="send", error="", delay_s=0, ttl_s=None, engine="claude")
    monkeypatch.setattr(messages, "resolve_target",
                        lambda t, **k: {"kind": "session", "session_id": SID, "worker": None,
                                        "engine": "claude"})
    result = messages.drain_outbox(now=10 ** 10)
    assert len(result["dead"]) == 1 and not result["retried"] and not result["delivered"]
    assert traps.calls == []


# ------------------------------------------------------------------ cli resume
@pytest.mark.parametrize("engine", ["claude", "codex", "kimi", "devin", "antigravity"])
def test_cli_resume_refuses_restricted_session_for_every_engine(env, monkeypatch, engine):
    cli = env.cli
    monkeypatch.setattr(cli.subprocess, "Popen",
                        lambda *a, **k: pytest.fail("a headless resume must not start"))
    assert cli._resume_session_headless(SID, str(env.sb_repo), "go", engine,
                                        queue="SB", worker_id="sb-w1") is False
    # Durable: the queue policy is off and the worker row is gone, only the
    # session tombstone remains.
    unsandbox(env)
    assert cli._resume_session_headless(SID, str(env.sb_repo), "go", engine) is False
    assert cli._resume_session_headless("eeeeeeee-1111-4222-8333-444444444444", str(env.sb_repo),
                                        "go", engine, worker_id="sb-w1") is False


def test_group_sweep_refuses_when_only_a_member_is_durably_restricted(env, monkeypatch):
    """Member built by a since-pruned sandbox worker (queue policy off, no
    registry row), group parent in a trusted-looking queue and repo."""
    from watchtower import integration_git
    calls = []
    monkeypatch.setattr(integration_git, "containing_sha",
                        lambda *a: calls.append(a) or ("x", "head", ""))
    parent, kids = _plan_group(env, env.main_repo, "MAIN", session="ffffffff-1111-4222-8333-444444444444",
                               worker="sb-w1")
    unsandbox(env)
    assert env.queue._group_sandbox_origin(env.queue.get(parent))
    acted = env.queue.group_sweep(only_ref=parent)
    assert acted and not calls

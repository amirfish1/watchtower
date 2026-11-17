"""WT-61: command gates of sandbox-origin tickets run in the restricted
sandbox (bridge no-code close, trusted adoption close, group integration).

The CAS tests are hermetic (a fake transport); the ``native`` ones run real
``codex sandbox`` processes against a disposable repository under the real
home and skip when the Linux sandbox is unavailable.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

from test_sandbox_bridge import close_body, env, submit  # noqa: F401  (shared fixture)
from test_sandbox_policy import REAL_HOME, native  # noqa: F401
from watchtower import sandbox_bridge as bridge, sandbox_stages


def _git(repo, *args):
    e = {**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@x",
         "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@x", "GIT_CONFIG_NOSYSTEM": "1"}
    return subprocess.run(["git", "-C", str(repo), *args], check=True, env=e,
                          capture_output=True, text=True).stdout.strip()


@pytest.fixture
def trusted_repo(monkeypatch):
    """Two commits in a host-owned repository under the real home."""
    root = Path(tempfile.mkdtemp(prefix="wt-sandbox-gates-", dir=str(REAL_HOME)))
    (root / "codex-home").mkdir()
    monkeypatch.setenv("CODEX_HOME", str(root / "codex-home"))
    monkeypatch.setenv("WATCHTOWER_SANDBOX_STAGE_ROOT", str(root / "stages"))
    repo = root / "trusted"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    (repo / "v.txt").write_text("one\n")
    _git(repo, "add", "v.txt")
    _git(repo, "commit", "-qm", "one")
    first = _git(repo, "rev-parse", "HEAD")
    (repo / "v.txt").write_text("two\n")
    _git(repo, "commit", "-qam", "two")
    yield {"root": root, "repo": repo, "first": first, "head": _git(repo, "rev-parse", "HEAD")}
    shutil.rmtree(root, ignore_errors=True)


def _claim(w, repo, gates):
    w.ticket = w.queue.enqueue(project="SB", note="gated work", repo_path=str(repo), gates=gates)
    result = submit(w, "claim", {})
    assert result["ok"], result
    w.claim = result["claim"]
    return result["item"]


def _no_stage_dirs():
    root = Path(os.environ["WATCHTOWER_SANDBOX_STAGE_ROOT"])
    return not root.exists() or not any(root.iterdir())


# ------------------------------------------------------------- native


def test_native_bridge_close_runs_gate_on_trusted_head_snapshot(native, env, trusted_repo):
    w = env
    item = _claim(w, trusted_repo["repo"], ["cmd:test \"$(cat v.txt)\" = two && echo head-two"])
    out = submit(w, "close", close_body(item["ref"]), w.claim)
    assert out["ok"], out
    done = w.queue.get(item["ref"])
    assert done["status"] == "closed"
    gate = done["gate_results"][0]
    assert gate["passed"] and gate["sandboxed"] is True and "head-two" in gate["output_tail"]
    assert gate["source"]["commit"] == trusted_repo["head"]
    assert _no_stage_dirs()


def test_native_bridge_close_failed_gate_reopens_with_reason(native, env, trusted_repo):
    w = env
    item = _claim(w, trusted_repo["repo"], ["cmd:echo nope >&2; exit 3"])
    out = submit(w, "close", close_body(item["ref"]), w.claim)
    assert out["ok"] is False and "exit 3" in out["error"] and "nope" in out["error"]
    after = w.queue.get(item["ref"])
    assert after["status"] == "open" and not after.get("claimed_by")
    assert after["gate_feedback"] and after["gate_results"][0]["exit_code"] == 3


def test_native_adoption_close_gates_the_adopted_commit_not_head(native, wt_env, trusted_repo,
                                                                 monkeypatch):
    w = wt_env
    monkeypatch.setattr(bridge, "restricted_session",
                        lambda session_id="", worker_id="": worker_id == "sb-worker")
    item = w.queue.enqueue(project="SB", note="adopted", repo_path=str(trusted_repo["repo"]),
                           gates=["cmd:cat v.txt"])
    w.queue.claim_by_ref(item["ref"], "sb-worker")
    closed = w.queue.close(item["ref"], "sb-worker",
                           resolution={"summary": "adopted", "commit": trusted_repo["first"]})
    assert closed["status"] == "closed"
    gate = closed["gate_results"][0]
    assert gate["sandboxed"] and gate["output_tail"].strip() == "one"   # pinned, not HEAD ("two")
    assert gate["source"]["commit"] == trusted_repo["first"]


def test_native_group_sweep_finds_commit_and_gates_in_sandbox(native, env, trusted_repo,
                                                              monkeypatch):
    from watchtower import integration_git
    w = env
    monkeypatch.setattr(integration_git, "_git", lambda *a: pytest.fail("host git"))
    monkeypatch.setattr(integration_git.subprocess, "run",
                        lambda *a, **k: pytest.fail("host subprocess"))
    repo = trusted_repo["repo"]
    kids = [w.queue.enqueue(project="SB", note=f"m{i}", repo_path=str(repo))["ref"] for i in range(2)]
    parent = w.queue.enqueue(project="SB", note="group", repo_path=str(repo), group_parent=True,
                             children=kids, gates=["cmd:test \"$(cat v.txt)\" = two"])["ref"]
    w.queue.plan_start(parent)
    mv = w.queue.group_mv(w.queue.get(parent))
    text = "## Shared\nshared\n\n" + "\n\n".join(f"## Section: {k}\nbuild {k}" for k in kids)
    w.queue.plan_submit(parent, text, by="planner", expect_mv=mv)
    w.queue.plan_verdict(parent, True, "sound", by="reviewer", expect_mv=mv)
    for k, sha in zip(kids, (trusted_repo["first"], trusted_repo["head"])):
        w.queue.claim_by_ref(k, "sb-w1")
        w.queue.close(k, force=True, resolution={"summary": "done", "commit": sha})
    acted = w.queue.group_sweep(only_ref=parent)
    assert acted == [(parent, "closed")], acted
    p = w.queue.get(parent)
    assert p["status"] == "closed"
    assert p["group"]["integration"]["sha"] == trusted_repo["head"]
    assert p["gate_results"][-1]["sandboxed"] is True and p["gate_results"][-1]["passed"]


# ------------------------------------------------- lock-time CAS (hermetic)


def _fake_gate(monkeypatch, mutate, passed=True):
    def fake(item, command, commit="", timeout=None):
        mutate()
        return {"gate": "cmd:" + command, "passed": passed, "exit_code": 0 if passed else 1,
                "output_tail": "boom" if not passed else "", "commit": commit, "sandboxed": True}
    monkeypatch.setattr(sandbox_stages, "run_cmd_gate", fake)


def _edit(w, ref, fn):
    with w.queue._FileLock(w.queue._lock_path()):
        data = w.queue._load_unlocked()
        fn(next(i for i in data["items"] if i["ref"] == ref))
        w.queue._save_unlocked(data)


@pytest.mark.parametrize("passed", [True, False])
@pytest.mark.parametrize("change", ["gates", "owner", "generation"])
def test_slow_gate_cannot_write_back_over_a_changed_claim(env, monkeypatch, change, passed):
    w = env
    item = _claim(w, w.repo, ["cmd:true"])

    def mutate():
        def fn(it):
            if change == "gates":
                it["gates"] = ["cmd:true", "cmd:echo added"]
            elif change == "owner":
                it["claimed_by"] = "replacement"
            else:
                it["last_answer_gen"] = 20
        _edit(w, item["ref"], fn)
    _fake_gate(monkeypatch, mutate, passed)
    out = submit(w, "close", close_body(item["ref"]), w.claim)
    after = w.queue.get(item["ref"])
    assert out["ok"] is False
    assert after["status"] == "in_progress"            # nothing was written over the change
    assert not after.get("gate_feedback") and after["status"] != "closed"


def test_slow_gate_cannot_write_back_over_a_changed_store(env, monkeypatch):
    from watchtower import store_identity
    w = env
    item = _claim(w, w.repo, ["cmd:true"])
    _fake_gate(monkeypatch, lambda: monkeypatch.setattr(
        store_identity, "read_unlocked", lambda base: "another-store"))
    out = submit(w, "close", close_body(item["ref"]), w.claim)
    assert out["ok"] is False and "identity" in out["error"]
    assert w.queue.get(item["ref"])["status"] == "in_progress"


def test_passing_fake_gate_closes_and_records_sandboxed_result(env, monkeypatch):
    w = env
    item = _claim(w, w.repo, ["cmd:true", "verify"])
    _fake_gate(monkeypatch, lambda: None)
    out = submit(w, "close", close_body(item["ref"]), w.claim)
    assert out["ok"], out
    after = w.queue.get(item["ref"])
    assert after["status"] == "in_review" and after["gate_pending"] == "verify"
    assert after["gate_results"][0]["sandboxed"] is True


def test_close_client_waits_longer_than_the_gate_budget():
    assert bridge.CLOSE_TIMEOUT_S >= sandbox_stages.GATES_BUDGET_S + 120

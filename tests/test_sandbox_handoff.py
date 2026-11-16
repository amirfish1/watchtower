"""A store commit hands off source without claiming release integration."""
import json
import os
import subprocess
from pathlib import Path

import pytest

from watchtower import sandbox_bridge as bridge, sandbox_handoff as handoff, sandbox_policy as policy
from tests.test_sandbox_bridge import env, claim, close_body, submit  # noqa: F401


def git(repo, *args):
    return subprocess.check_output(["/usr/bin/git", "-c", "core.hooksPath=/dev/null",
                                   "-C", str(repo), *args], text=True).strip()


def fixture_patch(w, monkeypatch, binary=False):
    git(w.repo, "init", "-q")
    (w.repo / "sum.py").write_text("answer = 4\n")
    git(w.repo, "add", "sum.py")
    odd_name = os.fsdecode(b"name-\xff")
    if binary:
        (w.repo / "payload.bin").write_bytes(b"\x00old\xff")
        (w.repo / odd_name).write_bytes(b"old\n")
        git(w.repo, "add", "payload.bin", odd_name)
    git(w.repo, "-c", "user.name=Test", "-c", "user.email=test@localhost", "commit", "-qm", "base")
    base = git(w.repo, "rev-parse", "HEAD")
    trusted = w.tmp / "integration"
    subprocess.run(["/usr/bin/git", "clone", "--no-local", "-q", str(w.repo), str(trusted)], check=True)
    (w.repo / "sum.py").write_text("answer = 5\n")
    if binary:
        (w.repo / "payload.bin").write_bytes(b"\x00new\xfe")
        (w.repo / odd_name).write_bytes(b"new\n")
    patch = subprocess.check_output(["/usr/bin/git", "-C", str(w.repo), "-c",
                                     "core.quotePath=false", "diff", "--binary", "--full-index"])
    if binary:
        assert b"\xff" in patch and b"GIT binary patch" in patch
    sha = "ab" * 20
    monkeypatch.setattr(policy, "launch_record", lambda _: {
        "repo": str(w.repo), "store": str(w.store), "base": base})
    monkeypatch.setattr(policy, "verify_worker_commit", lambda *a: {"ok": True, "commit": sha})
    monkeypatch.setattr(policy, "export_patch", lambda *a: patch)
    return sha, trusted


@pytest.mark.parametrize("binary", [False, True])
def test_code_close_exports_and_parks_until_trusted_adoption(env, monkeypatch, binary):
    w = env
    sha, trusted = fixture_patch(w, monkeypatch, binary)
    item = claim(w)
    body = dict(close_body(item["ref"]), commit=sha, no_code=False)
    result = submit(w, "close", body, w.claim)
    assert result["ok"] and result["integration_pending"]
    current = w.queue.get(item["ref"])
    assert current["status"] == "awaiting_answer"
    assert current["wait"]["on"] == "worker"
    assert not current.get("resolution")
    adopted = handoff.adopt(w.worker, sha, trusted)
    assert (trusted / "sum.py").read_text() == "answer = 5\n"
    if binary:
        assert (trusted / "payload.bin").read_bytes() == b"\x00new\xfe"
        assert (trusted / os.fsdecode(b"name-\xff")).read_bytes() == b"new\n"
    assert git(trusted, "rev-parse", "HEAD") == adopted["commit"]
    # Adoption is source preparation, never an automatic push or close.
    assert w.queue.get(item["ref"])["status"] == "awaiting_answer"
    with pytest.raises(ValueError, match="already adopted"):
        handoff.adopt(w.worker, sha, trusted)


def test_handoff_refuses_worker_destination_dirty_tree_and_tampered_bytes(env, monkeypatch):
    w = env
    sha, trusted = fixture_patch(w, monkeypatch)
    item = claim(w)
    assert submit(w, "close", dict(close_body(item["ref"]), commit=sha, no_code=False), w.claim)["ok"]
    with pytest.raises(ValueError, match="sandbox root"):
        handoff.adopt(w.worker, sha, w.repo)
    (trusted / "unrelated").write_text("owner work")
    with pytest.raises(ValueError, match="clean"):
        handoff.adopt(w.worker, sha, trusted)
    (trusted / "unrelated").unlink()
    patch = next((bridge.authority_dir() / "handoffs").glob("*.patch"))
    patch.write_text("tampered")
    with pytest.raises(ValueError, match="integrity"):
        handoff.adopt(w.worker, sha, trusted)


def test_control_tree_and_sibling_store_cannot_become_workspace(env):
    w = env
    control_child = w.workers.WORKERS_FILE.parent / "new-root"
    control_child.mkdir()
    with pytest.raises(ValueError, match="trusted control"):
        bridge.register("SB", "other", control_child)
    with pytest.raises(ValueError, match="private roots"):
        bridge.register("SB", "other", w.store)


def test_restricted_prompt_describes_available_operations(env):
    w = env
    text = w.workers.drain_goal("SB", w.worker, str(w.repo), engine="codex")
    assert "Do not push" in text and "integration worker" in text
    assert "~/.watchtower/learnings" not in text
    assert "git push origin" not in text and "--no-code" in text
    assert "--wait-on" not in text  # coordinator supplies trusted wait routing


def test_old_handoff_cannot_adopt_after_answer_generation_changes(env, monkeypatch):
    w = env
    sha, trusted = fixture_patch(w, monkeypatch)
    item = claim(w)
    assert submit(w, "close", dict(close_body(item["ref"]), commit=sha, no_code=False), w.claim)["ok"]
    with w.queue._FileLock(w.queue._lock_path()):
        data = w.queue._load_unlocked()
        next(x for x in data["items"] if x["ref"] == item["ref"])["last_answer_gen"] = 42
        w.queue._save_unlocked(data)
    with pytest.raises(ValueError, match="current parked claim"):
        handoff.adopt(w.worker, sha, trusted)
    assert (trusted / "sum.py").read_text() == "answer = 4\n"

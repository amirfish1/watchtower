"""WT-61: isolated gate/stage transports for sandbox-origin tickets.

Model-free: the native fixtures run real ``codex sandbox`` processes against
disposable repositories under the real home (``/tmp`` is ``/dev/shm`` on some
hosts, which the profile denies). They skip when the Linux sandbox is absent.
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from test_sandbox_policy import _git, base, native  # noqa: F401  (shared fixtures)
from watchtower import sandbox_policy as sp, sandbox_stages as ss


@pytest.fixture
def trusted(base, monkeypatch):  # noqa: F811
    """A trusted repository with one commit, and a private stage root."""
    monkeypatch.setenv("WATCHTOWER_SANDBOX_STAGE_ROOT", str(base / "stages"))
    repo = base / "trusted"
    repo.mkdir()
    _git(repo, "init", "-q")
    (repo / "ok.txt").write_text("hello\n")
    (repo / "run.sh").write_text("#!/bin/sh\necho ran\n")
    os.chmod(repo / "run.sh", 0o755)
    (repo / "sub").mkdir()
    (repo / "sub" / "deep.txt").write_text("deep\n")
    os.symlink("ok.txt", repo / "link")
    (repo / ".agents").mkdir()
    (repo / ".agents" / "x.md").write_text("project agent config\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "base")
    return {"base": base, "repo": repo, "head": _git(repo, "rev-parse", "HEAD")}


def _item(fx, **extra):
    return {"ref": "WT-9", "project": "P", "repo_path": str(fx["repo"]), **extra}


def test_native_gate_runs_in_materialized_pinned_source(native, trusted):
    res = ss.run_cmd_gate(_item(trusted), "cat ok.txt && test -x run.sh && ./run.sh && cat sub/deep.txt "
                          "&& test -L link && test -z \"$(ls -A .git; ls -A .agents; ls -A .codex)\"", trusted["head"])
    assert res["passed"], res
    assert res["sandboxed"] is True and res["commit"] == trusted["head"]
    assert "hello" in res["output_tail"] and "ran" in res["output_tail"]
    assert res["source"]["omitted"] == [".agents/x.md"]
    assert res["source"]["commit"] == trusted["head"]
    # fresh directories are gone afterwards
    assert not any(Path(os.environ["WATCHTOWER_SANDBOX_STAGE_ROOT"]).iterdir())


def test_native_gate_failure_reports_exit_and_tail(native, trusted):
    res = ss.run_cmd_gate(_item(trusted), "echo boom >&2; exit 7", trusted["head"])
    assert res["passed"] is False and res["exit_code"] == 7 and "boom" in res["output_tail"]
    assert not res.get("setup_failed")


def test_native_malicious_gate_cannot_touch_host_or_network(native, trusted):
    import socket
    repo, base_ = trusted["repo"], trusted["base"]
    sentinel = base_ / "host-sentinel"
    inside = base_ / "inside-trusted-sentinel"
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(4)
    listener.settimeout(0.1)
    port = listener.getsockname()[1]
    (repo / "evil.py").write_text(
        "import os, socket, sys\n"
        "for target in sys.argv[1:3]:\n"
        "    try:\n        open(target, 'w').write('x')\n    except OSError:\n        pass\n"
        "try:\n    s = socket.create_connection(('127.0.0.1', int(sys.argv[3])), 2); s.send(b'hi')\n"
        "except OSError:\n    pass\n"
        "try:\n    open(os.path.expanduser('~/.codex/auth.json')).read(); print('LEAK')\n"
        "except OSError:\n    pass\n"
        "open('inside-ok', 'w').write('ok')\n")
    _git(repo, "add", "evil.py")
    _git(repo, "commit", "-qm", "evil")
    head = _git(repo, "rev-parse", "HEAD")
    res = ss.run_cmd_gate(_item(trusted), f"python3 evil.py {sentinel} {inside} {port}; "
                          "test -f inside-ok && echo finished", head)
    # A successful connect remains in the listen backlog. Check it after the
    # gate instead of leaking an accept thread into unrelated later tests.
    try:
        connection, _ = listener.accept()
    except socket.timeout:
        reached = False
    else:
        reached = True
        connection.close()
    finally:
        listener.close()
    assert res["passed"], res
    assert "finished" in res["output_tail"] and "LEAK" not in res["output_tail"]
    assert not sentinel.exists() and not inside.exists()
    assert not reached, "the gate reached the network"
    # the trusted repo was never written to (no checkout, no .git change)
    assert _git(repo, "status", "--porcelain") == ""


def test_native_gate_refuses_unpinned_unknown_or_missing_source(native, trusted):
    short = ss.run_cmd_gate(_item(trusted), "true", trusted["head"][:10])
    assert short["setup_failed"] and "full commit id" in short["output_tail"]
    ghost = ss.run_cmd_gate(_item(trusted), "echo ran", "0" * 40)
    assert ghost["setup_failed"] and ghost["passed"] is False and "ran" not in ghost["output_tail"]
    none = ss.run_cmd_gate({"ref": "WT-9", "repo_path": ""}, "echo ran", trusted["head"])
    assert none["setup_failed"] and none["sandboxed"] is True
    plain = trusted["base"] / "plain"
    plain.mkdir()
    nogit = ss.run_cmd_gate({"ref": "WT-9", "repo_path": str(plain)}, "echo ran", trusted["head"])
    assert nogit["setup_failed"] and "ran" not in nogit["output_tail"]


def test_native_empty_commit_uses_head_from_files(native, trusted):
    res = ss.run_cmd_gate(_item(trusted), "cat ok.txt", "")
    assert res["passed"] and res["source"]["commit"] == trusted["head"]
    assert res["commit"] == ""


def test_native_materialize_rejects_traversal_and_hostile_entries(native, trusted):
    repo = trusted["repo"]
    import subprocess
    env = {**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@x",
           "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@x"}

    def run(*args, stdin=None):
        return subprocess.run(["git", "-C", str(repo), *args], input=stdin, env=env,
                              capture_output=True, text=True, check=True).stdout.strip()
    blob = run("hash-object", "-w", "--stdin", stdin="pwned\n")
    # `git mktree` refuses '/' in names, but accepts '..' as a single entry name.
    tree = subprocess.run(["git", "-C", str(repo), "mktree"], input=f"100644 blob {blob}\t..\n",
                          env=env, capture_output=True, text=True)
    if tree.returncode != 0:
        pytest.skip("this Git refuses to write a '..' tree entry")
    commit = run("commit-tree", tree.stdout.strip(), "-m", "evil")
    res = ss.run_cmd_gate(_item(trusted), "echo ran", commit)
    assert res["setup_failed"] and "safe relative path" in res["output_tail"]
    assert "ran" not in res["output_tail"]
    assert not (trusted["base"] / "escape").exists()
    assert not any(Path(os.environ["WATCHTOWER_SANDBOX_STAGE_ROOT"]).iterdir())


def test_native_materialize_bounds_are_enforced(native, trusted, monkeypatch):
    monkeypatch.setattr(ss, "MAX_FILES", 2)
    res = ss.run_cmd_gate(_item(trusted), "echo ran", trusted["head"])
    assert res["setup_failed"] and "limit" in res["output_tail"] and "ran" not in res["output_tail"]
    monkeypatch.setattr(ss, "MAX_FILES", 1000)
    monkeypatch.setattr(ss, "MAX_TREE_BYTES", 5)
    res = ss.run_cmd_gate(_item(trusted), "echo ran", trusted["head"])
    assert res["setup_failed"] and "byte limit" in res["output_tail"]


def test_gate_timeout_is_a_failure_not_a_hang(native, trusted, monkeypatch):
    monkeypatch.setattr(ss, "GATE_TIMEOUT_S", 3.0)
    res = ss.run_cmd_gate(_item(trusted), "sleep 60", trusted["head"])
    assert res["passed"] is False and "timed out" in res["output_tail"]
    assert not any(Path(os.environ["WATCHTOWER_SANDBOX_STAGE_ROOT"]).iterdir())


# ----------------------------------------------------------- directories


def test_release_removes_without_following_symlinks_and_is_bounded(base, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_SANDBOX_STAGE_ROOT", str(base / "stages"))
    roots = ss.allocate("gate")
    victim = base / "victim"
    victim.mkdir()
    (victim / "keep.txt").write_text("keep")
    ws = roots["workspace"]
    os.symlink(victim, ws / "to-victim")
    os.symlink(victim / "keep.txt", ws / "to-file")
    locked = ws / "locked" / "inner"
    locked.mkdir(parents=True)
    (locked / "f").write_text("x")
    os.chmod(locked, 0o000)
    os.chmod(ws / "locked", 0o500)
    assert ss.release(roots["dir"]) is True
    assert not roots["dir"].exists()
    assert (victim / "keep.txt").read_text() == "keep"
    # not a stage directory: refused, nothing removed
    outsider = base / "outsider"
    outsider.mkdir()
    assert ss.release(outsider) is False and outsider.exists()
    assert ss.release(base / "stages") is False
    # bounded: too many entries is a refusal that leaves the rest for a sweep
    roots = ss.allocate("gate")
    for i in range(6):
        (roots["workspace"] / f"f{i}").write_text("x")
    monkeypatch.setattr(ss, "MAX_REMOVE_ENTRIES", 3)
    assert ss.release(roots["dir"]) is False and roots["dir"].exists()
    monkeypatch.setattr(ss, "MAX_REMOVE_ENTRIES", 1000)
    assert ss.release(roots["dir"]) is True


def test_sweep_only_removes_old_owned_stage_directories(base, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_SANDBOX_STAGE_ROOT", str(base / "stages"))
    old, new = ss.allocate("gate"), ss.allocate("gate")
    stray = base / "stages" / "notes.txt"
    stray.write_text("keep")
    past = os.stat(old["dir"]).st_mtime - 10 * 86400
    os.utime(old["dir"], (past, past))
    assert ss.sweep() == 1
    assert not old["dir"].exists() and new["dir"].exists() and stray.exists()


# ------------------------------------------------- containing commit (group)


def test_native_containing_sha_matches_integration_git_semantics(native, trusted):
    repo = trusted["repo"]
    base_sha = trusted["head"]
    (repo / "b.txt").write_text("b")
    _git(repo, "add", "b.txt")
    _git(repo, "commit", "-qm", "b")
    second = _git(repo, "rev-parse", "HEAD")
    # a linear chain: the newest member contains the older one
    sha, source, problem = ss.containing_sha(repo, {"A": base_sha, "B": second})
    assert (sha, problem) == (second, "") and source.startswith("member:")
    # no-code members only: HEAD
    assert ss.containing_sha(repo, {"A": "", "B": ""}) == (second, "head", "")
    # a short (abbreviated) id resolves; a name never does
    assert ss.containing_sha(repo, {"A": base_sha[:12]})[0] == base_sha
    assert ss.containing_sha(repo, {"A": "HEAD"})[2].startswith("commit_missing")
    assert ss.containing_sha(repo, {"A": "0" * 40})[2].startswith("commit_missing")
    # diverged branches: nothing contains both
    trunk = _git(repo, "rev-parse", "--abbrev-ref", "HEAD")
    _git(repo, "checkout", "-q", "-b", "side", base_sha)
    (repo / "c.txt").write_text("c")
    _git(repo, "add", "c.txt")
    _git(repo, "commit", "-qm", "c")
    side = _git(repo, "rev-parse", "HEAD")
    _git(repo, "checkout", "-q", trunk)
    assert ss.containing_sha(repo, {"A": second, "B": side}) == ("", "", "diverged")
    assert _git(repo, "status", "--porcelain") == ""      # host repo untouched
    assert not any(Path(os.environ["WATCHTOWER_SANDBOX_STAGE_ROOT"]).iterdir())


def test_containing_sha_refuses_non_repositories_and_never_uses_host_git(base, monkeypatch):
    import subprocess as sp_
    monkeypatch.setenv("WATCHTOWER_SANDBOX_STAGE_ROOT", str(base / "stages"))
    monkeypatch.setattr(sp_, "run", lambda *a, **k: pytest.fail("host subprocess"))
    plain = base / "plain"
    plain.mkdir()
    assert ss.containing_sha(plain, {"A": "a" * 40})[2].startswith("sandbox_restricted")
    assert ss.containing_sha("", {"A": "a" * 40})[2].startswith("sandbox_restricted")

"""`wt defer --awaits`: a fix that is not on the release branch cannot be
parked as "awaiting release" (BECKY delivery-path review, 2026-11-14: 76
commits sat on a branch no release reads while their tickets waited "for
release"). Source-only holds and plain data defers are unchanged."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from tests.test_owner_view import _load, _ticket  # noqa: F401
from watchtower import release_wait


def _git(repo, *args):
    done = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True)
    assert done.returncode == 0, done.stderr
    return done.stdout.strip()


@pytest.fixture
def repo(tmp_path):
    """`origin/next` with one commit; a feature branch with a stranded fix; and
    a cherry-pick of a second fix onto next under a different sha."""
    r = tmp_path / "code"
    r.mkdir()
    _git(r, "init", "-q", "-b", "next")
    _git(r, "config", "user.email", "t@example.com")
    _git(r, "config", "user.name", "t")

    def commit(name, text):
        (r / name).write_text(text)
        _git(r, "add", name)
        _git(r, "commit", "-q", "-m", f"add {name}")
        return _git(r, "rev-parse", "HEAD")

    base = commit("base.txt", "base\n")
    _git(r, "checkout", "-q", "-b", "fix/feature")
    stranded = commit("stranded.txt", "only on the feature branch\n")
    relanded_src = commit("relanded.txt", "picked onto next\n")
    _git(r, "checkout", "-q", "next")
    merged = commit("merged.txt", "on next\n")
    _git(r, "cherry-pick", relanded_src)
    _git(r, "update-ref", "refs/remotes/origin/next", "HEAD")
    _git(r, "checkout", "-q", "fix/feature")          # the checkout sits on the wrong branch
    return {"path": str(r), "base": base, "stranded": stranded, "relanded": relanded_src,
            "merged": merged}


def test_on_branch_tells_contained_equivalent_missing_and_unknown(repo, tmp_path):
    at = lambda sha, branch="origin/next": release_wait.on_branch(repo["path"], sha, branch)["state"]  # noqa: E731
    assert at(repo["merged"]) == "contained" and at(repo["base"]) == "contained"
    assert at(repo["relanded"]) == "equivalent"            # same patch, other sha
    assert at(repo["stranded"]) == "missing"
    assert at(repo["stranded"], "fix/feature") == "contained"
    assert at("deadbeef") == "unknown" and at(repo["merged"], "origin/nope") == "unknown"
    assert release_wait.on_branch(str(tmp_path / "gone"), repo["merged"], "origin/next")["state"] == "unknown"


def test_release_claim_needs_proof_and_integration_is_retired(repo):
    ok = release_wait.check("release", repo["path"], repo["merged"], "origin/next")
    assert ok["tag"].startswith("[awaits release: ") and "on origin/next" in ok["tag"]
    assert "same patch on" in release_wait.check("release", repo["path"], repo["relanded"], "origin/next")["tag"]
    with pytest.raises(ValueError, match="not on origin/next, so no release will carry it"):
        release_wait.check("release", repo["path"], repo["stranded"], "origin/next")
    with pytest.raises(ValueError, match="cannot confirm"):          # unknown is never "yes"
        release_wait.check("release", repo["path"], repo["merged"], "origin/nope")
    with pytest.raises(ValueError, match="--commit SHA and --release-branch"):
        release_wait.check("release", repo["path"])
    assert "integration" not in release_wait.AWAITS
    for extra in ({}, {"owner": "session 01a1dafb", "path": "cherry-pick to next"}):
        with pytest.raises(ValueError, match="Land it yourself"):
            release_wait.check("integration", repo["path"], repo["stranded"], **extra)
    assert release_wait.check("", repo["path"]) == {"awaits": "data", "tag": ""}


@pytest.fixture(params=["file", "github"])
def env(request, tmp_path, monkeypatch):
    return _load(tmp_path, monkeypatch, request.param)


def test_defer_refuses_awaiting_release_for_a_commit_not_on_the_release_branch(env, repo, capsys):
    q, cli, _a, _ov, name = env
    ref = _ticket(q, name)
    base = ["defer", ref, "--worker", "w1", "--until", "+1d", "--repo", repo["path"]]
    rc = cli.main(base + ["--reason", "ships with the next release", "--awaits", "release",
                          "--commit", repo["stranded"], "--release-branch", "origin/next"])
    assert rc == 1 and "no release will carry it" in capsys.readouterr().err
    item = q.get(ref)
    assert item["status"] == "in_progress" and not item.get("deferred_until")     # nothing written

    before = q.get(ref)
    for extra in ([], ["--integration-owner", "session 01a1dafb",
                       "--integration-path", "cherry-pick to next via PR"]):
        rc = cli.main(base + ["--reason", "needs re-landing", "--awaits", "integration",
                              "--commit", repo["stranded"], *extra])
        err = capsys.readouterr().err
        assert rc == 1 and "Land it yourself" in err
        assert "wt edit PARENT --after CHILD" in err
        assert "wt block --wait-on owner|worker --wait-actor WHO" in err
        assert q.get(ref) == before  # reject without mutating either backend


def test_defer_accepts_awaiting_release_when_the_fix_is_on_the_release_branch(env, repo, capsys):
    q, cli, _a, _ov, name = env
    ref = _ticket(q, name)
    rc = cli.main(["defer", ref, "--worker", "w1", "--until", "+1d", "--repo", repo["path"],
                   "--reason", "ships with the next release", "--awaits", "release",
                   "--commit", repo["relanded"], "--release-branch", "origin/next"])
    assert rc == 0 and "note:" not in capsys.readouterr().err
    assert q.get(ref)["defer_reason"].startswith("[awaits release: ")


def test_plain_and_source_only_defers_still_work(env, repo, capsys):
    """A data window or a deliberate hold on unmerged source is not a release claim."""
    q, cli, _a, _ov, name = env
    hold = _ticket(q, name)
    rc = cli.main(["defer", hold, "--worker", "w1", "--until", "+2d",
                   "--reason", "source ready on fix/feature; holding until 3 days of audit rows exist"])
    assert rc == 0 and "note:" not in capsys.readouterr().err
    assert q.get(hold)["defer_reason"].startswith("source ready")

    data = _ticket(q, name, "data", "w2")
    assert cli.main(["defer", data, "--worker", "w2", "--until", "+2d", "--awaits", "data",
                     "--reason", "wait for the release metrics window"]) == 0
    assert "note:" not in capsys.readouterr().err          # said what it is: no nag

    vague = _ticket(q, name, "vague", "w3")
    rc = cli.main(["defer", vague, "--worker", "w3", "--until", "+2d",
                   "--reason", "pushed; awaiting release"])
    assert rc == 0                                          # compatibility: advice, not refusal
    assert "does not say what it waits on" in capsys.readouterr().err
    assert q.get(vague)["defer_reason"] == "pushed; awaiting release"


@pytest.mark.parametrize("repo_source", ["queue", "cwd"])
def test_defer_cli_resolves_repo_without_an_explicit_path(wt_env, repo, repo_source):
    """Workers can defer pathless tickets using queue config or their checkout."""
    awaits = "release"
    ref = _ticket(wt_env.queue, "T")
    assert not wt_env.queue.get(ref).get("repo_path")
    if repo_source == "queue":
        wt_env.config.set_repo_path("T", repo["path"])
    cwd = wt_env.tmp if repo_source == "queue" else Path(repo["path"])
    flags = ["--commit", repo["merged"], "--release-branch", "origin/next"]
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]))
    done = subprocess.run(
        [sys.executable, "-m", "watchtower.cli", "defer", ref, "--worker", "w1",
         "--until", "+30m", "--reason", "waiting for adoption", "--awaits", awaits,
         "--json", *flags],
        cwd=cwd, env=env, capture_output=True, text=True, timeout=30)
    assert done.returncode == 0, done.stderr
    item = json.loads(done.stdout)
    stored = wt_env.queue.get(ref)
    assert item["ref"] == ref and stored["status"] == "open"
    assert stored.get("deferred_until") and not stored.get("claimed_by")
    assert stored["defer_reason"].startswith(f"[awaits {awaits}")
    assert wt_env.queue.count_claimable(project="T") == 0

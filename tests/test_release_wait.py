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
    for retired in ("", "data"):
        with pytest.raises(ValueError, match="retired"):
            release_wait.check(retired, repo["path"])


@pytest.fixture(params=["file", "github"])
def env(request, tmp_path, monkeypatch):
    return _load(tmp_path, monkeypatch, request.param)


@pytest.mark.parametrize("awaits", ["data", "release", "integration"])
def test_defer_is_retired_for_every_old_spelling(env, repo, capsys, awaits):
    q, cli, _a, _ov, name = env
    ref = _ticket(q, name)
    before = q.get(ref)
    assert cli.main(["defer", ref, "--worker", "w1", "--until", "+1d", "--awaits", awaits,
                     "--reason", "future data", "--commit", repo["merged"],
                     "--release-branch", "origin/next"]) == 1
    assert "never wait on a clock" in capsys.readouterr().err
    assert q.get(ref) == before
    with pytest.raises(ValueError, match="retired"):
        q.defer(ref, "w1", "+1d", "future data")

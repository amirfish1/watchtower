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
from watchtower import release_wait, releases


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


def test_after_close_holds_requested_claim_until_live_acceptance(env, repo, capsys):
    q, cli, _a, _ov, name = env
    prerequisite = _ticket(q, name, note="fix to deploy")
    consumer = _ticket(q, name, note="verify downstream", worker="prior-worker")
    q.mark_runnable(consumer)
    assert cli.main(["edit", consumer, "--after-close", prerequisite]) == 0
    capsys.readouterr()

    target = "watchtower:local"
    parked = releases.park(prerequisite, "w1", repo=repo["path"], commit=repo["merged"],
                           branch="origin/next", target=target, reason="waiting for the build")
    assert parked["status"] == "awaiting_release"
    dependent = q.get(consumer)
    assert dependent["status"] == "open"
    assert dependent["run_requested"] is True
    assert q.waiting_on(dependent, {prerequisite: parked}) == [prerequisite]
    with pytest.raises(ValueError, match="waiting"):
        q.claim_by_ref(consumer, "worker")
    assert q.count_manual_eligible(project=name) == 0

    claim = ["claim", "-q", name, "--worker", "worker", "--requested-only", "--json"]
    assert cli.main(claim) == 0
    assert not capsys.readouterr().out.strip()
    source_work = q.enqueue(project=name, note="integrate source", blocked_by=[prerequisite])
    q.mark_runnable(source_work["ref"])
    assert cli.main(claim) == 0
    assert json.loads(capsys.readouterr().out)["ref"] == source_work["ref"]
    q.close(source_work["ref"], "worker", resolution={"summary": "Integrated", "no_code": True})

    observed = releases.observe(target, repo["merged"], evidence="test build", items=[parked])
    assert observed == [{"ref": prerequisite, "status": "open"}]
    q.claim_by_ref(prerequisite, "release-worker")
    assert cli.main(claim) == 0
    assert not capsys.readouterr().out.strip()
    q.close(prerequisite, "release-worker", resolution={"summary": "Live check passed", "no_code": True})
    assert cli.main(claim) == 0
    assert json.loads(capsys.readouterr().out)["ref"] == consumer


def test_add_after_close_retains_other_dependencies_and_clears_modes(env, capsys):
    q, cli, _a, _ov, name = env
    live = _ticket(q, name, note="live prerequisite")
    source = _ticket(q, name, note="source prerequisite", worker="other-worker")
    assert cli.main(["add", "-q", name, "--note", "consumer", "--no-dispatch", "--json",
                     "--after-close", live.lower() + ":live", "--after", source]) == 0
    dependent = json.loads(capsys.readouterr().out)
    ref = dependent["ref"]
    assert set(dependent["blocked_by"]) == {live, source}
    assert dependent["blocked_by_until_closed"] == [live]
    assert cli.main(["edit", live, "--after-close", ref]) == 1
    assert "dependency cycle" in capsys.readouterr().err

    assert cli.main(["edit", ref, "--after", source]) == 0
    assert q.get(ref)["blocked_by_until_closed"] == [live]
    assert cli.main(["edit", ref, "--clear-after", "--after", live]) == 0
    assert q.get(ref)["blocked_by"] == [live]
    assert not q.get(ref).get("blocked_by_until_closed")


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

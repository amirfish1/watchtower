"""Cross-repo close proofs (`wt close --commit <SHA>`).

Own module rather than test_smoke.py so the search-order and remote-parsing
rules stay legible together.

Context: the VM-NEXT queue tracking the 2026-08-10 Chuck incident had tickets
in five different places while the queue had one repo_path, so genuinely
committed work could not be closed. The fix widens WHERE a proof is looked for
without weakening WHAT counts as one -- these tests pin both halves.
"""

import subprocess

import pytest

from watchtower import close_proof


def _git(repo, *args):
    subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True)


@pytest.fixture
def repo_with_commit(tmp_path):
    """A real git repo with one commit; returns (path, sha)."""
    def _make(name):
        repo = tmp_path / name
        repo.mkdir()
        _git(repo, "init", "-q")
        _git(repo, "config", "user.email", "t@example.com")
        _git(repo, "config", "user.name", "T")
        (repo / "f.txt").write_text(name)
        _git(repo, "add", "f.txt")
        _git(repo, "commit", "-qm", f"commit in {name}")
        sha = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True
        ).stdout.strip()
        return str(repo), sha
    return _make


def test_resolves_in_the_primary_repo(repo_with_commit):
    repo, sha = repo_with_commit("primary")
    verified, found_in = close_proof.verify(sha[:10], repo)
    assert verified == sha
    assert found_in == repo


def test_resolves_in_a_secondary_repo_when_absent_from_primary(repo_with_commit):
    """The VM-NEXT case: the work is real and committed, just not in the repo
    the queue happens to name."""
    primary, _ = repo_with_commit("primary")
    other, other_sha = repo_with_commit("other")
    verified, found_in = close_proof.verify(other_sha, primary, extra=[other])
    assert verified == other_sha
    assert found_in == other


def test_primary_is_searched_first(repo_with_commit):
    primary, primary_sha = repo_with_commit("primary")
    other, _ = repo_with_commit("other")
    assert close_proof.search_order(primary, [other])[0] == primary


def test_a_sha_in_no_repo_is_still_refused(repo_with_commit):
    """Widening the search must not become accepting SHA-shaped strings --
    that would let dirty work masquerade as a committed resolution."""
    primary, _ = repo_with_commit("primary")
    other, _ = repo_with_commit("other")
    verified, found_in = close_proof.verify("deadbeef", primary, extra=[other])
    assert verified == ""
    assert found_in == ""


def test_non_sha_input_is_rejected_without_touching_git(repo_with_commit):
    primary, _ = repo_with_commit("primary")
    assert close_proof.verify("not-a-sha", primary) == ("", "")
    assert close_proof.verify("", primary) == ("", "")


def test_missing_directory_is_a_miss_not_a_crash(repo_with_commit):
    """One dead candidate must not abort the search across the others."""
    other, other_sha = repo_with_commit("other")
    verified, found_in = close_proof.verify(
        other_sha, "/nonexistent/repo/path", extra=[other]
    )
    assert verified == other_sha
    assert found_in == other


def test_search_order_dedupes_and_drops_blanks(repo_with_commit):
    repo, _ = repo_with_commit("primary")
    order = close_proof.search_order(repo, [repo, "", "   "])
    assert order.count(repo) == 1
    assert "" not in order


def test_remote_spec_is_parsed_as_ssh_not_a_local_path():
    argv, err = close_proof._rev_parse_argv("hermes:/home/h/projects/x", "abc1234")
    assert err == ""
    assert argv[0] == "ssh"
    assert "hermes" in argv
    assert "-C" in argv and "/home/h/projects/x" in argv
    # Same verification command as the local path -- a remote proof is exactly
    # as strong as a local one, not a trust fallback.
    assert "rev-parse" in argv and "--verify" in argv


def test_relative_colon_path_is_not_treated_as_remote():
    """`repo:sha`-style typos must not silently become network lookups."""
    argv, err = close_proof._rev_parse_argv("myrepo:notabsolute", "abc1234")
    assert argv == []
    assert "not a directory" in err

"""gh auth-failure self-heal + hold (2026-09-17 incident).

Five GitHub-backed queues ERRORed "To get started with GitHub CLI, please
run: gh auth login" in bursts while an interactive `gh auth status` in a
normal shell was fine. These tests pin the two defences added in response:
the self-heal retry in `_run` (transient read / masking env token) and the
`auth_broken_until` connectivity hold that stops per-queue ERROR spam while
auth is genuinely broken -- and that a live success clears on its own.
"""

import subprocess
from datetime import datetime, timezone

import pytest

import watchtower.github_backend as github_backend


def _proc(returncode, stdout="", stderr=""):
    return subprocess.CompletedProcess(
        args=["gh"], returncode=returncode, stdout=stdout, stderr=stderr
    )


@pytest.fixture(autouse=True)
def isolated_state(tmp_path, monkeypatch):
    """Keep the connectivity file and the scrub latch out of the developer's
    real ~/.watchtower and out of whichever test runs next."""
    monkeypatch.setenv(
        "WATCHTOWER_GH_CONNECTIVITY_FILE", str(tmp_path / "gh-connectivity.json")
    )
    monkeypatch.setattr(github_backend, "_GH_SCRUB_ENV", False)


def test_auth_error_signatures_are_detected():
    assert github_backend._is_auth_error(
        "To get started with GitHub CLI, please run:  gh auth login"
    )
    assert github_backend._is_auth_error("gh: Bad credentials (HTTP 401)")
    assert github_backend._is_auth_error("error: authentication required")
    assert not github_backend._is_auth_error("API rate limit already exceeded")
    assert not github_backend._is_auth_error("HTTP 503: Service Unavailable")
    assert not github_backend._is_auth_error("")


def test_auth_error_triggers_self_heal_retry_and_recovers(tmp_path, monkeypatch):
    """The observed incident shape: first call fails with the auth signature,
    the heal proves auth is fine, the retry succeeds -- no exception, no hold."""
    backend = github_backend.GitHubIssuesBackend("T", repo="acme/auth-heal")
    calls = []

    def flaky_raw(args):
        calls.append(args)
        if len(calls) == 1:
            return _proc(
                1,
                stderr="To get started with GitHub CLI, please run:  gh auth login",
            )
        return _proc(0, stdout="[]")

    monkeypatch.setattr(backend, "_run_raw", flaky_raw)
    monkeypatch.setattr(github_backend, "_attempt_auth_self_heal", lambda: True)

    out = backend._run(["issue", "list", "--repo", backend.repo])

    assert out == "[]"
    assert len(calls) == 2, "the original command is retried exactly once"
    state = github_backend._load_connectivity()
    assert state["auth_broken_until"] is None
    assert github_backend._gh_backoff_active()[0] is False


def test_failed_self_heal_sets_the_auth_hold(tmp_path, monkeypatch):
    """When the heal cannot recover, one failure must hold live calls off so
    every queue stops ERRORing every poll until auth is repaired."""
    backend = github_backend.GitHubIssuesBackend("T", repo="acme/auth-broken")
    monkeypatch.setattr(
        backend,
        "_run_raw",
        lambda args: _proc(1, stderr="gh auth login"),
    )
    monkeypatch.setattr(github_backend, "_attempt_auth_self_heal", lambda: False)

    with pytest.raises(github_backend.GitHubBackendError):
        backend._run(["issue", "list", "--repo", backend.repo])

    state = github_backend._load_connectivity()
    assert state["auth_broken_until"], "the hold must be persisted"
    assert state["last_error"] == "gh auth login"
    assert state["broken_since"], "the outage clock starts on the first break"
    active, _ = github_backend._gh_backoff_active()
    assert active is True, "an auth outage must suppress live calls"
    hold = github_backend._parse_iso(state["auth_broken_until"])
    delta = (hold - datetime.now(timezone.utc)).total_seconds()
    assert delta > 600, "the auth hold must outlast the generic ladder's cap"


def test_auth_hold_extends_next_retry_without_escalating_the_ladder(tmp_path):
    """Like the rate-limit hold: GitHub is reachable, auth is just broken, so
    the unreachability ladder (consecutive_failures) must not escalate."""
    github_backend._record_gh_auth_broken("gh auth login")
    state = github_backend._load_connectivity()
    assert state["consecutive_failures"] == 0
    assert state["next_retry_at"] == state["auth_broken_until"]


def test_a_successful_fetch_clears_the_auth_hold(tmp_path):
    """This is what makes the outage AUTO-FIXED: once auth is repaired out of
    band, the next live success reopens polling on its own."""
    github_backend._record_gh_auth_broken("gh auth login")
    assert github_backend._gh_backoff_active()[0] is True

    github_backend._record_gh_success()
    state = github_backend._load_connectivity()
    assert state["auth_broken_until"] is None
    assert github_backend._gh_backoff_active()[0] is False


def test_self_heal_treats_a_passing_auth_status_as_transient(tmp_path, monkeypatch):
    """hosts.yml mid-rewrite by another gh process: the status probe passes
    right after the original command failed -- no scrubbing needed."""
    runs = []

    def fake_run(cmd, **kw):
        runs.append((cmd, kw))
        return _proc(0, stdout="Logged in to github.com")

    monkeypatch.setattr(github_backend.subprocess, "run", fake_run)
    monkeypatch.delenv("GH_TOKEN", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)

    assert github_backend._attempt_auth_self_heal() is True
    assert [cmd for cmd, _ in runs] == [["gh", "auth", "status"]]
    assert github_backend._GH_SCRUB_ENV is False


def test_self_heal_scrubs_a_masking_env_token(tmp_path, monkeypatch):
    """A stale GH_TOKEN outranks valid stored auth in gh: when the probe only
    passes with the token scrubbed, latch the scrub for the whole process."""
    config_dir = tmp_path / "gh-config"
    config_dir.mkdir()
    (config_dir / "hosts.yml").write_text(
        "github.com:\n    oauth_token: gho_testtoken\n    user: tester\n"
    )
    monkeypatch.setenv("GH_CONFIG_DIR", str(config_dir))
    monkeypatch.setenv("GH_TOKEN", "stale-token")

    envs = []

    def fake_run(cmd, **kw):
        envs.append(kw.get("env"))
        # Fails under the masking env, passes once the token is scrubbed.
        return _proc(1) if len(envs) == 1 else _proc(0)

    monkeypatch.setattr(github_backend.subprocess, "run", fake_run)

    assert github_backend._attempt_auth_self_heal() is True
    assert github_backend._GH_SCRUB_ENV is True
    assert len(envs) == 2
    assert envs[0].get("GH_TOKEN") == "stale-token"
    assert "GH_TOKEN" not in envs[1]
    # The latch is what `_run_raw` consults for every later gh call.
    child = github_backend._gh_child_env()
    assert "GH_TOKEN" not in child


def test_self_heal_gives_up_without_stored_auth(tmp_path, monkeypatch):
    """No env token to blame and no oauth_token in hosts.yml: a real outage,
    reported as unhealable so the caller sets the hold."""
    config_dir = tmp_path / "gh-config"
    config_dir.mkdir()
    (config_dir / "hosts.yml").write_text("github.com:\n    user: tester\n")
    monkeypatch.setenv("GH_CONFIG_DIR", str(config_dir))
    monkeypatch.delenv("GH_TOKEN", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.setattr(
        github_backend.subprocess, "run", lambda cmd, **kw: _proc(1)
    )

    assert github_backend._attempt_auth_self_heal() is False
    assert github_backend._GH_SCRUB_ENV is False


def test_self_heal_never_raises(monkeypatch):
    """A blowing-up probe (gh missing, timeout, ...) is a plain False."""

    def boom(cmd, **kw):
        raise OSError("gh vanished")

    monkeypatch.setattr(github_backend.subprocess, "run", boom)
    assert github_backend._attempt_auth_self_heal() is False

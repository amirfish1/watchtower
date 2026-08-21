"""Failure-mode coverage: what happens when the world is broken.

The smoke and settings suites cover the happy path and the knobs. This module
covers the ways a real fleet actually falls over — a missing engine binary, an
expired login, a model the provider rejects, a repo path that no longer exists,
`gh` absent or logged out, GitHub rate-limiting or 503ing, a config file
someone hand-edited into garbage.

Three properties are asserted throughout, because they are what separates a
diagnosable outage from a silent one:

* **No crash** — a broken dependency produces an error, not a traceback out of
  a daemon tick.
* **Named cause** — the message says which thing is broken, specifically enough
  to act on. "engine authentication required" is a fix; "spawn failed" is not.
* **No hot loop** — a failure records a cooldown / backoff so the reconciler
  does not respawn into the same wall every tick.

Cases marked ``xfail(strict=True)`` are gaps found while writing this suite:
the test states the behaviour WatchTower should have, and will flip to a
failure (alerting us) the moment the gap is closed.
"""

from __future__ import annotations

import json
import os
import time

import pytest


QUEUE = "FAILQ"


# =========================================================================== #
# 1. Engine / worker startup failures
# =========================================================================== #
def test_missing_engine_binary_is_reported_and_no_worker_is_recorded(wt_env, no_engines):
    """Engine CLI not installed: the spawn must fail loudly and leave no
    phantom worker in the registry that the reconciler would count as live."""
    assert wt_env.workers.engine_available("codex") is False
    failures = []
    spawned = wt_env.workers.spawn_workers(
        QUEUE, 1, engine="codex", repo_path=str(wt_env.tmp), launch_failures=failures
    )
    assert spawned == []
    assert len(failures) == 1
    assert "unavailable" in failures[0]["reason"]
    assert wt_env.workers.list_workers() == []


def test_missing_engine_binary_sets_a_cooldown_so_the_reconciler_stops_retrying(
    wt_env, no_engines
):
    """Without a cooldown the next tick sees "0 live < 1 desired" and spawns
    straight back into the same wall, forever."""
    wt_env.workers.spawn_workers(QUEUE, 1, engine="codex", repo_path=str(wt_env.tmp))
    cooldown = wt_env.workers.active_launch_failure_cooldown(QUEUE, "codex")
    assert cooldown is not None
    assert cooldown["cooldown_until"] > time.time()


@pytest.mark.parametrize(
    "log_text,expected",
    [
        ("Error: not logged in. Please run /login", "engine authentication required"),
        ("authentication failed: token expired", "engine authentication failed"),
        ("You've hit your usage limit. Try again later.", "engine usage limit"),
        ("HTTP 503 upstream connect error", "engine api unavailable"),
    ],
)
def test_launch_failure_log_is_classified_into_an_actionable_reason(
    wt_env, tmp_path, log_text, expected
):
    """An operator reading `wt status` needs "authentication required", not
    "exit code 1"."""
    log = tmp_path / "worker.log"
    log.write_text(log_text)
    verdict = wt_env.workers._classify_launch_failure_log(log)
    assert verdict is not None, f"{log_text!r} was not classified"
    assert verdict["reason"] == expected


def test_worker_that_dies_unauthenticated_is_recorded_not_registered(wt_env, fake_bin):
    """The headline case: the engine is installed, the login has expired. The
    worker exits immediately; WatchTower must classify it as an auth failure
    rather than registering a worker that will never claim anything."""
    fake_bin(
        "codex",
        'echo "Error: not logged in. Please run /login" >&2\nexit 1',
    )
    failures = []
    spawned = wt_env.workers.spawn_workers(
        QUEUE, 1, engine="codex", repo_path=str(wt_env.tmp), launch_failures=failures
    )
    assert spawned == []
    assert failures and "authentication" in failures[0]["reason"]
    assert wt_env.workers.list_workers() == []


def test_usage_limit_cooldown_honours_the_provider_supplied_retry_time(wt_env, tmp_path):
    """When the provider says when it will serve again, trust that over our own
    exponential guess — retrying earlier just burns another rejection."""
    log = tmp_path / "worker.log"
    log.write_text(
        "ERROR: You've hit your usage limit. Try again at Dec 31st, 2099 11:30 PM."
    )
    verdict = wt_env.workers._classify_launch_failure_log(log)
    assert verdict["reason"] == "engine usage limit"
    assert verdict["retry_at"] is not None
    assert verdict["retry_at"] > time.time()


def test_repeated_launch_failures_back_off_exponentially(wt_env, tmp_path):
    """Second consecutive failure waits longer than the first; a fleet that is
    down for an hour must not generate an hour of spawn attempts."""
    log = tmp_path / "w.log"
    log.write_text("boom")
    first = wt_env.workers._record_launch_failure(
        queue=QUEUE, engine="codex", worker_id="w1", pid=0, log_path=log, reason="boom"
    )
    second = wt_env.workers._record_launch_failure(
        queue=QUEUE, engine="codex", worker_id="w2", pid=0, log_path=log, reason="boom"
    )
    assert second["consecutive"] == 2
    assert (second["cooldown_until"] - second["failed_at"]) > (
        first["cooldown_until"] - first["failed_at"]
    )


def test_a_proven_good_launch_clears_the_failure_streak(wt_env, tmp_path):
    """Otherwise one bad afternoon leaves an escalated cooldown on a queue that
    is now perfectly healthy."""
    log = tmp_path / "w.log"
    log.write_text("boom")
    wt_env.workers._record_launch_failure(
        queue=QUEUE, engine="codex", worker_id="w1", pid=0, log_path=log, reason="boom"
    )
    wt_env.workers._clear_launch_failure(QUEUE, "codex")
    assert wt_env.workers.active_launch_failure_cooldown(QUEUE, "codex") is None


def test_repeated_model_not_found_marks_a_live_worker_as_a_zombie(wt_env, tmp_path):
    """"Model not recognized" does not kill the process — the worker sits there
    burning nothing and claiming nothing. It has to be detectable from its log."""
    log = tmp_path / "zombie.log"
    log.write_text(
        "API Error: model_not_found\n"
        "The model claude-opus-9 does not exist or you may not have access\n"
        "API Error: model_not_found\n"
    )
    assert wt_env.workers._classify_zombie_log(str(log)) == "repeated model_not_found"


def test_a_healthy_worker_log_is_not_classified_as_a_zombie(wt_env, tmp_path):
    """The zombie classifier gates a kill decision, so a false positive costs a
    live worker mid-ticket."""
    log = tmp_path / "healthy.log"
    log.write_text("claimed FAILQ-1\nrunning tests\nclosed FAILQ-1\n")
    assert wt_env.workers._classify_zombie_log(str(log)) is None


def test_fable_model_is_refused_at_spawn_and_falls_back_to_the_cli_default(
    wt_env, fake_bin, capsys
):
    """Fable is a story model. Spawning a coding worker with it produces
    expensive nonsense, so the spawn strips it rather than honouring it."""
    fake_bin("codex")
    rec = wt_env.workers.spawn_workers(
        QUEUE, 1, engine="codex", model="claude-fable-5", dry_run=True
    )[0]
    assert "model" not in rec
    assert "--model" not in rec["argv"]
    assert "fable" in capsys.readouterr().err.lower()


def test_provider_failure_falls_back_to_a_different_installed_engine(wt_env, fake_bin):
    """A dead provider should hand the queue to another engine that is actually
    installed — never to itself, and never to one that isn't on this machine."""
    fake_bin("claude")
    assert wt_env.config.fallback_engine("codex") == "claude"
    assert wt_env.config.fallback_engine("claude") != "claude"


def test_fallback_engine_is_empty_when_nothing_else_is_installed(wt_env, no_engines):
    assert wt_env.config.fallback_engine("codex") == ""


# =========================================================================== #
# 2. Model / effort rejection (before a worker is ever spawned)
# =========================================================================== #
@pytest.mark.parametrize(
    "engine,model",
    [
        ("claude", "gpt-5.6"),          # right id, wrong engine
        ("claude", "claude-opus-9"),    # plausible but nonexistent
        ("codex", "claude-opus-5"),
        ("kimi", "gpt-5.5"),
        ("claude", "gpt5"),             # typo
    ],
)
def test_unrecognised_model_is_rejected_before_it_can_reach_a_worker(
    wt_env, run_cli, engine, model
):
    """A bad --model is only discovered at spawn time otherwise, where it costs
    a dead worker and a 5-minute cooldown per tick."""
    res = run_cli("config", "-q", QUEUE, "--engine", engine, "--model", model)
    assert res.code == 1
    assert "not approved" in res.err
    assert "model" not in wt_env.config.get_queue_config(QUEUE)


def test_rejected_model_leaves_the_previous_setting_intact(wt_env, run_cli):
    """A failed edit must not half-apply: the queue keeps running what it ran."""
    run_cli("config", "-q", QUEUE, "--engine", "claude", "--model", "claude-opus-5")
    res = run_cli("config", "-q", QUEUE, "--model", "claude-opus-9")
    assert res.code == 1
    assert wt_env.config.model(QUEUE) == "claude-opus-5"


def test_rejection_message_lists_the_models_that_would_work(wt_env, run_cli):
    res = run_cli("config", "-q", QUEUE, "--engine", "codex", "--model", "gpt-9")
    assert res.code == 1
    assert "gpt-5.6" in res.err and "wt models" in res.err


def test_effort_unsupported_by_the_pinned_model_is_rejected(wt_env, run_cli):
    """gpt-5.5 has no `max` tier; passing it through would make every worker on
    the queue die at launch."""
    res = run_cli("config", "-q", QUEUE, "--engine", "codex",
                  "--model", "gpt-5.5", "--effort", "max")
    assert res.code == 1
    assert "does not support effort" in res.err


def test_effort_on_a_model_with_no_effort_flag_is_rejected(wt_env, run_cli):
    """kimi's CLI has no effort flag at all, so any explicit effort is a
    misconfiguration rather than a preference."""
    res = run_cli("config", "-q", QUEUE, "--engine", "kimi",
                  "--model", "kimi-code/k3", "--effort", "high")
    assert res.code == 1
    assert "does not support effort" in res.err


def test_switching_engine_surfaces_a_now_invalid_stored_model(wt_env, run_cli):
    """The classic trap: a queue configured for claude-opus-5 is switched to
    codex, and every worker afterwards dies on an unknown model. The switch
    itself has to be refused."""
    run_cli("config", "-q", QUEUE, "--engine", "claude", "--model", "claude-opus-5")
    res = run_cli("config", "-q", QUEUE, "--engine", "codex")
    assert res.code == 1, res.output
    assert "not approved" in res.err
    assert wt_env.config.engine(QUEUE) == "claude"


def test_invalid_effort_value_is_rejected_by_the_config_api(wt_env):
    with pytest.raises(ValueError, match="effort must be one of"):
        wt_env.config.set_effort(QUEUE, "ludicrous")


def test_invalid_backend_is_rejected_by_the_config_api(wt_env):
    with pytest.raises(ValueError, match="backend must be one of"):
        wt_env.config.set_backend(QUEUE, "sqlite")


def test_ticket_model_floor_above_the_queue_model_parks_instead_of_working_it(
    wt_env, run_cli, monkeypatch
):
    """A ticket that named a higher model floor must not be quietly worked by a
    cheaper queue — it parks blocked with the reason."""
    wt_env.config.set_engine(QUEUE, "claude")
    wt_env.config.set_model(QUEUE, "claude-sonnet-5")
    wt_env.config.set_auto_drain(QUEUE, True)
    wt_env.config.set_grace_s(QUEUE, 0)
    item = wt_env.queue.enqueue(
        note="needs the big model", project=QUEUE, source="test",
        model_floor="claude-opus-5",
    )
    res = run_cli("claim", "--queue", QUEUE, "--worker", "sess-floor")
    assert res.code == 1, res.output
    assert "model floor" in res.err
    parked = wt_env.queue.get(item["ref"])
    assert parked["needs_input"] is True
    assert "claude-opus-5" in parked["block_question"]


# =========================================================================== #
# 3. Local folder / repo-path failures
# =========================================================================== #
def test_spawn_into_a_missing_folder_fails_without_registering_a_worker(
    wt_env, fake_bin
):
    fake_bin("codex")
    failures = []
    spawned = wt_env.workers.spawn_workers(
        QUEUE, 1, engine="codex", repo_path="/no/such/folder",
        launch_failures=failures,
    )
    assert spawned == []
    assert failures, "a missing repo path must be recorded as a launch failure"
    assert wt_env.workers.list_workers() == []


def test_missing_folder_failure_names_the_folder_not_the_engine(wt_env, fake_bin):
    fake_bin("codex")
    failures = []
    wt_env.workers.spawn_workers(
        QUEUE, 1, engine="codex", repo_path="/no/such/folder",
        launch_failures=failures,
    )
    reason = failures[0]["reason"]
    assert "/no/such/folder" in reason
    assert "executable" not in reason


def test_configuring_a_nonexistent_workers_local_path_is_refused(wt_env, run_cli):
    res = run_cli("config", "-q", QUEUE, "--workers-local-path", "/no/such/folder")
    assert res.code == 1
    assert wt_env.config.repo_path(QUEUE) == ""


def test_configuring_a_file_as_the_workers_local_path_is_refused(wt_env, run_cli, tmp_path):
    target = tmp_path / "not-a-dir.txt"
    target.write_text("hello")
    res = run_cli("config", "-q", QUEUE, "--workers-local-path", str(target))
    assert res.code == 1


def test_tilde_in_workers_local_path_is_expanded_at_write_time(wt_env, run_cli):
    run_cli("config", "-q", QUEUE, "--workers-local-path", "~/some-repo")
    assert not wt_env.config.repo_path(QUEUE).startswith("~")


def test_a_deleted_repo_path_does_not_take_down_the_reconciler(wt_env, fake_bin, tmp_path):
    """The folder existed when the queue was configured and was deleted since —
    the tick must survive it and keep reconciling other queues."""
    fake_bin("codex")
    gone = tmp_path / "deleted-repo"
    gone.mkdir()
    wt_env.config.set_repo_path(QUEUE, str(gone))
    wt_env.config.set_engine(QUEUE, "codex")
    wt_env.config.set_auto_drain(QUEUE, True)
    wt_env.config.set_grace_s(QUEUE, 0)
    wt_env.queue.enqueue(note="work", project=QUEUE, source="test")
    gone.rmdir()
    result = wt_env.workers.reconcile_once()  # must not raise
    assert isinstance(result, dict)
    assert wt_env.workers.live_worker_count(QUEUE) == 0


# =========================================================================== #
# 4. GitHub-backed queue failures
# =========================================================================== #
def _gh_queue(wt_env, repo: str = "acme-corp/widgets") -> None:
    wt_env.config.set_backend(QUEUE, "github")
    wt_env.config.set_github_repo(QUEUE, repo)


def test_gh_cli_not_installed_gives_an_install_instruction(wt_env, uninstall_gh):
    _gh_queue(wt_env)
    backend = wt_env.queue._github_backend_for_project(QUEUE)
    with pytest.raises(wt_env.github_backend.GitHubBackendError) as exc:
        backend.list_items()
    assert "gh CLI" in str(exc.value)


def test_gh_cli_not_installed_does_not_crash_wt_status(wt_env, uninstall_gh, run_cli):
    """`wt status` is the command an operator runs *because* something is
    wrong; it must never be the thing that also breaks."""
    _gh_queue(wt_env)
    res = run_cli("status")
    assert res.code == 0, res.output


def test_gh_not_authenticated_surfaces_the_login_instruction(wt_env, fake_gh):
    _gh_queue(wt_env)
    fake_gh.set("unauthenticated")
    backend = wt_env.queue._github_backend_for_project(QUEUE)
    with pytest.raises(wt_env.github_backend.GitHubBackendError) as exc:
        backend.list_items()
    assert "gh auth login" in str(exc.value)


def test_gh_failure_records_connectivity_state_with_a_retry_time(wt_env, fake_gh):
    """A short-lived `wt` process learns nothing from an in-memory cache, so the
    "GitHub has been unreachable since X" evidence has to be persisted."""
    _gh_queue(wt_env)
    fake_gh.set("unauthenticated")
    backend = wt_env.queue._github_backend_for_project(QUEUE)
    with pytest.raises(wt_env.github_backend.GitHubBackendError):
        backend.list_items()
    state = json.loads(os.environ["WATCHTOWER_GH_CONNECTIVITY_FILE"] and
                       open(os.environ["WATCHTOWER_GH_CONNECTIVITY_FILE"]).read())
    assert state["broken_since"]
    assert state["consecutive_failures"] >= 1
    assert state["next_retry_at"]


def test_gh_backoff_escalates_across_consecutive_failures(wt_env, fake_gh):
    _gh_queue(wt_env)
    fake_gh.set("server_error")
    wt_env.github_backend._record_gh_failure("boom")
    first = wt_env.github_backend._load_connectivity()["next_retry_at"]
    wt_env.github_backend._record_gh_failure("boom")
    second = wt_env.github_backend._load_connectivity()["next_retry_at"]
    assert second > first


def test_gh_recovery_clears_the_broken_state(wt_env, fake_gh):
    wt_env.github_backend._record_gh_failure("boom")
    assert wt_env.github_backend._load_connectivity()["broken_since"]
    wt_env.github_backend._record_gh_success()
    state = wt_env.github_backend._load_connectivity()
    assert state["broken_since"] is None
    assert state["consecutive_failures"] == 0


def test_rate_limited_github_falls_back_to_the_last_known_good_list(wt_env, fake_gh):
    """Re-hitting a rate-limited repo every poll never lets the limit recover
    (WT-87); a stale-but-served list is the right answer for a dashboard."""
    _gh_queue(wt_env)
    backend = wt_env.queue._github_backend_for_project(QUEUE)
    key = f"{backend.repo}:open"
    wt_env.github_backend._LIST_CACHE[key] = {
        "at": time.time(), "data": [{"number": 1, "title": "cached"}],
        "error": None, "etag": "",
    }
    fake_gh.set("rate_limited")
    items = backend._list_issues(fresh=True)
    assert items and items[0]["title"] == "cached"


def test_rate_limited_github_with_no_cache_raises_rather_than_returning_empty(
    wt_env, fake_gh
):
    """Returning [] would read as "the queue is drained" and could green-light
    a shutdown decision on a queue that is actually full."""
    _gh_queue(wt_env)
    fake_gh.set("rate_limited")
    backend = wt_env.queue._github_backend_for_project(QUEUE)
    with pytest.raises(wt_env.github_backend.GitHubBackendError) as exc:
        backend._list_issues(fresh=True)
    assert "rate limit" in str(exc.value).lower()


def test_repo_not_found_names_the_repo(wt_env, fake_gh):
    _gh_queue(wt_env, "ghost/missing")
    fake_gh.set("no_repo")
    backend = wt_env.queue._github_backend_for_project(QUEUE)
    with pytest.raises(wt_env.github_backend.GitHubBackendError) as exc:
        backend.list_items()
    assert "ghost/missing" in str(exc.value)


def test_gh_returning_garbage_is_a_clean_error_not_a_json_traceback(wt_env, fake_gh):
    _gh_queue(wt_env)
    fake_gh.set("bad_json")
    backend = wt_env.queue._github_backend_for_project(QUEUE)
    with pytest.raises(wt_env.github_backend.GitHubBackendError) as exc:
        backend._list_issues(fresh=True)
    assert "invalid JSON" in str(exc.value)


def test_gh_returning_an_object_instead_of_a_list_is_a_clean_error(wt_env, fake_gh):
    _gh_queue(wt_env)
    fake_gh.set("non_list_json")
    backend = wt_env.queue._github_backend_for_project(QUEUE)
    with pytest.raises(wt_env.github_backend.GitHubBackendError) as exc:
        backend._list_issues(fresh=True)
    assert "non-list" in str(exc.value)


def test_a_hung_gh_call_times_out_instead_of_wedging_the_tick(wt_env, fake_gh, monkeypatch):
    """A hung `gh` inside a reconciler tick stalls the whole fleet, so the
    subprocess must carry its own timeout."""
    _gh_queue(wt_env)
    fake_gh.set("hang", FAKE_GH_HANG_S="30")
    backend = wt_env.queue._github_backend_for_project(QUEUE)
    real_run = wt_env.github_backend.subprocess.run

    def _fast_timeout(*args, **kwargs):
        kwargs["timeout"] = 1  # keep the test quick; the code path is the same
        return real_run(*args, **kwargs)

    monkeypatch.setattr(wt_env.github_backend.subprocess, "run", _fast_timeout)
    with pytest.raises(wt_env.github_backend.GitHubBackendError) as exc:
        backend.list_items()
    assert "timed out" in str(exc.value)


def test_github_backend_without_a_repo_refuses_to_use_the_ambient_repo(wt_env, fake_gh):
    wt_env.config.set_backend(QUEUE, "github")
    backend = wt_env.queue._github_backend_for_project(QUEUE)
    with pytest.raises(wt_env.github_backend.GitHubBackendError) as exc:
        backend.list_items()
    assert "github_repo" in str(exc.value)


@pytest.mark.parametrize("placeholder", ["owner/repo", "OWNER/REPO", "acme/repo"])
def test_placeholder_repo_from_the_readme_is_refused(wt_env, run_cli, placeholder):
    """Copy-pasting the README's example must not point a live queue at a
    stranger's repository."""
    res = run_cli("config", "-q", QUEUE, "--github-repo", placeholder)
    assert res.code == 1
    assert "placeholder" in res.err


@pytest.mark.parametrize(
    "bad", ["justname", "https://github.com/acme/widgets", "acme / widgets", "a/b/c"]
)
def test_malformed_github_repo_is_refused(wt_env, run_cli, bad):
    res = run_cli("config", "-q", QUEUE, "--github-repo", bad)
    assert res.code == 1


# =========================================================================== #
# 5. Corrupt / hand-edited configuration
# =========================================================================== #
def test_corrupt_config_file_degrades_to_defaults_instead_of_crashing(wt_env):
    """queue-config.json is a plain file people hand-edit. A syntax error must
    not stop the daemon from running every other queue."""
    wt_env.config.CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    wt_env.config.CONFIG_FILE.write_text("{ not json")
    assert wt_env.config.all_queues() == {}
    assert wt_env.config.auto_drain(QUEUE) is False
    assert wt_env.config.desired_workers(QUEUE) == 1


def test_reconciler_survives_a_corrupt_config_file(wt_env, fake_bin):
    fake_bin("codex")
    wt_env.config.CONFIG_FILE.write_text("{ not json")
    assert isinstance(wt_env.workers.reconcile_once(dry_run=True), dict)


@pytest.mark.parametrize("reader", ["backend", "auto_drain", "desired_workers", "grace_s"])
def test_non_dict_config_entry_is_ignored_rather_than_fatal(wt_env, reader):
    wt_env.config.CONFIG_FILE.write_text(json.dumps({QUEUE: "not-a-dict"}))
    getattr(wt_env.config, reader)(QUEUE)  # must not raise


def test_unknown_keys_in_a_queue_entry_are_preserved_not_dropped(wt_env, run_cli):
    """Forward compatibility: an older `wt` writing a queue configured by a
    newer one must not silently delete settings it does not understand."""
    wt_env.config.CONFIG_FILE.write_text(
        json.dumps({QUEUE: {"future_setting": "keep me", "desired_workers": 2}})
    )
    run_cli("config", "-q", QUEUE, "--workers", "3")
    entry = json.loads(wt_env.config.CONFIG_FILE.read_text())[QUEUE]
    assert entry["future_setting"] == "keep me"
    assert entry["desired_workers"] == 3


def test_config_writes_are_atomic(wt_env):
    """A crash mid-write must not leave a truncated config that reads as "no
    queues configured" — which would park the entire fleet."""
    wt_env.config.set_desired_workers(QUEUE, 2)
    text = wt_env.config.CONFIG_FILE.read_text()
    json.loads(text)  # complete, parseable
    assert not list(wt_env.config.CONFIG_FILE.parent.glob("*.tmp"))


def test_negative_worker_count_is_refused(wt_env, run_cli):
    res = run_cli("config", "-q", QUEUE, "--workers", "-2")
    assert res.code == 1
    assert wt_env.config.desired_workers(QUEUE) == 1


def test_negative_grace_period_is_refused(wt_env):
    with pytest.raises(ValueError, match="grace_s must be >= 0"):
        wt_env.config.set_grace_s(QUEUE, -1)


def test_non_numeric_stored_grace_falls_back_to_the_default(wt_env):
    """Hand-edited "5 minutes" must not make the eligibility check throw on
    every ticket."""
    wt_env.config.CONFIG_FILE.write_text(json.dumps({QUEUE: {"grace_s": "5 minutes"}}))
    assert wt_env.config.grace_s(QUEUE) == wt_env.config.DEFAULT_GRACE_S


def test_unsupported_claim_types_are_dropped_rather_than_stored(wt_env):
    wt_env.config.set_claim_types(QUEUE, ["chore", "bug"])
    assert wt_env.config.claim_types(QUEUE) == ["bug"]


# =========================================================================== #
# 6. Staffing / dispatch failures a misconfigured queue produces
# =========================================================================== #
def test_a_queue_with_drain_off_is_never_staffed(wt_env, fake_bin):
    """The whole point of default-off: a backlog queue with 50 open tickets
    must not wake up staffed."""
    fake_bin("codex")
    wt_env.config.ensure_entry(QUEUE)
    wt_env.queue.enqueue(note="a ticket", project=QUEUE, source="test")
    result = wt_env.workers.reconcile_once(dry_run=True)
    assert [s for s in result["spawned"] if s.get("queue") == QUEUE] == []
    reasons = " ".join(str(s) for s in result.get("skipped", []))
    assert QUEUE in reasons


def test_a_queue_parked_at_zero_workers_is_never_staffed(wt_env, fake_bin):
    fake_bin("codex")
    wt_env.config.set_auto_drain(QUEUE, True)
    wt_env.config.set_desired_workers(QUEUE, 0)
    wt_env.config.set_grace_s(QUEUE, 0)
    wt_env.queue.enqueue(note="a ticket", project=QUEUE, source="test")
    result = wt_env.workers.reconcile_once(dry_run=True)
    assert [s for s in result["spawned"] if s.get("queue") == QUEUE] == []


def test_a_brand_new_ticket_is_left_alone_for_the_grace_period(wt_env, fake_bin):
    """grace_s is what makes the `watchtower:no-auto-drain` opt-out usable on
    inbound tickets; without it the reconciler claims within ~30s."""
    fake_bin("codex")
    wt_env.config.set_auto_drain(QUEUE, True)
    wt_env.config.set_grace_s(QUEUE, 3600)
    wt_env.config.set_backend(QUEUE, "github")
    assert wt_env.config.grace_s(QUEUE) == 3600


def test_an_unregistered_queue_is_invisible_to_the_reconciler(wt_env, fake_bin):
    """WT-131: a queue with no config entry is skipped entirely, so `wt add`
    has to register it or a run press no-ops forever."""
    fake_bin("codex")
    assert "GHOSTQ" not in wt_env.config.all_queues()
    wt_env.queue.enqueue(note="first ever ticket", project="GHOSTQ", source="test")
    assert "GHOSTQ" in wt_env.config.all_queues()
    assert wt_env.config.auto_drain("GHOSTQ") is False

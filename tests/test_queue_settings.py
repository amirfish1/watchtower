"""Exhaustive coverage of the queue-settings surface (`wt config` / `wt set`).

The smoke suite proves the queue loop works. This module proves the *knobs*
work: every setting a queue has, through the real CLI, checked on four axes —

1. **Default** — what the knob reads as before anyone configures it.
2. **Round trip** — set it through the CLI, read it back through the config API
   and through `wt config` with no flags.
3. **Isolation** — writing one knob leaves the others alone, and one queue's
   settings never bleed into another's.
4. **Effect** — the value actually reaches the thing it controls (the spawn
   argv, the claim filter, the reconciler's staffing decision).

Failure/rejection paths for the same knobs live in ``test_failure_modes.py``.
"""

from __future__ import annotations

import importlib
import json
import os

import pytest


QUEUE = "SETQ"


def _cfg_file(wt_env) -> dict:
    if not wt_env.config.CONFIG_FILE.exists():
        return {}
    return json.loads(wt_env.config.CONFIG_FILE.read_text())


def _entry(wt_env, queue: str = QUEUE) -> dict:
    return _cfg_file(wt_env).get(queue, {})


def _live_worker(wt_env, queue: str, engine: str, model: str) -> dict:
    """Create a tracked worker backed by this test process."""
    log = wt_env.tmp / f"{queue.lower()}-{engine}.log"
    log.write_text("")
    return wt_env.workers.record_worker(
        os.getpid(), queue, engine, f"{queue.lower()}-{engine}",
        log=str(log), model=model,
    )


def _write_ccc_defaults(wt_env, payload: dict) -> None:
    """Write the CCC spawn-defaults file WatchTower falls back to."""
    path = wt_env.config.CCC_SPAWN_DEFAULTS_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))


# --------------------------------------------------------------------------- 1. defaults
@pytest.mark.parametrize(
    "reader,expected",
    [
        ("auto_drain", False),
        ("backend", "file"),
        ("github_repo", ""),
        ("github_assignee", "@me"),
        ("repo_path", ""),
        ("desired_workers", 1),
        ("grace_s", 180),
        ("claim_types", []),
        ("effort", ""),
        ("model", ""),
    ],
)
def test_unconfigured_queue_reads_documented_defaults(wt_env, reader, expected):
    """A queue nobody has configured answers with its documented default —
    not None, not a crash, not an inherited value from another queue."""
    assert getattr(wt_env.config, reader)("NEVERCONFIGURED") == expected


def test_unconfigured_engine_default_is_codex_when_available(wt_env, fake_bin):
    """The engine default is codex when the codex CLI is installed, and falls
    back to claude (with a warning) when it is not (OPS-106)."""
    fake_bin("codex")
    assert wt_env.config.engine("NEVERCONFIGURED") == "codex"


def test_engine_default_falls_back_to_claude_without_codex(wt_env, no_engines, capsys):
    """No codex binary anywhere: returning "codex" would hand back an engine no
    worker could spawn with, so the default degrades to claude and says so."""
    assert wt_env.config.engine("NEVERCONFIGURED") == "claude"
    assert "codex not on PATH" in capsys.readouterr().err


def test_fresh_queue_is_a_backlog_not_a_worksite(wt_env, run_cli):
    """Creating a queue entry must not staff it: auto_drain stays off until an
    explicit opt-in, so a parking-lot queue never surprises anyone with
    workers."""
    wt_env.config.ensure_entry(QUEUE)
    assert wt_env.config.auto_drain(QUEUE) is False
    assert _entry(wt_env) == {}


def test_enqueue_alone_leaves_no_config_entry(wt_env):
    """OPS-563/OPS-559: a plain enqueue must not persist a config row.

    Regression for a caller that re-derives a session-tracking queue name
    per invocation (never run, never drain-enabled) — before this fix, every
    such enqueue left a dead entry behind in queue-config.json forever."""
    wt_env.queue.enqueue(project=QUEUE, note="never run", source="test")
    assert QUEUE not in wt_env.config.all_queues()
    assert _entry(wt_env) == {}


def test_run_request_registers_a_previously_unconfigured_queue(wt_env):
    """WT-131: an unregistered queue is invisible to the reconciler, so
    pressing ▶ on its very first ticket must register it (unlike a plain
    enqueue, see test_enqueue_alone_leaves_no_config_entry) or the run
    silently no-ops forever."""
    item = wt_env.queue.enqueue(project=QUEUE, note="press play", source="test")
    assert QUEUE not in wt_env.config.all_queues()

    wt_env.queue.mark_runnable(item["ref"])
    assert QUEUE in wt_env.config.all_queues()
    assert wt_env.config.auto_drain(QUEUE) is False


# --------------------------------------------------------------------------- 2. round trip
# (flag, value, reader, expected-read-back)
SETTINGS_MATRIX = [
    ("--backend", "github", "backend", "github"),
    ("--github-repo", "acme-corp/widgets", "github_repo", "acme-corp/widgets"),
    ("--github-assignee", "octocat", "github_assignee", "octocat"),
    ("--engine", "kimi", "engine", "kimi"),
    ("--model", "claude-sonnet-5", "model", "claude-sonnet-5"),
    ("--effort", "high", "effort", "high"),
    ("--workers", "3", "desired_workers", 3),
    ("--grace-s", "45", "grace_s", 45),
    ("--auto-drain", "on", "auto_drain", True),
]


@pytest.mark.parametrize("flag,value,reader,expected", SETTINGS_MATRIX)
def test_wt_config_round_trips_every_setting(wt_env, run_cli, flag, value, reader, expected):
    """Each `wt config` flag persists and reads back through the config API."""
    if flag == "--model":
        run_cli("config", "-q", QUEUE, "--engine", "claude")
    res = run_cli("config", "-q", QUEUE, flag, value)
    assert res.code == 0, res.output
    assert getattr(wt_env.config, reader)(QUEUE) == expected


@pytest.mark.parametrize("flag,value,reader,expected", SETTINGS_MATRIX)
def test_settings_survive_a_process_restart(wt_env, run_cli, flag, value, reader, expected):
    """Settings live on disk, not in module state: a reload (i.e. the next `wt`
    invocation) sees the same values."""
    if flag == "--model":
        run_cli("config", "-q", QUEUE, "--engine", "claude")
    assert run_cli("config", "-q", QUEUE, flag, value).code == 0
    importlib.reload(wt_env.config)
    assert getattr(wt_env.config, reader)(QUEUE) == expected


def test_workers_local_path_round_trips(wt_env, run_cli, tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    assert run_cli("config", "-q", QUEUE, "--workers-local-path", str(repo)).code == 0
    assert wt_env.config.repo_path(QUEUE) == str(repo)


def test_claim_types_round_trip_and_are_deduped_by_choice(wt_env, run_cli):
    res = run_cli("config", "-q", QUEUE, "--auto-drain", "on", "--type", "bug")
    assert res.code == 0, res.output
    assert wt_env.config.claim_types(QUEUE) == ["bug"]
    run_cli("config", "-q", QUEUE, "--auto-drain", "on", "--type", "bug", "--type", "feature")
    assert wt_env.config.claim_types(QUEUE) == ["bug", "feature"]


def test_wt_config_with_no_flags_is_read_only(wt_env, run_cli):
    """A bare `wt config -q Q` reports; it must not create or mutate anything —
    otherwise "let me look at this queue" silently registers it."""
    res = run_cli("config", "-q", "UNTOUCHED")
    assert res.code == 0
    assert "UNTOUCHED" in res.out
    assert "UNTOUCHED" not in _cfg_file(wt_env)


def test_wt_config_report_always_shows_grace_s(wt_env, run_cli):
    """grace_s silently gates auto-drain, so "why did nothing pick this up for
    three minutes" has to be answerable from the queue's own config output."""
    run_cli("config", "-q", QUEUE, "--workers", "2")
    res = run_cli("config", "-q", QUEUE)
    assert "grace_s" in res.out


# --------------------------------------------------------------------------- 3. clearing
def test_empty_model_clears_the_override(wt_env, run_cli):
    run_cli("config", "-q", QUEUE, "--engine", "claude", "--model", "claude-opus-5")
    assert wt_env.config.model(QUEUE) == "claude-opus-5"
    assert run_cli("config", "-q", QUEUE, "--model", "").code == 0
    assert "model" not in _entry(wt_env)
    assert wt_env.config.model(QUEUE) == ""


def test_config_engine_change_gracefully_retires_mismatched_worker(wt_env, run_cli):
    wt_env.config.set_engine(QUEUE, "claude")
    wt_env.config.set_model(QUEUE, "claude-sonnet-5")
    worker = _live_worker(wt_env, QUEUE, "claude", "claude-sonnet-5")

    result = run_cli(
        "config", "-q", QUEUE, "--engine", "kimi", "--model", "kimi-code/k3"
    )

    assert result.code == 0, result.output
    assert worker["worker_id"] not in {
        row["worker_id"] for row in wt_env.workers.list_workers()
        if row.get("alive") and not row.get("released_at")
    }


def test_set_model_change_gracefully_retires_mismatched_worker(wt_env, run_cli):
    wt_env.config.set_engine(QUEUE, "kimi")
    wt_env.config.set_model(QUEUE, "kimi-code/k3")
    worker = _live_worker(wt_env, QUEUE, "kimi", "kimi-code/k3")

    result = run_cli("set", "-q", QUEUE, "--model", "kimi-code/kimi-for-coding")

    assert result.code == 0, result.output
    assert worker["worker_id"] not in {
        row["worker_id"] for row in wt_env.workers.list_workers()
        if row.get("alive") and not row.get("released_at")
    }


def test_workers_release_engine_targets_only_that_engine(wt_env, run_cli):
    claude = _live_worker(wt_env, "CLAUDEQ", "claude", "claude-sonnet-5")
    kimi = _live_worker(wt_env, "KIMIQ", "kimi", "kimi-code/k3")

    result = run_cli("workers", "release", "--engine", "claude", "--json")

    assert result.code == 0, result.output
    assert [row["worker_id"] for row in json.loads(result.out)["released"]] == [
        claude["worker_id"]
    ]
    live = {
        row["worker_id"] for row in wt_env.workers.list_workers()
        if row.get("alive") and not row.get("released_at")
    }
    assert kimi["worker_id"] in live


def test_empty_effort_clears_the_override(wt_env, run_cli):
    run_cli("config", "-q", QUEUE, "--effort", "max")
    wt_env.config.set_effort(QUEUE, "")
    assert "effort" not in _entry(wt_env)
    assert wt_env.config.effort(QUEUE) == ""


def test_effort_can_be_cleared_from_the_cli_like_model(wt_env, run_cli):
    run_cli("config", "-q", QUEUE, "--effort", "max")
    assert run_cli("config", "-q", QUEUE, "--effort", "").code == 0
    assert wt_env.config.effort(QUEUE) == ""


def test_empty_github_repo_clears_the_override(wt_env, run_cli):
    run_cli("config", "-q", QUEUE, "--github-repo", "acme-corp/widgets")
    assert run_cli("config", "-q", QUEUE, "--github-repo", "").code == 0
    assert "github_repo" not in _entry(wt_env)


def test_empty_assignee_reverts_to_the_me_default(wt_env, run_cli):
    run_cli("config", "-q", QUEUE, "--github-assignee", "octocat")
    run_cli("config", "-q", QUEUE, "--github-assignee", "")
    assert wt_env.config.github_assignee(QUEUE) == "@me"


def test_backend_file_removes_the_key_rather_than_storing_a_default(wt_env, run_cli):
    run_cli("config", "-q", QUEUE, "--backend", "github")
    run_cli("config", "-q", QUEUE, "--backend", "file")
    assert "backend" not in _entry(wt_env)
    assert wt_env.config.backend(QUEUE) == "file"


def test_grace_s_zero_is_a_real_value_not_an_unset(wt_env, run_cli):
    """0 means "drain immediately" and must not be confused with "unset"
    (which means the 180s default)."""
    assert run_cli("config", "-q", QUEUE, "--grace-s", "0").code == 0
    assert wt_env.config.grace_s(QUEUE) == 0
    assert _entry(wt_env)["grace_s"] == 0


# --------------------------------------------------------------------------- 4. isolation
def test_setting_one_knob_leaves_the_others_untouched(wt_env, run_cli):
    run_cli(
        "config", "-q", QUEUE,
        "--engine", "codex", "--model", "gpt-5.5", "--effort", "high",
        "--workers", "2", "--grace-s", "10", "--github-assignee", "octocat",
    )
    before = _entry(wt_env)
    run_cli("config", "-q", QUEUE, "--workers", "5")
    after = _entry(wt_env)
    assert after["desired_workers"] == 5
    assert {k: v for k, v in after.items() if k != "desired_workers"} == {
        k: v for k, v in before.items() if k != "desired_workers"
    }


def test_queues_do_not_share_settings(wt_env, run_cli):
    run_cli("config", "-q", "ALPHA", "--engine", "kimi", "--workers", "4")
    run_cli("config", "-q", "BETA", "--engine", "codex")
    assert wt_env.config.engine("ALPHA") == "kimi"
    assert wt_env.config.desired_workers("ALPHA") == 4
    assert wt_env.config.engine("BETA") == "codex"
    assert wt_env.config.desired_workers("BETA") == 1


def test_legacy_wt_set_writes_the_same_store_as_wt_config(wt_env, run_cli):
    """`wt set` is a compatibility alias; it must not become a second,
    diverging source of truth."""
    assert run_cli("set", "-q", QUEUE, "--engine", "codex",
                   "--desired-workers", "3", "--repo-path", "/tmp").code == 0
    assert wt_env.config.engine(QUEUE) == "codex"
    assert wt_env.config.desired_workers(QUEUE) == 3
    res = run_cli("config", "-q", QUEUE)
    assert "codex" in res.out and "3" in res.out


# --------------------------------------------------------------------------- 5. precedence
def test_engine_precedence_explicit_beats_ccc_default(wt_env, fake_bin):
    fake_bin("codex")
    _write_ccc_defaults(wt_env, {"worker_engine": "kimi"})
    assert wt_env.config.engine(QUEUE) == "kimi"
    wt_env.config.set_engine(QUEUE, "claude")
    assert wt_env.config.engine(QUEUE) == "claude"


def test_model_precedence_explicit_then_worker_default_then_shared_default(wt_env):
    """Queue override > CCC's worker_model (same engine) > CCC's shared
    per-engine default > the engine CLI's own ambient default."""
    wt_env.config.set_engine(QUEUE, "claude")
    _write_ccc_defaults(wt_env, {"models": {"claude": "sonnet-5"}})
    assert wt_env.config.model(QUEUE) == "claude-sonnet-5"

    _write_ccc_defaults(wt_env, {
        "worker_engine": "claude", "worker_model": "opus-4-8",
        "models": {"claude": "sonnet-5"},
    })
    assert wt_env.config.model(QUEUE) == "claude-opus-4-8"

    wt_env.config.set_model(QUEUE, "claude-opus-5")
    assert wt_env.config.model(QUEUE) == "claude-opus-5"


def test_worker_model_for_a_different_engine_is_ignored(wt_env):
    """CCC's worker_model is paired with worker_engine. A queue that picks a
    different engine must not inherit an incompatible model id."""
    wt_env.config.set_engine(QUEUE, "codex")
    _write_ccc_defaults(wt_env, {
        "worker_engine": "claude", "worker_model": "opus-5",
        "models": {"codex": "gpt-5.5"},
    })
    assert wt_env.config.model(QUEUE) == "gpt-5.5"


def test_effort_precedence_explicit_beats_ccc_worker_default(wt_env):
    _write_ccc_defaults(wt_env, {"worker_reasoning_effort": "low"})
    assert wt_env.config.effort(QUEUE) == "low"
    wt_env.config.set_effort(QUEUE, "high")
    assert wt_env.config.effort(QUEUE) == "high"


def test_garbage_ccc_defaults_file_is_ignored_not_fatal(wt_env):
    """CCC is a separate system; a corrupt/foreign spawn-defaults file must
    degrade to "no shared default", never break queue configuration."""
    path = wt_env.config.CCC_SPAWN_DEFAULTS_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{ this is not json")
    assert wt_env.config.model(QUEUE) == ""
    assert wt_env.config.effort(QUEUE) == ""


# --------------------------------------------------------------------------- 6. aliases
@pytest.mark.parametrize(
    "engine,given,stored",
    [
        ("claude", "opus-5", "claude-opus-5"),
        ("claude", "sonnet-5", "claude-sonnet-5"),
        ("claude", "claude-opus-4-8", "claude-opus-4-8"),
        ("claude", "sonnet", "sonnet"),          # bare family name: CLI accepts it
        ("codex", "gpt-5.6", "gpt-5.6"),
        ("kimi", "kimi-code/k3", "kimi-code/k3"),
    ],
)
def test_model_aliases_are_canonicalised_at_write_time(wt_env, engine, given, stored):
    """The stored value is what gets passed to the engine's --model flag, so
    aliasing has to happen on the way in — not at spawn time, where a bad value
    kills the worker."""
    wt_env.config.set_engine(QUEUE, engine)
    wt_env.config.set_model(QUEUE, given)
    assert _entry(wt_env)["model"] == stored
    assert wt_env.config.model(QUEUE) == stored


# --------------------------------------------------------------------------- 7. auto-drain interplay
def test_drain_on_restores_a_parked_queue_to_one_worker(wt_env, run_cli):
    """`--workers 0` parks a queue. Turning drain back on without restoring the
    minimum would leave auto-drain visibly on but operationally inert."""
    run_cli("config", "-q", QUEUE, "--workers", "0")
    assert wt_env.config.desired_workers(QUEUE) == 0
    run_cli("config", "-q", QUEUE, "--auto-drain", "on")
    assert wt_env.config.desired_workers(QUEUE) == 1


def test_drain_on_preserves_an_explicit_parallel_worker_count(wt_env, run_cli):
    run_cli("config", "-q", QUEUE, "--workers", "4")
    run_cli("config", "-q", QUEUE, "--auto-drain", "on")
    assert wt_env.config.desired_workers(QUEUE) == 4


def test_drain_off_clears_the_claim_type_restriction(wt_env, run_cli):
    """Off means "no policy" — a stale type filter left behind would silently
    narrow the queue the next time it is turned on."""
    run_cli("config", "-q", QUEUE, "--auto-drain", "on", "--type", "bug")
    assert wt_env.config.claim_types(QUEUE) == ["bug"]
    run_cli("config", "-q", QUEUE, "--auto-drain", "off")
    assert wt_env.config.claim_types(QUEUE) == []


def test_drain_command_and_config_command_agree(wt_env, run_cli):
    assert run_cli("drain", "on", QUEUE).code == 0
    assert wt_env.config.auto_drain(QUEUE) is True
    assert run_cli("config", "-q", QUEUE, "--auto-drain", "off").code == 0
    assert wt_env.config.auto_drain(QUEUE) is False


# --------------------------------------------------------------------------- 8. settings reach the spawn
@pytest.mark.parametrize(
    "engine,model,effort,expect_present,expect_absent",
    [
        ("claude", "claude-opus-5", "max", ["--model", "claude-opus-5", "--effort", "max"], []),
        ("claude", "", "", [], ["--model", "--effort"]),
        ("codex", "gpt-5.6", "high", ["--model", "gpt-5.6", 'model_reasoning_effort="high"'], []),
        # kimi has no effort flag; a configured effort must be dropped, not
        # passed through as an argument the CLI would reject.
        ("kimi", "kimi-code/k3", "high", ["--model", "kimi-code/k3"], ["--effort"]),
    ],
)
def test_configured_model_and_effort_reach_the_worker_argv(
    wt_env, engine, model, effort, expect_present, expect_absent
):
    argv = wt_env.workers.build_drain_command(
        QUEUE, engine, "w-1", "/tmp", model, goal="do the thing", effort=effort
    )
    joined = " ".join(argv)
    for token in expect_present:
        assert token in argv or token in joined, f"{token} missing from {argv}"
    for token in expect_absent:
        assert token not in argv, f"{token} unexpectedly in {argv}"


def test_workers_local_path_becomes_the_spawn_cwd(wt_env, tmp_path, fake_bin):
    """The repo path is not decoration: it is the subprocess cwd, so a worker
    that lands in the wrong tree edits the wrong repo."""
    fake_bin("codex")
    repo = tmp_path / "workrepo"
    repo.mkdir()
    wt_env.config.set_repo_path(QUEUE, str(repo))
    recs = wt_env.workers.spawn_workers(
        QUEUE, 1, engine="codex", repo_path=wt_env.config.repo_path(QUEUE), dry_run=True
    )
    assert recs[0]["repo_path"] == str(repo)


def test_queue_model_and_effort_are_picked_up_by_spawn_without_explicit_args(
    wt_env, fake_bin
):
    fake_bin("codex")
    wt_env.config.set_engine(QUEUE, "codex")
    wt_env.config.set_model(QUEUE, "gpt-5.6")
    wt_env.config.set_effort(QUEUE, "xhigh")
    rec = wt_env.workers.spawn_workers(QUEUE, 1, engine="codex", dry_run=True)[0]
    assert rec["model"] == "gpt-5.6"
    assert rec["effort"] == "xhigh"
    assert 'model_reasoning_effort="xhigh"' in " ".join(rec["argv"])


def test_desired_workers_drives_the_reconciler_staffing_target(wt_env, fake_bin):
    """`--workers N` is the reconciler's target, so a dry-run tick on a stuck
    queue plans exactly N spawns."""
    fake_bin("codex")
    wt_env.config.set_engine(QUEUE, "codex")
    wt_env.config.set_auto_drain(QUEUE, True)
    wt_env.config.set_desired_workers(QUEUE, 3)
    wt_env.config.set_grace_s(QUEUE, 0)
    for i in range(3):
        wt_env.queue.enqueue(note=f"ticket {i}", project=QUEUE, source="test")
    result = wt_env.workers.reconcile_once(dry_run=True)
    planned = [s for s in result.get("spawned", []) if s.get("queue") == QUEUE]
    assert len(planned) == 3, result


def test_staffing_never_exceeds_the_number_of_claimable_tickets(wt_env, fake_bin):
    """`--workers 3` is a ceiling, not a quota: one ticket gets one worker, or
    two of the three spawn, find nothing claimable, and get reaped."""
    fake_bin("codex")
    wt_env.config.set_engine(QUEUE, "codex")
    wt_env.config.set_auto_drain(QUEUE, True)
    wt_env.config.set_desired_workers(QUEUE, 3)
    wt_env.config.set_grace_s(QUEUE, 0)
    wt_env.queue.enqueue(note="the only ticket", project=QUEUE, source="test")
    result = wt_env.workers.reconcile_once(dry_run=True)
    planned = [s for s in result.get("spawned", []) if s.get("queue") == QUEUE]
    assert len(planned) == 1, result


def test_claim_type_restriction_filters_what_a_worker_claims(wt_env):
    """`--type bug` must actually keep features in the backlog."""
    wt_env.config.set_auto_drain(QUEUE, True)
    wt_env.config.set_grace_s(QUEUE, 0)
    wt_env.config.set_claim_types(QUEUE, ["bug"])
    wt_env.queue.enqueue(
        note="a feature request", project=QUEUE, source="test", item_type="feature"
    )
    bug = wt_env.queue.enqueue(
        note="a bug", project=QUEUE, source="test", item_type="bug"
    )
    claimed = wt_env.queue.claim_next("sess-1", project=QUEUE, item_types=["bug"])
    assert claimed is not None
    assert claimed["ref"] == bug["ref"]
    assert wt_env.queue.claim_next("sess-2", project=QUEUE, item_types=["bug"]) is None

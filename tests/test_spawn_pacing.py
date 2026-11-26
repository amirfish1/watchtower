"""Fleet-wide cold-start pacing for automatic spawns (WT-126)."""
from types import SimpleNamespace

import pytest

from watchtower import spawn_pacing


@pytest.fixture
def pacing(monkeypatch):
    monkeypatch.setenv("WATCHTOWER_SPAWN_PACING", "on")
    load = [0.5]
    clock = [1000.0]
    monkeypatch.setattr(spawn_pacing, "load_per_core", lambda: load[0])
    monkeypatch.setattr(spawn_pacing, "time", SimpleNamespace(time=lambda: clock[0]))
    return SimpleNamespace(load=load, clock=clock)


def test_window_caps_cold_starts_fleet_wide(pacing):
    assert spawn_pacing.admit("A", 2) == (2, {})
    allowed, why = spawn_pacing.admit("B", 3)
    assert allowed == 1 and why["recent_starts"] == 2
    allowed, why = spawn_pacing.admit("C", 1)
    assert allowed == 0 and why["reason"].startswith("3 cold starts in the last 120s")
    pacing.clock[0] += 121
    # B and C waited, so a newcomer yields the freed slots to them.
    allowed, why = spawn_pacing.admit("A", 3)
    assert allowed == 1 and why["waiting_ahead"] == ["B", "C"]
    assert spawn_pacing.admit("B", 2) == (2, {})
    assert spawn_pacing.admit("C", 1)[0] == 0  # window full again; C keeps its place
    pacing.clock[0] += 121
    assert spawn_pacing.admit("C", 1) == (1, {})
    pacing.clock[0] += 400  # A stopped asking: its stale place lapses
    assert spawn_pacing.admit("D", 3) == (3, {})


def test_high_load_defers_with_starvation_floor_and_manual_run(pacing):
    pacing.load[0] = 2.7
    allowed, why = spawn_pacing.admit("A", 2)
    # Nothing started recently: the floor admits one even under load.
    assert allowed == 1 and "starvation floor" in why["reason"]
    allowed, why = spawn_pacing.admit("B", 1)
    assert allowed == 0 and why["reason"] == "host load 2.70/core >= 1.5"
    assert why["load_per_core"] == 2.7
    assert spawn_pacing.admit("B", 1, manual=True) == (1, {})
    pacing.clock[0] += 599
    assert spawn_pacing.admit("B", 1)[0] == 0
    pacing.clock[0] += 600
    assert spawn_pacing.admit("B", 1)[0] == 1
    pacing.load[0] = 1.49
    pacing.clock[0] += 121
    assert spawn_pacing.admit("B", 3) == (3, {})


def test_disabled_or_unknown_load_does_not_block(pacing, monkeypatch):
    pacing.load[0] = None
    assert spawn_pacing.admit("A", 3) == (3, {})
    monkeypatch.setenv("WATCHTOWER_SPAWN_PACING", "off")
    assert spawn_pacing.admit("A", 5) == (5, {})
    assert spawn_pacing.admit("A", 0) == (0, {})


def _queues(wt_env, names):
    for name in names:
        wt_env.config.set_auto_drain(name, True)
        wt_env.config.set_repo_path(name, str(wt_env.tmp))
        wt_env.config.set_grace_s(name, 0)
        wt_env.queue.enqueue(project=name, note=f"work for {name}")


def _reconcile_spawns(wt_env, monkeypatch):
    launches = []

    def spawn(queue, n=1, **kwargs):
        launches.extend([queue] * n)
        return []

    monkeypatch.setattr(wt_env.workers, "spawn_workers", spawn)
    return launches


def test_staffing_wave_is_paced_and_deferred_queues_resume(wt_env, pacing, monkeypatch):
    names = [f"W{i}" for i in range(6)]
    _queues(wt_env, names)
    launches = _reconcile_spawns(wt_env, monkeypatch)
    result = wt_env.workers.reconcile_once()
    assert len(launches) == 3
    deferred = {s["queue"] for s in result["skipped"] if s["reason"].startswith("spawn deferred")}
    assert deferred == set(names) - set(launches)
    log = wt_env.queue._resolve_activity_log_path().read_text()
    assert log.count("SPAWN_DEFERRED") == 3 and "admitted=0" in log
    pacing.clock[0] += 121
    # Deferred staffing is recomputed, not dropped: the next pass starts them.
    wt_env.workers.reconcile_once()
    assert set(deferred) <= set(launches[3:])


def test_staffing_wave_waits_while_host_is_saturated(wt_env, pacing, monkeypatch):
    names = [f"S{i}" for i in range(4)]
    _queues(wt_env, names)
    launches = _reconcile_spawns(wt_env, monkeypatch)
    pacing.load[0] = 2.7
    wt_env.workers.reconcile_once()
    assert len(launches) == 1  # starvation floor only
    pacing.clock[0] += 30
    wt_env.workers.reconcile_once()
    assert len(launches) == 1
    log = wt_env.queue._resolve_activity_log_path().read_text()
    assert "host load 2.70/core >= 1.5" in log


def test_dry_run_is_not_paced(wt_env, pacing):
    _queues(wt_env, [f"D{i}" for i in range(5)])
    pacing.load[0] = 9.0
    assert len(wt_env.workers.reconcile_once(dry_run=True)["spawned"]) == 5

"""Headroom-aware dispatch (31K B25): pick a launch engine from CCC quota
headroom before spawning, instead of only reacting to usage-limit failures."""

from __future__ import annotations

import importlib
import json
import time

import pytest


def _row(engine, pct, *, resets_in_h=5.0, available=True, stale=False,
         unlimited=False, account="default"):
    return {
        "id": f"{engine}:{account}", "engine": engine, "account": account,
        "available": available, "stale": stale, "unlimited": unlimited,
        "percent_left": pct,
        "resets_at": None if resets_in_h is None else time.time() + resets_in_h * 3600,
        "hours_to_reset": resets_in_h,
    }


@pytest.fixture()
def headroom():
    import watchtower.headroom as headroom
    importlib.reload(headroom)
    yield headroom
    headroom._reset_cache_for_tests()


# --- pick_engine -----------------------------------------------------------

def test_pick_soonest_reset_wins(headroom):
    rows = [_row("claude", 80, resets_in_h=20), _row("codex", 30, resets_in_h=2)]
    eng, reason = headroom.pick_engine(["claude", "codex"], rows)
    assert eng == "codex"
    assert "codex 30% left, resets in 2.0h" in reason


def test_pick_tie_on_reset_prefers_more_headroom(headroom):
    rows = [_row("claude", 40, resets_in_h=3), _row("codex", 70, resets_in_h=3)]
    rows[1]["resets_at"] = rows[0]["resets_at"]
    assert headroom.pick_engine(["claude", "codex"], rows)[0] == "codex"


def test_pick_low_headroom_engine_avoided(headroom):
    rows = [_row("claude", 4, resets_in_h=1), _row("codex", 61, resets_in_h=3)]
    eng, reason = headroom.pick_engine(["claude", "codex"], rows)
    assert eng == "codex"
    assert reason.startswith("headroom: claude 4% left; codex 61% left")


def test_pick_ignores_stale_unavailable_unlimited_and_unknown(headroom):
    rows = [
        _row("claude", 90, resets_in_h=1, stale=True),
        _row("codex", 90, resets_in_h=1, available=False),
        _row("kimi", 90, resets_in_h=1, unlimited=True),
        _row("devin", None, resets_in_h=1),
    ]
    eng, _ = headroom.pick_engine(["claude", "codex", "kimi", "devin"], rows)
    assert eng is None


def test_pick_best_account_per_engine(headroom):
    rows = [_row("claude", 2, resets_in_h=1, account="a"),
            _row("claude", 50, resets_in_h=4, account="b"),
            _row("codex", 50, resets_in_h=6)]
    assert headroom.pick_engine(["codex", "claude"], rows)[0] == "claude"


def test_pick_no_data_returns_none(headroom):
    assert headroom.pick_engine(["claude", "codex"], None)[0] is None
    assert headroom.pick_engine(["claude", "codex"], [])[0] is None
    # Rows only for engines outside the candidate list are ignored.
    assert headroom.pick_engine(["claude"], [_row("codex", 90)])[0] is None


def test_pick_everything_low_returns_none(headroom):
    rows = [_row("claude", 3), _row("codex", 5)]
    eng, reason = headroom.pick_engine(["claude", "codex"], rows)
    assert eng is None
    assert "claude 3% left" in reason and "codex 5% left" in reason


def test_read_rows_file_and_errors(headroom, tmp_path, monkeypatch):
    f = tmp_path / "h.json"
    f.write_text(json.dumps({"ok": True, "rows": [_row("codex", 50)]}))
    monkeypatch.setenv("WATCHTOWER_HEADROOM_FILE", str(f))
    assert headroom.read_rows()[0]["engine"] == "codex"
    f.write_text("not json")
    assert headroom.read_rows() is None
    monkeypatch.setenv("WATCHTOWER_HEADROOM_FILE", str(tmp_path / "missing.json"))
    assert headroom.read_rows() is None


def test_read_rows_http_unreachable_is_none_and_cached(headroom, monkeypatch):
    monkeypatch.delenv("WATCHTOWER_HEADROOM_FILE", raising=False)
    monkeypatch.setenv("WATCHTOWER_CCC_HEADROOM_URL", "http://127.0.0.1:9/api/headroom")
    calls = []
    real = headroom._fetch
    monkeypatch.setattr(headroom, "_fetch", lambda src: calls.append(src) or real(src))
    assert headroom.read_rows() is None
    assert headroom.read_rows() is None
    assert len(calls) == 1  # failure cached; one probe per cache window


# --- reconcile -------------------------------------------------------------

@pytest.fixture()
def wt(tmp_path, monkeypatch):
    for key, name in (
        ("WATCHTOWER_STORE", "queue.json"),
        ("WATCHTOWER_WORKERS_FILE", "workers.json"),
        ("WATCHTOWER_CONFIG_FILE", "config.json"),
        ("WATCHTOWER_STOP_SIGNALS_DIR", "stop-signals"),
        ("WATCHTOWER_WORKER_SESSIONS_FILE", "worker-sessions.json"),
        ("WATCHTOWER_WORKER_IDS_FILE", "worker-ids.json"),
        ("WATCHTOWER_LAUNCH_FAILURES_FILE", "launch-failures.json"),
        ("WATCHTOWER_ACTIVITY_LOG", "activity.log"),
        ("WATCHTOWER_CCC_SPAWN_DEFAULTS_FILE", "no-ccc-spawn-defaults.json"),
        ("WATCHTOWER_CODEX_THREAD_REGISTRY", "codex-thread-registry.json"),
    ):
        monkeypatch.setenv(key, str(tmp_path / name))
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude-home"))
    monkeypatch.setenv("WATCHTOWER_CCC_HEADROOM_URL", "http://127.0.0.1:9/none")
    monkeypatch.delenv("WATCHTOWER_HEADROOM_FILE", raising=False)

    import watchtower.queue as q
    import watchtower.health as health
    import watchtower.config as config
    import watchtower.workers as workers
    import watchtower.headroom as headroom
    for mod in (q, config, health, workers, headroom):
        importlib.reload(mod)
    monkeypatch.setattr(config, "_REGISTRY_FILE", tmp_path / "no-registry.json")
    monkeypatch.setattr(workers, "engine_available", lambda e: e in {"claude", "codex"})
    monkeypatch.setattr(config, "fallback_model",
                        lambda eng: {"codex": "gpt-test", "claude": "claude-test"}[eng])

    calls = []

    def fake_spawn(queue, n=1, engine="claude", *, model="",
                   inherit_queue_model=True, launch_failures=None, **kw):
        calls.append({"engine": engine, "model": model,
                      "inherit": inherit_queue_model, "effort": kw.get("effort")})
        return [{"worker_id": f"w-{engine}", "queue": queue, "engine": engine}]

    monkeypatch.setattr(workers, "spawn_workers", fake_spawn)

    class Ns:
        pass
    ns = Ns()
    ns.q, ns.config, ns.workers, ns.tmp, ns.calls = q, config, workers, tmp_path, calls
    ns.headroom = headroom
    config.set_auto_drain("Q", True)
    config.set_engine("Q", "claude")
    q.enqueue(project="Q", note="work")
    yield ns
    headroom._reset_cache_for_tests()


def _write_headroom(wt, monkeypatch, rows):
    f = wt.tmp / "headroom.json"
    f.write_text(json.dumps({"ok": True, "generated_at": "now", "rows": rows}))
    monkeypatch.setenv("WATCHTOWER_HEADROOM_FILE", str(f))


def test_reconcile_dispatch_on_launches_engine_with_headroom(wt, monkeypatch):
    _write_headroom(wt, monkeypatch, [_row("claude", 3, resets_in_h=1),
                                      _row("codex", 55, resets_in_h=3)])
    wt.config.set_headroom_dispatch("Q", True)

    result = wt.workers.reconcile_once(dry_run=False)

    assert [c["engine"] for c in wt.calls] == ["codex"]
    assert wt.calls[0]["model"] == "gpt-test"
    assert wt.calls[0]["inherit"] is False
    assert wt.calls[0]["effort"] is not None
    assert wt.config.engine("Q") == "claude"  # stored engine untouched
    fb = result["fallbacks"]
    assert fb and fb[0]["from_engine"] == "claude" and fb[0]["to_engine"] == "codex"
    assert "claude 3% left" in fb[0]["reason"]
    activity = (wt.tmp / "activity.log").read_text()
    assert "HEADROOM_DISPATCH" in activity


def test_reconcile_dispatch_off_is_unchanged(wt, monkeypatch):
    _write_headroom(wt, monkeypatch, [_row("claude", 3, resets_in_h=1),
                                      _row("codex", 55, resets_in_h=3)])
    assert wt.config.headroom_dispatch("Q") is False  # default off

    result = wt.workers.reconcile_once(dry_run=False)

    assert wt.calls == [{"engine": "claude", "model": "", "inherit": True,
                         "effort": None}]
    assert result["fallbacks"] == []


def test_reconcile_dispatch_on_without_headroom_is_unchanged(wt, monkeypatch):
    wt.config.set_headroom_dispatch("Q", True)
    # No file; HTTP points at a closed port -> no data.
    wt.workers.reconcile_once(dry_run=False)
    assert [c["engine"] for c in wt.calls] == ["claude"]
    assert wt.calls[0]["inherit"] is True

    wt.calls.clear()
    bad = wt.tmp / "bad.json"
    bad.write_text("{")
    monkeypatch.setenv("WATCHTOWER_HEADROOM_FILE", str(bad))
    wt.workers.reconcile_once(dry_run=False)
    assert [c["engine"] for c in wt.calls] == ["claude"]


def test_reconcile_dispatch_skips_engine_in_cooldown(wt, monkeypatch):
    _write_headroom(wt, monkeypatch, [_row("claude", 3, resets_in_h=1),
                                      _row("codex", 55, resets_in_h=3)])
    wt.config.set_headroom_dispatch("Q", True)
    log = wt.tmp / "fail.log"
    log.write_text("boom\n")
    wt.workers._record_launch_failure(
        queue="Q", engine="codex", worker_id="q-old", pid=1, log_path=log,
        reason="engine exited immediately (exit 2)",
    )
    wt.workers.reconcile_once(dry_run=False)
    assert [c["engine"] for c in wt.calls] == ["claude"]


def test_headroom_dispatch_config_inherits_spawn_defaults(wt):
    assert wt.config.headroom_dispatch("Q") is False
    (wt.tmp / "no-ccc-spawn-defaults.json").write_text(
        json.dumps({"worker_headroom_dispatch": True}))
    assert wt.config.headroom_dispatch("Q") is True
    wt.config.set_headroom_dispatch("Q", False)
    assert wt.config.headroom_dispatch("Q") is False


def test_wt_config_headroom_dispatch_flag(wt, capsys):
    from watchtower import cli
    importlib.reload(cli)
    assert cli.main(["config", "-q", "Q", "--headroom-dispatch", "on"]) in (0, None)
    assert "headroom_dispatch=on" in capsys.readouterr().out
    assert wt.config.headroom_dispatch("Q") is True
    assert cli.main(["config", "-q", "Q", "--headroom-dispatch", "off"]) in (0, None)
    assert wt.config.headroom_dispatch("Q") is False

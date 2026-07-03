"""Per-chat nudge-policy knobs (WT-61): chats.get_chat_policy/set_chat_policy,
sidecar persistence, the scheduler tick honoring overrides, and the
`wt chat set` CLI round trip.

Fixture patterns mirror tests/test_chats.py (lightweight ``chats`` fixture,
isolated $WATCHTOWER_CHATS_DIR) and tests/test_chat_cli.py (full ``wt``
sandbox for CLI invocations), replicated minimally here rather than imported
cross-module.
"""

from __future__ import annotations

import importlib
import json
import os
import time

import pytest

SID_A = "aaaa1111-0000-0000-0000-000000000001"
SID_B = "bbbb2222-0000-0000-0000-000000000002"


@pytest.fixture()
def chats(tmp_path, monkeypatch):
    """Isolated chats module: temp chats dir + temp activity log."""
    monkeypatch.setenv("WATCHTOWER_CHATS_DIR", str(tmp_path / "chats"))
    import watchtower.queue as q
    import watchtower.chats as chats_mod
    importlib.reload(q)
    importlib.reload(chats_mod)
    monkeypatch.setattr(q, "_ACTIVITY_LOG", tmp_path / "activity.log")
    return chats_mod


@pytest.fixture()
def wt(tmp_path, monkeypatch):
    """Isolated WatchTower: fresh store, workers, agents, outbox, chats dir
    (same env-override pattern as tests/test_chat_cli.py)."""
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_WORKERS_FILE", str(tmp_path / "workers.json"))
    monkeypatch.setenv("WATCHTOWER_CONFIG_FILE", str(tmp_path / "config.json"))
    monkeypatch.setenv("WATCHTOWER_STOP_SIGNALS_DIR", str(tmp_path / "stop-signals"))
    monkeypatch.setenv(
        "WATCHTOWER_WORKER_SESSIONS_FILE", str(tmp_path / "worker-sessions.json")
    )
    monkeypatch.setenv("WATCHTOWER_AGENTS_FILE", str(tmp_path / "agents.json"))
    monkeypatch.setenv("WATCHTOWER_OUTBOX_FILE", str(tmp_path / "outbox.json"))
    monkeypatch.setenv("WATCHTOWER_CHATS_DIR", str(tmp_path / "chats"))
    monkeypatch.setenv("WATCHTOWER_DASHBOARD_PID", str(tmp_path / "dashboard.pid"))
    monkeypatch.setenv("WATCHTOWER_DAEMON_PID", str(tmp_path / "daemon.pid"))
    monkeypatch.setenv("WATCHTOWER_DELEGATE_URL", "off")
    monkeypatch.setenv(
        "WATCHTOWER_CLAUDE_PROJECTS_DIR", str(tmp_path / "claude-projects")
    )

    import watchtower.queue as q
    import watchtower.config as config
    import watchtower.workers as workers
    import watchtower.messages as messages
    import watchtower.chats as chats
    import watchtower.cli as cli

    importlib.reload(q)
    importlib.reload(config)
    importlib.reload(workers)
    importlib.reload(messages)
    importlib.reload(chats)
    importlib.reload(cli)
    monkeypatch.setattr(config, "_REGISTRY_FILE", tmp_path / "no-registry.json")
    monkeypatch.setattr(q, "_ACTIVITY_LOG", tmp_path / "activity.log")

    class Ns:
        pass

    ns = Ns()
    ns.q = q
    ns.config = config
    ns.workers = workers
    ns.messages = messages
    ns.chats = chats
    ns.cli = cli
    ns.tmp_path = tmp_path
    return ns


def _new_chat(chats, topic="decide next step", parts=None, **kw):
    parts = parts if parts is not None else [
        {"session_id": SID_A, "name": "planner"},
        {"session_id": SID_B, "name": "builder"},
    ]
    return chats.create_chat(topic, parts, **kw)


def _collecting_deliver():
    calls = []

    def deliver(sid, text):
        calls.append((sid, text))
        return True

    return calls, deliver


# ------------------------------------------------------------------- get/set
def test_get_chat_policy_defaults_when_unset(chats):
    info = _new_chat(chats)
    policy = chats.get_chat_policy(info["path"])
    assert policy == {
        "nudge_interval_s": chats.DEFAULT_NUDGE_INTERVAL_S,
        "idle_close_s": chats.DEFAULT_IDLE_CLOSE_S,
        "max_auto_nudges_per_hour": chats.DEFAULT_MAX_AUTO_NUDGES_PER_HOUR,
    }


def test_set_chat_policy_persists_to_sidecar_json(chats):
    info = _new_chat(chats)
    policy = chats.set_chat_policy(info["path"], nudge_interval_s=120)
    assert policy["nudge_interval_s"] == 120
    # Other knobs stay at default since they were not passed.
    assert policy["idle_close_s"] == chats.DEFAULT_IDLE_CLOSE_S

    # Persisted directly on disk, not just in the returned dict.
    sidecar_path = str(info["path"])[: -len(".md")] + ".json"
    on_disk = json.loads(open(sidecar_path).read())
    assert on_disk["nudge_interval_s"] == 120

    # Re-reading via get_chat_policy reflects the override.
    assert chats.get_chat_policy(info["path"])["nudge_interval_s"] == 120


def test_set_chat_policy_multiple_knobs(chats):
    info = _new_chat(chats)
    policy = chats.set_chat_policy(
        info["path"], idle_close_s=999, max_auto_nudges_per_hour=5
    )
    assert policy["idle_close_s"] == 999
    assert policy["max_auto_nudges_per_hour"] == 5
    assert policy["nudge_interval_s"] == chats.DEFAULT_NUDGE_INTERVAL_S


def test_set_chat_policy_rejects_unknown_key(chats):
    info = _new_chat(chats)
    with pytest.raises(ValueError):
        chats.set_chat_policy(info["path"], not_a_real_knob=5)


def test_set_chat_policy_rejects_non_positive_values(chats):
    info = _new_chat(chats)
    with pytest.raises(ValueError):
        chats.set_chat_policy(info["path"], nudge_interval_s=0)
    with pytest.raises(ValueError):
        chats.set_chat_policy(info["path"], idle_close_s=-5)


def test_set_chat_policy_bad_ref_raises(chats):
    with pytest.raises(ValueError):
        chats.set_chat_policy("no-such-chat", nudge_interval_s=10)


def test_get_chat_policy_bad_ref_raises(chats):
    with pytest.raises(ValueError):
        chats.get_chat_policy("no-such-chat")


# --------------------------------------------------------- tick honors policy
def test_nudge_tick_honors_effective_policy_from_set_chat_policy(chats):
    """A chat given a short nudge_interval_s via set_chat_policy() nudges
    again promptly after a follow-up post; a sibling chat left at the module
    default (60s) does not, within the same short window -- proving the tick
    consults the sidecar override the CLI/API path writes, not just a
    module-level constant."""
    fast = _new_chat(chats, topic="fast lane")
    slow = _new_chat(chats, topic="slow lane")
    chats.post(fast["path"], "kick off", author_sid=SID_A)
    chats.post(slow["path"], "kick off", author_sid=SID_A)

    chats.set_chat_policy(fast["path"], nudge_interval_s=1)
    # slow lane keeps the (much larger) module default (60s).

    t0 = time.time()
    calls, deliver = _collecting_deliver()
    # First tick nudges both (no prior nudge_history yet).
    report0 = chats.nudge_tick(deliver, now=t0)
    fast_name = os.path.basename(fast["path"])
    slow_name = os.path.basename(slow["path"])
    assert report0["results"][fast_name]["action"] == "nudged"
    assert report0["results"][slow_name]["action"] == "nudged"

    # Both chats get a follow-up message shortly after.
    chats.post(fast["path"], "update", author_sid=SID_A)
    chats.post(slow["path"], "update", author_sid=SID_A)
    os.utime(fast["path"], (t0 + 2, t0 + 2))
    os.utime(slow["path"], (t0 + 2, t0 + 2))

    calls, deliver = _collecting_deliver()
    report = chats.nudge_tick(deliver, now=t0 + 3)
    assert report["results"][fast_name]["action"] == "nudged"
    assert report["results"][slow_name]["action"] == "skipped"
    assert report["results"][slow_name]["reason"] == "interval"
    assert [sid for sid, _ in calls] == [SID_B]


# ------------------------------------------------------------------------ CLI
def test_cli_chat_set_round_trip_json(wt, capsys):
    info = wt.chats.create_chat(
        "Topic", [{"session_id": SID_A, "name": "planner"}]
    )

    rc = wt.cli.main(
        ["chat", "set", info["path"], "--nudge-interval-s", "120", "--json"]
    )
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["nudge_interval_s"] == 120

    # Effective policy is now reflected on plain reads via chats.py directly.
    assert wt.chats.get_chat_policy(info["path"])["nudge_interval_s"] == 120

    # No-flag call prints the current effective policy (get, not set).
    rc2 = wt.cli.main(["chat", "set", info["path"], "--json"])
    assert rc2 == 0
    out2 = json.loads(capsys.readouterr().out)
    assert out2["nudge_interval_s"] == 120


def test_cli_chat_set_no_flags_prints_current_policy(wt, capsys):
    info = wt.chats.create_chat(
        "Topic", [{"session_id": SID_A, "name": "planner"}]
    )
    wt.chats.set_chat_policy(info["path"], idle_close_s=333)

    assert wt.cli.main(["chat", "set", info["path"], "--json"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["idle_close_s"] == 333
    assert out["nudge_interval_s"] == wt.chats.DEFAULT_NUDGE_INTERVAL_S


def test_cli_chat_set_invalid_value_errors(wt, capsys):
    info = wt.chats.create_chat(
        "Topic", [{"session_id": SID_A, "name": "planner"}]
    )
    rc = wt.cli.main(["chat", "set", info["path"], "--nudge-interval-s", "0"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "error" in err.lower()


def test_cli_chat_set_bad_ref_errors(wt, capsys):
    rc = wt.cli.main(["chat", "set", "no-such-chat", "--nudge-interval-s", "10"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "error" in err.lower()

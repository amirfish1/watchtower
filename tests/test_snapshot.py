"""Fire-window math and state-file plumbing for watchtower.snapshot."""
import json

from watchtower import snapshot


def test_next_action_sleeps_remainder_when_user_was_active():
    # armed at t=0, threshold 55m; user last active 20m ago at now=100m
    now, mtime = 6000.0, 6000.0 - 20 * 60
    action = snapshot.next_action(now, mtime, armed_at=0.0,
                                  threshold_s=55 * 60, ttl_s=60 * 60)
    assert action == ("sleep", 35 * 60)


def test_next_action_fires_inside_window():
    now = 100_000.0
    for idle_min in (55, 57, 59.9):
        action = snapshot.next_action(now, now - idle_min * 60, armed_at=now - 7200,
                                      threshold_s=55 * 60, ttl_s=60 * 60)
        assert action == ("fire",), idle_min


def test_next_action_skips_when_overslept_past_ttl():
    now = 100_000.0
    action = snapshot.next_action(now, now - 61 * 60, armed_at=now - 7200,
                                  threshold_s=55 * 60, ttl_s=60 * 60)
    assert action == ("skip",)


def test_next_action_expires_at_lifetime_cap():
    now = 200_000.0
    action = snapshot.next_action(now, now - 10, armed_at=now - snapshot.TIMER_CAP_S,
                                  threshold_s=55 * 60, ttl_s=60 * 60)
    assert action == ("expire",)


def test_state_roundtrip_and_paths(wt_env):
    sid = "abc123"
    snapshot.save_state(sid, {"session_id": sid, "outcome": "armed"})
    assert snapshot.load_state(sid)["outcome"] == "armed"
    assert snapshot.load_state("missing") is None
    assert snapshot.snapshot_path(sid).name == "abc123.md"
    assert snapshot.timer_state_path(sid).parent.name == "timers"
    link = snapshot.latest_link("/Users/x/Apps/demo.app")
    assert link.name == "latest"
    assert link.parent.name == "-Users-x-Apps-demo-app"

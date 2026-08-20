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


def test_arm_rejects_threshold_at_or_past_ttl(wt_env):
    r = snapshot.arm("s1", "claude", "/tmp/x", idle_min=60, spawn=False)
    assert not r["ok"] and "60" in r["error"]


def test_arm_writes_state_and_disarm_marks_it(wt_env):
    r = snapshot.arm("s1", "claude", "/tmp/x", idle_min=10, spawn=False)
    assert r["ok"] and snapshot.load_state("s1")["outcome"] == "armed"
    assert snapshot.disarm("s1")["ok"]
    assert snapshot.load_state("s1")["outcome"] == "disarmed"
    assert snapshot.status("s1")[0]["outcome"] == "disarmed"


def test_run_timer_sleeps_then_fires_once(wt_env, monkeypatch):
    fired = []
    clock = {"t": 1000.0}
    mtimes = iter([1000.0, 1000.0])  # active at first wake, then idle long enough
    monkeypatch.setattr(snapshot, "transcript_mtime", lambda sid, eng: next(mtimes, 1000.0))
    snapshot.arm("s1", "claude", "/tmp/x", idle_min=55, spawn=False)

    def fake_sleep(s):
        clock["t"] += s

    outcome = snapshot.run_timer(
        "s1", now_fn=lambda: clock["t"], sleep_fn=fake_sleep,
        fire_fn=lambda sid: fired.append(sid) or {"ok": True},
    )
    assert outcome == "fired" and fired == ["s1"]
    assert snapshot.load_state("s1")["outcome"] == "fired"


def test_run_timer_skips_when_overslept(wt_env, monkeypatch):
    clock = {"t": 100_000.0}
    # transcript last touched 61 minutes before the (single) wake
    monkeypatch.setattr(snapshot, "transcript_mtime",
                        lambda sid, eng: clock["t"] - 61 * 60)
    snapshot.arm("s1", "claude", "/tmp/x", idle_min=55, spawn=False)
    outcome = snapshot.run_timer("s1", now_fn=lambda: clock["t"],
                                 sleep_fn=lambda s: None,
                                 fire_fn=lambda sid: {"ok": True})
    assert outcome == "skipped-overslept"
    assert snapshot.load_state("s1")["outcome"] == "skipped-overslept"


def test_run_timer_exits_when_disarmed_meanwhile(wt_env, monkeypatch):
    clock = {"t": 1000.0}
    monkeypatch.setattr(snapshot, "transcript_mtime", lambda sid, eng: clock["t"])
    snapshot.arm("s1", "claude", "/tmp/x", idle_min=55, spawn=False)

    def sleep_and_disarm(s):
        clock["t"] += s
        snapshot.disarm("s1")

    outcome = snapshot.run_timer("s1", now_fn=lambda: clock["t"],
                                 sleep_fn=sleep_and_disarm,
                                 fire_fn=lambda sid: {"ok": True})
    assert outcome == "disarmed"


def test_fire_delivers_prompt_with_paths(wt_env, monkeypatch):
    sent = {}
    snapshot.arm("s1", "claude", "/tmp/proj", idle_min=55, spawn=False)
    monkeypatch.setattr(snapshot, "transcript_mtime",
                        lambda sid, eng: 0.0)  # idle forever -> prompt says ~cold

    def fake_deliver(resolved, text):
        sent.update(resolved=resolved, text=text)
        return {"ok": True, "transport": "tty"}

    monkeypatch.setattr("watchtower.messages.deliver", fake_deliver)
    r = snapshot.fire("s1")
    assert r["ok"]
    assert sent["resolved"]["session_id"] == "s1"
    assert sent["resolved"]["engine"] == "claude"
    assert str(snapshot.snapshot_path("s1")) in sent["text"]
    assert "wt snapshot record" in sent["text"]


def test_record_and_find_latest_and_consume(wt_env):
    p = snapshot.snapshot_path("s1")
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("---\nsession_id: s1\n---\nstate\n")
    assert snapshot.record("s1", "/tmp/proj")["ok"]
    assert snapshot.find_latest("/tmp/proj") == p
    archived = snapshot.consume(p)
    assert archived.exists() and archived.parent.name == "archive"
    assert snapshot.find_latest("/tmp/proj") is None


def test_record_fails_when_snapshot_missing(wt_env):
    r = snapshot.record("ghost", "/tmp/proj")
    assert not r["ok"] and "not found" in r["error"]


def _write_transcript(root, slug, sid, mtime, first_text="hello world"):
    import json as _json, os as _os
    d = root / slug
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{sid}.jsonl"
    lines = [
        _json.dumps({"type": "summary", "summary": "x"}),
        _json.dumps({"type": "user",
                     "message": {"role": "user", "content": [
                         {"type": "text", "text": first_text}]}}),
    ]
    p.write_text("\n".join(lines) + "\n")
    _os.utime(p, (mtime, mtime))
    return p


def test_list_sessions_orders_excludes_and_snippets(wt_env, tmp_path, monkeypatch):
    root = tmp_path / "claude-projects"
    monkeypatch.setenv("WATCHTOWER_CLAUDE_PROJECTS_DIR", str(root))
    slug = snapshot.cwd_slug("/tmp/proj")
    _write_transcript(root, slug, "old1", 1000.0, "first task ever")
    _write_transcript(root, slug, "new1", 3000.0, "  newest   task  ")
    _write_transcript(root, slug, "self1", 4000.0, "my own fresh session")
    rows = snapshot.list_sessions("/tmp/proj", limit=10, exclude="self1")
    assert [r["session_id"] for r in rows] == ["new1", "old1"]
    assert rows[0]["first_message"] == "newest task"


def test_list_sessions_respects_limit_and_missing_dir(wt_env, tmp_path, monkeypatch):
    root = tmp_path / "claude-projects"
    monkeypatch.setenv("WATCHTOWER_CLAUDE_PROJECTS_DIR", str(root))
    assert snapshot.list_sessions("/tmp/nowhere") == []
    slug = snapshot.cwd_slug("/tmp/proj")
    for i in range(4):
        _write_transcript(root, slug, f"s{i}", 1000.0 + i)
    assert len(snapshot.list_sessions("/tmp/proj", limit=2)) == 2



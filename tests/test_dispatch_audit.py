"""Staffing evidence and a long-lived GitHub eligibility transition (WT-51)."""
import json
import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest


@pytest.fixture
def audit(wt_env, monkeypatch):
    from watchtower import dispatch_audit

    dispatch_audit._LAST.clear()
    wt_env.config.set_backend("GHI", "github")
    wt_env.config.set_github_repo("GHI", "acme/queue")
    wt_env.config.set_auto_drain("GHI", True)
    monkeypatch.setattr(wt_env.github_backend.GitHubIssuesBackend, "_run",
                        lambda *_a, **_k: pytest.fail("unexpected gh call"))
    return dispatch_audit


def _record(audit, **changes):
    args = dict(items=[{"ref": "GHI-1", "auto_eligible": False,
                        "claimable": False, "text": "PRIVATE BODY"}],
                depth=0, total_open=1, health={"claimable_depth": 0, "state": "backlog"},
                live=0, desired=1, reconcile_id="tick-1", scoped=False,
                reason="within grace_s=180")
    args.update(changes)
    return audit.record("GHI", **args)


def _events(wt_env):
    path = wt_env.queue._resolve_activity_log_path()
    return [json.loads(line.split(" ELIGIBILITY ", 1)[1])
            for line in path.read_text().splitlines() if " ELIGIBILITY " in line]


def test_audit_records_changed_decisions_and_heartbeat_without_fetching(wt_env, audit, monkeypatch):
    clock = [100.0]
    monkeypatch.setattr(audit, "time", SimpleNamespace(monotonic=lambda: clock[0]))
    _record(audit)
    wt_env.github_backend._LIST_CACHE["acme/queue:open"] = {
        "at": 123, "fetched_at": 122, "data": [{"number": 1}],
    }
    _record(audit, reconcile_id="tick-2")  # clocks/ids don't defeat dedupe
    assert len(_events(wt_env)) == 1
    clock[0] += 300
    _record(audit, reconcile_id="tick-3")
    assert len(_events(wt_env)) == 2
    _record(audit, depth=1, reason="eligible_work")
    assert len(_events(wt_env)) == 3
    assert _events(wt_env)[-1]["cache"]["open"]["at"] == 123


def test_audit_bounds_content_and_records_drain_once(wt_env, audit):
    items = [{"ref": f"GHI-{i}", "readiness": "x" * 500,
              "text": "PRIVATE BODY", "blocked_by": ["x" * 200] * 30}
             for i in range(30)]
    _record(audit, items=items, total_open=30)
    event = _events(wt_env)[0]
    assert len(event["listed_items"]) == 10
    assert event["omitted_items"] == 20
    assert len(event["listed_items"][0]["readiness"]) == 128
    assert len(event["listed_items"][0]["blocked_by"]) == 10
    assert len(event["listed_items"][0]["blocked_by"][0]) == 80
    assert "PRIVATE BODY" not in json.dumps(event)
    _record(audit, items=[], total_open=0, reason="depth=0")
    _record(audit, items=[], total_open=0, reason="depth=0")
    assert len(_events(wt_env)) == 2


def test_audit_dry_run_and_failed_append_do_not_consume_retry(wt_env, audit, monkeypatch):
    calls = []
    monkeypatch.setattr(wt_env.queue, "_log", lambda *a, **k: calls.append(a) or False)
    _record(audit, dry_run=True)
    assert not calls and not audit._LAST
    _record(audit)
    _record(audit)
    assert len(calls) == 2 and not audit._LAST


@pytest.mark.parametrize("audit_raises", [False, True])
def test_daemon_tick_dispatches_github_issue_after_grace_in_same_process(
    wt_env, audit, monkeypatch, audit_raises
):
    """Warm the listing cache while ineligible, then cross the clock boundary.

    Drive the actual periodic daemon loop: no enqueue, play, or scoped pass.
    Only the engine-launch boundary and external services are replaced.
    """
    from watchtower import cli, dashboard, deploy, stages, takeover

    wt_env.config.set_grace_s("GHI", 180)
    wt_env.config.set_repo_path("GHI", str(wt_env.tmp))
    clock = [datetime.now(timezone.utc)]
    usage_dir = wt_env.tmp / "usage"
    usage_dir.mkdir()
    monkeypatch.setenv("WATCHTOWER_USAGE_DIR", str(usage_dir))
    old = clock[0].timestamp() - 60
    os.utime(usage_dir, (old, old))
    created = clock[0].strftime("%Y-%m-%dT%H:%M:%SZ")
    issue = {"number": 1, "title": "audit", "body": "", "state": "OPEN",
             "createdAt": created, "updatedAt": created,
             "labels": [], "assignees": [], "author": {"login": "owner"}}
    backend = wt_env.github_backend
    raw = {"open": [issue], "closed": []}
    monkeypatch.setattr(backend.GitHubIssuesBackend, "_list_issues",
                        lambda self, state="open", **_: raw[state])
    real_datetime = datetime

    class ClockDateTime(real_datetime):
        @classmethod
        def now(cls, tz=None):
            return clock[0]

    monkeypatch.setattr(backend, "datetime", ClockDateTime)
    monkeypatch.setattr(backend, "time", SimpleNamespace(time=lambda: clock[0].timestamp()))
    if audit_raises:
        monkeypatch.setattr(audit, "record", lambda *a, **k: (_ for _ in ()).throw(OSError("full")))

    launches = []

    def launch(queue, **kwargs):
        launches.append((queue, clock[0]))
        return []

    monkeypatch.setattr(wt_env.workers, "spawn_workers", launch)
    monkeypatch.setattr(takeover, "enroll_candidates", lambda: [])
    monkeypatch.setattr(takeover, "reconcile", lambda **_: [])
    monkeypatch.setattr(cli, "_maybe_self_update", lambda: None)
    monkeypatch.setattr(cli, "_stop_detached_dashboard", lambda *_: None)
    monkeypatch.setattr(deploy, "sync", lambda: {"action": "unchanged"})

    class Server:
        def __init__(self, *_):
            pass

        def serve_forever(self):
            pass

    monkeypatch.setattr(dashboard, "ThreadingHTTPServer", Server)

    class Done(Exception):
        pass

    ticks = []

    def next_tick(_, **kwargs):
        ticks.append(True)
        if len(ticks) == 1:
            assert not launches
            assert backend.GitHubIssuesBackend._LIST_ITEMS_MEMO
            clock[0] += timedelta(seconds=181)
            return False
        else:
            raise Done

    monkeypatch.setattr(stages, "sleep_until_wake", next_tick)
    with pytest.raises(Done):
        cli._daemon_loop_ticks(SimpleNamespace(
            interval=5, dry_run=False, host="127.0.0.1", port=0,
            auto_spawn=True, stuck_minutes=10, engine="claude",
        ))
    assert len(ticks) == 2
    assert launches == [("GHI", clock[0])]
    if not audit_raises:
        events = _events(wt_env)
        assert [e["counted_claimable"] for e in events] == [0, 1]
        assert events[0]["listed_items"][0]["auto_eligible"] is False
        assert events[1]["listed_items"][0]["auto_eligible"] is True
        assert all(e["scoped"] is False for e in events)

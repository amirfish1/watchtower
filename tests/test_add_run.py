"""Submission-time run requests, using the real CLI and reconciler."""

import pytest


@pytest.mark.parametrize("auto_drain", [False, True])
def test_add_run_dispatches_before_returning(wt_env, run_cli, monkeypatch, auto_drain):
    wt_env.config.set_auto_drain("Q", auto_drain)
    wt_env.config.set_repo_path("Q", str(wt_env.tmp))
    backlog = wt_env.queue.enqueue(project="Q", note="leave this alone")
    spawned = []

    def spawn(queue, n=1, **kwargs):
        spawned.append(queue)
        requested = [it for it in wt_env.queue.list_items(project=queue)
                     if it.get("run_requested")]
        assert len(requested) == 1
        return [{"worker_id": "q-worker", "queue": queue}]

    monkeypatch.setattr(wt_env.workers, "spawn_workers", spawn)
    result = run_cli("add", "-q", "Q", "--title", "start now", "--run")

    assert result.code == 0, result.output
    assert spawned == ["Q"]
    item = next(it for it in wt_env.queue.list_items(project="Q")
                if it["title"] == "start now")
    assert item["run_requested"] is True
    assert item["status"] == "open"
    assert not item.get("claimed_by")
    assert not wt_env.queue.get(backlog["ref"]).get("run_requested")
    assert wt_env.config.auto_drain("Q") is auto_drain
    assert wt_env.config.grace_s("Q") == 180
    assert "RUNNABLE:" in result.out
    assert "spawned worker q-worker" in result.out


def test_add_run_default_add_keeps_backlog(wt_env, run_cli, monkeypatch):
    spawned = []
    monkeypatch.setattr(wt_env.workers, "spawn_workers",
                        lambda *a, **kw: spawned.append(a) or [])
    result = run_cli("add", "-q", "Q", "--title", "backlog")
    assert result.code == 0, result.output
    assert not wt_env.queue.list_items(project="Q")[0].get("run_requested")
    assert spawned == []
    assert "auto_drain off" in result.out


@pytest.mark.parametrize("conflict", ["--claim", "--group-parent"])
def test_add_run_rejects_conflicts_before_filing(wt_env, run_cli, conflict):
    result = run_cli("add", "-q", "Q", "--title", "conflict", "--run", conflict)
    assert result.code != 0
    assert "error:" in result.err
    assert "--run" in result.err and conflict in result.err
    assert wt_env.queue.list_items(project="Q") == []


def test_add_run_preserves_dependencies(wt_env, run_cli, monkeypatch):
    wt_env.config.set_repo_path("Q", str(wt_env.tmp))
    dependency = wt_env.queue.enqueue(project="Q", note="finish first")
    spawned = []
    monkeypatch.setattr(wt_env.workers, "spawn_workers",
                        lambda *a, **kw: spawned.append(a) or [])
    result = run_cli("add", "-q", "Q", "--title", "wait for dependency",
                     "--after", dependency["ref"], "--run")
    assert result.code == 0, result.output
    requested = [it for it in wt_env.queue.list_items(project="Q")
                 if it.get("run_requested")]
    assert len(requested) == 1
    assert requested[0]["blocked_by"] == [dependency["ref"]]
    assert wt_env.queue.count_manual_eligible(project="Q") == 0
    assert spawned == []


@pytest.mark.parametrize("failure", ["exception", "missing"])
def test_add_run_reports_request_failure_without_dispatch(wt_env, run_cli, monkeypatch, failure):
    def fail_request(ref):
        if failure == "exception":
            raise ValueError("cannot record run")
        return None

    monkeypatch.setattr(wt_env.queue, "mark_runnable", fail_request)
    dispatched = []
    monkeypatch.setattr(wt_env.workers, "dispatch_after_enqueue",
                        lambda *a: dispatched.append(a) or "")
    result = run_cli("add", "-q", "Q", "--title", "saved ticket", "--run")
    assert result.code == 1, result.output
    items = wt_env.queue.list_items(project="Q")
    assert len(items) == 1
    assert not items[0].get("run_requested")
    assert items[0]["ref"] in result.err
    assert "could not request its run" in result.err
    assert dispatched == []


@pytest.mark.parametrize("gate", ["readiness", "plan", "group"])
def test_add_run_waits_for_existing_preconditions(wt_env, run_cli, monkeypatch, gate):
    wt_env.config.set_repo_path("Q", str(wt_env.tmp))
    spawned, planned = [], []
    monkeypatch.setattr(wt_env.workers, "spawn_workers",
                        lambda *a, **kw: spawned.append(a) or [])
    monkeypatch.setattr(wt_env.cli, "start_pending_plans",
                        lambda queue: planned.append(queue) or 0)
    if gate == "readiness":
        flags = ["--readiness", "needs-spec"]
    elif gate == "plan":
        flags = ["--gate", "plan"]
    else:
        parent = wt_env.queue.enqueue(project="Q", note="group plan", group_parent=True)
        flags = ["--group", parent["ref"]]
    result = run_cli("add", "-q", "Q", "--title", "waiting ticket", "--run", *flags)
    assert result.code == 0, result.output
    item = next(it for it in wt_env.queue.list_items(project="Q")
                if it["title"] == "waiting ticket")
    assert item["run_requested"] is True
    assert item["status"] == "open"
    assert wt_env.queue.count_manual_eligible(project="Q") == 0
    assert spawned == []
    assert planned == (["Q"] if gate == "plan" else [])


def test_claim_requested_only_skips_unrequested_higher_priority(wt_env, run_cli):
    """OPS-1492: a requested-only dispatcher must not get unrequested work."""
    wt_env.config.set_repo_path("Q", str(wt_env.tmp))
    asked = wt_env.queue.enqueue(project="Q", note="asked for", priority="p3")
    other = wt_env.queue.enqueue(project="Q", note="not asked", priority="p0")
    wt_env.queue.request_run(asked["ref"]) if hasattr(wt_env.queue, "request_run") else \
        wt_env.queue._set_run_requested(asked["ref"], True, "run_requested")

    result = run_cli("claim", "-q", "Q", "--worker", "w1", "--requested-only", "--json")
    assert result.code == 0, result.output
    assert asked["ref"] in result.out
    assert other["ref"] not in result.out

    result = run_cli("claim", "-q", "Q", "--worker", "w2", "--requested-only", "--json")
    assert other["ref"] not in result.out
    assert not wt_env.queue.get(other["ref"]).get("claimed_by")

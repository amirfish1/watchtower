"""`wt defer` on GitHub-backed queues (OPS-100).

The defer window lives in the issue's metadata block, so it must hold the
ticket out of claims and spawn depth on any host, and lapse by the clock alone.
"""

from __future__ import annotations

import json
import time

from test_github_backend import (
    _drainable,
    _install_fake_gh,
    _reload_isolated,
    restore_watchtower_modules,  # noqa: F401  (autouse fixture)
)


def _github_queue(tmp_path, monkeypatch):
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _drainable(config)
    return state, q


def test_github_defer_releases_claim_and_holds_ticket_out_of_pool(tmp_path, monkeypatch):
    state, q = _github_queue(tmp_path, monkeypatch)
    item = q.enqueue(project="GHI", note="wait for deploy")
    q.claim_by_ref(item["ref"], "worker-1")

    deferred = q.defer("GHI-1", "worker-1", "+90m", "waiting for prod deploy",
                       progress="pushed to next")

    assert deferred["status"] == "open"
    assert not deferred.get("claimed_by")
    assert q.deferred_now(deferred)
    assert deferred["defer_reason"] == "waiting for prod deploy"
    assert [e["event"] for e in deferred["history"]] == ["claim", "progress", "defer"]
    assert q.claim_next("worker-2", project="GHI") is None
    assert q.count_claimable(project="GHI") == 0
    issue = json.loads(state.read_text())["issues"][0]
    assert "deferred_until:" in issue["body"]


def test_github_defer_window_lapses_without_an_issue_edit(tmp_path, monkeypatch):
    _state, q = _github_queue(tmp_path, monkeypatch)
    item = q.enqueue(project="GHI", note="wait for data")
    q.defer(item["ref"], "worker-1", "+1h", "waiting for data")
    assert q.claim_next("worker-2", project="GHI") is None

    real_time = time.time
    monkeypatch.setattr(time, "time", lambda: real_time() + 2 * 3600)

    claimed = q.claim_next("worker-2", project="GHI")
    assert claimed and claimed["ref"] == "GHI-1"

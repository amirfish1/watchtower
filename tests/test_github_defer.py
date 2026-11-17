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


def test_github_defer_api_refuses_without_releasing_claim(tmp_path, monkeypatch):
    import pytest
    _state, q = _github_queue(tmp_path, monkeypatch)
    item = q.enqueue(project="GHI", note="future evidence")
    q.claim_by_ref(item["ref"], "worker-1")
    before = q.get(item["ref"])
    with pytest.raises(ValueError, match="retired"):
        q.defer(item["ref"], "worker-1", "+90m", "waiting for data")
    backend = q._github_backend_for_project("GHI")
    with pytest.raises(ValueError, match="retired"):
        backend.defer(item["ref"], session_id="worker-1", until_iso="2099-01-01T00:00:00Z", reason="data")
    assert q.get(item["ref"]) == before

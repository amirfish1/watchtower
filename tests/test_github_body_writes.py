"""GitHub body writers are serialized against claims (OPS-106).

Every writer rewrites the whole issue body; `gh issue edit` has no CAS. A
`wt comment` that read the body just before a claim landed wrote it back and
erased `claimed_by` (BECKY-2745), so the claimant could no longer close.
"""

from __future__ import annotations

import threading

from test_github_backend import (
    _drainable,
    _install_fake_gh,
    _reload_isolated,
    restore_watchtower_modules,  # noqa: F401  (autouse fixture)
)


def test_comment_racing_a_claim_keeps_the_claim(tmp_path, monkeypatch):
    _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _drainable(config)
    ref = q.enqueue(project="GHI", note="race")["ref"]
    backend = q._github_backend_for_project("GHI")

    real_get = type(backend).get
    claimer = threading.Thread(target=lambda: backend.claim_by_ref(ref, "worker-1"))
    raced = []

    def get_then_race(self, ident):
        item = real_get(self, ident)
        if not raced:   # the comment's read: a claim lands before its write
            raced.append(True)
            claimer.start()
            claimer.join(timeout=3)
        return item

    monkeypatch.setattr(type(backend), "get", get_then_race)
    backend.comment(ref, "status note", by="worker", session_id="worker-2")
    claimer.join(timeout=30)
    monkeypatch.setattr(type(backend), "get", real_get)

    item = backend.get(ref)
    assert item["status"] == "in_progress"
    assert item["claimed_by"] == "worker-1"
    assert [e["event"] for e in item["history"]] == ["comment", "claim"]

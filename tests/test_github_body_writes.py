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


# WT-20: a writer on another host is not serialized by the local flock. These
# tests play that writer by rewriting the fake gh state between our edit and
# our verification re-read, from a body it read before our write.
import json  # noqa: E402

import pytest  # noqa: E402


def _remote_writer(monkeypatch, backend, state, mutate):
    """After our first body edit lands, overwrite it with ``mutate(stale)``."""
    real_run = type(backend)._run
    fired = []

    def run(self, args, **kw):
        stale = None
        if args[:2] == ["issue", "edit"] and kw.get("body") is not None and not fired:
            stale = next(i for i in json.loads(state.read_text())["issues"]
                         if str(i["number"]) == str(args[2]))["body"]
        out = real_run(self, args, **kw)
        if stale is not None:
            fired.append(True)
            data = json.loads(state.read_text())
            issue = next(i for i in data["issues"] if str(i["number"]) == str(args[2]))
            issue["body"] = mutate(stale)
            state.write_text(json.dumps(data))
        return out

    monkeypatch.setattr(type(backend), "_run", run)
    return fired


def _setup(tmp_path, monkeypatch):
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _drainable(config)
    ref = q.enqueue(project="GHI", note="race")["ref"]
    return state, q, ref, q._github_backend_for_project("GHI")


def test_cross_host_stale_write_after_claim_is_reapplied(tmp_path, monkeypatch):
    from watchtower import github_backend as gb
    state, q, ref, backend = _setup(tmp_path, monkeypatch)

    def remote_comment(stale):
        human, meta = gb._split_body(stale)
        gb._append_history(meta, "comment", worker="remote", text="from the mac")
        return gb._body_with_metadata(human, meta)

    fired = _remote_writer(monkeypatch, backend, state, remote_comment)
    claimed = backend.claim_by_ref(ref, "worker-1")
    assert fired and claimed["claimed_by"] == "worker-1"
    item = backend.get(ref)
    assert item["claimed_by"] == "worker-1"
    # both writers' history survives the merge
    assert sorted(e["event"] for e in item["history"]) == ["claim", "comment"]


def test_claim_overwritten_by_another_hosts_claim_fails(tmp_path, monkeypatch):
    from watchtower import github_backend as gb
    state, q, ref, backend = _setup(tmp_path, monkeypatch)

    def remote_claim(stale):
        human, meta = gb._split_body(stale)
        meta.update(claimed_by="mac-worker", claimed_machine="mac")
        return gb._body_with_metadata(human, meta)

    _remote_writer(monkeypatch, backend, state, remote_claim)
    with pytest.raises(ValueError, match="did not persist"):
        backend.claim_by_ref(ref, "worker-1")
    assert backend.get(ref)["claimed_by"] == "mac-worker"


def test_merge_body_write_is_noop_when_fresh_carries_our_change():
    from watchtower import github_backend as gb
    base = gb._body_with_metadata("text", {"a": 1})
    ours = gb._body_with_metadata("text", {"a": 2, "b": 3})
    assert gb._merge_body_write(base, ours, ours) == (None, [])
    # a later writer that built on ours (added c) is not rewritten either
    later = gb._body_with_metadata("text", {"a": 2, "b": 3, "c": 4})
    assert gb._merge_body_write(base, ours, later) == (None, [])
    # theirs changed `a` too: kept, reported; our `b` is re-applied
    theirs = gb._body_with_metadata("text", {"a": 9})
    merged, conflicts = gb._merge_body_write(base, ours, theirs)
    assert conflicts == ["a"] and gb._split_body(merged)[1] == {"a": 9, "b": 3}
    # another claimant took it: nothing of ours is re-applied onto theirs
    claim = gb._body_with_metadata("text", {"claimed_by": "me", "claimed_at": "t"})
    mac = gb._body_with_metadata("text", {"claimed_by": "mac"})
    assert gb._merge_body_write(gb._body_with_metadata("text", {}), claim, mac) == (
        None, ["claimed_by"])

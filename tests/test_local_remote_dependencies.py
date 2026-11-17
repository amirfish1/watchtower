"""Real local persistence + offline GitHub issues, for both dependency directions."""
import pytest

from test_github_backend import _install_fake_gh, _reload_isolated, _drainable


@pytest.fixture
def mixed(tmp_path, monkeypatch):
    state = _install_fake_gh(tmp_path, monkeypatch)
    monkeypatch.setenv("WATCHTOWER_GH_ISSUE_TRANSPORT", "graphql")
    monkeypatch.setenv("WT_NO_AUTOSTART", "1")
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("REMOTE", "github")
    config.set_github_repo("REMOTE", "test-owner/test-repo")
    _drainable(config, "REMOTE")
    _drainable(config, "LOCAL")
    return q, state


def parent(q):
    return q.enqueue(project="REMOTE", title="owner acceptance", note="parent")["ref"]


def child(q, **kwargs):
    return q.enqueue(project="LOCAL", title="verify", note="child", **kwargs)["ref"]


def assert_waiting(q, ref):
    q.mark_runnable(ref)
    assert q.count_claimable(project="LOCAL") == 0
    assert q.count_manual_eligible(project="LOCAL") == 0
    assert q.peek_next(project="LOCAL") is None
    assert q.floor_routed_candidates("LOCAL", "") == []
    assert q.claim_next("downstream", project="LOCAL") is None
    with pytest.raises(ValueError, match="waiting|stuck"):
        q.claim_by_ref(ref, "downstream")


@pytest.mark.parametrize("edit", [False, True])
def test_local_dependency_waits_then_wakes(mixed, edit):
    q, _ = mixed
    upstream = parent(q)
    ref = child(q, blocked_by=[] if edit else [upstream])
    if edit:
        q.claim_by_ref(ref, "parked")
        q.block(ref, "parked", "existing dependency wait",
                wait_on="dependency", wait_ref=upstream)
        q.update(ref, blocked_by=[upstream], _dependency_wait=True)
    assert q.get(ref)["blocked_by"] == [upstream]
    assert q.get(ref)["needs_input"] is False
    assert_waiting(q, ref)
    # Owner-held remote parent must not wake its child.
    q.claim_by_ref(upstream, "upstream")
    q.block(upstream, "upstream", "owner acceptance remains outstanding")
    assert_waiting(q, ref)
    q.close(upstream, "upstream", resolution={"summary": "accepted", "no_code": True})
    assert q.count_claimable(project="LOCAL") == 1
    assert q.peek_next(project="LOCAL")["ref"] == ref
    import watchtower.health as health
    row = health.all_status(project="LOCAL", items=q.list_items(project="LOCAL"), fresh=True)[0]
    assert row["claimable_depth"] == 1 and row["blocked"] == 0
    assert q.claim_next("downstream", project="LOCAL")["ref"] == ref


@pytest.mark.parametrize("kind", ["unresolved", "product_nack"])
def test_local_remote_stuck_parent_never_wakes(mixed, kind, monkeypatch):
    q, state = mixed
    upstream = parent(q)
    ref = child(q, blocked_by=[upstream])
    q.claim_by_ref(upstream, "upstream")
    resolution = {"summary": "partial", "no_code": True}
    if kind == "unresolved":
        resolution["unresolved"] = ["REMOTE-999 pending"]
    q.close(upstream, "upstream", resolution=resolution)
    if kind == "product_nack":
        # GitHub has no product-gate mutation API; exercise its normalized
        # parent state, including the cached depth path, without inventing one.
        backend = q._github_backend_for_project("REMOTE")
        cls = type(backend)
        original_get, original_list = cls.get, cls.list_items
        def nack(item):
            if item and item.get("ref") == upstream:
                item["product_nack"] = {"comment": "declined"}
            return item
        monkeypatch.setattr(cls, "get", lambda self, *a, **kw: nack(original_get(self, *a, **kw)))
        monkeypatch.setattr(cls, "list_items", lambda self, *a, **kw: [nack(i) for i in original_list(self, *a, **kw)])
    assert_waiting(q, ref)
    if kind == "unresolved":
        # claim_next already ran the escalation, which is idempotent.
        assert q.get(ref)["needs_input"] is True
        assert q.escalate_stuck_blockers() is False


def test_validation_unknown_self_and_cross_backend_cycles(mixed):
    q, _ = mixed
    upstream = parent(q)
    ref = child(q, blocked_by=[upstream])
    with pytest.raises(ValueError, match="cycle"):
        q.update(upstream, blocked_by=[ref])
    with pytest.raises(ValueError, match="itself"):
        q.update(ref, blocked_by=[ref])
    with pytest.raises(ValueError, match="does not exist"):
        q.update(ref, blocked_by=["REMOTE-999"])
    with pytest.raises(ValueError, match="does not exist"):
        child(q, blocked_by=["REMOTE-999"])
    other = child(q)
    q.update(upstream, blocked_by=[other])
    with pytest.raises(ValueError, match="cycle"):
        q.update(other, blocked_by=[ref])


def test_missing_or_inaccessible_parent_fails_closed(mixed, monkeypatch):
    q, state = mixed
    upstream = parent(q)
    ref = child(q, blocked_by=[upstream])
    backend = q._github_backend_for_project("REMOTE")
    from watchtower.github_backend import GitHubBackendError
    monkeypatch.setattr(type(backend), "get", lambda *a, **kw: (_ for _ in ()).throw(GitHubBackendError("offline")))
    assert q.claim_next("downstream", project="LOCAL") is None
    with pytest.raises(ValueError, match="waiting"):
        q.claim_by_ref(ref, "downstream")
    with pytest.raises(GitHubBackendError, match="offline"):
        q.update(ref, blocked_by=[upstream])
    monkeypatch.setattr(type(backend), "get", lambda *a, **kw: None)
    assert q.claim_next("downstream", project="LOCAL") is None
    assert q.waiting_on(q.get(ref), {}) == [upstream]


def test_remote_reads_outside_local_lock_and_scoped(mixed, monkeypatch):
    q, _ = mixed
    upstream = parent(q)
    ref = child(q, blocked_by=[upstream])
    backend = q._github_backend_for_project("REMOTE")
    orig_get = type(backend).get
    held = []
    orig_enter, orig_exit = q._FileLock.__enter__, q._FileLock.__exit__
    def enter(lock):
        result = orig_enter(lock)
        held.append(lock)
        return result
    def exit(lock, *args):
        held.remove(lock)
        return orig_exit(lock, *args)
    def get(backend, ident):
        assert not held, "remote I/O under local lock"
        return orig_get(backend, ident)
    monkeypatch.setattr(q._FileLock, "__enter__", enter)
    monkeypatch.setattr(q._FileLock, "__exit__", exit)
    monkeypatch.setattr(type(backend), "get", get)
    q.update(ref, blocked_by=[upstream])
    q.peek_next(project="LOCAL")
    q.claim_next("downstream", project="LOCAL")
    # A different queue without dependencies must never contact GitHub.
    def forbidden(*a, **kw):
        raise AssertionError("unrelated remote lookup")
    monkeypatch.setattr(type(backend), "get", forbidden)
    monkeypatch.setattr(type(backend), "list_items", forbidden)
    q.enqueue(project="OTHER", note="independent")
    assert q.count_claimable(project="OTHER") == 1
    assert q.claim_next("independent", project="OTHER")


def test_concurrent_new_edge_cannot_skip_cycle_validation(mixed, monkeypatch):
    q, _ = mixed
    upstream = parent(q)
    first, second = child(q), child(q)
    original = q._dependency_snapshot
    def snapshot(**kwargs):
        result = original(**kwargs)
        # Another writer adds a previously unseen remote edge before our lock.
        with q._FileLock(q._lock_path()):
            data = q._load_unlocked()
            for it in data["items"]:
                if it["ref"] == second:
                    it["blocked_by"] = [upstream]
            q._save_unlocked(data)
        return result
    monkeypatch.setattr(q, "_dependency_snapshot", snapshot)
    with pytest.raises(ValueError, match="dependency snapshot"):
        q.update(first, blocked_by=[second])
    assert q.get(first)["blocked_by"] == []


def test_installed_cli_edit_wait_and_wake(mixed, tmp_path, monkeypatch):
    import json
    import os
    import shutil
    import subprocess

    q, _ = mixed
    upstream = parent(q)
    ref = child(q)
    # All state, worker files, fake gh and config paths are isolated by mixed.
    monkeypatch.setenv("WATCHTOWER_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("WT_NO_SELF_UPDATE", "1")
    exe = shutil.which("wt")
    assert exe, "installed wt CLI required for this acceptance check"
    def cli(*args):
        result = subprocess.run([exe, *args], capture_output=True, text=True,
                                env=dict(os.environ), timeout=30)
        assert result.returncode == 0, result.stderr + result.stdout
        return result.stdout
    changed = json.loads(cli("edit", ref, "--after", upstream, "--json"))
    assert changed["blocked_by"] == [upstream]
    assert changed["needs_input"] is False
    rows = json.loads(cli("ls", "-q", "LOCAL", "--json"))
    assert next(r for r in rows if r["ref"] == ref)["waiting_on"] == [upstream]
    assert not cli("claim", "-q", "LOCAL", "--worker", "synthetic-wt85", "--json").strip()
    q.claim_by_ref(upstream, "synthetic-upstream")
    q.close(upstream, "synthetic-upstream", resolution={"summary": "accepted", "no_code": True})
    claimed = json.loads(cli("claim", "-q", "LOCAL", "--worker", "synthetic-wt85", "--json"))
    assert claimed["ref"] == ref

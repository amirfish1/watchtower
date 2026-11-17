"""Release waits never expire into the pool; only a containing build moves them."""
import importlib
from datetime import datetime, timedelta, timezone

import pytest

from tests.legacy_defer import seed as seed_defer, snapshot

from tests.test_owner_view import _load, _ticket
from tests.test_release_wait import repo  # noqa: F401


@pytest.fixture(params=["file", "github"])
def env(request, tmp_path, monkeypatch):
    result = _load(tmp_path, monkeypatch, request.param)
    from watchtower import releases
    importlib.reload(releases)
    return (*result, releases)


def park(env, repo, *, live=True, commit=None):
    q, cli, _a, _ov, name, r = env
    ref = _ticket(q, name)
    r.park(ref, "w1", repo=repo["path"], commit=commit or repo["merged"],
           branch="origin/next", target="ccc:dashboard", reason="merged fix; awaiting adoption",
           live_check=live, summary="Fixed the reported problem.\n\n- Verified tests.",
           verified="focused regression suite passed")
    return ref


def test_wait_is_distinct_hidden_and_unclaimable_forever(env, repo, capsys):
    q, cli, _a, _ov, name, r = env
    ref = park(env, repo)
    item = q.get(ref)
    assert item["status"] == "awaiting_release"
    assert not item.get("deferred_until") and not item.get("claimed_by")
    assert q.count_claimable(project=name) == 0
    with pytest.raises(ValueError, match="not open"):
        q.claim_by_ref(ref, "w2")
    assert cli.main(["ls", "-q", name, "--json"]) == 0
    assert ref not in capsys.readouterr().out
    assert cli.main(["ls", "-q", name, "--status", "awaiting_release", "--json"]) == 0
    assert ref in capsys.readouterr().out
    from watchtower import liveness
    assert liveness.classify(liveness.project(item)).id == "release.wait"


def test_event_is_build_checked_and_live_check_happens_once(env, repo):
    q, _cli, _a, _ov, name, r = env
    ref = park(env, repo, commit=repo["relanded"])
    assert r.observe("watchtower:local", repo["merged"], evidence="other target") == []
    assert r.observe("ccc:dashboard", repo["base"], evidence="old build") == []
    assert r.observe("ccc:dashboard", "deadbeef", evidence="unknown build") == []
    # Cherry-picked fix: same patch in the serving build counts.
    from tests.test_release_wait import _git
    build = _git(repo["path"], "rev-parse", "origin/next")
    assert r.observe("ccc:dashboard", build, evidence="running process report") == [dict(ref=ref, status="open")]
    assert q.get(ref)["release_event"]["build"] == build
    assert not q.get(ref).get("release_wait")
    assert r.observe("ccc:dashboard", build, evidence="repeat") == []
    assert q.count_claimable(project=name) == 1
    assert q.claim_by_ref(ref, "w2")


def test_preproven_close_preserves_summary_commit_and_evidence(env, repo):
    q, _cli, _a, _ov, _name, r = env
    ref = park(env, repo, live=False)
    assert r.observe("ccc:dashboard", repo["merged"], evidence="process boot") == [dict(ref=ref, status="closed")]
    item = q.get(ref)
    assert item["resolution"]["commit"] == repo["merged"]
    assert item["resolution"]["summary"].startswith("Fixed the reported problem.")
    assert item["release_event"]["wait"]["verified"] == "focused regression suite passed"
    assert r.observe("ccc:dashboard", repo["merged"], evidence="repeat") == []


def test_cannot_park_stranded_wrong_owner_or_missing_acceptance(env, repo):
    q, _cli, _a, _ov, name, r = env
    ref = _ticket(q, name)
    kwargs = dict(repo=repo["path"], commit=repo["merged"], branch="origin/next",
                  target="ccc:dashboard", reason="waiting")
    with pytest.raises(ValueError, match="claimed by"):
        r.park(ref, "wrong", **kwargs)
    with pytest.raises(ValueError, match="no release will carry"):
        r.park(ref, "w1", **dict(kwargs, commit=repo["stranded"]))
    with pytest.raises(ValueError, match="acceptance evidence"):
        r.park(ref, "w1", live_check=False, **kwargs)
    assert q.get(ref)["status"] == "in_progress"


def test_observation_cas_does_not_reopen_manually_changed_ticket(env, repo):
    q, _cli, _a, _ov, _name, r = env
    ref = park(env, repo)
    stale = q.get(ref)
    q.update_status(ref, "closed", resolution={"summary": "manual close"})
    assert r.observe("ccc:dashboard", repo["merged"], evidence="late report", items=[stale]) == []
    assert q.get(ref)["status"] == "closed"


def test_owner_separates_waits_and_surfaces_three_day_or_dead_branch(env, repo):
    q, _cli, _a, ov, _name, r = env
    ref = park(env, repo)
    wait = q.get(ref)["release_wait"]
    entered = datetime.fromisoformat(wait["entered_at"].replace("Z", "+00:00"))
    fresh = ov.build(q.list_items(), now=entered + timedelta(days=2))
    assert fresh["blocked"] == 0 and len(fresh["awaiting_release"]) == 1
    assert fresh["release_attention"] == []
    stale = ov.build(q.list_items(), now=entered + timedelta(days=4))
    assert stale["release_attention"][0]["ref"] == ref
    from tests.test_release_wait import _git
    _git(repo["path"], "update-ref", "refs/remotes/origin/next", repo["base"])
    broken = ov.build(q.list_items(), now=entered)
    assert broken["release_attention"][0]["branch_state"] == "missing"
    assert r.observe("ccc:dashboard", repo["merged"], evidence="build") == []


def test_migration_preserves_old_window_and_never_touches_integration(env, repo, monkeypatch):
    q, _cli, _a, _ov, name, r = env
    good = _ticket(q, name)
    reason = f"[awaits release: {repo['merged'][:12]} on origin/next] next release"
    old = seed_defer(q, good, "w1", "+1d", reason)
    other = _ticket(q, name, worker="w2")
    seed_defer(q, other, "w2", "+1d", "[awaits integration: owner s; path PR] waiting")
    monkeypatch.setattr(r.config, "repo_path", lambda _: repo["path"])
    monkeypatch.setattr(r, "migration_target", lambda _: "ccc:dashboard")
    # GitHub backend config supplies its own path; fixture's is empty.
    dry = r.migrate(project=name)
    assert [i["ref"] for i in dry["candidates"]] == [good]
    assert q.get(good)["status"] == "open"
    result = r.migrate(project=name, apply=True)
    assert [i["ref"] for i in result["migrated"]] == [good]
    wait = q.get(good)["release_wait"]
    assert wait["previous_defer"]["until"] == old["deferred_until"]
    assert q.get(other)["status"] == "open" and snapshot(q, other)["deferred_until"]


def test_probe_failure_does_not_wake_and_targets_are_coalesced(env, repo, monkeypatch):
    q, _cli, _a, _ov, _name, r = env
    refs = [park(env, repo), park(env, repo)]
    calls = []
    def failed(target):
        calls.append(target)
        raise OSError("offline")
    monkeypatch.setattr(r, "probe", failed)
    assert r.reconcile(force=True) == [] and calls == ["ccc:dashboard"]
    assert all(q.get(ref)["status"] == "awaiting_release" for ref in refs)


def test_github_probe_uses_only_latest_successful_environment(env, monkeypatch):
    *_, r = env
    monkeypatch.setattr(r, "_gh", lambda path: ([dict(id=9, environment="production", sha="a" * 40)]
        if "deployments?" in path else [dict(state="pending")]))
    assert r.probe("github:test/repo:production") == ("", "")
    monkeypatch.setattr(r, "_gh", lambda path: ([dict(id=9, environment="production", sha="a" * 40)]
        if "deployments?" in path else [dict(state="success")]))
    assert r.probe("github:test/repo:production")[0] == "a" * 40


def test_same_status_new_wait_generation_is_not_overwritten(env, repo):
    q, _cli, _a, _ov, _name, r = env
    ref = park(env, repo)
    stale = q.get(ref)
    q.update_status(ref, "open", reason="different delivery")
    q.claim_by_ref(ref, "w2")
    r.park(ref, "w2", repo=repo["path"], commit=repo["relanded"], branch="origin/next",
           target="ccc:dashboard", reason="new fix must ship")
    assert r.observe("ccc:dashboard", repo["merged"], evidence="old event", items=[stale]) == []
    assert q.get(ref)["release_wait"]["worker"] == "w2"


def test_real_running_build_endpoint_wakes_wait(env, repo):
    import json
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    q, _cli, _a, _ov, name, r = env
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"code_rev": repo["merged"]}).encode())
        def log_message(self, *args):
            pass
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        ref = _ticket(q, name)
        r.park(ref, "w1", repo=repo["path"], commit=repo["merged"], branch="origin/next",
               target=f"http://127.0.0.1:{server.server_port}/api/version", reason="adoption")
        assert r.reconcile(force=True) == [dict(ref=ref, status="open")]
        assert q.get(ref)["release_event"]["evidence"].startswith("running build from http:")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_required_gates_cannot_be_bypassed_by_automatic_close(env, repo, monkeypatch):
    q, _cli, _a, _ov, name, r = env
    ref = _ticket(q, name)
    kwargs = dict(repo=repo["path"], commit=repo["merged"], branch="origin/next",
                  target="ccc:dashboard", reason="adoption", live_check=False,
                  summary="Fixed", verified="passed")
    monkeypatch.setattr(q, "effective_gates", lambda _: ["verify"])
    with pytest.raises(ValueError, match="required gates"):
        r.park(ref, "w1", **kwargs)
    monkeypatch.setattr(q, "effective_gates", lambda _: [])
    r.park(ref, "w1", **kwargs)
    monkeypatch.setattr(q, "effective_gates", lambda _: ["verify"])
    assert r.observe("ccc:dashboard", repo["merged"], evidence="report") == [dict(ref=ref, status="open")]


def test_github_production_environment_is_exact_and_url_encoded(env, monkeypatch):
    *_, r = env
    calls = []
    env_name = "Production – bookyourmat-app"
    def gh(path):
        calls.append(path)
        return ([dict(id=2, environment=env_name, sha="a" * 40)] if "deployments?" in path
                else [dict(state="success")])
    monkeypatch.setattr(r, "_gh", gh)
    assert r.probe(f"github:owner/repo:{env_name}")[0] == "a" * 40
    assert "environment=Production%20%E2%80%93%20bookyourmat-app" in calls[0]


def test_github_wait_uses_explicit_state_without_a_clock_sentinel(env, repo):
    q, _cli, _a, _ov, name, r = env
    if name != "GHI":
        pytest.skip("GitHub wire compatibility")
    ref = park(env, repo)
    from watchtower.github_backend import _split_body
    item = q.get(ref)
    _, meta = _split_body(item["_github_body"])
    assert "deferred_until" not in meta
    assert meta["release_wait"] and item["status"] == "awaiting_release"
    assert not item["work_it"] and q.claim_next("other", project=name) is None
    r.observe("ccc:dashboard", repo["merged"], evidence="process")
    item = q.get(ref)
    _, meta = _split_body(item["_github_body"])
    assert "deferred_until" not in meta and "release_wait" not in meta
    assert item["run_requested"]


def test_daemon_version_endpoint_reports_frozen_boot_build(env, monkeypatch, tmp_path):
    import json
    import threading
    import urllib.request
    from watchtower import dashboard
    *_, r = env
    monkeypatch.setattr(r, "RUNTIME_BUILD", "a" * 40)
    httpd = dashboard.ThreadingHTTPServer(("127.0.0.1", 0), dashboard._Handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{httpd.server_port}/api/version") as res:
            assert json.load(res)["code_rev"] == "a" * 40
        # CCC may publish a port different from 8090, which another app owns.
        monkeypatch.delenv("CCC_SERVER", raising=False)
        port_file = tmp_path / "ccc-port.txt"
        port_file.write_text(f"http://127.0.0.1:{httpd.server_port}")
        monkeypatch.setattr(r, "CCC_PORT_FILE", port_file)
        assert r.probe("ccc:dashboard")[0] == "a" * 40
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=2)


def test_legacy_release_defer_is_retired_even_when_target_is_known(env, repo, monkeypatch, capsys):
    q, cli, _a, _ov, name, r = env
    ref = _ticket(q, name)
    monkeypatch.setattr(r, "migration_target", lambda _: "ccc:dashboard")
    before = q.get(ref)
    assert cli.main(["defer", ref, "--worker", "w1", "--until", "+1d", "--reason", "adoption",
                     "--awaits", "release", "--commit", repo["merged"],
                     "--release-branch", "origin/next", "--repo", repo["path"], "--json"]) == 1
    assert "wt await-release" in capsys.readouterr().err
    assert q.get(ref) == before


def test_await_release_and_observe_cli_round_trip(env, repo, capsys):
    import json
    q, cli, _a, _ov, name, _r = env
    ref = _ticket(q, name)
    args = ["await-release", ref, "--worker", "w1", "--commit", repo["merged"],
            "--release-branch", "origin/next", "--repo", repo["path"],
            "--target", "ccc:dashboard", "--reason", "adoption", "--live-check", "--json"]
    assert cli.main(args) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "awaiting_release"
    assert cli.main(["release-observe", "--target", "ccc:dashboard", "--build", repo["merged"],
                     "--evidence", "frozen process revision"]) == 0
    assert json.loads(capsys.readouterr().out) == [dict(ref=ref, status="open")]
    assert q.get(ref)["run_requested"]


def test_new_release_commit_is_fetched_before_build_comparison(env, repo, tmp_path):
    from tests.test_release_wait import _git
    q, _cli, _a, _ov, name, r = env
    bare = tmp_path / "origin.git"
    _git(repo["path"], "init", "--bare", str(bare))
    _git(repo["path"], "remote", "add", "origin", str(bare))
    _git(repo["path"], "push", "origin", "next")
    ref = park(env, repo)
    builder = tmp_path / "builder"
    _git(repo["path"], "clone", "--branch", "next", str(bare), str(builder))
    _git(builder, "config", "user.email", "t@example.com")
    _git(builder, "config", "user.name", "t")
    (builder / "release.txt").write_text("new production build\n")
    _git(builder, "add", "release.txt")
    _git(builder, "commit", "-m", "release after parking")
    build = _git(builder, "rev-parse", "HEAD")
    _git(builder, "push", "origin", "next")
    assert r.release_wait.on_branch(repo["path"], repo["merged"], build)["state"] == "unknown"
    assert r.observe("ccc:dashboard", build, evidence="new running release") == [dict(ref=ref, status="open")]
    # A deleted release branch must not keep a stale origin/next as proof.
    q.claim_by_ref(ref, "w2")
    r.park(ref, "w2", repo=repo["path"], commit=repo["merged"], branch="origin/next",
           target="ccc:dashboard", reason="another release check")
    _git(builder, "push", "origin", "--delete", "next")
    assert r.observe("ccc:dashboard", build, evidence="branch was deleted") == []
    assert r.owner_rows([q.get(ref)])[0]["branch_state"] == "unknown"


def test_release_wait_satisfies_dependents_and_dispatches_once_per_queue(env, repo, monkeypatch, capsys):
    import json
    from watchtower import workers
    q, cli, _a, _ov, name, r = env
    parent = _ticket(q, name)
    children = [q.enqueue(project=name, note="dependent", blocked_by=[parent])["ref"]
                for _ in range(2)]
    assert q.count_claimable(project=name) == 0
    calls = []

    def dispatch(project, ref):
        # The transition must be persisted and its lock released first.
        assert q.get(parent)["status"] == "awaiting_release"
        assert q.count_claimable(project=name) == 2
        calls.append((project, ref))

    monkeypatch.setattr(workers, "dispatch_after_enqueue", dispatch)
    r.park(parent, "w1", repo=repo["path"], commit=repo["merged"],
           branch="origin/next", target="ccc:dashboard", reason="adoption")
    assert calls == [(name, children[0])]
    assert q.get(parent)["status"] == "awaiting_release"
    assert cli.main(["ls", "-q", name, "--json"]) == 0
    rows = json.loads(capsys.readouterr().out)
    assert {row["ref"] for row in rows} == set(children)
    assert all(not row["waiting_on"] for row in rows)
    assert q.claim_by_ref(children[0], "w2")
    assert q.claim_next("w3", project=name)["ref"] == children[1]


@pytest.mark.parametrize("status", ["closed", "awaiting_release"])
def test_release_completion_preserves_stuck_and_missing_blockers(env, status):
    q, *_ = env
    parent = dict(ref="T-1", status=status)
    child = dict(ref="T-2", status="open", blocked_by=["T-1"])
    assert q._blocker_state(parent) == "satisfied"
    assert q.blocker_verdict(child, {"T-1": parent}) == ("ok", "")
    for flags in ({"product_nack": True}, {"resolution": {"unresolved": ["T-3 remaining"]}}):
        stuck = dict(parent, **flags)
        assert q._blocker_state(stuck) == "stuck"
        assert q.blocker_verdict(child, {"T-1": stuck}) == ("stuck", "T-1")
        assert q.waiting_on(child, {"T-1": stuck}) == ["T-1"]
    assert q.blocker_verdict(child, {}) == ("waiting", "T-1")
    assert q.waiting_on(dict(child, status="awaiting_release"), {}) == []


def test_completed_dependents_do_not_appear_in_unblock_notifications(env, monkeypatch):
    q, *_ = env
    rows = [dict(ref=f"T-{i}", status=status, blocked_by=["T-1"])
            for i, status in enumerate(("open", "closed", "awaiting_release"), 2)]
    monkeypatch.setattr(q, "list_items", lambda **_: rows)
    assert q._dependents_of("T-1") == ["T-2"]


def test_legacy_dependency_answer_names_release_state(env):
    q, _cli, answers, *_ = env
    assert "code complete, awaiting release" in answers.dependency_answer_text(
        dict(ref="T-1", status="awaiting_release"))


def test_legacy_dependency_wait_resumes_on_release_completion(env, repo, monkeypatch):
    q, cli, answers, _ov, name, r = env
    parent = park(env, repo)
    child = _ticket(q, name, worker="w2")
    q.block(child, session_id="w2", question="wait for prerequisite",
            wait_on="dependency", wait_ref=parent)
    calls = []
    monkeypatch.setattr(cli, "deliver_dependency_answer", lambda *args: calls.append(args) or True)
    assert answers.resume_dependency_waits() == [f"{child}:{parent}"]
    assert "code complete, awaiting release" in calls[0][1]


def test_dashboard_progress_counts_release_wait_as_complete(env, monkeypatch):
    from watchtower import dashboard
    q, _cli, _a, _ov, name, _r = env
    rows = [dict(ref=f"{name}-{i}", project=name, status=status)
            for i, status in enumerate(("open", "closed", "awaiting_release"), 1)]
    monkeypatch.setattr(q, "list_items", lambda **_: rows)
    page = dashboard.render_index(dict(queues=[dict(queue=name)], workers=[]), chat_rows=[])
    assert '<span>2/3</span>' in page

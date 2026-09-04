"""GitHub Issues-backed queue tests.

These tests keep GitHub offline by putting a tiny fake ``gh`` executable at the
front of PATH. The fake persists issue state to a temp JSON file so the queue
module can exercise create/list/view/edit/close as subprocess calls.
"""

from __future__ import annotations

import importlib
import json
import os
import threading
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest


FAKE_GH = r'''#!/usr/bin/env python3
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

state_path = Path(os.environ["FAKE_GH_STATE"])


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load():
    if state_path.exists():
        return json.loads(state_path.read_text())
    return {"next": 1, "issues": [], "commands": []}


def save(data):
    state_path.write_text(json.dumps(data, indent=2))


def opt(args, name, default=""):
    if name in args:
        i = args.index(name)
        if i + 1 < len(args):
            return args[i + 1]
    return default


def opts(args, name):
    out = []
    i = 0
    while i < len(args):
        if args[i] == name and i + 1 < len(args):
            out.append(args[i + 1])
            i += 2
        else:
            i += 1
    return out


def issue_by_number(data, number):
    number = int(number.lstrip("#"))
    for issue in data["issues"]:
        if int(issue["number"]) == number:
            return issue
    print(f"issue {number} not found", file=sys.stderr)
    sys.exit(1)


def project_fields(issue):
    return {
        "number": issue["number"],
        "title": issue["title"],
        "body": issue["body"],
        "state": issue["state"],
        "url": issue["url"],
        "assignees": [{"login": a} for a in issue["assignees"]],
        "labels": [{"name": name} for name in issue["labels"]],
        "comments": issue.get("comments", []),
        "createdAt": issue["createdAt"],
        "updatedAt": issue["updatedAt"],
        "closedAt": issue.get("closedAt"),
    }


data = load()
args = sys.argv[1:]
data["commands"].append(args)

if args[:2] == ["label", "create"]:
    save(data)
    sys.exit(0)

if args[:2] == ["issue", "create"]:
    repo = opt(args, "--repo", "owner/repo")
    number = data["next"]
    data["next"] += 1
    issue = {
        "number": number,
        "title": opt(args, "--title"),
        "body": opt(args, "--body"),
        "state": "OPEN",
        "url": f"https://github.com/{repo}/issues/{number}",
        "assignees": [],
        "labels": opts(args, "--label"),
        "createdAt": now(),
        "updatedAt": now(),
        "closedAt": None,
        "comments": [],
    }
    data["issues"].append(issue)
    save(data)
    print(issue["url"])
    sys.exit(0)

if args[:2] == ["api", "-i"]:
    # The conditional GET WatchTower uses as a change detector. The ETag is a
    # hash of the same issue state `issue list` reads, so "unchanged" here
    # means exactly what it means on GitHub. Reproduces the landmine too:
    # real `gh api` EXITS 1 on a 304, with `gh: HTTP 304` on stderr.
    path = args[-1]
    want_state = "CLOSED" if "state=closed" in path else "OPEN"
    payload = json.dumps(
        [project_fields(i) for i in data["issues"] if i["state"] == want_state],
        sort_keys=True,
    )
    etag = '"' + hashlib.sha256(payload.encode()).hexdigest() + '"'
    sent = ""
    for i, a in enumerate(args):
        if a == "-H" and i + 1 < len(args):
            name, _, value = args[i + 1].partition(":")
            if name.strip().lower() == "if-none-match":
                sent = value.strip()
    save(data)
    if sent and sent == etag:
        print("HTTP/2.0 304 Not Modified")
        print("Etag: " + etag)
        print("gh: HTTP 304", file=sys.stderr)
        sys.exit(1)
    print("HTTP/2.0 200 OK")
    print("Etag: " + etag)
    print("Content-Type: application/json; charset=utf-8")
    print()
    print(payload)
    sys.exit(0)

if args[:2] == ["issue", "list"]:
    want_state = opt(args, "--state", "open").upper()
    want_label = opt(args, "--label")
    issues = list(data["issues"])
    if want_state != "ALL":
        issues = [i for i in issues if i["state"] == want_state]
    if want_label:
        issues = [i for i in issues if want_label in i["labels"]]
    save(data)
    print(json.dumps([project_fields(i) for i in issues]))
    sys.exit(0)

if args[:2] == ["issue", "view"]:
    issue = issue_by_number(data, args[2])
    save(data)
    print(json.dumps(project_fields(issue)))
    sys.exit(0)

if args[:2] == ["issue", "edit"]:
    issue = issue_by_number(data, args[2])
    for assignee in opts(args, "--add-assignee"):
        if assignee not in issue["assignees"]:
            issue["assignees"].append(assignee)
    for label in opts(args, "--add-label"):
        if label not in issue["labels"]:
            issue["labels"].append(label)
    for label in opts(args, "--remove-label"):
        if label in issue["labels"]:
            issue["labels"].remove(label)
    if "--title" in args:
        issue["title"] = opt(args, "--title")
    if "--body" in args:
        issue["body"] = opt(args, "--body")
    issue["updatedAt"] = now()
    save(data)
    sys.exit(0)

if args[:2] == ["issue", "close"]:
    issue = issue_by_number(data, args[2])
    issue["state"] = "CLOSED"
    issue["closedAt"] = now()
    issue["updatedAt"] = issue["closedAt"]
    comment = opt(args, "--comment")
    if comment:
        issue["comments"].append(comment)
    save(data)
    print(f"Closed issue #{issue['number']}")
    sys.exit(0)

if args[:2] == ["issue", "comment"]:
    issue = issue_by_number(data, args[2])
    body = opt(args, "--body")
    if body:
        issue["comments"].append({"author": {"login": "watchtower"}, "body": body})
    issue["updatedAt"] = now()
    save(data)
    print(f"https://github.com/{opt(args, '--repo', 'owner/repo')}/issues/{issue['number']}#issuecomment-{len(issue['comments'])}")
    sys.exit(0)

if args[:2] == ["repo", "view"]:
    save(data)
    print(json.dumps({
        "visibility": os.environ.get("FAKE_GH_VISIBILITY", "private"),
    }))
    sys.exit(0)

if args[:2] == ["issue", "reopen"]:
    issue = issue_by_number(data, args[2])
    issue["state"] = "OPEN"
    issue["closedAt"] = None
    issue["updatedAt"] = now()
    save(data)
    print(f"Reopened issue #{issue['number']}")
    sys.exit(0)

print("unsupported fake gh command: " + " ".join(args), file=sys.stderr)
save(data)
sys.exit(2)
'''


@pytest.fixture(autouse=True)
def restore_watchtower_modules(tmp_path, monkeypatch):
    # Isolate the GitHub connectivity state file for every test in this
    # module, including the many below that call `_list_issues` directly
    # without going through `_reload_isolated`. Without this, a synthetic
    # failure in one test (e.g. "gh auth unavailable") would write to the
    # real `~/.watchtower/gh-connectivity.json` on the developer's machine
    # and then leak into whichever test runs next.
    monkeypatch.setenv(
        "WATCHTOWER_GH_CONNECTIVITY_FILE", str(tmp_path / "autouse-gh-connectivity.json")
    )
    yield
    import watchtower.config as config
    import watchtower.health as health
    import watchtower.queue as q
    import watchtower.workers as workers

    importlib.reload(config)
    importlib.reload(q)
    importlib.reload(health)
    importlib.reload(workers)


def _install_fake_gh(tmp_path: Path, monkeypatch):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    gh = bin_dir / "gh"
    gh.write_text(FAKE_GH)
    gh.chmod(0o755)
    state = tmp_path / "gh-state.json"
    monkeypatch.setenv("FAKE_GH_STATE", str(state))
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}")
    return state


def _reload_isolated(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_CONFIG_FILE", str(tmp_path / "config.json"))
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(tmp_path / "activity.log"))
    monkeypatch.setenv(
        "WATCHTOWER_CCC_SPAWN_DEFAULTS_FILE", str(tmp_path / "no-ccc-spawn-defaults.json")
    )
    monkeypatch.setenv(
        "WATCHTOWER_GH_CONNECTIVITY_FILE", str(tmp_path / "gh-connectivity.json")
    )
    monkeypatch.setenv(
        "WATCHTOWER_GH_LIST_CACHE_FILE", str(tmp_path / "gh-list-cache.json")
    )
    monkeypatch.setenv(
        "WATCHTOWER_GH_CLAIM_LOCKS_DIR", str(tmp_path / "gh-claim-locks")
    )
    # The worker registry has to be sandboxed too: the drain-off tests below
    # call workers.reconcile_once(), which without this reads the machine's
    # live ~/.watchtower/workers.json (and takes the real reconcile lock).
    # Whether a spawn was planned then depended on what the actual fleet was
    # doing at that second -- these tests failed whenever enough real workers
    # were alive.
    monkeypatch.setenv("WATCHTOWER_WORKERS_FILE", str(tmp_path / "workers.json"))
    monkeypatch.setenv(
        "WATCHTOWER_LAUNCH_FAILURES_FILE", str(tmp_path / "launch-failures.json")
    )
    monkeypatch.setenv("WATCHTOWER_STOP_SIGNALS_DIR", str(tmp_path / "stop-signals"))
    monkeypatch.setenv(
        "WATCHTOWER_WORKER_SESSIONS_FILE", str(tmp_path / "worker-sessions.json")
    )
    monkeypatch.setenv("WATCHTOWER_WORKER_IDS_FILE", str(tmp_path / "worker-ids.json"))
    import watchtower.config as config
    import watchtower.github_backend as github_backend
    import watchtower.queue as q
    import watchtower.workers as _workers

    importlib.reload(config)
    # Reset github_backend's module-level `_list_issues` cache (WT-87): every
    # test here reuses the same "owner/repo" placeholder, so a stale entry
    # from a prior test would otherwise leak into this one within its TTL.
    importlib.reload(github_backend)
    importlib.reload(q)
    importlib.reload(_workers)
    # Reconciler tests must not import the developer's legacy registry, which
    # can contain live GitHub-backed queues and trigger real ``gh`` calls.
    monkeypatch.setattr(config, "_REGISTRY_FILE", tmp_path / "no-legacy-registry.json")
    # Pretend the one-time GitHub drain migration already ran (it has, on any
    # real install, long before these code paths run). Tests that exercise the
    # migration itself remove this marker first.
    config.GH_DRAIN_MIGRATION_MARKER.parent.mkdir(parents=True, exist_ok=True)
    config.GH_DRAIN_MIGRATION_MARKER.write_text("{}\n")
    return config, q


def test_github_backend_fixture_sandboxes_legacy_queue_registry(tmp_path, monkeypatch):
    """Reconciler tests must not import a developer's live legacy queues."""
    config, _q = _reload_isolated(tmp_path, monkeypatch)

    assert config._REGISTRY_FILE == tmp_path / "no-legacy-registry.json"


def _drainable(config, queue: str = "GHI") -> None:
    """Make ``queue`` behave like a queue an operator has opted in to draining:
    auto-drain on with no grace period. Under the eligibility model that is
    what makes an ordinary open ticket claimable at all."""
    config.set_auto_drain(queue, True)
    config.set_grace_s(queue, 0)


def _no_etag_probe(monkeypatch, backend):
    """Neutralise the ETag change detector on ``backend``.

    Tests that fake `gh` by patching ``_run`` need this: the probe is a
    *separate* `gh` invocation (``_run_raw``), so without it they would shell
    out to the real GitHub API. ``(None, etag)`` is the detector's "unusable"
    answer, which makes ``_list_issues`` behave exactly as it did before ETags
    -- one unconditional `gh issue list` per refresh, which is what these tests
    are counting.
    """
    monkeypatch.setattr(
        backend, "_probe_list_change", lambda state, etag: (None, etag)
    )


def _write_fake_issues(state: Path, issues):
    state.write_text(json.dumps({"next": 1 + len(issues), "issues": issues, "commands": []}, indent=2))


def _fake_issue(
    number: int, title: str, labels=None, assignees=None, body: str = "", comments=None,
):
    labels = labels or []
    assignees = assignees or []
    comments = comments or []
    return {
        "number": number,
        "title": title,
        "body": body,
        "state": "OPEN",
        "url": f"https://github.com/owner/repo/issues/{number}",
        "assignees": assignees,
        "labels": labels,
        "createdAt": "2026-07-01T12:00:00Z",
        "updatedAt": "2026-07-01T12:00:00Z",
        "closedAt": None,
        "comments": comments,
    }


def test_github_backend_rejects_placeholder_repos():
    """Common README/test placeholders must not be persisted as real repos."""
    from watchtower import config

    for placeholder in ("owner/repo", "acme/repo", "ACME/REPO"):
        with pytest.raises(ValueError, match="placeholder"):
            config.set_github_repo("GHI", placeholder)


def test_github_backend_enqueue_claim_close_round_trip(tmp_path, monkeypatch):
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _drainable(config)

    item = q.enqueue(
        project="GHI",
        title="Fix GitHub-backed queue",
        note="short note",
        text="full body",
        item_type="feature",
        readiness="ready",
        priority="p1",
    )

    assert item["ref"] == "GHI-1"
    assert item["status"] == "open"
    assert item["type"] == "feature"
    assert item["priority"] == "p1"

    assert q.list_items(project="GHI")[0]["ref"] == "GHI-1"

    claimed = q.claim_next("worker-1", project="GHI")
    assert claimed["ref"] == "GHI-1"
    assert claimed["status"] == "in_progress"
    assert claimed["claimed_by"] == "worker-1"
    assert q.claim_next("worker-2", project="GHI") is None
    # WT-87: claim/close append to an embedded, append-only history trail
    # (stored in the issue-body metadata block) instead of only overwriting
    # the latest claimed_by/closed_by snapshot.
    assert [e["event"] for e in claimed["history"]] == ["claim"]
    assert claimed["history"][0]["worker"] == "worker-1"

    closed = q.close("GHI-1", "worker-1", resolution={"summary": "fixed it"})
    assert closed["status"] == "closed"
    assert closed["closed_by"] == "worker-1"
    assert closed["resolution"]["summary"] == "fixed it"
    assert [e["event"] for e in closed["history"]] == ["claim", "close"]
    assert closed["history"][1]["resolution"]["summary"] == "fixed it"

    gh_state = json.loads(state.read_text())
    issue = gh_state["issues"][0]
    assert issue["state"] == "CLOSED"
    assert "@me" in issue["assignees"]
    assert any("fixed it" in c for c in issue["comments"])


def test_github_backend_claim_by_ref_serializes_concurrent_claimants(tmp_path, monkeypatch):
    """Regression for a double-claim race: two workers claiming the same ref
    at the same instant (e.g. both nudged awake for a stuck queue) must not
    both win. `gh issue edit` has no compare-and-swap, so without a lock
    around claim_by_ref's read-check-write section, two concurrent claimants
    can both read the issue as open before either write lands, then both
    write themselves in as the claimant -- observed live as
    BYM-GH-FINIE-780 claimed by two different workers one second apart."""
    _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _drainable(config)

    item = q.enqueue(project="GHI", note="racy ticket", source="test")

    start = threading.Barrier(2)
    results: list = []
    lock = threading.Lock()

    def attempt(worker: str) -> None:
        start.wait()
        try:
            claimed = q.claim_by_ref(item["ref"], worker)
            outcome = ("ok", claimed)
        except ValueError as exc:
            outcome = ("error", str(exc))
        with lock:
            results.append((worker, outcome))

    threads = [
        threading.Thread(target=attempt, args=("worker-a",)),
        threading.Thread(target=attempt, args=("worker-b",)),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    assert len(results) == 2
    winners = [w for w, (kind, _) in results if kind == "ok"]
    losers = [w for w, (kind, _) in results if kind == "error"]
    assert len(winners) == 1, f"expected exactly one winner, got {results}"
    assert len(losers) == 1

    final = q.get(item["ref"])
    assert final["status"] == "in_progress"
    assert final["claimed_by"] == winners[0]
    assert [e["event"] for e in final["history"]] == ["claim"]
    assert final["history"][0]["worker"] == winners[0]


def test_claim_next_falls_through_stale_closed_candidate(tmp_path, monkeypatch):
    """OPS-841: a candidate that is open in the listing snapshot but already
    closed by the time claim_by_ref re-reads it (closed between list and
    claim, or served from a lagging list cache) must be skipped in favour of
    the next open candidate -- not fail the whole claim and block the drain.
    When it is the only candidate, claim_next must report a drained queue
    (None) instead of raising."""
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _drainable(config)

    stale = q.enqueue(project="GHI", note="closed behind the listing's back", source="test")
    real = q.enqueue(project="GHI", note="real claimable work", source="test")
    assert stale["ref"] == "GHI-1"
    assert real["ref"] == "GHI-2"

    # Capture the open-shaped snapshot of GHI-1, then close it directly in
    # the fake GitHub state (a concurrent close the listing never saw).
    stale_snapshot = next(
        it for it in q.list_items(project="GHI") if it["ref"] == "GHI-1"
    )
    gh_state = json.loads(state.read_text())
    gh_state["issues"][0]["state"] = "CLOSED"
    gh_state["issues"][0]["closedAt"] = "2026-08-31T00:00:00Z"
    state.write_text(json.dumps(gh_state))

    # Serve the stale listing (GHI-1 still open) to every new backend.
    import watchtower.github_backend as github_backend

    orig_list_items = github_backend.GitHubIssuesBackend.list_items

    def stale_listing(self, *args, **kwargs):
        return [stale_snapshot] + [
            it for it in orig_list_items(self, *args, **kwargs)
            if it.get("ref") != "GHI-1"
        ]

    monkeypatch.setattr(
        github_backend.GitHubIssuesBackend, "list_items", stale_listing
    )

    claimed = q.claim_next("worker-1", project="GHI")
    assert claimed is not None
    assert claimed["ref"] == "GHI-2"
    assert claimed["claimed_by"] == "worker-1"

    # Once the real ticket is claimed too, the stale GHI-1 is the only
    # remaining listing entry: the queue must read as drained, not error.
    assert q.claim_next("worker-2", project="GHI") is None


def test_github_backend_rejects_crossworker_close(tmp_path, monkeypatch):
    _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _drainable(config)

    item = q.enqueue(project="GHI", note="claimed work", source="test")
    q.claim_by_ref(item["ref"], "worker-a")

    with pytest.raises(ValueError, match="claimed by worker-a"):
        q.close(item["ref"], "worker-b", resolution={"summary": "wrong worker"})
    assert q.get(item["ref"])["status"] == "in_progress"


def test_github_backend_imports_issue_title_and_comments_into_worker_text(
    tmp_path, monkeypatch,
):
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _write_fake_issues(state, [
        _fake_issue(
            1,
            "Actual GitHub title",
            body="Original report",
            comments=[{
                "author": {"login": "reporter"},
                "body": "Additional reproduction details",
                "createdAt": "2026-07-02T12:00:00Z",
            }],
        ),
    ])

    item = q.get("GHI-1")

    assert item["title"] == "Actual GitHub title"
    assert "GitHub comments" in item["text"]
    assert "@reporter" in item["text"]
    assert "Additional reproduction details" in item["text"]
    assert any(
        command[:2] == ["issue", "view"] and any("comments" in arg for arg in command)
        for command in json.loads(state.read_text())["commands"]
    )


def test_github_backend_lists_issues_without_requesting_bulk_comments(monkeypatch):
    """A GitHub 503 for nested comments must not make the queue unreadable."""
    import watchtower.github_backend as github_backend

    github_backend._LIST_CACHE.clear()
    backend = github_backend.GitHubIssuesBackend("GHI", repo="owner/repo")
    issue = _fake_issue(1, "Still listable", body="Issue body")
    calls = []

    def fake_run(args, *, check=True):
        calls.append(args)
        if "comments" in args[args.index("--json") + 1].split(","):
            raise github_backend.GitHubBackendError("gh issue list failed: HTTP 503")
        return json.dumps([{key: value for key, value in issue.items() if key != "comments"}])

    monkeypatch.setattr(backend, "_run", fake_run)
    _no_etag_probe(monkeypatch, backend)

    assert backend.list_items() == [backend._issue_to_item(issue)]
    assert all(
        "comments" not in call[call.index("--json") + 1].split(",")
        for call in calls
    )


def test_github_backend_blocks_claimed_ticket_by_documented_ref(tmp_path, monkeypatch):
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _drainable(config)

    item = q.enqueue(project="GHI", note="needs a decision")
    q.claim_by_ref(item["ref"], "worker-1")

    blocked = q.block(
        "GHI-1", session_id="worker-1",
        question="Which rollout should we use?", progress="Both options verified.",
    )

    assert blocked["ref"] == "GHI-1"
    assert blocked["status"] == "in_progress"
    assert blocked["needs_input"] is True
    assert blocked["block_question"] == "Which rollout should we use?"
    assert [event["event"] for event in blocked["history"]] == [
        "claim", "progress", "block",
    ]
    issue = json.loads(state.read_text())["issues"][0]
    assert "needs_input: true" in issue["body"]


def test_list_blocked_and_active_claims_see_github_backed_tickets(
    tmp_path, monkeypatch,
):
    """Regression guard: list_blocked/list_active_claims used to scan only
    the file-backed store, so they silently returned nothing for a
    github-backed queue. That fed workers.py's blocked-worker-exclusion
    staffing math (WT-129): a live worker holding nothing but a blocked
    github ticket read as fully productive, so its queue could sit at
    "staffed == desired" with a growing unclaimed backlog and never spawn a
    replacement -- exactly the state a real BYM-GH-FINIE queue was found in
    (2/2 desired workers, both blocked, 7 open bugs untouched)."""
    _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _drainable(config)

    blocked_item = q.enqueue(project="GHI", note="needs a decision")
    q.claim_by_ref(blocked_item["ref"], "worker-1")
    q.block("GHI-1", session_id="worker-1", question="A or B?")

    active_item = q.enqueue(project="GHI", note="still being worked")
    q.claim_by_ref(active_item["ref"], "worker-2")

    assert [it["ref"] for it in q.list_blocked(project="GHI")] == ["GHI-1"]
    assert [it["ref"] for it in q.list_active_claims(project="GHI")] == ["GHI-2"]
    # No-project form (what workers.py's reconciler actually calls) must see
    # it too, not just the project-scoped form.
    assert any(it["ref"] == "GHI-1" for it in q.list_blocked())
    assert any(it["ref"] == "GHI-2" for it in q.list_active_claims())


def test_github_backend_answer_posts_comment_and_keeps_claim_with_session(
    tmp_path, monkeypatch,
):
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _drainable(config)

    item = q.enqueue(project="GHI", note="needs a decision")
    q.claim_by_ref(item["ref"], "worker-1", session_uuid="session-abc")
    q.block("GHI-1", session_id="worker-1", question="A or B?")

    answered = q.answer("GHI-1", "Go with A", session_id="human-1")

    assert answered["ref"] == "GHI-1"
    assert answered["status"] == "in_progress"
    assert answered["needs_input"] is False
    assert answered["claimed_session_id"] == "session-abc"
    assert [event["event"] for event in answered["history"]] == [
        "claim", "block", "answer",
    ]

    issue = json.loads(state.read_text())["issues"][0]
    assert "needs_input: false" in issue["body"]
    assert any(
        isinstance(c, dict) and c.get("body") == "Go with A"
        for c in issue["comments"]
    )
    assert any(
        command[:2] == ["issue", "comment"]
        for command in json.loads(state.read_text())["commands"]
    )


def test_github_backend_answer_releases_claim_without_session(tmp_path, monkeypatch):
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _drainable(config)

    item = q.enqueue(project="GHI", note="needs a decision")
    q.claim_by_ref(item["ref"], "worker-1")
    q.block("GHI-1", session_id="worker-1", question="A or B?")

    answered = q.answer("GHI-1", "Go with B", session_id="human-1")

    assert answered["ref"] == "GHI-1"
    assert answered["status"] == "open"
    assert answered["needs_input"] is False
    assert answered.get("claimed_by") is None
    assert [event["event"] for event in answered["history"]] == [
        "claim", "block", "answer", "reopen",
    ]

    issue = json.loads(state.read_text())["issues"][0]
    assert issue["state"] == "OPEN"
    assert "watchtower:in-progress" not in issue["labels"]
    assert any(
        isinstance(c, dict) and c.get("body") == "Go with B"
        for c in issue["comments"]
    )


# --- Owner-answer ingestion (WATCHTOWER-5) -------------------------------

def _add_issue_comment(state_path, number, login, body):
    """Append a raw comment (as if typed on GitHub) to the fake gh state."""
    data = json.loads(state_path.read_text())
    for issue in data["issues"]:
        if int(issue["number"]) == int(number):
            issue.setdefault("comments", []).append(
                {"author": {"login": login}, "body": body}
            )
            # GitHub bumps an issue's updated_at when a comment is posted --
            # the property `_list_probe_path` relies on to make the ETag probe
            # notice comments, and the one the owner-answer sweep now uses to
            # decide a blocked ticket is worth re-reading. The fake has to
            # model it or those paths are tested against a repo that never
            # appears to move.
            # Strictly greater than whatever is there: GitHub's updated_at has
            # second resolution, and a test that blocks and comments inside the
            # same second would otherwise model an issue that never moved.
            stamp = datetime.now(timezone.utc).replace(microsecond=0)
            prev = issue.get("updatedAt")
            if prev:
                prev_dt = datetime.strptime(prev, "%Y-%m-%dT%H:%M:%SZ").replace(
                    tzinfo=timezone.utc
                )
                stamp = max(stamp, prev_dt + timedelta(seconds=1))
            issue["updatedAt"] = stamp.strftime("%Y-%m-%dT%H:%M:%SZ")
    state_path.write_text(json.dumps(data, indent=2))


def _view_count(state_path, number):
    """How many `gh issue view <number>` calls the fake gh has served.

    `gh issue view` is a GraphQL read, so this is the quota meter for the
    owner-answer sweep.
    """
    commands = json.loads(state_path.read_text())["commands"]
    return sum(
        1 for c in commands
        if c[:2] == ["issue", "view"] and str(c[2]) == str(number)
    )


def _setup_blocked_github_ticket(tmp_path, monkeypatch, *, with_session):
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _drainable(config)
    item = q.enqueue(project="GHI", note="needs a decision")
    if with_session:
        q.claim_by_ref(item["ref"], "worker-1", session_uuid="session-abc")
    else:
        q.claim_by_ref(item["ref"], "worker-1")
    q.block("GHI-1", session_id="worker-1", question="A or B?")
    return state, config, q


def test_ingest_owner_answer_requeues_blocked_ticket_without_session(
    tmp_path, monkeypatch,
):
    """A repo-owner comment carrying the marker clears the block and, with no
    resumable session, requeues the ticket -- the WATCHTOWER-5 gap."""
    import watchtower.github_backend as github_backend

    state, config, q = _setup_blocked_github_ticket(
        tmp_path, monkeypatch, with_session=False
    )
    _add_issue_comment(state, 1, "test-owner", "OWNER ANSWER: go with A")

    backend = q._github_backend_for_project("GHI")
    updated = backend.ingest_owner_answer("GHI-1")

    assert updated is not None
    assert updated["needs_input"] is False
    assert updated["status"] == "open"          # requeued for a fresh worker
    assert updated.get("claimed_by") is None
    assert [e["event"] for e in updated["history"]] == [
        "claim", "block", "answer", "reopen",
    ]
    issue = json.loads(state.read_text())["issues"][0]
    assert "ingested_answer_keys" in issue["body"]
    # The owner's own comment must NOT be echoed back as a duplicate; only the
    # best-effort "[watchtower] Ingested owner answer" ack is added.
    watchtower_comments = [
        c for c in issue["comments"]
        if isinstance(c, dict) and c.get("author", {}).get("login") == "watchtower"
    ]
    assert len(watchtower_comments) == 1
    assert "Ingested owner answer" in watchtower_comments[0]["body"]
    assert "requeued" in watchtower_comments[0]["body"]


def test_ingest_owner_answer_keeps_claim_when_session_resumable(
    tmp_path, monkeypatch,
):
    """With a resumable session the block clears but the claim is kept, so the
    live worker resumes rather than the ticket being requeued."""
    state, config, q = _setup_blocked_github_ticket(
        tmp_path, monkeypatch, with_session=True
    )
    _add_issue_comment(state, 1, "test-owner", "OWNER ANSWER: proceed")

    backend = q._github_backend_for_project("GHI")
    updated = backend.ingest_owner_answer("GHI-1")

    assert updated is not None
    assert updated["needs_input"] is False
    assert updated["status"] == "in_progress"
    assert updated["claimed_session_id"] == "session-abc"
    assert [e["event"] for e in updated["history"]] == ["claim", "block", "answer"]
    issue = json.loads(state.read_text())["issues"][0]
    ack = [
        c for c in issue["comments"]
        if isinstance(c, dict) and c.get("author", {}).get("login") == "watchtower"
    ]
    assert len(ack) == 1 and "resume" in ack[0]["body"]


def test_ingest_owner_answer_is_idempotent(tmp_path, monkeypatch):
    """The same owner comment can never clear a block twice."""
    import watchtower.github_backend as github_backend

    state, config, q = _setup_blocked_github_ticket(
        tmp_path, monkeypatch, with_session=True
    )
    _add_issue_comment(state, 1, "test-owner", "OWNER ANSWER: proceed")

    backend = q._github_backend_for_project("GHI")
    assert backend.ingest_owner_answer("GHI-1") is not None
    # Second sweep: block already cleared -> nothing to do.
    assert backend.ingest_owner_answer("GHI-1") is None

    # Even if the ticket is somehow blocked again while the SAME comment sits
    # there, the recorded key prevents a re-trigger (the direct idempotency
    # guarantee, independent of needs_input clearing).
    q.block("GHI-1", session_id="worker-1", question="again?")
    assert backend.ingest_owner_answer("GHI-1") is None
    # exactly one 'answer' event across the whole history
    hist = backend.get("GHI-1")["history"]
    assert [e["event"] for e in hist].count("answer") == 1


def test_ingest_owner_answer_ignores_nonowner_and_unmarked(tmp_path, monkeypatch):
    """A non-owner comment, or an owner comment without the marker, is ignored."""
    state, config, q = _setup_blocked_github_ticket(
        tmp_path, monkeypatch, with_session=False
    )
    _add_issue_comment(state, 1, "random-contributor", "OWNER ANSWER: do X")
    _add_issue_comment(state, 1, "test-owner", "looks good, thanks!")

    backend = q._github_backend_for_project("GHI")
    assert backend.ingest_owner_answer("GHI-1") is None
    assert backend.get("GHI-1")["needs_input"] is True


def test_poll_owner_answers_once_sweeps_blocked_tickets(tmp_path, monkeypatch):
    """The module sweep enumerates github queues, finds blocked tickets, and
    ingests owner answers -- the path wired into the daemon poller."""
    import watchtower.github_backend as github_backend

    state, config, q = _setup_blocked_github_ticket(
        tmp_path, monkeypatch, with_session=False
    )
    _add_issue_comment(state, 1, "test-owner", "OWNER ANSWER: ship it")

    github_backend.poll_owner_answers_once()

    assert q.get("GHI-1")["needs_input"] is False
    assert q.get("GHI-1")["status"] == "open"


def test_poll_owner_answers_skips_quiet_blocked_ticket(tmp_path, monkeypatch):
    """A blocked ticket nobody has touched costs ONE live `gh issue view`, not
    one per sweep.

    `ingest_owner_answer` must fetch comments before it can tell there is
    nothing to ingest, so the sweep used to spend a GraphQL read per blocked
    ticket every few seconds forever -- measured at ~1,800 points/hour against
    a 5,000/hour quota from two stuck tickets
    (docs/github-quota-exhaustion-2026-09-02.md).
    """
    import watchtower.github_backend as github_backend

    state, _config, _q = _setup_blocked_github_ticket(
        tmp_path, monkeypatch, with_session=False
    )

    base = _view_count(state, 1)
    github_backend.poll_owner_answers_once()
    first = _view_count(state, 1)
    for _ in range(5):
        github_backend.poll_owner_answers_once()
    later = _view_count(state, 1)

    assert first - base == 1     # the sweep pays for one look
    assert later == first        # nothing moved, so nothing more is spent


def test_poll_owner_answers_reads_again_once_the_issue_moves(tmp_path, monkeypatch):
    """The throttle is keyed on the snapshot's updated_at, so a real owner
    answer is still ingested -- the WATCHTOWER-5 guarantee survives."""
    import watchtower.github_backend as github_backend

    state, _config, q = _setup_blocked_github_ticket(
        tmp_path, monkeypatch, with_session=False
    )

    github_backend.poll_owner_answers_once()   # quiet sweep records the probe
    _add_issue_comment(state, 1, "test-owner", "OWNER ANSWER: ship it")
    # The sweep reads updated_at from the list snapshot, so it only sees the
    # comment once that snapshot refreshes -- within `_LIST_FETCH_MIN_INTERVAL_S`
    # in production, immediately here.
    github_backend._LIST_CACHE.clear()
    github_backend.poll_owner_answers_once()

    assert q.get("GHI-1")["needs_input"] is False
    assert q.get("GHI-1")["status"] == "open"


def test_owner_answer_probe_due_tracks_movement_and_backstops(tmp_path, monkeypatch):
    """The gate itself: quiet is skipped, movement re-reads, and an unchanged
    timestamp is never trusted past the backstop."""
    import watchtower.github_backend as gb

    gb._owner_answer_probes.clear()

    assert gb._owner_answer_probe_due("repo#1", "T1", 1000.0) is True   # first look
    assert gb._owner_answer_probe_due("repo#1", "T1", 1010.0) is False  # quiet
    assert gb._owner_answer_probe_due("repo#1", "T2", 1020.0) is True   # moved
    assert gb._owner_answer_probe_due("repo#1", "T2", 1025.0) is False
    # A frozen or missing timestamp must degrade to the old always-read
    # behaviour at a bounded rate, never to "silently never ingest again".
    assert gb._owner_answer_probe_due(
        "repo#1", "T2", 1025.0 + gb._OWNER_ANSWER_MAX_QUIET_S
    ) is True
    assert gb._owner_answer_probe_due("repo#2", "", 2000.0) is True
    assert gb._owner_answer_probe_due("repo#2", "", 2001.0) is True


def test_prune_owner_answer_probes_drops_unblocked_tickets(tmp_path, monkeypatch):
    """Entries for tickets that stopped being blocked must not accumulate over
    a daemon lifetime measured in weeks."""
    import watchtower.github_backend as gb

    gb._owner_answer_probes.clear()
    gb._owner_answer_probe_due("repo#stale", "T1", 1000.0)
    gb._owner_answer_probe_due("repo#live", "T1", 5000.0)

    gb._prune_owner_answer_probes(5000.0 + gb._OWNER_ANSWER_MAX_QUIET_S)

    assert "repo#stale" not in gb._owner_answer_probes
    assert "repo#live" in gb._owner_answer_probes


def test_github_backend_comment_posts_issue_comment_and_records_history(
    tmp_path, monkeypatch,
):
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _drainable(config)

    item = q.enqueue(project="GHI", note="needs a decision")
    q.claim_by_ref(item["ref"], "worker-1")

    commented = q.comment("GHI-1", "Heads up: checking dependencies.", session_id="human-1")

    assert commented["ref"] == "GHI-1"
    assert commented["status"] == "in_progress"
    assert [event["event"] for event in commented["history"]] == ["claim", "comment"]

    issue = json.loads(state.read_text())["issues"][0]
    assert any(
        isinstance(c, dict) and c.get("body") == "Heads up: checking dependencies."
        for c in issue["comments"]
    )


def test_cli_answer_and_comment_resolve_github_backed_refs(tmp_path, monkeypatch, capsys):
    # cmd_claim reads these from the real environment to attribute the
    # claiming session (cli.py's session_uuid lookup). Left unset, a claim
    # made inside a live Claude/Codex session picks up that session's real
    # id, and the comment/answer calls below then deliver a real
    # "[WATCHTOWER] ..." message into that live session instead of staying
    # inside the test's isolated queue state.
    monkeypatch.delenv("CLAUDE_CODE_SESSION_ID", raising=False)
    monkeypatch.delenv("CODEX_THREAD_ID", raising=False)
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, _q = _reload_isolated(tmp_path, monkeypatch)
    from watchtower.cli import main

    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _drainable(config)

    assert main(["add", "-q", "GHI", "--title", "blocked work", "--note", "blocked"]) == 0
    assert main(["claim", "-q", "GHI", "--worker", "cli-worker"]) == 0
    assert main([
        "block", "GHI-1", "--worker", "cli-worker",
        "--question", "Which path?",
    ]) == 0

    assert main([
        "comment", "GHI-1", "--by", "human",
        "--worker", "cli-worker", "Adding context before the answer.",
    ]) == 0
    out = capsys.readouterr().out
    assert "COMMENTED: GHI-1" in out

    assert main([
        "answer", "GHI-1", "--worker", "cli-worker", "Take path A",
    ]) == 0
    out = capsys.readouterr().out
    assert "ANSWERED: GHI-1" in out

    issue = json.loads(state.read_text())["issues"][0]
    assert any(
        isinstance(c, dict) and c.get("body") == "Take path A"
        for c in issue["comments"]
    )


def test_cli_can_configure_and_use_github_backend(tmp_path, monkeypatch, capsys):
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, _q = _reload_isolated(tmp_path, monkeypatch)
    from watchtower.cli import main

    assert main([
        "set", "-q", "GHCLI",
        "--backend", "github",
        "--github-repo", "test-owner/test-repo",
    ]) == 0
    _drainable(config, "GHCLI")
    assert main([
        "add", "-q", "GHCLI",
        "--title", "CLI issue",
        "--note", "from cli",
    ]) == 0
    out = capsys.readouterr().out
    assert "FILED: GHCLI-1" in out

    assert main(["claim", "-q", "GHCLI", "--worker", "cli-worker"]) == 0
    out = capsys.readouterr().out
    assert "CLAIMED: GHCLI-1 -> cli-worker" in out

    assert main([
        "close", "GHCLI-1",
        "--worker", "cli-worker",
        "--summary", "closed via gh",
        "--no-code",
    ]) == 0
    out = capsys.readouterr().out
    assert "CLOSED: GHCLI-1" in out

    gh_state = json.loads(state.read_text())
    commands = [" ".join(c) for c in gh_state["commands"]]
    assert any(c.startswith("issue create") for c in commands)
    assert any(c.startswith("issue edit 1") and "--add-assignee @me" in c for c in commands)
    assert any(c.startswith("issue close 1") for c in commands)


def test_cli_edit_text_replaces_github_issue_body_and_preserves_metadata(
    tmp_path, monkeypatch, capsys,
):
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    item = q.enqueue(
        project="GHI",
        note="short summary",
        text="original body",
        priority="p1",
    )
    from watchtower.cli import main

    assert main(["edit", item["ref"], "--text", "replacement body"]) == 0
    capsys.readouterr()

    edited = q.get(item["ref"])
    assert edited["text"] == "replacement body"
    assert edited["note"] == "short summary"
    assert edited["priority"] == "p1"
    issue_body = json.loads(state.read_text())["issues"][0]["body"]
    assert issue_body.startswith("replacement body\n\n<!-- watchtower\n")
    assert "original body" not in issue_body


def test_github_backend_ignores_legacy_queue_label_for_a_single_queue_repo(tmp_path, monkeypatch):
    """The `watchtower:<QUEUE>` whitelist is inert: with one queue on a repo,
    a plain issue nobody labelled is as workable as a labelled one."""
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _drainable(config)
    _write_fake_issues(state, [
        _fake_issue(1, "Plain GitHub issue"),
        _fake_issue(2, "Legacy-labelled issue", labels=["watchtower:GHI"]),
    ])

    items = q.list_items(project="GHI")
    assert [it["ref"] for it in items] == ["GHI-1", "GHI-2"]
    assert {it["ref"]: it["claimable"] for it in items} == {
        "GHI-1": True,
        "GHI-2": True,
    }

    assert q.claim_next("worker-1", project="GHI")["ref"] == "GHI-1"
    assert q.claim_next("worker-2", project="GHI")["ref"] == "GHI-2"
    assert q.claim_next("worker-3", project="GHI") is None


def test_github_backend_still_partitions_a_repo_shared_by_two_queues(tmp_path, monkeypatch):
    """The one job the legacy label keeps: when 2+ queues point at the same
    repo it is the only thing that can say which issue belongs to which."""
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    for name in ("GHA", "GHB"):
        config.set_backend(name, "github")
        config.set_github_repo(name, "owner/shared")
        config.set_auto_drain(name, True)
        config.set_grace_s(name, 0)
    _write_fake_issues(state, [
        _fake_issue(1, "Unlabelled — belongs to neither", labels=[]),
        _fake_issue(2, "Queue A work", labels=["watchtower:GHA"]),
        _fake_issue(3, "Queue B work", labels=["watchtower:GHB"]),
    ])

    assert [it["ref"] for it in q.list_items(project="GHA")] == ["GHA-2"]
    assert [it["ref"] for it in q.list_items(project="GHB")] == ["GHB-3"]
    assert q.count_claimable(project="GHA") == 1
    assert q.claim_next("worker-a", project="GHA")["ref"] == "GHA-2"
    assert q.claim_next("worker-a2", project="GHA") is None


def test_github_backend_lists_issues_closed_within_last_14_days(tmp_path, monkeypatch):
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")

    now = datetime.now(timezone.utc)
    recent = _fake_issue(1, "Recently completed")
    recent["state"] = "CLOSED"
    recent["closedAt"] = (now - timedelta(days=13)).strftime("%Y-%m-%dT%H:%M:%SZ")
    old = _fake_issue(2, "Old completion")
    old["state"] = "CLOSED"
    old["closedAt"] = (now - timedelta(days=15)).strftime("%Y-%m-%dT%H:%M:%SZ")
    open_issue = _fake_issue(3, "Still open")
    _write_fake_issues(state, [recent, old, open_issue])

    assert [it["ref"] for it in q.list_items(project="GHI")] == [
        "GHI-1", "GHI-3",
    ]
    assert [it["ref"] for it in q.list_items(project="GHI", status="closed")] == [
        "GHI-1",
    ]
    assert q.list_items(project="GHI", status="closed")[0]["claimable"] is False

    commands = json.loads(state.read_text())["commands"]
    closed_lists = [
        command for command in commands
        if command[:2] == ["issue", "list"] and "closed" in command
    ]
    assert closed_lists
    assert any(
        arg.startswith("closed:>=")
        for arg in closed_lists[0]
    )


def test_github_backend_refuses_direct_claim_until_a_run_is_requested(tmp_path, monkeypatch):
    """With drain off nothing is auto-eligible, so a targeted claim is refused
    — but pressing run (mark_runnable) overrides that, no whitelist involved."""
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _write_fake_issues(state, [_fake_issue(1, "Plain GitHub issue")])

    with pytest.raises(ValueError, match="not eligible to run"):
        q.claim_by_ref("GHI-1", "worker-1")

    marked = q.mark_runnable("GHI-1")
    assert marked["run_requested"] is True
    assert marked["manual_eligible"] is True
    assert marked["claimable"] is True
    assert "watchtower:play" in json.loads(state.read_text())["issues"][0]["labels"]
    # The inert label is never written any more.
    assert "watchtower:GHI" not in json.loads(state.read_text())["issues"][0]["labels"]

    claimed = q.claim_by_ref("GHI-1", "worker-1")
    assert claimed["status"] == "in_progress"


def test_github_drain_off_issues_are_visible_but_not_spawn_worthy(tmp_path, monkeypatch):
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    import watchtower.health as health
    import watchtower.workers as workers

    importlib.reload(health)
    importlib.reload(workers)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _write_fake_issues(state, [_fake_issue(1, "Plain GitHub issue")])

    row = {r["queue"]: r for r in health.all_status()}["GHI"]
    assert row["depth"] == 1
    assert row["claimable_depth"] == 0
    assert row["state"] == "backlog"

    result = workers.reconcile_once(dry_run=True)
    assert result["spawned"] == []
    assert any(skip["queue"] == "GHI" for skip in result["skipped"])

    # Opting the queue in is all it takes now -- no per-issue labelling.
    _drainable(config)
    importlib.reload(health)
    importlib.reload(workers)
    row = {r["queue"]: r for r in health.all_status()}["GHI"]
    assert row["claimable_depth"] == 1
    assert [s["queue"] for s in workers.reconcile_once(dry_run=True)["spawned"]] == ["GHI"]


def test_github_drain_off_queue_still_staffs_a_requested_run(tmp_path, monkeypatch):
    """The ▶ dead end: with drain off the reconciler skipped the queue outright,
    so a ticket a human asked to run never got a worker at all."""
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    import watchtower.workers as workers

    importlib.reload(workers)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _write_fake_issues(state, [_fake_issue(1, "Parked"), _fake_issue(2, "Run this")])

    assert workers.reconcile_once(dry_run=True)["spawned"] == []

    q.mark_runnable("GHI-2")

    # The queue is still not auto-draining -- the requested ticket is the only
    # thing that counts as depth now.
    assert q.count_claimable(project="GHI") == 0
    assert q.count_manual_eligible(project="GHI") == 1
    assert [s["queue"] for s in workers.reconcile_once(dry_run=True)["spawned"]] == ["GHI"]


def test_github_run_request_can_be_cancelled_while_still_queued(tmp_path, monkeypatch):
    """Press ▶ again while queued and the ticket goes back to parked."""
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _write_fake_issues(state, [_fake_issue(1, "Plain GitHub issue")])

    q.mark_runnable("GHI-1")
    cleared = q.clear_run_request("GHI-1")

    assert cleared["run_requested"] is False
    assert cleared["work_it"] is False
    assert "watchtower:play" not in json.loads(state.read_text())["issues"][0]["labels"]
    assert q.count_manual_eligible(project="GHI") == 0
    with pytest.raises(ValueError, match="not eligible to run"):
        q.claim_by_ref("GHI-1", "worker-1")


def test_github_no_auto_drain_label_keeps_a_ticket_out_of_auto_drain(tmp_path, monkeypatch):
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _drainable(config)
    _write_fake_issues(state, [
        _fake_issue(1, "Hands off", labels=["watchtower:no-auto-drain"]),
        _fake_issue(2, "Fair game"),
    ])

    assert q.count_claimable(project="GHI") == 1
    assert q.claim_next("worker-1", project="GHI")["ref"] == "GHI-2"
    with pytest.raises(ValueError, match="not eligible to run"):
        q.claim_by_ref("GHI-1", "worker-2")

    # ...until a human presses run, which beats the opt-out.
    q.mark_runnable("GHI-1")
    assert q.claim_by_ref("GHI-1", "worker-2")["status"] == "in_progress"


def test_cli_run_marks_existing_github_issue_runnable(tmp_path, monkeypatch, capsys):
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, _q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _write_fake_issues(state, [_fake_issue(1, "Plain GitHub issue")])
    from watchtower.cli import main

    assert main(["run", "GHI-1", "--no-dispatch"]) == 0
    out = capsys.readouterr().out
    assert "RUNNABLE: GHI-1" in out
    assert "watchtower:play" in json.loads(state.read_text())["issues"][0]["labels"]


def test_dashboard_run_api_marks_existing_github_issue_runnable(tmp_path, monkeypatch):
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, _q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _write_fake_issues(state, [_fake_issue(1, "Plain GitHub issue")])
    import watchtower.dashboard as dashboard

    importlib.reload(dashboard)
    dispatched = []
    monkeypatch.setattr(
        dashboard.workers,
        "dispatch_after_enqueue",
        lambda queue, ref="": dispatched.append((queue, ref)) or "nudged",
    )
    httpd = dashboard.ThreadingHTTPServer(("127.0.0.1", 0), dashboard._Handler)
    port = httpd.server_address[1]
    t = threading.Thread(target=httpd.handle_request, daemon=True)
    t.start()
    try:
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/ticket/GHI-1/run",
            data=b"{}",
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            payload = json.loads(resp.read().decode())
    finally:
        t.join(timeout=5)
        httpd.server_close()

    assert payload["ok"] is True
    assert payload["ticket"]["claimable"] is True
    assert payload["ticket"]["run_requested"] is True
    assert payload["dispatch"] == "nudged"
    assert dispatched == [("GHI", "GHI-1")]
    assert "watchtower:play" in json.loads(state.read_text())["issues"][0]["labels"]


def test_list_issues_caches_and_falls_back_to_stale_data_on_error(monkeypatch):
    """WT-87: a live dashboard calling list_items() every few seconds must not
    re-hit `gh issue list` on every single call -- especially once that repo
    is already rate-limited, which never gave the limit a chance to recover
    and flooded the activity log with one identical ERROR per poll."""
    import watchtower.github_backend as github_backend

    monkeypatch.setattr(github_backend, "_LIST_CACHE_TTL", 0.05)
    monkeypatch.setattr(github_backend, "_LIST_ERROR_BACKOFF", 0.2)
    # Error-backoff behaviour, not fetch cadence: let every expired read attempt gh.
    monkeypatch.setattr(github_backend, "_LIST_FETCH_MIN_INTERVAL_S", 0.0)
    github_backend._LIST_CACHE.clear()

    backend = github_backend.GitHubIssuesBackend("T", repo="acme/cache-test")
    calls = {"n": 0}
    good_issue = {
        "number": 1, "title": "t", "body": "", "state": "OPEN",
        "url": "https://github.com/acme/cache-test/issues/1",
        "assignees": [], "labels": [], "createdAt": "2026-07-01T00:00:00Z",
        "updatedAt": "2026-07-01T00:00:00Z", "closedAt": None,
    }

    def fake_run(args, *, check=True):
        calls["n"] += 1
        return json.dumps([good_issue])

    monkeypatch.setattr(backend, "_run", fake_run)
    _no_etag_probe(monkeypatch, backend)

    # Two calls within the TTL window share one cached result.
    first = backend._list_issues()
    second = backend._list_issues()
    assert first == second == [good_issue]
    assert calls["n"] == 1

    # Once the repo starts failing (e.g. rate-limited), a call within the
    # error backoff window reuses the last known-good list instead of
    # re-hitting `gh` and re-raising on every poll.
    def failing_run(args, *, check=True):
        calls["n"] += 1
        raise github_backend.GitHubBackendError("API rate limit already exceeded")

    monkeypatch.setattr(backend, "_run", failing_run)
    import time as _time
    _time.sleep(0.06)  # expire the TTL so the next call actually attempts gh
    third = backend._list_issues()
    assert third == [good_issue]  # stale-but-good data, served silently
    assert calls["n"] == 2  # exactly one real attempt, not one per call

    fourth = backend._list_issues()  # still within the error backoff window
    assert fourth == [good_issue]
    assert calls["n"] == 2  # no new `gh` invocation while backed off

    # A cold backend with no prior good data still surfaces the error --
    # there's nothing safe to fall back to.
    cold = github_backend.GitHubIssuesBackend("T", repo="acme/cache-test-cold")
    monkeypatch.setattr(cold, "_run", failing_run)
    _no_etag_probe(monkeypatch, cold)
    with pytest.raises(github_backend.GitHubBackendError):
        cold._list_issues()


def test_list_issues_fresh_request_still_honors_error_backoff(monkeypatch):
    """Reconciler freshness must not retry a known GitHub failure per call."""
    import watchtower.github_backend as github_backend

    monkeypatch.setattr(github_backend, "_LIST_ERROR_BACKOFF", 60.0)
    github_backend._LIST_CACHE.clear()

    backend = github_backend.GitHubIssuesBackend(
        "T", repo="acme/fresh-error-backoff-test"
    )
    calls = {"n": 0}

    def failing_run(args, *, check=True):
        calls["n"] += 1
        raise github_backend.GitHubBackendError("gh auth unavailable")

    monkeypatch.setattr(backend, "_run", failing_run)

    with pytest.raises(github_backend.GitHubBackendError):
        backend._list_issues(fresh=True)
    with pytest.raises(github_backend.GitHubBackendError):
        backend._list_issues(fresh=True)

    assert calls["n"] == 1


def test_list_issues_strict_never_uses_cached_or_stale_data(monkeypatch):
    """Destructive callers need authoritative state, not dashboard fallback."""
    import watchtower.github_backend as github_backend

    github_backend._LIST_CACHE.clear()
    backend = github_backend.GitHubIssuesBackend(
        "T", repo="acme/strict-list-test"
    )
    calls = {"n": 0}
    issue = {
        "number": 1, "title": "t", "body": "", "state": "OPEN",
        "url": "https://github.com/acme/strict-list-test/issues/1",
        "assignees": [], "labels": [], "createdAt": "2026-07-01T00:00:00Z",
        "updatedAt": "2026-07-01T00:00:00Z", "closedAt": None,
    }

    def succeed(args, *, check=True):
        calls["n"] += 1
        return json.dumps([issue])

    monkeypatch.setattr(backend, "_run", succeed)
    _no_etag_probe(monkeypatch, backend)
    assert backend._list_issues() == [issue]

    def fail(args, *, check=True):
        calls["n"] += 1
        raise github_backend.GitHubBackendError("authoritative read failed")

    monkeypatch.setattr(backend, "_run", fail)
    with pytest.raises(github_backend.GitHubBackendError):
        backend._list_issues(fresh=True, strict=True)
    with pytest.raises(github_backend.GitHubBackendError):
        backend._list_issues(fresh=True, strict=True)

    assert calls["n"] == 2


def test_list_issues_soft_read_uses_persisted_cache_without_calling_gh(
    tmp_path, monkeypatch
):
    """The reconciler-latency fix: a cold process (no in-memory _LIST_CACHE
    entry -- exactly what every fresh `wt run`/`wt claim`/dispatch CLI
    invocation is) must serve a fresh persisted-cache entry instead of
    shelling out to `gh` itself. Only the background poller pays that cost."""
    import watchtower.github_backend as github_backend

    monkeypatch.setenv(
        "WATCHTOWER_GH_LIST_CACHE_FILE", str(tmp_path / "gh-list-cache.json")
    )
    github_backend._LIST_CACHE.clear()

    repo = "acme/persisted-cache-test"
    issue = {
        "number": 1, "title": "t", "body": "", "state": "OPEN",
        "url": f"https://github.com/{repo}/issues/1",
        "assignees": [], "labels": [], "createdAt": "2026-07-01T00:00:00Z",
        "updatedAt": "2026-07-01T00:00:00Z", "closedAt": None,
    }
    github_backend._write_persisted_list_entry(
        f"{repo}:open", {"at": time.time(), "data": [issue]}
    )

    backend = github_backend.GitHubIssuesBackend("T", repo=repo)
    calls = {"n": 0}

    def unexpected_run(args, *, check=True):
        calls["n"] += 1
        raise AssertionError("must not shell out to gh for a soft read")

    monkeypatch.setattr(backend, "_run", unexpected_run)
    monkeypatch.setattr(backend, "_run_raw", unexpected_run)

    assert backend._list_issues() == [issue]
    assert calls["n"] == 0
    # And it warmed the in-process cache too, so a second call in the same
    # process doesn't even touch the persisted file again.
    assert github_backend._LIST_CACHE[f"{repo}:open"]["data"] == [issue]


def test_list_issues_fresh_read_uses_persisted_cache_during_github_backoff(
    tmp_path, monkeypatch
):
    """A status read remains useful while GitHub's retry backoff is active."""
    import watchtower.github_backend as github_backend

    monkeypatch.setenv(
        "WATCHTOWER_GH_LIST_CACHE_FILE", str(tmp_path / "gh-list-cache.json")
    )
    monkeypatch.setenv(
        "WATCHTOWER_GH_CONNECTIVITY_FILE", str(tmp_path / "gh-connectivity.json")
    )
    github_backend._LIST_CACHE.clear()

    repo = "acme/fresh-backoff-persisted-cache-test"
    issue = {
        "number": 1, "title": "cached", "body": "", "state": "OPEN",
        "url": f"https://github.com/{repo}/issues/1",
        "assignees": [], "labels": [], "createdAt": "2026-07-01T00:00:00Z",
        "updatedAt": "2026-07-01T00:00:00Z", "closedAt": None,
    }
    github_backend._write_persisted_list_entry(
        f"{repo}:open",
        {
            "at": time.time() - github_backend._PERSISTED_LIST_STALE_S - 1,
            "data": [issue],
        },
    )
    github_backend._record_gh_failure("API rate limit already exceeded")

    backend = github_backend.GitHubIssuesBackend("T", repo=repo)
    calls = {"n": 0}

    def unexpected_run(args, *, check=True):
        calls["n"] += 1
        raise AssertionError("must not shell out to gh during active backoff")

    monkeypatch.setattr(backend, "_run", unexpected_run)
    monkeypatch.setattr(backend, "_run_raw", unexpected_run)

    assert backend._list_issues(fresh=True) == [issue]
    assert calls["n"] == 0


def test_list_issues_ignores_stale_persisted_cache(tmp_path, monkeypatch):
    """If the poller stopped (daemon down/crashed) a soft reader must not
    serve indefinitely stale data -- it self-heals by falling back to its
    own live fetch, same as before this cache existed."""
    import watchtower.github_backend as github_backend

    monkeypatch.setenv(
        "WATCHTOWER_GH_LIST_CACHE_FILE", str(tmp_path / "gh-list-cache.json")
    )
    github_backend._LIST_CACHE.clear()

    repo = "acme/stale-persisted-cache-test"
    stale_issue = {
        "number": 1, "title": "stale", "body": "", "state": "OPEN",
        "url": f"https://github.com/{repo}/issues/1",
        "assignees": [], "labels": [], "createdAt": "2026-07-01T00:00:00Z",
        "updatedAt": "2026-07-01T00:00:00Z", "closedAt": None,
    }
    stale_at = time.time() - (github_backend._PERSISTED_LIST_STALE_S + 1)
    github_backend._write_persisted_list_entry(
        f"{repo}:open", {"at": stale_at, "data": [stale_issue]}
    )

    backend = github_backend.GitHubIssuesBackend("T", repo=repo)
    fresh_issue = {**stale_issue, "number": 2, "title": "fresh"}
    calls = {"n": 0}

    def fake_run(args, *, check=True):
        calls["n"] += 1
        return json.dumps([fresh_issue])

    monkeypatch.setattr(backend, "_run", fake_run)
    _no_etag_probe(monkeypatch, backend)

    assert backend._list_issues() == [fresh_issue]
    assert calls["n"] == 1


def test_list_issues_strict_ignores_persisted_cache(tmp_path, monkeypatch):
    """A claim/close about to write needs a live answer even when a fresh
    persisted entry exists -- persisted-cache freshness is not the same
    guarantee as ``strict``'s "pay for certainty" contract."""
    import watchtower.github_backend as github_backend

    monkeypatch.setenv(
        "WATCHTOWER_GH_LIST_CACHE_FILE", str(tmp_path / "gh-list-cache.json")
    )
    github_backend._LIST_CACHE.clear()

    repo = "acme/strict-persisted-cache-test"
    cached_issue = {
        "number": 1, "title": "cached", "body": "", "state": "OPEN",
        "url": f"https://github.com/{repo}/issues/1",
        "assignees": [], "labels": [], "createdAt": "2026-07-01T00:00:00Z",
        "updatedAt": "2026-07-01T00:00:00Z", "closedAt": None,
    }
    github_backend._write_persisted_list_entry(
        f"{repo}:open", {"at": time.time(), "data": [cached_issue]}
    )

    backend = github_backend.GitHubIssuesBackend("T", repo=repo)
    live_issue = {**cached_issue, "number": 2, "title": "live"}
    calls = {"n": 0}

    def fake_run(args, *, check=True):
        calls["n"] += 1
        return json.dumps([live_issue])

    monkeypatch.setattr(backend, "_run", fake_run)

    assert backend._list_issues(fresh=True, strict=True) == [live_issue]
    assert calls["n"] == 1


def test_refresh_persisted_list_cache_writes_file_from_live_fetch(
    tmp_path, monkeypatch
):
    """``refresh_persisted_list_cache`` (the background poller's only job) is
    the sole function allowed to pay for a live `gh` call on a soft reader's
    behalf, and its result must be readable back for both open and closed."""
    import watchtower.github_backend as github_backend

    monkeypatch.setenv(
        "WATCHTOWER_GH_LIST_CACHE_FILE", str(tmp_path / "gh-list-cache.json")
    )
    github_backend._LIST_CACHE.clear()

    repo = "acme/poller-refresh-test"
    open_issue = {
        "number": 1, "title": "open one", "body": "", "state": "OPEN",
        "url": f"https://github.com/{repo}/issues/1",
        "assignees": [], "labels": [], "createdAt": "2026-07-01T00:00:00Z",
        "updatedAt": "2026-07-01T00:00:00Z", "closedAt": None,
    }
    closed_issue = {
        "number": 2, "title": "closed one", "body": "", "state": "CLOSED",
        "url": f"https://github.com/{repo}/issues/2",
        "assignees": [], "labels": [], "createdAt": "2026-07-01T00:00:00Z",
        "updatedAt": "2026-07-01T00:00:00Z",
        "closedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    def fake_run(self, args, *, check=True):
        state = args[args.index("--state") + 1]
        return json.dumps([open_issue] if state == "open" else [closed_issue])

    def fake_run_raw(self, args, **kwargs):
        # Neutralise the ETag probe (`gh api ...`) the same way _no_etag_probe
        # does for a bound backend -- here it must work for a fresh instance
        # `refresh_persisted_list_cache` constructs internally.
        raise github_backend.GitHubBackendError("no probe in this fake")

    monkeypatch.setattr(github_backend.GitHubIssuesBackend, "_run", fake_run)
    monkeypatch.setattr(github_backend.GitHubIssuesBackend, "_run_raw", fake_run_raw)

    github_backend.refresh_persisted_list_cache(repo)

    persisted = github_backend._read_persisted_list_cache()
    assert persisted[f"{repo}:open"]["data"] == [open_issue]
    assert persisted[f"{repo}:closed"]["data"] == [closed_issue]

    # And a subsequent cold soft read serves it without another gh call.
    github_backend._LIST_CACHE.clear()
    backend = github_backend.GitHubIssuesBackend("T", repo=repo)

    def unexpected_run(args, *, check=True):
        raise AssertionError("must not shell out to gh for a soft read")

    monkeypatch.setattr(backend, "_run", unexpected_run)
    assert backend._list_issues() == [open_issue]


def test_poller_refetches_after_another_process_invalidates_snapshot(
    tmp_path, monkeypatch
):
    """A completed write must not let the poller republish its old snapshot.

    ``wt close`` runs in a short-lived worker process, so it invalidates the
    persisted cache while the daemon poller keeps its pre-close result in
    memory.  A changed ETag normally observes the fetch cap, but an absent
    persisted entry is an explicit invalidation and must win over that cap.
    """
    import watchtower.github_backend as github_backend

    monkeypatch.setenv(
        "WATCHTOWER_GH_LIST_CACHE_FILE", str(tmp_path / "gh-list-cache.json")
    )
    monkeypatch.setattr(github_backend, "_LIST_FETCH_MIN_INTERVAL_S", 60.0)
    github_backend._LIST_CACHE.clear()

    repo = "acme/invalidate-poller-test"
    stale = {
        "number": 1, "title": "still open", "body": "", "state": "OPEN",
        "url": f"https://github.com/{repo}/issues/1",
        "assignees": [], "labels": [], "createdAt": "2026-07-01T00:00:00Z",
        "updatedAt": "2026-07-01T00:00:00Z", "closedAt": None,
    }
    live = {**stale, "title": "closed elsewhere", "state": "CLOSED",
            "closedAt": "2026-07-01T00:01:00Z"}
    now = time.time()
    for state in ("open", "closed"):
        github_backend._LIST_CACHE[f"{repo}:{state}"] = {
            "at": now, "data": [stale], "error": None, "etag": '"old"',
            "fetched_at": now,
        }

    calls = []

    def fake_run(self, args, *, check=True):
        state = args[args.index("--state") + 1]
        calls.append(state)
        return json.dumps([] if state == "open" else [live])

    monkeypatch.setattr(github_backend.GitHubIssuesBackend, "_run", fake_run)
    monkeypatch.setattr(
        github_backend.GitHubIssuesBackend, "_run_raw", lambda self, args: _ok(etag="new")
    )

    # Simulate the worker process removing the shared snapshot after its close.
    github_backend._rewrite_persisted_list_cache(lambda data: False)
    github_backend.refresh_persisted_list_cache(repo)

    persisted = github_backend._read_persisted_list_cache()
    assert calls == ["open", "closed"]
    assert persisted[f"{repo}:open"]["data"] == []
    assert persisted[f"{repo}:closed"]["data"] == [live]


def test_local_write_invalidates_list_cache_for_read_your_own_writes(
    tmp_path, monkeypatch
):
    """Regression guard for making count_claimable/count_manual_eligible
    fresh=False (WT reconciler-latency fix): a mutation this process just
    made (mark_runnable here) must be visible to the very next soft read in
    this same process, even though that read now prefers the cache over a
    live `gh` call. Caught for real by
    test_github_drain_off_queue_still_staffs_a_requested_run when the
    invalidation hook didn't exist yet -- this pins the mechanism directly."""
    import watchtower.github_backend as github_backend

    monkeypatch.setenv(
        "WATCHTOWER_GH_LIST_CACHE_FILE", str(tmp_path / "gh-list-cache.json")
    )
    github_backend._LIST_CACHE.clear()

    repo = "acme/read-your-write-test"
    backend = github_backend.GitHubIssuesBackend("T", repo=repo)
    open_issue = {
        "number": 1, "title": "t", "body": "", "state": "OPEN",
        "url": f"https://github.com/{repo}/issues/1",
        "assignees": [], "labels": [], "createdAt": "2026-07-01T00:00:00Z",
        "updatedAt": "2026-07-01T00:00:00Z", "closedAt": None,
    }
    edited_issue = {**open_issue, "labels": [{"name": "watchtower:play"}]}
    responses = iter([
        "",                                                # the `gh issue edit`
        'HTTP/2.0 200 OK\r\nEtag: "post-write"\r\n\r\n[]',  # validator harvest
        json.dumps([edited_issue]),                        # the forced live list
    ])

    def fake_run_raw(args, **kwargs):
        raw = next(responses)
        return SimpleNamespace(returncode=0, stdout=raw, stderr="")

    monkeypatch.setattr(backend, "_run_raw", fake_run_raw)

    # 1) Seed both caches with the pre-write snapshot -- the in-memory one as
    # a live soft read would, the persisted one as the background poller
    # would (refresh_persisted_list_cache is exercised separately above).
    github_backend._LIST_CACHE[f"{repo}:open"] = {
        "at": time.time(), "data": [open_issue], "error": None, "etag": "",
    }
    github_backend._write_persisted_list_entry(
        f"{repo}:open", {"at": time.time(), "data": [open_issue]}
    )

    # 2) A local write (any `gh issue edit/create/close/reopen/comment`)
    # must mark both caches stale for this repo. The entries survive -- their
    # validator and fetch clock are still worth inheriting -- but a stale entry
    # is not allowed to answer a read.
    backend._run(["issue", "edit", "1", *backend._repo_args(), "--add-label", "x"])
    assert github_backend._LIST_CACHE[f"{repo}:open"]["stale"] is True
    persisted = github_backend._read_persisted_list_cache()[f"{repo}:open"]
    assert persisted["stale"] is True

    # 3) ...so the very next soft read is forced to see the post-write state,
    # not the pre-write snapshot it would otherwise have kept serving.
    assert backend._list_issues() == [edited_issue]

    # 4) And that forced read publishes what it learned: the shared entry goes
    # straight from stale to current, carrying a validator, so the poller does
    # not spend a second `gh issue list` rediscovering this process's write.
    entry = github_backend._read_persisted_list_cache()[f"{repo}:open"]
    assert entry["stale"] is False
    assert entry["data"] == [edited_issue]
    assert entry["etag"] == '"post-write"'


def test_poll_list_caches_once_refreshes_every_configured_github_queue(
    tmp_path, monkeypatch
):
    """The daemon-thread entrypoint discovers github-backed queues from
    config (deduping by repo) and refreshes each one -- this is what makes
    the persisted cache self-sustaining without any per-request trigger."""
    config, q = _reload_isolated(tmp_path, monkeypatch)
    import watchtower.github_backend as github_backend

    config.set_backend("GHQ1", "github")
    config.set_github_repo("GHQ1", "acme/poll-once-test")
    config.set_backend("GHQ2", "github")
    config.set_github_repo("GHQ2", "acme/poll-once-test")  # same repo, deduped
    config.set_backend("FILEQ", "file")

    refreshed = []
    monkeypatch.setattr(
        github_backend, "refresh_persisted_list_cache", refreshed.append
    )

    github_backend.poll_list_caches_once()

    assert refreshed == ["acme/poll-once-test"]


def test_cached_github_list_failure_is_logged_only_once(tmp_path, monkeypatch):
    """Repeated callers must not flood activity.log with one cached failure."""
    config, q = _reload_isolated(tmp_path, monkeypatch)
    import watchtower.github_backend as github_backend

    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")

    def failing_run(self, args, *, check=True):
        raise github_backend.GitHubBackendError("gh auth unavailable")

    monkeypatch.setattr(github_backend.GitHubIssuesBackend, "_run", failing_run)

    assert q.list_items() == []
    assert q.list_items() == []

    activity = (tmp_path / "activity.log").read_text()
    assert activity.count("GitHub list failed: gh auth unavailable") == 1


# ============================================================= ETag freshness

_PROBE_ISSUE = {
    "number": 1, "title": "t", "body": "", "state": "OPEN",
    "url": "https://github.com/acme/etag-test/issues/1",
    "assignees": [], "labels": [], "createdAt": "2026-07-01T00:00:00Z",
    "updatedAt": "2026-07-01T00:00:00Z", "closedAt": None,
}


def _proc(returncode, stdout="", stderr=""):
    import subprocess

    return subprocess.CompletedProcess(
        args=["gh"], returncode=returncode, stdout=stdout, stderr=stderr
    )


def _not_modified():
    """Exactly what `gh api -i` does on a 304: exit 1, `gh: HTTP 304` on
    stderr, status line on stdout. Verified against gh 2.96.0."""
    return _proc(
        1,
        stdout='HTTP/2.0 304 Not Modified\r\nEtag: "v1"\r\n\r\n',
        stderr="gh: HTTP 304\n",
    )


def _ok(etag="v1", body="[]"):
    return _proc(
        0,
        stdout=f'HTTP/2.0 200 OK\r\nEtag: "{etag}"\r\n'
               f"Content-Type: application/json\r\n\r\n{body}",
    )


def _etag_backend(monkeypatch, repo="acme/etag-test"):
    """A backend that always revalidates, with both `gh` seams instrumented.

    Returns ``(backend, counts, probes)``: ``counts['fetch']`` is the rich
    `gh issue list` calls, ``probes`` the argv of each conditional GET.
    """
    import watchtower.github_backend as github_backend

    monkeypatch.setattr(github_backend, "_LIST_CACHE_TTL", 0.0)
    # These tests exercise probe semantics (304 vs 200 vs unusable), where
    # every revalidating read must be allowed to fetch. The heavy-fetch rate
    # cap is neutralised here and covered by its own test.
    monkeypatch.setattr(github_backend, "_LIST_FETCH_MIN_INTERVAL_S", 0.0)
    github_backend._LIST_CACHE.clear()
    backend = github_backend.GitHubIssuesBackend("T", repo=repo)
    counts = {"fetch": 0}
    probes = []

    def fake_run(args, *, check=True):
        counts["fetch"] += 1
        return json.dumps([_PROBE_ISSUE])

    monkeypatch.setattr(backend, "_run", fake_run)
    return backend, counts, probes


def test_etag_304_reads_as_unchanged_and_never_as_an_error(monkeypatch):
    """The landmine: `gh api` exits 1 on a 304. Decoded as a failure it would
    trip _LIST_ERROR_BACKOFF and freeze the queue on stale data for 60s at a
    time -- on the poll that is supposed to be the cheap common case."""
    import watchtower.github_backend as github_backend

    backend, counts, probes = _etag_backend(monkeypatch)

    def fake_run_raw(args):
        probes.append(list(args))
        return _ok() if "-H" not in args else _not_modified()

    monkeypatch.setattr(backend, "_run_raw", fake_run_raw)

    # Cold: nothing cached to validate, so the probe runs unconditionally --
    # its whole job here is to bring back a validator to pair with the fetch.
    assert backend._list_issues() == [_PROBE_ISSUE]
    assert (counts["fetch"], len(probes)) == (1, 1)
    assert not any(a == "-H" for a in probes[0])

    # Which is what makes the very next poll a 304 rather than a second
    # unconditional 200 followed by a redundant re-list.
    for _ in range(3):
        assert backend._list_issues() == [_PROBE_ISSUE]
    assert counts["fetch"] == 1, "a 304 must not trigger the expensive fetch"
    assert len(probes) == 4
    assert 'If-None-Match: "v1"' in probes[-1]

    # And crucially it is not remembered as a failure: no error cached, so
    # nothing is backing off and the next real change is picked up at once.
    cached = github_backend._LIST_CACHE["acme/etag-test:open"]
    assert cached["error"] is None
    assert cached["etag"] == '"v1"'


def test_etag_200_refetches_and_stores_the_new_validator(monkeypatch):
    """A changed repo goes back through the rich fetch -- the probe is a
    detector, not a fetcher (its payload has comment counts, not bodies)."""
    import watchtower.github_backend as github_backend

    backend, counts, probes = _etag_backend(monkeypatch)
    versions = iter(["v1", "v2", "v3"])

    def fake_run_raw(args):
        probes.append(list(args))
        return _ok(etag=next(versions))

    monkeypatch.setattr(backend, "_run_raw", fake_run_raw)

    backend._list_issues()                       # cold fetch, harvests "v1"
    assert github_backend._LIST_CACHE["acme/etag-test:open"]["etag"] == '"v1"'
    backend._list_issues()                       # 200 -> fetch, stores "v2"
    assert github_backend._LIST_CACHE["acme/etag-test:open"]["etag"] == '"v2"'
    backend._list_issues()                       # 200 again -> fetch, "v3"
    assert github_backend._LIST_CACHE["acme/etag-test:open"]["etag"] == '"v3"'
    assert counts["fetch"] == 3
    assert 'If-None-Match: "v2"' in probes[-1]


def test_changed_probe_fetch_is_rate_capped_on_a_busy_repo(monkeypatch):
    """A busy repo makes the probe say 200 on EVERY sweep; the rich fetch may
    still run at most once per _LIST_FETCH_MIN_INTERVAL_S. The cap keys off
    fetched_at (last real fetch), never `at` -- a 304 refreshes `at`, so
    capping on it would suppress fetches forever on an actively-polled repo.
    """
    import watchtower.github_backend as github_backend

    monkeypatch.setattr(github_backend, "_LIST_CACHE_TTL", 0.0)
    monkeypatch.setattr(github_backend, "_LIST_FETCH_MIN_INTERVAL_S", 60.0)
    github_backend._LIST_CACHE.clear()
    backend = github_backend.GitHubIssuesBackend("T", repo="acme/cap-test")
    counts = {"fetch": 0}

    def fake_run(args, *, check=True):
        counts["fetch"] += 1
        return json.dumps([_PROBE_ISSUE])

    monkeypatch.setattr(backend, "_run", fake_run)
    monkeypatch.setattr(backend, "_run_raw", lambda args: _ok(etag="v1"))

    backend._list_issues()   # cold fetch
    backend._list_issues()   # probe 200, but last fetch is seconds old -> capped
    backend._list_issues()   # still capped
    assert counts["fetch"] == 1

    entry = github_backend._LIST_CACHE["acme/cap-test:open"]
    entry["fetched_at"] -= 61.0  # last real fetch now older than the cap
    backend._list_issues()       # changed AND past the cap -> fetch
    assert counts["fetch"] == 2


def test_invalidation_keeps_the_validator_and_still_forces_a_live_read(
    tmp_path, monkeypatch
):
    """A local write must not cost the entry its ETag and fetch clock.

    Deletion used to take both, so the replacement fetch stored ``etag=""``,
    the next probe went unconditional, and its validator was discarded by the
    cap (it cannot be paired with the older list already in hand). An entry
    could only earn a real ETag by surviving a whole cap interval untouched --
    which on a repo whose workers write every few seconds never happens.
    """
    import watchtower.github_backend as github_backend

    monkeypatch.setenv(
        "WATCHTOWER_GH_LIST_CACHE_FILE", str(tmp_path / "gh-list-cache.json")
    )
    monkeypatch.setattr(github_backend, "_LIST_CACHE_TTL", 0.0)
    monkeypatch.setattr(github_backend, "_LIST_FETCH_MIN_INTERVAL_S", 60.0)
    github_backend._LIST_CACHE.clear()
    repo = "acme/stale-marking-test"
    backend = github_backend.GitHubIssuesBackend("T", repo=repo)
    counts = {"fetch": 0}

    def fake_run(args, *, check=True):
        counts["fetch"] += 1
        return json.dumps([_PROBE_ISSUE])

    monkeypatch.setattr(backend, "_run", fake_run)
    monkeypatch.setattr(backend, "_run_raw", lambda args: _ok(etag="v1"))

    backend._list_issues()          # cold: harvests a validator, then fetches
    entry = github_backend._LIST_CACHE[f"{repo}:open"]
    assert (counts["fetch"], entry["etag"]) == (1, '"v1"')
    fetched_at = entry["fetched_at"]

    backend._list_issues()          # probe says changed, but the cap holds
    assert counts["fetch"] == 1

    github_backend._invalidate_list_cache(repo)
    entry = github_backend._LIST_CACHE[f"{repo}:open"]
    assert entry["stale"] is True
    assert entry["etag"] == '"v1"'        # the validator survives the write
    assert entry["fetched_at"] == fetched_at   # and so does the fetch clock

    # Stale still means what deletion meant: the cap may not stand in for the
    # live read that read-your-own-writes requires.
    backend._list_issues()
    assert counts["fetch"] == 2
    assert github_backend._LIST_CACHE[f"{repo}:open"]["stale"] is False


def test_poller_adopts_a_published_entry_instead_of_refetching_it(
    tmp_path, monkeypatch
):
    """The mutating process publishes the post-write list, so the poller must
    not spend a second `gh issue list` rediscovering it.

    That double fetch -- one by the worker that wrote, one by the poller that
    saw the entry vanish -- was what every claim and close cost the fleet.
    """
    import watchtower.github_backend as github_backend

    monkeypatch.setenv(
        "WATCHTOWER_GH_LIST_CACHE_FILE", str(tmp_path / "gh-list-cache.json")
    )
    github_backend._LIST_CACHE.clear()
    repo = "acme/publish-test"
    published = {**_PROBE_ISSUE, "title": "written by the worker"}
    now = time.time()

    # The poller holds a snapshot from before the write...
    github_backend._LIST_CACHE[f"{repo}:open"] = {
        "at": now - 10, "data": [_PROBE_ISSUE], "error": None,
        "etag": '"old"', "fetched_at": now - 10, "stale": False,
    }
    # ...while the process that wrote has already published the result.
    github_backend._write_persisted_list_entry(
        f"{repo}:open",
        {"at": now, "data": [published], "etag": '"v9"',
         "fetched_at": now, "stale": False},
    )

    counts = {"fetch": 0}

    def fake_run(self, args, *, check=True):
        counts["fetch"] += 1
        return json.dumps([_PROBE_ISSUE])

    def fake_run_raw(self, args, **kwargs):
        return _not_modified() if "-H" in args else _ok(etag="v9")

    monkeypatch.setattr(github_backend.GitHubIssuesBackend, "_run", fake_run)
    monkeypatch.setattr(github_backend.GitHubIssuesBackend, "_run_raw", fake_run_raw)

    github_backend.refresh_persisted_list_cache(repo)

    # Only the closed state -- which nobody published -- cost a fetch. The open
    # state adopted the published entry and revalidated it with a free 304.
    assert counts["fetch"] == 1
    entry = github_backend._read_persisted_list_cache()[f"{repo}:open"]
    assert entry["data"] == [published], "the poller must not overwrite it"
    assert entry["etag"] == '"v9"'


def test_unusable_etag_probe_falls_through_to_the_unconditional_fetch(monkeypatch):
    """Worst case must equal the behaviour we had before ETags: an unhelpful
    probe (5xx, network blip, an old gh that can't do -i) costs one wasted
    call and nothing else."""
    backend, counts, probes = _etag_backend(monkeypatch)

    def broken_probe(args):
        probes.append(list(args))
        return _proc(1, stdout="", stderr="gh: HTTP 502 Bad Gateway\n")

    monkeypatch.setattr(backend, "_run_raw", broken_probe)

    for _ in range(3):
        assert backend._list_issues() == [_PROBE_ISSUE]
    assert counts["fetch"] == 3  # every poll still gets real data
    # One probe per read and no more: the cold read's harvest and the warm
    # reads' revalidation are the same single wasted call, never doubled up.
    assert len(probes) == 3


def test_genuine_list_failure_still_backs_off_with_the_probe_in_play(monkeypatch):
    """The 304 handling must not soften the error path: a repo that really is
    failing (rate limit, auth) is still retried at most once per backoff."""
    import watchtower.github_backend as github_backend

    backend, counts, probes = _etag_backend(monkeypatch)
    monkeypatch.setattr(github_backend, "_LIST_ERROR_BACKOFF", 60.0)

    def changed_probe(args):
        probes.append(list(args))
        return _ok(etag="v1")

    monkeypatch.setattr(backend, "_run_raw", changed_probe)
    assert backend._list_issues() == [_PROBE_ISSUE]  # one good list to fall back to

    def failing_run(args, *, check=True):
        counts["fetch"] += 1
        raise github_backend.GitHubBackendError("API rate limit already exceeded")

    monkeypatch.setattr(backend, "_run", failing_run)
    assert backend._list_issues() == [_PROBE_ISSUE]  # stale-but-good, served silently
    assert (counts["fetch"], len(probes)) == (2, 2)

    # Inside the backoff window nothing hits GitHub at all -- not even the
    # cheap probe, because there is nothing worth revalidating until the
    # repo is healthy again.
    assert backend._list_issues() == [_PROBE_ISSUE]
    assert (counts["fetch"], len(probes)) == (2, 2)
    assert github_backend._LIST_CACHE["acme/etag-test:open"]["error"] is not None


def test_low_graphql_quota_skips_expensive_fetch_and_serves_cache(monkeypatch):
    """When GitHub GraphQL quota is low, `_list_issues` must not pay for a
    rich `gh issue list` fetch even if the ETag probe says the repo changed.
    It should return cached data instead."""
    import watchtower.github_backend as github_backend

    backend, counts, probes = _etag_backend(monkeypatch)

    def changed_probe(args):
        probes.append(list(args))
        return _ok(etag="v1")

    monkeypatch.setattr(backend, "_run_raw", changed_probe)
    monkeypatch.setattr(github_backend, "_graphql_rate_limit_remaining", lambda: 50)

    assert backend._list_issues() == [_PROBE_ISSUE]  # cold fetch seeds cache
    assert counts["fetch"] == 1
    assert len(probes) == 1  # cold fetch probes once, to harvest a validator

    # Quota is now low. The probe would say 200 (changed), but the expensive
    # fetch must be skipped and the cached issue returned.
    assert backend._list_issues() == [_PROBE_ISSUE]
    assert counts["fetch"] == 1, "low quota must skip the expensive gh issue list fetch"
    assert len(probes) == 1, "low quota should also skip the ETag probe"


def test_a_new_issue_is_visible_to_the_next_revalidating_read(tmp_path, monkeypatch):
    """End to end over the fake gh, which reproduces the 304 exit-1 landmine:
    an unchanged repo is answered from cache, a new issue is not."""
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    import watchtower.github_backend as github_backend

    # This test compresses a real change into seconds after the last fetch;
    # the fetch-rate cap (covered separately) would legitimately defer it.
    # Set after _reload_isolated, which reloads this module and would wipe it.
    monkeypatch.setattr(github_backend, "_LIST_FETCH_MIN_INTERVAL_S", 0.0)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _write_fake_issues(state, [_fake_issue(1, "first")])

    def refs():
        return [it["ref"] for it in q.list_items(project="GHI", fresh=True)]

    assert refs() == ["GHI-1"]          # cold
    assert refs() == ["GHI-1"]          # bootstraps the validator
    _write_fake_issues(state, [_fake_issue(1, "first")])  # same state, cmds reset
    assert refs() == ["GHI-1"]          # answered by a 304

    commands = [" ".join(c) for c in json.loads(state.read_text())["commands"]]
    assert any("If-None-Match" in c for c in commands)
    assert not [c for c in commands if c.startswith("issue list")], (
        "a 304 must not be followed by `gh issue list`"
    )

    _write_fake_issues(state, [_fake_issue(1, "first"), _fake_issue(2, "second")])
    assert refs() == ["GHI-1", "GHI-2"]


# ============================================================ eligibility model

def _eligibility_issue(number: int, *, age_s: float, labels=None):
    """An open issue ``age_s`` seconds old carrying ``labels``."""
    created = datetime.now(timezone.utc) - timedelta(seconds=age_s)
    issue = _fake_issue(number, f"issue {number}", labels=labels or [])
    issue["createdAt"] = created.strftime("%Y-%m-%dT%H:%M:%SZ")
    issue["updatedAt"] = issue["createdAt"]
    return issue


def _eligibility_backend(*, auto_drain: bool, grace_s: int = 180, repo="owner/elig"):
    import watchtower.github_backend as github_backend

    return github_backend.GitHubIssuesBackend(
        "ELIG",
        repo=repo,
        auto_drain=auto_drain,
        grace_s=grace_s,
        partition_by_label=False,
    )


# drain on/off x no-auto-drain label present/absent x inside/outside the grace
# period x play/not. auto_eligible is the AND of the first three; play alone
# decides manual_eligible and can carry work_it on its own.
ELIGIBILITY_TRUTH_TABLE = [
    # (auto_drain, no_auto_drain, inside_grace, play, auto_eligible, work_it)
    (False, False, False, False, False, False),
    (False, False, False, True,  False, True),
    (False, False, True,  False, False, False),
    (False, False, True,  True,  False, True),
    (False, True,  False, False, False, False),
    (False, True,  False, True,  False, True),
    (False, True,  True,  False, False, False),
    (False, True,  True,  True,  False, True),
    (True,  False, False, False, True,  True),
    (True,  False, False, True,  True,  True),
    (True,  False, True,  False, False, False),
    (True,  False, True,  True,  False, True),
    (True,  True,  False, False, False, False),
    (True,  True,  False, True,  False, True),
    (True,  True,  True,  False, False, False),
    (True,  True,  True,  True,  False, True),
]


@pytest.mark.parametrize(
    "auto_drain,no_auto_drain,inside_grace,play,auto_eligible,work_it",
    ELIGIBILITY_TRUTH_TABLE,
)
def test_eligibility_truth_table(
    auto_drain, no_auto_drain, inside_grace, play, auto_eligible, work_it,
):
    labels = []
    if no_auto_drain:
        labels.append("watchtower:no-auto-drain")
    if play:
        labels.append("watchtower:play")
    backend = _eligibility_backend(auto_drain=auto_drain, grace_s=180)
    issue = _eligibility_issue(1, age_s=5 if inside_grace else 4000, labels=labels)

    item = backend._issue_to_item(issue)

    assert item["no_auto_drain"] is no_auto_drain
    assert item["run_requested"] is play
    assert item["auto_eligible"] is auto_eligible
    assert item["manual_eligible"] is play
    assert item["work_it"] is work_it
    assert item["claimable"] is work_it


def test_grace_period_of_zero_makes_a_brand_new_ticket_auto_eligible():
    backend = _eligibility_backend(auto_drain=True, grace_s=0)
    assert backend._issue_to_item(_eligibility_issue(1, age_s=0))["auto_eligible"] is True


def test_closed_tickets_are_never_eligible_even_with_play():
    backend = _eligibility_backend(auto_drain=True, grace_s=0)
    issue = _eligibility_issue(1, age_s=4000, labels=["watchtower:play"])
    issue["state"] = "CLOSED"
    issue["closedAt"] = _eligibility_issue(1, age_s=1)["createdAt"]

    item = backend._issue_to_item(issue)

    assert item["status"] == "closed"
    assert (item["auto_eligible"], item["manual_eligible"], item["work_it"]) == (
        False, False, False,
    )


@pytest.mark.parametrize("auto_drain", [True, False])
@pytest.mark.parametrize("grace_s", [0, 180])
def test_auto_eligible_set_is_always_a_subset_of_work_it(monkeypatch, auto_drain, grace_s):
    """The invariant the two filters must never drift out of: everything the
    reconciler counts as spawn-worthy is something a worker would claim."""
    import watchtower.github_backend as github_backend

    github_backend._LIST_CACHE.clear()
    repo = f"owner/subset-{int(auto_drain)}-{grace_s}"
    backend = _eligibility_backend(auto_drain=auto_drain, grace_s=grace_s, repo=repo)
    issues = []
    number = 0
    for no_auto_drain in (False, True):
        for age_s in (1, 4000):
            for play in (False, True):
                number += 1
                labels = []
                if no_auto_drain:
                    labels.append("watchtower:no-auto-drain")
                if play:
                    labels.append("watchtower:play")
                issues.append(_eligibility_issue(number, age_s=age_s, labels=labels))
    monkeypatch.setattr(backend, "_run", lambda args, *, check=True: json.dumps(issues))

    items = backend.list_items(status="open")
    auto_items = {it["ref"] for it in items if it["auto_eligible"]}
    work_items = {it["ref"] for it in items if it["work_it"]}
    assert auto_items <= work_items

    auto_refs = {it["ref"] for it in backend._claim_candidates(auto_only=True)}
    work_refs = {it["ref"] for it in backend._claim_candidates()}
    manual_refs = {it["ref"] for it in backend._claim_candidates(manual_only=True)}
    assert auto_refs <= work_refs
    assert manual_refs <= work_refs
    # The two halves account for work_it exactly -- nothing a worker would
    # claim falls outside both, so neither counter can hide a claimable ticket.
    assert auto_refs | manual_refs == work_refs
    assert auto_refs == auto_items and work_refs == work_items
    # count_claimable (reconciler spawn depth) counts the auto set, never the
    # tickets that are only workable because a human pressed run;
    # count_manual_eligible is the counter that sees exactly those.
    assert backend.count_claimable() == len(auto_refs)
    assert backend.count_manual_eligible() == len(manual_refs)


def test_grace_period_delays_auto_claim_but_never_a_requested_run(tmp_path, monkeypatch):
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    config.set_auto_drain("GHI", True)
    config.set_grace_s("GHI", 180)
    _write_fake_issues(state, [
        _eligibility_issue(1, age_s=5),      # just filed: still protected
        _eligibility_issue(2, age_s=4000),   # old enough to drain
    ])

    assert q.count_claimable(project="GHI") == 1
    assert q.claim_next("worker-1", project="GHI")["ref"] == "GHI-2"
    assert q.claim_next("worker-2", project="GHI") is None

    q.mark_runnable("GHI-1")
    assert q.claim_next("worker-2", project="GHI")["ref"] == "GHI-1"


def test_close_no_longer_requires_the_legacy_queue_label(tmp_path, monkeypatch):
    """Dropped rule: close() refusing when the queue label is absent."""
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _write_fake_issues(state, [_fake_issue(1, "Never labelled")])

    closed = q.close("GHI-1", "worker-1", resolution={"summary": "done anyway"})

    assert closed["status"] == "closed"
    assert json.loads(state.read_text())["issues"][0]["state"] == "CLOSED"


# ==================================================================== migration

def test_github_drain_migration_turns_drain_off_exactly_once(tmp_path, monkeypatch):
    config, _q = _reload_isolated(tmp_path, monkeypatch)
    config.GH_DRAIN_MIGRATION_MARKER.unlink()
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    config.set_auto_drain("GHI", True)
    config.set_auto_drain("FILEQ", True)  # file-backed: not this migration's business

    assert config.migrate_github_auto_drain() == ["GHI"]
    assert config.auto_drain("GHI") is False
    assert config.auto_drain("FILEQ") is True

    # Turning it back on is a deliberate act the migration must never undo.
    config.set_auto_drain("GHI", True)
    assert config.migrate_github_auto_drain() == []
    assert config.auto_drain("GHI") is True


def test_reconciler_runs_the_drain_migration_and_says_why(tmp_path, monkeypatch, capsys):
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, _q = _reload_isolated(tmp_path, monkeypatch)
    import watchtower.workers as workers

    importlib.reload(workers)
    config.GH_DRAIN_MIGRATION_MARKER.unlink()
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    config.set_auto_drain("GHI", True)
    _write_fake_issues(state, [_eligibility_issue(1, age_s=4000)])

    result = workers.reconcile_once(dry_run=True)

    assert config.auto_drain("GHI") is False
    assert [s["queue"] for s in result["spawned"]] == []
    err = capsys.readouterr().err
    assert "drain was turned off for GHI" in err
    assert "drain was turned off for GHI" in (tmp_path / "activity.log").read_text()


# ========================================================== public-repo warning

def test_public_repo_warning_fires_only_for_public_repos(tmp_path, monkeypatch):
    _install_fake_gh(tmp_path, monkeypatch)
    import watchtower.github_backend as github_backend

    monkeypatch.setenv("FAKE_GH_VISIBILITY", "public")
    assert github_backend.repo_visibility("owner/repo") == "public"
    warning = github_backend.public_repo_warning("GHI", "owner/repo")
    assert "PUBLIC" in warning and "owner/repo" in warning

    monkeypatch.setenv("FAKE_GH_VISIBILITY", "private")
    assert github_backend.public_repo_warning("GHI", "owner/repo") == ""


def test_public_repo_visibility_is_unknown_without_gh(monkeypatch, tmp_path):
    """No gh, no auth, no network: report unknown rather than guess public."""
    import watchtower.github_backend as github_backend

    monkeypatch.setenv("PATH", str(tmp_path / "empty-bin"))
    assert github_backend.repo_visibility("owner/repo") == ""
    assert github_backend.public_repo_warning("GHI", "owner/repo") == ""


def test_drain_on_warns_before_enabling_on_a_public_repo(tmp_path, monkeypatch, capsys):
    _install_fake_gh(tmp_path, monkeypatch)
    config, _q = _reload_isolated(tmp_path, monkeypatch)
    import watchtower.cli as cli

    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    monkeypatch.setenv("FAKE_GH_VISIBILITY", "public")

    cli._warn_if_public_repo("GHI", config)
    assert "PUBLIC" in capsys.readouterr().err

    # A file-backed queue has no repo to check and must stay quiet.
    cli._warn_if_public_repo("PLAIN", config)
    assert capsys.readouterr().err == ""


# ================================================================ grace_s config

def test_grace_s_config_defaults_round_trips_and_is_visible_on_wt_config(
    tmp_path, monkeypatch, capsys,
):
    config, _q = _reload_isolated(tmp_path, monkeypatch)
    from watchtower.cli import main

    assert config.grace_s("GHI") == config.DEFAULT_GRACE_S == 180
    config.set_grace_s("GHI", 0)
    assert config.grace_s("GHI") == 0
    with pytest.raises(ValueError):
        config.set_grace_s("GHI", -1)
    config.set_grace_s("GHI", None)
    assert config.grace_s("GHI") == 180

    assert main(["config", "-q", "GHI", "--grace-s", "30"]) == 0
    assert "grace_s=30" in capsys.readouterr().out
    assert config.grace_s("GHI") == 30

    assert main(["config", "-q", "GHI"]) == 0
    assert "'grace_s': 30" in capsys.readouterr().out


# ==================================================== GitHub connectivity health

def test_gh_connectivity_backoff_escalates_and_resets_on_success(tmp_path, monkeypatch):
    """First failure backs off by the base delay; a further consecutive
    failure doubles it up to the cap; a success resets to the base again."""
    config, q = _reload_isolated(tmp_path, monkeypatch)
    import watchtower.github_backend as github_backend
    import time as _time

    # Whole-second values: `next_retry_at` is persisted at second precision
    # (matching every other WatchTower timestamp), so sub-second deltas would
    # round away and make the escalation assertion below meaningless.
    monkeypatch.setattr(github_backend, "_GH_BACKOFF_BASE_S", 1.0)
    monkeypatch.setattr(github_backend, "_GH_BACKOFF_CAP_S", 3.0)

    backend = github_backend.GitHubIssuesBackend("T", repo="acme/backoff-test")

    def failing_run(args, *, check=True):
        raise github_backend.GitHubBackendError("gh auth unavailable")

    monkeypatch.setattr(backend, "_run", failing_run)

    with pytest.raises(github_backend.GitHubBackendError):
        backend._list_issues()
    state = github_backend._load_connectivity()
    assert state["consecutive_failures"] == 1
    first_broken_since = state["broken_since"]
    assert first_broken_since is not None
    first_next_retry = github_backend._parse_iso(state["next_retry_at"])

    _time.sleep(1.2)  # cross the first (1s) backoff window
    github_backend._LIST_CACHE.clear()  # simulate a fresh process: cold cache

    with pytest.raises(github_backend.GitHubBackendError):
        backend._list_issues()
    state = github_backend._load_connectivity()
    assert state["consecutive_failures"] == 2
    assert state["broken_since"] == first_broken_since  # unchanged: still the same outage
    second_next_retry = github_backend._parse_iso(state["next_retry_at"])
    assert second_next_retry > first_next_retry  # escalated

    _time.sleep(2.2)  # cross the doubled (2s) backoff window
    github_backend._LIST_CACHE.clear()

    def succeeding_run(args, *, check=True):
        return json.dumps([])

    monkeypatch.setattr(backend, "_run", succeeding_run)
    assert backend._list_issues() == []
    state = github_backend._load_connectivity()
    assert state["consecutive_failures"] == 0
    assert state["broken_since"] is None
    assert state["next_retry_at"] is None
    assert state["last_success_at"] is not None


def test_gh_connectivity_backoff_blocks_cold_process_until_it_expires(tmp_path, monkeypatch):
    """A failure recorded by one call must block a *different, cache-cold*
    backend instance (simulating a fresh `wt status` process) from
    re-attempting `gh` until the persisted backoff window passes."""
    config, q = _reload_isolated(tmp_path, monkeypatch)
    import watchtower.github_backend as github_backend

    # A whole-second base: `next_retry_at` is persisted at second precision,
    # so a sub-second base (e.g. 0.2s) can truncate away to a near-zero
    # effective margin depending on where in the current second the failure
    # happens to land -- flaky by construction, not just slow.
    monkeypatch.setattr(github_backend, "_GH_BACKOFF_BASE_S", 2.0)

    backend = github_backend.GitHubIssuesBackend("T", repo="acme/cold-backoff-test")
    calls = {"n": 0}

    def failing_run(args, *, check=True):
        calls["n"] += 1
        raise github_backend.GitHubBackendError("gh auth unavailable")

    monkeypatch.setattr(backend, "_run", failing_run)
    with pytest.raises(github_backend.GitHubBackendError):
        backend._list_issues()
    assert calls["n"] == 1

    github_backend._LIST_CACHE.clear()  # fresh-process-like: cold in-memory cache
    cold = github_backend.GitHubIssuesBackend("T", repo="acme/cold-backoff-test")
    monkeypatch.setattr(cold, "_run", failing_run)
    with pytest.raises(github_backend.GitHubBackendError) as excinfo:
        cold._list_issues()
    assert calls["n"] == 1  # no new `gh` invocation -- served from persisted backoff
    assert excinfo.value.cached is True


def test_gh_connectivity_strict_bypasses_persisted_backoff(tmp_path, monkeypatch):
    """`strict=True` (claim/close today, `wt gh recheck` after Task 4) must
    force a live attempt immediately, regardless of an active backoff."""
    config, q = _reload_isolated(tmp_path, monkeypatch)
    import watchtower.github_backend as github_backend

    monkeypatch.setattr(github_backend, "_GH_BACKOFF_BASE_S", 60.0)

    backend = github_backend.GitHubIssuesBackend("T", repo="acme/recheck-test")

    def failing_run(args, *, check=True):
        raise github_backend.GitHubBackendError("gh auth unavailable")

    monkeypatch.setattr(backend, "_run", failing_run)
    with pytest.raises(github_backend.GitHubBackendError):
        backend._list_issues()

    calls = {"n": 0}

    def succeeding_run(args, *, check=True):
        calls["n"] += 1
        return json.dumps([])

    monkeypatch.setattr(backend, "_run", succeeding_run)
    github_backend._LIST_CACHE.clear()  # fresh-process-like: cold in-memory cache

    result = backend._list_issues(fresh=True, strict=True)
    assert result == []
    assert calls["n"] == 1
    state = github_backend._load_connectivity()
    assert state["broken_since"] is None  # the successful recheck cleared it


def test_gh_connectivity_stale_data_fallback_still_records_failure(tmp_path, monkeypatch):
    """WT-87's stale-data fallback returns cached good data without raising
    to the caller -- the connectivity state must still record the failure,
    since this is exactly the "silently degrading" case a human can't see
    any other way."""
    config, q = _reload_isolated(tmp_path, monkeypatch)
    import watchtower.github_backend as github_backend

    monkeypatch.setattr(github_backend, "_LIST_CACHE_TTL", 0.05)
    monkeypatch.setattr(github_backend, "_LIST_ERROR_BACKOFF", 60.0)
    # Ditto: this test is about failure recording, not the fetch-rate cap.
    monkeypatch.setattr(github_backend, "_LIST_FETCH_MIN_INTERVAL_S", 0.0)

    backend = github_backend.GitHubIssuesBackend("T", repo="acme/stale-fallback-test")
    good_issue = {
        "number": 1, "title": "t", "body": "", "state": "OPEN",
        "url": "https://github.com/acme/stale-fallback-test/issues/1",
        "assignees": [], "labels": [], "createdAt": "2026-07-01T00:00:00Z",
        "updatedAt": "2026-07-01T00:00:00Z", "closedAt": None,
    }
    monkeypatch.setattr(backend, "_run", lambda args, *, check=True: json.dumps([good_issue]))
    _no_etag_probe(monkeypatch, backend)
    assert backend._list_issues() == [good_issue]

    def failing_run(args, *, check=True):
        raise github_backend.GitHubBackendError("API rate limit already exceeded")

    monkeypatch.setattr(backend, "_run", failing_run)
    import time as _time
    _time.sleep(0.06)  # expire the TTL so the next call actually attempts gh
    stale = backend._list_issues()
    assert stale == [good_issue]  # served silently -- no exception reaches this caller

    state = github_backend._load_connectivity()
    assert state["broken_since"] is not None
    assert state["last_error"] == "API rate limit already exceeded"


def test_cli_gh_recheck_forces_live_check_and_reports_per_queue(tmp_path, monkeypatch, capsys):
    config, q = _reload_isolated(tmp_path, monkeypatch)
    import watchtower.github_backend as github_backend
    from watchtower.cli import main

    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")

    def succeed(self, args, *, check=True):
        return "[]"

    monkeypatch.setattr(github_backend.GitHubIssuesBackend, "_run", succeed)

    assert main(["gh", "recheck"]) == 0
    out = capsys.readouterr().out
    assert "GHI: ok" in out
    assert "GitHub connectivity: healthy" in out


def test_cli_gh_recheck_bypasses_backoff_after_a_prior_failure(tmp_path, monkeypatch, capsys):
    config, q = _reload_isolated(tmp_path, monkeypatch)
    import watchtower.github_backend as github_backend
    from watchtower.cli import main

    monkeypatch.setattr(github_backend, "_GH_BACKOFF_BASE_S", 3600.0)  # would not expire mid-test
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")

    def fail(self, args, *, check=True):
        raise github_backend.GitHubBackendError("gh auth unavailable")

    monkeypatch.setattr(github_backend.GitHubIssuesBackend, "_run", fail)
    assert q.list_items() == []  # records the failure and sets a long backoff

    def succeed(self, args, *, check=True):
        return "[]"

    monkeypatch.setattr(github_backend.GitHubIssuesBackend, "_run", succeed)
    github_backend._LIST_CACHE.clear()  # fresh-process-like: cold in-memory cache

    assert main(["gh", "recheck"]) == 0
    out = capsys.readouterr().out
    assert "GHI: ok" in out
    assert "GitHub connectivity: healthy" in out


def test_cli_status_prints_warning_when_github_alert_active(tmp_path, monkeypatch, capsys):
    config, q = _reload_isolated(tmp_path, monkeypatch)
    import watchtower.github_backend as github_backend
    from watchtower.cli import main

    state = github_backend._empty_connectivity()
    state["broken_since"] = "2026-01-01T00:00:00Z"  # far enough in the past to be >= threshold
    state["last_error"] = "gh auth login required"
    state["consecutive_failures"] = 9
    github_backend._save_connectivity(state)

    assert main(["status"]) == 0
    out = capsys.readouterr().out
    assert "GitHub unreachable" in out
    assert "gh auth login required" in out


def test_github_backend_acks_resolution_chips_in_issue_metadata(tmp_path, monkeypatch):
    """WATCHTOWER-13: `wt ack` works on GitHub-backed queues.

    The ack maps round-trip through the issue-body metadata block exactly like
    the resolution lists themselves, so a chip a human cleared stays cleared
    across re-reads -- and acking must not post a comment or re-close.
    """
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _drainable(config)

    q.enqueue(project="GHI", title="acked chips", note="n", text="b")
    q.claim_next("worker-1", project="GHI")
    q.close(
        "GHI-1",
        "worker-1",
        resolution={
            "summary": "done",
            "unresolved": ["stale one", "still live"],
            "caveats": ["watch out"],
        },
    )
    comments_before = len(json.loads(state.read_text())["issues"][0]["comments"])

    item = q.ack_resolution("GHI-1", targets=[("unresolved", 0)], by="amir")
    res = item["resolution"]
    assert q.is_acked(res, "unresolved", 0)
    assert not q.is_acked(res, "unresolved", 1)
    assert res["unresolved_ack"]["0"]["by"] == "amir"
    assert res["unresolved"] == ["stale one", "still live"]
    assert item["status"] == "closed"
    # Bookkeeping only: no new issue comment, no re-close.
    assert len(json.loads(state.read_text())["issues"][0]["comments"]) == comments_before

    # Survives a fresh read of the issue, and is idempotent.
    reread = q.get("GHI-1")["resolution"]
    assert reread["unresolved_ack"] == res["unresolved_ack"]
    again = q.ack_resolution("GHI-1", targets=[("unresolved", 0)], by="amir")
    assert again["resolution"]["unresolved_ack"]["0"]["at"] == res["unresolved_ack"]["0"]["at"]

    # --all covers every field; undo removes it again.
    every = q.ack_resolution("GHI-1", all_items=True)["resolution"]
    assert every["caveats_ack"].keys() == {"0"}
    assert every["unresolved_ack"].keys() == {"0", "1"}
    undone = q.ack_resolution("GHI-1", targets=[("caveats", 0)], undo=True)["resolution"]
    assert "caveats_ack" not in undone

    assert [e["event"] for e in q.get("GHI-1")["history"]][-4:] == [
        "close", "ack", "ack", "unack",
    ]


def test_github_backend_ack_rejects_bad_field_index_and_ticket(tmp_path, monkeypatch):
    import watchtower.github_backend as github_backend_mod

    _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _drainable(config)

    q.enqueue(project="GHI", title="no resolution yet", note="n", text="b")
    with pytest.raises(ValueError, match="no caveat/follow-up/unresolved"):
        q.ack_resolution("GHI-1", all_items=True)

    q.claim_next("worker-1", project="GHI")
    q.close("GHI-1", "worker-1", resolution={"summary": "s", "unresolved": ["a"]})
    with pytest.raises(ValueError, match="unknown resolution field"):
        q.ack_resolution("GHI-1", targets=[("bogus", 0)])
    with pytest.raises(ValueError, match="no index 4"):
        q.ack_resolution("GHI-1", targets=[("unresolved", 3)])
    with pytest.raises(ValueError, match="nothing selected"):
        q.ack_resolution("GHI-1", targets=[])
    # A ref with no issue behind it surfaces the backend's own not-found
    # error, as every other GitHub-backed operation does.
    with pytest.raises(github_backend_mod.GitHubBackendError):
        q.ack_resolution("GHI-999", all_items=True)


def test_fresh_read_seeds_from_persisted_cache_and_revalidates_with_etag(
    tmp_path, monkeypatch
):
    """WATCHTOWER-16, tightened by W4-4: a cold `fresh=True` process (wt claim
    / wt ls / wt status / the reconciler's dispatch path) must not pay an
    uncapped GraphQL `gh issue list`.

    WATCHTOWER-16 achieved that by seeding the process from the poller's
    snapshot so it could revalidate with a conditional REST probe instead.
    W4-4 goes one step further: inside `_persisted_list_ttl_s()` the snapshot
    is *served*, so a cold fresh read costs no `gh` invocation at all -- not
    the GraphQL list, and not the probe. Past the TTL the probe path below
    still applies, and the seeded validator is still what makes it cheap.
    """
    import watchtower.github_backend as github_backend

    monkeypatch.setenv(
        "WATCHTOWER_GH_LIST_CACHE_FILE", str(tmp_path / "gh-list-cache.json")
    )
    monkeypatch.setenv(
        "WATCHTOWER_GH_CONNECTIVITY_FILE", str(tmp_path / "gh-connectivity.json")
    )
    github_backend._LIST_CACHE.clear()

    repo = "acme/fresh-seed-test"
    issue = {
        "number": 1, "title": "cached", "body": "", "state": "OPEN",
        "url": f"https://github.com/{repo}/issues/1",
        "assignees": [], "labels": [], "createdAt": "2026-07-01T00:00:00Z",
        "updatedAt": "2026-07-01T00:00:00Z", "closedAt": None,
    }
    github_backend._write_persisted_list_entry(
        f"{repo}:open",
        {
            "at": time.time(),
            "data": [issue],
            "etag": '"v1"',
            "fetched_at": time.time(),
        },
    )

    backend = github_backend.GitHubIssuesBackend("T", repo=repo)
    probes = []

    def unexpected_run(args, *, check=True):
        raise AssertionError("fresh read must not spend a GraphQL list here")

    monkeypatch.setattr(backend, "_run", unexpected_run)
    monkeypatch.setattr(
        backend, "_run_raw", lambda args: (probes.append(list(args)), _not_modified())[1]
    )

    assert backend._list_issues("open", fresh=True) == [issue]
    assert probes == [], "a snapshot inside the TTL is served, not revalidated"

    # Past the TTL the reader does revalidate -- and inherits the poller's
    # validator to do it, which is the guarantee WATCHTOWER-16 added.
    github_backend._LIST_CACHE.clear()
    monkeypatch.setenv("WATCHTOWER_GH_LIST_TTL_S", "0.001")
    time.sleep(0.01)
    assert backend._list_issues("open", fresh=True) == [issue]
    assert 'If-None-Match: "v1"' in probes[-1]


def test_fresh_read_seeded_from_persisted_cache_honors_the_fetch_rate_cap(
    tmp_path, monkeypatch
):
    """Same seed, but the repo genuinely changed (probe 200). The heavy-fetch
    rate cap -- previously invisible to a cold CLI process -- now applies:
    within the cap the seeded snapshot is served, past it the fetch runs.

    The snapshot is aged past `_persisted_list_ttl_s()` throughout, because
    since W4-4 the TTL gate sits *in front* of this cap: inside the TTL the
    reader returns the snapshot and never reaches the probe. This test is
    about what happens once it does."""
    import watchtower.github_backend as github_backend

    monkeypatch.setenv(
        "WATCHTOWER_GH_LIST_CACHE_FILE", str(tmp_path / "gh-list-cache.json")
    )
    monkeypatch.setenv(
        "WATCHTOWER_GH_CONNECTIVITY_FILE", str(tmp_path / "gh-connectivity.json")
    )
    monkeypatch.setattr(github_backend, "_LIST_FETCH_MIN_INTERVAL_S", 60.0)
    github_backend._LIST_CACHE.clear()

    repo = "acme/fresh-seed-cap-test"
    issue = {
        "number": 1, "title": "cached", "body": "", "state": "OPEN",
        "url": f"https://github.com/{repo}/issues/1",
        "assignees": [], "labels": [], "createdAt": "2026-07-01T00:00:00Z",
        "updatedAt": "2026-07-01T00:00:00Z", "closedAt": None,
    }
    stale_at = time.time() - (github_backend._PERSISTED_LIST_STALE_S + 1)
    entry = {
        "at": stale_at, "data": [issue], "etag": '"v1"',
        "fetched_at": time.time(),
    }
    github_backend._write_persisted_list_entry(f"{repo}:open", dict(entry))

    backend = github_backend.GitHubIssuesBackend("T", repo=repo)
    counts = {"fetch": 0}

    def fake_run(args, *, check=True):
        counts["fetch"] += 1
        return json.dumps([issue])

    monkeypatch.setattr(backend, "_run", fake_run)
    monkeypatch.setattr(backend, "_run_raw", lambda args: _ok(etag="v2"))

    assert backend._list_issues("open", fresh=True) == [issue]
    assert counts["fetch"] == 0  # changed, but capped by the poller's clock

    # Past the cap, the same cold-process read does pay for the real list.
    github_backend._LIST_CACHE.clear()
    entry["fetched_at"] = time.time() - 61.0
    github_backend._write_persisted_list_entry(f"{repo}:open", dict(entry))
    assert backend._list_issues("open", fresh=True) == [issue]
    assert counts["fetch"] == 1


def test_poller_persists_the_etag_and_fetch_clock_for_other_processes(
    tmp_path, monkeypatch
):
    """The seed above only works if the poller writes its validator and
    fetch clock to the shared file, not just the data."""
    import watchtower.github_backend as github_backend

    monkeypatch.setenv(
        "WATCHTOWER_GH_LIST_CACHE_FILE", str(tmp_path / "gh-list-cache.json")
    )
    monkeypatch.setattr(github_backend, "_LIST_FETCH_MIN_INTERVAL_S", 0.0)
    github_backend._LIST_CACHE.clear()

    repo = "acme/poller-etag-persist-test"
    issue = {
        "number": 1, "title": "one", "body": "", "state": "OPEN",
        "url": f"https://github.com/{repo}/issues/1",
        "assignees": [], "labels": [], "createdAt": "2026-07-01T00:00:00Z",
        "updatedAt": "2026-07-01T00:00:00Z", "closedAt": None,
    }

    monkeypatch.setattr(
        github_backend.GitHubIssuesBackend,
        "_run",
        lambda self, args, check=True: json.dumps(
            [issue] if args[args.index("--state") + 1] == "open" else []
        ),
    )
    monkeypatch.setattr(
        github_backend.GitHubIssuesBackend,
        "_run_raw",
        lambda self, args, **kw: _ok(etag="v7"),
    )

    # First sweep bootstraps (nothing to validate yet); the second one probes.
    github_backend.refresh_persisted_list_cache(repo)
    github_backend.refresh_persisted_list_cache(repo)

    persisted = github_backend._read_persisted_list_cache()[f"{repo}:open"]
    assert persisted["etag"] == '"v7"'
    assert persisted["fetched_at"] > 0


# ============================================ user-level rate-limit hold (WT-19)

def test_rate_limit_error_on_any_gh_call_holds_off_live_calls(tmp_path, monkeypatch):
    """`gh api rate_limit` cannot see GitHub's user-level aggregate limit, so
    the pre-emptive guard passed while every GraphQL call failed. Observed
    rate-limit errors must set a hold of their own."""
    import watchtower.github_backend as github_backend

    monkeypatch.setenv(
        "WATCHTOWER_GH_CONNECTIVITY_FILE", str(tmp_path / "gh-connectivity.json")
    )
    # Don't shell out for the reset window in a unit test.
    monkeypatch.setattr(github_backend, "_rate_limit_hold_seconds", lambda: 300.0)
    backend = github_backend.GitHubIssuesBackend("T", repo="acme/ratelimit-test")
    monkeypatch.setattr(
        backend,
        "_run_raw",
        lambda args, **kw: _proc(
            1, stderr="API rate limit already exceeded for user ID 255024423."
        ),
    )

    with pytest.raises(github_backend.GitHubBackendError):
        backend._run(["issue", "view", "1", "--repo", backend.repo])

    state = github_backend._load_connectivity()
    assert state["rate_limited_until"], "the hold must be persisted"
    active, _ = github_backend._gh_backoff_active()
    assert active is True, "a rate-limit error must suppress live calls"


def test_rate_limit_hold_survives_when_the_generic_ladder_is_shorter(
    tmp_path, monkeypatch
):
    """The 60s unreachability ladder is too short for a quota wall: retrying
    in a minute just spends quota that is already gone."""
    import watchtower.github_backend as github_backend

    monkeypatch.setenv(
        "WATCHTOWER_GH_CONNECTIVITY_FILE", str(tmp_path / "gh-connectivity.json")
    )
    monkeypatch.setattr(github_backend, "_rate_limit_hold_seconds", lambda: 600.0)
    monkeypatch.setattr(github_backend, "_GH_BACKOFF_BASE_S", 60.0)

    github_backend._record_gh_failure("API rate limit exceeded for user ID 1234.")
    state = github_backend._load_connectivity()
    assert state["next_retry_at"] == state["rate_limited_until"]

    delta = (
        github_backend._parse_iso(state["next_retry_at"])
        - datetime.now(timezone.utc)
    ).total_seconds()
    assert delta > 120, "a rate-limit hold must outlast the first backoff step"


def test_a_successful_fetch_clears_the_rate_limit_hold(tmp_path, monkeypatch):
    import watchtower.github_backend as github_backend

    monkeypatch.setenv(
        "WATCHTOWER_GH_CONNECTIVITY_FILE", str(tmp_path / "gh-connectivity.json")
    )
    monkeypatch.setattr(github_backend, "_rate_limit_hold_seconds", lambda: 600.0)
    github_backend._record_gh_rate_limited("API rate limit exceeded")
    assert github_backend._gh_backoff_active()[0] is True

    github_backend._record_gh_success()
    state = github_backend._load_connectivity()
    assert state["rate_limited_until"] is None
    assert github_backend._gh_backoff_active()[0] is False


def test_non_rate_limit_errors_do_not_set_a_hold(tmp_path, monkeypatch):
    import watchtower.github_backend as github_backend

    monkeypatch.setenv(
        "WATCHTOWER_GH_CONNECTIVITY_FILE", str(tmp_path / "gh-connectivity.json")
    )
    github_backend._record_gh_failure("HTTP 503: Service Unavailable")
    assert github_backend._load_connectivity()["rate_limited_until"] is None


def test_rate_limit_hold_is_clamped_between_the_min_and_max(monkeypatch):
    """GitHub's reported reset is the wrong bucket's window, so it is a hint:
    never shorter than the floor, never a full-hour blackout."""
    import watchtower.github_backend as github_backend

    def reset_in(seconds):
        return lambda *a, **kw: _proc(0, stdout=str(int(time.time() + seconds)))

    monkeypatch.setattr(github_backend.subprocess, "run", reset_in(5))
    assert github_backend._rate_limit_hold_seconds() == pytest.approx(
        github_backend._GH_RATE_LIMIT_MIN_HOLD_S, abs=1
    )
    monkeypatch.setattr(github_backend.subprocess, "run", reset_in(3600))
    assert github_backend._rate_limit_hold_seconds() == pytest.approx(
        github_backend._GH_RATE_LIMIT_MAX_HOLD_S, abs=1
    )

    def boom(*a, **kw):
        raise OSError("gh missing")

    monkeypatch.setattr(github_backend.subprocess, "run", boom)
    assert (
        github_backend._rate_limit_hold_seconds()
        == github_backend._GH_RATE_LIMIT_MIN_HOLD_S
    )


def test_github_release_refuses_to_reopen_a_closed_ticket(tmp_path, monkeypatch):
    """OPS-854: `wt release` on a GitHub-backed ticket must honor the same
    require_status="in_progress" compare-and-swap the local backend has.
    The guard was silently dropped at the backend boundary, so releasing a
    ticket that was already closed ran `gh issue reopen` on it -- and the
    worker, told its close hadn't stuck, closed the ticket a second time
    (BECKY-1141)."""
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _drainable(config)

    item = q.enqueue(project="GHI", note="released after close", source="test")
    claimed = q.claim_next("worker-1", project="GHI")
    assert claimed["ref"] == item["ref"]
    closed = q.close(item["ref"], "worker-1", resolution={"summary": "done"})
    assert closed["status"] == "closed"

    # A release racing the close (or retrying after it) must be a no-op,
    # not a reopen.
    assert q.release(item["ref"], session_id="worker-1") is None
    assert q.get(item["ref"])["status"] == "closed"
    gh_state = json.loads(state.read_text())
    assert gh_state["issues"][0]["state"] == "CLOSED"
    assert not any(
        cmd[:2] == ["issue", "reopen"] for cmd in gh_state["commands"]
    )

    # Releasing a genuinely in-progress claim still works.
    item2 = q.enqueue(project="GHI", note="real release", source="test")
    claimed2 = q.claim_next("worker-1", project="GHI")
    assert claimed2["ref"] == item2["ref"]
    released = q.release(item2["ref"], session_id="worker-1")
    assert released is not None
    assert released["status"] == "open"
    assert q.get(item2["ref"])["status"] == "open"


def test_claim_guard_reverifies_a_stale_held_listing(tmp_path, monkeypatch):
    """OPS-854: after `wt close`, a GitHub queue's list snapshot can keep
    showing the just-closed ticket as in_progress for minutes (the poller's
    in-process cache survives the close's invalidation and re-persists the
    pre-close listing). The one-ticket-at-a-time guard must re-read each
    held ticket directly before refusing the worker's next claim."""
    _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _drainable(config)

    first = q.enqueue(project="GHI", note="closed by this worker", source="test")
    second = q.enqueue(project="GHI", note="next ticket", source="test")
    claimed = q.claim_next("worker-1", project="GHI")
    assert claimed["ref"] == first["ref"]
    stale_snapshot = dict(claimed)  # in_progress, claimed_by worker-1
    closed = q.close(first["ref"], "worker-1", resolution={"summary": "done"})
    assert closed["status"] == "closed"

    import watchtower.github_backend as github_backend

    orig_list_items = github_backend.GitHubIssuesBackend.list_items

    def stale_listing(self, *args, **kwargs):
        items = orig_list_items(self, *args, **kwargs)
        if kwargs.get("status") == "in_progress":
            return [stale_snapshot] + [
                it for it in items if it.get("ref") != first["ref"]
            ]
        return items

    monkeypatch.setattr(
        github_backend.GitHubIssuesBackend, "list_items", stale_listing
    )

    # The stale listing alone would refuse with "you still hold GHI-1"; the
    # direct re-read sees the close and the claim proceeds to GHI-2.
    nxt = q.claim_next("worker-1", project="GHI")
    assert nxt is not None
    assert nxt["ref"] == second["ref"]
    assert nxt["claimed_by"] == "worker-1"


def test_claim_guard_still_refuses_a_genuinely_held_ticket(tmp_path, monkeypatch):
    """The re-verification must not weaken the WATCHTOWER-20 guard: a ticket
    that is still genuinely in_progress under this worker still refuses the
    next claim."""
    _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _drainable(config)

    first = q.enqueue(project="GHI", note="still being worked", source="test")
    q.enqueue(project="GHI", note="next ticket", source="test")
    claimed = q.claim_next("worker-1", project="GHI")
    assert claimed["ref"] == first["ref"]

    with pytest.raises(ValueError, match=r"claim refused: you still hold GHI-1"):
        q.claim_next("worker-1", project="GHI")


def test_reconcile_skip_reason_names_grace_not_a_filter(tmp_path, monkeypatch):
    """A queue skipped during the grace window must SAY grace.

    The old reason string was inferred, not observed -- any depth==0 with
    open tickets was reported as "filtered by claim_types/readiness". Grace
    is the common real cause and was never one of the two named, so the log
    asserted a filter that wasn't acting. Two workers in a row believed it
    and went hunting for a stale-read bug that didn't exist (WATCHTOWER-24,
    WATCHTOWER-26); this is the guard against reintroducing that guess.
    """
    from datetime import datetime, timedelta, timezone

    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    import watchtower.workers as workers

    importlib.reload(workers)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    config.set_auto_drain("GHI", True)
    config.set_grace_s("GHI", 180)

    just_filed = _fake_issue(1, "Filed seconds ago")
    just_filed["createdAt"] = (
        datetime.now(timezone.utc) - timedelta(seconds=20)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    _write_fake_issues(state, [just_filed])

    result = workers.reconcile_once(dry_run=True)
    assert result["spawned"] == []
    reason = next(s["reason"] for s in result["skipped"] if s["queue"] == "GHI")

    assert "grace_s=180" in reason, reason
    # The specific false claim this ticket was filed about.
    assert "claim_types" not in reason and "readiness" not in reason, reason


def test_reconcile_skip_reason_names_readiness_when_readiness_is_the_gate(
    tmp_path, monkeypatch
):
    """The other half: when readiness really is what holds the queue back,
    the reason names it (and which value), rather than the old catch-all."""
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    import watchtower.workers as workers

    importlib.reload(workers)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _drainable(config)  # auto-drain on, grace 0 -- grace cannot be the cause
    import watchtower.github_backend as github_backend

    _write_fake_issues(
        state,
        [
            _fake_issue(
                1,
                "Half-formed",
                body=github_backend._body_with_metadata(
                    "Half-formed", {"readiness": "needs-spec"}
                ),
            )
        ],
    )

    result = workers.reconcile_once(dry_run=True)
    assert result["spawned"] == []
    reason = next(s["reason"] for s in result["skipped"] if s["queue"] == "GHI")

    assert "needs-spec" in reason, reason
    assert "grace" not in reason, reason

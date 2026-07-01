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
import urllib.request
from pathlib import Path

import pytest


FAKE_GH = r'''#!/usr/bin/env python3
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
def restore_watchtower_modules():
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
    import watchtower.config as config
    import watchtower.queue as q

    importlib.reload(config)
    importlib.reload(q)
    return config, q


def _write_fake_issues(state: Path, issues):
    state.write_text(json.dumps({"next": 1 + len(issues), "issues": issues, "commands": []}, indent=2))


def _fake_issue(number: int, title: str, labels=None, assignees=None, body: str = ""):
    labels = labels or []
    assignees = assignees or []
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
        "comments": [],
    }


def test_github_backend_enqueue_claim_close_round_trip(tmp_path, monkeypatch):
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "owner/repo")

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

    closed = q.close("GHI-1", "worker-1", resolution={"summary": "fixed it"})
    assert closed["status"] == "closed"
    assert closed["closed_by"] == "worker-1"
    assert closed["resolution"]["summary"] == "fixed it"

    gh_state = json.loads(state.read_text())
    issue = gh_state["issues"][0]
    assert issue["state"] == "CLOSED"
    assert "@me" in issue["assignees"]
    assert any("fixed it" in c for c in issue["comments"])


def test_cli_can_configure_and_use_github_backend(tmp_path, monkeypatch, capsys):
    state = _install_fake_gh(tmp_path, monkeypatch)
    _reload_isolated(tmp_path, monkeypatch)
    from watchtower.cli import main

    assert main([
        "set", "-q", "GHCLI",
        "--backend", "github",
        "--github-repo", "owner/repo",
    ]) == 0
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
    ]) == 0
    out = capsys.readouterr().out
    assert "CLOSED: GHCLI-1" in out

    gh_state = json.loads(state.read_text())
    commands = [" ".join(c) for c in gh_state["commands"]]
    assert any(c.startswith("issue create") for c in commands)
    assert any(c.startswith("issue edit 1") and "--add-assignee @me" in c for c in commands)
    assert any(c.startswith("issue close 1") for c in commands)


def test_github_backend_lists_all_open_issues_but_claims_only_queue_labeled(tmp_path, monkeypatch):
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "owner/repo")
    _write_fake_issues(state, [
        _fake_issue(1, "Plain GitHub issue"),
        _fake_issue(2, "Runnable WatchTower issue", labels=["watchtower:GHI"]),
    ])

    items = q.list_items(project="GHI")
    assert [it["ref"] for it in items] == ["GHI-1", "GHI-2"]
    assert {it["ref"]: it["claimable"] for it in items} == {
        "GHI-1": False,
        "GHI-2": True,
    }

    claimed = q.claim_next("worker-1", project="GHI")
    assert claimed["ref"] == "GHI-2"
    assert q.claim_next("worker-2", project="GHI") is None


def test_github_backend_refuses_direct_claim_until_issue_is_marked_runnable(tmp_path, monkeypatch):
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "owner/repo")
    _write_fake_issues(state, [_fake_issue(1, "Plain GitHub issue")])

    with pytest.raises(ValueError, match="missing label watchtower:GHI"):
        q.claim_by_ref("GHI-1", "worker-1")

    marked = q.mark_runnable("GHI-1")
    assert marked["claimable"] is True
    assert "watchtower:GHI" in json.loads(state.read_text())["issues"][0]["labels"]

    claimed = q.claim_by_ref("GHI-1", "worker-1")
    assert claimed["status"] == "in_progress"


def test_github_unlabeled_issues_count_as_visible_but_not_claimable_for_health_and_reconcile(tmp_path, monkeypatch):
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    import watchtower.health as health
    import watchtower.workers as workers

    importlib.reload(health)
    importlib.reload(workers)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "owner/repo")
    config.set_auto_drain("GHI", True)
    _write_fake_issues(state, [_fake_issue(1, "Plain GitHub issue")])

    row = {r["queue"]: r for r in health.all_status()}["GHI"]
    assert row["depth"] == 1
    assert row["claimable_depth"] == 0
    assert row["state"] == "backlog"

    result = workers.reconcile_once(dry_run=True)
    assert result["spawned"] == []
    assert any(
        skip["queue"] == "GHI" and "0 claimable" in skip["reason"]
        for skip in result["skipped"]
    )


def test_cli_run_marks_existing_github_issue_runnable(tmp_path, monkeypatch, capsys):
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, _q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "owner/repo")
    _write_fake_issues(state, [_fake_issue(1, "Plain GitHub issue")])
    from watchtower.cli import main

    assert main(["run", "GHI-1", "--no-dispatch"]) == 0
    out = capsys.readouterr().out
    assert "RUNNABLE: GHI-1" in out
    assert "watchtower:GHI" in json.loads(state.read_text())["issues"][0]["labels"]


def test_dashboard_run_api_marks_existing_github_issue_runnable(tmp_path, monkeypatch):
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, _q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "owner/repo")
    _write_fake_issues(state, [_fake_issue(1, "Plain GitHub issue")])
    import watchtower.dashboard as dashboard

    importlib.reload(dashboard)
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
    assert "watchtower:GHI" in json.loads(state.read_text())["issues"][0]["labels"]

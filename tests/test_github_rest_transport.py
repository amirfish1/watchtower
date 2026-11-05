"""REST transport for single-issue gh commands (GraphQL burst-limit fix).

`gh issue view/edit/close/reopen/comment/create` use GraphQL; under a fleet of
workers GraphQL answers "rate limit already exceeded" while REST still has
budget. `_run_raw` translates them to `gh api repos/...` by default. These
tests drive the real `_run` -> `_run_raw` -> subprocess path against a fake
`gh` that only speaks REST, so any untranslated call fails loudly.
"""
import json
import os
import stat
import sys
from pathlib import Path

import pytest

from watchtower import github_backend

FAKE_REST_GH = r'''#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
from urllib.parse import unquote
state = Path(os.environ["FAKE_GH_STATE"])
data = json.loads(state.read_text())
args = sys.argv[1:]
data["commands"].append(args)
def save(): state.write_text(json.dumps(data))
if args[:1] != ["api"]:
    save(); print("graphql path used: " + " ".join(args), file=sys.stderr); sys.exit(2)
method = args[args.index("-X") + 1] if "-X" in args else "GET"
path = [a for a in args[1:] if a.startswith("repos/") or a == "user"][0]
payload = json.loads(sys.stdin.read() or "{}") if "--input" in args else {}
if os.environ.get("FAKE_GH_RATELIMIT"):
    save(); print("gh: API rate limit exceeded for user ID 1. (HTTP 403)", file=sys.stderr); sys.exit(1)
if path == "user":
    save(); print(json.dumps({"login": "me-login"})); sys.exit(0)
parts = path.split("?")[0].split("/")  # repos/o/r/issues[/N[/sub[/x]]]
repo = "/".join(parts[1:3])
def issue(n):
    for i in data["issues"]:
        if i["number"] == int(n): return i
    save(); print("gh: Not Found (HTTP 404)", file=sys.stderr); sys.exit(1)
def out(obj): save(); print(json.dumps(obj)); sys.exit(0)
if len(parts) == 4 and method == "POST":
    n = len(data["issues"]) + 1
    i = {"number": n, "title": payload["title"], "body": payload.get("body", ""), "state": "open",
         "html_url": f"https://github.com/{repo}/issues/{n}", "labels": [{"name": l} for l in payload.get("labels", [])],
         "assignees": [], "user": {"login": "github-actions[bot]"}, "created_at": "2026-11-05T00:00:00Z",
         "updated_at": "2026-11-05T00:00:00Z", "closed_at": None, "comments": []}
    data["issues"].append(i); out(i)
i = issue(parts[4])
sub = parts[5] if len(parts) > 5 else None
if sub is None and method == "GET": out({k: v for k, v in i.items() if k != "comments"})
if sub is None and method == "PATCH":
    for k in ("title", "body"):
        if k in payload: i[k] = payload[k]
    if "state" in payload:
        i["state"] = payload["state"]; i["closed_at"] = "2026-11-05T01:00:00Z" if payload["state"] == "closed" else None
    out(i)
if sub == "comments" and method == "GET":
    cs = i["comments"]
    save(); print(json.dumps(cs[:1])); print(json.dumps(cs[1:])); sys.exit(0)  # two pages
if sub == "comments" and method == "POST":
    c = {"node_id": f"IC_{len(i['comments'])+1}", "user": {"login": "me-login"}, "body": payload["body"],
         "created_at": "2026-11-05T02:00:00Z", "html_url": f"{i['html_url']}#issuecomment-{len(i['comments'])+1}",
         "author_association": "OWNER"}
    i["comments"].append(c); out(c)
if sub == "labels" and method == "POST":
    for l in payload["labels"]:
        if l not in [x["name"] for x in i["labels"]]: i["labels"].append({"name": l})
    out(i["labels"])
if sub == "labels" and method == "DELETE":
    name = unquote(parts[6])
    if name not in [x["name"] for x in i["labels"]]:
        save(); print("gh: Label does not exist (HTTP 404)", file=sys.stderr); sys.exit(1)
    i["labels"] = [x for x in i["labels"] if x["name"] != name]; out(i["labels"])
if sub == "assignees" and method == "POST":
    i["assignees"] += [{"login": a} for a in payload["assignees"]]; out(i)
save(); print("unsupported: " + " ".join(args), file=sys.stderr); sys.exit(2)
'''


@pytest.fixture
def rest_gh(tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_GH_ISSUE_TRANSPORT", "rest")
    bindir = tmp_path / "bin"
    bindir.mkdir()
    exe = bindir / "gh"
    exe.write_text(FAKE_REST_GH.replace("#!/usr/bin/env python3", "#!" + sys.executable, 1))
    exe.chmod(exe.stat().st_mode | stat.S_IEXEC)
    state = tmp_path / "gh-state.json"
    state.write_text(json.dumps({"issues": [], "commands": []}))
    monkeypatch.setenv("PATH", f"{bindir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_GH_STATE", str(state))
    github_backend._REST_ME_LOGIN.clear()
    backend = github_backend.GitHubIssuesBackend.__new__(github_backend.GitHubIssuesBackend)
    backend.repo = "o/r"
    backend.repo_path = None
    return backend, state


def _state(path: Path):
    return json.loads(path.read_text())


def test_create_view_comment_edit_close_round_trip(rest_gh):
    b, state = rest_gh
    out = b._run(["issue", "create", "--repo", "o/r", "--title", github_backend._Text("T"),
                  "--label", "watchtower:BECKY"], body="hello")
    assert out.strip() == "https://github.com/o/r/issues/1"
    assert github_backend._ISSUE_URL_RE.search(out.strip()).group(1) == "1"

    url = b._run(["issue", "comment", "1", "--repo", "o/r"], body="first")
    b._run(["issue", "comment", "1", "--repo", "o/r"], body="second")
    assert url.strip().endswith("#issuecomment-1")

    b._run(["issue", "edit", "1", "--repo", "o/r", "--add-assignee", "@me",
            "--add-label", "wt:in-progress"], body="new body")
    b._run(["issue", "edit", "1", "--repo", "o/r", "--remove-label", "not-there"])

    view = json.loads(b._run(["issue", "view", "1", "--repo", "o/r", "--json",
                              "number,title,body,state,url,assignees,labels,author,comments,createdAt,updatedAt,closedAt"]))
    assert view["state"] == "OPEN" and view["body"] == "new body"
    assert [l["name"] for l in view["labels"]] == ["watchtower:BECKY", "wt:in-progress"]
    assert view["assignees"] == [{"login": "me-login"}]
    assert view["author"] == {"login": "github-actions"}  # GraphQL bot shape
    assert [c["body"] for c in view["comments"]] == ["first", "second"]  # both pages
    assert view["comments"][0]["author"] == {"login": "me-login"}
    assert view["comments"][0]["url"].endswith("#issuecomment-1")

    b._run(["issue", "edit", "1", "--repo", "o/r", "--remove-label", "wt:in-progress"])
    b._run(["issue", "close", "1", "--repo", "o/r"])
    assert json.loads(b._run(["issue", "view", "1", "--repo", "o/r", "--json", "state"])) == {"state": "CLOSED"}
    b._run(["issue", "reopen", "1", "--repo", "o/r"])
    assert json.loads(b._run(["issue", "view", "1", "--repo", "o/r", "--json", "state"])) == {"state": "OPEN"}

    cmds = _state(state)["commands"]
    assert all(c[0] == "api" for c in cmds), "every call went over REST"
    # Bodies travel on stdin, never argv (WT-40).
    assert not any("new body" in " ".join(c) or "hello" in " ".join(c) for c in cmds)


def test_rest_rate_limit_is_recorded_like_graphql(rest_gh, monkeypatch):
    b, _ = rest_gh
    recorded = []
    monkeypatch.setattr(github_backend, "_record_gh_rate_limited", lambda d: recorded.append(d))
    monkeypatch.setenv("FAKE_GH_RATELIMIT", "1")
    with pytest.raises(github_backend.GitHubBackendError):
        b._run(["issue", "view", "1", "--repo", "o/r", "--json", "state"])
    assert recorded and "rate limit" in recorded[0]


def test_unknown_flags_and_list_stay_on_gh(rest_gh):
    assert github_backend._parse_issue_argv(["issue", "list", "--repo", "o/r"]) is None
    assert github_backend._parse_issue_argv(["issue", "close", "1", "--repo", "o/r", "--comment", "x"]) is None
    assert github_backend._parse_issue_argv(["issue", "view", "1"]) is None  # no --repo: never guess


def test_graphql_opt_out(monkeypatch):
    monkeypatch.setenv("WATCHTOWER_GH_ISSUE_TRANSPORT", "graphql")
    assert not github_backend._rest_issue_transport_enabled()
    monkeypatch.delenv("WATCHTOWER_GH_ISSUE_TRANSPORT")
    assert github_backend._rest_issue_transport_enabled()

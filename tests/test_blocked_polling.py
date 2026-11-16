"""WT-81: filter before projection; share poll reads without subprocesses."""
import json
import threading
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

import pytest

from test_messages import wt  # noqa: F401
from test_github_backend import _fake_issue


def test_raw_filter_skips_unblocked_projection(wt, monkeypatch):
    from watchtower import github_backend as gh, github_pipeline as pipe
    backend = gh.GitHubIssuesBackend("GHI", repo="test-owner/test-repo")
    issues = [_fake_issue(n, f"GHI-{n}:some-name\tWork {n}") for n in range(1, 101)]
    issues[3]["body"] = 'work\n\n<!-- watchtower\nneeds_input: true\n-->'
    monkeypatch.setattr(backend, "_list_issues", lambda state, **kw: issues if state == "open" else [])
    monkeypatch.setattr(pipe, "_read_cached", lambda: {"items": {
        "test-owner/test-repo#5": {"parked": {"worker": "w"}, "pending_answer": {"state": "routing"}},
    }})
    projected = []
    original = backend._issue_to_item
    def project(issue):
        projected.append(issue["number"])
        return original(issue)
    monkeypatch.setattr(backend, "_issue_to_item", project)
    items = backend.list_items(blocked_only=True)
    assert projected == [4, 5]
    assert items[0]["title"] == "Work 4"
    assert items[1]["status"] == "awaiting_answer"
    assert items[1]["pending_answer"]["state"] == "routing"
    # The normal list memo must not reuse the filtered snapshot.
    assert len(backend.list_items()) == 100


@pytest.fixture
def api(wt, monkeypatch):
    from watchtower import dashboard as d
    monkeypatch.setattr(d, "_blocked_cache", {"at": 0., "payload": None})
    server = ThreadingHTTPServer(("127.0.0.1", 0), d._Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}", d
    server.shutdown()
    server.server_close()
    thread.join(5)


def read(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)


def test_http_shared_snapshot_scoping_and_expiry(api, monkeypatch):
    base, d = api
    calls = []
    rows = [{"ref": "WT-1", "project": "WT"}, {"ref": "OTHER-2", "project": "OTHER"}]
    monkeypatch.setattr(d.q, "list_blocked", lambda **kw: calls.append(kw) or rows)
    assert read(base + "/api/blocked") == rows
    assert read(base + "/api/blocked?queue=wt") == rows[:1]
    assert len(calls) == 1
    d._blocked_cache["at"] = 0
    assert read(base + "/api/blocked") == rows
    assert len(calls) == 2
    monkeypatch.setattr(d.q, "get", lambda ref: {"ref": ref} if ref == "WT-1" else None)
    assert read(base + "/api/ticket/WT-1") == {"ref": "WT-1"}
    with pytest.raises(urllib.error.HTTPError) as error:
        read(base + "/api/ticket/WT-9")
    assert error.value.code == 404


def test_http_failure_does_not_become_empty_snapshot(api, monkeypatch):
    base, d = api
    def fail(**kw):
        raise RuntimeError("source unavailable")
    monkeypatch.setattr(d.q, "list_blocked", fail)
    with pytest.raises(urllib.error.HTTPError) as error:
        read(base + "/api/blocked")
    assert error.value.code == 503
    assert d._blocked_cache["payload"] is None


def test_single_get_returns_private_copy(wt):
    item = wt.q.enqueue(project="WT", note="private projection")
    got = wt.q.get(item["ref"])
    got["note"] = "not persisted"
    got["history"].append({"event": "not persisted"})
    assert wt.q.get(item["ref"])["note"] == "private projection"
    assert not any(h.get("event") == "not persisted" for h in wt.q.get(item["ref"])["history"])

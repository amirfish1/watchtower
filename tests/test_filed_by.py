"""Who opened a ticket (CCC-1166): the filer rides the ``filed`` event so
timeline consumers (``wt find``, CCC's ticket detail) can show it without a
second lookup; GitHub-synced tickets expose the issue author as
``github_author``.
"""

from __future__ import annotations

from test_messages import wt  # noqa: F401  (shared isolated-sandbox fixture)
from test_github_backend import (
    _fake_issue,
    _install_fake_gh,
    _reload_isolated,
    _write_fake_issues,
)


def test_filed_event_records_the_submitter(wt):
    item = wt.q.enqueue(project="SUB", note="n", submitter="worker-a")
    filed = [e for e in item["history"] if e.get("event") == "filed"]
    assert filed and filed[0].get("submitter") == "worker-a"


def test_filed_event_omits_submitter_key_when_no_filer(wt):
    item = wt.q.enqueue(project="SUB", note="n")
    filed = [e for e in item["history"] if e.get("event") == "filed"]
    assert filed and "submitter" not in filed[0]


def test_timeline_enriches_a_legacy_filed_event_from_item_submitter(wt):
    """Tickets filed before the event field existed still show the filer:
    ``timeline()`` folds the ticket-level submitter onto the filed event at
    read time."""
    item = wt.q.enqueue(project="SUB", note="n", submitter="worker-a")
    stored = wt.q.get(item["ref"])
    for e in stored["history"]:
        if e.get("event") == "filed":
            e.pop("submitter", None)
    filed = [e for e in wt.q.timeline(stored) if e.get("event") == "filed"]
    assert filed and filed[0].get("submitter") == "worker-a"


def test_timeline_filed_event_falls_back_to_github_author(wt):
    item = {
        "created_at": "2026-01-01T00:00:00Z",
        "source": "github",
        "project": "GH",
        "github_author": "octocat",
    }
    filed = [e for e in wt.q.timeline(item) if e.get("event") == "filed"]
    assert filed and filed[0].get("submitter") == "octocat"


def test_event_summary_names_the_filer(wt):
    import watchtower.cli as cli

    assert (
        cli._event_summary({"event": "filed", "source": "ccc", "submitter": "amirfish"})
        == "filed from ccc by amirfish"
    )
    assert cli._event_summary({"event": "filed", "source": "ccc"}) == "filed from ccc"


def test_github_issue_author_maps_to_github_author(tmp_path, monkeypatch):
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    issue = _fake_issue(1, "opened by a human on github", labels=["watchtower:GHI"])
    issue["author"] = {"login": "octocat"}
    _write_fake_issues(state, [issue])

    items = q.list_items(project="GHI", fresh=True)

    assert items and items[0].get("github_author") == "octocat"

"""Named refs must remain interchangeable with stable IDs at every boundary."""
import json
import re

import pytest

from watchtower.refs import canonical_ref, display_ref, ticket_slug


@pytest.mark.parametrize("value,expected", [
    ("WT-79:anything", "WT-79"),
    ("wt-79-ADD-SUBSC", "WT-79"),
    ("SMALL-BUGS-139:stale", "SMALL-BUGS-139"),
    ("BYM-UX-97-something-else-42", "BYM-UX-97"),
    ("WT-79", "WT-79"), ("79", "79"),
    ("@agent-name", "@agent-name"),
])
def test_identity_ignores_name(value, expected):
    assert canonical_ref(value) == expected


def test_names_are_bounded_and_closed_tickets_render_lazily():
    assert ticket_slug("Add ticket subscribe controls today!") == "add-ticket-subscribe-controls"
    assert ticket_slug("Café / nice thing") == "cafe-nice-thing"
    assert ticket_slug("a" * 80) == "ticket"
    assert ticket_slug("Driver badge regression subagent") == "driver-badge-regression"
    assert ticket_slug("!!!") == "ticket"
    old = {"ref": "WT-79", "title": "Add ticket subscribe", "status": "closed"}
    assert display_ref(old) == "WT-79:add-ticket-subscribe"
    assert "slug" not in old


def test_file_list_find_edit_claim_close_and_notice(wt_env, run_cli, monkeypatch):
    q = wt_env.queue
    from watchtower import messages
    sent = []
    monkeypatch.setattr(messages, "send", lambda target, text, **kw: sent.append(text))
    monkeypatch.setattr(messages, "ccc_forward_target", lambda target: "")
    wt_env.config.set_subscribers("SMALL-BUGS", ["observer"])
    result = run_cli("add", "-q", "SMALL-BUGS", "--title", "Fix ticket name display")
    assert result.code == 0, result.output
    assert "FILED: SMALL-BUGS-1:fix-ticket-name-display" in result.out
    for ident in ("SMALL-BUGS-1", "SMALL-BUGS-1:wrong", "SMALL-BUGS-1-wrong"):
        result = run_cli("find", ident, "--json")
        assert result.code == 0, result.output
        item = json.loads(result.out)
        assert item["ref"] == "SMALL-BUGS-1"
        assert item["slug"] == "fix-ticket-name-display"
    assert "SMALL-BUGS-1:fix-ticket-name-display" in run_cli("ls", "-q", "SMALL-BUGS").out
    assert "SMALL-BUGS-1:fix-ticket-name-display  [open" in run_cli("find", "SMALL-BUGS-1").out
    result = run_cli("edit", "SMALL-BUGS-1:old", "--slug", "Better name", "--title", "Unrelated title")
    assert result.code == 0, result.output
    assert "SMALL-BUGS-1:better-name" in result.out
    assert run_cli("claim", "-q", "SMALL-BUGS", "SMALL-BUGS-1:stale", "--worker", "builder").code == 0
    result = run_cli("close", "SMALL-BUGS-1-old-name", "--worker", "builder", "--no-code", "--summary", "Done")
    assert result.code == 0, result.output
    assert "CLOSED: SMALL-BUGS-1:better-name" in result.out
    assert any("[watchtower] SMALL-BUGS-1:better-name closed" in text for text in sent)
    assert q.get("SMALL-BUGS-1")["status"] == "closed"


def test_named_dependencies_and_subscriptions_keep_canonical_keys(wt_env, run_cli):
    q = wt_env.queue
    first = q.enqueue(project="WT", note="First task", title="First task")
    second = q.enqueue(project="WT", note="Second task", title="Second task", blocked_by=["WT-1:stale"])
    assert second["blocked_by"] == [first["ref"]]
    result = run_cli("subscribe", "WT-2:stale", "observer")
    assert result.code == 0, result.output
    assert q.ticket_subscribers(q.get("WT-2"))
    page, _ = q.window_items("WT", q.WindowSpec(refs=["WT-2:stale"]))
    assert [it["ref"] for it in page] == ["WT-2"]
    assert set(q.items_by_refs(["WT-1:stale"])) == {"WT-1"}


def test_legacy_open_backfill_persists_on_save(wt_env):
    q = wt_env.queue
    item = q.enqueue(project="WT", note="Original title", title="Original title")
    with q._FileLock(q._lock_path()):
        data = q._load_unlocked()
        data["items"][0].pop("slug")
        q._save_unlocked(data)
    assert q.get(item["ref"])["slug"] == "original-title"
    q.update("WT-1:anything", title="A different title")
    assert q.get("WT-1")["slug"] == "original-title"


def test_existing_bare_ref_regex_consumers_still_match():
    text = "[watchtower] SMALL-BUGS-139:fix-display closed — done"
    # Existing CCC link and resolution-ref patterns intentionally capture IDs.
    assert re.search(r"\b([A-Z][A-Z0-9]*(?:-[A-Z0-9]+)*)-(\d+)\b", text).group() == "SMALL-BUGS-139"
    from watchtower.cli import _TICKET_REF_RE
    assert _TICKET_REF_RE.search(text).group() == "SMALL-BUGS-139"


def test_dashboard_labels_are_named_but_actions_keep_ids(wt_env):
    from watchtower import dashboard
    item = wt_env.queue.enqueue(project="WT", note="Fix display", title="Fix display")
    page = dashboard.render_queue("WT", {"queues": []}, [item])
    assert '>WT-1:fix-display</span>' in page
    assert "wtRun('WT-1')" in page
    closed = dict(item, status="closed", resolution={"summary": "Done"})
    assert '>WT-1:fix-display</span>' in dashboard._closed_block([closed], 1)

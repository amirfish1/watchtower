"""WATCHTOWER-42: a ticket's submitter lives on ONE host.

On a backend several hosts share (GitHub), a worker on another host blocks
or closes the ticket, and its notice to the bare submitter session id
searched only its own host (no socket, no FIFO, CCC "repo_required") and
dead-lettered after 20 attempts. The filing host's CCC node is now recorded
beside the submitter, and other hosts address the session as CCC's federated
``<node>:<sid>`` reference, which the delegate proxies to the owning node.
"""

from __future__ import annotations

import importlib
import json

import pytest

from test_messages import wt  # noqa: F401
from test_github_backend import _install_fake_gh, _reload_isolated, _drainable

REMOTE = "1b5f9cb4-8b60-46a9-b59f-fff2a9a56c7d"
SID = "cd3d8b4d-014e-4191-b6dd-7432b7ec66cf"


def _set_ccc_node(tmp_path, node):
    """Become CCC node ``node`` (sender_envelope reads CCC_STATE_DIR live)."""
    d = tmp_path / "ccc-state"
    d.mkdir(parents=True, exist_ok=True)
    (d / "node.json").write_text(json.dumps({"node_id": node}))


def _record_sends(monkeypatch, messages):
    calls = []

    def _fake_send(target, text, **kwargs):
        calls.append((target, text, kwargs))
        return {"ok": True, "transport": "fake"}

    monkeypatch.setattr(messages, "send", _fake_send)
    return calls


# ------------------------------------------------------------- addressing
def test_split_node_ref_only_accepts_a_uuid_node(wt):
    m = wt.messages
    assert m.split_node_ref(f"{REMOTE}:{SID}") == (REMOTE, SID)
    assert m.split_node_ref(SID) == ("", SID)
    assert m.split_node_ref("@agent") == ("", "@agent")
    assert m.split_node_ref("becky-7dedebca") == ("", "becky-7dedebca")
    assert m.split_node_ref("host:thing") == ("", "host:thing")


def test_node_qualified_only_for_another_node(wt, tmp_path):
    _set_ccc_node(tmp_path, "157eb096-890c-42b2-a78a-097e444c307c")
    m = wt.messages
    assert m.node_qualified(REMOTE, SID) == f"{REMOTE}:{SID}"
    assert m.node_qualified("157eb096-890c-42b2-a78a-097e444c307c", SID) == SID
    assert m.node_qualified("", SID) == SID  # legacy ticket: unchanged
    assert m.node_qualified(REMOTE, f"{REMOTE}:{SID}") == f"{REMOTE}:{SID}"
    assert m.node_qualified("not-a-uuid", SID) == SID
    assert m.node_qualified(REMOTE, "becky-7dedebca") == "becky-7dedebca"
    assert m.node_qualified(REMOTE, "@agent") == "@agent"


def test_resolve_target_remote_ref_routes_to_owner_node(wt, tmp_path):
    _set_ccc_node(tmp_path, "157eb096-890c-42b2-a78a-097e444c307c")
    r = wt.messages.resolve_target(f"{REMOTE}:{SID}")
    assert r["remote_node"] == REMOTE
    assert r["session_id"] == f"{REMOTE}:{SID}"
    assert r["engine"] == ""


def test_resolve_target_local_ref_resolves_as_the_bare_id(wt, tmp_path):
    local = "157eb096-890c-42b2-a78a-097e444c307c"
    _set_ccc_node(tmp_path, local)
    r = wt.messages.resolve_target(f"{local}:{SID}")
    assert r["session_id"] == SID
    assert not r.get("remote_node")


# ---------------------------------------------------------------- delivery
def test_remote_notice_goes_only_through_the_delegate_as_steer(wt, tmp_path, monkeypatch):
    _set_ccc_node(tmp_path, "157eb096-890c-42b2-a78a-097e444c307c")
    monkeypatch.setenv("WATCHTOWER_DELEGATE_URL", "http://127.0.0.1:9")
    posts = []
    monkeypatch.setattr(wt.messages, "_post_json",
                        lambda url, payload, timeout_s: posts.append((url, payload)) or {"ok": True})
    for name in ("_deliver_uds", "_deliver_fifo", "_deliver_resume"):
        monkeypatch.setattr(wt.messages, name, lambda *a, _n=name, **k: pytest.fail(f"{_n} used"))
    from watchtower import receipts
    monkeypatch.setattr(receipts, "record", lambda *a, **k: pytest.fail("local receipt for remote"))

    out = wt.messages.send(f"{REMOTE}:{SID}", "[watchtower] BECKY-1 needs input",
                           notify=True, start_new_if_needed=True)

    assert out["ok"] is True and out["transport"] == "delegate"
    assert len(posts) == 1
    url, payload = posts[0]
    assert url.endswith("/api/inject-input")
    assert payload["session_id"] == f"{REMOTE}:{SID}"
    assert payload["mode"] == "steer"
    assert "engine" not in payload


def test_remote_notice_parks_with_the_qualified_address(wt, tmp_path, monkeypatch):
    """The outbox retries resolve row["to"] again, so it must stay qualified."""
    _set_ccc_node(tmp_path, "157eb096-890c-42b2-a78a-097e444c307c")
    out = wt.messages.send(f"{REMOTE}:{SID}", "[watchtower] BECKY-1 closed", notify=True)
    assert out["queued"] is True
    rows = json.loads((tmp_path / "outbox.json").read_text())["messages"]
    assert [r["to"] for r in rows] == [f"{REMOTE}:{SID}"]


# ------------------------------------------------- file time + notify time
def test_enqueue_records_the_filing_node(wt, tmp_path):
    _set_ccc_node(tmp_path, REMOTE)
    item = wt.q.enqueue(submitter_subscribed=True, project="SUB", note="filed here", submitter=SID)
    assert item["submitter_node"] == REMOTE
    assert wt.q.get(item["ref"])["submitter_node"] == REMOTE


def test_enqueue_without_submitter_records_no_node(wt, tmp_path):
    _set_ccc_node(tmp_path, REMOTE)
    item = wt.q.enqueue(project="SUB", note="anonymous")
    assert "submitter_node" not in item


def test_notice_from_another_host_is_node_qualified(wt, tmp_path, monkeypatch):
    _set_ccc_node(tmp_path, REMOTE)              # filed on the VM ...
    item = wt.q.enqueue(submitter_subscribed=True, project="SUB", note="cross host", submitter=SID)
    wt.q.claim_by_ref(item["ref"], "worker-a")
    _set_ccc_node(tmp_path, "157eb096-890c-42b2-a78a-097e444c307c")  # ... blocked on the Mac
    calls = _record_sends(monkeypatch, wt.messages)
    forward = []
    monkeypatch.setattr(wt.messages, "ccc_forward_target",
                        lambda sid, **k: forward.append(sid))

    wt.q.block(item["ref"], session_id="worker-a", question="ship it?")

    assert [c[0] for c in calls] == [f"{REMOTE}:{SID}"]
    assert forward == []  # the local CCC's lineage map knows nothing of it


def test_notice_on_the_filing_host_stays_bare(wt, tmp_path, monkeypatch):
    _set_ccc_node(tmp_path, REMOTE)
    item = wt.q.enqueue(submitter_subscribed=True, project="SUB", note="same host", submitter=SID)
    wt.q.claim_by_ref(item["ref"], "worker-a")
    calls = _record_sends(monkeypatch, wt.messages)

    wt.q.block(item["ref"], session_id="worker-a", question="ship it?")

    assert [c[0] for c in calls] == [SID]


def test_remote_submitter_that_is_the_actor_gets_no_echo(wt, tmp_path, monkeypatch):
    _set_ccc_node(tmp_path, REMOTE)
    item = wt.q.enqueue(submitter_subscribed=True, project="SUB", note="self", submitter=SID)
    wt.q.claim_by_ref(item["ref"], "worker-a")
    _set_ccc_node(tmp_path, "157eb096-890c-42b2-a78a-097e444c307c")
    calls = _record_sends(monkeypatch, wt.messages)

    wt.q.close(item["ref"], SID, resolution={"summary": "done"}, force=True)

    assert calls == []


def test_github_ticket_filed_on_one_host_notifies_from_another(tmp_path, monkeypatch):
    """The production shape: BECKY-3532 was filed on the VM, blocked on the Mac."""
    _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _drainable(config)
    import watchtower.messages as messages
    importlib.reload(messages)
    calls = _record_sends(monkeypatch, messages)

    _set_ccc_node(tmp_path, REMOTE)
    item = q.enqueue(submitter_subscribed=True, project="GHI", note="gh cross host", submitter=SID)
    assert q.get(item["ref"])["submitter_node"] == REMOTE
    q.claim_by_ref(item["ref"], "worker-a")

    _set_ccc_node(tmp_path, "157eb096-890c-42b2-a78a-097e444c307c")
    blocked = q.block(item["ref"], session_id="worker-a", question="which slot?")

    assert blocked["submitter_node"] == REMOTE
    assert [c[0] for c in calls] == [f"{REMOTE}:{SID}"]

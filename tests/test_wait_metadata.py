"""Structured wait metadata on blocked tickets (CCC-244): who a block is
waiting on -- the owner, an external actor, a dependency, or a worker --
persisted per block generation on both backends."""
from __future__ import annotations

import importlib
import json

import pytest

from tests.test_github_backend import (  # noqa: F401 (fixtures/helpers)
    _drainable, _install_fake_gh, _reload_isolated,
)


@pytest.fixture()
def wt(tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(tmp_path / "activity.log"))
    import watchtower.queue as q
    import watchtower.cli as cli
    importlib.reload(q)
    importlib.reload(cli)
    return q, cli


def _blocked(q, note="needs a decision"):
    item = q.enqueue(project="T", note=note, source="test")
    q.claim_by_ref(item["ref"], "w1")
    return item["ref"]


def test_block_records_wait_for_its_generation(wt):
    q, _ = wt
    ref = _blocked(q)
    it = q.block(ref, session_id="w1", question="Ask Sonia?", wait_on="external",
                 wait_actor="Sonia")
    w = q.current_wait(it)
    assert w["on"] == "external" and w["actor"] == "Sonia"
    assert w["block_at"] == it["blocked_at"]
    block_ev = [e for e in it["history"] if e["event"] == "block"][-1]
    assert block_ev["wait_on"] == "external" and block_ev["wait_actor"] == "Sonia"


def test_newer_block_without_wait_supersedes_old_wait(wt):
    q, _ = wt
    ref = _blocked(q)
    q.block(ref, session_id="w1", question="Ask Sonia?", wait_on="external", wait_actor="Sonia")
    q.answer(ref, "Sonia replied")
    q.claim_by_ref(ref, "w1") if q.get(ref)["status"] == "open" else None
    it = q.block(ref, session_id="w1", question="Owner: approve the budget?")
    assert "wait" not in it
    assert q.current_wait(it) is None


def test_stale_wait_record_is_ignored(wt):
    q, _ = wt
    it = {"needs_input": True, "blocked_at": "2026-10-29T02:00:00Z",
          "wait": {"on": "external", "block_at": "2026-10-29T01:00:00Z"}}
    assert q.current_wait(it) is None
    it["wait"]["block_at"] = "2026-10-29T02:00:00Z"
    assert q.current_wait(it)["on"] == "external"
    it["needs_input"] = False
    assert q.current_wait(it) is None


def test_set_wait_corrects_legacy_block_without_unblocking(wt):
    q, _ = wt
    ref = _blocked(q)
    q.block(ref, session_id="w1", question="Awaiting OPS-193 credential")
    it = q.set_wait(ref, "dependency", ref="OPS-193", note="credential handoff", by="ops-1")
    assert it["needs_input"] is True
    assert it["block_question"] == "Awaiting OPS-193 credential"
    assert q.current_wait(it)["ref"] == "OPS-193"
    events = [e["event"] for e in it["history"]]
    assert events[-2:] == ["block", "wait"]
    # A second correction appends; history keeps both.
    it = q.set_wait(ref, "owner")
    assert [e["event"] for e in it["history"]].count("wait") == 2
    assert q.current_wait(it)["on"] == "owner"


def test_set_wait_rejects_unblocked_and_bad_values(wt):
    q, _ = wt
    item = q.enqueue(project="T", note="open", source="test")
    with pytest.raises(ValueError, match="not blocked"):
        q.set_wait(item["ref"], "external")
    ref = _blocked(q)
    q.block(ref, session_id="w1", question="?")
    with pytest.raises(ValueError, match="wait-on"):
        q.set_wait(ref, "sonia")
    with pytest.raises(ValueError):
        q.block(ref, session_id="w1", question="?", wait_on="nobody")


def test_cli_wait_and_block_flags(wt, capsys):
    q, cli = wt
    ref = _blocked(q)
    assert cli.main(["block", ref, "--worker", "w1", "--question", "Await client",
                     "--wait-on", "external", "--wait-actor", "Sonia", "--json"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["wait"]["actor"] == "Sonia"
    assert cli.main(["wait-on", ref, "--on", "dependency", "--ref", "OPS-167"]) == 0
    assert "WAITING: " + ref + " on dependency" in capsys.readouterr().out
    assert q.current_wait(q.get(ref))["ref"] == "OPS-167"


def test_github_block_persists_kind_and_wait(tmp_path, monkeypatch):
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _drainable(config)
    item = q.enqueue(project="GHI", note="client question")
    q.claim_by_ref(item["ref"], "worker-1")

    it = q.block("GHI-1", session_id="worker-1", question="Await Sonia",
                 kind="awaiting-client", wait_on="external", wait_actor="Sonia")
    assert it["block_kind"] == "awaiting-client"
    assert it["blocked_at"]
    assert q.current_wait(it)["actor"] == "Sonia"
    body = json.loads(state.read_text())["issues"][0]["body"]
    assert '"actor": "Sonia"' in body

    # Reload from the issue body: survives a host restart.
    config, q = _reload_isolated(tmp_path, monkeypatch)
    it = q.get("GHI-1")
    assert q.current_wait(it)["on"] == "external"

    it = q.set_wait("GHI-1", "owner", note="Sonia replied; owner must choose")
    assert q.current_wait(it)["on"] == "owner"
    assert [e["event"] for e in it["history"]][-1] == "wait"
    assert it["needs_input"] is True

"""Product gate (2026-09-01 design): block kinds, the needs-rationale icebox,
pre-ack, gate ack/nack, and the close guard."""
import json

import pytest

QUEUE = "GATEQ"


def _file_ticket(wt_env, **kw):
    return wt_env.queue.enqueue(project=QUEUE, note=kw.pop("note", "a bug"), **kw)


# ---------------------------------------------------------------- data model

def test_block_kind_defaults_to_input(wt_env):
    it = _file_ticket(wt_env)
    blocked = wt_env.queue.block(it["ref"], session_id="w1", question="which db?")
    assert blocked["block_kind"] == "input"
    assert blocked["needs_input"] is True


def test_block_kind_rationale_is_stored_and_survives_reload(wt_env):
    it = _file_ticket(wt_env)
    wt_env.queue.block(it["ref"], session_id="w1",
                       question="PITCH: worth fixing?", kind="rationale")
    fresh = wt_env.queue.get(it["ref"])
    assert fresh["block_kind"] == "rationale"
    assert fresh["block_question"] == "PITCH: worth fixing?"


def test_unknown_block_kind_degrades_to_input(wt_env):
    it = _file_ticket(wt_env)
    blocked = wt_env.queue.block(it["ref"], session_id="w1",
                                 question="q", kind="bogus")
    assert blocked["block_kind"] == "input"


def test_reopen_clears_block_kind(wt_env):
    it = _file_ticket(wt_env)
    wt_env.queue.block(it["ref"], session_id="w1", question="q", kind="rationale")
    wt_env.queue.update_status(it["ref"], "open")
    fresh = wt_env.queue.get(it["ref"])
    assert fresh["block_kind"] == ""
    assert fresh["needs_input"] is False


def test_needs_rationale_is_valid_and_unclaimable(wt_env):
    assert "needs-rationale" in wt_env.queue.VALID_READINESS
    assert "needs-rationale" in wt_env.queue.UNCLAIMABLE_READINESS
    _file_ticket(wt_env, readiness="needs-rationale")
    assert wt_env.queue.claim_next("w1", project=QUEUE) is None


def test_enqueue_pre_ack(wt_env):
    it = _file_ticket(wt_env, pre_ack=True)
    assert it["pre_ack"] is True
    assert _file_ticket(wt_env)["pre_ack"] is False


# ---------------------------------------------------------------------- CLI

def test_wt_add_pre_ack_and_block_kind(wt_env, run_cli):
    res = run_cli("add", "-q", QUEUE, "--note", "ship the widget", "--pre-ack")
    assert res.code == 0, res.output
    items = wt_env.queue.list_items(project=QUEUE)
    assert items[-1]["pre_ack"] is True
    ref = items[-1]["ref"]
    res = run_cli("block", ref, "--worker", "w1",
                  "--kind", "rationale", "--question", "PITCH: worth it?")
    assert res.code == 0, res.output
    assert wt_env.queue.get(ref)["block_kind"] == "rationale"

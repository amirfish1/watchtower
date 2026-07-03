"""Delivery receipts tests (WT-77) — sandboxed like test_messages."""

from __future__ import annotations

import importlib
import json
import os
import time
from pathlib import Path

from test_messages import SID_A, SID_B, _fake_popen, _write_transcript, wt  # noqa: F401


def _receipts(wt):
    import watchtower.receipts as receipts
    importlib.reload(receipts)
    return receipts


def _append_user_event(wt, sid, text):
    d = Path(os.environ["WATCHTOWER_CLAUDE_PROJECTS_DIR"]) / "some-project"
    p = d / f"{sid}.jsonl"
    with open(p, "a") as f:
        f.write(json.dumps({"type": "user", "message": {"content": text}}) + "\n")


def test_send_records_receipt_and_verifies_landed(wt, monkeypatch):
    rc = _receipts(wt)
    _write_transcript(wt, SID_B, age_s=600)
    calls = []
    monkeypatch.setattr(wt.messages.subprocess, "Popen", _fake_popen(calls))
    res = wt.messages.send(SID_B, "please do the thing")
    assert res["ok"] is True and res.get("receipt_id", "").startswith("rcpt-")
    rec = rc.get(res["receipt_id"], refresh=False)
    assert rec["status"] == "pending" and rec["transport"] == "resume"
    # the message lands in the transcript -> verified "landed"
    _append_user_event(wt, SID_B, "please do the thing")
    rec = rc.get(res["receipt_id"])
    assert rec["status"] == "landed"


def test_receipt_goes_lost_when_transcript_never_advances(wt, monkeypatch):
    rc = _receipts(wt)
    _write_transcript(wt, SID_B, age_s=600)
    calls = []
    monkeypatch.setattr(wt.messages.subprocess, "Popen", _fake_popen(calls))
    monkeypatch.setenv("WATCHTOWER_RECEIPT_WAIT_S", "60")
    res = wt.messages.send(SID_B, "into the void")
    rc.sweep(now=time.time() + 120)  # past the wait window, no advance
    rec = rc.get(res["receipt_id"], refresh=False)
    assert rec["status"] == "lost"


def test_receipt_advanced_when_transcript_grows_without_needle(wt, monkeypatch):
    rc = _receipts(wt)
    _write_transcript(wt, SID_B, age_s=600)
    calls = []
    monkeypatch.setattr(wt.messages.subprocess, "Popen", _fake_popen(calls))
    res = wt.messages.send(SID_B, "some very specific text")
    _append_user_event(wt, SID_B, "unrelated other activity")
    rec = rc.get(res["receipt_id"])
    assert rec["status"] == "advanced"


def test_stats_counts_by_outcome_inside_window(wt, monkeypatch):
    rc = _receipts(wt)
    _write_transcript(wt, SID_B, age_s=600)
    calls = []
    monkeypatch.setattr(wt.messages.subprocess, "Popen", _fake_popen(calls))
    r1 = wt.messages.send(SID_B, "message one")
    _append_user_event(wt, SID_B, "message one")
    rc.sweep()
    s = rc.stats()
    assert s["total"] == 1 and s["landed"] == 1 and s["lost"] == 0


def test_needle_matches_json_encoded_text(wt):
    rc = _receipts(wt)
    # unicode + quotes survive the jsonl encoding round-trip
    text = 'he said "shalom" — עובד'
    needle = rc._needle(text)
    encoded_line = json.dumps({"message": {"content": text}})
    assert needle in encoded_line

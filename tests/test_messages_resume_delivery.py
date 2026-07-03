"""Resume-adapter delivery correctness: session cwd + boot verification.

Regression tests for the 2026-07-02 silent text-loss incident: the resume
adapter spawned ``claude -p --resume <sid>`` from wt's own cwd, claude died
instantly with "No conversation found with session ID" (resume lookups are
scoped to the cwd's project bucket), and the adapter still reported ok —
every message delegated by CCC's inject path was dropped without a trace.

Reuses the isolated ``wt`` sandbox fixture and helpers from test_messages.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

from test_messages import (  # noqa: F401  (wt is a fixture, needed in scope)
    SID_B,
    _fake_popen,
    _write_transcript,
    wt,
)


def _write_transcript_with_cwd(wt, sid, cwd, age_s=600.0):
    """Transcript whose events carry the session's real working directory."""
    d = Path(os.environ["WATCHTOWER_CLAUDE_PROJECTS_DIR"]) / "some-project"
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{sid}.jsonl"
    p.write_text(json.dumps({"type": "user", "cwd": str(cwd)}) + "\n")
    old = time.time() - age_s
    os.utime(p, (old, old))
    return p


def test_resume_spawns_claude_in_session_cwd(wt, monkeypatch):
    """claude --resume only finds a session when run from that session's
    project directory — the adapter must read the cwd out of the transcript
    and spawn there."""
    proj = wt.tmp / "real-project"
    proj.mkdir()
    _write_transcript_with_cwd(wt, SID_B, proj)
    calls = []
    monkeypatch.setattr(wt.messages.subprocess, "Popen", _fake_popen(calls))
    res = wt.messages.send(SID_B, "resume me")
    assert res["ok"] is True and res["transport"] == "resume"
    assert calls[0][1].get("cwd") == str(proj)


def test_resume_falls_back_to_registry_cwd(wt, monkeypatch):
    """No cwd in the transcript: use the target's registered cwd."""
    proj = wt.tmp / "registered-project"
    proj.mkdir()
    _write_transcript(wt, SID_B, age_s=600)
    wt.messages.register_agent("mover", SID_B, cwd=str(proj))
    calls = []
    monkeypatch.setattr(wt.messages.subprocess, "Popen", _fake_popen(calls))
    res = wt.messages.send("mover", "resume me")
    assert res["ok"] is True
    assert calls[0][1].get("cwd") == str(proj)


def test_resume_reports_failure_when_claude_dies_at_boot(wt, monkeypatch):
    """A resume child that exits during the verify window is a delivery
    FAILURE — the old fire-and-forget reported ok and silently dropped the
    message. Honest failure lets send() park it for retry instead."""
    _write_transcript(wt, SID_B, age_s=600)

    class DeadProc:
        pid = 424243

        def poll(self):
            return 1

    monkeypatch.setattr(
        wt.messages.subprocess, "Popen", lambda *a, **k: DeadProc()
    )
    monkeypatch.setenv("WATCHTOWER_RESUME_VERIFY_S", "0.2")
    res = wt.messages.send(SID_B, "resume me")
    assert res["ok"] is False and res["queued"] is True
    assert "exited" in res["error"]
    assert wt.messages.outbox_list(status="pending")

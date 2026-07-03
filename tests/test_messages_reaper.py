"""Resume-child reaper (WT-82): ledger-based daemon backstop.

Post-6ab1aaf resume children exit via stdin EOF, so the reaper is
defense-in-depth: every wt-spawned resume child is recorded in a wt-owned
ledger at spawn, and the daemon tick SIGTERMs ledger pids whose msg log shows
a completed turn and has sat idle past the reap window. The hard rule under
test throughout: pids outside the ledger — or ledger pids that no longer look
like OUR ``claude --resume <sid>`` in ps — are never signalled (CCC keeps ITS
resume children alive for reuse and they are indistinguishable in ps).

Reuses the isolated ``wt`` sandbox fixture and helpers from test_messages.
"""

from __future__ import annotations

import json
import os
import time

from test_messages import (  # noqa: F401  (wt is a fixture, needed in scope)
    SID_B,
    _fake_popen,
    _write_transcript,
    wt,
)

PID = 424242


# ------------------------------------------------------------------- helpers
def _ledger(wt):
    return wt.messages._load_resume_ledger()


def _seed_ledger(wt, pid=PID, sid=SID_B, log_age_s=None, log_lines=None):
    """One ledger entry plus its msg log on disk."""
    log = wt.tmp / f"msg-{sid[:8]}-fake.log"
    log.write_text("".join(json.dumps(l) + "\n" for l in (log_lines or [])))
    if log_age_s is not None:
        old = time.time() - log_age_s
        os.utime(log, (old, old))
    wt.messages._resume_ledger_add(pid, sid, str(log))
    return log


RESULT_LINE = {"type": "result", "result": "done"}


def _fake_kill(monkeypatch, wt, *, alive=frozenset([PID])):
    """Replace os.kill: signal-0 probes answer from ``alive``; real signals
    are recorded, never sent."""
    sent = []

    def kill(pid, sig):
        if sig == 0:
            if pid not in alive:
                raise ProcessLookupError(pid)
            return None
        sent.append((pid, sig))

    monkeypatch.setattr(wt.messages.os, "kill", kill)
    return sent


# ---------------------------------------------------------------- spawn side
def test_resume_delivery_records_child_in_ledger(wt, monkeypatch):
    _write_transcript(wt, SID_B, age_s=600)
    calls = []
    monkeypatch.setattr(wt.messages.subprocess, "Popen", _fake_popen(calls))
    res = wt.messages.send(SID_B, "track me")
    assert res["ok"] is True
    entries = _ledger(wt)
    assert len(entries) == 1
    assert entries[0]["pid"] == PID and entries[0]["sid"] == SID_B
    assert entries[0]["log"] == res["log"]


# ----------------------------------------------------------------- reap pass
def test_reap_terminates_finished_idle_child(wt, monkeypatch):
    _seed_ledger(wt, log_age_s=700, log_lines=[RESULT_LINE])
    sent = _fake_kill(monkeypatch, wt)
    monkeypatch.setattr(
        wt.messages, "_pid_command",
        lambda pid: f"claude -p --verbose --resume {SID_B}",
    )
    out = wt.messages.reap_resume_children()
    assert [p for p, _ in sent] == [PID]
    assert out["reaped"][0]["pid"] == PID
    assert _ledger(wt) == []


def test_reap_leaves_midturn_child_alone(wt, monkeypatch):
    """No terminal record in the log = the turn may still be running."""
    _seed_ledger(wt, log_age_s=700, log_lines=[{"type": "assistant"}])
    sent = _fake_kill(monkeypatch, wt)
    monkeypatch.setattr(
        wt.messages, "_pid_command",
        lambda pid: f"claude -p --verbose --resume {SID_B}",
    )
    out = wt.messages.reap_resume_children()
    assert sent == [] and out["reaped"] == []
    assert len(_ledger(wt)) == 1


def test_reap_waits_out_the_idle_window(wt, monkeypatch):
    """Turn complete but log still fresh: give EOF-exit a chance first."""
    _seed_ledger(wt, log_age_s=5, log_lines=[RESULT_LINE])
    sent = _fake_kill(monkeypatch, wt)
    monkeypatch.setattr(
        wt.messages, "_pid_command",
        lambda pid: f"claude -p --verbose --resume {SID_B}",
    )
    out = wt.messages.reap_resume_children()
    assert sent == [] and out["reaped"] == []
    assert len(_ledger(wt)) == 1


def test_reap_clears_exited_children_without_signalling(wt, monkeypatch):
    """The normal post-EOF path: child already gone, entry just cleared."""
    _seed_ledger(wt, log_age_s=700, log_lines=[RESULT_LINE])
    sent = _fake_kill(monkeypatch, wt, alive=frozenset())
    out = wt.messages.reap_resume_children()
    assert sent == [] and out["reaped"] == [] and out["cleared"] == 1
    assert _ledger(wt) == []


def test_reap_never_signals_a_reused_pid(wt, monkeypatch):
    """Pid alive but ps no longer shows OUR claude --resume <sid>: the pid
    was recycled by an unrelated process. Clear the entry, never signal."""
    _seed_ledger(wt, log_age_s=700, log_lines=[RESULT_LINE])
    sent = _fake_kill(monkeypatch, wt)
    monkeypatch.setattr(
        wt.messages, "_pid_command", lambda pid: "vim /etc/hosts"
    )
    out = wt.messages.reap_resume_children()
    assert sent == [] and out["reaped"] == [] and out["cleared"] == 1
    assert _ledger(wt) == []


def test_reap_never_signals_another_sessions_resume_child(wt, monkeypatch):
    """A claude resume child for a DIFFERENT sid (e.g. CCC's, kept alive on
    purpose) fails the identity gate even if the pid matches the ledger."""
    _seed_ledger(wt, log_age_s=700, log_lines=[RESULT_LINE])
    sent = _fake_kill(monkeypatch, wt)
    monkeypatch.setattr(
        wt.messages, "_pid_command",
        lambda pid: "claude -p --resume 99999999-dead-beef-0000-000000000000",
    )
    wt.messages.reap_resume_children()
    assert sent == []


def test_reap_honours_env_idle_override(wt, monkeypatch):
    _seed_ledger(wt, log_age_s=30, log_lines=[RESULT_LINE])
    sent = _fake_kill(monkeypatch, wt)
    monkeypatch.setattr(
        wt.messages, "_pid_command",
        lambda pid: f"claude -p --verbose --resume {SID_B}",
    )
    monkeypatch.setenv("WATCHTOWER_REAP_IDLE_S", "10")
    out = wt.messages.reap_resume_children()
    assert [p for p, _ in sent] == [PID] and len(out["reaped"]) == 1


def test_reap_drops_malformed_entries(wt):
    wt.messages._save_resume_ledger([{"pid": 0, "sid": ""}, "not-a-dict-entry"])
    out = wt.messages.reap_resume_children()
    assert out["cleared"] == 2 and _ledger(wt) == []

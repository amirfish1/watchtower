"""WT-31 D2.7: every claim write binds ``claim_proc``; ``liveness.claim_owner``
proves the bound process alive, dead, or leaves it unproven.

Phase A asserts verdicts only; the reopen/resume actions on a dead owner are
phase B (``recover_claim``).
"""

from __future__ import annotations

import datetime as _dt
import json
import os
import subprocess
import time

import pytest

from liveness_golden import SID, SID2, wt  # noqa: F401  (fixture)

PQ = "LQ"


def _iso(ts: float) -> str:
    return _dt.datetime.fromtimestamp(ts, _dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Proc:
    """A real child process standing in for a worker, with its record."""

    def __init__(self, wt, worker_id: str, sid: str, engine: str = "claude"):
        self.wt = wt
        self.p = subprocess.Popen(["sleep", "60"])
        self.rec = {"worker_id": worker_id, "session_id": sid, "engine": engine,
                    "pid": self.p.pid, "pid_started": wt.workers._pid_start_token(self.p.pid),
                    "started_at": _iso(time.time() - 5), "exit_file": ""}
        data = wt.workers._load()
        data["workers"].append(self.rec)
        wt.workers._save(data)

    def kill(self):
        self.p.kill()
        self.p.wait()

    def prune(self):
        data = self.wt.workers._load()
        data["workers"] = [w for w in data["workers"] if w.get("pid") != self.p.pid]
        self.wt.workers._save(data)


@pytest.fixture()
def procs(wt, tmp_path):
    (tmp_path / "claude-home" / "sessions").mkdir(parents=True)   # registry readable
    made = []

    def make(*a, **k):
        p = Proc(wt, *a, **k)
        made.append(p)
        return p
    yield make
    for p in made:
        if p.p.poll() is None:
            p.kill()


def _owner(wt, ref):
    return wt.liveness.claim_owner(wt.q.get(ref), wt.liveness.ResolverContext())


def _file(wt, note="t", **kw):
    return wt.q.enqueue(project=PQ, note=note, source="test", **kw)["ref"]


# ------------------------------------------------------------ claim paths
def test_warm_worker_second_ticket_then_exit_is_dead(wt, procs):
    w = procs("w1", SID)
    a, b = _file(wt, "a"), _file(wt, "b")
    wt.q.claim_by_ref(a, "w1", session_uuid=SID)
    wt.q.update_status(a, "closed", "w1")
    got = wt.q.claim_next("w1", project=PQ, session_uuid=SID)
    assert got["ref"] == b
    cp = wt.q.get(b)["claim_proc"]
    assert cp["bound"] == "record" and cp["pid"] == w.p.pid
    assert _owner(wt, b).verdict == "alive"
    w.kill()
    v = _owner(wt, b)
    assert v.verdict == "dead" and "process gone" in v.evidence
    w.prune()                                   # the record is gone; claim_proc remains
    assert _owner(wt, b).verdict == "dead"
    assert wt.q.get(a)["prior_claim_proc"]["pid"] == w.p.pid


def test_hold_review_then_reject_inherits_the_builder(wt, procs):
    w = procs("w1", SID)
    ref = _file(wt, gates=["review"])
    wt.q.claim_by_ref(ref, "w1", session_uuid=SID)
    wt.q.update_status(ref, "closed", "w1", hold_review="review")
    it = wt.q.get(ref)
    assert it["status"] == "in_review" and it["claim_proc"]["pid"] == w.p.pid
    w.kill()
    w.prune()
    wt.q.reject_with(ref, "not yet")
    it = wt.q.get(ref)
    assert it["claim_proc"]["bound"] == "inherited" and it["claim_proc"]["pid"] == w.p.pid
    v = _owner(wt, ref)
    assert v.verdict == "dead" and not it.get("needs_input")


def test_park_carries_proc_and_resume_inherits_it(wt, procs):
    w = procs("w1", SID)
    ref = _file(wt)
    wt.q.claim_by_ref(ref, "w1", session_uuid=SID)
    wt.q.block(ref, session_id="w1", question="q?", origin="worker")
    it = wt.q.get(ref)
    assert "claim_proc" not in it and it["parked"]["proc"]["pid"] == w.p.pid
    wt.q.answer(ref, "yes")
    w.kill()
    bound = wt.q.resume_claim(ref, int(wt.q.get(ref)["pending_answer"]["gen"]))
    assert bound["claim_proc"]["bound"] == "inherited"
    assert _owner(wt, ref).verdict == "dead"


def test_resume_binds_fresh_when_the_parked_worker_lives(wt, procs):
    w = procs("w1", SID)
    ref = _file(wt)
    wt.q.claim_by_ref(ref, "w1", session_uuid=SID)
    wt.q.block(ref, session_id="w1", question="q?", origin="worker")
    wt.q.answer(ref, "yes")
    bound = wt.q.resume_claim(ref, int(wt.q.get(ref)["pending_answer"]["gen"]))
    assert bound["claim_proc"]["bound"] == "record" and bound["claim_proc"]["pid"] == w.p.pid
    assert _owner(wt, ref).verdict == "alive"


def test_release_keeps_the_prior_proc(wt, procs):
    procs("w1", SID)
    ref = _file(wt)
    wt.q.claim_by_ref(ref, "w1", session_uuid=SID)
    wt.q.release(ref)
    it = wt.q.get(ref)
    assert "claim_proc" not in it and it["prior_claim_proc"]["worker_id"] == "w1"


# --------------------------------------------------------------- unproven
def test_ambient_claim_is_never_dead(wt, procs):
    ref = _file(wt)
    wt.q.claim_by_ref(ref, "someone", session_uuid=SID2)
    assert wt.q.get(ref)["claim_proc"]["bound"] == "ambient"
    v = _owner(wt, ref)
    assert v.verdict == "unproven" and "ambient" in v.evidence


def test_legacy_claim_without_proc_is_unproven(wt):
    ref = _file(wt)
    wt.q.claim_by_ref(ref, "w1", session_uuid=SID)
    it = dict(wt.q.get(ref))
    it.pop("claim_proc")
    assert wt.liveness.claim_owner(it).verdict == "unproven"


def test_claude_registry_unreadable_is_unproven(wt, procs, tmp_path):
    w = procs("w1", SID)
    ref = _file(wt)
    wt.q.claim_by_ref(ref, "w1", session_uuid=SID)
    w.kill()
    os.rmdir(tmp_path / "claude-home" / "sessions")
    v = _owner(wt, ref)
    assert v.verdict == "unproven" and "registry unreadable" in v.evidence


def test_claude_registry_alive_wins(wt, procs, monkeypatch):
    w = procs("w1", SID)
    ref = _file(wt)
    wt.q.claim_by_ref(ref, "w1", session_uuid=SID)
    w.kill()
    monkeypatch.setattr(wt.liveness, "_registry_alive", lambda sid: sid == SID)
    assert _owner(wt, ref).source == "claude_registry"


@pytest.mark.parametrize("engine", ["codex", "kimi"])
def test_non_claude_engines_need_ps_and_transcript(wt, procs, monkeypatch, engine):
    w = procs("w1", SID, engine=engine)
    ref = _file(wt)
    wt.q.claim_by_ref(ref, "w1", session_uuid=SID)
    w.kill()
    lv = wt.liveness
    monkeypatch.setattr(lv, "ps_argv", lambda: None)
    v = _owner(wt, ref)
    assert v.verdict == "unproven" and "ps scan failed" in v.evidence
    monkeypatch.setattr(lv, "ps_argv", lambda: [f"{engine} resume {SID}"])
    assert "still names the session" in _owner(wt, ref).evidence
    monkeypatch.setattr(lv, "ps_argv", lambda: ["bash"])
    monkeypatch.setattr(lv, "_transcript_mtime", lambda e, s: 0.0)
    assert "transcript not found" in _owner(wt, ref).evidence
    monkeypatch.setattr(lv, "_transcript_mtime", lambda e, s: time.time() + 60)
    assert "written after" in _owner(wt, ref).evidence
    monkeypatch.setattr(lv, "_transcript_mtime", lambda e, s: time.time() - 600)
    assert _owner(wt, ref).verdict == "dead"


def test_hosted_codex_thread_is_unproven(wt, procs, monkeypatch):
    w = procs("w1", SID, engine="codex")
    ref = _file(wt)
    wt.q.claim_by_ref(ref, "w1", session_uuid=SID)
    w.kill()
    monkeypatch.setattr(wt.q, "_hosted_codex_thread_owns_worker", lambda wid, sid: True)
    v = _owner(wt, ref)
    assert v.verdict == "unproven" and "hosted codex" in v.evidence


def test_exit_file_names_the_death(wt, procs, tmp_path):
    w = procs("w1", SID)
    exit_file = tmp_path / "exit.json"
    exit_file.write_text(json.dumps({"ended_at": _iso(time.time() - 60), "rc": 0,
                                     "signal": 15}))
    data = wt.workers._load()
    data["workers"][-1]["exit_file"] = str(exit_file)
    wt.workers._save(data)
    ref = _file(wt)
    wt.q.claim_by_ref(ref, "w1", session_uuid=SID)
    v = _owner(wt, ref)
    assert v.verdict == "dead" and v.evidence == "killed by signal 15"
    w.kill()

"""OPS-627: exact outcomes preserve spent budgets; cancellation is enforced."""
import json
from pathlib import Path
import subprocess
import sys

import pytest

from watchtower.replay_control import ReplayControl
from tests.test_auto_decision_ungated_github import lane, answer


def approvals(env):
    q = env.queue
    item = q.enqueue(project="T", note="bounded replay")
    ids = [f"{item['ref']}@2026-11-15T05:0{n}:00Z" for n in (1, 2)]
    with q._FileLock(q._lock_path()):
        data = q._load_unlocked()
        current = next(it for it in data["items"] if it["ref"] == item["ref"])
        for decision_id in ids:
            q._append_history(current, "auto_decision", by=q._by("system"),
                              decision_id=decision_id, **{"class": "capped-replay-spend"})
        q._save_unlocked(data)
    return item["ref"], ids


def test_selected_consumption_is_idempotent_without_reversal_or_evidence_deletion(wt_env, tmp_path):
    ref, ids = approvals(wt_env)
    evidence = tmp_path / "shared-output.json"
    evidence.write_text(json.dumps(dict(ticket=ref, httpAttempts=12, capRequests=12)))
    raw = evidence.read_bytes()
    base = ["undo", ref, "--consumed", "--evidence", str(evidence), "--note", "archived first attempt"]
    assert wt_env.cli.main(base) == 1  # two approvals require explicit selection
    assert wt_env.cli.main(base + ["--decision", "unknown"]) == 1
    command = base + ["--decision", ids[0]]
    assert wt_env.cli.main(command) == 0
    assert wt_env.cli.main(command) == 0
    rows = wt_env.queue._auto_decision_rows(wt_env.queue.get(ref))
    assert len(rows[0]["outcomes"]) == 1 and not rows[0]["reversed"]
    assert rows[1]["outcomes"] == [] and not rows[1]["reversed"]
    assert evidence.read_bytes() == raw
    evidence.write_text(json.dumps(dict(ticket=ref, httpAttempts=7, capRequests=12)))
    assert wt_env.cli.main(command) == 1  # reused path cannot replace the original receipt


def test_cancel_is_separate_and_legacy_or_wrong_decision_is_untouched(wt_env, tmp_path):
    ref, ids = approvals(wt_env)
    evidence = tmp_path / "control.json"
    control = ReplayControl(evidence, ids[0])
    control.create(2)
    with control.request():
        pass
    base = ["undo", ref, "--cancel", "--evidence", str(evidence)]
    raw = evidence.read_bytes()
    assert wt_env.cli.main(base + ["--decision", ids[1]]) == 1
    assert evidence.read_bytes() == raw
    assert wt_env.cli.main(base + ["--decision", ids[0]]) == 0
    cancelled = evidence.read_bytes()
    assert wt_env.cli.main(base + ["--decision", ids[0]]) == 0
    assert evidence.read_bytes() == cancelled
    with pytest.raises(ValueError):
        with control.request():
            pytest.fail("cancelled runner forwarded another request")
    with pytest.raises(FileExistsError):
        control.create(2)
    legacy = tmp_path / "legacy.json"
    legacy.write_text(json.dumps(dict(terminal=True, forwarded=10, cap=18)))
    raw = legacy.read_bytes()
    assert wt_env.cli.main(["undo", ref, "--decision", ids[1], "--cancel", "--evidence", str(legacy)]) == 1
    assert legacy.read_bytes() == raw and not (tmp_path / "legacy.json.lock").exists()
    outcomes = wt_env.queue._auto_decision_rows(wt_env.queue.get(ref))[0]["outcomes"]
    assert [h["event"] for h in outcomes] == ["auto_decision_cancelled"]
    assert outcomes[0]["proof"]["requests"] == 1


def test_cancellation_waits_for_inflight_then_refuses_next_request(tmp_path):
    path = tmp_path / "control.json"
    control = ReplayControl(path, "T-1@attempt")
    control.create(3)
    runner = """
import sys
from watchtower.replay_control import ReplayControl
c = ReplayControl(sys.argv[1], 'T-1@attempt')
with c.request():
    print('inflight', flush=True)
    sys.stdin.readline()
print('finished', flush=True)
sys.stdin.readline()
try:
    with c.request():
        print('EXTRA REQUEST', flush=True)
except ValueError:
    print('refused', flush=True)
"""
    cancel = """
import sys, json
from watchtower.replay_control import ReplayControl
print('cancelling', flush=True)
print(json.dumps(ReplayControl(sys.argv[1], 'T-1@attempt').cancel()), flush=True)
"""
    a = subprocess.Popen([sys.executable, "-c", runner, str(path)], stdin=subprocess.PIPE,
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    b = None
    try:
        assert a.stdout.readline().strip() == "inflight"
        b = subprocess.Popen([sys.executable, "-c", cancel, str(path)], stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, text=True)
        assert b.stdout.readline().strip() == "cancelling"
        a.stdin.write("finish request\n"); a.stdin.flush()
        assert a.stdout.readline().strip() == "finished"
        out, err = b.communicate(timeout=10)
        assert b.returncode == 0, err
        proof = json.loads(out)
        assert proof["requests"] == 1 and proof["terminal"] is True
        raw = path.read_bytes()
        a.stdin.write("try next request\n"); a.stdin.flush()
        out, err = a.communicate(timeout=10)
        assert a.returncode == 0 and out.strip() == "refused", err
        control.finish()
        assert path.read_bytes() == raw
    finally:
        for process in (a, b):
            if process and process.poll() is None:
                process.kill(); process.communicate()


@pytest.mark.parametrize("sidecar", [False, True])
def test_github_outcome_is_durable_idempotent_and_failed_write_is_visible(lane, sidecar, monkeypatch):
    q, ref, _ = lane
    result, _ = answer(lane, sidecar)
    assert result
    decision_id = next(h["decision_id"] for h in q.get(ref)["history"] if h["event"] == "auto_decision")
    from watchtower.decision_undo import record_outcome
    fields = dict(proof={"requests": 12, "sha256": "preserved"}, note="archived attempt", worker="test")
    for _ in range(2):
        fresh = record_outcome(ref, decision_id, "auto_decision_consumed", **fields)
    fresh = record_outcome(ref, decision_id, "auto_decision_consumed", **{**fields, "worker": "replacement"})
    events = [h for h in fresh["history"] if h["event"] == "auto_decision_consumed"]
    assert len(events) == 1 and events[0]["proof"] == fields["proof"]
    assert events[0]["worker"] == "test"
    backend = q._github_backend_for_project("GHI")
    remote = backend.get(ref)
    assert any(h["event"] == "auto_decision_consumed" for h in remote["history"])
    assert not q._auto_decision_rows(fresh)[0]["reversed"]
    with pytest.raises(ValueError, match="conflicting"):
        record_outcome(ref, decision_id, "auto_decision_consumed", **{**fields, "note": "different"})
    original = type(backend).update
    def fail(self, ident, **kw):
        if "_system_history" in kw:
            raise RuntimeError("injected receipt failure")
        return original(self, ident, **kw)
    monkeypatch.setattr(type(backend), "update", fail)
    with pytest.raises(ValueError, match="NOT persisted"):
        record_outcome(ref, decision_id, "auto_decision_cancelled", **fields)
    assert not any(h["event"] == "auto_decision_cancelled" for h in q.get(ref)["history"])

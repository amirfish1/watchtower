"""WT-34: a sent-back (verifier-rejected) claim is handed back before any new
ticket, ages out to the pool with its rejection text, and the failed-gate
close writeback is ownership-checked."""

from __future__ import annotations

import importlib
import time

import pytest

S = "803f761a-1111-2222-3333-444444444444"
W = "wt-w"


@pytest.fixture()
def wt(tmp_path, monkeypatch):
    for env, name in (
        ("WATCHTOWER_STORE", "queue.json"), ("WATCHTOWER_WORKERS_FILE", "workers.json"),
        ("WATCHTOWER_CONFIG_FILE", "config.json"),
        ("WATCHTOWER_WORKER_SESSIONS_FILE", "worker-sessions.json"),
        ("WATCHTOWER_WORKER_IDS_FILE", "worker-ids.json"),
        ("WATCHTOWER_ACTIVITY_LOG", "activity.log"),
        ("WATCHTOWER_OUTBOX_FILE", "outbox.json"),
        ("WATCHTOWER_STOP_SIGNALS_DIR", "stop-signals"),
    ):
        monkeypatch.setenv(env, str(tmp_path / name))
    monkeypatch.setenv("WATCHTOWER_DELEGATE_URL", "off")
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude-home"))
    import watchtower.queue as q
    import watchtower.config as config
    import watchtower.workers as workers
    importlib.reload(q)
    importlib.reload(config)
    importlib.reload(workers)
    monkeypatch.setattr(q, "_notify_review", lambda *a, **k: None)
    monkeypatch.setattr(q, "_notify_ticket_event", lambda *a, **k: None)

    class Ns:
        pass

    ns = Ns()
    ns.q, ns.config, ns.workers = q, config, workers
    return ns


def _sent_back(wt, why="verifier failed: add a test", **kw):
    """A ticket W built, then sent back (reject_with re-binds it to W)."""
    q = wt.q
    item = q.enqueue(project="Q", note="work", source="test", **kw)
    data = q._load_unlocked()
    for it in data["items"]:
        if it["ref"] == item["ref"]:
            it.update(status="in_review", claimed_by=W, claimed_session_id=S,
                      claimed_at="2000-01-01T00:00:00Z")
    q._save_unlocked(data)
    return q.reject_with(item["ref"], why, by_label="verifier")["ref"]


def _set(wt, ref, **fields):
    data = wt.q._load_unlocked()
    for it in data["items"]:
        if it["ref"] == ref:
            it.update(fields)
    wt.q._save_unlocked(data)


def _deadline(wt, ref, minutes=30):
    return wt.q._sent_back_deadline(wt.q.get(ref), minutes)


# --- marker ------------------------------------------------------------------

def test_reject_writes_marker_and_reason(wt):
    ref = _sent_back(wt)
    sb = wt.q.get(ref)["sent_back"]
    assert sb["worker_id"] == W and sb["session_id"] == S
    assert sb["reason"].startswith("verifier failed") and sb["by"] == "verifier"
    assert sb["hist_from"] == len(wt.q.get(ref)["history"])
    assert sb["handed_back_at"] is None and sb["progress_at"] is None


def test_reopen_resume_marker_uses_resume_text(wt):
    q = wt.q
    it = q.enqueue(project="Q", note="w", source="test")
    _set(wt, it["ref"], status="closed", claimed_by=W, claimed_session_id=S)
    q.reopen_and_claim(it["ref"], W, session_uuid=S, reason="", sent_back_reason="client says no",
                       sent_back_by="client")
    assert q.get(it["ref"])["sent_back"]["reason"] == "client says no"


# --- claim gate --------------------------------------------------------------

def test_claim_next_hands_back_before_anything_new(wt):
    q = wt.q
    ref = _sent_back(wt)
    other = q.enqueue(project="Q", note="new", source="test")
    got = q.claim_next(W, project="Q", session_uuid=S)
    assert got["ref"] == ref and got["handed_back"] is True
    assert q.get(other["ref"])["status"] == "open"
    again = q.claim_next(W, project="Q", session_uuid=S)   # idempotent
    assert again["ref"] == ref
    it = q.get(ref)
    assert [h["event"] for h in it["history"]].count("handback") == 1
    assert it["sent_back"]["handed_back_at"]


def test_hand_back_matches_session_under_another_alias(wt):
    q = wt.q
    ref = _sent_back(wt)
    q.enqueue(project="Q", note="new", source="test")
    got = q.claim_next("some-alias", project="Q", session_uuid=S)
    assert got["ref"] == ref


def test_claim_by_ref_other_ticket_refused_with_reason(wt):
    q = wt.q
    ref = _sent_back(wt)
    other = q.enqueue(project="Q", note="new", source="test")
    with pytest.raises(ValueError) as e:
        q.claim_by_ref(other["ref"], W, session_uuid=S)
    msg = str(e.value)
    assert ref in msg and "verifier failed: add a test" in msg and "wt close" in msg
    assert q.claim_by_ref(ref, W, session_uuid=S)["handed_back"] is True
    assert q.get(other["ref"])["status"] == "open"


def test_unrelated_worker_claims_normally(wt):
    q = wt.q
    _sent_back(wt)
    other = q.enqueue(project="Q", note="new", source="test")
    assert q.claim_next("wt-other", project="Q")["ref"] == other["ref"]


def test_marker_cleared_when_ticket_closes(wt):
    q = wt.q
    ref = _sent_back(wt)
    q.close(ref, session_id=W, session_uuid=S, resolution={"summary": "fixed"})
    assert "sent_back" not in q.get(ref)


def test_block_clears_marker_and_parks(wt):
    q = wt.q
    ref = _sent_back(wt)
    q.block(ref, session_id=W, question="which way?", origin="worker")
    it = q.get(ref)
    assert "sent_back" not in it and it["status"] != "in_progress"
    assert q.sent_back_claims("Q") == []


# --- age bound ---------------------------------------------------------------

def test_no_progress_releases_with_rejection_text(wt):
    q = wt.q
    ref = _sent_back(wt)
    dl = _deadline(wt, ref)
    assert q.release_stalled_sent_back(now=dl - 1) == []
    rel = q.release_stalled_sent_back(now=dl + 1)
    assert [r["ref"] for r in rel] == [ref]
    it = q.get(ref)
    assert it["status"] == "open" and it["claimed_by"] is None
    assert "sent_back" not in it and "resume" not in it
    assert it["gate_feedback"].startswith("verifier failed")
    r = it["sent_back_released"]
    assert r["worker_id"] == W and r["session_id"] == S and r["reason"].startswith("verifier failed")
    events = [h["event"] for h in it["history"]]
    assert "sent_back_release" in events
    assert "verifier failed" in it["history"][-1]["text"]


def test_progress_comment_stops_the_clock(wt):
    q = wt.q
    ref = _sent_back(wt)
    q.comment(ref, "on it", by="worker", session_id=W)
    assert q.release_stalled_sent_back(now=_deadline(wt, ref) + 600) == []
    it = q.get(ref)
    assert it["status"] == "in_progress" and it["sent_back"]["progress_at"]
    assert q.sent_back_claims("Q") == []


def test_foreign_comment_is_not_progress(wt):
    q = wt.q
    ref = _sent_back(wt)
    q.comment(ref, "reconciler nudge", by="system")
    q.comment(ref, "someone else", by="worker", session_id="wt-other")
    assert [i["ref"] for i in q.sent_back_claims("Q")] == [ref]
    assert len(q.release_stalled_sent_back(now=_deadline(wt, ref) + 1)) == 1


def test_hand_back_resets_the_clock_once(wt):
    q = wt.q
    ref = _sent_back(wt)
    _set(wt, ref, sent_back=dict(q.get(ref)["sent_back"], at="2000-01-01T00:00:00Z"))
    q.claim_next(W, project="Q", session_uuid=S)   # hand back now: clock restarts
    stamp = q.get(ref)["sent_back"]["handed_back_at"]
    assert q.release_stalled_sent_back(now=time.time()) == []
    q.claim_next(W, project="Q", session_uuid=S)
    assert q.get(ref)["sent_back"]["handed_back_at"] == stamp


def test_failed_resume_shortens_deadline(wt):
    q = wt.q
    ref = _sent_back(wt)
    assert q.set_resume_state(ref, S, "failed", error="boom")
    it = q.get(ref)
    assert q._sent_back_deadline(it, 30) <= q._iso_ts(it["resume"]["at"]) + 300 + 1


def test_zero_minutes_disables_release(wt):
    wt.config.set_sent_back_release_min("Q", 0)
    ref = _sent_back(wt)
    assert wt.q.release_stalled_sent_back(now=time.time() + 10 ** 7) == []
    assert wt.q.get(ref)["status"] == "in_progress"


def test_configured_minutes_apply(wt):
    wt.config.set_sent_back_release_min("Q", 5)
    ref = _sent_back(wt)
    assert wt.q.release_stalled_sent_back(now=time.time() + 6 * 60) != []
    assert wt.q.get(ref)["status"] == "open"


def test_released_ticket_sorts_ahead_of_same_priority_peers(wt):
    q = wt.q
    older = q.enqueue(project="Q", note="old", source="test")
    ref = _sent_back(wt)
    q.release_stalled_sent_back(now=_deadline(wt, ref) + 1)
    got = q.claim_next("wt-next", project="Q")
    assert got["ref"] == ref and got["ref"] != older["ref"]
    assert got["gate_feedback"].startswith("verifier failed")


def test_release_races_with_a_concurrent_close(wt):
    q = wt.q
    ref = _sent_back(wt)
    q.close(ref, session_id=W, session_uuid=S, resolution={"summary": "done"})
    assert q.release_stalled_sent_back(now=time.time() + 10 ** 7) == []
    assert q.get(ref)["status"] == "closed"


# --- failed-gate close guard (D3a) ------------------------------------------

def _gated_sent_back(wt):
    ref = _sent_back(wt, gates=["cmd:exit 1"])
    return ref, _deadline(wt, ref) + 1


def test_late_close_by_released_holder_is_refused_and_writes_nothing(wt):
    q = wt.q
    ref, late = _gated_sent_back(wt)
    q.release_stalled_sent_back(now=late)
    before = q.get(ref)
    with pytest.raises(ValueError, match="released to the pool"):
        q.close(ref, session_id=W, session_uuid=S, resolution={"summary": "x"})
    with pytest.raises(ValueError, match="released to the pool"):
        q.close(ref, session_id="other-alias", session_uuid=S, resolution={"summary": "x"})
    after = q.get(ref)
    assert after["status"] == "open" and after["claimed_by"] is None
    assert after["sent_back_released"] == before["sent_back_released"]
    assert after["gate_feedback"] == before["gate_feedback"]
    assert len(after["history"]) == len(before["history"])


def test_late_close_after_replacement_claim_keeps_replacement(wt):
    q = wt.q
    ref, late = _gated_sent_back(wt)
    q.release_stalled_sent_back(now=late)
    assert q.claim_by_ref(ref, "wt-x")["status"] == "in_progress"
    with pytest.raises(ValueError, match="claimed by wt-x"):
        q.close(ref, session_id=W, session_uuid=S, resolution={"summary": "x"})
    it = q.get(ref)
    assert it["status"] == "in_progress" and it["claimed_by"] == "wt-x"
    # the rightful holder's own failing close still reopens normally
    out = q.close(ref, session_id="wt-x", resolution={"summary": "x"})
    assert out["status"] == "open" and "gate cmd:exit 1 failed" in out["gate_feedback"]


def test_replacement_claim_during_gate_evaluation(wt, monkeypatch):
    q = wt.q
    ref, late = _gated_sent_back(wt)
    real = q.evaluate_gates

    def racing(item, commit=""):
        results, stages = real(item, commit)
        q.release_stalled_sent_back(now=late)
        q.claim_by_ref(ref, "wt-x")
        return results, stages

    monkeypatch.setattr(q, "evaluate_gates", racing)
    with pytest.raises(ValueError):
        q.close(ref, session_id=W, session_uuid=S, resolution={"summary": "x"})
    it = q.get(ref)
    assert it["status"] == "in_progress" and it["claimed_by"] == "wt-x"
    assert not it.get("gate_feedback", "").startswith("gate")


def test_claim_between_gates_and_writeback_on_open_ticket(wt, monkeypatch):
    q = wt.q
    item = q.enqueue(project="Q", note="w", source="test", gates=["cmd:exit 1"])
    real = q.evaluate_gates

    def racing(it, commit=""):
        out = real(it, commit)
        q.claim_by_ref(item["ref"], "wt-x")
        return out

    monkeypatch.setattr(q, "evaluate_gates", racing)
    with pytest.raises(ValueError, match="changed while its gates ran"):
        q.close(item["ref"], session_id=W, resolution={"summary": "x"})
    assert q.get(item["ref"])["claimed_by"] == "wt-x"


def test_human_and_forced_closes_are_preserved(wt):
    q = wt.q
    ref, late = _gated_sent_back(wt)
    q.release_stalled_sent_back(now=late)
    out = q.close(ref)                      # human: no session id
    assert out["status"] == "open" and out["gate_feedback"].startswith("gate cmd:exit 1")
    out = q.close(ref, force=True, session_id=W, resolution={"summary": "f"})
    assert out["status"] == "closed"


def test_failing_gate_close_by_foreign_worker_cannot_reopen_live_claim(wt):
    q = wt.q
    item = q.enqueue(project="Q", note="w", source="test", gates=["cmd:exit 1"])
    q.claim_by_ref(item["ref"], "wt-y")
    with pytest.raises(ValueError, match="claimed by wt-y"):
        q.close(item["ref"], session_id=W, resolution={"summary": "x"})
    assert q.get(item["ref"])["claimed_by"] == "wt-y"


def test_failing_gate_close_cannot_touch_parked_ticket_of_another_worker(wt):
    q = wt.q
    item = q.enqueue(project="Q", note="w", source="test", gates=["cmd:exit 1"])
    q.claim_by_ref(item["ref"], "wt-y")
    q.block(item["ref"], session_id="wt-y", question="?", origin="worker")
    assert q.get(item["ref"])["status"] == q.PARKED_STATUS
    with pytest.raises(ValueError, match="parked by wt-y"):
        q.close(item["ref"], session_id=W, resolution={"summary": "x"})
    assert q.get(item["ref"])["status"] == q.PARKED_STATUS


# --- nudge / reconcile wiring -----------------------------------------------

def test_targeted_nudge_names_ticket_and_age(wt, monkeypatch):
    ref = _sent_back(wt)
    sent = []
    monkeypatch.setattr(wt.workers, "notify_workers",
                        lambda queue, text, **kw: sent.append((text, kw)) or 1)
    out = wt.workers._nudge_sent_back_holders("Q")
    assert out == {W}
    text, kw = sent[0]
    assert ref in text and "sent back to you" in text and kw["only"] == {W}


def test_release_notifies_former_holder(wt, monkeypatch):
    ref = _sent_back(wt)
    _set(wt, ref, sent_back=dict(wt.q.get(ref)["sent_back"], at="2000-01-01T00:00:00Z"))
    sent = []
    monkeypatch.setattr(wt.workers, "notify_workers",
                        lambda queue, text, **kw: sent.append((text, kw)) or 1)
    assert [i["ref"] for i in wt.workers._release_stalled_sent_back()] == [ref]
    assert ref in sent[0][0] and f"wt claim {ref}" in sent[0][0] and sent[0][1]["only"] == {W}


def test_release_clears_session_ownership_and_isolates_replacement(wt):
    q = wt.q
    ref = _sent_back(wt)
    q.release_stalled_sent_back(now=_deadline(wt, ref) + 1)
    it = q.get(ref)
    assert it["status"] == "open" and not it.get("claimed_session_id")
    assert it["sent_back_released"]["session_id"] == S
    # replacement X claims without a session uuid: it must not inherit S
    x = q.claim_by_ref(ref, "wt-x")
    assert x["claimed_by"] == "wt-x" and not x.get("claimed_session_id")
    # X's close is rejected: the send-back binds to X, never to the old holder
    data = q._load_unlocked()
    for i in data["items"]:
        if i["ref"] == ref:
            i["status"] = "in_review"
    q._save_unlocked(data)
    q.reject_with(ref, "second rejection", by_label="verifier")
    again = q.get(ref)
    assert again.get("claimed_session_id") != S
    assert not (again.get("resume") or {}).get("sid") == S
    assert again["claimed_by"] in (None, "wt-x")
    # the former holder under an alias of S is not handed X's ticket
    out = q.claim_next(W, project="Q", session_uuid=S)
    assert not (out or {}).get("handed_back")

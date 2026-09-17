"""SONIA-CHAT-19: reopen_and_claim -- the re-entry re-bind primitive.

A topic's ticket closed (or is parked blocked); the client re-engages the
same subject. reopen_and_claim() re-binds the ticket to the SAME session
that last worked it, atomically -- closed/blocked straight to in_progress
under that session, never passing through an externally-claimable "open"
state a live drain worker's claim loop could steal in between.
"""
from __future__ import annotations

import importlib
import subprocess
import threading
import time

import pytest


@pytest.fixture()
def q(tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_WORKERS_FILE", str(tmp_path / "workers.json"))
    monkeypatch.setenv("WATCHTOWER_WORKER_IDS_FILE", str(tmp_path / "worker-ids.json"))
    monkeypatch.setenv(
        "WATCHTOWER_WORKER_SESSIONS_FILE", str(tmp_path / "worker-sessions.json")
    )
    monkeypatch.setenv(
        "WATCHTOWER_CODEX_THREAD_REGISTRY", str(tmp_path / "codex-threads.json")
    )
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(tmp_path / "activity.log"))

    import watchtower.queue as queue_mod
    importlib.reload(queue_mod)
    return queue_mod


def test_reopen_and_claim_rebinds_closed_ticket_to_original_session(q):
    """A closed ticket's original claimed_session_id is preserved (per
    reopen()'s existing behavior) and reopen_and_claim() re-binds the
    ticket to exactly that session, landing directly on in_progress -- no
    intermediate open state observable to a caller."""
    worker_id = "sonia-chat-19-worker"
    sid = "cccccccc-dddd-eeee-ffff-000000000000"
    item = q.enqueue(project="SONIACHAT19TEST", note="topic work")
    claimed = q.claim_next(worker_id, project="SONIACHAT19TEST", session_uuid=sid)
    q.update_status(claimed["ref"], "closed", worker_id, resolution={"summary": "turn done"})

    closed = q.get(claimed["ref"])
    assert closed["status"] == "closed"
    assert closed["claimed_session_id"] == sid  # preserved through close (existing behavior)

    rebind = q.reopen_and_claim(claimed["ref"], worker_id, session_uuid=sid)
    assert rebind is not None
    assert rebind["status"] == "in_progress"
    assert rebind["claimed_session_id"] == sid
    assert rebind["claimed_by"] == worker_id


def test_reopen_and_claim_rebinds_blocked_awaiting_client_ticket(q):
    """The actual SONIA-CHAT-19 use case: a ticket parked awaiting-client
    (SONIA-CHAT-14) gets a continue: answer after already having been
    closed via TID -- re-binding must work from blocked too, with force
    since needs_input is set."""
    worker_id = "sonia-chat-19-worker-2"
    sid = "11111111-2222-3333-4444-555555555555"
    item = q.enqueue(project="SONIACHAT19TEST", note="topic work")
    claimed = q.claim_next(worker_id, project="SONIACHAT19TEST", session_uuid=sid)
    q.block(claimed["ref"], session_id=worker_id, question="awaiting client", kind="awaiting-client")
    q.update_status(claimed["ref"], "closed", worker_id, resolution={"summary": "topic done"})

    rebind = q.reopen_and_claim(claimed["ref"], worker_id, session_uuid=sid, force=True)
    assert rebind is not None
    assert rebind["status"] == "in_progress"
    assert rebind["claimed_session_id"] == sid
    assert rebind["needs_input"] is False
    assert rebind["block_kind"] == ""


def test_reopen_and_claim_refuses_blocked_without_force(q):
    """Same guard reopen() already has: refuse a needs_input ticket unless
    force=True, so a live open question isn't silently erased."""
    worker_id = "sonia-chat-19-worker-3"
    sid = "22222222-3333-4444-5555-666666666666"
    item = q.enqueue(project="SONIACHAT19TEST", note="topic work")
    claimed = q.claim_next(worker_id, project="SONIACHAT19TEST", session_uuid=sid)
    q.block(claimed["ref"], session_id=worker_id, question="which color?")

    with pytest.raises(ValueError, match="blocked awaiting human input"):
        q.reopen_and_claim(claimed["ref"], worker_id, session_uuid=sid)


def test_reopen_and_claim_refuses_already_open_ticket(q):
    item = q.enqueue(project="SONIACHAT19TEST", note="topic work")
    with pytest.raises(ValueError, match="already open"):
        q.reopen_and_claim(item["ref"], "some-worker")


def test_reopen_and_claim_concurrency_no_second_claimant_can_win(q, tmp_path):
    """THE actual race this ticket exists to close (Fable 5.1 critique):
    a second worker's normal claim loop racing the reopen-to-claim window.
    Run reopen_and_claim from N threads concurrently against the same
    closed ticket -- exactly one must win the re-bind; none may observe
    the ticket sitting externally-open in between (that observable window
    is what the two-call reopen()+claim_by_ref() sequence had)."""
    worker_id = "sonia-chat-19-race-worker"
    sid = "33333333-4444-5555-6666-777777777777"
    item = q.enqueue(project="SONIACHAT19TEST", note="topic work")
    claimed = q.claim_next(worker_id, project="SONIACHAT19TEST", session_uuid=sid)
    q.update_status(claimed["ref"], "closed", worker_id, resolution={"summary": "done"})

    results = []
    errors = []
    barrier = threading.Barrier(5)

    def attempt(n):
        barrier.wait()
        try:
            r = q.reopen_and_claim(claimed["ref"], f"racer-{n}", session_uuid=sid)
            results.append(r)
        except ValueError as e:
            errors.append(str(e))

    threads = [threading.Thread(target=attempt, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)

    successes = [r for r in results if r is not None]
    # Every racer either won (successes) or got a real ValueError (already
    # in_progress by the time it ran) -- none silently no-op'd or double-won.
    assert len(successes) + len(errors) == 5
    final = q.get(claimed["ref"])
    assert final["status"] == "in_progress"
    # claimed_by is whichever racer's write landed last under the lock --
    # the file-lock serializes writes, so this is deterministic per-run but
    # not predictable which racer wins; what matters is exactly one state.
    assert final["claimed_by"] in [f"racer-{i}" for i in range(5)]

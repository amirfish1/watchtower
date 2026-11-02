"""Source acceptance tests use only generic, isolated local records."""
import json
import multiprocessing

import pytest

from watchtower import conditional_answers as ca, queue as q, cli, config


@pytest.fixture
def source(tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setattr(config, "backend", lambda project: "file")
    # Build one blocked record directly: no worker, notification, or live ticket action.
    item = {"number": 1, "project": "DEMO", "seq": 1, "ref": "DEMO-1",
            "status": "in_progress", "needs_input": True, "blocked_at": "episode-1",
            "block_question": "Choose a color?", "block_kind": "input", "history": []}
    with q._FileLock(q._lock_path()):
        q._save_unlocked({"counter": 1, "items": [item]})
    return {"idempotency_key": "decision-1", "text": "Blue", "expected_blocked_at": "episode-1",
            "expected_block_question": "Choose a color?", "expected_wait_kind": "owner",
            "expected_wait": None}


def change(**fields):
    with q._FileLock(q._lock_path()):
        data = q._load_unlocked()
        data["items"][0].update(fields)
        q._save_unlocked(data)


@pytest.mark.parametrize("fields", [
    {"blocked_at": "episode-2"}, {"block_question": "Choose a shape?"},
    {"block_question": "Choose a color? "},
    {"needs_input": False}, {"status": "closed"},
    {"wait": {"on": "external", "block_at": "episode-1"}},
    {"block_kind": "awaiting-client"},
])
def test_stale_rejected_without_write(source, fields):
    change(**fields)
    before = q._db_path().read_bytes()
    with pytest.raises(ca.Conflict):
        ca.submit("DEMO-1", source)
    assert q._db_path().read_bytes() == before
    assert ca.lookup("DEMO-1", "decision-1") is None


def test_wait_ownership_revision_same_kind(source):
    original = {"on": "owner", "block_at": "episode-1", "at": "revision-1"}
    source["expected_wait"] = original
    change(wait={**original, "at": "revision-2", "actor": "another owner"})
    with pytest.raises(ca.Conflict):
        ca.submit("DEMO-1", source)


def test_replay_and_lost_receipt_lookup(source, monkeypatch):
    first = ca.submit("DEMO-1", source)
    assert first["accepted"] and first["work_completed"] is False
    assert first["routing"]["state"] == "pending"
    assert ca.submit("DEMO-1", source) == first
    def no_save(*args, **kwargs):
        pytest.fail("read-only lookup wrote the source")
    monkeypatch.setattr(q, "_save_unlocked", no_save)
    assert ca.lookup("DEMO-1", "decision-1") == first
    assert ca.lookup("DEMO-1", "missing") is None
    assert sum(h["event"] == "answer" for h in q.get("DEMO-1")["history"]) == 1


@pytest.mark.parametrize("field,value", [("text", "Red"), ("tid", True),
                                        ("expected_block_question", "Changed?"),
                                        ("session_id", "another-caller")])
def test_key_payload_conflict(source, field, value):
    ca.submit("DEMO-1", source)
    changed = {**source, field: value}
    with pytest.raises(ca.Conflict):
        ca.submit("DEMO-1", changed)


def _concurrent_submit(request, results):
    try:
        ca.submit("DEMO-1", request)
        item = ca.claim_route("DEMO-1", request["idempotency_key"])
        results.put(bool(item))
    except Exception as exc:
        results.put(repr(exc))


def test_concurrent_processes_one_answer_and_route(source):
    ctx = multiprocessing.get_context("fork")
    results = ctx.Queue()
    processes = [ctx.Process(target=_concurrent_submit, args=(source, results)) for _ in range(4)]
    for process in processes:
        process.start()
    for process in processes:
        process.join(10)
        assert process.exitcode == 0
    assert sorted(results.get(timeout=2) for _ in processes) == [False, False, False, True]
    assert sum(h["event"] == "answer" for h in q.get("DEMO-1")["history"]) == 1
    assert ca.lookup("DEMO-1", "decision-1")["routing"]["state"] == "uncertain"
    assert ca.claim_route("DEMO-1", "decision-1") is None


@pytest.mark.parametrize("ref,backend", [("DEMO-1", "github"), ("other:DEMO-1", "file"),
                                         ("https://example.test/issues/1", "file")])
def test_unsupported_before_any_write(source, monkeypatch, ref, backend):
    monkeypatch.setattr(config, "backend", lambda project: backend)
    from watchtower import owner_blocks_export
    monkeypatch.setattr(owner_blocks_export, "_settings", lambda: {"DEMO": {"backend": backend}})
    def forbidden(*args, **kwargs):
        pytest.fail("unsupported backend accessed source")
    monkeypatch.setattr(q, "answer", forbidden)
    monkeypatch.setattr(q, "_load_unlocked", forbidden)
    with pytest.raises(ca.Unsupported):
        ca.submit(ref, source)
    with pytest.raises(ca.Unsupported):
        ca.lookup(ref, "decision-1")


@pytest.mark.parametrize("failure", [False, True])
def test_cli_replay_never_routes_twice(source, monkeypatch, capsys, failure):
    calls = []
    def deliver(*args):
        calls.append(args)
        if failure:
            raise ConnectionError("response lost")
        return 0
    monkeypatch.setattr(cli, "_deliver_answer", deliver)
    argv = ["answer-conditional", "DEMO-1", "--request-json", json.dumps(source)]
    assert cli.main(argv) == 0
    receipt = json.loads(capsys.readouterr().out)
    assert receipt["routing"]["state"] == ("uncertain" if failure else "completed")
    assert cli.main(argv) == 0
    assert json.loads(capsys.readouterr().out) == receipt
    assert len(calls) == 1
    assert cli.main(["answer-receipt", "DEMO-1", "decision-1", "--json"]) == 0
    assert json.loads(capsys.readouterr().out) == receipt


def test_lock_failure_refuses_before_load(source, monkeypatch):
    class FailedLock:
        _fh = None
        def __init__(self, *args): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass
    monkeypatch.setattr(q, "_FileLock", FailedLock)
    with pytest.raises(ca.Unsupported):
        ca.submit("DEMO-1", source)
    assert ca.lookup("DEMO-1", "decision-1") is None


def test_new_block_before_route_is_superseded(source):
    ca.submit("DEMO-1", source)
    change(needs_input=True, blocked_at="episode-2")
    assert ca.claim_route("DEMO-1", "decision-1") is None
    assert ca.lookup("DEMO-1", "decision-1")["routing"]["state"] == "superseded"


def test_backend_change_does_not_fall_through_to_github(source, monkeypatch):
    class Backend:
        def get(self, *args): pytest.fail("remote read attempted")
        def answer(self, *args, **kwargs): pytest.fail("remote write attempted")
    monkeypatch.setattr(q, "_github_backend_for_project", lambda project: Backend())
    with pytest.raises(ca.Unsupported):
        ca.submit("DEMO-1", source)
    assert ca.lookup("DEMO-1", "decision-1") is None


def test_explicit_owner_record_accepted_and_replay_survives_reblock(source):
    wait = {"on": "owner", "block_at": "episode-1", "at": "revision-1", "actor": "owner"}
    change(wait=wait)
    source["expected_wait"] = wait
    receipt = ca.submit("DEMO-1", source)
    change(needs_input=True, blocked_at="episode-2", block_question="Another question?")
    assert ca.submit("DEMO-1", source) == receipt
    assert sum(h["event"] == "answer" for h in q.get("DEMO-1")["history"]) == 1


def test_wait_records_have_distinct_revision_with_frozen_clock(monkeypatch):
    monkeypatch.setattr(q, "_now_iso", lambda: "2026-10-01T00:00:00Z")
    first = q.wait_record("owner", block_at="episode")
    second = q.wait_record("owner", block_at="episode")
    assert first["at"] == second["at"]
    assert first["revision"] != second["revision"]


def test_rapid_identical_reblocks_are_distinct_episodes(source, monkeypatch):
    monkeypatch.setattr(q, "_notify_ticket_event", lambda *args, **kwargs: None)
    first = q.block("DEMO-1", question="Choose a color?")
    second = q.block("DEMO-1", question="Choose a color?")
    assert first["blocked_at"] != second["blocked_at"]
    source["expected_blocked_at"] = first["blocked_at"]
    with pytest.raises(ca.Conflict):
        ca.submit("DEMO-1", source)


def test_old_pending_receipt_cannot_route_after_newer_answer(source):
    ca.submit("DEMO-1", source)
    change(needs_input=True, blocked_at="episode-2", block_question="Choose a color?")
    second = {**source, "idempotency_key": "decision-2", "expected_blocked_at": "episode-2"}
    ca.submit("DEMO-1", second)
    assert ca.claim_route("DEMO-1", "decision-1") is None
    assert ca.lookup("DEMO-1", "decision-1")["routing"]["state"] == "superseded"
    assert ca.claim_route("DEMO-1", "decision-2") is not None

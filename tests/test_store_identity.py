"""Durable source fences use only isolated generic stores."""
import copy
import json
import uuid

import pytest

from watchtower import cli, conditional_answers as ca, owner_blocks_export as export, queue as q, store_identity


@pytest.fixture
def source(tmp_path, monkeypatch):
    base = tmp_path / "a.json"
    monkeypatch.setenv("WATCHTOWER_STORE", str(base))
    settings = tmp_path / "settings.json"
    settings.write_text('{}')
    monkeypatch.setenv("WATCHTOWER_CONFIG_FILE", str(settings))
    item = {"number": 1, "project": "DEMO", "seq": 1, "ref": "DEMO-1",
            "status": "in_progress", "needs_input": True, "blocked_at": "episode-1",
            "block_question": "Choose a color?", "block_kind": "input", "history": []}
    with q._FileLock(q._lock_path()):
        q._save_unlocked({"counter": 1, "items": [item]})
    request = {"idempotency_key": "decision-1", "text": "Blue", "expected_blocked_at": "episode-1",
               "expected_block_question": "Choose a color?", "expected_wait_kind": "owner", "expected_wait": None}
    return base, request


def test_new_identity_random_stable_and_exported(source):
    base, _ = source
    first = export.snapshot()
    identity = first["scope"]["store_id"]
    assert uuid.UUID(identity).version == 4
    assert first["items"][0]["expected_store_id"] == identity
    with q._FileLock(q._lock_path()):
        q._save_unlocked(q._load_unlocked())
    assert export.snapshot()["scope"]["store_id"] == identity
    moved = base.with_name("moved.json")
    base.with_suffix(".db").rename(moved.with_suffix(".db"))
    base.with_suffix(".lock").rename(moved.with_suffix(".lock"))
    with pytest.MonkeyPatch.context() as patch:
        patch.setenv("WATCHTOWER_STORE", str(moved))
        assert export.snapshot()["scope"]["store_id"] == identity


@pytest.mark.parametrize("copied_receipt", [False, True])
def test_store_switch_rejects_before_answer_or_receipt(source, monkeypatch, copied_receipt):
    base, request = source
    identity_a = export.snapshot()["scope"]["store_id"]
    request["expected_store_id"] = identity_a
    if copied_receipt:
        ca.submit("DEMO-1", request)
    data = q._load_unlocked()
    base_b = base.with_name("b.json")
    monkeypatch.setenv("WATCHTOWER_STORE", str(base_b))
    with q._FileLock(q._lock_path()):
        q._save_unlocked(data)
    assert export.snapshot()["scope"]["store_id"] != identity_a
    before = q._db_path().read_bytes()
    with pytest.raises(ca.Conflict, match="store identity"):
        ca.submit("DEMO-1", request)
    with pytest.raises(ca.Conflict, match="store identity"):
        ca.lookup("DEMO-1", request["idempotency_key"], identity_a)
    with pytest.raises(ca.Conflict, match="store identity"):
        ca.claim_route("DEMO-1", request["idempotency_key"], identity_a)
    assert q._db_path().read_bytes() == before


def test_matching_store_and_default_null_replay_compatibility(source):
    _, request = source
    identity = export.snapshot()["scope"]["store_id"]
    request["expected_store_id"] = identity
    first = ca.submit("DEMO-1", request)
    assert ca.submit("DEMO-1", request) == first
    assert ca.lookup("DEMO-1", request["idempotency_key"], identity) == first
    assert first["request"]["expected_store_id"] == identity
    assert ca.payload({k: v for k, v in request.items() if k != "expected_store_id"})["expected_store_id"] is None


def test_old_receipt_without_identity_field_still_replays(source):
    _, request = source
    ca.submit("DEMO-1", request)
    with q._FileLock(q._lock_path()):
        data = q._load_unlocked()
        receipt = data["items"][0]["answer_receipts"][request["idempotency_key"]]
        receipt["request"].pop("expected_store_id")
        original = copy.deepcopy(receipt)
        q._save_unlocked(data)
    assert ca.submit("DEMO-1", {**request, "expected_store_id": None}) == original


def test_expected_identity_checked_under_actual_source_lock(source, monkeypatch):
    base, request = source
    request["expected_store_id"] = export.snapshot()["scope"]["store_id"]
    original = store_identity.read_unlocked
    def checked(path):
        with base.with_suffix(".lock").open("r") as other:
            with pytest.raises(BlockingIOError):
                q.fcntl.flock(other.fileno(), q.fcntl.LOCK_EX | q.fcntl.LOCK_NB)
        return original(path)
    monkeypatch.setattr(store_identity, "read_unlocked", checked)
    assert ca.submit("DEMO-1", request)["accepted"]


def test_existing_store_read_only_export_refuses_until_explicit_init(source, capsys):
    _, _ = source
    connection = q._connect(q._db_path())
    try:
        connection.execute("DELETE FROM meta WHERE key='store_id'")
        connection.commit()
    finally:
        connection.close()
    before = q._db_path().read_bytes()
    assert cli.main(["owner-blocks-export", "--json"]) == 2
    assert json.loads(capsys.readouterr().out)["complete"] is False
    assert q._db_path().read_bytes() == before
    items_before = q._load_unlocked()["items"]
    assert cli.main(["answer-store-init", "--json"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["initialized"]
    assert store_identity.initialize() == {**result, "initialized": False}
    assert export.snapshot()["scope"]["store_id"] == result["store_id"]
    assert q._load_unlocked()["items"] == items_before


def test_legacy_json_identity_survives_sqlite_migration(source):
    base, request = source
    data = q._load_unlocked()
    q._db_path().unlink()
    base.write_text(json.dumps(data))
    identity = store_identity.initialize()["store_id"]
    assert not base.with_suffix(".db").exists()
    assert export.snapshot()["scope"]["store_id"] == identity
    request["expected_store_id"] = identity
    assert ca.submit("DEMO-1", request)["accepted"]
    assert base.with_suffix(".db").exists()
    assert export.snapshot()["scope"]["store_id"] == identity


def test_no_identity_fence_preserves_old_caller_on_uninitialized_store(source):
    _, request = source
    connection = q._connect(q._db_path())
    try:
        connection.execute("DELETE FROM meta WHERE key='store_id'")
        connection.commit()
    finally:
        connection.close()
    assert ca.submit("DEMO-1", request)["accepted"]
    assert uuid.UUID(export.snapshot()["scope"]["store_id"]).version == 4


def test_receipt_cli_fences_wrong_store(source, capsys):
    _, request = source
    ca.submit("DEMO-1", request)
    assert cli.main(["answer-receipt", "DEMO-1", "decision-1", "--expected-store-id", str(uuid.uuid4())]) == 2
    assert json.loads(capsys.readouterr().out)["error"] == "conflict"


def test_old_null_bound_key_cannot_be_upgraded_in_place(source):
    _, request = source
    ca.submit("DEMO-1", request)
    with pytest.raises(ca.Conflict, match="different payload"):
        ca.submit("DEMO-1", {**request, "expected_store_id": export.snapshot()["scope"]["store_id"]})


def test_mismatched_identity_cli_has_no_delivery_or_receipt_write(source, monkeypatch, capsys):
    _, request = source
    request["expected_store_id"] = str(uuid.uuid4())
    def forbidden(*args, **kwargs):
        pytest.fail("wrong-store request attempted delivery")
    monkeypatch.setattr(cli, "_deliver_answer", forbidden)
    assert cli.main(["answer-conditional", "DEMO-1", "--request-json", json.dumps(request)]) == 2
    assert json.loads(capsys.readouterr().out)["error"] == "conflict"
    assert ca.lookup("DEMO-1", "decision-1") is None


def test_identity_init_refuses_missing_store_without_creating_files(tmp_path, monkeypatch):
    base = tmp_path / "missing" / "queue.json"
    monkeypatch.setenv("WATCHTOWER_STORE", str(base))
    with pytest.raises(FileNotFoundError):
        store_identity.initialize()
    assert not base.parent.exists()

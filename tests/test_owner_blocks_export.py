"""Hermetic full-set export and retirement evidence; no live source access."""
import json

import pytest

from watchtower import cli, config, owner_blocks_export as export, queue as q


@pytest.fixture
def store(tmp_path, monkeypatch):
    base = tmp_path / "queue.json"
    settings = tmp_path / "settings.json"
    monkeypatch.setenv("WATCHTOWER_STORE", str(base))
    monkeypatch.setenv("WATCHTOWER_CONFIG_FILE", str(settings))
    settings.write_text(json.dumps({"REMOTE": {"backend": "github"}}))
    def item(number, **changes):
        return {"number": number, "project": "DEMO", "seq": number, "ref": f"DEMO-{number}",
                "status": "in_progress", "needs_input": True, "blocked_at": f"episode-{number}",
                "block_question": "Choose a color? ", "block_kind": "input", "history": [], **changes}
    items = [
        item(1), item(2, wait={"on": "owner", "block_at": "episode-2", "revision": "r1"}),
        item(3, wait={"on": "external", "block_at": "episode-3"}),
        item(4, wait={"on": "dependency", "block_at": "episode-4"}),
        item(5, block_kind="awaiting-client"), item(6, status="closed"),
        item(7, needs_input=False, status="in_review", needs_review=True),
        item(8, project="REMOTE", seq=1, ref="REMOTE-1"),
        item(9, wait={"on": "worker", "block_at": "episode-9"}),
    ]
    with q._FileLock(q._lock_path()):
        q._save_unlocked({"counter": 9, "items": items})
    return base, settings


def mutate(number, **fields):
    with q._FileLock(q._lock_path()):
        data = q._load_unlocked()
        data["items"][number - 1].update(fields)
        q._save_unlocked(data)


def test_complete_set_capabilities_and_exact_cas_fields(store):
    result = export.snapshot()
    assert result["complete"] is result["authoritative"] is True
    assert result["scope"]["backend"] == "file"
    assert result["store_version"]["revision"] >= 1
    assert len(result["scope"]["store_id"]) == 36
    assert result["capabilities"] == export.CAPABILITIES
    assert [row["origin_id"] for row in result["items"]] == ["DEMO-1", "DEMO-2"]
    first, second = result["items"]
    assert first["body"] == first["expected_block_question"] == "Choose a color? "
    assert first["generation"] == first["expected_blocked_at"] == "episode-1"
    assert first["expected_wait"] is None
    assert second["expected_wait"] == {"on": "owner", "block_at": "episode-2", "revision": "r1"}
    assert first["expected_wait_kind"] == first["wait_kind"] == "owner"
    assert export.snapshot() == result


def test_answer_ownership_change_and_delete_retire_disappeared_rows(store):
    first = export.snapshot()
    mutate(1, needs_input=False)
    mutate(2, wait={"on": "external", "block_at": "episode-2"})
    second = export.snapshot()
    assert second["items"] == []
    assert second["complete"] and second["scope"] == first["scope"]
    assert second["store_version"] != first["store_version"]
    with q._FileLock(q._lock_path()):
        q._save_unlocked({"counter": 9, "items": []})
    assert export.snapshot()["items"] == []


def test_revision_changes_with_question_or_ownership_revision(store):
    first = export.snapshot()["items"][0]
    mutate(1, block_question="Choose a shape?")
    second = export.snapshot()["items"][0]
    assert first["generation"] == second["generation"]
    assert first["source_revision"] != second["source_revision"]
    mutate(1, wait={"on": "owner", "block_at": "episode-1", "revision": "changed"})
    third = export.snapshot()["items"][0]
    assert third["source_revision"] != second["source_revision"]


def test_uses_shared_lock_and_no_mutating_resolvers(store, monkeypatch):
    base, _ = store
    original = q._normalize_items
    def check_lock(items):
        # A distinct open description cannot take an exclusive writer lock.
        with base.with_suffix(".lock").open("r") as other:
            with pytest.raises(BlockingIOError):
                q.fcntl.flock(other.fileno(), q.fcntl.LOCK_EX | q.fcntl.LOCK_NB)
        return original(items)
    def forbidden(*args, **kwargs):
        pytest.fail("mutating resolver/store helper used")
    monkeypatch.setattr(q, "_normalize_items", check_lock)
    for name in ("_save_unlocked", "_resolve_store_path", "_load_unlocked", "_connect"):
        monkeypatch.setattr(q, name, forbidden)
    monkeypatch.setattr(config, "config_path", forbidden)
    before = base.with_suffix(".db").read_bytes()
    export.snapshot()
    from watchtower import conditional_answers
    assert conditional_answers.lookup("DEMO-1", "missing-key") is None
    assert base.with_suffix(".db").read_bytes() == before


@pytest.mark.parametrize("failure", ["missing_lock", "missing_store", "corrupt_store", "corrupt_settings"])
def test_failure_is_never_authoritative_absence(store, failure, capsys):
    base, settings = store
    if failure == "missing_lock":
        base.with_suffix(".lock").unlink()
    elif failure == "missing_store":
        base.with_suffix(".db").unlink()
    elif failure == "corrupt_store":
        base.with_suffix(".db").write_bytes(b"invalid database")
    else:
        settings.write_text("not json")
    assert cli.main(["owner-blocks-export", "--json"]) == 2
    result = json.loads(capsys.readouterr().out)
    assert result["error"] == "unavailable"
    assert result["authoritative"] is result["complete"] is False
    assert cli.main(["answer-receipt", "DEMO-1", "missing-key", "--json"]) == 2
    assert json.loads(capsys.readouterr().out)["error"] != "not_found"


def test_legacy_json_read_does_not_migrate(store, monkeypatch):
    base, _ = store
    data = q._load_unlocked()
    data["store_id"] = export.snapshot()["scope"]["store_id"]
    base.write_text(json.dumps(data))
    base.with_suffix(".db").unlink()
    before = base.read_bytes()
    result = export.snapshot()
    assert result["store_version"]["revision"] is None
    assert len(result["items"]) == 2
    assert base.read_bytes() == before
    assert not base.with_suffix(".db").exists()


def test_no_implicit_default_migration(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("WATCHTOWER_STORE", raising=False)
    monkeypatch.setenv("WATCHTOWER_DATA_DIR", str(tmp_path / "absent"))
    assert cli.main(["owner-blocks-export"]) == 2
    assert not (tmp_path / "absent").exists()
    assert json.loads(capsys.readouterr().out)["complete"] is False


def test_config_migration_required_is_not_empty_snapshot(store, monkeypatch):
    base, _ = store
    legacy = base.parent / "legacy-settings.json"
    legacy.write_text('{}')
    monkeypatch.delenv("WATCHTOWER_CONFIG_FILE")
    monkeypatch.setattr(config, "CONFIG_FILE", legacy)
    monkeypatch.setattr(config, "_LEGACY_CONFIG_FILE", legacy)
    monkeypatch.setenv("WATCHTOWER_DATA_DIR", str(base.parent / "uninitialized"))
    with pytest.raises(export.Unsupported, match="migration required"):
        export.snapshot()
    assert not (base.parent / "uninitialized").exists()


def test_cli_json_full_set(store, capsys):
    assert cli.main(["owner-blocks-export", "--json"]) == 0
    assert json.loads(capsys.readouterr().out) == export.snapshot()


def test_unknown_backend_cannot_authoritatively_withdraw(store, capsys):
    _, settings = store
    settings.write_text(json.dumps({"DEMO": {"backend": "unsupported"}}))
    assert cli.main(["owner-blocks-export"]) == 2
    assert json.loads(capsys.readouterr().out)["authoritative"] is False


def test_busy_writer_is_unavailable_not_empty_snapshot(store, capsys):
    base, _ = store
    with base.with_suffix(".lock").open("r") as lock:
        q.fcntl.flock(lock.fileno(), q.fcntl.LOCK_EX)
        try:
            assert cli.main(["owner-blocks-export"]) == 2
        finally:
            q.fcntl.flock(lock.fileno(), q.fcntl.LOCK_UN)
    assert json.loads(capsys.readouterr().out)["complete"] is False


def test_sqlite_export_includes_committed_wal_data(store):
    base, _ = store
    # Keep a connection open so the committed change remains in the WAL.
    connection = q._connect(base.with_suffix(".db"))
    try:
        row = json.loads(connection.execute("SELECT item_json FROM items WHERE number=1").fetchone()[0])
        row["block_question"] = "A question committed in WAL?"
        connection.execute("UPDATE items SET item_json=? WHERE number=1", (json.dumps(row),))
        connection.commit()
        assert base.with_suffix(".db-wal").exists()
        assert export.snapshot()["items"][0]["body"] == row["block_question"]
    finally:
        connection.close()

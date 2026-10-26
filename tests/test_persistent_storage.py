"""Queue data must outlive removal of both applications' settings trees."""

import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

import pytest

from watchtower import config, queue, storage


@pytest.fixture
def home(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: tmp_path))
    for key in ("WATCHTOWER_STORE", "WATCHTOWER_CONFIG_FILE", "WATCHTOWER_DATA_DIR", "XDG_DATA_HOME"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(queue, "_CCC_LEGACY_STORE", tmp_path / ".claude/command-center/ux-fixes-queue.json")
    monkeypatch.setattr(queue, "_WT_DEFAULT_STORE", tmp_path / ".watchtower/queues.json")
    monkeypatch.setattr(config, "_LEGACY_CONFIG_FILE", tmp_path / ".watchtower/queue-config.json")
    monkeypatch.setattr(config, "CONFIG_FILE", config._LEGACY_CONFIG_FILE)
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(tmp_path / "activity.log"))
    return tmp_path


def seed(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {"counter": 7, "items": [{
        "number": 7, "project": "KEEP", "seq": 1, "ref": "KEEP-1",
        "status": "closed", "note": "keep this ticket", "text": "full body",
        "history": [{"event": "close", "worker": "test-worker"}],
        "resolution": {"summary": "all done", "no_code": True},
    }]}
    path.write_text(json.dumps(data))
    return data


def fresh_process(home, code):
    prefix = "from pathlib import Path; import sys; Path.home = classmethod(lambda cls: Path(sys.argv[1])); "
    return subprocess.run([sys.executable, "-c", prefix + code, str(home)],
                          env=os.environ.copy(), check=True, text=True, capture_output=True)


@pytest.mark.parametrize("legacy", ["ccc", "wt"])
@pytest.mark.parametrize("backend", ["json", "sqlite"])
def test_reinstall_preserves_tickets_history_settings_and_numbering(home, legacy, backend):
    source = queue._CCC_LEGACY_STORE if legacy == "ccc" else queue._WT_DEFAULT_STORE
    data = seed(source)
    if backend == "sqlite":
        queue._create_db(source.with_suffix(".db"), data)
    settings = config._LEGACY_CONFIG_FILE
    settings.parent.mkdir(parents=True, exist_ok=True)
    settings.write_text(json.dumps({
        "KEEP": {"repo_path": "/example/project", "auto_drain": False},
        "EMPTY": {"backend": "github", "github_repo": "example/project"},
    }))

    ticket = queue.get("KEEP-1")
    assert ticket["history"] == data["items"][0]["history"]
    assert ticket["resolution"]["summary"] == "all done"
    assert config.github_repo("EMPTY") == "example/project"
    assert queue.store_path() == home / ".local/share/watchtower/queues.db"
    assert source.exists() and settings.exists()  # migration retains originals

    # Actual recursive removal, confined to the sandbox, followed by a new
    # interpreter (no cached objects or config can rescue the assertions).
    for directory in (home / ".claude", home / ".watchtower"):
        if directory.exists():
            shutil.rmtree(directory)
    fresh_process(home, "from watchtower import queue as q, config as c; "
                  "assert q.get('KEEP-1')['resolution']['summary'] == 'all done'; "
                  "assert c.github_repo('EMPTY') == 'example/project'; "
                  "item = q.enqueue(project='KEEP', note='after reinstall'); "
                  "assert item['ref'] == 'KEEP-2'; assert item['number'] == 8")
    assert queue.get("KEEP-2")["note"] == "after reinstall"


def test_sqlite_migration_includes_uncheckpointed_wal(home):
    source = queue._CCC_LEGACY_STORE
    queue._create_db(source.with_suffix(".db"), seed(source))
    conn = queue._connect(source.with_suffix(".db"))
    try:
        conn.execute("PRAGMA wal_autocheckpoint=0")
        item = dict(json.loads(conn.execute("SELECT item_json FROM items").fetchone()[0]), note="committed in WAL")
        conn.execute("UPDATE items SET item_json=?", (json.dumps(item),))
        conn.commit()
        assert source.with_suffix(".db-wal").stat().st_size > 0
        assert queue.get("KEEP-1")["note"] == "committed in WAL"
    finally:
        conn.close()


def test_existing_destination_never_reimports_stale_source(home):
    source = queue._CCC_LEGACY_STORE
    seed(source)
    queue.get("KEEP-1")
    with queue._FileLock(queue._lock_path()):
        queue._save_unlocked({"counter": 7, "items": []})
    assert queue.export_data() == {"counter": 7, "items": []}
    assert source.exists()


@pytest.mark.parametrize("corruption", ["json", "sqlite", "config"])
def test_corrupt_legacy_source_fails_without_publishing_empty_destination(home, corruption):
    if corruption == "config":
        source = config._LEGACY_CONFIG_FILE
        destination = storage.data_dir() / "queue-config.json"
        read = config.config_path
    else:
        source = queue._CCC_LEGACY_STORE
        if corruption == "sqlite":
            source = source.with_suffix(".db")
        destination = storage.data_dir() / "queues.db"
        read = queue.export_data
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_text("broken data")
    with pytest.raises((ValueError, sqlite3.Error)):
        read()
    assert not destination.exists()
    assert source.read_text() == "broken data"


def test_explicit_paths_are_not_migrated(home, monkeypatch):
    explicit = home / "custom/tickets.json"
    seed(explicit)
    monkeypatch.setenv("WATCHTOWER_STORE", str(explicit))
    settings = home / "custom/config.json"
    monkeypatch.setattr(config, "CONFIG_FILE", settings)
    config.set_backend("OTHER", "github")
    assert queue.get("KEEP-1") is not None
    assert config.config_path() == settings
    assert not storage.data_dir().exists()


def test_data_directory_overrides(home, monkeypatch):
    monkeypatch.setenv("XDG_DATA_HOME", str(home / "user-data"))
    assert storage.data_dir() == home / "user-data/watchtower"
    monkeypatch.setenv("WATCHTOWER_DATA_DIR", str(home / "durable"))
    assert storage.data_dir() == home / "durable"
    queue.enqueue(project="NEW", note="persist")
    assert queue.store_path() == home / "durable/queues.db"


def test_completed_github_migration_survives_reinstall(home, monkeypatch):
    settings = config._LEGACY_CONFIG_FILE
    settings.parent.mkdir(parents=True)
    settings.write_text(json.dumps({"GH": {"backend": "github", "auto_drain": True}}))
    marker = settings.parent / "gh-drain-migration.done"
    marker.write_text("{}\n")
    monkeypatch.setattr(config, "GH_DRAIN_MIGRATION_MARKER", marker)
    assert config.migrate_github_auto_drain() == []
    shutil.rmtree(settings.parent)
    assert config.migrate_github_auto_drain() == []
    assert config.auto_drain("GH") is True


def test_concurrent_first_access_imports_once_without_losing_writes(home):
    seed(queue._CCC_LEGACY_STORE)
    code = "from watchtower import queue; queue.enqueue(project='KEEP', note='parallel')"
    with ThreadPoolExecutor(max_workers=6) as pool:
        list(pool.map(lambda _: fresh_process(home, code), range(6)))
    items = queue.export_data()["items"]
    assert len(items) == 7
    assert {it["ref"] for it in items} == {f"KEEP-{i}" for i in range(1, 8)}


def test_uninstall_migrates_and_keeps_queue_data(home, monkeypatch, capsys):
    from watchtower import cli, skills_sync
    seed(queue._CCC_LEGACY_STORE)
    monkeypatch.setattr(cli, "_LAUNCHAGENT_PLIST", home / "absent.plist")
    monkeypatch.setattr(skills_sync, "remove", lambda: [])
    assert cli.cmd_uninstall(None) == 0
    assert "Queue data and settings retained" in capsys.readouterr().out
    assert queue.store_path() == storage.data_dir() / "queues.db"
    assert queue.get("KEEP-1") is not None

"""Disposable SQLite and legacy JSON stores only; never production data."""
import copy
import json
import uuid

import pytest

from watchtower import cli, queue as q, owner_blocks_export as export, owner_wait_init as migration
from watchtower.conditional_answers import Conflict


@pytest.fixture(params=["sqlite", "json"])
def source(tmp_path, monkeypatch, request):
    base = tmp_path / "source.json"
    settings = tmp_path / "settings.json"
    monkeypatch.setenv("WATCHTOWER_STORE", str(base))
    monkeypatch.setenv("WATCHTOWER_CONFIG_FILE", str(settings))
    settings.write_text('{}')
    item = {"number": 1, "project": "EXAMPLE", "seq": 1, "ref": "EXAMPLE-1",
            "title": "Independent title", "status": "in_progress", "needs_input": True,
            "blocked_at": None, "block_question": "  Choose a color?\n", "block_kind": "input",
            "history": [], "owner": "example-owner", "answers": ["old answer"],
            "pending_answer": {"state": "none"}}
    data = {"counter": 1, "store_id": str(uuid.uuid4()), "items": [item]}
    base.write_text(json.dumps(data))
    base.with_suffix('.lock').touch()
    if request.param == "sqlite":
        with q._FileLock(base.with_suffix('.lock')):
            q._save_unlocked(copy.deepcopy(data))
    return base, settings


def edit(source, change):
    base, _ = source
    with base.with_suffix('.lock').open('r') as lock:
        q.fcntl.flock(lock.fileno(), q.fcntl.LOCK_EX)
        if base.with_suffix('.db').exists():
            connection = q.sqlite3.connect(base.with_suffix('.db'))
            item = json.loads(connection.execute('SELECT item_json FROM items').fetchone()[0])
            change(item)
            connection.execute('UPDATE items SET item_json=?', (json.dumps(item),))
            connection.commit()
            connection.close()
        else:
            data = json.loads(base.read_text())
            change(data['items'][0])
            base.write_text(json.dumps(data))


def read(source):
    return export.read_source()[1][0]


def test_dry_run_apply_export_replay_and_preservation(source):
    before = read(source)
    snapshot = export.snapshot()
    result = migration.initialize('EXAMPLE-1')
    request = result['request']
    assert request['expected_store_version'] == snapshot['store_version']
    assert request['expected_store_id'] == snapshot['scope']['store_id']
    assert result['dry_run'] and result['receipt'] is None
    assert read(source) == before
    result = migration.initialize('EXAMPLE-1', request, apply=True)
    after = read(source)
    for key, value in before.items():
        if key not in ('blocked_at', 'wait'):
            assert after[key] == value
    assert set(after) - set(before) == {'wait', migration.PROVENANCE, migration.RECEIPTS}
    assert after['blocked_at'] == result['receipt']['initialized_at']
    assert len(after['wait']['revision']) == 32
    provenance = export.snapshot()['items'][0]['episode_provenance']
    assert provenance['historical_blocked_at'] is None
    assert provenance['historical_block_time'] == 'unknown'
    assert export.snapshot()['items'][0]['answerable']
    receipt = copy.deepcopy(result['receipt'])
    edit(source, lambda item: item.update(status='closed', needs_input=False))
    replay = migration.initialize('EXAMPLE-1', request, apply=True)
    assert replay['replayed'] and replay['receipt'] == receipt
    assert read(source)['status'] == 'closed'
    altered = copy.deepcopy(request)
    altered['expected_item']['block_question'] += ' '
    with pytest.raises(Conflict, match='different payload'):
        migration.initialize('EXAMPLE-1', altered, apply=True)


@pytest.mark.parametrize('change', [
    {'block_question': 'Choose a shape?'}, {'status': 'open'}, {'needs_input': False},
    {'wait': {'on': 'owner', 'block_at': '', 'revision': 'ABA'}},
    {'answers': ['another answer']}, {'blocked_at': 'new-episode'},
])
def test_stale_exact_source_conflicts(source, change):
    request = migration.initialize('EXAMPLE-1')['request']
    edit(source, lambda item: item.update(change))
    before = read(source)
    with pytest.raises(Conflict):
        migration.initialize('EXAMPLE-1', request, apply=True)
    assert read(source) == before


@pytest.mark.parametrize('change', [
    {'duplicate_of': 'EXAMPLE-2'}, {'status': 'closed'}, {'status': 'in_review'},
    {'needs_review': True}, {'block_kind': 'rationale'}, {'block_kind': 'awaiting-client'},
    {'wait': {'on': 'external'}}, {'wait': {'on': 'dependency'}},
    {'wait': {'on': 'worker'}}, {'wait': {'on': 'unknown'}},
    {'history': [{'event': 'block', 'at': 'old'}]}, {'block_history': ['old']},
    {'blocked_at': 'already-known'}, {'block_question': '  '},
])
def test_ineligible_unchanged(source, change):
    edit(source, lambda item: item.update(change))
    before = read(source)
    with pytest.raises(Conflict):
        migration.initialize('EXAMPLE-1')
    assert read(source) == before


def test_wrong_store_before_replay(source):
    request = migration.initialize('EXAMPLE-1')['request']
    migration.initialize('EXAMPLE-1', request, apply=True)
    request['expected_store_id'] = str(uuid.uuid4())
    with pytest.raises(Conflict, match='identity'):
        migration.initialize('EXAMPLE-1', request, apply=True)


def test_cli_default_and_explicit_apply_requirement(source, capsys):
    assert cli.main(['owner-wait-init', 'EXAMPLE-1', '--json']) == 0
    result = json.loads(capsys.readouterr().out)
    assert result['dry_run']
    assert read(source)['blocked_at'] is None
    assert cli.main(['owner-wait-init', 'EXAMPLE-1', '--apply']) == 2
    assert json.loads(capsys.readouterr().out)['error'] == 'invalid_request'
    assert cli.main(['owner-wait-init', 'EXAMPLE-1', '--request-json', json.dumps(result['request']), '--apply']) == 0
    assert not json.loads(capsys.readouterr().out)['dry_run']


def test_busy_actual_lock_and_missing_lock_fail_closed(source):
    base, _ = source
    request = migration.initialize('EXAMPLE-1')['request']
    with base.with_suffix('.lock').open('r') as lock:
        q.fcntl.flock(lock.fileno(), q.fcntl.LOCK_EX)
        with pytest.raises(BlockingIOError):
            migration.initialize('EXAMPLE-1', request, apply=True)
    base.with_suffix('.lock').unlink()
    with pytest.raises(FileNotFoundError):
        migration.initialize('EXAMPLE-1')
    assert not base.with_suffix('.lock').exists()


def test_settings_change_full_snapshot_fence(source):
    request = migration.initialize('EXAMPLE-1')['request']
    source[1].write_text('{"EXAMPLE": {"backend": "github"}}')
    with pytest.raises(Conflict):
        migration.initialize('EXAMPLE-1', request, apply=True)


def test_full_owner_wait_preserves_fields_and_rotates_revision(source):
    wait = {'on': 'owner', 'block_at': '', 'revision': 'old', 'actor': 'example-owner',
            'ref': 'EXAMPLE-2', 'by': 'system', 'note': 'Exact note', 'host': 'example-host'}
    edit(source, lambda item: item.update(wait=wait))
    request = migration.initialize('EXAMPLE-1')['request']
    assert request['expected_wait'] == wait
    migration.initialize('EXAMPLE-1', request, apply=True)
    after = read(source)['wait']
    for key in ('actor', 'ref', 'by', 'note', 'host', 'on'):
        assert after[key] == wait[key]
    assert after['revision'] != wait['revision']


def test_actual_lock_and_sqlite_transaction_cover_validation_and_commit(source, monkeypatch):
    base, _ = source
    request = migration.initialize('EXAMPLE-1')['request']
    original = migration._eligible
    def check(item, settings):
        with base.with_suffix('.lock').open('r') as lock:
            with pytest.raises(BlockingIOError):
                q.fcntl.flock(lock.fileno(), q.fcntl.LOCK_EX | q.fcntl.LOCK_NB)
        original(item, settings)
    monkeypatch.setattr(migration, '_eligible', check)
    # Any ordinary writer/resolver would introduce non-metadata side effects.
    def forbidden(*a, **kw):
        pytest.fail('ordinary mutating helper used')
    for name in ('_save_unlocked', '_load_unlocked', '_resolve_store_path', '_connect'):
        monkeypatch.setattr(q, name, forbidden)
    migration.initialize('EXAMPLE-1', request, apply=True)
    assert base.with_suffix('.db').exists() == (request['expected_store_version']['revision'] is not None)


def test_full_version_conflict_without_ticket_change(source):
    request = migration.initialize('EXAMPLE-1')['request']
    # An unrelated settings edit changes the full source hash.
    source[1].write_text('{"OTHER": {"backend": "file"}}')
    with pytest.raises(Conflict):
        migration.initialize('EXAMPLE-1', request, apply=True)


def test_json_types_are_not_coerced_in_frozen_cas(source):
    request = migration.initialize('EXAMPLE-1')['request']
    request['expected_item']['needs_input'] = 1
    with pytest.raises(Conflict):
        migration.initialize('EXAMPLE-1', request, apply=True)


def test_stale_raw_owner_wait_is_not_revived(source):
    edit(source, lambda item: item.update(wait={'on': 'owner', 'block_at': 'obsolete'}))
    with pytest.raises(Conflict):
        migration.initialize('EXAMPLE-1')


def test_old_initialization_provenance_not_applied_to_new_episode(source):
    request = migration.initialize('EXAMPLE-1')['request']
    migration.initialize('EXAMPLE-1', request, apply=True)
    edit(source, lambda item: item.update(blocked_at='new-episode', wait=None))
    assert export.snapshot()['items'][0]['episode_provenance'] is None


def test_dry_run_is_byte_preserving(source):
    base, _ = source
    paths = [p for p in base.parent.iterdir() if p.is_file()]
    before = {p: p.read_bytes() for p in paths}
    migration.initialize('EXAMPLE-1')
    assert {p: p.read_bytes() for p in paths} == before


def test_missing_identity_never_initialized(source):
    base, _ = source
    if base.with_suffix('.db').exists():
        connection = q.sqlite3.connect(base.with_suffix('.db'))
        connection.execute("DELETE FROM meta WHERE key='store_id'")
        connection.commit()
        connection.close()
    else:
        data = json.loads(base.read_text())
        del data['store_id']
        base.write_text(json.dumps(data))
    with pytest.raises(ValueError, match='persisted store UUID'):
        migration.initialize('EXAMPLE-1')
    assert export.read_source()[4] is None


def test_sqlite_failure_rolls_back_metadata_and_receipt(source, monkeypatch):
    base, _ = source
    if not base.with_suffix('.db').exists():
        pytest.skip('SQLite transaction fault injection')
    request = migration.initialize('EXAMPLE-1')['request']
    before = export.snapshot()
    real_connect = q.sqlite3.connect
    class FailingConnection(q.sqlite3.Connection):
        def execute(self, sql, parameters=()):
            if sql.startswith('UPDATE meta SET value='):
                raise q.sqlite3.OperationalError('injected revision write failure')
            return super().execute(sql, parameters)
    with monkeypatch.context() as patch:
        patch.setattr(q.sqlite3, 'connect', lambda *a, **kw: real_connect(*a, factory=FailingConnection, **kw))
        with pytest.raises(q.sqlite3.OperationalError):
            migration.initialize('EXAMPLE-1', request, apply=True)
    assert export.snapshot() == before
    assert migration.RECEIPTS not in read(source)
    assert migration.initialize('EXAMPLE-1', request, apply=True)['receipt']


@pytest.mark.parametrize('field,value', [('expected_store_id', 7), ('version', True), ('extra', 'unknown')])
def test_invalid_envelope_returns_json_without_changes(source, capsys, field, value):
    request = migration.initialize('EXAMPLE-1')['request']
    request[field] = value
    before = read(source)
    assert cli.main(['owner-wait-init', 'EXAMPLE-1', '--apply', '--request-json', json.dumps(request)]) == 2
    assert json.loads(capsys.readouterr().out)['error'] == 'invalid_request'
    assert read(source) == before

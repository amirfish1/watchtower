"""Public CLI clients: opaque correlation and safe recovery, with no live services."""
import ast
import builtins
from importlib.machinery import PathFinder
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from watchtower import queue as q, stages


def test_client_id_local_cli_roundtrip_filter(wt_env, run_cli):
    token = '  Case/opaque:雪\n"\\ --flag  '
    result = run_cli('add', '-q', 'TEST', '--title', 'linked', '--client-id', token, '--json')
    assert result.code == 0, result.output
    item = json.loads(result.out)
    assert item['client_id'] == token
    q.enqueue(project='TEST', note='unrelated')
    assert q.get(item['ref'])['client_id'] == token
    rows = run_cli('ls', '-q', 'TEST', '--status', 'all', '--client-id', token, '--json')
    assert [r['ref'] for r in json.loads(rows.out)] == [item['ref']]
    assert json.loads(run_cli('ls', '-q', 'TEST', '--client-id', token.lower(), '--json').out) == []
    edited = run_cli('edit', item['ref'], '--client-id', 'replacement', '--json')
    assert json.loads(edited.out)['client_id'] == 'replacement'
    assert json.loads(run_cli('find', item['ref'], '--json').out)['client_id'] == 'replacement'
    assert json.loads(run_cli('ls', '-q', 'TEST', '--client-id', token, '--json').out) == []
    assert run_cli('edit', item['ref'], '--client-id', '').code == 0
    assert len(json.loads(run_cli('ls', '-q', 'TEST', '--client-id', '', '--json').out)) == 2


def test_find_exposes_resolved_gates_and_config_roles(wt_env, run_cli):
    wt_env.config.set_gates('TEST', ['verify'])
    item = q.enqueue(project='TEST', note='default gates')
    snapshot = json.loads(run_cli('find', item['ref'], '--json').out)
    assert snapshot['effective_gates'] == ['verify']
    q.update(item['ref'], gates=['review'])
    snapshot = json.loads(run_cli('find', item['ref'], '--json').out)
    assert snapshot['effective_gates'] == ['review']
    config = json.loads(run_cli('config', '-q', 'TEST', '--json').out)
    assert {'engine', 'model', 'source'} <= config['roles']['planner'].keys()
    assert {'engine', 'model', 'source'} <= config['roles']['verifier'].keys()


def escalated(role='planner'):
    item = q.enqueue(project='TEST', note='stage task', gates=['plan'] if role in ('planner','plan_reviewer') else ['verify'])
    def setup(it, ss):
        ss.update(role=role, key={'planner':'plan:r1', 'plan_reviewer':'review:r1',
                                 'verifier':'verify:2','assessor':'assess:1'}[role],
                  attempt=2, escalated=True, escalated_from='planning' if role=='planner' else 'reviewing')
        it.update(needs_input=True, block_origin='stage', block_question='Stage exhausted', blocked_at='episode')
        if role in ('planner', 'plan_reviewer'):
            it['plan'] = dict(status='blocked', escalated='stage_watch', round=1)
        elif role=='verifier':
            it.update(status='in_review', gate_pending='verify', verify_cycle=2)
        else:
            it.update(status='closed', assessment=dict(status='failed', cycle=1))
    q.stage_session_update(item['ref'], setup)
    return item['ref']


@pytest.mark.parametrize('role', ['planner','plan_reviewer','verifier','assessor'])
def test_retry_stage_cli_fences_and_queues(wt_env, run_cli, role, monkeypatch):
    ref = escalated(role)
    monkeypatch.setattr(wt_env.workers, 'spawn_adhoc', lambda *a, **k: pytest.fail('inline spawn'))
    before = q.get(ref)
    stale = run_cli('stages', 'retry', ref, '--expected-cycle', '9')
    assert stale.code == 1 and not json.loads(stale.out)['ok']
    assert q.get(ref) == before
    result = run_cli('stages', 'retry', ref, '--expected-cycle', '0', '--json')
    assert result.code == 0, result.output
    response = json.loads(result.out)
    assert response['ok'] and response['retried'] and response['cycle'] == 1
    item = q.get(ref)
    assert not item['needs_input'] and not item['stage_session']['escalated']
    assert item['stage_session']['attempt'] == 0
    assert stages.desired([item])[0]['role'] == role
    duplicate = run_cli('stages', 'retry', ref, '--expected-cycle', '0')
    assert duplicate.code == 1 and 'stale' in json.loads(duplicate.out)['error']
    repeat = run_cli('stages', 'retry', ref, '--expected-cycle', '1')
    assert repeat.code == 1
    assert q.get(ref) == item


@pytest.mark.parametrize('patch', [
    dict(block_origin='worker'), dict(duplicate_of='TEST-99'), dict(parked={'session_id':'other'}),
    dict(pending_answer={'state':'routing'}), dict(pending_answer={'state':'handed_off'}),
    dict(supervision={'state':'waiting'}), dict(status='closed'),
])
def test_retry_refuses_changed_authority_or_delivery(wt_env, patch):
    ref = escalated()
    q.stage_session_update(ref, lambda it, ss: it.update(patch))
    before = q.get(ref)
    with pytest.raises(ValueError):
        q.retry_stage(ref, 0)
    assert q.get(ref) == before


def test_retry_refuses_live_stage(wt_env, monkeypatch):
    ref = escalated()
    q.stage_session_update(ref, lambda it, ss: ss.update(worker_id='stage-worker'))
    monkeypatch.setattr(stages, 'session_alive', lambda wid: True)
    before = q.get(ref)
    with pytest.raises(ValueError, match='alive'):
        q.retry_stage(ref, 0)
    assert q.get(ref) == before


def test_retry_serializes_duplicates_and_keeps_builder_claim(wt_env):
    ref = escalated('verifier')
    q.stage_session_update(ref, lambda it, ss: it.update(claimed_by='builder', claimed_session_id='builder-sid'))
    def attempt(_):
        try:
            return q.retry_stage(ref, 0)['ok']
        except ValueError:
            return False
    with ThreadPoolExecutor(4) as pool:
        results = list(pool.map(attempt, range(4)))
    assert results.count(True) == 1
    assert q.get(ref)['claimed_by'] == 'builder'
    assert q.get(ref)['claimed_session_id'] == 'builder-sid'


def test_stage_cycle_survives_key_changes_and_human_retry(wt_env):
    ref = escalated()
    q.answer(ref, 'retry')
    assert q.get(ref)['stage_session']['retry_cycle'] == 1
    stages._ss_replace(ref, dict(key='review:r1', role='plan_reviewer', escalated=False))
    assert q.get(ref)['stage_session']['retry_cycle'] == 2
    # A delayed retry from the previous stage cannot affect the new stage.
    with pytest.raises(ValueError, match='stale'):
        q.retry_stage(ref, 1)


def test_removed_engine_modules_and_cli(wt_env, run_cli):
    names = {'outcomes', 'outcome_intake'}
    for name in names:
        assert PathFinder.find_spec('watchtower.' + name, [str(Path(__file__).parents[1] / 'watchtower')]) is None
    for path in (Path(__file__).parents[1] / 'watchtower').glob('*.py'):
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.ImportFrom):
                assert not any(a.name in names for a in node.names)
                assert (node.module or '').split('.')[-1] not in names
            elif isinstance(node, ast.Import):
                assert not any(a.name.split('.')[-1] in names for a in node.names)
    assert run_cli('outcome', '--help').code == 2


def test_ordinary_tick_requires_no_optional_engine(wt_env, monkeypatch):
    calls=[]
    real_import=builtins.__import__
    def guarded(name, globals=None, locals=None, fromlist=(), level=0):
        if any(part in {'outcomes','outcome_intake'} for part in (name, *(fromlist or ()))):
            calls.append(name)
            raise AssertionError('removed engine imported')
        return real_import(name,globals,locals,fromlist,level)
    monkeypatch.setattr(builtins,'__import__',guarded)
    monkeypatch.setattr(wt_env.workers,'spawn_workers',lambda *a,**kw: pytest.fail('ordinary disabled queue spawned'))
    q.enqueue(project='TEST',note='ordinary task')
    result=wt_env.workers.reconcile_once(dry_run=False)
    assert not calls and not result['spawned']
    assert q.get('TEST-1')['status']=='open'


def test_retry_refuses_unavailable_locks(wt_env, monkeypatch):
    ref = escalated()
    before = q.get(ref)
    monkeypatch.setattr(q, 'fcntl', None)
    with pytest.raises(ValueError, match='exclusive'):
        q.retry_stage(ref, 0)
    assert q.get(ref) == before


def test_retry_refuses_changed_key_and_dependency(wt_env):
    ref = escalated()
    q.stage_session_update(ref, lambda it, ss: ss.update(key='plan:r99'))
    before = q.get(ref)
    with pytest.raises(ValueError, match='key changed'):
        q.retry_stage(ref, 0)
    assert q.get(ref) == before
    blocker = q.enqueue(project='TEST', note='dependency')
    q.update(ref, blocked_by=[blocker['ref']])
    with pytest.raises(ValueError, match='dependency'):
        q.retry_stage(ref, 0)

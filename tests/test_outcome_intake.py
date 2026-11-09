"""Generic, isolated intake fixtures: no live CCC, queues or models."""
from unittest.mock import Mock

import pytest

from watchtower import outcome_intake as intake, outcomes as oc

_REAL_GET = intake._get


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv('WATCHTOWER_DATA_DIR', str(tmp_path))
    oc.set_settings(True)
    rows, tickets, census, requests, events, associations, calls = [], [], [], [], [], [], []
    monkeypatch.setattr(oc, 'list_outcomes', lambda: rows)
    monkeypatch.setattr(intake, '_tickets', lambda: tickets)
    monkeypatch.setattr(oc, 'associate', lambda p: associations.append(p))
    monkeypatch.setattr(oc, 'event', lambda p: events.append(p))
    def get(path):
        calls.append(path)
        if 'census' in path:
            return dict(ok=True, sessions=census)
        return dict(ok=True, source_current=True, requests=requests)
    monkeypatch.setattr(intake, '_get', get)
    return dict(rows=rows, tickets=tickets, census=census, requests=requests,
                events=events, associations=associations, calls=calls)


def outcome(oid='oc-111111111111', **kw):
    return dict(id=oid, state='active', repo_path='/workspace/project',
                sessions=kw.pop('sessions', []), **kw)


def session(sid, **kw):
    return dict(session_id=sid, name='Example session', repo_path='/workspace/project', **kw)


def decision(env, **kw):
    ref = dict(id='decision-1', request_id='request-1', origin_id='origin-1',
               revision='7', state='pending')
    ref.update(kw)
    env['rows'].append(outcome(decision_refs=[ref]))
    req = dict(request_id='request-1', origin_id='origin-1', revision=7, generation='episode-1',
               answer=dict(answer_id='answer-1', state='accepted', text='Use option A.',
                           expected_revision=7, session_consumed=False))
    env['requests'].append(req)
    return ref, req


def test_off_no_storage_or_network(env, monkeypatch):
    oc.set_settings(False)
    monkeypatch.setattr(intake, '_path', Mock(side_effect=AssertionError('storage')))
    assert intake.reconcile() == {'enabled': False}
    assert intake.view()['enabled'] is False
    assert not env['calls']


def test_unmatched_work_has_one_existing_lead_review_duty(env):
    env['rows'].extend([outcome(commitment=True, rank=1), outcome('oc-222222222222', rank=2)])
    env['census'].append(dict(session('unmatched'), repo_path='/workspace/other'))
    intake.reconcile(now=100)
    assert env['associations'] == []
    assert len(env['events']) == 1
    payload = env['events'][0]['payload']
    assert payload['administrative_intake'][0]['session_id'] == 'unmatched'
    assert payload['acceptance_credit'] is False
    assert intake.view()['unfiled'][0]['next_action']['owner'] == 'agent'


def test_claim_parent_runtime_and_explicit_provenance(env):
    oid = 'oc-111111111111'
    env['rows'].append(outcome(sessions=[{'session_id': 'parent'}],
        links=[dict(kind='ticket', relation='serves', target='DEMO-1', step_id='step-1')],
        attempts=[dict(session_id='runtime')]))
    env['tickets'].append(dict(ref='DEMO-1', claimed_session_id='claimed', claimed_by='worker'))
    env['census'].extend([session('claimed'), session('child', parent_session_id='parent'),
                          session('runtime'), session('explicit', outcome_id=oid),
                          session('step', step_id='step-1'), session('ticket', ticket_ref='DEMO-1')])
    intake.reconcile(now=100)
    assert {a['session_id'] for a in env['associations']} == {'claimed', 'child', 'runtime', 'explicit', 'step', 'ticket'}
    assert intake.view()['unfiled'] == []


def test_ambiguity_owned_next_check_and_repo_suggestions(env):
    env['rows'].extend([outcome(sessions=[{'session_id': 'parent'}]),
                        outcome('oc-222222222222', sessions=[{'session_id': 'parent'}])])
    env['census'].extend([session('ambiguous', parent_session_id='parent'), session('unrelated')])
    intake.reconcile(now=100)
    assert not env['associations']
    rows = {r['session_id']: r for r in intake.view()['unfiled']}
    assert 'ambiguous' in rows['ambiguous']['reason']
    for r in rows.values():
        assert r['next_action']['owner'] == 'agent' and r['next_action']['due_at']
        assert r['suggestions_are_evidence'] is False
    assert len(env['events']) == 2
    assert all(e['type'] == 'owner_review' and not e['payload']['acceptance_credit'] for e in env['events'])


def test_hourly_cap_cursor_and_review_coalescing(env, monkeypatch):
    monkeypatch.setattr(intake, 'SCAN_LIMIT', 2)
    env['rows'].append(outcome())
    env['census'].extend(session(f's{i}') for i in range(5))
    intake.reconcile(now=10)
    assert intake.view()['summary']['coverage']['census']['state'] == 'bounded'
    assert len(intake.view()['unfiled']) == 2 and len(env['events']) == 1
    intake.reconcile(now=100)
    assert len(env['calls']) == 1
    intake.reconcile(now=3610)
    assert len(intake.view()['unfiled']) == 4 and len(env['events']) == 2
    assert intake.view()['summary']['cursor'] == 's3'


def test_unchanged_unfiled_provenance_does_not_spend_another_lead_wake(env):
    env['rows'].append(outcome())
    env['census'].append(session('unassigned'))
    intake.reconcile(now=100)
    intake.reconcile(now=3700)
    assert len(env['events']) == 1
    env['census'][0]['name'] = 'New provenance to inspect'
    intake.reconcile(now=7300)
    assert len(env['events']) == 2


def test_skip_archived_polls_and_unassociated_helpers(env):
    env['rows'].append(outcome(sessions=[{'session_id': 'business'}]))
    env['census'].extend([session('archive', archived=True), session('trash', trashed=True),
        session('poll', empty_worker_poll=True), session('helper', helper=True, parent_session_id='unknown'),
        session('business-helper', helper=True, parent_session_id='business')])
    intake.reconcile(now=100)
    assert [a['session_id'] for a in env['associations']] == ['business-helper']
    assert not intake.view()['unfiled']


def test_parent_inheritance_does_not_walk_new_associations(env):
    env['rows'].append(outcome(sessions=[{'session_id': 'root'}]))
    env['census'].extend([session('a', parent_session_id='root'), session('b', parent_session_id='a')])
    intake.reconcile(now=100)
    assert [a['session_id'] for a in env['associations']] == ['a']
    assert intake.view()['unfiled'][0]['session_id'] == 'b'


@pytest.mark.parametrize('change', ['pending', 'unknown', 'revision', 'origin', 'request', 'expected', 'no-answer', 'duplicate'])
def test_unproven_answer_never_wakes(env, change):
    ref, req = decision(env)
    if change in ('pending', 'unknown'):
        req['answer']['state'] = change
    elif change == 'revision':
        req['revision'] = 8
    elif change == 'origin':
        req['origin_id'] = 'other'
    elif change == 'request':
        req['request_id'] = 'other'
    elif change == 'expected':
        req['answer']['expected_revision'] = 6
    elif change == 'no-answer':
        req['answer'] = None
    else:
        env['requests'].append(dict(req))
    intake.reconcile(now=100)
    assert not env['events'] and not intake.view()['receipts']


def test_exact_generation_with_distinct_source_revision(env):
    ref, req = decision(env, revision='episode-1')
    intake.reconcile(now=100)
    assert env['events'][0]['payload']['revision'] == 'episode-1'
    assert env['events'][0]['payload']['source_revision'] == 7
    assert intake.view()['receipts'][0]['generation'] == 'episode-1'


def test_explicit_generation_mismatch_rejected(env):
    decision(env, generation='wrong')
    intake.reconcile(now=100)
    assert not env['events']


def test_accepted_once_consumption_is_separate_and_each_tick_reads(env):
    ref, req = decision(env)
    intake.reconcile(now=100)
    ref['state'] = 'answered'
    assert intake.view()['receipts'][0]['session_consumed'] is False
    req['answer']['session_consumed'] = True
    intake.reconcile(now=101)
    assert len(env['events']) == 1
    receipt = intake.view()['receipts'][0]
    assert receipt['source_accepted'] is True and receipt['session_consumed'] is True
    assert receipt['text'] == 'Use option A.' and receipt['answer_id'] == 'answer-1'
    assert sum('decision-inbox' in c for c in env['calls']) == 2


def test_source_failures_preserve_unfiled_and_coverage(env, monkeypatch):
    env['census'].append(session('unrelated'))
    intake.reconcile(now=100)
    before = intake.view()['unfiled']
    decision(env)
    monkeypatch.setattr(intake, '_get', Mock(side_effect=OSError('offline')))
    intake.reconcile(now=3700)
    assert intake.view()['unfiled'] == before
    assert all(c['state'] == 'unavailable' for c in intake.view()['summary']['coverage'].values())
    assert not intake.view()['receipts']


def test_stale_source_no_wake(env, monkeypatch):
    decision(env)
    monkeypatch.setattr(intake, '_get', lambda path: dict(ok=True, sessions=[], requests=env['requests'], source_current=False))
    intake.reconcile(now=100)
    assert not env['events']
    assert intake.view()['summary']['coverage']['decisions']['state'] == 'unavailable'


def test_http_bounds_and_discovery(env, monkeypatch):
    from watchtower import messages
    # env patches _get; restore the actual implementation for this transport test.
    monkeypatch.setattr(intake, '_get', _REAL_GET)
    monkeypatch.setattr(messages, '_delegate_base', lambda: 'http://localhost:1234')
    response = Mock()
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=False)
    response.read.return_value = b'{"ok":true,"sessions":[]}'
    open_mock = Mock(return_value=response)
    monkeypatch.setattr(intake, 'urlopen', open_mock)
    assert intake._get('/api/sessions/census')['sessions'] == []
    open_mock.assert_called_once_with('http://localhost:1234/api/sessions/census', timeout=intake.HTTP_TIMEOUT)
    response.read.assert_called_once_with(intake.MAX_RESPONSE_BYTES + 1)
    response.read.return_value = b'x' * (intake.MAX_RESPONSE_BYTES + 1)
    with pytest.raises(ValueError, match='byte limit'):
        intake._get('/api/sessions/census')


def test_real_outcome_event_contract(tmp_path, monkeypatch):
    monkeypatch.setenv('WATCHTOWER_DATA_DIR', str(tmp_path))
    oc.set_settings(True)
    o, _ = oc.create(dict(title='Example', goal='Deliver example', criteria=['Verified'], state='active'))
    with oc._txn() as data:
        data['outcomes'][o['id']]['decision_refs'] = [dict(id='d1', request_id='r1', revision='episode-1', state='pending')]
    monkeypatch.setattr(intake, '_tickets', lambda: [])
    monkeypatch.setattr(intake, '_get', lambda path: dict(ok=True, sessions=[], source_current=True,
        requests=[dict(request_id='r1', revision=3, generation='episode-1',
                       answer=dict(state='accepted', answer_id='a1', text='Proceed.', expected_revision=3))]))
    intake.reconcile(now=100)
    intake.reconcile(now=101)
    saved = oc.get(o['id'])
    assert saved['decision_refs'][0]['state'] == 'answered'
    assert len([e for e in saved['events'] if e['type'] == 'owner_answer']) == 1


def test_corrupt_store_not_rewritten(env):
    intake._path().write_text('broken')
    with pytest.raises(ValueError):
        intake.reconcile(now=100)
    assert intake._path().read_text() == 'broken'


def test_dry_run_does_not_scan_or_save(env):
    assert intake.reconcile(dry_run=True)['enabled']
    assert not env['calls'] and not intake._path().exists()


def test_claimed_by_unique_name_only(env):
    env['rows'].append(outcome(links=[dict(kind='ticket', relation='serves', target='DEMO-1')]))
    env['tickets'].append(dict(ref='DEMO-1', claimed_by='worker-name'))
    env['census'].append(dict(session_id='session-1', name='worker-name'))
    intake.reconcile(now=100)
    assert env['associations'][0]['session_id'] == 'session-1'
    env['associations'].clear()
    env['census'].append(dict(session_id='session-2', name='worker-name'))
    intake.reconcile(now=3700)
    assert not env['associations']
    assert len(intake.view()['unfiled']) == 2


def test_decision_cap_rotates_without_waiting_for_census(env, monkeypatch):
    monkeypatch.setattr(intake, 'REQUEST_LIMIT', 1)
    decision(env)
    env['rows'][0]['decision_refs'].append(dict(id='decision-2', request_id='request-2', revision='7', state='pending'))
    env['requests'].append(dict(request_id='request-2', revision=7,
                               answer=dict(answer_id='answer-2', expected_revision=7, state='accepted', text='B')))
    intake.reconcile(now=100)
    assert len(env['events']) == 1
    intake.reconcile(now=101)
    assert len(env['events']) == 2
    assert sum('census' in p for p in env['calls']) == 1


def test_bounded_store_records(env, monkeypatch):
    monkeypatch.setattr(intake, 'RECORD_LIMIT', 2)
    env['census'].extend(session(f's{i}') for i in range(5))
    intake.reconcile(now=100)
    assert len(intake.view()['unfiled']) == 2


def test_pending_without_real_request_does_not_read_decisions(env):
    env['rows'].append(outcome(decision_refs=[dict(id='desk-1', state='pending')]))
    intake.reconcile(now=100)
    assert all('decision-inbox' not in p for p in env['calls'])

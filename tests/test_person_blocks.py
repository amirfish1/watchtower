"""WT-89: write barriers and fenced migrations on both storage backends."""

import pytest

from tests.test_owner_view import env  # noqa: F401
from tests.legacy_blocks import block as legacy_block
from watchtower import person_blocks as migration


def _ticket(q, name):
    it = q.enqueue(project=name, note="Finish original acceptance", source="test")
    q.claim_by_ref(it['ref'], 'w1')
    return it['ref']


@pytest.mark.parametrize('on', ['worker', 'dependency'])
def test_all_writer_paths_refuse_parking_without_mutation(env, on):
    q, cli, _, _, name = env
    ref = _ticket(q, name)
    before = q.get(ref)
    with pytest.raises(ValueError, match='--after'):
        q.block(ref, 'w1', question='Await coordinator', wait_on=on)
    backend = q._github_backend_for_project(name)
    if backend:
        with pytest.raises(ValueError, match='--after'):
            backend.block(ref, session_id='w1', question='Await coordinator', wait_on=on)
    assert q.get(ref)['history'] == before['history']
    q.block(ref, 'w1', origin='worker', question='Approve the exact unsent draft?', wait_on='owner')
    before = q.get(ref)
    with pytest.raises(ValueError, match='--after'):
        q.set_wait(ref, on)
    if backend:
        with pytest.raises(ValueError, match='--after'):
            backend.set_wait(ref, on)
    assert q.get(ref)['history'] == before['history']
    with pytest.raises(SystemExit):
        cli.main(['block', ref, '--worker', 'w1', '--question', 'Await coordinator', '--wait-on', on])


@pytest.mark.parametrize('question,on,actor', [
    ('', 'owner', ''), ('?', 'owner', ''),
    ('Await coordinator completion', 'owner', ''),
    ('Await the next natural customer event', 'owner', ''),
    ('Await OPS-123', 'owner', ''),
    ('Supply the missing facts?', 'external', ''),
    ('Supply the missing facts?', 'external', 'next natural visitor'),
    ('Supply the missing facts?', 'external', 'implementation worker'),
])
def test_worker_block_requires_person_question(env, question, on, actor):
    q, _, _, _, name = env
    ref = _ticket(q, name)
    with pytest.raises(ValueError):
        q.block(ref, 'w1', origin='worker', question=question, wait_on=on, wait_actor=actor)
    assert not q.get(ref).get('needs_input')


def test_real_approval_mentions_dependency_without_being_work_parking(env):
    q, _, _, _, name = env
    ref = _ticket(q, name)
    it = q.block(ref, 'w1', origin='worker', question='[Q10:send] Approve the exact unsent draft for OPS-123?',
                 wait_on='owner')
    assert it['wait']['on'] == 'owner'
    it = q.set_wait(ref, 'external', actor='Sonia')
    assert it['wait']['actor'] == 'Sonia'


def _operation(q, ref, action, **kwargs):
    item = q.get(ref)
    # A GitHub block's claimant may be hidden by the sidecar overlay. The
    # migration fences the issue's actual shared claim fields.
    item = dict(item, **(item.get('_issue_claim') or {}))
    return dict(ref=ref, expected=migration.fingerprint(item), action=action,
                reason='Reviewed original acceptance and explicit source-only holds.', **kwargs)


@pytest.mark.parametrize('action', ['dependency', 'reopen', 'person'])
def test_migration_preserves_guards_and_original_question_receipt(env, action):
    q, _, _, _, name = env
    dep = q.enqueue(project=name, note='Owning work', source='test')
    ref = _ticket(q, name)
    legacy_block(q, ref, 'w1', origin='worker', question='Await coordinator; no sends or replay authorized',
                 wait_on='worker')
    op = _operation(q, ref, action, **({'after': [dep['ref']]} if action == 'dependency'
                                      else {'on': 'owner'} if action == 'person' else {}))
    migration.apply(op, q.list_items(), 'migration-worker')
    it = q.get(ref)
    ev = [h for h in it['history'] if h['event'] == 'person_block_migrated'][-1]
    assert ev['previous_block']['block_question'] == 'Await coordinator; no sends or replay authorized'
    assert not any(h['event'] in ('answer','close') for h in it['history'])
    if action == 'person':
        assert it['needs_input'] and it['wait']['on'] == 'owner'
    else:
        assert it['status'] == 'open' and not it.get('needs_input')
        assert not it.get('parked') and not it.get('claimed_by')
        assert it.get('blocked_by') == ([dep['ref']] if action == 'dependency' else [])
        if action == 'dependency':
            assert q.blocker_verdict(it, {i['ref']: i for i in q.list_items()})[0] == 'waiting'


def test_migration_preserves_closure_requirement_for_retained_edges(env):
    q, _, _, _, name = env
    kept, removed = [q.enqueue(project=name, note=note, source='test')['ref']
                     for note in ('Retained prerequisite', 'Removed prerequisite')]
    ref = _ticket(q, name)
    legacy_block(q, ref, 'w1', question='Await coordinator', wait_on='worker')
    q.update(ref, blocked_by=[], blocked_by_until_closed=[kept, removed])

    op = _operation(q, ref, 'dependency', after=[kept])
    migration.apply(op, q.list_items(), 'migration-worker')

    migrated = q.get(ref)
    assert migrated['blocked_by'] == [kept]
    assert migrated['blocked_by_until_closed'] == [kept]
    assert q.blocker_verdict(migrated, {kept: {'status': 'awaiting_release'}}) == ('waiting', kept)


def test_migration_fences_changed_question_and_preserves_real_gate(env):
    q, _, _, _, name = env
    ref = _ticket(q, name)
    legacy_block(q, ref, 'w1', question='Await coordinator', wait_on='worker')
    op = _operation(q, ref, 'reopen')
    q.block(ref, 'w1', origin='worker', question='[Q11:money] Approve the reviewed money change?', wait_on='owner')
    with pytest.raises(ValueError, match='changed since review'):
        migration.apply(op, q.list_items(), 'migration-worker')
    assert q.get(ref)['needs_input'] and '[Q11:money]' in q.get(ref)['block_question']


def _sidecar_migration(tmp_path, monkeypatch):
    from tests.test_github_pipeline_sidecar import _claimed_gated, _park_in_sidecar

    q, ref = _claimed_gated(tmp_path, monkeypatch)
    legacy_block(q, ref, 'w1', origin='worker', question='Await coordinator; no sends authorized',
                 wait_on='worker')
    carried = [{'gen': 3, 'question': 'Earlier gate?', 'answer': 'Source only'}]
    pending = dict(state='routing', gen=4, question='Reviewed gate?', answer='Keep all guards',
                   carried=carried)
    _park_in_sidecar(q, ref, pending_answer=pending, last_answer_gen=4)
    return q, ref, pending


@pytest.mark.parametrize('action', ['dependency', 'reopen'])
@pytest.mark.parametrize('state', ['routing', 'delivering', 'queued', 'delivered', 'affinity', 'handed_off'])
def test_sidecar_migration_hands_off_answer_with_history(tmp_path, monkeypatch, action, state):
    from watchtower import github_pipeline
    q, ref, pending = _sidecar_migration(tmp_path, monkeypatch)
    pending['state'] = state
    github_pipeline.mutate(ref, lambda it: it['pending_answer'].update(state=state))
    monkeypatch.setenv('WATCHTOWER_STRICT_EDGES', '1')
    dep = q.enqueue(project='GHI', note='Owning prerequisite', source='test')
    op = _operation(q, ref, action, **({'after': [dep['ref']]} if action == 'dependency' else {}))
    history = list(q.get(ref)['history'])
    cancelled = []
    monkeypatch.setattr(q, '_supersede_outbox', cancelled.append)
    migration.apply(op, q.list_items(), 'migration-worker')
    item = q.get(ref)
    assert item['status'] == 'open' and not item.get('parked') and not item.get('needs_input')
    assert item['pending_answer']['state'] == 'handed_off'
    for field in ('gen', 'question', 'answer', 'carried'):
        assert item['pending_answer'][field] == pending[field]
    assert item['last_answer_gen'] == 4
    assert all(event in item['history'] for event in history)
    assert any(h['event'] == 'person_block_delivery_released' and h['gen'] == 4 for h in item['history'])
    assert cancelled == [ref]
    if action == 'dependency':
        assert q.blocker_verdict(item, {i['ref']: i for i in q.list_items()})[0] == 'waiting'
    else:
        claimed = q.claim_by_ref(ref, 'replacement-worker')
        assert claimed['pending_answer']['answer'] == pending['answer']
        assert claimed['pending_answer']['carried'] == pending['carried']


@pytest.mark.parametrize('change', ['answer', 'answer-state', 'park', 'pipeline', 'reblock', 'foreign', 'missing'])
def test_sidecar_migration_refuses_changed_delivery(tmp_path, monkeypatch, change):
    import copy
    from watchtower import github_pipeline
    q, ref, _ = _sidecar_migration(tmp_path, monkeypatch)
    op = _operation(q, ref, 'reopen')
    real_mutate = github_pipeline.mutate
    retained = {}

    def raced(ident, fn, **kwargs):
        def replace(it):
            if change == 'answer':
                it['pending_answer'].update(gen=5, answer='New answer')
            elif change == 'answer-state':
                it['pending_answer']['state'] = 'delivering'
            elif change == 'park':
                it['parked']['session_id'] = 'replacement-session'
        if change in ('answer', 'answer-state', 'park'):
            real_mutate(ident, replace)
        else:
            from tests.test_github_pipeline_sidecar import _sidecar
            import json
            p = _sidecar(q)
            data = json.loads(p.read_text())
            if change == 'pipeline':
                next(iter(data['items'].values()))['gen'] += 1
                p.write_text(json.dumps(data))
            elif change == 'missing':
                data['items'].clear()
                p.write_text(json.dumps(data))
            elif change == 'foreign':
                q._github_backend_for_project('GHI').update(ref, pipeline_host='another-host/owner')
            else:
                q._github_backend_for_project('GHI').update(ref, needs_input=True,
                                                         block_question='Approve the exact new gate?')
        retained.update(copy.deepcopy(github_pipeline._read_strict()))
        return real_mutate(ident, fn, **kwargs)

    monkeypatch.setattr(github_pipeline, 'mutate', raced)
    with pytest.raises(ValueError, match='changed during|reblocked during|owned by host|missing'):
        migration.apply(op, q.list_items(), 'migration-worker')
    assert github_pipeline._read_strict() == retained


def test_cycle_refused_before_ticket_changes(env):
    q, _, _, _, name = env
    ref = _ticket(q, name)
    legacy_block(q, ref, 'w1', question='Await coordinator', wait_on='worker')
    op = _operation(q, ref, 'dependency', after=[ref])
    with pytest.raises(ValueError, match='itself'):
        migration.apply(op, q.list_items(), 'migration-worker')
    assert q.get(ref)['needs_input']


@pytest.mark.parametrize('action', ['retain', 'person'])
def test_old_human_block_releases_in_progress_claim_without_answer(env, action):
    q, _, _, _, name = env
    ref = _ticket(q, name)
    legacy_block(q, ref, 'w1', origin='system', question='Approve the exact unsent draft?', wait_on='owner')
    assert q.get(ref)['status'] == 'in_progress'
    op = _operation(q, ref, action, on='owner')
    migration.apply(op, q.list_items(), 'migration-worker')
    item = q.get(ref)
    assert item['status'] != 'in_progress' and not item.get('claimed_by')
    assert item['needs_input'] and item['block_question'] == 'Approve the exact unsent draft?'
    assert not any(h['event']=='answer' for h in item['history'])


def test_owner_pause_iceboxes_without_claim_or_acceptance_claim(env):
    q, _, _, _, name = env
    ref = _ticket(q, name)
    legacy_block(q, ref, 'w1', origin='worker', question='Parked by owner until a business is lined up',wait_on='external',wait_actor='first real business')
    migration.apply(_operation(q, ref, 'icebox'), q.list_items(), 'migration-worker')
    item = q.get(ref)
    assert item['status'] == 'open' and item['readiness']=='needs-rationale'
    assert not item.get('needs_input') and not item.get('claimed_by')
    assert q.count_claimable(project=name) == 0


def test_release_reason_migration_preserves_owed_acceptance_and_fences_generation(env):
    q, _, _, _, name = env
    ref = _ticket(q, name)
    # Seed legacy release metadata directly. Integration/observe behavior is
    # independently tested with real git repos in test_await_release.
    wait = dict(commit='a'*40,branch='origin/main',target='watchtower:local',reason='then natural QA acceptance')
    backend=q._github_backend_for_project(name)
    if backend:
        from watchtower.github_backend import _split_body
        i=backend.get(ref);body,meta=_split_body(i['_github_body']);meta['release_wait']=wait
        backend._edit_body(i,body,meta)
    else:
        with q._FileLock(q._lock_path()):
            data=q._load_unlocked();it=next(i for i in data['items'] if i['ref']==ref)
            it.update(status='awaiting_release',release_wait=wait)
            q._save_unlocked(data)
    migration.release_reason(ref,wait,'migration-worker')
    item=q.get(ref)
    assert 'natural' not in item['release_wait']['reason']
    assert any(h['event']=='progress' and 'then natural QA acceptance' in h.get('text','') for h in item['history'])
    with pytest.raises(ValueError,match='generation changed'):
        migration.release_reason(ref,wait,'migration-worker')


def test_reviewed_question_clarification_preserves_tag_and_original_receipt(env):
    q, _, _, _, name = env
    ref=_ticket(q,name)
    legacy_block(q,ref,'w1',question='[Q12:login] Parked off the owner list: log in when convenient',wait_on='owner',origin='worker')
    op=_operation(q,ref,'person',on='owner',question='[Q12:login] Sign in to the existing account, or explicitly retire this task?')
    wrong=dict(op,question='[Q13:other] Approve another action?')
    with pytest.raises(ValueError,match='preserve'):
        migration.apply(wrong,q.list_items(),'migration-worker')
    migration.apply(op,q.list_items(),'migration-worker')
    it=q.get(ref)
    assert it['needs_input'] and it['block_question']==op['question']
    receipt=next(h for h in reversed(it['history']) if h['event']=='person_block_migrated')
    assert 'when convenient' in receipt['previous_block']['block_question']


def test_convenience_parking_is_not_a_new_person_question(env):
    q, _, _, _, name=env
    ref=_ticket(q,name)
    with pytest.raises(ValueError,match='convenience'):
        q.block(ref,'w1',question='Parked off the owner list: do this when convenient',wait_on='owner')


@pytest.mark.parametrize('protection', ['expired','alive','recent','foreign','replacement','question','answer','ps-failed','heartbeat','postwrite-conflict'])
def test_github_lease_recovery_rechecks_proof_under_serialized_writer(env,monkeypatch,protection):
    from watchtower import liveness as lv
    q, _, _, _, name=env
    backend=q._github_backend_for_project(name)
    if backend is None:
        pytest.skip('GitHub cross-host writer contract')
    ref=_ticket(q,name);item=q.get(ref)
    expected=lv._expect(item,{ref:item})
    monkeypatch.setattr(lv,'_store_activity',lambda i: 0 if protection!='recent' else __import__('time').time())
    monkeypatch.setattr(lv,'_file_activity',lambda i,c:0)
    monkeypatch.setattr(lv,'claim_heartbeat_age',lambda i,now:0 if protection=='heartbeat' else None)
    monkeypatch.setattr(lv.ResolverContext,'ps',lambda ctx:None if protection=='ps-failed' else [])
    monkeypatch.setattr(lv,'claim_owner',lambda i,ctx:lv.Verdict('alive' if protection=='alive' else 'unproven','','test proof',None))
    if protection=='foreign':
        monkeypatch.setattr(q,'machine_tag',lambda:'another-host')
    if protection=='replacement':
        expected=dict(expected,claimed_by='previous-worker')
    if protection=='question':
        q.block(ref,'w1',question='Approve this exact consequential operation?',wait_on='owner',origin='worker')
    if protection=='answer':
        # A routed answer generation cannot be handed to a competing worker.
        from watchtower.github_backend import _split_body, _serialized_body_write
        @_serialized_body_write
        def pending(b):
            current=b.get(ref);body,meta=_split_body(current['_github_body'])
            meta['pending_answer']={'state':'routing','gen':9}
            b._edit_body(current,body,meta)
        pending(backend)
    if protection=='postwrite-conflict':
        monkeypatch.setattr(type(backend),'update_status',lambda self,*a,**k:dict(item,claimed_by='replacement'))
    result=q.recover_claim(ref,expect=expected,action='reopen',reason='lease expired')
    if protection=='expired':
        assert result['status']=='open' and not result.get('claimed_by')
    else:
        assert result is None
        assert q.get(ref)['status']!='closed'


@pytest.mark.parametrize('host,alive,heartbeat,expected', [
    ('here',True,False,'pass'),('here',False,False,'fail'),
    ('remote',False,False,'fail'),('remote',False,True,'pass'),
])
@pytest.mark.parametrize('needs_input',[False,True])
def test_permanent_audit_cannot_pass_without_current_claim_liveness(wt_env,monkeypatch,host,alive,heartbeat,expected,needs_input):
    import runpy,time
    from pathlib import Path
    from datetime import datetime,timezone
    from watchtower import queue as q,liveness as lv,watches,config
    item=dict(ref='AUDIT-1',project='AUDIT',status='in_progress',claimed_by='worker',
              needs_input=needs_input,block_question='Approve the exact proposed operation?',wait={'on':'owner'},
              claimed_machine=q.machine_tag() if host=='here' else 'another-host',
              claimed_session_id='bound-session',claimed_at='2000-01-01T00:00:00Z',updated_at='2000-01-01T00:00:00Z')
    if heartbeat:
        item['claim_heartbeat']=dict(machine=item['claimed_machine'],claimed_by='worker',session_id='bound-session',
                                    at=datetime.now(timezone.utc).isoformat())
    monkeypatch.setattr(watches,'inventory',lambda:([item],[]))
    monkeypatch.setattr(config,'ticket_is_archived',lambda i:False)
    monkeypatch.setattr(lv,'claim_owner',lambda i,ctx:lv.Verdict('alive' if alive else 'unproven','','proof',None))
    namespace=runpy.run_path(str(Path(__file__).resolve().parents[1]/'scripts/audit-no-parking.py'))
    assert namespace['audit']()['status']==expected


@pytest.mark.parametrize('question,expected', [
    ('Choose whether to release the unstaffed work?', 'pass'),
    ('Await coordinator completion', 'fail'),
])
def test_audit_unclaimed_escalation_checks_person_gate_not_worker(wt_env, monkeypatch, question, expected):
    import runpy
    from pathlib import Path
    from watchtower import queue as q, liveness as lv, watches
    item = dict(ref='AUDIT-1', project='AUDIT', status='open',
                updated_at='2000-01-01T00:00:00Z')
    q._recover_escalate_unlocked(item, '2000-01-01T00:00:00Z', question)
    assert item['status'] == 'in_progress' and not item.get('claimed_by')
    monkeypatch.setattr(watches, 'inventory', lambda: ([item], []))
    def no_worker_probe(*args):
        raise AssertionError('An unclaimed escalation has no worker to probe')
    monkeypatch.setattr(lv, 'claim_owner', no_worker_probe)
    audit = runpy.run_path(str(Path(__file__).resolve().parents[1]/'scripts/audit-no-parking.py'))['audit']
    assert audit()['status'] == expected


def test_audit_stale_session_only_claim_still_fails(wt_env, monkeypatch):
    import runpy
    from pathlib import Path
    from watchtower import liveness as lv, watches
    item = dict(ref='AUDIT-1', project='AUDIT', status='in_progress',
                claimed_session_id='legacy-session', updated_at='2000-01-01T00:00:00Z')
    monkeypatch.setattr(watches, 'inventory', lambda: ([item], []))
    monkeypatch.setattr(lv, 'claim_owner', lambda i, ctx: lv.Verdict('unproven', '', 'missing process', None))
    audit = runpy.run_path(str(Path(__file__).resolve().parents[1]/'scripts/audit-no-parking.py'))['audit']
    result = audit()
    assert result['status'] == 'fail'
    assert result['evidence']['violations'] == [dict(ref=item['ref'], reason='stale local claim: missing process')]


@pytest.mark.parametrize('protection',['expired','alive','recent','foreign','new-question','answer'])
def test_expired_github_person_claim_parks_without_answering_or_losing_gate(env,monkeypatch,protection):
    from watchtower import liveness as lv
    q,_,_,_,name=env;backend=q._github_backend_for_project(name)
    if backend is None:
        pytest.skip('GitHub legacy human claim')
    ref=_ticket(q,name)
    q.block(ref,'w1',question='[Q42:release] Approve this exact production release?',wait_on='owner',origin='worker')
    before=q.get(ref)
    fields=('status','claimed_by','claimed_session_id','claimed_machine','needs_input','claimed_at','blocked_at','block_question','wait')
    expect={k:before.get(k) for k in fields}
    monkeypatch.setattr(lv,'_store_activity',lambda i:0 if protection!='recent' else __import__('time').time())
    monkeypatch.setattr(lv,'_file_activity',lambda i,c:0)
    monkeypatch.setattr(lv,'claim_heartbeat_age',lambda i,now:None)
    monkeypatch.setattr(lv.ResolverContext,'ps',lambda ctx:[])
    monkeypatch.setattr(lv,'claim_owner',lambda i,c:lv.Verdict('alive' if protection=='alive' else 'unproven','','proof',None))
    if protection=='alive':
        assert lv.assess(before,{ref:before},lv.ResolverContext())['verdict']=='alive'
    if protection=='foreign':
        monkeypatch.setattr(q,'machine_tag',lambda:'other-host')
    if protection=='new-question':
        expect['block_question']='An obsolete question'
    if protection=='answer':
        from watchtower.github_backend import _split_body,_serialized_body_write
        @_serialized_body_write
        def pending(b):
            i=b.get(ref);body,meta=_split_body(i['_github_body'])
            meta['pending_answer']={'state':'routing','gen':7}
            b._edit_body(i,body,meta)
        pending(backend)
    if protection=='expired':
        action=lv.assess(before,{ref:before},lv.ResolverContext())
        assert action['action']=='park_person_claim'
        assert lv._apply(before,action,{ref:before},set())=='done'
        result=q.get(ref)
    else:
        result=backend.park_expired_person_claim(ref,expect=expect)
    if protection=='expired':
        assert result and result['needs_input'] and not result.get('claimed_by')
        assert result['block_question']==before['block_question'] and result['wait']==before['wait']
        assert result['claimed_session_id']==before['claimed_session_id']
        assert not any(h['event']=='answer' for h in result['history'])
    else:
        assert result is None
        assert q.get(ref)['claimed_by']=='w1'

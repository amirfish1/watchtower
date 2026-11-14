"""Isolated native stores and receipt-backed takeover transitions; no live sessions."""
import importlib
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import pytest
from watchtower import takeover as t, observations as o, queue as q, liveness

SID='11111111-2222-4333-8444-555555555555'
OTHER='22222222-2222-4333-8444-555555555555'

@pytest.fixture
def env(wt_env, tmp_path, monkeypatch):
    monkeypatch.setenv('CODEX_HOME',str(tmp_path/'codex'))
    monkeypatch.setenv('WATCHTOWER_DELEGATE_URL','off')
    monkeypatch.setenv('WATCHTOWER_CLAUDE_PROJECTS_DIR',str(tmp_path/'claude-home/projects'))
    root=Path(o._roots()['claude'])/'repo'; root.mkdir(parents=True)
    path=root/f'{SID}.jsonl'
    path.write_text(json.dumps(dict(type='assistant',sessionId=SID,cwd=str(tmp_path),timestamp='2026-10-01T00:00:00Z',uuid='turn-1',message=dict(stop_reason='end_turn',content=[dict(type='text',text='Working')])) )+'\n')
    ledger={}; sends=[]
    monkeypatch.setattr(liveness,'delivery_row',lambda key:ledger.get(key))
    def deliver(target,text,**kw):
        sends.append((target,text,kw))
        ledger[kw['dedupe_key']]={'state':'pending'}
        return dict(ok=True,state='pending')
    monkeypatch.setattr(liveness,'deliver',deliver)
    return tmp_path,path,ledger,sends


def adopt(env):
    return t.takeover(SID,queue='TEST',title='Finish the authorized task',engine='claude')


def test_atomic_duplicate_and_no_staffing(env):
    a=adopt(env)
    with ThreadPoolExecutor(4) as pool:
        rows=list(pool.map(lambda _:t.takeover(SID,a['ticket'],engine='claude'),range(8)))
    assert {r['supervision_id'] for r in rows}=={a['supervision_id']}
    assert len(q.list_items())==1
    item=q.get(a['ticket'])
    assert item['status']=='in_progress' and item['claim_proc']['bound']=='ambient'
    assert q.count_claimable(project='TEST')==0
    from watchtower import workers
    assert workers.list_workers(prune=False)==[]
    assert env[3]==[]


def test_dry_run_has_no_ticket_or_index(env):
    result=t.takeover(SID,queue='TEST',title='Task',engine='claude',dry_run=True)
    assert result['dry_run'] and q.list_items()==[]
    assert not (o.storage.data_dir()/'observations').exists()


def test_conflicts(env):
    item=q.enqueue(project='TEST',note='owned',repo_path=str(env[0]))
    q.claim_by_ref(item['ref'],OTHER,session_uuid=OTHER)
    with pytest.raises(ValueError,match='claimant'):
        t.takeover(SID,item['ref'],engine='claude')
    wrong=q.enqueue(project='TEST',note='wrong repo',repo_path='/different/repository')
    with pytest.raises(ValueError,match='repositories'):
        t.takeover(SID,wrong['ref'],engine='claude')
    with pytest.raises(ValueError,match='specify'):
        t.takeover(SID,engine='claude')
    with pytest.raises(ValueError,match='broker unavailable'):
        t.takeover(SID,item['ref'],engine='kimi')


def test_restart_and_receipt_recovery(env):
    a=adopt(env); importlib.reload(t)
    t.reconcile(); assert len(env[3])==1
    importlib.reload(t); t.reconcile(); assert len(env[3])==1
    key=a['supervision']['intent']['key']; env[2][key]={'state':'confirmed'}
    t.reconcile()
    assert t.status(SID)['state']=='running'
    assert t.status(SID)['delivery_state']=='confirmed'


def test_cancel_keeps_claim_and_fences_pending(env):
    a=adopt(env); c=t.cancel(SID)
    assert c['generation']==a['generation']+1
    t.reconcile(); assert not env[3]
    assert q.get(a['ticket'])['status']=='in_progress'
    b=t.takeover(SID,a['ticket'],engine='claude')
    assert b['generation']>c['generation']
    assert b['supervision']['intent']['key']!=a['supervision']['intent']['key']


def test_cancel_serializes_dispatch(env,monkeypatch):
    adopt(env); entered=threading.Event(); release=threading.Event()
    def send(*a,**kw):
        entered.set(); assert release.wait(5); return dict(ok=True)
    monkeypatch.setattr(liveness,'deliver',send)
    with ThreadPoolExecutor(2) as pool:
        sending=pool.submit(t.reconcile)
        assert entered.wait(5)
        cancelling=pool.submit(t.cancel,SID)
        assert not cancelling.done()
        release.set(); sending.result(); cancelling.result()
    assert t.status(SID)['state']=='cancelled'
    t.reconcile()


def test_busy_unknown_and_questions_never_send(env):
    a=adopt(env)
    with env[1].open('a') as f:
        f.write(json.dumps(dict(type='user',sessionId=SID,message=dict(content='new work')))+'\n')
    t.reconcile(); assert not env[3]
    env[1].unlink(); t.reconcile()
    assert t.status(SID)['state']=='paused_unreachable'
    assert not env[3]


def test_answer_retains_original_session_no_fallback(env):
    a=adopt(env)
    q.block(a['ticket'],session_id=SID,question='Choose?',origin='worker')
    t.reconcile(); assert not env[3]
    assert t.status(SID)['state']=='needs_input'
    q.answer(a['ticket'],'Continue')
    it=q.get(a['ticket'])
    assert it['claimed_session_id']==SID and not it.get('pending_answer')
    env[1].unlink(); t.reconcile()
    assert q.get(a['ticket'])['status']=='in_progress'
    assert q.peek_next(project='TEST') is None
    assert not env[3]


def test_cancelled_answer_is_held(env):
    a=adopt(env); t.cancel(SID)
    q.answer(a['ticket'],'Remember this')
    assert t.status(SID)['supervision']['held_answer']=='Remember this'
    t.reconcile(); assert not env[3]


def test_takeover_answer_preserves_superseded_legacy_answer(env):
    a=adopt(env)
    q.block(a['ticket'],session_id=SID,question='Choose?',origin='worker')
    with q._FileLock(q._lock_path()):
        data=q._load_unlocked(strict=True)
        it=next(it for it in data['items'] if it['ref']==a['ticket'])
        q._write_pending_answer_unlocked(it, 'Earlier guidance', q._now_iso())
        q._save_unlocked(data)
    q.answer(a['ticket'], 'Continue with the new guidance')
    it=q.get(a['ticket'])
    assert not it.get('pending_answer')
    assert it['carried_answers'][-1]['answer']=='Earlier guidance'
    assert any(row['event']=='superseded' for row in it['history'])
    assert 'Continue with the new guidance' in it['supervision']['intent']['text']


def test_reminders_only_count_confirmed_and_bound(env):
    a=adopt(env); now=time.time()
    t.reconcile(now=now)
    key=a['supervision']['intent']['key']; env[2][key]={'state':'confirmed'}
    t.reconcile(now=now)
    for n in range(3):
        now+=1300
        t.reconcile(now=now)
        state=t.status(SID)
        assert state['supervision']['reminders']==n
        env[2][state['supervision']['intent']['key']]={'state':'confirmed'}
        t.reconcile(now=now)
    t.reconcile(now=now+1300)
    assert t.status(SID)['state']=='needs_input'
    assert len(env[3])==4
    t.reconcile(now=now+2600); assert len(env[3])==4


def test_lost_receipt_resends_once_then_cancels(env):
    a=adopt(env); t.reconcile()
    env[2][a['supervision']['intent']['key']]={'state':'lost','nonce':'<wt:n1>'}
    now=time.time()+5000
    t.reconcile(now=now)
    s=t.status(SID)['supervision']
    assert s['intent']['resent'] and s['intent']['key']!=a['supervision']['intent']['key']
    t.reconcile(now=now+1)
    assert len(env[3])==2
    env[2][s['intent']['key']]={'state':'lost','nonce':'<wt:n2>'}
    t.reconcile(now=now+5000)
    assert t.status(SID)['state']=='cancelled'
    assert len(env[3])==2


def test_legacy_import_idempotent_no_enrollment_send(env):
    it=q.enqueue(project='TEST',note='legacy',repo_path=str(env[0]))
    legacy=dict(migration_id='legacy:1',owner_fenced=True,state='waiting',reminders=2,
                next_deadline=123,processed_turn='old',wait_identity='dependency')
    a=t.takeover(SID,it['ref'],engine='claude',legacy=legacy)
    b=t.takeover(SID,it['ref'],engine='claude',legacy=legacy)
    assert a==b
    assert a['supervision']['reminders']==2 and a['delivery_state']=='none'
    t.reconcile(); assert not env[3]
    assert t.status(SID)['state']=='waiting'


def test_normal_closure_only(env):
    a=adopt(env)
    # A claimed resolution in a transcript must never trigger permissive close.
    t.reconcile()
    assert q.get(a['ticket'])['status']=='in_progress'
    q.close(a['ticket'],session_id=SID,resolution={'summary':'checked','no_code':True})
    t.reconcile()
    assert t.status(SID)['state']=='completed'


def test_candidates_off_and_source_fence(env):
    assert t.enroll_candidates()==[]
    with pytest.raises(ValueError,match='fenced'):
        t.replace_candidates('ccc',dict(schema_version=1,enabled=True))
    r=t.replace_candidates('ccc',dict(schema_version=1,enabled=True,delegation_fenced=True,scopes=[dict(repo_path=str(env[0]),queue='TEST',candidates=[dict(session_id=SID,engine='claude')])]))
    assert r['candidates']==1
    assert t.enroll_candidates()[0]['state']=='ineligible'
    assert not q.list_items()


def test_cli_status_and_dry_run(env,capsys):
    from watchtower.cli import main
    assert main(['takeover','status','--json'])==0
    assert json.loads(capsys.readouterr().out)['schema_version']==1
    assert main(['takeover',SID,'--queue','TEST','--title','Task','--dry-run','--json'])==0
    assert json.loads(capsys.readouterr().out)['dry_run']


def test_native_incremental_codex(env,monkeypatch):
    root=o._roots()['codex']/'2026/10/31';root.mkdir(parents=True)
    p=root/f'rollout-{SID}.jsonl'
    events=[dict(type='session_meta',payload=dict(id=SID,cwd=str(env[0]))),
            dict(type='event_msg',payload=dict(type='task_started',turn_id='a'))]
    p.write_text(''.join(json.dumps(e)+'\n' for e in events))
    assert o.observe(SID,'codex')['state']=='busy'
    with p.open('a') as f:
        f.write(json.dumps(dict(type='event_msg',payload=dict(type='task_complete',turn_id='a',last_agent_message='done')))+'\n')
    a=o.observe(SID,'codex');assert a['completed_turn']=='a' and a['state']=='idle'
    original=Path.open
    def guarded(self,*args,**kwargs):
        assert self!=p,'unchanged native store reread'
        return original(self,*args,**kwargs)
    monkeypatch.setattr(Path,'open',guarded)
    assert o.observe(SID,'codex')['cursor']==a['cursor']


def test_ambiguous_identity_and_malformed_footer(env):
    root=o._roots()['codex']/'2026/10/31'; root.mkdir(parents=True)
    (root/f'rollout-{SID}.jsonl').write_text('{}\n')
    assert o.observe(SID)['state']=='unknown'
    assert 'ambiguous' in o.observe(SID)['error']
    assert o.footer('<session-state>\nCOMPLETION: {}\nNEXT_STEP_USER: None\n</session-state>') is None


def test_retained_answer_survives_explicit_reenrollment(env):
    a=adopt(env);t.cancel(SID);q.answer(a['ticket'],'Keep this answer')
    b=t.takeover(SID,a['ticket'],engine='claude')
    assert 'Keep this answer' in b['supervision']['intent']['text']


def test_import_question_and_unfenced_import(env):
    it=q.enqueue(project='TEST',note='legacy',repo_path=str(env[0]))
    with pytest.raises(ValueError,match='owner_fenced'):
        t.takeover(SID,it['ref'],engine='claude',legacy=dict(migration_id='x'))
    t.takeover(SID,it['ref'],engine='claude',legacy=dict(migration_id='x',owner_fenced=True,state='needs_input'))
    t.reconcile(now=time.time()+100000)
    assert not env[3] and t.status(SID)['state']=='needs_input'


def test_dependency_wake_uses_retained_answer_route(env):
    a=adopt(env)
    dep=q.enqueue(project='TEST',note='Dependency')
    q.block(a['ticket'],session_id=SID,question='Wait',origin='worker',wait_on='dependency',wait_ref=dep['ref'])
    t.reconcile()
    assert not env[3]
    q.close(dep['ref'],resolution={'summary':'done','no_code':True})
    from watchtower import answers
    answers.resume_dependency_waits()
    it=q.get(a['ticket'])
    assert it['claimed_session_id']==SID and it['supervision']['intent']['kind']=='answer'


def test_native_scan_increment_budget_progresses(env,monkeypatch):
    monkeypatch.setattr(o,'MAX_READ',350)
    with env[1].open('a') as f:
        for i in range(10):
            f.write(json.dumps(dict(type='custom-title',customTitle='x'*100))+'\n')
    first=o.observe(SID,'claude')
    assert first['state']=='unknown'
    second=o.observe(SID,'claude')
    assert second['cursor']>first['cursor']


def test_same_session_delegate_protocol_guard(env,monkeypatch):
    from watchtower import messages
    monkeypatch.setattr(messages,'_delegate_base',lambda:'http://broker')
    sent=[]
    monkeypatch.setattr(messages,'_post_json',lambda url,payload,**kw:sent.append(payload) or dict(ok=True,same_session=True))
    monkeypatch.setattr(messages,'_get_json',lambda *a,**kw:None)
    messages._TAKEOVER_CAPABILITIES.clear()
    resolved=dict(session_id=SID,same_session=True)
    assert not messages._deliver_delegate(resolved,'text','send')['ok']
    assert not sent
    monkeypatch.setattr(messages,'_takeover_delegate_capable',lambda base:True)
    assert messages._deliver_delegate(resolved,'text','send')['ok']
    assert sent[0]['same_session'] and sent[0]['no_queue'] and sent[0]['skip_wt']


def test_real_receipt_ledger_survives_restart(env,monkeypatch):
    from watchtower import messages, receipts
    monkeypatch.setenv('WATCHTOWER_DELIVERIES_FILE',str(env[0]/'deliveries.json'))
    monkeypatch.setenv('WATCHTOWER_OUTBOX_FILE',str(env[0]/'outbox.json'))
    # Restore real ledger/send orchestration; only the native adapter is simulated.
    importlib.reload(liveness)
    def transport(resolved,text,mode,**kw):
        with env[1].open('a') as f:
            f.write(json.dumps(dict(type='user',sessionId=SID,cwd=str(env[0]),message=dict(content=text)))+'\n')
        return dict(ok=True,transport='fixture-native')
    monkeypatch.setattr(messages,'_deliver_unreceipted',transport)
    a=adopt(env);t.reconcile()
    assert t.status(SID)['delivery_state']=='pending'
    importlib.reload(t)
    liveness.sweep_deliveries()
    t.reconcile()
    assert t.status(SID)['delivery_state']=='confirmed'
    assert liveness.delivery_row(a['supervision']['intent']['key'])['state']=='confirmed'


def test_candidate_eligible_then_cancelled_is_suppressed(env):
    it=q.enqueue(project='TEST',note='Continue authorized implementation',repo_path=str(env[0]))
    completion=dict(version=1,scope=dict(goal='Finish implementation',closure_met=False),
      resolution=dict(score=.5),verification=dict(score=.5),delivery=dict(score=.5),decision_dependency=dict(score=0),
      caveats=[],remaining_actions=[dict(owner='agent',text='Run tests',within_scope=True)])
    body='<session-state>\nNEXT_STEP_USER: None\nCOMPLETION: '+json.dumps(completion)+'\n</session-state>'
    from datetime import datetime,timezone
    with env[1].open('a') as f:
        f.write(json.dumps(dict(type='assistant',sessionId=SID,cwd=str(env[0]),uuid='eligible',
         timestamp=datetime.fromtimestamp(time.time()-180,timezone.utc).isoformat(),
         message=dict(stop_reason='end_turn',content=[dict(type='text',text=body)])))+'\n')
    t.replace_candidates('ccc',dict(schema_version=1,enabled=True,delegation_fenced=True,
      scopes=[dict(repo_path=str(env[0]),queue='TEST',candidates=[dict(session_id=SID,engine='claude',
        ticket=it['ref'],task_brief='Continue authorized implementation',authorized=True)])]))
    assert t.enroll_candidates()[0]['state']=='pending_delivery'
    t.cancel(SID)
    assert t.enroll_candidates()[0]['state']=='ineligible'


def test_scope_only_reconcile_runs_takeover(env,monkeypatch):
    from watchtower import workers
    seen=[]
    monkeypatch.setattr(t,'reconcile',lambda **kw:seen.append(kw) or [])
    workers.reconcile_once(dry_run=True,only_queue='UNRELATED',supervise_stages=False)
    assert seen==[dict(dry_run=True)]


def test_gate_wait_and_resolution_do_not_remind_builder(env):
    a=adopt(env)
    t._update(a['ticket'],a['generation'],lambda i,s:i.update(status='in_review',gate_pending='verify'))
    t.reconcile(now=time.time()+5000)
    assert not env[3] and t.status(SID)['state']=='waiting'
    t._update(a['ticket'],a['generation'],lambda i,s:(i.update(status='open'),i.pop('gate_pending',None)))
    t.reconcile(now=time.time()+6000)
    assert q.get(a['ticket'])['claimed_session_id']==SID
    assert t.status(SID)['state']!='waiting'


def test_resolved_queue_wait_unpauses(env):
    a=adopt(env)
    q.block(a['ticket'],session_id=SID,question='Wait externally',wait_on='external',origin='worker')
    t.reconcile();assert t.status(SID)['state']=='waiting'
    t._update(a['ticket'],a['generation'],lambda i,s:i.update(needs_input=False,status='in_progress'))
    t.reconcile();assert t.status(SID)['state']!='waiting'


def test_footer_user_action_becomes_question(env):
    a=adopt(env)
    obs=o.observe(SID,'claude')
    obs.update(completed_turn='new',footer=dict(next_step_user='None',completion=dict(decision_dependency=dict(score=0),remaining_actions=[dict(owner='user',text='Choose the destination') ])))
    t._update(a['ticket'],a['generation'],lambda i,s:t._tick(i,obs,time.time()))
    assert t.status(SID)['state']=='needs_input'
    assert t.status(SID)['question']=='Choose the destination'


@pytest.mark.parametrize('style', ['next-step', 'dependency', 'user-action'])
def test_footer_owner_wait_exports_stable_episode(env, style):
    from watchtower import owner_blocks_export as export
    a = adopt(env)
    obs = o.observe(SID, 'claude')
    question = 'Choose the destination'
    footer = dict(next_step_user=question if style == 'next-step' else 'None',
                  completion=dict(decision_dependency=dict(score=1 if style == 'dependency' else 0),
                                  remaining_actions=[dict(owner='user', text=question)]
                                  if style == 'user-action' else []))
    obs.update(completed_turn='owner-turn', footer=footer)
    t._update(a['ticket'], a['generation'], lambda i, s: t._tick(i, obs, time.time()))
    first = export.snapshot()['items'][0]
    assert first['answerable'] is True
    assert first['generation'] and '.' in first['generation']
    assert first['expected_wait']['on'] == 'owner'
    assert first['expected_wait']['block_at'] == first['generation']
    assert first['expected_wait']['revision']
    t.reconcile()
    assert export.snapshot()['items'][0] == first
    assert not env[3]


def test_identical_footer_question_after_answer_is_new_episode(env, monkeypatch):
    from watchtower import conditional_answers as ca, owner_blocks_export as export
    from datetime import datetime, timezone
    class FrozenClock:
        @staticmethod
        def now(zone):
            return datetime(2026, 1, 15, 18, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(t, 'datetime', FrozenClock)
    a = adopt(env)
    obs = o.observe(SID, 'claude')
    obs.update(completed_turn='question-1', footer=dict(next_step_user='Choose?',
               completion=dict(decision_dependency=dict(score=1), remaining_actions=[])))
    t._update(a['ticket'], a['generation'], lambda i, s: t._tick(i, obs, time.time()))
    first = export.snapshot()['items'][0]
    q.answer(a['ticket'], 'Use blue')
    obs['completed_turn'] = 'question-2'
    t._update(a['ticket'], a['generation'], lambda i, s: t._tick(i, obs, time.time()))
    second = export.snapshot()['items'][0]
    assert second['generation'] != first['generation']
    assert second['expected_wait']['revision'] != first['expected_wait']['revision']
    request = {k: first[k] for k in ('expected_blocked_at', 'expected_block_question',
                                    'expected_wait_kind', 'expected_wait', 'expected_store_id')}
    request.update(idempotency_key='stale-answer', text='Use red')
    with pytest.raises(ca.Conflict):
        ca.submit(a['ticket'], request)
    assert ca.lookup(a['ticket'], 'stale-answer') is None
    assert len([h for h in q.get(a['ticket'])['history'] if h['event'] == 'answer']) == 1
    assert not env[3]


@pytest.mark.parametrize('on', ['owner', 'external', 'dependency', 'worker'])
def test_footer_adoption_preserves_existing_source_wait(env, on):
    import copy
    a = adopt(env)
    q.block(a['ticket'], session_id=SID, question='Existing source question', wait_on=on)
    before = q.get(a['ticket'])
    item = copy.deepcopy(before)
    footer = dict(next_step_user='A different footer question',
                  completion=dict(decision_dependency=dict(score=1), remaining_actions=[]))
    assert t._footer_wait(item, item['supervision'], footer, 'new-turn') is True
    for field in ('block_question', 'blocked_at', 'block_kind', 'wait', 'history'):
        assert item[field] == before[field]
    assert item['supervision']['state'] == ('needs_input' if on == 'owner' else 'waiting')


def test_footer_does_not_implicitly_initialize_legacy_wait(env):
    a = adopt(env)
    item = q.get(a['ticket'])
    item.update(needs_input=True, block_question='Existing legacy question', blocked_at=None,
                wait=dict(on='owner', block_at='old-episode', revision='old-revision'))
    footer = dict(next_step_user='Another question',
                  completion=dict(decision_dependency=dict(score=1), remaining_actions=[]))
    assert t._footer_wait(item, item['supervision'], footer, 'new-turn') is True
    assert item['blocked_at'] is None
    assert item['block_question'] == 'Existing legacy question'
    assert item['wait']['block_at'] == 'old-episode'


def test_adoption_footer_wait_exports_and_conditional_answer_replays(env, monkeypatch):
    from watchtower import conditional_answers as ca, owner_blocks_export as export
    original = o.observe
    def observed(*args, **kwargs):
        obs = original(*args, **kwargs)
        obs['footer'] = dict(next_step_user='Choose a destination',
            completion=dict(decision_dependency=dict(score=1), remaining_actions=[]))
        return obs
    monkeypatch.setattr(o, 'observe', observed)
    preview = t.takeover(SID, queue='TEST', title='Task', engine='claude', dry_run=True)
    assert preview['state'] == 'needs_input' and q.list_items() == []
    a = adopt(env)
    first = export.snapshot()['items'][0]
    request = {k: first[k] for k in ('expected_blocked_at', 'expected_block_question',
                                    'expected_wait_kind', 'expected_wait', 'expected_store_id')}
    request.update(idempotency_key='answer-once', text='  Use blue\nexactly  ')
    receipt = ca.submit(a['ticket'], request)
    assert receipt['accepted'] is True and receipt['work_completed'] is False
    assert ca.submit(a['ticket'], request) == receipt
    assert ca.lookup(a['ticket'], 'answer-once', first['expected_store_id']) == receipt
    item = q.get(a['ticket'])
    assert len([h for h in item['history'] if h['event'] == 'answer']) == 1
    assert item['status'] == 'in_progress' and not item['needs_input']
    assert not env[3]


def test_reminder_escalation_exports_owner_episode(env):
    from watchtower import owner_blocks_export as export
    a = adopt(env)
    obs = o.observe(SID, 'claude')
    def escalate(i, s):
        s.update(intent=None, reminders=t.MAX_REMINDERS, next_deadline=0)
        t._tick(i, obs, time.time() + t.IDLE_SECONDS + 1)
    t._update(a['ticket'], a['generation'], escalate)
    row = export.snapshot()['items'][0]
    assert row['answerable'] is True and row['generation']
    assert row['expected_wait']['revision']
    assert q.get(a['ticket'])['history'][-1]['event'] == 'takeover_escalation'
    assert not env[3]


def test_codex_racing_busy_turn_is_not_steered(env,monkeypatch):
    from watchtower import codex_rpc
    monkeypatch.setattr(codex_rpc,'is_available',lambda:True)
    calls=[]
    def request(method,*args,**kwargs):
        calls.append(method)
        return dict(result=dict(thread=dict(status=dict(type='active'),turns=[dict(id='busy',status='inProgress')])))
    monkeypatch.setattr(codex_rpc,'request',request)
    result=codex_rpc.deliver(SID,'do not interrupt',idle_only=True,session_settings=dict(model='original',approval_policy='on-request',sandbox_policy={'type':'read-only'}))
    assert not result['ok'] and calls==['thread/resume']


def test_capability_handshake_cached_and_old_broker_denied(env,monkeypatch):
    from watchtower import messages
    messages._TAKEOVER_CAPABILITIES.clear();calls=[]
    monkeypatch.setattr(messages,'_get_json',lambda url,**kw:calls.append(url) or dict(schema_version=1,same_session=True,no_queue=True))
    assert messages._takeover_delegate_capable('http://new')
    assert messages._takeover_delegate_capable('http://new')
    assert len(calls)==1
    monkeypatch.setattr(messages,'_get_json',lambda *a,**kw:dict(schema_version=1,same_session=True))
    assert not messages._takeover_delegate_capable('http://old')


def test_cancel_dry_run_does_not_change_generation(env):
    a=adopt(env)
    c=t.cancel(SID,dry_run=True)
    assert c['state']=='cancelled' and c['dry_run']
    assert t.status(SID)['generation']==a['generation']
    assert t.status(SID)['state']=='pending_delivery'


def test_dependency_gate_cannot_be_bypassed_at_adoption(env):
    dep=q.enqueue(project='TEST',note='dependency')
    it=q.enqueue(project='TEST',note='blocked work',blocked_by=[dep['ref']],repo_path=str(env[0]))
    with pytest.raises(ValueError,match='dependency gates'):
        t.takeover(SID,it['ref'],engine='claude')
    assert not q.get(it['ref']).get('supervision')


def test_partial_native_cache_preserves_completed_state(env,monkeypatch):
    monkeypatch.setattr(o,'MAX_READ',400)
    with env[1].open('a') as f:
        for i in range(8):f.write(json.dumps(dict(type='cost-state',padding='x'*100))+'\n')
    seen=[]
    for _ in range(12):
        obs=o.observe(SID,'claude');seen.append(obs['cursor'])
        if not obs['error']:break
    assert obs['state']=='idle' and obs['completed_turn']=='turn-1'
    assert seen==sorted(seen)


@pytest.mark.parametrize('source',['footer','legacy'])
def test_fresh_agent_work_clears_footer_or_legacy_wait(env,source):
    a=adopt(env)
    t._update(a['ticket'],a['generation'],lambda i,s:s.update(state='waiting',wait_source=source,wait_identity='old-trigger',intent=None))
    obs=o.observe(SID,'claude')
    obs.update(completed_turn='new-agent-turn',footer=dict(next_step_user='None',completion=dict(decision_dependency=dict(score=0),remaining_actions=[dict(owner='agent',within_scope=True,text='Run verification')])))
    now=time.time()
    t._update(a['ticket'],a['generation'],lambda i,s:t._tick(i,obs,now))
    result=t.status(SID)
    assert result['state']=='running' and result['wait_identity']==''
    assert result['supervision']['next_deadline']==now+t.IDLE_SECONDS


def test_codex_native_settings_preserved(env,monkeypatch):
    from watchtower import codex_rpc
    monkeypatch.setattr(codex_rpc,'is_available',lambda:True)
    calls=[]
    def request(method,params,**kwargs):
        calls.append((method,params))
        return dict(result=dict(thread=dict(status=dict(type='idle'),turns=[]),turn=dict(id='new')))
    monkeypatch.setattr(codex_rpc,'request',request)
    settings=dict(model='original-model',effort='low',approval_policy='on-request',
                  sandbox_policy=dict(type='workspace-write',writable_roots=['/authorized'],network_access=False))
    assert codex_rpc.deliver(SID,'continue',idle_only=True,session_settings=settings)['ok']
    resume,start=calls
    assert resume[1]['model']=='original-model' and resume[1]['approvalPolicy']=='on-request'
    assert start[1]['model']=='original-model' and start[1]['effort']=='low'
    assert start[1]['sandboxPolicy']==dict(type='workspaceWrite',writableRoots=['/authorized'],networkAccess=False)


def test_codex_existing_writer_prevents_private_resume(env,monkeypatch):
    from watchtower import codex_rpc,codex_app_worker
    monkeypatch.setattr(codex_app_worker,'find_worker',lambda sid:dict(pid=123))
    monkeypatch.setattr(codex_rpc,'request',lambda *a,**kw:pytest.fail('competing writer'))
    result=codex_rpc.deliver(SID,'continue',idle_only=True,session_settings=dict(model='original',approval_policy='on-request',sandbox_policy={'type':'read-only'}))
    assert not result['ok'] and result['stage']=='owner-check'


def test_uncertain_intent_without_receipt_row_resends_once(env):
    a=adopt(env); t.reconcile()
    env[2][a['supervision']['intent']['key']]={'state':'lost','nonce':'<wt:n1>'}
    now=time.time()+5000
    # Force the old failure shape: uncertain intent whose receipt row has aged out.
    def mark(i,r): r['intent']['state']='uncertain'; r['state']='paused_unreachable'
    t._update(a['ticket'],1,mark); env[2].clear()
    t.reconcile(now=now)
    assert t.status(SID)['supervision']['intent']['resent']

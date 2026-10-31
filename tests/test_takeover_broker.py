"""Engine-neutral takeover contract; all broker traffic stays in fixtures."""
import importlib
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pytest
from watchtower import observations as o, takeover as t, messages, receipts, liveness
from test_takeover import env, SID  # noqa: F401


def completion_text():
    completion = dict(version=1, scope=dict(goal='Finish authorized work', closure_met=False),
        resolution=dict(score=.5), verification=dict(score=.5), delivery=dict(score=0),
        decision_dependency=dict(score=0), caveats=[],
        remaining_actions=[dict(owner='agent', text='Run relevant checks', within_scope=True)])
    return '<session-state>\nNEXT_STEP_USER: None\nCOMPLETION: '+json.dumps(completion)+'\n</session-state>'


def observation(repo, sid='opaque-session', engine='new-engine', **kw):
    return dict(schema_version=1, session_id=sid, engine=engine, host='broker-host',
        repo_path=str(repo), transcript='adapter-store', cursor=10, completed_turn='turn-1',
        activity_at=time.time()-180, state='idle', settings=dict(model='original', permissions='retained'),
        observed_at=time.time(), source='registered-adapter', error='', completed_text=completion_text(), **kw)


def evidence(row, token, **kw):
    return dict(dict(session_id=row['session_id'], nonce=token, role='user', persisted=True,
                     source='persisted-test-events'), **kw)


@pytest.fixture
def broker(env, monkeypatch):
    state = dict(row=observation(env[0]), requests=[], sends=[], persisted=[], reject=False)
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass
        def respond(self, value):
            body = json.dumps(value).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        def do_GET(self):
            parsed = urlparse(self.path)
            state['requests'].append(self.path)
            if parsed.path.endswith('/capabilities'):
                self.respond(dict(schema_version=1, same_session=True, no_queue=True))
                return
            query = parse_qs(parsed.query)
            row = dict(state['row'])
            nonce = query.get('nonce', [''])[0]
            if nonce:
                landed = any(nonce in text for text in state['persisted'])
                row.update(receipt_confirmed=landed, receipt_evidence=evidence(row, nonce) if landed else None)
            self.respond(row)
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
            state['sends'].append(body)
            self.respond(dict(ok=not state['reject'], same_session=not state['reject'], queued=False))
    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    monkeypatch.setenv('WATCHTOWER_DELEGATE_URL', f'http://127.0.0.1:{server.server_port}')
    messages._TAKEOVER_CAPABILITIES.clear()
    yield state
    server.shutdown()
    server.server_close()
    thread.join(2)


@pytest.mark.parametrize('engine', ['devin', 'kimi', 'grok', 'gemini', 'antigravity', 'cursor', 'future-engine'])
def test_broker_observation_and_cli_manual_enrollment(env, broker, run_cli, engine):
    sid = 'devincli-fixture-literature' if engine == 'devin' else 'opaque /?+session'
    broker['row'] = observation(env[0], sid, engine)
    observed = o.observe(sid, engine)
    assert observed['state'] == 'idle' and observed['engine'] == engine
    assert observed['source'] == 'broker' and observed['footer']['completion']['scope']['goal']
    response = run_cli('takeover', sid, '--engine', engine, '--queue', 'TEST', '--title', 'Authorized task', '--json')
    assert response.code == 0, response.output
    result = json.loads(response.out)
    assert result['session']['session_id'] == sid and result['session']['engine'] == engine
    assert result['supervision']['settings'] == broker['row']['settings']
    assert not broker['sends']
    query = parse_qs(urlparse(broker['requests'][0]).query)
    assert query['session_id'] == [sid] and query['engine'] == [engine]


def test_unknown_id_queries_broker_without_native_engine_assumption(env, broker):
    broker['row'] = observation(env[0], 'no-native-record', 'registered-engine')
    result = o.observe('no-native-record')
    assert result['engine'] == 'registered-engine' and not result['error']
    discovered = o.discover(engine='registered-engine', session_ids=['no-native-record'])
    assert not discovered['observations'][0]['error']
    assert 'requires explicit session_ids' in o.discover(engine='registered-engine')['errors'][0]


def test_broker_native_timestamps_and_opaque_cursor(env, broker):
    broker['row'].update(observed_at='2026-10-31T12:00:00Z', activity_at='2026-10-31T11:57:00+00:00', cursor='event:123')
    observed = o.observe('opaque-session', 'new-engine')
    assert not observed['error'] and isinstance(observed['activity_at'], float)
    assert observed['cursor'] == 'event:123' and observed['observed_at'] > observed['activity_at']


def test_opaque_canonical_id_is_not_redirected_by_address_book(env, broker, monkeypatch):
    monkeypatch.setenv('WATCHTOWER_AGENTS_FILE', str(env[0]/'agents.json'))
    messages.register_agent('opaque-session', SID, engine='claude')
    result = messages.send('opaque-session', receipts.with_nonce('continue', receipts.make_nonce('exact-id')),
                           engine='new-engine', same_session=True, queue_on_fail=False)
    assert result['ok'] and broker['sends'][0]['session_id'] == 'opaque-session'


def test_registered_broker_agent_and_dry_run(env, broker, run_cli, monkeypatch):
    sid = 'devincli-fixture-literature'
    broker['row'] = observation(env[0], sid, 'devin')
    # The address book uses opaque broker session IDs, preserving native UUID validation.
    monkeypatch.setenv('WATCHTOWER_AGENTS_FILE', str(env[0]/'agents.json'))
    messages.register_agent('fixture-devin', sid, engine='devin')
    result = run_cli('takeover', '@fixture-devin', '--queue', 'TEST', '--title', 'Task', '--dry-run', '--json')
    assert result.code == 0, result.output
    assert json.loads(result.out)['session']['engine'] == 'devin'
    assert not t.q.list_items() and not broker['sends']
    adopted = t.takeover('@fixture-devin', queue='TEST', title='Task')
    assert adopted['session']['session_id'] == sid


def test_missing_native_session_falls_back_but_ambiguous_native_does_not(env, broker):
    broker['row'] = observation(env[0], 'absent-native', 'codex')
    assert o.observe('absent-native', 'codex')['source'] == 'broker'
    codex = o._roots()['codex']/'2026/10/31'
    codex.mkdir(parents=True)
    (codex/f'rollout-{SID}.jsonl').write_text('{}\n')
    broker['requests'].clear()
    assert 'ambiguous' in o.observe(SID)['error']
    assert not broker['requests']


@pytest.mark.parametrize('change', [dict(session_id='other'), dict(engine='other'),
    dict(schema_version=2), dict(repo_path=''), dict(host=''), dict(state='ready'),
    dict(activity_at='yesterday'), dict(observed_at=float('nan')), dict(settings=[]), dict(error='store unavailable')])
def test_bad_broker_observation_pauses(env, broker, change):
    broker['row'].update(change)
    result = o.observe('opaque-session', 'new-engine')
    assert result['state'] == 'unknown' and result['error']


def test_candidates_share_enrollment_state_machine_for_arbitrary_engine(env, broker, run_cli):
    it = t.q.enqueue(project='TEST', note='Authorized task', repo_path=str(env[0]))
    candidate = dict(session_id='opaque-session', engine='new-engine', ticket=it['ref'],
                     task_brief='Authorized task', authorized=True)
    policy = dict(schema_version=1, enabled=True, delegation_fenced=True,
        scopes=[dict(repo_path=str(env[0]), queue='TEST', candidates=[candidate])])
    published = run_cli('takeover', 'candidates', '--candidates-json', json.dumps(policy), '--json')
    assert published.code == 0 and json.loads(published.out)['candidates'] == 1
    assert t.enroll_candidates()[0]['state'] == 'pending_delivery'
    t.cancel('opaque-session')
    assert t.enroll_candidates()[0]['state'] == 'ineligible'
    assert not broker['sends']


@pytest.mark.parametrize('engine', ['claude', 'new-engine'])
@pytest.mark.parametrize('wait', ['question', 'decision', 'user-action', 'external'])
def test_adoption_honors_current_footer_before_first_delivery(env, broker, engine, wait):
    parsed = o.footer(completion_text())
    completion = parsed['completion']
    next_user = 'Choose a destination' if wait == 'question' else 'None'
    if wait == 'decision':
        completion['decision_dependency']['score'] = .5
    if wait in ('user-action', 'external'):
        completion['remaining_actions'] = [dict(owner='user' if wait == 'user-action' else 'external',
                                              text='Wait for named trigger', within_scope=True)]
    text = '<session-state>\nNEXT_STEP_USER: '+next_user+'\nCOMPLETION: '+json.dumps(completion)+'\n</session-state>'
    sid = SID if engine == 'claude' else 'opaque-session'
    def final(body, turn):
        if engine == 'claude':
            with env[1].open('a') as stream:
                stream.write(json.dumps(dict(type='assistant', sessionId=SID, cwd=str(env[0]), uuid=turn,
                    message=dict(stop_reason='end_turn', content=[dict(type='text', text=body)])))+'\n')
        else:
            broker['row'].update(completed_text=body, completed_turn=turn)
    final(text, 'waiting-turn')
    adopted = t.takeover(sid, queue='TEST', title='Task', engine=engine)
    assert adopted['state'] == ('waiting' if wait == 'external' else 'needs_input')
    assert adopted['supervision']['processed_turn'] == 'waiting-turn'
    t.reconcile(now=time.time()+5000)
    assert not env[3] and not broker['sends']
    if wait == 'external':
        final(completion_text(), 'authorized-agent-turn')
        t.reconcile()
        assert t.status(sid)['state'] == 'pending_delivery' and len(env[3]) == 1
    else:
        assert t.q.get(adopted['ticket'])['needs_input']


@pytest.mark.parametrize('confirmed,proof', [(True, None), (True, 'accepted'), (False, 'valid')])
def test_confirmation_requires_structured_persisted_evidence(env, monkeypatch, confirmed, proof):
    row = observation(env[0])
    nonce = receipts.make_nonce('confirmation-proof')
    row.update(receipt_confirmed=confirmed, receipt_evidence=evidence(row, nonce) if proof == 'valid' else proof)
    monkeypatch.setattr(messages, '_delegate_base', lambda: 'http://fixture')
    monkeypatch.setattr(messages, '_get_json', lambda *a, **kw: row)
    assert not o.broker_observe(row['session_id'], row['engine'], nonce=nonce)['receipt_confirmed']


@pytest.mark.parametrize('engine,sid', [('devin', 'devincli-fixture-literature'),
    ('new-engine', 'opaque-id'), ('new-engine', SID), ('gemini', 'opaque-gemini'),
    ('antigravity', 'opaque-antigravity')])
def test_actual_delegate_routing_and_durable_receipt(env, broker, monkeypatch, engine, sid):
    importlib.reload(liveness)  # restore real ledger orchestration from env fixture
    broker['row'] = observation(env[0], sid, engine)
    # Neither an unfamiliar UUID nor a legacy resume transport may substitute Claude.
    for name in ('_deliver_resume', '_deliver_fifo', '_deliver_gemini_resume', '_deliver_antigravity_language_server'):
        monkeypatch.setattr(messages, name, lambda *a, **kw: pytest.fail('wrong engine transport'))
    adopted = t.takeover(sid, queue='TEST', title='Authorized task', engine=engine)
    t.reconcile()
    body = broker['sends'][0]
    assert body['session_id'] == sid and body['engine'] == engine
    assert body['same_session'] and body['no_queue'] and body['skip_wt'] and body['origin'] == 'wt'
    assert body['session_settings'] == broker['row']['settings']
    key = adopted['supervision']['intent']['key']
    liveness.sweep_deliveries()
    assert liveness.delivery_row(key)['state'] == 'pending'  # HTTP acceptance is insufficient
    assert t.status(sid)['delivery_state'] == 'pending'
    broker['persisted'].append(body['text'])
    importlib.reload(receipts)
    importlib.reload(t)
    liveness.sweep_deliveries()
    t.reconcile()
    assert t.status(sid)['delivery_state'] == 'confirmed'
    assert liveness.delivery_row(key)['state'] == 'confirmed'
    rec = next(r for r in receipts.list_receipts() if r['sid'] == sid)
    assert rec['receipt_evidence']['nonce'] == rec['nonce'] and rec['source'] == 'broker'
    assert len(broker['sends']) == 1


@pytest.mark.parametrize('change', [dict(session_id='wrong-session'), dict(nonce='wrong-nonce'),
    dict(role='assistant'), dict(role='system'), dict(persisted=False), dict(source='')])
def test_nonce_proof_rejects_wrong_identity_or_echo(env, monkeypatch, change):
    row = observation(env[0])
    nonce = receipts.make_nonce('test-proof')
    row.update(receipt_confirmed=True, receipt_evidence=evidence(row, nonce, **change))
    monkeypatch.setattr(messages, '_delegate_base', lambda: 'http://fixture')
    monkeypatch.setattr(messages, '_get_json', lambda *a, **kw: row)
    rec = receipts.record(row['session_id'], receipts.with_nonce('continue', nonce), 'delegate', engine=row['engine'], nonce=nonce)
    receipts.sweep()
    assert receipts.get(rec['id'], refresh=False)['status'] == 'pending'


def test_broker_loss_retains_claim_and_pauses_without_replacement(env, broker, monkeypatch):
    sid = broker['row']['session_id']
    adopted = t.takeover(sid, queue='TEST', title='Authorized task', engine='new-engine')
    monkeypatch.setenv('WATCHTOWER_DELEGATE_URL', 'off')
    t.reconcile()
    assert t.status(sid)['state'] == 'paused_unreachable'
    assert t.q.get(adopted['ticket'])['claimed_session_id'] == sid
    assert t.q.count_claimable(project='TEST') == 0 and not broker['sends']
    monkeypatch.setenv('WATCHTOWER_DELEGATE_URL', 'http://fixture')
    monkeypatch.setattr(messages, '_get_json', lambda *a, **kw: broker['row'])
    t.reconcile()
    assert t.status(sid)['state'] == 'pending_delivery' and env[3]


@pytest.mark.parametrize('response', ['timeout', 'uncertain-rejection', 'wrong-session'])
def test_uncertain_broker_send_keeps_nonce_and_never_blindly_retries(env, broker, monkeypatch, response):
    importlib.reload(liveness)
    sid = broker['row']['session_id']
    def timeout(*a, **kw):
        if response == 'uncertain-rejection':
            return dict(ok=False, code='delivery_unknown', uncertain=True)
        if response == 'wrong-session':
            return dict(ok=True, same_session=True, session_id='wrong-session')
        raise TimeoutError('reply lost after possible persisted input')
    monkeypatch.setattr(messages, '_post_json', timeout)
    adopted = t.takeover(sid, queue='TEST', title='Authorized task', engine='new-engine')
    t.reconcile()
    key = adopted['supervision']['intent']['key']
    assert t.status(sid)['delivery_state'] == 'pending'
    row = liveness.delivery_row(key)
    assert row['receipt_id'] and row['delivery_uncertain']
    liveness.sweep_deliveries(now=time.time()+1300)
    t.reconcile(now=time.time()+1300)
    assert t.status(sid)['delivery_state'] == 'uncertain'
    assert t.status(sid)['state'] == 'paused_unreachable'
    assert not broker['sends']


def test_uncertain_send_can_confirm_original_nonce_after_restart(env, broker, monkeypatch):
    importlib.reload(liveness)
    sid = broker['row']['session_id']
    def accepted_then_timeout(url, body, **kw):
        broker['persisted'].append(body['text'])
        raise TimeoutError('reply lost')
    monkeypatch.setattr(messages, '_post_json', accepted_then_timeout)
    adopted = t.takeover(sid, queue='TEST', title='Task', engine='new-engine')
    t.reconcile()
    assert t.status(sid)['delivery_state'] == 'pending'
    importlib.reload(receipts)
    importlib.reload(t)
    liveness.sweep_deliveries()
    t.reconcile()
    assert t.status(sid)['delivery_state'] == 'confirmed'
    assert len(broker['persisted']) == 1


def test_definite_rejection_can_retry_with_backoff(env, broker, monkeypatch):
    importlib.reload(liveness)
    broker['reject'] = True
    sid = broker['row']['session_id']
    adopted = t.takeover(sid, queue='TEST', title='Task', engine='new-engine')
    t.reconcile()
    assert t.status(sid)['state'] == 'paused_unreachable'
    assert not receipts.list_receipts()
    t.reconcile()
    assert len(broker['sends']) == 1
    broker['reject'] = False
    t.reconcile(now=time.time()+61)
    assert len(broker['sends']) == 2 and t.status(sid)['delivery_state'] == 'pending'
    assert t.status(sid)['generation'] == adopted['generation']

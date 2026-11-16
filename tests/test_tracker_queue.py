"""Trackers keep durable records without starting workers or comment turns."""
import pytest
from watchtower import cli, config, queue as q, workers, messages, liveness


def test_config_refuses_drain_even_with_combined_flags(wt_env, capsys):
    assert cli.main(['config', '-q', 'LOG', '--kind', 'tracker']) == 0
    assert config.queue_kind('LOG') == 'tracker'
    assert not config.auto_drain('LOG')
    assert cli.main(['config', '-q', 'LOG', '--auto-drain', 'on']) == 1
    with pytest.raises(ValueError, match='tracker'):
        config.set_auto_drain('LOG', True)
    assert cli.main(['config', '-q', 'NEW', '--kind', 'tracker', '--auto-drain', 'on']) == 1
    assert config.queue_kind('NEW') == 'work'
    assert cli.main(['config', '-q', 'LOG', '--kind', 'work']) == 0
    config.set_auto_drain('LOG', True)
    assert config.auto_drain('LOG')


def test_claim_run_and_dispatch_boundaries(wt_env, monkeypatch):
    item = q.enqueue(project='LOG', note='Feature record')
    config.set_queue_kind('LOG', 'tracker')
    for operation in (
        lambda: q.claim_next('worker', project='LOG'),
        lambda: q.claim_by_ref(item['ref'], 'worker'),
        lambda: q.reopen_and_claim(item['ref'], 'worker', force=True),
        lambda: q.mark_runnable(item['ref']),
        lambda: q.update_status(item['ref'], 'in_progress', 'worker'),
        lambda: workers.spawn_workers('LOG', dry_run=True),
        lambda: workers.spawn_run_once_worker('LOG', item['ref']),
        lambda: workers.spawn_adhoc('Plan this', ticket_ref=item['ref'], dry_run=True),
    ):
        with pytest.raises(ValueError, match='tracker'):
            operation()
    assert q.claim_next('worker') is None
    assert q.count_claimable('LOG') == q.count_manual_eligible('LOG') == 0
    assert q.peek_next(project='LOG') is None
    assert q.floor_routed_candidates('LOG', '') == []
    monkeypatch.setattr(workers, 'reconcile_once', lambda **kw: pytest.fail('tracker dispatched'))
    assert 'tracker' in workers.dispatch_after_enqueue('LOG', item['ref'])
    assert not q.get(item['ref'])['claimed_by']
    assert not q.get(item['ref'])['run_requested']


def test_tracker_comments_and_close_notices_are_silent(wt_env, monkeypatch):
    item = q.enqueue(project='LOG', note='Legacy feature', submitter='filer', submitter_explicit=True)
    q.claim_by_ref(item['ref'], 'legacy')
    # Even stale ownership cannot turn tracker comments into a wake.
    config.set_queue_kind('LOG', 'tracker')
    monkeypatch.setattr(messages, 'deliver_message', lambda *a, **kw: pytest.fail('comment delivered'))
    assert cli.main(['comment', item['ref'], 'Durable feature update']) == 0
    current = q.get(item['ref'])
    assert current['history'][-1]['text'] == 'Durable feature update'
    monkeypatch.setattr(q, '_submitter_wants', lambda *a: pytest.fail('closed notice allowed'))
    q._notify_ticket_event(current, 'closed', detail='Done')
    # Liveness must not recover a legacy tracker claim into a new worker.
    monkeypatch.setattr(liveness, 'assess', lambda *a: pytest.fail('tracker backstop'))
    assert liveness.sweep() == []


def test_kind_transition_detaches_legacy_claims_preserving_wait(wt_env):
    item = q.enqueue(project='LOG', note='Legacy feature')
    q.claim_by_ref(item['ref'], 'legacy')
    with q._FileLock(q._lock_path()):
        data = q._load_unlocked()
        row = next(it for it in data['items'] if it['ref'] == item['ref'])
        row.update(needs_input=True, block_question='Keep this decision', blocked_at='2026-11-16T00:00:00Z')
        q._save_unlocked(data)
    assert cli.main(['config', '-q', 'LOG', '--kind', 'tracker']) == 0
    current = q.get(item['ref'])
    assert current['claimed_by'] == current['claimed_session_id'] == ''
    assert current['status'] == 'open'
    assert current['needs_input'] and current['block_question'] == 'Keep this decision'
    assert q.detach_tracker_claims('LOG') == []


def test_tracker_blocks_and_answers_never_create_worker_ownership(wt_env, monkeypatch):
    config.set_queue_kind('LOG', 'tracker')
    item = q.enqueue(project='LOG', note='Decision log')
    monkeypatch.setattr(q, '_notify_ticket_event', lambda *a, **kw: None)
    blocked = q.block(item['ref'], session_id='worker', question='Which option?', wait_on='worker', origin='worker')
    assert blocked['needs_input'] and blocked['status'] == 'open'
    assert not blocked['claimed_by'] and not blocked.get('parked')
    monkeypatch.setattr(messages, 'deliver_message', lambda *a, **kw: pytest.fail('tracker answer dispatched'))
    assert cli.main(['answer', item['ref'], 'Use the existing choice']) == 0
    current = q.get(item['ref'])
    assert not current['needs_input'] and not current['claimed_by']
    assert current['history'][-1]['event'] == 'answer'


def test_trackers_never_request_stage_sessions(wt_env):
    from watchtower import stages
    item = q.enqueue(project='LOG', note='Feature record')
    config.set_queue_kind('LOG', 'tracker')
    item.update(status='in_review', gate_pending='verify')
    assert stages.desired([item]) == []
    item.update(status='closed', assessment={'status': 'due'})
    assert stages.desired([item]) == []


def test_work_queue_can_depend_on_closed_tracker(wt_env):
    config.set_queue_kind('LOG', 'tracker')
    record = q.enqueue(project='LOG', note='Feature decision')
    q.close(record['ref'], resolution={'summary': 'Decision recorded', 'no_code': True})
    work = q.enqueue(project='BUILD', note='Implement decision', blocked_by=[record['ref']])
    assert q.claim_next('builder')['ref'] == work['ref']


def test_tracker_needs_input_notifications_remain_available(wt_env, monkeypatch):
    config.set_queue_kind('LOG', 'tracker')
    item = q.enqueue(project='LOG', note='Decision', submitter='filer', submitter_explicit=True)
    assert q._submitter_wants(item, 'needs_input', config)
    assert q._submitter_wants(item, 'awaits_decision', config)

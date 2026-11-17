"""WT-78: attribution is not subscription; dependencies are not questions."""
from __future__ import annotations

import importlib

import pytest

from test_ticket_notifications import wt_cli, wt, _record_sends  # noqa: F401
from test_github_backend import _install_fake_gh, _reload_isolated, _drainable
from tests.legacy_blocks import block as seed_legacy_block


@pytest.mark.parametrize('subscribe', [False, True])
@pytest.mark.parametrize('explicit', [False, True])
def test_cli_subscription_controls_all_filer_status_notices(wt_cli, monkeypatch, capsys, subscribe, explicit):
    wt = wt_cli
    sid = '11111111-2222-3333-4444-555555555555'
    monkeypatch.setattr(wt.cli, '_default_report_to', lambda: (sid, ''))
    calls = _record_sends(monkeypatch, wt.messages)
    argv = ['add', '-q', 'SUB', '--note', 'subscription', '--pre-ack']
    if subscribe:
        argv.append('--subscribe')
    if explicit:
        argv.extend(['--submitter', sid])
    assert wt.cli.main(argv) == 0
    capsys.readouterr()
    item = wt.q.list_items(project='SUB')[0]
    assert item['submitter'] == sid
    assert item['submitter_subscribed'] is subscribe
    wt.q.claim_by_ref(item['ref'], 'builder')
    for kind in ['input', 'rationale']:
        wt.q.block(item['ref'], 'builder', question='Which option?', kind=kind)
        wt.q.answer(item['ref'], 'A', session_id='human')
    wt.q.close(item['ref'], 'builder', resolution={'summary': 'done'})
    assert len(calls) == (3 if subscribe else 0)


def test_legacy_filer_is_silent_even_with_pre_ack_and_explicit_identity(wt, monkeypatch):
    calls = _record_sends(monkeypatch, wt.messages)
    item = {'project': 'SUB', 'ref': 'SUB-1', 'submitter': 'filer',
            'submitter_explicit': True, 'pre_ack': True}
    for event in ['claimed', 'closed', 'needs_input', 'awaits_decision']:
        wt.q._notify_ticket_event(item, event)
    assert calls == []


def test_subscribed_close_follows_a_continued_filer(wt, monkeypatch):
    calls = _record_sends(monkeypatch, wt.messages)
    item = wt.q.enqueue(project='SUB', note='follow me', submitter='old', submitter_subscribed=True)
    monkeypatch.setattr(wt.messages, 'ccc_forward_target', lambda target: 'continued' if target == 'old' else target)
    wt.q.claim_by_ref(item['ref'], 'builder')
    wt.q.close(item['ref'], 'builder', resolution={'summary': 'done'})
    assert [target for target, _ in calls] == ['continued']


@pytest.mark.parametrize('legacy_block', [False, True])
def test_edit_after_releases_claim_or_old_block_without_notices(wt_cli, monkeypatch, legacy_block):
    wt = wt_cli
    wt.config.add_subscriber('SUB', 'queue-watcher')
    dep = wt.q.enqueue(project='SUB', note='dependency')
    item = wt.q.enqueue(project='SUB', note='dependent', submitter='filer', submitter_subscribed=True)
    wt.q.claim_by_ref(item['ref'], 'builder')
    calls = _record_sends(monkeypatch, wt.messages)
    if legacy_block:
        seed_legacy_block(wt.q, item['ref'], 'builder', question='Waiting on dependency', origin='worker',
                   wait_on='dependency', wait_ref=dep['ref'])
    assert calls == []
    assert wt.cli.main(['edit', item['ref'], '--after', dep['ref']]) == 0
    cur = wt.q.get(item['ref'])
    assert cur['status'] == 'open'
    assert cur['blocked_by'] == [dep['ref']]
    assert not cur.get('needs_input') and not cur.get('parked') and not cur.get('wait')
    assert not cur.get('claimed_by') and not cur.get('claimed_session_id')
    assert calls == []
    assert wt.q.peek_next(project='SUB')['ref'] == dep['ref']
    wt.q.claim_by_ref(dep['ref'], 'dep-builder')
    assert wt.q.claim_next('next', project='SUB') is None
    wt.q.close(dep['ref'], 'dep-builder', resolution={'summary': 'done'})
    calls.clear()
    assert wt.q.get(item['ref'])['status'] == 'open'
    assert calls == []  # becoming eligible itself sends no notice
    assert wt.q.claim_by_ref(item['ref'], 'next')['ref'] == item['ref']


@pytest.mark.parametrize('question', [
    'Await canonical SUB-1 adoption, then verify.',
    'Waiting on SUB-1',
    'Waiting on SUB-1?',
    'SUB-1?',
    'SUB-1 must authorize adoption before final acceptance.',
])
def test_block_refuses_dependency_only_questions(wt_cli, capsys, question):
    item = wt_cli.q.enqueue(project='SUB', note='dependent')
    wt_cli.q.claim_by_ref(item['ref'], 'builder')
    assert wt_cli.cli.main(['block', item['ref'], '--worker', 'builder', '--question', question]) == 1
    assert f"wt edit {item['ref']}:dependent --after SUB-1:dependent" in capsys.readouterr().err
    assert not wt_cli.q.get(item['ref']).get('needs_input')


def test_real_question_mentioning_ref_is_not_dependency_only(wt_cli):
    assert wt_cli.q.dependency_only_question('Should we cancel SUB-1 or keep it?') == []
    assert wt_cli.q.dependency_only_question('Await owner approval to cancel SUB-1') == []


def test_github_subscription_and_dependency_roundtrip(tmp_path, monkeypatch):
    _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend('GHI', 'github')
    config.set_github_repo('GHI', 'test-owner/test-repo')
    _drainable(config)
    import watchtower.messages as messages
    import watchtower.cli as cli
    importlib.reload(messages)
    importlib.reload(cli)
    calls = _record_sends(monkeypatch, messages)
    dep = q.enqueue(project='GHI', note='dependency')
    for subscribe in [False, True]:
        item = q.enqueue(project='GHI', note='dependent', submitter='filer', submitter_subscribed=subscribe)
        assert q.get(item['ref'])['submitter_subscribed'] is subscribe
        q.claim_by_ref(item['ref'], 'builder')
        assert cli.main(['edit', item['ref'], '--after', dep['ref']]) == 0
        cur = q.get(item['ref'])
        assert cur['blocked_by'] == [dep['ref']]
        assert cur['status'] == 'open' and not cur.get('claimed_by')
        with pytest.raises(ValueError, match='waiting on'):
            q.claim_by_ref(item['ref'], 'next')
    assert calls == []
    q.claim_by_ref(dep['ref'], 'dep-builder')
    q.close(dep['ref'], 'dep-builder', resolution={'summary': 'done'})
    for item in q.list_items(project='GHI', status='open'):
        q.claim_by_ref(item['ref'], 'next')
        q.close(item['ref'], 'next', resolution={'summary': 'done'})
    assert len(calls) == 1 and calls[0][0] == 'filer'


def test_dependency_edit_preserves_handed_off_answer_and_human_questions(wt_cli):
    wt = wt_cli
    dep = wt.q.enqueue(project='SUB', note='dependency')
    item = wt.q.enqueue(project='SUB', note='dependent')
    wt.q.claim_by_ref(item['ref'], 'builder')
    wt.q.block(item['ref'], 'builder', question='Choose A or B?', origin='worker', wait_on='owner')
    assert wt.cli.main(['edit', item['ref'], '--after', dep['ref']]) == 1
    assert wt.q.get(item['ref'])['needs_input']
    assert wt.q.get(item['ref'])['blocked_by'] == []
    wt.q.answer(item['ref'], 'Choose A', session_id='human')
    assert wt.cli.main(['edit', item['ref'], '--after', dep['ref']]) == 0
    cur = wt.q.get(item['ref'])
    assert cur['pending_answer']['state'] == 'handed_off'
    assert cur['pending_answer']['answer'] == 'Choose A'


def test_github_dependency_edit_clears_sidecar_park(tmp_path, monkeypatch):
    from test_github_pipeline_sidecar import _setup
    _, config, q, ref = _setup(tmp_path, monkeypatch)
    q.update(ref, gates=['review'])
    dep = q.enqueue(project='GHI', note='dependency')
    q.claim_by_ref(ref, 'builder')
    seed_legacy_block(q, ref, 'builder', question='Waiting on ' + dep['ref'], origin='worker',
            wait_on='dependency', wait_ref=dep['ref'])
    assert q.get(ref)['status'] == 'awaiting_answer'
    done = []
    monkeypatch.setattr(q, '_note_worker_ticket_done', lambda worker: done.append(worker))
    import watchtower.cli as cli
    importlib.reload(cli)
    assert cli.main(['edit', ref, '--after', dep['ref']]) == 0
    cur = q.get(ref)
    assert cur['status'] == 'open' and not cur.get('parked') and not cur.get('needs_input')
    assert cur['gates'] == ['review']
    assert done == []  # original block already counted the worker's completed turn
    assert q.peek_next(project='GHI')['ref'] == dep['ref']
    q.claim_by_ref(dep['ref'], 'dep-builder')
    q.close(dep['ref'], 'dep-builder', resolution={'summary': 'done'})
    assert q.claim_next('next', project='GHI')['ref'] == ref

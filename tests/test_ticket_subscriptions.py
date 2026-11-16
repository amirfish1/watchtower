"""WT-79: later ticket opt-in uses the existing notification transport."""
import importlib
import json
from concurrent.futures import ThreadPoolExecutor

import pytest

from test_ticket_notifications import wt_cli, wt, _record_sends  # noqa: F401
from test_github_backend import _install_fake_gh, _reload_isolated, _drainable


def exercise_lifecycle(wt, monkeypatch, capsys):
    monkeypatch.setattr(wt.cli, '_default_report_to', lambda: ('filer', ''))
    monkeypatch.setattr(wt.messages, 'ccc_forward_target', lambda t: 'continued' if t == 'filer' else t)
    calls = _record_sends(monkeypatch, wt.messages)
    item = wt.q.enqueue(project=wt.project, note='later subscription', submitter='filer')
    ref = item['ref']
    assert wt.cli.main(['subscribe', ref]) == 0
    assert wt.cli.main(['subscribe', ref, 'observer']) == 0
    assert wt.cli.main(['subscribe', ref, 'observer']) == 0
    cur = wt.q.get(ref)
    assert set(wt.q.ticket_subscribers(cur)) == {'filer', 'observer'}
    assert cur['submitter'] == 'filer'
    assert wt.cli.main(['find', ref]) == 0
    assert 'subscribed: filer, observer' in capsys.readouterr().out
    assert wt.cli.main(['find', ref, '--json']) == 0
    assert set(json.loads(capsys.readouterr().out)['subscribed_targets']) == {'filer', 'observer'}
    wt.q.claim_by_ref(ref, 'builder')
    wt.q.close(ref, 'builder', resolution={'summary': 'done'})
    assert [t for t, _ in calls] == ['continued', 'observer']
    assert wt.cli.main(['unsubscribe', ref]) == 0
    assert wt.cli.main(['unsubscribe', ref, 'observer']) == 0
    assert wt.q.ticket_subscribers(wt.q.get(ref)) == []
    calls.clear()
    wt.q.reopen(ref, reason='verify unsubscribe before another close')
    wt.q.claim_by_ref(ref, 'builder')
    wt.q.close(ref, 'builder', resolution={'summary': 'done after opt-out'})
    assert calls == []
    # Queue subscriptions retain their independent event stream.
    wt.config.add_subscriber(wt.project, 'observer')
    wt.q._notify_ticket_event(wt.q.get(ref), 'closed')
    assert [t for t, _ in calls] == ['observer']


def test_file_ticket_lifecycle(wt_cli, monkeypatch, capsys):
    wt_cli.project = 'SUB'
    exercise_lifecycle(wt_cli, monkeypatch, capsys)


def test_github_ticket_lifecycle(tmp_path, monkeypatch, capsys):
    from types import SimpleNamespace
    _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend('GHI', 'github')
    config.set_github_repo('GHI', 'test-owner/test-repo')
    _drainable(config)
    import watchtower.messages as messages
    import watchtower.cli as cli
    importlib.reload(messages)
    importlib.reload(cli)
    wt = SimpleNamespace(q=q, config=config, messages=messages, cli=cli, project='GHI')
    exercise_lifecycle(wt, monkeypatch, capsys)


@pytest.mark.parametrize('initial', [False, True])
def test_edit_subscription_only_changes_callers_opt_in(wt_cli, monkeypatch, initial):
    wt = wt_cli
    item = wt.q.enqueue(project='SUB', note='edit', submitter='original', submitter_subscribed=initial)
    wt.q.update(item['ref'], _subscription=('other', True))
    monkeypatch.setattr(wt.cli, '_default_report_to', lambda: ('caller', ''))
    for flag, expected in [('--subscribe', True), ('--no-subscribe', False)]:
        assert wt.cli.main(['edit', item['ref'], flag]) == 0
        cur = wt.q.get(item['ref'])
        assert cur['submitter'] == 'original'
        assert cur['submitter_subscribed'] is initial
        assert ('caller' in wt.q.ticket_subscribers(cur)) is expected
        assert 'other' in wt.q.ticket_subscribers(cur)


def test_concurrent_local_subscriptions_preserve_both(wt_cli):
    q = wt_cli.q
    ref = q.enqueue(project='SUB', note='concurrent')['ref']
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(lambda t: q.update(ref, _subscription=(t, True)), ['one', 'two']))
    assert set(q.ticket_subscribers(q.get(ref))) == {'one', 'two'}


@pytest.mark.parametrize('subscribe', [False, True])
def test_filing_prints_subscription_status(wt_cli, monkeypatch, capsys, subscribe):
    monkeypatch.setattr(wt_cli.cli, '_default_report_to', lambda: ('caller', ''))
    args = ['add', '-q', 'SUB', '--note', 'filing output']
    if subscribe:
        args.append('--subscribe')
    assert wt_cli.cli.main(args) == 0
    out = capsys.readouterr().out
    from watchtower.refs import display_ref
    ref = display_ref(wt_cli.q.list_items(project='SUB')[0])
    assert 'FILED: ' + ref in out
    assert ("you'll be notified" if subscribe else f'no updates to you; run wt subscribe {ref} to hear back') in out


def test_missing_ref_and_missing_identity_fail_without_queue_mutation(wt_cli, monkeypatch):
    wt = wt_cli
    monkeypatch.setattr(wt.cli, '_default_report_to', lambda: ('', ''))
    ref = wt.q.enqueue(project='SUB', note='no identity')['ref']
    assert wt.cli.main(['subscribe', ref]) == 1
    assert wt.cli.main(['edit', ref, '--subscribe']) == 1
    assert wt.cli.main(['subscribe', 'SUB-999', 'someone']) == 1
    assert wt.config.subscribers('SUB-999') == []
    assert wt.q.ticket_subscribers(wt.q.get(ref)) == []


def test_ticket_actor_dedup_and_cross_host_address(wt_cli, monkeypatch):
    wt = wt_cli
    monkeypatch.setattr(wt.messages, 'local_node_ids', lambda: ['local'])
    monkeypatch.setattr(wt.messages, 'node_qualified', lambda node, target: f'{node}:{target}' if node else target)
    monkeypatch.setattr(wt.messages, 'split_node_ref', lambda target: tuple(target.split(':', 1)) if ':' in target else ('', target))
    item = wt.q.enqueue(project='SUB', note='remote', submitter='filer', submitter_subscribed=True)
    wt.q.update(item['ref'], _subscription=('watcher', True))
    wt.config.add_subscriber('SUB', 'local:watcher')
    calls = _record_sends(monkeypatch, wt.messages)
    wt.q._notify_ticket_event(wt.q.get(item['ref']), 'closed', actor='filer')
    assert [t for t, _ in calls] == ['local:watcher']


def test_later_subscription_retains_owning_node_for_remote_workers(wt_cli, monkeypatch):
    wt = wt_cli
    node = '11111111-2222-3333-4444-555555555555'
    other_node = '22222222-2222-3333-4444-555555555555'
    sid = '33333333-2222-3333-4444-555555555555'
    monkeypatch.setattr(wt.messages, 'local_node_ids', lambda: (node,))
    item = wt.q.enqueue(project='SUB', note='cross host')
    wt.q.update(item['ref'], _subscription=(sid, True))
    assert wt.q.ticket_subscribers(wt.q.get(item['ref'])) == [sid]
    monkeypatch.setattr(wt.messages, 'local_node_ids', lambda: (other_node,))
    calls = _record_sends(monkeypatch, wt.messages)
    wt.q._notify_ticket_event(wt.q.get(item['ref']), 'closed')
    assert [t for t, _ in calls] == [f'{node}:{sid}']
    wt.q.update(item['ref'], _subscription=(f'{node}:{sid}', False))
    calls.clear()
    wt.q._notify_ticket_event(wt.q.get(item['ref']), 'closed')
    assert calls == []

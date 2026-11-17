import json
import os

from watchtower.watch_checks import production_logs


def test_real_alert_success_failure_and_absence_are_distinct(tmp_path):
    state = tmp_path / "state.json"
    state.write_text('{"fails": 0}')
    os.utime(state, (1000, 1000))
    args = (tmp_path, "/auth/callback", "Falling through", 900, "code=false")
    assert production_logs(*args, now=1000)["status"] == "pending"
    alerts = tmp_path / "alerted.jsonl"
    alerts.write_text(json.dumps(dict(ts=999, path="/auth/callback", level="warning",
                                     status=307, excerpt="Falling through code=false", log_id="natural-1"))+'\n')
    assert production_logs(*args, now=1000) == dict(status="pass", real_data=True,
                                                 evidence="natural warning-level 307 with expected condition; log natural-1")
    with alerts.open("a") as f:
        f.write(json.dumps(dict(ts=1000, path="/auth/callback", level="error",
                               excerpt="Falling through code=false", log_id="regression-2"))+'\n')
    result = production_logs(*args, now=1000)
    assert result["status"] == "fail" and "regression-2" in result["evidence"]
    assert production_logs(*args, now=3000)["status"] == "fail"  # lost collector isn't success


def test_historical_errors_and_unrelated_routes_do_not_claim_failure(tmp_path):
    (tmp_path / "state.json").write_text('{"fails": 0}')
    (tmp_path / "alerted.jsonl").write_text(json.dumps(dict(ts=50, path="/a", level="error", excerpt="timeout"))+'\n')
    assert production_logs(tmp_path, "/a", "timeout", 60)["status"] == "pending"
    assert production_logs(tmp_path, "/b", "timeout", 0)["status"] == "pending"


def test_timer_audit_reads_raw_issue_metadata_including_closed_and_release_records():
    from watchtower.watch_checks import ticket_timers
    from watchtower.github_backend import _body_with_metadata
    assert ticket_timers([])['status'] == 'pending'
    item = dict(ref='Q-1', status='closed', _github_body=_body_with_metadata('Historical text',
                dict(deferred_until='9999-12-31T00:00:00Z', release_wait={'commit':'a'*40})))
    result = ticket_timers([item])
    assert result['status'] == 'fail' and 'Q-1' in result['evidence']
    # Mentioning the former field in prose is not a timer.
    item['_github_body'] = _body_with_metadata('deferred_until is retired', dict(release_wait={'commit':'a'*40}))
    assert ticket_timers([item])['status'] == 'pass'
    assert ticket_timers([item])['real_data'] is True
    assert ticket_timers([dict(ref='LOCAL-1', deferred_until='2026-01-01')])['status'] == 'fail'


def test_timer_audit_never_passes_incomplete_poller_coverage(monkeypatch):
    from watchtower import watches
    from watchtower.watch_checks import ticket_timers
    monkeypatch.setattr(watches, 'inventory', lambda: ([dict(ref='REAL-1')],
        [dict(source='repo:open', missing=False, stale=True)]))
    result = ticket_timers()
    assert result['status'] == 'pending' and result['real_data'] is False
    monkeypatch.setattr(watches, 'inventory', lambda: ([dict(ref='REAL-1',deferred_until='tomorrow')],
        [dict(source='repo:open', missing=True, stale=True)]))
    assert ticket_timers()['status'] == 'fail'


def test_raw_inventory_preserves_watches_and_timer_fields_without_conversion(monkeypatch):
    import time
    from watchtower import watches, queue as q, config, github_backend as gh
    monkeypatch.setattr(q, '_load_unlocked', lambda **kw: {'items': []})
    monkeypatch.setattr(q, 'list_items', lambda *a, **k: (_ for _ in ()).throw(AssertionError('slow conversion')))
    monkeypatch.setattr(config, 'all_queues', lambda: {'GHI': {'backend':'github','github_repo':'test/repo'}})
    monkeypatch.setattr(config, 'github_queues_for_repo', lambda repo: ['GHI'])
    raw = dict(number=42,state='CLOSED',body=gh._body_with_metadata('prose',
        dict(deferred_until='9999-12-31',resolution_watches=[{'name':'registered'}])))
    monkeypatch.setattr(gh, '_read_persisted_list_cache', lambda: {
        'test/repo:open':dict(at=time.time(),data=[]),
        'test/repo:closed':dict(at=time.time(),data=[raw])})
    items, coverage = watches.inventory()
    assert items[0]['ref'] == 'GHI-42' and items[0]['status'] == 'closed'
    assert items[0]['deferred_until'] == '9999-12-31'
    assert items[0]['resolution']['watches'] == [{'name':'registered'}]
    assert not any(c['missing'] or c['stale'] for c in coverage)


def test_raw_inventory_honors_pipeline_claim_release_without_hiding_active_claims(monkeypatch):
    import time
    from watchtower import watches, queue as q, config, github_backend as gh, github_pipeline as gp
    monkeypatch.setattr(q, '_load_unlocked', lambda **kw: {'items': []})
    monkeypatch.setattr(config, 'all_queues', lambda: {'G': {'backend': 'github', 'github_repo': 'test/repo'}})
    monkeypatch.setattr(config, 'github_queues_for_repo', lambda repo: ['G'])
    monkeypatch.setattr(gp, '_read_cached', lambda: {'items': {
        'test/repo#1': {'parked': {'worker_id': 'previous-worker'}},
    }})
    meta = dict(claimed_by='previous-worker', claimed_at='2026-01-01T00:00:00Z',
                needs_input=True, wait={'on': 'owner'}, block_question='Choose retention policy')
    raw = [dict(number=n, state='OPEN', labels=[{'name': 'watchtower:in-progress'}],
                body=gh._body_with_metadata('Question', meta)) for n in (1, 2)]
    monkeypatch.setattr(gh, '_read_persisted_list_cache', lambda: {
        'test/repo:open': dict(at=time.time(), data=raw),
        'test/repo:closed': dict(at=time.time(), data=[]),
    })
    items, _ = watches.inventory()
    parked, active = sorted(items, key=lambda it: it['ref'])
    assert parked['status'] == q.PARKED_STATUS
    assert parked['claimed_by'] is None
    assert parked['needs_input'] and parked['block_question'] == meta['block_question']
    assert parked['wait'] == meta['wait']
    assert active['status'] == 'in_progress' and active['claimed_by'] == meta['claimed_by']

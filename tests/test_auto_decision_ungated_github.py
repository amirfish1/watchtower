"""Real GitHub answer serialization, proof persistence and failure/race gates."""
import pytest
from tests.test_github_backend import _install_fake_gh, _reload_isolated, _drainable


@pytest.fixture
def lane(tmp_path, monkeypatch):
    monkeypatch.setenv('WATCHTOWER_DATA_DIR', str(tmp_path / 'data'))
    _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend('GHI', 'github')
    config.set_github_repo('GHI', 'test-owner/test-repo')
    _drainable(config)
    from watchtower import autodecide
    monkeypatch.setattr(autodecide, 'kill_switch_on', lambda: False)
    item = q.enqueue(project='GHI', note='Availability', text='Original report')
    return q, item['ref'], autodecide


def answer(lane, sidecar=False):
    q, ref, auto = lane
    if sidecar:
        q.update(ref, gates=['verify'])
    q.claim_by_ref(ref, 'worker', session_uuid='session')
    q.block(ref, 'worker', 'Compare frozen data?', origin='worker' if sidecar else 'system')
    blocked = q.get(ref)
    generation = blocked['blocked_at']
    result = q.auto_answer(ref, 'narrow recommendation', expect_blocked_at=generation,
                           revision=auto.ticket_revision(blocked),
                           decision={'class': 'capped-replay-spend', 'recommend': 'narrow recommendation'})
    return result, blocked


@pytest.mark.parametrize('sidecar', [False, True])
def test_auto_answer_retains_one_real_receipt_and_retry_is_idempotent(lane, sidecar):
    q, ref, auto = lane
    result, blocked = answer(lane, sidecar)
    assert result
    fresh = q.get(ref)
    events = [h for h in fresh['history'] if h['event'] == 'auto_decision']
    assert len(events) == 1
    event = events[0]
    assert event['decision_id'] == f"{ref}@{blocked['blocked_at']}"
    assert event['by']['kind'] == 'system'
    from watchtower.github_backend import _history_id
    assert _history_id(event) == _history_id({**event, 'at': '2099-01-01T00:00:00Z'})
    assert _history_id(event) != _history_id({**event, 'recommend': 'different scope'})
    assert event['revision'] == auto.ticket_revision(blocked)
    assert all(event[k] for k in ['answer_entry_key', 'answer_comment_ref', 'answer_comment_id', 'answer_sha256', 'answer_length'])
    from watchtower.auto_decision_receipts import verify_decision_content
    for key in ['answer_entry_key', 'answer_comment_ref', 'answer_comment_id', 'answer_sha256', 'answer_length']:
        with pytest.raises(ValueError):
            verify_decision_content(fresh, {**event, key: 'wrong'})
    fields = {k: v for k, v in event.items() if k not in ('event', 'at', 'by')}
    assert q._gh_history(ref, 'auto_decision', **fields)
    assert len([h for h in q.get(ref)['history'] if h['event'] == 'auto_decision']) == 1
    assert not q._gh_history(ref, 'auto_decision', **{**fields, 'recommend': 'broader scope'})
    assert q.auto_answer(ref, 'duplicate', expect_blocked_at=blocked['blocked_at'], revision=auto.ticket_revision(blocked), decision={}) is None
    if sidecar:
        gen = fresh['pending_answer']['gen']
        assert q.resume_claim(ref, gen)
        assert q.pa_transition(ref, gen, 'delivering', 'delivered', from_status='in_progress', route='resume')
        from watchtower.auto_decision_receipts import verify_decision_content
        assert verify_decision_content(q.get(ref), event) == auto.ticket_revision(blocked)


def test_backend_persistence_failure_returns_no_approval_and_logs_it(lane, monkeypatch, tmp_path):
    q, ref, _ = lane
    backend = q._github_backend_for_project('GHI')
    original = type(backend).update
    def fail(self, ident, **fields):
        if '_system_history' in fields:
            raise RuntimeError('injected receipt storage failure')
        return original(self, ident, **fields)
    monkeypatch.setattr(type(backend), 'update', fail)
    result, _ = answer(lane)
    assert result is None
    assert any(h['event'] == 'answer' for h in q.get(ref)['history'])
    assert not any(h['event'] == 'auto_decision' for h in q.get(ref)['history'])
    assert 'WITHOUT usable approval receipt' in (tmp_path / 'activity.log').read_text()


@pytest.mark.parametrize('race', ['reblock', 'owner-answer', 'source', 'comment'])
def test_race_after_answer_preserves_changes_and_returns_no_approval(lane, monkeypatch, race):
    q, ref, _ = lane
    backend = q._github_backend_for_project('GHI')
    original = type(backend).answer
    def concurrent(self, ident, text, **kwargs):
        result = original(self, ident, text, **kwargs)
        if race == 'reblock':
            from watchtower import github_backend
            monkeypatch.setattr(github_backend, '_now_iso', lambda: '2099-01-01T00:00:00Z')
            q.block(ref, 'worker', 'New owner decision')
        elif race == 'owner-answer':
            original(self, ident, 'STOP; owner changed the decision', kind='human')
        elif race == 'source':
            backend.update(ident, text='Concurrent changed source')
        else:
            q.comment(ref, 'Concurrent owner comment: STOP', by='human')
        return result
    monkeypatch.setattr(type(backend), 'answer', concurrent)
    result, _ = answer(lane)
    assert result is None
    fresh = q.get(ref)
    assert not any(h['event'] == 'auto_decision' for h in fresh['history'])
    if race == 'source':
        assert fresh['text'].startswith('Concurrent changed source')
    if race == 'comment':
        assert 'Concurrent owner comment: STOP' in fresh['text']
        assert any(h['event'] == 'comment' for h in fresh['history'])
    if race == 'reblock':
        assert fresh['needs_input']

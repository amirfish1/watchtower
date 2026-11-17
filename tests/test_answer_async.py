"""WT-93: acceptance is independent of slow delivery and survives CLI exit."""
import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

from tests.legacy_blocks import block as legacy_block
from tests.test_github_backend import restore_watchtower_modules  # noqa: F401


def _blocked(env, *, parked=True, worker='w', sid='11111111-2222-3333-4444-555555555555'):
    q = env.queue
    item = q.enqueue(project='ASYNC', note='which way?')
    q.claim_by_ref(item['ref'], worker, session_uuid=sid)
    return legacy_block(q, item['ref'], session_id=worker, question='left or right?',
                        origin='worker' if parked else 'system')


def _answer(env, item, text='left', **options):
    return env.cli.cmd_answer(argparse.Namespace(
        ref=item['ref'], text=text, worker='owner', by='human',
        engine=options.get('engine'), tid=options.get('tid', False)))


def test_ten_cli_answers_do_not_wait_for_dispatch_lock(wt_env):
    from watchtower import takeover, answer_dispatch
    items = [_blocked(wt_env, worker=f'w{i}', sid=f'11111111-2222-3333-4444-{i:012d}') for i in range(10)]
    procs = []
    start = time.monotonic()
    try:
        # The real reconciler holds this lock across native observations/transport.
        # All ten *processes* must finish before it is released, not one at a time
        # behind a session resume timeout. Each exited CLI leaves a durable intent.
        with takeover._lock():
            for item in items:
                procs.append(subprocess.Popen(
                    [sys.executable, '-m', 'watchtower.cli', 'answer', item['ref'],
                     'left', '--by', 'human'], env=os.environ.copy(),
                    cwd=Path(__file__).resolve().parents[1],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True))
            for proc in procs:
                out, err = proc.communicate(timeout=15)
                assert proc.returncode == 0, (out, err)
                assert 'answer recorded' in out
        assert time.monotonic() - start < 15
    finally:
        for proc in procs:
            if proc.poll() is None:
                proc.kill()
                proc.communicate()
    for item in items:
        current = wt_env.queue.get(item['ref'])
        assert not current['needs_input']
        assert current['answer_dispatch']['state'] == 'pending'
        assert current['pending_answer']['answer'] == 'left'
    # A later daemon pass recovers the persisted answers after every CLI exited.
    import watchtower.answers as answers
    from unittest.mock import patch
    with patch.object(answers, 'worker_alive', return_value=False), patch.object(answers, '_resumable', return_value=False):
        assert len(answers.route_pending_answers()) == 10
    assert all(wt_env.queue.get(it['ref'])['status'] == 'open' for it in items)


def test_legacy_delivery_keeps_options_and_is_not_inline(wt_env, monkeypatch):
    from watchtower import answer_dispatch
    item = _blocked(wt_env, parked=False)
    seen = []
    monkeypatch.setattr(wt_env.cli, '_deliver_to_blocked_session',
                        lambda it, text, prompt, engine, worker:
                        seen.append((text, prompt, engine, worker)) or 0)
    assert _answer(wt_env, item, engine='codex', tid=True) == 0
    assert not seen
    assert not wt_env.queue.get(item['ref'])['needs_input']
    assert answer_dispatch.reconcile() == [item['ref']]
    assert seen[0][0] == 'left' and seen[0][2:] == ('codex', 'owner')
    assert answer_dispatch.reconcile() == []


def test_reblock_and_close_cancel_stale_dispatch(wt_env, monkeypatch):
    from watchtower import answer_dispatch
    monkeypatch.setattr(wt_env.cli, '_deliver_answer',
                        lambda *a: (_ for _ in ()).throw(AssertionError('stale delivery')))
    item = _blocked(wt_env, parked=False)
    _answer(wt_env, item)
    legacy_block(wt_env.queue, item['ref'], session_id='w', question='new question', origin='system')
    assert answer_dispatch.reconcile() == []
    assert wt_env.queue.get(item['ref'])['answer_dispatch']['state'] == 'cancelled'
    other = _blocked(wt_env, worker='other', sid='other')
    _answer(wt_env, other)
    wt_env.queue.update_status(other['ref'], 'closed', resolution={'summary': 'done'})
    assert answer_dispatch.reconcile() == []


def test_transport_exception_retries_after_durable_lease(wt_env, monkeypatch):
    from watchtower import answer_dispatch
    item = _blocked(wt_env, parked=False)
    _answer(wt_env, item)
    start = time.time()
    calls = []
    def deliver(*args):
        calls.append(args)
        if len(calls) == 1:
            raise OSError('temporary transport outage')
        return 0
    monkeypatch.setattr(wt_env.cli, '_deliver_answer', deliver)
    assert answer_dispatch.reconcile(now=start) == []
    assert answer_dispatch.reconcile(now=start + 1) == []
    assert answer_dispatch.reconcile(now=start + answer_dispatch.LEASE_S + 1) == [item['ref']]
    assert len(calls) == 2
    assert wt_env.queue.get(item['ref'])['answer_dispatch']['state'] == 'done'


def test_github_acceptance_defers_transport_and_preserves_full_comment(tmp_path, monkeypatch):
    from tests.test_github_backend import _install_fake_gh, _reload_isolated, _drainable
    from watchtower import answer_dispatch, cli
    _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend('GHI', 'github')
    config.set_github_repo('GHI', 'test-owner/test-repo')
    _drainable(config)
    item = q.enqueue(project='GHI', note='needs decision')
    q.claim_by_ref(item['ref'], 'worker', session_uuid='session-abc')
    q.block(item['ref'], session_id='worker', question='what next?')
    seen = []
    monkeypatch.setattr(cli, '_deliver_to_blocked_session',
                        lambda it, text, *a: seen.append(text) or 0)
    text = 'long answer with unicode ☃\n' * 2200
    assert cli.cmd_answer(argparse.Namespace(ref=item['ref'], text=text, worker='owner',
                                            by='human', engine='codex', tid=False)) == 0
    saved = q.get(item['ref'])
    assert not saved['needs_input'] and not seen
    assert saved['answer_dispatch']['text'] == ''
    assert saved['answer_dispatch']['text_ref']
    assert answer_dispatch.reconcile([saved]) == [item['ref']]
    assert seen == [text]
    assert q.get(item['ref'])['answer_dispatch']['state'] == 'done'


def test_one_unavailable_dispatch_does_not_starve_other_answers(wt_env, monkeypatch):
    from watchtower import answer_dispatch
    bad = _blocked(wt_env, parked=False, worker='bad')
    good = _blocked(wt_env, parked=False, worker='good')
    _answer(wt_env, bad)
    _answer(wt_env, good)
    real = answer_dispatch._transition
    def transition(ref, *args, **kwargs):
        if ref == bad['ref']:
            raise OSError('backend offline')
        return real(ref, *args, **kwargs)
    monkeypatch.setattr(answer_dispatch, '_transition', transition)
    seen = []
    monkeypatch.setattr(wt_env.cli, '_deliver_answer', lambda *args: seen.append(args) or 0)
    assert answer_dispatch.reconcile() == [good['ref']]
    assert len(seen) == 1


def test_parked_tid_survives_acceptance_until_daemon(wt_env, monkeypatch):
    from watchtower import answer_dispatch, answers
    item = _blocked(wt_env)
    captured = []
    monkeypatch.setattr(answers, 'route_answer',
                        lambda ref, gen, tid=False: captured.append((ref, tid)) or {'route': 'test', 'reason': ''})
    assert _answer(wt_env, item, tid=True, engine='codex') == 0
    pending = wt_env.queue.get(item['ref'])['pending_answer']
    assert pending['tid'] is True and pending['prior_engine'] == 'codex'
    assert captured == []
    assert answer_dispatch.reconcile() == [item['ref']]
    assert captured == [(item['ref'], True)]


def test_github_parked_sidecar_answer_runs_on_daemon_pass(tmp_path, monkeypatch):
    from tests.test_github_pipeline_sidecar import _claimed_gated, _block_parks
    from watchtower import answer_dispatch, cli, answers
    q, ref = _claimed_gated(tmp_path, monkeypatch)
    _block_parks(q, ref)
    # No prior session on this legacy fixture: delivery hands off to the pool.
    assert cli.cmd_answer(argparse.Namespace(ref=ref, text='use postgres', worker='owner',
                                            by='human', engine=None, tid=False)) == 0
    saved = q.get(ref)
    assert not saved['needs_input'] and saved['pending_answer']['state'] == 'routing'
    assert saved['answer_dispatch']['state'] == 'pending'
    assert answer_dispatch.reconcile([saved]) == [ref]
    assert q.get(ref)['status'] == 'open'
    assert q.get(ref)['pending_answer']['answer'] == 'use postgres'
    assert q.get(ref)['answer_dispatch']['state'] == 'done'


def test_foreign_host_does_not_resume_shared_github_intent(wt_env, monkeypatch):
    from watchtower import answer_dispatch
    item = _blocked(wt_env, parked=False)
    _answer(wt_env, item)
    saved = wt_env.queue.get(item['ref'])
    saved['answer_dispatch']['host'] = 'a-different-host'
    monkeypatch.setattr(wt_env.cli, '_deliver_answer',
                        lambda *a: (_ for _ in ()).throw(AssertionError('foreign delivery')))
    assert answer_dispatch.reconcile([saved]) == []

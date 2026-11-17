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


def test_migration_fences_changed_question_and_preserves_real_gate(env):
    q, _, _, _, name = env
    ref = _ticket(q, name)
    legacy_block(q, ref, 'w1', question='Await coordinator', wait_on='worker')
    op = _operation(q, ref, 'reopen')
    q.block(ref, 'w1', origin='worker', question='[Q11:money] Approve the reviewed money change?', wait_on='owner')
    with pytest.raises(ValueError, match='changed since review'):
        migration.apply(op, q.list_items(), 'migration-worker')
    assert q.get(ref)['needs_input'] and '[Q11:money]' in q.get(ref)['block_question']


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

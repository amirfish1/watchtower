"""Durable answer delivery intents; acceptance never runs a transport.

The intent is saved in the same transaction/body edit as the answer. Only the
reconciler consumes it. A generation and claim identity fence stale snapshots;
expired attempts retry through the existing liveness delivery deduplication.
"""
from __future__ import annotations

import argparse
import copy
import time
import uuid

from . import queue as q

LEASE_S = 300


def pending_options(pa, options):
    """Keep topic/engine options with the CAS-routed answer across crash recovery."""
    if pa.get('state') == 'routing':
        pa['tid'] = bool(options.get('tid'))
        if options.get('engine'):
            pa['prior_engine'] = options['engine']


def record(item, text, worker, by_kind, options):
    pending_options(item.get('pending_answer') or {}, options)
    item['answer_dispatch'] = dict(
        id=uuid.uuid4().hex, state='pending', at=time.time(), text=text, host=q.machine_tag(),
        worker=worker, by_kind=by_kind, engine=options.get('engine'),
        tid=bool(options.get('tid')), session=item.get('claimed_session_id'),
        claimant=item.get('claimed_by'), blocked_at=item.get('blocked_at'))


def _transition(ref, token, state, *, now):
    def change(item):
        job = item.get('answer_dispatch') or {}
        if job.get('id') != token:
            return False
        if state != 'dispatching':
            # Routing may itself hand off/release the claim. Settle only our
            # consumed intent, never a replacement answer or its ticket fields.
            if job.get('state') != 'dispatching':
                return False
            job.update(state=state, at=now)
            return True
        if (item.get('needs_input') or item.get('status') == 'closed'
                or item.get('blocked_at') != job.get('blocked_at')
                or item.get('claimed_session_id') != job.get('session')
                or item.get('claimed_by') != job.get('claimant')):
            job['state'] = 'cancelled'
            return False
        if state == 'dispatching' and not (
                job.get('state') == 'pending' or
                (job.get('state') == 'dispatching' and now - job.get('at', now) >= LEASE_S)):
            return False
        job.update(state=state, at=now)
        return True

    backend = q._github_backend_for_project(q._project_from_ident(ref))
    if backend is not None:
        with backend._body_lock():
            item = backend.get(ref)
            if not item:
                return None
            accepted = change(item)
            from .github_backend import _split_body
            body, meta = _split_body(item.get('_github_body') or item.get('text', ''))
            meta['answer_dispatch'] = item.get('answer_dispatch')
            backend._edit_body(item, body, meta)
            return item if accepted else None
    with q._FileLock(q._lock_path()):
        data = q._load_unlocked()
        item = next((it for it in data['items'] if q._matches(it, ref)), None)
        if not item:
            return None
        accepted = change(item)
        q._save_unlocked(data)
        return copy.deepcopy(item) if accepted else None


def reconcile(items=None, *, now=None):
    """Run from the daemon, outside the answer/store lock; recover after restart."""
    from . import cli, config
    now = time.time() if now is None else now
    acted = []
    for item in q.list_items() if items is None else items:
        job = item.get('answer_dispatch') or {}
        if job.get('host') != q.machine_tag() or job.get('state') not in ('pending', 'dispatching'):
            continue
        # Archived queues and foreign pipeline owners retain their authority fences.
        if config.ticket_is_archived(item) or item.get('pipeline_foreign') or item.get('pipeline_missing'):
            continue
        if job.get('state') == 'dispatching' and now - job.get('at', now) < LEASE_S:
            continue
        ref, token = item['ref'], job['id']
        try:
            current = _transition(ref, token, 'dispatching', now=now)
            if not current:
                continue
            job = current['answer_dispatch']
            text = job['text']
            if job.get('text_ref'):
                from .github_backend import _comment_ref, _strip_entry_marker, _text_sha256
                comment = next((c for c in current.get('_github_comments', [])
                                if _comment_ref(c) == job['text_ref']), None)
                text = _strip_entry_marker(str((comment or {}).get('body') or ''))[0]
                if comment is None or _text_sha256(text) != job['text_sha256']:
                    raise ValueError('durable answer comment missing or changed')
            args = argparse.Namespace(ref=ref, text=text, worker=job['worker'],
                                      engine=job.get('engine'), tid=job.get('tid', False))
            code = cli._deliver_answer(args, current, job['by_kind'])
            _transition(ref, token, 'done' if code == 0 else 'pending', now=now)
            acted.append(ref)
        except Exception as exc:  # one unavailable backend must not stop the pass
            q._log('ANSWER_DISPATCH_ERROR', f'{ref}: {exc}', queue=item.get('project', ''))
    return acted

"""Bounded, pull-only outcome intake; no commitments, dispatch or acceptance.

Call reconcile before outcomes reads its active snapshot (never under its lock).
Only explicit provenance associates sessions. Repository suggestions are review
material, never evidence. All CCC imports are lazy to avoid runtime cycles.
"""
from __future__ import annotations

import copy
import hashlib
import json
import time
from datetime import datetime, timezone
from urllib.request import urlopen

from . import storage

SCAN_INTERVAL = 3600
SCAN_LIMIT = 200
RECORD_LIMIT = 1000
SUGGESTION_LIMIT = 10
REQUEST_LIMIT = 200
MAX_RESPONSE_BYTES = 2 * 1024 * 1024
HTTP_TIMEOUT = 3


def _iso(now):
    return datetime.fromtimestamp(now, timezone.utc).isoformat()


def _path():
    return storage.data_dir() / 'outcome-intake.json'


def _load():
    try:
        data = json.loads(_path().read_text())
    except FileNotFoundError:
        data = {}
    if not isinstance(data, dict):
        raise ValueError('invalid outcome intake store')
    for key, default in dict(unfiled={}, receipts={}, reviews={}, cursor='',
                             last_scan=None, coverage={}, decision_cursor=0).items():
        data.setdefault(key, default)
    return data


def _get(path):
    from . import messages
    base = messages._delegate_base()
    if not base:
        raise ValueError('CCC discovery unavailable')
    with urlopen(base + path, timeout=HTTP_TIMEOUT) as response:
        raw = response.read(MAX_RESPONSE_BYTES + 1)
    if len(raw) > MAX_RESPONSE_BYTES:
        raise ValueError('CCC response exceeds byte limit')
    data = json.loads(raw)
    if not isinstance(data, dict) or data.get('ok') is not True:
        raise ValueError('CCC source unavailable')
    return data


def _tickets():
    # This deliberately avoids list_items(): that API may refresh remote queues.
    from . import queue
    return queue._load_unlocked(strict=True, readonly=True).get('items', [])


def _summary(data):
    return dict(unfiled=len(data['unfiled']), receipts=len(data['receipts']),
                last_scan=data['last_scan'], cursor=data['cursor'],
                coverage=copy.deepcopy(data['coverage']))


def view():
    """Read-only cached summary, durable Unfiled rows, and decision receipts."""
    from . import outcomes
    if not outcomes.enabled():
        return dict(enabled=False, summary={}, unfiled=[], receipts=[])
    data = _load()
    return dict(enabled=True, summary=_summary(data),
                unfiled=copy.deepcopy(list(data['unfiled'].values())),
                receipts=copy.deepcopy(list(data['receipts'].values())))


def _indexes(rows, tickets):
    sessions, refs, steps, claim_names = {}, {}, {}, {}
    def add(index, key, oid):
        if key:
            index.setdefault(str(key), set()).add(oid)
    for o in rows:
        oid = o['id']
        for s in o.get('sessions', []):
            add(sessions, s.get('session_id'), oid)
        for sid in o.get('session_ids', []):
            add(sessions, sid, oid)
        for a in o.get('attempts', []):
            add(sessions, a.get('session_id'), oid)
            add(steps, a.get('step_id'), oid)
        for link in o.get('links', []):
            if link.get('kind') == 'ticket' and link.get('relation') == 'serves':
                add(refs, link.get('target'), oid)
                add(steps, link.get('step_id'), oid)
            if link.get('kind') == 'session' and link.get('relation') in ('serves', 'supports'):
                add(sessions, link.get('target'), oid)
    for t in tickets:
        for oid in refs.get(t.get('ref'), set()):
            add(sessions, t.get('claimed_session_id'), oid)
            add(sessions, t.get('claimed_by'), oid)
            add(claim_names, t.get('claimed_by'), oid)
    return sessions, refs, steps, claim_names


def _scan(data, rows, now):
    from . import outcomes
    source = _get('/api/sessions/census?since=86400')
    census = source.get('sessions')
    if not isinstance(census, list):
        raise ValueError('census sessions unavailable')
    sessions, tickets, steps, claim_names = _indexes(rows, _tickets())
    by_id = {o['id']: o for o in rows}
    valid = sorted((r for r in census if isinstance(r, dict) and
                    isinstance(r.get('session_id'), str) and 0 < len(r['session_id']) <= 200),
                   key=lambda r: r['session_id'])
    names = {}
    for r in valid:
        if r.get('name'):
            names.setdefault(str(r['name']), set()).add(r['session_id'])
    after = [r for r in valid if r['session_id'] > data['cursor']]
    batch = (after + [r for r in valid if r['session_id'] <= data['cursor']])[:SCAN_LIMIT]
    # Parent inheritance uses the pre-scan index only: one explicit hop, never
    # a chain of guessed ancestors or arbitrary same-repository workers.
    for row in batch:
        sid = row['session_id']
        if any(row.get(k) for k in ('archived', 'trashed', 'empty_worker_poll')):
            data['unfiled'].pop(sid, None)
            continue
        direct = set(sessions.get(sid, ()))
        if len(names.get(row.get('name'), ())) == 1:
            direct.update(claim_names.get(row.get('name'), ()))
        for field, index in (('outcome_id', {k: {k} for k in by_id}),
                             ('ticket_ref', tickets), ('step_id', steps)):
            direct.update(index.get(row.get(field), ()))
        parent = row.get('parent_session_id')
        inherited = set(sessions.get(parent, ()))
        candidates = direct | inherited
        if row.get('helper') and not candidates:
            data['unfiled'].pop(sid, None)
            continue
        if len(candidates) == 1:
            outcomes.associate(dict(id=next(iter(candidates)), session_id=sid,
                                    parent_session_id=parent or '', source='outcome-intake'))
            data['unfiled'].pop(sid, None)
            continue
        suggestions = sorted(candidates or {o['id'] for o in rows
                             if row.get('repo_path') and o.get('repo_path') == row['repo_path']})[:SUGGESTION_LIMIT]
        data['unfiled'][sid] = dict(session_id=sid, name=str(row.get('name') or '')[:300],
            repo_path=str(row.get('repo_path') or '')[:1000],
            reason='ambiguous explicit provenance' if candidates else 'no explicit business association',
            suggestions=suggestions, suggestions_are_evidence=False,
            next_action=dict(owner='agent', text='Inspect explicit ticket/outcome provenance on next census review',
                             due_at=_iso(now + SCAN_INTERVAL)), observed_at=_iso(now))
    data['cursor'] = batch[-1]['session_id'] if batch else ''
    data['last_scan'] = now
    data['coverage']['census'] = dict(state='bounded' if len(valid) > len(batch) else 'current',
                                            checked_at=_iso(now), scanned=len(batch), total=len(valid))
    # Old unfiled rows survive source failures and bounded partial scans.
    data['unfiled'] = dict(list(data['unfiled'].items())[-RECORD_LIMIT:])
    bucket = int(now // SCAN_INTERVAL)
    # Give truly unassigned work a real bounded reviewer too. This is an
    # administrative duty of one existing lead, never a business association,
    # contribution credit, extra outcome, or a new resident manager.
    unmatched = [r for r in data['unfiled'].values() if not r['suggestions']][:SUGGESTION_LIMIT]
    active = sorted((o for o in rows if o.get('state') == 'active'),
                    key=lambda o: (not o.get('commitment'), o.get('rank') if isinstance(o.get('rank'), int) else 1000000,
                                   o['id']))
    intake_owner = active[0]['id'] if active else None
    for o in rows:
        suggestions = [r for r in data['unfiled'].values() if o['id'] in r['suggestions']][:SUGGESTION_LIMIT]
        duty = unmatched if o['id'] == intake_owner else []
        material = [{k: r.get(k) for k in ('session_id', 'name', 'reason', 'suggestions')}
                    for r in suggestions + duty]
        fingerprint = hashlib.sha256(json.dumps(material, sort_keys=True).encode()).hexdigest()
        previous = data['reviews'].get(o['id'])
        previous = previous if isinstance(previous, dict) else {'bucket': previous}
        if ((suggestions or duty) and previous.get('bucket') != bucket
                and previous.get('fingerprint') != fingerprint):
            outcomes.event(dict(id=o['id'], type='owner_review', key=f'intake-review:{bucket}',
                payload=dict(source='outcome-intake', unfiled=suggestions,
                             administrative_intake=duty, acceptance_credit=False)))
            data['reviews'][o['id']] = dict(bucket=bucket, fingerprint=fingerprint)
    data['reviews'] = dict([(k, v) for k, v in data['reviews'].items() if k in by_id][-RECORD_LIMIT:])


def _decisions(data, rows, now):
    from . import outcomes
    refs = [(o['id'], d) for o in rows for d in o.get('decision_refs', [])
            if d.get('request_id') and (d.get('state') == 'pending' or
               any(r.get('request_id') == d['request_id'] for r in data['receipts'].values()))]
    if not refs:
        return
    source = _get('/api/decision-inbox/requests')
    requests = source.get('requests')
    if not isinstance(requests, list):
        raise ValueError('decision requests unavailable')
    data['coverage']['decisions'] = dict(state='current' if source.get('source_current') is True else 'unavailable',
                                         checked_at=_iso(now), issues=source.get('coverage_issues', [])[:10])
    index = {}
    for request in requests:
        if isinstance(request, dict):
            index.setdefault(request.get('request_id'), []).append(request)
    start = int(data['decision_cursor']) % len(refs)
    batch = (refs[start:] + refs[:start])[:REQUEST_LIMIT]
    data['decision_cursor'] = (start + len(batch)) % len(refs)
    for oid, ref in batch:
        hits = index.get(ref['request_id'], [])
        if len(hits) != 1:
            continue
        r = hits[0]
        if r.get('source_current', source.get('source_current')) is not True:
            continue
        if ref.get('origin_id') and r.get('origin_id') != ref['origin_id']:
            continue
        # Local refs store revision OR generation in `revision`. Generation is
        # a distinct identity; answer.expected_revision always binds the source
        # request revision, even when the local identity is its generation.
        expected = ref.get('revision') if ref.get('revision') is not None else ref.get('generation')
        if expected in (None, '') or str(expected) not in (str(r.get('revision')), str(r.get('generation'))):
            continue
        if ref.get('generation') is not None and str(ref['generation']) != str(r.get('generation')):
            continue
        answer = r.get('answer') or {}
        if (not isinstance(answer, dict) or answer.get('state') != 'accepted' or
                not answer.get('answer_id') or r.get('revision') is None or
                str(answer.get('expected_revision')) != str(r['revision'])):
            continue
        aid = str(answer['answer_id'])
        key = hashlib.sha256((oid + ':' + aid).encode()).hexdigest()
        receipt = dict(outcome_id=oid, request_id=r['request_id'], origin_id=r.get('origin_id'),
                       revision=r['revision'], generation=r.get('generation'), answer_id=aid,
                       text=answer.get('text', ''), source_accepted=True,
                       session_consumed=answer.get('session_consumed'), checked_at=_iso(now))
        if key not in data['receipts']:
            outcomes.event(dict(id=oid, type='owner_answer', key=f'intake-answer:{key}',
                payload=dict(decision_id=ref['id'], request_id=r['request_id'], origin_id=r.get('origin_id'),
                             revision=str(expected), generation=r.get('generation'),
                             source_revision=r['revision'], answer_id=aid,
                             answer=answer.get('text', ''), receipt=aid,
                             source_accepted=True, session_consumed=answer.get('session_consumed'))))
        data['receipts'][key] = receipt
    data['receipts'] = dict(list(data['receipts'].items())[-RECORD_LIMIT:])


def reconcile(*, now=None, dry_run=False):
    """One bounded pass. Off touches neither storage nor network; no spawns.

    Failures preserve prior records and report unavailable coverage. Stable
    event keys make replay safe if a crash occurs before the intake save.
    """
    from . import outcomes
    if not outcomes.enabled():
        return {'enabled': False}
    if dry_run:
        return view()
    now = time.time() if now is None else now
    path = _path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with storage.file_lock(path.with_suffix('.lock')):
        data = _load()  # Corrupt cache fails closed, never silently overwritten.
        rows = [o for o in outcomes.list_outcomes() if o.get('state') not in ('stopped', 'delivered')]
        for name, run in (('decisions', _decisions), ('census', _scan)):
            if name == 'census' and data['last_scan'] is not None and now < data['last_scan'] + SCAN_INTERVAL:
                continue
            try:
                run(data, rows, now)
            except Exception as exc:  # Source outages must not stop outcome reconciliation.
                data['coverage'][name] = dict(state='unavailable', checked_at=_iso(now),
                                               error=type(exc).__name__)
                if name == 'census':
                    data['last_scan'] = now  # Bound outage retries too.
        with storage.atomic_destination(path) as tmp:
            tmp.write_text(json.dumps(data, sort_keys=True) + '\n')
    return dict(enabled=True, summary=_summary(data))

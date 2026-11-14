# Copyright (c) 2026 Amir Fish. All rights reserved.
# SPDX-License-Identifier: LicenseRef-WatchTower-Software-License

"""Durable same-session supervision, owned by the existing WT reconciler.

Lock order: takeover dispatch lock -> queue lock. Transport runs outside the
queue lock. Intents live with the ticket; receipts live in the delivery ledger.
Cancellation fences dispatch, not an already accepted native turn.
"""
from __future__ import annotations
import copy
from datetime import datetime, timedelta, timezone
import json
import os
import time
import uuid
from pathlib import Path
from . import queue as q, observations, storage

SCHEMA_VERSION = 1
IDLE_SECONDS = 1200
MAX_REMINDERS = 3
TERMINAL = ('cancelled', 'completed')


def _lock():
    return q._FileLock(q._lock_path().with_name('takeover.lock'))


def _items():
    return q._load_unlocked(strict=True).get('items', [])


def _response(it):
    s = copy.deepcopy(it['supervision'])
    return dict(schema_version=1, supervision_id=s['id'], generation=s['generation'],
                ticket=it['ref'], session=s['session'], state=s['state'],
                delivery_state=(s.get('intent') or {}).get('state', 'none'),
                reason=s.get('reason',''), question=it.get('block_question','') if it.get('needs_input') else '',
                wait_identity=s.get('wait_identity',''), supervision=s)


def status(session_id):
    rows = [it for it in _items() if (it.get('supervision') or {}).get('session',{}).get('session_id') == session_id]
    if not rows:
        raise ValueError('session has no takeover binding')
    active = [it for it in rows if it['supervision']['state'] not in TERMINAL]
    if len(active)>1:
        raise ValueError('ambiguous supervision ownership')
    return _response((active or sorted(rows, key=lambda x:x['supervision']['enrolled_at']))[-1])


def _identity(obs):
    return {k: obs[k] for k in ('engine', 'session_id', 'host', 'repo_path')}


def _intent(s, kind, text, turn=''):
    seq = s.get('intent_seq', 0)+1
    s['intent_seq'] = seq
    s['intent'] = dict(key=f"takeover:{s['id']}:{s['generation']}:{kind}:{turn}:{seq}",
                       kind=kind, text=text, state='queued', attempts=0, next_attempt=0)
    s['state'] = 'pending_delivery'


def _claim(it, s):
    sid = s['session']['session_id']
    it.update(status='in_progress', claimed_by=sid, claimed_session_id=sid,
              claimed_machine=q.machine_tag(), claimed_at=q._now_iso(),
              claim_proc=dict(bound='ambient', session_id=sid, worker_id=sid,
                              engine=s['session']['engine'], pid=0))


def _owner_question_unlocked(it, s, question, *, source, turn=''):
    """Record a newly observed owner wait in the caller's queue transaction.

    Existing waits, including malformed legacy waits, require their own explicit
    resolution. Never rewrite their question, ownership or historical episode.
    """
    if it.get('needs_input'):
        wait = q.current_wait(it)
        s.update(state='waiting' if wait and wait['on'] != 'owner' else 'needs_input',
                 reason=it.get('block_question') or question)
        return
    stamp = datetime.now(timezone.utc)
    used = {h.get('at') for h in it.get('history', []) if h.get('event') == 'block'}
    used.add(it.get('blocked_at'))
    episode = stamp.isoformat(timespec='microseconds').replace('+00:00', 'Z')
    while episode in used:
        stamp += timedelta(microseconds=1)
        episode = stamp.isoformat(timespec='microseconds').replace('+00:00', 'Z')
    edge = q._pa_edge_start(it)
    q._supersede_pending_unlocked(it, episode)
    it.update(needs_input=True, block_question=question, block_kind='input',
              blocked_at=episode, updated_at=episode,
              wait=q.wait_record('owner', block_at=episode, by=s['session']['session_id']))
    it.pop('owner_wait_initialization', None)
    q._append_history(it, 'block', by=q._by('system'), at=episode,
                      question=question, kind='input', wait_on='owner',
                      source=source, turn=turn, session_id=s['session']['session_id'])
    q._check_answer_edge_from(it, edge)
    s.update(state='needs_input', reason=question)


def _footer_wait(it, s, f, turn):
    """Honor recorded owner/external waits at adoption and on fresh turns."""
    if not f:
        return False
    next_user = f['next_step_user']
    if next_user.lower().rstrip('.') != 'none' or f['completion']['decision_dependency']['score'] > 0:
        question = next_user if next_user.lower().rstrip('.') != 'none' else 'Owner decision recorded in session footer'
        _owner_question_unlocked(it, s, question, source='takeover-footer', turn=turn)
        return True
    actions = f['completion']['remaining_actions']
    user_actions = [a for a in actions if isinstance(a, dict) and a.get('owner') == 'user']
    if user_actions:
        question = '; '.join(str(a.get('text') or 'Owner action required') for a in user_actions)
        _owner_question_unlocked(it, s, question, source='takeover-footer', turn=turn)
        return True
    external = [a for a in actions if isinstance(a, dict) and a.get('owner') == 'external']
    if external:
        s.update(state='waiting', reason='; '.join(str(a.get('text') or 'External trigger required') for a in external),
                 wait_identity=turn, wait_source='footer')
        return True
    return False


def _new(data, queue, title, repo):
    return q._new_item_unlocked(data, note=title, text=title, source='takeover', proj=queue,
        annotation_id='', url='', title=title, selector='', screenshot_path='', repo_path=repo,
        lane='normal', item_type='', readiness='', priority='', value='', confidence='',
        model_floor='', planner_model='', verifier_model='', submitter='',
        submitter_explicit=False, pre_ack=False, blocked_by=[], gates=None, accept_line='')


def takeover(session_id, ticket_ref='', *, queue='', title='', engine='', dry_run=False, legacy=None):
    """Atomically adopt/create a local ticket and persist intent; never sends inline.

    Legacy import requires caller-fenced CCC ownership and a unique migration_id.
    No legacy enrollment instruction is sent. Cancellation requires explicit
    takeover to enroll again, preserving the prior generation in history.
    """
    if session_id.startswith('@'):
        from . import messages
        resolved = messages.resolve_target(session_id)
        session_id = resolved.get('session_id') or ''
        engine = engine or resolved.get('engine', '')
    obs = observations.observe(session_id, engine, persist=not dry_run)
    if obs.get('error') or not obs.get('repo_path'):
        raise ValueError(obs.get('error') or 'native identity unavailable')
    project = q._norm_project(queue or q._project_from_ident(ticket_ref))
    if project and q._github_backend_for_project(project) is not None:
        raise ValueError('takeover supports local queue storage only')
    if legacy and (not legacy.get('migration_id') or legacy.get('owner_fenced') is not True):
        raise ValueError('legacy import requires migration_id and owner_fenced=true')
    with _lock(), q._FileLock(q._lock_path()):
        data = q._load_unlocked(strict=True)
        rows = data['items']
        binding = [it for it in rows if (it.get('supervision') or {}).get('session',{}).get('session_id') == session_id]
        if not ticket_ref:
            candidates = binding or [it for it in rows if it.get('claimed_session_id') == session_id]
            if len(candidates)==1:
                ticket_ref = candidates[0]['ref']
            elif candidates:
                raise ValueError('ambiguous ticket binding; specify --ticket')
        it = next((r for r in rows if r['ref']==ticket_ref), None) if ticket_ref else None
        if ticket_ref and it is None:
            raise ValueError('ticket not found')
        if it is None:
            if not queue or not title:
                raise ValueError('specify --ticket or explicit --queue and --title')
            it = _new(data, project, title, obs['repo_path'])
        if q._github_backend_for_project(it['project']) is not None:
            raise ValueError('takeover supports local queue storage only')
        if it.get('repo_path') and (q._repo_identity(it['repo_path']) or str(Path(it['repo_path']).resolve())) != (q._repo_identity(obs['repo_path']) or str(Path(obs['repo_path']).resolve())):
            raise ValueError('ticket and session repositories differ')
        old = it.get('supervision') or {}
        if old and old['session'] != _identity(obs):
            raise ValueError('ticket already bound to another canonical session')
        if old and legacy and old.get('legacy_import_id') == legacy['migration_id']:
            return _response(it)
        parked_owner=(it.get('parked') or {}).get('session_id')
        if parked_owner and parked_owner != session_id:
            raise ValueError('ticket has another parked claimant')
        if it.get('claimed_session_id') and it['claimed_session_id'] != session_id:
            raise ValueError('ticket has another claimant')
        if old and old['state'] not in TERMINAL:
            return _response(it)
        if it.get('status') == 'closed':
            raise ValueError('closed ticket cannot be adopted')
        for other in rows:
            if other is it:
                continue
            sup = other.get('supervision') or {}
            if (sup.get('session',{}).get('session_id') == session_id and sup.get('state') not in TERMINAL
                    or other.get('claimed_session_id') == session_id and other.get('status') == 'in_progress'):
                raise ValueError('session already owns another ticket')
        owners = [it.get('claimed_session_id'), (it.get('parked') or {}).get('session_id')]
        if it.get('claimed_by') and session_id not in owners and it['claimed_by'] != session_id:
            raise ValueError('ticket has another claimant')
        if it.get('status') not in ('open', 'in_progress', q.PARKED_STATUS):
            raise ValueError('ticket status is not adoptable')
        if it.get('duplicate_of') or q._plan_active_unlocked(it):
            raise ValueError('duplicate or active plan gate prevents adoption')
        if (it.get('pending_answer') or {}).get('state') in ('routing','delivering','queued','affinity'):
            raise ValueError('settle existing pending answer delivery before takeover')
        q._group_claim_check_unlocked(rows, it)
        if q.blocker_verdict(it, q._refs_index(rows))[0] != 'ok':
            raise ValueError('ticket has unresolved dependency gates')

        now = time.time()
        s = dict(id=old.get('id') or uuid.uuid4().hex, schema_version=1,
                 generation=int(old.get('generation',0))+1, session=_identity(obs),
                 settings=obs.get('settings') or {},
                 origin='legacy' if legacy else 'explicit', policy_version=1,
                 task_brief=it.get('text') or it.get('title') or it.get('note'),
                 authorization_scope='Existing task only; no additional consequential authority.',
                 state='running', enrolled_at=now, processed_turn=obs['completed_turn'],
                 next_deadline=now+IDLE_SECONDS, reminders=0, reason='', intent=None,
                 wait_identity='', answer_generation=int(old.get('answer_generation',0)), legacy_import_id='')
        if legacy:
            allowed = ('reminders','next_deadline','processed_turn','wait_identity','state','enrolled_at','answer_generation')
            s.update({k:legacy[k] for k in allowed if k in legacy})
            if s['state'] not in ('running','waiting','needs_input','cancelled','paused_unreachable'):
                raise ValueError('unsupported legacy state; settle in-flight sends before import')
            if not isinstance(s['reminders'], int) or not 0 <= s['reminders'] <= MAX_REMINDERS:
                raise ValueError('invalid legacy reminder count')
            s['legacy_import_id'] = legacy['migration_id']
        else:
            _intent(s, 'enroll', f"WatchTower now supervises your existing task as {it['ref']}. "
                    'Continue only already authorized work in this same conversation and repository. '
                    f"Use `wt block {it['ref']} --question ...` for owner input; use normal `wt close {it['ref']}` "
                    'with required proof and gates when done. Do not create a replacement worker. '
                    'Ignore repeated takeover action keys already handled. Task: '+str(s['task_brief']))
        if old.get('held_answer') and not legacy:
            _intent(s,'answer','Retained owner answer: '+str(old['held_answer']))
        elif not legacy:
            _footer_wait(it, s, obs.get('footer'), obs['completed_turn'])
        if legacy and s['state']=='needs_input':
            it.update(needs_input=True,block_question=legacy.get('reason') or it.get('block_question') or 'Imported owner question remains unanswered')
        it['supervision'] = s
        if not it.get('needs_input') and it.get('status') != q.PARKED_STATUS:
            _claim(it,s)
        if dry_run:
            return dict(_response(it), dry_run=True)
        q._append_history(it, 'takeover', by=q._by('system'), generation=s['generation'], supervision_id=s['id'])
        q._save_unlocked(data)
        return _response(it)


def cancel(session_id, *, dry_run=False):
    with _lock(), q._FileLock(q._lock_path()):
        data = q._load_unlocked(strict=True)
        rows = [it for it in data['items'] if (it.get('supervision') or {}).get('session',{}).get('session_id') == session_id]
        if not rows:
            raise ValueError('session has no takeover binding')
        for it in rows:
            s=it['supervision']
            if s['state'] in TERMINAL:
                continue
            s.update(generation=s['generation']+1, state='cancelled', reason='Explicitly cancelled; automatic enrollment suppressed')
            if s.get('intent') and s['intent'].get('state') != 'confirmed':
                s['intent']['state']='cancelled'
            q._append_history(it,'takeover_cancel',by=q._by('human'),generation=s['generation'])
        if not dry_run:
            q._save_unlocked(data)
        return dict(_response(rows[-1]), **({'dry_run': True} if dry_run else {}))


def answer_unlocked(it, text, now):
    """Queue.answer transaction hook: retain affinity, including when cancelled."""
    s=it['supervision']
    s['answer_generation']=int(s.get('answer_generation',0))+1
    _claim(it,s)
    q._supersede_pending_unlocked(it, now)
    it.pop('parked',None)
    it.update(needs_input=False, block_question='', block_kind='', blocked_at=None)
    if s['state'] in TERMINAL:
        s['held_answer']=text
        s['reason']='Answer retained; explicit takeover required to resume cancelled supervision'
        return
    _intent(s,'answer',f"Answer for {it['ref']} #{s['answer_generation']}: {text}. Continue the same authorized task.",str(s['answer_generation']))
    s['wait_identity']=''
    s['reason']='Answer accepted for same-session delivery'


def _update(ref, generation, fn):
    with q._FileLock(q._lock_path()):
        data=q._load_unlocked(strict=True)
        it=next((i for i in data['items'] if i['ref']==ref),None)
        if not it or it.get('supervision',{}).get('generation') != generation:
            return None
        fn(it,it['supervision'])
        q._save_unlocked(data)
        return copy.deepcopy(it)


def _tick(it, obs, now):
    """Persist a transition before dispatch. Receipts never close a ticket."""
    s=it['supervision']
    if s['state'] in TERMINAL:
        return
    if it['status']=='closed':
        s.update(state='completed',reason='Ticket closed through normal queue gates')
        if s.get('intent') and s['intent'].get('state') != 'confirmed':
            s['intent']['state']='cancelled'
        return
    if it.get('status')=='in_review' or it.get('gate_pending') or q._plan_active_unlocked(it):
        s.update(state='waiting',wait_identity='queue-gate',reason='Normal queue gate owns the next action')
        return
    owner=it.get('claimed_session_id')
    if owner and owner != s['session']['session_id']:
        s.update(state='paused_unreachable',reason='Ticket claim changed; intervention fenced')
        return
    if it.get('status')=='open':
        _claim(it,s)  # a failed normal close returns work to this same session
    if s.get('wait_identity')=='queue-gate':
        s.update(state='running',wait_identity='',next_deadline=now+IDLE_SECONDS)
    intent=s.get('intent') or {}
    # Always settle a receipt, even if the native turn has since become busy.
    if intent and intent['state'] not in ('confirmed','cancelled'):
        from . import liveness
        row=liveness.delivery_row(intent['key'])
        if row and row['state']=='confirmed':
            intent['state']='confirmed'
            s['next_deadline']=now+IDLE_SECONDS
            if intent['kind']=='remind':
                s['reminders']+=1
            s['state']='running'
        elif row and row['state'] in ('pending','sending'):
            intent['state']='pending'
        elif (row and row['state'] in ('lost','failed')) or (not row and intent['state']=='uncertain'):
            # An aged-out receipt row leaves an uncertain intent; settle it too.
            if row:
                intent['state']='queued'
            # A lost receipt is ambiguous. Settle it from the transcript, then
            # resend at most once under a new nonce; a second loss cancels.
            if row and row['state']=='failed' and intent.get('resent'):
                intent['state']='cancelled'
                s.update(state='cancelled',reason='Takeover resend failed; supervision cancelled')
                return
            if not row or row['state']=='lost':
                # The delivered text always carries the action key.
                nonce=(row or {}).get('nonce') or intent['key']
                tpath=obs.get('transcript') or ''
                try:
                    text=Path(tpath).read_text(errors='replace') if tpath and Path(tpath).is_file() else None
                except OSError:
                    text=None
                landed=bool(nonce) and text is not None and (nonce in text or intent['key'] in text)
                if text is None or not nonce:
                    # No readable transcript (e.g. broker): absence is unproven.
                    s.update(state='paused_unreachable', reason='Delivery outcome unknown; inspect receipt before explicit recovery')
                    intent['state']='uncertain'
                    return
                if landed:
                    intent['state']='confirmed'
                    s.update(state='running',next_deadline=now+IDLE_SECONDS)
                    if intent['kind']=='remind':
                        s['reminders']+=1
                elif intent.get('resent'):
                    intent['state']='cancelled'
                    s.update(state='cancelled',reason='Takeover message lost twice; supervision cancelled')
                    return
                else:
                    turn=intent['key'].split(':')[4] if intent['key'].count(':')>=5 else ''
                    _intent(s,intent['kind'],intent['text'],turn)
                    s['intent']['resent']=True
                    return
    wait=q.current_wait(it)
    if wait and wait.get('on') in ('dependency','external','worker'):
        s.update(state='waiting',wait_identity=json.dumps(wait,sort_keys=True),wait_source='queue',reason='Recorded dependency wait')
        return
    if s.get('wait_source')=='queue' and not wait:
        s.update(state='running',wait_identity='',wait_source='',next_deadline=now+IDLE_SECONDS)
    if it.get('needs_input'):
        s.update(state='needs_input', reason=it.get('block_question','Owner question pending'))
        return
    if obs['state']=='unknown' or obs.get('error'):
        s.update(state='paused_unreachable', reason=obs.get('error') or 'Native state unknown')
        return
    if _identity(obs)!=s['session']:
        s.update(state='paused_unreachable',reason='Canonical session identity changed')
        return
    if obs['state']=='busy':
        s['reason']='Native turn is busy; no intervention'
        return
    fresh=bool(obs['completed_turn'] and obs['completed_turn']!=s['processed_turn'])
    if fresh:
        s['processed_turn']=obs['completed_turn']
        f=obs.get('footer')
        if _footer_wait(it, s, f, obs['completed_turn']):
            return
        if f:
            actions=f['completion']['remaining_actions']
            if (s['state']=='waiting' and not wait and actions
                    and f['completion']['decision_dependency']['score']==0
                    and f['next_step_user'].lower().rstrip('.')=='none'
                    and all(isinstance(a,dict) and a.get('owner')=='agent'
                            and a.get('within_scope') is True for a in actions)):
                s.update(state='running',wait_identity='',wait_source='',next_deadline=now+IDLE_SECONDS,
                         reason='Fresh completed turn resumes authorized agent work')
    if s['state']=='waiting':
        return
    if intent.get('state')=='uncertain':
        return
    if intent and intent.get('state') not in ('confirmed','cancelled'):
        s['state']='pending_delivery'
        return
    s['state']='running'
    if now < max(float(s['next_deadline']),float(obs['activity_at'])+IDLE_SECONDS):
        return
    if s['reminders']>=MAX_REMINDERS:
        from . import outcomes
        oid=outcomes.route_exception(it['ref'])
        if oid:
            # Linked outcome (feature on): a bounded lead diagnosis owns the
            # technical exhaustion before any owner question.
            s.update(state='waiting',wait_identity=f'outcome:{oid}',wait_source='outcome',
                     reason='Reminder budget exhausted; linked outcome lead diagnosis owns the next action')
            q._append_history(it,'takeover_escalation',by=q._by('system'),reason=s['reason'],routed_to=oid)
            return
        _owner_question_unlocked(it, s,
            'Three confirmed work reminders exhausted; owner must review the retained session',
            source='takeover-escalation', turn=s['processed_turn'])
        q._append_history(it,'takeover_escalation',by=q._by('system'),reason=s['reason'])
        return
    _intent(s,'remind',f"WatchTower reminder for {it['ref']}: continue the remaining authorized work in this same session. "
            'If waiting, record the dependency; if an owner decision is required, use wt block. '
            'When finished, use normal wt close with all required proof and gates.',s['processed_turn'])


def reconcile(*, now=None, dry_run=False):
    """Called on every serialized worker tick, regardless of scope/auto-drain."""
    now=time.time() if now is None else now
    if dry_run:
        return [_response(it) for it in _items() if it.get('supervision')]
    results=[]
    with _lock():
        for it in _items():
            s=it.get('supervision')
            if not s or s['state'] in TERMINAL:
                continue
            try:
                obs=observations.observe(s['session']['session_id'],s['session']['engine'])
                updated=_update(it['ref'],s['generation'],lambda i,r:_tick(i,obs,now))
                if not updated:
                    continue
                s=updated['supervision']; intent=s.get('intent') or {}
                if (s['state']=='pending_delivery' and obs['state']=='idle'
                    and intent.get('state')=='queued' and intent.get('next_attempt',0)<=now):
                    from . import liveness
                    # Intent and attempts are committed before any transport side effect.
                    def attempting(i,r):
                        r['intent']['attempts']+=1
                        r['intent']['next_attempt']=now+min(1200,30*2**min(r['intent']['attempts'],5))
                    updated=_update(it['ref'],s['generation'],attempting)
                    s=updated['supervision']; intent=s['intent']
                    sent=liveness.deliver(s['session']['session_id'],intent['text']+'\nAction key: '+intent['key'],
                        purpose='takeover', dedupe_key=intent['key'],ref=it['ref'],session_id=s['session']['session_id'],
                        engine=s['session']['engine'],transports=('message',),
                        message=dict(fn='send',mode='send',queue_on_fail=False,same_session=True,session_settings=s.get('settings',{}),engine=s['session']['engine'],
                                     dedupe_key=intent['key'],ticket_ref=it['ref'],ticket_session=s['session']['session_id'],
                                     delegate_timeout_s=5),meta=dict(generation=s['generation']))
                    def outcome(i,r):
                        # An answer may replace this intent during transport.
                        if r['intent']['key']!=intent['key']:
                            return
                        r['intent']['state']='pending' if sent.get('ok') else 'queued'
                        r['reason']=sent.get('error','')
                        if not sent.get('ok'):
                            r['state']='paused_unreachable'
                    updated=_update(it['ref'],s['generation'],outcome)
                results.append(_response(updated))
            except Exception as exc:
                results.append(dict(ticket=it['ref'],state='error',reason=str(exc)))
    return results


def resume_from_outcome(ref, outcome_id):
    """Outcome lead result: resume same-session supervision after a routed
    reminder exhaustion. Once per supervision generation; never re-sends an
    uncertain intent and never takes over another claim."""
    with _lock(), q._FileLock(q._lock_path()):
        data=q._load_unlocked(strict=True)
        it=next((i for i in data['items'] if i['ref']==ref),None)
        s=(it or {}).get('supervision') or {}
        if not s or s.get('wait_source')!='outcome' or s.get('wait_identity')!=f'outcome:{outcome_id}':
            return dict(ref=ref, resumed=False, reason='supervision is not waiting on this outcome')
        if (s.get('intent') or {}).get('state')=='uncertain':
            return dict(ref=ref, resumed=False, reason='delivery outcome unknown; receipt must be reconciled first')
        if int(s.get('outcome_resumed_generation') or 0)==int(s['generation']):
            return dict(ref=ref, resumed=False, reason='already resumed once for this generation')
        s.update(state='running',wait_identity='',wait_source='',reminders=0,next_deadline=time.time()+IDLE_SECONDS,
                 outcome_resumed_generation=s['generation'],reason='Outcome lead resumed same-session supervision')
        q._append_history(it,'takeover_outcome_resume',by=q._by('system'),outcome_id=outcome_id,generation=s['generation'])
        q._save_unlocked(data)
        return dict(ref=ref, resumed=True)


def status_all():
    return dict(schema_version=1, capabilities=['manual','legacy-import','observations','scoped-candidates'],
                supervisions=[_response(it) for it in _items() if it.get('supervision')])


def _enroll_policy(path, *, now=None):
    """Opt-in candidate policy; no CCC state reads or implicit ticket creation.

    File: WATCHTOWER_DATA_DIR/takeover-policy.json. Each scope has repo_path,
    queue and candidates [{session_id, engine, ticket, task_brief,
    authorized:true, related_session_ids:[]}]. Legacy CCC candidates require
    delegation_fenced=true at top level. All siblings must be idle >=120s.
    """
    now=time.time() if now is None else now
    if not path.exists():
        return []
    try:
        policy=json.loads(path.read_text())
    except (OSError,ValueError) as exc:
        return [dict(state='ineligible',reason=str(exc))]
    if policy.get('schema_version')!=1 or policy.get('enabled') is not True:
        return []
    result=[]
    for scope in policy.get('scopes',[]):
        for c in scope.get('candidates',[]):
            why=''
            sid=c.get('session_id',''); engine=c.get('engine','')
            if not scope.get('repo_path') or not scope.get('queue'):
                why='explicit repository/queue scope required'
            elif c.get('authorized') is not True or not c.get('task_brief') or not c.get('ticket'):
                why='explicit authorized task and ticket binding required'
            elif c.get('origin')=='ccc' and policy.get('delegation_fenced') is not True:
                why='CCC delegation is not fenced'
            obs=observations.observe(sid,engine)
            it=q.get(c.get('ticket','')) if c.get('ticket') else None
            f=obs.get('footer')
            if not why and (not it or it.get('project')!=scope['queue'] or it.get('supervision')):
                why='missing/scoped-out ticket or existing/suppressed generation'
            if not why and (obs.get('repo_path')!=scope['repo_path'] or obs['state']!='idle' or obs.get('error')
                            or not 120 <= now-obs['activity_at'] <= 172800):
                why='native session is outside scope, busy, unknown, too recent or stale'
            if not why and (not f or f['next_step_user'].lower().rstrip('.')!='none'
                            or f['completion']['decision_dependency']['score']!=0
                            or f['completion']['resolution']['score']>=0.9
                            or not f['completion']['remaining_actions']):
                why='no eligible unfinished-work footer'
            if not why:
                actions=f['completion']['remaining_actions']
                if any(not isinstance(a,dict) or a.get('owner')!='agent' or a.get('within_scope') is not True for a in actions):
                    why='remaining work is not explicitly agent-owned and authorized'
            if not why:
                for sibling in c.get('related_session_ids',[]):
                    other=observations.observe(sibling.get('session_id',''),sibling.get('engine','')) if isinstance(sibling,dict) else observations.observe(sibling)
                    if other['state']!='idle' or other.get('error') or now-other['activity_at']<120:
                        why='related session is busy, unknown or recently active'
                        break
            if why:
                result.append(dict(session_id=sid,state='ineligible',reason=why))
                continue
            try:
                result.append(takeover(sid,c['ticket'],engine=engine))
            except (ValueError,OSError) as exc:
                result.append(dict(session_id=sid,state='ineligible',reason=str(exc)))
    # Persist eligibility reasons for inspection; no mutation of candidate authority.
    dest=storage.data_dir()/'takeover-candidates.json'
    with storage.file_lock(dest.with_suffix('.lock')):
        with storage.atomic_destination(dest) as tmp:
            tmp.write_text(json.dumps(dict(schema_version=1,observed_at=now,results=result)))
    return result


def replace_candidates(source, policy):
    """Replace only a publisher's candidates; no enrollment or delivery inline."""
    if source != 'ccc':
        raise ValueError('supported candidate publisher: ccc')
    if not isinstance(policy,dict) or policy.get('schema_version')!=1:
        raise ValueError('candidate policy schema_version must be 1')
    if policy.get('enabled') is True and policy.get('delegation_fenced') is not True:
        raise ValueError('enabled CCC candidates require delegation_fenced=true')
    scopes=copy.deepcopy(policy.get('scopes',[]))
    if not isinstance(scopes,list):
        raise ValueError('scopes must be a list')
    for scope in scopes:
        if not isinstance(scope,dict) or not scope.get('repo_path') or not scope.get('queue'):
            raise ValueError('each scope requires repo_path and queue')
        scope['source']=source
        for c in scope.get('candidates',[]):
            if not isinstance(c,dict) or not isinstance(c.get('session_id'),str) or not c['session_id'].strip() or not isinstance(c.get('engine'),str) or not c['engine'].strip():
                raise ValueError('candidate requires session_id and engine')
            c['origin']=source
    path=storage.data_dir()/'takeover-policy-ccc.json'
    path.parent.mkdir(parents=True,exist_ok=True)
    with storage.file_lock(path.with_suffix('.lock')):
        with storage.atomic_destination(path) as tmp:
            tmp.write_text(json.dumps(dict(schema_version=1,enabled=policy.get('enabled') is True,
                delegation_fenced=policy.get('delegation_fenced') is True,scopes=scopes)))
    return dict(schema_version=1,source=source,enabled=policy.get('enabled') is True,
                candidates=sum(len(s.get('candidates',[])) for s in scopes))


def enroll_candidates(*, now=None):
    return [row for name in ('takeover-policy.json','takeover-policy-ccc.json')
            for row in _enroll_policy(storage.data_dir()/name,now=now)]

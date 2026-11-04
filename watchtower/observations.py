# Copyright (c) 2026 Amir Fish. All rights reserved.
# SPDX-License-Identifier: LicenseRef-WatchTower-Software-License

"""Version 1 engine-neutral observations; registration confers no authority.

Only completed native turns carry footer evidence. The incremental index is a
cache, never delivery proof. Native readers work without a broker; other
engines use the optional same-session broker observation contract.
"""
from __future__ import annotations
import hashlib
import json
import math
import os
from pathlib import Path
import re
import socket
import time
from datetime import datetime
from urllib.parse import urlencode
from . import storage

SCHEMA_VERSION = 1
MAX_READ = 8 * 1024 * 1024


def footer(text):
    blocks = re.findall(r'<session-state>(.*?)</session-state>', text, re.S)
    if not blocks:
        return None
    block = blocks[-1]
    match = re.search(r'^COMPLETION:\s*(\{[^\n]*\})\s*$', block, re.M)
    user = re.search(r'^NEXT_STEP_USER:\s*(.*)$', block, re.M)
    try:
        value = json.loads(match[1]) if match else None
        if not isinstance(value, dict) or value.get('version') != 1 or not user:
            return None
        for axis in ('resolution', 'verification', 'delivery', 'decision_dependency'):
            score = value[axis]['score']
            if isinstance(score, bool) or not isinstance(score, (int, float)) or not 0 <= score <= 1:
                return None
        if not isinstance(value['remaining_actions'], list) or not isinstance(value['caveats'], list):
            return None
        if not isinstance(value['scope'], dict) or not value['scope'].get('goal'):
            return None
        return {'completion': value, 'next_step_user': user[1].strip(), 'raw': block}
    except (ValueError, KeyError, TypeError):
        return None


def _epoch(value):
    try:
        return datetime.fromisoformat(str(value).replace('Z', '+00:00')).timestamp()
    except (ValueError, TypeError):
        return 0.0


def _roots():
    return {'claude': Path(os.environ.get('CLAUDE_CONFIG_DIR', str(Path.home()/'.claude')))/'projects',
            'codex': Path(os.environ.get('CODEX_HOME', str(Path.home()/'.codex')))/'sessions'}


def _paths(engine, sid=''):
    root = _roots()[engine]
    if not root.is_dir():
        raise OSError(f'{engine} native store unavailable: {root}')
    pattern = f'*/{sid or "*"}.jsonl' if engine == 'claude' else f'*/*/*/*{sid}.jsonl'
    if engine != 'codex':
        return root.glob(pattern)
    # Workers may run under another Codex account's home (CCC worker account).
    from . import config
    roots = [root] + [r for r in config.codex_session_roots() if r.resolve() != root.resolve()]
    return (p for r in roots for p in r.glob(pattern))


def _empty(session_id, engine='', source='native'):
    return dict(schema_version=1, session_id=str(session_id), engine=engine, host=socket.gethostname(),
                repo_path='', transcript='', cursor=0, completed_turn='', activity_at=0,
                state='unknown', footer=None, settings={}, observed_at=time.time(), source=source, error='')


def broker_observe(session_id, engine='', *, nonce=''):
    """Read the broker adapter, validating identity before trusting any evidence.

    No local transcript format or session-id shape is assumed. A nonce result
    is evidence only when the broker found persisted same-session user input;
    HTTP acceptance, busy state and transcript growth never prove receipt.
    """
    from . import messages
    out = _empty(session_id, engine, 'broker')
    base = messages._delegate_base()
    if not base:
        return dict(out, error='session observation broker unavailable: no delegate configured')
    query = dict(session_id=str(session_id), engine=engine)
    if nonce:
        query['nonce'] = nonce
    try:
        row = messages._get_json(base+'/api/takeover/observe?'+urlencode(query), timeout_s=5)
        if not isinstance(row, dict):
            raise ValueError('session observation broker unreachable or protocol unavailable')
        if row.get('schema_version') != 1 or row.get('session_id') != str(session_id):
            raise ValueError('broker observation schema or session identity mismatch')
        if not isinstance(row.get('engine'), str) or not row['engine'] or (engine and row['engine'] != engine):
            raise ValueError('broker observation engine identity mismatch')
        if row.get('error'):
            raise ValueError(str(row['error']))
        if any(not isinstance(row.get(k), str) or not row[k] for k in ('host', 'repo_path')):
            raise ValueError('broker canonical host/repository identity missing')
        if row.get('state') not in ('busy', 'idle', 'unknown'):
            raise ValueError('broker turn state invalid')
        for k in ('activity_at', 'observed_at'):
            if isinstance(row.get(k), str):
                row[k] = datetime.fromisoformat(row[k].replace('Z', '+00:00')).timestamp()
            if isinstance(row.get(k), bool) or not isinstance(row.get(k), (int, float)) or not math.isfinite(row[k]) or row[k] < 0:
                raise ValueError('broker observation timestamps invalid')
        if not isinstance(row.get('settings', {}), dict):
            raise ValueError('broker session settings invalid')
        if any(not isinstance(row.get(k, ''), str) for k in ('transcript', 'completed_turn')):
            raise ValueError('broker transcript/turn identity invalid')
        # Broker cursors may be opaque store positions rather than native
        # byte offsets; their interpretation belongs to the engine adapter.
        if isinstance(row.get('cursor'), bool) or not isinstance(row.get('cursor'), (int, str)):
            raise ValueError('broker cursor invalid')
        for k in out:
            if k in row and k not in ('footer', 'source'):
                out[k] = row[k]
        # The broker may supply raw final text or an already parsed footer.
        # Revalidate both through the same WT parser used by native readers.
        parsed = row.get('footer')
        if row.get('completed_text'):
            out['footer'] = footer(row['completed_text'])
        elif isinstance(parsed, dict):
            out['footer'] = footer('<session-state>\nNEXT_STEP_USER: '+str(parsed.get('next_step_user', ''))+
                                   '\nCOMPLETION: '+json.dumps(parsed.get('completion'))+'\n</session-state>')
        elif isinstance(parsed, str):
            out['footer'] = footer(parsed)
        if nonce:
            evidence = row.get('receipt_evidence')
            out['receipt_confirmed'] = (row.get('receipt_confirmed') is True
                and isinstance(evidence, dict) and evidence.get('session_id') == str(session_id)
                and evidence.get('nonce') == nonce and evidence.get('role') == 'user'
                and evidence.get('persisted') is True
                and isinstance(evidence.get('source'), str) and bool(evidence['source']))
            out['receipt_evidence'] = row.get('receipt_evidence') if out['receipt_confirmed'] else None
        return out
    except (OSError, ValueError, TypeError) as exc:
        return dict(_empty(session_id, engine, 'broker'), error=str(exc))


def observe(session_id, engine='', *, persist=True, transcript=None):
    """Return v1 identity/activity/turn/footer observation, including unknown errors.

    Explicit engine avoids ambiguous IDs. `persist=False` is read-only dry run.
    `transcript` is a native path override for controlled fixtures/adapters.
    """
    sid = str(session_id)
    out = _empty(sid, engine)
    if engine and engine not in _roots():
        return broker_observe(sid, engine)
    matches = []
    for eng in ([engine] if engine else _roots()):
        try:
            paths = [Path(transcript)] if transcript else list(_paths(eng, sid))
            matches.extend((eng, p) for p in paths)
        except OSError:
            pass  # No native identity; ask the broker before declaring it missing.
    if len(matches) != 1:
        if len(matches)>1:
            return dict(out, error='ambiguous native identity')
        return broker_observe(sid, engine)
    eng, path = matches[0]
    out.update(engine=eng, transcript=str(path))
    cache = storage.data_dir()/'observations'/ (hashlib.sha256(str(path).encode()).hexdigest()+'.json')
    try:
        stat = path.stat()
        signature = [stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns]
        old = json.loads(cache.read_text()) if cache.exists() else {}
        if old.get('reader_version') == 2 and old.get('signature') == signature and old.get('observation',{}).get('cursor') == stat.st_size:
            return dict(old['observation'], observed_at=time.time())
        prior = old.get('observation', {})
        incremental = (old.get('reader_version') == 2 and old.get('signature', [])[:2] == signature[:2]
                       and 0 <= prior.get('cursor', 0) <= stat.st_size)
        if incremental:
            out.update(prior, observed_at=time.time())
            out['state'] = old.get('parsed_state', out['state'])
        offset = out['cursor'] if incremental else 0
        pending = old.get('pending', '') if incremental else ''
        with path.open('rb') as f:
            f.seek(offset)
            raw = f.read(MAX_READ)
        end = raw.rfind(b'\n')+1
        for line in raw[:end].splitlines():
            ev = json.loads(line)
            out['activity_at'] = max(out['activity_at'], _epoch(ev.get('timestamp')))
            if eng == 'codex':
                p = ev.get('payload') or {}
                if ev.get('type') == 'session_meta':
                    if p.get('id') != sid:
                        raise ValueError('native session identity mismatch')
                    out['repo_path'] = p.get('cwd', '')
                if ev.get('type') == 'turn_context':
                    out['settings'] = {k:p[k] for k in ('model','approval_policy','sandbox_policy','effort') if k in p}
                if ev.get('type') == 'event_msg':
                    kind = p.get('type')
                    if kind == 'task_started':
                        out['state'] = 'busy'
                    elif kind in ('task_complete', 'task_completed'):
                        out.update(state='idle', completed_turn=str(p.get('turn_id') or hashlib.sha256(line).hexdigest()),
                                   footer=footer(p.get('last_agent_message') or pending))
                    elif kind in ('turn_aborted', 'error'):
                        out['state'] = 'unknown'
                if ev.get('type') == 'response_item' and p.get('type') == 'message':
                    if p.get('role') == 'user':
                        out['state'] = 'busy'
                    elif p.get('role') == 'assistant':
                        pending = '\n'.join(c.get('text', '') for c in p.get('content', []))
            else:
                if ev.get('sessionId') and ev['sessionId'] != sid:
                    raise ValueError('native session identity mismatch')
                out['repo_path'] = ev.get('cwd') or out['repo_path']
                msg = ev.get('message') or {}
                if ev.get('type') == 'user':
                    out['state'] = 'busy'
                if ev.get('type') == 'assistant':
                    out['state'] = 'busy'
                    if ev.get('isApiErrorMessage') or ev.get('error'):
                        out['state'] = 'unknown'
                        continue
                    if msg.get('stop_reason') in ('end_turn', 'stop_sequence'):
                        content = msg.get('content', [])
                        text = content if isinstance(content, str) else '\n'.join(c.get('text','') for c in content)
                        out.update(state='idle', completed_turn=str(ev.get('uuid') or msg.get('id') or hashlib.sha256(line).hexdigest()), footer=footer(text))
        out['cursor'] = offset+end
        parsed_state = out['state']
        if out['cursor'] < stat.st_size:
            out.update(state='unknown', error='incremental scan incomplete')
        else:
            out['error'] = ''
        if not out['repo_path']:
            out.update(state='unknown', error='native repository identity missing')
        if persist:
            cache.parent.mkdir(parents=True, exist_ok=True)
            with storage.file_lock(cache.with_suffix('.lock')):
                with storage.atomic_destination(cache) as tmp:
                    tmp.write_text(json.dumps(dict(reader_version=2, signature=signature, observation=out, pending=pending, parsed_state=parsed_state)))
        return out
    except (OSError, ValueError, TypeError) as exc:
        return dict(out, state='unknown', error=str(exc))


def discover(*, engine='', limit=100, session_ids=()):
    """Bounded inventory, explicitly non-authoritative; never enrolls anything."""
    if session_ids:
        rows = [observe(s, engine) for s in list(session_ids)[:limit]]
        return dict(schema_version=1, authoritative=False, observations=rows, errors=[])
    rows, errors = [], []
    for eng in ([engine] if engine else _roots()):
        if eng not in _roots():
            errors.append(f'{eng} inventory requires explicit session_ids for broker observation')
            continue
        try:
            for p in _paths(eng):
                if len(rows) >= limit:
                    break
                sid = p.stem if eng == 'claude' else p.stem[-36:]
                rows.append(observe(sid, eng, transcript=p))
        except OSError as exc:
            errors.append(str(exc))
    return dict(schema_version=1, authoritative=False, observations=rows, errors=errors)

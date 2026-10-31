# Copyright (c) 2026 Amir Fish. All rights reserved.
# SPDX-License-Identifier: LicenseRef-WatchTower-Software-License

"""Version 1 headless native observations; registration confers no authority.

Only completed native turns carry footer evidence. The incremental index is a
cache, never delivery proof. No CCC imports or server requests.
"""
from __future__ import annotations
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import time
from datetime import datetime
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
    return root.glob(pattern)


def observe(session_id, engine='', *, persist=True, transcript=None):
    """Return v1 identity/activity/turn/footer observation, including unknown errors.

    Explicit engine avoids ambiguous IDs. `persist=False` is read-only dry run.
    `transcript` is a native path override for controlled fixtures/adapters.
    """
    sid = str(session_id)
    out = dict(schema_version=1, session_id=sid, engine=engine, host=socket.gethostname(),
               repo_path='', transcript='', cursor=0, completed_turn='', activity_at=0,
               state='unknown', footer=None, settings={}, observed_at=time.time(), source='native', error='')
    if engine and engine not in _roots():
        return dict(out, error=f'unsupported engine: {engine}')
    matches, errors = [], []
    for eng in ([engine] if engine else _roots()):
        try:
            paths = [Path(transcript)] if transcript else list(_paths(eng, sid))
            matches.extend((eng, p) for p in paths)
        except OSError as exc:
            errors.append(str(exc))
    if len(matches) != 1:
        return dict(out, error='ambiguous native identity' if len(matches)>1 else '; '.join(errors) or 'session not found')
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
            errors.append(f'unsupported engine: {eng}')
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

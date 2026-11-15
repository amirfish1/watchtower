"""OPS-463: independent hosts must not both return a GitHub claim."""
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from watchtower import github_backend as gb
from test_github_body_writes import _setup
from test_github_backend import restore_watchtower_modules  # noqa: F401

CHILD = r'''
import json, os, subprocess, sys, time
from pathlib import Path
from watchtower.github_backend import GitHubIssuesBackend
from watchtower import github_write_lock as lock
b = GitHubIssuesBackend('GHI', repo='test-owner/test-repo', auto_drain=True,
                        grace_s=0, partition_by_label=False, queue_label='watchtower:GHI')
root = Path(os.environ['HOST_EVENTS'])
name = os.environ['HOST_NAME']
mode = os.environ['HOST_MODE']
real_get, real_api = b.get, b._gh_api
first = True

def get(ref):
    global first
    item = real_get(ref)
    if first:
        first = False
        (root / (name + '-read')).touch()
        if mode == 'pause':
            deadline = time.monotonic() + 30
            while not (root / 'continue').exists():
                if time.monotonic() > deadline: raise RuntimeError('parent did not continue')
                time.sleep(.02)
    return item

def api(method, path, *args, **kw):
    result = real_api(method, path, *args, **kw)
    if method == 'POST' and path.endswith('/labels') and result.returncode:
        (root / (name + '-contended')).touch()
    return result
b.get, b._gh_api = get, api
if mode in ('crash', 'orphan-child'):
    with b._body_lock():
        if mode == 'orphan-child':
            child = subprocess.Popen([sys.executable, '-c',
                "import time; from pathlib import Path; "
                "p=Path(__import__('os').environ['HOST_EVENTS'])/'child-done'; "
                "exec('while not p.exists(): time.sleep(.02)')"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, **lock.child_options())
            (root / 'orphan-pid').write_text(str(child.pid))
        (root / (name + '-locked')).touch()
        os._exit(42)
try:
    item = b.claim_by_ref('GHI-1', name)
    print(json.dumps({'ok': True, 'claimed_by': item['claimed_by']}))
except Exception as exc:
    print(json.dumps({'ok': False, 'error': str(exc)}))
'''


def wait_path(path):
    deadline = time.monotonic() + 30
    while not path.exists():
        if time.monotonic() >= deadline:
            raise AssertionError(f"no event {path}")
        time.sleep(.02)


def host(tmp_path, name, mode, *, home=None):
    home = home or tmp_path / name
    home.mkdir(exist_ok=True)
    env = dict(os.environ, HOME=str(home), HOST_EVENTS=str(tmp_path), HOST_NAME=name,
               HOST_MODE=mode, WATCHTOWER_STORE=str(home / 'queue.json'),
               WATCHTOWER_GH_CLAIM_LOCKS_DIR=str(home / 'claim-locks'))
    env.pop('WT_WORKER_ID', None)
    return subprocess.Popen([sys.executable, '-c', CHILD], env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def finish(proc):
    out, err = proc.communicate(timeout=45)
    assert proc.returncode == 0, (out, err)
    return json.loads(out)


def stop(proc):
    if proc.poll() is None:
        proc.kill()
    proc.wait(timeout=10)


def test_isolated_hosts_exclude_stale_claim_and_owner_mutations(tmp_path, monkeypatch):
    state, q, ref, backend = _setup(tmp_path, monkeypatch)
    a = host(tmp_path, 'worker-a', 'pause')
    b = None
    try:
        wait_path(tmp_path / 'worker-a-read')
        b = host(tmp_path, 'worker-b', 'claim')
        wait_path(tmp_path / 'worker-b-contended')
        # B reached the SERVER lock while A held a snapshot. It did not get
        # to read that same open issue, even with independent local files.
        assert not (tmp_path / 'worker-b-read').exists()
        (tmp_path / 'continue').touch()
        assert finish(a) == {'ok': True, 'claimed_by': 'worker-a'}
        loser = finish(b)
        assert loser['ok'] is False and 'not open' in loser['error']
    finally:
        stop(a)
        if b: stop(b)
    old = q.get(ref)
    assert old['claimed_by'] == 'worker-a'
    assert q.release(ref, 'worker-a')['status'] == 'open'
    backend.claim_by_ref(ref, 'worker-b')
    with pytest.raises(ValueError, match='claimed by worker-b'):
        q.release(ref, 'worker-a')
    with pytest.raises(ValueError, match='claimed by worker-b'):
        q.close(ref, 'worker-a', resolution={'summary': 'stale close'})
    # A release that began with an OLD snapshot must recheck it under the
    # mutex, even when both the old and the new status are in_progress.
    with monkeypatch.context() as mp:
        mp.setattr(q, 'get', lambda ident: old)
        assert q.release(ref, 'worker-a') is None
    assert backend.get(ref)['claimed_by'] == 'worker-b'
    assert json.loads(state.read_text())['issues'][0]['state'] == 'OPEN'
    q.close(ref, 'worker-b', resolution={'summary': 'current owner done'})
    with pytest.raises(ValueError, match='already closed'):
        q.close(ref, 'worker-a', resolution={'summary': 'stale reclose'})
    assert backend.get(ref)['resolution']['summary'] == 'current owner done'


@pytest.mark.parametrize('mode', ['crash', 'orphan-child'])
def test_crash_recovery_requires_owning_host_and_drained_children(tmp_path, monkeypatch, mode):
    state, q, ref, backend = _setup(tmp_path, monkeypatch)
    from watchtower import github_write_lock as lock
    owner_home = tmp_path / 'owner'
    crashed = host(tmp_path, 'owner', mode, home=owner_home)
    try:
        wait_path(tmp_path / 'owner-locked')
        crashed.communicate(timeout=30)
        assert crashed.returncode == 42
        original = json.loads(state.read_text())['mutex_labels'][lock.LABEL]
        monkeypatch.setattr(lock, 'WAIT_SECONDS', 0)
        # This is the wrong host/store. A dead foreign PID is no proof here.
        with pytest.raises(gb.GitHubBackendError, match='write lock busy'):
            with backend._body_lock(): pytest.fail('foreign lock stolen')
        monkeypatch.setenv('WATCHTOWER_STORE', str(owner_home / 'queue.json'))
        if mode == 'orphan-child':
            try:
                with pytest.raises(gb.GitHubBackendError, match='write lock busy'):
                    with backend._body_lock(): pytest.fail('child request was not drained')
                assert json.loads(state.read_text())['mutex_labels'][lock.LABEL] == original
            finally:
                (tmp_path / 'child-done').touch()
            # Wait for the inherited request fence to drain, not just a PID
            # to disappear (an adopted child may remain a zombie).
            deadline = time.monotonic() + 15
            while True:
                try:
                    with backend._body_lock():
                        break
                except gb.GitHubBackendError as exc:
                    if 'write lock busy' not in str(exc) or time.monotonic() >= deadline:
                        raise
                    time.sleep(.02)
        else:
            with backend._body_lock():
                replacement = json.loads(state.read_text())['mutex_labels'][lock.LABEL]
                assert replacement['node_id'] != original['node_id']
                # Delayed cleanup of the OLD object cannot erase this one.
                with pytest.raises(gb.GitHubBackendError, match='API failed'):
                    lock._delete(backend, original['node_id'])
                assert json.loads(state.read_text())['mutex_labels'][lock.LABEL] == replacement
        assert json.loads(state.read_text())['mutex_labels'] == {}
        assert backend.claim_by_ref(ref, 'recovered')['claimed_by'] == 'recovered'
    finally:
        (tmp_path / 'child-done').touch()
        stop(crashed)


def test_lock_api_failure_never_writes_issue_or_falls_back(tmp_path, monkeypatch):
    state, q, ref, backend = _setup(tmp_path, monkeypatch)
    before = json.loads(state.read_text())['issues']
    real_api = backend._gh_api
    def denied(method, path, *args, **kw):
        if method == 'POST':
            return subprocess.CompletedProcess([], 1, '{}', 'gh: Forbidden (HTTP 403)')
        return real_api(method, path, *args, **kw)
    monkeypatch.setattr(backend, '_gh_api', denied)
    with pytest.raises(gb.GitHubBackendError, match='Cannot acquire GitHub write lock'):
        backend.claim_by_ref(ref, 'denied')
    assert json.loads(state.read_text())['issues'] == before


def test_failed_cleanup_recovers_ended_transaction_in_live_process(tmp_path, monkeypatch):
    state, q, ref, backend = _setup(tmp_path, monkeypatch)
    from watchtower import github_write_lock as lock
    real_api = backend._gh_api
    failed = []
    def lost_cleanup(method, path, *args, **kw):
        if path == 'graphql' and not failed:
            failed.append(True)
            return subprocess.CompletedProcess([], 1, '{}', 'gh: Service Unavailable (HTTP 503)')
        return real_api(method, path, *args, **kw)
    monkeypatch.setattr(backend, '_gh_api', lost_cleanup)
    with pytest.raises(gb.GitHubBackendError, match='API failed'):
        with backend._body_lock():
            pass
    stranded = json.loads(state.read_text())['mutex_labels'][lock.LABEL]
    monkeypatch.setattr(lock, 'WAIT_SECONDS', 0)
    # The daemon PID is STILL alive. Its completed transaction, not its
    # liveness or a TTL, is what makes cleanup safe to retry.
    with backend._body_lock():
        replacement = json.loads(state.read_text())['mutex_labels'][lock.LABEL]
        assert replacement['node_id'] != stranded['node_id']
    assert json.loads(state.read_text())['mutex_labels'] == {}

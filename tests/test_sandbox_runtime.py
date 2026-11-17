"""WT-86 runtime integrity and actual native launcher acceptance."""
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess

import pytest

from watchtower import sandbox_runtime as runtime, sandbox_stages as ss
from test_sandbox_policy import _git, base, native  # noqa: F401
from test_sandbox_stages import trusted  # noqa: F401


def test_offline_runtime_runs_system_python_without_host_environment(tmp_path):
    env, proof = runtime.prepare(tmp_path)
    result = subprocess.run(['/usr/bin/python3', '-c',
        'import pytest, pluggy, packaging, iniconfig, pygments; '
        'import importlib.metadata as m; print(m.version("pytest"))'],
        cwd='/', env={'PATH': '/usr/bin:/bin', **env}, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == '9.1.1'
    assert proof['files'] > 400 and proof['offline'] is True
    assert hashlib.sha256((tmp_path / 'offline-pytest.zip').read_bytes()).hexdigest() == proof['archive_sha256']
    assert json.loads((tmp_path / 'offline-pytest.manifest.json').read_text())['schema'] == 1
    with pytest.raises(FileExistsError):
        runtime.prepare(tmp_path)


@pytest.mark.parametrize('problem', ['missing', 'changed', 'linked', 'writable'])
def test_runtime_refuses_missing_or_untrusted_controller_assets(tmp_path, monkeypatch, problem):
    assets = tmp_path / 'controller'
    assets.mkdir()
    for name in runtime.PINS:
        (assets / name).write_bytes((Path(runtime.__file__).parent / name).read_bytes())
        (assets / name).chmod(0o644)
    path = assets / 'sandbox_pytest_runtime.zip'
    if problem == 'missing':
        path.unlink()
    elif problem == 'changed':
        path.write_bytes(b'hostile archive')
    elif problem == 'linked':
        path.unlink()
        path.symlink_to(Path(runtime.__file__).parent / path.name)
    else:
        path.chmod(0o666)
    monkeypatch.setattr(runtime, '__file__', str(assets / 'sandbox_runtime.py'))
    store = tmp_path / 'store'
    store.mkdir()
    with pytest.raises((OSError, ValueError)):
        runtime.prepare(store)
    assert not list(store.iterdir())


def test_native_normal_launcher_pytest_and_boundaries(native, trusted, monkeypatch):
    repo, outer = trusted['repo'], trusted['base']
    host_python = outer / 'host-python'
    host_python.mkdir()
    canary = outer / 'host-import-ran'
    (host_python / 'sitecustomize.py').write_text(f'open({str(canary)!r}, "w").write("bad")')
    monkeypatch.setenv('PYTHONPATH', str(host_python))
    monkeypatch.setenv('PYTEST_PLUGINS', 'host_plugin_must_not_load')
    monkeypatch.setenv('PYTEST_ADDOPTS', '--bad-host-option')
    monkeypatch.setenv('WATCHTOWER_TEST_SECRET', 'must-not-be-inherited')
    outside = outer / 'forbidden-host-write'
    (repo / 'test_runtime_acceptance.py').write_text('''import os, socket, pytest

def test_runtime_and_boundaries():
    assert pytest.__version__ == "9.1.1"
    assert os.environ["PYTEST_DISABLE_PLUGIN_AUTOLOAD"] == "1"
    assert os.environ["PYTHONNOUSERSITE"] == "1"
    for key in ("PYTEST_PLUGINS", "PYTEST_ADDOPTS", "WATCHTOWER_TEST_SECRET"):
        assert key not in os.environ
    for family in (socket.AF_INET, socket.AF_INET6):
        with pytest.raises(OSError):
            socket.socket(family, socket.SOCK_STREAM)
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    with pytest.raises(OSError):
        s.bind(os.path.join(os.environ["TMPDIR"], "deny.sock"))
    s.close()
    try:
        open(os.environ["OUTSIDE"], "w").write("bad")
    except OSError:
        pass
    with pytest.raises(OSError):
        open("/dev/shm/wt86-runtime-denied", "w")
''')
    _git(repo, 'add', 'test_runtime_acceptance.py')
    _git(repo, 'commit', '-qm', 'native runtime acceptance')
    head = _git(repo, 'rev-parse', 'HEAD')
    from watchtower.sandbox_handoff import sandbox_test
    result = sandbox_test(repo, head,
        f'OUTSIDE={shlex.quote(str(outside))} python3 -m pytest test_runtime_acceptance.py -q')
    assert result['passed'], result
    assert result['runtime']['distributions']['pytest'] == '9.1.1'
    assert not outside.exists() and not canary.exists()
    # Injected assertion failure must be a real pytest failure, not setup success.
    (repo / 'test_runtime_acceptance.py').write_text('def test_injected():\n    assert False, "WT86-injected"\n')
    _git(repo, 'add', 'test_runtime_acceptance.py')
    _git(repo, 'commit', '-qm', 'injected failure')
    result = sandbox_test(repo, _git(repo, 'rev-parse', 'HEAD'),
                          'python3 -m pytest test_runtime_acceptance.py -q --tb=short')
    assert not result['passed'] and result['exit_code'] == 1
    assert 'WT86-injected' in result['output_tail'] and not result.get('setup_failed')
    assert not any(Path(os.environ['WATCHTOWER_SANDBOX_STAGE_ROOT']).iterdir())


def test_failed_runtime_setup_never_launches_gate(tmp_path, monkeypatch):
    roots = {'dir': tmp_path, 'workspace': tmp_path / 'ws', 'store': tmp_path / 'store'}
    roots['store'].mkdir()
    monkeypatch.setattr(ss, 'prepared_source', lambda *args: roots)
    monkeypatch.setattr(runtime, '_asset', lambda name: (_ for _ in ()).throw(ValueError('tampered')))
    monkeypatch.setattr(ss.policy, 'run_sandboxed', lambda *a, **k: pytest.fail('gate launched'))
    monkeypatch.setattr(ss, 'release', lambda path: True)
    result = ss.run_cmd_gate({'repo_path': 'x'}, 'true', 'a' * 40)
    assert result['setup_failed'] and not result['passed']
    assert 'tampered' in result['output_tail']

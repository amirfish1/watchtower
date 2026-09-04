"""Shared sandbox fixtures for the queue-settings and failure-mode suites.

Every fixture here is opt-in by name (``wt_env``, ``run_cli``, ``fake_bin``,
``fake_gh``) so the pre-existing per-module fixtures in this directory keep
working untouched.

The point of ``wt_env`` is that a test can exercise the *real* CLI and the
*real* reconciler without touching the developer's machine: every file
WatchTower writes (queue store, worker registry, queue config, launch-failure
ledger, GitHub connectivity state, activity log) is redirected under
``tmp_path``, the LaunchAgent/daemon side effects of ``wt drain on`` are
neutralised, and engine binaries are resolved from a fake bin directory
instead of PATH.
"""

from __future__ import annotations

import importlib
import io
import os
import stat
import sys
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture(autouse=True)
def hermetic_outbox_and_caller_identity(tmp_path, monkeypatch):
    """Keep the real outbox, the real delegate and the real caller identity
    out of every test.

    Three leaks closed here (OPS-835: live sessions spammed with
    "[watchtower] Q-1 claimed" after any pytest run under an agent harness):

    * ``messages._outbox_file()`` reads ``$WATCHTOWER_OUTBOX_FILE`` fresh per
      call, and module-local fixtures (e.g. test_workers_lifecycle's ``wt``)
      never set it -- so a notification send from any test parked in the
      developer's real ``~/.watchtower/outbox.json`` and the live daemon kept
      delivering it. Point it at tmp_path for every test; tests that set
      their own (test_messages, test_chat_cli, test_chat_policy) simply
      override this again.
    * ``cmd_add`` defaults a ticket's ``submitter`` from the ambient
      ``CLAUDE_CODE_SESSION_ID``/``CODEX_THREAD_ID`` (``_default_report_to``),
      so a claim inside a test notified whatever real session ran pytest.
      Tests that exercise that defaulting set the var themselves.
    * ``messages._delegate_base()`` auto-detects a local CCC from
      ``~/.claude/command-center/port.txt`` whenever
      ``$WATCHTOWER_DELEGATE_URL`` is unset, so the delegate adapter -- the
      last one in ``deliver()``'s chain, reached by every send to a target
      no other adapter can serve -- POSTed the developer's *live* CCC on
      ``/api/inject-input`` with ``origin=wt``. That is how the reconciler's
      release instruction, addressed to a fabricated worker session_id
      (``66666666-...``/``22222222-...`` from test_workers_lifecycle's
      release tests), reached a real machine on every ``pytest`` run and
      then parked in the outbox for the daemon to retry. Disable the
      delegate for every test; the ones that exercise it (test_messages)
      point it at their own stub server.
    """
    monkeypatch.setenv("WATCHTOWER_DELEGATE_URL", "off")
    monkeypatch.setenv("WATCHTOWER_OUTBOX_FILE", str(tmp_path / "outbox.json"))
    # Same class of leak, GraphQL-quota accounting (W4-4): `_log_quota` appends
    # on the tail of every heavy list, so without this every `gh`-faking test
    # in the suite wrote fabricated poll costs into the live fleet's
    # ~/.watchtower/gh-quota.log -- the one file an operator reads to decide
    # whether the real burn is under control.
    monkeypatch.setenv("WATCHTOWER_GH_QUOTA_LOG", str(tmp_path / "gh-quota.log"))
    monkeypatch.delenv("CLAUDE_CODE_SESSION_ID", raising=False)
    monkeypatch.delenv("CODEX_THREAD_ID", raising=False)


@pytest.fixture(autouse=True)
def inert_codex_binary(tmp_path, monkeypatch):
    """Keep a test from resolving the developer's real Codex executable."""
    bin_dir = tmp_path / "watchtower-test-bin"
    bin_dir.mkdir()
    codex = bin_dir / "codex"
    codex.write_text("#!/bin/sh\nexit 0\n")
    codex.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}")


# Every path-ish knob WatchTower reads from the environment. Redirecting all of
# them is what makes a test hermetic; missing one means a test writes into
# ~/.watchtower on a live fleet machine.
_ENV_FILES = {
    "WATCHTOWER_STORE": "queue.json",
    "WATCHTOWER_WORKERS_FILE": "workers.json",
    "WATCHTOWER_CONFIG_FILE": "queue-config.json",
    "WATCHTOWER_ACTIVITY_LOG": "activity.log",
    "WATCHTOWER_LAUNCH_FAILURES_FILE": "launch-failures.json",
    "WATCHTOWER_WORKER_SESSIONS_FILE": "worker-sessions.json",
    "WATCHTOWER_WORKER_IDS_FILE": "worker-ids.json",
    "WATCHTOWER_CODEX_THREAD_REGISTRY": "codex-threads.json",
    "WATCHTOWER_GH_CONNECTIVITY_FILE": "gh-connectivity.json",
    "WATCHTOWER_GH_LIST_CACHE_FILE": "gh-list-cache.json",
    "WATCHTOWER_GH_QUOTA_LOG": "gh-quota.log",
    "WATCHTOWER_CCC_SPAWN_DEFAULTS_FILE": "no-ccc-spawn-defaults.json",
    "WATCHTOWER_DAEMON_PID": "daemon.pid",
    "WATCHTOWER_DASHBOARD_PID": "dashboard.pid",
}
_ENV_DIRS = {
    "WATCHTOWER_STOP_SIGNALS_DIR": "stop-signals",
    "CLAUDE_CONFIG_DIR": "claude-home",
    "WATCHTOWER_GH_CLAIM_LOCKS_DIR": "gh-claim-locks",
    "WATCHTOWER_SNAPSHOTS_DIR": "snapshots",
}

# Leftover state from an enclosing shell (a real fleet machine runs `wt` with
# some of these exported) would otherwise leak into a sandboxed test.
_ENV_CLEAR = (
    "WATCHTOWER_CLAUDE_BIN",
    "WATCHTOWER_CODEX_BIN",
    "WATCHTOWER_KIMI_BIN",
    "WATCHTOWER_ANTIGRAVITY_BIN",
    "WATCHTOWER_SESSION_ID",
    "FAKE_GH_STATE",
)


@pytest.fixture()
def wt_env(tmp_path, monkeypatch):
    """A fully isolated WatchTower install rooted at ``tmp_path``."""
    # These modules cache their paths at import time, so this fixture has to
    # reload them — and then reload them again at teardown against the restored
    # environment. Without that second reload the *next* test file (which may
    # not reload `workers` itself) inherits module globals pointing into a
    # deleted tmp_path, and fails for reasons that have nothing to do with it.
    saved_env = {
        var: os.environ.get(var)
        for var in (*_ENV_FILES, *_ENV_DIRS, *_ENV_CLEAR, "PATH")
    }

    for var, name in _ENV_FILES.items():
        monkeypatch.setenv(var, str(tmp_path / name))
    for var, name in _ENV_DIRS.items():
        monkeypatch.setenv(var, str(tmp_path / name))
    for var in _ENV_CLEAR:
        monkeypatch.delenv(var, raising=False)

    # A fake bin dir that shadows PATH: engine/`gh` lookups resolve here, so a
    # test decides for itself which tools "exist" on this machine.
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ.get('PATH', '')}")

    import watchtower.cli as cli
    import watchtower.codex_registry as codex_registry
    import watchtower.config as config
    import watchtower.github_backend as github_backend
    import watchtower.health as health
    import watchtower.queue as queue
    import watchtower.workers as workers

    # Order matters: config first (everything reads it), then the store, then
    # the modules that read both. reload() re-executes into the same module
    # object, so cli's module-level `from . import queue as q` stays valid.
    for mod in (config, queue, github_backend, health, workers, codex_registry):
        importlib.reload(mod)

    # Legacy-registry migration and the one-time GitHub drain migration are
    # install-time events; neither should fire inside a test.
    monkeypatch.setattr(config, "_REGISTRY_FILE", tmp_path / "no-registry.json")
    config.GH_DRAIN_MIGRATION_MARKER.parent.mkdir(parents=True, exist_ok=True)
    config.GH_DRAIN_MIGRATION_MARKER.write_text("{}\n")

    # `wt drain on` / `wt config --auto-drain on` load a LaunchAgent and
    # background a real `wt start` when the daemon looks dead. Point the plist
    # at nothing and claim the daemon is alive (our own pid) so the settings
    # path is testable without spawning anything.
    monkeypatch.setattr(cli, "_LAUNCHAGENT_PLIST", tmp_path / "no-such.plist")
    monkeypatch.setattr(cli, "DAEMON_PID_FILE", tmp_path / "daemon.pid")
    cli.DAEMON_PID_FILE.write_text(str(os.getpid()))

    yield SimpleNamespace(
        cli=cli,
        config=config,
        queue=queue,
        github_backend=github_backend,
        health=health,
        workers=workers,
        codex_registry=codex_registry,
        tmp=tmp_path,
        bin=bin_dir,
    )

    # monkeypatch's own undo runs after this finalizer, so restore the
    # environment by hand before reloading; otherwise the reload re-reads the
    # sandbox paths we are trying to forget.
    for var, value in saved_env.items():
        if value is None:
            os.environ.pop(var, None)
        else:
            os.environ[var] = value
    for mod in (config, queue, github_backend, health, workers, codex_registry):
        importlib.reload(mod)


class CliResult(SimpleNamespace):
    """Exit code plus captured streams from one ``wt`` invocation."""

    @property
    def output(self) -> str:
        return self.out + self.err


@pytest.fixture()
def run_cli(wt_env):
    """Invoke ``wt`` in-process and capture code/stdout/stderr.

    In-process (rather than a subprocess) so ``monkeypatch`` in the test still
    applies and coverage sees the real code path; ``SystemExit`` from argparse
    is translated into an exit code like a shell would see.
    """

    def _run(*argv: str) -> CliResult:
        out, err = io.StringIO(), io.StringIO()
        code = 0
        with redirect_stdout(out), redirect_stderr(err):
            try:
                code = wt_env.cli.main(list(argv)) or 0
            except SystemExit as exc:  # argparse --help / bad usage
                code = exc.code if isinstance(exc.code, int) else 1
        return CliResult(code=code, out=out.getvalue(), err=err.getvalue())

    return _run


@pytest.fixture()
def fake_bin(wt_env):
    """Install a fake executable at the front of PATH.

    ``fake_bin("codex", "exit 0")`` makes the codex engine look installed;
    omitting a name leaves that engine genuinely unavailable, which is how the
    "engine not installed" failure mode is reproduced without uninstalling
    anything.
    """

    def _install(name: str, body: str = "exit 0", *, shebang: str = "#!/bin/sh") -> Path:
        path = wt_env.bin / name
        path.write_text(f"{shebang}\n{body}\n")
        path.chmod(path.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
        return path

    return _install


# --------------------------------------------------------------------------- gh
# A scriptable fake `gh`. Unlike test_github_backend's stateful fake (which
# simulates a working GitHub), this one simulates GitHub *failing* in the
# specific ways an operator hits: gh missing, gh not logged in, repo not found,
# rate limited, hung, or answering with garbage.
_GH_SCRIPT = r"""#!/bin/sh
mode="${FAKE_GH_MODE:-ok}"
printf '%s\n' "$*" >> "${FAKE_GH_CALLS:-/dev/null}"
case "$mode" in
  unauthenticated)
    echo 'gh: To get started with GitHub CLI, please run:  gh auth login' >&2
    echo 'Alternatively, populate the GH_TOKEN environment variable with a GitHub API authentication token.' >&2
    exit 4 ;;
  no_repo)
    echo 'GraphQL: Could not resolve to a Repository with the name '"'"'ghost/missing'"'"'. (repository)' >&2
    exit 1 ;;
  rate_limited)
    echo 'API rate limit exceeded for user ID 1234. (HTTP 403)' >&2
    exit 1 ;;
  server_error)
    echo 'HTTP 503: Service Unavailable (https://api.github.com/graphql)' >&2
    exit 1 ;;
  bad_json)
    echo 'not json at all'
    exit 0 ;;
  non_list_json)
    echo '{"issues": []}'
    exit 0 ;;
  hang)
    sleep "${FAKE_GH_HANG_S:-120}"
    exit 0 ;;
  *)
    case "$1 $2" in
      "issue list") echo '[]' ;;
      "label create") ;;
      "repo view") echo '{"visibility": "private"}' ;;
      *) echo '[]' ;;
    esac
    exit 0 ;;
esac
"""


@pytest.fixture()
def fake_gh(wt_env, fake_bin):
    """Install the scriptable fake ``gh`` and return a mode setter."""
    calls = wt_env.tmp / "gh-calls.log"
    os.environ["FAKE_GH_CALLS"] = str(calls)
    path = wt_env.bin / "gh"
    path.write_text(_GH_SCRIPT)
    path.chmod(0o755)

    def _mode(mode: str, **env: str) -> None:
        os.environ["FAKE_GH_MODE"] = mode
        for key, value in env.items():
            os.environ[key] = str(value)

    _mode("ok")
    yield SimpleNamespace(
        set=_mode,
        path=path,
        calls=lambda: calls.read_text().splitlines() if calls.exists() else [],
    )
    for key in ("FAKE_GH_MODE", "FAKE_GH_CALLS", "FAKE_GH_HANG_S"):
        os.environ.pop(key, None)


@pytest.fixture()
def uninstall_gh(wt_env, monkeypatch):
    """Make ``gh`` genuinely absent from PATH (only the sandbox bin dir)."""
    monkeypatch.setenv("PATH", str(wt_env.bin))
    assert not (wt_env.bin / "gh").exists()


@pytest.fixture()
def no_engines(wt_env, monkeypatch):
    """Make every agent engine unavailable.

    PATH alone is not enough: ``_resolve_engine_bin`` also probes
    ``~/.local/bin`` and ``~/.kimi-code/bin``, which on a real fleet machine do
    contain these CLIs. The ``WATCHTOWER_*_BIN`` overrides short-circuit that
    search, so pointing them at a path that does not exist is the only way to
    simulate an engine-less host without uninstalling anything.
    """
    monkeypatch.setenv("PATH", str(wt_env.bin))
    missing = str(wt_env.tmp / "no-such-bin")
    for var in ("WATCHTOWER_CLAUDE_BIN", "WATCHTOWER_CODEX_BIN",
                "WATCHTOWER_KIMI_BIN", "WATCHTOWER_ANTIGRAVITY_BIN"):
        monkeypatch.setenv(var, missing)
    for engine in ("claude", "codex", "kimi"):
        assert not wt_env.workers.engine_available(engine)

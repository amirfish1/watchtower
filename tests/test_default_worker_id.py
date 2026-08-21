"""Bare `wt claim` then bare `wt close` must compose (WATCHTOWER-9).

The default worker id used to be `wt-cli-<pid>`. Every CLI invocation is a fresh
pid, so a bare claim and a bare close ran under different ids and the close was
refused as another worker's ticket. The default is now derived from the parent
shell, which is stable across invocations typed in the same terminal.
"""

from __future__ import annotations

import importlib
import os

import pytest


@pytest.fixture()
def cli(tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(tmp_path / "activity.log"))
    for var in ("WT_WORKER", "CODEX_THREAD_ID", "CLAUDE_CODE_SESSION_ID"):
        monkeypatch.delenv(var, raising=False)
    import watchtower.queue as q
    import watchtower.cli as cli_mod

    importlib.reload(q)
    importlib.reload(cli_mod)
    return cli_mod


def test_default_worker_id_ignores_pid_stable_on_shell(cli, monkeypatch):
    monkeypatch.setattr(os, "getppid", lambda: 5000)
    monkeypatch.setattr(os, "getpid", lambda: 111)
    first = cli._default_worker_id()
    monkeypatch.setattr(os, "getpid", lambda: 222)  # next invocation: new pid
    second = cli._default_worker_id()
    assert first == second == "wt-cli-5000"


def test_bare_claim_then_close_compose_across_invocations(cli, monkeypatch):
    import watchtower.queue as q

    q.enqueue(project="DEMO", title="t", note="t", text="")
    # Same shell (getppid constant), but each invocation is a distinct pid.
    monkeypatch.setattr(os, "getppid", lambda: 9000)
    monkeypatch.setattr(os, "getpid", lambda: 40417)
    assert cli.main(["claim", "-q", "DEMO", "--json"]) == 0
    monkeypatch.setattr(os, "getpid", lambda: 41244)  # fresh process for the close
    rc = cli.main(["close", "DEMO-1", "--no-code", "--summary", "done"])
    assert rc == 0
    assert q.get("DEMO-1")["status"] == "closed"


def test_bare_find_marks_own_claim_as_you(cli, capsys, monkeypatch):
    import watchtower.queue as q

    q.enqueue(project="DEMO", title="t", note="t", text="")
    monkeypatch.setattr(os, "getppid", lambda: 7000)
    assert cli.main(["claim", "-q", "DEMO"]) == 0
    capsys.readouterr()
    assert cli.main(["find", "DEMO-1"]) == 0
    out = capsys.readouterr().out
    assert "(you)" in out

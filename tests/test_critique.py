"""`wt critique` + `wt spawn` (WT-100): native ad-hoc agent spawns.

No CCC, no real agent processes: engine selection, prompt composition, and
argv construction are all exercised via --dry-run / spawn_adhoc(dry_run=True).
"""

from __future__ import annotations

import json
import os
import time

from test_messages import wt  # noqa: F401 - sandboxed WT env fixture


def _cli(wt):
    import importlib
    import watchtower.cli as cli
    importlib.reload(cli)
    return cli


def _workers(wt):
    import watchtower.workers as workers
    return workers


# ------------------------------------------------------------------ critique
PARENT_SID = "aaaaaaaa-bbbb-4ccc-8ddd-000000000001"


def test_critique_spawns_the_two_other_families(wt, monkeypatch, capsys):
    cli = _cli(wt)
    workers = _workers(wt)
    monkeypatch.setattr(workers, "engine_available", lambda e: True)
    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", PARENT_SID)

    rc = cli.main(["critique", "does this design hold up?", "--dry-run", "--json"])
    assert rc == 0

    out = json.loads(capsys.readouterr().out)
    # default family is claude -> the other two, in fixed order
    assert [r["engine"] for r in out] == ["codex", "antigravity"]
    assert all(r["ok"] and r["kind"] == "adhoc" for r in out)
    for r in out:
        prompt = r["argv"][-1]
        assert "does this design hold up?" in prompt
        assert "contrarian" in prompt
        # WT-native reply-to footer targets the parent session via wt send
        assert f"wt send {PARENT_SID}" in prompt


def test_critique_family_excludes_itself(wt, monkeypatch, capsys):
    cli = _cli(wt)
    workers = _workers(wt)
    monkeypatch.setattr(workers, "engine_available", lambda e: True)

    rc = cli.main(["critique", "goal", "--family", "codex", "--dry-run", "--json"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert [r["engine"] for r in out] == ["claude", "antigravity"]


def test_critique_model_overrides_win(wt, monkeypatch, capsys):
    cli = _cli(wt)
    workers = _workers(wt)
    monkeypatch.setattr(workers, "engine_available", lambda e: True)

    rc = cli.main([
        "critique", "goal", "--model1", "claude", "--model2", "codex",
        "--dry-run", "--json",
    ])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert [r["engine"] for r in out] == ["claude", "codex"]


def test_critique_unsupported_override_errors(wt, monkeypatch, capsys):
    cli = _cli(wt)
    workers = _workers(wt)
    monkeypatch.setattr(workers, "engine_available", lambda e: True)

    rc = cli.main([
        "critique", "goal", "--model1", "gemini", "--model2", "codex",
        "--dry-run",
    ])
    assert rc == 1
    err = capsys.readouterr().err
    assert "error spawning gemini" in err
    assert "unsupported engine" in err


def test_critique_unavailable_default_falls_back_to_own_family(wt, monkeypatch, capsys):
    """A missing default engine (e.g. agy not installed) is substituted with
    the spawner's own family -- a same-family critic beats one fewer critic.
    Explicit overrides never fall back."""
    cli = _cli(wt)
    workers = _workers(wt)
    monkeypatch.setattr(
        workers, "engine_available", lambda e: e != "antigravity"
    )

    rc = cli.main(["critique", "goal", "--dry-run", "--json"])
    assert rc == 0
    captured = capsys.readouterr()
    out = json.loads(captured.out)
    assert [r["engine"] for r in out] == ["codex", "claude"]
    assert "falling back to claude" in captured.err


def test_critique_without_report_to_has_no_send_footer(wt, monkeypatch, capsys):
    cli = _cli(wt)
    workers = _workers(wt)
    monkeypatch.setattr(workers, "engine_available", lambda e: True)
    monkeypatch.delenv("CLAUDE_CODE_SESSION_ID", raising=False)

    rc = cli.main(["critique", "goal", "--dry-run", "--json"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    for r in out:
        assert "wt send" not in r["argv"][-1]


# -------------------------------------------------------------------- spawn
def test_spawn_dry_run_claude_argv(wt, capsys):
    cli = _cli(wt)
    rc = cli.main(["spawn", "do the thing", "--model", "claude-sonnet-5",
                   "--dry-run", "--json"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    argv = out["argv"]
    assert argv[0] == "claude" and "-p" in argv
    assert ["--permission-mode", "bypassPermissions"] == argv[2:4]
    assert ["--model", "claude-sonnet-5"] == argv[4:6]
    assert argv[-1] == "do the thing"
    assert out["kind"] == "adhoc"


def test_spawn_dry_run_codex_argv(wt, capsys):
    cli = _cli(wt)
    rc = cli.main(["spawn", "goal", "--engine", "codex", "--dry-run", "--json"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["argv"][:2] == ["codex", "exec"]
    assert out["argv"][-1] == "goal"


def test_spawn_report_to_appends_wt_send_footer(wt, capsys):
    cli = _cli(wt)
    wt.messages.register_agent("amir", PARENT_SID)
    rc = cli.main(["spawn", "goal", "--report-to", "@amir",
                   "--dry-run", "--json"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert "wt send @amir" in out["argv"][-1]


def test_spawn_report_to_unresolvable_errors_before_spawn(wt, capsys):
    """An unresolvable --report-to (typo, unregistered name) must fail fast
    at spawn time -- if it only failed inside the spawned agent's own `wt
    send`, the report would be silently lost (unresolvable targets don't
    park in the outbox) (OPS-89)."""
    cli = _cli(wt)
    rc = cli.main(["spawn", "goal", "--report-to", "@nobody",
                   "--dry-run", "--json"])
    assert rc == 1
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] is False
    assert "unresolvable" in out["error"]


def test_spawn_unsupported_engine_fails(wt, capsys):
    cli = _cli(wt)
    rc = cli.main(["spawn", "goal", "--engine", "hermes", "--dry-run"])
    assert rc == 1
    assert "unsupported engine" in capsys.readouterr().err


def test_spawn_antigravity_requires_binary(wt, monkeypatch, capsys):
    cli = _cli(wt)
    monkeypatch.setenv("WATCHTOWER_ANTIGRAVITY_BIN", "/nonexistent/agy")
    rc = cli.main(["spawn", "goal", "--engine", "antigravity", "--dry-run"])
    assert rc == 1
    assert "antigravity CLI not found" in capsys.readouterr().err


def test_spawn_antigravity_argv_shape(wt, monkeypatch, tmp_path, capsys):
    cli = _cli(wt)
    fake = tmp_path / "agy"
    fake.write_text("#!/bin/sh\n")
    fake.chmod(0o755)
    monkeypatch.setenv("WATCHTOWER_ANTIGRAVITY_BIN", str(fake))
    rc = cli.main(["spawn", "goal", "--engine", "antigravity",
                   "--repo", str(tmp_path), "--dry-run", "--json"])
    assert rc == 0
    argv = json.loads(capsys.readouterr().out)["argv"]
    assert argv[0] == str(fake)
    assert "--dangerously-skip-permissions" in argv
    assert "--add-dir" in argv and str(tmp_path) in argv
    assert argv[-2:] == ["-p", "goal"]


def test_adhoc_workers_are_exempt_from_reap(wt):
    """A one-shot ad-hoc agent buffers its output, so its log-mtime idle clock
    lies; reap must never SIGTERM it (it exits on its own)."""
    workers = _workers(wt)
    log = wt.tmp / "adhoc.log"
    log.write_text("")
    ts = time.time() - 10_000  # far past WARM_TTL_S
    os.utime(log, (ts, ts))
    workers.record_worker(
        os.getpid(), "ADHOC", "claude", "adhoc-test-1", str(wt.tmp),
        str(log), kind="adhoc",
    )
    reaped = workers.reap_stale_workers()
    assert reaped == []

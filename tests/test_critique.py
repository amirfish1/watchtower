"""`wt critique` + `wt spawn` (WT-100): native ad-hoc agent spawns.

No CCC, no real agent processes: engine selection, prompt composition, and
argv construction are all exercised via --dry-run / spawn_adhoc(dry_run=True).
Engine binaries are stubbed (`_stub_bins`) so results don't depend on which
CLIs happen to be installed on the host.
"""

from __future__ import annotations

import io
import json
import os
import sys
import time

import pytest

from test_messages import wt  # noqa: F401 - sandboxed WT env fixture


def _cli(wt):
    import importlib
    import watchtower.cli as cli
    importlib.reload(cli)
    return cli


def _workers(wt):
    import watchtower.workers as workers
    return workers


def _stub_bins(monkeypatch, workers, missing=()):
    """Make every engine CLI resolvable (minus ``missing``) regardless of
    what's installed on the host -- build_adhoc_command hard-checks
    shutil.which so dry-run argv construction must not depend on the host."""
    def fake_which(cmd, *a, **kw):
        return None if cmd in missing else f"/stub/{cmd}"
    monkeypatch.setattr(workers.shutil, "which", fake_which)
    monkeypatch.delenv("WATCHTOWER_ANTIGRAVITY_BIN", raising=False)


# ------------------------------------------------------------------ critique
PARENT_SID = "aaaaaaaa-bbbb-4ccc-8ddd-000000000001"
CODEX_TID = "019f3abb-2026-7fe0-b437-ed9c858534b5"


def test_critique_spawns_the_two_other_families(wt, monkeypatch, capsys):
    cli = _cli(wt)
    workers = _workers(wt)
    _stub_bins(monkeypatch, workers)
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
        # WT-native reply-to footer targets the parent session via wt send,
        # quote-safe over stdin (heredoc), not as a shell argument.
        assert f"wt send {PARENT_SID} - <<'WT_REPORT'" in prompt


def test_critique_family_excludes_itself(wt, monkeypatch, capsys):
    cli = _cli(wt)
    workers = _workers(wt)
    _stub_bins(monkeypatch, workers)
    monkeypatch.setattr(workers, "engine_available", lambda e: True)

    rc = cli.main(["critique", "goal", "--family", "codex", "--dry-run", "--json"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert [r["engine"] for r in out] == ["claude", "antigravity"]


def test_critique_invalid_family_is_rejected_by_argparse(wt, monkeypatch, capsys):
    """--family gemini must be a hard parse error, not 3 spawned critics."""
    cli = _cli(wt)
    with pytest.raises(SystemExit):
        cli.main(["critique", "goal", "--family", "gemini", "--dry-run"])
    assert "invalid choice" in capsys.readouterr().err


def test_critique_family_autodetected_from_codex_env(wt, monkeypatch, capsys):
    """A Codex caller (CODEX_THREAD_ID set, no --family) gets claude +
    antigravity critics, and its thread id is auto-registered so replies
    route via the codex transport."""
    cli = _cli(wt)
    workers = _workers(wt)
    _stub_bins(monkeypatch, workers)
    monkeypatch.setattr(workers, "engine_available", lambda e: True)
    monkeypatch.delenv("CLAUDE_CODE_SESSION_ID", raising=False)
    monkeypatch.setenv("CODEX_THREAD_ID", CODEX_TID)

    rc = cli.main(["critique", "goal", "--dry-run", "--json"])
    assert rc == 0
    captured = capsys.readouterr()
    out = json.loads(captured.out)
    assert [r["engine"] for r in out] == ["claude", "antigravity"]
    for r in out:
        assert "wt send @codex-thread-" in r["argv"][-1]
    reg = wt.messages.resolve_target(
        f"@codex-thread-{CODEX_TID.replace('-', '')[:12]}"
    )
    assert reg["engine"] == "codex"
    assert reg["session_id"] == CODEX_TID


def test_critique_engine_overrides_win(wt, monkeypatch, capsys):
    cli = _cli(wt)
    workers = _workers(wt)
    _stub_bins(monkeypatch, workers)
    monkeypatch.setattr(workers, "engine_available", lambda e: True)

    rc = cli.main([
        "critique", "goal", "--engine1", "claude", "--engine2", "codex",
        "--dry-run", "--json",
    ])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert [r["engine"] for r in out] == ["claude", "codex"]


def test_critique_model_flags_still_work_as_aliases(wt, monkeypatch, capsys):
    cli = _cli(wt)
    workers = _workers(wt)
    _stub_bins(monkeypatch, workers)
    monkeypatch.setattr(workers, "engine_available", lambda e: True)

    rc = cli.main([
        "critique", "goal", "--model1", "claude", "--model2", "codex",
        "--dry-run", "--json",
    ])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert [r["engine"] for r in out] == ["claude", "codex"]


def test_critique_unsupported_override_errors_before_any_spawn(wt, monkeypatch, capsys):
    cli = _cli(wt)
    workers = _workers(wt)
    _stub_bins(monkeypatch, workers)
    monkeypatch.setattr(workers, "engine_available", lambda e: True)

    rc = cli.main([
        "critique", "goal", "--engine1", "gemini", "--engine2", "codex",
        "--dry-run",
    ])
    assert rc == 1
    err = capsys.readouterr().err
    assert "unsupported engine 'gemini'" in err


def test_critique_unavailable_explicit_override_errors(wt, monkeypatch, capsys):
    """An explicitly requested engine whose CLI is missing errors up front
    (never falls back, never half-spawns)."""
    cli = _cli(wt)
    workers = _workers(wt)
    _stub_bins(monkeypatch, workers)
    monkeypatch.setattr(workers, "engine_available", lambda e: e != "antigravity")
    spawned = []
    monkeypatch.setattr(workers, "spawn_adhoc",
                        lambda *a, **kw: spawned.append(a) or {})

    rc = cli.main([
        "critique", "goal", "--engine1", "codex", "--engine2", "antigravity",
        "--dry-run",
    ])
    assert rc == 1
    assert "never falls back" in capsys.readouterr().err
    assert spawned == []  # preflight failed -> nothing spawned, not even one


def test_critique_duplicate_explicit_engines_rejected(wt, monkeypatch, capsys):
    cli = _cli(wt)
    workers = _workers(wt)
    _stub_bins(monkeypatch, workers)
    monkeypatch.setattr(workers, "engine_available", lambda e: True)

    rc = cli.main([
        "critique", "goal", "--engine1", "codex", "--engine2", "codex",
        "--dry-run",
    ])
    assert rc == 1
    assert "identical" in capsys.readouterr().err


def test_critique_unavailable_default_falls_back_to_own_family(wt, monkeypatch, capsys):
    """A missing default engine (e.g. agy not installed) is substituted with
    the spawner's own family -- a same-family critic beats one fewer critic.
    Explicit overrides never fall back."""
    cli = _cli(wt)
    workers = _workers(wt)
    _stub_bins(monkeypatch, workers, missing=("agy", "antigravity"))
    monkeypatch.setattr(
        workers, "engine_available", lambda e: e != "antigravity"
    )
    monkeypatch.delenv("CODEX_THREAD_ID", raising=False)
    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", PARENT_SID)

    rc = cli.main(["critique", "goal", "--dry-run", "--json"])
    assert rc == 0
    captured = capsys.readouterr()
    out = json.loads(captured.out)
    assert [r["engine"] for r in out] == ["codex", "claude"]
    assert "falling back to claude" in captured.err


def test_critique_partial_override_still_checks_the_default_slot(wt, monkeypatch, capsys):
    """--engine1 alone must not disable availability checking for the
    defaulted second slot: with agy missing, slot 2 falls back."""
    cli = _cli(wt)
    workers = _workers(wt)
    _stub_bins(monkeypatch, workers, missing=("agy", "antigravity"))
    monkeypatch.setattr(workers, "engine_available", lambda e: e != "antigravity")

    rc = cli.main(["critique", "goal", "--engine1", "codex", "--dry-run", "--json"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert [r["engine"] for r in out] == ["codex", "claude"]


def test_critique_default_slot_never_duplicates_an_explicit_pick(wt, monkeypatch, capsys):
    """--engine1 antigravity (family claude): the defaulted slot 2 would
    also be antigravity -- it must pick something else, not duplicate."""
    cli = _cli(wt)
    workers = _workers(wt)
    _stub_bins(monkeypatch, workers)
    monkeypatch.setattr(workers, "engine_available", lambda e: True)

    rc = cli.main(["critique", "goal", "--engine1", "antigravity",
                   "--dry-run", "--json"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    engines = [r["engine"] for r in out]
    assert engines[0] == "antigravity"
    assert len(engines) == len(set(engines)) == 2


def test_critique_single_family_installed_spawns_one_critic_not_twins(wt, monkeypatch, capsys):
    """Claude-only machine: never ["claude", "claude"] -- one critic and a
    printed note beats two identical spawns burning tokens."""
    cli = _cli(wt)
    workers = _workers(wt)
    _stub_bins(monkeypatch, workers, missing=("codex", "agy", "antigravity"))
    monkeypatch.setattr(workers, "engine_available", lambda e: e == "claude")

    rc = cli.main(["critique", "goal", "--dry-run", "--json"])
    assert rc == 0
    captured = capsys.readouterr()
    out = json.loads(captured.out)
    assert [r["engine"] for r in out] == ["claude"]
    assert "fewer critics" in captured.err


def test_critique_no_engines_installed_errors(wt, monkeypatch, capsys):
    cli = _cli(wt)
    workers = _workers(wt)
    monkeypatch.setattr(workers, "engine_available", lambda e: False)

    rc = cli.main(["critique", "goal", "--dry-run", "--json"])
    assert rc == 1
    out = json.loads(capsys.readouterr().out)
    assert out[0]["ok"] is False
    assert "no critique engine" in out[0]["error"]


def test_critique_without_report_to_has_no_send_footer(wt, monkeypatch, capsys):
    cli = _cli(wt)
    workers = _workers(wt)
    _stub_bins(monkeypatch, workers)
    monkeypatch.setattr(workers, "engine_available", lambda e: True)
    monkeypatch.delenv("CLAUDE_CODE_SESSION_ID", raising=False)
    monkeypatch.delenv("CODEX_THREAD_ID", raising=False)

    rc = cli.main(["critique", "goal", "--dry-run", "--json"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    for r in out:
        assert "wt send" not in r["argv"][-1]


def test_critique_without_report_to_warns_loudly_on_real_spawn(wt, monkeypatch, capsys):
    """Silently dropping the reports is the failure mode; a real (non
    dry-run) spawn with no reachable report target must say so on stderr."""
    cli = _cli(wt)
    workers = _workers(wt)
    monkeypatch.setattr(workers, "engine_available", lambda e: True)
    monkeypatch.delenv("CLAUDE_CODE_SESSION_ID", raising=False)
    monkeypatch.delenv("CODEX_THREAD_ID", raising=False)
    monkeypatch.setattr(
        workers, "spawn_adhoc",
        lambda prompt, engine, **kw: {
            "worker_id": f"critique-{engine}-00000000", "engine": engine,
            "kind": "adhoc", "log": "-",
        },
    )

    rc = cli.main(["critique", "goal"])
    assert rc == 0
    err = capsys.readouterr().err
    assert "warning:" in err and "--report-to" in err


# -------------------------------------------------------------------- spawn
def test_spawn_dry_run_claude_argv(wt, monkeypatch, capsys):
    cli = _cli(wt)
    _stub_bins(monkeypatch, _workers(wt))
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


def test_spawn_dry_run_codex_argv(wt, monkeypatch, capsys):
    cli = _cli(wt)
    _stub_bins(monkeypatch, _workers(wt))
    rc = cli.main(["spawn", "goal", "--engine", "codex", "--dry-run", "--json"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["argv"][:2] == ["codex", "exec"]
    assert out["argv"][-1] == "goal"


def test_spawn_missing_engine_binary_is_a_clean_error(wt, monkeypatch, capsys):
    """A missing claude/codex binary must be a clean preflight error, not a
    FileNotFoundError out of Popen."""
    cli = _cli(wt)
    _stub_bins(monkeypatch, _workers(wt), missing=("codex",))
    rc = cli.main(["spawn", "goal", "--engine", "codex", "--dry-run"])
    assert rc == 1
    assert "codex CLI not found" in capsys.readouterr().err


def test_spawn_report_to_appends_wt_send_footer(wt, monkeypatch, capsys):
    cli = _cli(wt)
    _stub_bins(monkeypatch, _workers(wt))
    wt.messages.register_agent("amir", PARENT_SID)
    rc = cli.main(["spawn", "goal", "--report-to", "@amir",
                   "--dry-run", "--json"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    prompt = out["argv"][-1]
    assert "wt send @amir - <<'WT_REPORT'" in prompt


def test_spawn_report_to_unresolvable_errors_before_spawn(wt, monkeypatch, capsys):
    """An unresolvable --report-to (typo, unregistered name) must fail fast
    at spawn time -- if it only failed inside the spawned agent's own `wt
    send`, the report would be silently lost (unresolvable targets don't
    park in the outbox) (OPS-89)."""
    cli = _cli(wt)
    _stub_bins(monkeypatch, _workers(wt))
    rc = cli.main(["spawn", "goal", "--report-to", "@nobody",
                   "--dry-run", "--json"])
    assert rc == 1
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] is False
    assert "unresolvable" in out["error"]


def test_spawn_unsupported_engine_fails(wt, monkeypatch, capsys):
    cli = _cli(wt)
    _stub_bins(monkeypatch, _workers(wt))
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
    # AGY's internal log is a *sibling* .agy.log, not worker.log.agy.log
    agy_log = argv[argv.index("--log-file") + 1]
    assert agy_log.endswith(".agy.log") and not agy_log.endswith(".log.agy.log")


def test_spawn_antigravity_passes_model_through(wt, monkeypatch, tmp_path, capsys):
    """agy supports --model natively; an explicit override must reach the
    argv, not be silently dropped."""
    cli = _cli(wt)
    fake = tmp_path / "agy"
    fake.write_text("#!/bin/sh\n")
    fake.chmod(0o755)
    monkeypatch.setenv("WATCHTOWER_ANTIGRAVITY_BIN", str(fake))
    rc = cli.main(["spawn", "goal", "--engine", "antigravity",
                   "--model", "gemini-3-pro", "--dry-run", "--json"])
    assert rc == 0
    argv = json.loads(capsys.readouterr().out)["argv"]
    assert "--model" in argv and "gemini-3-pro" in argv


def test_spawn_footer_shell_quotes_hostile_report_target(wt, monkeypatch, capsys):
    """The footer interpolates report_to into a shell command the critic
    runs verbatim; a resolvable-but-hostile target (worker id with shell
    metacharacters) must arrive as one inert quoted token."""
    cli = _cli(wt)
    workers = _workers(wt)
    _stub_bins(monkeypatch, workers)
    hostile = "bad; touch pwned"
    workers.record_worker(
        os.getpid(), "Q", "claude", hostile, str(wt.tmp), str(wt.tmp / "w.log"),
    )
    rc = cli.main(["spawn", "goal", "--report-to", hostile,
                   "--dry-run", "--json"])
    assert rc == 0
    prompt = json.loads(capsys.readouterr().out)["argv"][-1]
    assert "wt send 'bad; touch pwned' - <<'WT_REPORT'" in prompt
    assert "send bad;" not in prompt  # never the raw, splittable form


def test_register_agent_rejects_shell_unsafe_names(wt):
    """Registered names end up inside shell commands (report footer); names
    outside [A-Za-z0-9._-] are an injection foothold and must be refused."""
    for bad in ("x; touch /tmp/nope", "a b", "who`ami`", "$(x)", "it's"):
        with pytest.raises(ValueError):
            wt.messages.register_agent(bad, PARENT_SID)


def test_critique_warns_when_nonclaude_family_reports_to_unknown_uuid(wt, monkeypatch, capsys):
    """An unknown bare UUID resolves as engine=claude by assumption; a
    non-claude caller pointing replies at their own session UUID would
    never receive them -- must warn with the register recipe."""
    cli = _cli(wt)
    workers = _workers(wt)
    _stub_bins(monkeypatch, workers)
    monkeypatch.setattr(workers, "engine_available", lambda e: True)

    rc = cli.main(["critique", "goal", "--family", "antigravity",
                   "--report-to", PARENT_SID, "--dry-run", "--json"])
    assert rc == 0
    err = capsys.readouterr().err
    assert "claude" in err and "wt agents register" in err


# ------------------------------------------------------------------ wt send -
def test_send_preserves_body_verbatim_except_one_trailing_newline(wt, monkeypatch, capsys):
    """The footer promises delivery of "the complete report text": leading
    blank lines and indentation must survive; only the single newline a
    heredoc appends is dropped."""
    cli = _cli(wt)
    raw = "\n    indented code\n\ntail line\n\n"
    sent = {}
    monkeypatch.setattr(
        wt.messages, "send",
        lambda target, text, **kw: sent.update(text=text) or {"ok": True},
    )
    monkeypatch.setattr(sys, "stdin", io.StringIO(raw))
    rc = cli.main(["send", "@amir", "-"])
    assert rc == 0
    assert sent["text"] == "\n    indented code\n\ntail line\n"


def test_send_reads_body_from_stdin_with_dash(wt, monkeypatch, capsys):
    """`wt send <t> -` is the quote-safe delivery path the report footer
    uses: bodies full of "quotes", $vars and `backticks` arrive verbatim."""
    cli = _cli(wt)
    body = 'line one "quoted"\n$HOME `whoami`\nline three'
    sent = {}
    monkeypatch.setattr(
        wt.messages, "send",
        lambda target, text, **kw: sent.update(target=target, text=text)
        or {"ok": True},
    )
    monkeypatch.setattr(sys, "stdin", io.StringIO(body))
    rc = cli.main(["send", "@amir", "-"])
    assert rc == 0
    assert sent["text"] == body


def test_send_empty_stdin_errors(wt, monkeypatch, capsys):
    cli = _cli(wt)
    monkeypatch.setattr(sys, "stdin", io.StringIO("   \n"))
    rc = cli.main(["send", "@amir", "-"])
    assert rc == 1
    assert "empty message" in capsys.readouterr().err


# --------------------------------------------------------------------- reap
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

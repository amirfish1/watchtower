"""Native macOS tty keystroke adapter (WT-55).

Covers ``watchtower.tty`` in isolation (ps-output parsing, AppleScript
generation/escaping, delivery outcomes) and its position in the
``messages`` adapter chain (after fifo, before resume, claude-only).

Never invokes real ``osascript`` or real ``ps`` — every subprocess boundary
(``tty._run_ps`` / ``tty._run_osascript``) is monkeypatched.

Reuses the isolated ``wt`` sandbox fixture and helpers from test_messages.
"""

from __future__ import annotations

import watchtower.tty as tty

from test_messages import (  # noqa: F401  (wt is a fixture, needed in scope)
    SID_B,
    _fake_popen,
    wt,
)


# --------------------------------------------------------------- ps parsing
_PS_SAMPLE = """\
  PID TTY           COMMAND
    1 ??            /sbin/launchd
 2201 ttys001       /Applications/iTerm.app/Contents/MacOS/iTerm2
 2345 ttys001       /Users/amir/.local/bin/claude --resume 7f72634b-b0bd-4c78-b931-3d877ed84187 --verbose
 2400 ttys002       /usr/local/bin/claude --print --output-format text
 2500 ??            /usr/local/bin/claude -p --resume aaaaaaaa-1111-4111-8111-111111111111
 2600 ttys003       -bash
"""


def test_parse_ps_maps_sid_to_tty_from_resume_argv():
    disc = tty.parse_ps(_PS_SAMPLE)
    assert disc["sid_to_tty"] == {
        "7f72634b-b0bd-4c78-b931-3d877ed84187": "ttys001",
    }
    assert disc["terminal_app"] == "iTerm2"


def test_parse_ps_does_not_map_claude_tui_without_resume_flag():
    # pid 2400 is a claude process on a real tty but carries no --resume: it
    # must NOT be guessed at (e.g. by cwd) — unmapped is an adapter miss.
    disc = tty.parse_ps(_PS_SAMPLE)
    assert "ttys002" not in disc["sid_to_tty"].values()


def test_parse_ps_ignores_headless_resume_on_tty_questionmark():
    # pid 2500 has --resume but tty "??" (headless -p run) — not a TUI target.
    disc = tty.parse_ps(_PS_SAMPLE)
    assert "aaaaaaaa-1111-4111-8111-111111111111" not in disc["sid_to_tty"]


def test_parse_ps_prefers_iterm_when_both_terminals_running():
    sample = _PS_SAMPLE + " 2700 ttys004       /System/Applications/Utilities/Terminal.app/Contents/MacOS/Terminal\n"
    disc = tty.parse_ps(sample)
    assert disc["terminal_app"] == "iTerm2"


def test_parse_ps_detects_terminal_app_alone():
    sample = " 2700 ttys004       /System/Applications/Utilities/Terminal.app/Contents/MacOS/Terminal\n"
    disc = tty.parse_ps(sample)
    assert disc["terminal_app"] == "Terminal"


def test_discover_uses_monkeypatched_ps_runner(monkeypatch):
    monkeypatch.setattr(tty, "_run_ps", lambda: _PS_SAMPLE)
    disc = tty.discover()
    assert disc["sid_to_tty"]["7f72634b-b0bd-4c78-b931-3d877ed84187"] == "ttys001"
    assert disc["terminal_app"] == "iTerm2"


# --------------------------------------------------------------- escaping
def test_applescript_literal_escapes_backslash_before_quote():
    raw = 'She said "go" \\ now'
    lit = tty.applescript_literal(raw)
    assert lit == 'She said \\"go\\" \\\\ now'


def test_applescript_literal_passes_newlines_through_unescaped():
    raw = "line one\nline two"
    lit = tty.applescript_literal(raw)
    # Newlines are legal inside an AppleScript string literal and must stay
    # as real newline characters, not be turned into a literal "\n" escape.
    assert "\n" in lit
    assert "\\n" not in lit


def test_build_script_embeds_escaped_text_and_tty():
    script = tty.build_script("ttys007", "iTerm2", 'say "hi" \\ ok')
    assert "/dev/ttys007" in script
    assert 'say \\"hi\\" \\\\ ok' in script
    assert 'tell application "iTerm2"' in script


def test_build_script_terminal_app_uses_do_script():
    script = tty.build_script("ttys007", "Terminal", "hello")
    assert 'tell application "Terminal"' in script
    assert "do script" in script
    assert "/dev/ttys007" in script


# ------------------------------------------------------------- deliver_tty
def _resolved(sid=SID_B, engine="claude"):
    return {"session_id": sid, "engine": engine, "kind": "session"}


def test_deliver_tty_miss_when_no_live_tty(monkeypatch):
    monkeypatch.setattr(
        tty, "discover", lambda: {"sid_to_tty": {}, "terminal_app": "iTerm2"}
    )
    res = tty.deliver_tty(_resolved(), "hi")
    assert res["ok"] is False
    assert "no live tty" in res["error"]


def test_deliver_tty_miss_when_no_terminal_app(monkeypatch):
    monkeypatch.setattr(
        tty, "discover",
        lambda: {"sid_to_tty": {SID_B: "ttys003"}, "terminal_app": None},
    )
    res = tty.deliver_tty(_resolved(), "hi")
    assert res["ok"] is False
    assert "no supported terminal app" in res["error"]


def test_deliver_tty_miss_when_engine_not_claude(monkeypatch):
    called = []
    monkeypatch.setattr(tty, "discover", lambda: called.append(1) or {})
    res = tty.deliver_tty(_resolved(engine="codex"), "hi")
    assert res["ok"] is False
    assert not called  # must not even run discovery for a non-claude target


def test_deliver_tty_notfound_is_a_miss_after_one_retry(monkeypatch):
    monkeypatch.setattr(
        tty, "discover",
        lambda: {"sid_to_tty": {SID_B: "ttys003"}, "terminal_app": "iTerm2"},
    )
    monkeypatch.setattr(tty.time, "sleep", lambda s: None)
    calls = []

    def fake_osa(script):
        calls.append(script)
        return {"ok": True, "returncode": 0, "stdout": "notfound", "stderr": ""}

    monkeypatch.setattr(tty, "_run_osascript", fake_osa)
    res = tty.deliver_tty(_resolved(), "hi")
    assert res["ok"] is False
    assert "no iTerm2 tab found" in res["error"]
    assert len(calls) == 2  # one retry, per module docstring


def test_deliver_tty_miss_on_nonzero_returncode(monkeypatch):
    monkeypatch.setattr(
        tty, "discover",
        lambda: {"sid_to_tty": {SID_B: "ttys003"}, "terminal_app": "iTerm2"},
    )
    monkeypatch.setattr(
        tty, "_run_osascript",
        lambda script: {"ok": True, "returncode": 1, "stdout": "", "stderr": "boom"},
    )
    res = tty.deliver_tty(_resolved(), "hi")
    assert res["ok"] is False
    assert "boom" in res["error"]


def test_deliver_tty_miss_on_osascript_transport_failure(monkeypatch):
    monkeypatch.setattr(
        tty, "discover",
        lambda: {"sid_to_tty": {SID_B: "ttys003"}, "terminal_app": "iTerm2"},
    )
    monkeypatch.setattr(
        tty, "_run_osascript",
        lambda script: {"ok": False, "error": "osascript unavailable"},
    )
    res = tty.deliver_tty(_resolved(), "hi")
    assert res["ok"] is False
    assert "osascript unavailable" in res["error"]


def test_deliver_tty_success_generates_script_with_tty_and_escaped_text(monkeypatch):
    monkeypatch.setattr(
        tty, "discover",
        lambda: {"sid_to_tty": {SID_B: "ttys009"}, "terminal_app": "iTerm2"},
    )
    calls = []

    def fake_osa(script):
        calls.append(script)
        return {"ok": True, "returncode": 0, "stdout": "ok", "stderr": ""}

    monkeypatch.setattr(tty, "_run_osascript", fake_osa)
    res = tty.deliver_tty(_resolved(), 'hello "world" \\ end')
    assert res == {
        "ok": True,
        "transport": "tty",
        "tty": "ttys009",
        "terminal_app": "iTerm2",
        "submitted": True,
    }
    assert len(calls) == 1
    assert "/dev/ttys009" in calls[0]
    assert 'hello \\"world\\" \\\\ end' in calls[0]


def test_deliver_tty_ok_no_submit_still_reports_success(monkeypatch):
    monkeypatch.setattr(
        tty, "discover",
        lambda: {"sid_to_tty": {SID_B: "ttys009"}, "terminal_app": "iTerm2"},
    )
    monkeypatch.setattr(
        tty, "_run_osascript",
        lambda script: {
            "ok": True, "returncode": 0,
            "stdout": "ok-no-submit:some AppleScript error", "stderr": "",
        },
    )
    res = tty.deliver_tty(_resolved(), "hi")
    assert res["ok"] is True
    assert res["submitted"] is False
    assert "warning" in res


def test_deliver_tty_disabled_via_env(monkeypatch):
    monkeypatch.setenv("WATCHTOWER_TTY_ADAPTER", "off")
    called = []
    monkeypatch.setattr(tty, "discover", lambda: called.append(1) or {})
    res = tty.deliver_tty(_resolved(), "hi")
    assert res["ok"] is False
    assert not called


# --------------------------------------------------------- adapter chain
def test_chain_prefers_tty_over_resume_for_live_claude_session(wt, monkeypatch):
    """A claude session with a live tty mapping must be delivered via the
    tty adapter, never forking a parallel resume Popen (that would fork the
    conversation — exactly what this adapter exists to prevent)."""
    monkeypatch.setattr(
        wt.messages.tty_mod, "deliver_tty",
        lambda resolved, text: {
            "ok": True, "transport": "tty",
            "tty": "ttys011", "terminal_app": "iTerm2", "submitted": True,
        },
    )
    popen_calls = []
    monkeypatch.setattr(wt.messages.subprocess, "Popen", _fake_popen(popen_calls))
    res = wt.messages.send(SID_B, "hello there")
    assert res["ok"] is True
    assert res["transport"] == "tty"
    assert popen_calls == []


def test_chain_falls_through_to_resume_on_tty_miss(wt, monkeypatch):
    monkeypatch.setattr(
        wt.messages.tty_mod, "deliver_tty",
        lambda resolved, text: {"ok": False, "error": "no live tty found"},
    )
    from test_messages import _write_transcript
    _write_transcript(wt, SID_B, age_s=600)  # quiet: resume isn't busy
    popen_calls = []
    monkeypatch.setattr(wt.messages.subprocess, "Popen", _fake_popen(popen_calls))
    res = wt.messages.send(SID_B, "hello there")
    assert res["ok"] is True
    assert res["transport"] == "resume"
    assert len(popen_calls) == 1


def test_chain_skips_tty_for_codex_targets(wt, monkeypatch):
    called = []
    monkeypatch.setattr(
        wt.messages.tty_mod, "deliver_tty",
        lambda resolved, text: called.append(1) or {"ok": False, "error": "n/a"},
    )
    resolved = {"kind": "session", "session_id": "thread-1", "worker": None,
                "engine": "codex"}
    wt.messages._deliver_unreceipted(resolved, "hi")
    assert not called

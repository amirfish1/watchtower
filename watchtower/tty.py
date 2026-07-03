#!/usr/bin/env python3
"""Native macOS tty keystroke delivery (WT-55): type a message into a LIVE
terminal claude TUI (Terminal.app / iTerm2) via AppleScript — no CCC needed.

Chain position (see ``messages._deliver_unreceipted``): after the fifo
adapter (wt's own workers stay the cheapest path) and BEFORE the resume
adapter. Rationale: spawning a parallel ``claude -p --resume <sid>`` against
a session whose TUI is live in a terminal would fork the conversation — and
the resume adapter refuses busy sessions anyway (a live TUI keeps its
transcript mtime hot, so those messages used to park in the outbox; this
adapter is exactly what should catch them first).

Injection mechanism, ported faithfully from CCC's
``inject_input_via_keystroke`` (claude-command-center ``server.py``),
native-submit (Return) paths only:

  * iTerm2: find the session by tty via iTerm2's native API, ``write text``
    the body, then after ``delay 0.3`` write an empty string — the TUI reads
    a CR glued to the body as a literal newline inside a paste burst, but a
    lone CR in its own input burst as a real Enter keypress. No focus steal,
    no System Events / Accessibility grant, and it cannot land in the wrong
    window.
  * Terminal.app: the same two-burst pattern via ``do script ... in tab``
    (tab found by tty).
  * The script returns the string ``notfound`` when no tab/session matches
    the tty; that is an adapter MISS (fall through to the next adapter), not
    an error surfaced to the user.

Session→tty discovery is ONE ``ps -Ao pid,tty,command`` pass — never a
subprocess per row:

  * A live claude process whose argv carries ``--resume <sid>`` maps that
    sid to its controlling tty directly.
  * A claude TUI on a real tty WITHOUT ``--resume`` in its argv cannot be
    mapped safely. We deliberately do NOT guess by cwd: two sessions can
    share a cwd, and typing into the wrong person's live terminal is
    strictly worse than falling through to the next adapter. Unmapped means
    adapter miss.
  * Codex TUI processes rarely carry the thread id in argv at all, so codex
    is out of scope for v1 — this adapter is claude-only.
  * Headless resumes (``claude -p --resume``) sit on tty ``??`` and are
    filtered out by the real-tty check, so only genuine TUIs are targets.
  * The same ps pass detects which terminal app is running ("iTerm2" /
    "Terminal.app" substrings in process paths); iTerm2 is preferred when
    both are up (its native tty lookup is the more precise of the two).

Busy guard: NONE, deliberately. A live TUI accepts typed input mid-turn (it
queues the text internally and picks it up at the next prompt), so unlike
the resume adapter this one must not block on transcript-mtime busyness.

``WATCHTOWER_TTY_ADAPTER=off`` disables the adapter entirely (it then
reports a plain miss and the chain continues). Stdlib only.
"""

from __future__ import annotations

import os
import re
import subprocess
import time
from typing import Any, Dict, Optional

# Bind the real Popen at import time. Tests fake the resume adapter by
# monkeypatching ``subprocess.Popen`` module-wide; ``subprocess.run`` calls
# Popen internally, so going through the module attribute would (a) hand this
# discovery pass a fake that can't run ``ps`` and (b) pollute the fake's
# recorded calls with a ps row. The bound reference is immune to both.
_REAL_POPEN = subprocess.Popen

# ``--resume <sid>`` (or ``--resume=<sid>``) in a claude process's argv.
_RESUME_RE = re.compile(r"--resume[=\s]+(\S+)")


def _enabled() -> bool:
    return os.environ.get("WATCHTOWER_TTY_ADAPTER", "").strip().lower() != "off"


def _normalized_tty(tty: Any) -> Optional[str]:
    value = str(tty or "").strip()
    if not value or value in ("??", "?", "-"):
        return None
    return value


def _is_real_tty(tty: Any) -> bool:
    return _normalized_tty(tty) is not None


def _run_ps() -> str:
    """One ``ps -Ao pid,tty,command`` pass; empty string on any failure."""
    proc = None
    try:
        proc = _REAL_POPEN(
            ["ps", "-Ao", "pid,tty,command"],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
        )
        out, _ = proc.communicate(timeout=2)
        return out or ""
    except subprocess.TimeoutExpired:
        try:
            proc.kill()
            proc.communicate(timeout=1)
        except Exception:  # noqa: BLE001 - best-effort reap
            pass
        return ""
    except (FileNotFoundError, OSError):
        return ""


def parse_ps(text: str) -> Dict[str, Any]:
    """Parse ``ps -Ao pid,tty,command`` output into
    ``{"sid_to_tty": {sid: tty}, "terminal_app": "iTerm2"|"Terminal"|None}``.

    Only claude processes on a real tty whose argv carries ``--resume <sid>``
    are mapped (see module docstring for why TUIs without ``--resume`` stay
    unmapped). Terminal-app presence is detected from process paths in the
    same pass; iTerm2 wins when both apps are running.
    """
    sid_to_tty: Dict[str, str] = {}
    seen_iterm = False
    seen_terminal = False
    for line in text.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) != 3:
            continue
        pid, tty, command = parts
        if not pid.isdigit():  # header row ("PID TTY COMMAND")
            continue
        if "iTerm2" in command:
            seen_iterm = True
        if "Terminal.app" in command:
            seen_terminal = True
        exe = command.split(None, 1)[0].rsplit("/", 1)[-1]
        if exe != "claude":
            continue
        norm = _normalized_tty(tty)
        if norm is None:
            continue  # headless (tty "??") — not a TUI target
        m = _RESUME_RE.search(command)
        if not m:
            continue  # live TUI but unmappable — do NOT guess by cwd
        sid_to_tty[m.group(1)] = norm
    terminal_app = "iTerm2" if seen_iterm else ("Terminal" if seen_terminal else None)
    return {"sid_to_tty": sid_to_tty, "terminal_app": terminal_app}


def discover() -> Dict[str, Any]:
    """sid→tty map + running terminal app, from one ps pass."""
    return parse_ps(_run_ps())


def applescript_literal(s: str) -> str:
    """Escape text for an AppleScript double-quoted string literal:
    backslashes first, then quotes. Raw newlines are legal inside AppleScript
    string literals and pass through unescaped — inside the paste burst the
    TUI reads them as literal newlines, not as Enter (the submit is the
    separate empty write; see module docstring)."""
    return str(s).replace("\\", "\\\\").replace('"', '\\"')


def build_script(tty: str, terminal_app: str, text: str) -> str:
    """The AppleScript that finds the tab/session owning ``tty`` and performs
    the two-burst native write (body, ``delay 0.3``, lone empty write =
    Enter). Returns ``notfound`` when nothing owns the tty. Ported from CCC's
    ``inject_input_via_keystroke`` native-submit branches."""
    tty_short = str(tty).replace("/dev/", "")
    tty_full = "/dev/" + tty_short
    text_lit = applescript_literal(text)

    if terminal_app == "iTerm2":
        # iTerm2: find the session by tty, write the body via the native
        # session API, then after a beat write a lone empty line — the TUI
        # reads that isolated CR as an Enter keypress. No focus, no System
        # Events.
        return f'''
        tell application "iTerm2"
          set foundSession to missing value
          set winCount to count of windows
          repeat with i from 1 to winCount
            try
              set w to window i
              repeat with j from 1 to (count of tabs of w)
                try
                  set t to tab j of w
                  repeat with s in sessions of t
                    try
                      if tty of s is "{tty_full}" then
                        set foundSession to s
                        exit repeat
                      end if
                    end try
                  end repeat
                  if foundSession is not missing value then exit repeat
                end try
              end repeat
              if foundSession is not missing value then exit repeat
            end try
          end repeat
          if foundSession is missing value then return "notfound"
          tell foundSession to write text "{text_lit}"
          delay 0.3
          set submitErr to ""
          try
            tell foundSession to write text ""
          on error errMsg
            set submitErr to errMsg
          end try
        end tell
        if submitErr is not "" then return "ok-no-submit:" & submitErr
        return "ok"
        '''
    # Terminal.app: find the tab by tty, send text through Terminal's native
    # `do script ... in tab` API, then after a beat send a lone empty
    # `do script` — its bare CR arrives in its own input burst, which the TUI
    # reads as an Enter keypress. No focus, no System Events.
    return f'''
        tell application "Terminal"
          set foundTab to missing value
          set winCount to count of windows
          repeat with i from 1 to winCount
            try
              set w to window i
              repeat with j from 1 to (count of tabs of w)
                try
                  set t to tab j of w
                  if tty of t is "{tty_full}" then
                    set foundTab to t
                    exit repeat
                  end if
                end try
              end repeat
              if foundTab is not missing value then exit repeat
            end try
          end repeat
          if foundTab is missing value then return "notfound"
          do script "{text_lit}" in foundTab
          delay 0.3
          set submitErr to ""
          try
            do script "" in foundTab
          on error errMsg
            set submitErr to errMsg
          end try
        end tell
        if submitErr is not "" then return "ok-no-submit:" & submitErr
        return "ok"
        '''


def _run_osascript(script: str) -> Dict[str, Any]:
    """Run one osascript, normalized to
    ``{"ok": True, "returncode", "stdout", "stderr"}`` or
    ``{"ok": False, "error"}`` for transport-level failures.
    ALWAYS monkeypatched in tests — tests must never execute real osascript.
    """
    timeout_s = 5
    proc = None
    try:
        proc = _REAL_POPEN(
            ["osascript", "-e", script],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        out, err = proc.communicate(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        try:
            proc.kill()
            proc.communicate(timeout=1)
        except Exception:  # noqa: BLE001 - best-effort reap
            pass
        return {
            "ok": False,
            "error": f"osascript timed out after {timeout_s}s "
                     f"controlling the terminal",
        }
    except (FileNotFoundError, OSError) as e:
        return {"ok": False, "error": f"osascript unavailable: {e}"}
    return {
        "ok": True,
        "returncode": proc.returncode,
        "stdout": (out or "").strip(),
        "stderr": (err or "").strip(),
    }


def deliver_tty(resolved: Dict[str, Any], text: str) -> Dict[str, Any]:
    """Deliver ``text`` into the live terminal TUI attached to the resolved
    claude session, by typing into its tty.

    Returns ``{"ok": True, "transport": "tty", ...}`` on success, or
    ``{"ok": False, "error": ...}`` for every kind of miss (no tty mapping,
    no terminal app, tab not found, osascript failure) — a miss means the
    caller's adapter chain just continues; it is never a user-facing error
    by itself."""
    if not _enabled():
        return {"ok": False, "error": "tty adapter disabled (WATCHTOWER_TTY_ADAPTER=off)"}
    sid = str(resolved.get("session_id") or "")
    if resolved.get("engine") != "claude" or not sid:
        return {"ok": False, "error": "tty adapter needs a claude session_id"}
    disc = discover()
    tty = (disc.get("sid_to_tty") or {}).get(sid)
    if not tty:
        return {"ok": False,
                "error": f"no live tty found for session {sid[:8]}"}
    terminal_app = disc.get("terminal_app")
    if not terminal_app:
        return {"ok": False,
                "error": "no supported terminal app running (iTerm2/Terminal)"}
    script = build_script(tty, terminal_app, text)
    res = _run_osascript(script)
    if not res.get("ok"):
        return {"ok": False, "error": str(res.get("error") or "osascript failed")}
    # Auto-retry once on notfound — the tab often becomes findable ~200ms
    # later after a focus/Spaces transition settles (ported from CCC).
    if res.get("stdout") == "notfound":
        time.sleep(0.2)
        res = _run_osascript(script)
        if not res.get("ok"):
            return {"ok": False, "error": str(res.get("error") or "osascript failed")}
    if res.get("returncode") != 0:
        return {"ok": False,
                "error": str(res.get("stderr") or "AppleScript failed")}
    out = str(res.get("stdout") or "")
    if out == "notfound":
        # Adapter MISS, not a user error: the tab may be hidden, on another
        # Space, or behind a fullscreen app. Fall through.
        return {"ok": False,
                "error": f"no {terminal_app} tab found for {tty}"}
    if out.startswith("ok-no-submit:"):
        # Body was typed but the follow-up empty write (the Enter) failed:
        # the text sits in the TUI input buffer. The message DID reach the
        # right session, so this is a delivery success — falling through to
        # resume here would fork the conversation, the exact thing this
        # adapter exists to prevent.
        return {
            "ok": True,
            "transport": "tty",
            "tty": tty,
            "terminal_app": terminal_app,
            "submitted": False,
            "detail": out.split(":", 1)[1].strip(),
            "warning": "text typed but the follow-up Enter write failed; "
                       "press Enter in the session to send it",
        }
    return {
        "ok": True,
        "transport": "tty",
        "tty": tty,
        "terminal_app": terminal_app,
        "submitted": True,
    }

#!/usr/bin/env python3
"""WT-owned Codex app-server JSON-RPC client (WT-54).

Codex's app-server is a persistent subprocess speaking JSON-RPC 2.0 over its
own stdin/stdout: one ``{"jsonrpc":"2.0","id":N,"method":...,"params":...}\\n``
line in, matching responses read back by id on a background reader thread.
Unlike the one-shot ``codex exec``, the app-server can steer an already
running turn (``turn/steer``) — the Codex analog of a FIFO write into a live
Claude worker. WT uses this only as the standalone fallback; when CCC's
delegate/broker is configured, ``messages.deliver`` routes Codex sends there
first so one local owner drives user-visible Codex threads.

This module ports the pattern proven in Claude Command Center's
``server.py`` (``_ensure_codex_app_server`` / ``_codex_app_server_request_to_proc``
/ ``_codex_resume_or_steer_via_app_server``), not the code verbatim: WT has no
HTTP endpoint layer here, just a small lazy client with three public verbs:

  * ``is_available()``: is a codex binary reachable (env override or PATH)?
  * ``ensure_server()``: lazily start + initialize the subprocess; idempotent.
  * ``deliver(thread_id, text)``: ``thread/resume`` the thread, then
    ``turn/steer`` into an active turn if one is running, else ``turn/start``
    a fresh one.
  * ``shutdown()``: terminate the subprocess; also runs at interpreter exit.

Every RPC has a timeout (default ``DEFAULT_TIMEOUT_S``) so a stuck app-server
can never hang the caller — a broken/absent process degrades to
``{"ok": False, "error": ...}``, letting ``messages.deliver`` fall through to
the next adapter.

``$WATCHTOWER_CODEX_BIN`` overrides which binary is spawned (tests point it at
a fake stdio JSON-RPC script; never a real ``codex`` binary in tests).
"""

from __future__ import annotations

import atexit
import json
import os
import shutil
import subprocess
import threading
import time
from typing import Any, Dict, List, Optional

DEFAULT_TIMEOUT_S = 15.0
_INIT_TIMEOUT_S = 10.0

_LOCK = threading.Condition()
_PROC: Optional[subprocess.Popen] = None
_READER_THREAD: Optional[threading.Thread] = None
_INITIALIZED = False
_INITIALIZING = False
_NEXT_ID = 1
_RESPONSES: Dict[int, Dict[str, Any]] = {}


def _elapsed_ms(start: float) -> float:
    return round((time.monotonic() - start) * 1000.0, 1)


# --------------------------------------------------------------------- binary
def _codex_bin() -> Optional[str]:
    """Resolve the codex binary: $WATCHTOWER_CODEX_BIN overrides, else PATH."""
    env_bin = os.environ.get("WATCHTOWER_CODEX_BIN")
    if env_bin:
        return env_bin
    return shutil.which("codex")


def is_available() -> bool:
    """True iff a codex (or codex-shaped) binary can be spawned right now.

    Does not start the app-server; a cheap existence/executability check so
    callers (``messages.deliver``) can skip straight to the next adapter
    without paying subprocess-start latency."""
    b = _codex_bin()
    if not b:
        return False
    if os.path.isfile(b):
        return os.access(b, os.X_OK)
    return shutil.which(b) is not None


# ---------------------------------------------------------------------- wire
def _reader(proc: subprocess.Popen) -> None:
    """Background thread: read newline-delimited JSON-RPC responses from the
    app-server's stdout and file them by id for ``_request_to_proc`` to pop.
    Exits (and clears the shared proc state) when the pipe closes, which is
    how a crashed/quit app-server is detected without polling."""
    global _PROC, _INITIALIZED, _INITIALIZING
    try:
        for line in proc.stdout:  # type: ignore[union-attr]
            line = (line or "").strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict) or "id" not in payload:
                continue
            with _LOCK:
                _RESPONSES[payload.get("id")] = payload
                _LOCK.notify_all()
    finally:
        with _LOCK:
            if _PROC is proc:
                _PROC = None
                _INITIALIZED = False
                _INITIALIZING = False
            _LOCK.notify_all()


def _request_to_proc(
    proc: subprocess.Popen,
    method: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: float = DEFAULT_TIMEOUT_S,
) -> Dict[str, Any]:
    """Send one JSON-RPC request to an already-started app-server and block
    (bounded by ``timeout``) for its matching response."""
    with _LOCK:
        global _NEXT_ID
        req_id = _NEXT_ID
        _NEXT_ID += 1
        try:
            proc.stdin.write(  # type: ignore[union-attr]
                json.dumps({
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "method": method,
                    "params": params or {},
                }) + "\n"
            )
            proc.stdin.flush()  # type: ignore[union-attr]
        except (BrokenPipeError, OSError) as e:
            return {"ok": False, "error": str(e)}

        deadline = time.time() + timeout
        while time.time() < deadline:
            response = _RESPONSES.pop(req_id, None)
            if response is not None:
                return response
            remaining = max(0.05, deadline - time.time())
            _LOCK.wait(min(0.5, remaining))
        _RESPONSES.pop(req_id, None)
        return {
            "ok": False,
            "error": f"codex app-server request timed out after {timeout}s: {method}",
        }


def ensure_server() -> Optional[subprocess.Popen]:
    """Start and initialize a persistent codex app-server if one isn't
    already up. Returns the live process, or None if unavailable/failed.

    Idempotent and safe to call from multiple threads: a caller that arrives
    while another thread is mid-handshake waits on the same result instead of
    racing a second Popen."""
    global _PROC, _READER_THREAD, _INITIALIZED, _INITIALIZING
    with _LOCK:
        while _INITIALIZING:
            _LOCK.wait(0.5)
            proc = _PROC
            if proc is not None and proc.poll() is None and _INITIALIZED:
                return proc
            if not _INITIALIZING:
                break
        proc = _PROC
        if proc is not None and proc.poll() is None and _INITIALIZED:
            return proc
        if proc is not None and proc.poll() is None:
            try:
                proc.terminate()
            except OSError:
                pass
        _PROC = None
        _INITIALIZED = False
        _INITIALIZING = False

        bin_path = _codex_bin()
        if not bin_path:
            return None
        try:
            proc = subprocess.Popen(
                [bin_path, "app-server", "--listen", "stdio://"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                bufsize=1,
                start_new_session=True,
            )
        except (FileNotFoundError, OSError):
            return None
        _PROC = proc
        _INITIALIZING = True
        _READER_THREAD = threading.Thread(
            target=_reader,
            args=(proc,),
            daemon=True,
            name="wt-codex-app-server-reader",
        )
        _READER_THREAD.start()

    # experimentalApi negotiates turn/steer + thread/inject at connection
    # init (docs/engine-capability-matrix.md section 4); without it the
    # app-server may reject the steer/inject calls WT needs.
    init = _request_to_proc(
        proc,
        "initialize",
        {
            "clientInfo": {
                "name": "watchtower",
                "title": "WatchTower",
                "version": "1",
            },
            "capabilities": {"experimentalApi": True},
        },
        timeout=_INIT_TIMEOUT_S,
    )
    if init.get("result") is not None:
        with _LOCK:
            if _PROC is proc and proc.poll() is None:
                _INITIALIZED = True
                _INITIALIZING = False
                _LOCK.notify_all()
                return proc
    try:
        proc.terminate()
    except OSError:
        pass
    with _LOCK:
        if _PROC is proc:
            _PROC = None
            _INITIALIZED = False
            _INITIALIZING = False
        _LOCK.notify_all()
    return None


def is_running() -> bool:
    """Read-only: is WT's own codex app-server up and past the handshake?
    Never starts the process, so it's cheap enough to poll."""
    return bool(_PROC is not None and _PROC.poll() is None and _INITIALIZED)


def request(
    method: str, params: Optional[Dict[str, Any]] = None, timeout: float = DEFAULT_TIMEOUT_S
) -> Dict[str, Any]:
    """Ensure the app-server is up, then send one JSON-RPC request."""
    proc = ensure_server()
    if proc is None:
        return {"ok": False, "error": "codex app-server is unavailable"}
    return _request_to_proc(proc, method, params=params, timeout=timeout)


def shutdown() -> None:
    """Terminate WT's codex app-server subprocess, waiting briefly for it to
    exit cleanly before escalating to kill. Safe to call repeatedly and from
    ``atexit`` (registered at import time)."""
    global _PROC, _INITIALIZED, _INITIALIZING
    with _LOCK:
        proc = _PROC
        _PROC = None
        _INITIALIZED = False
        _INITIALIZING = False
        _LOCK.notify_all()
    if proc is None:
        return
    if proc.poll() is None:
        try:
            proc.terminate()
        except OSError:
            pass
        try:
            proc.wait(timeout=2)
        except subprocess.TimeoutExpired:
            try:
                proc.kill()
            except OSError:
                pass
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                pass
        except Exception:
            pass
    else:
        try:
            proc.wait(timeout=0.1)
        except Exception:
            pass


atexit.register(shutdown)


# ------------------------------------------------------------------- deliver
def _user_input(text: str) -> List[Dict[str, str]]:
    return [{"type": "text", "text": str(text or "")}]


def _latest_active_turn(thread: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    for turn in reversed((thread or {}).get("turns") or []):
        if isinstance(turn, dict) and turn.get("status") == "inProgress" and turn.get("id"):
            return turn
    return None


def _response_succeeded(response: Dict[str, Any]) -> bool:
    return isinstance(response, dict) and "result" in response and not response.get("error")


def _error_text(response: Dict[str, Any]) -> str:
    if not isinstance(response, dict):
        return "codex app-server returned no response"
    err = response.get("error")
    if not isinstance(err, dict):
        return str(response.get("error") or "")
    message = str(err.get("message") or "codex app-server request failed")
    data = err.get("data")
    if data is not None:
        return f"{message}: {data}"
    return message


def deliver(
    thread_id: str,
    text: str,
    cwd: Optional[str] = None,
    timeout: float = DEFAULT_TIMEOUT_S,
) -> Dict[str, Any]:
    """Deliver ``text`` into a codex thread: resume it, then steer an active
    turn if one is running, else start a fresh one.

    Mirrors CCC's ``_codex_resume_or_steer_via_app_server`` /
    ``_codex_steer_via_app_server`` combined into one call, since WT's
    ``messages.deliver`` only needs "get this text into the thread", not the
    separate resume-vs-steer decision CCC's UI exposes.

    Returns ``{"ok": True, "via": "steer"|"start", "turn_id": ...}`` plus
    warm/cold and latency fields, or ``{"ok": False, "error": ...}``. Never
    raises: a dead/unavailable app-server, a resume failure, or a steer/start
    failure all come back as a plain failed result so ``messages.deliver`` can
    fall through to the next adapter (delegate or outbox)."""
    total_start = time.monotonic()
    app_server_warm = is_running()
    if not is_available():
        return {
            "ok": False,
            "error": "codex binary not found",
            "stage": "resolve",
            "app_server_warm": app_server_warm,
            "latency_ms": _elapsed_ms(total_start),
        }
    resume_params: Dict[str, Any] = {"threadId": thread_id, "excludeTurns": False}
    if cwd:
        resume_params["cwd"] = cwd
    resume_start = time.monotonic()
    resumed = request("thread/resume", resume_params, timeout=timeout)
    resume_ms = _elapsed_ms(resume_start)
    if not _response_succeeded(resumed):
        return {
            "ok": False,
            "error": _error_text(resumed) or "thread/resume failed",
            "stage": "thread/resume",
            "app_server_warm": app_server_warm,
            "resume_ms": resume_ms,
            "latency_ms": _elapsed_ms(total_start),
        }
    thread = (resumed.get("result") or {}).get("thread") or {}
    active_turn = _latest_active_turn(thread)
    status = (thread.get("status") or {}).get("type")

    if status == "active" and active_turn:
        turn_start = time.monotonic()
        steered = request(
            "turn/steer",
            {
                "threadId": thread_id,
                "expectedTurnId": active_turn["id"],
                "input": _user_input(text),
            },
            timeout=timeout,
        )
        turn_ms = _elapsed_ms(turn_start)
        if _response_succeeded(steered):
            return {
                "ok": True,
                "via": "steer",
                "turn_id": (steered.get("result") or {}).get("turnId") or active_turn["id"],
                "app_server_warm": app_server_warm,
                "resume_ms": resume_ms,
                "turn_ms": turn_ms,
                "latency_ms": _elapsed_ms(total_start),
            }
        return {
            "ok": False,
            "error": _error_text(steered) or "turn/steer failed",
            "stage": "turn/steer",
            "app_server_warm": app_server_warm,
            "resume_ms": resume_ms,
            "turn_ms": turn_ms,
            "latency_ms": _elapsed_ms(total_start),
        }

    start_params: Dict[str, Any] = {"threadId": thread_id, "input": _user_input(text)}
    if cwd:
        start_params["cwd"] = cwd
    turn_start = time.monotonic()
    started = request("turn/start", start_params, timeout=timeout)
    turn_ms = _elapsed_ms(turn_start)
    if _response_succeeded(started):
        turn = (started.get("result") or {}).get("turn") or {}
        return {
            "ok": True,
            "via": "start",
            "turn_id": turn.get("id"),
            "app_server_warm": app_server_warm,
            "resume_ms": resume_ms,
            "turn_ms": turn_ms,
            "latency_ms": _elapsed_ms(total_start),
        }
    return {
        "ok": False,
        "error": _error_text(started) or "turn/start failed",
        "stage": "turn/start",
        "app_server_warm": app_server_warm,
        "resume_ms": resume_ms,
        "turn_ms": turn_ms,
        "latency_ms": _elapsed_ms(total_start),
    }

#!/usr/bin/env python3
# Copyright (c) 2026 Amir Fish. All rights reserved.
# SPDX-License-Identifier: LicenseRef-WatchTower-Software-License

"""Steerable Codex queue worker: one app-server per worker (CODEX-CCC-WT-10).

``codex exec`` cannot be steered: nothing can reach its thread while it runs,
and a second process that resumes it fails with JSON-RPC -32600 "thread already
has an active writer" (OPS-155 T2c). Here the worker process instead owns a
private ``codex app-server --listen unix://SOCK``, starts the drain goal as one
thread + turn on it, and exits when that turn completes. Codex's unix listener
speaks WebSocket and accepts several clients on the SAME server, so any other
process (``wt``'s codex delivery, CCC ``send --steer``/report-to) connects as a
second *client* and calls ``turn/steer``. There is still one writer (OPS-155
T8), the server process.

The process keeps ``codex exec``'s worker contract: its pid is the worker pid,
stdout is the worker log (the first line carries ``session_id`` for
``resolve_session_id_from_log``), the rollout under ``$CODEX_HOME/sessions``
is written by the app-server as usual (activity clock + token telemetry), and
the exit code reports the turn outcome.

Opt-in per queue (``wt config -q Q --codex-transport app-server``); the default
stays ``codex exec``. A live worker advertises itself at
``~/.watchtower/codex-app/<thread_id>.json`` so steerers can find its socket.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

CLIENT_NAME = "watchtower_app_worker"
_SOCKET_WAIT_S = 60.0
_RPC_TIMEOUT_S = 60.0
STEER_TIMEOUT_S = 15.0
# Streaming deltas would multiply the log size for no liveness value.
_QUIET_SUFFIXES = ("/delta", "Delta")


def registry_dir() -> Path:
    from .workers import WORKERS_FILE
    return WORKERS_FILE.parent / "codex-app"


def _record_path(thread_id: str) -> Path:
    return registry_dir() / f"{thread_id}.json"


def _pid_alive(pid: Any) -> bool:
    try:
        os.kill(int(pid), 0)
    except (OSError, TypeError, ValueError):
        return False
    return True


def _is_our_server(pid: Any, sock: str) -> bool:
    """True when ``pid`` is the app-server listening on ``sock``: the FULL
    ``--listen unix://<sock>`` argument must match (sock paths are unique per
    worker), so a recycled pid is never signalled."""
    try:
        argv = Path(f"/proc/{int(pid)}/cmdline").read_bytes().split(b"\0")
    except (OSError, TypeError, ValueError):
        return False
    want = f"unix://{sock}".encode()
    return b"app-server" in argv and any(
        arg == want and i > 0 and argv[i - 1] == b"--listen" for i, arg in enumerate(argv)
    )


def _retire(path: Path, rec: Dict[str, Any]) -> None:
    """Drop a dead worker's record and stop its orphaned app-server.

    Codex does not honour PR_SET_PDEATHSIG while a turn runs: a host killed
    with SIGKILL mid-turn left its server working the ticket as a hidden
    writer (OPS-155 pilot 3). WT's own kill path signals the whole process
    group, which covers the server; this catches everything else."""
    sock = str(rec.get("sock") or "")
    server_pid = rec.get("server_pid")
    if sock and _is_our_server(server_pid, sock):
        try:
            os.kill(int(server_pid), signal.SIGTERM)
        except OSError:
            pass
    for stale in (path, Path(sock) if sock else None):
        if stale is None:
            continue
        try:
            stale.unlink()
        except OSError:
            pass


def find_worker(thread_id: str) -> Optional[Dict[str, Any]]:
    """The live app-server worker hosting ``thread_id``, or None (a dead
    worker's record is retired on the way)."""
    thread_id = str(thread_id or "").strip()
    if not thread_id or "/" in thread_id:
        return None
    path = _record_path(thread_id)
    try:
        rec = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(rec, dict):
        rec = {}
    if not _pid_alive(rec.get("pid")) or not os.path.exists(str(rec.get("sock") or "")):
        _retire(path, rec)
        return None
    return rec


def sweep_orphans() -> int:
    """Retire every record whose worker process is gone; returns the count.
    Runs each reconcile tick so an orphaned server stops within one tick even
    when nobody tries to steer its thread."""
    retired = 0
    try:
        paths = list(registry_dir().glob("*.json"))
    except OSError:
        return 0
    for path in paths:
        try:
            rec = json.loads(path.read_text())
        except (OSError, ValueError):
            continue
        if isinstance(rec, dict) and not _pid_alive(rec.get("pid")):
            _retire(path, rec)
            retired += 1
    return retired


class UnixWebSocket:
    """Minimal RFC 6455 client over a unix socket (WatchTower is stdlib-only)."""

    def __init__(self, path: str, timeout: Optional[float] = None):
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.settimeout(timeout)
        self._sock.connect(path)
        self._buf = b""
        self._send_lock = threading.Lock()
        key = base64.b64encode(os.urandom(16)).decode()
        self._sock.sendall((
            "GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\n"
            f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n\r\n"
        ).encode())
        while b"\r\n\r\n" not in self._buf:
            self._fill()
        head, self._buf = self._buf.split(b"\r\n\r\n", 1)
        status = head.split(b"\r\n", 1)[0]
        if b" 101 " not in status:
            raise ConnectionError(f"websocket upgrade refused: {status.decode(errors='replace')}")

    def settimeout(self, timeout: Optional[float]) -> None:
        self._sock.settimeout(timeout)

    def _fill(self) -> None:
        chunk = self._sock.recv(65536)
        if not chunk:
            raise ConnectionError("app-server closed the connection")
        self._buf += chunk

    def _take(self, n: int) -> bytes:
        while len(self._buf) < n:
            self._fill()
        out, self._buf = self._buf[:n], self._buf[n:]
        return out

    @staticmethod
    def _frame(opcode: int, payload: bytes) -> bytes:
        n = len(payload)
        if n < 126:
            head = struct.pack("!BB", 0x80 | opcode, 0x80 | n)
        elif n < 65536:
            head = struct.pack("!BBH", 0x80 | opcode, 0x80 | 126, n)
        else:
            head = struct.pack("!BBQ", 0x80 | opcode, 0x80 | 127, n)
        mask = os.urandom(4)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        return head + mask + masked

    def send(self, message: Dict[str, Any]) -> None:
        frame = self._frame(0x1, json.dumps(message).encode())
        with self._send_lock:
            self._sock.sendall(frame)

    def recv(self) -> Optional[Dict[str, Any]]:
        """Next JSON message; None when the server closes the stream."""
        parts: List[bytes] = []
        while True:
            b0, b1 = self._take(2)
            n = b1 & 0x7F
            if n == 126:
                n = struct.unpack("!H", self._take(2))[0]
            elif n == 127:
                n = struct.unpack("!Q", self._take(8))[0]
            mask = self._take(4) if b1 & 0x80 else b""
            data = self._take(n)
            if mask:
                data = bytes(b ^ mask[i % 4] for i, b in enumerate(data))
            opcode = b0 & 0x0F
            if opcode == 0x8:
                return None
            if opcode == 0x9:
                with self._send_lock:
                    self._sock.sendall(self._frame(0xA, data))
                continue
            if opcode == 0xA:
                continue
            parts.append(data)
            if b0 & 0x80:
                return json.loads(b"".join(parts))

    def close(self) -> None:
        try:
            with self._send_lock:
                self._sock.sendall(self._frame(0x8, b""))
        except OSError:
            pass
        self._sock.close()


class _Client:
    """JSON-RPC over one websocket; notifications go to ``on_note``."""

    def __init__(self, sock_path: str, on_note=None, timeout: Optional[float] = None):
        self._ws = UnixWebSocket(sock_path, timeout=timeout)
        self._next_id = 0
        self._responses: Dict[int, Dict[str, Any]] = {}
        self._cond = threading.Condition()
        self._on_note = on_note
        self.closed = threading.Event()
        threading.Thread(target=self._reader, daemon=True).start()
        init = self.call("initialize", {"clientInfo": {"name": CLIENT_NAME, "version": "1"}})
        if "result" not in init:
            raise ConnectionError(f"initialize failed: {init.get('error')}")
        self._ws.send({"jsonrpc": "2.0", "method": "initialized"})

    def _reader(self) -> None:
        try:
            self._ws.settimeout(None)
            while True:
                msg = self._ws.recv()
                if msg is None:
                    break
                if "id" in msg and ("result" in msg or "error" in msg):
                    with self._cond:
                        self._responses[msg["id"]] = msg
                        self._cond.notify_all()
                elif "method" in msg and self._on_note is not None:
                    self._on_note(msg)
        except (OSError, ConnectionError, ValueError):
            pass
        finally:
            self.closed.set()
            with self._cond:
                self._cond.notify_all()

    def call(self, method: str, params: Dict[str, Any], timeout: float = _RPC_TIMEOUT_S) -> Dict[str, Any]:
        with self._cond:
            self._next_id += 1
            rid = self._next_id
        self._ws.send({"jsonrpc": "2.0", "id": rid, "method": method, "params": params})
        deadline = time.monotonic() + timeout
        with self._cond:
            while rid not in self._responses:
                left = deadline - time.monotonic()
                if left <= 0 or self.closed.is_set():
                    return {"error": {"message": f"{method}: no response ({'closed' if self.closed.is_set() else 'timeout'})"}}
                self._cond.wait(left)
            return self._responses.pop(rid)

    def close(self) -> None:
        self._ws.close()


def _error_text(resp: Dict[str, Any]) -> str:
    err = resp.get("error")
    if isinstance(err, dict):
        return str(err.get("message") or err)
    return str(err or "")


def steer(thread_id: str, text: str, timeout: float = STEER_TIMEOUT_S) -> Dict[str, Any]:
    """Steer ``text`` into the live turn of a WT app-server worker.

    ``code`` is ``no_app_worker`` when no live worker hosts the thread (the
    caller should fall through to its normal transport) and
    ``app_worker_steer_failed`` when the worker exists but rejected the steer
    (typically: its turn just completed and it is exiting)."""
    rec = find_worker(thread_id)
    if rec is None:
        return {"ok": False, "code": "no_app_worker", "error": f"no live app-server worker hosts {thread_id}"}
    try:
        client = _Client(str(rec["sock"]), timeout=timeout)
    except (OSError, ConnectionError, ValueError) as exc:
        return {"ok": False, "code": "app_worker_steer_failed", "error": f"connect: {exc}", "pid": rec.get("pid")}
    try:
        resp = client.call("turn/steer", {
            "threadId": thread_id,
            "expectedTurnId": rec.get("turn_id"),
            "input": [{"type": "text", "text": str(text or "")}],
        }, timeout=timeout)
    finally:
        client.close()
    if "result" in resp:
        return {"ok": True, "via": "wt-app-worker-steer", "turn_id": (resp["result"] or {}).get("turnId") or rec.get("turn_id"),
                "pid": rec.get("pid"), "worker_id": rec.get("worker_id")}
    return {"ok": False, "code": "app_worker_steer_failed", "error": _error_text(resp) or "turn/steer failed",
            "pid": rec.get("pid")}


def _pdeathsig() -> None:
    """Linux: ask for the app-server to die with the worker. Codex only honours
    it while idle (see ``_retire`` for the mid-turn case)."""
    if not sys.platform.startswith("linux"):
        return
    try:
        import ctypes
        ctypes.CDLL("libc.so.6", use_errno=True).prctl(1, signal.SIGTERM)  # PR_SET_PDEATHSIG
    except (OSError, AttributeError):
        pass


def _emit(obj: Dict[str, Any]) -> None:
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def run(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(prog="python -m watchtower.codex_app_worker run")
    p.add_argument("--codex-bin", default=os.environ.get("WATCHTOWER_CODEX_BIN") or "codex")
    p.add_argument("--cwd", default=os.getcwd())
    p.add_argument("--worker-id", default="")
    p.add_argument("--model", default="")
    p.add_argument("-c", "--config", action="append", default=[],
                   help="codex config override passed to the app-server (repeatable)")
    p.add_argument("goal")
    args = p.parse_args(argv)

    reg = registry_dir()
    (reg / "sock").mkdir(parents=True, exist_ok=True)
    sock = str(reg / "sock" / f"{args.worker_id or os.getpid()}.sock")
    if os.path.lexists(sock):
        os.unlink(sock)
    cmd = [args.codex_bin, "app-server", "--listen", f"unix://{sock}"]
    for override in args.config:
        cmd += ["-c", override]
    server = subprocess.Popen(cmd, cwd=args.cwd, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                              preexec_fn=_pdeathsig if os.name == "posix" else None)
    record_path: Optional[Path] = None
    state: Dict[str, Any] = {"turn_id": None, "status": None}
    done = threading.Event()

    def on_note(msg: Dict[str, Any]) -> None:
        method = str(msg.get("method") or "")
        if not method.endswith(_QUIET_SUFFIXES):
            _emit({"t": round(time.time(), 3), "method": method, "params": msg.get("params")})
        if method == "turn/completed":
            turn = (msg.get("params") or {}).get("turn") or {}
            if turn.get("id") and turn.get("id") == state["turn_id"]:
                state["status"] = turn.get("status")
                done.set()

    def on_signal(signum, _frame):
        state["status"] = f"signal:{signum}"
        done.set()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    client: Optional[_Client] = None
    try:
        deadline = time.monotonic() + _SOCKET_WAIT_S
        while not os.path.exists(sock):
            if server.poll() is not None or time.monotonic() > deadline:
                _emit({"type": "error", "error": f"app-server did not open {sock} (exit {server.poll()})"})
                return 1
            time.sleep(0.1)
        client = _Client(sock, on_note=on_note)
        params: Dict[str, Any] = {"cwd": args.cwd, "approvalPolicy": "never", "sandbox": "danger-full-access"}
        if args.model:
            params["model"] = args.model
        started = client.call("thread/start", params)
        thread = (started.get("result") or {}).get("thread") or {}
        thread_id = thread.get("id")
        if not thread_id:
            _emit({"type": "error", "error": f"thread/start failed: {_error_text(started)}"})
            return 1
        # First log line: resolve_session_id_from_log reads session_id here.
        _emit({"type": "thread.started", "session_id": thread_id, "transport": "app-server",
               "pid": os.getpid(), "server_pid": server.pid, "sock": sock})
        turn = client.call("turn/start", {"threadId": thread_id, "input": [{"type": "text", "text": args.goal}]})
        state["turn_id"] = ((turn.get("result") or {}).get("turn") or {}).get("id")
        if not state["turn_id"]:
            _emit({"type": "error", "error": f"turn/start failed: {_error_text(turn)}"})
            return 1
        record_path = _record_path(thread_id)
        tmp = record_path.with_suffix(".tmp")
        tmp.write_text(json.dumps({
            "thread_id": thread_id, "turn_id": state["turn_id"], "sock": sock, "pid": os.getpid(),
            "server_pid": server.pid, "worker_id": args.worker_id, "cwd": args.cwd,
            "started_at": time.time(),
        }))
        os.replace(tmp, record_path)
        while not done.wait(1.0):
            if client.closed.is_set() or server.poll() is not None:
                state["status"] = "app-server-exited"
                break
        if str(state["status"]).startswith("signal:") and not client.closed.is_set():
            client.call("turn/interrupt", {"threadId": thread_id, "turnId": state["turn_id"]}, timeout=10)
        _emit({"type": "turn.finished", "session_id": thread_id, "turn_id": state["turn_id"], "status": state["status"]})
        if state["status"] == "completed":
            return 0
        return 143 if str(state["status"]).startswith("signal:") else 1
    finally:
        if record_path is not None:
            try:
                record_path.unlink()
            except OSError:
                pass
        if client is not None:
            client.close()
        if server.poll() is None:
            server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
        try:
            os.unlink(sock)
        except OSError:
            pass


def main(argv: Optional[List[str]] = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv[:1] == ["run"]:
        return run(argv[1:])
    if argv[:1] == ["steer"]:
        p = argparse.ArgumentParser(prog="python -m watchtower.codex_app_worker steer")
        p.add_argument("thread_id")
        p.add_argument("--timeout", type=float, default=STEER_TIMEOUT_S)
        a = p.parse_args(argv[1:])
        result = steer(a.thread_id, sys.stdin.read(), timeout=a.timeout)
        print(json.dumps(result))
        return 0 if result.get("ok") else (2 if result.get("code") == "no_app_worker" else 1)
    print("usage: python -m watchtower.codex_app_worker {run|steer} ...", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())

"""Steerable Codex workers hosted on their own app-server (CODEX-CCC-WT-10).

Regression for OPS-155: a `codex exec` worker cannot be steered by anyone, so
opt-in queues host the worker thread on a private `codex app-server --listen
unix://` that other processes reach as extra clients.
"""

import json
import os
import stat
import subprocess
import sys
import textwrap
import time
from pathlib import Path

# Fake `codex app-server --listen unix://SOCK`: a stdlib WebSocket server
# that answers thread/start + turn/start, and completes the turn only after a
# turn/steer for that turn arrives over ANY client connection. The steer text
# is echoed into turn/completed so the test sees it reached the owning server.
FAKE_CODEX = textwrap.dedent(r'''
    import base64, hashlib, json, os, socket, struct, sys, threading
    sock_path = sys.argv[sys.argv.index("--listen") + 1][len("unix://"):]
    srv = socket.socket(socket.AF_UNIX); srv.bind(sock_path); srv.listen(8)
    owner = {}
    def frame(obj):
        data = json.dumps(obj).encode(); n = len(data)
        head = struct.pack("!BB", 0x81, n) if n < 126 else struct.pack("!BBH", 0x81, 126, n)
        return head + data
    def serve(c):
        buf = b""
        while b"\r\n\r\n" not in buf: buf += c.recv(4096)
        head, buf = buf.split(b"\r\n\r\n", 1)
        key = [l.split(b":", 1)[1].strip() for l in head.split(b"\r\n") if l.lower().startswith(b"sec-websocket-key")][0]
        acc = base64.b64encode(hashlib.sha1(key + b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest())
        c.sendall(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + acc + b"\r\n\r\n")
        def take(n):
            nonlocal buf
            while len(buf) < n:
                chunk = c.recv(65536)
                if not chunk: raise ConnectionError
                buf += chunk
            out, buf = buf[:n], buf[n:]; return out
        while True:
            b0, b1 = take(2); n = b1 & 0x7F
            if n == 126: n = struct.unpack("!H", take(2))[0]
            elif n == 127: n = struct.unpack("!Q", take(8))[0]
            mask = take(4); data = bytes(b ^ mask[i % 4] for i, b in enumerate(take(n)))
            if b0 & 0x0F == 0x8: return
            m = json.loads(data); method = m.get("method"); p = m.get("params") or {}
            if "id" not in m: continue
            if method == "initialize": res = {}
            elif method == "thread/start":
                res = {"thread": {"id": "11111111-2222-7333-8444-555555555555"}}
            elif method == "turn/start":
                owner["c"] = c; res = {"turn": {"id": "turn-1"}}
            elif method == "turn/steer":
                if p.get("expectedTurnId") != "turn-1":
                    c.sendall(frame({"id": m["id"], "error": {"message": "turn mismatch"}})); continue
                res = {"turnId": "turn-1"}
                owner["c"].sendall(frame({"method": "turn/completed", "params": {"threadId": p["threadId"],
                    "turn": {"id": "turn-1", "status": "completed", "steered": p["input"][0]["text"]}}}))
            else: res = {}
            c.sendall(frame({"id": m["id"], "result": res}))
    while True:
        conn, _ = srv.accept()
        threading.Thread(target=lambda: (lambda c: serve(c) if True else None)(conn), daemon=True).start()
''')


def test_app_server_transport_is_opt_in_and_keeps_exec_settings(wt_env):
    wt = wt_env
    exec_argv = wt.workers.build_drain_command("Q", "codex", "q-1", "/repo", model="gpt-x", effort="high")
    assert exec_argv[:2] == ["codex", "exec"]

    wt.config.set_codex_transport("Q", "app-server")
    argv = wt.workers.build_drain_command("Q", "codex", "q-1", "/repo", model="gpt-x", effort="high")
    assert "watchtower.codex_app_worker" in " ".join(argv) and "exec" not in argv
    exec_overrides = [exec_argv[i + 1] for i, a in enumerate(exec_argv) if a in ("-c", "--config")]
    app_overrides = [argv[i + 1] for i, a in enumerate(argv) if a == "-c" and i > 2]
    assert exec_overrides and app_overrides == exec_overrides
    assert argv[argv.index("--model") + 1] == "gpt-x"
    assert argv[argv.index("--worker-id") + 1] == "q-1"
    assert argv[argv.index("--cwd") + 1] == "/repo"
    assert argv[-1] == exec_argv[-1]

    wt.config.set_codex_transport("Q", "")
    assert wt.workers.build_drain_command("Q", "codex", "q-1", "/repo")[:2] == ["codex", "exec"]


def test_second_process_steers_hosted_worker_through_its_own_server(wt_env):
    """The real launch argv, as a real process: it advertises its thread, a
    separate client's deliver() steers the live turn on the worker's own
    server, and the worker exits 0 and withdraws its record on completion."""
    wt = wt_env
    fake = wt.bin / "codex"
    fake.write_text(f"#!{sys.executable}\n" + FAKE_CODEX)
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    from watchtower import codex_app_worker, codex_rpc

    wt.config.set_codex_transport("Q", "app-server")
    argv = wt.workers.build_drain_command("Q", "codex", "q-1", str(wt.tmp))
    argv[argv.index("--codex-bin") + 1] = str(fake)
    log = wt.tmp / "q-1.log"
    with open(log, "wb") as out:
        proc = subprocess.Popen(argv, cwd=wt.tmp, stdout=out, stderr=subprocess.STDOUT, start_new_session=True)
    try:
        thread_id = "11111111-2222-7333-8444-555555555555"
        deadline = time.monotonic() + 30
        while codex_app_worker.find_worker(thread_id) is None:
            assert time.monotonic() < deadline and proc.poll() is None, log.read_text()
            time.sleep(0.05)
        assert wt.workers.resolve_session_id_from_log(str(log)) == thread_id

        result = codex_rpc.deliver(thread_id, "STEER-TOKEN-7")
        assert proc.wait(30) == 0, log.read_text()
    finally:
        if proc.poll() is None:
            proc.kill()

    assert result["ok"] and result["via"] == "steer"
    events = [json.loads(line) for line in log.read_text().splitlines() if line.startswith("{")]
    completed = [e for e in events if e.get("method") == "turn/completed"]
    assert completed and completed[0]["params"]["turn"]["steered"] == "STEER-TOKEN-7"
    assert codex_app_worker.find_worker(thread_id) is None

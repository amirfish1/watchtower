"""Unit tests for the vendored UDS peer-messaging client (sending side only)."""

import hashlib
import json
import socket
import threading

from watchtower import peer_uds as uds


def test_wrap_builds_cross_session_wrapper():
    out = uds.wrap("hello", from_addr="uds:/tmp/x.sock", from_name="wt-reconciler")
    assert out == (
        '<cross-session-message from="uds:/tmp/x.sock" from-name="wt-reconciler">'
        "\nhello\n</cross-session-message>"
    )


def test_key_path_uses_sha256_of_socket_path(tmp_path):
    sock = "/tmp/cc-socks/4242.sock"
    digest = hashlib.sha256(sock.encode()).hexdigest()
    assert uds.key_path_for(tmp_path, 4242, sock) == tmp_path / f"4242.{digest}.key"


def test_load_peer_token_reads_json_key_file(tmp_path):
    sock = "/tmp/cc-socks/4242.sock"
    uds.key_path_for(tmp_path, 4242, sock).write_text(json.dumps({"peerToken": "tok-123"}))
    assert uds.load_peer_token(tmp_path, 4242, sock) == "tok-123"
    assert uds.load_peer_token(tmp_path, 9999, sock) == ""


def test_resolve_target_refusals(tmp_path):
    sock = tmp_path / "s.sock"
    base = {"pid": 4242, "messagingSocketPath": str(sock), "peerProtocol": 1, "version": "2.1.251"}
    assert uds.resolve_target(base)["reason"] == "socket_missing"
    sock.touch()
    r = uds.resolve_target(base)
    assert r["ok"] is True and r["pid"] == 4242
    assert uds.resolve_target(dict(base, peerProtocol=2))["reason"] == "peer_protocol"
    assert uds.resolve_target(dict(base, version="2.1.200"))["reason"] == "version_too_old"
    assert uds.resolve_target({})["reason"] == "no_socket_path"


def test_build_and_send_lines_round_trip(tmp_path):
    sock_path = str(tmp_path / "worker.sock")
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    srv.bind(sock_path)
    srv.listen(1)
    received = {}

    def accept_once():
        conn, _ = srv.accept()
        data = b""
        conn.settimeout(2.0)
        try:
            while True:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                data += chunk
        except socket.timeout:
            pass
        received["lines"] = data.splitlines()
        conn.close()

    t = threading.Thread(target=accept_once, daemon=True)
    t.start()
    lines = uds.build_frame_lines(
        uds.wrap("you are released"), token="tok-123", msg_id="m-1"
    )
    result = uds.send_lines(sock_path, lines)
    t.join(timeout=2.0)
    srv.close()

    assert result == {"ok": True, "error": ""}
    assert len(received["lines"]) == 2
    auth_frame = json.loads(received["lines"][0])
    user_frame = json.loads(received["lines"][1])
    assert auth_frame == {"type": "auth", "token": "tok-123"}
    assert user_frame["type"] == "user"
    assert "you are released" in user_frame["message"]["content"]
    assert user_frame["msg_id"] == "m-1"

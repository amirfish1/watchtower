"""Test transports for no-network gates; never imported by production.

Use real sockets when allowed. EPERM/EACCES alone selects memory transport;
other failures stay failures. The same handler/parser and peer frame writer run
in both cases. This does not grant loopback or Unix-socket access to the gate.
"""
import errno
import http.client
import io
import socket
import threading
import urllib.request
from types import SimpleNamespace


def socket_denied(exc):
    return isinstance(exc, OSError) and exc.errno in (errno.EPERM, errno.EACCES)


def http_json(dashboard, path, *, method='GET', body=None, force_memory=False):
    import json
    try:
        if force_memory:
            raise PermissionError(errno.EPERM, 'forced memory test transport')
        server = dashboard.ThreadingHTTPServer(('127.0.0.1', 0), dashboard._Handler)
    except OSError as exc:
        if not socket_denied(exc):
            raise
        # Feed an actual HTTP request through BaseHTTPRequestHandler.handle,
        # including its parser, dispatch, headers, and response serialization.
        payload = b'' if body is None else json.dumps(body).encode()
        request = (f'{method} {path} HTTP/1.0\r\nHost: 127.0.0.1\r\n'
                   f'Content-Length: {len(payload)}\r\n\r\n').encode() + payload
        sink = io.BytesIO()

        class Connection:
            def makefile(self, mode, *args):
                return io.BytesIO(request) if 'r' in mode else sink

        class Handler(dashboard._Handler):
            # StreamRequestHandler normally wraps the socket for sendall.
            wbufsize = 1

            def finish(self):
                self.wfile.flush()
                self.rfile.close()

        Handler(Connection(), ('127.0.0.1', 1), SimpleNamespace(server_name='localhost'))
        response = http.client.HTTPResponse(SimpleNamespace(makefile=lambda *a: io.BytesIO(sink.getvalue())))
        response.begin()
        assert response.status == 200, sink.getvalue()
        assert response.getheader('Content-Type') == 'application/json'
        return json.loads(response.read())
    thread = threading.Thread(target=server.handle_request, daemon=True)
    thread.start()
    try:
        request = urllib.request.Request(
            f'http://127.0.0.1:{server.server_address[1]}{path}',
            data=None if body is None else json.dumps(body).encode(), method=method)
        with urllib.request.urlopen(request, timeout=5) as response:
            assert response.status == 200
            return json.load(response)
    finally:
        thread.join(timeout=5)
        server.server_close()


def peer_listener(path, monkeypatch, *, memory_path, force_memory=False):
    """A real UDS listener or a connected byte-stream double for send_lines."""
    server = None
    try:
        if not force_memory:
            server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            server.bind(path)
            server.listen(1)
            return server, path
    except OSError as exc:
        if server is not None:
            server.close()
        if not socket_denied(exc):
            raise
    from watchtower import peer_uds
    from pathlib import Path
    # resolve_target requires a published endpoint; this is a fixture file,
    # not a live socket and is scoped to the test's writable temp directory.
    path = str(memory_path)
    Path(path).touch()
    ready = threading.Event()
    finished = threading.Event()
    chunks = []

    class Client:
        def __init__(self, family, kind):
            assert (family, kind) == (socket.AF_UNIX, socket.SOCK_STREAM)

        def settimeout(self, timeout):
            pass

        def connect(self, address):
            assert address == path
            ready.set()

        def sendall(self, data):
            assert ready.is_set() and not finished.is_set()
            chunks.append(data)

        def shutdown(self, how):
            assert how == socket.SHUT_WR
            finished.set()

        def close(self):
            finished.set()

    class Receiver:
        def settimeout(self, timeout):
            pass

        def recv(self, limit):
            assert finished.wait(2), 'peer writer did not finish'
            return chunks.pop(0) if chunks else b''

        def close(self):
            pass

    class Listener:
        def accept(self):
            assert ready.wait(2), 'peer writer never connected'
            return Receiver(), path

        def close(self):
            pass

    monkeypatch.setattr(peer_uds, 'socket', SimpleNamespace(
        socket=Client, AF_UNIX=socket.AF_UNIX, SOCK_STREAM=socket.SOCK_STREAM,
        SHUT_WR=socket.SHUT_WR, timeout=socket.timeout))
    return Listener(), path

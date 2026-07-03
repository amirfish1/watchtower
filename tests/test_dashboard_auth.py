"""Bearer-token gate for the messaging endpoints (WT-65).

Spins the real dashboard handler on an ephemeral port; targets an
unresolvable session so no delivery adapter ever runs — we only assert the
HTTP gate in front of it."""

from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

import pytest

from test_messages import wt  # noqa: F401


@pytest.fixture()
def api(wt):
    from watchtower import dashboard
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), dashboard._Handler)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()
    yield f"http://127.0.0.1:{httpd.server_address[1]}"
    httpd.shutdown()
    httpd.server_close()
    t.join(timeout=5)


def _post_send(base, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(
        base + "/api/send",
        data=json.dumps({"to": "no-such-target", "text": "hi"}).encode(),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as res:
            return res.status
    except urllib.error.HTTPError as e:
        return e.code


def test_no_token_configured_keeps_open_localhost_posture(api):
    assert _post_send(api) == 200


def test_token_configured_rejects_missing_and_wrong(api, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_API_TOKEN", "s3cret-token")
    assert _post_send(api) == 401
    assert _post_send(api, token="wrong") == 401


def test_token_configured_accepts_correct_bearer(api, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_API_TOKEN", "s3cret-token")
    assert _post_send(api, token="s3cret-token") == 200


def test_delegate_client_sends_bearer_token(wt, monkeypatch):
    """messages._post_json attaches Authorization when
    WATCHTOWER_DELEGATE_TOKEN is set (the client half of federation)."""
    captured = {}

    class _Res:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return b'{"ok": true}'

    def fake_urlopen(req, timeout=None):
        captured["auth"] = req.headers.get("Authorization")
        return _Res()

    monkeypatch.setenv("WATCHTOWER_DELEGATE_TOKEN", "fed-token")
    monkeypatch.setattr(wt.messages.urllib.request, "urlopen", fake_urlopen)
    wt.messages._post_json("http://example.invalid/api/inject-input", {}, 5)
    assert captured["auth"] == "Bearer fed-token"

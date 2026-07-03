"""Dashboard HTML group-chat view (WT-60): the index page's "Group chats"
section and the ``/chat/<ref>`` transcript page.

Same isolated-sandbox + real-handler-on-ephemeral-port pattern as
``tests/test_chat_cli.py``: fresh queue/chats/workers state under
``tmp_path``, the dashboard's own ``_Handler`` spun up via
``ThreadingHTTPServer`` on port 0, requests made with ``urllib.request``.
No mocking of ``chats``/``dashboard`` internals -- these are true end-to-end
HTML-rendering assertions, including the XSS-escaping check."""

from __future__ import annotations

import threading
import urllib.error
import urllib.request
from pathlib import Path

from test_chat_cli import wt  # noqa: F401  (shared isolated-sandbox fixture)

SID_A = "aaaa1111-0000-4000-8000-000000000001"


def _serve_once(dashboard):
    httpd = dashboard.ThreadingHTTPServer(("127.0.0.1", 0), dashboard._Handler)
    port = httpd.server_address[1]
    t = threading.Thread(target=httpd.handle_request, daemon=True)
    t.start()
    return httpd, port, t


def _get(port, path):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", method="GET")
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


# --------------------------------------------------------------- index page
def test_index_lists_created_chat_topic_escaped(wt):
    wt.chats.create_chat(
        "<b>Roadmap</b> & Q3", [{"session_id": SID_A, "name": "planner"}]
    )

    dashboard = wt.dashboard
    httpd, port, t = _serve_once(dashboard)
    try:
        status, page = _get(port, "/")
    finally:
        t.join(timeout=5)
        httpd.server_close()

    assert status == 200
    assert "Group chats" in page
    # The raw topic must never appear unescaped (that would be a stored-XSS hole).
    assert "<b>Roadmap</b>" not in page
    assert "&lt;b&gt;Roadmap&lt;/b&gt; &amp; Q3" in page
    assert "planner" in page


def test_index_shows_empty_state_when_no_chats(wt):
    dashboard = wt.dashboard
    httpd, port, t = _serve_once(dashboard)
    try:
        status, page = _get(port, "/")
    finally:
        t.join(timeout=5)
        httpd.server_close()

    assert status == 200
    assert "Group chats" in page
    assert "No active chats" in page


# --------------------------------------------------------------- chat page
def test_chat_page_renders_messages_with_html_escaping(wt):
    info = wt.chats.create_chat(
        "Ship the feature", [{"session_id": SID_A, "name": "planner"}]
    )
    wt.chats.post(
        info["path"],
        "<script>alert('xss')</script> looks done",
        author_sid=SID_A,
        author_name="planner",
    )

    dashboard = wt.dashboard
    httpd, port, t = _serve_once(dashboard)
    ref = Path(info["path"]).stem
    try:
        status, page = _get(port, f"/chat/{ref}")
    finally:
        t.join(timeout=5)
        httpd.server_close()

    assert status == 200
    assert "Ship the feature" in page
    assert "planner" in page
    # The message body must be escaped, never a live <script> tag.
    assert "<script>alert('xss')</script>" not in page
    assert "&lt;script&gt;alert(&#x27;xss&#x27;)&lt;/script&gt;" in page
    assert "looks done" in page


def test_chat_page_includes_nudge_policy_when_available(wt):
    info = wt.chats.create_chat(
        "Topic", [{"session_id": SID_A, "name": "planner"}]
    )
    dashboard = wt.dashboard
    httpd, port, t = _serve_once(dashboard)
    ref = Path(info["path"]).stem
    try:
        status, page = _get(port, f"/chat/{ref}")
    finally:
        t.join(timeout=5)
        httpd.server_close()

    assert status == 200
    if hasattr(wt.chats, "get_chat_policy"):
        assert "Nudge policy" in page


def test_chat_page_unknown_ref_returns_404(wt):
    dashboard = wt.dashboard
    httpd, port, t = _serve_once(dashboard)
    try:
        status, page = _get(port, "/chat/no-such-chat-at-all")
    finally:
        t.join(timeout=5)
        httpd.server_close()

    assert status == 404
    assert "not found" in page.lower()

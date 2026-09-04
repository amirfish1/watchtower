"""Mobile pane (WT-26): responsive dashboard + triage-first ordering.

Same isolated-sandbox + real-handler-on-ephemeral-port pattern as
``tests/test_chat_cli.py`` / ``tests/test_dashboard_chats_view.py``: fresh
queue/config/chats state under ``tmp_path``, the dashboard's own ``_Handler``
spun up via ``ThreadingHTTPServer`` on port 0, requests made with
``urllib.request``. No mocking of ``dashboard`` internals -- these are true
end-to-end HTML-rendering assertions.

Covers the three v1 acceptance points from the WT-26 shaping note:

1. Every rendered page (index, ``/q/<name>``, ``/chat/<ref>``) carries a
   viewport meta tag plus the apple-mobile-web-app tags that make
   add-to-home-screen behave like an app rather than a bookmark.
2. STUCK queues sort ahead of non-stuck queues in the rendered index HTML
   (triage-first), regardless of the order the caller's payload arrives in.
3. The index page's ``<title>`` is prefixed with the stuck count when any
   queue is stuck, and is the plain "WatchTower" title otherwise -- so a
   pinned phone tab surfaces trouble without opening the page.

Does NOT touch push notifications (out of v1 scope by design; see the WT-26
ticket note -- that's an open owner decision for a later version).
"""

from __future__ import annotations

import os
import threading
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

from test_chat_cli import wt  # noqa: F401  (shared isolated-sandbox fixture)


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


def _make_stuck_queue(wt, name="STUCKQ"):
    """Enqueue an old, un-progressing item in an auto-drain queue so
    ``health.all_status`` reports it as ``state == "stuck"``."""
    wt.q.enqueue(project=name, note="an item that never moves")
    wt.config.set_auto_drain(name, True)
    old = (datetime.now(timezone.utc) - timedelta(minutes=30)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    with wt.q._FileLock(wt.q._lock_path()):
        data = wt.q._load_unlocked()
        for it in data["items"]:
            if it.get("project") == name:
                it["created_at"] = old
        wt.q._save_unlocked(data)


def _make_clear_queue(wt, name="CLEARQ"):
    """A queue with no open work -- ``state == "clear"``, never stuck."""
    item = wt.q.enqueue(project=name, note="done already")
    wt.q.close(item["ref"], resolution={"summary": "closed"})


# ----------------------------------------------------------------- viewport
def test_index_page_has_viewport_and_apple_meta(wt):
    dashboard = wt.dashboard
    httpd, port, t = _serve_once(dashboard)
    try:
        status, page = _get(port, "/")
    finally:
        t.join(timeout=5)
        httpd.server_close()

    assert status == 200
    assert '<meta name="viewport" content="width=device-width, initial-scale=1">' in page
    assert 'name="apple-mobile-web-app-capable" content="yes"' in page
    assert 'name="mobile-web-app-capable" content="yes"' in page


def test_queue_page_has_viewport_and_apple_meta(wt):
    _make_clear_queue(wt, "MOBQ")

    dashboard = wt.dashboard
    httpd, port, t = _serve_once(dashboard)
    try:
        status, page = _get(port, "/q/MOBQ")
    finally:
        t.join(timeout=5)
        httpd.server_close()

    assert status == 200
    assert '<meta name="viewport" content="width=device-width, initial-scale=1">' in page
    assert 'name="apple-mobile-web-app-capable" content="yes"' in page


def test_stuck_auto_drain_queue_has_persistent_diagnostic_banner():
    """A queue that cannot make progress must explain why on its own page."""
    import watchtower.dashboard as dashboard

    payload = {
        "queues": [{
            "queue": "OPS", "depth": 3, "state": "stuck", "stuck": True,
            "auto_drain": True, "workers_live": 1, "workers_effective": 0,
            "claimable_depth": 3, "awaiting_human": 2,
            "staffing_diagnostics": ["worker ops-orphan missing session identity"],
            "configuration_error": "kimi-code/k3 does not support effort 'high'",
        }],
        "workers": [{"worker_id": "ops-orphan", "alive": True, "queue": "OPS"}],
    }

    page = dashboard.render_queue("OPS", payload, [])

    assert 'class="queue-alert"' in page
    assert 'role="alert"' in page
    assert "0 effective workers" in page
    assert "3 claimable tickets" in page
    assert "missing session identity" in page
    assert "kimi-code/k3 does not support effort" in page
    assert "2 awaiting human input" in page
    assert "wtReconcile('OPS')" in page
    assert 'href="#worker-ops-orphan"' in page


def test_status_payload_reports_unreachable_workers_and_invalid_effort(wt):
    wt.q.enqueue(project="OPS", note="first")
    wt.q.enqueue(project="OPS", note="second")
    wt.config.set_auto_drain("OPS", True)
    wt.config.set_engine("OPS", "kimi")
    wt.config.set_model("OPS", "kimi-code/k3")
    wt.config.set_effort("OPS", "high")
    wt.workers.record_worker(os.getpid(), "OPS", "kimi", "ops-orphan")

    payload = wt.dashboard.status_payload()
    row = payload["queues"][0]

    assert row["workers_live"] == 1
    assert row["workers_effective"] == 0
    assert row["claimable_depth"] == 2
    assert row["awaiting_human"] == 0
    assert "missing session identity" in row["staffing_diagnostics"][0]
    assert row["configuration_error"] == "kimi-code/k3 does not support effort 'high'"


def test_queue_banner_reconcile_action_runs_one_reconcile(wt, monkeypatch):
    dashboard = wt.dashboard
    seen = []
    monkeypatch.setattr(
        dashboard.workers, "reconcile_once", lambda: seen.append(True) or {"ok": "reconciled"}
    )
    httpd, port, thread = _serve_once(dashboard)
    try:
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/queue/OPS/reconcile",
            data=b"{}", method="POST",
            headers={"Content-Type": "application/json", "Origin": f"http://127.0.0.1:{port}"},
        )
        with urllib.request.urlopen(req, timeout=5) as response:
            body = response.read().decode()
    finally:
        thread.join(timeout=5)
        httpd.server_close()

    assert seen == [True]
    assert '"ok": true' in body
    assert '"queue": "OPS"' in body


def test_queue_banner_reconcile_rejects_cross_origin_request(wt, monkeypatch):
    dashboard = wt.dashboard
    monkeypatch.setattr(
        dashboard.workers, "reconcile_once", lambda: (_ for _ in ()).throw(AssertionError("must not run"))
    )
    httpd, port, thread = _serve_once(dashboard)
    try:
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/queue/OPS/reconcile",
            data=b"{}", method="POST",
            headers={"Content-Type": "application/json", "Origin": "https://untrusted.example"},
        )
        try:
            urllib.request.urlopen(req, timeout=5)
        except urllib.error.HTTPError as error:
            assert error.code == 403
        else:
            raise AssertionError("cross-origin reconcile request was accepted")
    finally:
        thread.join(timeout=5)
        httpd.server_close()


def test_chat_page_has_viewport_and_apple_meta(wt):
    SID_A = "aaaa1111-0000-4000-8000-000000000001"
    info = wt.chats.create_chat(
        "Mobile check", [{"session_id": SID_A, "name": "planner"}]
    )

    dashboard = wt.dashboard
    httpd, port, t = _serve_once(dashboard)
    try:
        from pathlib import Path

        ref = Path(info["path"]).stem
        status, page = _get(port, f"/chat/{ref}")
    finally:
        t.join(timeout=5)
        httpd.server_close()

    assert status == 200
    assert '<meta name="viewport" content="width=device-width, initial-scale=1">' in page
    assert 'name="apple-mobile-web-app-capable" content="yes"' in page


# ------------------------------------------------------------ triage order
def test_stuck_queue_sorts_before_non_stuck_in_index_html():
    """render_index() must not depend on the caller's payload already being
    sorted -- feed it stuck-last on purpose and assert the rendered HTML
    still shows the stuck card first."""
    import watchtower.dashboard as dashboard

    payload = {
        "queues": [
            {"queue": "AAAA_CLEAR", "depth": 0, "state": "clear", "auto_drain": True,
             "stuck": False, "workers_live": 0, "in_progress": 0,
             "drain_rate_per_min": 0, "eta_human": "empty"},
            {"queue": "ZZZZ_STUCK", "depth": 4, "state": "stuck", "auto_drain": True,
             "stuck": True, "workers_live": 0, "in_progress": 0,
             "drain_rate_per_min": 0, "eta_human": "STALLED"},
        ],
        "workers": [],
    }
    page = dashboard.render_index(payload, chat_rows=[])

    assert page.index("ZZZZ_STUCK") < page.index("AAAA_CLEAR")
    assert "is-stuck" in page


def test_stuck_queue_sorts_first_end_to_end(wt):
    """Same assertion, but through a real queue store + status_payload()."""
    _make_clear_queue(wt, "AAAA_CLEAR")
    _make_stuck_queue(wt, "ZZZZ_STUCK")

    dashboard = wt.dashboard
    httpd, port, t = _serve_once(dashboard)
    try:
        status, page = _get(port, "/")
    finally:
        t.join(timeout=5)
        httpd.server_close()

    assert status == 200
    assert page.index("ZZZZ_STUCK") < page.index("AAAA_CLEAR")


# --------------------------------------------------------------- title
def test_title_prefixed_with_stuck_count_when_stuck():
    import watchtower.dashboard as dashboard

    payload = {
        "queues": [
            {"queue": "A", "depth": 1, "state": "stuck", "auto_drain": True,
             "stuck": True, "workers_live": 0, "in_progress": 0,
             "drain_rate_per_min": 0, "eta_human": "STALLED"},
            {"queue": "B", "depth": 1, "state": "stuck", "auto_drain": True,
             "stuck": True, "workers_live": 0, "in_progress": 0,
             "drain_rate_per_min": 0, "eta_human": "STALLED"},
        ],
        "workers": [],
    }
    page = dashboard.render_index(payload, chat_rows=[])
    assert "<title>2 STUCK — WatchTower</title>" in page


def test_title_plain_when_no_stuck():
    import watchtower.dashboard as dashboard

    payload = {
        "queues": [
            {"queue": "A", "depth": 0, "state": "clear", "auto_drain": True,
             "stuck": False, "workers_live": 0, "in_progress": 0,
             "drain_rate_per_min": 0, "eta_human": "empty"},
        ],
        "workers": [],
    }
    page = dashboard.render_index(payload, chat_rows=[])
    assert "<title>WatchTower</title>" in page
    assert "STUCK" not in page.split("</head>")[0]


def test_title_reflects_live_stuck_count_end_to_end(wt):
    _make_stuck_queue(wt, "ONEQ")

    dashboard = wt.dashboard
    httpd, port, t = _serve_once(dashboard)
    try:
        status, page = _get(port, "/")
    finally:
        t.join(timeout=5)
        httpd.server_close()

    assert status == 200
    assert "<title>1 STUCK — WatchTower</title>" in page

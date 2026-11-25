"""GitHub connectivity alert surfaced on the CCC dashboard header (2026-07-27
design): the existing "beacon" element that already turns red for stuck
queues now also turns red when GitHub has been unreachable for a sustained
period, with a short text line explaining why."""

from __future__ import annotations


def _base_payload(github=None):
    return {
        "queues": [
            {"queue": "A", "depth": 0, "state": "clear", "auto_drain": True,
             "stuck": False, "workers_live": 0, "in_progress": 0,
             "drain_rate_per_min": 0, "eta_human": "empty"},
        ],
        "workers": [],
        "github": github or {"alert": False},
    }


def test_beacon_is_not_alert_when_github_healthy_and_nothing_stuck():
    import watchtower.dashboard as dashboard

    page = dashboard.render_index(_base_payload(), chat_rows=[])
    assert 'class="beacon alert"' not in page


def test_beacon_turns_alert_when_github_unreachable_even_with_no_stuck_queues():
    import watchtower.dashboard as dashboard

    payload = _base_payload(github={
        "alert": True, "outage_duration": "6m", "outage_duration_s": 360,
        "last_error": "gh auth login required", "consecutive_failures": 4,
        "broken_since": "2026-07-27T00:00:00Z",
    })
    page = dashboard.render_index(payload, chat_rows=[])
    assert 'class="beacon alert"' in page
    assert "GitHub unreachable 6m" in page
    assert "gh auth login required" in page


def test_beacon_ignores_a_github_alert_that_is_false():
    import watchtower.dashboard as dashboard

    payload = _base_payload(github={"alert": False, "outage_duration": None})
    page = dashboard.render_index(payload, chat_rows=[])
    assert 'class="beacon alert"' not in page
    assert "GitHub unreachable" not in page


def test_render_index_tolerates_a_payload_with_no_github_key():
    """Callers that built a payload before this feature (or any test payload
    that never set one) must not crash render_index."""
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
    assert 'class="beacon alert"' not in page


def test_status_payload_includes_github_connectivity_block(tmp_path, monkeypatch):
    import importlib
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_CONFIG_FILE", str(tmp_path / "config.json"))
    monkeypatch.setenv("WATCHTOWER_GH_CONNECTIVITY_FILE", str(tmp_path / "gh-connectivity.json"))
    import watchtower.config as config
    import watchtower.health as health
    import watchtower.dashboard as dashboard
    importlib.reload(config)
    importlib.reload(health)
    importlib.reload(dashboard)

    payload = dashboard.status_payload()
    assert "github" in payload
    assert payload["github"]["alert"] is False


def test_home_page_counts_never_start_a_live_github_read(monkeypatch):
    """WT-113: GET / counted tickets with a live list of every GitHub queue,
    up to two ``gh`` probes per queue inside the request (up to 14 s). The
    display-only counts must come from the poller's snapshot."""
    import watchtower.dashboard as dashboard

    calls = []
    monkeypatch.setattr(dashboard.q, "list_items",
                        lambda **kw: calls.append(kw) or [{"project": "A", "status": "closed"}])
    fallback = dashboard.render_index(_base_payload(), chat_rows=[])
    assert calls and all(kw.get("snapshot_only") for kw in calls)

    # The status refresh precomputes the counts: GET / reads no store at all.
    calls.clear()
    payload = dict(_base_payload(), ticket_counts={"A": [1, 1]})
    assert dashboard.render_index(payload, chat_rows=[]) == fallback
    assert calls == []
    assert dashboard._ticket_counts([
        {"project": "a", "status": "closed"}, {"project": "A", "status": "open"},
        {"project": "A", "status": "awaiting_release"}, {"project": ""},
    ]) == {"A": [2, 3]}

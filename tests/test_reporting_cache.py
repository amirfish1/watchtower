"""GitHub reporting is local, bounded, and honest about snapshot coverage."""
import json
import time
from datetime import datetime, timezone

import pytest


@pytest.fixture
def report(tmp_path, monkeypatch):
    from watchtower import github_backend as gh, queue as q, reporting_cache as rc
    monkeypatch.setenv("WATCHTOWER_GH_LIST_CACHE_FILE", str(tmp_path / "gh.json"))
    monkeypatch.setenv("WATCHTOWER_CONFIG_FILE", str(tmp_path / "config.json"))
    monkeypatch.setenv("WATCHTOWER_GH_PIPELINE_FILE", str(tmp_path / "pipeline.json"))
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_USAGE_DIR", str(tmp_path / "usage"))
    gh._PERSISTED_LIST_MEM_CACHE.update(mtime_ns=0, size=-1, data={})
    backend = gh.GitHubIssuesBackend("REPORT", repo="test/repo", auto_drain=False,
                                     grace_s=0, partition_by_label=False)
    monkeypatch.setattr(q, "_github_backend_for_project", lambda proj: backend if proj == "REPORT" else None)
    def no_network(*a, **kw):
        raise AssertionError("reporting must not call gh or scan list_items")
    monkeypatch.setattr(backend, "_run", no_network)
    monkeypatch.setattr(q, "list_items", no_network)
    monkeypatch.setattr(backend, "_list_issues", no_network)
    now = datetime.now(timezone.utc).isoformat()
    def issue(n, state="CLOSED", events=()):
        return {"number": n, "title": f"issue {n}", "state": state,
                "body": gh._body_with_metadata("report", {"history": list(events)}),
                "createdAt": "2020-01-01T00:00:00Z", "updatedAt": now,
                "closedAt": now if state == "CLOSED" else None,
                "labels": [], "assignees": [], "url": f"https://github.com/test/repo/issues/{n}"}
    def publish(state, rows, **extra):
        entry = dict(at=time.time(), fetched_at=time.time(), stale=False, data=rows)
        entry.update(extra)
        gh._write_persisted_list_entry(f"test/repo:{state}", entry)
    publish("open", [issue(3, "OPEN")])
    publish("closed", [issue(1), issue(2)])
    return q, gh, rc, backend, issue, publish


def test_local_mirror_paginates_without_reopening_json(report, monkeypatch):
    q, gh, rc, backend, issue, publish = report
    spec = q.WindowSpec(closed_since=time.time() - 86400)
    page, meta = q.window_items("REPORT", spec, limit=1)
    assert [r["ref"] for r in page] == ["REPORT-1"]
    assert not meta["complete"] and meta["next_offset"] == 1 and meta["source_complete"]
    def no_snapshot():
        raise AssertionError("steady reporting read parsed the full JSON snapshot")
    monkeypatch.setattr(gh, "_read_persisted_list_cache", no_snapshot)
    page, meta = q.window_items("REPORT", spec, limit=1, offset=1)
    assert [r["ref"] for r in page] == ["REPORT-2"] and meta["complete"]


def test_writer_updates_projection_and_catches_same_clock_edits(report, monkeypatch):
    q, gh, rc, backend, issue, publish = report
    spec = q.WindowSpec(events=["auto_decision", "auto_decision_reversed"],
                        event_since=q._window_ts("2026-11-01T00:00:00Z"))
    q.window_items("REPORT", spec)  # creates mirror
    edited = issue(3, "OPEN", [{"event": "auto_decision", "at": "2026-11-02T00:00:00Z"}])
    publish("open", [edited])
    def no_snapshot():
        raise AssertionError("writer failed to maintain projection")
    original = gh._read_persisted_list_cache
    monkeypatch.setattr(gh, "_read_persisted_list_cache", no_snapshot)
    page, meta = q.window_items("REPORT", spec)
    assert [it["ref"] for it in page] == ["REPORT-3"]
    assert len(page[0]["window_events"]) == 1
    assert not meta["complete"] and not meta["source_complete"] and "retention" in meta["coverage"]
    monkeypatch.setattr(gh, "_read_persisted_list_cache", original)
    publish("open", [])
    page, meta = q.window_items("REPORT", spec)
    assert page == []


def test_missing_stale_and_capped_snapshots_never_claim_complete(report, monkeypatch):
    q, gh, rc, backend, issue, publish = report
    spec = q.WindowSpec(closed_since=time.time() - 3600)
    publish("closed", [issue(1)], stale=True)
    page, meta = q.window_items("REPORT", spec)
    assert not meta["complete"] and "stale" in meta["coverage"]
    publish("closed", [issue(1)])
    monkeypatch.setattr(gh, "_list_limit", lambda: 1)
    page, meta = q.window_items("REPORT", spec)
    assert not meta["complete"] and "capped" in meta["coverage"]
    gh._list_cache_path().unlink()
    page, meta = q.window_items("REPORT", spec)
    assert page == [] and not meta["complete"] and "missing" in meta["coverage"]


def test_older_closed_reports_are_available_but_retention_gap_is_explicit(report):
    q, gh, rc, backend, issue, publish = report
    old = issue(9, events=[{"event": "auto_decision_reversed", "at": "2026-11-10T00:00:00Z"}])
    old["closedAt"] = "2020-01-01T00:00:00Z"
    publish("closed", [old])
    # Unlike ordinary list_items this path does not apply a second retention
    # filter to locally available older rows.
    page, meta = q.window_items("REPORT", q.WindowSpec(closed_since=0))
    assert [it["ref"] for it in page] == ["REPORT-9"] and not meta["complete"]
    page, meta = q.window_items("REPORT", q.WindowSpec(events=["auto_decision_reversed"]))
    assert page[0]["window_events"][0]["at"] == "2026-11-10T00:00:00Z"


def test_config_partition_change_rebuilds_unchanged_raw_rows(report):
    q, gh, rc, backend, issue, publish = report
    spec = q.WindowSpec(refs=["REPORT-3"])
    page, _ = q.window_items("REPORT", spec)
    assert len(page) == 1
    backend.partition_by_label = True
    page, _ = q.window_items("REPORT", spec)
    assert not page  # same raw issue; queue label now required


def test_reopening_with_overlap_between_independent_snapshots(report):
    q, gh, rc, backend, issue, publish = report
    spec = q.WindowSpec(refs=["REPORT-3"])
    original_open = issue(3, "OPEN")
    publish("open", [original_open])
    page, _ = q.window_items("REPORT", spec)
    assert page[0]["status"] == "open"
    # The delayed closed list briefly contains the very same issue, while
    # its open row is unchanged from the previous generation.
    publish("closed", [issue(3, "CLOSED")])
    page, _ = q.window_items("REPORT", spec)
    assert page[0]["status"] == "open"
    assert q.window_items("REPORT", q.WindowSpec(closed_since=0))[0] == []


def test_unrelated_writer_advances_only_current_projection(report, monkeypatch):
    q, gh, rc, backend, issue, publish = report
    spec = q.WindowSpec(refs=["REPORT-3"])
    before, _ = q.window_items("REPORT", spec)
    def no_rebuild(*a, **kw):
        raise AssertionError("unrelated writer rescanned report issues")
    monkeypatch.setattr(rc, "_publish", no_rebuild)
    gh._write_persisted_list_entry("other/repo:open", {"data": [issue(99, "OPEN")]})
    def no_snapshot():
        raise AssertionError("generation was not advanced by writer")
    monkeypatch.setattr(gh, "_read_persisted_list_cache", no_snapshot)
    after, _ = q.window_items("REPORT", spec)
    assert before == after


def test_unrelated_writer_repairs_missed_publication(report, monkeypatch):
    q, gh, rc, backend, issue, publish = report
    spec = q.WindowSpec(refs=["REPORT-3"])
    q.window_items("REPORT", spec)
    with monkeypatch.context() as patch:
        def failed(*a, **kw):
            raise OSError("interrupted publication")
        patch.setattr(rc, "publish_existing", failed)
        updated = issue(3, "OPEN")
        updated["title"] = "changed without changing issue clocks"
        publish("open", [updated])
    # This writer did not change our repo, but our projection missed the last
    # generation and must be repaired rather than marked current.
    gh._write_persisted_list_entry("other/repo:closed", {"data": []})
    def no_snapshot():
        raise AssertionError("stale projection was not repaired by writer")
    monkeypatch.setattr(gh, "_read_persisted_list_cache", no_snapshot)
    page, _ = q.window_items("REPORT", spec)
    assert page[0]["title"] == updated["title"]


def test_unrelated_writer_applies_changed_partition_configuration(report):
    q, gh, rc, backend, issue, publish = report
    spec = q.WindowSpec(refs=["REPORT-3"])
    assert q.window_items("REPORT", spec)[0]
    backend.partition_by_label = True
    gh._write_persisted_list_entry("other/repo:open", {"data": []})
    assert not q.window_items("REPORT", spec)[0]


def test_invalidation_keeps_affected_projection_coverage_stale(report):
    q, gh, rc, backend, issue, publish = report
    spec = q.WindowSpec(closed_since=time.time() - 3600)
    assert q.window_items("REPORT", spec)[1]["source_complete"]
    gh._invalidate_list_cache(backend.repo)
    _, meta = q.window_items("REPORT", spec)
    assert not meta["source_complete"] and "stale" in meta["coverage"]


def test_coverage_only_refresh_does_not_read_raw_issue_blobs(report, monkeypatch):
    q, gh, rc, backend, issue, publish = report
    spec = q.WindowSpec(closed_since=time.time() - 3600)
    before, _ = q.window_items("REPORT", spec)
    original = q._connect
    statements = []

    def traced(path):
        conn = original(path)
        conn.set_trace_callback(statements.append)
        return conn

    monkeypatch.setattr(q, "_connect", traced)
    gh._invalidate_list_cache(backend.repo)
    assert not any("SELECT number, blob FROM raw_issues" in sql for sql in statements)
    after, meta = q.window_items("REPORT", spec)
    assert after == before
    assert not meta["source_complete"] and "stale" in meta["coverage"]


def test_quiet_revalidation_is_bounded_but_changes_publish_immediately(report):
    q, gh, rc, backend, issue, publish = report
    path = gh._list_cache_path()
    key = f"{backend.repo}:open"
    entry = gh._read_persisted_list_cache()[key]
    stamp = rc._stamp(path)
    gh._write_persisted_list_entry(key, dict(entry))
    gh._write_persisted_list_entry(key, dict(entry, at=entry["at"] + 5))
    assert rc._stamp(path) == stamp
    gh._write_persisted_list_entry(key, dict(entry, at=entry["at"] + 61))
    assert rc._stamp(path) != stamp
    updated = issue(3, "OPEN")
    updated["title"] = "same clock, changed body"
    gh._write_persisted_list_entry(key, dict(entry, at=entry["at"] + 62, data=[updated]))
    assert gh._read_persisted_list_cache()[key]["data"] == [updated]


def test_short_ttl_still_gets_a_timely_heartbeat(report, monkeypatch):
    q, gh, rc, backend, issue, publish = report
    monkeypatch.setenv("WATCHTOWER_GH_LIST_TTL_S", "10")
    key = f"{backend.repo}:open"
    entry = gh._read_persisted_list_cache()[key]
    gh._write_persisted_list_entry(key, dict(entry, at=entry["at"] + 6))
    assert gh._read_persisted_list_cache()[key]["at"] == entry["at"] + 6

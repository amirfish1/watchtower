"""Listing closed tickets that were flagged with unresolved work (WATCHTOWER-17).

`resolution.unresolved` has always been stored and badged on the dashboard, but
the CLI had no query for it — owners dumped `wt ls --status closed --json` and
filtered it themselves. These tests pin the three surfaces that replaced that:
`wt unresolved`, `wt ls --unresolved`, and the count in the `wt ls` header.
"""

from __future__ import annotations

import json


def _closed(wt_env, project, note, **resolution):
    q = wt_env.queue
    item = q.enqueue(project=project, note=note)
    q.claim_next("w1", project=project)
    q.close(item["ref"], "w1", resolution={"summary": "did it", **resolution})
    return item["ref"]


def test_unresolved_lists_only_flagged_closed_tickets(wt_env, run_cli):
    flagged = _closed(wt_env, "UNQ", "flagged", unresolved=["the flaky test"])
    clean = _closed(wt_env, "UNQ", "clean")
    _closed(wt_env, "UNQ", "caveat only", caveats=["watch out"])
    wt_env.queue.enqueue(project="UNQ", note="still open")

    out = run_cli("unresolved", "-q", "UNQ").out

    assert flagged in out
    assert clean not in out
    assert "the flaky test" in out
    assert "1 closed ticket with 1 unresolved item" in out


def test_unresolved_scans_every_queue_without_a_queue_flag(wt_env, run_cli):
    a = _closed(wt_env, "UNQA", "a", unresolved=["one"])
    b = _closed(wt_env, "UNQB", "b", unresolved=["two"])

    out = run_cli("unresolved").out

    assert a in out and b in out
    assert "all queues" in out


def test_unresolved_marks_acked_entries(wt_env, run_cli):
    ref = _closed(wt_env, "UNQ", "flagged", unresolved=["stale", "live"])
    wt_env.queue.ack_resolution(ref, targets=[("unresolved", 0)], by="amir")

    out = run_cli("unresolved", "-q", "UNQ").out

    assert "2 unresolved, 1 acked" in out
    assert "- stale (acked)" in out
    assert "- live" in out
    assert "live (acked)" not in out


def test_unresolved_is_empty_when_nothing_is_flagged(wt_env, run_cli):
    _closed(wt_env, "UNQ", "clean")

    out = run_cli("unresolved", "-q", "UNQ").out

    assert "no closed tickets with unresolved items" in out


def test_unresolved_json_returns_full_ticket_records(wt_env, run_cli):
    ref = _closed(wt_env, "UNQ", "flagged", unresolved=["the flaky test"])
    _closed(wt_env, "UNQ", "clean")

    rows = json.loads(run_cli("unresolved", "-q", "UNQ", "--json").out)

    assert [r["ref"] for r in rows] == [ref]
    assert rows[0]["resolution"]["unresolved"] == ["the flaky test"]


def test_ls_unresolved_flag_and_status_agree(wt_env, run_cli):
    ref = _closed(wt_env, "UNQ", "flagged", unresolved=["the flaky test"])
    _closed(wt_env, "UNQ", "clean")

    by_flag = json.loads(run_cli("ls", "-q", "UNQ", "--unresolved", "--json").out)
    by_status = json.loads(
        run_cli("ls", "-q", "UNQ", "--status", "unresolved", "--json").out
    )

    assert [r["ref"] for r in by_flag] == [ref]
    assert by_flag == by_status


def test_ls_header_reports_unresolved_count_from_the_active_view(wt_env, run_cli):
    _closed(wt_env, "UNQ", "flagged", unresolved=["the flaky test"])
    wt_env.queue.enqueue(project="UNQ", note="still open")

    out = run_cli("ls", "-q", "UNQ").out

    # The closed row itself is filtered out of the active view, so the count is
    # the only thing telling the owner unresolved work exists.
    assert "UNQ: 1 closed ticket with unresolved items" in out
    assert "wt unresolved -q UNQ" in out


def test_ls_header_is_silent_when_nothing_is_unresolved(wt_env, run_cli):
    _closed(wt_env, "UNQ", "clean")
    wt_env.queue.enqueue(project="UNQ", note="still open")

    out = run_cli("ls", "-q", "UNQ").out

    assert "unresolved" not in out


def test_legacy_string_resolution_is_not_treated_as_unresolved():
    """Old rows store `resolution` as a bare summary string, not a dict.

    Real data on this machine has both shapes, and the dict-only reader
    crashed with AttributeError on the first legacy row it hit.
    """
    from watchtower import cli

    legacy = {"ref": "OLD-1", "status": "closed", "resolution": "did it"}

    assert cli._unresolved_entries(legacy) == []
    assert cli._unresolved_items([legacy]) == []
    assert cli._ack_counts(legacy) == (0, 0)

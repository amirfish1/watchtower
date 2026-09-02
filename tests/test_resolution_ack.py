"""Acknowledging a closed ticket's resolution warnings (WATCHTOWER-12).

The dashboard renders caveats / follow-ups / unresolved entries as coloured
chips, and the only way to clear a stale one used to be ``wt close --force``
with a rebuilt resolution — which rewrites the close record and re-fires close
notifications purely to quiet the UI. These tests pin the non-destructive
alternative: the text is preserved verbatim, the ack is recorded alongside it,
and the chip renders dimmed.
"""

from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request

import pytest


def _closed(wt_env, **resolution):
    q = wt_env.queue
    item = q.enqueue(project="ACKQ", note="a ticket")
    q.claim_next("w1", project="ACKQ")
    q.close(item["ref"], "w1", resolution={"summary": "did it", **resolution})
    return item["ref"]


def test_ack_marks_one_item_without_touching_the_text(wt_env):
    q = wt_env.queue
    ref = _closed(wt_env, unresolved=["stale thing", "live thing"])

    item = q.ack_resolution(ref, targets=[("unresolved", 0)], by="amir")
    res = item["resolution"]

    assert res["unresolved"] == ["stale thing", "live thing"]
    assert res["summary"] == "did it"
    assert q.is_acked(res, "unresolved", 0)
    assert not q.is_acked(res, "unresolved", 1)
    assert res["unresolved_ack"]["0"]["by"] == "amir"
    assert res["unresolved_ack"]["0"]["at"]


def test_ack_is_idempotent_and_reversible(wt_env):
    q = wt_env.queue
    ref = _closed(wt_env, caveats=["watch out"])

    first = q.ack_resolution(ref, targets=[("caveats", 0)])["resolution"]
    again = q.ack_resolution(ref, targets=[("caveats", 0)])["resolution"]
    assert again["caveats_ack"]["0"]["at"] == first["caveats_ack"]["0"]["at"]

    undone = q.ack_resolution(ref, targets=[("caveats", 0)], undo=True)["resolution"]
    assert "caveats_ack" not in undone
    assert undone["caveats"] == ["watch out"]
    # Un-acking twice is a no-op, not an error.
    assert "caveats_ack" not in q.ack_resolution(
        ref, targets=[("caveats", 0)], undo=True
    )["resolution"]


def test_ack_all_covers_every_field(wt_env):
    q = wt_env.queue
    ref = _closed(
        wt_env, caveats=["c"], follow_ups=["f1", "f2"], unresolved=["u"]
    )

    res = q.ack_resolution(ref, all_items=True)["resolution"]

    assert res["caveats_ack"].keys() == {"0"}
    assert res["follow_ups_ack"].keys() == {"0", "1"}
    assert res["unresolved_ack"].keys() == {"0"}


def test_ack_records_a_timeline_event(wt_env):
    q = wt_env.queue
    ref = _closed(wt_env, unresolved=["u"])

    q.ack_resolution(ref, targets=[("unresolved", 0)], by="amir")
    q.ack_resolution(ref, targets=[("unresolved", 0)], by="amir", undo=True)

    events = [e["event"] for e in q.get(ref)["history"]]
    assert events[-2:] == ["ack", "unack"]
    ack = [e for e in q.get(ref)["history"] if e["event"] == "ack"][0]
    assert ack["text"] == "unresolved#1"


def test_ack_rejects_bad_field_index_and_ticket(wt_env):
    q = wt_env.queue
    ref = _closed(wt_env, unresolved=["u"])

    with pytest.raises(ValueError, match="unknown resolution field"):
        q.ack_resolution(ref, targets=[("bogus", 0)])
    with pytest.raises(ValueError, match="no index 4"):
        q.ack_resolution(ref, targets=[("unresolved", 3)])
    with pytest.raises(ValueError, match="nothing selected"):
        q.ack_resolution(ref, targets=[])

    plain = q.enqueue(project="ACKQ", note="no resolution here")
    with pytest.raises(ValueError, match="no caveat/follow-up/unresolved"):
        q.ack_resolution(plain["ref"], all_items=True)

    assert q.ack_resolution("ACKQ-999", all_items=True) is None


def test_acks_survive_resolution_normalization(wt_env):
    """A stored resolution re-normalized (e.g. on a rewrite path) keeps its
    acks — otherwise every chip a human cleared silently lights up again."""
    q = wt_env.queue
    ref = _closed(wt_env, unresolved=["u1", "u2"])
    res = q.ack_resolution(ref, targets=[("unresolved", 1)])["resolution"]

    renormalized = q._normalize_resolution(res)
    assert renormalized["unresolved_ack"] == res["unresolved_ack"]

    # An ack pointing past a shortened list is dropped rather than mis-aimed.
    shrunk = dict(res, unresolved=["u1"])
    assert "unresolved_ack" not in q._normalize_resolution(shrunk)


def test_cli_lists_then_acks(wt_env, run_cli):
    ref = _closed(wt_env, unresolved=["stale", "live"], caveats=["careful"])

    listing = run_cli("unresolved-ack", ref)
    assert listing.code == 0
    assert "--unresolved 1" in listing.out and "[     ]" in listing.out

    acked = run_cli("unresolved-ack", ref, "--unresolved", "1", "--by", "amir")
    assert acked.code == 0
    assert "ACKED:" in acked.out
    assert "[acked] --unresolved 1" in acked.out

    assert wt_env.queue.is_acked(wt_env.queue.get(ref)["resolution"], "unresolved", 0)

    undone = run_cli("unresolved-ack", ref, "--unresolved", "1", "--undo")
    assert undone.code == 0
    assert "UNACKED:" in undone.out
    assert not wt_env.queue.is_acked(wt_env.queue.get(ref)["resolution"], "unresolved", 0)


def test_cli_ack_rejects_zero_and_out_of_range(wt_env, run_cli):
    ref = _closed(wt_env, unresolved=["only one"])

    assert run_cli("unresolved-ack", ref, "--unresolved", "0").code == 1
    bad = run_cli("unresolved-ack", ref, "--unresolved", "7")
    assert bad.code == 1 and "no index 7" in bad.err


def test_ls_reports_how_many_warnings_are_acked(wt_env, run_cli):
    ref = _closed(wt_env, unresolved=["a", "b"])
    wt_env.queue.ack_resolution(ref, targets=[("unresolved", 0)])

    out = run_cli("ls", "-q", "ACKQ", "--status", "closed").out
    assert "2 unresolveds, 1 acked" in out


def test_chips_dim_when_acked(wt_env):
    from watchtower import dashboard

    q = wt_env.queue
    ref = _closed(wt_env, unresolved=["stale", "live"])
    q.ack_resolution(ref, targets=[("unresolved", 0)])
    res = q.get(ref)["resolution"]

    chips = dashboard._resolution_chips(res, ref)
    assert 'class="chip unresolved acked"' in chips
    assert 'class="chip unresolved"' in chips
    # The text is never dropped — an ack quiets a chip, it doesn't delete it.
    assert "stale" in chips and "live" in chips
    assert f"wtAck('{ref}','unresolved',0,true)" in chips
    assert f"wtAck('{ref}','unresolved',1,false)" in chips

    # Without a ref (no ack transport) the chips render as before.
    assert "ackbtn" not in dashboard._resolution_chips(res)


def _serve(dashboard):
    srv = dashboard.ThreadingHTTPServer(("127.0.0.1", 0), dashboard._Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, f"http://127.0.0.1:{srv.server_address[1]}"


def test_dashboard_ack_endpoint(wt_env):
    from watchtower import dashboard

    q = wt_env.queue
    ref = _closed(wt_env, unresolved=["stale", "live"])
    srv, base = _serve(dashboard)
    try:
        def post(body):
            req = urllib.request.Request(
                f"{base}/api/ticket/{ref}/ack",
                data=json.dumps(body).encode(),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(req) as resp:
                    return resp.status, json.load(resp)
            except urllib.error.HTTPError as exc:
                return exc.code, json.load(exc)

        code, payload = post({"field": "unresolved", "index": 0})
        assert code == 200 and payload["ok"]
        assert q.is_acked(q.get(ref)["resolution"], "unresolved", 0)

        assert post({"field": "unresolved", "index": 0, "undo": True})[0] == 200
        assert not q.is_acked(q.get(ref)["resolution"], "unresolved", 0)

        assert post({"all": True})[0] == 200
        assert q.is_acked(q.get(ref)["resolution"], "unresolved", 1)

        code, payload = post({"field": "unresolved", "index": 99})
        assert code == 400 and "no index" in payload["error"]
        assert post({"field": "unresolved"})[0] == 400
    finally:
        srv.shutdown()


def test_queue_page_ships_the_ack_toggle_even_with_no_active_tickets(wt_env):
    """A drained queue is exactly where you sit clearing old warning chips,
    so the no-active-tickets branch must carry the script too."""
    from watchtower import dashboard

    q = wt_env.queue
    ref = _closed(wt_env, unresolved=["stale"])
    q.ack_resolution(ref, targets=[("unresolved", 0)])
    srv, base = _serve(dashboard)
    try:
        with urllib.request.urlopen(f"{base}/q/ACKQ") as resp:
            page = resp.read().decode()
    finally:
        srv.shutdown()

    assert "No active tickets" in page
    assert "async function wtAck" in page
    assert 'class="chip unresolved acked"' in page


# --------------------------------------------------------- bulk ack (WATCHTOWER-18)

def _closed_in(wt_env, project, note, **resolution):
    q = wt_env.queue
    item = q.enqueue(project=project, note=note)
    q.claim_next("w1", project=project)
    q.close(item["ref"], "w1", resolution={"summary": "did it", **resolution})
    return item["ref"]


def test_bulk_ack_covers_every_closed_ticket_in_the_queue(wt_env, run_cli):
    a = _closed_in(wt_env, "BULKQ", "a", unresolved=["not-applicable"])
    b = _closed_in(wt_env, "BULKQ", "b", caveats=["watch out"])
    wt_env.queue.enqueue(project="BULKQ", note="still open")

    out = run_cli("unresolved-ack", "-q", "BULKQ", "--all").out

    assert "ACKED: 2 tickets in BULKQ" in out
    for ref in (a, b):
        res = wt_env.queue.get(ref)["resolution"]
        field = "unresolved" if ref == a else "caveats"
        assert wt_env.queue.is_acked(res, field, 0)


def test_bulk_ack_matching_narrows_by_resolution_text(wt_env, run_cli):
    hit = _closed_in(wt_env, "BULKQ", "a", unresolved=["closed as not-applicable"])
    miss = _closed_in(wt_env, "BULKQ", "b", unresolved=["the flaky test"])

    run_cli("unresolved-ack", "-q", "BULKQ", "--all", "--matching", "NOT-APPLICABLE")

    assert wt_env.queue.is_acked(wt_env.queue.get(hit)["resolution"], "unresolved", 0)
    assert not wt_env.queue.is_acked(
        wt_env.queue.get(miss)["resolution"], "unresolved", 0
    )


def test_bulk_ack_matching_also_searches_the_summary(wt_env, run_cli):
    """Terminal verdicts are usually written in the summary, not each bullet."""
    q = wt_env.queue
    item = q.enqueue(project="BULKQ", note="a")
    q.claim_next("w1", project="BULKQ")
    q.close(item["ref"], "w1", resolution={
        "summary": "closed as not-applicable", "unresolved": ["see above"],
    })

    run_cli("unresolved-ack", "-q", "BULKQ", "--all", "--matching", "not-applicable")

    assert q.is_acked(q.get(item["ref"])["resolution"], "unresolved", 0)


def test_bulk_ack_dry_run_changes_nothing(wt_env, run_cli):
    ref = _closed_in(wt_env, "BULKQ", "a", unresolved=["not-applicable"])

    out = run_cli("unresolved-ack", "-q", "BULKQ", "--all", "--dry-run").out

    assert "would ack 1 ticket in BULKQ" in out
    assert ref in out
    assert not wt_env.queue.is_acked(
        wt_env.queue.get(ref)["resolution"], "unresolved", 0
    )


def test_bulk_ack_undo_reverses_it(wt_env, run_cli):
    ref = _closed_in(wt_env, "BULKQ", "a", unresolved=["not-applicable"])
    run_cli("unresolved-ack", "-q", "BULKQ", "--all")

    out = run_cli("unresolved-ack", "-q", "BULKQ", "--all", "--undo").out

    assert "UNACKED: 1 ticket in BULKQ" in out
    assert not wt_env.queue.is_acked(
        wt_env.queue.get(ref)["resolution"], "unresolved", 0
    )


def test_bulk_ack_skips_closed_tickets_with_no_resolution_items(wt_env, run_cli):
    """A clean close raises ValueError on ack; it must not be selected at all."""
    _closed_in(wt_env, "BULKQ", "clean")

    out = run_cli("unresolved-ack", "-q", "BULKQ", "--all").out

    assert "no closed tickets in BULKQ with resolution items" in out


def test_bulk_ack_requires_a_queue(wt_env, run_cli):
    res = run_cli("unresolved-ack", "--all")

    assert res.code == 1
    assert "needs -q QUEUE" in res.err


def test_bulk_ack_requires_all(wt_env, run_cli):
    _closed_in(wt_env, "BULKQ", "a", unresolved=["x"])

    res = run_cli("unresolved-ack", "-q", "BULKQ", "--unresolved", "1")

    assert res.code == 1
    assert "needs --all" in res.err


def test_matching_is_rejected_with_a_ref(wt_env, run_cli):
    ref = _closed_in(wt_env, "BULKQ", "a", unresolved=["x"])

    res = run_cli("unresolved-ack", ref, "--all", "--matching", "x")

    assert res.code == 1
    assert "--matching is bulk mode" in res.err


def test_single_ref_ack_still_works_unchanged(wt_env, run_cli):
    """The ref form must not regress now that `ref` is optional."""
    ref = _closed(wt_env, unresolved=["stale thing"])

    out = run_cli("unresolved-ack", ref, "--unresolved", "1").out

    assert f"ACKED: {ref}" in out
    assert wt_env.queue.is_acked(
        wt_env.queue.get(ref)["resolution"], "unresolved", 0
    )


def test_old_ack_name_is_gone_until_gate_lands(wt_env, run_cli):
    """`wt ack` was renamed; between Task 1 and Task 5 it must not silently
    keep the old resolution-ack behavior."""
    res = run_cli("ack", "X-1", "--all")
    assert res.code != 0


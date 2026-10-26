"""Deep tickets (`--depth deep`) and `wt defer` data windows."""

from __future__ import annotations

import importlib
import json

import pytest


@pytest.fixture()
def wt(tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(tmp_path / "activity.log"))
    monkeypatch.delenv("WATCHTOWER_MACHINE", raising=False)

    import watchtower.queue as q
    import watchtower.deep as deep
    import watchtower.cli as cli

    importlib.reload(q)
    importlib.reload(deep)
    importlib.reload(cli)

    class Ns:
        pass

    ns = Ns()
    ns.store = tmp_path / "queue.json"
    ns.q, ns.deep, ns.cli = q, deep, cli
    return ns


def _expire_defer(wt, ref):
    q = wt.q
    with q._FileLock(q._lock_path()):
        data = q._load_unlocked()
        for it in data["items"]:
            if it.get("ref") == ref:
                it["deferred_until"] = "2000-01-01T00:00:00Z"
        q._save_unlocked(data)


def test_quick_is_default_and_stores_nothing(wt):
    it = wt.q.enqueue(note="tiny ui fix", project="ZZT")
    assert "depth" not in it and "budget_min" not in it
    assert wt.deep.deep_brief(it) == ""


def test_deep_ticket_stores_depth_and_default_budget(wt):
    it = wt.q.enqueue(note="slow list", project="ZZT", depth="deep")
    assert it["depth"] == "deep"
    assert it["budget_min"] == wt.q.DEEP_DEFAULT_BUDGET_MIN
    it2 = wt.q.enqueue(note="slow open", project="ZZT", depth="deep", budget_min=45)
    assert it2["budget_min"] == 45


def test_bad_depth_rejected(wt):
    with pytest.raises(ValueError):
        wt.q.enqueue(note="x", project="ZZT", depth="medium")


def test_deep_brief_carries_goal_rules_and_measure_gate(wt):
    it = wt.q.enqueue(note="slow list", project="ZZT", depth="deep",
                      accept_line="list refresh p95 < 1s", gates=["cmd:./measure.sh"])
    brief = wt.deep.deep_brief(it)
    for needle in ("DEEP TICKET", "EVIDENCE FIRST", "OPTIONS MEMO", "product option",
                   "STOP RULE", "ACCEPT LINE: list refresh p95 < 1s", "./measure.sh",
                   f"wt defer {it['ref']}", "90 minutes"):
        assert needle in brief, needle
    assert "{" not in brief and "}" not in brief  # safe inside .format() templates


def test_edit_depth_round_trip(wt):
    it = wt.q.enqueue(note="perf", project="ZZT")
    e = wt.q.update(it["ref"], depth="deep", budget_min=30)
    assert (e["depth"], e["budget_min"]) == ("deep", 30)
    e = wt.q.update(it["ref"], budget_min=60)
    assert e["budget_min"] == 60
    e = wt.q.update(it["ref"], depth="quick")
    assert "depth" not in e and "budget_min" not in e
    with pytest.raises(ValueError):
        wt.q.update(it["ref"], budget_min=30)  # a budget needs a deep ticket


def test_claim_json_includes_deep_brief(wt, capsys):
    it = wt.q.enqueue(note="slow list", project="ZZT", depth="deep")
    rc = wt.cli.main(["claim", "-q", "ZZT", it["ref"], "--worker", "w1", "--json"])
    assert rc == 0
    shown = json.loads(capsys.readouterr().out)
    assert "DEEP TICKET" in shown["deep_brief"]


def test_claim_json_quick_has_no_brief(wt, capsys):
    it = wt.q.enqueue(note="tiny", project="ZZT")
    assert wt.cli.main(["claim", "-q", "ZZT", it["ref"], "--worker", "w1", "--json"]) == 0
    assert "deep_brief" not in json.loads(capsys.readouterr().out)


def test_defer_releases_claim_and_hides_until_window_ends(wt):
    it = wt.q.enqueue(note="slow list", project="ZZT", depth="deep")
    wt.q.claim_by_ref(it["ref"], "w1")
    d = wt.q.defer(it["ref"], "w1", "+2h", "need 24h of phase timings", progress="added timers")
    assert d["status"] == "open" and d["claimed_by"] is None
    assert wt.q.deferred_now(d)
    assert wt.q.count_claimable(project="ZZT") == 0
    events = [h["event"] for h in d["history"]]
    assert "progress" in events and events[-1] == "defer"
    _expire_defer(wt, it["ref"])
    assert wt.q.count_claimable(project="ZZT") == 1


def test_resumed_deep_brief_mentions_defer(wt):
    it = wt.q.enqueue(note="slow list", project="ZZT", depth="deep")
    wt.q.claim_by_ref(it["ref"], "w1")
    wt.q.defer(it["ref"], "w1", "+1h", "collect logs")
    brief = wt.deep.deep_brief(wt.q.get(it["ref"]))
    assert "RESUMING: deferred 1x" in brief and "collect logs" in brief


@pytest.mark.parametrize("until", ["", "-1h", "+30d", "yesterday", "2000-01-01T00:00:00Z"])
def test_defer_rejects_bad_windows(wt, until):
    it = wt.q.enqueue(note="x", project="ZZT")
    with pytest.raises(ValueError):
        wt.q.defer(it["ref"], "w1", until, "data")


def test_defer_requires_reason_and_claimant(wt):
    it = wt.q.enqueue(note="x", project="ZZT")
    with pytest.raises(ValueError):
        wt.q.defer(it["ref"], "w1", "+1h", "")
    wt.q.claim_by_ref(it["ref"], "w1")
    with pytest.raises(ValueError):
        wt.q.defer(it["ref"], "w2", "+1h", "not mine")


def test_defer_refuses_closed(wt):
    it = wt.q.enqueue(note="x", project="ZZT")
    wt.q.claim_by_ref(it["ref"], "w1")
    wt.q.update_status(it["ref"], "closed", "w1")
    with pytest.raises(ValueError):
        wt.q.defer(it["ref"], "w1", "+1h", "data")


def test_cli_add_deep_and_defer(wt, capsys):
    rc = wt.cli.main(["add", "-q", "ZZT", "--title", "slow open", "--depth", "deep",
                      "--budget-min", "60"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "[deep, 60m budget]" in out
    ref = out.split()[1]
    assert wt.cli.main(["claim", "-q", "ZZT", ref, "--worker", "w1"]) == 0
    assert "DEEP TICKET" in capsys.readouterr().out
    assert wt.cli.main(["defer", ref, "--worker", "w1", "--until", "+3h",
                        "--reason", "need samples"]) == 0
    assert "DEFERRED:" in capsys.readouterr().out
    assert wt.cli.main(["find", ref]) == 0
    found = capsys.readouterr().out
    assert "depth: deep (60m budget)" in found and "deferred until" in found


def test_worker_goal_templates_point_at_deep_brief():
    from watchtower import workers
    for tpl in (workers.DRAIN_GOAL_TEMPLATE, workers.RUN_ONCE_GOAL_TEMPLATE):
        assert "deep_brief" in tpl
        assert "reason the ticket was filed" in tpl

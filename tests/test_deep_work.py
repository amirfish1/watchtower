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
                   "registered --watch NAME", "90 minutes"):
        assert needle in brief, needle
    assert "{" not in brief and "}" not in brief  # safe inside .format() templates


def test_deep_brief_supports_sustained_work_without_weakening_caps(wt):
    it = wt.q.enqueue(note="multi-day investigation", project="ZZT", depth="deep",
                      budget_min=240)
    brief = wt.deep.deep_brief(it)
    for needle in ("hours or days", "240 minutes", "not a total time limit",
                   "Continue authorized work", "resumable progress note",
                   "two attempts in a row with no measured gain",
                   "never exceed an explicit cap without approval"):
        assert needle in brief, needle
    assert "four attempts" not in brief
    assert "override the quick-fix defaults" not in brief


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


def test_defer_api_always_refuses_without_mutating_claim(wt):
    it = wt.q.enqueue(note="slow list", project="ZZT", depth="deep")
    wt.q.claim_by_ref(it["ref"], "w1")
    before = wt.q.get(it["ref"])
    with pytest.raises(ValueError, match="retired"):
        wt.q.defer(it["ref"], "w1", "+2h", "future data")
    assert wt.q.get(it["ref"]) == before


def test_cli_defer_refuses_legacy_arguments_and_bare_command(wt, capsys):
    for args in (["defer"], ["defer", "ZZT-1", "--until", "+3h", "--reason", "need samples"]):
        assert wt.cli.main(args) == 1
        assert "registered --watch NAME" in capsys.readouterr().err


@pytest.mark.parametrize("engine", ["claude", "codex", "kimi", "devin", "antigravity"])
def test_rendered_worker_goals_require_thorough_work_at_every_depth(wt, engine):
    from watchtower import workers
    goals = [workers.drain_goal("ZZT", "w1", "/repo", engine=engine),
             workers.run_once_goal("ZZT", "w1", "ZZT-1", "/repo")]
    for goal in goals:
        for needle in ("every ticket requires thorough investigation",
                       "verification of the reported outcome",
                       "Continue authorized structural work",
                       "hours or days", "deep_brief", "reason the ticket was filed"):
            assert needle in goal, needle
        for shortcut in ("fix-and-look", "a quick look is enough", "no design step",
                         "asking the owner to promote it to deep"):
            assert shortcut not in goal, shortcut


@pytest.mark.parametrize("command", ["add", "take", "edit"])
def test_depth_help_explains_quality_and_checkpoints(wt, capsys, command):
    with pytest.raises(SystemExit) as exc:
        wt.cli.main([command, "--help"])
    assert exc.value.code == 0
    help_text = " ".join(capsys.readouterr().out.split())
    assert "thorough" in help_text
    assert "hours or days" in help_text
    assert "checkpoint interval" in help_text
    assert "fix-and-look" not in help_text

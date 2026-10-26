"""Machine placement: a ticket may be pinned to a Mac or a cloud box, and
claim filtering respects the claiming machine's kind."""
from __future__ import annotations

import importlib

import pytest


@pytest.fixture()
def wt(tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(tmp_path / "activity.log"))
    monkeypatch.delenv("WATCHTOWER_PLACEMENT", raising=False)
    import watchtower.queue as q
    import watchtower.cli as cli
    importlib.reload(q)
    importlib.reload(cli)

    class Ns:
        pass

    ns = Ns()
    ns.q, ns.cli = q, cli
    return ns


def _on(monkeypatch, kind):
    monkeypatch.setenv("WATCHTOWER_PLACEMENT", kind)


def test_machine_placement_env_and_platform(wt, monkeypatch):
    monkeypatch.setattr(wt.q.sys, "platform", "darwin")
    assert wt.q.machine_placement() == "mac"
    monkeypatch.setattr(wt.q.sys, "platform", "linux")
    assert wt.q.machine_placement() == "cloud"
    _on(monkeypatch, "mac")
    assert wt.q.machine_placement() == "mac"


def test_legacy_and_any_tickets_claimable_everywhere(wt, monkeypatch):
    legacy = wt.q.enqueue(project="PL", note="plain old ticket", placement="any")
    del_ok = wt.q.enqueue(project="PL", note="another")
    items = wt.q.list_items(project="PL")
    for it in items:
        it.pop("placement", None)  # pre-existing tickets have no field
    for kind in ("mac", "cloud"):
        assert len(wt.q._claim_candidates(items, project="PL", machine=kind)) == 2
    assert legacy["placement"] == "any" and del_ok["ref"]


def test_mac_ticket_waits_for_the_mac(wt, monkeypatch):
    wt.q.enqueue(project="PL", note="build the iOS app", placement="mac")
    anyone = wt.q.enqueue(project="PL", note="write docs", placement="any")
    _on(monkeypatch, "cloud")
    got = wt.q.claim_next("w-cloud", project="PL", session_uuid="")
    assert got["ref"] == anyone["ref"]
    assert wt.q.claim_next("w-cloud2", project="PL") is None
    assert wt.q.count_claimable(project="PL") == 0
    _on(monkeypatch, "mac")
    assert wt.q.count_claimable(project="PL") == 1
    mac = wt.q.claim_next("w-mac", project="PL")
    assert mac["placement"] == "mac"


def test_cloud_ticket_skipped_by_mac(wt, monkeypatch):
    wt.q.enqueue(project="PL", note="nightly job", placement="cloud")
    _on(monkeypatch, "mac")
    assert wt.q.count_claimable(project="PL") == 0
    _on(monkeypatch, "cloud")
    assert wt.q.count_claimable(project="PL") == 1


def test_heuristic_marks_mac_only_for_a_mac_filer(wt, monkeypatch):
    monkeypatch.setattr(wt.q.sys, "platform", "darwin")
    _on(monkeypatch, "mac")
    hit = wt.q.enqueue(project="PL", note="fix Xcode build for the simulator")
    assert hit["placement"] == "mac" and hit["placement_source"] == "auto"
    miss = wt.q.enqueue(project="PL", note="refactor the parser")
    assert "placement" not in miss
    over = wt.q.enqueue(project="PL", note="Xcode doc typo", placement="any")
    assert over["placement"] == "any" and "placement_source" not in over
    # A Linux filer never strands its own tickets behind "mac".
    monkeypatch.setattr(wt.q.sys, "platform", "linux")
    _on(monkeypatch, "cloud")
    lin = wt.q.enqueue(project="PL", note="fix Xcode build")
    assert "placement" not in lin


def test_update_validates_and_clears(wt):
    it = wt.q.enqueue(project="PL", note="thing")
    assert wt.q.update(it["ref"], placement="mac")["placement"] == "mac"
    with pytest.raises(ValueError):
        wt.q.update(it["ref"], placement="moon")
    assert wt.q.update(it["ref"], placement="")["placement"] == ""


def test_cli_add_edit_ls(wt, monkeypatch, capsys):
    _on(monkeypatch, "mac")
    assert wt.cli.main(["add", "-q", "PL", "--title", "sim run", "--placement", "mac"]) == 0
    ref = wt.q.list_items(project="PL")[0]["ref"]
    capsys.readouterr()
    assert wt.cli.main(["ls", "-q", "PL"]) == 0
    assert "[mac only]" in capsys.readouterr().out
    assert wt.cli.main(["edit", ref, "--placement", "any"]) == 0
    capsys.readouterr()
    assert wt.cli.main(["ls", "-q", "PL"]) == 0
    assert "only]" not in capsys.readouterr().out


def test_explicit_claim_on_wrong_machine_released(wt, monkeypatch, capsys):
    it = wt.q.enqueue(project="PL", note="gui check", placement="mac")
    _on(monkeypatch, "cloud")
    rc = wt.cli.main(["claim", it["ref"], "-q", "PL", "--worker", "w-x"])
    assert rc == 1
    assert wt.q.get(it["ref"])["status"] == "open"

"""`wt find <ref>`: look up one ticket by ref across every queue, with no -q
needed -- the CLI surface for queue.get(), which already matches globally."""

from __future__ import annotations

import argparse
import importlib
import json

import pytest


@pytest.fixture()
def wt(tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(tmp_path / "activity.log"))

    import watchtower.queue as q
    import watchtower.cli as cli

    importlib.reload(q)
    importlib.reload(cli)

    class Ns:
        pass

    ns = Ns()
    ns.q = q
    ns.cli = cli
    return ns


def test_find_locates_ticket_without_knowing_its_queue(wt, capsys):
    item = wt.q.enqueue(project="HERMES", title="fix the thing", note="fix the thing", text="")
    rc = wt.cli.cmd_find(argparse.Namespace(ref=item["ref"], json=True))
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["ref"] == item["ref"] == "HERMES-1"


def test_find_is_case_insensitive_and_accepts_bare_number(wt, capsys):
    item = wt.q.enqueue(project="HERMES", title="x", note="x", text="")
    rc = wt.cli.cmd_find(argparse.Namespace(ref="hermes-1", json=True))
    assert rc == 0
    assert json.loads(capsys.readouterr().out)["ref"] == item["ref"]


def test_find_reports_not_found(wt, capsys):
    rc = wt.cli.cmd_find(argparse.Namespace(ref="NOPE-99", json=True))
    assert rc == 1
    assert "not found" in capsys.readouterr().err

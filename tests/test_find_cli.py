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


# Self-attribution (CCC-675): a worker that reads its own close in the
# third person ("closed by ccc-XXXX") must be able to see it was itself.

def _clear_identity_env(monkeypatch):
    for var in ("WT_WORKER", "CODEX_THREAD_ID", "CLAUDE_CODE_SESSION_ID"):
        monkeypatch.delenv(var, raising=False)


def test_find_marks_own_claim_and_close_for_matching_worker(wt, capsys, monkeypatch):
    _clear_identity_env(monkeypatch)
    item = wt.q.enqueue(project="CCC", title="x", note="x", text="")
    wt.q.claim_by_ref(item["ref"], "ccc-worker-a")
    wt.q.close(item["ref"], "ccc-worker-a", resolution={"summary": "done"})
    rc = wt.cli.cmd_find(
        argparse.Namespace(ref=item["ref"], json=True, worker="ccc-worker-a")
    )
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["claimed_by_you"] is True
    assert out["closed_by_you"] is True
    close_events = [e for e in out["timeline"] if e.get("event") == "close"]
    assert close_events and all(e.get("you") for e in close_events)


def test_find_marks_nothing_for_a_different_worker(wt, capsys, monkeypatch):
    _clear_identity_env(monkeypatch)
    item = wt.q.enqueue(project="CCC", title="x", note="x", text="")
    wt.q.claim_by_ref(item["ref"], "ccc-worker-a")
    wt.q.close(item["ref"], "ccc-worker-a", resolution={"summary": "done"})
    rc = wt.cli.cmd_find(
        argparse.Namespace(ref=item["ref"], json=True, worker="ccc-worker-b")
    )
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert "claimed_by_you" not in out
    assert "closed_by_you" not in out
    assert not any(e.get("you") for e in out["timeline"])


def test_find_marks_self_via_harness_session_env_without_worker_flag(
    wt, capsys, monkeypatch
):
    """Hosted workers run `wt find <ref> --json` with no --worker at all;
    the session id recorded at claim time must still establish self."""
    _clear_identity_env(monkeypatch)
    sid = "11111111-2222-3333-4444-555555555555"
    item = wt.q.enqueue(project="CCC", title="x", note="x", text="")
    wt.q.claim_by_ref(item["ref"], "ccc-worker-a", session_uuid=sid)
    wt.q.close(item["ref"], "ccc-worker-a", resolution={"summary": "done"})
    monkeypatch.setenv("CODEX_THREAD_ID", sid)
    rc = wt.cli.cmd_find(argparse.Namespace(ref=item["ref"], json=True, worker=""))
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["claimed_by_you"] is True
    # Close events record only the worker id; a close under the id you
    # claimed with is inferred to be yours.
    assert out["closed_by_you"] is True


def test_find_human_output_appends_you_markers(wt, capsys, monkeypatch):
    _clear_identity_env(monkeypatch)
    item = wt.q.enqueue(project="CCC", title="x", note="x", text="")
    wt.q.claim_by_ref(item["ref"], "ccc-worker-a")
    wt.q.close(item["ref"], "ccc-worker-a", resolution={"summary": "done"})
    rc = wt.cli.cmd_find(
        argparse.Namespace(ref=item["ref"], json=False, worker="ccc-worker-a")
    )
    assert rc == 0
    text = capsys.readouterr().out
    assert "closed_by: ccc-worker-a (you)" in text

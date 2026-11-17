"""Bounded fixture supervision and process cleanup never reach peer workers."""
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def runner():
    path = Path(__file__).resolve().parents[1] / "scripts" / "verify_codex_sandbox.py"
    spec = importlib.util.spec_from_file_location("sandbox_acceptance_runner", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_supervision_requires_assessment_completion_and_scopes_ticks(runner, monkeypatch):
    states = iter([
        {"status": "in_review", "gate_pending": "verify"},
        {"status": "closed", "assessment": {"status": "running"}},
        {"status": "closed", "assessment": {"status": "done"}},
    ])
    calls = []
    monkeypatch.setattr(runner.time, "sleep", lambda _: None)
    item, transitions = runner.settle_stages(
        SimpleNamespace(get=lambda ref: next(states)),
        SimpleNamespace(reconcile_stages=lambda **kw: calls.append(kw)), "FIXTURE-1", 30)
    assert item["assessment"]["status"] == "done"
    assert calls == [{"only_ref": "FIXTURE-1"}] * 2
    assert len(transitions) == 3


@pytest.mark.parametrize("item", [
    {"status": "open"}, {"status": "awaiting_answer"},
    {"status": "in_review", "needs_input": True},
    {"status": "closed", "assessment": {"status": "failed"}},
])
def test_failed_gates_and_blocks_do_not_launch_more_stages(runner, item):
    def unexpected(**kw):
        pytest.fail("a failed or blocked fixture must stop supervision")
    result, _ = runner.settle_stages(SimpleNamespace(get=lambda _: item),
                                     SimpleNamespace(reconcile_stages=unexpected), "F-1", 30)
    assert result is item


def test_stalled_stage_stops_at_deadline(runner, monkeypatch):
    clock = [0]
    monkeypatch.setattr(runner.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(runner.time, "sleep", lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    calls = []
    item, _ = runner.settle_stages(
        SimpleNamespace(get=lambda _: {"status": "in_review"}),
        SimpleNamespace(reconcile_stages=lambda **kw: calls.append(kw)), "F-1", 30)
    assert item["status"] == "in_review" and clock[0] == 30
    assert len(calls) == 30


def test_cleanup_rejects_peer_builder_and_reused_process_ids(runner):
    own = dict(ref="F-1", ticket_queue="FIXTURE", stage="verifier", pid=21,
               pid_started="old", worker_id="fixture-verifier")
    rows = [dict(own, ref="F-2", pid=22), dict(own, ticket_queue="LIVE", pid=23),
            dict(own, stage="", pid=24), dict(own, pid=25), own]
    killed = []
    workers = SimpleNamespace(list_workers=lambda **kw: rows,
                              _pid_start_token=lambda pid: "reused" if pid == 25 else "old",
                              _pid_alive=lambda pid: True)
    assert runner.stop_fixture_stages(workers, SimpleNamespace(_kill=killed.append),
                                      "F-1", "FIXTURE") == ["fixture-verifier"]
    assert killed == [21]

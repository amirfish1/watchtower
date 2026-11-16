"""The review replay must not inherit a fleet's live persistence paths."""
import hashlib
import json
from pathlib import Path
import runpy
import subprocess
import sys


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "repro_sbq_group.py"


def test_replay_preserves_inherited_live_store_config_events_and_workers(tmp_path):
    harness = runpy.run_path(str(SCRIPT))
    live = tmp_path / "live-fleet"
    live.mkdir()
    env = harness["isolated_env"](live)
    source = str(SCRIPT.parents[1])
    seed = """
import sys
sys.path.insert(0, sys.argv[1])
from watchtower import queue, config
config.set_repo_path('REAL', '/real/workspace')
queue.enqueue(project='REAL', title='Preserve this real ticket', note='real work', pre_ack=True)
"""
    subprocess.run([sys.executable, "-I", "-c", seed, source], env=env, check=True)
    (live / "workers.json").write_text('{"real-worker": {"pid": 123}}')
    env.update(WATCHTOWER_DIR=str(live), WT_WORKER_ID="real-worker",
               WATCHTOWER_SESSION_ID="11111111-1111-1111-1111-111111111111")

    def snapshot():
        return {str(p.relative_to(live)): hashlib.sha256(p.read_bytes()).hexdigest()
                for p in live.rglob("*") if p.is_file()}

    before = snapshot()
    result = subprocess.run([sys.executable, str(SCRIPT)], env=env,
                            capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["outcome"] == [["SBQ-1", "commit_missing"]]
    assert report["tickets"] == 3
    assert not Path(report["isolated_store"]).is_relative_to(live)
    assert not Path(report["isolated_store"]).parent.exists()  # disposable
    assert snapshot() == before  # includes DB, event log, config audit & workers


def test_replay_from_unrelated_cwd(tmp_path):
    result = subprocess.run([sys.executable, str(SCRIPT)], cwd=tmp_path,
                            capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["tickets"] == 3


def test_retired_group_is_not_reintegrated_and_history_survives(wt_env):
    import argparse
    from watchtower import queue, config, cli, owner_view, liveness

    config.set_repo_path("SBQ", "/synthetic/missing-workspace")
    parent = queue.enqueue(project="SBQ", title="fixture", note="test", group_parent=True,
                           pre_ack=True, gates=["cmd:echo hi"])
    for i in (1, 2):
        queue.enqueue(project="SBQ", title=f"child{i}", note="test", group=parent["ref"],
                      pre_ack=True)
    queue.group_seal(parent["ref"])
    with queue._FileLock(queue._lock_path()):
        data = queue._load_unlocked()
        for it in data["items"]:
            if it["ref"] == parent["ref"]:
                it["plan"] = {"status": "accepted"}
                it["needs_input"] = True
                it["block_question"] = "missing synthetic commit"
            else:
                it["status"] = "closed"
                it["resolution"] = {"commit": "deadbeef" * 5, "summary": "synthetic fixture"}
        queue._save_unlocked(data)
    before = queue.get(parent["ref"])
    assert owner_view.build(queue.list_items())["blocked"] == 1
    assert cli.cmd_queue_archive(argparse.Namespace(name="SBQ", apply=True, force=True, json=True)) == 0
    assert queue.get(parent["ref"]) == before  # no gate edits or manufactured close
    assert owner_view.build(queue.list_items())["blocked"] == 0
    assert queue.list_blocked() == []  # hourly queue care consumes this surface
    assert not any(r["ref"] == parent["ref"] for r in liveness.report())
    assert liveness.report(ref=parent["ref"])  # explicit inspection remains available
    assert liveness.sweep(only_ref=parent["ref"]) == []
    # Even a dependency auto-answer must not restart the retired integration.
    queue.answer(parent["ref"], "Fixture retired, no commit to recover", by_kind="system")
    assert queue.group_sweep(only_ref=parent["ref"]) == []
    assert queue.get(parent["ref"])["status"] == "open"
    assert cli.cmd_queue_unarchive(argparse.Namespace(name="SBQ", json=True)) == 0
    assert not config.ticket_is_archived(queue.get(parent["ref"]))
    assert queue._group_due_unlocked(queue.list_items())  # original gates still owed

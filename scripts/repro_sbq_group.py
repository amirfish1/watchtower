#!/usr/bin/env python3
"""Replay WT-72's synthetic group in disposable storage, never the live fleet.

Run with python3 scripts/repro_sbq_group.py. No model, daemon or network needed.
This checks fixture isolation; it is not sandbox security acceptance evidence.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile


def isolated_env(root: Path) -> dict[str, str]:
    # WATCHTOWER_DIR is NOT a runtime-storage override. Discard inherited
    # fleet paths before any watchtower import, including explicit overrides
    # which would take precedence over WATCHTOWER_DATA_DIR.
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("WATCHTOWER_", "WT_"))}
    paths = {
        "DATA_DIR": "data", "STORE": "queue.json",
        "CONFIG_FILE": "queue-config.json", "ACTIVITY_LOG": "activity.log",
        "WORKERS_FILE": "workers.json", "WORKER_SESSIONS_FILE": "sessions.json",
        "WORKER_IDS_FILE": "worker-ids.json", "STOP_SIGNALS_DIR": "stops",
        "LAUNCH_FAILURES_FILE": "launch-failures.json",
        "SESSION_ORIGINS_FILE": "origins.json", "AGENTS_FILE": "agents.json",
        "OUTBOX_FILE": "outbox.json", "DELIVERY_LEDGER_FILE": "deliveries.json",
        "CODEX_THREAD_REGISTRY": "threads.json", "DAEMON_PID": "daemon.pid",
        "CCC_SPAWN_DEFAULTS_FILE": "spawn-defaults.json",
        "MODEL_POLICY_FILE": "model-policy.json",
    }
    env.update({"WATCHTOWER_" + key: str(root / value) for key, value in paths.items()})
    env.update(WT_NO_AUTOSTART="1", WT_NO_SELF_UPDATE="1")
    # Existing empty destinations prevent importing legacy live data.
    (root / "queue.json").write_text(json.dumps({"counter": 0, "items": []}))
    (root / "queue-config.json").write_text("{}")
    return env


def replay(root: Path) -> dict:
    from unittest.mock import patch
    from watchtower import config, queue

    assert queue._resolve_store_path().resolve() == root / "queue.json"
    assert config.config_path().resolve() == root / "queue-config.json"
    assert queue.list_items() == []
    # Real config shape: queue names at the top level, not {"queues": ...}.
    config.set_repo_path("SBQ", str(root / "ws"))
    # Only external delivery and daemon wakeups are suppressed. Queue, config,
    # history and activity persistence use their real implementations.
    with patch.object(queue, "_notify_ticket_event"), patch.object(queue, "_group_wake"):
        parent = queue.enqueue(project="SBQ", title="Parent", note="test",
                               repo_path=str(root / "ws"), group_parent=True,
                               pre_ack=True, gates=["cmd:echo hi"])
        children = [queue.enqueue(project="SBQ", title=f"Child{i}", note="test",
                                  group=parent["ref"], pre_ack=True) for i in (1, 2)]
        queue.group_seal(parent["ref"])
        # This is the original synthetic setup, not a real accepted plan.
        with queue._FileLock(queue._lock_path()):
            data = queue._load_unlocked()
            next(it for it in data["items"] if it["ref"] == parent["ref"])["plan"] = {
                "status": "accepted"}
            queue._save_unlocked(data)
        for i, child in enumerate(children, 1):
            worker = f"sb-w{i}"
            uid = f"11111111-1111-1111-1111-11111111111{i}"
            queue.claim_by_ref(child["ref"], worker, uid)
            queue.close(child["ref"], worker, resolution={"summary": "synthetic fixture",
                        "commit": "deadbeef" * 5, "no_code": False}, session_uuid=uid)
        # No Git command is needed to prove the persistence regression. Return
        # the same missing-proof result without inspecting any host checkout.
        from watchtower import integration_git
        with patch.object(integration_git, "containing_sha",
                          return_value=("", "", "commit_missing: synthetic fixture")):
            outcome = queue.group_sweep(only_ref=parent["ref"])
    assert outcome == [("SBQ-1", "commit_missing")], outcome
    assert queue.get("SBQ-1")["needs_input"]
    assert len(queue.list_items(project="SBQ")) == 3
    assert config.repo_path("SBQ") == str(root / "ws")
    return {"outcome": outcome, "tickets": 3, "isolated_store": str(queue._resolve_store_path()),
            "isolated_config": str(config.config_path())}


def main() -> int:
    source = Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory(prefix="wt-sbq-isolated-") as name:
        root = Path(name).resolve()
        env = isolated_env(root)
        code = ("import runpy,sys; from pathlib import Path; "
                "sys.path.insert(0,sys.argv[1]); "
                "m=runpy.run_path(sys.argv[1]+'/scripts/repro_sbq_group.py'); "
                "print(m['json'].dumps(m['replay'](Path(sys.argv[2]))))")
        return subprocess.run([sys.executable, "-I", "-c", code, str(source), str(root)],
                              env=env, cwd=root, check=False, timeout=60).returncode


if __name__ == "__main__":
    raise SystemExit(main())

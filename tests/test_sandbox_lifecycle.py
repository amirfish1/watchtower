"""Sandbox restrictions must survive queue, claim and lifecycle changes."""
from types import SimpleNamespace
import uuid

import pytest

from watchtower import sandbox_bridge


def claimed(w):
    item = w.queue.enqueue(project="SB", note="sandbox work")
    return w.queue.claim_by_ref(item["ref"], "sb-worker")


@pytest.mark.parametrize("operation", ["block", "comment", "close"])
@pytest.mark.parametrize("change", ["generation", "owner", "store"])
def test_mutation_refuses_changed_claim_or_source(wt_env, monkeypatch, operation, change):
    w = wt_env
    item = claimed(w)
    fence = sandbox_bridge._fence(item)
    from watchtower import store_identity
    identity = store_identity.initialize()["store_id"]
    if change == "store":
        monkeypatch.setattr(store_identity, "read_unlocked", lambda base: str(uuid.uuid4()))
    else:
        with w.queue._FileLock(w.queue._lock_path()):
            data = w.queue._load_unlocked()
            current = next(it for it in data["items"] if it["ref"] == item["ref"])
            if change == "generation":
                current["last_answer_gen"] = 20
            else:
                current["claimed_by"] = "replacement"
            w.queue._save_unlocked(data)
    before = w.queue.get(item["ref"])
    kw = {"sandbox_expect": fence, "sandbox_store_id": identity}
    with pytest.raises(ValueError, match="sandbox"):
        if operation == "block":
            w.queue.block(item["ref"], "sb-worker", question="wait", origin="worker", **kw)
        elif operation == "comment":
            w.queue.comment(item["ref"], "stale progress", by="worker", session_id="sb-worker", **kw)
        else:
            w.queue.close(item["ref"], "sb-worker", resolution={"summary": "fixed", "no_code": True}, **kw)
    assert w.queue.get(item["ref"]) == before


def test_progress_then_block_uses_current_fence_and_preserves_attribution(wt_env):
    w = wt_env
    item = claimed(w)
    kw = {"sandbox_expect": sandbox_bridge._fence(item)}
    commented = w.queue.comment(item["ref"], "verified progress", by="worker", session_id="sb-worker", **kw)
    assert commented["history"][-1]["by"]["worker"] == "sb-worker"
    blocked = w.queue.block(item["ref"], "sb-worker", question="dependency is unavailable",
                            origin="worker", wait_on="dependency", wait_ref="OPS-1", **kw)
    assert blocked["needs_input"]
    assert blocked["wait"]["on"] == "dependency"


def test_parent_command_gate_cannot_run_on_host_after_queue_policy_changes(wt_env, monkeypatch):
    w = wt_env
    child = claimed(w)
    monkeypatch.setattr(sandbox_bridge, "restricted_session",
                        lambda session_id="", worker_id="": worker_id == "sb-worker")
    parent = {"project": "SB", "ref": "SB-99", "gates": ["cmd:touch escaped"],
              "group": {"role": "parent", "members": [child["ref"]]}}
    monkeypatch.setattr(w.queue, "_run_cmd_gate", lambda *a, **kw: pytest.fail("host gate execution"))
    results, stages = w.queue.evaluate_gates(parent)
    assert not results[0]["passed"] and results[0]["setup_failed"]
    assert "refusing host execution" in results[0]["output_tail"]
    assert stages == []


def test_restricted_interactive_discussion_refuses_even_printed_unrestricted_resume(wt_env, monkeypatch):
    w = wt_env
    sid = str(uuid.uuid4())
    item = {"ref": "SB-1", "claimed_by": "sb-worker", "claimed_session_id": sid}
    monkeypatch.setattr(w.cli.q, "get", lambda ref: item)
    monkeypatch.setattr(w.cli.q, "answer_session", lambda it: sid)
    monkeypatch.setattr(sandbox_bridge, "restricted_session", lambda *a, **k: True)
    monkeypatch.setattr(w.cli.os, "system", lambda *a: pytest.fail("unsafe interactive resume"))
    assert w.cli.cmd_discuss(SimpleNamespace(ref="SB-1", engine="codex", print=True)) == 1


def test_assessor_launch_carries_stage_policy(wt_env, monkeypatch):
    w = wt_env
    monkeypatch.setattr(w.workers, "assessment_repo", lambda item: str(w.tmp))
    monkeypatch.setattr(w.queue, "assessor_target", lambda item: {
        "engine": "codex", "model": "test", "blocked": False, "source": "queue"})
    monkeypatch.setattr(w.queue, "assessment_reserve", lambda *a, **k: "token")
    monkeypatch.setattr(w.queue, "assessment_set_running", lambda *a, **k: None)
    calls = []
    monkeypatch.setattr(w.workers, "spawn_adhoc", lambda *a, **kw: calls.append(kw) or {"worker_id": "assessor"})
    result = w.workers.start_assessment({"ref": "SB-1", "project": "SB"})
    assert result["status"] == "spawned"
    assert calls[0]["stage"] == "assessor"
    assert calls[0]["ticket_ref"] == "SB-1"
    assert calls[0]["ticket_queue"] == "SB"


def test_recorded_and_backfilled_sessions_stay_restricted_after_prune(wt_env, monkeypatch):
    import os
    w = wt_env
    repo = w.tmp / "repo"
    repo.mkdir()
    w.queue.enqueue(project="SB", note="initialize source")
    w.config.set_codex_sandbox("SB", "workspace-write", str(w.tmp / "isolated"))
    for worker, sid, backfill in (("sb-direct", str(uuid.uuid4()), False),
                                  ("sb-backfill", str(uuid.uuid4()), True)):
        sandbox_bridge.register("SB", worker, str(repo))
        w.workers.record_worker(os.getpid(), "SB", "codex", worker, str(repo),
                                str(w.tmp / "worker.log"), session_id="" if backfill else sid)
        if backfill:
            monkeypatch.setattr(w.workers, "resolve_session_id_from_log", lambda path: sid)
            monkeypatch.setattr(w.workers, "_pid_alive", lambda pid: False)
            w.workers.list_workers(prune=True)
        assert sandbox_bridge._tombstoned("s", sid)
        w.config.set_codex_sandbox("SB", "")
        assert sandbox_bridge.restricted_session(sid)
        w.config.set_codex_sandbox("SB", "workspace-write", str(w.tmp / "isolated"))


def test_bridge_commit_proof_refuses_unrecorded_or_mismatched_roots(wt_env, monkeypatch):
    from watchtower import sandbox_policy
    scope = {"worker": "sb-worker", "repo": str(wt_env.tmp), "store": str(wt_env.tmp / "store")}
    monkeypatch.setattr(sandbox_policy, "verify_worker_commit",
                        lambda *a: pytest.fail("proof must not run before scope validation"))
    for record in (None, {"repo": scope["repo"], "store": "/other-store"},
                   {"repo": "/other-repo", "store": scope["store"]}):
        monkeypatch.setattr(sandbox_policy, "launch_record", lambda worker: record)
        with pytest.raises(ValueError, match="mismatched sandbox launch record"):
            sandbox_bridge._commit_proof(scope, "f" * 40)

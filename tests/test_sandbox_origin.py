"""WT-61 F2: sandbox origin is sticky on the ticket, not on today's claimant.

Every test drives a real consumer (command gates, adoption close, stage
launch, assessment start, group integration), not just the predicate. The
trusted reclaim and "policy turned off" are the review's exact exploit.
"""
from __future__ import annotations

import os
import stat

import pytest

from tests.test_sandbox_bridge import claim, env  # noqa: F401
from tests.test_sandbox_stage_transport import change  # noqa: F401
from watchtower import sandbox_bridge as bridge, sandbox_stages

SHA = "ab" * 20


def _trusted_reclaim(w, ref, worker="integ-1"):
    """Policy off, then an unrestricted live integration worker holds the ticket."""
    w.config.set_codex_sandbox("SB", "full-access")
    w.workers.record_worker(os.getpid(), "OTHER", "claude", worker, str(w.tmp), str(w.tmp / "i.log"))
    return change(w, ref, status="in_progress", claimed_by=worker, claimed_session_id="",
                  needs_input=False, parked=None, wait=None)


def _fake_gates(w, monkeypatch):
    seen = []

    def run(item, command, commit="", timeout=None):
        seen.append((command, commit))
        return {"gate": "cmd:" + command, "passed": True, "exit_code": 0, "output_tail": "",
                "commit": commit, "sandboxed": True}
    monkeypatch.setattr(sandbox_stages, "run_cmd_gate", run)
    monkeypatch.setattr(sandbox_stages, "pin_source",
                        lambda item, commit="": {"commit": commit or SHA, "head": "", "common": "/c"})
    monkeypatch.setattr(sandbox_stages, "source_unchanged", lambda pin: "")
    monkeypatch.setattr(w.queue, "_run_cmd_gate",
                        lambda *a, **k: pytest.fail("a sandbox-origin gate ran on the host"))
    return seen


def test_first_bridge_claim_stamps_immutable_origin(env):
    w = env
    item = claim(w)
    origin = w.queue.get(item["ref"])["sandbox_origin"]
    assert origin["worker"] == w.worker and origin["at"]
    # A later claimant never rewrites or clears the stamp.
    _trusted_reclaim(w, item["ref"])
    w.queue._stamp_sandbox_origin_unlocked(w.queue.get(item["ref"]), "claim_by_ref", "later")
    assert w.queue.get(item["ref"])["sandbox_origin"] == origin


def test_exploit_trusted_reclaim_and_policy_off_still_gates_in_sandbox(env, monkeypatch):
    """The review's F2 repro, continued through the real close/gate path."""
    w = env
    item = claim(w)
    ref = item["ref"]
    change(w, ref, gates=["cmd:true"])
    seen = _fake_gates(w, monkeypatch)
    _trusted_reclaim(w, ref)
    assert w.queue._sandbox_restricted(w.queue.get(ref))
    done = w.queue.close(ref, "integ-1", resolution={"summary": "adopted", "commit": SHA})
    assert done["status"] == "closed"
    assert seen == [("true", SHA)]
    assert done["gate_results"][0]["sandboxed"] is True


def test_legacy_ticket_without_stamp_is_restricted_by_history_tombstone(env, monkeypatch):
    w = env
    item = claim(w)
    ref = item["ref"]
    change(w, ref, gates=["cmd:true"])
    with w.queue._FileLock(w.queue._lock_path()):
        data = w.queue._load_unlocked()
        next(i for i in data["items"] if i["ref"] == ref).pop("sandbox_origin")
        w.queue._save_unlocked(data)
    assert "sandbox_origin" not in w.queue.get(ref)
    seen = _fake_gates(w, monkeypatch)
    _trusted_reclaim(w, ref)
    assert w.queue._sandbox_restricted(w.queue.get(ref))      # history names sb-w1
    w.queue.close(ref, "integ-1", resolution={"summary": "adopted", "commit": SHA})
    assert seen, "legacy-origin gate did not run in the sandbox"


def test_session_tombstone_in_history_is_enough(env):
    w = env
    item = claim(w)
    ref = item["ref"]
    bridge._tombstone("s", "sess-legacy-1")
    with w.queue._FileLock(w.queue._lock_path()):
        data = w.queue._load_unlocked()
        it = next(i for i in data["items"] if i["ref"] == ref)
        it.pop("sandbox_origin")
        it["history"] = [{"event": "claim", "by": {"kind": "worker", "session_id": "sess-legacy-1"}}]
        it["claimed_by"] = "someone-else"
        w.queue._save_unlocked(data)
    w.config.set_codex_sandbox("SB", "full-access")
    assert w.queue._sandbox_restricted(w.queue.get(ref))


def test_full_access_ticket_is_preserved(env):
    w = env
    w.config.set_codex_sandbox("SB", "full-access")
    w.workers.record_worker(os.getpid(), "OTHER", "claude", "plain-1", str(w.tmp), str(w.tmp / "p.log"))
    item = w.queue.enqueue(project="SB", note="ordinary", repo_path=str(w.repo))
    w.queue.claim_by_ref(item["ref"], "plain-1")
    cur = w.queue.get(item["ref"])
    assert "sandbox_origin" not in cur
    assert not w.queue._sandbox_restricted(cur)
    # Reclaim by another unrestricted worker keeps it that way.
    change(w, item["ref"], claimed_by="plain-2")
    assert not w.queue._sandbox_restricted(w.queue.get(item["ref"]))


def test_group_parent_stays_restricted_after_child_is_reclaimed(env, monkeypatch):
    w = env
    kids = [w.queue.enqueue(project="SB", note=f"m{i}", repo_path=str(w.repo))["ref"] for i in range(2)]
    parent = w.queue.enqueue(project="SB", note="group", repo_path=str(w.repo), group_parent=True,
                             children=kids)["ref"]
    w.queue.plan_start(parent)
    mv = w.queue.group_mv(w.queue.get(parent))
    text = "## Shared\nshared\n\n" + "\n\n".join(f"## Section: {k}\nbuild {k}" for k in kids)
    w.queue.plan_submit(parent, text, by="planner", expect_mv=mv)
    w.queue.plan_verdict(parent, True, "sound", by="reviewer", expect_mv=mv)
    w.queue.claim_by_ref(kids[0], w.worker)
    assert w.queue.get(kids[0])["sandbox_origin"]
    _trusted_reclaim(w, kids[0])
    assert w.queue._sandbox_restricted(w.queue.get(parent))
    assert w.queue._group_sandbox_origin(w.queue.get(parent))


def test_assessment_start_for_reclaimed_origin_ticket_uses_isolated_stages(env, monkeypatch):
    from watchtower import stages
    w = env
    item = claim(w)
    ref = item["ref"]
    _trusted_reclaim(w, ref)
    calls = []
    monkeypatch.setattr(w.workers, "assessment_repo", lambda it: str(w.repo))
    monkeypatch.setattr(w.queue, "assessor_target", lambda it: {"engine": "codex"})
    monkeypatch.setattr(stages, "request_assessment", lambda r, force=False: calls.append(("request", r)))
    monkeypatch.setattr(stages, "reconcile_stages", lambda only_ref="": calls.append(("reconcile", only_ref)) or [])
    result = w.workers.start_assessment(w.queue.get(ref))
    assert calls == [("request", ref), ("reconcile", ref)], (calls, result)


def test_stage_launch_for_reclaimed_origin_ticket_never_uses_host_spawn(env, monkeypatch):
    from watchtower import sandbox_stage_transport
    w = env
    item = claim(w)
    ref = item["ref"]
    _trusted_reclaim(w, ref)
    launched = []
    monkeypatch.setattr(sandbox_stage_transport, "launch",
                        lambda it, stage, engine, model, dry_run=False: launched.append(stage) or {"sandboxed": True})
    out = w.workers.spawn_adhoc("verify", "codex", stage="verifier", ticket_ref=ref,
                                ticket_queue="SB", repo_path=str(w.repo))
    assert launched == ["verifier"] and out == {"sandboxed": True}


def test_unreadable_restriction_ledger_fails_closed(env, monkeypatch):
    w = env
    item = claim(w)
    ref = item["ref"]
    with w.queue._FileLock(w.queue._lock_path()):
        data = w.queue._load_unlocked()
        next(i for i in data["items"] if i["ref"] == ref).pop("sandbox_origin")
        w.queue._save_unlocked(data)
    _trusted_reclaim(w, ref)
    # Make the tombstone ledger unreadable: a legacy ticket must be restricted.
    monkeypatch.setattr(bridge, "_tombstoned", lambda kind, value: (_ for _ in ()).throw(OSError("eio")))
    assert w.queue._sandbox_restricted(w.queue.get(ref)) is True


def test_replaced_ledger_directory_fails_closed(env):
    w = env
    plain = w.queue.enqueue(project="SB", note="n", repo_path=str(w.repo))
    w.config.set_codex_sandbox("SB", "full-access")
    cur = w.queue.get(plain["ref"])
    assert not w.queue._sandbox_restricted(cur)
    tomb = bridge._tomb_dir()
    moved = tomb.with_name("tombstones.moved")
    tomb.rename(moved)
    tomb.write_text("not a directory")
    try:
        assert w.queue._sandbox_restricted(cur) is True
    finally:
        tomb.unlink()
        moved.rename(tomb)


def test_unreadable_permissions_fail_closed(env):
    w = env
    plain = w.queue.enqueue(project="SB", note="n", repo_path=str(w.repo))
    w.config.set_codex_sandbox("SB", "full-access")
    tomb = bridge._tomb_dir()
    os.chmod(tomb, 0)
    try:
        if os.access(tomb, os.R_OK | os.X_OK):
            pytest.skip("permissions are not enforced for this user")
        cur = w.queue.get(plain["ref"])
        cur = dict(cur, claimed_by="x")
        assert w.queue._sandbox_restricted(cur) is True
    finally:
        os.chmod(tomb, stat.S_IRWXU)

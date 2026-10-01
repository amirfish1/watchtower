"""Stage sessions owned by the reconciler (WT-24).

Real-process tests: ``build_adhoc_command`` is routed to a stdlib sleeper, run
through the real ``spawn_adhoc`` + ``_exitwrap``, killed with ``os.killpg``,
and supervised by ``stages.reconcile_stages()`` (the daemon's pass).
"""

from __future__ import annotations

import ast
import datetime as dt
import importlib
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

SLEEP = "import time; time.sleep(120)"
EXIT0 = "pass"
EXIT1 = "import sys; sys.exit(1)"
SELF_KILL = "import os, signal; os.kill(os.getpid(), signal.SIGKILL)"
AUTH_FAIL = "import sys; print('Not logged in - please run /login'); sys.exit(1)"
ENV_DUMP = ("import json, os, time; "
            "open(os.environ['STAGE_ENV_OUT'], 'w').write(json.dumps("
            "{k: v for k, v in os.environ.items() if k.startswith('WT_')})); "
            "time.sleep(120)")


@pytest.fixture()
def stg(tmp_path, monkeypatch):
    for var, name in (("WATCHTOWER_STORE", "queue.json"),
                      ("WATCHTOWER_WORKERS_FILE", "workers.json"),
                      ("WATCHTOWER_CONFIG_FILE", "config.json"),
                      ("WATCHTOWER_WORKER_SESSIONS_FILE", "worker-sessions.json"),
                      ("WATCHTOWER_WORKER_IDS_FILE", "worker-ids.json"),
                      ("WATCHTOWER_LAUNCH_FAILURES_FILE", "launch-failures.json"),
                      ("WATCHTOWER_ACTIVITY_LOG", "activity.log"),
                      ("WATCHTOWER_DAEMON_PID", "daemon.pid"),
                      ("WATCHTOWER_CCC_SPAWN_DEFAULTS_FILE", "no-ccc-spawn-defaults.json"),
                      ("WATCHTOWER_CODEX_THREAD_REGISTRY", "codex-registry.json"),
                      ("STAGE_ENV_OUT", "env.json")):
        monkeypatch.setenv(var, str(tmp_path / name))
    monkeypatch.setenv("WATCHTOWER_DELEGATE_URL", "off")
    monkeypatch.setenv("WATCHTOWER_STAGE_LAUNCH_GRACE_S", "0")
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude-home"))
    monkeypatch.setenv("WATCHTOWER_SPAWN_STAGGER_S", "0")
    import watchtower.config as config
    import watchtower.queue as q
    import watchtower.workers as workers
    import watchtower.stages as stages
    for mod in (config, q, workers):
        importlib.reload(mod)
    import watchtower.cli as cli
    importlib.reload(cli)
    monkeypatch.setattr(config, "_REGISTRY_FILE", tmp_path / "no-registry.json")
    monkeypatch.setattr(q, "_notify_review", lambda *a, **k: None)
    monkeypatch.setattr(q, "_notify_ticket_event", lambda *a, **k: None)
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    config.set_repo_path("ST", str(repo))
    config.set_post_fix_assessment("ST", True)
    (tmp_path / "daemon.pid").write_text(str(os.getpid()))

    ns = SimpleNamespace(q=q, config=config, workers=workers, stages=stages, cli=cli,
                         tmp=tmp_path, repo=repo, script=[SLEEP], spawned=[])

    def build(engine, prompt, **kw):
        ns.spawned.append({"engine": engine, "prompt": prompt, **kw})
        script = ns.script[min(len(ns.spawned) - 1, len(ns.script) - 1)]
        return [sys.executable, "-c", script]

    monkeypatch.setattr(workers, "build_adhoc_command", build)
    ns.procs = []
    yield ns
    # Reap anything still running.
    for w in workers._load().get("workers", []):
        try:
            os.killpg(int(w["pid"]), signal.SIGKILL)
        except (OSError, ValueError, KeyError):
            pass


# --------------------------------------------------------------------- helpers

def _bug(ns, **kw):
    it = ns.q.enqueue(project="ST", title="boom", note="boom", item_type="bug", **kw)
    ns.q.claim_next("w1", project="ST")
    return it["ref"]


def _verify_ticket(ns):
    ref = _bug(ns, gates=["verify"])
    ns.q.close(ref, session_id="w1", resolution={"summary": "fixed", "no_code": True})
    assert ns.q.get(ref)["status"] == "in_review"
    return ref


def _plan_ticket(ns):
    it = ns.q.enqueue(project="ST", title="plan me", note="plan me", gates=["plan"])
    return it["ref"]


def _assess_ticket(ns):
    ref = _bug(ns)
    ns.q.close(ref, session_id="w1", resolution={"summary": "fixed", "no_code": True})
    assert ns.q.get(ref)["assessment"]["status"] == "due"
    return ref


def _ss(ns, ref):
    return ns.q.get(ref).get("stage_session") or {}


def _tick(ns, ref=""):
    return ns.stages.reconcile_stages(only_ref=ref)


def _wait(cond, timeout=10.0):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.05)
    return False


def _kill(ns, ref, sig=signal.SIGTERM):
    """Kill the ticket's current stage session and wait for its exit file."""
    ss = _ss(ns, ref)
    pid, exit_file = int(ss["pid"]), ss["exit_file"]
    # let the wrapper start its child and install its signal handlers first
    assert _wait(lambda: subprocess.run(["pgrep", "-P", str(pid)],
                                        capture_output=True).returncode == 0)
    time.sleep(0.2)
    os.kill(pid, sig)       # the wrapper forwards it to the child and records the exit

    def done():
        try:
            return "ended_at" in json.loads(Path(exit_file).read_text())
        except (OSError, ValueError):
            return False
    assert _wait(done), "exit file never got ended_at"
    assert _wait(lambda: not ns.workers._pid_alive(pid))


def _activity(ns, verb, ref):
    path = ns.tmp / "activity.log"
    lines = path.read_text().splitlines() if path.exists() else []
    return [ln for ln in lines if f" {verb} " in ln and ref in ln]


def _age_spawn(ns, ref, seconds=120):
    old = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(seconds=seconds)
           ).strftime("%Y-%m-%dT%H:%M:%SZ")
    ns.q.stage_session_update(ref, lambda it, ss: ss.update(spawned_at=old))


# ------------------------------------------------------------- 1-4 real kills

def test_planner_killed_twice_escalates(stg):
    ref = _plan_ticket(stg)
    _tick(stg)
    ss = _ss(stg, ref)
    assert ss["role"] == "planner" and ss["key"] == "plan:r1" and ss["attempt"] == 1
    assert stg.q.get(ref)["plan"]["planner"]["stage_key"] == "plan:r1"
    _kill(stg, ref)
    _tick(stg)
    ss = _ss(stg, ref)
    assert ss["attempt"] == 2 and ss["deaths"][0]["signal"] == 15      # from the exit file
    assert "respawn" in stg.q.get(ref)["history"][-1].get("cause", "respawn")
    _kill(stg, ref)
    _tick(stg)
    it = stg.q.get(ref)
    assert it["plan"]["status"] == "blocked" and it["needs_input"] is True
    assert "planner" in it["block_question"] and it["block_question"].count("attempt") >= 2
    assert len(_activity(stg, "STAGE_SPAWN", ref)) == 2
    assert len(_activity(stg, "STAGE_DEAD", ref)) == 2
    assert len(_activity(stg, "STAGE_BLOCK", ref)) == 1
    # an escalated stage is not respawned by further ticks
    n = len(stg.spawned)
    _tick(stg)
    assert len(stg.spawned) == n


def test_plan_reviewer_killed_twice_escalates(stg):
    ref = _plan_ticket(stg)
    _tick(stg)
    stg.q.plan_submit(ref, "the plan")
    _tick(stg)
    ss = _ss(stg, ref)
    assert ss["role"] == "plan_reviewer" and ss["key"] == "review:r1"
    _kill(stg, ref)
    _tick(stg)
    assert _ss(stg, ref)["attempt"] == 2
    _kill(stg, ref)
    _tick(stg)
    it = stg.q.get(ref)
    assert it["plan"]["status"] == "blocked" and it["needs_input"]
    assert "plan reviewer" in it["block_question"]


def test_verifier_killed_twice_stays_in_review_then_verdict_closes(stg, monkeypatch):
    stg.script[:] = [ENV_DUMP]
    ref = _verify_ticket(stg)
    _tick(stg)
    assert _wait(lambda: (stg.tmp / "env.json").exists())
    env = json.loads((stg.tmp / "env.json").read_text())
    assert env.get("WT_VERIFY") == "1" and env.get("WT_WORKER_ID")
    rec = next(w for w in stg.workers._load()["workers"] if w.get("stage") == "verifier")
    assert rec["ref"] == ref and rec["ticket_queue"] == "ST" and rec["kind"] == "adhoc"
    _kill(stg, ref)
    _tick(stg)
    assert _ss(stg, ref)["attempt"] == 2
    _kill(stg, ref)
    _tick(stg)
    it = stg.q.get(ref)
    assert it["status"] == "in_review" and it["needs_input"] is True
    assert it["closed_at"] is None
    v = stg.q.verdict(ref, True, "looks right", by="human")
    assert v["status"] == "closed"


def test_assessor_killed_twice_rotates_token_then_fails(stg):
    ref = _assess_ticket(stg)
    _tick(stg)
    a = stg.q.get(ref)["assessment"]
    t1 = a["token"]
    assert a["status"] == "running" and a["assessor"]["stage_key"] == "assess:1"
    w1 = a["assessor"]["worker_id"]
    _kill(stg, ref)
    _tick(stg)
    a = stg.q.get(ref)["assessment"]
    assert a["token"] != t1 and t1 in a["fenced_tokens"] and a["attempt"] == 2
    assert a["assessor"]["worker_id"] != w1                # metadata replaced
    assert _ss(stg, ref)["attempt"] == 2 and _ss(stg, ref)["key"] == "assess:1"
    with pytest.raises(stg.q.AssessmentFenced, match="superseded"):
        stg.q.assessment_accept_submission(ref, t1, {})
    _kill(stg, ref)
    _tick(stg)
    it = stg.q.get(ref)
    assert it["assessment"]["status"] == "failed" and it["needs_input"] is True
    assert it["status"] == "closed"
    assert len(_activity(stg, "STAGE_BLOCK", ref)) == 1


def test_answer_after_assessor_escalation_retries_with_new_cycle(stg):
    ALL = {p: {"verdict": "adequate", "note": ""} for p in
           ("logging", "ui_message", "automation", "monitoring", "auditors", "other")}
    ref = _assess_ticket(stg)
    _tick(stg)
    for _ in range(2):
        _kill(stg, ref)
        _tick(stg)
    t2 = stg.q.get(ref)["assessment"]["token"]
    assert stg.q.get(ref)["needs_input"]
    stg.q.answer(ref, "retry please")
    a = stg.q.get(ref)["assessment"]
    assert a["status"] == "due" and a["cycle"] == 2 and "assessor" not in a
    assert t2 in a["fenced_tokens"]
    assert _ss(stg, ref)["retry_note"] == "retry please"
    _tick(stg)
    a = stg.q.get(ref)["assessment"]
    assert a["status"] == "running" and a["assessor"]["stage_key"] == "assess:2"
    ss = _ss(stg, ref)
    assert ss["attempt"] == 1 and ss["key"] == "assess:2"
    assert "Human note: retry please" in stg.spawned[-1]["prompt"]
    _kill(stg, ref)
    _tick(stg)
    assert _ss(stg, ref)["attempt"] == 2
    token = stg.q.get(ref)["assessment"]["token"]
    stg.q.assessment_accept_submission(ref, token, ALL)
    stg.q.assessment_run_ops(ref, token)
    it = stg.q.get(ref)
    assert it["assessment"]["status"] == "done" and not it["needs_input"]


def test_forced_assess_run_fences_and_spawns_a_new_worker(stg):
    ref = _assess_ticket(stg)
    _tick(stg)
    a = stg.q.get(ref)["assessment"]
    t1, w1 = a["token"], a["assessor"]["worker_id"]
    stg.stages.request_assessment(ref, force=True)
    a = stg.q.get(ref)["assessment"]
    assert a["status"] == "due" and a["cycle"] == 2 and "assessor" not in a
    assert t1 in a["fenced_tokens"] and _ss(stg, ref)["attempt"] == 0
    _tick(stg)
    a = stg.q.get(ref)["assessment"]
    assert a["status"] == "running" and a["assessor"]["worker_id"] != w1
    assert _ss(stg, ref)["key"] == "assess:2"


# ------------------------------------------------- 7/8 adoption never crosses a key

def test_plan_round_change_keeps_no_stale_adoption(stg, monkeypatch):
    monkeypatch.setattr(stg.q, "_plan_reachable", lambda plan: False)   # force the legacy path
    ref = _plan_ticket(stg)
    _tick(stg)
    _kill(stg, ref)
    _tick(stg)                                          # attempt 2 of plan:r1
    assert _ss(stg, ref)["attempt"] == 2
    stg.q.plan_submit(ref, "v1")
    _tick(stg)                                          # reviewer review:r1, attempt 1
    assert _ss(stg, ref)["key"] == "review:r1" and _ss(stg, ref)["attempt"] == 1
    # legacy path (roles unreachable): a rejection goes back to planning, round 2
    stg.q.plan_verdict(ref, False, "no")
    plan = stg.q.get(ref)["plan"]
    assert plan["status"] == "planning" and plan["round"] == 2
    assert "planner" not in plan and plan["role_history"]
    _tick(stg)
    ss = _ss(stg, ref)
    assert ss["key"] == "plan:r2" and ss["attempt"] == 1
    _kill(stg, ref)
    _tick(stg)
    assert _ss(stg, ref)["attempt"] == 2 and not stg.q.get(ref)["needs_input"]
    # a re-injected stale planner with an old key is never adopted
    stg.q.stage_session_update(ref, lambda it, s: s.clear())
    stg.q._plan_update(ref, lambda it, p: p.__setitem__(
        "planner", {"worker_id": "ghost", "stage_key": "plan:r1"}))
    _tick(stg)
    assert _ss(stg, ref)["attempt"] == 1 and _ss(stg, ref)["worker_id"] != "ghost"


def test_verify_rework_keeps_no_stale_adoption(stg):
    ref = _verify_ticket(stg)
    assert stg.q.get(ref)["verify_cycle"] == 1
    _tick(stg)
    _kill(stg, ref)
    _tick(stg)
    assert _ss(stg, ref)["attempt"] == 2
    stg.q.verdict(ref, False, "wrong", by="human")      # fail -> rework
    it = stg.q.get(ref)
    assert it["status"] == "open"
    stg.q.claim_next("w1", project="ST")
    stg.q.close(ref, session_id="w1", resolution={"summary": "again", "no_code": True})
    it = stg.q.get(ref)
    assert it["status"] == "in_review" and it["verify_cycle"] == 2
    assert it["verifier_history"] and "verifier" not in it
    assert it["closed_at"] is None
    _tick(stg)
    ss = _ss(stg, ref)
    assert ss["key"] == "verify:2" and ss["attempt"] == 1
    _kill(stg, ref)
    _tick(stg)
    assert _ss(stg, ref)["attempt"] == 2 and not stg.q.get(ref)["needs_input"]


# ---------------------------------------------------------- 9 answer on escalation

@pytest.mark.parametrize("role", ["planner", "verifier", "assessor"])
def test_answer_on_stage_escalation_queues_a_retry_not_delivery(stg, monkeypatch, role):
    if role == "planner":
        ref = _plan_ticket(stg)
    elif role == "verifier":
        ref = _verify_ticket(stg)
    else:
        ref = _assess_ticket(stg)
    stg.q.stage_session_update(ref, lambda it, s: None)
    _tick(stg)
    for _ in range(2):
        _kill(stg, ref)
        _tick(stg)
    stg.q.stage_session_update(ref, lambda it, s: None)
    with stg.q._FileLock(stg.q._lock_path()):
        data = stg.q._load_unlocked()
        for it in data["items"]:
            if it["ref"] == ref:
                it["claimed_session_id"] = "sid-builder"
        stg.q._save_unlocked(data)
    calls = []
    monkeypatch.setattr(stg.cli, "_deliver_to_blocked_session",
                        lambda *a, **k: calls.append(a) or 0)
    spawn_calls = []
    real_spawn = stg.workers.spawn_adhoc
    monkeypatch.setattr(stg.workers, "spawn_adhoc",
                        lambda *a, **k: spawn_calls.append(a) or real_spawn(*a, **k))
    import argparse
    rc = stg.cli.cmd_answer(argparse.Namespace(ref=ref, text="retry", worker="", engine="",
                                               tid=False))
    assert rc == 0 and calls == [] and spawn_calls == []
    assert _activity(stg, "STAGE_QUEUED", ref)
    _tick(stg)
    assert len(spawn_calls) == 1
    assert _ss(stg, ref)["attempt"] == 1
    assert "Human note: retry" in stg.spawned[-1]["prompt"]


def test_answer_on_a_normal_block_still_delivers(stg, monkeypatch):
    ref = _bug(stg)
    stg.q.block(ref, "", question="which way?")
    with stg.q._FileLock(stg.q._lock_path()):
        data = stg.q._load_unlocked()
        for it in data["items"]:
            if it["ref"] == ref:
                it["claimed_session_id"] = "sid-builder"
        stg.q._save_unlocked(data)
    calls = []
    monkeypatch.setattr(stg.cli, "_deliver_to_blocked_session",
                        lambda *a, **k: calls.append(a) or 0)
    import argparse
    stg.cli.cmd_answer(argparse.Namespace(ref=ref, text="left", worker="", engine="",
                                          tid=False))
    assert len(calls) == 1


# --------------------------------------------------------- 10/11 legacy replays

def _legacy_plan(stg, age_min=80):
    ref = _plan_ticket(stg)
    old = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=age_min)
           ).strftime("%Y-%m-%dT%H:%M:%SZ")
    with stg.q._FileLock(stg.q._lock_path()):
        data = stg.q._load_unlocked()
        for it in data["items"]:
            if it["ref"] == ref:
                it["plan"] = {"status": "planning", "text": "", "round": 1, "reviews": [],
                              "planner": {"worker_id": "adhoc-x", "engine": "claude"}}
                it["updated_at"] = old
        stg.q._save_unlocked(data)
    return ref


def test_replay_vm_next_86_legacy_planning_with_pruned_record(stg):
    ref = _legacy_plan(stg)
    _tick(stg)
    ss = _ss(stg, ref)
    assert ss["attempt"] == 2 and ss["deaths"][0]["reason"].startswith("worker record gone")
    causes = [ln for ln in _activity(stg, "STAGE_SPAWN", ref)]
    assert any("cause=adopted" in ln for ln in causes) and any("cause=respawn" in ln for ln in causes)
    stg.q.plan_submit(ref, "a plan")                     # the respawned planner files it
    _tick(stg)
    assert _ss(stg, ref)["role"] == "plan_reviewer" and _ss(stg, ref)["attempt"] == 1


def test_replay_vm_next_104_legacy_in_review_with_pruned_verifier(stg):
    ref = _verify_ticket(stg)
    with stg.q._FileLock(stg.q._lock_path()):
        data = stg.q._load_unlocked()
        for it in data["items"]:
            if it["ref"] == ref:
                it.pop("verify_cycle", None)
                it["verifier"] = {"worker_id": "adhoc-old", "engine": "claude"}
                it["updated_at"] = "2020-01-01T00:00:00Z"
        stg.q._save_unlocked(data)
    _tick(stg)
    ss = _ss(stg, ref)
    assert ss["key"] == "verify:0" and ss["attempt"] == 2
    assert stg.spawned, "verifier respawned"
    stg.q.verdict(ref, True, "ok", by="human")
    assert stg.q.get(ref)["status"] == "closed"


# --------------------------------------------------- 12/13 immediate launch failure

def test_classified_launch_failure_sets_cooldown_and_refunds(stg, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_STAGE_LAUNCH_GRACE_S", "5")
    stg.script[:] = [AUTH_FAIL, SLEEP]
    ref = _verify_ticket(stg)
    _tick(stg)
    ss = _ss(stg, ref)
    assert ss["attempt"] == 0 and ss["deaths"][0]["refunded"] is True
    engine = stg.q.verifier_target(stg.q.get(ref))["engine"]
    assert stg.workers.active_launch_failure_cooldown("ST", engine)
    n = len(stg.spawned)
    _tick(stg)
    _tick(stg)
    assert len(stg.spawned) == n                                   # cooling down: no spawn
    assert len(_activity(stg, "STAGE_WAIT", ref)) == 1             # logged once
    # cooldown over: attempt 1 spawns (budget intact), a later kill gives attempt 2
    launch = stg.workers._load_launch_failures()
    for v in launch.values():
        v["cooldown_until"] = 0
    stg.workers._save_launch_failures(launch)
    _tick(stg)
    assert _ss(stg, ref)["attempt"] == 1
    _kill(stg, ref)
    _tick(stg)
    assert _ss(stg, ref)["attempt"] == 2


@pytest.mark.parametrize("script,expect", [
    (EXIT1, "engine exited immediately (exit 1)"),
    (SELF_KILL, "engine exited immediately (exit 137)"),     # the wrapper reports 128+sig
    (EXIT0, "exited without submitting"),
])
def test_unclassified_immediate_deaths_consume_attempts_no_cooldown(stg, monkeypatch, script, expect):
    monkeypatch.setenv("WATCHTOWER_STAGE_LAUNCH_GRACE_S", "5")
    stg.script[:] = [script]
    ref = _verify_ticket(stg)
    engine = stg.q.verifier_target(stg.q.get(ref))["engine"]
    for _ in range(6):                       # death, respawn, death, escalate
        _tick(stg)
        if stg.q.get(ref).get("needs_input"):
            break
        time.sleep(0.3)
    it = stg.q.get(ref)
    ss = _ss(stg, ref)
    assert it["needs_input"] is True and ss["escalated"] is True
    assert len(ss["deaths"]) == 2 and ss["attempt"] == 2
    assert all(expect in d["reason"] and not d.get("refunded") for d in ss["deaths"])
    assert _activity(stg, "STAGE_BLOCK", ref)
    assert stg.workers.active_launch_failure_cooldown("ST", engine) is None


def test_builder_generic_immediate_failure_still_sets_cooldown(stg, tmp_path, monkeypatch):
    """The default helper behaviour (builders) is unchanged."""
    log = tmp_path / "b.log"
    log.write_bytes(b"")
    proc = subprocess.Popen([sys.executable, "-c", EXIT1])
    stg.workers._LAUNCH_FAILURE_GRACE_S = 5
    rec = stg.workers._wait_for_immediate_launch_failure(
        proc, queue="ST", engine="codex", worker_id="w", log_path=log)
    assert rec and "engine exited immediately" in rec["reason"]
    assert stg.workers.active_launch_failure_cooldown("ST", "codex")
    # classified_only: no record, no cooldown
    monkeypatch.setenv("WATCHTOWER_STAGE_LAUNCH_GRACE_S", "5")
    stg.workers._save_launch_failures({})
    proc = subprocess.Popen([sys.executable, "-c", EXIT1])
    res = stg.workers._wait_for_immediate_launch_failure(
        proc, queue="ST", engine="codex", worker_id="w", log_path=log, classified_only=True)
    assert res["classified"] is False and "exit 1" in res["reason"]
    assert stg.workers.active_launch_failure_cooldown("ST", "codex") is None


# ----------------------------------------------------------- 14 idle / max-age

def test_idle_session_is_killed_and_respawned_then_escalates(stg, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_STAGE_IDLE_S", "1")
    ref = _verify_ticket(stg)
    _tick(stg)
    time.sleep(1.3)
    _tick(stg)                                                   # idle -> SIGTERM -> respawn
    ss = _ss(stg, ref)
    assert ss["attempt"] == 2 and "no progress (idle" in ss["deaths"][0]["reason"]
    time.sleep(1.3)
    _tick(stg)
    assert stg.q.get(ref)["needs_input"] is True


def test_max_age_kills_a_session_with_a_fresh_log(stg, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_STAGE_MAX_AGE_S", "1")
    stg.script[:] = ["import time\nwhile True:\n    print('tick', flush=True)\n    time.sleep(0.2)"]
    ref = _verify_ticket(stg)
    _tick(stg)
    time.sleep(1.3)
    _tick(stg)
    ss = _ss(stg, ref)
    assert ss["attempt"] == 2 and "no progress (age" in ss["deaths"][0]["reason"]


def test_claude_idle_uses_transcript_mtime_not_log(stg, monkeypatch):
    """claude -p writes nothing to its log until exit: a stale log must not read as idle."""
    rec = {"engine": "claude", "session_id": "abc", "log": str(stg.tmp / "none.log")}
    tp = stg.tmp / "t.jsonl"
    tp.write_text("x")
    from watchtower import messages
    monkeypatch.setattr(messages, "locate_transcript", lambda sid, eng="": str(tp))
    assert abs(stg.stages._activity_mtime(rec) - tp.stat().st_mtime) < 1
    (stg.tmp / "l.log").write_text("x")
    assert stg.stages._activity_mtime({"engine": "codex", "log": str(stg.tmp / "l.log")}) > 0


# ------------------------------------------- 16 transitions never spawn locally

def test_cli_transitions_request_and_never_spawn(stg, monkeypatch):
    """close(verify), plan submit, plan reject, plan-gated add: state + wake only."""
    def boom(*a, **k):
        raise AssertionError("a CLI transition spawned a session")
    monkeypatch.setattr(stg.workers, "spawn_adhoc", boom)
    monkeypatch.setattr(subprocess, "Popen", boom)
    wake = stg.stages.wake_path()
    ref = _plan_ticket(stg)
    assert stg.cli.start_pending_plans("ST") == 1
    assert wake.exists() and _activity(stg, "STAGE_QUEUED", ref)
    m0 = wake.stat().st_mtime
    stg.q.plan_start(ref)
    stg.q.plan_submit(ref, "p")
    import argparse
    ns = argparse.Namespace(plan_cmd="verdict", ref=ref, accept=False, reject=True,
                            reasons="no", by="t", version=None, json=False)
    assert stg.cli.cmd_plan(ns) == 0
    assert wake.stat().st_mtime > m0
    vref = _bug(stg, gates=["verify"])
    rc = stg.cli.main(["close", vref, "--worker", "w1", "--summary", "s", "--no-code"])
    assert rc == 0 and _activity(stg, "STAGE_QUEUED", vref)
    aref = _bug(stg)
    assert stg.cli.main(["close", aref, "--worker", "w1", "--summary", "s", "--no-code"]) == 0
    assert _activity(stg, "STAGE_QUEUED", aref)
    assert stg.cli.main(["assess", "run", aref, "--wait", "0"]) == 0
    assert stg.q.get(aref)["assessment"]["cycle"] == 2


def test_request_functions_never_reference_spawn_or_popen():
    src = Path(__file__).resolve().parent.parent / "watchtower" / "stages.py"
    tree = ast.parse(src.read_text())
    for fn in tree.body:
        if isinstance(fn, ast.FunctionDef) and fn.name in ("request", "request_assessment",
                                                           "_touch_wake"):
            names = {n.id for n in ast.walk(fn) if isinstance(n, ast.Name)} | \
                    {n.attr for n in ast.walk(fn) if isinstance(n, ast.Attribute)}
            assert not names & {"spawn_adhoc", "Popen", "_spawn", "spawn_workers"}, fn.name


# --------------------------------------------------- wake, burst cap, gate, retention

def test_sleep_until_wake_returns_early_and_a_lost_wake_is_caught_by_the_tick(stg):
    naps = []
    assert stg.stages.sleep_until_wake(1.0, sleep=lambda s: naps.append(s)) is False
    assert sum(naps) == pytest.approx(1.0)

    def nap(_s):
        stg.stages._touch_wake("ST-1", "test")
        naps.append("woken")
    naps.clear()
    assert stg.stages.sleep_until_wake(5.0, sleep=nap) is True and naps == ["woken"]
    # A wake that never arrives (file deleted) is harmless: the next tick still finds the intent.
    ref = _plan_ticket(stg)
    stg.stages.request(ref, "test")
    stg.stages.wake_path().unlink()
    assert ("ST-1", "spawned") in [(r, a) for r, a in _tick(stg)]


def test_per_tick_spawn_cap_defers_the_rest(stg, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_STAGE_SPAWNS_PER_TICK", "2")
    refs = [_plan_ticket(stg) for _ in range(4)]
    _tick(stg)
    assert sum(1 for r in refs if _ss(stg, r).get("worker_id")) == 2
    assert _activity(stg, "STAGE_DEFER", refs[-1]) or _activity(stg, "STAGE_DEFER", refs[-2])
    _tick(stg)
    assert sum(1 for r in refs if _ss(stg, r).get("worker_id")) == 4


def test_spawn_gate_serialises_engine_starts(stg, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_SPAWN_STAGGER_S", "0.4")
    assert stg.workers._spawn_gate("codex") < 0.1
    t0 = time.time()
    waited = stg.workers._spawn_gate("codex")
    assert waited >= 0.3 and time.time() - t0 >= 0.3
    monkeypatch.setenv("WATCHTOWER_SPAWN_STAGGER_S", "0")
    assert stg.workers._spawn_gate("codex") == 0.0


def test_dead_stage_record_is_kept_then_pruned_and_postmortem_runs_once(stg, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_ADHOC_DEAD_KEEP_S", "1")
    ref = _verify_ticket(stg)
    _tick(stg)
    wid = _ss(stg, ref)["worker_id"]
    _kill(stg, ref)
    calls = []
    monkeypatch.setattr(stg.workers, "_postmortem_launch_failure",
                        lambda w: calls.append([w]), raising=False)
    def ids():
        stg.workers.list_workers(prune=True)
        return [w["worker_id"] for w in stg.workers._load().get("workers", [])]
    assert wid in ids()                     # first dead observation: kept, died_at stamped
    assert wid in ids()                     # still within the keep window
    time.sleep(1.2)
    assert wid not in ids()
    assert sum(1 for batch in calls for w in batch if w["worker_id"] == wid) <= 1


# ------------------------------------------------- single supervisor (one real daemon)

def test_dry_run_and_spawnless_reconcile_never_supervise_stages(stg):
    ref = _verify_ticket(stg)
    stg.workers.reconcile_once(dry_run=True)
    stg.workers.reconcile_once(dry_run=False, supervise_stages=False)
    assert stg.spawned == [] and not _ss(stg, ref).get("worker_id")
    stg.workers.reconcile_once(dry_run=False)            # the real daemon's pass
    assert len(stg.spawned) == 1 and _ss(stg, ref).get("worker_id")


def test_racing_stage_passes_spawn_once(stg):
    import threading
    ref = _verify_ticket(stg)
    barrier = threading.Barrier(2)
    errs = []

    def run():
        try:
            barrier.wait()
            stg.workers.reconcile_stages_only()
        except Exception as exc:  # noqa: BLE001
            errs.append(exc)
    ts = [threading.Thread(target=run) for _ in range(2)]
    [t.start() for t in ts]
    [t.join(30) for t in ts]
    assert not errs
    assert len(stg.spawned) == 1 and _ss(stg, ref)["attempt"] == 1
    assert len(_activity(stg, "STAGE_SPAWN", ref)) == 1


def test_stage_cwd_comes_from_the_ticket_or_queue_never_the_daemon(stg):
    it = stg.q.enqueue(project="ST", title="v", note="v", item_type="bug",
                       repo_path=str(stg.tmp / "other-repo"))
    (stg.tmp / "other-repo").mkdir()
    ref = it["ref"]
    stg.q.claim_next("w1", project="ST")
    stg.q.close(ref, session_id="w1", resolution={"summary": "s", "no_code": True})
    # no resolvable repo: refused with a logged reason, nothing spawned
    stg.config.set_repo_path("ST", "")
    bare = stg.q.enqueue(project="ST", title="p", note="p", gates=["plan"])["ref"]
    _tick(stg)
    assert not any(s.get("repo_path") in ("", None) for s in stg.spawned)
    assert _activity(stg, "STAGE_DEAD", bare)
    assert "could not spawn" in _activity(stg, "STAGE_DEAD", bare)[0]


def test_trusted_directory_refusal_is_a_classified_launch_failure(stg, tmp_path):
    log = tmp_path / "x.log"
    log.write_text("Error: Not inside a trusted directory and --skip-git-repo-check was not specified.")
    c = stg.workers._classify_launch_failure_log(log)
    assert c and "not trusted" in c["reason"]


# ------------------------------------------------ missing engine CLI (WT-24 fix)

@pytest.mark.parametrize("make", ["plan", "verify"])
def test_missing_engine_cli_sets_cooldown_and_refunds_not_fail_open(stg, monkeypatch, make):
    def missing(engine, prompt, **kw):
        raise ValueError(f"{engine} CLI not found on PATH")
    monkeypatch.setattr(stg.workers, "build_adhoc_command", missing)
    ref = _plan_ticket(stg) if make == "plan" else _verify_ticket(stg)
    _tick(stg)
    item = stg.q.get(ref)
    ss = _ss(stg, ref)
    assert (item.get("plan") or {}).get("status") != "failed"   # gate not released
    assert item["needs_input"] is False
    assert ss["attempt"] == 0 and ss["deaths"][-1].get("refunded") is True
    assert "CLI" in ss["deaths"][-1]["reason"]
    assert stg.workers.active_launch_failure_cooldown("ST", _engine_of(stg, ref)) is not None
    assert _activity(stg, "STAGE_DEAD", ref)


def _engine_of(ns, ref):
    item = ns.q.get(ref)
    role = "planner" if (item.get("plan") or {}).get("status") else "verifier"
    return ns.stages._role_target(item, role)["engine"]


# ------------------------------------------- WT-29: plan-discussion turn sessions

def _to_discussion(ns, reasons="missing tests"):
    """Planner v1 filed and exited, reviewer rejected: the WT-28 shape."""
    ns.script[:] = [EXIT0]
    ref = _plan_ticket(ns)
    _tick(ns)                                   # planner spawn (exits at once)
    ns.q.plan_submit(ref, "plan v1 text")
    _tick(ns)                                   # reviewer spawn
    ns.script[:] = [SLEEP]
    ns.q.plan_verdict(ref, False, reasons)
    assert ns.q.get(ref)["plan"]["status"] == "discussing"
    return ref


def test_reject_after_planner_exited_respawns_planner_without_human(stg, monkeypatch):
    from watchtower import messages
    monkeypatch.setattr(messages, "send", lambda *a, **k: pytest.fail("no message to a dead peer"))
    ref = _to_discussion(stg)
    it = stg.q.get(ref)
    old_wid = it["plan"]["planner"]["worker_id"]
    assert _wait(lambda: not stg.stages.session_alive(old_wid))
    _tick(stg)
    it = stg.q.get(ref)
    ss = _ss(stg, ref)
    assert ss["key"] == "discuss:r1:d1:planner" and ss["attempt"] == 1
    assert it["plan"]["planner"]["worker_id"] not in ("", old_wid)
    assert it["plan"]["discussion"]["participants"]["planner"] == it["plan"]["planner"]["worker_id"]
    assert it["needs_input"] is False
    prompt = stg.spawned[-1]["prompt"]
    assert "plan v1 text" in prompt and "missing tests" in prompt
    # the amended plan goes to a fresh supervised reviewer turn
    stg.q.plan_submit(ref, "plan v2 text")
    _tick(stg)
    ss = _ss(stg, ref)
    assert ss["key"] == "discuss:r1:v2:reviewer" and ss["role"] == "plan_reviewer"
    prompt = stg.spawned[-1]["prompt"]
    assert "plan v2 text" in prompt and "--version 2" in prompt
    stg.q.plan_verdict(ref, True, "ok", version_seen=2)
    assert stg.q.get(ref)["plan"]["status"] == "accepted"


def test_dead_reviewer_during_discussion_is_respawned(stg):
    ref = _to_discussion(stg)
    _tick(stg)                                  # planner turn
    stg.q.plan_submit(ref, "plan v2 text")
    _tick(stg)                                  # reviewer turn
    _kill(stg, ref)
    _tick(stg)
    ss = _ss(stg, ref)
    assert ss["key"] == "discuss:r1:v2:reviewer" and ss["attempt"] == 2
    assert stg.q.get(ref)["needs_input"] is False


def test_discussion_turn_killed_twice_escalates_then_answer_retry_resumes(stg, monkeypatch):
    ref = _to_discussion(stg)
    _tick(stg)
    for _ in range(2):
        _kill(stg, ref)
        _tick(stg)
    it = stg.q.get(ref)
    assert it["plan"]["status"] == "blocked" and it["needs_input"] is True
    assert "discussion turn" in it["block_question"]
    assert it["plan"]["discussion"]["status"] == "active"
    n = len(stg.spawned)
    _tick(stg)
    assert len(stg.spawned) == n                # escalated: no more spawns
    import argparse
    stg.cli.cmd_answer(argparse.Namespace(ref=ref, text="retry", worker="", engine="",
                                          tid=False))
    it = stg.q.get(ref)
    assert it["plan"]["status"] == "discussing"
    _tick(stg)
    assert len(stg.spawned) == n + 1
    assert _ss(stg, ref)["key"] == "discuss:r1:d1:planner" and _ss(stg, ref)["attempt"] == 1


def _lose_plan_message(ns, ref, monkeypatch, text="please answer"):
    """``wt plan discuss`` to the live planner turn over a transport that
    takes it but gives no receipt (unverified), then the daemon's delivery
    sweep: the ledger reads it lost and runs ``liveness._h_plan``."""
    from watchtower import liveness, messages
    monkeypatch.setattr(messages, "send", lambda t, x, **kw: {"ok": True, "transport": "uds"})
    res = ns.cli._plan_send(ns.q.get(ref), "planner", text)
    assert res["ok"] and res["state"] == "unverified"
    out = [r for r in liveness.sweep_deliveries() if r["purpose"] == "plan"]
    assert [(r["state"], r["result"]) for r in out] == \
        [("lost", "respawn: stage session marked lost")]


def test_lost_plan_message_respawns_the_live_stage_session(stg, monkeypatch):
    """WT-31 D4 (verifier finding): a lost discuss delivery to a LIVE stage
    session must actually respawn it through the real supervisor -- kill the
    deaf session, record the death against the attempt budget, spawn fresh --
    and escalate once the budget is spent. No mocked ``stages.request``."""
    ref = _to_discussion(stg)
    _tick(stg)                                   # planner discussion turn (live)
    ss = _ss(stg, ref)
    old_wid, old_pid = ss["worker_id"], int(ss["pid"])
    assert ss["key"] == "discuss:r1:d1:planner" and ss["attempt"] == 1
    assert stg.stages.session_alive(old_wid)
    n = len(stg.spawned)
    _lose_plan_message(stg, ref, monkeypatch)
    assert _ss(stg, ref)["lost"]["worker_id"] == old_wid
    assert _activity(stg, "STAGE_LOST", ref)
    acted = _tick(stg)
    assert (ref, "spawned") in acted
    ss = _ss(stg, ref)
    assert len(stg.spawned) == n + 1 and ss["attempt"] == 2
    assert ss["worker_id"] not in ("", old_wid) and "lost" not in ss
    assert ss["deaths"][-1]["reason"].startswith("delivery lost")
    assert _wait(lambda: not stg.workers._pid_alive(old_pid))      # the deaf one is gone
    it = stg.q.get(ref)
    assert it["plan"]["planner"]["worker_id"] == ss["worker_id"]
    spawn = [h for h in it["history"] if h.get("event") == "stage_spawn"][-1]
    assert spawn["cause"] == "respawn" and it["needs_input"] is False
    # lost again on the last attempt: the budget is spent -> a human decides
    _lose_plan_message(stg, ref, monkeypatch, "still there?")
    _tick(stg)
    it = stg.q.get(ref)
    assert len(stg.spawned) == n + 1             # no third session
    assert it["needs_input"] is True and it["plan"]["status"] == "blocked"
    assert "died twice" in it["block_question"] and "delivery lost" in it["block_question"]


def test_live_planner_is_adopted_not_duplicated_on_reject(stg):
    stg.script[:] = [SLEEP]
    ref = _plan_ticket(stg)
    _tick(stg)
    wid = stg.q.get(ref)["plan"]["planner"]["worker_id"]
    stg.q.plan_submit(ref, "plan v1 text")
    _tick(stg)                                  # reviewer
    stg.q.plan_verdict(ref, False, "bad")
    n = len(stg.spawned)
    _tick(stg)
    ss = _ss(stg, ref)
    assert len(stg.spawned) == n                # adopted, nothing spawned
    assert ss["key"] == "discuss:r1:d1:planner" and ss["attempt"] == 0
    assert ss["worker_id"] == wid
    os.killpg(int(ss["pid"]), signal.SIGKILL)
    assert _wait(lambda: not stg.stages.session_alive(wid))
    _tick(stg)
    assert len(stg.spawned) == n + 1
    assert _ss(stg, ref)["attempt"] == 1        # the adopted death used no budget


def test_discussion_keys_in_desired(stg):
    ref = _plan_ticket(stg)
    stg.q.plan_start(ref)
    stg.q.plan_set_role(ref, "planner", {"worker_id": "p1"})
    stg.q.plan_submit(ref, "v1")
    stg.q.plan_set_role(ref, "reviewer", {"worker_id": "r1"})
    stg.q.plan_verdict(ref, False, "bad")
    d = stg.stages.desired([stg.q.get(ref)])
    assert [(x["role"], x["key"]) for x in d] == [("planner", "discuss:r1:d1:planner")]
    stg.q.plan_submit(ref, "v2")
    d = stg.stages.desired([stg.q.get(ref)])
    assert [(x["role"], x["key"]) for x in d] == [("plan_reviewer", "discuss:r1:v2:reviewer")]
    it = stg.q.get(ref)
    assert stg.stages.desired([dict(it, needs_input=True)]) == []
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(stg.stages, "_github_backed", lambda i: True)
        assert stg.stages.desired([it]) == []


def test_refunds_are_capped_then_consume_attempts(stg, monkeypatch):
    def missing(engine, prompt, **kw):
        raise ValueError(f"{engine} CLI not found on PATH")
    monkeypatch.setattr(stg.workers, "build_adhoc_command", missing)
    ref = _verify_ticket(stg)
    cap = stg.stages.MAX_REFUNDS
    for _ in range(cap + 2):
        stg.workers._save_launch_failures({})            # lift the cooldown between ticks
        _tick(stg)
    deaths = _ss(stg, ref)["deaths"]
    assert sum(1 for d in deaths if d.get("refunded")) == cap
    assert len(deaths) > cap and not deaths[-1].get("refunded")


def test_concurrent_spawn_gate_staggers_engine_starts(stg, monkeypatch):
    import threading
    monkeypatch.setenv("WATCHTOWER_SPAWN_STAGGER_S", "0.3")
    stamps = []

    def run():
        stg.workers._spawn_gate("codex")
        stamps.append(time.time())
    ts = [threading.Thread(target=run) for _ in range(4)]
    [t.start() for t in ts]
    [t.join(30) for t in ts]
    stamps.sort()
    assert len(stamps) == 4
    assert all(b - a >= 0.25 for a, b in zip(stamps, stamps[1:]))

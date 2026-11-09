"""Durable outcomes: default-off gate, non-claimability, CAS, fenced attempts,
outbox replay, acceptance, and the linked-ticket exception routes. No real
agents: spawn and liveness are fixtures."""
import json
import time

import pytest

from watchtower import outcomes as oc, queue as q, stages


@pytest.fixture
def env(wt_env, tmp_path, monkeypatch):
    from watchtower import outcome_intake
    monkeypatch.setattr(outcome_intake, "reconcile", lambda **kw: {"enabled": oc.enabled()})
    repo = tmp_path / "repo"
    repo.mkdir()
    spawns, alive, kills = [], {}, []

    def spawn(prompt, engine, **kw):
        wid = f"{kw['name']}-{engine}-0001"
        rec = {"worker_id": wid, "pid": 4242 + len(spawns), "pid_started": "t", "exit_file": "",
               "log": "", "session_id": f"sess-{len(spawns)}"}
        spawns.append(dict(kw, prompt=prompt, engine=engine, worker_id=wid))
        alive[wid] = True
        return rec

    monkeypatch.setattr(oc, "_spawn", spawn)
    monkeypatch.setattr(oc, "_list_workers", lambda: [{"worker_id": w, "alive": a} for w, a in alive.items()])
    monkeypatch.setattr(oc, "_liveness", lambda att, rows: ("alive", "") if alive.get(att.get("worker_id"))
                        else ("dead", "exited"))
    monkeypatch.setattr(oc, "_kill", lambda pid: kills.append(pid))
    monkeypatch.setattr(oc, "_role_target", lambda o, role: {"engine": "claude", "model": "approved-model",
                                                             "source": "planner:queue", "blocked": False})
    return dict(repo=str(repo), spawns=spawns, alive=alive, kills=kills)


def on(**limits):
    base = {"coalesce_s": 0, "coalesce_max_s": 0, "retry_backoff_s": 0, "recovery_backoff_s": 0}
    base.update(limits)
    oc.set_settings(True, base)


def make(env, **kw):
    payload = dict(title="Fast releases", goal="Hotfix reaches production in minutes",
                   criteria=[{"id": "c1", "text": "Serving latency under budget"}],
                   repo_path=env["repo"], queue="TEST", state="active")
    payload.update(kw)
    return oc.api("create", payload)["outcome"]


def lead(o_id):
    o = oc.get(o_id)
    att = [a for a in o["attempts"] if a["role"] == "lead"][-1]
    full = oc._load()["outcomes"][o_id]
    fence = next(a for a in full["attempts"] if a["id"] == att["id"])["fence"]
    return att, fence


def submit(o_id, result, **kw):
    att, fence = lead(o_id)
    return oc.api("lead-result", dict({"id": o_id, "attempt_id": att["id"], "fence": fence, "result": result}, **kw))


def independent_check(o_id, result, artifact):
    oc.reconcile()
    submit(o_id, {"kind": "verification_request", "criterion_ids": ["c1"],
                   "artifact": {"kind": "deployment", "id": artifact}})
    oc.reconcile()
    att = next(a for a in reversed(oc._load()["outcomes"][o_id]["attempts"]) if a["role"] == "verifier")
    return oc.api("evidence", {"id": o_id, "attempt_id": att["id"], "fence": att["fence"],
                              "criterion_id": "c1", "criteria_revision": 1,
                              "method": "independent_verification", "result": result,
                              "artifact": {"kind": "deployment", "id": artifact}})["outcome"]


def test_incident_evidence_displacement_and_recovery(env, monkeypatch):
    on(lead_wakes_per_day=10)
    now = time.time()
    monkeypatch.setattr(oc, "_now", lambda: now)
    incident = make(env)
    demo = make(env, title="Prepare demo", state="proposed", commitment=True, deadline="2026-11-10")
    payload = {"id": incident["id"], "type": "incident_observed", "key": "incident-1",
               "payload": {"title": "Requests timing out", "reason": "Requests fail for users",
                           "displaced_outcome_ids": [demo["id"]]}}
    with pytest.raises(oc.OutcomeError, match="independent evidence"):
        oc.api("event", payload)
    failed = independent_check(incident["id"], "fail", "release-1")
    payload["payload"]["evidence_id"] = failed["evidence"][-1]["id"]
    active = oc.api("event", payload)["outcome"]
    iid = active["incidents"][0]["id"]
    oc.api("event", payload)  # retry cannot duplicate displacement
    displaced = oc.get(demo["id"])
    assert displaced["deadline"] == "2026-11-10" and displaced["commitment"] is True
    assert len(displaced["displacements"]) == 1 and displaced["displacements"][0]["state"] == "active"
    assert "active verified incident" in oc._blockers(active)
    with pytest.raises(oc.OutcomeError):
        oc.api("event", {"id": incident["id"], "type": "incident_recovered",
                         "payload": {"incident_id": iid, "evidence_id": failed["evidence"][-1]["id"]}})
    now += 10
    passed = independent_check(incident["id"], "pass", "release-2")
    assert passed["state"] == "active"  # passing check alone does not silently recover incident
    recovered = oc.api("event", {"id": incident["id"], "type": "incident_recovered",
                                "payload": {"incident_id": iid, "evidence_id": passed["evidence"][-1]["id"]}})["outcome"]
    assert recovered["incidents"][0]["state"] == "recovered"
    assert "active verified incident" not in oc._blockers(recovered)
    displaced = oc.get(demo["id"])
    assert displaced["displacements"][0]["state"] == "recovered"
    assert displaced["deadline"] == "2026-11-10" and displaced["commitment"] is True


def test_intake_runs_before_active_snapshot_and_list_exposes_cached_view(env, monkeypatch):
    from watchtower import outcome_intake
    on()
    called = []
    def intake(**kw):
        called.append(kw)
        make(env)
        return {"enabled": True}
    monkeypatch.setattr(outcome_intake, "reconcile", intake)
    result = oc.reconcile(spawn=False)
    assert result["outcomes"] == 1 and len(called) == 1
    assert oc.api("list", {})["intake"]["enabled"] is True


def test_late_runtime_identity_is_adopted_from_exact_worker_receipt(env, monkeypatch):
    on()
    o = make(env)
    oc.reconcile()
    att = oc.get(o["id"])["attempts"][-1]
    monkeypatch.setattr(oc, "_list_workers", lambda: [{"worker_id": att["worker_id"],
                                                     "session_id": "resolved-session", "alive": True}])
    oc.reconcile(spawn=False)
    assert oc.get(o["id"])["attempts"][-1]["session_id"] == "resolved-session"


NEW_TICKET = {"kind": "dispatched_work", "work": {"mode": "new_ticket", "title": "Cache test deps",
                                                   "text": "Add dependency caching; measure.", "reversible": True}}


# ------------------------------------------------------------ default off

@pytest.mark.parametrize("result", [
    {"kind": "decision", "decision_id": "Q1", "question": "Choose?"},
    {"kind": "decision", "decision_id": "Q1", "question": "Choose?", "request_id": "r1", "revision": 1},
    {"kind": "stop", "reason": "Low value"},
    {"kind": "next_action"},
])
def test_unprepared_owner_wait_or_missing_action_cannot_finish_a_lead(env, result):
    on()
    o = make(env)
    oc.reconcile()
    with pytest.raises(oc.OutcomeError):
        submit(o["id"], result)
    got = oc.get(o["id"])
    assert got["decision_refs"] == [] and got["health"] != "waiting_owner"

def test_default_off_starts_nothing_and_rejects_admission(env, run_cli):
    assert oc.settings()["enabled"] is False
    r = run_cli("outcome", "api", "settings", "--payload-json", "{}")
    assert r.code == 0 and json.loads(r.out) == {**json.loads(r.out), "ok": True, "enabled": False}
    r = run_cli("outcome", "api", "create", "--payload-json", json.dumps({"title": "x"}))
    assert r.code == 1 and json.loads(r.out)["code"] == "outcomes_disabled"
    assert oc.reconcile()["enabled"] is False and env["spawns"] == []
    assert oc.route_exception("TEST-1") == ""
    assert not oc._store_path().exists()


def test_settings_toggle_is_durable(env, run_cli):
    r = run_cli("outcome", "api", "settings", "--payload-json", '{"enabled": true}')
    assert json.loads(r.out)["enabled"] is True and oc.enabled()
    r = run_cli("outcome", "api", "settings", "--payload-json", '{"enabled": "yes"}')
    assert r.code == 1
    oc.api("settings", {"enabled": False})
    assert oc.enabled() is False


# ------------------------------------------------------------ store / wire

def test_create_dedupes_source_key_and_is_never_claimable(env, run_cli):
    on()
    a = oc.api("create", dict(title="T", goal="G", criteria=["works"], queue="TEST", source_key="s1",
                              repo_path=env["repo"], state="active"))
    b = oc.api("create", dict(title="T2", goal="G2", criteria=["x"], queue="TEST", source_key="s1"))
    assert a["created"] and not b["created"] and a["outcome"]["id"] == b["outcome"]["id"]
    oid = a["outcome"]["id"]
    # Outside the executable queue: no ticket, nothing claimable, not addressable as a ticket.
    assert q.list_items() == [] and q.count_claimable(project="TEST") == 0
    assert q.claim_next("w1", project="TEST") is None
    assert q.get(oid) is None
    r = run_cli("outcome", "api", "list", "--payload-json", json.dumps({"repo_path": env["repo"]}))
    rows = json.loads(r.out)["outcomes"]
    assert [x["id"] for x in rows] == [oid]
    for field in ("id", "title", "goal", "criteria", "repo_path", "queue", "state", "health", "responsibility",
                  "next_action", "commitment", "rank", "deadline", "evidence", "links", "attempts", "findings",
                  "decision_refs", "updated_at", "revision", "sessions", "associations", "description"):
        assert field in rows[0]
    assert all("fence" not in at for at in rows[0]["attempts"])


def test_update_cas_aliases_and_no_self_delivery(env):
    on()
    o = make(env, state="draft")
    assert o["state"] == "proposed"
    with pytest.raises(oc.OutcomeError) as e:
        oc.api("update", {"id": o["id"], "expected_revision": o["revision"] + 5, "patch": {"title": "x"}})
    assert e.value.code == "revision_conflict" and e.value.extra["current_revision"] == o["revision"]
    with pytest.raises(oc.OutcomeError) as e:
        oc.api("update", {"id": o["id"], "expected_revision": o["revision"], "patch": {"state": "delivered"}})
    assert e.value.code == "forbidden"
    u = oc.api("update", {"id": o["id"], "expected_revision": o["revision"],
                          "patch": {"state": "active", "description": "why it matters"}})["outcome"]
    assert u["state"] == "active" and u["description"] == "why it matters" and u["revision"] == o["revision"] + 1
    u2 = oc.api("update", {"id": o["id"], "expected_revision": u["revision"],
                           "patch": {"criteria": [{"id": "c1", "text": "Serving latency under 5 minutes"}]}})
    assert u2["outcome"]["criteria_revision"] == 2
    p = oc.api("update", {"id": o["id"], "expected_revision": u2["outcome"]["revision"], "patch": {"state": "paused"}})
    assert p["outcome"]["state"] == "parked" and p["outcome"]["health"] == "parked"
    restored = oc.api("update", {"id": o["id"], "expected_revision": p["outcome"]["revision"],
                                 "patch": {"state": "proposed", "commitment": False}})["outcome"]
    assert restored["state"] == "proposed" and not restored["commitment"]
    assert restored == oc.get(o["id"])


def test_each_mutation_receipt_can_drive_the_next_cas_without_refresh(env):
    on()
    o = make(env, state="proposed")
    for action, extra in [("associate", {"session_id": "source-session"}),
                          ("event", {"type": "owner_review", "key": "review-1"}),
                          ("update", {"patch": {"state": "active", "commitment": True}})]:
        o = oc.api(action, dict(id=o["id"], expected_revision=o["revision"], **extra))["outcome"]
        assert o["revision"] == oc.get(o["id"])["revision"]
        o = oc.api("update", {"id": o["id"], "expected_revision": o["revision"],
                               "patch": {"description": "Continue from the saved receipt"}})["outcome"]
    refused = oc.api("accept", {"id": o["id"]})
    assert not refused["accepted"]
    o = refused["outcome"]
    oc.api("update", {"id": o["id"], "expected_revision": o["revision"], "patch": {"rank": 3}})


def test_concurrent_cas_only_one_wins(env):
    on()
    o = make(env)
    from concurrent.futures import ThreadPoolExecutor

    def try_update(i):
        try:
            oc.api("update", {"id": o["id"], "expected_revision": o["revision"], "patch": {"rank": i}})
            return True
        except oc.OutcomeError as exc:
            assert exc.code == "revision_conflict"
            return False
    with ThreadPoolExecutor(6) as pool:
        wins = list(pool.map(try_update, range(6)))
    assert wins.count(True) == 1


def test_associate_explicit_and_inherited_only(env):
    on()
    a, b = make(env), make(env, title="Other")
    oc.api("associate", {"id": a["id"], "session_id": "parent-1", "source": "ccc-spawn"})
    got = oc.api("associate", {"session_id": "child-1", "parent_session_id": "parent-1", "source": "ccc-spawn"})
    assert got["outcome"]["id"] == a["id"]
    assert {s["session_id"]: s["provenance"] for s in got["outcome"]["associations"]} == \
        {"parent-1": "explicit", "child-1": "inherited"}
    with pytest.raises(oc.OutcomeError, match="no explicit"):
        oc.api("associate", {"session_id": "c2", "parent_session_id": "unknown"})
    oc.api("associate", {"id": b["id"], "session_id": "parent-1", "source": "x"})
    with pytest.raises(oc.OutcomeError) as e:
        oc.api("associate", {"session_id": "c3", "parent_session_id": "parent-1"})
    assert e.value.code == "ambiguous"
    assert [x["id"] for x in oc.api("list", {"session_id": "child-1"})["outcomes"]] == [a["id"]]
    # Association never links tickets or claims.
    assert q.list_items() == [] and got["outcome"]["links"] == []


# ------------------------------------------------------------ lead runtime

def test_admission_spawns_one_lead_and_coalesces_events(env):
    on()
    o = make(env)
    oc.api("event", {"id": o["id"], "type": "external_event", "key": "e1", "payload": {"summary": "a"}})
    oc.api("event", {"id": o["id"], "type": "external_event", "key": "e1", "payload": {"summary": "dup"}})
    r = oc.reconcile()
    assert len(env["spawns"]) == 1 and r["spawned"][0].endswith(":spawned")
    sp = env["spawns"][0]
    assert sp["stage"] == "outcome_lead" and sp["model"] == "approved-model" and sp["repo_path"] == env["repo"]
    att, fence = lead(o["id"])
    assert fence in sp["prompt"] and "lead-result" in sp["prompt"]
    assert len(att["event_ids"]) == 2  # admitted + e1 (dup key dropped) in one wake
    oc.reconcile()
    assert len(env["spawns"]) == 1  # running attempt: no second lead
    assert oc.get(o["id"])["health"] == "running"


def test_global_concurrency_counts_only_outcome_attempts(env):
    on(max_concurrent_attempts=1, spawns_per_tick=5)
    a, b = make(env), make(env, title="B")
    oc.reconcile()
    assert len(env["spawns"]) == 1
    assert {oc.get(a["id"])["health"], oc.get(b["id"])["health"]} == {"running", "scheduled"}


def test_suggestions_only_are_rejected_and_persisted(env):
    on()
    o = make(env)
    oc.reconcile()
    with pytest.raises(oc.OutcomeError) as e:
        submit(o["id"], {"kind": "suggestion", "text": "maybe cache things"})
    assert e.value.code == "invalid_result"
    att, _ = lead(o["id"])
    assert att["rejected_results"] == 1 and att["state"] == "running"
    with pytest.raises(oc.OutcomeError):
        submit(o["id"], {"kind": "next_action", "next_action": {"text": "think more", "owner": "agent"}})
    # The process exits with no valid result: a failed try, and one technical retry.
    env["alive"][att["worker_id"]] = False
    oc.reconcile()
    o2 = oc.get(o["id"])
    first = next(a for a in o2["attempts"] if a["id"] == att["id"])
    assert first["state"] == "failed" and "invalid result" in first["failure"]
    assert len(env["spawns"]) == 2


def test_fenced_and_late_results_are_preserved_not_applied(env):
    on()
    o = make(env)
    oc.reconcile()
    att, fence = lead(o["id"])
    with pytest.raises(oc.OutcomeError) as e:
        oc.api("lead-result", {"id": o["id"], "attempt_id": att["id"], "fence": "stale", "result": NEW_TICKET})
    assert e.value.code == "fenced"
    full = oc._load()["outcomes"][o["id"]]
    assert next(a for a in full["attempts"] if a["id"] == att["id"])["late_results"]
    assert q.list_items() == []


def test_dispatch_creates_ordinary_ticket_once_and_one_executor(env):
    on()
    o = make(env)
    oc.reconcile()
    att, fence = lead(o["id"])
    payload = {"id": o["id"], "attempt_id": att["id"], "fence": fence, "result": NEW_TICKET}
    r = oc.api("lead-result", payload)
    assert r["work_ids"]
    items = q.list_items()
    assert len(items) == 1
    t = items[0]
    assert t["project"] == "TEST" and t["outcome_id"] == o["id"] and o["id"] in t["text"]
    assert t["status"] == "open"  # ordinary ticket, normal gates and claims
    # Replay of the same result: idempotent, still one ticket.
    again = oc.api("lead-result", payload)
    assert again["outcome"].get("duplicate") is True and len(q.list_items()) == 1
    got = oc.get(o["id"])
    assert got["health"] == "running" and got["responsibility"]["role"] == "executor"
    assert any(l["relation"] == "serves" and l["target"] == t["ref"] for l in got["links"])
    # A second mutating executor is refused at the next lead.
    oc.api("event", {"id": o["id"], "type": "external_event", "key": "x"})
    oc.reconcile()
    with pytest.raises(oc.OutcomeError) as e:
        submit(o["id"], NEW_TICKET)
    assert e.value.code == "executor_active" and len(q.list_items()) == 1


def test_dispatch_out_of_scope_queue_and_hard_stop_refused(env):
    on()
    o = make(env)
    oc.reconcile()
    bad = json.loads(json.dumps(NEW_TICKET))
    bad["work"]["queue"] = "OTHER"
    with pytest.raises(oc.OutcomeError) as e:
        submit(o["id"], bad)
    assert e.value.code == "out_of_scope"
    stop = json.loads(json.dumps(NEW_TICKET))
    stop["work"]["hard_stop"] = True
    with pytest.raises(oc.OutcomeError, match="hard stops"):
        submit(o["id"], stop)
    assert q.list_items() == []


def test_disabled_runtime_holds_new_dispatch_but_ingests_results(env):
    on()
    o = make(env)
    oc.reconcile()
    oc.set_settings(False)
    r = submit(o["id"], NEW_TICKET)  # result ingestion still works
    assert r["work_ids"] and q.list_items() == []  # but no new dispatch while off
    assert oc.reconcile()["enabled"] is False
    on()
    oc.reconcile()
    assert len(q.list_items()) == 1


def test_outbox_crash_between_sending_and_receipt_replays_once(env, monkeypatch):
    on()
    o = make(env)
    oc.reconcile()
    real = oc._record_receipt
    monkeypatch.setattr(oc, "_record_receipt", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("crash")))
    with pytest.raises(RuntimeError):
        submit(o["id"], NEW_TICKET)
    assert len(q.list_items()) == 1  # the effect happened; receipt lost
    monkeypatch.setattr(oc, "_record_receipt", real)
    with oc._txn() as data:  # age the in-flight op past the recovery window
        for op in data["outbox"]:
            if op["state"] == "sending":
                op["updated_at"] = oc._iso(time.time() - 3600)
    oc.reconcile()
    assert len(q.list_items()) == 1  # replay found the dedupe receipt
    got = oc.get(o["id"])
    assert sum(1 for l in got["links"] if l["relation"] == "serves") == 1


def test_unknown_spawn_receipt_revokes_fence_before_replacement(env):
    on(spawn_receipt_grace_s=0)
    o = make(env)
    with oc._txn() as data:  # simulate: reserved + marked sending, then the daemon died
        oo = data["outcomes"][o["id"]]
        oc._plan_spawns(data, oo, time.time(), oc.limits(), [2, 1])
        for op in data["outbox"]:
            if op["kind"] == "spawn":
                op.update(state="sending", updated_at=oc._iso(time.time() - 10))
    old, old_fence = lead(o["id"])
    oc.reconcile(now=time.time() + 5)
    got = oc.get(o["id"])
    first = next(a for a in got["attempts"] if a["id"] == old["id"])
    assert first["state"] == "failed" and first["receipt"] == "unknown"
    assert len(env["spawns"]) == 1  # one replacement, never a blind duplicate of the same attempt
    with pytest.raises(oc.OutcomeError) as e:
        oc.api("lead-result", {"id": o["id"], "attempt_id": old["id"], "fence": old_fence, "result": NEW_TICKET})
    assert e.value.code == "fenced"


def test_wall_limit_kills_then_bounded_recovery_ends_in_owned_gap(env):
    on(lead_wall_s=1)
    o = make(env)
    t0 = time.time()
    oc.reconcile(now=t0)
    oc.reconcile(now=t0 + 10)  # wall exceeded: killed, retried
    assert env["kills"] and len(env["spawns"]) == 2
    for w in list(env["alive"]):
        env["alive"][w] = False
    oc.reconcile(now=t0 + 20)  # second try dies: wake fails, event recovery 1, new wake
    for w in list(env["alive"]):
        env["alive"][w] = False
    oc.reconcile(now=t0 + 30)
    for w in list(env["alive"]):
        env["alive"][w] = False
    oc.reconcile(now=t0 + 40)
    got = oc.get(o["id"])
    assert got["health"] == "gap" and got["responsibility"]["role"] == "recovery"
    assert len(env["spawns"]) == 4  # 2 tries x 2 recovery wakes, then stop
    oc.reconcile(now=t0 + 50)
    assert len(env["spawns"]) == 4
    assert q.list_items() == []  # no owner question was filed
    assert any(f["kind"] == "gap" for f in oc.feed())
    oc.api("event", {"id": o["id"], "type": "recovery_retry", "key": "r1"})
    oc.reconcile(now=t0 + 60)
    assert len(env["spawns"]) == 5


def test_daily_budget_defers_visibly(env):
    on(lead_wakes_per_day=1)
    o = make(env)
    oc.reconcile()
    submit(o["id"], {"kind": "next_action", "next_action": {"text": "wait", "owner": "external",
                                                            "trigger": "next hotfix"}})
    assert oc.get(o["id"])["health"] == "waiting"
    oc.api("event", {"id": o["id"], "type": "external_event", "key": "later"})
    oc.reconcile()
    got = oc.get(o["id"])
    assert len(env["spawns"]) == 1 and got["health"] == "deferred"


def test_overdue_wait_and_ownership_gap_wake_the_lead(env):
    on()
    o = make(env)
    oc.reconcile()
    past = oc._iso(time.time() + 2)
    submit(o["id"], {"kind": "next_action", "next_action": {"text": "check", "owner": "agent", "due_at": past}})
    assert oc.get(o["id"])["health"] == "waiting"
    oc.reconcile(now=time.time() + 60)
    got = oc.get(o["id"])
    assert any(e["type"] == "deadline_due" for e in got["events"]) and len(env["spawns"]) == 2


def test_lead_cannot_finish_with_an_already_due_wait(env):
    on()
    o = make(env)
    oc.reconcile()
    with pytest.raises(oc.OutcomeError, match="future"):
        submit(o["id"], {"kind": "next_action", "next_action": {
            "text": "check now", "owner": "agent", "due_at": oc._iso(time.time() - 10)}})
    assert oc.get(o["id"])["attempts"][-1]["state"] == "running"


def test_lead_brief_contains_focus_timing_and_real_future_wait_example(env):
    on()
    o = make(env, commitment=True, rank=2, deadline="2026-11-10", description="Deadline preparation")
    oc.reconcile()
    prompt = env["spawns"][-1]["prompt"]
    assert '"commitment": true' in prompt and '"rank": 2' in prompt
    assert '"deadline": "2026-11-10"' in prompt and '"description": "Deadline preparation"' in prompt
    assert '"health"' in prompt and '"health_reason"' in prompt
    assert '2026-01-01T00:00:00Z' not in prompt
    assert 'never invent an ID' in prompt


# ------------------------------------------------------------ linked tickets

def _link_ticket(env, o, **fields):
    t = q.enqueue(project="TEST", note="work", repo_path=env["repo"])
    oc.api("link", {"id": o["id"], "ref": t["ref"], "relation": "serves", "source": "test"})
    if fields:
        with q._FileLock(q._lock_path()):
            data = q._load_unlocked()
            for it in data["items"]:
                if it["ref"] == t["ref"]:
                    it.update(fields)
            q._save_unlocked(data)
    return t["ref"]


def test_partial_close_findings_are_stable_and_block_acceptance(env):
    on()
    o = make(env, criteria=[{"id": "c1", "text": "gate", "accepts": ["ticket_gate"]}])
    ref = _link_ticket(env, o, status="closed", closed_at="2026-11-09T00:00:00Z",
                       resolution={"summary": "half", "commit": "abc123", "unresolved": ["flake in suite B"]},
                       gate_results=[{"gate": "verify", "passed": True}], gates=["verify"])
    oc.reconcile()
    oc.reconcile()
    got = oc.get(o["id"])
    finds = [f for f in got["findings"] if f["state"] == "unapplied"]
    assert len(finds) == 1 and finds[0]["id"].startswith("f-")
    assert sum(1 for e in got["events"] if e["type"] == "partial_close") == 1
    ex = next(a for a in got["attempts"] if a["role"] == "executor")
    assert ex["state"] == "succeeded" and ex["result"]["kind"] == "artifact_produced"
    oc.api("evidence", {"id": o["id"], "criterion_id": "c1", "criteria_revision": 1, "method": "ticket_gate",
                        "result": "pass", "ticket_ref": ref})
    got = oc.get(o["id"])
    assert got["state"] == "active" and "unapplied findings" in got["last_rejection"]["blockers"]
    # Lead disposes the finding with evidence; deterministic acceptance follows.
    submit(o["id"], {"kind": "next_action", "next_action": {"text": "watch", "owner": "agent",
                                                            "trigger": "next release"}},
           findings=[{"id": finds[0]["id"], "disposition": "parked", "note": "tracked separately"}])
    assert oc.get(o["id"])["state"] == "delivered"


def test_ticket_closure_alone_never_delivers(env):
    on()
    o = make(env)
    _link_ticket(env, o, status="closed", closed_at="2026-11-09T00:00:00Z",
                 resolution={"summary": "done", "commit": "abc"})
    oc.reconcile()
    got = oc.get(o["id"])
    assert got["state"] == "active" and any(e["type"] == "child_result" for e in got["events"])


def test_ticket_gate_requires_trusted_gates_and_criterion_opt_in(env):
    on()
    o = make(env)  # c1 accepts only independent verification
    ref = _link_ticket(env, o, status="closed", resolution={"commit": "abc"},
                       gate_results=[{"gate": "verify", "passed": True}], gates=["verify"])
    with pytest.raises(oc.OutcomeError, match="does not accept"):
        oc.api("evidence", {"id": o["id"], "criterion_id": "c1", "criteria_revision": 1,
                            "method": "ticket_gate", "result": "pass", "ticket_ref": ref})
    o2 = make(env, criteria=[{"id": "c1", "text": "x", "accepts": ["ticket_gate"]}])
    ref2 = _link_ticket(env, o2, status="closed", resolution={"commit": "abc"}, gates=["verify"])
    with pytest.raises(oc.OutcomeError, match="no recorded gate"):
        oc.api("evidence", {"id": o2["id"], "criterion_id": "c1", "criteria_revision": 1,
                            "method": "ticket_gate", "result": "pass", "ticket_ref": ref2})
    with pytest.raises(oc.OutcomeError, match="self-reports"):
        oc.api("evidence", {"id": o["id"], "criterion_id": "c1", "criteria_revision": 1,
                            "method": "self_report", "result": "pass"})


def test_independent_verification_and_criteria_revision_race(env):
    on()
    o = make(env)
    oc.reconcile()
    submit(o["id"], {"kind": "verification_request", "criterion_ids": ["c1"],
                     "artifact": {"kind": "commit", "id": "deadbeef"}, "checks": "time a hotfix"})
    oc.reconcile()
    got = oc.get(o["id"])
    ver = [a for a in got["attempts"] if a["role"] == "verifier"][-1]
    assert env["spawns"][-1]["stage"] == "outcome_verifier"
    fence = next(a for a in oc._load()["outcomes"][o["id"]]["attempts"] if a["id"] == ver["id"])["fence"]
    base = {"id": o["id"], "attempt_id": ver["id"], "fence": fence, "criterion_id": "c1",
            "criteria_revision": 1, "method": "independent_verification", "result": "pass"}
    with pytest.raises(oc.OutcomeError, match="immutable artifact"):
        oc.api("evidence", dict(base, artifact={"kind": "commit", "id": "other"}))
    # Criteria change while the verifier runs: its result is late and never counts.
    oc.api("update", {"id": o["id"], "expected_revision": oc.get(o["id"])["revision"],
                      "patch": {"criteria": [{"id": "c1", "text": "Serving latency under 3 minutes"}]}})
    with pytest.raises(oc.OutcomeError) as e:
        oc.api("evidence", dict(base, artifact={"kind": "commit", "id": "deadbeef"}))
    assert e.value.code == "fenced"
    got = oc.get(o["id"])
    assert got["state"] == "active" and got["evidence"] == []


def test_verified_evidence_delivers_through_acceptance_gate(env):
    on()
    o = make(env)
    oc.reconcile()
    submit(o["id"], {"kind": "verification_request", "criterion_ids": ["c1"],
                     "artifact": {"kind": "deployment", "id": "dep-7"}})
    oc.reconcile()
    ver = [a for a in oc.get(o["id"])["attempts"] if a["role"] == "verifier"][-1]
    fence = next(a for a in oc._load()["outcomes"][o["id"]]["attempts"] if a["id"] == ver["id"])["fence"]
    got = oc.api("evidence", {"id": o["id"], "attempt_id": ver["id"], "fence": fence, "criterion_id": "c1",
                              "criteria_revision": 1, "method": "independent_verification", "result": "pass",
                              "artifact": {"kind": "deployment", "id": "dep-7"}})["outcome"]
    assert got["state"] == "delivered" and got["acceptance"]["evidence_ids"]["c1"]
    assert got["health"] == "delivered"
    with pytest.raises(oc.OutcomeError):
        oc.api("update", {"id": o["id"], "expected_revision": got["revision"], "patch": {"state": "active"}})


def test_unknown_delivery_routes_to_lead_without_resend(env, monkeypatch):
    on()
    from watchtower import liveness
    sent = []
    monkeypatch.setattr(liveness, "deliver", lambda *a, **k: sent.append(a) or {"ok": True})
    o = make(env)
    _link_ticket(env, o, status="in_progress", supervision={
        "state": "paused_unreachable", "generation": 1, "reason": "Delivery outcome unknown",
        "intent": {"key": "takeover:x:1:remind::3", "state": "uncertain"}})
    oc.reconcile()
    oc.reconcile()
    got = oc.get(o["id"])
    assert sum(1 for e in got["events"] if e["type"] == "delivery_uncertain") == 1
    assert sent == []


def test_stage_exhaustion_routes_to_lead_only_when_linked_and_enabled(env):
    on()
    o = make(env)
    linked = _link_ticket(env, o, status="in_review", gate_pending="verify")
    unlinked = q.enqueue(project="TEST", note="other", repo_path=env["repo"])["ref"]
    for ref in (linked, unlinked):
        ss = {"key": "verify:1", "role": "verifier", "attempt": 2,
              "deaths": [{"key": "verify:1", "attempt": 1, "reason": "died"},
                         {"key": "verify:1", "attempt": 2, "reason": "died"}]}
        q.stage_session_update(ref, lambda it, s: s.update(ss))
        stages._escalate(q.get(ref), "verifier", "verify:1", ss)
    assert q.get(unlinked)["needs_input"] is True  # unchanged behaviour
    li = q.get(linked)
    assert not li.get("needs_input") and li["stage_session"]["routed_outcome"] == o["id"]
    oc.reconcile()
    got = oc.get(o["id"])
    assert any(e["type"] == "stage_exhausted" for e in got["events"])
    # The lead retries the stage on the linked ticket (existing reset path).
    submit(o["id"], {"kind": "dispatched_work", "work": {"mode": "retry_stage", "ticket_ref": linked,
                                                          "reversible": True}})
    assert q.get(linked)["stage_session"]["escalated"] is False


def test_stage_exhaustion_unchanged_when_disabled(env):
    on()
    o = make(env)
    ref = _link_ticket(env, o, status="in_review", gate_pending="verify")
    oc.set_settings(False)
    ss = {"key": "verify:1", "role": "verifier", "attempt": 2, "deaths": []}
    q.stage_session_update(ref, lambda it, s: s.update(ss))
    stages._escalate(q.get(ref), "verifier", "verify:1", ss)
    assert q.get(ref)["needs_input"] is True


def test_decision_requires_genuine_answer_receipt_then_lead_consumes(env):
    on()
    o = make(env)
    oc.reconcile()
    submit(o["id"], {"kind": "decision", "decision_id": "Q9", "question": "Pay for runners?", "hard_stop": True,
                     "request_id": "req-1", "revision": "3",
                     "preparation": {"why_now": "release slow", "options": ["pay", "optimize"],
                                     "recommendation": "optimize first", "tradeoffs": "capacity versus maintenance",
                                     "reversibility": "revert the configuration"}})
    got = oc.get(o["id"])
    d = got["decision_refs"][0]
    assert got["health"] == "waiting_owner" and d["request_id"] == "req-1" and d["preparation"]["why_now"]
    with pytest.raises(oc.OutcomeError, match="receipt"):
        oc.api("event", {"id": o["id"], "type": "owner_answer", "payload": {"decision_id": "Q9", "answer": "no"}})
    with pytest.raises(oc.OutcomeError) as e:
        oc.api("event", {"id": o["id"], "type": "owner_answer",
                         "payload": {"request_id": "req-1", "revision": "2", "answer": "no", "receipt": "m1"}})
    assert e.value.code == "stale_revision"
    oc.api("event", {"id": o["id"], "type": "owner_answer",
                     "payload": {"request_id": "req-1", "revision": "3", "answer": "optimize", "receipt": "m1"}})
    oc.api("event", {"id": o["id"], "type": "owner_answer",
                     "payload": {"request_id": "req-1", "revision": "3", "answer": "optimize", "receipt": "m1"}})
    oc.reconcile()
    assert len(env["spawns"]) == 2
    assert "optimize" in env["spawns"][-1]["prompt"]
    submit(o["id"], {"kind": "next_action", "next_action": {"text": "baseline", "owner": "agent",
                                                            "trigger": "baseline ticket"}})
    assert oc.get(o["id"])["decision_refs"][0]["state"] == "consumed"


def test_takeover_exhaustion_routes_when_linked(env, monkeypatch):
    on()
    from watchtower import takeover as t
    o = make(env)
    ref = _link_ticket(env, o, status="in_progress")
    item = q.get(ref)
    s = {"id": "s1", "generation": 1, "session": {"engine": "claude", "session_id": "sid", "host": "h",
                                                     "repo_path": env["repo"]},
         "state": "running", "reminders": t.MAX_REMINDERS, "next_deadline": 0, "processed_turn": "t1",
         "intent": None}
    item["supervision"] = s
    item["claimed_session_id"] = "sid"
    obs = {"engine": "claude", "session_id": "sid", "host": "h", "repo_path": env["repo"], "state": "idle",
           "completed_turn": "t1", "activity_at": 0}
    t._tick(item, obs, time.time())
    assert s["state"] == "waiting" and s["wait_source"] == "outcome" and not item.get("needs_input")
    oc.set_settings(False)
    s.update(state="running", wait_source="", wait_identity="")
    t._tick(item, obs, time.time())
    assert item.get("needs_input") is True  # disabled: existing owner escalation


def test_corrupt_store_fails_closed(env):
    on()
    oc._store_path().write_text("{not json")
    with pytest.raises(oc.OutcomeError) as e:
        oc.api("list", {})
    assert e.value.code == "store_corrupt"
    assert oc._store_path().read_text() == "{not json"
    assert oc.route_exception("TEST-1") == ""

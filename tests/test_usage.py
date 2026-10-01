"""Provider counters and durable lifecycle attribution, without real agent calls."""
import json

import pytest

from watchtower import usage


def write(path, rows):
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n")


def claude(mid, model="actual-model", inp=100, cache=50, out=10, **extra):
    return {"type": "assistant", "requestId": "request-" + mid, "message": {
        "id": mid, "model": model, "usage": {"input_tokens": inp,
        "cache_read_input_tokens": cache, "output_tokens": out,
        "cache_creation_input_tokens": 5, **extra}}}


def codex(inp, cache, out):
    return {"type": "event_msg", "payload": {"type": "token_count", "info": {
        "total_token_usage": {"input_tokens": inp, "cached_input_tokens": cache,
                              "output_tokens": out}}}}


def test_claude_deduplicates_stream_blocks_preserves_semantics(tmp_path):
    p = tmp_path / "session.jsonl"
    write(p, [claude("m1"), claude("m1", out=12), claude("m2", model="fallback-model")])
    snap = usage.parse(str(p), "claude")
    assert len(snap["units"]) == 2
    assert snap["units"]["m1"]["output"] == 12
    assert snap["units"]["m1"]["input"] == 100  # cache is NOT subtracted
    assert snap["units"]["m1"]["request_id"] == "request-m1"
    assert snap["input_semantics"] == "excludes_cache_read_and_write"


def test_codex_cumulative_deltas_duplicates_and_actual_models(tmp_path):
    p = tmp_path / "rollout-test.jsonl"
    write(p, [{"type": "session_meta", "payload": {}},
              {"type": "turn_context", "payload": {"model": "model-a"}},
              codex(100, 60, 10), codex(100, 60, 10),
              {"type": "turn_context", "payload": {"model": "model-b"}},
              codex(300, 150, 40)])
    units = list(usage.parse(str(p), "codex")["units"].values())
    assert [(u["model"], u["input"], u["cache_read"], u["output"]) for u in units] == [
        ("model-a", 40, 60, 10), ("model-b", 110, 90, 30)]
    assert all(u["cache_write"] is None for u in units)


def test_missing_fields_and_zero_are_distinct(tmp_path):
    p = tmp_path / "session.jsonl"
    row = claude("zero", inp=0, cache=0, out=0)
    del row["message"]["usage"]["cache_creation_input_tokens"]
    write(p, [row])
    unit = usage.parse(str(p), "claude")["units"]["zero"]
    assert unit["input"] == unit["cache_read"] == unit["output"] == 0
    assert unit["cache_write"] is None
    assert usage.parse(str(tmp_path / "missing"), "claude")["status"] == "unavailable"


def test_codex_missing_cache_does_not_invent_ordinary_input(tmp_path):
    p = tmp_path / "rollout-test.jsonl"
    row = codex(100, 40, 10)
    del row["payload"]["info"]["total_token_usage"]["cached_input_tokens"]
    write(p, [{"type": "session_meta", "payload": {}}, row])
    unit = next(iter(usage.parse(str(p), "codex")["units"].values()))
    assert unit["input"] is None and unit["cache_read"] is None
    assert unit["output"] == 10 and unit["model"] is None


def test_truncated_rollout_and_counter_reset_are_unknown(tmp_path):
    p = tmp_path / "rollout-test.jsonl"
    write(p, [codex(100, 50, 10), codex(90, 45, 9)])
    with p.open("a") as f:
        f.write('{"type":')
    snap = usage.parse(str(p), "codex")
    assert snap["malformed_lines"] == 1
    assert all(u["input"] is None for u in snap["units"].values())


@pytest.fixture
def measured(tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_USAGE_DIR", str(tmp_path / "usage"))
    p = tmp_path / "session.jsonl"
    write(p, [claude("preexisting")])
    def observe(worker, sid=""):
        return dict(usage.parse(str(p), "claude"), worker_id=worker,
                    session_id="session", requested_model="configured-tier")
    monkeypatch.setattr(usage, "observe", observe)
    return p


def test_two_tickets_reused_session_and_idempotent_finish(measured):
    first, second = {}, {}
    usage.begin(first, "worker", "w", "session", "start-1")
    write(measured, [claude("preexisting"), claude("work-1", inp=20, cache=5, out=3)])
    usage.finish(first, "worker", "end-1", "close")
    usage.finish(first, "worker", "end-1", "close")
    usage.begin(second, "worker", "w", "session", "start-2")
    write(measured, [claude("preexisting"), claude("work-1", inp=20, cache=5, out=3),
                     claude("work-2", inp=30, cache=10, out=4)])
    usage.finish(second, "worker", "end-2", "close")
    assert first["token_usage"]["totals"]["input"] == 20
    assert second["token_usage"]["totals"]["input"] == 30
    attempt = second["token_usage"]["attempts"][0]
    assert attempt["models"][0]["model"] == "actual-model"
    assert attempt["observed"]["requested_model"] == "configured-tier"
    assert len(first["token_usage"]["attempts"]) == 1


def test_stage_failure_and_fallback_attempt_are_additive(measured):
    item = {}
    usage.begin(item, "planner", "p1", "session", "t0", run="plan:1", dedicated=True)
    usage.finish(item, "planner", "t1", "quota failure")
    usage.begin(item, "planner", "p2", "session", "t2", run="plan:1", attempt=2, dedicated=True)
    write(measured, [claude("preexisting"), claude("fallback", model="fallback-model", inp=25)])
    usage.finish(item, "planner", "t3", "plan")
    attempts = item["token_usage"]["attempts"]
    assert attempts[0]["outcome"] == "quota failure"
    assert attempts[1]["attempt"] == 2
    assert attempts[1]["models"][0]["model"] == "fallback-model"
    assert item["token_usage"]["totals"]["input"] == 125


def test_missing_baseline_never_charges_entire_reused_session(measured, monkeypatch):
    observed = usage.observe
    monkeypatch.setattr(usage, "observe", lambda w, sid="": {
        "status": "missing", "units": {}, "path": "", "engine": "", "session_id": sid})
    item = {}
    usage.begin(item, "worker", "w", "session", "t0")
    monkeypatch.setattr(usage, "observe", observed)
    usage.finish(item, "worker", "t1", "close")
    assert item["token_usage"]["totals"]["input"] is None
    assert item["token_usage"]["attempts"][0]["attribution"] == "missing_baseline"


def test_queue_claim_close_reopen_and_stages_persist(measured, tmp_path, monkeypatch):
    from watchtower import queue as q
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setattr(q, "_notify_ticket_event", lambda *a, **k: None)
    monkeypatch.setattr(q, "_notify_review", lambda *a, **k: None)
    monkeypatch.setattr(q, "_note_worker_ticket_done", lambda *a: None)
    monkeypatch.setattr(q, "assessment_mark_due", lambda *a: None)
    monkeypatch.setattr(q, "_verify_worker_live", lambda *a: None)
    it = q.enqueue(project="TOK", note="measured")
    ref = it["ref"]
    q.claim_by_ref(ref, "w")
    write(measured, [claude("preexisting"), claude("work", inp=20)])
    it = q.close(ref, "w", force=True, resolution={"summary": "done", "no_code": True})
    assert it["token_usage"]["totals"]["input"] == 20
    q.reopen(ref)
    q.claim_by_ref(ref, "w")  # same second, distinct claim attempt
    q.stage_session_update(ref, lambda it, ss: ss.update(
        role="verifier", key="verify:1", worker_id="v1", attempt=1))
    assert len(q.get(ref)["token_usage"]["attempts"]) == 3
    assert "input / cache read / output" in usage.render(q.get(ref))


def test_private_projection_and_late_finalization_are_idempotent(measured, tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_USAGE_DIR", str(tmp_path / "usage"))
    item = {"ref": "TOK-1", "history": []}
    usage.capture(item, usage.on_event, {"event": "claim", "at": "t0", "by": {"worker": "w"}})
    write(measured, [claude("preexisting"), claude("work", inp=20)])
    usage.capture(item, usage.on_event, {"event": "close", "at": "t1", "by": {"worker": "w"}})
    assert item["token_usage"]["totals"]["input"] == 20
    assert "baseline" not in json.dumps(item["token_usage"])
    assert str(measured) not in json.dumps(item["token_usage"])
    write(measured, [claude("preexisting"), claude("work", inp=20), claude("late", inp=7),
                     {"type": "result"}])
    usage.reconcile()
    usage.reconcile()
    usage.attach(item)
    assert item["token_usage"]["totals"]["input"] == 27
    assert item["token_usage"]["completeness"] == "complete"
    private = usage._read(usage._location(item))["token_usage"]
    assert private["attempts"][0]["observed"]["units"]["late"]["request_id"] == "request-late"
    assert usage._location(item).stat().st_mode & 0o777 == 0o600


def test_late_usage_shared_across_tickets_is_not_double_counted(measured, tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_USAGE_DIR", str(tmp_path / "usage"))
    first = {"ref": "TOK-1", "history": []}
    second = {"ref": "TOK-2", "history": []}
    claim = {"event": "claim", "at": "t0", "by": {"worker": "w"}}
    close = {"event": "close", "at": "t1", "by": {"worker": "w"}}
    usage.capture(first, usage.on_event, claim)
    write(measured, [claude("preexisting"), claude("work-1", inp=20)])
    usage.capture(first, usage.on_event, close)
    usage.capture(second, usage.on_event, claim)
    write(measured, [claude("preexisting"), claude("work-1", inp=20), claude("work-2", inp=30)])
    usage.capture(second, usage.on_event, close)
    write(measured, [claude("preexisting"), claude("work-1", inp=20), claude("work-2", inp=30),
                     claude("late-shared", inp=70), {"type": "result"}])
    usage.reconcile()
    usage.attach(first)
    usage.attach(second)
    assert first["token_usage"]["totals"]["input"] == 20
    assert second["token_usage"]["totals"]["input"] == 30
    assert first["token_usage"]["attempts"][0]["completeness"] == "ambiguous_shared_session"
    assert "late_observed" in usage._read(usage._location(first))["token_usage"]["attempts"][0]


def test_github_namespace_isolates_same_ref_and_safe_metadata(measured, tmp_path, monkeypatch):
    from watchtower.github_backend import GitHubIssuesBackend
    monkeypatch.setenv("WATCHTOWER_USAGE_DIR", str(tmp_path / "usage"))
    # Exercise the exact durable history seam used by claim/close, offline.
    a = GitHubIssuesBackend("TOK", repo="owner/repo-a", auto_drain=False, partition_by_label=False)
    b = GitHubIssuesBackend("TOK", repo="owner/repo-b", auto_drain=False, partition_by_label=False)
    meta_a, meta_b = {}, {}
    a._usage_history(meta_a, "1", "claim", worker="w", session_id="session")
    b._usage_history(meta_b, "1", "claim", worker="w", session_id="session")
    write(measured, [claude("preexisting"), claude("work", inp=20)])
    a._usage_history(meta_a, "1", "close", worker="w", session_id="session")
    assert meta_a["token_usage"]["totals"]["input"] == 20
    assert meta_b["token_usage"]["totals"]["input"] is None
    assert len(list((tmp_path / "usage").glob("*.json"))) == 2
    encoded = json.dumps(meta_a["token_usage"])
    assert "baseline" not in encoded and "provider_usage" not in encoded and str(measured) not in encoded


def test_private_write_failure_does_not_block_ticket_transition(measured, monkeypatch):
    monkeypatch.setattr(usage, "_write", lambda *a: (_ for _ in ()).throw(OSError("disk full")))
    item = {"ref": "TOK-1"}
    usage.capture(item, usage.on_event, {"event": "claim", "at": "t0", "by": {"worker": "w"}})
    assert item["token_usage"]["errors"] == [{"error": "OSError"}]
    assert item["token_usage"]["totals"]["input"] is None


def test_github_real_close_roundtrip_exposes_only_safe_usage(measured, tmp_path, monkeypatch):
    from test_github_backend import _install_fake_gh, _reload_isolated, _drainable
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _drainable(config)
    it = q.enqueue(project="GHI", note="measured work", source="test")
    q.claim_next("worker-1", project="GHI")
    write(measured, [claude("preexisting"), claude("ticket-work", inp=31)])
    closed = q.close(it["ref"], "worker-1", resolution={"summary": "done", "no_code": True})
    assert closed["token_usage"]["totals"]["input"] == 31
    body = json.loads(state.read_text())["issues"][0]["body"]
    assert "actual-model" in body
    assert "provider_usage" not in body and "baseline" not in body and str(measured) not in body
    write(measured, [claude("preexisting"), claude("ticket-work", inp=31), claude("late", inp=9),
                     {"type": "result"}])
    usage.reconcile()
    assert q.get(it["ref"])["token_usage"]["totals"]["input"] == 40
    # Reconciliation is private: it performs no new writes to the remote issue.
    assert json.loads(state.read_text())["issues"][0]["body"] == body


@pytest.mark.parametrize("role,event", [("planner", "plan"), ("plan_reviewer", "plan_review"),
                                        ("verifier", "verify"), ("assessor", "assessment")])
def test_all_supervised_roles_capture_terminal_usage(measured, tmp_path, monkeypatch, role, event):
    monkeypatch.setenv("WATCHTOWER_USAGE_DIR", str(tmp_path / "usage"))
    item = {"ref": "TOK-1"}
    stage = {"role": role, "key": role + ":1", "worker_id": "stage-w", "attempt": 1}
    usage.capture(item, usage.track_stage, {}, stage, "t0")
    write(measured, [claude("preexisting"), claude("stage-work", model="actual-stage-model", inp=31)])
    usage.capture(item, usage.on_event, {"event": event, "at": "t1", "by": {"kind": "system"}})
    a = item["token_usage"]["attempts"][0]
    assert a["role"] == role and a["outcome"] == event
    assert "actual-stage-model" in [m["model"] for m in a["models"]]


def test_corrupt_private_ledger_is_not_overwritten(measured, tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_USAGE_DIR", str(tmp_path / "usage"))
    item = {"ref": "TOK-1"}
    target = usage._location(item)
    target.parent.mkdir()
    target.write_text("corrupt preserved evidence")
    usage.capture(item, usage.on_event, {"event": "claim", "at": "t0", "by": {"worker": "w"}})
    assert target.read_text() == "corrupt preserved evidence"
    assert item["token_usage"]["totals"]["input"] is None
    assert item["token_usage"]["errors"] == [{"error": "JSONDecodeError"}]


def test_rebound_worker_cannot_change_old_attempt_provider(tmp_path, monkeypatch):
    from watchtower import workers, messages
    p = tmp_path / "old.jsonl"
    write(p, [claude("old-work")])
    monkeypatch.setattr(workers, "_load", lambda: {"workers": [{
        "worker_id": "w", "session_id": "new-session", "engine": "codex", "model": "new-model"}]})
    monkeypatch.setattr(messages, "locate_transcript", lambda sid, engine: str(p))
    snap = usage.observe("w", "old-session")
    assert snap["engine"] == "claude"
    assert snap["requested_model"] is None
    assert snap["units"]["old-work"]["input"] == 100


def test_recorded_exit_settles_pruned_worker_without_terminal_event(measured, tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_USAGE_DIR", str(tmp_path / "usage"))
    observed = usage.observe
    exit_file = tmp_path / "exit.json"
    def source(worker, sid=""):
        return dict(observed(worker, sid), process={"exit_file": str(exit_file)})
    monkeypatch.setattr(usage, "observe", source)
    item = {"ref": "TOK-1", "history": []}
    usage.capture(item, usage.on_event, {"event": "claim", "at": "t0", "by": {"worker": "w"}})
    monkeypatch.setattr(usage, "observe", lambda w, sid="": dict(observed(w, sid), process={"pid": None, "exit_file": None}))
    write(measured, [claude("preexisting"), claude("work", inp=20)])
    usage.capture(item, usage.on_event, {"event": "close", "at": "t1", "by": {"worker": "w"}})
    exit_file.write_text(json.dumps({"ended_at": "2026-01-01T00:00:00Z", "rc": 0}))
    write(measured, [claude("preexisting"), claude("work", inp=20), claude("late", inp=7)])
    usage.reconcile()
    usage.attach(item)
    assert item["token_usage"]["completeness"] == "complete"
    assert item["token_usage"]["totals"]["input"] == 27

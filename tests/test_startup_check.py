import json
import os

from watchtower.startup_check import check, instant


BASE = instant("2026-11-17T00:00:00Z")


def receipt(tmp_path, *, claim=20, first_ref="WT-1", created=0, extra="", since=BASE, now=100):
    path = tmp_path / "activity.log"
    text = """2026-11-17 00:00:00 UTC  WT ENQUEUE WT-1 — natural ticket
2026-11-17 00:00:02 UTC  WT SPAWN_PLAN reconcile_id=r1 cause=initial_staffing eligible_after=0 requested=1 extra=x
2026-11-17 00:00:03 UTC  WT SPAWN_START worker_id=wt-abcdef12 pid=123
2026-11-17 00:00:09 UTC  WT SPAWN worker_id=wt-abcdef12 reconcile_id=r1
2026-11-17 00:00:09 UTC  WT DISPATCH WT-1 — spawned worker wt-abcdef12
"""
    if claim is not None:
        text += f"2026-11-17 00:00:{claim:02} UTC  WT CLAIM {first_ref} by wt-abcdef1 — first\n"
    path.write_text(text + extra)
    os.utime(path, (BASE+now, BASE+now))
    def lookup(ref):
        assert ref == "WT-1"
        return {"created_at": f"2026-11-17T00:00:{created:02}Z"} if created is not None else None
    return check(path, since=since, now=BASE+now, lookup=lookup)


def test_real_startup_has_separate_dispatch_and_agent_timings(tmp_path):
    result = receipt(tmp_path)
    assert result["status"] == "pass" and result["real_data"]
    sample = json.loads(result["evidence"])["samples"][0]
    assert sample["enqueue_to_process_s"] == 3
    assert sample["process_to_claim_s"] == 17


def test_slow_sample_is_not_hidden_by_later_fast_claim(tmp_path):
    result = receipt(tmp_path, claim=45, extra="2026-11-17 00:00:46 UTC WT CLAIM WT-2 by wt-abcdef1 — next\n")
    assert result["status"] == "fail" and result["real_data"]
    assert json.loads(result["evidence"])["slow_or_unclaimed"][0]["enqueue_to_claim_s"] == 45


def test_unclaimed_initial_worker_fails_after_threshold(tmp_path):
    assert receipt(tmp_path, claim=None, now=20)["status"] == "pending"
    assert receipt(tmp_path, claim=None, now=31)["status"] == "fail"


def test_backlog_and_leaked_fixtures_cannot_clear_real_data_watch(tmp_path):
    for kwargs in ({"first_ref": "WT-99"}, {"created": 50}, {"since": BASE+10}):
        result = receipt(tmp_path, **kwargs)
        assert result["status"] == "pending" and not result["real_data"]


def test_unavailable_source_is_not_success(tmp_path):
    assert receipt(tmp_path, created=None)["status"] == "pending"
    result = check(tmp_path / "absent", since=BASE, now=BASE+100, lookup=lambda ref: None)
    assert result["status"] == "fail" and not result["real_data"]


def test_rolling_window_does_not_reuse_old_success(tmp_path):
    assert receipt(tmp_path, now=86500)["status"] == "pending"


def test_explicit_run_and_earlier_capacity_gate_are_not_fresh_dispatch(tmp_path):
    for event in ("RUN WT-1 — run requested", "DISPATCH WT-1 — no action — capacity"):
        result = receipt(tmp_path, extra=f"2026-11-17 00:00:01 UTC WT {event}\n")
        assert result["status"] == "pending" and not result["real_data"]
        assert "WT-1" in json.loads(result["evidence"])["gated"]


def test_later_run_does_not_erase_original_slow_startup(tmp_path):
    result = receipt(tmp_path, claim=45, extra="2026-11-17 00:00:50 UTC WT RUN WT-1 — retry\n")
    assert result["status"] == "fail" and result["real_data"]

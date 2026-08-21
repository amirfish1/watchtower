"""SIDE-39 — reconciler watchdog that auto-bumps a queue past a model-floor
park.

FEAT-NEXT-120 made a claim on an under-tiered queue auto-park the ticket
blocked ("this ticket's floor is X, queue runs Y"). Left alone, that block
waits on a human forever. The watchdog under test
(``workers.bump_timeboxed_model_floor_blocks``) keys off ``blocked_at``: once
a recognizable floor-park has sat blocked past the queue's timebox, it bumps
the queue's model one same-engine tier, records an auto-answer, and reopens
the ticket for a stronger worker.

The non-negotiables asserted here:

* Fires only AFTER the timebox, and only on blocks carrying the claim-time
  floor-park question — an ordinary human-decision block is never
  auto-answered (that would silently erase a real question).
* One tier per bump, same engine only.
* Caps out: with no higher same-engine tier the ticket stays blocked.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

QUEUE = "FLOORQ"


def _park_floor_ticket(wt_env, run_cli, *, engine, model, floor, note="needs a bigger model"):
    """File a floor-carrying ticket and drive the real CLI claim path so it
    parks exactly the way production does (same block question, same stamps)."""
    wt_env.config.set_engine(QUEUE, engine)
    wt_env.config.set_model(QUEUE, model)
    wt_env.config.set_auto_drain(QUEUE, True)
    wt_env.config.set_grace_s(QUEUE, 0)
    item = wt_env.queue.enqueue(
        note=note, project=QUEUE, source="test", model_floor=floor,
    )
    res = run_cli("claim", "--queue", QUEUE, "--worker", "sess-floor")
    assert res.code == 1, res.output  # parked, not claimed
    parked = wt_env.queue.get(item["ref"])
    assert parked["needs_input"] is True
    return parked


def _backdate_block(wt_env, ref, minutes):
    """Rewind a ticket's ``blocked_at`` so the timebox reads as elapsed.

    Goes through the queue module's own load/save internals so the helper
    works against whichever store backend (JSON or SQLite) is live."""
    q = wt_env.queue
    past = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    with q._FileLock(q._lock_path()):
        data = q._load_unlocked()
        for it in data["items"]:
            if it.get("ref") == ref:
                it["blocked_at"] = past
        q._save_unlocked(data)


def test_bump_fires_after_timebox_and_reopens_one_tier_up(wt_env, run_cli):
    """Past the timebox: queue model climbs exactly one claude tier
    (sonnet-5 -> opus-4-8, NOT the kimi model that sits between them on the
    cross-engine ladder), the block is answered, and the ticket reopens."""
    parked = _park_floor_ticket(
        wt_env, run_cli,
        engine="claude", model="claude-sonnet-5", floor="claude-opus-5",
    )
    _backdate_block(wt_env, parked["ref"], minutes=31)

    result = wt_env.workers.bump_timeboxed_model_floor_blocks(reconcile_id="r-test")

    assert [r["action"] for r in result] == ["bumped"]
    assert result[0]["from_model"] == "claude-sonnet-5"
    assert result[0]["to_model"] == "claude-opus-4-8"
    assert wt_env.config.model(QUEUE) == "claude-opus-4-8"
    after = wt_env.queue.get(parked["ref"])
    assert after["status"] == "open"
    assert after["needs_input"] is False
    assert after["block_question"] == ""
    events = [e.get("event") for e in after.get("history", [])]
    assert "answer" in events  # the auto-answer is on the record


def test_bump_does_not_fire_before_timebox(wt_env, run_cli):
    parked = _park_floor_ticket(
        wt_env, run_cli,
        engine="claude", model="claude-sonnet-5", floor="claude-opus-5",
    )
    _backdate_block(wt_env, parked["ref"], minutes=5)

    result = wt_env.workers.bump_timeboxed_model_floor_blocks()

    assert result == []
    assert wt_env.config.model(QUEUE) == "claude-sonnet-5"
    after = wt_env.queue.get(parked["ref"])
    assert after["needs_input"] is True
    assert after["status"] == "in_progress"


def test_timebox_is_config_overridable_per_queue(wt_env, run_cli):
    """A queue-level ``model_floor_bump_minutes`` shortens (or lengthens) the
    default 30-minute timebox."""
    parked = _park_floor_ticket(
        wt_env, run_cli,
        engine="claude", model="claude-sonnet-5", floor="claude-opus-4-8",
    )
    cfg_file = wt_env.tmp / "queue-config.json"
    cfg = json.loads(cfg_file.read_text())
    cfg[QUEUE]["model_floor_bump_minutes"] = 10
    cfg_file.write_text(json.dumps(cfg))
    _backdate_block(wt_env, parked["ref"], minutes=11)

    result = wt_env.workers.bump_timeboxed_model_floor_blocks()

    assert [r["action"] for r in result] == ["bumped"]
    assert wt_env.config.model(QUEUE) == "claude-opus-4-8"


def test_human_decision_block_is_never_auto_answered(wt_env, run_cli):
    """A worker's real question to a human — even on a ticket that carries a
    model_floor, and even long past the timebox — must be left alone."""
    wt_env.config.set_engine(QUEUE, "claude")
    wt_env.config.set_model(QUEUE, "claude-opus-5")  # floor met: claim works
    wt_env.config.set_auto_drain(QUEUE, True)
    wt_env.config.set_grace_s(QUEUE, 0)
    item = wt_env.queue.enqueue(
        note="risky change", project=QUEUE, source="test",
        model_floor="claude-sonnet-5",
    )
    res = run_cli("claim", "--queue", QUEUE, "--worker", "sess-human")
    assert res.code == 0, res.output
    wt_env.queue.block(
        item["ref"], "sess-human",
        question="Should I drop the legacy table before migrating?",
    )
    _backdate_block(wt_env, item["ref"], minutes=600)

    result = wt_env.workers.bump_timeboxed_model_floor_blocks()

    assert result == []
    after = wt_env.queue.get(item["ref"])
    assert after["needs_input"] is True
    assert after["block_question"].startswith("Should I drop")
    assert wt_env.config.model(QUEUE) == "claude-opus-5"


def test_caps_at_engine_top_tier_and_leaves_ticket_blocked(wt_env, run_cli):
    """A kimi queue already at kimi's top ranked tier cannot reach a
    claude-opus-5 floor: no bump, no auto-answer, ticket stays parked for a
    human."""
    parked = _park_floor_ticket(
        wt_env, run_cli,
        engine="kimi", model="kimi-code/kimi-for-coding-highspeed",
        floor="claude-opus-5",
    )
    _backdate_block(wt_env, parked["ref"], minutes=31)

    result = wt_env.workers.bump_timeboxed_model_floor_blocks()

    assert [r["action"] for r in result] == ["left_blocked"]
    assert wt_env.config.model(QUEUE) == "kimi-code/kimi-for-coding-highspeed"
    after = wt_env.queue.get(parked["ref"])
    assert after["needs_input"] is True
    assert after["status"] == "in_progress"


def test_at_most_one_tier_per_queue_per_pass(wt_env, run_cli):
    """Two floor-parked tickets on one queue must not ladder the queue two
    tiers in a single pass; the second ticket waits for the next pass."""
    parked_a = _park_floor_ticket(
        wt_env, run_cli,
        engine="claude", model="claude-sonnet-5", floor="claude-opus-5",
    )
    item_b = wt_env.queue.enqueue(
        note="also needs the big model", project=QUEUE, source="test",
        model_floor="claude-opus-5",
    )
    res = run_cli("claim", "--queue", QUEUE, "--worker", "sess-floor-b")
    assert res.code == 1, res.output
    _backdate_block(wt_env, parked_a["ref"], minutes=31)
    _backdate_block(wt_env, item_b["ref"], minutes=31)

    result = wt_env.workers.bump_timeboxed_model_floor_blocks()

    assert wt_env.config.model(QUEUE) == "claude-opus-4-8"  # one tier only
    assert [r["action"] for r in result] == ["bumped"]  # second ticket deferred


def test_floor_already_met_reopens_without_bumping(wt_env, run_cli):
    """If a human raised the queue's model after the park, the watchdog just
    answers + reopens; it must not climb another tier on top."""
    parked = _park_floor_ticket(
        wt_env, run_cli,
        engine="claude", model="claude-sonnet-5", floor="claude-opus-4-8",
    )
    wt_env.config.set_model(QUEUE, "claude-opus-4-8")
    _backdate_block(wt_env, parked["ref"], minutes=31)

    result = wt_env.workers.bump_timeboxed_model_floor_blocks()

    assert [r["action"] for r in result] == ["reopened"]
    assert wt_env.config.model(QUEUE) == "claude-opus-4-8"
    after = wt_env.queue.get(parked["ref"])
    assert after["status"] == "open"
    assert after["needs_input"] is False


def test_reconciler_pass_runs_the_watchdog(wt_env, run_cli, monkeypatch):
    """End-to-end through ``reconcile_once``: the watchdog is wired into the
    real pass and its outcome lands in the result dict."""
    parked = _park_floor_ticket(
        wt_env, run_cli,
        engine="claude", model="claude-sonnet-5", floor="claude-opus-4-8",
    )
    _backdate_block(wt_env, parked["ref"], minutes=31)
    # Keep the pass from actually spawning anything for the reopened ticket.
    monkeypatch.setattr(wt_env.workers, "spawn_workers", lambda *a, **k: [])

    result = wt_env.workers.reconcile_once()

    assert [r["action"] for r in result["model_floor_bumped"]] == ["bumped"]
    assert wt_env.config.model(QUEUE) == "claude-opus-4-8"
    assert wt_env.queue.get(parked["ref"])["status"] == "open"

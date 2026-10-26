"""WT-10 — model floors are filtered at claim time and routed per ticket.

A worker never claims a ticket above its model (no park, no spawn-to-learn),
the reconciler spawns one run-once worker at the floor model pinned to that
ref, and the queue's configured model is never changed.
"""

from __future__ import annotations

QUEUE = "FLOORQ"


def _sonnet_queue(wt_env):
    wt_env.config.set_engine(QUEUE, "claude")
    wt_env.config.set_model(QUEUE, "claude-sonnet-5-5")
    wt_env.config.set_auto_drain(QUEUE, True)
    wt_env.config.set_grace_s(QUEUE, 0)


def test_every_priced_catalog_model_is_ranked_and_a_valid_floor(wt_env):
    from watchtower import models
    for eng in ("claude", "kimi"):
        for m in models.catalog(eng):
            assert models.rank(m) is not None, m
            assert wt_env.config.is_valid_model_floor(m)
    assert wt_env.config.is_valid_model_floor("")
    assert not wt_env.config.is_valid_model_floor("not-a-model")


def test_sonnet_5_5_queue_does_not_meet_an_opus_5_5_floor(wt_env):
    _sonnet_queue(wt_env)
    assert wt_env.config.model_floor_met(QUEUE, "claude-opus-5-5") is False
    assert wt_env.config.model_floor_met(QUEUE, "claude-sonnet-5-5") is True


def test_weak_worker_skips_floor_ticket_and_takes_next(wt_env, run_cli):
    _sonnet_queue(wt_env)
    a = wt_env.queue.enqueue(
        note="A", project=QUEUE, source="test", priority="p0",
        model_floor="claude-opus-5-5",
    )
    b = wt_env.queue.enqueue(note="B", project=QUEUE, source="test")
    res = run_cli("claim", "--queue", QUEUE, "--worker", "sess-weak", "--json")
    assert res.code == 0, res.output
    assert '"ref": "%s"' % b["ref"] in res.out
    after = wt_env.queue.get(a["ref"])
    assert after["status"] == "open" and not after["needs_input"]
    assert wt_env.queue.count_claimable(
        project=QUEUE, worker_model="claude-sonnet-5-5"
    ) == 0
    assert wt_env.queue.count_claimable(
        project=QUEUE, worker_model="claude-opus-5-5"
    ) == 1


def test_reconciler_spawns_one_floor_worker_and_keeps_queue_model(
    wt_env, monkeypatch
):
    _sonnet_queue(wt_env)
    a = wt_env.queue.enqueue(
        note="A", project=QUEUE, source="test", model_floor="claude-opus-5-5",
    )
    wt_env.queue.enqueue(note="B", project=QUEUE, source="test")
    monkeypatch.setattr(wt_env.workers, "engine_available", lambda e: True)
    spawned = []

    def fake_spawn(queue, ref, **kw):
        spawned.append((queue, ref, kw))
        return {"worker_id": "floorq-abc"}

    monkeypatch.setattr(wt_env.workers, "spawn_run_once_worker", fake_spawn)
    monkeypatch.setattr(wt_env.workers, "list_workers", lambda *a, **k: [])

    out = wt_env.workers.spawn_floor_routed_workers()

    assert [(s[0], s[1], s[2]["model"], s[2]["engine"]) for s in spawned] == [
        (QUEUE, a["ref"], "claude-opus-5-5", "claude")
    ]
    assert out[0]["action"] == "spawn"
    assert wt_env.config.model(QUEUE) == "claude-sonnet-5-5"


def test_no_second_spawn_while_a_floor_worker_is_live(wt_env, monkeypatch):
    _sonnet_queue(wt_env)
    a = wt_env.queue.enqueue(
        note="A", project=QUEUE, source="test", model_floor="claude-opus-5-5",
    )
    monkeypatch.setattr(wt_env.workers, "engine_available", lambda e: True)
    monkeypatch.setattr(
        wt_env.workers, "list_workers",
        lambda *a_, **k: [{"ref": a["ref"], "alive": True, "started_at": ""}],
    )
    monkeypatch.setattr(
        wt_env.workers, "spawn_run_once_worker",
        lambda *a_, **k: (_ for _ in ()).throw(AssertionError("respawned")),
    )
    assert wt_env.workers.spawn_floor_routed_workers() == []


def test_worker_model_prefers_the_spawn_record(wt_env, monkeypatch):
    _sonnet_queue(wt_env)
    assert wt_env.workers.worker_model("nobody", QUEUE) == "claude-sonnet-5-5"
    monkeypatch.setattr(wt_env.workers, "_load", lambda: {"workers": [
        {"worker_id": "floor-w", "engine": "claude", "model": "claude-opus-5-5"},
    ]})
    assert wt_env.workers.worker_model("floor-w", QUEUE) == "claude-opus-5-5"


def test_legacy_floor_park_is_reopened_without_bumping_the_queue(wt_env):
    _sonnet_queue(wt_env)
    item = wt_env.queue.enqueue(
        note="A", project=QUEUE, source="test", model_floor="claude-opus-5-5",
    )
    wt_env.queue.claim_by_ref(item["ref"], "sess-old")
    wt_env.queue.block(
        item["ref"], "sess-old",
        question=f"{wt_env.config.MODEL_FLOOR_BLOCK_PREFIX} 'claude-opus-5-5', but ...",
    )
    out = wt_env.workers.reopen_legacy_floor_parks()
    assert [r["action"] for r in out] == ["reopened"]
    assert wt_env.queue.get(item["ref"])["status"] == "open"
    assert wt_env.config.model(QUEUE) == "claude-sonnet-5-5"


def test_unapproved_model_pin_is_ignored_and_reported(wt_env, run_cli):
    wt_env.config.set_engine(QUEUE, "codex")
    wt_env.config.set_model(QUEUE, "gpt-9-imaginary")  # set_model stays permissive
    assert wt_env.config.model(QUEUE) != "gpt-9-imaginary"
    assert "gpt-9-imaginary" in wt_env.config.model_pin_warning(QUEUE)
    wt_env.queue.enqueue(note="x", project=QUEUE, source="test")
    res = run_cli("status", "--json")
    row = next(r for r in __import__("json").loads(res.out) if r["queue"] == QUEUE)
    assert row["worker_model"] == wt_env.config.model(QUEUE)
    assert "gpt-9-imaginary" in row["model_pin_warning"]
    wt_env.config.set_model(QUEUE, "gpt-5.5")
    assert wt_env.config.model(QUEUE) == "gpt-5.5"
    assert wt_env.config.model_pin_warning(QUEUE) == ""

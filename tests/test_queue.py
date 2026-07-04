from __future__ import annotations

import importlib
import json

import pytest


@pytest.fixture()
def wt(tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(tmp_path / "activity.log"))

    import watchtower.queue as q
    import watchtower.cli as cli

    importlib.reload(q)
    importlib.reload(cli)

    class Ns:
        pass

    ns = Ns()
    ns.store = tmp_path / "queue.json"
    ns.q = q
    ns.cli = cli
    return ns


def _events(timeline):
    return [event["event"] for event in timeline]


def test_mutations_append_canonical_history_and_stop_legacy_lists(wt):
    item = wt.q.enqueue(project="EVT", note="canonical log", source="test")
    assert _events(item["history"]) == ["filed"]
    assert item["history"][0]["by"] == {"kind": "system"}
    assert item["history"][0]["source"] == "test"
    assert item["history"][0]["project"] == "EVT"

    claimed = wt.q.claim_by_ref(item["ref"], "worker-a", session_uuid="11111111-2222-3333-4444-555555555555")
    assert claimed["history"][-1]["event"] == "claim"
    assert claimed["history"][-1]["by"] == {
        "kind": "worker",
        "worker": "worker-a",
        "session_id": "11111111-2222-3333-4444-555555555555",
    }

    blocked = wt.q.block(claimed["ref"], session_id="worker-a", question="ship it?", progress="ready except decision")
    assert "progress_notes" not in blocked
    assert _events(blocked["history"])[-2:] == ["progress", "block"]
    assert blocked["history"][-2]["text"] == "ready except decision"
    assert blocked["history"][-1]["question"] == "ship it?"

    answered = wt.q.answer(claimed["ref"], "yes", session_id="human-a")
    assert "answers" not in answered
    assert answered["history"][-1]["event"] == "answer"
    assert answered["history"][-1]["by"] == {"kind": "human", "worker": "human-a"}
    assert answered["history"][-1]["text"] == "yes"

    commented = wt.q.comment(claimed["ref"], "leaving a status note", by="human", session_id="human-a")
    assert commented["history"][-1]["event"] == "comment"
    assert commented["history"][-1]["text"] == "leaving a status note"

    edited = wt.q.update(claimed["ref"], priority="p1", value="H")
    assert edited["history"][-1]["event"] == "edit"
    assert edited["history"][-1]["fields"] == {"priority": "p1", "value": "H"}

    closed = wt.q.close(claimed["ref"], "worker-a", resolution={"summary": "done"})
    assert closed["history"][-1]["event"] == "close"
    assert closed["history"][-1]["resolution"] == {"summary": "done"}


def test_timeline_normalizes_old_answers_progress_sentinels_and_snapshot(wt):
    item = {
        "ref": "OLD-1",
        "project": "OLD",
        "source": "legacy",
        "created_at": "2026-07-04T00:00:00Z",
        "claimed_at": "2026-07-04T00:01:00Z",
        "claimed_by": "worker-a",
        "claimed_session_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "blocked_at": "2026-07-04T00:04:00Z",
        "block_question": "newest question",
        "closed_at": "2026-07-04T00:06:00Z",
        "closed_by": "worker-a",
        "resolution": {"summary": "fixed"},
        "history": [
            {"event": "claim", "at": "2026-07-04T00:01:00Z", "worker": "worker-a", "session_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"},
            {"event": "block", "at": "2026-07-04T00:02:00Z", "worker": "worker-a", "question": "old question"},
        ],
        "progress_notes": [
            {"at": "2026-07-04T00:01:30Z", "text": "analysis so far"},
            {"at": "2026-07-04T00:02:30Z", "text": "human note", "by": "human-comment"},
            {"at": "2026-07-04T00:03:30Z", "text": "reopened by person", "by": "human-reopen"},
        ],
        "answers": [
            {"at": "2026-07-04T00:03:00Z", "text": "try option A", "by": "amir"},
        ],
    }

    timeline = wt.q.timeline(item)

    assert _events(timeline) == [
        "filed",
        "claim",
        "progress",
        "block",
        "comment",
        "answer",
        "reopen",
        "close",
    ]
    assert timeline[1]["by"] == {
        "kind": "worker",
        "worker": "worker-a",
        "session_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
    }
    assert timeline[4]["event"] == "comment"
    assert timeline[4]["text"] == "human note"
    assert timeline[6]["event"] == "reopen"
    assert timeline[6]["reason"] == "reopened by person"
    assert timeline[7]["resolution"] == {"summary": "fixed"}


def test_timeline_synthesizes_snapshot_only_ticket(wt):
    item = {
        "ref": "SNAP-1",
        "project": "SNAP",
        "source": "wt",
        "created_at": "2026-07-04T00:00:00Z",
        "claimed_at": "2026-07-04T00:01:00Z",
        "claimed_by": "worker-a",
        "blocked_at": "2026-07-04T00:02:00Z",
        "block_question": "what now?",
        "closed_at": "2026-07-04T00:03:00Z",
        "closed_by": "worker-a",
        "resolution": "done",
    }

    timeline = wt.q.timeline(item)

    assert _events(timeline) == ["filed", "claim", "block", "close"]
    assert timeline[0]["at"] == "2026-07-04T00:00:00Z"
    assert timeline[2]["question"] == "what now?"
    assert timeline[3]["resolution"] == {"summary": "done"}


def test_timeline_preserves_multi_round_block_answer(wt):
    item = wt.q.enqueue(project="ROUND", note="two rounds")
    claimed = wt.q.claim_by_ref(item["ref"], "worker-a")
    wt.q.block(claimed["ref"], session_id="worker-a", question="first?")
    wt.q.answer(claimed["ref"], "first answer", session_id="human-a")
    wt.q.block(claimed["ref"], session_id="worker-a", question="second?")
    wt.q.answer(claimed["ref"], "second answer", session_id="human-a")

    timeline = wt.q.timeline(wt.q.get(claimed["ref"]))

    assert _events(timeline) == ["filed", "claim", "block", "answer", "block", "answer"]
    assert [e.get("question") for e in timeline if e["event"] == "block"] == ["first?", "second?"]
    assert [e.get("text") for e in timeline if e["event"] == "answer"] == ["first answer", "second answer"]


def test_find_json_includes_timeline(wt, capsys):
    item = wt.q.enqueue(project="FINDTL", note="show activity")
    wt.q.comment(item["ref"], "visible in find", by="human", session_id="human-a")

    rc = wt.cli.main(["find", item["ref"], "--json"])

    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["ref"] == item["ref"]
    assert _events(out["timeline"]) == ["filed", "comment"]


def test_timeline_same_timestamp_precedence(wt):
    """filed (synthesized) must sort before claim (real history) at same timestamp.

    The WT-95/WT-96 bug: wt take files+claims within one second, so
    created_at == claimed_at. Old/current tickets that store only 'claim' in
    history (not 'filed') synthesize 'filed' from created_at. Without the
    precedence tier the stable sort leaves claim first.
    """
    ts = "2026-07-04T12:00:00Z"
    item = {
        "created_at": ts,
        "claimed_at": ts,
        "claimed_by": "w-test",
        "claimed_session_id": None,
        # history has only claim — filed is absent and will be synthesized
        "history": [
            {"event": "claim", "at": ts, "by": {"kind": "worker", "worker": "w-test"}},
        ],
    }
    tl = wt.q.timeline(item)
    assert _events(tl) == ["filed", "claim"]

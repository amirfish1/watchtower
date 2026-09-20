from __future__ import annotations

import importlib
import json

import pytest


@pytest.fixture()
def wt(tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(tmp_path / "activity.log"))
    monkeypatch.delenv("WATCHTOWER_MACHINE", raising=False)

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
        "machine": wt.q.machine_tag(),
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


def test_ticket_updates_appear_in_activity_log(wt):
    item = wt.q.enqueue(project="EVT", note="activity log", source="test")
    wt.q.claim_by_ref(item["ref"], "worker-a")
    wt.q.block(
        item["ref"],
        session_id="worker-a",
        question="Which option?",
        progress="Investigated both options.",
    )
    wt.q.comment(item["ref"], "Use option A.")
    wt.q.answer(item["ref"], "Approved.")

    log = (wt.store.parent / "activity.log").read_text()
    assert f"PROGRESS {item['ref']} — Investigated both options." in log
    assert f"BLOCK    {item['ref']} — Which option?" in log
    assert f"COMMENT  {item['ref']} — Use option A." in log
    assert f"ANSWER   {item['ref']} — Approved." in log


def test_cli_edit_text_replaces_file_backed_ticket_body(wt, capsys):
    item = wt.q.enqueue(
        project="EDIT",
        note="short summary",
        text="original body",
        source="test",
    )

    assert wt.cli.main(["edit", item["ref"], "--text", "replacement body"]) == 0
    capsys.readouterr()

    edited = wt.q.get(item["ref"])
    assert edited["text"] == "replacement body"
    assert edited["note"] == "short summary"
    assert edited["history"][-1]["event"] == "edit"
    assert edited["history"][-1]["fields"] == {"text": "replacement body"}


def test_cli_block_json_returns_the_blocked_ticket(wt, capsys):
    item = wt.q.enqueue(project="BLOCK", note="needs a decision", source="test")
    wt.q.claim_by_ref(item["ref"], "worker-a")

    assert wt.cli.main([
        "block", item["ref"], "--worker", "worker-a",
        "--question", "approve the rollout?", "--progress", "verified the options",
        "--json",
    ]) == 0

    blocked = json.loads(capsys.readouterr().out)
    assert blocked["ref"] == item["ref"]
    assert blocked["status"] == "in_progress"
    assert blocked["block_question"] == "approve the rollout?"


def test_cli_comment_injects_guidance_into_claimed_worker(wt, monkeypatch, capsys):
    item = wt.q.enqueue(project="EVT", note="canonical log", source="test")
    sid = "11111111-2222-3333-4444-555555555555"
    claimed = wt.q.claim_by_ref(item["ref"], "worker-a", session_uuid=sid)
    calls = []

    import watchtower.messages as messages

    monkeypatch.setattr(
        messages,
        "send",
        lambda target, text, **kwargs: calls.append((target, text, kwargs))
        or {"ok": True, "transport": "fifo"},
    )

    assert wt.cli.main(["comment", claimed["ref"], "Use the safer parser."]) == 0

    assert len(calls) == 1
    target, text, kwargs = calls[0]
    assert target == sid
    assert text == (
        f"[WATCHTOWER] A new comment was added to your claimed ticket "
        f"{claimed['ref']}:\n\nUse the safer parser."
    )
    # A comment must not cost the worker its in-flight turn: the steer verb
    # prefers the peer socket, which cannot truncate one (CCC-1000).
    assert kwargs["prefer_uds"] is True
    assert kwargs["mode"] == "steer"
    assert kwargs["force_queue"] is False
    assert "injected into claimed worker via fifo" in capsys.readouterr().out


def test_cli_comment_by_claimant_is_not_echoed_back(wt, monkeypatch, capsys):
    """A session's own comment must not be steered back into that session
    (WATCHTOWER-21). Matches on either the worker id or the harness session id."""
    import watchtower.messages as messages

    monkeypatch.setattr(
        messages,
        "send",
        lambda *a, **kw: pytest.fail("author must not be injected with own comment"),
    )
    monkeypatch.delenv("CLAUDE_CODE_SESSION_ID", raising=False)
    monkeypatch.delenv("CODEX_THREAD_ID", raising=False)

    by_worker = wt.q.enqueue(project="EVT", note="worker id match", source="test")
    wt.q.claim_by_ref(by_worker["ref"], "worker-a")
    assert wt.cli.main(
        ["comment", by_worker["ref"], "ready for gate", "--worker", "worker-a"]
    ) == 0
    assert "not injected back at you" in capsys.readouterr().out

    sid = "11111111-2222-3333-4444-555555555555"
    by_session = wt.q.enqueue(project="EVT", note="session id match", source="test")
    wt.q.claim_by_ref(by_session["ref"], "worker-b", session_uuid=sid)
    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", sid)
    assert wt.cli.main(["comment", by_session["ref"], "ready for gate"]) == 0
    assert "not injected back at you" in capsys.readouterr().out

    # The comment is still durably recorded — only the echo is suppressed.
    events = [e for e in wt.q.get(by_session["ref"])["history"]
              if e.get("event") == "comment"]
    assert [e["text"] for e in events] == ["ready for gate"]


def test_cli_comment_from_another_session_still_injects(wt, monkeypatch, capsys):
    """The guard is author-scoped: a coordinator commenting on someone else's
    claimed ticket must still reach the worker."""
    item = wt.q.enqueue(project="EVT", note="canonical log", source="test")
    sid = "11111111-2222-3333-4444-555555555555"
    claimed = wt.q.claim_by_ref(item["ref"], "worker-a", session_uuid=sid)
    calls = []

    import watchtower.messages as messages

    monkeypatch.setattr(
        messages,
        "send",
        lambda target, text, **kwargs: calls.append(target)
        or {"ok": True, "transport": "fifo"},
    )
    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID",
                       "99999999-8888-7777-6666-555555555555")

    assert wt.cli.main(
        ["comment", claimed["ref"], "Use the safer parser.", "--worker", "coordinator"]
    ) == 0
    assert calls == [sid]
    assert "injected into claimed worker via fifo" in capsys.readouterr().out


def test_cli_comment_on_unclaimed_ticket_does_not_send(wt, monkeypatch, capsys):
    item = wt.q.enqueue(project="EVT", note="canonical log", source="test")

    import watchtower.messages as messages

    monkeypatch.setattr(
        messages,
        "send",
        lambda *args, **kwargs: pytest.fail("unclaimed comment must not send"),
    )

    assert wt.cli.main(["comment", item["ref"], "For later."]) == 0
    assert capsys.readouterr().out.strip() == f"COMMENTED: {item['ref']}"


def test_close_ownership_guard_blocks_reap_duplicate(wt):
    """A worker reaped mid-ticket, whose claim was re-drained + closed by a
    fresh worker, must NOT be able to silently re-close and clobber the real
    resolution (CCC-502 double-close)."""
    item = wt.q.enqueue(project="OWN", note="dropdown bug", source="test")
    wt.q.claim_by_ref(item["ref"], "worker-a")
    # worker-a is reaped; the reconciler reopens and worker-b re-drains + closes.
    wt.q.update_status(item["ref"], "open", reason="worker gone")
    wt.q.claim_by_ref(item["ref"], "worker-b")
    real = wt.q.close(item["ref"], "worker-b", resolution={"summary": "real fix"})
    assert real["resolution"] == {"summary": "real fix"}

    # worker-a resumes from stale context and tries to close it too -> rejected.
    with pytest.raises(ValueError, match="already closed"):
        wt.q.close(item["ref"], "worker-a", resolution={"summary": "duplicate fix"})
    # The real resolution is untouched.
    assert wt.q.get(item["ref"])["resolution"] == {"summary": "real fix"}
    # A close event was NOT appended for the rejected attempt.
    assert _events(wt.q.get(item["ref"])["history"]).count("close") == 1


def test_close_guard_rejects_crosscloser_but_allows_force_and_unclaimed(wt):
    """Only the claimant may close an in-progress ticket without --force."""
    # A different worker must not close a still-open claim.
    x = wt.q.enqueue(project="OWN", note="crossclose", source="test")
    wt.q.claim_by_ref(x["ref"], "worker-a")
    with pytest.raises(ValueError, match="claimed by worker-a"):
        wt.q.close(x["ref"], "worker-b", resolution="closed by b")
    assert wt.q.get(x["ref"])["status"] == "in_progress"

    # Own in_progress ticket closes normally.
    a = wt.q.enqueue(project="OWN", note="mine", source="test")
    wt.q.claim_by_ref(a["ref"], "worker-a")
    assert wt.q.close(a["ref"], "worker-a", resolution="ok")["status"] == "closed"

    # force lets a human re-close an already-closed ticket.
    forced = wt.q.close(a["ref"], "human-x", resolution="override", force=True)
    assert forced["status"] == "closed"

    # dedup-close by ref (no session_id -> expect_owner empty) is unguarded.
    b = wt.q.enqueue(project="OWN", note="dupe", source="test")
    assert wt.q.close(b["ref"], resolution="duplicate of OWN-1")["status"] == "closed"


def test_cli_ready_reopens_a_closed_file_backed_ticket(wt, capsys):
    """`wt ready` must make a previously closed local ticket claimable again."""
    item = wt.q.enqueue(project="LOCAL", note="retry this", source="test")
    wt.q.close(item["ref"], "worker-a", resolution={"summary": "first attempt"})

    assert wt.cli.main(["ready", item["ref"], "--no-dispatch"]) == 0
    reopened = wt.q.get(item["ref"])

    assert reopened["status"] == "open"
    assert reopened["claimed_by"] is None
    assert reopened["closed_at"] is None
    # Reopened AND marked run_requested: `wt ready` is the ▶ path, so it leaves
    # the same request behind on a file-backed ticket as on a GitHub one.
    assert [h["event"] for h in reopened["history"]][-2:] == ["reopen", "run_requested"]
    assert reopened["run_requested"] is True
    assert "RUNNABLE: LOCAL-1" in capsys.readouterr().out


def test_reopen_returns_closed_ticket_to_open_pool(wt):
    item = wt.q.enqueue(project="REOP", note="not actually done", source="test")
    wt.q.close(item["ref"], "worker-a", resolution={"summary": "premature"})

    reopened = wt.q.reopen(item["ref"], reason="regressed in staging")

    assert reopened["status"] == "open"
    assert reopened["claimed_by"] is None
    assert reopened["closed_at"] is None
    assert "resolution" not in reopened
    assert "closed_by" not in reopened
    assert reopened["history"][-1]["event"] == "reopen"
    assert reopened["history"][-1]["reason"] == "regressed in staging"
    assert reopened["history"][-1]["by"]["kind"] == "human"
    # The old claim handle survives so `wt discuss` can still resume context.
    assert reopened["claimed_session_id"] == item["claimed_session_id"]


def test_reopen_refuses_already_open_and_missing(wt):
    item = wt.q.enqueue(project="REOP", note="fine as-is", source="test")
    with pytest.raises(ValueError, match="already open"):
        wt.q.reopen(item["ref"])
    assert wt.q.reopen("REOP-999") is None


def test_reopen_refuses_blocked_ticket_unless_force(wt):
    item = wt.q.enqueue(project="REOP", note="blocked one", source="test")
    claimed = wt.q.claim_by_ref(item["ref"], "worker-a")
    wt.q.block(claimed["ref"], session_id="worker-a", question="which way?")

    with pytest.raises(ValueError, match="wt answer"):
        wt.q.reopen(item["ref"])

    forced = wt.q.reopen(item["ref"], reason="block went stale", force=True)
    assert forced["status"] == "open"
    assert forced["needs_input"] is False


def test_cli_reopen_closed_ticket(wt, capsys):
    item = wt.q.enqueue(project="REOP", note="retry me", source="test")
    wt.q.close(item["ref"], "worker-a", resolution={"summary": "wrong fix"})

    assert wt.cli.main(["reopen", item["ref"], "--reason", "still broken"]) == 0
    out = capsys.readouterr().out
    assert "REOPENED: REOP-1" in out
    assert "still broken" in out
    assert wt.q.get(item["ref"])["status"] == "open"


def test_file_backed_run_request_is_set_cleared_and_counted(wt):
    """Backend parity: ▶ on a file-backed ticket leaves the same
    ``run_requested`` state a GitHub-backed one does, and can be withdrawn."""
    item = wt.q.enqueue(project="LOCAL", note="press play", source="test")
    assert item["run_requested"] is False
    assert wt.q.count_manual_eligible(project="LOCAL") == 0

    requested = wt.q.mark_runnable(item["ref"])
    assert requested["run_requested"] is True
    assert requested["history"][-1]["event"] == "run_requested"
    assert wt.q.count_manual_eligible(project="LOCAL") == 1

    cleared = wt.q.clear_run_request(item["ref"])
    assert cleared["run_requested"] is False
    assert cleared["history"][-1]["event"] == "run_request_cleared"
    assert wt.q.count_manual_eligible(project="LOCAL") == 0
    # Withdrawing a request touches nothing else about the ticket.
    assert cleared["status"] == "open"


def test_run_request_on_an_unclaimable_ticket_is_not_counted(wt):
    """A ▶ on a ticket claim_next would never hand out (needs-spec) must not
    make the reconciler think there is a run to staff."""
    item = wt.q.enqueue(project="LOCAL", note="half an idea", readiness="needs-spec")
    wt.q.mark_runnable(item["ref"])
    assert wt.q.count_manual_eligible(project="LOCAL") == 0


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
    claimed = wt.q.claim_by_ref(
        item["ref"], "worker-a",
        session_uuid="11111111-2222-3333-4444-555555555555",
    )
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


def test_machine_tag_env_override_and_derivation(wt, monkeypatch):
    """WATCHTOWER_MACHINE wins; otherwise the tag is derived from the
    hostname's first label, lowercase alnum, first 3 chars."""
    monkeypatch.setenv("WATCHTOWER_MACHINE", "Hermes-VM")
    wt.q._MACHINE_TAG_CACHE = None
    assert wt.q.machine_tag() == "her"

    monkeypatch.delenv("WATCHTOWER_MACHINE")
    wt.q._MACHINE_TAG_CACHE = None
    monkeypatch.setattr("socket.gethostname", lambda: "Bobs-Mac-mini.local")
    assert wt.q.machine_tag() == "bob"


def test_worker_events_carry_machine_tag(wt, monkeypatch):
    """claim/progress/block/close events record by.machine so a shared queue's
    timeline can show which host the worker ran on (the human/system events
    around them don't claim a machine)."""
    monkeypatch.setenv("WATCHTOWER_MACHINE", "tst")
    wt.q._MACHINE_TAG_CACHE = None

    item = wt.q.enqueue(project="MAC", note="machine tag")
    assert item["history"][0]["by"] == {"kind": "system"}

    claimed = wt.q.claim_by_ref(item["ref"], "worker-a")
    assert claimed["claimed_machine"] == "tst"
    claim_by = claimed["history"][-1]["by"]
    assert claim_by["worker"] == "worker-a" and claim_by["machine"] == "tst"

    blocked = wt.q.block(item["ref"], session_id="worker-a",
                         question="ok?", progress="halfway")
    assert blocked["history"][-2]["by"]["machine"] == "tst"
    assert blocked["history"][-1]["by"]["machine"] == "tst"

    closed = wt.q.close(item["ref"], "worker-a",
                        resolution={"summary": "done"}, force=True)
    assert closed["closed_machine"] == "tst"
    assert closed["history"][-1]["by"]["machine"] == "tst"

    # Human events (answer/comment) carry no machine tag.
    assert all(
        "machine" not in (e.get("by") or {})
        for e in closed["history"]
        if (e.get("by") or {}).get("kind") in ("human", "system")
    )


def test_machine_tag_survives_timeline_and_reopen_clears(wt, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_MACHINE", "tst")
    wt.q._MACHINE_TAG_CACHE = None

    item = wt.q.enqueue(project="MAC2", note="reopen clears machine")
    claimed = wt.q.claim_by_ref(item["ref"], "worker-a")
    timeline = wt.q.timeline(wt.q.get(item["ref"]))
    assert timeline[-1]["by"]["machine"] == "tst"

    reopened = wt.q.reopen(item["ref"], reason="retry", force=True)
    assert reopened.get("claimed_machine") is None
    assert reopened.get("closed_machine") is None


def test_with_machine_prefix_display(wt):
    assert wt.q.with_machine("bym-zxcv", "her") == "her-bym-zxcv"
    assert wt.q.with_machine("her-bym-zxcv", "her") == "her-bym-zxcv"
    assert wt.q.with_machine("bym-zxcv", "") == "bym-zxcv"
    assert wt.q.with_machine("", "her") == ""
    assert wt.q.with_machine(None, None) == ""

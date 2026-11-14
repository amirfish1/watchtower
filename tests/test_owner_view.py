"""Owner view (BECKY queue reform, plan section 8 items 2-7): who a blocked
ticket really waits on, on both backends, with owner context (root, decide-by
with a time zone, consequence), root grouping, visible unclassified blocks, a
re-check flag that never implies completion, an explicit dependency host and
a rule-revision notice on resume."""
from __future__ import annotations

import importlib
import os
from datetime import datetime, timedelta, timezone

import pytest

from tests.test_github_backend import (  # noqa: F401 (fixtures/helpers)
    _drainable, _install_fake_gh, _reload_isolated,
)

NOW = datetime(2026, 11, 14, 18, 0, tzinfo=timezone.utc)


def _load(tmp_path, monkeypatch, backend):
    """(q, cli, answers, owner_view, queue name) on the file or GitHub backend."""
    if backend == "github":
        _install_fake_gh(tmp_path, monkeypatch)
        config, q = _reload_isolated(tmp_path, monkeypatch)
        config.set_backend("GHI", "github")
        config.set_github_repo("GHI", "test-owner/test-repo")
        _drainable(config)
        name = "GHI"
    else:
        monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
        monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(tmp_path / "activity.log"))
        import watchtower.queue as q
        importlib.reload(q)
        name = "T"
    import watchtower.answers as answers
    import watchtower.cli as cli
    import watchtower.owner_view as owner_view
    for m in (answers, cli, owner_view):
        importlib.reload(m)
    return q, cli, answers, owner_view, name


@pytest.fixture(params=["file", "github"])
def env(request, tmp_path, monkeypatch):
    return _load(tmp_path, monkeypatch, request.param)


def _ticket(q, name, note="x", worker="w1"):
    item = q.enqueue(project=name, note=note, source="test")
    q.claim_by_ref(item["ref"], worker)
    return item["ref"]


def _rows(view):
    return {r["ref"]: r for g in view["groups"] for r in g["members"]}


def test_unlabelled_block_is_listed_as_unclassified_not_owner(env):
    q, _cli, _a, ov, name = env
    plain = _ticket(q, name)
    q.block(plain, session_id="w1", question="Approve the draft to the client?")
    owned = _ticket(q, name)
    q.block(owned, session_id="w1", question="Refund or credit?", wait_on="owner")
    view = ov.build(q.list_items(), project=name, now=NOW)
    rows = _rows(view)
    assert rows[plain]["wait"] == "unclassified"
    assert rows[owned]["wait"] == "owner"
    assert view["unclassified"] == [plain]           # visible in its own list
    assert view["counts"]["owner"] == 1 and view["counts"]["unclassified"] == 1
    assert view["blocked"] == 2                      # nothing dropped
    assert plain in ov.render(view)


def test_current_wait_beats_a_stale_owner_question(env):
    """A withdrawn or already-answered owner question keeps its old text; the
    current wait record says a worker has it, so it is not an owner item."""
    q, _cli, _a, ov, name = env
    ref = _ticket(q, name)
    q.block(ref, session_id="w1", question="[Q1025:access] Approve the saved draft to the studio?",
            wait_on="owner")
    q.set_wait(ref, "worker", note="Question withdrawn: draft dropped, remaining work is a release")
    view = ov.build(q.list_items(), project=name, now=NOW)
    row = _rows(view)[ref]
    assert row["wait"] == "worker"
    assert "Approve the saved draft" in row["question"]     # text kept, not trusted
    assert view["counts"]["owner"] == 0 and not view["unclassified"]
    assert not any(g["needs_owner"] for g in view["groups"])
    # The wait note (withdrawn, and what is still owed) is shown; the ticket stays blocked.
    assert "Question withdrawn: draft dropped, remaining work is a release" in ov.render(view)
    assert q.get(ref)["needs_input"] and view["blocked"] == 1


def test_owner_context_is_stored_and_needs_a_time_zone(env):
    q, _cli, _a, ov, name = env
    ref = _ticket(q, name)
    with pytest.raises(ValueError, match="time zone"):
        q.block(ref, session_id="w1", question="Restore the booking?", wait_on="owner",
                wait_extra={"decide_by": "2026-11-17T09:00:00"})
    assert not q.get(ref).get("needs_input")                 # refused before any write
    with pytest.raises(ValueError, match="owner wait"):
        q.block(ref, session_id="w1", question="?", wait_on="dependency", wait_ref="T-1",
                wait_extra={"decide_by": "none"})
    with pytest.raises(ValueError, match="owner wait"):
        q.block(ref, session_id="w1", question="?", wait_extra={"consequence": "none"})
    q.block(ref, session_id="w1", question="Restore the booking?", wait_on="owner",
            wait_extra={"decide_by": "2026-11-17T09:00:00-08:00",
                        "consequence": "Client arrives for the wrong class type"})
    w = q.get(ref)["wait"]
    assert w["decide_by"] == "2026-11-17T09:00:00-08:00"
    assert w["consequence"].startswith("Client arrives")
    row = _rows(ov.build(q.list_items(), project=name, now=NOW))[ref]
    assert row["missing"] == [] and row["recheck_due"] is False
    # 'none' is an explicit, valid answer -- not a missing field.
    q.set_wait(ref, "owner", extra={"decide_by": "none", "consequence": "none"})
    view = ov.build(q.list_items(), project=name, now=NOW)
    assert _rows(view)[ref]["missing"] == [] and not view["owner_missing_context"]


def test_owner_wait_without_context_is_reported_missing(env):
    q, _cli, _a, ov, name = env
    ref = _ticket(q, name)
    q.block(ref, session_id="w1", question="Which option?", wait_on="owner")
    view = ov.build(q.list_items(), project=name, now=NOW)
    assert view["owner_missing_context"] == [{"ref": ref, "missing": ["decide_by", "consequence"]}]


def test_passed_decide_by_flags_a_recheck_and_never_completion(env):
    q, _cli, _a, ov, name = env
    ref = _ticket(q, name)
    q.block(ref, session_id="w1", question="Approve the staff question?", wait_on="owner",
            wait_extra={"decide_by": "2026-11-09T13:00:00-05:00",
                        "consequence": "Nobody confirms who teaches the 1 PM private"})
    before = q.get(ref)
    view = ov.build(q.list_items(), project=name, now=NOW)
    assert [r["ref"] for r in view["recheck_due"]] == [ref]
    assert view["groups"][0]["root"] == ref and view["groups"][0]["recheck_due"]
    assert "does NOT mean handled" in ov.render(view)
    after = q.get(ref)
    # Read-only: still blocked, still the owner's, still open.
    assert after["needs_input"] and after["status"] == before["status"] != "closed"
    assert after["wait"] == before["wait"] and _rows(view)[ref]["wait"] == "owner"
    later = ov.build(q.list_items(), project=name, now=NOW + timedelta(days=30))
    assert [r["ref"] for r in later["recheck_due"]] == [ref]   # stays flagged; never ages out


def test_groups_by_root_with_dependents(env):
    q, _cli, _a, ov, name = env
    root = _ticket(q, name, "root incident")
    q.block(root, session_id="w1", question="Raise the platform limit?", wait_on="owner")
    mid = _ticket(q, name, "same incident, other route", "w2")
    q.block(mid, session_id="w2", question="Await root", wait_on="dependency", wait_ref=root)
    leaf = _ticket(q, name, "follower", "w3")
    q.block(leaf, session_id="w3", question="Await mid", wait_on="dependency", wait_ref=mid)
    linked = _ticket(q, name, "same decision, own ticket", "w4")
    q.block(linked, session_id="w4", question="Same question", wait_on="owner",
            wait_extra={"root": root})
    view = ov.build(q.list_items(), project=name, now=NOW)
    assert len(view["groups"]) == 1
    g = view["groups"][0]
    assert g["root"] == root and g["root_state"] == "blocked:owner" and g["needs_owner"]
    assert sorted(g["dependents"]) == sorted([mid, leaf, linked])
    assert g["members"][0]["ref"] == root
    assert f"+ 3 waiting on it" in ov.render(view)


def test_dependency_closed_with_caveats_is_stuck_and_does_not_wake(env):
    q, _cli, answers, ov, name = env
    dep = _ticket(q, name, "dependency", "w2")
    a = _ticket(q, name, "dependent")
    q.block(a, session_id="w1", question="Await dep", wait_on="dependency", wait_ref=dep)
    q.update_status(dep, "closed", session_id="w2",
                    resolution={"summary": "partly landed", "unresolved": ["migration not applied"]})
    assert answers.resume_dependency_waits() == []
    assert q.get(a).get("needs_input")
    view = ov.build(q.list_items(), project=name, now=NOW)
    assert view["stuck_dependencies"] == [{"ref": a, "waits_on": dep, "state": "stuck"}]


def test_dependency_ref_missing_here_is_reported_not_guessed(env):
    q, _cli, answers, ov, name = env
    a = _ticket(q, name)
    q.block(a, session_id="w1", question="Await OPS-1635", wait_on="dependency",
            wait_ref="OPS-1635", wait_extra={"ref_host": "ami"})
    assert q.get(a)["wait"]["ref_host"] == "ami"
    assert answers.resume_dependency_waits() == []
    view = ov.build(q.list_items(), project=name, now=NOW)
    (d,) = view["unresolved_dependencies"]
    assert d["ref"] == a and d["waits_on"] == "OPS-1635"
    assert d["state"] == "not_found_here" and d["lives_on"] == "ami"
    with pytest.raises(ValueError, match="dependency"):
        q.set_wait(a, "owner", extra={"ref_host": "ami"})


def test_same_local_ref_on_two_hosts_wakes_only_on_the_named_host(tmp_path, monkeypatch):
    """BECKY-3448 on OPS-1635: the wait was written on 'her' but the local
    ticket lives on 'ami'. With ref_host, 'her' must not answer from its own
    same-number ticket; 'ami' may."""
    q, _cli, answers, ov, _ = _load(tmp_path, monkeypatch, "github")
    dep = q.enqueue(project="PK", note="a local ticket with the same number", source="test")
    q.claim_by_ref(dep["ref"], "worker-2")
    q.update_status(dep["ref"], "closed", session_id="worker-2", resolution={"summary": "landed"})
    a = q.enqueue(project="GHI", note="shared dependent")
    monkeypatch.setattr(q, "_MACHINE_TAG_CACHE", "her")
    q.claim_by_ref(a["ref"], "worker-1")
    q.block(a["ref"], session_id="worker-1", question="wait", wait_on="dependency",
            wait_ref=dep["ref"], wait_extra={"ref_host": "ami"})
    assert q.get(a["ref"])["wait"]["host"] == "her"
    assert answers.resume_dependency_waits() == []              # writer host: not its ticket
    assert q.get(a["ref"]).get("needs_input")
    # A same-number LOCAL ticket that is blocked on the owner must not adopt the remote wait.
    q.update_status(dep["ref"], "open")
    q.claim_by_ref(dep["ref"], "worker-2")
    q.block(dep["ref"], session_id="worker-2", question="UNRELATED LOCAL DECISION", wait_on="owner")
    view = ov.build(q.list_items(), project="GHI", now=NOW)
    (d,) = view["unresolved_dependencies"]
    assert d["state"] == "other_host" and d["lives_on"] == "ami"
    (g,) = view["groups"]
    assert g["root"] == f"{dep['ref']}@ami" and g["root_state"] == "other_host"
    assert g["root_title"] == "" and not g["needs_owner"] and g["dependents"] == [a["ref"]]
    assert "UNRELATED LOCAL DECISION" not in ov.render(view)
    whole = ov.build(q.list_items(), now=NOW)                 # the local ticket is its own root
    assert {x["root"] for x in whole["groups"]} == {f"{dep['ref']}@ami", dep["ref"]}
    q.answer(dep["ref"], "decided")
    q.update_status(dep["ref"], "closed", session_id="worker-2", resolution={"summary": "landed"})
    assert answers.resume_dependency_waits() == []              # still not this host's to answer
    monkeypatch.setattr(q, "_MACHINE_TAG_CACHE", "ami")
    assert answers.resume_dependency_waits() == [f"{a['ref']}:{dep['ref']}"]
    assert not q.get(a["ref"]).get("needs_input")


def test_recovery_listing_is_unchanged(env):
    """`wt blocked` / list_blocked keep meaning every needs_input ticket."""
    q, _cli, _a, _ov, name = env
    refs = []
    for on in ("", "owner", "worker", "external"):
        ref = _ticket(q, name)
        q.block(ref, session_id="w1", question="q", wait_on=on)
        refs.append(ref)
    assert sorted(i["ref"] for i in q.list_blocked(project=name)) == sorted(refs)


def test_cli_owner_view_and_hints(env, capsys):
    q, cli, _a, _ov, name = env
    ref = _ticket(q, name)
    assert cli.main(["block", ref, "--worker", "w1", "--question", "Approve?"]) == 0
    out = capsys.readouterr()
    assert "no --wait-on given" in out.err and "BLOCKED" in out.out     # a note, not an error
    ref2 = _ticket(q, name)
    assert cli.main(["block", ref2, "--worker", "w1", "--question", "Approve?",
                     "--wait-on", "owner"]) == 0
    assert "--decide-by" in capsys.readouterr().err
    ref3 = _ticket(q, name)
    assert cli.main(["block", ref3, "--worker", "w1", "--question", "Approve?", "--wait-on", "owner",
                     "--decide-by", "2026-11-17T09:00:00-08:00", "--if-unanswered", "none",
                     "--root", ref2]) == 0
    assert capsys.readouterr().err.strip() == ""
    assert cli.main(["block", _ticket(q, name), "--worker", "w1", "--question", "?",
                     "--wait-on", "owner", "--decide-by", "tomorrow"]) == 1
    capsys.readouterr()
    assert cli.main(["wait-on", ref, "--on", "owner", "--decide-by", "none",
                     "--if-unanswered", "none"]) == 0
    capsys.readouterr()
    assert cli.main(["owner-view", "-q", name]) == 0
    text = capsys.readouterr().out
    assert "By root:" in text and ref2 in text and "+ 1 waiting on it" in text


def test_rule_revision_notice_only_when_rules_changed_after_the_block(tmp_path, monkeypatch):
    q, _cli, answers, _ov, name = _load(tmp_path, monkeypatch, "file")
    import watchtower.rule_revision as rr
    import watchtower.workers as workers
    runbook = tmp_path / "runbook.md"
    runbook.write_text("old rule")
    learn = tmp_path / "learnings"
    learn.mkdir()
    (learn / f"{name}.md").write_text("old learnings")
    monkeypatch.setattr(workers, "_WORKER_RUNBOOK_PATH", runbook)
    monkeypatch.setenv("WATCHTOWER_LEARNINGS_DIR", str(learn))
    old = (NOW - timedelta(days=9)).timestamp()
    os.utime(runbook, (old, old))
    os.utime(learn / f"{name}.md", (old, old))
    ref = _ticket(q, name)
    q.block(ref, session_id="w1", question="Promote to deep?", wait_on="owner")
    item = q.get(ref)
    assert rr.notice(item) == ""                               # nothing changed since the block
    assert answers.rules_notice(item) == ""
    future = datetime.now(timezone.utc).timestamp() + 3600
    (learn / f"{name}.md").write_text("complexity alone does not need promotion")
    os.utime(learn / f"{name}.md", (future, future))
    note = rr.notice(item)
    assert f"{name} learnings" in note and "worker runbook" not in note
    assert "does not lift an explicit owner stop" in note and "does not approve" in note
    pa = {"gen": 1, "answer": "go ahead", "prior_worker_id": "w1"}
    assert note in answers.answer_prompt(item, pa)
    # The answer path clears blocked_at; the block event in history still dates it.
    cleared = dict(item, blocked_at="", needs_input=False)
    assert rr.blocked_since(cleared) is not None and rr.notice(cleared) == note


def test_missing_ref_groups_under_a_qualified_name_not_a_bare_ref(env):
    q, _cli, _a, ov, name = env
    a = _ticket(q, name)
    q.block(a, session_id="w1", question="Await", wait_on="dependency", wait_ref="OPS-9999")
    (g,) = ov.build(q.list_items(), project=name, now=NOW)["groups"]
    assert g["root"] == "OPS-9999@?" and g["root_state"] == "not_found_here" and not g["needs_owner"]


def test_owner_roots_order_by_real_instant_across_offsets(env):
    """09:00-08:00 is later than 11:30-05:00 although its string sorts first."""
    q, _cli, _a, ov, name = env
    late = _ticket(q, name, "pacific")
    q.block(late, session_id="w1", question="A?", wait_on="owner",
            wait_extra={"decide_by": "2026-11-17T09:00:00-08:00", "consequence": "late one"})
    early = _ticket(q, name, "eastern", "w2")
    q.block(early, session_id="w2", question="B?", wait_on="owner",
            wait_extra={"decide_by": "2026-11-17T11:30:00-05:00", "consequence": "early one"})
    undated = _ticket(q, name, "undated", "w3")
    q.block(undated, session_id="w3", question="C?", wait_on="owner")
    view = ov.build(q.list_items(), project=name, now=NOW)
    assert [g["root"] for g in view["groups"]] == [early, late, undated]
    assert [u["ref"] for u in view["upcoming"]] == [early, late]
    assert view["upcoming"][0]["decide_by_utc"] == "2026-11-17T16:30:00+00:00"
    assert view["upcoming"][0]["decide_by"] == "2026-11-17T11:30:00-05:00"     # as written
    assert not view["recheck_due"]


def test_owner_wait_note_is_shown_beside_the_original_question(env):
    q, _cli, _a, ov, name = env
    ref = _ticket(q, name)
    q.block(ref, session_id="w1", question="Should Sonia book Rieko and pick Anne's dates?",
            wait_on="owner")
    q.set_wait(ref, "owner", note="Rieko is booked. Only Anne's waitlist dates remain open.")
    view = ov.build(q.list_items(), project=name, now=NOW)
    row = _rows(view)[ref]
    assert row["wait"] == "owner" and row["note"].startswith("Rieko is booked")
    assert "Should Sonia book Rieko" in row["question"]          # not replaced
    assert "now: Rieko is booked" in ov.render(view)


def test_unreadable_queue_is_an_error_or_a_warning_never_an_empty_view(env, monkeypatch, capsys):
    q, cli, _a, ov, name = env
    ref = _ticket(q, name)
    q.block(ref, session_id="w1", question="?", wait_on="owner")
    real = q.list_items

    def flaky(*a, **k):
        if k.get("strict"):
            raise RuntimeError("API rate limit exceeded")
        return real(*a, **k)

    monkeypatch.setattr(q, "list_items", flaky)
    assert cli.main(["owner-view", "-q", name]) == 1                # asked-for queue unreadable
    assert "rate limit" in capsys.readouterr().err
    view = ov.snapshot()                                            # all queues: degrade, loudly
    assert view["incomplete"] and "rate limit" in view["warnings"][0]
    assert "WARNING:" in ov.render(view) and view["blocked"] == 1


def test_blocks_raised_by_watchtower_itself_are_listed_as_system_not_unclassified(env):
    """A stage/plan/review block gets no wait record (that would change who is
    notified), but it is not a worker forgetting to say who it waits on."""
    q, _cli, _a, ov, name = env
    sysref = _ticket(q, name, "stage died")
    q.block(sysref, "", origin="stage", question="Could not spawn the verifier. File a verdict.")
    worker = _ticket(q, name, "worker said nothing", "w2")
    q.block(worker, session_id="w2", question="?", origin="worker")
    view = ov.build(q.list_items(), project=name, now=NOW)
    assert q.get(sysref)["block_origin"] == "stage" and q.current_wait(q.get(sysref)) is None
    assert [r["ref"] for r in view["system"]] == [sysref]
    assert view["unclassified"] == [worker]                      # still visible, still unknown
    assert view["counts"]["system"] == 1 and view["counts"]["unclassified"] == 1
    assert "Raised by WatchTower itself (1)" in ov.render(view)
    q.set_wait(sysref, "owner", note="needs a force-accept decision")   # an explicit wait wins
    assert _rows(ov.build(q.list_items(), project=name, now=NOW))[sysref]["wait"] == "owner"


def test_worker_briefs_tell_workers_to_name_a_wait_target():
    from watchtower import workers
    assert "--wait-on" in workers.WAIT_TARGET_CONTRACT and "{" not in workers.WAIT_TARGET_CONTRACT
    assert "does not change what needs the owner's approval" in workers.WAIT_TARGET_CONTRACT
    src = open(workers.__file__).read()
    assert src.count("+ MINOR_BLOCK_CONTRACT + WAIT_TARGET_CONTRACT + INTEGRATION_CONTRACT +") == 2

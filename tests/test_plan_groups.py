"""WT-33 plan groups: one plan for a cluster of tickets, per-member build and
verify, one integration check at a commit containing every member.

Real store, real git repos under tmp_path (the ``wt`` fixture points every
WatchTower file at tmp_path). No engine is ever spawned: stage spawning is
patched to fail loudly.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time

import pytest

from liveness_golden import SID, Golden, wt  # noqa: F401  (fixture)

PQ = "GQ"


# ------------------------------------------------------------------ helpers
def _git(repo, *args):
    return subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True,
                          check=True).stdout.strip()


@pytest.fixture()
def repo(tmp_path):
    r = tmp_path / "repo"
    r.mkdir()
    _git(r, "init", "-q", "-b", "main")
    _git(r, "config", "user.email", "t@example.com")
    _git(r, "config", "user.name", "t")
    (r / "base.txt").write_text("base\n")
    _git(r, "add", "base.txt")
    _git(r, "commit", "-q", "-m", "base")
    return r


@pytest.fixture(autouse=True)
def no_spawns(wt, monkeypatch):
    def _refuse(*a, **k):
        raise AssertionError("a stage session must not be spawned in these tests")
    monkeypatch.setattr(wt.workers, "spawn_adhoc", _refuse)
    monkeypatch.setattr(wt.workers, "spawn_workers", _refuse)


_N = [0]


def _commit(repo, name, ts=None):
    _N[0] += 1
    (repo / f"{name}.txt").write_text(f"{name} {_N[0]}\n")
    _git(repo, "add", f"{name}.txt")
    env = dict(os.environ)
    if ts is not None:
        env["GIT_COMMITTER_DATE"] = env["GIT_AUTHOR_DATE"] = f"{int(ts)} +0000"
    subprocess.run(["git", "-C", str(repo), "commit", "-q", "-m", name], check=True, env=env,
                   capture_output=True)
    return _git(repo, "rev-parse", "HEAD")


def _ticket(wt, note="t", **kw):
    kw.setdefault("source", "test")
    return wt.q.enqueue(project=PQ, note=note, **kw)["ref"]


def _group(wt, repo, n=2, gates=None, seal=True, **kw):
    kids = [_ticket(wt, f"member {i}", repo_path=str(repo)) for i in range(n)]
    p = wt.q.enqueue(project=PQ, note="the group", source="test", repo_path=str(repo),
                     group_parent=True, children=kids if seal else None, gates=gates, **kw)
    if not seal:
        wt.q.group_attach(p["ref"], kids)
    return p["ref"], kids


def _mv(wt, ref):
    return wt.q.group_mv(wt.q.get(ref))


def _members(wt, ref):
    return list(wt.q.get(ref)["group"]["members"])


def _plan_text(members, shared="shared contract", **secs):
    return "## Shared\n" + shared + "\n\n" + "\n\n".join(
        f"## Section: {m}\n{secs.get(m, 'build ' + m)}" for m in members)


def _accept_plan(wt, P):
    wt.q.plan_start(P)
    mv = _mv(wt, P)
    wt.q.plan_submit(P, _plan_text(_members(wt, P)), by="planner", expect_mv=mv)
    return wt.q.plan_verdict(P, True, "sound", by="reviewer", expect_mv=mv)


def _build(wt, ref, sha="", worker="w1"):
    wt.q.claim_by_ref(ref, worker, session_uuid=SID)
    return wt.q.close(ref, force=True, resolution={"summary": "done", "commit": sha})


def _ready_group(wt, repo, n=2, gates=None):
    """Group with an accepted plan and every member closed at its own commit
    (each on top of the previous: HEAD contains all)."""
    P, kids = _group(wt, repo, n=n, gates=gates)
    _accept_plan(wt, P)
    shas = {}
    for k in kids:
        shas[k] = _commit(repo, k)
        _build(wt, k, shas[k])
    return P, kids, shas


def _integ(wt, P):
    return wt.q.get(P)["group"]["integration"]


def _events(item, name):
    return [h for h in item.get("history") or [] if h.get("event") == name]


# ------------------------------------------------------------ 1-2 filing
def test_parent_defaults_and_children_seal(wt, repo):
    P, kids = _group(wt, repo)
    p = wt.q.get(P)
    assert p["gates"] == ["plan", "verify"]
    assert p["group"]["sealed"] and p["group"]["members"] == kids
    assert p["blocked_by"] == kids
    assert _mv(wt, P) == 3   # two attaches + seal
    for k in kids:
        assert wt.q.get(k)["group"] == {"role": "child", "parent": P, "kind": "member"}
    # explicit gates keep plan first and drop verify when omitted
    P2 = wt.q.enqueue(project=PQ, note="g2", source="test", group_parent=True,
                      gates=["cmd:true"])["ref"]
    assert wt.q.get(P2)["gates"] == ["plan", "cmd:true"]


def test_filing_refusals(wt, repo):
    a = _ticket(wt)
    with pytest.raises(ValueError):
        wt.q.enqueue(project=PQ, note="x", group_parent=True, group="GQ-1")
    with pytest.raises(ValueError):
        wt.q.enqueue(project=PQ, note="x", children=[a])
    with pytest.raises(ValueError):
        wt.q.enqueue(project=PQ, note="x", group_parent=True, blocked_by=[a])
    with pytest.raises(ValueError):
        wt.q.enqueue(project=PQ, note="x", group_parent=True, children=[a])   # < 2 to seal
    P, kids = _group(wt, repo)
    with pytest.raises(ValueError):
        wt.q.enqueue(project=PQ, note="late", group=P)   # sealed
    assert len(wt.q.list_items(project=PQ)) == 4   # nothing half-written


def test_add_group_claim_is_refused_until_the_plan_settles(wt, repo, capsys):
    P, _ = _group(wt, repo, seal=False)
    rc = wt.cli.main(["add", "-q", PQ, "--title", "member 3", "--group", P, "--claim",
                      "--worker", "w1"])
    assert rc == 0
    out = capsys.readouterr()
    assert "member of" in out.out and "could not claim" in out.err
    ref = wt.q.get(P)["group"]["members"][-1]
    assert wt.q.get(ref)["status"] == "open"


# -------------------------------------------------- 3 membership lifecycle
def test_no_planner_before_seal_and_seal_starts_it(wt, repo):
    P, kids = _group(wt, repo, seal=False)
    g = Golden(wt, P)
    g.check(row="group.parent.forming", owner="human", desired=[])
    assert wt.q.plan_start(P)["plan"] == {} or not wt.q.get(P).get("plan", {}).get("status")
    g.step(wt.q.group_seal, P, row="plan.start", desired=["planner"])
    assert wt.stages.desired([wt.q.get(P)])[0]["key"] == f"plan:r1:m{_mv(wt, P)}"
    for k in kids:
        Golden(wt, k).check(row="group.child.plan_wait", owner="dependency", desired=[])


def test_attach_replan_and_manual_blockers(wt, repo):
    P, _ = _group(wt, repo, seal=False)
    own = _ticket(wt, "own plan", gates=["plan"])
    wt.q.plan_start(own)
    with pytest.raises(ValueError, match="--replan"):
        wt.q.group_attach(P, [own])
    wt.q.group_attach(P, [own], replan=True)
    it = wt.q.get(own)
    assert "plan" not in it and _events(it, "plan_superseded")
    with pytest.raises(ValueError):
        wt.q.update(P, blocked_by=[own])
    with pytest.raises(ValueError):
        wt.q.update(own, blocked_by=[P])


def test_detach_table(wt, repo):
    P, kids = _group(wt, repo, n=3)
    wt.q.plan_start(P)
    key0 = wt.stages.desired([wt.q.get(P)])[0]["key"]
    wt.q.group_detach(kids[2])
    assert "group" not in wt.q.get(kids[2])
    p = wt.q.get(P)
    assert p["plan"]["status"] == "planning" and _events(p, "group_plan_restart")
    key1 = wt.stages.desired([p])[0]["key"]
    assert key0 != key1 and key1 == f"plan:r1:m{_mv(wt, P)}"
    assert p["blocked_by"] == kids[:2]
    _accept_plan(wt, P)
    _build(wt, kids[0], "")
    with pytest.raises(ValueError, match="closed"):
        wt.q.group_detach(kids[0])
    wt.q.group_detach(kids[1])            # open after accept: allowed
    assert _members(wt, P) == [kids[0]]
    P2, k2 = _group(wt, repo)
    wt.q.group_detach(k2[0])
    with pytest.raises(ValueError, match="last member"):
        wt.q.group_detach(k2[1])


# ------------------------------------------------------------ 4 claims
def test_claims_wait_for_the_group_plan(wt, repo):
    P, kids = _group(wt, repo)
    assert wt.q.claim_next("w1", project=PQ) is None
    with pytest.raises(wt.q.GroupClaimRefused):
        wt.q.claim_by_ref(kids[0], "w1")
    with pytest.raises(ValueError):
        wt.q.claim_by_ref(P, "w1")
    with pytest.raises(ValueError):
        wt.q.update_status(P, "in_progress", "w1")
    _accept_plan(wt, P)
    got = wt.q.claim_next("w1", project=PQ)
    assert got["ref"] in kids
    stamp = got["group_claim"]
    assert stamp["parent"] == P and stamp["mv"] == _mv(wt, P) and stamp["via"] == "claim_next"
    assert wt.q.claim_next("w2", project=PQ)["ref"] in kids
    assert wt.q.claim_next("w3", project=PQ) is None   # the parent never


def test_reject_with_rebinds_or_defers(wt, repo):
    P, kids = _group(wt, repo)
    _accept_plan(wt, P)
    k = kids[0]
    wt.q.claim_by_ref(k, "w1", session_uuid=SID)
    wt.q.update(k, gates=["review:human"])
    wt.q.close(k, session_id="w1", resolution={"summary": "s", "commit": ""})
    assert wt.q.get(k)["status"] == "in_review"
    it = wt.q.reject_with(k, "redo it")
    assert it["status"] == "in_progress" and it["group_claim"]["via"] == "reopen"
    # the group plan restarted meanwhile: the re-bind is deferred, ticket to open
    wt.q.close(k, session_id="w1", resolution={"summary": "s", "commit": ""})
    wt.q._plan_update(P, lambda it, plan: plan.update(status="planning"))
    it = wt.q.reject_with(k, "again")
    assert it["status"] == "open" and _events(it, "group_reclaim_deferred")
    assert it["gate_feedback"] == "again"


def test_reopen_resume_on_parent_errors(wt, repo, capsys):
    P, _ = _group(wt, repo)
    assert wt.cli.main(["reopen", P, "--resume", "hi"]) == 1


# ------------------------------------------------------------- 5 split
def test_plan_split_rules(wt, repo):
    P, kids = _group(wt, repo)
    wt.q.plan_start(P)
    mv = _mv(wt, P)
    bad = [
        "no headings at all",
        f"## Section: {kids[0]}\nx\n\n## Section: {kids[1]}\ny",                  # no shared
        "## Shared\ns\n\n" + f"## Section: {kids[0]}\nx",                          # missing
        _plan_text(kids) + "\n\n## Section: GQ-999\nz",                              # unknown
        _plan_text(kids) + f"\n\n## Section: {kids[0]}\nagain",                      # duplicate
        _plan_text(kids, **{kids[0]: "x" * (wt.q.PLAN_TEXT_MAX + 1)}),             # section clip
        _plan_text(kids, **{kids[0]: wt.q.GROUP_PLAN_UNCHANGED}),                  # no prior
    ]
    for text in bad:
        with pytest.raises(ValueError):
            wt.q.plan_submit(P, text, expect_mv=mv)
        assert wt.q.get(P)["plan"]["status"] == "planning"
    it = wt.q.plan_submit(P, "preamble\n" + _plan_text(kids), expect_mv=mv)
    assert it["plan"]["text"].startswith("preamble") and set(it["plan"]["sections"]) == set(kids)
    wt.q.plan_verdict(P, False, "fix one", expect_mv=mv, sections=[kids[1].lower()])
    wt.q.plan_submit(P, _plan_text(kids, **{kids[0]: wt.q.GROUP_PLAN_UNCHANGED,
                                          kids[1]: "better"}), expect_mv=mv)
    secs = wt.q.get(P)["plan"]["sections"]
    assert secs[kids[0]] == f"build {kids[0]}" and secs[kids[1]] == "better"


# --------------------------------------------- 6 section reject / exhausted
def test_section_reject_and_exhausted_question_names_detach(wt, repo):
    P, kids = _group(wt, repo)
    wt.q.plan_start(P)
    mv = _mv(wt, P)
    wt.q.plan_submit(P, _plan_text(kids), expect_mv=mv)
    with pytest.raises(ValueError):
        wt.q.plan_verdict(P, False, "x", expect_mv=mv, sections=["GQ-999"])
    with pytest.raises(ValueError):
        wt.q.plan_verdict(kids[0], False, "x", sections=[kids[0]])
    wt.q._plan_update(P, lambda it, plan: plan.update(revision_limit=0))
    rc = wt.cli.main(["plan", "verdict", P, "--reject", "--reasons", "member 1 is wrong",
                      "--mv", str(mv), "--section", kids[1]])
    assert rc == 0
    p = wt.q.get(P)
    assert p["plan"]["status"] == "blocked"
    assert p["plan"]["section_reviews"][kids[1]]["accepted"] is False
    assert p["plan"]["section_reviews"][kids[0]]["accepted"] is True
    assert "wt group detach" in p["block_question"] and p["status"] == "open"
    assert not p.get("claimed_by") and not p.get("parked")
    Golden(wt, P).check(row="human.block", owner="human")
    # detaching the rejected member sends the rest back to review
    wt.q.group_detach(kids[1])
    p = wt.q.get(P)
    assert p["plan"]["status"] == "reviewing" and not p.get("needs_input")
    assert list(p["plan"]["sections"]) == [kids[0]]


# ------------------------------------------------------------ 7 fencing
def test_membership_and_cycle_fences(wt, repo):
    P, kids = _group(wt, repo, n=3)
    wt.q.plan_start(P)
    old = _mv(wt, P)
    with pytest.raises(ValueError, match="--mv"):
        wt.q.plan_submit(P, _plan_text(kids))
    wt.q.group_detach(kids[2])
    with pytest.raises(wt.q.GroupFenced, match="stale"):
        wt.q.plan_submit(P, _plan_text(kids), expect_mv=old)
    rc = wt.cli.main(["plan", "submit", P, "--text", _plan_text(kids[:2]), "--mv", str(old)])
    assert rc == 1
    mv = _mv(wt, P)
    wt.q.plan_submit(P, _plan_text(kids[:2]), expect_mv=mv)
    with pytest.raises(wt.q.GroupFenced):
        wt.q.plan_verdict(P, True, "ok", expect_mv=old)
    wt.q.plan_verdict(P, True, "ok", expect_mv=mv)


# ------------------------------------------------------------ 8 notes
def test_claim_note_shows_shared_and_own_section_only(wt, repo, capsys):
    P, kids = _group(wt, repo)
    _accept_plan(wt, P)
    note = wt.cli._plan_note(wt.q.get(kids[0]))
    assert "GROUP PLAN" in note and "shared contract" in note
    assert f"build {kids[0]}" in note and f"build {kids[1]}" not in note
    assert kids[1] in note   # listed as a sibling
    rc = wt.cli.main(["claim", "-q", PQ, kids[0], "--worker", "w1", "--json"])
    assert rc == 0
    shown = json.loads(capsys.readouterr().out)
    assert "shared contract" in shown["plan_instructions"]
    pending_P, pending_kids = _group(wt, repo)
    assert "PENDING" in wt.cli._plan_note(wt.q.get(pending_kids[0]))


# --------------------------------------------------- 9 per-child verify
def test_child_verify_is_local(wt, repo):
    P, kids = _group(wt, repo)
    _accept_plan(wt, P)
    k = kids[0]
    wt.q.update(k, gates=["verify"])
    wt.q.claim_by_ref(k, "w1", session_uuid=SID)
    sha = _commit(repo, k)
    wt.q.close(k, session_id="w1", resolution={"summary": "s", "commit": sha})
    it = wt.q.get(k)
    assert it["status"] == "in_review" and it["gate_pending"] == "verify"
    goal = wt.cli._verifier_goal(it)
    assert "shared contract" in goal and f"build {kids[1]}" not in goal
    wt.q.verdict(k, True, "ok")
    assert wt.q.get(k)["status"] == "closed"
    assert _integ(wt, P)["state"] == "idle" and wt.q.get(P)["status"] == "open"


# -------------------------------------------------------- 10 integration SHA
def test_containing_sha_out_of_order_and_no_code(wt, repo):
    P, kids = _group(wt, repo, n=3)
    _accept_plan(wt, P)
    a = _commit(repo, "a", ts=1_700_000_000)
    b = _commit(repo, "b", ts=1_700_000_100)
    _build(wt, kids[0], b)        # newer commit first
    _build(wt, kids[1], a)
    _build(wt, kids[2], "")       # no-code member is ignored
    out = wt.q.group_sweep()
    assert out == [(P, "verifying")]
    integ = _integ(wt, P)
    assert integ["sha"] == b and integ["sha_source"] == f"member:{kids[0]}"
    p = wt.q.get(P)
    assert p["resolution"]["commit"] == b and p["status"] == "in_review"
    assert integ["proven"]["commits"] == {kids[0]: b, kids[1]: a, kids[2]: ""}


def test_all_no_code_integrates_at_head(wt, repo):
    P, kids = _group(wt, repo)
    _accept_plan(wt, P)
    for k in kids:
        _build(wt, k, "")
    wt.q.group_sweep()
    assert _integ(wt, P)["sha"] == _git(repo, "rev-parse", "HEAD")


def test_divergence_files_a_fix(wt, repo):
    P, kids = _group(wt, repo)
    _accept_plan(wt, P)
    base = _git(repo, "rev-parse", "HEAD")
    a = _commit(repo, "a")
    _git(repo, "checkout", "-q", "-b", "side", base)
    b = _commit(repo, "b")
    _git(repo, "checkout", "-q", "main")
    _build(wt, kids[0], a)
    _build(wt, kids[1], b)
    assert wt.q.group_sweep() == [(P, "diverged")]
    p = wt.q.get(P)
    integ = p["group"]["integration"]
    assert integ["state"] == "fixing" and len(p["group"]["fixes"]) == 1
    fix = wt.q.get(p["group"]["fixes"][0])
    assert fix["group"]["kind"] == "integration_fix" and "diverge" in fix["text"]
    assert p["blocked_by"] == kids + p["group"]["fixes"]
    Golden(wt, P).check(row="group.parent.wait", owner="dependency")
    # the fix merges both; integration re-runs at it
    _git(repo, "merge", "-q", "--no-edit", "side")
    m = _git(repo, "rev-parse", "HEAD")
    _build(wt, fix["ref"], m)
    assert wt.q.group_sweep() == [(P, "verifying")]
    assert _integ(wt, P)["sha"] == m


# --------------------------------------------------------------- 11 gates
def test_cmd_verify_review_order(wt, repo):
    P, kids, shas = _ready_group(wt, repo, gates=["cmd:true", "verify", "review:human"])
    g = Golden(wt, P)
    assert wt.q.group_sweep() == [(P, "verifying")]
    p = wt.q.get(P)
    assert p["gate_stages"] == ["verify", "review:human"]
    assert not any(s.startswith("cmd:") for s in p["gate_stages"])
    g.check(row="review.verify", owner="verifier", desired=["verifier"])
    vc = p["verify_cycle"]
    wt.q.verdict(P, True, "ok", expect_verify_cycle=vc)
    g.check(row="review.gate", owner="human", desired=[])
    assert _integ(wt, P)["state"] == "reviewing"
    wt.q.accept(P)
    p = wt.q.get(P)
    assert p["status"] == "closed" and _integ(wt, P)["state"] == "done"
    assert p["resolution"]["commit"] == shas[kids[-1]]
    g.check(row="closed", owner="terminal")


def test_cmd_fail_files_a_fix_and_bumps_cycle_only(wt, repo):
    P, kids, _ = _ready_group(wt, repo, gates=["cmd:false", "verify"])
    vc0 = wt.q.get(P).get("verify_cycle") or 0
    assert wt.q.group_sweep() == [(P, "gate_failed")]
    integ = _integ(wt, P)
    assert integ["cycle"] == 1 and integ["state"] == "fixing"
    assert (wt.q.get(P).get("verify_cycle") or 0) == vc0


def test_review_only_and_cmd_only(wt, repo):
    P, _, _ = _ready_group(wt, repo, gates=["review:human"])
    assert wt.q.group_sweep() == [(P, "reviewing")]
    assert wt.q.get(P)["gate_pending"] == "review:human"
    P2, _, _ = _ready_group(wt, repo, gates=["cmd:true"])
    assert wt.q.group_sweep() == [(P2, "closed")]
    p2 = wt.q.get(P2)
    assert p2["status"] == "closed" and _events(p2, "accept")[-1]["proof"] == "proven"


# -------------------------------------------------------------- 12 pinned
def test_pinned_gate_runs_clean_at_x_even_with_a_dirty_checkout(wt, repo, tmp_path):
    marker = tmp_path / "ran.txt"
    P, kids, shas = _ready_group(
        wt, repo, gates=[f"cmd:test ! -e dirty.txt && git status --porcelain > {marker}", ])
    (repo / "dirty.txt").write_text("uncommitted\n")
    assert wt.q.group_sweep() == [(P, "closed")]
    assert marker.exists() and marker.read_text() == ""


def test_setup_failure_blocks_without_running_then_answer_retries(wt, repo, tmp_path,
                                                                  monkeypatch):
    marker = tmp_path / "ran.txt"
    P, _, _ = _ready_group(wt, repo, gates=[f"cmd:touch {marker}"])
    real = wt.q._run_cmd_gate

    def broken(command, repo_path, commit, ref, pinned=False):
        return real(command, str(tmp_path / "nope"), commit, ref, pinned=pinned)
    monkeypatch.setattr(wt.q, "_run_cmd_gate", broken)
    assert wt.q.group_sweep() == [(P, "gate_setup")]
    p = wt.q.get(P)
    assert not marker.exists() and p["needs_input"] and _integ(wt, P)["blocked"] == "gate_setup"
    assert p["status"] == "open"
    Golden(wt, P).check(row="human.block")
    monkeypatch.setattr(wt.q, "_run_cmd_gate", real)
    wt.q.answer(P, "retry")
    assert not wt.q.get(P).get("needs_input") and _integ(wt, P)["blocked"] == ""
    assert wt.q.group_sweep() == [(P, "closed")]
    assert marker.exists()


# ------------------------------------------------------- 13 TTL / missing
def test_stale_lease_is_retaken_once(wt, repo, monkeypatch):
    P, _, _ = _ready_group(wt, repo)
    with wt.q._FileLock(wt.q._lock_path()):
        data = wt.q._load_unlocked()
        p = next(i for i in data["items"] if i["ref"] == P)
        p["group"]["integration"].update(state="gating",
                                         lease={"token": "old", "at": "2000-01-01T00:00:00Z"})
        wt.q._save_unlocked(data)
    Golden(wt, P).check(row="group.parent.gating_stale", owner="reconciler")
    assert wt.q.group_sweep() == [(P, "verifying")]
    p = wt.q.get(P)
    assert _integ(wt, P)["cycle"] == 1 and _events(p, "group_lease_retaken")


def test_commit_missing_blocks_until_fetched(wt, repo):
    P, kids = _group(wt, repo)
    _accept_plan(wt, P)
    _build(wt, kids[0], _commit(repo, "a"))
    _build(wt, kids[1], "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef")
    assert wt.q.group_sweep() == [(P, "commit_missing")]
    p = wt.q.get(P)
    assert p["needs_input"] and "not found" in p["block_question"]
    wt.q.answer(P, "retry")          # still missing: blocks again
    assert wt.q.group_sweep() == [(P, "commit_missing")]


# ----------------------------------------------------- 14 fixes and cap
def test_six_members_two_fixes_then_capped_then_override(wt, repo):
    P, kids, _ = _ready_group(wt, repo, n=6, gates=["cmd:false"])
    mv = _mv(wt, P)
    for i in range(2):
        assert wt.q.group_sweep() == [(P, "gate_failed")]
        fix = wt.q.get(P)["group"]["fixes"][-1]
        _build(wt, fix, "")
    assert _mv(wt, P) == mv and len(wt.q.get(P)["group"]["fixes"]) == 2
    assert wt.q.group_sweep() == [(P, "gate_failed")]
    p = wt.q.get(P)
    assert _integ(wt, P)["state"] == "capped" and p["needs_input"]
    assert "wt group fix" in p["block_question"] and p["status"] == "open"
    wt.q.answer(P, "try the other API")
    p = wt.q.get(P)
    assert _integ(wt, P)["allowance"] == 3 and len(p["group"]["fixes"]) == 3
    assert "try the other API" in wt.q.get(p["group"]["fixes"][-1])["text"]
    with pytest.raises(ValueError, match="not capped"):
        wt.q.group_fix(P, "more")
    with wt.q._FileLock(wt.q._lock_path()):
        data = wt.q._load_unlocked()
        pp = next(i for i in data["items"] if i["ref"] == P)
        pp["group"]["integration"].update(state="capped", allowance=wt.q.GROUP_MAX_FIXES)
        wt.q._save_unlocked(data)
    with pytest.raises(ValueError, match="only"):
        wt.q.group_fix(P, "more")


# ------------------------------------------------------- 15 force accept
def test_force_accept_during_gating_discards_the_late_commit(wt, repo, monkeypatch):
    P, _, _ = _ready_group(wt, repo, gates=["cmd:true", "verify"])
    from watchtower import integration_git
    real = integration_git.containing_sha
    seen = []

    def during(repo_, commits):
        seen.append(wt.q.group_sweep())   # a concurrent sweep: nothing due (21)
        out = real(repo_, commits)
        with pytest.raises(ValueError, match="not proven"):
            wt.q.accept(P, force=True)
        wt.q.accept(P, force=True, no_proof=True)
        return out
    monkeypatch.setattr(integration_git, "containing_sha", during)
    assert wt.q.group_sweep() == [(P, "discarded")]
    assert seen == [[]]
    p = wt.q.get(P)
    assert p["status"] == "closed" and _integ(wt, P)["proof"] == "none"
    assert "WITHOUT" in p["resolution"]["summary"]
    Golden(wt, P).check(row="closed")


def test_force_accept_during_verify_fences_the_late_verdict(wt, repo):
    P, _, _ = _ready_group(wt, repo)
    wt.q.group_sweep()
    vc = wt.q.get(P)["verify_cycle"]
    with pytest.raises(ValueError, match="--verify-cycle"):
        wt.q.verdict(P, True, "ok")
    with pytest.raises(wt.q.GroupFenced):
        wt.q.verdict(P, True, "ok", expect_verify_cycle=vc - 1)
    wt.q.accept(P, force=True)
    p = wt.q.get(P)
    assert p["status"] == "closed" and _events(p, "accept")[-1]["proof"] == "proven"
    with pytest.raises(wt.q.GroupFenced):
        wt.q.verdict(P, True, "late", expect_verify_cycle=vc)


def test_proof_predating_a_later_fix_is_not_proven(wt, repo):
    P, kids, _ = _ready_group(wt, repo, gates=["cmd:false"])
    wt.q.group_sweep()          # proven at X, then the gate failed: a fix is open
    fix = wt.q.get(P)["group"]["fixes"][0]
    _build(wt, fix, _commit(repo, "fix"))
    with pytest.raises(ValueError, match="not proven"):
        wt.q.accept(P, force=True)


# ---------------------------------------------------------- 16 R7 proofs
def _verifying(wt, repo):
    P, kids, shas = _ready_group(wt, repo)
    assert wt.q.group_sweep() == [(P, "verifying")]
    return P, kids, shas


def test_r7_closed_member_locked_during_integration(wt, repo):
    P, kids, shas = _verifying(wt, repo)
    before = json.dumps(wt.q.get(kids[0]), sort_keys=True)
    with pytest.raises(wt.q.GroupChildLocked, match="--force"):
        wt.q.reopen(kids[0], reason="oops")
    with pytest.raises(wt.q.GroupChildLocked):
        wt.q.reopen_and_claim(kids[0], "w1", session_uuid=SID)
    assert json.dumps(wt.q.get(kids[0]), sort_keys=True) == before
    assert wt.cli.main(["reopen", kids[0]]) == 1
    vc = wt.q.get(P)["verify_cycle"]
    wt.q.reopen(kids[0], reason="really", force=True)
    p = wt.q.get(P)
    integ = p["group"]["integration"]
    assert p["status"] == "open" and integ["state"] == "idle" and integ["proven"] is None
    assert p["verify_cycle"] == vc + 1 and _events(p, "group_integration_invalidated")
    with pytest.raises(wt.q.GroupFenced):
        wt.q.verdict(P, True, "late pass", expect_verify_cycle=vc)
    y = _commit(repo, "y")
    _build(wt, kids[0], y)
    assert wt.q.group_sweep() == [(P, "verifying")]
    integ = _integ(wt, P)
    assert integ["commits"][kids[0]] == y and integ["sha"] == y


def test_r7_tampered_map_refuses_completion(wt, repo):
    P, kids, shas = _verifying(wt, repo)
    with wt.q._FileLock(wt.q._lock_path()):
        data = wt.q._load_unlocked()
        k = next(i for i in data["items"] if i["ref"] == kids[0])
        k["resolution"]["commit"] = "0" * 40      # a raw write, no hook
        wt.q._save_unlocked(data)
    vc = wt.q.get(P)["verify_cycle"]
    with pytest.raises(wt.q.GroupFenced, match="completion refused"):
        wt.q.verdict(P, True, "ok", expect_verify_cycle=vc)
    p = wt.q.get(P)
    assert p["status"] == "open" and _events(p, "group_completion_refused")
    Golden(wt, P).check()     # still classifies (idle, proof none)


def test_r7_reopen_allowed_when_fixing_or_after_done(wt, repo):
    P, kids, _ = _ready_group(wt, repo, gates=["cmd:false"])
    wt.q.group_sweep()
    assert _integ(wt, P)["state"] == "fixing"
    wt.q.reopen(kids[0], reason="fine while fixing")
    assert wt.q.get(kids[0])["status"] == "open"


# --------------------------------------------------------- R7 amendment
def test_done_parent_keeps_its_proof_when_a_member_reopens_and_recloses(wt, repo):
    """Amendment: done parent -> member reopen -> member reclose at another
    SHA. The parent stays closed with its ORIGINAL proof; the WT-31 row is
    still ``closed`` (done x stale is reachable, never re-evaluated)."""
    P, kids, shas = _verifying(wt, repo)
    g = Golden(wt, P)
    g.step(wt.q.verdict, P, True, "ok", expect_verify_cycle=wt.q.get(P)["verify_cycle"],
           row="closed", owner="terminal", desired=[])
    proof = json.loads(json.dumps(_integ(wt, P)["proven"]))
    resolution = dict(wt.q.get(P)["resolution"])
    assert wt.liveness.project(wt.q.get(P)).gproof == "match"
    wt.q.reopen(kids[0], reason="follow-up")
    p = wt.q.get(P)
    assert p["status"] == "closed" and _events(p, "group_member_reopened_after_close")
    g.check(row="closed")
    z = _commit(repo, "z")
    _build(wt, kids[0], z)
    p = wt.q.get(P)
    assert p["status"] == "closed" and _integ(wt, P)["state"] == "done"
    assert _integ(wt, P)["proven"] == proof and p["resolution"] == resolution
    st = wt.liveness.project(p)
    assert (st.gint, st.gproof) == ("done", "stale")
    g.check(row="closed", owner="terminal", desired=[])
    assert wt.q.group_sweep() == []


# ------------------------------------------------------------- 17 R8 kill
def _sleeper():
    return subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"],
                            start_new_session=True)


def _as_stage(wt, ref, proc, role="planner", wid="wk-old"):
    tok = wt.workers._pid_start_token(proc.pid)
    assert tok

    def _do(it, ss):
        ss.update(key="plan:r1:m3", role=role, attempt=1, worker_id=wid, pid=proc.pid,
                  pid_started=tok)
    wt.q.stage_session_update(ref, _do)
    if role == "planner":
        wt.q.plan_set_role(ref, "planner", {"worker_id": wid, "stage_key": "plan:r1:m3"})
    else:
        wt.q.set_verifier_info(ref, {"worker_id": wid})


def test_r8_detach_retires_and_the_pass_kills_it(wt, repo):
    P, kids = _group(wt, repo, n=3)
    wt.q.plan_start(P)
    proc = _sleeper()
    try:
        _as_stage(wt, P, proc)
        wt.q.group_detach(kids[2])
        p = wt.q.get(P)
        assert [e["worker_id"] for e in p["stage_retired"]] == ["wk-old"]
        assert "wk-old" in wt.q.stage_retired_ids(p)
        acted = wt.stages._retire_pass(wt.q.list_items())
        assert acted == [(P, "retired:killed")]
        assert proc.wait(timeout=10) in (-15, -9)
        p = wt.q.get(P)
        assert "stage_retired" not in p and _events(p, "stage_retired")[-1]["outcome"] == "killed"
        assert "wk-old" in wt.q.stage_retired_ids(p)     # never adopted again
        assert wt.stages._retire_pass(wt.q.list_items()) == []
    finally:
        if proc.poll() is None:
            proc.kill()


def test_r8_token_mismatch_is_never_signalled(wt, repo):
    P, kids = _group(wt, repo, n=3)
    wt.q.plan_start(P)
    proc = _sleeper()
    try:
        _as_stage(wt, P, proc)
        wt.q.stage_session_update(P, lambda it, ss: ss.update(pid_started="bogus"))
        wt.q.group_detach(kids[2])
        assert wt.stages._retire_pass(wt.q.list_items()) == [(P, "retired:token_mismatch")]
        assert proc.poll() is None
    finally:
        proc.kill()


def test_r8_force_accept_retires_the_verifier_with_no_targets(wt, repo):
    P, _, _ = _verifying(wt, repo)
    proc = _sleeper()
    try:
        _as_stage(wt, P, proc, role="verifier", wid="wk-ver")
        wt.q.stage_session_update(P, lambda it, ss: ss.update(key="verify:1", role="verifier"))
        wt.q.accept(P, force=True)
        assert wt.stages.desired(wt.q.list_items()) == []
        acted = wt.stages.reconcile_stages()
        assert (P, "retired:killed") in acted
        assert proc.wait(timeout=10) in (-15, -9)
    finally:
        if proc.poll() is None:
            proc.kill()


def test_r8_reject_parent_retires_the_live_verifier(wt, repo):
    P, _, _ = _verifying(wt, repo)
    wt.q.set_verifier_info(P, {"worker_id": "wk-ver2"})
    wt.q.reject(P, "integration broken")
    p = wt.q.get(P)
    assert [e["worker_id"] for e in p["stage_retired"]] == ["wk-ver2"]
    assert _integ(wt, P)["state"] == "fixing"
    with pytest.raises(ValueError, match="not in_review"):
        wt.q.reject(P, "again")


# ------------------------------------------------- 18-19 parent invariants
def test_parent_is_never_claimed_parked_or_in_progress(wt, repo):
    P, kids = _group(wt, repo)
    it = wt.q.block(P, "", question="what scope?", origin="worker")
    assert it["status"] == "open" and not it.get("parked") and not it.get("claimed_by")
    Golden(wt, P).check(row="human.block")
    wt.q.answer(P, "this scope")
    p = wt.q.get(P)
    assert p["status"] == "open" and not p.get("needs_input")
    for status in ("in_progress", "in_review", "closed", "awaiting_answer"):
        with pytest.raises(ValueError):
            wt.q.update_status(P, status, "w1")
    with pytest.raises(ValueError):
        wt.q.reopen(P)
    with pytest.raises(ValueError):
        wt.q.close(P, force=True)


def test_dependents_unblock_when_the_group_closes(wt, repo):
    P, kids, _ = _ready_group(wt, repo, gates=["cmd:true"])
    dep = _ticket(wt, "after the group", blocked_by=[P])
    Golden(wt, dep).check(row="dep.waiting")
    wt.q.group_sweep()
    Golden(wt, dep).check(row="work.open")


def test_member_waiting_on_a_sibling_reads_waiting(wt, repo):
    P, kids = _group(wt, repo)
    wt.q.update(kids[1], blocked_by=[kids[0]])
    _accept_plan(wt, P)
    Golden(wt, kids[1]).check(row="dep.waiting")
    Golden(wt, kids[0]).check(row="work.open")
    Golden(wt, P).check(row="group.parent.wait")


# ------------------------------------------------------- liveness goldens
def test_group_lifecycle_goldens(wt, repo):
    P, kids = _group(wt, repo, seal=False)
    g = Golden(wt, P)
    g.check(row="group.parent.forming", desired=[])
    g.step(wt.q.group_seal, P, row="plan.start", desired=["planner"])
    g.step(wt.q.plan_start, P, row="plan.planning", desired=["planner"])
    mv = _mv(wt, P)
    g.step(wt.q.plan_submit, P, _plan_text(kids), expect_mv=mv, row="plan.reviewing",
           desired=["plan_reviewer"])
    g.step(wt.q.plan_verdict, P, True, "ok", expect_mv=mv, row="group.parent.wait",
           owner="dependency", desired=[])
    for k in kids:
        Golden(wt, k).check(row="work.open")
        _build(wt, k, _commit(repo, k))
    g.check(row="group.parent.ready", owner="reconciler", desired=[])
    a = wt.liveness.assess(wt.q.get(P), wt.q._refs_index(wt.q.list_items()), stall=0)
    assert a["action"] == "group_sweep"
    g.step(wt.q.group_sweep, row="review.verify", owner="verifier", desired=["verifier"])
    assert "INTEGRATION" in wt.stages._goal(wt.q.get(P), "verifier", token="", respawn=False,
                                            note="")
    g.step(wt.q.verdict, P, True, "ok", expect_verify_cycle=wt.q.get(P)["verify_cycle"],
           row="closed", owner="terminal", desired=[])


def test_cli_group_show_and_ls(wt, repo, capsys):
    P, kids = _group(wt, repo)
    assert wt.cli.main(["group", "show", kids[0], "--json"]) == 0
    info = json.loads(capsys.readouterr().out)
    assert info["ref"] == P and [m["ref"] for m in info["members"]] == kids
    assert info["proof"] == "none" and info["integration"]["state"] == "idle"
    assert wt.cli.main(["ls", "-q", PQ]) == 0
    out = capsys.readouterr().out
    assert "[group: 2 members" in out and f"[group {P}]" in out

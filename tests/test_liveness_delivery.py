"""WT-31 D4 verified delivery + D2.8 transport allowlist.

A send counts as delivered only when its nonce (``⟨wt:<delivery_id>⟩``, last
line) shows up in the target transcript at or after the pre-send offset. The
ledger (deliveries.json) settles each row confirmed / lost and runs the
purpose's handler. Everything runs on a temp store, outbox, transcript dir
and config (liveness_golden.wt + the ``dw`` fixture below).
"""

from __future__ import annotations

import ast
import os
import time
from pathlib import Path

import pytest

from liveness_golden import SID, SID2, Golden, pa_state, wt  # noqa: F401  (fixture)

PQ = "LQ"
ROOT = Path(__file__).resolve().parent.parent / "watchtower"


@pytest.fixture()
def dw(wt, tmp_path, monkeypatch):
    """``wt`` plus a fake transcript dir, codex home and agents registry."""
    monkeypatch.setenv("WATCHTOWER_CLAUDE_PROJECTS_DIR", str(tmp_path / "claude-projects"))
    monkeypatch.setenv("CODEX_HOME", str(tmp_path / "codex-home"))
    monkeypatch.setenv("WATCHTOWER_AGENTS_FILE", str(tmp_path / "agents.json"))
    monkeypatch.setenv("WATCHTOWER_RECEIPT_WAIT_S", "600")
    monkeypatch.delenv("WATCHTOWER_DELIVERIES_FILE", raising=False)
    import watchtower.receipts as receipts
    wt.receipts = receipts
    wt.window = 600.0
    return wt


def _transcript(sid: str = SID, body: str = '{"type":"user"}\n') -> Path:
    d = Path(os.environ["WATCHTOWER_CLAUDE_PROJECTS_DIR"]) / "proj"
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{sid}.jsonl"
    p.write_text(body)
    return p


def _append(p: Path, text: str) -> None:
    import json
    with open(p, "a") as f:
        f.write(json.dumps({"type": "user", "message": {"content": text}},
                           ensure_ascii=False) + "\n")


def _uds_send(dw, key, text="hello", *, purpose="resume", land=None, attempt=1,
              sid=SID, **kw):
    """A UDS send to ``sid``; ``land`` (a transcript path) gets the body."""
    def uds(body):
        if land is not None:
            _append(land, body)
        return True
    return dw.liveness.deliver(sid, text, purpose=purpose, dedupe_key=key, session_id=sid,
                               engine="claude", transports=("uds",), uds_fn=uds,
                               attempt=attempt, **kw)


def _later(dw) -> float:
    return time.time() + 2 * dw.window + 5


# ----------------------------------------------------------------- receipts
def test_ok_without_nonce_is_lost(dw, monkeypatch):
    """The transport said ok, nothing landed: the row is lost, never confirmed."""
    import watchtower.messages as messages
    monkeypatch.setattr(messages, "deliver_message",
                        lambda *a, **k: {"ok": True, "transport": "delegate"})
    sent = dw.liveness.deliver(SID, "hi", purpose="resume", dedupe_key="k1",
                               transports=("message",), session_id=SID)
    assert sent["ok"] and sent["state"] == "unverified"
    # a receipt that never sees its nonce, though the transcript grows
    p = _transcript()
    sent2 = _uds_send(dw, "k2")
    _append(p, "something else entirely")
    assert sent2["state"] == "pending" and sent2["receipt_id"]
    out = dw.liveness.sweep_deliveries()     # no receipt source: lost at once
    assert [(r["dedupe_key"], r["state"]) for r in out] == [("k1", "lost")]
    assert "unverified" in out[0]["reason"]
    assert dw.liveness.delivery_row("k2")["state"] == "pending"   # advanced, in window
    out = dw.liveness.sweep_deliveries(now=_later(dw))
    assert [(r["dedupe_key"], r["state"]) for r in out] == [("k2", "lost")]
    assert dw.liveness.sweep_deliveries(now=_later(dw)) == []   # settled once


def test_repeated_text_confirms_only_the_send_that_landed(dw):
    p = _transcript()
    first = _uds_send(dw, "a", "same words", land=p)
    second = _uds_send(dw, "b", "same words")
    assert first["nonce"] != second["nonce"]
    out = {r["dedupe_key"]: r["state"]
           for r in dw.liveness.sweep_deliveries(now=_later(dw))}
    assert out == {"a": "confirmed", "b": "lost"}


def test_new_nonce_per_retry_and_old_nonce_does_not_confirm(dw):
    p = _transcript()
    one = _uds_send(dw, "nudge:w1", "wake up")
    two = _uds_send(dw, "nudge:w1", "wake up", attempt=2)
    assert one["nonce"] != two["nonce"] and one["delivery_id"] != two["delivery_id"]
    rows = {r["delivery_id"]: r for r in dw.liveness.deliveries()}
    assert rows[one["delivery_id"]]["state"] == "superseded"
    assert dw.liveness.delivery_row("nudge:w1")["delivery_id"] == two["delivery_id"]
    _append(p, f"wake up\n{one['nonce']}")                # the superseded send lands late
    out = dw.liveness.sweep_deliveries(now=_later(dw))
    assert [(r["delivery_id"], r["state"]) for r in out] == [(two["delivery_id"], "lost")]


def test_receipt_recorded_before_send_and_baseline_excludes_old_bytes(dw, monkeypatch):
    lv, rc = dw.liveness, dw.receipts
    monkeypatch.setattr(lv, "new_delivery_id", lambda: "dlv-fixed0001")
    nonce = rc.make_nonce("dlv-fixed0001")
    p = _transcript()
    _append(p, f"an older copy\n{nonce}")                 # before the send: not proof
    seen = []

    def uds(body):
        seen.append([r for r in rc._load() if r.get("nonce") == nonce])
        return True
    sent = lv.deliver(SID, "hi", purpose="resume", dedupe_key="k", session_id=SID,
                      engine="claude", transports=("uds",), uds_fn=uds)
    assert seen and len(seen[0]) == 1, "the receipt must exist before the send"
    assert seen[0][0]["offset"] == p.stat().st_size
    assert rc.nonce_of(lv.deliveries()[0]["text"] + "\n" + nonce) == nonce
    rc.sweep()
    assert rc.get(sent["receipt_id"], refresh=False)["status"] == "pending"
    _append(p, f"hi\n{nonce}")
    rc.sweep()
    assert rc.get(sent["receipt_id"], refresh=False)["status"] == "landed"


def test_pasted_nonce_is_not_the_delivery_nonce(dw):
    rc = dw.receipts
    p = _transcript()
    pasted = rc.make_nonce("dlv-pasted0001")
    sent = _uds_send(dw, "k", f"quoting an old message:\n{pasted}\nplease look")
    assert sent["nonce"] != pasted
    assert rc.nonce_of(rc.with_nonce(f"x\n{pasted}\ny", sent["nonce"])) == sent["nonce"]
    assert rc.nonce_of(f"{pasted}\ntrailing words") == ""
    _append(p, f"quoting an old message:\n{pasted}\nplease look")   # nonce line missing
    out = dw.liveness.sweep_deliveries(now=_later(dw))
    assert [r["state"] for r in out] == ["lost"]


def test_truncated_transcript_resets_offset(dw):
    rc = dw.receipts
    p = _transcript(body="x" * 5000 + "\n")
    sent = _uds_send(dw, "k")
    p.write_text("")                                      # compacted / rewritten
    _append(p, f"hello\n{sent['nonce']}")
    rc.sweep()
    rec = rc.get(sent["receipt_id"], refresh=False)
    assert rec["status"] == "landed" and rec["offset"] == 0 and rec.get("truncated_at")


def test_slash_command_carries_no_nonce(dw):
    sent = _uds_send(dw, "k", "/compact")
    assert sent["nonce"] == "" and sent["state"] == "unverified"


def test_headless_refused_for_engine_without_receipts(dw, monkeypatch):
    calls = []
    monkeypatch.setattr(dw.cli, "_resume_session_headless",
                        lambda *a, **k: calls.append(a) or True)
    sent = dw.liveness.deliver("session_abc", "hi", purpose="resume", dedupe_key="k",
                               session_id="session_abc", engine="kimi",
                               transports=("headless",))
    assert not sent["ok"] and "no receipt source" in sent["error"] and calls == []
    # D2.8: there is no escape hatch any more (``unverified_ok`` is gone)
    with pytest.raises(TypeError):
        dw.liveness.deliver("session_abc", "hi", purpose="resume", dedupe_key="k",
                            session_id="session_abc", engine="kimi",
                            transports=("headless",), unverified_ok=True)
    assert calls == []


# ------------------------------------------------------------------ ledger
def test_ledger_path_overrides_and_prune(dw, tmp_path, monkeypatch):
    lv = dw.liveness
    assert lv.deliveries_file() == Path(os.environ["WATCHTOWER_OUTBOX_FILE"]).parent \
        / "deliveries.json"
    monkeypatch.setattr(lv, "DELIVERIES_FILE", str(tmp_path / "mod.json"))
    assert lv.deliveries_file() == tmp_path / "mod.json"
    monkeypatch.setenv("WATCHTOWER_DELIVERIES_FILE", str(tmp_path / "env.json"))
    assert lv.deliveries_file() == tmp_path / "env.json"
    now = time.time()
    monkeypatch.setattr(lv, "DELIVERY_MAX_ROWS", 3)
    rows = [{"delivery_id": f"d{i}", "sent_at": now - (i % 2) * (lv.DELIVERY_PRUNE_S + 1)}
            for i in range(10)]
    lv._save_deliveries(rows, now)
    assert [r["delivery_id"] for r in lv.deliveries()] == ["d4", "d6", "d8"]


def test_undeclared_purpose_is_refused(dw):
    with pytest.raises(ValueError):
        dw.liveness.deliver(SID, "x", purpose="gossip", dedupe_key="k")


# ------------------------------------------------- answers (E9 / E10 / E14)
def _claimed(dw):
    it = dw.q.enqueue(project=PQ, note="ticket", source="test")
    dw.q.claim_by_ref(it["ref"], "w1", session_uuid=SID)
    return it["ref"]


def _resuming(dw, monkeypatch, msg_result=None):
    """Park + answer; the message transport fails (or returns ``msg_result``)
    and the headless resume 'starts', capturing the body it would send."""
    import watchtower.messages as messages
    bodies = []
    monkeypatch.setattr(dw.answers, "_resumable", lambda engine, sid: True)
    def _msg(target, body, **k):
        if msg_result:
            bodies.append(body)
            return dict(msg_result)
        return {"ok": False, "error": "no route"}
    monkeypatch.setattr(messages, "deliver_message", _msg)
    monkeypatch.setattr(dw.cli, "_resume_session_headless",
                        lambda sid, repo, body, engine, **k: bodies.append(body) or True)
    g = Golden(dw, _claimed(dw))
    g.step(dw.q.block, g.ref, session_id="w1", question="which way?", origin="worker",
           row="answer.await_human", owner="human", desired=[])
    g.step(dw.q.answer, g.ref, "left", edge="E1", row="answer.routing", desired=[])
    return g, bodies


def _gen(dw, ref):
    return int(dw.q.get(ref)["pending_answer"]["gen"])


def test_answer_delivered_only_when_nonce_lands(dw, monkeypatch):
    p = _transcript()
    g, bodies = _resuming(dw, monkeypatch)
    g.step(dw.answers.route_answer, g.ref, _gen(dw, g.ref), edge="E3",
           row="answer.delivering", desired=[])
    row = dw.liveness.deliveries(purpose="answer")[-1]
    assert row["state"] == "pending" and row["transport"] == "headless-resume"
    assert dw.receipts.nonce_of(bodies[-1]) == row["nonce"]
    # the transport's ok is not delivery
    g.step(dw.liveness.sweep_deliveries, row="answer.delivering")
    g.step(dw.answers.route_pending_answers, row="answer.delivering")
    assert pa_state(g.item()) == "delivering"
    _append(p, bodies[-1])
    out = g.step(dw.liveness.sweep_deliveries, edge="E9")
    assert [r["result"] for r in out] == ["E9"]
    assert pa_state(g.item()) == "delivered"


def test_answer_ok_without_receipt_is_lost_then_handed_off(dw, monkeypatch):
    g, bodies = _resuming(dw, monkeypatch, {"ok": True, "transport": "delegate"})
    dw.answers.route_answer(g.ref, _gen(dw, g.ref))
    assert pa_state(g.item()) == "delivering"
    row = dw.liveness.deliveries(purpose="answer")[-1]
    assert row["unverified"] is True
    out = g.step(dw.liveness.sweep_deliveries, edge="E10", row="work.open")
    assert [r["result"] for r in out] == ["E10"]


def test_stale_confirm_after_release_is_a_noop(dw, monkeypatch):
    p = _transcript()
    g, bodies = _resuming(dw, monkeypatch)
    gen = _gen(dw, g.ref)
    dw.answers.route_answer(g.ref, gen)
    g.step(dw.q.release, g.ref, edge="E14", row="work.open")
    before = pa_state(g.item())
    _append(p, bodies[-1])
    out = dw.liveness.sweep_deliveries()
    assert [r["state"] for r in out] == ["confirmed"]
    assert out[0]["result"] == "noop (state moved on)"
    assert pa_state(g.item()) == before


def test_awaiting_receipt_suppresses_redelivery(dw, monkeypatch):
    _transcript()
    g, bodies = _resuming(dw, monkeypatch)
    dw.answers.route_answer(g.ref, _gen(dw, g.ref))
    n = len(bodies)
    monkeypatch.setattr(dw.answers, "ROUTE_LEASE_S", 0)
    dw.answers.route_pending_answers()
    assert len(bodies) == n, "a pending receipt must not be re-sent"


# ---------------------------------------------------------------- handlers
def _lost(dw, purpose, key, attempt=1, **kw):
    sent = _uds_send(dw, key, "do the thing", purpose=purpose, attempt=attempt, **kw)
    assert sent["state"] == "pending"
    return dw.liveness.sweep_deliveries(now=_later(dw))


def test_nudge_resends_once_then_marks_undeliverable(dw, monkeypatch):
    w = dw.workers.record_worker(os.getpid(), PQ, "claude", "lq-w1", str(dw.tmp),
                                 str(dw.tmp / "w.log"), session_id=SID)
    resent = []
    monkeypatch.setattr(dw.workers, "_nudge_one",
                        lambda w, text, attempt=1: resent.append((w["worker_id"], attempt))
                        or "sent")
    out = _lost(dw, "nudge", "nudge:lq-w1", worker_id="lq-w1")
    assert out[0]["result"] == "resent: sent" and resent == [("lq-w1", 2)]
    out = _lost(dw, "nudge", "nudge:lq-w1", attempt=2, worker_id="lq-w1")
    assert out[0]["result"] == "undeliverable_since"
    row = next(x for x in dw.workers.list_workers(prune=False)
               if x["worker_id"] == w["worker_id"])
    assert row.get("undeliverable_since")


def test_release_resends_once_then_logs_undelivered(dw, monkeypatch):
    dw.workers.record_worker(os.getpid(), PQ, "claude", "lq-w2", str(dw.tmp),
                             str(dw.tmp / "w.log"), session_id=SID)
    resent = []
    monkeypatch.setattr(dw.workers, "_deliver_release_instruction",
                        lambda w, text, attempt=1: resent.append(attempt)
                        or {"transport": "uds", "delivered": True, "error": ""})
    assert _lost(dw, "release", "release:lq-w2", worker_id="lq-w2")[0]["result"] \
        == "resent: uds"
    assert resent == [2]
    assert _lost(dw, "release", "release:lq-w2", attempt=2,
                 worker_id="lq-w2")[0]["result"] == "RELEASE_UNDELIVERED"
    assert "RELEASE_UNDELIVERED" in Path(os.environ["WATCHTOWER_ACTIVITY_LOG"]).read_text()


def test_review_renotifies_once_then_blocks(dw, monkeypatch):
    item = {"ref": "LQ-9", "status": "in_review", "gate_pending": "review:alice"}
    calls = []
    monkeypatch.setattr(dw.q, "get", lambda ref, *a, **k: dict(item))
    monkeypatch.setattr(dw.q, "_notify_review",
                        lambda it, gate, actor, attempt=1: calls.append(("notify", attempt))
                        or {"ok": True, "state": "pending"})
    monkeypatch.setattr(dw.q, "block", lambda ref, *a, **k: calls.append(("block", k["origin"])))
    meta = {"gate": "review:alice"}
    assert _lost(dw, "review", "review:LQ-9:0", ref="LQ-9", meta=meta)[0]["result"] \
        == "renotified"
    assert _lost(dw, "review", "review:LQ-9:0", attempt=2, ref="LQ-9",
                 meta=meta)[0]["result"] == "blocked"
    assert calls == [("notify", 2), ("block", "system")]
    item["gate_pending"] = "verify"                       # moved on: nothing to do
    assert _lost(dw, "review", "review:LQ-9:1", attempt=2, ref="LQ-9",
                 meta=meta)[0]["result"] == "noop (review moved on)"


def test_stage_answer_confirm_and_lost(dw, monkeypatch):
    lv, rc = dw.liveness, dw.receipts
    confirmed = []
    monkeypatch.setattr(dw.stages, "_confirm_stage_answer",
                        lambda ref, gen: confirmed.append((ref, gen)) or {"ok": 1})
    did = lv.new_delivery_id()
    nonce = rc.make_nonce(did)
    p = _transcript(SID2, "")
    lv.register_spawn_delivery(purpose="stage_answer", dedupe_key="stage_answer:LQ-1:1:plan",
                               nonce=nonce, delivery_id=did, session_id=SID2,
                               engine="claude", ref="LQ-1", meta={"gen": 1})
    _append(p, f"goal...\n{nonce}")
    out = lv.sweep_deliveries()
    assert [r["result"] for r in out] == ["E13"] and confirmed == [("LQ-1", 1)]
    # codex stage: no session id at spawn -> unverified -> stays handed_off
    did = lv.new_delivery_id()
    lv.register_spawn_delivery(purpose="stage_answer", dedupe_key="stage_answer:LQ-2:1:plan",
                               nonce=rc.make_nonce(did), delivery_id=did, session_id="",
                               engine="codex", ref="LQ-2", meta={"gen": 1})
    out = lv.sweep_deliveries()
    assert [r["result"] for r in out] == ["stays handed_off"] and len(confirmed) == 1


def test_plan_lost_to_a_non_stage_target_is_only_recorded(dw, monkeypatch):
    """No live stage session for the target: nothing to respawn (the real
    respawn path is tests/test_stage_sessions.py::test_lost_plan_message_*)."""
    asked = []
    monkeypatch.setattr(dw.stages, "request", lambda ref, why: asked.append(ref))
    assert _lost(dw, "plan", "plan:LQ-3:planner:discuss", ref="LQ-3", worker_id="w9",
                 meta={"kind": "discuss", "role": "planner"})[0]["result"] \
        == "recorded only (target is not the live stage session)"
    assert asked == []


def _reminder_ticket(dw, monkeypatch):
    """An unsupervised (github-backed) plan discussion awaiting the planner,
    whose session SID is running; the fallback reminder rides a fake live
    transport that records a real pre-send receipt (nonce offset)."""
    ref = dw.q.enqueue(project=PQ, note="plan me", source="test", gates=["plan"])["ref"]

    def _do(it, plan):
        plan.update(status="discussing", round=1, version=1, text="v1",
                    planner={"worker_id": "lq-planner"},
                    discussion={"status": "active", "awaiting": "planner", "round": 1,
                                "nudges": 0, "objections": "no tests",
                                "updated_at": "2000-01-01T00:00:00Z"})
    dw.q._plan_update(ref, _do)
    monkeypatch.setattr(dw.stages, "_github_backed", lambda it: True)
    monkeypatch.setattr(dw.cli, "_plan_role_alive", lambda item, role: True)
    p = _transcript()
    import watchtower.messages as messages
    sent = []

    def send(target, body, **kw):
        rec = dw.receipts.record(SID, body, "uds", nonce=dw.receipts.nonce_of(body))
        sent.append(body)
        return {"ok": True, "transport": "uds", "receipt_id": rec["id"]}
    monkeypatch.setattr(messages, "send", send)
    return ref, p, sent


def test_plan_reminder_counts_only_when_its_nonce_lands(dw, monkeypatch):
    """Verifier probe (WT-31 D4): delivery {ok, pending} must not bump
    ``nudges`` nor count as success; the ledger's confirmed handler does."""
    ref, p, sent = _reminder_ticket(dw, monkeypatch)
    assert dw.cli.recover_plan_discussions(PQ, stale_s=0) == 0
    disc = dw.q.get(ref)["plan"]["discussion"]
    assert disc["nudges"] == 0 and disc["reminder"]["role"] == "planner"
    row = dw.liveness.delivery_row(f"plan:{ref}:planner:reminder")
    assert row["state"] == "pending" and not row.get("unverified")
    assert dw.liveness.sweep_deliveries() == []          # nothing landed yet: still pending
    assert dw.q.get(ref)["plan"]["discussion"]["nudges"] == 0
    assert dw.cli.recover_plan_discussions(PQ, stale_s=0) == 0 and len(sent) == 1
    _append(p, "unrelated output")
    assert dw.liveness.sweep_deliveries() == []          # advanced is not landed
    _append(p, sent[0])                                  # the planner read it
    out = dw.liveness.sweep_deliveries()
    assert [(r["state"], r["result"]) for r in out] == [("confirmed", "nudge counted")]
    disc = dw.q.get(ref)["plan"]["discussion"]
    assert disc["nudges"] == 1 and "reminder" not in disc


def test_plan_reminder_lost_escalates_without_counting(dw, monkeypatch):
    ref, p, sent = _reminder_ticket(dw, monkeypatch)
    assert dw.cli.recover_plan_discussions(PQ, stale_s=0) == 0
    out = dw.liveness.sweep_deliveries(now=_later(dw))
    assert [(r["state"], r["result"]) for r in out] == [("lost", "escalated")]
    it = dw.q.get(ref)
    assert it["plan"]["status"] == "blocked" and it["plan"]["discussion"]["nudges"] == 0
    assert it["needs_input"] and "never landed" in it["block_question"]


# ------------------------------------------------- D2.8 transport allowlist
LOW_LEVEL = frozenset({"deliver_via_uds", "write_to_worker_fifo", "_write_fifo_frame",
                       "send_lines", "deliver_message", "_resume_session_headless"})
STATE_WRITES = frozenset({
    "pa_transition", "set_resume_state", "release", "block", "update_status",
    "recover_claim", "update", "reopen", "plan_discussion_record_nudge", "_fallback_reopen",
    "resume_claim", "claim_by_ref", "claim_next", "close", "answer", "unblock",
    "_set_undeliverable", "_confirm_stage_answer", "_on_answer_confirmed", "_plan_update",
})


def _call_kind(call: ast.Call) -> str:
    """'low' (a raw transport), 'blocked' (_deliver_to_blocked_session),
    'verified' (liveness.deliver) or ''."""
    f = call.func
    name = f.id if isinstance(f, ast.Name) else f.attr if isinstance(f, ast.Attribute) else ""
    base = f.value.id if isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name) else ""
    if name in LOW_LEVEL or (name == "send" and "messages" in base):
        return "low"
    if name == "_deliver_to_blocked_session":
        return "blocked"
    if name == "deliver" and base == "liveness":
        return "verified"
    return ""


def _tops(tree: ast.Module):
    for n in tree.body:
        if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)):
            yield n.name, n
        elif isinstance(n, ast.ClassDef):
            for m in n.body:
                if isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    yield m.name, m


def _names(node) -> set:
    return {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}


def _has_transport(node) -> bool:
    return any(isinstance(c, ast.Call) and _call_kind(c) == "low" for c in ast.walk(node))


def _advisory_leaks(fn) -> list:
    """State writes fed by a transport result in ``fn`` (directly, through a
    name tainted by it, or under an ``if`` on it)."""
    tainted: set = set()
    changed = True
    while changed:
        changed = False
        for n in ast.walk(fn):
            targets, value = [], None
            if isinstance(n, ast.Assign):
                targets, value = n.targets, n.value
            elif isinstance(n, (ast.AnnAssign, ast.AugAssign, ast.NamedExpr)):
                targets, value = [n.target], n.value
            elif isinstance(n, ast.For):
                targets, value = [n.target], n.iter
            if value is None or not (_has_transport(value) or _names(value) & tainted):
                continue
            new = set().union(*(_names(t) for t in targets)) - tainted
            if new:
                tainted |= new
                changed = True

    def dirty(node) -> bool:
        return _has_transport(node) or bool(_names(node) & tainted)

    def is_write(c) -> bool:
        f = c.func
        name = f.id if isinstance(f, ast.Name) else f.attr if isinstance(f, ast.Attribute) else ""
        return name in STATE_WRITES

    leaks = []
    for n in ast.walk(fn):
        if isinstance(n, ast.Call) and is_write(n) and \
                any(dirty(a) for a in list(n.args) + [k.value for k in n.keywords]):
            leaks.append(f"line {n.lineno}: {ast.unparse(n)[:80]}")
        if isinstance(n, (ast.If, ast.IfExp, ast.While)) and dirty(n.test):
            for c in ast.walk(n):
                if isinstance(c, ast.Call) and is_write(c) and c is not n.test:
                    leaks.append(f"line {c.lineno}: {ast.unparse(c)[:80]} under a "
                                 f"transport-result test")
    return leaks


def scan_transports(sources, lv):
    """Problems with the D2.8 sender lists over ``{module: source}``."""
    verified, advisory, layer = lv.VERIFIED_SENDERS, lv.ADVISORY_SENDERS, lv.TRANSPORT_LAYER
    problems, seen = [], set()
    for a, b in ((verified, advisory), (verified, layer), (advisory, layer)):
        for x in sorted(a & b):
            problems.append(f"{x} is in two lists")
    for mod, src in sorted(sources.items()):
        tree = ast.parse(src)
        for name, fn in _tops(tree):
            kinds = {_call_kind(c) for c in ast.walk(fn) if isinstance(c, ast.Call)} - {""}
            if not kinds:
                continue
            q = f"{mod}.{name}"
            seen.add(q)
            if q not in verified | advisory | layer:
                problems.append(f"{q} sends ({sorted(kinds)}) but is in no sender list")
                continue
            if "low" in kinds and q in verified:
                problems.append(f"{q} is verified but calls a raw transport")
            if kinds & {"blocked", "verified"} and q not in verified:
                problems.append(f"{q} sends verified but is not in VERIFIED_SENDERS")
            if q in advisory:
                problems += [f"{q}: {x}" for x in _advisory_leaks(fn)]
        for n in tree.body:
            if not isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and \
                    any(isinstance(c, ast.Call) and _call_kind(c) for c in ast.walk(n)):
                problems.append(f"{mod}: module-level transport call (line {n.lineno})")
    for x in sorted((verified | advisory | layer) - seen):
        problems.append(f"{x} is listed but sends nothing (stale)")
    return problems


def _sources():
    return {p.stem: p.read_text() for p in sorted(ROOT.glob("*.py"))}


def test_transport_allowlist():
    import watchtower.liveness as lv
    assert scan_transports(_sources(), lv) == []


def test_transport_allowlist_catches_mutations():
    import watchtower.liveness as lv
    src = _sources()
    src["zz_mut"] = (
        "def rogue(t):\n"
        "    return messages.deliver_message(t, 'x')\n"
        "def _wake(t):\n"
        "    r = messages.send(t, 'x')\n"
        "    if r.get('ok'):\n"
        "        q.pa_transition('R', 1, 'a', 'b')\n"
        "def _deliver(t):\n"
        "    workers.deliver_via_uds(t, 'x')\n"
        "    liveness.deliver(t, 'x', purpose='answer', dedupe_key='k')\n"
    )

    class Lists:
        VERIFIED_SENDERS = lv.VERIFIED_SENDERS | {"zz_mut._deliver"}
        ADVISORY_SENDERS = lv.ADVISORY_SENDERS | {"zz_mut._wake"}
        TRANSPORT_LAYER = lv.TRANSPORT_LAYER | {"zz_mut.gone"}
    got = "\n".join(scan_transports(src, Lists))
    assert "zz_mut.rogue sends" in got
    assert "zz_mut._wake: line 5" in got or "zz_mut._wake: line 6" in got
    assert "zz_mut._deliver is verified but calls a raw transport" in got
    assert "zz_mut.gone is listed but sends nothing" in got


# ------------------------------------- outright failed sends (verifier F1)
# A send every transport refused leaves a ``failed`` row with ``failed_at``.
# It is settled through the purpose's handler exactly once: synchronously by
# a caller that owns the outcome (``settle_failed``), else by the sweep once
# FAILED_SETTLE_GRACE_S passed. Each test injects the real failure path.
GRACE = 31.0


def _fail_messages(monkeypatch, error="no live transport"):
    import watchtower.messages as messages
    sends = []

    def fail(target, body, **kw):
        sends.append(body)
        return {"ok": False, "error": error}
    monkeypatch.setattr(messages, "send", fail)
    monkeypatch.setattr(messages, "deliver_message", fail)
    return sends


def _sweep(dw, dt):
    return [(r["purpose"], r["state"], r["result"])
            for r in dw.liveness.sweep_deliveries(now=time.time() + dt)]


@pytest.mark.parametrize("purpose", ["nudge", "release", "plan", "review", "answer",
                                     "stage_answer", "resume"])
def test_every_purpose_settles_a_failed_send_exactly_once(dw, monkeypatch, purpose):
    _fail_messages(monkeypatch)
    lv = dw.liveness
    seen = []
    monkeypatch.setitem(lv.HANDLERS, purpose,
                        lambda row, confirmed: seen.append((row["state"], confirmed)) or "handled")
    sent = lv.deliver("someone", "hi", purpose=purpose, dedupe_key=f"{purpose}:k",
                      transports=("message",))
    assert sent["state"] == "failed" and not sent["ok"]
    row = lv.deliveries(purpose=purpose)[-1]
    assert row["state"] == "failed" and row["failed_at"] and "no live transport" in row["reason"]
    assert _sweep(dw, 1) == []                       # grace: the caller may settle it
    assert _sweep(dw, GRACE) == [(purpose, "failed", "handled")]
    assert seen == [("failed", False)]
    assert _sweep(dw, 7200) == [] and lv.settle_failed(sent["delivery_id"]) is None
    assert seen == [("failed", False)]


def test_settle_failed_runs_the_handler_once_and_the_sweep_skips_it(dw, monkeypatch):
    _fail_messages(monkeypatch)
    lv = dw.liveness
    seen = []
    monkeypatch.setitem(lv.HANDLERS, "resume", lambda row, c: seen.append(row["delivery_id"])
                        or "handled")
    sent = lv.deliver("x", "hi", purpose="resume", dedupe_key="resume:k", transports=("message",))
    assert lv.settle_failed(sent["delivery_id"]) == "handled"
    assert lv.settle_failed(sent["delivery_id"]) is None
    assert _sweep(dw, 7200) == [] and seen == [sent["delivery_id"]]


def test_legacy_failed_rows_without_failed_at_are_left_alone(dw):
    lv = dw.liveness
    lv._save_deliveries([{"delivery_id": "old", "purpose": "review", "dedupe_key": "k",
                          "state": "failed", "sent_at": time.time()}], time.time())
    assert _sweep(dw, 7200) == []


def test_review_failed_send_renotifies_once_then_blocks(dw, monkeypatch):
    """The verifier's probe: resolvable agent target, messages.send fails
    outright. The real _notify_review records the failed row; the sweep
    renotifies once (also failing), then blocks for a human; nothing after."""
    import watchtower.messages as messages
    sends = _fail_messages(monkeypatch)
    monkeypatch.setattr(messages, "resolve_target", lambda t: {"kind": "agent", "name": t})
    monkeypatch.setattr(messages, "ccc_forward_target", lambda t: None)
    item = {"ref": "LQ-7", "project": PQ, "status": "in_review",
            "gate_pending": "review:alice", "title": "t"}
    monkeypatch.setattr(dw.q, "get", lambda ref, *a, **k: dict(item))
    blocks = []
    monkeypatch.setattr(dw.q, "block", lambda ref, *a, **k: blocks.append((ref, k)))
    res = dw.q._notify_review(item, "review:alice", None)
    assert res["state"] == "failed" and len(sends) == 1
    assert "REVIEW_NOTIFY" in Path(os.environ["WATCHTOWER_ACTIVITY_LOG"]).read_text()
    assert [(r["state"], r["attempts"]) for r in dw.liveness.deliveries(purpose="review")] \
        == [("failed", 1)]
    assert _sweep(dw, GRACE) == [("review", "failed", "renotify failed (its row settles it)")]
    assert len(sends) == 2 and blocks == []
    assert _sweep(dw, 2 * GRACE) == [("review", "failed", "blocked")]
    assert len(blocks) == 1 and blocks[0][1]["origin"] == "system"
    assert "no live transport" in blocks[0][1]["question"]
    assert _sweep(dw, 7200) == [] and len(sends) == 2 and len(blocks) == 1


def test_review_renotify_with_no_target_left_blocks_at_once(dw, monkeypatch):
    item = {"ref": "LQ-8", "status": "in_review", "gate_pending": "review:alice"}
    monkeypatch.setattr(dw.q, "get", lambda ref, *a, **k: dict(item))
    monkeypatch.setattr(dw.q, "_notify_review", lambda *a, **k: None)
    blocks = []
    monkeypatch.setattr(dw.q, "block", lambda ref, *a, **k: blocks.append(ref))
    assert _lost(dw, "review", "review:LQ-8:0", ref="LQ-8",
                 meta={"gate": "review:alice"})[0]["result"] == "blocked"
    assert blocks == ["LQ-8"]


def test_answer_failed_send_hands_off_through_the_handler_once(dw, monkeypatch):
    """Initial send fails outright (messages + headless): E3 then E10 via the
    answer handler, synchronously; the sweep never acts on it again."""
    _fail_messages(monkeypatch)
    monkeypatch.setattr(dw.answers, "_resumable", lambda engine, sid: True)
    monkeypatch.setattr(dw.cli, "_resume_session_headless", lambda *a, **k: False)
    handled = []
    real = dw.liveness.HANDLERS["answer"]
    monkeypatch.setitem(dw.liveness.HANDLERS, "answer",
                        lambda row, c: handled.append(row["state"]) or real(row, c))
    g = Golden(dw, _claimed(dw))
    g.step(dw.q.block, g.ref, session_id="w1", question="which way?", origin="worker",
           row="answer.await_human", owner="human", desired=[])
    g.step(dw.q.answer, g.ref, "left", edge="E1", row="answer.routing", desired=[])
    out = dw.answers.route_answer(g.ref, _gen(dw, g.ref))
    assert out["route"] == "reopen" and "delivery failed" in out["reason"]
    assert pa_state(g.item()) == "handed_off" and g.item()["status"] == "open"
    assert handled == ["failed"]
    row = dw.liveness.deliveries(purpose="answer")[-1]
    assert row["state"] == "failed" and row["settled_at"]
    assert _sweep(dw, 7200) == [] and handled == ["failed"]


def test_answer_failed_resend_hands_off_once(dw, monkeypatch):
    """A failed resend (the retry after a pending row went away) is settled
    the same way: E10, once."""
    _fail_messages(monkeypatch)
    monkeypatch.setattr(dw.answers, "_resumable", lambda engine, sid: True)
    bodies = []
    monkeypatch.setattr(dw.cli, "_resume_session_headless",
                        lambda sid, repo, body, engine, **k: bodies.append(body) or True)
    g = Golden(dw, _claimed(dw))
    dw.q.block(g.ref, session_id="w1", question="q?", origin="worker")
    dw.q.answer(g.ref, "left")
    gen = _gen(dw, g.ref)
    dw.answers.route_answer(g.ref, gen)                    # headless started: pending
    assert pa_state(g.item()) == "delivering"
    dw.liveness._save_deliveries([], time.time())          # its row vanished
    monkeypatch.setattr(dw.cli, "_resume_session_headless", lambda *a, **k: False)
    monkeypatch.setattr(dw.answers, "ROUTE_LEASE_S", 0)
    dw.answers.route_pending_answers()
    assert pa_state(g.item()) == "handed_off" and g.item()["status"] == "open"
    rows = dw.liveness.deliveries(purpose="answer")
    assert [(r["state"], bool(r.get("settled_at"))) for r in rows] == [("failed", True)]
    assert _sweep(dw, 7200) == []


def _live_worker(dw, wid):
    return dw.workers.record_worker(os.getpid(), PQ, "claude", wid, str(dw.tmp),
                                    str(dw.tmp / "w.log"), session_id=SID)


def test_nudge_failed_send_resends_once_then_undeliverable(dw, monkeypatch):
    sends = _fail_messages(monkeypatch)
    monkeypatch.setattr(dw.workers, "worker_turn_open", lambda w: False)
    _live_worker(dw, "lq-n1")
    w = next(x for x in dw.workers.list_workers(prune=False) if x["worker_id"] == "lq-n1")
    assert dw.workers._nudge_one(w, "claim something") == "failed"
    assert _sweep(dw, GRACE) == [("nudge", "failed", "resent: failed")]
    assert len(sends) == 2
    assert _sweep(dw, 2 * GRACE) == [("nudge", "failed", "undeliverable_since")]
    row = next(x for x in dw.workers.list_workers(prune=False) if x["worker_id"] == "lq-n1")
    assert row.get("undeliverable_since")
    assert _sweep(dw, 7200) == [] and len(sends) == 2


def test_release_failed_send_resends_once_then_logs(dw, monkeypatch):
    sends = _fail_messages(monkeypatch)
    monkeypatch.setattr(dw.workers, "worker_turn_open", lambda w: False)
    _live_worker(dw, "lq-r1")
    w = next(x for x in dw.workers.list_workers(prune=False) if x["worker_id"] == "lq-r1")
    assert dw.workers._deliver_release_instruction(w, "you are released")["delivered"] is False
    out = _sweep(dw, GRACE)
    assert len(out) == 1 and out[0][:2] == ("release", "failed") \
        and out[0][2].startswith("resent")
    assert _sweep(dw, 2 * GRACE) == [("release", "failed", "RELEASE_UNDELIVERED")]
    assert _sweep(dw, 7200) == [] and len(sends) == 2


def test_plan_reminder_failed_send_escalates_once(dw, monkeypatch):
    ref, p, sent = _reminder_ticket(dw, monkeypatch)
    _fail_messages(monkeypatch)
    assert dw.cli.recover_plan_discussions(PQ, stale_s=0) == 1        # escalated now
    it = dw.q.get(ref)
    assert it["plan"]["status"] == "blocked" and it["plan"]["discussion"]["nudges"] == 0
    assert "undelivered" in it["block_question"] and "no live transport" in it["block_question"]
    assert _sweep(dw, 7200) == []


def test_plan_discuss_failed_send_is_settled_by_the_sweep(dw, monkeypatch):
    _fail_messages(monkeypatch)
    asked = []
    monkeypatch.setattr(dw.stages, "request", lambda ref, why: asked.append(ref))
    item = {"ref": "LQ-4", "project": PQ, "plan": {"planner": {"worker_id": "w9"}}}
    assert dw.cli._plan_send(item, "planner", "hello")["state"] == "failed"
    assert _sweep(dw, GRACE) == [
        ("plan", "failed", "recorded only (target is not the live stage session)")]
    assert _sweep(dw, 7200) == []


# ---------------------- D2.8: non-receiptable engines hand off (verifier F3)
def _blocked_on(dw, engine, sid):
    ref = dw.q.enqueue(project=PQ, note="ticket", source="test")["ref"]
    dw.q.claim_by_ref(ref, "w-" + engine, session_uuid=sid)
    dw.workers.record_worker(os.getpid(), PQ, engine, "w-" + engine, str(dw.tmp),
                             str(dw.tmp / "w.log"), session_id=sid)
    return ref


@pytest.mark.parametrize("engine,sid", [("kimi", "session_11111111-2222-3333-4444-555555555555"),
                                        ("devin", "22222222-3333-4444-5555-666666666666")])
def test_non_receiptable_answer_hands_off_before_any_resume(dw, monkeypatch, engine, sid):
    """The verifier's probe: a transport failure used to fall through to a
    headless resume (unverified_ok) whose row the sweep then read lost while
    the process ran. Now nothing is sent or spawned: the claim is released
    with the answer on the ticket, and no ledger row exists."""
    sends = _fail_messages(monkeypatch)
    spawned = []
    monkeypatch.setattr(dw.cli, "_resume_session_headless",
                        lambda *a, **k: spawned.append(a) or True)
    ref = _blocked_on(dw, engine, sid)
    item = dw.q.get(ref)
    assert dw.cli._deliver_to_blocked_session(item, "use B", "apply: use B", engine, "") == 0
    assert spawned == [] and sends == [] and dw.liveness.deliveries() == []
    it = dw.q.get(ref)
    assert it["status"] == "open" and not it.get("claimed_by")
    assert "A: use B" in it["text"]
    assert _sweep(dw, 7200) == []


def test_non_receiptable_reject_hands_off_with_feedback(dw, monkeypatch):
    spawned = []
    monkeypatch.setattr(dw.cli, "_resume_session_headless",
                        lambda *a, **k: spawned.append(a) or True)
    sends = _fail_messages(monkeypatch)
    import argparse
    sid = "session_33333333-4444-5555-6666-777777777777"
    ref = dw.q.enqueue(project=PQ, note="ticket", source="test", gates=["review"])["ref"]
    dw.q.claim_by_ref(ref, "w-kimi", session_uuid=sid)
    dw.workers.record_worker(os.getpid(), PQ, "kimi", "w-kimi", str(dw.tmp),
                             str(dw.tmp / "w.log"), session_id=sid)
    dw.q.update_status(ref, "closed", "w-kimi", hold_review="review")
    assert dw.q.get(ref)["status"] == "in_review"
    assert dw.cli.cmd_reject(argparse.Namespace(ref=ref, reason="missing tests", by="boss",
                                                json=False, engine=None)) == 0
    assert spawned == [] and sends == [] and dw.liveness.deliveries() == []
    it = dw.q.get(ref)
    assert it["status"] == "open" and not it.get("claimed_by")
    assert "missing tests" in str(it.get("gate_feedback"))       # rides the release


def test_non_receiptable_engines_are_not_resumable_for_answers(dw):
    import importlib
    import watchtower.answers as answers
    real = importlib.reload(answers)
    try:
        assert real._resumable("kimi", "session_x") is False
        assert real._resumable("devin", "dv-x") is False
    finally:
        importlib.reload(answers)


def test_receipt_engine_failure_still_resumes_then_releases(dw, monkeypatch):
    """Claude keeps the steer -> headless path; an outright failure settles
    its row through the resume handler once, then releases the claim."""
    _fail_messages(monkeypatch)
    monkeypatch.setattr(dw.cli, "_resume_session_headless", lambda *a, **k: False)
    handled = []
    real = dw.liveness.HANDLERS["resume"]
    monkeypatch.setitem(dw.liveness.HANDLERS, "resume",
                        lambda row, c: handled.append(row["state"]) or real(row, c))
    ref = _blocked_on(dw, "claude", SID)
    assert dw.cli._deliver_to_blocked_session(dw.q.get(ref), "B", "apply B", "claude", "") == 0
    assert handled == ["failed"]
    it = dw.q.get(ref)
    assert it["status"] == "open" and "A: B" in it["text"]
    assert _sweep(dw, 7200) == [] and handled == ["failed"]

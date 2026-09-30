"""WT-31 test helpers: box algebra for the table checks, synthetic items for
``stages.desired()`` agreement, and the real-transition golden harness.

Not a test module (no ``test_`` prefix); imported by tests/test_liveness_*.py.
"""

from __future__ import annotations

import importlib
import itertools
import time
from typing import Any, Dict, Iterator, List, Optional

import pytest

SID = "11111111-2222-3333-4444-555555555555"
SID2 = "22222222-3333-4444-5555-666666666666"


# ------------------------------------------------------------ box algebra
def uncovered(region: Dict[str, frozenset], boxes, dims) -> Optional[Dict[str, Any]]:
    """A point of ``region`` in none of ``boxes`` (None when covered)."""
    if any(not region[d] for d in dims):
        return None
    live = [b for b in boxes if all(region[d] & b.get(d, region[d]) for d in dims)]
    if not live:
        return {d: sorted(region[d], key=str)[0] for d in dims}
    b = live[0]
    for d in dims:
        if not region[d] <= b[d]:
            w = uncovered(dict(region, **{d: region[d] - b[d]}), live, dims)
            if w:
                return w
            return uncovered(dict(region, **{d: region[d] & b[d]}), live, dims)
    return None


def intersect(b1, b2, dims) -> Optional[Dict[str, frozenset]]:
    out = {d: b1[d] & b2[d] for d in dims}
    return None if any(not v for v in out.values()) else out


def coverage_problems(lv, vocab: Optional[Dict[str, tuple]] = None) -> List[str]:
    """D2.1: every point of the live product is in a row or UNREACHABLE, and
    no reachable point is in two rows."""
    vocab = vocab if vocab is not None else lv.live_vocab()
    dims = lv.DIMS
    full = {d: frozenset(vocab[d]) for d in dims}
    problems = []
    every = [b for r in lv.ROWS for b in r.boxes] + [b for u in lv.UNREACHABLE for b in u.boxes]
    w = uncovered(full, every, dims)
    if w:
        problems.append(f"uncovered state {w}")
    unreach = [b for u in lv.UNREACHABLE for b in u.boxes]
    for r1, r2 in itertools.combinations(lv.ROWS, 2):
        for b1 in r1.boxes:
            for b2 in r2.boxes:
                inter = intersect(b1, b2, dims)
                if inter is None:
                    continue
                w = uncovered(inter, unreach, dims)
                if w:
                    problems.append(f"rows {r1.id} and {r2.id} both match {w}")
    return problems


# ----------------------------------------------------------- synthetic items
BY_REF = {
    "B-OK": {"ref": "B-OK", "status": "closed"},
    "B-WAIT": {"ref": "B-WAIT", "status": "open"},
    "B-STUCK": {"ref": "B-STUCK", "status": "closed", "product_nack": True},
}
_DEP = {"ok": ["B-OK"], "waiting": ["B-WAIT"], "stuck": ["B-STUCK"]}


def synth(s: Any, i: int, now: float) -> Dict[str, Any]:
    """An item whose projection is the state ``s`` (queue ``GH`` = github)."""
    proj = "GH" if s.backend == "github" else "LQ"
    it: Dict[str, Any] = {"ref": f"{proj}-{i}", "project": proj, "status": s.status,
                          "gates": ["plan"] if s.gated else [], "verify_cycle": 4}
    plan: Dict[str, Any] = {}
    if s.plan:
        plan.update(status=s.plan, round=2, version=3)
    if s.disc != "none" or s.awaiting != "none":
        disc: Dict[str, Any] = {"round": 2,
                                "awaiting": "" if s.awaiting == "none" else s.awaiting}
        if s.disc != "none":
            disc["status"] = s.disc
        plan["discussion"] = disc
    if plan:
        it["plan"] = plan
    if s.gate_pending == "verify":
        it["gate_pending"] = "verify"
    elif s.gate_pending == "review":
        it["gate_pending"] = "review:alice"
    if s.assessment != "none":
        it["assessment"] = {"status": s.assessment, "cycle": 3}
    if s.block != "none":
        it["needs_input"] = True
        it["block_kind"] = s.block
    if s.readiness != "claimable":
        it["readiness"] = s.readiness
    it["blocked_by"] = list(_DEP[s.dep])
    if s.claimed:
        it["claimed_by"] = "w1"
    if s.parked:
        it["parked"] = {"worker_id": "w1", "session_id": SID}
    if s.answer != "none":
        stored = "affinity" if s.answer == "affinity_expired" else s.answer
        until = now + (-600 if s.answer == "affinity_expired" else 600)
        it["pending_answer"] = {"gen": 1, "state": stored,
                                "affinity_until": time.strftime(
                                    "%Y-%m-%dT%H:%M:%SZ", time.gmtime(until))}
    return it


def github_by_name(project: Any):
    return object() if str(project or "").upper().startswith("GH") else None


# -------------------------------------------------------------- the store
@pytest.fixture()
def wt(tmp_path, monkeypatch):
    """A real, isolated store with queue/answers/stages/workers reloaded."""
    for var, name in (("WATCHTOWER_STORE", "queue.json"),
                      ("WATCHTOWER_ACTIVITY_LOG", "activity.log"),
                      ("WATCHTOWER_OUTBOX_FILE", "outbox.json"),
                      ("WATCHTOWER_WORKERS_FILE", "workers.json"),
                      ("WATCHTOWER_WORKER_IDS_FILE", "worker-ids.json"),
                      ("WATCHTOWER_WORKER_SESSIONS_FILE", "worker-sessions.json"),
                      ("WATCHTOWER_CONFIG_FILE", "config.json"),
                      ("WATCHTOWER_STOP_SIGNALS_DIR", "stop-signals"),
                      ("CLAUDE_CONFIG_DIR", "claude-home")):
        monkeypatch.setenv(var, str(tmp_path / name))
    monkeypatch.setenv("WATCHTOWER_DELEGATE_URL", "off")
    monkeypatch.delenv("WATCHTOWER_MACHINE", raising=False)
    import watchtower.queue as q
    import watchtower.workers as workers
    importlib.reload(q)
    importlib.reload(workers)
    import watchtower.answers as answers
    import watchtower.stages as stages
    import watchtower.liveness as liveness
    import watchtower.cli as cli
    for mod in (answers, stages, liveness, cli):
        importlib.reload(mod)

    class Ns:
        pass

    ns = Ns()
    ns.q, ns.workers, ns.answers, ns.stages, ns.liveness, ns.cli = (
        q, workers, answers, stages, liveness, cli)
    ns.tmp = tmp_path
    # Answer routing reads worker/transcript facts; tests opt in per case.
    monkeypatch.setattr(answers, "worker_alive", lambda wid: False)
    monkeypatch.setattr(answers, "_wake", lambda *a, **k: None)
    monkeypatch.setattr(answers, "_resumable", lambda engine, sid: False)
    return ns


# ---------------------------------------------------------- golden harness
def pa_state(item: Optional[Dict[str, Any]]) -> str:
    return str(((item or {}).get("pending_answer") or {}).get("state") or "none")


class Golden:
    """Drive one ticket through real transitions. Each ``step`` asserts the
    edge check (a changed answer state is the named declared edge, whose
    writers/callers list the called function; an unchanged one is a
    status-only move), then the row, owner and ``desired()``."""

    def __init__(self, wt, ref: str):
        self.wt, self.ref = wt, ref
        self.trail: List[str] = []

    def item(self) -> Dict[str, Any]:
        return self.wt.q.get(self.ref)

    def step(self, fn, *args, edge: Optional[str] = None, row: Optional[str] = None,
             owner: Optional[str] = None, desired: Optional[List[str]] = None, **kw):
        q = self.wt.q
        before = self.item()
        out = fn(*args, **kw)
        after = self.item()
        tup = (pa_state(before), pa_state(after),
               str(before.get("status")), str(after.get("status")))
        name = getattr(fn, "__name__", str(fn))
        if tup[0] != tup[1] or edge is not None:
            assert edge is not None, f"{name}: undeclared-in-test answer move {tup}"
            assert tup in q.answer_edges(edge), f"{name}: {tup} is not edge {edge}"
            e = next(x for x in q.ANSWER_TRANSITIONS if x["id"] == edge)
            assert name in e["writers"] + tuple(e.get("callers") or ()), \
                f"{name} is not a writer/caller of {edge}"
        self.trail.append(f"{name}:{edge or 'status-only'}")
        self.check(row=row, owner=owner, desired=desired)
        return out

    def check(self, row: Optional[str] = None, owner: Optional[str] = None,
              desired: Optional[List[str]] = None) -> Any:
        lv = self.wt.liveness
        it = self.item()
        r = lv.row_of(it)   # every reachable state classifies (raises otherwise)
        if row is not None:
            assert r.id == row, f"{self.ref} row {r.id}, expected {row} ({lv.project(it)})"
        if owner is not None:
            assert r.owner == owner
        got = [d["role"] for d in self.wt.stages.desired([it])]
        if desired is not None:
            assert got == desired, f"desired {got}, expected {desired}"
        proof = lv.prove(it, r)
        if proof["kind"] == "stage_session":
            assert [(d["role"], d["key"]) for d in self.wt.stages.desired([it])] == \
                [(proof["role"], proof["key"])]
        else:
            assert got == []
        return r


def states(lv, vocab: Optional[Dict[str, tuple]] = None, **fixed: Any) -> Iterator[Any]:
    vocab = vocab or lv.ALL()
    dims = lv.DIMS
    pools = [(fixed[d],) if d in fixed else vocab[d] for d in dims]
    for combo in itertools.product(*pools):
        yield lv.State(*combo)

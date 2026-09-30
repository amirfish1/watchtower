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


# ------------------------------------------------ D2.6 answer-transition scan
import ast  # noqa: E402
from pathlib import Path  # noqa: E402

PKG = Path(__file__).resolve().parent.parent / "watchtower"


def package_sources() -> Dict[str, str]:
    return {p.stem: p.read_text() for p in sorted(PKG.glob("*.py"))}


def _callee(node: ast.Call) -> str:
    f = node.func
    return f.attr if isinstance(f, ast.Attribute) else f.id if isinstance(f, ast.Name) else ""


def _const_str(node: Any) -> Optional[str]:
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def _str_or_tuple(node: Any) -> Optional[tuple]:
    """A str literal or tuple/list of str literals, as a tuple; else None."""
    s = _const_str(node)
    if s is not None:
        return (s,)
    if isinstance(node, (ast.Tuple, ast.List)):
        out = tuple(_const_str(e) for e in node.elts)
        return out if out and all(x is not None for x in out) else None
    return None


def _is_pa_ref(node: Any) -> bool:
    """``pa`` or ``<x>["pending_answer"]`` -- a pending-answer dict."""
    if isinstance(node, ast.Name):
        return node.id == "pa"
    return (isinstance(node, ast.Subscript)
            and _const_str(getattr(node, "slice", None)) == "pending_answer")


def _top_defs(tree: ast.Module):
    """Top-level functions (methods count under their own name)."""
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            yield node
        elif isinstance(node, ast.ClassDef):
            for sub in node.body:
                if isinstance(sub, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    yield sub


def _wrapper_args(call: ast.Call, name: str) -> Dict[str, Any]:
    pos = {"pa_transition": ("ident", "gen", "from_state", "to_state"),
           "pa_bump_attempts": ("ident", "gen", "state")}[name]
    out: Dict[str, Any] = {pos[i]: a for i, a in enumerate(call.args[:len(pos)])}
    out.update({k.arg: k.value for k in call.keywords if k.arg})
    return out


class Scan:
    """Wrapper-aware scan of every ``pending_answer`` state write. Each
    violation is ``(function, message)``; ``edges`` maps a function to the
    ``(old, new, old_status, new_status)`` tuples its call sites can make."""

    def __init__(self, lv, q, sources: Optional[Dict[str, str]] = None):
        self.lv, self.q = lv, q
        self.sources = sources if sources is not None else package_sources()
        self.violations: List[tuple] = []
        self.edges: Dict[str, set] = {}
        self.writes_delivered: set = set()
        self.wrappers = lv.PA_WRAPPERS
        self.inlock = lv.PA_INLOCK_WRITERS
        by_id = {e["id"]: e for e in q.ANSWER_TRANSITIONS}
        self._by_id = by_id
        for mod, src in self.sources.items():
            tree = ast.parse(src, filename=f"{mod}.py")
            for fn in _top_defs(tree):
                self._scan_def(mod, fn)

    def bad(self, fn: str, msg: str) -> None:
        self.violations.append((fn, msg))

    # a/d/e: direct writes, per enclosing top-level function
    def _scan_def(self, mod: str, fn: ast.AST) -> None:
        name = fn.name
        if name in self.wrappers and mod == "queue":
            self._scan_wrapper(fn)
        for node in ast.walk(fn):
            if isinstance(node, ast.Assign):
                for t in node.targets:
                    if (isinstance(t, ast.Subscript) and _const_str(t.slice) == "state"
                            and _is_pa_ref(t.value) and name != "_pa_set_state"):
                        self.bad(name, "writes pa['state'] outside _pa_set_state")
                    if isinstance(t, ast.Subscript) and _const_str(t.slice) == "pending_answer":
                        self._inlock(name, self._dict_state(node.value), "assigns pending_answer")
            if isinstance(node, ast.Dict):
                keys = {_const_str(k) for k in node.keys if k is not None}
                if {"state", "gen"} <= keys and name != "_write_pending_answer_unlocked":
                    self.bad(name, "builds a pending_answer dict literal")
            if isinstance(node, ast.Delete):
                for t in node.targets:
                    if isinstance(t, ast.Subscript) and _const_str(t.slice) == "pending_answer":
                        self._inlock(name, "none", "deletes pending_answer")
            if not isinstance(node, ast.Call):
                continue
            callee = _callee(node)
            if callee == "pop" and node.args and _const_str(node.args[0]) == "pending_answer":
                self._inlock(name, "none", "pops pending_answer")
            elif callee == "_pa_set_state" and name not in self.wrappers:
                to = _const_str(node.args[2]) if len(node.args) > 2 else None
                self._inlock(name, to, "calls _pa_set_state")
            elif callee == "_pa_cas" and name not in self.wrappers:
                self.bad(name, "calls _pa_cas (only the PA_WRAPPERS may)")
            elif callee in self.wrappers and name not in self.wrappers:
                self._call_site(name, node, callee)

    @staticmethod
    def _dict_state(node: Any) -> Optional[str]:
        if isinstance(node, ast.Dict):
            for k, v in zip(node.keys, node.values):
                if k is not None and _const_str(k) == "state":
                    return _const_str(v)
        return None

    def _inlock(self, fn: str, to: Optional[str], what: str) -> None:
        if fn not in self.inlock:
            self.bad(fn, f"{what} but is not a PA_INLOCK_WRITER")
            return
        if to is None:
            self.bad(fn, f"{what} with a non-literal state")
            return
        tos = {self._by_id[e]["to"] for e in self.inlock[fn]}
        if to not in tos:
            self.bad(fn, f"{what} to {to!r}, not in its edges {self.inlock[fn]}")
        if to == "delivered":
            self.writes_delivered.add(fn)
            if fn not in self.lv.RECEIPT_CONFIRMED_WRITERS:
                self.bad(fn, "moves an answer to delivered without a receipt")

    # b: wrapper bodies
    def _scan_wrapper(self, fn: ast.FunctionDef) -> None:
        name, spec = fn.name, self.wrappers[fn.name]
        params = {s[1] for s in spec.values() if s[0] in ("param", "reopen")}
        cas = [n for n in ast.walk(fn) if isinstance(n, ast.Call) and _callee(n) == "_pa_cas"]
        if len(cas) != 1:
            self.bad(name, f"wrapper has {len(cas)} _pa_cas calls, expected 1")
        else:
            args = cas[0].args
            for idx, side in ((2, "from"), (3, "from_status")):
                want = spec[side]
                got = args[idx] if len(args) > idx else None
                ok = (isinstance(got, ast.Name) and got.id == want[1]) if want[0] == "param" \
                    else (_str_or_tuple(got) == tuple(want[1]))
                if not ok:
                    self.bad(name, f"wrapper's _pa_cas {side} is not {want}")
        for n in ast.walk(fn):
            if isinstance(n, ast.Call) and _callee(n) == "_pa_set_state":
                arg = n.args[2] if len(n.args) > 2 else None
                if spec["to"][0] != "param" or not (isinstance(arg, ast.Name)
                                                    and arg.id == spec["to"][1]):
                    self.bad(name, "wrapper's _pa_set_state does not pass its to_state param")
            if isinstance(n, (ast.Assign, ast.AugAssign, ast.AnnAssign)):
                targets = n.targets if isinstance(n, ast.Assign) else [n.target]
                for t in targets:
                    for sub in ast.walk(t):
                        if isinstance(sub, ast.Name) and sub.id in params:
                            self.bad(name, f"wrapper rebinds {sub.id}")
                        if (isinstance(sub, ast.Subscript)
                                and _const_str(sub.slice) == "status"):
                            self.bad(name, "wrapper writes status directly")
            if isinstance(n, ast.Call) and _callee(n) == "_release_claim_to_open_unlocked":
                if not self._under_if_reopen(fn, n):
                    self.bad(name, "wrapper releases the claim outside `if reopen:`")

    @staticmethod
    def _under_if_reopen(fn: ast.AST, target: ast.AST) -> bool:
        for n in ast.walk(fn):
            if (isinstance(n, ast.If) and isinstance(n.test, ast.Name) and n.test.id == "reopen"
                    and any(target is x for s in n.body for x in ast.walk(s))):
                return True
        return False

    # c/f: wrapper call sites
    def _call_site(self, fn: str, call: ast.Call, wrapper: str) -> None:
        spec = self.wrappers[wrapper]
        args = _wrapper_args(call, wrapper)

        def side(key):
            s = spec[key]
            if s[0] == "param":
                v = _str_or_tuple(args.get(s[1]))
                if v is None:
                    self.bad(fn, f"{wrapper} call passes a non-literal {s[1]}")
                return v
            if s[0] == "literal":
                return tuple(s[1])
            return None

        froms, statuses = side("from"), side("from_status")
        tos = froms if spec["to"][0] == "same" else side("to")
        reopen = False
        if spec["to_status"][0] == "reopen" and "reopen" in args:
            r = args["reopen"]
            if not (isinstance(r, ast.Constant) and isinstance(r.value, bool)):
                self.bad(fn, f"{wrapper} call passes a non-literal reopen")
                return
            reopen = r.value
        if froms is None or tos is None or statuses is None:
            return
        pairs = zip(froms, froms) if spec["to"][0] == "same" else \
            ((a, b) for a in froms for b in tos)
        for old, new in pairs:
            for st in statuses:
                if self.lv.pair_unreachable(old, st):
                    continue
                new_st = "open" if reopen else st
                edge = (old, new, st, new_st)
                self.edges.setdefault(fn, set()).add(edge)
                if self.lv.pair_unreachable(new, new_st):
                    self.bad(fn, f"{wrapper} can land in unreachable {new}/{new_st}")
                    continue
                ids = [e["id"] for e in self.q.ANSWER_TRANSITIONS
                       if edge in self.q.answer_edges(e["id"]) and fn in e["writers"]]
                if not ids:
                    self.bad(fn, f"{wrapper} makes {edge}, not an edge {fn} writes")
                if new == "delivered" and old != "delivered":
                    self.writes_delivered.add(fn)
                    if fn not in self.lv.RECEIPT_CONFIRMED_WRITERS:
                        self.bad(fn, "moves an answer to delivered without a receipt")

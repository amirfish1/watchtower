"""Ticket liveness state table (WT-31).

Every ticket state is a point in a product of declared dimensions
(``project``). The table below gives each reachable point exactly one row:
who owns progress from there (``owner``), what proves the owner is working
(``proof``) and what recovers the ticket when it is not (``recover``). Points
no code path can produce are listed in UNREACHABLE with the reason. The
tests (tests/test_liveness_table.py) hold the table to the code: coverage and
disjointness over the whole product, ``stages.desired()`` agreement, frozen
vocabularies, and real-transition goldens.

The claim resolver (``claim_owner``) answers "is the process bound to this
claim alive": ``alive`` on any live source, ``dead`` only on death evidence
plus the engine's checks, else ``unproven``. Nothing here acts on a ticket.

Adding a state or an edge: docs/worker-lifecycle.md.
"""

from __future__ import annotations

import collections
import os
import subprocess
import time
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Tuple

from . import queue as q


class UndeclaredState(ValueError):
    """A stored value outside the declared vocabularies."""


class UnreachableState(ValueError):
    """A projected state the table declares unreachable."""


class UnclassifiedState(ValueError):
    """A reachable state with zero or several matching rows."""


DIMS = ("status", "gated", "backend", "plan", "disc", "awaiting", "gate_pending",
        "assessment", "block", "readiness", "dep", "claimed", "parked", "answer")
State = collections.namedtuple("State", DIMS)

_BOOLS = (False, True)

# Frozen copies of the live vocabularies (queue.py). Rows are written against
# these; D2.4 holds them equal to the live tuples, so a value added there fails
# until a row (or UNREACHABLE entry) covers it.
_FROZEN: Dict[str, tuple] = {
    "status": ("open", "in_progress", "in_review", "awaiting_answer", "closed"),
    "gated": _BOOLS,
    "backend": ("file", "github"),
    "plan": ("", "planning", "reviewing", "discussing", "accepted", "failed", "blocked"),
    "disc": ("none", "active", "agreed", "escalated"),
    "awaiting": ("none", "planner", "reviewer"),
    "gate_pending": ("none", "verify", "review"),
    "assessment": ("none", "due", "running", "filing", "done", "failed"),
    "block": ("none", "input", "rationale", "awaiting-client"),
    "readiness": ("claimable", "needs-shaping", "needs-spec", "needs-rationale"),
    "dep": ("ok", "waiting", "stuck"),
    "claimed": _BOOLS,
    "parked": _BOOLS,
    "answer": ("none", "routing", "delivering", "queued", "delivered", "affinity",
               "affinity_expired", "handed_off"),
}
_FROZEN_SETTLED = ("none", "delivered", "handed_off")
_FROZEN_INFLIGHT = ("routing", "delivering", "queued", "affinity", "affinity_expired")


def ALL(dim: Optional[str] = None) -> Any:
    """Frozen vocabulary of ``dim`` (a copy of all of them when None)."""
    if dim is None:
        return {d: tuple(v) for d, v in _FROZEN.items()}
    return tuple(_FROZEN[dim])


def live_vocab() -> Dict[str, tuple]:
    """The live vocabularies, read from queue.py at call time."""
    return {
        "status": tuple(q.VALID_STATUSES), "gated": _BOOLS,
        "backend": tuple(q.BACKENDS), "plan": tuple(q.PLAN_STATUSES),
        "disc": tuple(q.DISC_STATUSES), "awaiting": tuple(q.DISC_AWAITING),
        "gate_pending": tuple(q.GATE_KINDS), "assessment": tuple(q.ASSESSMENT_STATUSES),
        "block": ("none",) + tuple(q.BLOCK_KINDS),
        "readiness": ("claimable",) + tuple(q.UNCLAIMABLE_READINESS),
        "dep": tuple(q.DEP_VERDICTS), "claimed": _BOOLS, "parked": _BOOLS,
        "answer": tuple(q.ANSWER_STATES),
    }


# ------------------------------------------------------------------ boxes
# A box fixes a set of values per dim (unlisted dims: every value). A row is a
# union of boxes, so conditions like "not PLAN_ACTIVE" stay plain data.
Box = Dict[str, FrozenSet[Any]]


def _box(**dims: Any) -> Box:
    out: Box = {}
    for d in DIMS:
        if d in dims:
            v = dims[d]
            vals = frozenset(v) if isinstance(v, (tuple, list, set, frozenset)) else frozenset((v,))
            unknown = vals - set(_FROZEN[d])
            if unknown:
                raise UndeclaredState(f"row uses undeclared {d} values {sorted(map(str, unknown))}")
            out[d] = vals
        else:
            out[d] = frozenset(_FROZEN[d])
    return out


def _not(dim: str, *vals: Any) -> tuple:
    return tuple(v for v in _FROZEN[dim] if v not in vals)


_PA_PLANS = ("", "planning", "reviewing", "discussing", "blocked")
_PENDING_PLANS = ("", "planning", "reviewing", "discussing")
# PLAN_ACTIVE = gated and plan not in {accepted, failed}; its complement as boxes.
PA = {"gated": True, "plan": _PA_PLANS}
NOT_PA = ({"gated": False}, {"gated": True, "plan": ("accepted", "failed")})
SETTLED = _FROZEN_SETTLED
INFLIGHT = _FROZEN_INFLIGHT
_LIVE = ("open", "in_progress")

Row = collections.namedtuple("Row", "id owner proof recover boxes")


def _row(rid: str, owner: str, proof: str, recover: str,
         alts: Tuple[Dict[str, Any], ...] = ({},), **common: Any) -> Row:
    return Row(rid, owner, proof, recover,
               tuple(_box(**dict(common, **alt)) for alt in alts))


# proof kinds: "stage:<role>" (a supervised stage session; key from prove()),
# "claim_owner", "system_op", "lease", "staffing", "tick", "-" (a human).
ROWS: Tuple[Row, ...] = (
    _row("closed", "terminal", "-", "-",
         ({"assessment": ("none", "done")}, {"assessment": "failed", "block": "none"}),
         status="closed"),
    _row("assess.escalated", "human", "-", "-",
         status="closed", assessment="failed", block=_not("block", "none")),
    _row("assess.filing", "reconciler", "system_op", "assessment_run_ops",
         status="closed", assessment="filing"),
    _row("assess.run.file", "assessor", "stage:assessor", "stage",
         status="closed", assessment=("due", "running"), backend="file"),
    _row("assess.run.github", "human", "-", "-",
         status="closed", assessment=("due", "running"), backend="github"),
    _row("human.block", "human", "-", "-",
         status=("open", "in_progress", "in_review"), block=_not("block", "none")),
    _row("answer.await_human", "human", "-", "-",
         status="awaiting_answer", block=_not("block", "none")),
    _row("answer.parked_bare", "reconciler", "system_op", "update_status(open, handoff)",
         status="awaiting_answer", block="none", answer="none"),
    _row("answer.routing", "answer_router", "lease", "E5", NOT_PA,
         status="awaiting_answer", block="none", answer="routing"),
    _row("answer.plan_conflict", "reconciler", "tick", "E5/E10/E11",
         status=("awaiting_answer", "open", "in_progress"), block="none",
         answer=INFLIGHT, **PA),
    _row("answer.delivering", "bound_worker", "claim_owner", "E10", NOT_PA,
         status="in_progress", block="none", answer="delivering"),
    _row("answer.queued", "bound_worker", "claim_owner", "E7/E10", NOT_PA,
         status="in_progress", block="none", answer="queued"),
    _row("answer.affinity", "prior_worker", "claim_owner", "E11", NOT_PA,
         status="open", block="none", answer="affinity"),
    _row("answer.affinity_expired", "reconciler", "tick", "E11", NOT_PA,
         status="open", block="none", answer="affinity_expired"),
    _row("review.verify", "verifier", "stage:verifier", "stage",
         status="in_review", gate_pending="verify", block="none"),
    _row("review.gate", "human", "-", "-",
         status="in_review", gate_pending="review", block="none"),
    _row("plan.github", "human", "-", "-",
         status=_LIVE, backend="github", block="none", answer=SETTLED, **PA),
    _row("plan.blocked", "human", "-", "BACKSTOP_UNBLOCKED_PLAN",
         status=_LIVE, gated=True, plan="blocked", backend="file", block="none",
         answer=SETTLED),
    _row("plan.start", "planner", "stage:planner", "stage",
         ({"status": "open"}, {"status": "in_progress", "claimed": True}),
         gated=True, plan="", backend="file", block="none", answer=SETTLED),
    _row("plan.start.unclaimed_ip", "human", "-", "-",
         status="in_progress", claimed=False, gated=True, plan="", backend="file",
         block="none", answer=SETTLED),
    _row("plan.planning", "planner", "stage:planner", "stage",
         status=_LIVE, gated=True, plan="planning", backend="file", block="none",
         answer=SETTLED),
    _row("plan.reviewing", "plan_reviewer", "stage:plan_reviewer", "stage",
         status=_LIVE, gated=True, plan="reviewing", disc=_not("disc", "active"),
         backend="file", block="none", answer=SETTLED),
    _row("plan.discuss.planner", "planner", "stage:planner", "stage",
         status=_LIVE, gated=True, plan="discussing", disc="active", awaiting="planner",
         backend="file", block="none", answer=SETTLED),
    _row("plan.discuss.reviewer", "plan_reviewer", "stage:plan_reviewer", "stage",
         status=_LIVE, gated=True, plan="reviewing", disc="active", awaiting="reviewer",
         backend="file", block="none", answer=SETTLED),
    _row("plan.discuss.orphan", "human", "-", "-",
         ({"plan": "reviewing", "disc": "active", "awaiting": ("none", "planner")},
          {"plan": "discussing", "disc": _not("disc", "active")},
          {"plan": "discussing", "disc": "active", "awaiting": ("none", "reviewer")}),
         status=_LIVE, gated=True, backend="file", block="none", answer=SETTLED),
    _row("human.readiness", "human", "-", "-", NOT_PA,
         status="open", readiness=_not("readiness", "claimable"), block="none",
         answer=SETTLED),
    _row("dep.waiting", "dependency", "-", "-", NOT_PA,
         status="open", readiness="claimable", dep="waiting", block="none", answer=SETTLED),
    _row("dep.stuck", "reconciler", "system_op", "_escalate_stuck_blockers", NOT_PA,
         status="open", readiness="claimable", dep="stuck", block="none", answer=SETTLED),
    _row("work.claimed", "worker", "claim_owner", "reopen", NOT_PA,
         status="in_progress", claimed=True, block="none", answer=SETTLED),
    _row("work.unowned", "human", "-", "-", NOT_PA,
         status="in_progress", claimed=False, block="none", answer=SETTLED),
    _row("work.open", "pool", "staffing", "spawn", NOT_PA,
         status="open", readiness="claimable", dep="ok", block="none", answer=SETTLED),
)
ROWS_BY_ID = {r.id: r for r in ROWS}

Unreachable = collections.namedtuple("Unreachable", "id reason cite boxes")


def _unreach(uid: str, reason: str, cite: str, *alts: Dict[str, Any]) -> Unreachable:
    return Unreachable(uid, reason, cite, tuple(_box(**a) for a in (alts or ({},))))


UNREACHABLE: Tuple[Unreachable, ...] = (
    _unreach("block_inflight", "a block supersedes the answer record; the stuck-blocker "
             "escalation skips in-flight answers", "E17, queue._escalate_stuck_blockers",
             {"block": _not("block", "none"), "answer": INFLIGHT}),
    _unreach("parked_claimed", "parking releases the claim; a block on a parked ticket "
             "never claims", "queue._park_unlocked, queue.block",
             {"status": "awaiting_answer", "claimed": True}),
    _unreach("parked_answer", "a parked ticket's answer is none or routing; every other "
             "state leaves awaiting_answer", "E1, E3-E5, E15-E17",
             {"status": "awaiting_answer", "answer": _not("answer", "none", "routing")}),
    _unreach("routing_unparked", "routing leaves only by E2-E5, E14-E17",
             "answers.route_answer", {"answer": "routing", "status": _not("status", "awaiting_answer")}),
    _unreach("delivering_not_ip", "delivering/queued exist only on the resumed claim",
             "E3, E6, E7, E10, E14", {"answer": ("delivering", "queued"),
                                       "status": _not("status", "in_progress")}),
    _unreach("affinity_not_open", "affinity is entered by a reopen and left by E11/E12",
             "E4, E11, E12", {"answer": ("affinity", "affinity_expired"),
                              "status": _not("status", "open")}),
    _unreach("terminal_answer", "close / hold_review clears the record", "E16",
             {"status": ("closed", "in_review"), "answer": _not("answer", "none")}),
    _unreach("github_parked", "parking is local-store only", "queue.block",
             {"status": "awaiting_answer", "backend": "github"}),
    _unreach("github_answer", "no GitHub-backed writer of pending_answer",
             "answers.route_pending_answers", {"backend": "github", "answer": _not("answer", "none")}),
    _unreach("review_no_gate", "hold_review always writes gate_pending; accept/reject "
             "pop it with the status change", "queue.update_status(hold_review)",
             {"status": "in_review", "gate_pending": "none"}),
    _unreach("parked_record_elsewhere", "every exit from awaiting_answer drops parked",
             "update_status, resume_claim, _release_claim_to_open_unlocked, close",
             {"parked": True, "status": _not("status", "awaiting_answer")}),
    _unreach("parked_without_record", "_park_unlocked writes parked with the status",
             "queue._park_unlocked", {"parked": False, "status": "awaiting_answer"}),
)


def _vecs(boxes: Tuple[Box, ...]) -> Tuple[Tuple[FrozenSet[Any], ...], ...]:
    return tuple(tuple(b[d] for d in DIMS) for b in boxes)


def _in(state: Any, vecs: Tuple[Tuple[FrozenSet[Any], ...], ...]) -> bool:
    for vec in vecs:
        for value, allowed in zip(state, vec):
            if value not in allowed:
                break
        else:
            return True
    return False


def _by_status(entries: Tuple[Any, ...]) -> Dict[str, Tuple[Any, ...]]:
    """Per status, the (entry, vecs) pairs with a box admitting it (fast path)."""
    out: Dict[str, Tuple[Any, ...]] = {}
    for st in _FROZEN["status"]:
        out[st] = tuple((e, tuple(v for v in _vecs(e.boxes) if st in v[0]))
                        for e in entries if any(st in b["status"] for b in e.boxes))
    return out


_UNREACH_BY_STATUS = _by_status(UNREACHABLE)
_ROWS_BY_STATUS = _by_status(ROWS)


def unreachable_reason(state: Any) -> Optional[Unreachable]:
    for u, vecs in _UNREACH_BY_STATUS.get(state[0], ()):
        if _in(state, vecs):
            return u
    return None


def matching_rows(state: Any) -> List[Row]:
    return [r for r, vecs in _ROWS_BY_STATUS.get(state[0], ()) if _in(state, vecs)]


def classify(state: Any) -> Row:
    """The one row of a reachable state; raises UnreachableState or
    UnclassifiedState."""
    u = unreachable_reason(state)
    if u is not None:
        raise UnreachableState(f"{u.id}: {u.reason} ({u.cite})")
    rows = matching_rows(state)
    if len(rows) != 1:
        raise UnclassifiedState(f"{len(rows)} rows match {state}: {[r.id for r in rows]}")
    return rows[0]


def pair_unreachable(answer: str, status: str) -> bool:
    """Whether (answer state, status) is unreachable whatever the other dims
    (the scanner drops such expanded call-site pairs)."""
    for u in UNREACHABLE:
        for b in u.boxes:
            if (answer in b["answer"] and status in b["status"]
                    and all(b[d] == frozenset(_FROZEN[d]) for d in DIMS
                            if d not in ("answer", "status"))):
                return True
    return False


# ------------------------------------------------------------- projection
def _github(project: str) -> bool:
    try:
        return q._github_backend_for_project(project) is not None
    except Exception:  # noqa: BLE001
        return False


def _check(dim: str, value: Any, vocab: Dict[str, tuple]) -> Any:
    if value not in vocab[dim]:
        raise UndeclaredState(f"undeclared {dim} value {value!r}")
    return value


def project(item: Dict[str, Any], by_ref: Optional[Dict[str, Dict[str, Any]]] = None,
            now: Optional[float] = None) -> State:
    """The declared dims of ``item``. ``by_ref`` resolves ``blocked_by``
    (loaded from the store when needed and not given)."""
    now = time.time() if now is None else now
    vocab = live_vocab()
    plan = item.get("plan") or {}
    disc = plan.get("discussion") or {}
    gp = str(item.get("gate_pending") or "")
    if not gp:
        gate = "none"
    elif gp == "review" or gp.startswith("review:"):
        gate = "review"
    else:
        gate = gp
    assessment = (item.get("assessment") or {}).get("status") or "none"
    block = str(item.get("block_kind") or "input") if item.get("needs_input") else "none"
    readiness = str(item.get("readiness") or "")
    if readiness not in q.UNCLAIMABLE_READINESS:
        if readiness not in q.VALID_READINESS:
            raise UndeclaredState(f"undeclared readiness value {readiness!r}")
        readiness = "claimable"
    if item.get("blocked_by"):
        if by_ref is None:
            by_ref = q._refs_index(q.list_items())
        dep = q.blocker_verdict(item, by_ref)[0]
    else:
        dep = "ok"
    pa = item.get("pending_answer")
    if not pa:
        answer = "none"
    else:
        answer = str(pa.get("state") or "")
        if answer == "affinity_expired":
            raise UndeclaredState("affinity_expired is projection-only; never stored")
        if answer == "affinity" and not q._affinity_reserved(item, now):
            answer = "affinity_expired"
    return State(
        status=_check("status", item.get("status") or "open", vocab),
        gated=q.plan_gate(item) is not None,
        backend="github" if _github(str(item.get("project") or "")) else "file",
        plan=_check("plan", str(plan.get("status") or ""), vocab),
        disc=_check("disc", str(disc.get("status") or "none"), vocab),
        awaiting=_check("awaiting", str(disc.get("awaiting") or "none"), vocab),
        gate_pending=_check("gate_pending", gate, vocab),
        assessment=_check("assessment", assessment, vocab),
        block=_check("block", block, vocab),
        readiness=_check("readiness", readiness, vocab),
        dep=_check("dep", dep, vocab),
        claimed=bool(item.get("claimed_by")),
        parked=bool(item.get("parked")),
        answer=_check("answer", answer, vocab),
    )


def prove(item: Dict[str, Any], row: Row) -> Dict[str, str]:
    """What proves ``row``'s owner is working: ``{"kind": ...}``; a stage row
    adds the supervision ``role`` and ``key`` ``stages.desired()`` must emit."""
    if not row.proof.startswith("stage:"):
        return {"kind": row.proof}
    role = row.proof[len("stage:"):]
    plan = item.get("plan") or {}
    rnd = int(plan.get("round") or 1)
    disc = plan.get("discussion") or {}
    if row.id == "assess.run.file":
        key = f"assess:{int((item.get('assessment') or {}).get('cycle') or 1)}"
    elif row.id == "review.verify":
        key = f"verify:{int(item.get('verify_cycle') or 0)}"
    elif row.id == "plan.start":
        key = "plan:r1"
    elif row.id == "plan.planning":
        key = f"plan:r{rnd}"
    elif row.id == "plan.reviewing":
        key = f"review:r{rnd}"
    elif row.id == "plan.discuss.planner":
        key = f"discuss:r{rnd}:d{int(disc.get('round') or 1)}:planner"
    elif row.id == "plan.discuss.reviewer":
        key = f"discuss:r{rnd}:v{int(plan.get('version') or rnd)}:reviewer"
    else:  # pragma: no cover - a new stage row needs its key here
        raise UnclassifiedState(f"no stage key rule for row {row.id}")
    return {"kind": "stage_session", "role": role, "key": key}


def row_of(item: Dict[str, Any], by_ref: Optional[Dict[str, Dict[str, Any]]] = None,
           now: Optional[float] = None) -> Row:
    return classify(project(item, by_ref, now))


# ------------------------------------------------ answer-transition scanner specs
# D2.6: the static scan (tests/test_liveness_table.py) reads these. A wrapper's
# from/to/status sides come from its call sites' literal arguments.
PA_WRAPPERS: Dict[str, Dict[str, Any]] = {
    "pa_transition": {"from": ("param", "from_state"), "to": ("param", "to_state"),
                      "from_status": ("param", "from_status"),
                      "to_status": ("reopen", "reopen")},
    "pa_bump_attempts": {"from": ("param", "state"), "to": ("same",),
                         "from_status": ("literal", ("in_progress",)),
                         "to_status": ("same",)},
}
# In-lock writers that change pending_answer directly, and the edges each may make.
PA_INLOCK_WRITERS: Dict[str, Tuple[str, ...]] = {
    "_write_pending_answer_unlocked": ("E1",),
    "resume_claim": ("E3",),
    "_bind_pending_unlocked": ("E12",),
    "_reopen_pending_unlocked": ("E14", "E15"),
    "_clear_parked_unlocked": ("E16",),
    "_supersede_pending_unlocked": ("E17",),
}
# The only functions allowed to move an answer to ``delivered``: each holds a
# receipt (the confirmed ledger row, the claim output, the stage's ledger row).
RECEIPT_CONFIRMED_WRITERS = frozenset({"_on_answer_confirmed", "_bind_pending_unlocked",
                                       "_confirm_stage_answer"})


# --------------------------------------------------------------- resolver
Verdict = collections.namedtuple("Verdict", "verdict source evidence proc")


class ResolverContext:
    """Per-sweep caches: one worker listing, one ``ps`` argv scan."""

    def __init__(self, now: Optional[float] = None):
        self.now = time.time() if now is None else now
        self._rows: Optional[List[Dict[str, Any]]] = None
        self._ps: Any = False

    def rows(self) -> List[Dict[str, Any]]:
        if self._rows is None:
            try:
                from . import workers
                self._rows = workers.list_workers(prune=False)
            except Exception:  # noqa: BLE001
                self._rows = []
        return self._rows

    def ps(self) -> Optional[List[str]]:
        """Every process's argv, or None when ``ps`` failed."""
        if self._ps is False:
            self._ps = ps_argv()
        return self._ps


def ps_argv() -> Optional[List[str]]:
    try:
        proc = subprocess.run(["ps", "-ax", "-o", "command="], capture_output=True,
                              text=True, timeout=3)
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0:
        return None
    return (proc.stdout or "").splitlines()


def claim_proc_of(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """The process record bound to the ticket's claim (or park)."""
    if item.get("status") == q.PARKED_STATUS:
        return (item.get("parked") or {}).get("proc") or None
    return item.get("claim_proc") or None


def _legacy_proc(item: Dict[str, Any], ctx: ResolverContext) -> Optional[Dict[str, Any]]:
    """A claim without ``claim_proc``: its worker record is trusted only when
    it launched before the claim and its pid token / exit file is valid."""
    wid = str(item.get("claimed_by") or "")
    rec = next((w for w in reversed(ctx.rows()) if str(w.get("worker_id") or "") == wid), None)
    if not rec:
        return None
    started = q._iso_ts(rec.get("started_at"))
    claimed = q._iso_ts(item.get("claimed_at"))
    if not started or not claimed or started > claimed:
        return None
    if not (rec.get("pid_started") or rec.get("exit_file")):
        return None
    return q._claim_proc_from_record(rec, str(item.get("claimed_session_id") or ""), "legacy")


def _registry_alive(sid: str) -> bool:
    from . import workers
    row = workers._find_claude_session_row(sid) if sid else None
    return bool(row and row.get("pid") and workers._pid_alive(int(row.get("pid") or 0)))


def _registry_readable() -> bool:
    home = os.environ.get("CLAUDE_CONFIG_DIR") or os.path.join(os.path.expanduser("~"), ".claude")
    try:
        os.listdir(os.path.join(home, "sessions"))
        return True
    except OSError:
        return False


def _transcript_mtime(engine: str, sid: str) -> float:
    from . import workers
    probe = {"engine": engine, "session_id": sid}
    if engine == "codex":
        return workers._codex_rollout_mtime(probe)
    if engine == "kimi":
        return workers._kimi_wire_mtime(probe)
    if engine == "claude":
        return workers._claude_transcript_mtime(probe)
    return 0.0


def _engine_checks(cp: Dict[str, Any], sid: str, died_at: float,
                   ctx: ResolverContext) -> str:
    """'' when the engine's death checks pass, else why death is unproven."""
    engine = str(cp.get("engine") or "claude")
    if engine == "claude":
        return "" if _registry_readable() else "claude session registry unreadable"
    if engine == "codex" and q._hosted_codex_thread_owns_worker(
            str(cp.get("worker_id") or ""), sid):
        return "hosted codex thread owns the worker"
    argv = ctx.ps()
    if argv is None:
        return "ps scan failed"
    if sid and any(sid in line for line in argv):
        return "a process still names the session"
    mt = _transcript_mtime(engine, sid)
    if not mt:
        return f"{engine} transcript not found"
    if mt > died_at:
        return f"{engine} transcript written after the process died"
    return ""


def claim_owner(item: Dict[str, Any], ctx: Optional[ResolverContext] = None) -> Verdict:
    """Is the process bound to this claim alive? ``alive`` on any live source;
    ``dead`` = death evidence + engine checks + no live source; else
    ``unproven``. An ambient claim is never dead."""
    from . import workers
    ctx = ctx or ResolverContext()
    cp = claim_proc_of(item)
    if cp is None and item.get("status") != q.PARKED_STATUS:
        cp = _legacy_proc(item, ctx)
    if cp is None:
        return Verdict("unproven", "", "no claim_proc (legacy claim)", None)
    sid = str(cp.get("session_id") or item.get("claimed_session_id") or "")
    if sid and _registry_alive(sid):
        return Verdict("alive", "claude_registry", "", cp)
    if sid and any(w.get("alive") and str(w.get("session_id") or "") == sid
                   and workers.record_liveness(w)[0] == "alive" for w in ctx.rows()):
        return Verdict("alive", "wt_record", "", cp)
    if cp.get("bound") == "ambient" or not cp.get("pid"):
        return Verdict("unproven", "", "ambient claim (no bound process)"
                       if cp.get("bound") == "ambient" else "no pid recorded", cp)
    state, why, extra = workers.record_liveness(cp)
    if state == "alive":
        return Verdict("alive", "bound_process", "", cp)
    died_at = q._iso_ts(extra.get("ended_at")) or float(cp.get("died_at") or 0) or ctx.now
    blocked = _engine_checks(cp, sid, died_at, ctx)
    if blocked:
        return Verdict("unproven", "", f"{why}; {blocked}", cp)
    return Verdict("dead", "", why, cp)

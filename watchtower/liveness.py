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
    # in_progress with no claimer (``update_status(in_progress, worker='')``):
    # nobody owns it, so after STALL_S idle the backstop reopens it to the
    # pool unless a session bound to it is provably alive (GitHub: escalate).
    _row("work.unowned", "reconciler", "claim_owner", "reopen", NOT_PA,
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


# --------------------------------------------------------------- backstop (D3)
# A floor under every row's own handler: once per reconciler tick (after
# reconcile_stages) and on ``wt stages tick``. It acts only where the row's
# owner is not proven at work and the ticket has been idle for STALL_S (the
# answer floors and 1-tick rows use their own clocks), and never twice on the
# same (ref, row, fingerprint): a second stall escalates to a human instead.
# Every write is a CAS (``queue.recover_claim`` / ``queue.pa_transition``).

STALL_S = 1800.0
_SKIP_OWNERS = ("human", "terminal", "dependency")
_LOOP_GUARDED = ("stage", "spawn", "reopen", "resume", "assessment_run_ops")
# "1 tick" rows whose state can be transient between two writes of one
# command (plan_verdict, then block) wait this long before acting.
ONE_TICK_S = 120.0


def stall_s() -> float:
    try:
        return float(os.environ.get("WATCHTOWER_STALL_S", STALL_S))
    except (TypeError, ValueError):
        return STALL_S


def fingerprint(item: Dict[str, Any],
                by_ref: Optional[Dict[str, Dict[str, Any]]] = None) -> str:
    """The state a backstop decision was made on: the projected dims plus the
    fields any owner's progress moves (``updated_at``, history length,
    ``answered_at``, ``pending_answer``, the backstop count, the stage key).
    ``backstop.fingerprint`` / ``.at`` are left out, so the record can store
    the fingerprint it produced."""
    import hashlib
    import json
    try:
        dims: Any = list(project(item, by_ref if by_ref is not None else {}))
    except Exception as exc:  # noqa: BLE001 - an unprojectable state still fingerprints
        dims = ["unprojectable", str(exc)]
    bs = item.get("backstop") if isinstance(item.get("backstop"), dict) else {}
    payload = [dims, item.get("updated_at"), len(item.get("history") or []),
               item.get("answered_at"), item.get("pending_answer"),
               [bs.get("state"), bs.get("action"), bs.get("count")],
               (item.get("stage_session") or {}).get("key")]
    return hashlib.sha1(json.dumps(payload, sort_keys=True, default=str).encode()
                        ).hexdigest()[:16]


def _store_activity(item: Dict[str, Any]) -> float:
    ss = item.get("stage_session") or {}
    hist = item.get("history") or []
    ts = [q._iso_ts(item.get("updated_at")), q._iso_ts(item.get("claimed_at")),
          q._iso_ts(ss.get("spawned_at")),
          q._iso_ts((item.get("pending_answer") or {}).get("state_at")),
          q._iso_ts((item.get("parked") or {}).get("at"))]
    ts += [q._iso_ts(h.get("at")) for h in hist[-5:] if isinstance(h, dict)]
    return max(ts)


def _file_activity(item: Dict[str, Any], ctx: "ResolverContext") -> float:
    """Transcript / log mtimes of the claim's session and the stage session."""
    ts = [0.0]
    cp = claim_proc_of(item) or {}
    sid = str(cp.get("session_id") or item.get("claimed_session_id") or "")
    if sid:
        try:
            ts.append(_transcript_mtime(str(cp.get("engine") or "claude"), sid))
        except Exception:  # noqa: BLE001
            pass
    ss = item.get("stage_session") or {}
    if ss.get("worker_id"):
        rec = next((w for w in ctx.rows() if str(w.get("worker_id") or "") == ss["worker_id"]),
                   None)
        try:
            from . import stages
            ts.append(stages._activity_mtime(rec or {"log": ss.get("log", "")}))
        except Exception:  # noqa: BLE001
            pass
    return max(ts)


def last_activity(item: Dict[str, Any], ctx: Optional["ResolverContext"] = None) -> float:
    """Latest sign of progress: store timestamps (``updated_at``, history, the
    stage spawn, ``pending_answer.state_at``, ``parked.at``) and the claim's /
    stage's transcript or log mtime."""
    return max(_store_activity(item), _file_activity(item, ctx or ResolverContext()))


def _stage_verdict(item: Dict[str, Any], proof: Dict[str, str], ctx: "ResolverContext",
                   now: float) -> Tuple[str, str]:
    from . import workers
    ss = item.get("stage_session") or {}
    if ss.get("key") != proof.get("key"):
        return "none", f"no stage session for {proof.get('key')}"
    wid = str(ss.get("worker_id") or "")
    if wid:
        rec = next((w for w in ctx.rows() if str(w.get("worker_id") or "") == wid), None)
        if rec and rec.get("alive") and workers.record_liveness(rec)[0] == "alive":
            return "alive", f"stage {wid} running"
        return "dead", f"stage {wid} not running"
    if q._iso_ts(ss.get("waiting_until")) > now:
        return "alive", f"launch cooldown until {ss.get('waiting_until')}"
    return "none", "no stage session running"


def _staffed(queue: str, ctx: "ResolverContext") -> bool:
    from . import workers
    return any(str(w.get("queue") or "") == queue and w.get("alive")
               and not workers._worker_released(w) and w.get("kind") != "adhoc"
               and not w.get("stage") for w in ctx.rows())


def _sent_back_window(item: Dict[str, Any], now: float) -> bool:
    """A live sent-back claim inside its release window (WT-34 owns it)."""
    sb = item.get("sent_back")
    if not isinstance(sb, dict) or sb.get("progress_at"):
        return False
    dl = q._sent_back_deadline(item, q._sent_back_minutes(item))
    return bool(dl) and now < dl


def _resume_first(item: Dict[str, Any]) -> bool:
    """A review rejection whose builder session was never resumed (WT-30):
    resume it before handing the work to someone else."""
    r = item.get("resume")
    if not (item.get("gate_feedback") and isinstance(r, dict) and r.get("state") == "pending"):
        return False
    sid = str(item.get("claimed_session_id") or "")
    engine = str((item.get("claim_proc") or {}).get("engine") or "claude")
    try:
        from . import answers
        return answers._resumable(engine, sid)
    except Exception:  # noqa: BLE001
        return False


def _answer_floor(item: Dict[str, Any], row: Row, ctx: "ResolverContext",
                  now: float) -> Tuple[str, str, str, str]:
    """(verdict, evidence, action, reason) for the answer rows."""
    from . import answers
    pa = item.get("pending_answer") or {}
    age = now - q._iso_ts(pa.get("state_at"))
    lease = float(answers.ROUTE_LEASE_S)
    if row.id == "answer.parked_bare":
        return "none", "parked with no answer record", "reopen", "parked with no answer (handoff)"
    if row.id == "answer.plan_conflict":
        return "conflict", f"answer {pa.get('state')} under an active plan gate", \
            "answer_plan_conflict", answers.PLAN_ACTIVE_REASON
    if row.id == "answer.affinity_expired":
        return "expired", f"reserved until {pa.get('affinity_until')}", "answer_handoff", \
            "affinity expired (backstop)"
    if row.id == "answer.affinity":
        if answers.worker_alive(str(pa.get("prior_worker_id") or "")):
            return "alive", "prior worker alive", "", ""
        if age < lease:
            return "unproven", "prior worker gone", "", ""
        return "dead", "prior worker gone", "answer_handoff", "prior worker gone (backstop)"
    if row.id == "answer.routing":
        if age < 2 * lease:
            return "lease", f"routing {int(age)}s", "", ""
        return "stale", f"routing {int(age)}s > 2x lease", "answer_handoff", \
            f"answer stuck routing {int(age)}s (backstop)"
    if row.id in ("answer.queued", "answer.delivering"):
        led = delivery_row(answers.answer_key(str(item.get("ref") or ""),
                                              str(pa.get("prior_session_id") or ""),
                                              pa.get("gen")))
        if led and led.get("state") in ("sending", "pending"):
            # D4: the delivery sweep owns a sent answer until its window closes
            sent_age = now - float(led.get("sent_at") or 0)
            if sent_age < float(led.get("expire_s") or 0) + 2 * _receipt_window_s() + lease:
                return "pending", f"receipt pending {int(sent_age)}s", "", ""
    if row.id == "answer.queued":
        floor = float(answers.ANSWER_QUEUE_TTL_S) + lease
        if age < floor:
            return "lease", f"queued {int(age)}s", "", ""
        return "stale", f"queued {int(age)}s > TTL+lease", "answer_handoff", \
            f"queued answer never delivered in {int(age)}s (backstop)"
    # answer.delivering
    v = claim_owner(item, ctx)
    if age >= lease * (answers.MAX_DELIVERY_ATTEMPTS + 1):
        return v.verdict, f"delivering {int(age)}s", "answer_handoff", \
            f"answer delivery stuck {int(age)}s (backstop)"
    if v.verdict == "dead" and age >= lease:
        return "dead", v.evidence, "answer_handoff", f"resumed session dead: {v.evidence}"
    return v.verdict, v.evidence or f"delivering {int(age)}s", "", ""


def assess(item: Dict[str, Any], by_ref: Optional[Dict[str, Dict[str, Any]]] = None,
           ctx: Optional[ResolverContext] = None, now: Optional[float] = None,
           stall: Optional[float] = None) -> Dict[str, Any]:
    """Read-only: the ticket's row, owner, proof verdict, idle time and the
    backstop action it is due (``action`` '' = none). ``wt liveness`` prints
    this; ``sweep`` applies it."""
    ctx = ctx or ResolverContext()
    now = ctx.now if now is None else now
    stall = stall_s() if stall is None else stall
    by_ref = by_ref if by_ref is not None else {}
    out: Dict[str, Any] = {"ref": str(item.get("ref") or ""),
                           "queue": str(item.get("project") or ""), "row": "", "owner": "",
                           "proof": "", "verdict": "", "evidence": "", "idle_s": 0,
                           "action": "", "reason": ""}
    try:
        row = row_of(item, by_ref, now)
    except (UndeclaredState, UnreachableState, UnclassifiedState) as exc:
        out.update(row="?", verdict="unclassified", evidence=str(exc))
        return out
    proof = prove(item, row)
    out.update(row=row.id, owner=row.owner, proof=proof["kind"]
               + (f":{proof['role']}:{proof['key']}" if proof["kind"] == "stage_session" else ""))
    github = _github(out["queue"])
    verdict, evidence, action, reason = "", "", "", ""
    min_idle: Optional[float] = stall
    if row.id.startswith("answer.") and row.id != "answer.await_human":
        verdict, evidence, action, reason = _answer_floor(item, row, ctx, now)
        min_idle = None
    elif row.id == "plan.blocked":
        verdict, evidence = "unowned", "plan blocked with no open question"
        action, reason, min_idle = "unblocked_plan", "plan blocked but nobody is asked", ONE_TICK_S
    elif row.id == "dep.stuck":
        verdict, evidence = "unescalated", f"blocker {q.blocker_verdict(item, by_ref)[1]} stuck"
        action, reason, min_idle = "escalate_blockers", "stuck blocker not escalated", None
    elif row.owner in _SKIP_OWNERS:
        verdict = "human" if row.owner == "human" else row.owner
    elif proof["kind"] == "stage_session":
        verdict, evidence = _stage_verdict(item, proof, ctx, now)
        if verdict != "alive":
            action, reason = "stage", f"{proof['role']} not at work ({evidence})"
    elif row.id == "assess.filing":
        verdict, evidence = "due", "assessment ops pending"
        action, reason = "assessment_run_ops", "assessment filing stalled"
    elif row.id == "work.unowned":
        # No claimer to protect; only a provably live bound session vetoes.
        v = claim_owner(item, ctx) if claim_proc_of(item) else None
        if v is not None and v.verdict == "alive":
            verdict, evidence = "alive", v.evidence or v.source
        else:
            verdict = "unowned"
            evidence = "in_progress with no claimer" + (f" ({v.evidence})" if v else "")
            if github:
                action, reason = "escalate", "in_progress with no claimer (GitHub)"
            else:
                action, reason = "reopen", "in_progress with no claimer and no live owner"
    elif proof["kind"] == "claim_owner":
        v = claim_owner(item, ctx)
        verdict, evidence = v.verdict, v.evidence or v.source
        if _sent_back_window(item, now):
            evidence = "sent back; inside its release window (WT-34)"
        elif v.verdict == "alive":
            pass
        elif github:
            action, reason = "escalate", f"claim owner {v.verdict} ({v.evidence})"
        elif v.verdict == "dead":
            action = "resume" if _resume_first(item) else "reopen"
            reason = f"claim owner dead ({v.evidence})"
        else:
            action, reason = "escalate", f"claim owner unproven ({v.evidence})"
    elif proof["kind"] == "staffing":
        from . import config, workers
        queue = out["queue"]
        if _staffed(queue, ctx):
            verdict = "staffed"
        elif github or not config.auto_drain(queue):
            verdict, evidence = "unstaffed", "auto_drain off" if not github else "github"
        else:
            engine = config.engine(queue)
            cooldown = workers.active_launch_failure_cooldown(queue, engine)
            if cooldown:
                verdict, evidence = "unstaffed", f"{engine} launch cooldown"
            else:
                verdict, evidence = "unstaffed", "no effective worker"
                action, reason = "spawn", "open work, no effective worker"
    if action and min_idle is not None:
        idle = now - _store_activity(item)
        if idle >= min_idle:
            idle = now - max(_store_activity(item), _file_activity(item, ctx))
        out["idle_s"] = int(max(0.0, idle))
        if idle < min_idle:
            action, reason = "", ""
    elif action:
        out["idle_s"] = int(max(0.0, now - _store_activity(item)))
    if action in _LOOP_GUARDED:
        bs = item.get("backstop") if isinstance(item.get("backstop"), dict) else {}
        if bs.get("state") == row.id and bs.get("fingerprint") == fingerprint(item, by_ref):
            if bs.get("action") == "resume" and action in ("resume", "reopen"):
                action, reason = "reopen", "resumed builder made no progress"
            else:
                action = "escalate"
                reason = (f"second stall in {row.id} after backstop "
                          f"{bs.get('action')} ({reason})")
    if action == "stage":
        from . import stages
        if not any(d["role"] == proof["role"] and d["key"] == proof["key"]
                   for d in stages.desired([item])):
            action, reason = "escalate", f"no supervisor for state {row.id}"
    out.update(verdict=verdict, evidence=evidence, action=action, reason=reason)
    return out


# Answer floors: one declared edge each (E5 / E10 / E11), CAS on the gen.
def _floor_routing(ref: str, gen: int, reason: str) -> Optional[Dict[str, Any]]:
    return q.pa_transition(ref, gen, "routing", "handed_off", from_status="awaiting_answer",
                           reopen=True, route="reopen", reason=reason)


def _floor_bound(ref: str, gen: int, reason: str) -> Optional[Dict[str, Any]]:
    return q.pa_transition(ref, gen, ("delivering", "queued"), "handed_off",
                           from_status="in_progress", reopen=True, route="reopen",
                           reason=reason)


def _floor_affinity(ref: str, gen: int, reason: str) -> Optional[Dict[str, Any]]:
    return q.pa_transition(ref, gen, "affinity", "handed_off", from_status="open",
                           reason=reason)


def _answer_handoff(item: Dict[str, Any], reason: str) -> bool:
    pa = item.get("pending_answer") or {}
    ref, gen, state = str(item["ref"]), int(pa.get("gen") or 0), pa.get("state")
    if state == "routing":
        return _floor_routing(ref, gen, reason) is not None
    if state in ("delivering", "queued"):
        return _floor_bound(ref, gen, reason) is not None
    if state == "affinity":
        return _floor_affinity(ref, gen, reason) is not None
    return False


def _escalation_question(item: Dict[str, Any], a: Dict[str, Any]) -> str:
    ref = a["ref"]
    return (f"WatchTower backstop: {ref} stalled in state {a['row']} (owner {a['owner']}, "
            f"{a['verdict'] or '-'}: {a['evidence'] or '-'}); {a['reason']}. Decide: "
            f"`wt answer {ref} \"...\"` to continue, `wt release {ref} --force` to hand "
            f"it to the pool, or close it.")


def _expect(item: Dict[str, Any], by_ref: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    return {"status": item.get("status"), "claimed_by": item.get("claimed_by"),
            "claimed_session_id": item.get("claimed_session_id"),
            "claim_proc": item.get("claim_proc"), "fingerprint": fingerprint(item, by_ref)}


def _log_backstop(a: Dict[str, Any], result: str) -> None:
    q._log("BACKSTOP", f"{a['ref']} state={a['row']} owner={a['owner']} proof={a['proof']} "
           f"verdict={a['verdict'] or '-'} evidence={(a['evidence'] or '-')[:200]} "
           f"idle={a['idle_s']}s action={a['action']}"
           + ("" if result == "done" else f" ({result})"), queue=a["queue"])


def _apply(item: Dict[str, Any], a: Dict[str, Any], by_ref: Dict[str, Dict[str, Any]],
           spawned: set) -> str:
    """Carry out one decided action; returns ``done`` or why it did not."""
    ref, action, reason = a["ref"], a["action"], a["reason"]
    evidence = f"{a['verdict']}: {a['evidence']}"
    if action in ("answer_handoff", "answer_plan_conflict"):
        ok = _answer_handoff(item, reason)
        if ok:
            q._log("BACKSTOP_ANSWER_PLAN_CONFLICT" if action == "answer_plan_conflict"
                   else "BACKSTOP_ANSWER_HANDOFF", f"{ref} {a['row']}: {reason}",
                   queue=a["queue"])
        return "done" if ok else "race"
    if action == "escalate_blockers":
        q.escalate_stuck_blockers()
        now_it = q.get(ref) or {}
        if not now_it.get("needs_input"):
            q._log("BACKSTOP_NO_ESCALATION", f"{ref} dep.stuck: the stuck-blocker "
                   "escalation did not flag it", queue=a["queue"])
            return "no_escalation"
        return "done"
    if action == "escalate" or action == "unblocked_plan":
        question = _escalation_question(item, a)
        if action == "unblocked_plan":
            question = (f"The plan for {ref} is blocked and nobody is asked: "
                        f"`wt plan decide {ref} --accept` or `--retry`, or "
                        f"`wt answer {ref} \"retry\"`.")
            q._log("BACKSTOP_UNBLOCKED_PLAN", f"{ref} plan blocked with no open question",
                   queue=a["queue"])
        if _github(a["queue"]):
            return "done" if q.block(ref, "", question=question, origin="system") else "race"
        if a["row"] == "assess.run.file":
            asmt = item.get("assessment") or {}
            q.assessment_fail(ref, str(asmt.get("token") or ""), a["reason"])
            item = q.get(ref) or item
        done = q.recover_claim(ref, expect=_expect(item, by_ref), action="escalate",
                               reason=reason, evidence=evidence, state=a["row"],
                               question=question)
        return "done" if done else "race"
    if action in ("reopen", "resume"):
        done = q.recover_claim(ref, expect=_expect(item, by_ref), action=action,
                               reason=f"backstop: {reason}", evidence=evidence,
                               state=a["row"])
        if not done:
            return "race"
        if action == "resume":
            from . import cli
            try:
                cli._resume_rejected(done, str(done.get("gate_feedback") or ""))
            except Exception as exc:  # noqa: BLE001 - the next stall reopens it
                return f"resume failed: {exc}"
        return "done"
    # stage / spawn / assessment_run_ops: record first (CAS), then act.
    if action == "spawn" and a["queue"] in spawned:
        return "queue already staffed this sweep"
    marked = q.recover_claim(ref, expect=_expect(item, by_ref), action="mark",
                             reason=f"backstop {action}: {reason}", evidence=evidence,
                             state=a["row"])
    if not marked:
        return "race"
    try:
        if action == "stage":
            from . import stages
            stages.reconcile_stages(only_ref=ref)
        elif action == "assessment_run_ops":
            q.assessment_run_ops(ref)
        elif action == "spawn":
            from . import config, workers
            queue = a["queue"]
            repo = config.repo_path(queue) or str(item.get("repo_path") or "")
            if not repo:
                return "no repo path for the queue"
            spawned.add(queue)
            workers.spawn_workers(queue, 1, engine=config.engine(queue), repo_path=repo)
    except Exception as exc:  # noqa: BLE001 - one ticket never stops the sweep
        return f"failed: {exc}"
    return "done"


def report(queue: str = "", ref: str = "", now: Optional[float] = None) -> List[Dict[str, Any]]:
    """``assess`` for every ticket (or one), read-only (``wt liveness``)."""
    items = q.list_items()
    by_ref = q._refs_index(items)
    ctx = ResolverContext(now)
    out = []
    for it in items:
        if ref and str(it.get("ref") or "") != ref:
            continue
        if queue and str(it.get("project") or "") != queue:
            continue
        if not ref and it.get("status") == "closed" and \
                (it.get("assessment") or {}).get("status") in (None, "", "none", "done"):
            continue
        out.append(assess(it, by_ref, ctx))
    return out


def sweep(now: Optional[float] = None, only_ref: str = "") -> List[Dict[str, Any]]:
    """One backstop pass: ``assess`` every live ticket and apply the due
    actions. Logs one BACKSTOP line per action. Returns the acted rows."""
    try:
        items = q.list_items()
    except Exception:  # noqa: BLE001
        return []
    by_ref = q._refs_index(items)
    ctx = ResolverContext(now)
    acted: List[Dict[str, Any]] = []
    spawned: set = set()
    for it in items:
        if only_ref and str(it.get("ref") or "") != only_ref:
            continue
        if it.get("status") == "closed" and \
                (it.get("assessment") or {}).get("status") not in ("due", "running", "filing"):
            continue
        try:
            a = assess(it, by_ref, ctx)
            if not a["action"]:
                continue
            result = _apply(it, a, by_ref, spawned)
            _log_backstop(a, result)
            acted.append(dict(a, result=result))
        except Exception as exc:  # noqa: BLE001 - one ticket never stops the sweep
            q._log("BACKSTOP", f"{it.get('ref', '?')} error: {exc}",
                   queue=str(it.get("project") or ""))
    return acted


# ------------------------------------------------------- verified delivery (D4)
# "The transport said ok" is not delivery. ``deliver`` puts a nonce
# ``⟨wt:<delivery_id>⟩`` on the message's last line, records a receipt before
# the send (receipts.record(nonce=), offset = the pre-send transcript size)
# and registers a ledger row under a dedupe key. ``sweep_deliveries`` (daemon
# tick) moves a row to ``confirmed`` only when that nonce landed at/after the
# offset, and to ``lost`` when the receipt is lost / missing / pending past
# twice the window, or the delivery had no receipt source (``unverified``);
# then it runs the purpose's handler (HANDLERS below).

DELIVERIES_FILE: Optional[str] = None   # module override (tests); env wins
DELIVERY_PRUNE_S = 24 * 3600.0
DELIVERY_MAX_ROWS = 2000
DELIVERY_PURPOSES = ("nudge", "release", "plan", "review", "answer", "stage_answer", "resume")
# Engines whose transcript a receipt can read (receipts._transcript_stat).
RECEIPT_ENGINES = ("claude", "codex")
_LIVE_STATES = ("sending", "pending")

# D2.8: every call of a low-level transport (deliver_via_uds,
# write_to_worker_fifo, _write_fifo_frame, peer_uds.send_lines,
# messages.send / deliver_message, cli._resume_session_headless,
# cli._deliver_to_blocked_session) and of ``liveness.deliver`` sits in exactly
# one list. A verified sender reaches a session only through ``deliver``
# (nonce + ledger); an advisory sender's result never feeds a ticket-state
# write; the transport layer is the plumbing itself.
VERIFIED_SENDERS = frozenset({
    "answers._deliver", "cli._deliver_to_blocked_session", "workers._nudge_one",
    "workers._deliver_release_instruction", "cli._plan_send", "queue._notify_review", "cli.cmd_plan", "cli._resume_rejected", "cli.cmd_reopen",
    "cli.cmd_answer", "cli.cmd_gate_ack",
})
ADVISORY_SENDERS = frozenset({
    "answers._wake", "queue._notify_ticket_event", "cli.cmd_comment", "cli.cmd_send",
    "cli.cmd_chat_new", "cli.cmd_chat_nudge", "cli._daemon_loop_ticks", "dashboard.do_POST",
    "messages.ask",
})
TRANSPORT_LAYER = frozenset({
    "liveness.deliver", "liveness._send_headless", "messages._deliver_fifo",
    "messages._deliver_uds", "workers.write_to_worker_fifo", "workers.interrupt_worker_turn",
    "workers.deliver_via_uds", "workers._deliver_release_instruction_via_uds",
    "workers.spawn_workers", "workers.spawn_run_once_worker",
})


def deliveries_file():
    """``~/.watchtower/deliveries.json`` (next to the outbox, so tests are
    sandboxed by $WATCHTOWER_OUTBOX_FILE); $WATCHTOWER_DELIVERIES_FILE or
    ``DELIVERIES_FILE`` override it."""
    from pathlib import Path
    from . import messages
    env = os.environ.get("WATCHTOWER_DELIVERIES_FILE")
    if env:
        return Path(env).expanduser()
    if DELIVERIES_FILE:
        return Path(DELIVERIES_FILE).expanduser()
    return messages._outbox_file().parent / "deliveries.json"


def _deliveries_lock():
    return deliveries_file().with_suffix(".lock")


def _load_deliveries() -> List[Dict[str, Any]]:
    import json
    try:
        with open(deliveries_file(), "r") as f:
            data = json.load(f)
    except (OSError, ValueError):
        return []
    rows = data.get("deliveries") if isinstance(data, dict) else None
    return [r for r in rows if isinstance(r, dict)] if isinstance(rows, list) else []


def _save_deliveries(rows: List[Dict[str, Any]], now: float) -> None:
    import json
    cutoff = now - DELIVERY_PRUNE_S
    rows = [r for r in rows if float(r.get("sent_at") or 0) >= cutoff][-DELIVERY_MAX_ROWS:]
    path = deliveries_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = str(path) + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"deliveries": rows}, f, indent=1)
    os.replace(tmp, path)


def _mutate_deliveries(fn: Callable[[List[Dict[str, Any]]], Any],
                       now: Optional[float] = None) -> Any:
    now = time.time() if now is None else now
    with q._FileLock(_deliveries_lock()):
        rows = _load_deliveries()
        out = fn(rows)
        _save_deliveries(rows, now)
    return out


def deliveries(state: str = "", purpose: str = "") -> List[Dict[str, Any]]:
    return [r for r in _load_deliveries()
            if (not state or r.get("state") == state)
            and (not purpose or r.get("purpose") == purpose)]


def delivery_row(dedupe_key: str) -> Optional[Dict[str, Any]]:
    """The current row for ``dedupe_key`` (newest not superseded/failed)."""
    for r in reversed(_load_deliveries()):
        if r.get("dedupe_key") == dedupe_key and r.get("state") not in ("superseded", "failed"):
            return r
    return None


def new_delivery_id() -> str:
    import uuid
    return f"dlv-{uuid.uuid4().hex[:12]}"


def _receipt_window_s() -> float:
    from . import receipts
    return receipts._wait_window_s()


def _pre_receipt(sid: str, engine: str, body: str, nonce: str, did: str,
                 transport: str) -> Optional[Dict[str, Any]]:
    """The nonce receipt, recorded before the send; None without a receipt
    source (no session id, or an engine whose transcript is unreadable)."""
    if not nonce or not sid or engine not in RECEIPT_ENGINES:
        return None
    from . import receipts
    try:
        return receipts.record(sid, body, transport, engine=engine, nonce=nonce,
                               delivery_id=did)
    except Exception:  # noqa: BLE001 - no receipt: the row reads unverified
        return None


def _drop_receipt(rec: Optional[Dict[str, Any]]) -> None:
    if rec:
        from . import receipts
        try:
            receipts.discard(rec["id"])
        except Exception:  # noqa: BLE001
            pass


def _register(row: Dict[str, Any], now: float) -> None:
    def _do(rows):
        rows.append(row)
    _mutate_deliveries(_do, now)


def _finish(did: str, key: str, fields: Dict[str, Any], now: float) -> None:
    """Settle the row after the send; a live outcome supersedes the key's
    older live rows (one pending delivery per dedupe key)."""
    def _do(rows):
        for r in rows:
            if r.get("delivery_id") == did:
                r.update(fields)
            elif (fields.get("state") == "pending" and r.get("dedupe_key") == key
                  and r.get("state") in _LIVE_STATES):
                r["state"] = "superseded"
                r["settled_at"] = now
    _mutate_deliveries(_do, now)


def _unregister(did: str, now: float) -> None:
    def _do(rows):
        rows[:] = [r for r in rows if r.get("delivery_id") != did]
    _mutate_deliveries(_do, now)


def deliver(target: str, text: str, *, purpose: str, dedupe_key: str, ref: str = "",
            queue: str = "", worker_id: str = "", session_id: str = "", engine: str = "",
            transports: Tuple[str, ...] = ("uds", "fifo", "message", "headless"),
            fifo: str = "", fifo_ready: Optional[Callable[[], bool]] = None,
            fifo_busy: str = "skip", uds_fn: Optional[Callable[[str], Any]] = None,
            from_name: str = "watchtower", message: Optional[Dict[str, Any]] = None,
            headless: Optional[Dict[str, Any]] = None, unverified_ok: bool = False,
            meta: Optional[Dict[str, Any]] = None, on_lost: str = "",
            attempt: int = 1) -> Dict[str, Any]:
    """Send ``text`` over the first transport that takes it (UDS -> FIFO ->
    ``messages.deliver_message``/``send`` -> headless resume, as listed) with
    a nonce, a pre-send receipt and a ledger row.

    Returns ``{"ok", "state", "transport", "delivery_id", "nonce",
    "receipt_id", "error"}``; ``state``: ``pending`` (sent, awaiting the
    receipt), ``unverified`` (sent, no receipt source: the sweep reads it
    lost), ``queued`` (+``msg_id``; the outbox holds it), ``deferred``
    (``fifo_busy="defer"`` and the FIFO is not ready), ``in_flight`` /
    ``duplicate`` (the messages ledger already holds the key; no row),
    ``failed``. ``ok`` = sent now (pending / unverified / duplicate).

    A headless resume needs a receipt source: an engine outside
    RECEIPT_ENGINES is handed off instead (``failed``) unless
    ``unverified_ok``. A slash command carries no nonce (it would become the
    command's argument), so it is always ``unverified``."""
    from . import receipts
    from . import workers
    now = time.time()
    purpose = str(purpose)
    if purpose not in DELIVERY_PURPOSES:
        raise ValueError(f"undeclared delivery purpose {purpose!r}")
    engine = str(engine or "claude")
    did = new_delivery_id()
    slash = bool(workers._SLASH_COMMAND_RE.match(str(text or "")))
    nonce = "" if slash else receipts.make_nonce(did)
    body = str(text) if slash else receipts.with_nonce(text, nonce)
    row = {"delivery_id": did, "purpose": purpose, "dedupe_key": str(dedupe_key),
           "target": str(target or ""), "sid": str(session_id or ""), "engine": engine,
           "ref": str(ref or ""), "queue": str(queue or ""), "worker_id": str(worker_id or ""),
           "nonce": nonce, "receipt_id": "", "sent_at": now, "attempts": int(attempt),
           "state": "sending", "transport": "", "text": str(text)[:4000],
           "meta": dict(meta or {}), "on_lost": str(on_lost or "")}
    _register(row, now)
    out: Dict[str, Any] = {"ok": False, "state": "failed", "transport": "", "delivery_id": did,
                           "nonce": nonce, "receipt_id": "", "error": ""}
    errors: List[str] = []
    msg_error = ""

    def _sent(transport: str, rec: Optional[Dict[str, Any]], **extra: Any) -> Dict[str, Any]:
        rid = (rec or {}).get("id", "") or str(extra.pop("receipt_id", "") or "")
        state = "pending" if rid and nonce else "unverified"
        _finish(did, dedupe_key, dict(state="pending", unverified=state == "unverified",
                                      transport=transport, receipt_id=rid, **extra), now)
        out.update(ok=True, state=state, transport=transport, receipt_id=rid, error="")
        return out

    sid = str(session_id or "")
    for step in transports:
        if step == "uds":
            if engine != "claude" or not sid:
                continue
            rec = _pre_receipt(sid, engine, body, nonce, did, "uds")
            try:
                got = (uds_fn(body) if uds_fn is not None
                       else workers.deliver_via_uds(sid, body, from_name=from_name))
            except Exception as exc:  # noqa: BLE001 - fall through
                got, errors = None, errors + [f"uds: {exc}"]
            if got:
                return _sent("uds", rec)
            _drop_receipt(rec)
            errors.append("uds: declined")
        elif step == "fifo":
            if fifo_ready is not None and not fifo_ready():
                if fifo_busy == "defer":
                    _unregister(did, now)
                    out.update(state="deferred", error="turn open")
                    return out
                continue
            if not fifo:
                continue
            rec = _pre_receipt(sid, engine, body, nonce, did, "fifo")
            try:
                got = workers.write_to_worker_fifo(fifo, body, engine=engine)
            except Exception as exc:  # noqa: BLE001
                _drop_receipt(rec)
                _finish(did, dedupe_key, {"state": "failed"}, now)
                out.update(transport="fifo", error=f"{type(exc).__name__}: {exc}")
                return out
            if got:
                return _sent("fifo", rec)
            _drop_receipt(rec)
            errors.append("fifo: write failed")
        elif step == "message":
            if not target:
                continue
            from . import messages
            kw = dict(message or {})
            fn = kw.pop("fn", "deliver_message")
            try:
                res = (messages.send(target, body, **kw) if fn == "send"
                       else messages.deliver_message(target, body, **kw)) or {}
            except Exception as exc:  # noqa: BLE001 - never lose a message to a crash
                res = {"ok": False, "error": str(exc)}
            if res.get("deduped") and (res.get("ok") or res.get("queued") or res.get("in_flight")):
                # the messages ledger already holds this key: no second row
                _unregister(did, now)
                out.update(ok=bool(res.get("ok")), transport=str(res.get("transport") or "ledger"),
                           msg_id=str(res.get("id") or ""), busy=bool(res.get("busy")),
                           state="duplicate" if res.get("ok") else
                           ("queued" if res.get("queued") else "in_flight"),
                           error=str(res.get("error") or ""))
                return out
            if res.get("ok"):
                return _sent(str(res.get("transport") or "?"), None,
                             receipt_id=str(res.get("receipt_id") or ""))
            if res.get("queued"):
                expire = kw.get("expire", kw.get("ttl_s"))
                _finish(did, dedupe_key, {"state": "pending", "queued": True,
                                          "msg_id": str(res.get("id") or ""),
                                          "expire_s": float(expire or 0)}, now)
                out.update(state="queued", msg_id=str(res.get("id") or ""),
                           busy=bool(res.get("busy")), error=str(res.get("error") or ""))
                return out
            if res.get("in_flight"):
                _unregister(did, now)
                out.update(state="in_flight", error=str(res.get("error") or ""))
                return out
            msg_error = str(res.get("error") or "delivery failed")
            out["message_result"] = res
            errors.append(f"message: {msg_error}")
        elif step == "headless":
            h = dict(headless or {})
            if not sid:
                errors.append("headless: no session id")
                continue
            if engine not in RECEIPT_ENGINES and not unverified_ok:
                errors.append(f"headless: {engine} has no receipt source; handed off")
                continue
            rec = _pre_receipt(sid, engine, body, nonce, did, "headless-resume")
            started = _send_headless(sid, body, engine, h)
            if started:
                return _sent("headless-resume", rec)
            _drop_receipt(rec)
            errors.append("headless: resume did not start")
    _finish(did, dedupe_key, {"state": "failed"}, now)
    out["error"] = msg_error or "; ".join(errors) or "no transport"
    out["errors"] = errors
    return out


def _send_headless(sid: str, body: str, engine: str, h: Dict[str, Any]) -> bool:
    try:
        from . import cli
        return bool(cli._resume_session_headless(
            sid, str(h.get("repo") or os.getcwd()), body, engine,
            queue=str(h.get("queue") or ""), worker_id=str(h.get("worker_id") or "")))
    except Exception:  # noqa: BLE001
        return False


def register_spawn_delivery(*, purpose: str, dedupe_key: str, nonce: str, delivery_id: str,
                            session_id: str, engine: str, ref: str = "", queue: str = "",
                            worker_id: str = "", meta: Optional[Dict[str, Any]] = None,
                            text: str = "") -> Dict[str, Any]:
    """Ledger row for a message that rode a fresh session's spawn (a stage
    prompt): the session did not exist before, so the receipt baseline is
    offset 0. No session id / receipt source -> unverified."""
    now = time.time()
    rid = ""
    if session_id and engine in RECEIPT_ENGINES and nonce:
        from . import receipts
        try:
            rid = receipts.record(session_id, text, "stage-spawn", engine=engine, nonce=nonce,
                                  delivery_id=delivery_id,
                                  at_send={"path": "", "size": 0, "mtime": 0.0})["id"]
        except Exception:  # noqa: BLE001
            rid = ""
    row = {"delivery_id": delivery_id, "purpose": purpose, "dedupe_key": dedupe_key,
           "target": session_id, "sid": session_id, "engine": engine, "ref": ref,
           "queue": queue, "worker_id": worker_id, "nonce": nonce, "receipt_id": rid,
           "sent_at": now, "attempts": 1, "state": "sending", "transport": "stage-spawn",
           "text": "", "meta": dict(meta or {}), "on_lost": ""}
    _register(row, now)
    _finish(delivery_id, dedupe_key, {"state": "pending", "unverified": not rid}, now)
    return dict(row, state="pending", unverified=not rid)


# ------------------------------------------------------------- the sweep
def _outcome(row: Dict[str, Any], receipt_rows: List[Dict[str, Any]],
             now: float) -> Tuple[str, str]:
    """('confirmed' | 'lost' | '', reason) for one pending row."""
    from . import receipts
    if row.get("unverified"):
        return "lost", "unverified: no receipt source"
    window = _receipt_window_s()
    age = now - float(row.get("sent_at") or 0)
    st = receipts.by_nonce(str(row.get("nonce") or ""), receipt_rows)
    if st == "landed":
        return "confirmed", "nonce landed"
    if st == "lost":
        return "lost", "receipt lost (nonce never landed)"
    if row.get("queued"):
        if age > float(row.get("expire_s") or 0) + 2 * window:
            return "lost", "queued message never landed"
        return "", ""
    if not st:
        return "lost", "receipt missing"
    if age > 2 * window:
        return "lost", f"receipt pending {int(age)}s > 2x window"
    return "", ""


def sweep_deliveries(now: Optional[float] = None, verify: bool = True) -> List[Dict[str, Any]]:
    """Settle every pending ledger row and run its handler (daemon tick).
    A row is settled under the ledger lock before its handler runs, so two
    sweeps never both act on it. ``sending`` rows older than the dedupe
    lease were cut off mid-send and read lost too."""
    from . import receipts
    now = time.time() if now is None else now
    if verify:
        try:
            receipts.sweep(now)
        except Exception:  # noqa: BLE001
            pass
    receipt_rows = receipts._load()
    settled: List[Dict[str, Any]] = []

    def _do(rows):
        for r in rows:
            if r.get("state") == "sending":
                if now - float(r.get("sent_at") or 0) > 2 * _receipt_window_s():
                    outcome, why = "lost", "send never finished"
                else:
                    continue
            elif r.get("state") == "pending":
                outcome, why = _outcome(r, receipt_rows, now)
                if not outcome:
                    continue
            else:
                continue
            r.update(state=outcome, reason=why, settled_at=now)
            settled.append(dict(r))
    _mutate_deliveries(_do, now)
    out = []
    for r in settled:
        try:
            result = _handle(r)
        except Exception as exc:  # noqa: BLE001 - one row never stops the sweep
            result = f"error: {exc}"
        q._log("DELIVERY", f"{r.get('ref') or r.get('worker_id') or '-'} {r['purpose']} "
               f"{r['dedupe_key']} {r['delivery_id']} {r['state']} ({r.get('reason')}) "
               f"-> {result}", queue=str(r.get("queue") or ""))
        out.append(dict(r, result=result))
    return out


def _handle(row: Dict[str, Any]) -> str:
    kind = str(row.get("on_lost") or row["purpose"])
    fn = HANDLERS.get(kind)
    if fn is None:
        return "no handler"
    return fn(row, row["state"] == "confirmed") or "done"


def _gen_of(row: Dict[str, Any]) -> int:
    return int((row.get("meta") or {}).get("gen") or 0)


def _on_answer_confirmed(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """E9: the parked session's answer landed (its nonce is in the
    transcript). A CAS on gen + state: after E14/E17 it is a no-op."""
    return q.pa_transition(str(row["ref"]), _gen_of(row), ("delivering", "queued"), "delivered",
                           from_status="in_progress", route="resume")


def _h_answer(row: Dict[str, Any], confirmed: bool) -> str:
    if confirmed:
        return "E9" if _on_answer_confirmed(row) else "noop (state moved on)"
    from . import answers
    done = answers._fallback_reopen(str(row["ref"]), _gen_of(row),
                                    f"answer delivery lost ({row.get('reason')}); handed off")
    return "E10" if done else "noop (state moved on)"


def _h_stage_answer(row: Dict[str, Any], confirmed: bool) -> str:
    if not confirmed:
        return "stays handed_off"   # carried into the next stage attempt
    from . import stages
    return "E13" if stages._confirm_stage_answer(str(row["ref"]), _gen_of(row)) \
        else "noop (state moved on)"


def _h_nudge(row: Dict[str, Any], confirmed: bool) -> str:
    from . import workers
    wid = str(row.get("worker_id") or "")
    if confirmed:
        workers._set_undeliverable(wid, None)
        return "confirmed"
    if int(row.get("attempts") or 1) < 2:
        w = next((x for x in workers.list_workers() if str(x.get("worker_id") or "") == wid
                  and x.get("alive")), None)
        if w is not None:
            return "resent: " + workers._nudge_one(w, str(row.get("text") or ""), attempt=2)
    workers._set_undeliverable(wid, time.time())
    return "undeliverable_since"


def _h_release(row: Dict[str, Any], confirmed: bool) -> str:
    if confirmed:
        return "confirmed"
    from . import workers
    wid = str(row.get("worker_id") or "")
    if int(row.get("attempts") or 1) < 2:
        w = next((x for x in workers.list_workers(prune=False)
                  if str(x.get("worker_id") or "") == wid), None)
        if w is not None:
            res = workers._deliver_release_instruction(w, str(row.get("text") or ""), attempt=2)
            return f"resent: {res.get('transport')}"
    q._log("RELEASE_UNDELIVERED", f"{wid or '-'}: release instruction never landed "
           f"({row.get('reason')})", queue=str(row.get("queue") or ""))
    return "RELEASE_UNDELIVERED"


def _h_plan(row: Dict[str, Any], confirmed: bool) -> str:
    """Plan messages (``plan:<ref>:<role>:<kind>``). The ledger owns the
    outcome: a fallback ``reminder`` (WT-29, tickets the supervisor does not
    own) counts toward the nudge budget only here, on its confirmed nonce,
    and a lost one blocks the plan for a human. A lost message to a live
    stage session marks that session lost, so the supervisor kills and
    respawns it within the stage's attempt budget (escalating past it)."""
    ref = str(row.get("ref") or "")
    meta = row.get("meta") or {}
    from . import cli, stages
    if meta.get("kind") == "reminder":
        did = str(row.get("delivery_id") or "")
        if confirmed:
            item = q.plan_discussion_record_nudge(ref, True, delivery_id=did)
        else:
            item = q.plan_discussion_record_nudge(
                ref, False, f"reminder to {meta.get('role')} never landed "
                            f"({row.get('reason')})", delivery_id=did)
        if item is None:
            return "noop (discussion moved on)"
        if cli._block_stalled_plan(ref, item):
            return "escalated"
        return "nudge counted"
    if confirmed:
        return "confirmed"
    wid = str(row.get("worker_id") or "")
    if stages.mark_delivery_lost(ref, wid, f"plan {meta.get('kind') or 'message'} to "
                                           f"{meta.get('role') or wid} never landed "
                                           f"({row.get('reason')})"):
        stages.request(ref, "plan message lost")
        return "respawn: stage session marked lost"
    return "recorded only (target is not the live stage session)"


def _h_review(row: Dict[str, Any], confirmed: bool) -> str:
    if confirmed:
        return "confirmed"
    ref = str(row.get("ref") or "")
    item = q.get(ref) or {}
    meta = row.get("meta") or {}
    if item.get("status") != "in_review" or str(item.get("gate_pending") or "") != meta.get("gate"):
        return "noop (review moved on)"
    if int(row.get("attempts") or 1) < 2:
        q._notify_review(item, str(meta.get("gate") or ""), None, attempt=2)
        return "renotified"
    q.block(ref, "", origin="system", kind="input",
            question=(f"The review request for {ref} never reached {row.get('target')} (no "
                      f"receipt after two sends). Review it yourself: `wt accept {ref}` or "
                      f"`wt reject {ref} --reason \"...\"`."))
    return "blocked"


def _h_resume(row: Dict[str, Any], confirmed: bool) -> str:
    if confirmed:
        return "confirmed"
    ref, sid = str(row.get("ref") or ""), str(row.get("sid") or "")
    if ref and sid:
        q.set_resume_state(ref, sid, "failed", error=f"delivery lost: {row.get('reason')}")
    return "resume failed"


HANDLERS: Dict[str, Callable[[Dict[str, Any], bool], str]] = {
    "answer": _h_answer, "stage_answer": _h_stage_answer, "nudge": _h_nudge,
    "release": _h_release, "plan": _h_plan, "review": _h_review, "resume": _h_resume,
}

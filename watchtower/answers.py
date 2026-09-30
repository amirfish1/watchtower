"""Routing a human answer back to the session that parked a ticket (WT-28).

``q.answer`` on a parked (``awaiting_answer``) ticket only persists
``pending_answer`` (state ``routing``, keyed by ``gen``). This module moves it
forward, every step a compare-and-swap in ``queue.pa_transition`` so a stale
router (CLI vs reconciler, or a ticket that was re-blocked/closed meanwhile)
can never deliver or rebind a ticket whose state moved on.

Routes: ``reopen`` (no resumable session / over the context budget: next
claimer gets question + answer + transcript pointer), ``affinity`` (parked
worker alive: ticket opens reserved for it, it is woken, nothing is bound),
``resume`` (worker gone but session resumable: ticket rebinds to the parked
session and the answer is delivered to it).

Delivery is at-least-once between a started transport and the ledger write; the
``[answer REF#gen]`` tag in the prompt lets a session ignore a repeat.
"""

from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

from . import queue as q

ROUTE_LEASE_S = 60.0
MAX_DELIVERY_ATTEMPTS = 3
ANSWER_QUEUE_TTL_S = 30 * 60.0


def affinity_s() -> float:
    try:
        return float(os.environ.get("WATCHTOWER_ANSWER_AFFINITY_S", "1800"))
    except ValueError:
        return 1800.0


def requeue_bytes() -> int:
    try:
        return int(os.environ.get(
            "WATCHTOWER_ANSWER_REQUEUE_BYTES",
            str(2 * int(os.environ.get("WATCHTOWER_CONTEXT_RECYCLE_BYTES", "2500000") or 0)),
        ) or 0)
    except (TypeError, ValueError):
        return 5_000_000


def _iso(ts: float) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def answer_key(ref: str, sid: str, gen: Any) -> str:
    return f"answer:{ref}:{sid}:{gen}"


def wake_key(ref: str, gen: Any) -> str:
    return f"wake:{ref}:{gen}"


# ------------------------------------------------------------------ facts
def engine_for(worker_id: str, session_id: str) -> str:
    """The parked session's engine: worker record, kimi session shape, Codex
    registry, else claude."""
    if str(session_id or "").startswith("session_"):
        return "kimi"
    try:
        from . import workers
        known = workers.list_workers(prune=False)
        for field, value in (("session_id", session_id), ("worker_id", worker_id)):
            if not value:
                continue
            for w in reversed(known):
                if str(w.get(field) or "") == value:
                    eng = str(w.get("engine") or "")
                    if eng in ("claude", "codex", "kimi", "devin"):
                        return eng
    except (OSError, ValueError):
        pass
    if session_id:
        try:
            from . import codex_registry
            if (codex_registry.entry(session_id) or {}).get("engine") == "codex":
                return "codex"
        except (OSError, ValueError, ImportError):
            pass
    return "claude"


def transcript_for(session_id: str) -> str:
    if not session_id:
        return ""
    try:
        from . import messages
        return str(messages.locate_transcript(session_id, "claude") or "")
    except Exception:  # noqa: BLE001
        return ""


def _resumable(engine: str, sid: str) -> bool:
    if not sid:
        return False
    if engine == "claude":
        return bool(transcript_for(sid))
    if engine == "codex":
        try:
            from . import codex_registry
            return bool(codex_registry.entry(sid))
        except Exception:  # noqa: BLE001
            return False
    return engine in ("kimi", "devin")


def worker_alive(worker_id: str) -> bool:
    """Alive and not released from queue staffing."""
    if not worker_id:
        return False
    try:
        from . import workers
        for w in workers.list_workers(prune=False):
            if str(w.get("worker_id") or "") == worker_id:
                return bool(w.get("alive")) and not workers._worker_released(w)
    except Exception:  # noqa: BLE001
        pass
    return False


# ---------------------------------------------------------------- prompts
def _carried_text(pa: Dict[str, Any], item: Dict[str, Any]) -> str:
    rows = list(pa.get("carried") or []) + list(item.get("carried_answers") or [])
    return "".join(
        f"\nEarlier answer (superseded by your new question): {r.get('answer', '')} "
        f"(to: {r.get('question', '')})" for r in rows)


def answer_prompt(item: Dict[str, Any], pa: Dict[str, Any]) -> str:
    ref, gen, text = item["ref"], pa.get("gen"), pa.get("answer", "")
    tag = (f"[answer {ref}#{gen}; ignore a repeat of a tag you already applied] ")
    kind = str(item.get("block_kind") or "")
    if kind == "rationale":
        comment = f" The approver added: {text}." if text and text != "approved" else ""
        return (tag + f"Your product-gate pitch on ticket {ref} was APPROVED — proceed to "
                f"implementation now.{comment} Implement, verify, and close with `wt close "
                f"{ref} --worker <your-id> --summary \"...\" --commit <SHA>` (or `--no-code`).")
    if kind == "awaiting-client" and pa.get("tid"):
        return (tag + f"The client answered on ticket {ref} and this is the end of the topic "
                f"(Topic-Is-Done): {text}. Push anything still local, then close with "
                f"`wt close {ref} --worker <your-id> --summary \"...\" --commit <SHA>` "
                f"(or `--no-code`).{_carried_text(pa, item)}")
    if kind == "awaiting-client":
        return (tag + f"The client replied on ticket {ref}: {text}. Do this turn, then park "
                f"again with `wt block {ref} --worker <your-id> --kind awaiting-client` "
                f"unless the topic is finished, in which case close it instead."
                f"{_carried_text(pa, item)}")
    return (tag + f"A human answered your blocked question on ticket {ref}. Their answer: "
            f"{text}.{_carried_text(pa, item)} Apply it, finish the ticket, and close it with "
            f"`wt close {ref} --worker <your-id> --summary \"...\" --commit <SHA>` (or "
            f"`--no-code` if no code changed). If it still cannot be resolved, run `wt block` "
            f"again with the new open question.")


def claim_brief(item: Dict[str, Any], worker_id: str = "", session_id: str = "") -> str:
    """Q&A block for a claim's output (prior session vs. a handoff)."""
    pa = item.get("pending_answer")
    if not pa or pa.get("state") != "delivered":
        return ""
    owner = ((worker_id and worker_id == pa.get("prior_worker_id"))
             or (session_id and session_id == pa.get("prior_session_id")))
    extra = _carried_text(pa, item)
    if owner:
        return (f"Your question: {pa.get('question', '')}\n"
                f"Human answered: {pa.get('answer', '')}{extra}")
    where = f"; prior transcript at {pa['transcript_path']}" if pa.get("transcript_path") else ""
    return (f"Prior worker asked: {pa.get('question', '')}\n"
            f"Human answered: {pa.get('answer', '')}{extra}{where}")


# --------------------------------------------------------------- delivery
def _deliver(item: Dict[str, Any], pa: Dict[str, Any]) -> Dict[str, Any]:
    """Deliver the answer to the parked session. ``status``: ``ok`` (taken or a
    headless resume started), ``queued`` (+msg_id), ``in_flight`` (try later)
    or ``failed``."""
    from . import messages
    ref, gen = item["ref"], pa["gen"]
    sid = str(pa.get("prior_session_id") or "")
    target = sid or str(pa.get("prior_worker_id") or "")
    engine = str(pa.get("prior_engine") or "") or engine_for(
        str(pa.get("prior_worker_id") or ""), sid)
    hold = not (engine == "kimi" and not messages._delegate_base())
    try:
        sent = messages.deliver_message(
            target, answer_prompt(item, pa), verb="steer", engine=engine,
            on_busy="hold" if hold else "reject", expire=ANSWER_QUEUE_TTL_S,
            ticket_ref=ref, ticket_session=sid,
            dedupe_key=answer_key(ref, sid, gen), ticket_gen=int(gen))
    except Exception as exc:  # noqa: BLE001 - never lose the answer to a delivery crash
        sent = {"ok": False, "error": str(exc)}
    if sent.get("ok"):
        return {"status": "ok", "transport": sent.get("transport", "?")}
    if sent.get("queued"):
        return {"status": "queued", "msg_id": sent.get("id", ""),
                "busy": bool(sent.get("busy"))}
    if sent.get("in_flight"):
        return {"status": "in_flight"}
    try:  # unresolvable target: headless resume fork under the worker id
        from . import cli
        started = cli._resume_session_headless(
            sid, str(pa.get("repo_path") or item.get("repo_path") or os.getcwd()),
            answer_prompt(item, pa), engine, queue=str(item.get("project") or ""),
            worker_id=str(pa.get("prior_worker_id") or ""))
    except Exception:  # noqa: BLE001
        started = False
    if started:
        return {"status": "ok", "transport": "headless-resume"}
    return {"status": "failed", "error": str(sent.get("error") or "delivery failed")}


def _wake(item: Dict[str, Any], pa: Dict[str, Any]) -> None:
    """Tell an alive parked worker its answer is waiting. Binds nothing: if it
    is lost, the worker's next claim or the affinity expiry covers it."""
    from . import messages
    ref = item["ref"]
    sid = str(pa.get("prior_session_id") or "")
    target = sid or str(pa.get("prior_worker_id") or "")
    if not target:
        return
    try:
        messages.deliver_message(
            target,
            f"Answer for {ref} arrived; when done (or now if idle) run "
            f"`wt claim -q {item.get('project', '')} --worker {pa.get('prior_worker_id', '<your-id>')} --json`.",
            verb="steer", on_busy="hold", expire=affinity_s(),
            dedupe_key=wake_key(ref, pa["gen"]))
    except Exception:  # noqa: BLE001
        pass


# ---------------------------------------------------------------- routing
def _fallback_reopen(ref: str, gen: int, reason: str) -> Optional[Dict[str, Any]]:
    return q.pa_transition(ref, gen, ("routing", "delivering", "queued"), "handed_off",
                           from_status=("awaiting_answer", "in_progress"), reopen=True,
                           route="reopen", reason=reason)


def route_answer(ref: str, gen: int, tid: bool = False) -> Dict[str, Any]:
    """Route one pending answer. No-op unless ``gen`` matches and the record
    is still ``routing``. Returns ``{"route": ..., "reason": ...}``."""
    item = q.get(ref)
    pa = (item or {}).get("pending_answer") or {}
    if (not item or int(pa.get("gen") or -1) != int(gen) or pa.get("state") != "routing"
            or item.get("status") != q.PARKED_STATUS):
        return {"route": "noop", "reason": "nothing to route"}
    if tid and not pa.get("tid"):
        q.pa_transition(ref, gen, "routing", "routing", from_status="awaiting_answer",
                        fields={"tid": True})
        pa = (q.get(ref) or {}).get("pending_answer") or pa
    wid, sid = str(pa.get("prior_worker_id") or ""), str(pa.get("prior_session_id") or "")
    engine = str(pa.get("prior_engine") or "") or engine_for(wid, sid)
    limit = requeue_bytes()
    size = 0
    if sid and limit > 0:
        try:
            from . import workers
            size = workers._claude_transcript_bytes(sid)
        except Exception:  # noqa: BLE001
            size = 0
    if not sid:
        why = "no prior session recorded"
    elif limit > 0 and size >= limit:
        why = f"prior session over the answer context budget ({size} bytes)"
    else:
        why = ""
    if why:
        done = q.pa_transition(ref, gen, "routing", "handed_off", from_status="awaiting_answer",
                               reopen=True, route="reopen", reason=why)
        return {"route": "reopen" if done else "noop", "reason": why}
    if worker_alive(wid):
        until = _iso(time.time() + affinity_s())
        done = q.pa_transition(ref, gen, "routing", "affinity", from_status="awaiting_answer",
                               reopen=True, route="affinity",
                               reason="parked worker alive; reserved for it",
                               fields={"affinity_until": until})
        if not done:
            return {"route": "noop", "reason": "superseded"}
        _wake(done, done["pending_answer"])
        return {"route": "affinity", "reason": "parked worker alive; it will claim the answer",
                "until": until}
    if not _resumable(engine, sid):
        why = "parked session is gone and not resumable"
        done = q.pa_transition(ref, gen, "routing", "handed_off", from_status="awaiting_answer",
                               reopen=True, route="reopen", reason=why)
        return {"route": "reopen" if done else "noop", "reason": why}
    bound = q.resume_claim(ref, gen)
    if bound is None:
        # The parked worker/session holds another claim (or the CAS lost).
        if (q.get(ref) or {}).get("pending_answer", {}).get("gen") != gen:
            return {"route": "noop", "reason": "superseded"}
        why = "parked session already holds another ticket"
        done = q.pa_transition(ref, gen, "routing", "handed_off", from_status="awaiting_answer",
                               reopen=True, route="reopen", reason=why)
        return {"route": "reopen" if done else "noop", "reason": why}
    return _deliver_bound(bound, gen)


def _deliver_bound(item: Dict[str, Any], gen: int) -> Dict[str, Any]:
    """Deliver for a ticket already rebound to the parked session."""
    ref = item["ref"]
    pa = item["pending_answer"]
    res = _deliver(item, pa)
    st = res["status"]
    if st == "ok":
        q.pa_transition(ref, gen, ("delivering", "queued"), "delivered",
                        from_status="in_progress", route="resume")
        return {"route": "resume", "reason": f"delivered via {res.get('transport')}",
                "transport": res.get("transport")}
    if st == "queued":
        q.pa_transition(ref, gen, "delivering", "queued", from_status="in_progress",
                        fields={"msg_id": res.get("msg_id", "")})
        return {"route": "resume", "reason": "answer queued for delivery",
                "queued": True, "msg_id": res.get("msg_id", "")}
    if st == "in_flight":
        return {"route": "resume", "reason": "delivery in flight; the reconciler retries"}
    _fallback_reopen(ref, gen, f"delivery failed: {res.get('error', 'unknown')}")
    return {"route": "reopen", "reason": f"delivery failed ({res.get('error', 'unknown')}); handed off"}


# ------------------------------------------------------------- reconciler
def _age(pa: Dict[str, Any], now: float) -> float:
    return now - q._iso_ts(pa.get("state_at"))


def route_pending_answers(now: Optional[float] = None) -> List[str]:
    """Reconciler pass (local queues). Every step is a CAS, so a race with the
    CLI is a no-op for the loser."""
    now = time.time() if now is None else now
    acted: List[str] = []
    try:
        items = q.list_items()
    except Exception:  # noqa: BLE001
        return acted
    try:
        if q.migrate_legacy_blocks():
            acted.append("migrated")
    except Exception:  # noqa: BLE001
        pass
    for it in items:
        pa = it.get("pending_answer")
        ref = str(it.get("ref") or "")
        if not pa or not ref:
            continue
        try:
            if q._github_backend_for_project(str(it.get("project") or "")) is not None:
                continue
            gen, state = int(pa.get("gen") or 0), pa.get("state")
            if state == "routing" and _age(pa, now) >= ROUTE_LEASE_S:
                route_answer(ref, gen)
                acted.append(f"{ref}:routed")
            elif state == "delivering" and _age(pa, now) >= ROUTE_LEASE_S:
                acted.append(f"{ref}:{_retry(it, gen, now)}")
            elif state == "queued":
                tag = _check_queued(it, gen, now)
                if tag:
                    acted.append(f"{ref}:{tag}")
            elif state == "affinity":
                if (q._iso_ts(pa.get("affinity_until")) <= now
                        or not worker_alive(str(pa.get("prior_worker_id") or ""))):
                    if q.pa_transition(ref, gen, "affinity", "handed_off", from_status="open",
                                       reason="affinity expired"):
                        q._log("AFFINITY-EXPIRED", f"{ref} worker={pa.get('prior_worker_id')}",
                               queue=str(it.get("project") or ""))
                        acted.append(f"{ref}:affinity_expired")
        except Exception:  # noqa: BLE001 - one ticket never stops the pass
            continue
    return acted


def _still_bound(it: Dict[str, Any], pa: Dict[str, Any]) -> bool:
    return (it.get("status") == "in_progress"
            and str(it.get("claimed_session_id") or "") == str(pa.get("prior_session_id") or ""))


def _retry(it: Dict[str, Any], gen: int, now: float) -> str:
    ref, pa = str(it["ref"]), it["pending_answer"]
    if not _still_bound(it, pa):
        return "stale"
    if int(pa.get("attempts") or 0) + 1 >= MAX_DELIVERY_ATTEMPTS:
        _fallback_reopen(ref, gen, "answer delivery gave up; handed off")
        return "handed_off"
    bumped = q.pa_bump_attempts(ref, gen, "delivering")
    if not bumped:
        return "stale"  # CAS lost (re-block / new gen / state moved): no delivery
    _deliver_bound(bumped, gen)
    return "redelivered"


def _check_queued(it: Dict[str, Any], gen: int, now: float) -> str:
    from . import messages
    ref, pa = str(it["ref"]), it["pending_answer"]
    row = messages.outbox_row(str(pa.get("msg_id") or "")) if pa.get("msg_id") else None
    status = (row or {}).get("status")
    if status == "delivered":
        q.pa_transition(ref, gen, "queued", "delivered", from_status="in_progress")
        return "delivered"
    if status in ("pending", "held"):
        return ""
    if not _still_bound(it, pa):
        return ""
    if int(pa.get("attempts") or 0) + 1 >= MAX_DELIVERY_ATTEMPTS:
        _fallback_reopen(ref, gen, "queued answer never delivered; handed off")
        return "handed_off"
    if not q.pa_bump_attempts(ref, gen, "queued"):
        return ""
    if q.pa_transition(ref, gen, "queued", "delivering", from_status="in_progress"):
        _deliver_bound(q.get(ref) or it, gen)
        return "redelivered"
    return ""

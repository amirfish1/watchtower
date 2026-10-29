"""Duplicate links on GitHub-backed queues (OPS-208).

The link is a ticket relationship, so it lives on the follower's issue meta
(``duplicate_of``, ``duplicate_gen``, ``duplicate_result``) where every host
sees it. The follower carries the issue-level hold (OPS-215, ``why =
duplicate``) so no ``wt`` claims it while it waits.

Completion is inherited only from proof: the original's issue is closed, its
review hold is settled, and every gate either ticket requires passed in the
original's CURRENT pipeline generation, which must be the generation the link
was made against. A host that cannot read the original's pipeline (foreign or
missing sidecar) never decides, so two hosts cannot flip a follower back and
forth.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

STALE_PREFIX = "duplicate lineage changed"


def _backend(ident: Any):
    from . import queue as q
    return q._github_backend_for_project(q._project_from_ident(ident))


def _gates(item: Dict[str, Any]) -> List[str]:
    """Only this host's sidecar gates gate a GitHub ticket (OPS-204)."""
    return list(item.get("gates") or []) if item.get("pipeline_local") else []


def _unreadable(item: Dict[str, Any]) -> str:
    if item.get("pipeline_foreign"):
        return (f"{item.get('ref')}'s pipeline is owned by host "
                f"{item.get('pipeline_owner_host')!r}; link it there")
    if item.get("pipeline_missing"):
        return f"{item.get('ref')}'s pipeline sidecar entry is missing on this host"
    return ""


def proven(original: Dict[str, Any], required: List[str]) -> bool:
    """Every required gate passed in the original's current generation, and
    the issue close came after the review it settled. The sidecar keeps only
    the current generation's history and results, so a cleared generation's
    proof cannot leak into the next one.

    The overlay merges sidecar events after issue events of the same second,
    so verify/accept keep their order behind ``in_review`` (all sidecar), but
    the issue's close is compared by time, not position."""
    results = {r.get("gate"): r for r in (original.get("gate_results") or [])}
    history = list(original.get("history") or [])
    last_review = max((n for n, e in enumerate(history) if e.get("event") == "in_review"),
                      default=-1)
    after = history[last_review + 1:]
    review_at = str(history[last_review].get("at") or "") if last_review >= 0 else ""
    last_close = max((str(e.get("at") or "") for e in history if e.get("event") == "close"),
                     default=None)
    closed_after = last_close is not None and last_close >= review_at

    def ok(g: str) -> bool:
        if g == "verify":
            return bool(results.get(g, {}).get("passed") and closed_after and any(
                e.get("event") == "verify" and e.get("passed") for e in after))
        if g == "review" or g.startswith("review:"):
            return closed_after and any(e.get("event") == "accept" for e in after)
        if g == "plan" or g.startswith("plan:"):
            return (original.get("plan") or {}).get("status") == "accepted"
        return bool(results.get(g, {}).get("passed"))
    return all(ok(g) for g in required)


def evaluate(follower: Dict[str, Any], original: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """The follower's duplicate snapshot, or None when this host cannot read
    the original's pipeline (it must not decide)."""
    ref = str(follower.get("duplicate_of") or "")
    if original is not None and _unreadable(original):
        return None
    linked_gen = int(follower.get("duplicate_gen") or 0)
    gen = int((original or {}).get("pipeline_gen") or 0)
    stale = original is not None and gen != linked_gen
    required = sorted(set(_gates(follower)) | set(_gates(original or {})))
    complete = bool(original and not stale
                    and original.get("status") == "closed"
                    and not original.get("review_hold")
                    and proven(original, required))
    return {"ref": ref, "status": (original or {}).get("status", "missing"),
            "gen": gen, "linked_gen": linked_gen, "stale": stale,
            "verify_cycle": int((original or {}).get("verify_cycle") or 0),
            "closed_at": (original or {}).get("closed_at"),
            "gates": [{"gate": g, "passed": True} for g in required] if complete else [],
            "complete": complete}


def _stale_question(snap: Dict[str, Any], follower_ref: str) -> str:
    return (f"{STALE_PREFIX}: original {snap['ref']} moved from gate generation "
            f"g{snap['linked_gen']} to g{snap['gen']} since {follower_ref} was linked, so "
            f"its proof no longer covers this link. Relink (`wt duplicate {follower_ref} "
            f"--of {snap['ref']}`) or clear it (`wt duplicate {follower_ref} --clear`).")


def sync(follower_ref: str) -> Optional[Dict[str, Any]]:
    """Bring one follower in line with its original (fresh reads). Writes only
    on a change, so it is safe to run every tick."""
    backend = _backend(follower_ref)
    f = backend.get(follower_ref)
    if f is None or not f.get("duplicate_of"):
        return f
    o = backend.get(str(f["duplicate_of"]))
    snap = evaluate(f, o)
    if snap is None:
        return f
    closed_by_dup = str(f.get("closed_by") or "").startswith("duplicate:")
    in_line = (f.get("duplicate_result") == snap
               and (f.get("status") == "closed") == snap["complete"])
    if in_line:
        return f
    ref = snap["ref"]
    if snap["complete"] and f.get("status") != "closed":
        res = dict((o or {}).get("resolution") or {})
        backend.update_status(follower_ref, "closed", f"duplicate:{ref}", resolution={
            "summary": f"Duplicate of {ref}; completion inherited from the original",
            "commit": str(res.get("commit") or "")})
    elif not snap["complete"] and f.get("status") == "closed" and closed_by_dup:
        backend.update_status(follower_ref, "open",
                              reason=f"duplicate original {ref} is no longer complete")
    fields: Dict[str, Any] = {"duplicate_result": snap,
                              "pipeline_hold": "" if snap["complete"] else "duplicate"}
    if snap["stale"]:
        fields.update(needs_input=True, block_question=_stale_question(snap, follower_ref))
    elif str(f.get("block_question") or "").startswith(STALE_PREFIX):
        fields.update(needs_input=False, block_question="")
    return backend.update(follower_ref, **fields)


def sweep(items: List[Dict[str, Any]], only_ref: str = "") -> List[str]:
    """Stage-tick pass: sync GitHub followers whose listed state is out of
    line with their listed original. Reads the cheap list first; ``sync``
    re-reads both issues fresh before any write."""
    by_ref = {str(i.get("ref") or ""): i for i in items}
    acted = []
    for f in items:
        ref = str(f.get("ref") or "")
        if not f.get("duplicate_of") or not f.get("github_repo"):
            continue
        if only_ref and only_ref not in (ref, f.get("duplicate_of")):
            continue
        snap = evaluate(f, by_ref.get(str(f["duplicate_of"])))
        if snap is None:
            continue
        if (f.get("duplicate_result") == snap
                and (f.get("status") == "closed") == snap["complete"]):
            continue
        sync(ref)
        acted.append(ref)
    return acted


def link(ident: Any, original_ref: str, worker: str = "") -> Optional[Dict[str, Any]]:
    """``wt close <ref> --duplicate-of <orig>`` / ``wt duplicate`` on GitHub.
    Same refusals as the local link, plus: both pipelines must be readable on
    this host (the proof lives in its sidecar), and a held review on the
    follower settles first. ``original_ref == ""`` clears the link and
    reopens the follower for independent work."""
    from . import queue as q
    from . import github_pipeline
    backend = _backend(ident)
    f = backend.get(ident)
    if f is None:
        return None
    if worker:
        owner = str(f.get("claimed_by") or "")
        if f.get("status") == "in_progress" and owner and owner != worker:
            raise ValueError(f"{f.get('ref')} is claimed by {owner}; you are {worker}. "
                             f"Only the claiming worker may close an in-progress ticket.")
    fref = str(f.get("ref") or ident)
    if not original_ref:
        if not f.get("duplicate_of"):
            return f
        if f.get("status") == "closed":
            backend.update_status(fref, "open", reason="duplicate link cleared")
        fields: Dict[str, Any] = {"duplicate_of": "", "duplicate_gen": None,
                                  "duplicate_result": None, "pipeline_hold": ""}
        if str(f.get("block_question") or "").startswith(STALE_PREFIX):
            fields.update(needs_input=False, block_question="")
        backend.update(fref, **fields)
        fresh = backend.get(fref)
        if fresh is not None and fresh.get("pipeline_local"):
            github_pipeline._sync_hold(backend, fref, fresh)   # restore its own plan/review hold
        return backend.get(fref)
    o = backend.get(original_ref)
    if o is None or o.get("ref") == fref:
        raise ValueError("duplicate original must exist and be a different ticket")
    if str(o.get("project") or "") != str(f.get("project") or ""):
        raise ValueError("duplicate original must be in the same queue")
    for it in (f, o):
        why = _unreadable(it)
        if why:
            raise ValueError(f"cannot link duplicates here: {why}")
    followers = [i for i in backend.list_items(None, fresh=True)
                 if i.get("duplicate_of") == fref and i.get("ref") != fref]
    if o.get("duplicate_of") or followers:
        raise ValueError("duplicate chains and cycles are not allowed")
    if not set(_gates(f)).issubset(_gates(o)):
        raise ValueError("duplicate original must cover every gate required by the follower")
    if f.get("status") == "in_review":
        raise ValueError(f"{fref} is in review; settle it (wt accept / wt reject) before linking")
    if f.get("status") == "in_progress":
        if backend.update_status(fref, "open", reason=f"duplicate of {original_ref}",
                                 require_status="in_progress") is None:
            raise ValueError(f"{fref} moved while linking; retry")
    backend.update(fref, duplicate_of=original_ref,
                   duplicate_gen=int(o.get("pipeline_gen") or 0),
                   duplicate_result=None, pipeline_hold="duplicate",
                   _history=("duplicate_link", {"by": q._by("human", worker),
                                                "original": original_ref}))
    return sync(fref)

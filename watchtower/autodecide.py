"""Auto-approval of minor worker blocks (CCC-370 / Q849).

A worker may *propose* that its block is minor. WatchTower approves it only
when every deterministic check below passes; anything else stays an ordinary
owner card with the reason shown. The worker's words never grant authority:
the authority is the enabled ACTION CLASS plus the hard-stop floor veto.

Pure policy functions live here; the compare-and-swap write is
``queue.auto_answer``. History on the ticket is the single store.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

POLICY_VERSION = 1
ATTEST_KEYS = ("money", "customer-sends", "public-posts", "ad-spend", "irreversible")
MAJOR_BECAUSE = ("item", "unknown", "preference", "security")
MIN_UNDO_LEN = 12
_NO_UNDO = re.compile(r"^\W*(none|nothing|n/?a|na|unknown|tbd|no|can'?t|cannot|not possible|irreversible)\b", re.I)
_SHA = r"[0-9a-f]{7,64}"

# Global defaults. A queue may add classes / attestations and tighten numbers
# (queue-config.json -> "auto_decision"), never remove a class's requirements
# or a hard stop.
GLOBAL_POLICY: Dict[str, Any] = {
    "version": POLICY_VERSION,
    "threshold": 0.8,
    "rate_per_hour": 6,
    "attest": list(ATTEST_KEYS),
    "classes": {
        "code-change-revertable": {
            "undo": rf"^git revert {_SHA}\b",
            "needs_commit": True,
        },
    },
}

DESK_SCRIPT = Path.home() / ".hermes" / "scripts" / "decision_desk.py"


# ------------------------------------------------------------------ policy
def kill_switch_path() -> Path:
    from . import storage
    return storage.data_dir() / "auto-decision.off"


def kill_switch_on() -> bool:
    return kill_switch_path().exists()


def queue_policy(queue: str) -> Dict[str, Any]:
    """Effective policy for ``queue``: global defaults tightened by the queue.

    ``enabled`` is per queue and defaults to False. Queue overrides may raise
    the threshold, lower the rate, add attestation keys and add classes; the
    global classes and attestation keys can never be dropped."""
    from . import config
    ov = (config.get_queue_config(queue) or {}).get("auto_decision") or {}
    if not isinstance(ov, dict):
        ov = {}
    pol = json.loads(json.dumps(GLOBAL_POLICY))
    pol["enabled"] = bool(ov.get("enabled", False))
    try:
        pol["threshold"] = max(pol["threshold"], float(ov.get("threshold", 0)))
    except (TypeError, ValueError):
        pass
    try:
        pol["rate_per_hour"] = min(pol["rate_per_hour"], int(ov.get("rate_per_hour", pol["rate_per_hour"])))
    except (TypeError, ValueError):
        pass
    extra = [str(k) for k in (ov.get("attest") or []) if str(k) not in pol["attest"]]
    pol["attest"] = pol["attest"] + extra
    for name, spec in (ov.get("classes") or {}).items():
        if name in pol["classes"] or not isinstance(spec, dict):
            continue                       # a queue cannot redefine a global class
        if spec.get("undo"):
            pol["classes"][name] = {"undo": str(spec["undo"]), "needs_commit": bool(spec.get("needs_commit"))}
    # Which classes are ENABLED is explicit and per queue; none by default.
    pol["enabled_classes"] = [c for c in (ov.get("enabled_classes") or []) if c in pol["classes"]]
    return pol


def policy_hash(pol: Dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(pol, sort_keys=True).encode()).hexdigest()[:16]


# ------------------------------------------------------------ hard-stop floor
def floor_hits(text: str) -> Tuple[List[str], str]:
    """Matched hard-stop rule names, reusing decision_desk.HARD_STOPS.

    Returns (hits, error). If the desk script cannot be loaded the floor is
    unavailable and the caller must fail closed (error is non-empty)."""
    try:
        spec = importlib.util.spec_from_file_location("_wt_decision_desk", DESK_SCRIPT)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)          # type: ignore[union-attr]
        return list(mod.hard_stops(text)), ""
    except Exception as e:  # noqa: BLE001 - any failure means "no veto data": fail closed
        return [], f"hard-stop floor unavailable ({type(e).__name__})"


# ------------------------------------------------------------------ helpers
def ticket_revision(item: Dict[str, Any]) -> str:
    """Hash of the ticket's own content; a change voids a pending approval."""
    basis = json.dumps([item.get("title"), item.get("text"), item.get("note"),
                        item.get("type"), item.get("repo_path")], sort_keys=True)
    return hashlib.sha256(basis.encode()).hexdigest()[:16]


def parse_attest(raw: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for part in str(raw or "").split(","):
        if "=" in part:
            k, v = part.split("=", 1)
            out[k.strip().lower()] = v.strip().lower()
    return out


def _recent_count(items: List[Dict[str, Any]], queue: str, now: float) -> int:
    n = 0
    for it in items:
        if it.get("project") != queue:
            continue
        for h in it.get("history") or []:
            if h.get("event") != "auto_decision":
                continue
            try:
                ts = datetime.fromisoformat(str(h.get("at")).replace("Z", "+00:00")).timestamp()
            except ValueError:
                continue
            if now - ts < 3600:
                n += 1
    return n


def proposal_text(p: Dict[str, Any]) -> str:
    return "\n".join(str(p.get(k) or "") for k in ("question", "recommend", "undo", "progress"))


# --------------------------------------------------------------------- gate
def evaluate(item: Dict[str, Any], p: Dict[str, Any], *, items: Optional[List[Dict[str, Any]]] = None,
             now: Optional[float] = None) -> Dict[str, Any]:
    """Decide whether a worker's block proposal may be auto-approved.

    ``p`` carries only the worker's structured fields: class, weight,
    confidence, recommend, undo, attest, major_because, question, progress.
    Ticket text is never an input. Returns {approve, reasons, ...}; on a
    decline ``reasons`` is the owner-card text ("needs you: ...")."""
    now = time.time() if now is None else now
    queue = str(item.get("project") or "")
    pol = queue_policy(queue)
    res: Dict[str, Any] = {"approve": False, "reasons": [], "policy_hash": policy_hash(pol),
                           "policy_version": pol["version"], "matched": []}
    why = res["reasons"]
    if not p.get("class") and not p.get("weight"):
        why.append("no auto-decision proposal")        # plain block: today's behaviour
        res["plain"] = True
        return res
    if kill_switch_on():
        why.append("kill switch is on")
    if not pol["enabled"]:
        why.append("auto-approval is off for this queue")
    if p.get("weight") != "minor":
        why.append(f"worker marked it {p.get('weight') or 'unweighted'}"
                   + (f" ({p['major_because']})" if p.get("major_because") else ""))
    if p.get("major_because"):
        why.append(f"major because {p['major_because']}")
    cls = str(p.get("class") or "")
    spec = pol["classes"].get(cls)
    if not cls or spec is None or cls not in pol["enabled_classes"]:
        why.append("no enabled class" + (f" ({cls})" if cls else ""))
    att = p.get("attest") or {}
    for k in pol["attest"]:
        v = att.get(k)
        if v is None:
            why.append(f"checklist incomplete: {k} not attested")
        elif v != "no":
            why.append(f"attested {k}={v}")
    try:
        conf = float(p.get("confidence"))
    except (TypeError, ValueError):
        conf = -1.0
    if conf < pol["threshold"]:
        why.append(f"confidence {conf if conf >= 0 else 'missing'} below {pol['threshold']}")
    undo = str(p.get("undo") or "").strip()
    if len(undo) < MIN_UNDO_LEN or _NO_UNDO.match(undo):
        why.append("no concrete undo")
    elif spec and not re.match(spec["undo"], undo):
        why.append("undo does not match the class's undo form")
    rec = str(p.get("recommend") or "").strip()
    if len(rec) < 8:
        why.append("no named recommendation")
    if spec and spec.get("needs_commit"):
        commit = str(item.get("block_commit") or "")
        m = re.match(rf"^git revert ({_SHA})\b", undo)
        if not commit:
            why.append("no verified --commit recorded")
        elif m and not (commit.startswith(m.group(1)) or m.group(1).startswith(commit)):
            why.append("undo sha is not the recorded commit")
    if items is not None and pol["enabled"] and _recent_count(items, queue, now) >= pol["rate_per_hour"]:
        why.append(f"rate limit ({pol['rate_per_hour']}/hour) reached")
    # Veto-only floor: scanned over the worker's own words, never the ticket.
    hits, err = floor_hits(proposal_text(p))
    res["matched"] = hits
    if err:
        why.append(err)
    for h in hits:
        why.append(f"matched hard stop: {h}")
    # Smoke alarm (no authority): signed "no" yet the text names the thing.
    res["contradiction"] = bool(hits) and any(att.get(k) == "no" for k in att)
    res["approve"] = not why
    return res


def card_reason(res: Dict[str, Any]) -> str:
    return "needs you: " + "; ".join(res["reasons"])


AUTO_PROMPT = (
    "[answer {ref}#{gen}; ignore a repeat of a tag you already applied] This is an AUTO-DECISION by "
    "WatchTower, NOT a human approval. It authorizes ONLY your own recommendation, exactly as you "
    "stated it: {rec!r}. It approves nothing else: no customer send, money, price, ad, public post, "
    "or client/product sign-off. Apply it, verify it, then finish and close the ticket the normal "
    "way with `wt close {ref} --worker <your-id> --summary \"...\" --commit <SHA>`; state in the "
    "summary that this was an auto-decision. If anything beyond the recommendation turns out to be "
    "needed, run `wt block` again as an ordinary question. Undo, if the owner asks: `wt undo {ref}`."
)

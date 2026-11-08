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
MAJOR_BECAUSE = ("money", "customer-sends", "public-posts", "ad-spend", "irreversible",
                 "live-product", "safety-guard", "owner-only-action", "needs-information")
MIN_MAJOR_DETAIL_LEN = 40
_VAGUE_DETAIL = re.compile(
    r"\b(preference|unknown|needs? (the )?(owner|you)|owner decision|owner'?s call|not sure|tbd|your call|"
    r"unsure|don'?t know|n/?a)\b", re.I)


def validate_major(because: str, detail: str) -> Optional[str]:
    """Return an error message when a major claim lacks a concrete reason, else None (CCC-374)."""
    allowed = ", ".join(MAJOR_BECAUSE)
    because = (because or "").strip()
    detail = " ".join((detail or "").split())
    if because not in MAJOR_BECAUSE:
        return (f"--weight major needs --major-because with one of: {allowed} "
                f"(got {because or 'nothing'}). 'preference', 'unknown' and 'security' are not reasons. "
                "Nothing was recorded; rerun wt block with a concrete reason.")
    if len(detail) < MIN_MAJOR_DETAIL_LEN:
        return (f"--major-detail must be one real sentence of at least {MIN_MAJOR_DETAIL_LEN} characters "
                "saying what concretely goes wrong or what exactly only the owner can do. "
                "Nothing was recorded; restate it and rerun wt block.")
    stripped = _VAGUE_DETAIL.sub("", detail)
    if _VAGUE_DETAIL.search(detail) and len(stripped.strip()) < MIN_MAJOR_DETAIL_LEN:
        return ("--major-detail is mostly vague words (preference/unknown/needs owner/not sure/tbd/your call). "
                "State the concrete consequence or the exact fact or action only the owner has. "
                "Nothing was recorded; restate it and rerun wt block.")
    return None
MIN_UNDO_LEN = 12
_NO_UNDO = re.compile(r"^\W*(none|nothing|n/?a|na|unknown|tbd|no|can'?t|cannot|not possible|irreversible)\b", re.I)
_SHA = r"[0-9a-f]{7,64}"

# A concrete command or file edit (verb + a path, file name or number), not prose.
_REPLAY_UNDO = (r"^(?:git (?:revert|checkout|restore)|wt policy|rm|cp|mv|sed|jq|echo|tee|python3?|"
                r"restore|reset|set|edit)\b(?=.*(?:[/~]|\d|\.\w{2,5}\b))")
SUBSCRIPTION_BACKENDS = ("subscription-proxy",)
# Words that are not real money when the replay is zero-dollar on the subscription proxy.
_BENIGN_MONEY = re.compile(r"\bsubscription(?:\s+(?:proxy|auth|plan|quota|token))?|\$0(?:\.0+)?(?![\d.])", re.I)

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
        # A capped model replay/evaluation: output to a file only, no sends,
        # no production writes, zero dollars on the subscription proxy.
        "capped-replay-spend": {
            "undo": _REPLAY_UNDO,
            "needs_commit": False,
            "replay": True,
            "attest_extra": ["production-writes"],
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
    # Dry-run classes run the whole gate and record eligibility but never approve.
    pol["dry_run_classes"] = [c for c in (ov.get("dry_run_classes") or [])
                              if c in pol["classes"] and c not in pol["enabled_classes"]]
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
                           "policy_version": pol["version"], "matched": [], "checks": []}
    why = res["reasons"]
    if not p.get("class") and not p.get("weight"):
        why.append("no auto-decision proposal")        # plain block: today's behaviour
        res["plain"] = True
        return res
    def early(name: str, ok: bool, msg: str, fix: str) -> None:
        res["checks"].append({"name": name, "ok": ok, "detail": "ok" if ok else msg, "fix": "" if ok else fix})
        if not ok:
            why.append(msg)

    early("kill switch", not kill_switch_on(), "kill switch is on", "owner must clear the kill switch")
    early("queue enabled", pol["enabled"], "auto-approval is off for this queue",
          "owner must enable auto-approval for this queue (wt policy -q Q set enabled=true)")
    early("weight", p.get("weight") in ("minor", "major"), f"worker marked it {p.get('weight') or 'unweighted'}",
          "pass --weight minor (or major with --major-because)")
    if p.get("weight") == "major" or p.get("major_because"):
        # The worker itself says this needs the owner: show only that on the card,
        # not a wall of unrelated missing-field reasons.
        res["reasons"] = [r for r in why if r.startswith(("kill switch", "auto-approval is off"))] or []
        res["reasons"].append(f"Major: {p.get('major_because') or 'unspecified'}. "
                              + str(p.get("major_detail") or "").strip())
        hits, _err = floor_hits(proposal_text(p))
        res["matched"] = hits
        res["reasons"] += [f"matched hard stop: {h}" for h in hits]
        return res
    cls = str(p.get("class") or "")
    spec = pol["classes"].get(cls)
    checks: List[Dict[str, Any]] = res["checks"]
    in_dry_class = cls in pol.get("dry_run_classes", [])
    res["dry_run"] = bool(p.get("gate_dry_run")) or in_dry_class

    def chk(name: str, ok: bool, msg: str, fix: str = "") -> None:
        checks.append({"name": name, "ok": ok, "detail": "ok" if ok else msg, "fix": "" if ok else fix})
        if not ok:
            why.append(msg)

    att = p.get("attest") or {}
    cap_usd, cap_req = None, None
    if spec and spec.get("replay"):
        try:
            cap_usd = float(p.get("replay_cap_usd"))
            if cap_usd < 0:
                cap_usd = None
        except (TypeError, ValueError):
            cap_usd = None
        try:
            cap_req = int(p.get("replay_cap_requests"))
            if cap_req <= 0:
                cap_req = None
        except (TypeError, ValueError):
            cap_req = None
    chk("class enabled", bool(cls and spec is not None
                              and (cls in pol["enabled_classes"] or in_dry_class)),
        "no enabled class" + (f" ({cls})" if cls else ""),
        "use --class code-change-revertable or capped-replay-spend (ask the owner to enable it for this queue)")
    required = list(pol["attest"]) + list((spec or {}).get("attest_extra") or [])
    for k in required:
        v = att.get(k)
        if v is None:
            chk(f"attest {k}", False, f"checklist incomplete: {k} not attested",
                f"add {k}=no to --attest (only if it is true)")
        elif v != "no":
            chk(f"attest {k}", False, f"attested {k}={v}",
                f"{k}={v} means this is major: block with --weight major --major-because ...")
        else:
            chk(f"attest {k}", True, "")
    if spec and spec.get("replay"):
        chk("replay request cap", cap_req is not None,
            "replay request cap missing or not a positive integer",
            "pass --replay-cap-requests N (hard max model calls the replay may make)")
        chk("replay dollar cap", cap_usd is not None,
            "replay dollar cap missing", "pass --replay-cap-usd 0 (or the real cap; >0 makes it major)")
        backend = str(p.get("replay_backend") or "").strip().lower()
        free = cap_usd == 0 and backend in SUBSCRIPTION_BACKENDS
        chk("zero-dollar subscription replay", free,
            "replay is not zero-dollar on the subscription proxy: it spends real money, so it is major",
            "pass --replay-cap-usd 0 --replay-backend subscription-proxy, or block as --weight major "
            "--major-because money")
        if att.get("money") == "no" and cap_usd is not None and cap_usd > 0:
            chk("money attestation consistent", False, "attested money=no but replay dollar cap is above 0",
                "a paid replay is money=yes and major")
    try:
        conf = float(p.get("confidence"))
    except (TypeError, ValueError):
        conf = -1.0
    chk("confidence", conf >= pol["threshold"],
        f"confidence {conf if conf >= 0 else 'missing'} below {pol['threshold']}",
        f"pass --confidence >= {pol['threshold']} only if you truly are that sure")
    undo = str(p.get("undo") or "").strip()
    if len(undo) < MIN_UNDO_LEN or _NO_UNDO.match(undo):
        chk("undo", False, "no concrete undo", "give an exact command or file edit that restores the "
            "previous state, e.g. 'rm /path/to/replay-output.json'")
    elif spec and not re.match(spec["undo"], undo):
        chk("undo", False, "undo does not match the class's undo form",
            "git revert <sha> for code-change-revertable; a concrete command or file edit for replays")
    else:
        chk("undo", True, "")
    rec = str(p.get("recommend") or "").strip()
    chk("recommendation", len(rec) >= 8, "no named recommendation", "state the exact choice with --recommend")
    if spec and spec.get("needs_commit"):
        commit = str(item.get("block_commit") or "")
        m = re.match(rf"^git revert ({_SHA})\b", undo)
        if not commit:
            chk("commit", False, "no verified --commit recorded", "pass --commit <SHA> to wt block")
        elif m and not (commit.startswith(m.group(1)) or m.group(1).startswith(commit)):
            chk("commit", False, "undo sha is not the recorded commit", "undo must be git revert <that commit>")
        else:
            chk("commit", True, "")
    if items is not None and pol["enabled"] and _recent_count(items, queue, now) >= pol["rate_per_hour"]:
        chk("rate limit", False, f"rate limit ({pol['rate_per_hour']}/hour) reached", "wait for the hour to pass")
    # Veto-only floor: scanned over the worker's own words, never the ticket.
    text = proposal_text(p)
    hits, err = floor_hits(text)
    if (hits and spec and spec.get("replay") and cap_usd == 0
            and str(p.get("replay_backend") or "").strip().lower() in SUBSCRIPTION_BACKENDS
            and all(att.get(k) == "no" for k in required)):
        # Zero-dollar subscription replay: benign words (subscription, $0) do not veto the money
        # rule. Anything else (stripe, refund, $5, a paid run, a send) still does.
        hits2, _e2 = floor_hits(_BENIGN_MONEY.sub("", text))
        if len(hits2) < len(hits):
            res["exempted"] = [h for h in hits if h not in hits2]
            hits = hits2
    res["matched"] = hits
    if err:
        chk("hard-stop floor", False, err, "retry; the floor must load")
    for h in hits:
        chk(f"hard stop: {h}", False, f"matched hard stop: {h}",
            "reword the question/recommend/undo/progress so it does not name this; if it really is that, it is major")
    if not hits and not err:
        chk("hard-stop floor", True, "")
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
    "way with `wt close {ref} --worker <your-id> --summary \"...\"` plus `--commit <SHA>` if you "
    "changed code or `--no-code` if you did not; state in the "
    "summary that this was an auto-decision. If anything beyond the recommendation turns out to be "
    "needed, run `wt block` again as an ordinary question. Undo, if the owner asks: `wt undo {ref}`."
)

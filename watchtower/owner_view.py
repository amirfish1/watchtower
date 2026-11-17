"""Who each blocked ticket is really waiting on, for the owner.

Read-only projection over ``queue.list_items`` -- so it covers file-backed
and GitHub-backed queues alike (``owner_blocks_export`` is file-only). It
changes nothing and is not used by recovery: ``list_blocked`` and the
liveness table keep their own meaning of "blocked".

Rules it encodes:

* The CURRENT wait record decides, never the question text. A block whose
  question still reads like an owner request but whose current wait says
  ``worker`` (the owner answered, or the question was withdrawn) is not an
  owner item.
* A block with no current wait record is ``unclassified``. It is listed in
  its own section -- never folded silently into "owner", never dropped.
* Tickets are grouped by root: an owner wait's ``root`` (default: itself),
  and for a dependency wait the end of its ``ref`` chain. One root with its
  dependents is one decision, not N.
* A passed ``decide_by`` means "re-check the facts now". It never means the
  matter was handled, expired or can be closed.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from . import queue as q
from . import config

KINDS = ("owner", "unclassified", "system", "external", "dependency", "worker")
# Blocks WatchTower raised itself (a stage could not start, a plan discussion
# stalled, a review request never arrived). They carry no wait record on
# purpose: recording one would change who gets notified.
# "system" is also q.block's default origin, so it proves nothing and stays
# unclassified (that includes the review-notify block in liveness.py).
_SYSTEM_ORIGINS = ("stage", "plan_gate")
_MAX_CHAIN = 25
_UNRESOLVED = ("not_found_here", "other_host", "no_ref")


def _foreign_root(dep: Dict[str, Any]) -> str:
    """Group name for a dependency this host cannot identify: ``REF@host``
    when the host is known, ``REF@?`` when it is not. Deliberately not a bare
    ref, so it can never collide with (or borrow facts from) a local ticket."""
    ref = dep.get("ref") or "(no ref)"
    return f"{ref}@{dep.get('lives_on') or '?'}"


def _instant(value: str) -> Optional[datetime]:
    try:
        return q.parse_decide_by(value)
    except ValueError:
        return None


def classify(item: Dict[str, Any]) -> str:
    """owner | external | dependency | worker from the current wait record;
    else ``external`` for an awaiting-client block, ``owner`` for a product-gate
    pitch (only a human can ack it), ``unclassified`` for everything else."""
    w = q.current_wait(item)
    if w:
        return str(w["on"])
    kind = str(item.get("block_kind") or "")
    if kind == "awaiting-client":
        return "external"
    if kind == "rationale":
        return "owner"
    if str(item.get("block_origin") or "") in _SYSTEM_ORIGINS:
        return "system"
    return "unclassified"


def _is_blocked(item: Dict[str, Any]) -> bool:
    return bool(item.get("needs_input")) and item.get("status") != "closed"


def _dep_lookup(item: Dict[str, Any], w: Dict[str, Any],
                by_ref: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Resolve a dependency wait's ref on THIS host, without guessing."""
    ref = str(w.get("ref") or "").strip().upper()
    if not ref:
        return {"ref": "", "state": "no_ref"}
    dep = by_ref.get(ref)
    if dep is None:
        # Not here. It may live on another host (local queues are per host).
        return {"ref": ref, "state": "not_found_here",
                "lives_on": str(w.get("ref_host") or ""), "recorded_on": str(w.get("host") or "")}
    try:
        from . import answers
        if answers._foreign_local_ref(item, dep, w):
            return {"ref": ref, "state": "other_host",
                    "lives_on": str(w.get("ref_host") or answers._wait_host(item, w))}
    except Exception:  # noqa: BLE001 - classification must not fail the view
        pass
    return {"ref": ref, "state": q._blocker_state(dep), "item": dep}


def _row(item: Dict[str, Any], kind: str, now: datetime) -> Dict[str, Any]:
    w = q.current_wait(item) or {}
    row: Dict[str, Any] = {
        "ref": str(item.get("ref") or ""), "queue": str(item.get("project") or ""),
        "title": str(item.get("title") or item.get("note") or "")[:200],
        "wait": kind, "status": str(item.get("status") or ""),
        "blocked_at": str(item.get("blocked_at") or ""),
        "question": str(item.get("block_question") or "")[:600],
        "note": str(w.get("note") or "")[:600],
    }
    if w.get("actor"):
        row["actor"] = str(w["actor"])
    if kind == "owner":
        decide_by = str(w.get("decide_by") or "")
        row.update(root=str(w.get("root") or ""), decide_by=decide_by,
                   consequence=str(w.get("consequence") or ""))
        row["missing"] = [k for k in q.WAIT_OWNER_FIELDS if k != "root" and not row[k]]
        try:
            due = q.parse_decide_by(decide_by)
            row["recheck_due"] = bool(due and due <= now)
            row["decide_by_utc"] = (due.astimezone(timezone.utc).isoformat(timespec="seconds")
                                    if due else "")
        except ValueError:
            row["recheck_due"] = False
            row["decide_by_utc"] = ""
            row["decide_by_invalid"] = True
    return row


def build(items: List[Dict[str, Any]], project: Optional[str] = None,
          now: Optional[datetime] = None) -> Dict[str, Any]:
    """The owner view over ``items`` (every queue, for dependency lookups),
    reporting only ``project`` when given."""
    now = now or datetime.now(timezone.utc)
    by_ref = {str(it.get("ref") or "").upper(): it for it in items if it.get("ref")}
    want = q._norm_project(project) if project else ""
    rows: Dict[str, Dict[str, Any]] = {}
    for it in items:
        if _is_blocked(it) and not config.ticket_is_archived(it):
            rows[str(it.get("ref") or "").upper()] = _row(it, classify(it), now)

    def root_of(ref: str) -> Dict[str, Any]:
        """End of the wait chain from ``ref``: the ticket whose decision or
        completion everything upstream hangs on."""
        seen: List[str] = []
        cur = ref
        while cur and cur not in seen and len(seen) < _MAX_CHAIN:
            seen.append(cur)
            it = by_ref.get(cur)
            row = rows.get(cur)
            if it is None or row is None:
                return {"root": cur, "via": seen[1:]}       # an open, closed or foreign ticket
            if row["wait"] == "owner" and row.get("root") and row["root"] != cur:
                cur = row["root"]
                continue
            if row["wait"] == "dependency":
                dep = _dep_lookup(it, q.current_wait(it) or {}, by_ref)
                row["dependency"] = {k: v for k, v in dep.items() if k != "item"}
                if dep["state"] in _UNRESOLVED:
                    # Fail closed: the dependency is not a ticket this host can
                    # identify. Never continue into a same-number local ticket;
                    # the root is the host-qualified name, with no local data.
                    return {"root": _foreign_root(dep), "via": seen[1:], "foreign": dep["state"]}
                cur = dep["ref"]
                continue
            return {"root": cur, "via": seen[1:]}
        return {"root": cur or ref, "via": seen[1:], "cycle": True}

    groups: Dict[str, Dict[str, Any]] = {}
    for ref, row in rows.items():
        found = root_of(ref)
        row["root_ref"] = found["root"]
        if found.get("cycle"):
            row["root_cycle"] = True
        if want and q._norm_project(row["queue"]) != want:
            continue
        g = groups.setdefault(found["root"], {"root": found["root"], "members": [],
                                              "foreign": found.get("foreign", "")})
        g["members"].append(row)

    out_groups: List[Dict[str, Any]] = []
    for root, g in groups.items():
        members = sorted(g["members"], key=lambda r: (r["ref"] != root, r["ref"]))
        foreign = g["foreign"]
        root_item = None if foreign else by_ref.get(root)
        root_row = None if foreign else rows.get(root)
        if foreign:
            root_state = foreign
        elif root_row is not None:
            root_state = f"blocked:{root_row['wait']}"
        elif root_item is not None:
            root_state = str(root_item.get("status") or "open")
        else:
            root_state = "not_found_here"
        counts = {k: sum(1 for r in members if r["wait"] == k) for k in KINDS}
        out_groups.append({
            "root": root, "root_state": root_state,
            "root_title": str((root_item or {}).get("title") or "")[:200],
            "counts": counts,
            "dependents": [r["ref"] for r in members if r["ref"].upper() != root],
            "needs_owner": counts["owner"] > 0 or root_state == "blocked:owner",
            "recheck_due": any(r.get("recheck_due") for r in members),
            # Earliest decide-by among this root's owner waits, as a UTC
            # instant (offsets differ, so strings must never be compared).
            "decide_by_utc": min((r["decide_by_utc"] for r in members if r.get("decide_by_utc")),
                                 default=""),
            "members": members,
        })
    # Passed dates first, then owner roots by the earliest real instant
    # (undated after dated), then everything else.
    out_groups.sort(key=lambda g: (not g["recheck_due"], not g["needs_owner"],
                                   g["decide_by_utc"] == "", g["decide_by_utc"],
                                   -len(g["members"]), g["root"]))

    shown = [r for g in out_groups for r in g["members"]]
    def dep_line(r: Dict[str, Any]) -> Dict[str, Any]:
        d = dict(r["dependency"])
        return {"ref": r["ref"], "waits_on": d.pop("ref", ""), **d}

    stuck = [dep_line(r) for r in shown if r.get("dependency", {}).get("state") == "stuck"]
    unresolved = [dep_line(r) for r in shown
                  if r.get("dependency", {}).get("state") in ("not_found_here", "other_host", "no_ref")]
    from . import releases, watches
    watched = watches.owner_rows([i for i in items if not want or q._norm_project(i.get("project")) == want])
    release_rows = releases.owner_rows([i for i in items if not want or q._norm_project(i.get("project")) == want], now=now)
    return {
        "watched": watched,
        "awaiting_release": release_rows,
        "release_attention": [r for r in release_rows if r["attention"]],
        "generated_at": now.isoformat(timespec="seconds"),
        "queue": want or "",
        "counts": {k: sum(1 for r in shown if r["wait"] == k) for k in KINDS},
        "blocked": len(shown),
        "groups": out_groups,
        # Listed apart so nothing hides inside a group:
        "unclassified": [r["ref"] for r in shown if r["wait"] == "unclassified"],
        # Raised by WatchTower itself; whoever operates the queue can clear
        # them with the command in the question. Listed, never hidden.
        "system": [{"ref": r["ref"], "question": r["question"][:200]}
                   for r in shown if r["wait"] == "system"],
        "recheck_due": [{"ref": r["ref"], "decide_by": r["decide_by"],
                         "decide_by_utc": r["decide_by_utc"],
                         "consequence": r["consequence"]}
                        for r in sorted(shown, key=lambda r: r.get("decide_by_utc") or "")
                        if r.get("recheck_due")],
        # Owner waits with a future decide-by, soonest first. The reader picks
        # its own horizon (the desk: before the digest after next).
        "upcoming": [{"ref": r["ref"], "root": r["root_ref"], "decide_by": r["decide_by"],
                      "decide_by_utc": r["decide_by_utc"], "consequence": r["consequence"]}
                     for r in sorted(shown, key=lambda r: r.get("decide_by_utc") or "")
                     if r["wait"] == "owner" and r.get("decide_by_utc") and not r["recheck_due"]],
        "incomplete": False, "warnings": [],
        "fragile_jobs": _fragile_jobs(items, want, now),
        "owner_missing_context": [{"ref": r["ref"], "missing": r["missing"]} for r in shown
                                  if r["wait"] == "owner" and r.get("missing")],
        "stuck_dependencies": stuck,
        "unresolved_dependencies": unresolved,
    }


def snapshot(project: Optional[str] = None) -> Dict[str, Any]:
    """The view over the live store. A queue that could not be read is never
    shown as "nothing blocked": if the requested queue itself fails this
    raises; if another queue fails the view is marked ``incomplete`` (its
    tickets, and dependency lookups into it, are missing)."""
    warnings: List[str] = []
    if project:
        q.list_items(project=project, strict=True)        # the asked-for queue must be readable
    try:
        items = q.list_items(strict=True)
    except Exception as e:  # noqa: BLE001 - degrade to what can be read, loudly
        warnings.append(f"some queues could not be read, so this view may miss tickets: "
                        f"{' '.join(str(e).split())[:200]}")
        items = q.list_items()
    view = build(items, project=project)
    # GitHub's shallow list intentionally omits comments. Read retained local
    # prose for confirmed links, without hydrating or adding a GitHub call.
    try:
        from . import related
        cached, _ = related.cached_tickets()
        combined = {it["ref"]: dict(it) for it in items}
        for it in cached:
            if it["ref"] in combined and it.get("comments"):
                combined[it["ref"]]["comments"] = it["comments"]
        view["fragile_jobs"] = _fragile_jobs(list(combined.values()),
                                            q._norm_project(project) if project else "",
                                            datetime.now(timezone.utc))
    except (OSError, ValueError) as exc:
        warnings.append(f"Confirmed repeat-job history could not be read: {exc}")
    # The decision desk's existing morning digest renders owner-view warnings.
    # One connected confirmed job produces one line, never one line per filing.
    view["incomplete"] = bool(warnings)
    warnings.extend("Fragile job: " + job["title"][:50] + "; " +
                    ", ".join(job["refs"]) + ". Fix the design, not the latest error."
                    for job in view.get("fragile_jobs") or [])
    warnings.extend(f"Watching closed {r['ref']}: {r['check']} at {r['where']} "
                    f"({r['schedule']}); last {r['last'].get('status', 'not-run')}"
                    for r in view.get("watched") or [])
    view["warnings"] = warnings
    return view


def _fragile_jobs(items, project, now):
    from . import related
    jobs = related.fragile_jobs([it for it in items if not config.ticket_is_archived(it)],
                               now=now.timestamp())
    if project:
        jobs = [j for j in jobs if any(r.startswith(project + "-") for r in j["refs"])]
    return jobs


def render(view: Dict[str, Any]) -> str:
    c = view["counts"]
    lines = [f"Blocked: {view['blocked']}"
             + (f" in {view['queue']}" if view["queue"] else "")
             + f"  (owner {c['owner']}, unclassified {c['unclassified']}, dependency "
               f"{c['dependency']}, external {c['external']}, worker {c['worker']})"]
    lines.append(f"Closed tickets being watched: {len(view.get('watched') or [])}")
    for row in view.get("watched") or []:
        lines.append(f"  {row['ref']} {row['check']} at {row['where']} ({row['schedule']}); "
                     f"last {row['last'].get('status', 'not-run')}")
    release_rows = view.get("awaiting_release") or []
    lines.append(f"Awaiting release: {len(release_rows)} (separate from deferred; no timer wake)")
    for row in view.get("release_attention") or []:
        lines.append(f"  CHECK RELEASE: {row['ref']} {row['age_days']} days; {row['target']}; branch {row['branch_state']}")
    for w in view.get("warnings") or []:
        lines.append(f"WARNING: {w}")
    if view["recheck_due"]:
        lines.append("")
        lines.append("Re-check now (the decide-by time has passed; this does NOT mean handled):")
        for r in view["recheck_due"]:
            lines.append(f"  {r['ref']}  decide by {r['decide_by']}  if unanswered: {r['consequence'] or '-'}")
    lines.append("")
    lines.append("By root:")
    for g in view["groups"]:
        n = len(g["dependents"])
        lines.append(f"  {g['root']} [{g['root_state']}]"
                     + (f" + {n} waiting on it" if n else "")
                     + ("  NEEDS OWNER" if g["needs_owner"] else ""))
        for r in g["members"]:
            extra = ""
            if r["wait"] == "owner":
                extra = f"  decide by {r.get('decide_by') or '(not set)'}"
                if r.get("note"):
                    # The current wait note can narrow or correct an older question.
                    extra += "  now: " + " ".join(r["note"].split())[:90]
            elif r["wait"] == "dependency":
                d = r.get("dependency") or {}
                extra = f"  on {d.get('ref') or '(no ref)'} [{d.get('state', '?')}]"
            elif r.get("note"):
                # Why a worker/external wait is not the owner's (e.g. a question
                # that was withdrawn, and what work is still owed).
                extra = "  " + " ".join(r["note"].split())[:110]
            lines.append(f"    {r['ref']:<18} {r['wait']:<12}{extra}")
    if view.get("system"):
        lines.append("")
        lines.append(f"Raised by WatchTower itself ({len(view['system'])}): a stage, plan or review step "
                     f"stopped. Whoever runs the queue can clear these:")
        for r in view["system"]:
            lines.append(f"  {r['ref']:<18} " + " ".join(r["question"].split())[:110])
    if view["unclassified"]:
        lines.append("")
        lines.append(f"Unclassified ({len(view['unclassified'])}): no wait target recorded. They may "
                     f"need the owner. Fix with `wt wait-on REF --on ...`:")
        lines.append("  " + ", ".join(view["unclassified"]))
    if view["stuck_dependencies"]:
        lines.append("")
        lines.append("Dependency closed with open items (will not wake by itself):")
        for d in view["stuck_dependencies"]:
            lines.append(f"  {d['ref']} waits on {d['waits_on']}")
    if view["unresolved_dependencies"]:
        lines.append("")
        lines.append("Dependency not resolvable on this host (will not wake here):")
        for d in view["unresolved_dependencies"]:
            where = f" (lives on {d['lives_on']})" if d.get("lives_on") else ""
            lines.append(f"  {d['ref']} waits on {d['waits_on'] or '(no ref)'}: {d.get('state')}{where}")
    return "\n".join(lines)

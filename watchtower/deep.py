"""Deep-work brief: the claim-time guidance for ``depth: deep`` tickets.

A quick ticket (the default) is fix-and-look work: the filer uses the product
and sees within minutes whether the fix is right. A deep ticket is work whose
outcome you cannot judge by using the product -- performance, chronic
regressions, multi-step optimisation. The generic drain contract rewards
"fix one plausible cause, close, file the rest", which is exactly how a
recurring perf alert gets five honest ~10% fixes and never a solution. So a
deep claim carries this brief, and it overrides the quick defaults.

Kept free of braces so callers can embed it in .format() templates.
"""

from __future__ import annotations

from typing import Any, Dict, List

from . import queue as q


def _history(item: Dict[str, Any], event: str) -> List[Dict[str, Any]]:
    return [h for h in (item.get("history") or []) if h.get("event") == event]


def deep_brief(item: Dict[str, Any]) -> str:
    """The brief for a deep ticket, or "" for a quick one."""
    if not q.is_deep(item):
        return ""
    ref = str(item.get("ref") or "this ticket")
    budget = int(item.get("budget_min") or q.DEEP_DEFAULT_BUDGET_MIN)
    accept = str(item.get("accept") or "").strip()
    gates = [str(g) for g in (item.get("gates") or [])]
    cmd_gates = [g[4:] for g in gates if g.startswith("cmd:")]
    lines = [
        f"DEEP TICKET ({ref}) -- these rules override the quick-fix defaults.",
        "",
        "GOAL: done means the reason this ticket was filed is gone, measured the "
        "same way it was detected and under the conditions that caused it (cold "
        "paths, cache misses, real load -- not only a warm happy path). Fixing one "
        "plausible cause is progress, not done.",
    ]
    if accept:
        lines.append(f"ACCEPT LINE: {accept}")
    if cmd_gates:
        lines.append("MEASURE GATE (runs at close; run it yourself first): "
                     + " ; ".join(cmd_gates))
    lines += [
        "",
        "1. EVIDENCE FIRST. Before changing behaviour, check you have the data to "
        "know the cause: per-phase timings, enough samples, the failing case "
        "captured. If you don't: build the logging/instrumentation, commit and "
        "push it, decide how much data you need and how long it takes to collect "
        "(e.g. 24h of normal use incl. 3 restarts), then run "
        f"`wt defer {ref} --until +24h --reason \"what data, why\" --progress "
        "\"what you built, what to analyse next\"`. The ticket returns to the pool "
        "when the window ends; whoever resumes reads your progress note.",
        "",
        "2. OPTIONS MEMO before building. List 3-5 candidate solutions with "
        "expected gain and risk. At least one MUST be a product option, not a "
        "code change: cut the scope (what does the user actually look at?), tier "
        "the data (hot vs archive), change the UX so the slow path is not on the "
        "critical path. Read the Product constraints section of the queue "
        "learnings file first -- the owner's past decisions live there. If the "
        "best option needs a product decision, `wt block` with the memo as the "
        "question (recommended option first) instead of guessing; record the "
        "owner's answer as a Product constraints line in the learnings file.",
        "",
        f"3. BUDGET: about {budget} minutes of active work. Record every attempt "
        f"as `wt comment {ref} \"attempt N: <change> -- metric before -> after\"`.",
        "",
        "4. STOP RULE (whichever comes first): the budget is spent; two attempts "
        "in a row with no measured gain; four attempts without converging on the "
        "goal. On stop do NOT close: `wt block` with what you tried, the measured "
        "trajectory and your best remaining options -- usually a product "
        "question.",
        "",
        "5. CLOSING: only when the goal is met, with the measurement in the "
        "summary (before -> after, which conditions). A follow-up ticket may "
        "carry genuinely separate work, never this ticket's own goal.",
    ]
    defers = _history(item, "defer")
    if defers:
        last = defers[-1]
        lines += [
            "",
            f"RESUMING: deferred {len(defers)}x; last until {last.get('until') or '?'} "
            f"-- {last.get('reason') or ''}. Read the latest progress note in the "
            "ticket history and analyse the collected data before changing anything.",
        ]
    attempts = [c for c in _history(item, "comment")
                if str(c.get("text") or "").lower().startswith("attempt")]
    if attempts:
        lines.append(f"PRIOR ATTEMPTS on record: {len(attempts)} (see the ticket comments).")
    return "\n".join(lines)

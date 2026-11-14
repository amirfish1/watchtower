"""Deep-work brief: the claim-time guidance for ``depth: deep`` tickets.

Every ticket requires thorough investigation, a complete solution and verified
outcomes. ``quick`` is the legacy name for the default depth, not a speed or
quality target. Deep tickets add structure for sustained work across hours or
days, including performance, chronic regressions and multi-step optimisation.
Budgets are active-work checkpoints, not a ceiling on the ticket's lifetime.

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
        f"DEEP TICKET ({ref}) -- sustained work across hours or days; the same "
        "thoroughness and completion standard applies to every ticket.",
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
        f"3. CHECKPOINT BUDGET: about {budget} minutes of active work per "
        "checkpoint, not a total time limit for the ticket. Honour any explicit "
        "ticket or owner spending/time cap. Record every attempt "
        f"as `wt comment {ref} \"attempt N: <change> -- metric before -> after\"`.",
        "",
        "4. STOP RULE: at each checkpoint, record the measured trajectory, "
        "remaining work and next checkpoint plan. Continue authorized work "
        "while evidence supports progress, including across sessions or days. "
        "After two attempts in a row with no measured gain, stop repeating "
        "that approach: reassess the evidence and options before trying a "
        "different approach. Defer when waiting for data; block only for a "
        "genuine decision, access or authorization gap. Preserve a resumable "
        "progress note before a session ends. Never close because a checkpoint "
        "or session ended, and never exceed an explicit cap without approval.",
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

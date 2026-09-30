"""Per-stage role models (WT-14): planner, plan reviewer, builder, verifier.

:func:`effective_role_model` is the ONE place that decides which engine/model
runs a role for a queue (and optionally a ticket); spawn paths and every
display call it instead of reading raw config. Sources, in precedence order:
``ticket`` (per-ticket override), ``queue`` (explicit queue setting),
``default`` (derived rule below), ``policy-fallback`` (the queue's pin was
policy-blocked and substituted).

Defaults when unset:
  planner       builder's engine at its strongest ranked model
  plan_reviewer a different engine family than the builder
  verifier      a different engine family than the builder
  assessor      a different engine family than the builder (WT-21)
Nothing here names a model; ranking and availability come from the catalogs
(``models``) and the installed engine CLIs.
"""
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

from . import config, models

ROLES = ("planner", "plan_reviewer", "builder", "verifier", "assessor")
# Order in which a *different* family is tried for reviewer/verifier defaults.
_CROSS_FAMILY_ORDER = ("codex", "claude", "kimi", "devin")


def _strongest(eng: str) -> str:
    """Highest-ranked non-blocked catalog model of ``eng``; if none is ranked,
    the catalog's first (best/newest) entry; "" when there is no catalog."""
    cands = [m for m in (models.catalog(eng) or ()) if not config.is_blocked_model(m)]
    ranked = [(models.rank(m), i, m) for i, m in enumerate(cands)
              if models.rank(m) is not None]
    if ranked:
        return max(ranked, key=lambda t: (t[0], -t[1]))[2]
    return cands[0] if cands else ""


def _other_family(builder_engine: str) -> str:
    from . import workers
    for eng in _CROSS_FAMILY_ORDER:
        if eng != builder_engine and workers.engine_available(eng):
            return eng
    return builder_engine


def _builder(queue: str, ticket: Optional[dict]) -> Tuple[str, str, str]:
    floor = str((ticket or {}).get("model_floor") or "").strip()
    if floor:
        return (models.engine_of(floor, prefer=config.engine(queue))
                or config.engine(queue), floor, "ticket")
    eng = config.engine(queue)
    mdl = config.model(queue)
    pinned = str(config.get_queue_config(queue).get("model") or "").strip()
    if pinned and mdl != config.raw_model(queue):
        return eng, mdl, "policy-fallback"
    return eng, mdl, "queue" if pinned else "default"


def effective_role_model(queue: str, ticket: Optional[dict], role: str) -> Tuple[str, str, str]:
    """``(engine, model, source)`` for ``role`` on ``queue`` (and ``ticket``)."""
    if role not in ROLES:
        raise ValueError(f"unknown role {role!r}; expected one of {ROLES}")
    b_eng, b_model, b_src = _builder(queue, ticket)
    if role == "builder":
        return b_eng, b_model, b_src
    # Per-ticket override: a model id; its engine is whichever catalog lists it.
    t_model = str((ticket or {}).get(f"{role}_model") or "").strip()
    if t_model:
        eng = models.engine_of(t_model, prefer=b_eng) or b_eng
        return eng, config.canonical_model(eng, t_model), "ticket"
    o_eng, o_model = config.role_override(queue, role)
    if o_eng or o_model:
        eng = o_eng or b_eng
        mdl = o_model or (_strongest(eng) if eng != b_eng else "")
        return eng, config.canonical_model(eng, mdl), "queue"
    if role == "planner":
        return b_eng, _strongest(b_eng), "default"
    eng = _other_family(b_eng)
    return eng, _strongest(eng), "default"


def role_table(queue: str, ticket: Optional[dict] = None) -> Dict[str, Dict[str, Any]]:
    """All four roles as ``{role: {engine, model, source}}`` for display."""
    out = {}
    for role in ROLES:
        eng, mdl, src = effective_role_model(queue, ticket, role)
        out[role] = {"engine": eng, "model": mdl, "source": src}
    return out

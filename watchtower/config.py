#!/usr/bin/env python3
# Copyright (c) 2026 Amir Fish. All rights reserved.
# SPDX-License-Identifier: LicenseRef-WatchTower-Software-License

"""Per-queue configuration for WatchTower.

Currently holds the ``auto_drain`` policy (WT-FEATURES #16): the watcher's
``--auto-spawn`` only starts a worker for a stuck queue when that queue is
auto-drained. Auto-drain is **off by default** — a new queue is a backlog
until you explicitly opt in with ``wt drain on <queue>``. This prevents
surprise worker spawns on queues that are just parking lots.

It also holds ``grace_s`` (see :data:`DEFAULT_GRACE_S`), the other queue-level
input to a GitHub-backed ticket's eligibility.

It also holds ``subscribers``: a list of addressable targets (worker id /
``@agent`` name / session UUID -- the same shape ``messages.resolve_target``
already resolves for a ticket's ``submitter`` and for ``--report-to``) that
hear about every enqueue/claim/close/needs-input event on a queue, not just
their own tickets. Managed via ``wt subscribe``/``wt unsubscribe``; delivered
by ``queue._notify_ticket_event``, the same helper that pushes a ticket's own
``submitter`` its status changes.

``notify_events`` is the filter on that second (submitter) half: which events
a filer hears about by default. See :data:`DEFAULT_NOTIFY_EVENTS`.

Stored as ``~/.watchtower/queue-config.json`` = ``{queue: {auto_drain: bool}}``.
"""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

VALID_BACKENDS = ("file", "github")
VALID_EFFORTS = ("low", "medium", "high", "xhigh", "max")
STANDARD_EFFORTS = VALID_EFFORTS[:-1]

# How long a ticket is left alone before auto-drain may claim it. With
# auto-drain on, the reconciler claims a brand-new issue within ~30s, so a
# human never gets a chance to label it `watchtower:no-auto-drain` -- the
# grace period is what makes that opt-out usable for *inbound* issues rather
# than only pre-existing ones. 0 disables it (fast queues that should drain
# immediately). It gates auto-eligibility only; a human pressing play ignores
# it.
DEFAULT_GRACE_S = 180

# Which ticket events are worth pushing to the session that FILED the ticket
# (see ``notify_events``). Every event a worker can raise:
VALID_NOTIFY_EVENTS = ("claimed", "closed", "needs_input", "awaits_decision")
# ...but "claimed" is off by default (WATCHTOWER-23). A claim carries nothing
# the filer can act on -- nothing to read, nothing to answer -- while landing
# it costs the receiving session a turn it did not ask for. The other three
# all hand the filer something: a summary, a question, a decision to make.
# ``awaits_decision`` is in the default because it is ``needs_input``'s
# product-gate sibling (same "a worker is stuck on you" class, different
# wording), and dropping it would silently stall the gate.
DEFAULT_NOTIFY_EVENTS = ("closed", "needs_input", "awaits_decision")

# WatchTower's explicitly supported worker model identifiers. This is a
# deployment policy rather than a claim about every model an account may be
# entitled to: the engine CLIs do not offer a portable, machine-readable model
# discovery command. Keep this conservative and update it intentionally when a
# fleet adopts a new model.
MODEL_EFFORTS = {
    "codex": (
        ("gpt-5.6", VALID_EFFORTS),
        ("gpt-5.6-sol", VALID_EFFORTS),
        ("gpt-5.6-terra", VALID_EFFORTS),
        ("gpt-5.6-luna", VALID_EFFORTS),
        ("gpt-5.5", STANDARD_EFFORTS),
        ("gpt-5.4", STANDARD_EFFORTS),
    ),
    "claude": (
        ("claude-opus-5", VALID_EFFORTS),
        ("claude-opus-4-8", VALID_EFFORTS),
        ("claude-sonnet-5", VALID_EFFORTS),
    ),
    # kimi has no effort flag on its CLI; pinned models accept no explicit
    # effort. Plain kimi-for-coding is the low-cost tier ($0.95/$4 per 1M
    # tok); highspeed is the fast tier at 2x ($1.90/$8) — pick per queue.
    "kimi": (
        ("kimi-code/k3", ()),
        ("kimi-code/kimi-for-coding", ()),
        ("kimi-code/kimi-for-coding-highspeed", ()),
    ),
    # antigravity (AGY, spawn-only via `wt spawn --engine antigravity`) has no
    # effort flag; effort is baked into the model id suffix (-high/-medium/
    # -low), so pinned models accept no explicit effort — same shape as kimi.
    # Curated to the Gemini ids AGY serves (`agy models` lists the full
    # vocabulary, which also carries other vendors' models; those are reached
    # through their own engines, not through AGY).
    "antigravity": (
        ("gemini-3.1-pro-high", ()),
        ("gemini-3.1-pro-low", ()),
        ("gemini-3.7-flash-high", ()),
        ("gemini-3.7-flash-medium", ()),
        ("gemini-3.7-flash-low", ()),
    ),
}

# Short aliases that callers may type (e.g. ``opus-5``) but that the engine CLI
# does not accept verbatim as a ``--model`` value. Each resolves to the canonical
# WatchTower identifier before being stored or passed to a worker.
MODEL_ALIASES: Dict[str, Dict[str, str]] = {
    "claude": {
        "opus-5": "claude-opus-5",
    },
}

# Claude short forms that carry a version (``sonnet-5``, ``opus-4-8``) and so
# need the ``claude-`` prefix to be a valid ``--model`` flag value. Bare family
# names (``sonnet``) are already accepted by the CLI and must NOT be rewritten.
_CLAUDE_VERSIONED_ALIAS = re.compile(r"^(sonnet|opus|haiku|fable)-\d", re.IGNORECASE)

CONFIG_FILE = Path(
    os.environ.get("WATCHTOWER_CONFIG_FILE")
    or (Path.home() / ".watchtower" / "queue-config.json")
)

# CCC (Claude Command Center) keeps its own per-engine default model at this
# path. WT and CCC are separate systems, but sharing this one file means a
# queue with no explicit `wt set --model` falls back to whatever CCC's own
# workers default to, instead of silently inheriting the bare CLI's ambient
# default (which drifts independently of either system's intent -- e.g. a
# machine-wide `/model` change unexpectedly re-flavoring every WT worker).
CCC_SPAWN_DEFAULTS_FILE = Path(
    os.environ.get("WATCHTOWER_CCC_SPAWN_DEFAULTS_FILE")
    or (Path.home() / ".claude" / "command-center" / "spawn-defaults.json")
)


def _load() -> Dict[str, Any]:
    try:
        with open(CONFIG_FILE, "r") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _save(data: Dict[str, Any]) -> None:
    CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = str(CONFIG_FILE) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, CONFIG_FILE)


def _queue_entry(queue: str) -> Dict[str, Any]:
    """The raw config dict for ``queue``, or ``{}`` for a missing/non-dict entry.

    A hand-edited or merge-mangled config file can leave a queue's value as a
    scalar (e.g. ``{"WT": "oops"}``) instead of a dict. Every getter reads
    through this instead of ``_load().get(queue, {})`` directly, so a mangled
    entry degrades to defaults everywhere instead of raising ``AttributeError``
    in whichever getter happens to be called first.
    """
    entry = _load().get(queue, {})
    return entry if isinstance(entry, dict) else {}


def get_queue_config(queue: str) -> Dict[str, Any]:
    return dict(_queue_entry(queue))


def set_backend(queue: str, backend: str) -> Dict[str, Any]:
    backend = str(backend or "file").strip().lower()
    if backend not in VALID_BACKENDS:
        raise ValueError(f"backend must be one of {VALID_BACKENDS}")
    data = _load()
    q = data.setdefault(queue, {})
    if backend == "file":
        q.pop("backend", None)
    else:
        q["backend"] = backend
    _save(data)
    return q


def backend(queue: str) -> str:
    value = str(_queue_entry(queue).get("backend") or "file").strip().lower()
    return value if value in VALID_BACKENDS else "file"


_GITHUB_REPO_SHAPE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")


def _validate_github_repo(repo: str) -> None:
    """Reject common placeholder values and malformed shapes.

    Catches a bad value at config time instead of leaving it to fail much
    later as an opaque ``gh`` error on every poll.
    """
    placeholder = str(repo or "").strip().lower()
    if placeholder in {"owner/repo", "acme/repo"}:
        raise ValueError(
            f"github_repo cannot be the literal placeholder '{repo}'; "
            "use a real OWNER/REPO value"
        )
    if not _GITHUB_REPO_SHAPE.match(str(repo or "").strip()):
        raise ValueError(
            f"github_repo {repo!r} must look like OWNER/REPO (no scheme, "
            "spaces, or extra path segments)"
        )


def set_github_repo(queue: str, repo: str) -> Dict[str, Any]:
    data = _load()
    q = data.setdefault(queue, {})
    repo = str(repo or "").strip()
    if repo:
        _validate_github_repo(repo)
        q["github_repo"] = repo
    else:
        q.pop("github_repo", None)
    _save(data)
    return q


def github_repo(queue: str) -> str:
    return str(_queue_entry(queue).get("github_repo") or "")


def set_github_assignee(queue: str, assignee: str) -> Dict[str, Any]:
    data = _load()
    q = data.setdefault(queue, {})
    assignee = str(assignee or "").strip()
    if assignee:
        q["github_assignee"] = assignee
    else:
        q.pop("github_assignee", None)
    _save(data)
    return q


def github_assignee(queue: str) -> str:
    return str(_queue_entry(queue).get("github_assignee") or "@me")


def set_auto_drain(queue: str, enabled: bool) -> Dict[str, Any]:
    data = _load()
    q = data.setdefault(queue, {})
    q["auto_drain"] = bool(enabled)
    # ``drain on`` promises that the reconciler will staff the queue.  A queue
    # may still carry ``desired_workers: 0`` from when it was deliberately
    # parked; leaving that value in place makes auto-drain visibly on but
    # operationally inert.  Restore the normal minimum when opting back in,
    # while preserving explicit parallel-worker settings above zero.
    if enabled:
        try:
            desired = int(q.get("desired_workers", 1))
        except (TypeError, ValueError):
            desired = 0
        if desired < 1:
            q["desired_workers"] = 1
    _save(data)
    return q


def auto_drain(queue: str) -> bool:
    """False unless explicitly opted in. Default-off so a fresh queue is a
    backlog until you run ``wt drain on <queue>``."""
    return bool(_queue_entry(queue).get("auto_drain", False))


def set_product_gate(queue: str, enabled: bool) -> Dict[str, Any]:
    data = _load()
    q = data.setdefault(queue, {})
    q["product_gate"] = bool(enabled)
    _save(data)
    return q


def product_gate(queue: str) -> bool:
    """False unless explicitly opted in. When on, workers must post a
    decision-grade pitch (wt block --kind rationale) and wait for a human
    Ack before implementing — see the 2026-09-01 product-gate design."""
    return bool(_queue_entry(queue).get("product_gate", False))


def set_grace_s(queue: str, seconds: Any) -> Dict[str, Any]:
    """Set this queue's auto-drain grace period in seconds (see DEFAULT_GRACE_S).

    ``None`` clears the override so the queue falls back to the default; 0 is a
    meaningful value (drain immediately) and is stored as such."""
    data = _load()
    q = data.setdefault(queue, {})
    if seconds is None:
        q.pop("grace_s", None)
    else:
        value = int(seconds)
        if value < 0:
            raise ValueError("grace_s must be >= 0")
        q["grace_s"] = value
    _save(data)
    return q


def grace_s(queue: str) -> int:
    """Seconds a ticket must age before auto-drain may claim it."""
    raw = _queue_entry(queue).get("grace_s", DEFAULT_GRACE_S)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_GRACE_S
    return value if value >= 0 else DEFAULT_GRACE_S


def github_queues_for_repo(repo: str) -> list:
    """Every github-backed queue configured against ``repo`` (OWNER/REPO).

    Used to decide whether the legacy ``watchtower:<QUEUE>`` label still has a
    job: with one queue per repo it is inert, with two or more it is the only
    thing that can partition the repo's issues between them.
    """
    target = str(repo or "").strip().lower()
    if not target:
        return []
    out = []
    for name, entry in _load().items():
        if not isinstance(entry, dict):
            continue
        if str(entry.get("backend") or "").strip().lower() != "github":
            continue
        if str(entry.get("github_repo") or "").strip().lower() == target:
            out.append(name)
    return sorted(out)


def set_claim_types(queue: str, types: Any) -> Dict[str, Any]:
    """Restrict which ticket types an auto-drain worker claims (e.g. ['bug']).

    Empty/None means no restriction — the worker drains all types. Stored as a
    list under ``claim_types`` so ``wt drain on Q --type bug`` makes the queue's
    workers claim only bugs and leave features for a human."""
    valid = {"bug", "feature"}
    norm = [t for t in (types or []) if t in valid]
    data = _load()
    q = data.setdefault(queue, {})
    if norm:
        q["claim_types"] = norm
    else:
        q.pop("claim_types", None)
    _save(data)
    return q


def claim_types(queue: str) -> list:
    """Return the configured claim-type restriction for a queue, or [] (all)."""
    v = _queue_entry(queue).get("claim_types", [])
    return list(v) if isinstance(v, list) else []


def set_notify_events(queue: str, events: Any) -> Dict[str, Any]:
    """Choose which ticket events reach a ticket's ``submitter`` on this queue.

    ``None`` restores the default (``DEFAULT_NOTIFY_EVENTS``); an explicit
    empty list means "notify the submitter about nothing". Anything not in
    ``VALID_NOTIFY_EVENTS`` is dropped rather than raising -- the setting is a
    preference, and a typo must not wedge a queue's config.

    Applies to the ticket's own submitter only. A target that ran
    ``wt subscribe`` asked for the queue's whole event stream and keeps it;
    ``wt unsubscribe`` is how that one is turned down.
    """
    if events is None:
        norm = None
    else:
        norm = [e for e in events if e in VALID_NOTIFY_EVENTS]
    data = _load()
    q = data.setdefault(queue, {})
    if norm is None:
        q.pop("notify_events", None)
    else:
        q["notify_events"] = norm
    _save(data)
    return q


def notify_events(queue: str) -> list:
    """Events whose notices reach a ticket's submitter (see ``set_notify_events``)."""
    entry = _queue_entry(queue)
    if "notify_events" not in entry:
        return list(DEFAULT_NOTIFY_EVENTS)
    v = entry.get("notify_events")
    return [e for e in v if e in VALID_NOTIFY_EVENTS] if isinstance(v, list) else []


def _norm_subscriber_targets(values: Any) -> list:
    """Trimmed, order-preserving, de-duplicated list of subscriber targets.

    A target is opaque here (worker id / ``@agent`` name / session UUID) --
    the same shape ``messages.resolve_target`` resolves for a ticket's
    ``submitter`` and for ``--report-to``. This module never imports
    ``messages`` (it would be circular: ``messages`` imports ``queue``, which
    can import ``config``), so a target is stored as typed and only resolved
    at send time by ``queue._notify_ticket_event``."""
    out: list = []
    seen: set = set()
    for raw in values or []:
        t = str(raw or "").strip()
        if t and t not in seen:
            seen.add(t)
            out.append(t)
    return out


def set_subscribers(queue: str, targets: Any) -> Dict[str, Any]:
    """Replace this queue's subscriber list wholesale (see ``subscribers``).

    Empty/None clears the list. Subscribers hear about every enqueue/claim/
    close/needs-input event on the queue, not just tickets they filed
    themselves (see ``add_subscriber``/``remove_subscriber`` for the
    subscribe/unsubscribe CLI's incremental counterpart)."""
    data = _load()
    q = data.setdefault(queue, {})
    norm = _norm_subscriber_targets(targets)
    if norm:
        q["subscribers"] = norm
    else:
        q.pop("subscribers", None)
    _save(data)
    return q


def subscribers(queue: str) -> list:
    """Return the configured subscriber targets for a queue, or [] (none)."""
    v = _queue_entry(queue).get("subscribers", [])
    return list(v) if isinstance(v, list) else []


def add_subscriber(queue: str, target: str) -> Dict[str, Any]:
    """Add one target to a queue's subscriber list (idempotent)."""
    target = str(target or "").strip()
    if not target:
        raise ValueError("target is required")
    data = _load()
    q = data.setdefault(queue, {})
    subs = _norm_subscriber_targets(q.get("subscribers"))
    if target not in subs:
        subs.append(target)
    q["subscribers"] = subs
    _save(data)
    return q


def remove_subscriber(queue: str, target: str) -> Dict[str, Any]:
    """Remove one target from a queue's subscriber list, if present."""
    target = str(target or "").strip()
    data = _load()
    q = data.setdefault(queue, {})
    subs = [t for t in _norm_subscriber_targets(q.get("subscribers")) if t != target]
    if subs:
        q["subscribers"] = subs
    else:
        q.pop("subscribers", None)
    _save(data)
    return q


def set_repo_path(queue: str, path: str) -> Dict[str, Any]:
    data = _load()
    q = data.setdefault(queue, {})
    q["repo_path"] = str(path)
    _save(data)
    return q


def repo_path(queue: str) -> str:
    """Return the configured repo_path for a queue, or empty string."""
    return _queue_entry(queue).get("repo_path", "")


def set_engine(queue: str, eng: str) -> Dict[str, Any]:
    data = _load()
    q = data.setdefault(queue, {})
    q["engine"] = eng
    _save(data)
    return q


def _ccc_worker_engine_default() -> str:
    """CCC's shared *worker*-spawn engine default, read from spawn-defaults.json's
    ``worker_engine`` field -- a separate key from that file's own top-level
    ``engine``, which is CCC's "new session" spawn-button default and must
    stay untouched by WT (WT-105). Returns "" if the file is missing,
    unreadable, or has no such key."""
    try:
        with open(CCC_SPAWN_DEFAULTS_FILE) as f:
            data = json.load(f)
        return str(data.get("worker_engine") or "")
    except (OSError, ValueError, AttributeError):
        return ""


def _ccc_worker_effort_default() -> str:
    """CCC's shared worker-only reasoning effort, if one is configured."""
    try:
        with open(CCC_SPAWN_DEFAULTS_FILE) as f:
            data = json.load(f)
        value = str(data.get("worker_reasoning_effort") or "").strip().lower()
        return value if value in VALID_EFFORTS else ""
    except (OSError, ValueError, AttributeError):
        return ""


def engine(queue: str) -> str:
    """Return the worker engine for a queue (used by both DRAIN and
    RUN_ONCE spawns): an explicit `wt set --engine` override wins; else
    CCC's shared `worker_engine` default (see `_ccc_worker_engine_default`);
    else `codex`.

    The bare `codex` fallback is availability-guarded -- OPS-106 found codex
    missing from PATH on a VM, so blindly returning it here would hand back
    an engine no worker could actually spawn with. An explicit per-queue or
    `worker_engine` choice is honored as-is, with no such guard.

    This intentionally flips the default engine for every currently-unset
    queue (WT, CCC, BYM, OPS, HERMES) from the old hardcoded `claude` to
    `codex` (WT-105). Codex workers don't get the WT-49 ticket-context
    session rename (`messages.set_session_title` is claude-transcript-only)
    -- accepted tradeoff, tracked as a follow-up."""
    explicit = _queue_entry(queue).get("engine", "")
    if explicit:
        return explicit
    worker_default = _ccc_worker_engine_default()
    if worker_default:
        return worker_default
    from . import workers as _workers
    if _workers.engine_available("codex"):
        return "codex"
    print("[config] engine(): codex not on PATH, falling back to claude", file=sys.stderr)
    return "claude"


def set_model(queue: str, m: str) -> Dict[str, Any]:
    """Set (or clear, with "") the model workers on this queue are spawned with.

    Supported engine-specific aliases (e.g. ``opus-5`` for Claude) are stored
    as the canonical model id so downstream spawn logic receives a value the
    engine CLI understands.
    """
    data = _load()
    q = data.setdefault(queue, {})
    model_value = str(m or "").strip()
    if model_value:
        q["model"] = canonical_model(engine(queue), model_value)
    else:
        q.pop("model", None)
    _save(data)
    return q


def _ccc_default_model(eng: str) -> str:
    """CCC's own default model for `eng`, read from its spawn-defaults.json
    (``{"models": {"claude": "sonnet-5", ...}}``). Returns "" if the file is
    missing, unreadable, or has no entry for this engine -- a fresh install
    or a machine without CCC installed just gets the pre-existing "" (ambient
    CLI default) behavior."""
    try:
        with open(CCC_SPAWN_DEFAULTS_FILE) as f:
            data = json.load(f)
        m = str((data.get("models") or {}).get(eng) or "")
    except (OSError, ValueError, AttributeError):
        return ""
    # CCC's stored aliases (e.g. "sonnet-5") are bare short-forms meant for its
    # own UI/its `/model` picker, not `--model` flag values -- the claude CLI
    # spawn path needs the full `claude-` prefixed id (see build_drain_command
    # in workers.py). Only claude's aliases need this; other engines' ids are
    # used as-is.
    if eng == "claude" and m and not m.startswith("claude-"):
        m = f"claude-{m}"
    return m


def _ccc_worker_model_default(eng: str) -> str:
    """Return CCC's worker-only model when it belongs to ``eng``.

    ``worker_model`` is paired with ``worker_engine`` in CCC's Spawn defaults.
    A queue that explicitly selects a different engine must fall through to its
    own engine's shared model instead of receiving an incompatible worker
    override.
    """
    try:
        with open(CCC_SPAWN_DEFAULTS_FILE) as f:
            data = json.load(f)
        worker_engine = str(data.get("worker_engine") or "").strip().lower()
        model = str(data.get("worker_model") or "").strip()
    except (OSError, ValueError, AttributeError):
        return ""
    if not model or worker_engine != str(eng or "").strip().lower():
        return ""
    if worker_engine == "claude" and not model.startswith("claude-"):
        model = f"claude-{model}"
    return model


def default_model(eng: str) -> str:
    """Return the shared default model for an engine, if CCC configured one."""
    return _ccc_default_model(eng)


def fallback_engine(failed_engine: str) -> str:
    """Choose an available replacement engine after a provider-level failure.

    Prefer CCC's worker default so queue workers follow the fleet policy, then
    use the deterministic local order Codex -> Claude -> Kimi. The failed
    engine is never retried as its own fallback.
    """
    failed = str(failed_engine or "").strip().lower()
    candidates = [_ccc_worker_engine_default(), "codex", "claude", "kimi"]
    from . import workers as _workers
    seen = set()
    for candidate in candidates:
        candidate = str(candidate or "").strip().lower()
        if not candidate or candidate == failed or candidate in seen:
            continue
        seen.add(candidate)
        if candidate in MODEL_EFFORTS and _workers.engine_available(candidate):
            return candidate
    return ""


def model(queue: str) -> str:
    """Return the worker model for a queue: an explicit `wt set --model`
    override if one is configured, else CCC's worker-only default for this
    queue's engine, then CCC's shared New-session default, else "" (the
    engine's own ambient default, e.g. the bare `claude` CLI's configured
    default).

    Supported aliases are resolved to their canonical engine-CLI identifiers.
    """
    eng = engine(queue)
    explicit = _queue_entry(queue).get("model", "")
    if explicit:
        return canonical_model(eng, explicit)
    return canonical_model(eng, _ccc_worker_model_default(eng) or default_model(eng))


def set_effort(queue: str, value: str) -> Dict[str, Any]:
    """Set (or clear, with "") a queue worker's reasoning effort."""
    effort_value = str(value or "").strip().lower()
    if effort_value and effort_value not in VALID_EFFORTS:
        raise ValueError(f"effort must be one of {VALID_EFFORTS}")
    data = _load()
    q = data.setdefault(queue, {})
    if effort_value:
        q["effort"] = effort_value
    else:
        q.pop("effort", None)
    _save(data)
    return q


def effort(queue: str) -> str:
    """Return a queue override, CCC worker default, or engine default."""
    value = str(_queue_entry(queue).get("effort") or "").strip().lower()
    if value in VALID_EFFORTS:
        return value
    return _ccc_worker_effort_default()


def canonical_model(eng: str, model_value: str) -> str:
    """Resolve a supported alias to the canonical model id for ``eng``.

    Pass-through for values that are not aliases so legacy and CCC values stay
    unchanged. This is the single point where user-facing shortcuts like
    ``opus-5`` become the actual ``--model`` flag value the engine CLI accepts.

    Two layers, in order:

    1. The explicit ``MODEL_ALIASES`` table, for remaps where the short form
       does not simply prefix (``opus-5`` -> ``claude-opus-5`` survived a
       retarget from ``claude-opus-4-8``, so it must stay table-driven).
    2. A structural fallback for claude's *versioned* short forms
       (``sonnet-5`` -> ``claude-sonnet-5``). CCC stores these bare for its
       own ``/model`` picker, but the claude CLI's ``--model`` flag rejects
       them, so an explicit ``wt set --model sonnet-5`` used to reach
       ``build_drain_command`` verbatim and kill the worker at spawn with
       "There's an issue with the selected model (sonnet-5)". Bare *family*
       names (``sonnet``, ``opus``) and already-prefixed ids are valid as-is
       and pass through untouched.
    """
    eng = str(eng or "").strip().lower()
    m = str(model_value or "").strip()
    aliased = MODEL_ALIASES.get(eng, {}).get(m)
    if aliased:
        return aliased
    if eng == "claude" and m and not m.lower().startswith("claude-") \
            and _CLAUDE_VERSIONED_ALIAS.match(m):
        return f"claude-{m}"
    return m


def approved_models(eng: str) -> tuple[str, ...]:
    """Return the intentionally supported model identifiers for one engine.

    Includes both canonical engine-CLI identifiers and any supported aliases.
    """
    eng = str(eng or "").strip().lower()
    canonical = tuple(
        model for model, _ in MODEL_EFFORTS.get(eng, ())
    )
    return canonical + tuple(MODEL_ALIASES.get(eng, {}).keys())


# FEAT-NEXT-120 — per-ticket model floor. Index in this tuple is the tier
# (0 = lowest). Cross-engine on purpose: a ticket's floor is one model id
# from any approved engine's list, compared against whichever engine/model
# the CLAIMING queue actually runs. Keep in sync with queue.py's
# VALID_MODEL_FLOORS and MODEL_EFFORTS above as new models get approved --
# no ordering is implied by MODEL_EFFORTS itself (kimi's own list there
# already isn't junior->senior), this is the single explicit ranking.
MODEL_FLOOR_TIERS = (
    "kimi-code/k3",
    "kimi-code/kimi-for-coding",
    "claude-sonnet-5",
    "kimi-code/kimi-for-coding-highspeed",
    "claude-opus-4-8",
    "claude-opus-5",
)


def model_floor_met(queue: str, floor: str) -> bool:
    """True if ``queue``'s configured model meets or exceeds ``floor``'s tier.

    Fails OPEN (returns True, i.e. does not block a claim) for an empty
    floor, an unranked floor value, or a queue running a model this ranking
    doesn't cover -- a per-ticket floor is an interim, best-effort signal
    (the filer never blocks on certainty at filing time), not a hard
    guarantee; spuriously refusing a claim over a ranking gap is worse than
    occasionally under-enforcing one.
    """
    floor = str(floor or "").strip()
    if not floor or floor not in MODEL_FLOOR_TIERS:
        return True
    queue_model = canonical_model(engine(queue), model(queue))
    if queue_model not in MODEL_FLOOR_TIERS:
        return True
    return MODEL_FLOOR_TIERS.index(queue_model) >= MODEL_FLOOR_TIERS.index(floor)


# SIDE-39 -- the recognizable opening of the claim-time model-floor auto-park
# question. cli.py's claim path builds the block question from this constant,
# and workers.bump_timeboxed_model_floor_blocks() matches on it to tell a
# floor-park apart from an ordinary human-decision block (which must never be
# auto-answered). Single source of truth so detection cannot drift from the
# text the block actually writes.
MODEL_FLOOR_BLOCK_PREFIX = "This ticket's model floor is"

# SIDE-39 -- minutes a model-floor-parked ticket may sit blocked before the
# reconciler auto-bumps its queue's model one tier (see
# workers.bump_timeboxed_model_floor_blocks). Distinct from
# health.STUCK_MINUTES, which is queue-level (no close anywhere in the
# queue); this timebox is per-ticket, keyed off ``blocked_at``.
DEFAULT_MODEL_FLOOR_BUMP_MINUTES = 30


def model_floor_bump_minutes(queue: str) -> int:
    """Per-queue override for the model-floor auto-bump timebox.

    A ``model_floor_bump_minutes`` key on the queue's config entry wins;
    anything missing or unparseable falls back to
    ``DEFAULT_MODEL_FLOOR_BUMP_MINUTES``. Zero/negative values also fall
    back rather than meaning "bump instantly" -- an accidental 0 turning
    every floor-park into an immediate escalation is worse than a slow one.
    """
    raw = _queue_entry(queue).get(
        "model_floor_bump_minutes", DEFAULT_MODEL_FLOOR_BUMP_MINUTES
    )
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return DEFAULT_MODEL_FLOOR_BUMP_MINUTES
    return value if value > 0 else DEFAULT_MODEL_FLOOR_BUMP_MINUTES


def next_model_floor_tier(eng: str, current_model: str) -> str:
    """The next ``MODEL_FLOOR_TIERS`` entry above ``current_model`` that
    belongs to engine ``eng``, or "" when there is none.

    The global ladder is cross-engine on purpose (see its comment), so a
    naive index+1 could hand a claude queue a kimi model id -- which the
    claude CLI rejects at spawn, killing every worker on the queue. Climbing
    is therefore restricted to the same engine's models: "" comes back when
    ``current_model`` is unranked or already this engine's top ranked tier,
    and the caller leaves the ticket blocked for a human.
    """
    current = str(current_model or "").strip()
    if current not in MODEL_FLOOR_TIERS:
        return ""
    engine_models = {
        m for m, _ in MODEL_EFFORTS.get(str(eng or "").strip().lower(), ())
    }
    for candidate in MODEL_FLOOR_TIERS[MODEL_FLOOR_TIERS.index(current) + 1:]:
        if candidate in engine_models:
            return candidate
    return ""


def is_approved_model(eng: str, value: str) -> bool:
    """Whether ``value`` is empty or is an approved model/alias for ``eng``.

    The lower-level :func:`set_model` deliberately remains permissive so old
    configuration and programmatic callers remain readable. User-facing CLI
    commands use this predicate before persisting a new model selection.
    """
    model_value = str(value or "").strip()
    return not model_value or model_value in approved_models(eng)


def approved_efforts(eng: str, model: str = "") -> tuple[str, ...]:
    """Return supported explicit effort levels for a catalogued model.

    An unpinned model leaves effort to the engine default; allow the complete
    CLI vocabulary in that case because a local default can legitimately vary.
    """
    model_value = canonical_model(eng, model)
    if not model_value:
        return VALID_EFFORTS
    for candidate, efforts in MODEL_EFFORTS.get(
        str(eng or "").strip().lower(), ()
    ):
        if candidate == model_value:
            return efforts
    return ()


def is_approved_effort(eng: str, model: str, value: str) -> bool:
    """Whether ``value`` is empty or supported by the selected model."""
    effort_value = str(value or "").strip().lower()
    return not effort_value or effort_value in approved_efforts(eng, model)


def set_desired_workers(queue: str, n: int) -> Dict[str, Any]:
    value = int(n)
    if value < 0:
        raise ValueError("desired_workers must be >= 0")
    data = _load()
    q = data.setdefault(queue, {})
    q["desired_workers"] = value
    _save(data)
    return q


def desired_workers(queue: str) -> int:
    return int(_queue_entry(queue).get("desired_workers", 1))


def all_queues() -> Dict[str, Any]:
    """Return all configured queues (any queue with an entry in the config file)."""
    return dict(_load())


def ensure_entry(queue: str) -> Dict[str, Any]:
    """Create a config entry for queue if none exists yet."""
    data = _load()
    if queue not in data:
        data[queue] = {}
        _save(data)
    return dict(data[queue])

def ensure_entries(queues: Any) -> list:
    """Batched ``ensure_entry()``: one load/save for many queues at once.

    A queue with no config entry is invisible to
    ``workers._reconcile_once_locked()`` (it only iterates
    ``all_queues()``), so a manual ▶ run on its very first ticket silently
    no-ops forever -- no worker spawns, and the dispatch reason surfaced is
    the generic "no live worker accepted and none spawned" with no hint that
    the real cause is "this queue was never registered" (WT-131). Does not
    change ``auto_drain`` (stays default-off) or any other staffing
    behavior -- it only makes an already-visible-in-``wt status`` queue
    visible to the reconciler too. Returns the queue names newly created."""
    proj = sorted({str(q) for q in queues if q})
    if not proj:
        return []
    data = _load()
    created = [q for q in proj if q not in data]
    if not created:
        return []
    for q in created:
        data[q] = {}
    _save(data)
    return created


# One-time marker for the GitHub eligibility migration below. It lives next to
# the config file (not *inside* it) because every top-level key of that file is
# a queue name -- a reserved key would show up as a phantom queue in
# all_queues() and everything that iterates it.
GH_DRAIN_MIGRATION_MARKER = CONFIG_FILE.parent / "gh-drain-migration.done"

GH_DRAIN_MIGRATION_MESSAGE = (
    "WatchTower changed how GitHub queues pick work; drain was turned off for "
    "{queue} so nothing runs unexpectedly. Turn it back on when ready."
)


def migrate_github_auto_drain() -> list:
    """One-time: turn ``auto_drain`` off for every GitHub-backed queue.

    The dangerous moment in the eligibility change (2026-07-26 design) is the
    flip itself: the ``watchtower:<QUEUE>`` whitelist stops admitting tickets,
    so someone who upgrades with auto-drain on would have agents immediately
    start working *every* open issue in their repo. Turning drain off once, and
    saying why, makes re-enabling a deliberate act (which is also where the
    public-repo warning fires).

    Returns the queues that were switched off — empty on every later run,
    guarded by :data:`GH_DRAIN_MIGRATION_MARKER` so it cannot fire twice and
    undo a user who has since turned drain back on.
    """
    if GH_DRAIN_MIGRATION_MARKER.exists():
        return []
    data = _load()
    switched = []
    for name, entry in data.items():
        if not isinstance(entry, dict):
            continue
        if str(entry.get("backend") or "").strip().lower() != "github":
            continue
        if entry.get("auto_drain"):
            entry["auto_drain"] = False
            switched.append(name)
    if switched:
        _save(data)
    try:
        GH_DRAIN_MIGRATION_MARKER.parent.mkdir(parents=True, exist_ok=True)
        GH_DRAIN_MIGRATION_MARKER.write_text(
            json.dumps({
                "at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "queues": switched,
            }) + "\n"
        )
    except OSError:
        # Marker unwritable: the migration is still correct, it just may repeat
        # on the next run. Better than crashing the reconciler at startup.
        pass
    return switched


_REGISTRY_FILE = Path.home() / ".watchtower" / "queue-registry.json"


def migrate_from_registry() -> int:
    """One-time import of legacy queue-registry.json into queue-config.json.

    Renames the source file to ``*.migrated`` so it won't be re-processed.
    Returns the number of queues imported.
    """
    if not _REGISTRY_FILE.exists():
        return 0
    try:
        import json as _json
        with open(_REGISTRY_FILE) as f:
            reg = _json.load(f)
    except (OSError, ValueError):
        return 0
    if not isinstance(reg, dict):
        return 0
    data = _load()
    count = 0
    for name, rec in reg.items():
        entry = data.setdefault(name, {})
        for key in (
            "auto_drain", "engine", "desired_workers", "repo_path",
            "backend", "github_repo", "github_assignee",
        ):
            if key in rec and key not in entry:
                entry[key] = rec[key]
        count += 1
    if count:
        _save(data)
    try:
        _REGISTRY_FILE.rename(_REGISTRY_FILE.with_suffix(".json.migrated"))
    except OSError:
        pass
    return count

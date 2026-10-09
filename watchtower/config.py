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

Stored as ``queue-config.json`` in the persistent data directory, with
``~/.watchtower/queue-config.json`` imported automatically on first access.
"""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

VALID_BACKENDS = ("file", "github")
from . import models as _models

VALID_EFFORTS = _models.VALID_EFFORTS
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

# Model ids/aliases/efforts/ranks come from each engine's own catalog
# (see models.py); nothing in this module names a model.

# Claude short forms that carry a version (``sonnet-5``, ``opus-4-8``) and so
# need the ``claude-`` prefix to be a valid ``--model`` flag value. Bare family
# names (``sonnet``) are already accepted by the CLI and must NOT be rewritten.
_CLAUDE_VERSIONED_ALIAS = re.compile(r"^(sonnet|opus|haiku|fable)-\d", re.IGNORECASE)

_LEGACY_CONFIG_FILE = Path.home() / ".watchtower" / "queue-config.json"
CONFIG_FILE = Path(
    os.environ.get("WATCHTOWER_CONFIG_FILE")
    or _LEGACY_CONFIG_FILE
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

# Machine-wide model deny-list, shared with CCC (which owns the file):
#   {"blocked_models": ["gpt-6-astra"]}
# WATCHTOWER_BLOCKED_MODELS="a,b" is unioned in. A blocked model can neither
# be pinned on a queue (set_model raises) nor inherited from CCC's defaults
# or an old pin (model() substitutes an allowed fallback), and
# build_drain_command re-checks right before the argv is built. Motivation
# (2026-09-05): a 2.5x-priced model was the default in three places at once
# and drained a weekly allowance in half an hour.
CCC_MODEL_POLICY_FILE = Path(
    os.environ.get("WATCHTOWER_MODEL_POLICY_FILE")
    or (Path.home() / ".claude" / "command-center" / "model-policy.json")
)


def blocked_models() -> frozenset:
    """Lowercased model ids blocked by policy (env + CCC policy file)."""
    blocked = set()
    for token in str(os.environ.get("WATCHTOWER_BLOCKED_MODELS") or "").split(","):
        key = token.strip().lower()
        if key:
            blocked.add(key)
    try:
        with open(CCC_MODEL_POLICY_FILE) as f:
            data = json.load(f)
        raw = data.get("blocked_models") if isinstance(data, dict) else None
        for item in raw or []:
            key = str(item or "").strip().lower()
            if key:
                blocked.add(key)
    except (OSError, ValueError, AttributeError):
        pass
    return frozenset(blocked)


def is_blocked_model(value: str) -> bool:
    return str(value or "").strip().lower() in blocked_models()


def policy_fallback_model(eng: str) -> str:
    """First allowed model for ``eng``: CCC's worker default, then CCC's
    shared default, then the approved catalog in order; "" if none."""
    eng = str(eng or "").strip().lower()
    for candidate in (_ccc_worker_model_default(eng), default_model(eng)):
        candidate = canonical_model(eng, candidate)
        if candidate and not is_blocked_model(candidate):
            return candidate
    for candidate in _models.catalog(eng) or ():
        if not is_blocked_model(candidate):
            return candidate
    return ""


def config_path() -> Path:
    """Authoritative settings path, also used by CCC's direct file reader."""
    if CONFIG_FILE != _LEGACY_CONFIG_FILE or os.environ.get("WATCHTOWER_CONFIG_FILE"):
        return CONFIG_FILE.expanduser()
    from . import storage
    return storage.migrate_config(_LEGACY_CONFIG_FILE)


def _load() -> Dict[str, Any]:
    path = config_path()  # Migration errors must not become empty settings.
    try:
        with open(path, "r") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _audit_path() -> Path:
    return config_path().with_name("queue-config.audit.json")


def actor() -> Dict[str, Any]:
    """Who is making this change: source, session, host, user, pid, parent cmd."""
    import getpass
    import socket
    ppid = os.getppid()
    try:
        raw = Path(f"/proc/{ppid}/cmdline").read_bytes()
        parent = raw.replace(b"\0", b" ").decode("utf-8", "replace").strip()
    except OSError:
        parent = ""
    try:
        user = getpass.getuser()
    except Exception:  # noqa: BLE001 - no passwd entry in some containers
        user = os.environ.get("USER", "")
    return {
        "source": os.environ.get("WATCHTOWER_CONFIG_SOURCE") or "wt",
        "session": (os.environ.get("CLAUDE_SESSION_ID")
                    or os.environ.get("CLAUDE_CODE_SESSION_ID")
                    or os.environ.get("CCC_SESSION_ID") or ""),
        "host": socket.gethostname(),
        "user": user,
        "pid": os.getpid(),
        "parent": f"{ppid} {parent[:120]}".strip(),
    }


_MISSING = object()


def _config_diff(before: Dict[str, Any], after: Dict[str, Any]) -> Dict[str, List[str]]:
    """Per-queue ``key: old -> new`` lines between two config dicts."""
    out: Dict[str, List[str]] = {}
    for queue in sorted(set(before) | set(after)):
        b, a = before.get(queue), after.get(queue)
        if b == a:
            continue
        b = {} if b is None else b
        a = {} if a is None else a
        if not isinstance(b, dict) or not isinstance(a, dict):
            out[queue] = [f"{json.dumps(b)} \u2192 {json.dumps(a)}"]
            continue
        out[queue] = [
            f"{k}: {json.dumps(b[k]) if k in b else 'unset'} \u2192 "
            f"{json.dumps(a[k]) if k in a else 'unset'}"
            for k in sorted(set(b) | set(a)) if b.get(k, _MISSING) != a.get(k, _MISSING)
        ]
    return out


def _log_config(diff: Dict[str, List[str]], tail: str) -> None:
    try:
        from . import queue as _q
        for queue, lines in diff.items():
            _q._log("CONFIG", f"{queue} updated \u2014 {'; '.join(lines)} ({tail})", queue=queue)
    except Exception:  # noqa: BLE001 - auditing must never block a config write
        pass


def _record_audit(text: str, data: Dict[str, Any]) -> None:
    import hashlib
    try:
        _audit_path().write_text(json.dumps(
            {"sha": hashlib.sha256(text.encode()).hexdigest(), "config": data}))
    except OSError:
        pass


def check_outside_changes() -> Dict[str, List[str]]:
    """Daemon tick: log any edit to the live config that wt did not make.

    Compares the file's hash with the one recorded by the last ``_save``.
    Returns the diff (empty when unchanged or when there is no baseline yet).
    """
    import hashlib
    path = config_path()
    try:
        text = path.read_text()
        mtime = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
    except OSError:
        return {}
    sha = hashlib.sha256(text.encode()).hexdigest()
    try:
        base = json.loads(_audit_path().read_text())
    except (OSError, ValueError):
        base = None
    if not isinstance(base, dict) or "config" not in base:
        _record_audit(text, _load())  # first sight: establish the baseline
        return {}
    if base.get("sha") == sha:
        return {}
    try:
        now = json.loads(text)
    except ValueError:
        now = {}
    now = now if isinstance(now, dict) else {}
    diff = _config_diff(base["config"] if isinstance(base["config"], dict) else {}, now)
    stamp = mtime.strftime("%Y-%m-%d %H:%M:%S UTC")
    _log_config(diff, f"CONFIG changed outside wt; file mtime {stamp}")
    _record_audit(text, now)
    return diff


def _save(data: Dict[str, Any]) -> None:
    path = config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    # A pending outside edit is reported before our write buries it.
    check_outside_changes()
    try:
        before = json.loads(path.read_text())
    except (OSError, ValueError):
        before = {}
    tmp = str(path) + ".tmp"
    text = json.dumps(data, indent=2)
    with open(tmp, "w") as f:
        f.write(text)
    os.replace(tmp, path)
    _record_audit(text, data)
    a = actor()
    tail = (f"via {a['source']}; session={a['session'] or '-'} host={a['host']} "
            f"user={a['user']} pid={a['pid']} parent={a['parent']!r}")
    _log_config(_config_diff(before if isinstance(before, dict) else {}, data), tail)


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


DEFAULT_QUEUE_LABEL_PREFIX = "watchtower:"
# A queue whose ``queue_label`` is this value is the repo's catch-all: on a repo
# shared by 2+ queues it owns every issue that no *other* queue's label claims,
# so it needs no label of its own. At most one catch-all per repo.
CATCH_ALL_LABEL = "*"
# Labels that already mean something else to the GitHub backend.
_RESERVED_QUEUE_LABELS = {
    "watchtower:in-progress",
    "watchtower:no-auto-drain",
    "watchtower:play",
}


def _validate_queue_label(label: str) -> None:
    """Reject values ``gh`` would split, mangle, or that collide with a
    WatchTower control label, so a bad label fails at config time."""
    if "," in label or "\n" in label or "\r" in label:
        raise ValueError(
            f"queue_label {label!r} cannot contain commas or newlines"
        )
    if len(label) > 50:
        raise ValueError("queue_label must be 50 characters or fewer (GitHub's limit)")
    if label.lower() in _RESERVED_QUEUE_LABELS:
        raise ValueError(
            f"queue_label {label!r} is reserved for a WatchTower control label"
        )


def set_queue_label(queue: str, label: str) -> Dict[str, Any]:
    """Override the GitHub label that marks an issue as belonging to ``queue``
    on a repo shared by two or more queues. An empty value clears the override
    and restores the default ``watchtower:<queue>``."""
    data = _load()
    q = data.setdefault(queue, {})
    label = str(label or "").strip()
    if label:
        _validate_queue_label(label)
        if label == CATCH_ALL_LABEL:
            repo = str(q.get("github_repo") or "").strip().lower()
            for other, entry in data.items():
                if other == queue or not isinstance(entry, dict):
                    continue
                if (
                    str(entry.get("queue_label") or "").strip() == CATCH_ALL_LABEL
                    and str(entry.get("github_repo") or "").strip().lower() == repo
                ):
                    raise ValueError(
                        f"{other} is already the catch-all queue for {repo or 'this repo'}; "
                        "a repo can have only one"
                    )
        q["queue_label"] = label
    else:
        q.pop("queue_label", None)
    _save(data)
    return q


def is_catch_all(queue: str) -> bool:
    return queue_label(queue) == CATCH_ALL_LABEL


def sibling_queue_labels(queue: str, repo: str) -> list:
    """The membership labels of every OTHER queue sharing ``repo`` -- what a
    catch-all queue must leave alone. Catch-alls contribute no label."""
    out = []
    for name in github_queues_for_repo(repo):
        if name == queue:
            continue
        label = queue_label(name)
        if label != CATCH_ALL_LABEL:
            out.append(label)
    return out


def queue_label(queue: str) -> str:
    """The effective membership label: the configured ``queue_label``, else
    ``watchtower:<queue>``."""
    configured = str(_queue_entry(queue).get("queue_label") or "").strip()
    return configured or f"{DEFAULT_QUEUE_LABEL_PREFIX}{queue}"


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


def set_gates(queue: str, gates: List[str]) -> Dict[str, Any]:
    """Set this queue's default acceptance gates (WT-5); [] clears them."""
    from . import queue as _q
    data = _load()
    entry = data.setdefault(queue, {})
    clean = _q.validate_gates(gates)
    if clean:
        entry["gates"] = clean
    else:
        entry.pop("gates", None)
    _save(data)
    return entry


def gates(queue: str) -> List[str]:
    return list(_queue_entry(queue).get("gates") or [])


GATE_WORKTREE_MODES = ("fresh", "persistent")


def set_gate_worktree(queue: str, mode: str) -> Dict[str, Any]:
    """WATCHTOWER-38: where cmd gates run when the closing commit is not the
    checkout's HEAD. ``fresh`` (default) = a throwaway detached worktree;
    ``persistent`` = one reusable worktree per repo, force-reset to the commit,
    that keeps gitignored files (node_modules, caches) between runs."""
    if mode not in GATE_WORKTREE_MODES:
        raise ValueError(f"gate worktree must be one of {', '.join(GATE_WORKTREE_MODES)}")
    data = _load()
    q = data.setdefault(queue, {})
    if mode == "persistent":
        q["gate_worktree"] = mode
    else:
        q.pop("gate_worktree", None)
    _save(data)
    return q


def gate_worktree(queue: str) -> str:
    mode = str(_queue_entry(queue).get("gate_worktree") or "fresh")
    return mode if mode in GATE_WORKTREE_MODES else "fresh"


def set_post_fix_assessment(queue: str, enabled: bool) -> Dict[str, Any]:
    data = _load()
    q = data.setdefault(queue, {})
    if enabled:
        q["post_fix_assessment"] = True
    else:
        q.pop("post_fix_assessment", None)
    _save(data)
    return q


def post_fix_assessment(queue: str) -> bool:
    """WT-21: opt-in per queue. When on, every bug that closes as completed
    gets an independent six-point post-fix assessment."""
    return bool(_queue_entry(queue).get("post_fix_assessment", False))


def product_gate(queue: str) -> bool:
    """False unless explicitly opted in. When on, workers must post a
    decision-grade pitch (wt block --kind rationale) and wait for a human
    Ack before implementing — see the 2026-09-01 product-gate design."""
    return bool(_queue_entry(queue).get("product_gate", False))


DEFAULT_SENT_BACK_RELEASE_MIN = 30


def set_sent_back_release_min(queue: str, minutes: Any) -> Dict[str, Any]:
    """WT-34: minutes a sent-back claim may sit without progress before it is
    released to the pool. 0 disables the bound; ``None`` clears the override."""
    data = _load()
    q = data.setdefault(queue, {})
    if minutes is None:
        q.pop("sent_back_release_min", None)
    else:
        n = int(minutes)
        if n < 0:
            raise ValueError("sent-back release minutes must be >= 0")
        q["sent_back_release_min"] = n
    _save(data)
    return q


def sent_back_release_min(queue: str) -> int:
    """WT-34: default 30; 0 = never auto-release a sent-back claim."""
    try:
        return max(0, int(_queue_entry(queue).get(
            "sent_back_release_min", DEFAULT_SENT_BACK_RELEASE_MIN)))
    except (TypeError, ValueError):
        return DEFAULT_SENT_BACK_RELEASE_MIN


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


def worker_auto_compact_tokens() -> int:
    """Auto-compact threshold in tokens for workers, from CCC's
    spawn-defaults.json ``worker_auto_compact_k`` (thousands; the same
    setting CCC passes Claude as CLAUDE_CODE_AUTO_COMPACT_WINDOW). Default
    250000. Codex ignores that env var, so build_drain_command passes this
    as ``-c model_auto_compact_token_limit=<N>``. 0 means "do not set"."""
    k = 250
    try:
        with open(CCC_SPAWN_DEFAULTS_FILE) as f:
            data = json.load(f)
        if "worker_auto_compact_k" in data:
            k = float(data["worker_auto_compact_k"])
    except (OSError, ValueError, TypeError, AttributeError):
        pass
    return max(0, int(k * 1000))


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


def set_model(queue: str, m: str, *, confirm_blocked: bool = False) -> Dict[str, Any]:
    """Set (or clear, with "") the model workers on this queue are spawned with.

    Supported engine-specific aliases (e.g. ``opus-5`` for Claude) are stored
    as the canonical model id so downstream spawn logic receives a value the
    engine CLI understands. A policy-blocked model still raises unless the
    caller passes confirm_blocked=True (a deliberate, human-confirmed pick
    from CCC's queue-config dialog or `wt config -q ... --confirm-blocked`,
    2026-09-06) -- the automatic worker-dispatch path never sets this.
    """
    data = _load()
    q = data.setdefault(queue, {})
    model_value = str(m or "").strip()
    if model_value:
        resolved = canonical_model(engine(queue), model_value)
        if is_blocked_model(resolved) and not confirm_blocked:
            raise ValueError(
                f"model {model_value!r} is blocked by model policy "
                f"({CCC_MODEL_POLICY_FILE}); remove it from blocked_models to allow it, "
                "or pass confirm_blocked=True for a deliberate one-off override"
            )
        q["model"] = resolved
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


MODEL_PROFILES = ("fast", "standard", "deep")
FALLBACK_ENGINES = ("claude", "codex", "kimi", "grok", "devin")


def _ccc_model_settings() -> Dict[str, Any]:
    try:
        data = json.loads(CCC_SPAWN_DEFAULTS_FILE.read_text())
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _configured_model_routes(raw: Any) -> List[Dict[str, str]]:
    """Validate ordered routes without inventing a model or raising budgets.

    One entry per engine: an exhausted account cannot recover by selecting a
    second model on the same provider. Reject malformed config rather than
    accidentally reverting to ambient CLI defaults.
    """
    if not isinstance(raw, list) or len(raw) > len(FALLBACK_ENGINES):
        raise ValueError(f"model routes must be a list of at most {len(FALLBACK_ENGINES)} engines")
    seen = set()
    routes = []
    for entry in raw:
        if not isinstance(entry, dict):
            raise ValueError("model route must be an object")
        eng = str(entry.get("engine") or "").strip().lower()
        model_value = canonical_model(eng, str(entry.get("model") or "").strip())
        effort_value = str(entry.get("effort") or "").strip().lower()
        if eng not in FALLBACK_ENGINES or eng in seen:
            raise ValueError("model routes must use distinct supported engines")
        if (not model_value or is_blocked_model(model_value)
                or not is_approved_model(eng, model_value)):
            raise ValueError("model route is missing, unsupported, or blocked")
        if effort_value and (effort_value not in VALID_EFFORTS
                             or _effort_rejected(eng, model_value, effort_value)):
            raise ValueError("model route effort is not supported")
        routes.append({"engine": eng, "model": model_value, "effort": effort_value})
        seen.add(eng)
    return routes


def worker_fallback_policy() -> Optional[Dict[str, Any]]:
    """Machine policy owned by CCC; absence preserves legacy queue behavior."""
    data = _ccc_model_settings()
    if "worker_fallback" not in data:
        return None
    policy = data["worker_fallback"]
    if not isinstance(policy, dict) or not isinstance(policy.get("enabled"), bool):
        return {"enabled": False, "models": []}
    try:
        routes = _configured_model_routes(policy.get("models"))
    except ValueError:
        return {"enabled": False, "models": []}
    return {"enabled": policy["enabled"], "models": routes}


def model_profile(name: str) -> Dict[str, Any]:
    """Resolve an application capability profile without provider assumptions."""
    if name not in MODEL_PROFILES:
        raise ValueError("unknown model profile; use fast, standard, or deep")
    profiles = _ccc_model_settings().get("model_profiles")
    profile = profiles.get(name) if isinstance(profiles, dict) else None
    if not isinstance(profile, dict):
        raise ValueError(f"model profile {name!r} has not been configured in CCC Settings")
    routes = _configured_model_routes(profile.get("models"))
    if not routes:
        raise ValueError(f"model profile {name!r} has no configured models")
    if any(route["engine"] not in ("claude", "codex", "devin") for route in routes):
        raise ValueError("model profiles require engines supporting tool-free structured output")
    return {"models": routes}


def fallback_effort(queue: str, eng: str) -> str:
    """Explicit queue effort wins; otherwise use the selected fallback route."""
    pin = str(_queue_entry(queue).get("effort") or "").strip().lower()
    if pin in VALID_EFFORTS:
        return pin
    policy = worker_fallback_policy()
    for row in (policy or {}).get("models", []):
        if row["engine"] == eng:
            return row["effort"]
    return effort(queue)


def fallback_chain() -> List[str]:
    """The ordered engines a queue may be moved to: CCC's ``worker_fallback``
    routes when that policy exists, else the worker default then the local
    order Codex -> Claude -> Kimi. Raw order, unfiltered (may repeat or hold
    blanks); callers skip unavailable/excluded engines themselves."""
    policy = worker_fallback_policy()
    return ([row["engine"] for row in policy["models"]] if policy is not None
            else [_ccc_worker_engine_default(), "codex", "claude", "kimi"])


def fallback_engine(failed_engine: str, *, excluded=()) -> str:
    """Choose an available replacement engine after a provider-level failure.

    Prefer CCC's worker default so queue workers follow the fleet policy, then
    use the deterministic local order Codex -> Claude -> Kimi. The failed
    engine is never retried as its own fallback.
    """
    failed = str(failed_engine or "").strip().lower()
    candidates = fallback_chain()
    excluded = {str(item).strip().lower() for item in excluded}
    from . import workers as _workers
    seen = set()
    for candidate in candidates:
        candidate = str(candidate or "").strip().lower()
        if not candidate or candidate == failed or candidate in seen or candidate in excluded:
            continue
        seen.add(candidate)
        if candidate in _models.ENGINES and _workers.engine_available(candidate):
            return candidate
    return ""


def set_fallback_to_default_worker(queue: str, enabled: Optional[bool]) -> Dict[str, Any]:
    data = _load()
    q = data.setdefault(queue, {})
    if enabled is None:
        q.pop("fallback_to_default_worker", None)
    else:
        q["fallback_to_default_worker"] = bool(enabled)
    _save(data)
    return q


def fallback_to_default_worker(queue: str) -> bool:
    """"Revert to CCC default worker if current model is exhausted."

    An explicit queue On/Off overrides CCC's machine policy; a queue without
    an override inherits it. With no machine policy the legacy default is Off.
    When on, a queue whose engine keeps failing gets ``fallback_engine`` for
    that launch only -- the queue's stored engine/model are never rewritten.
    When off, such a queue is parked instead of switched."""
    entry = _queue_entry(queue)
    if "fallback_to_default_worker" in entry:
        return bool(entry["fallback_to_default_worker"])
    return bool((worker_fallback_policy() or {}).get("enabled", False))


def set_headroom_dispatch(queue: str, enabled: Optional[bool]) -> Dict[str, Any]:
    data = _load()
    q = data.setdefault(queue, {})
    if enabled is None:
        q.pop("headroom_dispatch", None)
    else:
        q["headroom_dispatch"] = bool(enabled)
    _save(data)
    return q


def headroom_dispatch(queue: str) -> bool:
    """Headroom-aware dispatch (31K B25): before launching, consult CCC's
    ``/api/headroom`` and start this queue's workers on whichever engine in
    its fallback chain has quota left -- soonest reset first, so the quota
    that expires first is spent first -- instead of the queue engine when
    that one is nearly exhausted. Launch-time only; the stored engine/model
    never change. Opt-in: an explicit queue On/Off wins, else CCC's
    spawn-defaults ``worker_headroom_dispatch``, else Off."""
    entry = _queue_entry(queue)
    if "headroom_dispatch" in entry:
        return bool(entry["headroom_dispatch"])
    return _ccc_model_settings().get("worker_headroom_dispatch") is True


def fallback_model(eng: str) -> str:
    """The model a substituted worker on ``eng`` runs with: the same shared
    defaults ``model()`` resolves for an unpinned queue on that engine. The
    failed queue's own pin belongs to its engine, so it is never carried over."""
    policy = worker_fallback_policy()
    if policy is not None:
        for row in policy["models"]:
            if row["engine"] == eng:
                return row["model"]
        return ""
    resolved = canonical_model(eng, _ccc_worker_model_default(eng) or default_model(eng))
    if resolved and is_blocked_model(resolved):
        return policy_fallback_model(eng)
    return resolved


def _usable_pin(queue: str, eng: str) -> str:
    """The queue's explicit model pin, or "" when it is unset or not approved
    for ``eng``. A raw config entry written around ``wt config`` (another tool,
    an old file) must not make the effective model unspawnable or unstable
    (WT-10): an unapproved pin is ignored and surfaced by
    :func:`model_pin_warning`. Blocked-by-policy pins are handled by the caller."""
    pin = str(_queue_entry(queue).get("model", "") or "").strip()
    if not pin:
        return ""
    resolved = canonical_model(eng, pin)
    if is_blocked_model(resolved):
        return pin  # model() substitutes the policy fallback for these
    if pin in ("sonnet", "opus", "haiku") or "fable" in pin.lower():
        # Bare claude family names are valid --model values; a fable pin must
        # reach spawn_workers, which refuses it loudly (WT-89).
        return pin
    # Approved for ANY engine: a spawn may override the queue's engine, and
    # the pin then rides along (a pin for no engine at all is the bad case).
    if is_approved_model(eng, pin) or any(pin in approved_models(e) for e in _models.ENGINES):
        return pin
    return ""


def model_pin_warning(queue: str) -> str:
    """Why the queue's stored model pin is being ignored, or ""."""
    eng = engine(queue)
    pin = str(_queue_entry(queue).get("model", "") or "").strip()
    if pin and not _usable_pin(queue, eng):
        return (
            f"pinned model {pin!r} is not approved for {eng}; ignored, "
            f"running {model(queue) or '(engine default)'!r} "
            f"(`wt models --engine {eng}`)"
        )
    return ""


def raw_model(queue: str) -> str:
    """The queue's configured/inherited model exactly as :func:`model` would
    resolve it, but WITHOUT the blocked-model substitution -- for callers that
    must fail loudly instead of quietly swapping models (the verify gate)."""
    eng = engine(queue)
    explicit = _usable_pin(queue, eng)
    if explicit:
        return canonical_model(eng, explicit)
    return canonical_model(eng, _ccc_worker_model_default(eng) or default_model(eng))


ROLE_KEYS = ("planner", "plan_reviewer", "verifier", "assessor")


def set_role(queue: str, role: str, eng: Any = None, model_value: Any = None) -> Dict[str, Any]:
    """Per-queue engine/model override for a non-builder role (WT-6, WT-14);
    "" clears a field, None leaves it. A model is stored canonicalised against
    the role's effective engine and must be in that engine's catalog (blocked
    models are rejected at spawn, not here)."""
    if role not in ROLE_KEYS:
        raise ValueError(f"unknown role {role!r}; expected one of {ROLE_KEYS}")
    data = _load()
    q = data.setdefault(queue, {})
    if eng is not None:
        e = str(eng or "").strip().lower()
        if e and e not in _models.ENGINES:
            raise ValueError(f"unknown engine {e!r}; expected one of {_models.ENGINES}")
        if e:
            q[f"{role}_engine"] = e
        else:
            q.pop(f"{role}_engine", None)
    if model_value is not None:
        m = str(model_value or "").strip()
        if m:
            role_eng = q.get(f"{role}_engine") or engine(queue)
            m = canonical_model(role_eng, m)
            if not is_approved_model(role_eng, m):
                raise ValueError(f"{m!r} is not approved for {role_eng}; "
                                 f"run `wt models --engine {role_eng}`")
            q[f"{role}_model"] = m
        else:
            q.pop(f"{role}_model", None)
    _save(data)
    return q


def role_override(queue: str, role: str) -> Tuple[str, str]:
    e = _queue_entry(queue)
    return str(e.get(f"{role}_engine") or ""), str(e.get(f"{role}_model") or "")


def set_verifier(queue: str, eng: Any = None, model_value: Any = None) -> Dict[str, Any]:
    return set_role(queue, "verifier", eng, model_value)


def verifier_override(queue: str) -> Tuple[str, str]:
    return role_override(queue, "verifier")


def model(queue: str) -> str:
    """Return the worker model for a queue: an explicit `wt set --model`
    override if one is configured, else CCC's worker-only default for this
    queue's engine, then CCC's shared New-session default, else "" (the
    engine's own ambient default, e.g. the bare `claude` CLI's configured
    default).

    Supported aliases are resolved to their canonical engine-CLI identifiers.
    """
    eng = engine(queue)
    explicit = _usable_pin(queue, eng)
    if explicit:
        resolved = canonical_model(eng, explicit)
    else:
        resolved = canonical_model(eng, _ccc_worker_model_default(eng) or default_model(eng))
    if resolved and is_blocked_model(resolved):
        # A pin that predates the policy, or an inherited CCC default that
        # policy now blocks: never spawn it. Substitute rather than fail so
        # the queue keeps draining on an allowed model.
        return policy_fallback_model(eng)
    return resolved


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
    inherited = _ccc_worker_effort_default()
    # A shared default cannot add reasoning controls to models that have none.
    # Keep explicit queue pins above intact; only omit an unsupported default.
    if inherited and _effort_rejected(engine(queue), model(queue), inherited):
        return ""
    return inherited


def canonical_model(eng: str, model_value: str) -> str:
    """Resolve a supported alias to the canonical model id for ``eng``.

    Pass-through for values that are not aliases so legacy and CCC values stay
    unchanged. This is the single point where user-facing shortcuts like
    ``opus-5`` become the actual ``--model`` flag value the engine CLI accepts.

    Two layers, in order:

    1. The catalog's aliases (``models.aliases``): display labels such as
       antigravity's picker labels, and claude ids without their prefix.
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
    aliased = _models.aliases(eng).get(m)
    if aliased:
        return aliased
    if eng == "claude" and m and not m.lower().startswith("claude-") \
            and _CLAUDE_VERSIONED_ALIAS.match(m):
        return f"claude-{m}"
    return m


def approved_models(eng: str) -> tuple[str, ...]:
    """Model identifiers the engine's catalog offers (minus policy-blocked
    ones), plus their aliases. Empty when no catalog is readable -- see
    :func:`models.catalog_warning`; :func:`is_approved_model` then accepts
    any explicit pin."""
    eng = str(eng or "").strip().lower()
    canonical = tuple(m for m in (_models.catalog(eng) or ()) if not is_blocked_model(m))
    return canonical + tuple(_models.aliases(eng))


# FEAT-NEXT-120 -- per-ticket model floor: one model id from any catalog,
# compared against whichever engine/model the CLAIMING queue actually runs.
# Ranks are data-derived (models.rank): catalog output price, or a user
# override in ~/.watchtower/models.json.
def model_meets_floor(model_id: str, floor: str) -> bool:
    """True if ``model_id`` (canonical) meets or exceeds ``floor``'s rank.

    An empty or unranked *floor* is not enforceable, so it is met. An
    unranked *model* fails CLOSED against a ranked floor (WT-10): routing the
    ticket to a known floor model is cheap, silently letting an unknown model
    work it is the bug this replaced.
    """
    floor_rank = _models.rank(floor)
    if not str(floor or "").strip() or floor_rank is None:
        return True
    model_rank = _models.rank(model_id)
    return model_rank is not None and model_rank >= floor_rank


def floor_engine(floor_model: str) -> str:
    """Engine whose catalog serves ``floor_model`` ("" when none does)."""
    return _models.engine_of(floor_model)


def is_valid_model_floor(value: str) -> bool:
    """A ticket floor is empty or any model some catalog knows."""
    value = str(value or "").strip()
    return not value or _models.known(value)


def queue_model_id(queue: str) -> str:
    """The queue's configured model as a canonical id ("" when unset)."""
    return canonical_model(engine(queue), model(queue))


def model_floor_met(queue: str, floor: str) -> bool:
    """True if ``queue``'s configured model meets or exceeds ``floor``'s tier
    (fails closed on an unranked queue model; see ``model_meets_floor``)."""
    return model_meets_floor(queue_model_id(queue), floor)


# The recognizable opening of the retired claim-time floor-park question
# (SIDE-39). Nothing writes it any more (WT-10); workers.reopen_legacy_floor_parks
# matches on it to put old parked tickets back in the pool.
MODEL_FLOOR_BLOCK_PREFIX = "This ticket's model floor is"


def is_approved_model(eng: str, value: str) -> bool:
    """Whether ``value`` is empty or is an approved model/alias for ``eng``.

    The lower-level :func:`set_model` deliberately remains permissive so old
    configuration and programmatic callers remain readable. User-facing CLI
    commands use this predicate before persisting a new model selection.
    """
    model_value = str(value or "").strip()
    if not model_value:
        return True
    if is_blocked_model(canonical_model(eng, model_value)):
        return False
    if _models.catalog(eng) is None:
        return True  # unknown catalog: never reject an explicit pin
    return model_value in approved_models(eng)


def approved_efforts(eng: str, model: str = "") -> tuple[str, ...]:
    """Return supported explicit effort levels for a catalogued model.

    An unpinned model leaves effort to the engine default; allow the complete
    CLI vocabulary in that case because a local default can legitimately vary.
    """
    model_value = canonical_model(eng, model)
    if not model_value:
        return VALID_EFFORTS
    cat = _models.catalog(eng)
    if cat is None:
        return VALID_EFFORTS
    return cat.get(model_value, ())


def is_approved_effort(eng: str, model: str, value: str) -> bool:
    """Whether ``value`` is empty or supported by the selected model."""
    effort_value = str(value or "").strip().lower()
    return not effort_value or effort_value in approved_efforts(eng, model)


def _effort_rejected(eng: str, model_value: str, effort_value: str) -> bool:
    """True only when a readable catalog lists ``model_value`` and does not
    offer ``effort_value`` for it. A missing catalog or a model it does not
    list may just be a stale catalog, so that is never a rejection."""
    effort_value = str(effort_value or "").strip().lower()
    canon = canonical_model(eng, model_value)
    cat = _models.catalog(eng)
    if not effort_value or not canon or cat is None or canon not in cat:
        return False
    return effort_value not in cat[canon]


def effort_pin_warning(queue: str) -> str:
    """Why the queue's pinned effort will be dropped at launch, or ""."""
    eng = engine(queue)
    eff = str(_queue_entry(queue).get("effort") or "").strip().lower()
    mdl = model(queue)
    if eff and _effort_rejected(eng, mdl, eff):
        return (f"effort {eff!r} not listed for {mdl!r} in the {eng} catalog; "
                "kept, but dropped at launch (`wt models --engine "
                f"{eng}`)")
    return ""


def sanitize_worker_settings(*, log=None) -> List[Dict[str, Any]]:
    """Report (never delete) queue efforts the model catalog rejects.

    WT-19: this used to pop the key, and a stale catalog made valid pins
    (VM-NEXT, CHUCK ``effort: medium``) vanish silently. A user pin is now
    never modified: an unapproved one is kept, surfaced by
    :func:`effort_pin_warning`, and dropped per launch by
    :func:`launch_effort`. A missing/stale catalog reports nothing.

    ``log`` is called once per flagged queue. Returns
    ``{"queue", "unapproved_effort", "engine", "model"}`` rows.
    """
    findings: List[Dict[str, Any]] = []
    for queue_name, entry in _load().items():
        if not isinstance(entry, dict):
            continue
        effort_value = str(entry.get("effort") or "").strip().lower()
        if not effort_value:
            continue
        try:
            eng = engine(queue_name)
            model_value = str(entry.get("model") or "").strip()
            rejected = _effort_rejected(eng, model_value, effort_value)
        except Exception:
            continue  # a queue we cannot evaluate is left exactly as-is
        if not rejected:
            continue
        findings.append({"queue": queue_name, "unapproved_effort": effort_value,
                         "engine": eng, "model": model_value})
        if log is not None:
            try:
                log(f"queue {queue_name}: effort {effort_value!r} not listed "
                    f"for model {model_value!r} (engine {eng}); kept")
            except Exception:
                pass
    return findings


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


def is_archived(queue: str) -> bool:
    return bool(_queue_entry(queue).get("archived", False))


def set_archived(queue: str, archived: bool) -> Dict[str, Any]:
    """Retire (or restore) a queue. Archiving also turns auto_drain off and
    records when; the queue's tickets are untouched."""
    data = _load()
    q = data.get(queue)
    if not isinstance(q, dict):
        q = data[queue] = {}
    if archived:
        q["archived"] = True
        q["archived_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        q["auto_drain"] = False
    else:
        q.pop("archived", None)
        q.pop("archived_at", None)
    _save(data)
    return q


def all_queues(include_archived: bool = False) -> Dict[str, Any]:
    """Return all configured queues (any queue with an entry in the config file).

    Archived queues (``wt queue archive``) are hidden unless ``include_archived``.
    """
    return {
        k: v for k, v in _load().items()
        if include_archived or not (isinstance(v, dict) and v.get("archived"))
    }


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
    marker = GH_DRAIN_MIGRATION_MARKER
    if marker == _LEGACY_CONFIG_FILE.parent / "gh-drain-migration.done":
        marker = config_path().parent / marker.name
    if marker.exists():
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
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text(
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

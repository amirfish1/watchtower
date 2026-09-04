# Copyright (c) 2026 Amir Fish. All rights reserved.
# SPDX-License-Identifier: LicenseRef-WatchTower-Software-License

"""GitHub Issues-backed WatchTower queue backend.

The public queue module stays the stable API. This module is an opt-in backing
store for a queue configured with ``backend=github`` and uses the installed
``gh`` CLI for auth, repository selection, and issue operations.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .queue import UNCLAIMABLE_READINESS, _FileLock

VALID_LANES = ("normal", "express")
# `gh issue` subcommands that actually change an issue's list-visible state
# (as opposed to `list`/`view`) -- `_run` invalidates the list cache for
# `self.repo` after any of these succeed, so a write is never invisible to
# a same-process soft read that follows it. See `_invalidate_list_cache`.
_MUTATING_ISSUE_VERBS = frozenset({"create", "edit", "close", "reopen", "comment"})
# Ticket-level eligibility inputs, stored as GitHub labels so they are visible
# and editable from GitHub's own UI (the file backend mirrors them as the plain
# booleans `no_auto_drain`/`run_requested` on each item, so downstream code
# never learns where they came from). `no-auto-drain` is an opt-*out*:
# the old `watchtower:<QUEUE>` whitelist is gone, so an issue is workable
# unless someone says otherwise. `play` is a human pressing the run button.
NO_AUTO_DRAIN_LABEL = "watchtower:no-auto-drain"
RUN_REQUESTED_LABEL = "watchtower:play"
# Mirrors config.DEFAULT_GRACE_S; duplicated only as the fallback for a backend
# built without a readable config (embedders, tests).
DEFAULT_GRACE_S = 180
VALID_READINESS = ("ready", "needs-shaping", "needs-spec", "")
VALID_PRIORITIES = ("p0", "p1", "p2", "p3", "p4", "")
VALID_VALUES = ("H", "M", "L", "")
VALID_CONFIDENCES = ("H", "M", "L", "")
DEFAULT_ITEM_TYPE = "bug"
COMPLETED_ISSUE_RETENTION_DAYS = 14

# Per-call list cost. GitHub GraphQL charges 1 point per 100 nodes returned,
# so `--limit` -- not the field list -- is the lever on what one
# `gh issue list` costs. Measured 2026-09-03 on amirfish1/BYM-Finie: the
# 14-day closed window is 459 issues, i.e. ~5 points per fetch at
# `--limit 1000` and ~2 at `--limit 200`.
#
# Measured too, and worth writing down because it is counter-intuitive: adding
# `body` to the field list costs *nothing*. `issues(first:100)` reports
# `cost: 1` with body and `cost: 1` without it. So `body` stays -- the
# `<!-- watchtower -->` metadata block that carries claimed_by, lane,
# priority, readiness, needs_input, history and resolution lives in the issue
# body, and dropping it from the list would break status and eligibility for
# every ticket in exchange for zero quota.
_LIST_LIMIT_DEFAULT = 200
_LIST_JSON_FIELDS = (
    "number,title,body,state,url,assignees,labels,createdAt,updatedAt,closedAt"
)


def _list_limit() -> int:
    """Max issues one ``gh issue list`` may return, per state.

    ``WATCHTOWER_GH_LIST_LIMIT`` overrides the default; a non-positive or
    unparseable value falls back to it.
    """
    raw = os.environ.get("WATCHTOWER_GH_LIST_LIMIT")
    if raw:
        try:
            limit = int(raw)
        except ValueError:
            return _LIST_LIMIT_DEFAULT
        if limit > 0:
            return limit
    return _LIST_LIMIT_DEFAULT

_ISSUE_URL_RE = re.compile(r"/issues/(\d+)(?:\D*)?$")
_META_START = "<!-- watchtower"
_META_END = "-->"


class GitHubBackendError(RuntimeError):
    """Raised when the configured ``gh`` backend cannot complete an operation."""

    def __init__(self, message: str, *, cached: bool = False):
        super().__init__(message)
        self.cached = cached


# `_list_issues` is on the hot path of a live dashboard (CCC polls
# list_items() every few seconds per open conversation-list refresh) but each
# GitHubIssuesBackend is a fresh, state-less instance per call (see
# `_github_backend_for_project`), so per-instance caching would do nothing --
# the cache has to live at module level, keyed by repo. Without it, a
# rate-limited repo got re-hit on every single poll, which never let the
# limit recover and flooded the activity log with an identical ERROR line
# every couple of seconds (WT-87). `_LIST_CACHE_TTL` reuses a recent good
# result instead of re-listing; `_LIST_ERROR_BACKOFF` throttles how often a
# failing repo is retried at all, and falls back to the last known-good list
# (silently, if we have one) rather than re-raising the same error forever.
#
# The TTL is deliberately short (2s, was 20s): it is no longer what keeps the
# poll cheap -- the ETag probe below is. It now only dedupes bursts of calls
# within a single agent turn, so a new issue shows up on the board within
# seconds instead of up to twenty.
_LIST_CACHE: Dict[str, Dict[str, Any]] = {}
_LIST_CACHE_TTL = 2.0
_LIST_ERROR_BACKOFF = 60.0

# Cap on how often a non-strict reader honors a "changed" ETag probe with a
# real GraphQL fetch. On a busy repo the probe fires on every poll sweep, so
# without this cap the fetch rate -- not the per-call cost -- is what burns
# the hourly GraphQL quota. 60s bounds a busy repo to one heavy fetch per
# state per minute; quiet repos never notice (a change past the cap fetches
# immediately).
_LIST_FETCH_MIN_INTERVAL_S = 60.0

# Persisted list cache (2026-08-11, WT reconciler-latency fix). `_LIST_CACHE`
# above is in-process only, so it does nothing for the common case: `wt run`
# / `wt claim` / the reconciler's own dispatch_after_enqueue path are each a
# fresh short-lived process (same blind spot noted for connectivity state
# above), so every one of them paid a live `gh` probe-or-fetch inline, on the
# critical path, even seconds after some other process just did the exact
# same fetch. Measured cost: a single `reconcile_once()` sweep across two
# github-backed queues cost ~15s in `gh` subprocesses alone.
#
# The fix is to make GitHub reads and reconciler reads two different
# activities. A background poller (started by the foreground daemon, see
# cli.py `_daemon_loop` / `poll_list_caches_forever`) is now the ONLY thing
# that calls `refresh_persisted_list_cache()` -- it owns paying the live `gh`
# cost, on its own interval, off the reconciler's critical path. Every other
# (non-strict) reader of `_list_issues` reads this file instead of calling
# `gh` itself. `strict` callers (claim, close) are unaffected: they are
# about to write and still pay for a live call, same as before.
#
# `_PERSISTED_LIST_STALE_S` is the self-healing bound: if the poller stops
# (daemon not running, or crashed), a soft reader falls back to its own live
# fetch rather than serving indefinitely stale data.
_GH_LIST_CACHE_FILE = Path.home() / ".watchtower" / "gh-list-cache.json"
_PERSISTED_LIST_STALE_S = 300.0

# A poll is "in flight" while the poller holds this marker. Readers use it to
# answer the one question that decides whether they may spend a live `gh`
# call: is the staleness I'm seeing a *dead* poller (fetch it myself, or the
# queue freezes) or a poller that is fetching the replacement right now (wait
# -- my duplicate fetch would buy nothing and cost quota)? Before W4-4 there
# was no way to tell them apart, so every one of ~8 concurrent lanes answered
# "dead" simultaneously and each paid for the same list.
#
# The marker is a file whose mtime is the poll's start. `_GH_POLL_INFLIGHT_MAX_S`
# bounds how long a marker is believed: a poller killed mid-fetch leaves its
# marker behind forever, and an unbounded marker would be indistinguishable
# from a healthy poll and would stall live reads permanently.
_GH_POLL_INFLIGHT_MAX_S = 120.0


def _list_cache_path() -> Path:
    env = os.environ.get("WATCHTOWER_GH_LIST_CACHE_FILE")
    if env:
        return Path(env).expanduser()
    return _GH_LIST_CACHE_FILE


def _persisted_list_ttl_s() -> float:
    """How long a poller-written snapshot is served without a live read.

    ``WATCHTOWER_GH_LIST_TTL_S`` overrides the 5-minute default; a
    non-positive or unparseable value falls back to it, so a typo in the
    environment cannot silently turn every read back into a live `gh` call.
    """
    raw = os.environ.get("WATCHTOWER_GH_LIST_TTL_S")
    if raw:
        try:
            ttl = float(raw)
        except ValueError:
            return _PERSISTED_LIST_STALE_S
        if ttl > 0:
            return ttl
    return _PERSISTED_LIST_STALE_S


def _poll_marker_path(repo: str) -> Path:
    safe_repo = re.sub(r"[^A-Za-z0-9_.-]", "_", repo or "") or "unknown"
    return _claim_locks_dir() / f"{safe_repo}.polling"


def _mark_poll_started(repo: str) -> None:
    path = _poll_marker_path(repo)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(os.getpid()))
    except OSError:
        pass


def _mark_poll_finished(repo: str) -> None:
    try:
        _poll_marker_path(repo).unlink()
    except OSError:
        pass


def _poll_in_flight(repo: str) -> bool:
    """Whether a poller is mid-fetch for ``repo`` right now."""
    try:
        age = time.time() - _poll_marker_path(repo).stat().st_mtime
    except OSError:
        return False
    return age < _GH_POLL_INFLIGHT_MAX_S


def _try_claim_poll(repo: str) -> bool:
    """Atomically become the owner of a fetch for ``repo``; False if taken.

    Readers race each other, so the check and the claim have to be one
    operation -- `if not _poll_in_flight(): _mark_poll_started()` lets every
    lane that runs the check in the same instant win it, which is the exact
    stampede the marker exists to prevent. `O_CREAT | O_EXCL` makes the kernel
    pick one winner.

    An expired marker (owner killed mid-fetch) is removed first, so a dead
    poller cannot make the claim permanently unwinnable.
    """
    path = _poll_marker_path(repo)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        return False
    if path.exists() and not _poll_in_flight(repo):
        try:
            path.unlink()
        except OSError:
            pass
    try:
        fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
    except FileExistsError:
        return False
    except OSError:
        return False
    try:
        os.write(fd, str(os.getpid()).encode())
    finally:
        os.close(fd)
    return True


def _claim_locks_dir() -> Path:
    env = os.environ.get("WATCHTOWER_GH_CLAIM_LOCKS_DIR")
    if env:
        return Path(env).expanduser()
    return Path.home() / ".watchtower" / "gh-claim-locks"


_PERSISTED_LIST_MEM_CACHE: Dict[str, Any] = {"mtime_ns": 0, "size": -1, "data": {}}
_PERSISTED_LIST_MEM_LOCK = threading.Lock()


def _read_persisted_list_cache() -> Dict[str, Any]:
    path = _list_cache_path()
    try:
        st = path.stat()
        mtime_ns, size = st.st_mtime_ns, st.st_size
    except OSError:
        return {}
    with _PERSISTED_LIST_MEM_LOCK:
        if (
            _PERSISTED_LIST_MEM_CACHE["mtime_ns"] == mtime_ns
            and _PERSISTED_LIST_MEM_CACHE["size"] == size
        ):
            return dict(_PERSISTED_LIST_MEM_CACHE["data"])
        try:
            raw = path.read_text()
            data = json.loads(raw)
        except (OSError, json.JSONDecodeError):
            return {}
        if not isinstance(data, dict):
            data = {}
        _PERSISTED_LIST_MEM_CACHE["mtime_ns"] = mtime_ns
        _PERSISTED_LIST_MEM_CACHE["size"] = size
        _PERSISTED_LIST_MEM_CACHE["data"] = data
        return dict(data)


def _rewrite_persisted_list_cache(mutate) -> None:
    """Load-modify-atomic-write the persisted cache file under its lock.

    ``mutate(data) -> bool`` edits ``data`` in place and returns whether
    anything actually changed (skipping the write entirely when it didn't)."""
    path = _list_cache_path()
    with _FileLock(path.with_suffix(".lock")):
        data = _read_persisted_list_cache()
        if not mutate(data):
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = str(path) + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, path)


def _write_persisted_list_entry(key: str, entry: Dict[str, Any]) -> None:
    def mutate(data: Dict[str, Any]) -> bool:
        data[key] = entry
        return True

    _rewrite_persisted_list_cache(mutate)


def _invalidate_list_cache(repo: str) -> None:
    """Drop the in-memory AND persisted list cache for ``repo`` (both
    states) after a local write.

    Read-your-own-writes: without this, a mutation this process just made
    (mark_runnable, claim, close, comment, ...) would be invisible to the
    next soft (``fresh=False``) read in this same process, for up to the
    persisted cache's staleness bound -- e.g. ``count_manual_eligible()``
    called right after ``mark_runnable()`` in ``dispatch_after_enqueue``
    would see the pre-mutation list. Clearing both caches forces exactly one
    fresh live read on the very next call, which is the same live-read
    frequency this had before the persisted cache existed -- just paid only
    right after a write, not on every read.

    It marks the entries stale rather than deleting them. Deleting worked, but
    it threw away the two fields WATCHTOWER-16 added the persisted cache to
    share -- `etag` and `fetched_at` -- and both the ETag probe and the
    heavy-fetch cap in `_list_issues` are gated on `cached is not None`. So on
    a repo with live workers every claim/close reset the entry to "no
    validator, no fetch clock", the replacement fetch stored `etag=""` (no
    probe had run to supply one), and the next mutation landed before the
    entry could ever earn a real validator back. Measured 2026-09-02:
    `amirfish1/BYM-Finie` sat permanently at `etag=""` and an age of seconds
    while every other repo held a real ETag and an age of hours -- ~9
    `gh issue list` per minute against one repo, the whole residual burn after
    the owner-answer fix (docs/github-quota-exhaustion-2026-09-02.md).

    Stale means exactly what deletion meant for correctness -- the next read
    must go live, no probe and no cap may short-circuit it -- while the
    validator and the fetch clock survive to be re-paired with the list that
    read brings back."""
    for state in ("open", "closed"):
        entry = _LIST_CACHE.get(f"{repo}:{state}")
        if entry is not None:
            entry["stale"] = True
            # `stale_local` is the part of read-your-own-writes that is
            # genuinely non-negotiable: *this* process just wrote, so its next
            # read must be live, full stop. The persisted `stale` flag below is
            # a weaker claim -- it tells other processes "a write landed" -- and
            # W4-4 lets those coalesce behind one refresh instead of each
            # paying for its own. Conflating the two is what made the busy repo
            # re-list once per reader per write. See `_list_issues`.
            entry["stale_local"] = True

    def mutate(data: Dict[str, Any]) -> bool:
        changed = False
        for state in ("open", "closed"):
            entry = data.get(f"{repo}:{state}")
            if isinstance(entry, dict) and not entry.get("stale"):
                entry["stale"] = True
                changed = True
        return changed

    _rewrite_persisted_list_cache(mutate)


def refresh_persisted_list_cache(repo: str) -> None:
    """Do a live ``gh`` fetch for every state of ``repo`` and persist it.

    The only function in this module allowed to spend a blocking `gh` call
    on behalf of a *soft* (non-strict) reader -- called exclusively by the
    background poller, never by ``_list_issues`` itself. Best-effort per
    state: a repo that is unreachable/rate-limited leaves the previous
    persisted entry in place (``_list_issues`` already recorded the failure
    via ``_record_gh_failure``) rather than wiping out the last known-good
    list.
    """
    inst = GitHubIssuesBackend(repo, repo=repo)
    before = _graphql_quota_snapshot(force=True)
    _mark_poll_started(repo)
    try:
        _refresh_persisted_list_cache_states(inst, repo)
    finally:
        _mark_poll_finished(repo)
    after = _graphql_quota_snapshot(force=True)
    _log_quota(
        "poll",
        repo=repo,
        cost=(
            None
            if before is None or after is None
            else max(0, after["used"] - before["used"])
        ),
        remaining=None if after is None else after["remaining"],
        limit=None if after is None else after["limit"],
    )


def _refresh_persisted_list_cache_states(
    inst: "GitHubIssuesBackend", repo: str
) -> None:
    for state in ("open", "closed"):
        key = f"{repo}:{state}"
        # A worker's successful mutation invalidates this shared entry from a
        # different process.  The daemon poller can still retain its old
        # snapshot in memory, where a changed ETag would otherwise be held by
        # the normal heavy-fetch cap and then re-persisted as current.  A
        # stale-marked (or, from an entry written before stale-marking existed,
        # a missing) shared entry is an explicit invalidation, so discard the
        # poller's private copy and fetch the authoritative replacement.
        shared = _read_persisted_list_cache().get(key)
        private = _LIST_CACHE.get(key)
        if not isinstance(shared, dict) or shared.get("stale"):
            _LIST_CACHE.pop(key, None)
        elif (
            private is not None
            and shared.get("data") is not None
            and float(shared.get("at") or 0) > float(private.get("at") or 0)
        ):
            # Not stale, and newer than what the poller holds: the mutating
            # process already published the post-write list. Adopt it instead
            # of revalidating a copy we know is behind -- otherwise the poller
            # probes with its own outdated ETag, gets a 200, is held by the cap,
            # and re-persists its older list over the newer one.
            _LIST_CACHE[key] = {
                "at": float(shared["at"]), "data": shared["data"],
                "error": None,
                "etag": str(shared.get("etag") or ""),
                "fetched_at": float(shared.get("fetched_at") or 0),
                "stale": False,
            }
        try:
            inst._list_issues(state, fresh=True, poller=True)
        except GitHubBackendError:
            continue
        entry = _LIST_CACHE.get(key)
        if entry is not None and entry.get("data") is not None:
            # `etag`/`fetched_at` ride along so a short-lived CLI process can
            # seed its empty `_LIST_CACHE` from this file and inherit the
            # poller's validator and heavy-fetch clock (see `_list_issues`)
            # instead of paying an uncapped GraphQL list of its own.
            _write_persisted_list_entry(
                key,
                {
                    "at": entry["at"],
                    "data": entry["data"],
                    "etag": str(entry.get("etag") or ""),
                    "fetched_at": float(entry.get("fetched_at") or 0),
                    "stale": False,
                },
            )


def poll_list_caches_once() -> None:
    """One sweep: refresh the persisted cache for every configured
    github-backed queue's repo (deduped -- two queues can share a repo)."""
    from . import config
    repos = set()
    for qname in config.all_queues():
        if config.backend(qname) == "github":
            repo = config.github_repo(qname)
            if repo:
                repos.add(repo)
    for repo in repos:
        try:
            refresh_persisted_list_cache(repo)
        except Exception:
            pass  # one bad repo must not stop the sweep or kill the poller


# Owner-answer sweep throttle. `ingest_owner_answer` has to spend a live
# `gh issue view` -- the only path that fetches comments -- *before* it can
# tell there is nothing to ingest. So "only tickets that are actually blocked"
# was never "normally zero": one blocked ticket cost one GraphQL read on every
# sweep, i.e. every ~12s, indefinitely. Measured 2026-09-02: two permanently
# blocked tickets accounted for 50 of the daemon's 59 heavy GraphQL calls in a
# five-minute window, ~1,800 points/hour against a 5,000/hour quota
# (docs/github-quota-exhaustion-2026-09-02.md).
#
# The sweep now spends that read only when the warm list snapshot says the
# issue actually moved. GitHub bumps an issue's `updatedAt` when a comment is
# posted -- the same property `_list_probe_path` already relies on to make the
# ETag probe notice comments on old issues -- so a real owner answer still
# lands on the next sweep, with no added latency.
#
# Two costs, both bounded and both deliberate. The snapshot is a cached list,
# so an answer is seen once that list refreshes rather than on the next sweep
# -- but the sweep already depended on the same snapshot to learn the ticket is
# blocked at all, so this adds no new staleness bound. And `updated_at` has
# second resolution, so an answer posted in the same second as the preceding
# block reads as "unchanged" until the backstop fires.
#
# `_OWNER_ANSWER_MAX_QUIET_S` is that backstop: an unchanged `updated_at` is
# never trusted for longer than this. Under a same-second collision, a missing
# timestamp, or a frozen one, the sweep degrades to its old behaviour at a
# bounded rate rather than silently never ingesting again -- WATCHTOWER-5 is a
# correctness path, so it may get slower, never wrong.
_OWNER_ANSWER_MAX_QUIET_S = 300.0
# repo-scoped ticket key -> (probed_at, updated_at seen at that probe).
_owner_answer_probes: Dict[str, "tuple[float, str]"] = {}


def _owner_answer_probe_due(key: str, updated_at: str, now: float) -> bool:
    """True when a blocked ticket is worth a live ``gh issue view``.

    Records the decision, so a caller that gets True must go on to probe (the
    sweep's per-ticket `except` swallows failures, which only costs one skipped
    re-read before the backstop fires).
    """
    seen = _owner_answer_probes.get(key)
    if seen is not None:
        probed_at, last_updated = seen
        if (
            now - probed_at < _OWNER_ANSWER_MAX_QUIET_S
            and updated_at
            and updated_at == last_updated
        ):
            return False
    _owner_answer_probes[key] = (now, updated_at)
    return True


def _prune_owner_answer_probes(now: float) -> None:
    """Drop entries for tickets that stopped being blocked.

    A still-blocked ticket re-probes at least every ``_OWNER_ANSWER_MAX_QUIET_S``
    (the backstop above), so anything several multiples older is gone --
    unblocked, closed, or its queue removed. Keeps the map bounded over a
    daemon lifetime measured in weeks without any cross-queue bookkeeping.
    """
    cutoff = now - (_OWNER_ANSWER_MAX_QUIET_S * 4)
    for key in [k for k, v in _owner_answer_probes.items() if v[0] < cutoff]:
        _owner_answer_probes.pop(key, None)


def poll_owner_answers_once() -> None:
    """One sweep: ingest owner-authored answer comments on blocked tickets for
    every github-backed queue (WATCHTOWER-5).

    The blocked set comes from the already-warm list cache (issue bodies carry
    ``needs_input``), and a live ``gh issue view`` is spent on a blocked ticket
    only when that snapshot shows the issue moved since the last read -- see
    ``_owner_answer_probe_due``. Each step is best-effort; one bad queue or
    ticket must never stop the sweep."""
    from . import config
    from . import queue as _queue
    seen_targets = set()
    now = time.time()
    _prune_owner_answer_probes(now)
    for qname in config.all_queues():
        try:
            if config.backend(qname) != "github":
                continue
            backend = _queue._github_backend_for_project(qname)
            if backend is None:
                continue
            dedup_key = (
                backend.repo,
                bool(backend.partition_by_label),
                getattr(backend, "project_label", None) if backend.partition_by_label else None,
            )
            if dedup_key in seen_targets:
                continue
            seen_targets.add(dedup_key)
            for item in backend.list_items(status="in_progress"):
                if not item.get("needs_input"):
                    continue
                ident = item.get("ref") or item.get("number")
                if not _owner_answer_probe_due(
                    f"{backend.repo}#{ident}",
                    str(item.get("updated_at") or ""),
                    now,
                ):
                    continue
                try:
                    backend.ingest_owner_answer(ident)
                except Exception:
                    pass  # one wedged ticket must not stop the queue's sweep
        except Exception:
            pass  # one bad queue must not stop the whole sweep


def poll_list_caches_forever(interval_s: float = 5.0, *, stop_event=None) -> None:
    """Background loop for the foreground daemon: keep every github-backed
    queue's list cache warm on disk so the reconciler's reads never block on
    `gh`. Runs until ``stop_event`` is set (never, if omitted, but a caller
    may still pass its own ``threading.Event`` for a graceful stop).

    Deliberately waits on a ``threading.Event`` rather than ``time.sleep``:
    it is the correct primitive for a cancellable background loop, and,
    unlike ``time.sleep``, is not the same global entry point every other
    interval-based loop in this codebase patches to fast-forward a test.

    Same never-kill-the-loop contract as the rest of cli.py's daemon loop
    (outbox drain, receipts sweep, log prune, ...): a bad repo or a raised
    exception during a sweep may not be allowed to end this thread, since
    nothing else keeps the persisted cache warm."""
    event = stop_event if stop_event is not None else threading.Event()
    while not event.is_set():
        try:
            poll_list_caches_once()
        except Exception:
            pass
        try:
            poll_owner_answers_once()
        except Exception:
            pass
        event.wait(interval_s)

# Global GitHub-reachability tracking (2026-07-27 design). Persisted, not
# in-memory: `wt status`/`wt ls` each run in a fresh short-lived process, so
# the module-level `_LIST_CACHE` above is invisible across CLI invocations --
# "no successful poll in 5 minutes" only means something if the evidence
# survives between them. `_GH_BACKOFF_BASE_S`/`_GH_BACKOFF_CAP_S` are the
# escalating-retry ladder (60s -> 120s -> 240s -> 480s -> capped at 600s)
# applied while GitHub stays unreachable, so a prolonged outage doesn't keep
# retrying every 60 seconds forever. `_GH_SUCCESS_WRITE_THROTTLE_S` caps how
# often a healthy poll rewrites the state file -- this sits on a hot path
# (CCC's dashboard polls every few seconds).
_GH_CONNECTIVITY_FILE = Path.home() / ".watchtower" / "gh-connectivity.json"
_GH_BACKOFF_BASE_S = 60.0
_GH_BACKOFF_CAP_S = 600.0
_GH_SUCCESS_WRITE_THROTTLE_S = 30.0

# Pre-emptive GraphQL quota guard. `gh issue list` is a GraphQL operation that
# can burn the hourly quota fast on a busy repo. Before paying for a rich
# fetch, read the quota and skip the fetch when we're close to the limit,
# serving cached data instead. The reading is cached so the extra call is
# amortized across all polls.
#
# THE WRONG METER (measured 2026-09-03, W4-4). This guard used to read the REST
# endpoint `gh api rate_limit` and look at `.resources.graphql.remaining`. That
# number is not this token's GraphQL quota. At one instant, on one machine:
#
#     gh api rate_limit  -> graphql: used=0     remaining=5000
#     gh api graphql -f query='{rateLimit{used remaining}}'
#                        -> rateLimit: used=1025 remaining=3975
#
# ...while `~/.watchtower/watcher.log` was full of "API rate limit already
# exceeded for user ID 255024423". So the guard compared against a number
# pinned at the limit and could never trip -- it passed at exactly the moments
# it existed to catch. (WATCHTOWER-19 read the same discrepancy as an
# invisible *user-level aggregate* limit that `rate_limit` could not see. It is
# simpler than that: `rate_limit` can see it, we were reading the wrong field
# of the wrong endpoint. The observed-evidence hold below stays regardless --
# believing real failures is still correct -- but it is no longer the only
# defence.)
#
# The authoritative meter is the `rateLimit` block returned in-band by a real
# GraphQL request. Reading it is self-funding: a query whose only selection is
# `rateLimit` reports `cost: 1` but does not move `used` (measured: two
# back-to-back reads report an identical `used`).
_GH_GRAPHQL_LOW_THRESHOLD = 300
_GH_RATE_LIMIT_CHECK_INTERVAL_S = 30.0
_GH_GRAPHQL_QUOTA_CACHE: Dict[str, Any] = {"ts": 0.0, "snapshot": None}
_GH_QUOTA_LOG_FILE = Path.home() / ".watchtower" / "gh-quota.log"
_GRAPHQL_QUOTA_QUERY = "{rateLimit{limit cost remaining used resetAt}}"


def _quota_log_path() -> Path:
    env = os.environ.get("WATCHTOWER_GH_QUOTA_LOG")
    if env:
        return Path(env).expanduser()
    return _GH_QUOTA_LOG_FILE


def _log_quota(event: str, **fields: Any) -> None:
    """Append one JSON line of GraphQL quota accounting to the quota log.

    Best-effort and deliberately unable to raise: quota *bookkeeping* must
    never be what breaks a poll or a queue read.
    """
    record: Dict[str, Any] = {"ts": _now_iso(), "event": event}
    record.update({k: v for k, v in fields.items() if v is not None})
    try:
        path = _quota_log_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a") as f:
            f.write(json.dumps(record) + "\n")
    except (OSError, TypeError, ValueError):
        pass


def _graphql_quota_snapshot(*, force: bool = False) -> Optional[Dict[str, int]]:
    """Read the authoritative GraphQL quota, in-band. ``None`` if unreadable.

    Cached for ``_GH_RATE_LIMIT_CHECK_INTERVAL_S`` seconds; ``force`` bypasses
    the cache, which is what the poller's before/after accounting needs.
    Any failure is swallowed and returns None, in which case callers proceed
    normally -- an unreadable meter must not be able to freeze the queue.
    """
    now = time.time()
    if not force and now - _GH_GRAPHQL_QUOTA_CACHE["ts"] < _GH_RATE_LIMIT_CHECK_INTERVAL_S:
        return _GH_GRAPHQL_QUOTA_CACHE["snapshot"]
    try:
        result = subprocess.run(
            ["gh", "api", "graphql", "-f", f"query={_GRAPHQL_QUOTA_QUERY}"],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if result.returncode != 0 or not result.stdout.strip():
            return None
        payload = json.loads(result.stdout)
        block = (payload.get("data") or {}).get("rateLimit") or {}
        snapshot = {
            "limit": int(block.get("limit") or 0),
            "remaining": int(block.get("remaining") or 0),
            "used": int(block.get("used") or 0),
        }
    except (OSError, ValueError, TypeError, json.JSONDecodeError, subprocess.SubprocessError):
        return None
    _GH_GRAPHQL_QUOTA_CACHE["ts"] = now
    _GH_GRAPHQL_QUOTA_CACHE["snapshot"] = snapshot
    return snapshot


def _graphql_rate_limit_remaining() -> Optional[int]:
    """Remaining points on the authoritative GraphQL quota, or None."""
    snapshot = _graphql_quota_snapshot()
    return None if snapshot is None else snapshot["remaining"]


def _graphql_quota_remaining_cached() -> Optional[int]:
    """Remaining points *if already known*, without spending a call to find out.

    For accounting only. `_log_quota` runs on the tail of every heavy list, and
    reading the meter there through `_graphql_rate_limit_remaining` would spawn
    a `gh` subprocess on any fetch whose 30s meter cache had expired -- i.e.
    metering the burn would itself become part of the burn, and would drag a
    live network call into every unit test that fakes `_run`. A stale-by-30s
    number in the log is worth more than a call.
    """
    snapshot = _GH_GRAPHQL_QUOTA_CACHE.get("snapshot")
    return None if not isinstance(snapshot, dict) else snapshot.get("remaining")


# Observed-evidence rate-limit hold. The pre-emptive guard above reads
# `gh api rate_limit`, which only reports the per-token REST/GraphQL buckets.
# GitHub also enforces a *user-level* aggregate limit across all of an
# account's tokens, and that one is invisible to `rate_limit`: during the
# 2026-08-30 storm it read 5000 remaining / 0 used while every GraphQL call
# came back "API rate limit already exceeded for user ID ..." (WATCHTOWER-19).
# So the guard passed exactly when it had to trip. The fix is to believe the
# failures: when a real `gh` call reports a rate-limit error, hold live calls
# off until the reset window passes and serve caches meanwhile. The generic
# 60s->600s unreachability ladder is not enough -- a first rate-limit error
# would only defer 60s, and the retry burns quota that is already spent.
#
# The hold length is clamped: GitHub's reported reset (whichever bucket resets
# last) is a hint, not the user-level window, so we neither trust a reset a
# second away nor black out for a full hour on one error.
_GH_RATE_LIMIT_MIN_HOLD_S = 300.0
_GH_RATE_LIMIT_MAX_HOLD_S = 900.0
_RATE_LIMIT_ERROR_RE = re.compile(r"rate limit|abuse detection", re.IGNORECASE)


def _is_rate_limit_error(error: Any) -> bool:
    """Whether a gh error message is GitHub telling us we're out of quota."""
    return bool(_RATE_LIMIT_ERROR_RE.search(str(error or "")))


def _rate_limit_hold_seconds() -> float:
    """How long to hold off live GitHub calls after a rate-limit error.

    Asks GitHub when its buckets reset and clamps the answer into
    ``[_GH_RATE_LIMIT_MIN_HOLD_S, _GH_RATE_LIMIT_MAX_HOLD_S]``. Any failure to
    read the reset falls back to the minimum hold.
    """
    try:
        result = subprocess.run(
            ["gh", "api", "rate_limit", "--jq", "[.resources[].reset] | max"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0 and result.stdout.strip():
            delta = float(result.stdout.strip()) - time.time()
            return max(
                _GH_RATE_LIMIT_MIN_HOLD_S, min(_GH_RATE_LIMIT_MAX_HOLD_S, delta)
            )
    except Exception:
        pass
    return _GH_RATE_LIMIT_MIN_HOLD_S


def _connectivity_path() -> Path:
    env = os.environ.get("WATCHTOWER_GH_CONNECTIVITY_FILE")
    if env:
        return Path(env).expanduser()
    return _GH_CONNECTIVITY_FILE


def _empty_connectivity() -> Dict[str, Any]:
    return {
        "last_success_at": None,
        "broken_since": None,
        "consecutive_failures": 0,
        "next_retry_at": None,
        "last_error": "",
        "rate_limited_until": None,
    }


def _load_connectivity() -> Dict[str, Any]:
    try:
        return json.loads(_connectivity_path().read_text())
    except (OSError, json.JSONDecodeError):
        return _empty_connectivity()


def _save_connectivity(state: Dict[str, Any]) -> None:
    path = _connectivity_path()
    with _FileLock(path.with_suffix(".lock")):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(state, indent=2) + "\n")


def _record_gh_success() -> None:
    """Record a real, live GitHub fetch that succeeded.

    Throttled: on a healthy streak this runs on nearly every poll (a hot
    path), and a healthy queue doesn't need sub-second precision on "last
    success". A recovery (``broken_since`` was set) always writes
    immediately so the alert clears without waiting out the throttle.
    """
    state = _load_connectivity()
    recovering = state.get("broken_since") is not None
    if not recovering:
        last = _parse_iso(state.get("last_success_at"))
        if last is not None:
            elapsed = (datetime.now(timezone.utc) - last).total_seconds()
            if elapsed < _GH_SUCCESS_WRITE_THROTTLE_S:
                return
    state["last_success_at"] = _now_iso()
    state["broken_since"] = None
    state["consecutive_failures"] = 0
    state["next_retry_at"] = None
    state["last_error"] = ""
    # A live fetch that succeeded is direct proof the account is out of the
    # penalty box, whatever the hold said.
    state["rate_limited_until"] = None
    _save_connectivity(state)


def _record_gh_failure(error: str) -> None:
    """Record a real, live GitHub fetch that failed and escalate the backoff."""
    state = _load_connectivity()
    if state.get("broken_since") is None:
        state["broken_since"] = _now_iso()
    failures = int(state.get("consecutive_failures") or 0) + 1
    state["consecutive_failures"] = failures
    delay = min(_GH_BACKOFF_CAP_S, _GH_BACKOFF_BASE_S * (2 ** (failures - 1)))
    state["next_retry_at"] = (
        datetime.now(timezone.utc) + timedelta(seconds=delay)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    state["last_error"] = str(error)
    if _is_rate_limit_error(error):
        hold_until = datetime.now(timezone.utc) + timedelta(
            seconds=_rate_limit_hold_seconds()
        )
        state["rate_limited_until"] = hold_until.strftime("%Y-%m-%dT%H:%M:%SZ")
        # The hold never shortens the generic ladder, only extends it.
        if hold_until > (datetime.now(timezone.utc) + timedelta(seconds=delay)):
            state["next_retry_at"] = state["rate_limited_until"]
    _save_connectivity(state)


def _record_gh_rate_limited(error: str) -> None:
    """Record a rate-limit error observed on any live `gh` call.

    Separate from ``_record_gh_failure`` because it fires from ``_run`` for
    every gh verb (a ticket read, a comment write), not just the list fetch:
    a quota wall applies to the whole account, so whichever call hits it first
    should stop the rest. It sets the hold without escalating the
    unreachability ladder -- GitHub is reachable, we're just out of budget.
    """
    state = _load_connectivity()
    hold_until = datetime.now(timezone.utc) + timedelta(
        seconds=_rate_limit_hold_seconds()
    )
    state["rate_limited_until"] = hold_until.strftime("%Y-%m-%dT%H:%M:%SZ")
    state["last_error"] = str(error)
    if state.get("broken_since") is None:
        state["broken_since"] = _now_iso()
    existing = _parse_iso(state.get("next_retry_at"))
    if existing is None or existing < hold_until:
        state["next_retry_at"] = state["rate_limited_until"]
    _save_connectivity(state)


def _gh_backoff_active() -> "tuple[bool, Dict[str, Any]]":
    """Whether a live GitHub fetch should be skipped right now, plus the
    current persisted state (so the caller can reuse it for the error
    message without a second read)."""
    state = _load_connectivity()
    now = datetime.now(timezone.utc)
    next_retry = _parse_iso(state.get("next_retry_at"))
    rate_limited_until = _parse_iso(state.get("rate_limited_until"))
    active = (next_retry is not None and now < next_retry) or (
        rate_limited_until is not None and now < rate_limited_until
    )
    return active, state

# Cheap change *detector* for the issue list. A conditional GET answers "did
# anything move in this repo?" in ~0.5s and, on a 304, costs nothing against
# the rate limit (`X-RateLimit-Remaining` is unchanged across it).
#
# It is a detector and not a replacement fetcher on purpose: it mixes pull
# requests into the payload and lacks several fields WatchTower needs to render
# queue rows. A 200 only means "go look" -- `gh issue list --json ...` still
# produces the queue data. Full comment context is loaded by `get()` for the
# individual ticket, not by this bulk fetch.
_HTTP_STATUS_RE = re.compile(r"^HTTP/[\d.]+\s+(\d{3})")


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _age_seconds(created_at: Any, *, now: Optional[datetime] = None) -> float:
    """Seconds since ``created_at``. An unparseable/missing timestamp reads as
    infinitely old: the grace period exists to protect *new* issues, and a
    ticket we cannot date is not new evidence."""
    created = _parse_iso(created_at)
    if created is None:
        return float("inf")
    return ((now or datetime.now(timezone.utc)) - created).total_seconds()


def _recently_closed(issue: Dict[str, Any], *, now: Optional[datetime] = None) -> bool:
    closed_at = _parse_iso(issue.get("closedAt"))
    if closed_at is None:
        return False
    current = now or datetime.now(timezone.utc)
    return closed_at >= current - timedelta(days=COMPLETED_ISSUE_RETENTION_DAYS)


def _clip(value: Any, max_len: int) -> str:
    s = "" if value is None else str(value)
    s = " ".join(s.split()) if max_len <= 240 else s
    return s if len(s) <= max_len else s[:max_len].rstrip() + "..."


# Owner-answer ingestion (WATCHTOWER-5): a repo owner can answer a blocked
# ticket by typing a comment straight onto the GitHub issue, marked with this
# phrase (case-insensitive). We require BOTH the marker AND author==repo owner
# so an ordinary owner comment ("looks good!") can never silently clear a block.
_OWNER_ANSWER_MARKER = "OWNER ANSWER"


def _owner_answer_comment_key(comment: Dict[str, Any]) -> str:
    """Stable idempotency key for one issue comment.

    Prefers GitHub's own node id / url; falls back to a content fingerprint for
    comment payloads that omit them (older gh, test fakes). The key is recorded
    in issue metadata once the comment is ingested so re-polling the same
    comment can never clear the block a second time (WATCHTOWER-5)."""
    cid = str(comment.get("id") or comment.get("url") or "").strip()
    if cid:
        return cid
    author = str((comment.get("author") or {}).get("login") or "")
    raw = f"{author}|{comment.get('createdAt', '')}|{comment.get('body', '')}"
    return "sha:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def _append_history(meta: Dict[str, Any], event: str, **fields: Any) -> None:
    """Append-only lifecycle trail (WT-87), mirroring the local backend's
    ``queue._append_history`` — stored in the issue-body metadata block so it
    survives round-trips through ``_split_body``/``_body_with_metadata``."""
    hist = meta.get("history")
    if not isinstance(hist, list):
        hist = []
    entry: Dict[str, Any] = {"event": event, "at": _now_iso()}
    for key, value in fields.items():
        if value:
            entry[key] = value
    hist.append(entry)
    meta["history"] = hist


def _http_status(raw: str) -> Optional[int]:
    """Status code from ``gh api -i`` output, whose first line is the status
    line. Returns None when the output does not look like an HTTP response."""
    first = (raw or "").lstrip().split("\n", 1)[0].strip()
    match = _HTTP_STATUS_RE.match(first)
    return int(match.group(1)) if match else None


def _etag_header(raw: str) -> str:
    """The ETag out of ``gh api -i`` output (headers only: the scan stops at
    the blank line, so a body that happens to contain `etag:` can't spoof it)."""
    for line in (raw or "").splitlines():
        if not line.strip():
            break
        name, sep, value = line.partition(":")
        if sep and name.strip().lower() == "etag":
            return value.strip()
    return ""


def _norm_choice(value: Any, valid_values: tuple, default: str = "") -> str:
    s = str(value or "").strip()
    if s in valid_values:
        return s
    return default


def _effective_type(value: Any) -> str:
    s = str(value or "").strip().lower()
    return s if s in ("bug", "feature") else DEFAULT_ITEM_TYPE


def _prio_rank(it: Dict[str, Any]) -> int:
    return {"p0": 0, "p1": 1, "p2": 2, "p3": 3, "p4": 4}.get(
        it.get("priority", ""), 5
    )


def _type_rank(it: Dict[str, Any]) -> int:
    return {"bug": 0, "feature": 1}.get(_effective_type(it.get("type")), 2)


# Mirrors queue.RESOLUTION_LIST_FIELDS (defined here rather than imported:
# queue imports this module, not the other way round).
RESOLUTION_LIST_FIELDS = ("caveats", "follow_ups", "unresolved")


def _normalize_resolution(resolution: Any) -> Optional[Dict[str, Any]]:
    if resolution is None:
        return None
    if isinstance(resolution, str):
        resolution = {"summary": resolution}
    if not isinstance(resolution, dict):
        return None
    out: Dict[str, Any] = {}
    summary = _clip(resolution.get("summary", ""), 4000)
    if summary:
        out["summary"] = summary
    for field in ("caveats", "follow_ups", "unresolved"):
        raw = resolution.get(field)
        if raw is None:
            continue
        if isinstance(raw, str):
            raw = [raw]
        vals = [_clip(v, 4000) for v in raw if str(v or "").strip()]
        if vals:
            out[field] = vals
    # Acknowledgements (queue.ack_resolution) ride alongside their list as
    # ``<field>_ack``; keep them through normalization so a re-read of the
    # issue metadata doesn't silently un-acknowledge every chip a human
    # cleared. Mirrors queue._normalize_resolution.
    for field in ("caveats", "follow_ups", "unresolved"):
        acks = resolution.get(f"{field}_ack")
        if isinstance(acks, dict) and acks and out.get(field):
            kept = {
                str(k): v for k, v in acks.items()
                if str(k).isdigit() and int(k) < len(out[field])
            }
            if kept:
                out[f"{field}_ack"] = kept
    return out or None


def _resolution_comment(resolution: Optional[Dict[str, Any]]) -> str:
    if not resolution:
        return "WatchTower closed this ticket."
    lines = [f"WatchTower resolution: {resolution.get('summary', '')}".rstrip()]
    for key, label in (
        ("caveats", "Caveats"),
        ("follow_ups", "Follow-ups"),
        ("unresolved", "Unresolved"),
    ):
        values = resolution.get(key) or []
        if values:
            lines.append(f"{label}:")
            lines.extend(f"- {value}" for value in values)
    return "\n".join(lines)


def _meta_block(meta: Dict[str, Any]) -> str:
    lines = [_META_START]
    for key in sorted(meta):
        value = meta.get(key)
        if value in (None, ""):
            continue
        lines.append(f"{key}: {json.dumps(value)}")
    lines.append(_META_END)
    return "\n".join(lines)


def _split_body(body: str) -> tuple[str, Dict[str, Any]]:
    body = body or ""
    start = body.find(_META_START)
    if start < 0:
        return body.rstrip(), {}
    end = body.find(_META_END, start)
    if end < 0:
        return body.rstrip(), {}
    human = body[:start].rstrip()
    raw = body[start + len(_META_START):end].strip()
    meta: Dict[str, Any] = {}
    for line in raw.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        try:
            meta[key] = json.loads(value)
        except json.JSONDecodeError:
            meta[key] = value
    return human, meta


def _body_with_metadata(human_body: str, meta: Dict[str, Any]) -> str:
    human_body = (human_body or "").rstrip()
    if human_body:
        return f"{human_body}\n\n{_meta_block(meta)}"
    return _meta_block(meta)


def _first_line(value: str) -> str:
    """First non-blank line of real content, skipping markdown ATX headings.

    Bug-report bodies from the studio-assistant template open with a bare
    ``## Problem`` / ``## Feature request`` heading — returning that verbatim
    left every queue-panel row reading "## Problem" instead of the actual
    complaint, since ``note`` is what list rows preview.
    """
    for line in (value or "").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        return line
    return ""


def _label_names(raw: Any) -> List[str]:
    out: List[str] = []
    if not isinstance(raw, list):
        return out
    for label in raw:
        if isinstance(label, dict):
            name = label.get("name")
        else:
            name = label
        if name:
            out.append(str(name))
    return out


def _assignee_logins(raw: Any) -> List[str]:
    out: List[str] = []
    if not isinstance(raw, list):
        return out
    for assignee in raw:
        if isinstance(assignee, dict):
            login = assignee.get("login")
        else:
            login = assignee
        if login:
            out.append(str(login))
    return out


def _issue_comments_text(raw: Any) -> str:
    """Render GitHub issue comments as worker-readable ticket context."""
    if not isinstance(raw, list):
        return ""
    comments: List[str] = []
    for comment in raw:
        if isinstance(comment, dict):
            body = str(comment.get("body") or "").strip()
            author = comment.get("author")
            login = str(author.get("login") or "").strip() if isinstance(author, dict) else ""
        else:
            body = str(comment or "").strip()
            login = ""
        if body:
            comments.append(f"@{login}: {body}" if login else body)
    return "\n\n".join(comments)


def _issue_text(body: str, note: Any, title: Any, comments: Any) -> str:
    """Combine an issue's body and discussion without altering its source body."""
    text = body or str(note or "") or str(title or "")
    comment_text = _issue_comments_text(comments)
    if not comment_text:
        return text
    return f"{text}\n\n## GitHub comments\n\n{comment_text}" if text else comment_text


def repo_visibility(repo: str) -> str:
    """``"public"``/``"private"``/``"internal"`` for a repo, or "" if unknown.

    Best-effort by design: no gh, no auth, no network, a renamed repo — all
    come back "" so a caller can say "couldn't check" instead of guessing.
    """
    repo = str(repo or "").strip()
    if not repo:
        return ""
    try:
        proc = subprocess.run(
            ["gh", "repo", "view", repo, "--json", "visibility"],
            capture_output=True, text=True, timeout=20,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return ""
    if proc.returncode != 0:
        return ""
    try:
        data = json.loads(proc.stdout or "{}")
    except json.JSONDecodeError:
        return ""
    return str((data or {}).get("visibility") or "").strip().lower()


def public_repo_warning(queue: str, repo: str) -> str:
    """The warning to show before auto-drain is switched on, or "".

    Turning drain on for a queue backed by a *public* repo points agents at
    every issue anyone on the internet can file. That is a legitimate setup,
    so this warns rather than refuses — but it must be said out loud, once,
    at the moment of the decision.
    """
    if repo_visibility(repo) != "public":
        return ""
    return (
        f"WARNING: {repo} is a PUBLIC repo. With drain on, {queue} workers will "
        f"pick up issues filed by anyone, including strangers. Label an issue "
        f"{NO_AUTO_DRAIN_LABEL} to keep agents off it, or run `wt drain off {queue}`."
    )


class GitHubIssuesBackend:
    """A WatchTower queue backed by GitHub Issues via ``gh``."""

    def __init__(
        self,
        queue: str,
        *,
        repo: str = "",
        repo_path: str = "",
        assignee: str = "@me",
        auto_drain: Optional[bool] = None,
        grace_s: Optional[int] = None,
        partition_by_label: Optional[bool] = None,
    ):
        self.queue = queue
        self.repo = str(repo or "").strip()
        self.repo_path = str(repo_path or "").strip()
        self.assignee = str(assignee or "@me").strip() or "@me"
        self.queue_label = f"watchtower:{queue}"
        self.in_progress_label = "watchtower:in-progress"
        self.no_auto_drain_label = NO_AUTO_DRAIN_LABEL
        self.run_requested_label = RUN_REQUESTED_LABEL
        # The queue-level half of the eligibility model. Read once per backend
        # instance (one is built per queue operation, see
        # queue._github_backend_for_project) so every item produced by this
        # instance is judged against one consistent policy snapshot. Callers
        # may inject explicit values; tests and embedders that never wrote a
        # config file get the safe defaults (drain off, standard grace).
        self.auto_drain = bool(auto_drain) if auto_drain is not None else self._config_auto_drain()
        self.grace_s = int(grace_s) if grace_s is not None else self._config_grace_s()
        # `watchtower:<QUEUE>` is inert when this repo backs exactly one queue.
        # It keeps one job -- and only this one: partitioning a repo shared by
        # two or more queues, where nothing else can say which issue is whose.
        self.partition_by_label = (
            bool(partition_by_label)
            if partition_by_label is not None
            else self._config_partitions_by_label()
        )

    def _config_auto_drain(self) -> bool:
        try:
            from . import config
            return bool(config.auto_drain(self.queue))
        except Exception:
            return False

    def _config_grace_s(self) -> int:
        try:
            from . import config
            return int(config.grace_s(self.queue))
        except Exception:
            return DEFAULT_GRACE_S

    def _config_partitions_by_label(self) -> bool:
        try:
            from . import config
            return len(config.github_queues_for_repo(self.repo)) > 1
        except Exception:
            return False

    def _repo_args(self) -> List[str]:
        if not self.repo:
            raise GitHubBackendError(
                "backend=github requires github_repo to be configured; "
                "refusing to fall back to the ambient cwd repo"
            )
        return ["--repo", self.repo]

    def _run_raw(self, args: List[str]) -> "subprocess.CompletedProcess[str]":
        """Run ``gh`` and hand back the whole result.

        Split out of ``_run`` because the ETag probe has to read the exit code
        and stderr itself: ``gh api`` exits 1 on a 304, which is a success for
        us, and ``_run``'s exit-code-means-failure rule cannot express that.
        """
        try:
            return subprocess.run(
                ["gh", *args],
                cwd=self.repo_path or None,
                capture_output=True,
                text=True,
                timeout=60,
            )
        except FileNotFoundError as exc:
            raise GitHubBackendError(
                "GitHub backend requires the gh CLI to be installed and on PATH"
            ) from exc
        except subprocess.TimeoutExpired as exc:
            raise GitHubBackendError(f"gh {' '.join(args)} timed out") from exc

    def _run(self, args: List[str], *, check: bool = True) -> str:
        proc = self._run_raw(args)
        if check and proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "").strip()
            if _is_rate_limit_error(detail):
                # Observed evidence beats `gh api rate_limit`, which cannot
                # see the user-level aggregate limit (WATCHTOWER-19).
                _record_gh_rate_limited(detail)
            raise GitHubBackendError(
                f"gh {' '.join(args)} failed"
                + (f": {detail}" if detail else "")
            )
        if (
            proc.returncode == 0
            and len(args) > 1
            and args[0] == "issue"
            and args[1] in _MUTATING_ISSUE_VERBS
        ):
            _invalidate_list_cache(self.repo)
        return proc.stdout

    def _ensure_label(self, name: str, color: str, description: str) -> None:
        self._run(
            [
                "label", "create", name,
                *self._repo_args(),
                "--color", color,
                "--description", description,
                "--force",
            ],
            check=False,
        )

    def _ensure_labels(self) -> None:
        self._ensure_label(
            self.queue_label,
            "5319e7",
            f"WatchTower queue {self.queue}",
        )
        self._ensure_label(
            self.in_progress_label,
            "fbca04",
            "Claimed by a WatchTower worker",
        )
        # Created even though nothing here adds them automatically: they are
        # the user's controls, and GitHub only offers labels that already
        # exist in the repo's picker.
        self._ensure_label(
            self.no_auto_drain_label,
            "b60205",
            "WatchTower: never auto-drain this ticket",
        )
        self._ensure_label(
            self.run_requested_label,
            "0e8a16",
            "WatchTower: run this ticket now",
        )

    def _issue_number(self, ident: Any) -> Optional[int]:
        s = str(ident or "").strip()
        if not s:
            return None
        if s.isdigit():
            return int(s)
        prefix = f"{self.queue}-"
        if s.upper().startswith(prefix.upper()):
            suffix = s[len(prefix):]
            return int(suffix) if suffix.isdigit() else None
        return None

    def _issue_to_item(self, issue: Dict[str, Any]) -> Dict[str, Any]:
        body, meta = _split_body(str(issue.get("body") or ""))
        text = _issue_text(body, meta.get("note"), issue.get("title"), issue.get("comments"))
        labels = _label_names(issue.get("labels"))
        assignees = _assignee_logins(issue.get("assignees"))
        # Membership, not admission: under the blacklist model every issue in
        # the repo belongs to this queue unless the repo is shared, in which
        # case the legacy label is the only thing that can divide them.
        queue_member = (not self.partition_by_label) or self.queue_label in labels
        number = int(issue.get("number") or 0)
        state = str(issue.get("state") or "").upper()
        if state == "CLOSED":
            status = "closed"
        elif queue_member and (meta.get("claimed_by") or self.in_progress_label in labels):
            status = "in_progress"
        else:
            status = "open"

        # Eligibility: three independent inputs, two derived predicates.
        #   auto_eligible   = auto_drain AND NOT no-auto-drain AND age >= grace_s
        #   manual_eligible = run_requested   (play beats all three of them)
        #   work_it         = auto_eligible OR manual_eligible
        no_auto_drain = self.no_auto_drain_label in labels
        run_requested = self.run_requested_label in labels
        workable = queue_member and status == "open"
        auto_eligible = bool(
            workable
            and self.auto_drain
            and not no_auto_drain
            and _age_seconds(issue.get("createdAt")) >= self.grace_s
        )
        manual_eligible = bool(workable and run_requested)
        work_it = auto_eligible or manual_eligible

        resolution = _normalize_resolution({
            "summary": meta.get("resolution_summary", ""),
            "caveats": meta.get("resolution_caveats", []),
            "follow_ups": meta.get("resolution_follow_ups", []),
            "unresolved": meta.get("resolution_unresolved", []),
            "caveats_ack": meta.get("resolution_caveats_ack") or {},
            "follow_ups_ack": meta.get("resolution_follow_ups_ack") or {},
            "unresolved_ack": meta.get("resolution_unresolved_ack") or {},
        })

        item: Dict[str, Any] = {
            "number": number,
            "project": self.queue,
            "seq": number,
            "ref": f"{self.queue}-{number}",
            "id": f"github:{number}",
            "status": status,
            "lane": _norm_choice(meta.get("lane", "normal"), VALID_LANES, "normal"),
            "source": str(meta.get("source") or "github"),
            "note": _clip(meta.get("note") or _first_line(body) or issue.get("title", ""), 4000),
            "text": _clip(text, 24000),
            "url": str(issue.get("url") or ""),
            "title": _clip(issue.get("title", ""), 200),
            "selector": "",
            "screenshot_path": "",
            "repo_path": self.repo_path,
            "type": _effective_type(meta.get("type")),
            "readiness": _norm_choice(meta.get("readiness", ""), VALID_READINESS),
            "priority": _norm_choice(meta.get("priority", ""), VALID_PRIORITIES),
            "value": _norm_choice(meta.get("value", ""), VALID_VALUES),
            "confidence": _norm_choice(meta.get("confidence", ""), VALID_CONFIDENCES),
            "needs_input": bool(meta.get("needs_input", False)),
            "block_question": str(meta.get("block_question") or ""),
            "submitter": str(meta.get("submitter") or ""),
            "claimed_by": (
                meta.get("claimed_by")
                or (",".join(assignees) if queue_member and status == "in_progress" and assignees else None)
            ),
            "claimed_at": meta.get("claimed_at"),
            "closed_at": issue.get("closedAt") or meta.get("closed_at"),
            "claimed_session_id": meta.get("claimed_session_id"),
            "created_at": issue.get("createdAt") or _now_iso(),
            "updated_at": issue.get("updatedAt") or issue.get("createdAt") or _now_iso(),
            "github_repo": self.repo,
            "github_labels": labels,
            "github_assignees": assignees,
            "watchtower_label": self.queue_label,
            "watchtower_runnable": bool(queue_member),
            "no_auto_drain": no_auto_drain,
            "run_requested": run_requested,
            "auto_eligible": auto_eligible,
            "manual_eligible": manual_eligible,
            "work_it": work_it,
            # `claimable` predates the three-input model and is still what
            # health.queue_status and CCC read; it now means work_it, so those
            # consumers follow the new rules without knowing about them.
            "claimable": work_it,
            "_github_body": str(issue.get("body") or ""),
            # Only get() requests comments; the list path omits them, so this is
            # [] for list-produced items and the real comment array for get().
            # Owner-answer ingestion (WATCHTOWER-5) reads it to detect a human's
            # answer typed straight onto the issue.
            "_github_comments": (
                issue.get("comments")
                if isinstance(issue.get("comments"), list) else []
            ),
        }
        if status == "closed":
            item["closed_by"] = meta.get("closed_by") or item.get("claimed_by")
        if resolution:
            item["resolution"] = resolution
        history = meta.get("history")
        if isinstance(history, list):
            item["history"] = history
        return item

    def _list_probe_path(self, state: str) -> str:
        """REST path the change detector polls.

        ``sort=updated`` matters: the endpoint defaults to creation order, so
        on a repo with more than one page of open issues a comment on an old
        issue would never reach page 1 and the ETag would not move. Sorted by
        update time, *any* change -- new issue, comment, label, close --
        surfaces on the page we ask for and flips the ETag.
        """
        return (
            f"repos/{self.repo}/issues"
            f"?state={state}&per_page=100&sort=updated&direction=desc"
        )

    def _probe_list_change(
        self, state: str, etag: str
    ) -> "tuple[Optional[bool], str]":
        """Ask GitHub whether the issue list moved since ``etag``.

        Returns ``(unchanged, etag)``: True on a 304 (keep the cached list),
        False on a 200 (go fetch), None when the probe itself was unusable --
        in which case the caller falls through to an unconditional list, so the
        worst case is exactly the behaviour we had before ETags.

        THE LANDMINE: ``gh api`` **exits 1** on a 304 and prints ``gh: HTTP
        304`` to stderr. Read as a failure that would trip
        ``_LIST_ERROR_BACKOFF`` and freeze the queue on stale data for a minute
        at a time, on the poll that is *supposed* to be the cheap common case.
        So "unchanged" is decoded from the response status, never from the exit
        code.
        """
        if not self.repo:
            return None, etag
        args = ["api", "-i"]
        if etag:
            args.extend(["-H", f"If-None-Match: {etag}"])
        args.append(self._list_probe_path(state))
        try:
            proc = self._run_raw(args)
        except GitHubBackendError:
            return None, etag  # gh missing or hung: let the real fetch report it
        status = _http_status(proc.stdout)
        if status == 304 or (status is None and "HTTP 304" in (proc.stderr or "")):
            return True, etag
        if status == 200 and proc.returncode == 0:
            return False, _etag_header(proc.stdout)
        return None, etag

    def _list_issues(
        self,
        state: str = "open",
        *,
        fresh: bool = False,
        strict: bool = False,
        poller: bool = False,
    ) -> List[Dict[str, Any]]:
        """Read the issue list for ``state``.

        ``poller=True`` marks the one caller that *owns* live GitHub reads for
        this repo (``refresh_persisted_list_cache``). Everything else is a
        reader, and a reader is gated: it serves the poller's snapshot and only
        goes live when that snapshot is older than ``_persisted_list_ttl_s()``
        AND no poll is in flight. ``strict=True`` (claim/close, about to write)
        is exempt -- correctness of a write outranks quota."""
        key = f"{self.repo}:{state}"
        now = time.time()
        cached = _LIST_CACHE.get(key)
        backoff_active = False
        conn_state: Dict[str, Any] = {}
        if not strict:
            backoff_active, conn_state = _gh_backoff_active()
        # Only a 200 from the probe below sets this. Every other route to the
        # fetch stores no ETag, so the next poll re-bootstraps one rather than
        # pairing a fresh list with a validator taken at some other moment.
        etag = ""
        probed = False
        if cached is not None:
            age = now - cached["at"]
            if cached.get("error") is not None:
                if age < _LIST_ERROR_BACKOFF:
                    if cached.get("data") is not None and not strict:
                        return cached["data"]
                    raise GitHubBackendError(str(cached["error"]), cached=True)
                # Backoff expired: retry unconditionally. Deliberately no probe
                # -- the stored ETag pairs with data we already know is stale,
                # and a 304 would leave the error latched forever.
            elif (
                not fresh
                and age < _LIST_CACHE_TTL
                and not cached.get("stale")
            ):
                return cached["data"]
        if not strict and not poller and (not fresh or backoff_active):
            # The common case for every fresh CLI process (wt run, wt claim,
            # the reconciler's dispatch path): no in-process cache yet, but
            # the background poller (poll_list_caches_forever) almost
            # certainly refreshed this repo within the last few seconds.
            # Read its file instead of paying for a live `gh` call here.
            # A fresh status read normally bypasses this snapshot, except
            # during a recorded GitHub outage: cached state is safer and more
            # useful than failing the status command while retry is deferred.
            persisted = _read_persisted_list_cache().get(key)
            if persisted is not None and persisted.get("data") is not None:
                persisted_age = now - float(persisted.get("at") or 0)
                persisted_stale = bool(persisted.get("stale"))
                if (
                    persisted_age < _persisted_list_ttl_s()
                    or backoff_active
                    # Past the TTL, but the poller is fetching the replacement
                    # right now: one more second of staleness is strictly
                    # better than N lanes each paying for the same list.
                    or _poll_in_flight(self.repo)
                ):
                    # Seeding must not lose this process's own
                    # `stale_local` flag: that flag is the record that *we*
                    # wrote, and it is the one claim on freshness the snapshot
                    # can never satisfy.
                    stale_local = bool(
                        (cached or {}).get("stale_local")
                    )
                    cached = {
                        "at": persisted["at"], "data": persisted["data"],
                        "error": None,
                        "etag": str(persisted.get("etag") or ""),
                        "fetched_at": float(persisted.get("fetched_at") or 0),
                        "stale": persisted_stale or stale_local,
                        "stale_local": stale_local,
                    }
                    _LIST_CACHE[key] = cached
                    # A stale-marked snapshot is one some process's write has
                    # already superseded, so it is seeded (its validator and
                    # fetch clock are still worth inheriting) but NOT served --
                    # we fall through to the live read that read-your-own-writes
                    # requires, exactly as the old deletion forced. During a
                    # recorded outage there is no live read to fall through to,
                    # and stale data still beats failing the command (OPS-589).
                    #
                    # W4-4 adds the third case: someone *else*'s write marked it
                    # stale and a refresh is already in flight. Only the writer
                    # needs the live read; the rest can wait for the one that is
                    # already running rather than each starting their own.
                    if not stale_local and (
                        not persisted_stale
                        or backoff_active
                        or _poll_in_flight(self.repo)
                    ):
                        return persisted["data"]
        elif not strict and not poller:
            # `fresh=True`, non-strict (`wt claim`, `wt ls`, `wt status`, the
            # reconciler's dispatch path). THIS is the burst the quota
            # exhaustion was made of: ~8 concurrent Becky lanes, each a fresh
            # short-lived process, each treating "the caller asked for current
            # state" as a licence to go to GitHub itself.
            #
            # It previously only *seeded* this process's `_LIST_CACHE` from the
            # snapshot and then always fell through to the ETag probe and,
            # on a repo where the probe returns 200 on every sweep (any repo
            # with live workers), to a real fetch bounded only by the shared
            # 60s cap. Seeding fixed the uncapped case (WATCHTOWER-16 /
            # OPS-838/839) but left the fleet paying a probe per lane per
            # invocation and a heavy fetch per minute per state on top of the
            # poller's own.
            #
            # Now the snapshot is *served*, not just seeded, while it is
            # within `_persisted_list_ttl_s()`. The poller refreshes every few
            # seconds, so "fresh" in practice still means seconds old, and the
            # TTL is the explicit bound on how wrong that can get. Two
            # exemptions survive, and only two:
            #   - `stale`: a write already superseded this snapshot, and
            #     read-your-own-writes outranks quota (see _invalidate_list_cache).
            #   - past the TTL with no poll in flight: the poller is dead or
            #     wedged, and a reader fetching for itself is what keeps the
            #     queue moving instead of freezing on old data.
            persisted = _read_persisted_list_cache().get(key)
            if persisted is not None and persisted.get("data") is not None:
                persisted_age = now - float(persisted.get("at") or 0)
                persisted_stale = bool(persisted.get("stale"))
                if cached is None or cached.get("data") is None:
                    cached = {
                        "at": float(persisted.get("at") or 0),
                        "data": persisted["data"],
                        "error": None,
                        "etag": str(persisted.get("etag") or ""),
                        "fetched_at": float(persisted.get("fetched_at") or 0),
                        "stale": persisted_stale,
                    }
                    _LIST_CACHE[key] = cached
                if cached.get("stale_local"):
                    pass  # this process wrote: nothing may stand in for a live read
                elif not persisted_stale:
                    if (
                        persisted_age < _persisted_list_ttl_s()
                        or _poll_in_flight(self.repo)
                    ):
                        return persisted["data"]
                elif _poll_in_flight(self.repo):
                    # Someone else's write marked this stale and a refresh is
                    # already running. On the repo the fleet actually works in,
                    # writes land continuously and every reader saw every one of
                    # them as "go live" -- measured after the TTL gate shipped,
                    # 6 reader fetches to 0 poller fetches in one 90s window on
                    # amirfish1/BYM-Finie. Waiting out the in-flight refresh
                    # costs one fetch of extra staleness and turns N duplicate
                    # lists back into one.
                    return persisted["data"]
        # Pre-emptive GraphQL quota guard. If we're close to the hourly limit,
        # skip the expensive rich fetch (and the ETag probe that would only tell
        # us to do it) and serve whatever cached data we have. This keeps a
        # busy repo from burning the remaining quota on repeated `gh issue
        # list` calls before GitHub's reset window.
        if (
            not strict
            and cached is not None
            and cached.get("data") is not None
            # A stale entry used to be an absent entry, so this guard never saw
            # one and a post-write read raised rather than quietly returning
            # pre-write data. Keep that: running low on quota is not a reason
            # to break read-your-own-writes.
            and not cached.get("stale")
        ):
            remaining = _graphql_rate_limit_remaining()
            if remaining is not None and remaining < _GH_GRAPHQL_LOW_THRESHOLD:
                return cached["data"]

        if (
            cached is not None
            and cached.get("data") is not None
            and not strict
            and not backoff_active
            # Same reason as the guard above: a write already superseded this
            # list, so neither a 304 nor the cap may stand in for the live read.
            and not cached.get("stale")
        ):
            # Poller not running / persisted cache stale or absent: fall
            # back to the in-process revalidation this always did. Past the
            # 2s TTL nearly every poll finds an unchanged repo, and a 304
            # settles it without spending rate limit. Strict callers (claim,
            # close) skip the detour: they are about to write and pay for
            # certainty.
            probed = True
            unchanged, etag = self._probe_list_change(
                state, str(cached.get("etag") or "")
            )
            if unchanged:
                cached["at"] = now  # unchanged is as good as re-fetched
                return cached["data"]
            # Heavy-fetch rate cap. On a busy repo the probe returns 200 on
            # EVERY sweep (active workers touch issues every few seconds),
            # so the "cheap detector" bought nothing: the daemon poller was
            # paying a full GraphQL `gh issue list` (~3-12 pts) per state
            # every 5s -- ~10k pts/hr against the 5k/hr quota. Serve the
            # cached list until the last real FETCH reaches the cap age.
            # Keyed by fetched_at, not at: a 304 refreshes `at` ("unchanged
            # is as good as re-fetched"), so capping on `at` would suppress
            # fetches forever on any actively-polled repo. Quiet repos are
            # unaffected -- their last fetch is older than the cap, so a
            # genuine change still fetches on the next sweep. Strict callers
            # never cap.
            if now - float(cached.get("fetched_at") or 0) < _LIST_FETCH_MIN_INTERVAL_S:
                return cached["data"]
        if not strict:
            if backoff_active:
                if cached is not None and cached.get("data") is not None:
                    return cached["data"]
                raise GitHubBackendError(
                    str(conn_state.get("last_error") or "GitHub unreachable (backoff)"),
                    cached=True,
                )
        if not etag and not probed:
            # Every route to this fetch that skipped the probe above -- first
            # fetch of a repo, a stale entry, a retry after backoff expired --
            # used to store `etag=""` with the result, which forced the next
            # probe to go unconditional, which returned a validator the cap
            # then discarded (it can't be paired with the older list already in
            # hand). An entry could only earn a real ETag by surviving a full
            # `_LIST_FETCH_MIN_INTERVAL_S` untouched, and on the repo the fleet
            # actually works in nothing survives that long.
            #
            # So harvest one here instead. This is `gh api` -- conditional REST,
            # zero GraphQL points -- and passing no ETag guarantees a 200 with a
            # fresh validator rather than a 304 (which on the post-backoff path
            # must never be honoured anyway: it would latch the error forever).
            # It pairs with the list fetched immediately below, which is the
            # same probe-then-fetch ordering, and the same race, that the
            # normal changed-list path above already relies on.
            #
            # Gated on `not probed` as well: if the probe above already ran and
            # came back unusable, `gh` itself is broken or hung, and retrying it
            # here would only double the time we spend discovering that.
            _, etag = self._probe_list_change(state, "")
        # Claim ownership of this fetch for its duration. Readers race each
        # other, not just the poller: ~8 lanes reaching this line within the
        # same second each used to pay for the same list. Whoever gets here
        # first publishes the marker; the rest take the branches above and
        # serve the snapshot. Strict callers are exempt from *waiting* on the
        # marker but still publish one, since their fetch is just as good a
        # reason for everyone else to hold off.
        own_marker = _try_claim_poll(self.repo)
        try:
            args = [
                "issue", "list",
                *self._repo_args(),
                "--state", state,
                # Listing requests must stay shallow. Asking GitHub GraphQL
                # for nested comment bodies across the whole queue can return
                # a 503, making even an otherwise healthy queue unavailable.
                # `get()` still requests comments for one ticket at a time.
                "--json", _LIST_JSON_FIELDS,
                "--limit", str(_list_limit()),
            ]
            if state == "closed":
                cutoff = (
                    datetime.now(timezone.utc)
                    - timedelta(days=COMPLETED_ISSUE_RETENTION_DAYS)
                ).date().isoformat()
                # `sort:updated-desc` is what makes `_list_limit()` safe to
                # lower. Without an explicit sort the truncation point is
                # GitHub's relevance ranking, i.e. arbitrary; with it, the rows
                # we keep are the newest, which are exactly the ones
                # health.queue_status reads (most recent `closed_at`, and the
                # drain rate over a short window). Only the cosmetic total
                # `closed` count is affected by truncation.
                args.extend(["--search", f"closed:>={cutoff} sort:updated-desc"])
            raw = self._run(args)
            try:
                data = json.loads(raw or "[]")
            except json.JSONDecodeError as exc:
                raise GitHubBackendError("gh issue list returned invalid JSON") from exc
            if not isinstance(data, list):
                raise GitHubBackendError("gh issue list returned a non-list JSON value")
            result = [issue for issue in data if isinstance(issue, dict)]
        except GitHubBackendError as exc:
            if own_marker:
                _mark_poll_finished(self.repo)
                own_marker = False
            _record_gh_failure(str(exc))
            prev_data = cached.get("data") if cached else None
            _LIST_CACHE[key] = {
                # The stale data keeps its own validator; nothing probes with
                # it while an error is recorded, so it cannot mislead.
                # fetched_at carries over: a failed attempt is not a fetch,
                # and must not reset the rate cap's clock.
                "at": now, "data": prev_data, "error": exc,
                "etag": (cached.get("etag") or "") if cached else "",
                "fetched_at": (cached.get("fetched_at") or 0) if cached else 0,
            }
            if prev_data is not None and not strict:
                return prev_data
            raise
        if own_marker:
            _mark_poll_finished(self.repo)
        _record_gh_success()
        _log_quota(
            "list",
            repo=self.repo,
            state=state,
            rows=len(result),
            strict=strict,
            poller=poller,
            remaining=_graphql_quota_remaining_cached(),
        )
        entry = {
            "at": now, "data": result, "error": None, "etag": etag,
            "fetched_at": now, "stale": False, "stale_local": False,
        }
        _LIST_CACHE[key] = entry
        if cached is not None and cached.get("stale"):
            # This read was the one the write forced, and it just produced the
            # authoritative post-write list. Publish it, so the shared entry
            # goes from stale to current here rather than staying stale until
            # the poller notices and spends a second `gh issue list` of its own
            # -- the double fetch every claim/close used to cost the fleet.
            _write_persisted_list_entry(
                key,
                {
                    "at": now, "data": result, "etag": etag,
                    "fetched_at": now, "stale": False,
                },
            )
        return result

    def enqueue(
        self,
        *,
        note: str,
        text: str = "",
        source: str = "wt",
        annotation_id: str = "",
        url: str = "",
        title: str = "",
        selector: str = "",
        screenshot_path: str = "",
        repo_path: str = "",
        lane: str = "normal",
        item_type: str = "",
        readiness: str = "",
        priority: str = "",
        value: str = "",
        confidence: str = "",
        submitter: str = "",
    ) -> Dict[str, Any]:
        note = _clip(note, 4000)
        text = _clip(text or note, 24000)
        if not note and not text:
            raise ValueError("note or text is required")
        self._ensure_labels()
        meta = {
            "queue": self.queue,
            "annotation_id": str(annotation_id or ""),
            "source": str(source or "wt"),
            "note": note,
            "url": _clip(url, 1000),
            "selector": _clip(selector, 1000),
            "screenshot_path": str(screenshot_path or ""),
            "repo_path": str(repo_path or self.repo_path or ""),
            "lane": lane if lane in VALID_LANES else "normal",
            "type": _effective_type(item_type),
            "readiness": _norm_choice(readiness, VALID_READINESS),
            "priority": _norm_choice(priority, VALID_PRIORITIES),
            "value": _norm_choice(value, VALID_VALUES),
            "confidence": _norm_choice(confidence, VALID_CONFIDENCES),
            # Addressable filer target (WT submitter-notify design, see
            # queue._notify_ticket_event) -- stored in the issue-body metadata
            # block like every other ticket field that has no natural GitHub
            # home (labels only carry booleans/enums cheaply; a free-form
            # target string belongs in the body, same as `note`/`resolution`).
            "submitter": str(submitter or ""),
        }
        issue_title = _clip(title or note or "WatchTower ticket", 200)
        body = _body_with_metadata(text, meta)
        out = self._run([
            "issue", "create",
            *self._repo_args(),
            "--title", issue_title,
            "--body", body,
            "--label", self.queue_label,
        ])
        match = _ISSUE_URL_RE.search(out.strip())
        if not match:
            raise GitHubBackendError("gh issue create did not print an issue URL")
        return self.get(f"{self.queue}-{match.group(1)}") or {
            "number": int(match.group(1)),
            "project": self.queue,
            "seq": int(match.group(1)),
            "ref": f"{self.queue}-{match.group(1)}",
            "status": "open",
            "note": note,
            "text": text,
            "title": issue_title,
            "type": meta["type"],
            "readiness": meta["readiness"],
            "priority": meta["priority"],
            "submitter": meta["submitter"],
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
        }

    def list_items(
        self,
        status: Optional[str] = None,
        lane: Optional[str] = None,
        *,
        fresh: bool = False,
        strict: bool = False,
    ) -> List[Dict[str, Any]]:
        issues: List[Dict[str, Any]] = []
        if status != "closed":
            issues.extend(
                self._list_issues("open", fresh=fresh, strict=strict)
            )
        if status in (None, "closed"):
            issues.extend(
                issue
                for issue in self._list_issues(
                    "closed", fresh=fresh, strict=strict
                )
                if _recently_closed(issue)
            )
        items = [self._issue_to_item(issue) for issue in issues]
        if self.partition_by_label:
            # Two or more queues share this repo: each shows only its own
            # slice, because nothing but the legacy label can tell them apart.
            items = [it for it in items if it.get("watchtower_runnable")]
        if status:
            items = [it for it in items if it.get("status") == status]
        if lane:
            items = [it for it in items if it.get("lane") == lane]
        return sorted(items, key=lambda it: int(it.get("number", 0)))

    def mark_runnable(self, ident: Any) -> Optional[Dict[str, Any]]:
        """Request a run for this ticket (the ▶ / ``wt run`` path).

        This used to add the `watchtower:<QUEUE>` whitelist label. That label
        no longer admits anything, so marking runnable now sets
        ``run_requested`` — the one input that beats drain being off, the
        no-auto-drain opt-out, and the grace period. On a repo shared by
        several queues the queue label is still added, since it is what says
        which queue the ticket belongs to.
        """
        item = self.get(ident)
        if item is None:
            return None
        if item.get("status") == "closed":
            raise ValueError(f"{item.get('ref', ident)} is closed")
        self._ensure_labels()
        add_labels = ["--add-label", self.run_requested_label]
        if self.partition_by_label:
            add_labels += ["--add-label", self.queue_label]
        self._run([
            "issue", "edit", str(item["number"]),
            *self._repo_args(),
            *add_labels,
        ])
        return self.get(ident)

    def clear_run_request(self, ident: Any) -> Optional[Dict[str, Any]]:
        """Withdraw a run request by removing the ▶ label (see
        queue.clear_run_request). The queue label, if this repo is shared, says
        which queue the ticket belongs to and is left alone."""
        item = self.get(ident)
        if item is None:
            return None
        if not item.get("run_requested", False):
            return item
        self._run([
            "issue", "edit", str(item["number"]),
            *self._repo_args(),
            "--remove-label", self.run_requested_label,
        ])
        return self.get(ident)

    def get(self, ident: Any) -> Optional[Dict[str, Any]]:
        number = self._issue_number(ident)
        if number is None:
            return None
        raw = self._run([
            "issue", "view", str(number),
            *self._repo_args(),
            "--json", "number,title,body,state,url,assignees,labels,comments,createdAt,updatedAt,closedAt",
        ])
        try:
            issue = json.loads(raw or "{}")
        except json.JSONDecodeError as exc:
            raise GitHubBackendError("gh issue view returned invalid JSON") from exc
        if not isinstance(issue, dict) or not issue:
            return None
        return self._issue_to_item(issue)

    def _claim_lock_path(self) -> Path:
        """Per-repo advisory lock serializing claim_by_ref's read-then-write.

        `gh issue edit` has no compare-and-swap: two workers can both read an
        issue as open (via `self.get`) before either write lands, then both
        write `--add-label in-progress` and `claimed_by`, believing they each
        hold an exclusive claim (observed: BYM-GH-FINIE-780 claimed by two
        workers one second apart, right after a stuck-queue nudge asked
        several live workers to claim next in the same instant). Serializing
        the whole check-then-write section per repo closes that window.
        """
        safe_repo = re.sub(r"[^A-Za-z0-9_.-]", "_", self.repo) or "unknown"
        path = _claim_locks_dir() / f"{safe_repo}.lock"
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def _stop_signal_path(self, session_id: str):
        try:
            from . import workers as _workers
            stop_dir = _workers.STOP_SIGNALS_DIR
        except Exception:
            from pathlib import Path
            stop_dir = Path.home() / ".watchtower" / "stop-signals"
        return stop_dir / session_id

    def _claim_candidates(
        self,
        *,
        lane: Optional[str] = None,
        shaping: bool = False,
        oldest: bool = False,
        item_types: Optional[List[str]] = None,
        readiness_filters: Optional[List[str]] = None,
        auto_only: bool = False,
        manual_only: bool = False,
        fresh: bool = True,
    ) -> List[Dict[str, Any]]:
        """Tickets a worker could claim right now, in claim order.

        ``auto_only``/``manual_only`` narrow the eligibility test from
        ``work_it`` (what a worker would actually claim) to one of its two
        halves: ``auto_eligible`` (what the reconciler may spawn a worker *for*
        unattended) or ``manual_eligible`` (what a human pressed ▶ on). All
        three run through this one filter, over predicates derived together in
        ``_issue_to_item``, so neither half can drift out of the work_it set.

        ``fresh=True`` (the default, used by ``claim_next``/``peek_next``):
        claiming must see the current claimed/open state, not a cached
        snapshot -- otherwise two workers could both pick a ticket that was
        already claimed moments ago. ``count_claimable``/
        ``count_manual_eligible`` pass ``fresh=False``: they only answer "is
        this queue spawn-worthy", never hand out a ticket, so a snapshot up
        to the persisted-cache's staleness bound old is fine -- and letting
        them stay fresh was exactly what kept the reconciler's own depth
        checks on GitHub's critical path.
        """
        eligibility = "work_it"
        if auto_only:
            eligibility = "auto_eligible"
        elif manual_only:
            eligibility = "manual_eligible"
        candidates = [
            it for it in self.list_items(status="open", lane=lane, fresh=fresh)
            if it.get(eligibility, False)
        ]
        if readiness_filters:
            candidates = [
                it for it in candidates
                if it.get("readiness", "") in readiness_filters
            ]
        elif not shaping:
            candidates = [
                it for it in candidates
                if it.get("readiness", "") not in UNCLAIMABLE_READINESS
            ]
        if item_types:
            candidates = [
                it for it in candidates
                if _effective_type(it.get("type")) in item_types
            ]
        if oldest:
            candidates.sort(key=lambda it: int(it.get("number", 0)))
        else:
            candidates.sort(
                key=lambda it: (
                    0 if it.get("lane") == "express" else 1,
                    _prio_rank(it),
                    _type_rank(it),
                    int(it.get("number", 0)),
                )
            )
        return candidates

    def claim_next(
        self,
        session_id: str,
        *,
        lane: Optional[str] = None,
        session_uuid: str = "",
        shaping: bool = False,
        oldest: bool = False,
        item_types: Optional[List[str]] = None,
        readiness_filters: Optional[List[str]] = None,
    ) -> Optional[Dict[str, Any]]:
        if not session_id:
            raise ValueError("session_id is required")
        signal_file = self._stop_signal_path(session_id)
        has_stop_signal = signal_file.exists()
        candidates = self._claim_candidates(
            lane=lane,
            shaping=shaping,
            oldest=oldest,
            item_types=item_types,
            readiness_filters=readiness_filters,
        )
        if has_stop_signal:
            try:
                signal_file.unlink()
            except OSError:
                pass
            if not candidates:
                return {"stop": True}
        if not candidates:
            return None
        # A candidate can be stale by the time claim_by_ref re-reads it:
        # closed or claimed between the listing snapshot and the claim, or
        # served from a lagging list cache (e.g. while GraphQL quota guards
        # pin the snapshot). claim_by_ref then raises "is not open" /
        # "is not eligible" ValueErrors. Skip that entry and fall through to
        # the next candidate instead of failing the whole claim and blocking
        # the drain (OPS-841). Non-ValueError failures (gh outages, lock
        # errors) still surface.
        for candidate in candidates:
            try:
                item = self.claim_by_ref(
                    candidate["ref"], session_id, session_uuid=session_uuid
                )
            except ValueError:
                continue
            if item is not None:
                return item
        return None

    def claim_by_ref(
        self,
        ref: str,
        session_id: str,
        session_uuid: str = "",
    ) -> Optional[Dict[str, Any]]:
        if not session_id:
            raise ValueError("session_id is required")
        with _FileLock(self._claim_lock_path()):
            item = self.get(ref)
            if item is None:
                return None
            status = item.get("status", "open")
            if status != "open":
                raise ValueError(f"{ref} is not open (status={status})")
            # No whitelist to fail any more: the only way to be ineligible is
            # to be opted out, inside the grace period, or on a queue that is
            # not draining. `wt run` (run_requested) overrides all three.
            if not item.get("work_it", False):
                raise ValueError(
                    f"{ref} is not eligible to run "
                    f"(auto_drain={'on' if self.auto_drain else 'off'}, "
                    f"no-auto-drain={'yes' if item.get('no_auto_drain') else 'no'}, "
                    f"grace={self.grace_s}s); run `wt run {ref}` to work it anyway"
                )
            number = str(item["number"])
            body, meta = _split_body(item.get("_github_body") or item.get("text", ""))
            meta.update({
                "claimed_by": str(session_id),
                "claimed_at": _now_iso(),
            })
            if session_uuid:
                meta["claimed_session_id"] = str(session_uuid)
            _append_history(meta, "claim", session_id=str(session_uuid or ""), worker=str(session_id))
            self._ensure_labels()
            self._run([
                "issue", "edit", number,
                *self._repo_args(),
                "--body", _body_with_metadata(body, meta),
                "--add-assignee", self.assignee,
                "--add-label", self.in_progress_label,
            ])
            claimed = self.get(ref)
            if claimed:
                claimed["claimed_by"] = str(session_id)
                claimed["status"] = "in_progress"
            return claimed

    def update_status(
        self,
        ident: Any,
        status: str,
        session_id: str = "",
        session_uuid: str = "",
        resolution: Any = None,
        reason: str = "",
        expect_owner: str = "",
        require_status: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        if status not in ("open", "in_progress", "closed"):
            raise ValueError("status must be one of ('open', 'in_progress', 'closed')")
        if status == "in_progress":
            return self.claim_by_ref(str(ident), session_id, session_uuid=session_uuid)

        item = self.get(ident)
        if item is None:
            return None
        # Compare-and-swap guard, same contract as the local backend's
        # `require_status` (queue.update_status): a caller acting on a stale
        # snapshot must not clobber a state that moved since. This is what
        # stops `wt release` from REOPENING a ticket that was already closed
        # (OPS-854: release's require_status="in_progress" was silently
        # dropped at the backend boundary, so `gh issue reopen` ran on a
        # closed ticket and the worker had to close it twice).
        if require_status is not None and item.get("status") != require_status:
            return None
        # No queue-label guard on close: the label admitted nothing to begin
        # with, and refusing to close an issue a worker just finished because
        # of a missing label only ever stranded finished work.
        if (
            status == "closed"
            and expect_owner
            and item.get("status") == "in_progress"
            and item.get("claimed_by")
            and str(item.get("claimed_by")) != expect_owner
        ):
            raise ValueError(
                f"{item.get('ref', ident)} is claimed by {item.get('claimed_by')}; "
                f"you are {expect_owner}. Only the claiming worker may close "
                "an in-progress ticket. Pass --force to override deliberately."
            )
        number = str(item["number"])
        body, meta = _split_body(item.get("_github_body") or item.get("text", ""))

        if status == "open":
            for key in (
                "claimed_by", "claimed_at", "closed_by", "closed_at",
                "resolution_summary", "resolution_caveats",
                "resolution_follow_ups", "resolution_unresolved",
                "resolution_caveats_ack", "resolution_follow_ups_ack",
                "resolution_unresolved_ack",
                "needs_input", "block_question",
            ):
                meta.pop(key, None)
            _append_history(meta, "reopen", reason=reason)
            self._run([
                "issue", "edit", number,
                *self._repo_args(),
                "--body", _body_with_metadata(body, meta),
                "--remove-label", self.in_progress_label,
            ])
            self._run(["issue", "reopen", number, *self._repo_args()], check=False)
            return self.get(ident)

        norm = _normalize_resolution(resolution)
        now = _now_iso()
        meta["closed_at"] = now
        # A closed ticket isn't waiting on anyone: mirror the local backend
        # (queue.py update_status) so a ticket closed while blocked doesn't
        # keep showing NEEDS INPUT on the dashboard forever.
        meta["needs_input"] = False
        if session_id:
            meta["closed_by"] = str(session_id)
            # Backfill claimed_by on a never-claimed issue so attribution
            # isn't dropped when a worker closes by ref without claiming
            # first (WT-81). Never overwrites a real claimant.
            if not meta.get("claimed_by"):
                meta["claimed_by"] = str(session_id)
        if norm:
            meta["resolution_summary"] = norm.get("summary", "")
            meta["resolution_caveats"] = norm.get("caveats", [])
            meta["resolution_follow_ups"] = norm.get("follow_ups", [])
            meta["resolution_unresolved"] = norm.get("unresolved", [])
            # A (re-)close writes a fresh resolution, so acks carry over only
            # when the caller handed them back -- same as the local backend,
            # where close replaces `resolution` wholesale.
            for field in ("caveats", "follow_ups", "unresolved"):
                acks = norm.get(f"{field}_ack")
                if acks:
                    meta[f"resolution_{field}_ack"] = acks
                else:
                    meta.pop(f"resolution_{field}_ack", None)
        _append_history(meta, "close", session_id=str(session_uuid or ""),
                         worker=str(session_id or meta.get("closed_by") or ""),
                         resolution=norm)
        self._run([
            "issue", "edit", number,
            *self._repo_args(),
            "--body", _body_with_metadata(body, meta),
            "--remove-label", self.in_progress_label,
        ])
        self._run([
            "issue", "close", number,
            *self._repo_args(),
            "--comment", _resolution_comment(norm),
        ])
        closed = self.get(ident)
        if closed:
            closed["closed_by"] = str(session_id or closed.get("claimed_by") or "")
            if norm:
                closed["resolution"] = norm
        return closed

    def ack_resolution(
        self,
        ident: Any,
        targets: Any = None,
        all_items: bool = False,
        by: str = "human",
        session_id: str = "",
        undo: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """Acknowledge (or un-acknowledge) resolution warnings on an issue.

        The GitHub twin of ``queue.ack_resolution``: same index-keyed
        ``<field>_ack`` maps, same idempotence, same errors. Acks are stored
        in the issue's metadata block as ``resolution_<field>_ack`` — the same
        round-trip the resolution lists themselves already use — so acking
        edits the body only and never touches the close comment or re-fires a
        close. No issue comment is posted: an ack is bookkeeping about noise,
        and a comment per dismissed chip is exactly the noise it removes.
        """
        pairs = [] if targets is None else [(str(f), int(i)) for f, i in targets]
        for field, _idx in pairs:
            if field not in RESOLUTION_LIST_FIELDS:
                raise ValueError(
                    f"unknown resolution field {field!r}; expected one of "
                    f"{', '.join(RESOLUTION_LIST_FIELDS)}"
                )
        item = self.get(ident)
        if item is None:
            return None
        ref = item.get("ref", ident)
        body, meta = _split_body(item.get("_github_body") or item.get("text", ""))
        lists = {
            field: list(meta.get(f"resolution_{field}") or [])
            for field in RESOLUTION_LIST_FIELDS
        }
        if not any(lists.values()):
            raise ValueError(
                f"{ref} has no caveat/follow-up/unresolved items to acknowledge"
            )
        wanted = list(pairs)
        if all_items:
            wanted = [
                (field, i)
                for field in RESOLUTION_LIST_FIELDS
                for i in range(len(lists[field]))
            ]
        if not wanted:
            raise ValueError(
                "nothing selected: pass all_items=True or at least one "
                "(field, index) target"
            )
        for field, idx in wanted:
            n = len(lists[field])
            if idx < 0 or idx >= n:
                raise ValueError(
                    f"{ref} has {n} {field} item{'' if n == 1 else 's'}; "
                    f"no index {idx + 1}"
                )
        now = _now_iso()
        changed = []
        for field, idx in wanted:
            key = f"resolution_{field}_ack"
            acks = meta.get(key)
            if not isinstance(acks, dict):
                acks = {}
            if undo:
                if acks.pop(str(idx), None) is not None:
                    changed.append((field, idx))
            elif str(idx) not in acks:
                acks[str(idx)] = {"at": now, "by": _clip(str(by or "human"), 128)}
                changed.append((field, idx))
            if acks:
                meta[key] = acks
            else:
                meta.pop(key, None)
        if not changed:
            return item
        detail = ", ".join(f"{f}#{i + 1}" for f, i in changed)
        _append_history(
            meta,
            "unack" if undo else "ack",
            worker=str(by or ""),
            session_id=str(session_id or ""),
            text=detail,
        )
        self._run([
            "issue", "edit", str(item["number"]),
            *self._repo_args(),
            "--body", _body_with_metadata(body, meta),
        ])
        return self.get(ident)

    def block(
        self,
        ident: Any,
        *,
        session_id: str = "",
        question: str = "",
        progress: str = "",
    ) -> Optional[Dict[str, Any]]:
        """Record a worker's request for input on a GitHub-backed ticket."""
        item = self.get(ident)
        if item is None:
            return None
        number = str(item["number"])
        body, meta = _split_body(item.get("_github_body") or item.get("text", ""))
        now = _now_iso()
        meta["needs_input"] = True
        meta["block_question"] = _clip(question, 4000)
        meta["blocked_at"] = now
        if session_id:
            meta.setdefault("claimed_by", str(session_id))
        if progress:
            _append_history(meta, "progress", worker=str(session_id), text=_clip(progress, 24000))
        _append_history(meta, "block", worker=str(session_id), question=_clip(question, 4000))
        self._run([
            "issue", "edit", number,
            *self._repo_args(),
            "--body", _body_with_metadata(body, meta),
        ])
        return self.get(ident)

    def answer(
        self,
        ident: Any,
        text: str,
        session_id: str = "",
        *,
        post_comment: bool = True,
        source_comment_key: str = "",
    ) -> Optional[Dict[str, Any]]:
        """Record a human answer on a blocked GitHub-backed ticket and post it
        as an issue comment.

        Clears ``needs_input``. When the ticket has no resumable session, the
        claim is released so the worker pool can pick the answer back up —
        mirroring the file-backed queue's release/requeue behaviour.

        ``post_comment=False`` skips echoing the answer back as a new issue
        comment — used by owner-answer ingestion (WATCHTOWER-5), where the
        answer text IS an existing owner comment and re-posting it would
        duplicate the discussion. ``source_comment_key`` records that comment's
        idempotency key in metadata, in the same atomic body edit, so the same
        comment can never be ingested twice.
        """
        item = self.get(ident)
        if item is None:
            return None
        number = str(item["number"])
        body, meta = _split_body(item.get("_github_body") or item.get("text", ""))
        now = _now_iso()
        answer_text = _clip(text, 24000)
        meta["needs_input"] = False
        meta["answered_at"] = now
        if source_comment_key:
            keys = meta.get("ingested_answer_keys")
            if not isinstance(keys, list):
                keys = []
            if source_comment_key not in keys:
                keys.append(source_comment_key)
            meta["ingested_answer_keys"] = keys[-50:]
        _append_history(
            meta,
            "answer",
            kind="human",
            worker=str(session_id or ""),
            text=answer_text,
            source="github-owner-comment" if source_comment_key else "",
        )
        releasing = bool(
            item.get("status") == "in_progress"
            and not item.get("claimed_session_id")
        )
        if releasing:
            for key in (
                "claimed_by", "claimed_at", "claimed_session_id",
                "block_question", "blocked_at",
            ):
                meta.pop(key, None)
            _append_history(
                meta,
                "reopen",
                kind="human",
                worker=str(session_id or ""),
                reason="answered_without_resumable_session",
            )
        self._run([
            "issue", "edit", number,
            *self._repo_args(),
            "--body", _body_with_metadata(body, meta),
        ])
        if releasing:
            self._run([
                "issue", "edit", number,
                *self._repo_args(),
                "--remove-label", self.in_progress_label,
            ])
            self._run(["issue", "reopen", number, *self._repo_args()], check=False)
        # Post the answer as a real GitHub issue comment so it is visible in
        # the issue discussion and the worker session can resume from it. Skipped
        # for owner-answer ingestion, where the answer already IS an issue
        # comment and re-posting it would duplicate the discussion.
        if post_comment:
            self._run([
                "issue", "comment", number,
                *self._repo_args(),
                "--body", answer_text,
            ])
        return self.get(ident)

    def _repo_owner(self) -> str:
        """The repo owner login (``amirfish1`` for ``amirfish1/BYM-Finie``).

        This is the identity authorized to answer a blocked ticket by commenting
        straight on the issue. Empty when the repo string is not ``owner/name``.
        """
        repo = self.repo or ""
        return repo.split("/", 1)[0].strip() if "/" in repo else ""

    def ingest_owner_answer(self, ident: Any) -> Optional[Dict[str, Any]]:
        """Ingest an owner-authored answer comment on a blocked ticket, once.

        The gap this closes (WATCHTOWER-5): a repo owner who answers a blocked
        ticket by commenting straight on the GitHub issue (rather than via
        ``wt answer``) was never picked up — the ticket stayed blocked under an
        idle worker with no progress. This detects such a comment and routes it
        through :meth:`answer`, which clears ``needs_input`` and resumes or
        requeues exactly as ``wt answer`` does.

        Detection is deliberately conservative: the comment author must be the
        repo owner AND the body must carry the ``OWNER ANSWER`` marker AND (when
        timestamps are available) post-date the block. Idempotent: the source
        comment's key is recorded in metadata, so re-polling can never clear the
        block twice. Returns the updated item when it ingested one, else None.
        """
        owner = self._repo_owner()
        if not owner:
            return None
        item = self.get(ident)  # get() is the only path that fetches comments
        if item is None or not item.get("needs_input"):
            return None
        _body, meta = _split_body(item.get("_github_body") or "")
        blocked_at = str(meta.get("blocked_at") or "")
        already = set(meta.get("ingested_answer_keys") or [])
        owner_l = owner.lower()

        def _is_owner_answer(c: Dict[str, Any]) -> bool:
            author = str((c.get("author") or {}).get("login") or "").lower()
            if author != owner_l:
                return False
            if _OWNER_ANSWER_MARKER not in str(c.get("body") or "").upper():
                return False
            created = str(c.get("createdAt") or "")
            # A comment from before the block can't be answering it. Only
            # enforce this when both timestamps are present.
            if blocked_at and created and created < blocked_at:
                return False
            return True

        comments = item.get("_github_comments") or []
        candidates = [
            c for c in comments
            if _is_owner_answer(c) and _owner_answer_comment_key(c) not in already
        ]
        if not candidates:
            return None
        # If the owner posted more than one answer, the most recent wins.
        chosen = max(candidates, key=lambda c: str(c.get("createdAt") or ""))
        key = _owner_answer_comment_key(chosen)
        updated = self.answer(
            item.get("ref") or ident,
            str(chosen.get("body") or ""),
            session_id="",
            post_comment=False,
            source_comment_key=key,
        )
        # Human-visible confirmation on the issue itself, distinguishing the two
        # outcomes the ticket asked to surface. Best-effort: a failed ack must
        # not undo the ingestion (needs_input is already cleared).
        requeued = bool(updated and updated.get("status") == "open")
        outcome = (
            "requeued for a fresh worker (no live session was attached)"
            if requeued else
            "cleared the block so the claimed worker can resume"
        )
        self._run([
            "issue", "comment", str(item["number"]),
            *self._repo_args(),
            "--body", f"[watchtower] Ingested owner answer ({key}); {outcome}.",
        ], check=False)
        return updated

    def comment(
        self,
        ident: Any,
        text: str,
        by: str = "human",
        session_id: str = "",
    ) -> Optional[Dict[str, Any]]:
        """Append a plain activity comment to a GitHub-backed ticket.

        The comment is posted as a GitHub issue comment and recorded in the
        issue-body metadata history so the ticket timeline stays complete.
        """
        item = self.get(ident)
        if item is None:
            return None
        number = str(item["number"])
        body, meta = _split_body(item.get("_github_body") or item.get("text", ""))
        actor_kind = by if by in ("worker", "human", "system") else "human"
        comment_text = _clip(text, 24000)
        _append_history(
            meta,
            "comment",
            kind=actor_kind,
            worker=str(session_id or ""),
            text=comment_text,
        )
        self._run([
            "issue", "edit", number,
            *self._repo_args(),
            "--body", _body_with_metadata(body, meta),
        ])
        self._run([
            "issue", "comment", number,
            *self._repo_args(),
            "--body", comment_text,
        ])
        return self.get(ident)

    def update(self, ident: Any, **fields: Any) -> Optional[Dict[str, Any]]:
        item = self.get(ident)
        if item is None:
            return None
        body, meta = _split_body(item.get("_github_body") or item.get("text", ""))
        title = item.get("title", "")
        for key, value in fields.items():
            if key == "title":
                title = _clip(value, 200)
            elif key == "text":
                body = _clip(value, 24000)
            elif key == "item_type":
                meta["type"] = _effective_type(value)
            elif key == "type":
                meta["type"] = _effective_type(value)
            elif key in {
                "readiness", "priority", "value", "confidence", "note",
                "url", "selector", "screenshot_path", "repo_path",
                "needs_input", "block_question",
            }:
                meta[key] = value
        self._run([
            "issue", "edit", str(item["number"]),
            *self._repo_args(),
            "--title", title,
            "--body", _body_with_metadata(body, meta),
        ])
        return self.get(ident)

    def peek_next(
        self,
        *,
        lane: Optional[str] = None,
        item_types: Optional[List[str]] = None,
    ) -> Optional[Dict[str, Any]]:
        candidates = self._claim_candidates(lane=lane, item_types=item_types)
        return dict(candidates[0]) if candidates else None

    def count_claimable(
        self,
        *,
        lane: Optional[str] = None,
        item_types: Optional[List[str]] = None,
    ) -> int:
        """How many tickets auto-drain would currently pick from — the
        reconciler's single source of truth for spawn-worthy depth on a
        GitHub-backed queue (see queue.count_claimable).

        Deliberately narrower than claim_next()'s candidate set: a ticket that
        is only workable because a human pressed ▶ must not, by itself, make
        the reconciler decide this queue wants unattended workers.

        ``fresh=False``: this only decides spawn-worthiness, never hands out
        a ticket, so it reads the persisted list cache instead of blocking
        the reconciler on a live `gh` call (see ``_claim_candidates``)."""
        return len(
            self._claim_candidates(
                lane=lane, item_types=item_types, auto_only=True, fresh=False
            )
        )

    def count_manual_eligible(
        self,
        *,
        lane: Optional[str] = None,
        item_types: Optional[List[str]] = None,
    ) -> int:
        """How many claimable tickets carry a run request (see
        queue.count_manual_eligible) — the depth the reconciler staffs even
        when this queue is not auto-draining. ``fresh=False`` for the same
        reason as ``count_claimable``."""
        return len(
            self._claim_candidates(
                lane=lane, item_types=item_types, manual_only=True, fresh=False
            )
        )

    def last_progress_iso(self) -> Optional[str]:
        latest: Optional[str] = None
        for it in self.list_items(status="closed"):
            closed_at = it.get("closed_at")
            if closed_at and (latest is None or closed_at > latest):
                latest = closed_at
        return latest

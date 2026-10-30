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
import contextlib
import copy
import functools
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .config import CATCH_ALL_LABEL
from .queue import (
    UNCLAIMABLE_READINESS, _FileLock, _refs_index, _validate_blocked_by,
    blocker_verdict, machine_tag,
)

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
    "number,title,body,state,url,assignees,labels,author,createdAt,updatedAt,closedAt"
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
# `wt edit` fields the issue metadata never round-trips (OPS-201).
_LOCAL_ONLY_EDIT_FIELDS = frozenset({
    "gates", "accept", "planner_model", "verifier_model", "model_floor", "placement",
})
# Old close metadata sometimes embedded terminal output as JSON without
# escaping its literal backslashes (for example ``\^[[0m``).  Preserve the
# original terminal text by escaping only sequences JSON cannot decode.
_LEGACY_INVALID_JSON_ESCAPE_RE = re.compile(r'\\(?!["\\/bfnrt]|u[0-9a-fA-F]{4})')


# Per-thread set of repo lock paths already held, so a locked writer may call
# another (update_status -> claim_by_ref, ingest_owner_answer -> answer)
# without flock deadlocking on its own second open of the same file.
_BODY_LOCKS_HELD = threading.local()


def _serialized_body_write(method):
    """Run a read-modify-write of the issue body under the per-repo lock.

    Every writer reads the body, edits its metadata block and writes the
    whole body back; `gh issue edit` has no compare-and-swap, so two
    unserialized writers lose one update. Observed on BECKY-2745: a `wt
    comment` read the body just before a claim landed and wrote it back a
    second later, erasing `claimed_by`; the read then fell back to the
    GitHub assignee and `wt close` rejected the claiming worker (OPS-106).
    """
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        with self._body_lock():
            return method(self, *args, **kwargs)
    return wrapper


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
        # Every repo's poll rewrites this one file, so most re-parses leave a
        # given repo's list unchanged. Keep that list the same object: the
        # list_items memo keys on its identity (WT-16).
        previous = _PERSISTED_LIST_MEM_CACHE["data"]
        for key, entry in data.items():
            old = previous.get(key)
            if (
                isinstance(entry, dict)
                and isinstance(old, dict)
                and entry.get("data") is not None
                and entry.get("data") == old.get("data")
            ):
                entry["data"] = old["data"]
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
    # Metering must not become part of the burn it measures. A `force=True`
    # snapshot on each side of every repo's poll is 2 `gh` subprocesses per
    # repo per sweep -- at three repos on a 5s loop that was ~4,300 extra
    # `gh api graphql` invocations an hour, and it made `gh api graphql
    # -f query={rateLimit...}` the single most frequent gh command on the
    # machine. (The points cost of a rateLimit-only query is ~0 -- measured:
    # 20 back-to-back queries moved `used` by less than the idle drift over
    # the same interval -- but the process churn and the added latency on
    # every poll are real.)
    #
    # So: reuse the previous sweep's closing snapshot as this one's opening
    # reading, and only take a fresh closing reading when this poll actually
    # fetched something. A sweep that was answered entirely by 304s has no
    # cost to report, which is most sweeps on a quiet repo.
    before = _GH_POLL_LAST_SNAPSHOT.get(repo) or _graphql_quota_snapshot()
    fetches_before = _LIST_FETCH_COUNT["n"]
    _mark_poll_started(repo)
    try:
        _refresh_persisted_list_cache_states(inst, repo)
    finally:
        _mark_poll_finished(repo)
    if _LIST_FETCH_COUNT["n"] == fetches_before:
        return  # nothing was fetched, so there is nothing to account for
    after = _graphql_quota_snapshot(force=True)
    if after is not None:
        _GH_POLL_LAST_SNAPSHOT[repo] = after
    _log_quota(
        "poll",
        repo=repo,
        fetches=_LIST_FETCH_COUNT["n"] - fetches_before,
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
            parked = [it for it in backend.list_items(status="awaiting_answer")   # WT-40
                      if it.get("parked")]
            for item in backend.list_items(status="in_progress") + parked:
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
# Closing quota reading of each repo's last metered poll, reused as the
# opening reading of its next one so a sweep costs one meter read, not two.
# `cost` is therefore a *window* delta: on a busy machine it includes whatever
# else spent GraphQL points during the same interval, which is the honest
# thing for an operator to see -- the quota is per-account, not per-process.
_GH_POLL_LAST_SNAPSHOT: Dict[str, Dict[str, int]] = {}
# Count of heavy `gh issue list` fetches this process has actually paid for.
_LIST_FETCH_COUNT: Dict[str, int] = {"n": 0}
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


# Observed-evidence auth-failure self-heal + hold (2026-09-17 incident). Five
# GitHub-backed queues ERRORed "To get started with GitHub CLI, please run:
# gh auth login" in bursts while an interactive `gh auth status` in a normal
# shell was fine -- hosts.yml on disk was valid before and after, so the
# daemon's reads were transient (e.g. hosts.yml mid-rewrite by another gh
# process) or masked by a stale GH_TOKEN/GITHUB_TOKEN in the daemon's
# supervisor environment. Each failure cost one ERROR line per queue per poll
# in the activity log, which surfaces as alert banners in dashboards.
#
# Two defences, in order:
# 1. Self-heal: on an auth-signature failure, re-run `gh auth status`. If
#    stored auth reads fine now, the failure was transient -- retry the
#    original command once. If it fails only under the current env while a
#    valid oauth_token sits in hosts.yml, scrub GH_TOKEN/GITHUB_TOKEN from
#    the child env (an env token outranks stored auth in gh, so a bad one
#    masks a good one) and latch that scrub for every later gh call in this
#    process.
# 2. Hold: when the heal also fails, set `auth_broken_until` -- like the
#    rate-limit hold, this stops every queue from ERRORing every poll, and it
#    deliberately does not escalate the unreachability ladder: GitHub is
#    reachable, auth is just broken. `_record_gh_success` clears the hold, so
#    once auth is repaired out of band the next live success un-breaks
#    polling on its own.
_GH_AUTH_HOLD_S = 900.0
_AUTH_ERROR_RE = re.compile(
    r"To get started with GitHub CLI|gh auth login"
    r"|authentication required|Bad credentials",
    re.IGNORECASE,
)
# Latched once a token-scrubbed environment is proven to fix gh; consulted by
# `_gh_child_env` for every later call in this process. Re-probing per call
# would re-fail first, so the proof is remembered, not re-derived.
_GH_SCRUB_ENV = False


def _is_auth_error(error: Any) -> bool:
    """Whether a gh error message is gh reporting it has no usable auth."""
    return bool(_AUTH_ERROR_RE.search(str(error or "")))


class _Text(str):
    """A free-text argv value (an issue title). gh receives it verbatim; error
    and log rendering print only its length, never its contents (WT-40)."""
    __slots__ = ()


# `gh issue <verb>` commands that take a body (`--body-file -`, read from stdin).
_BODY_VERBS = frozenset({"create", "edit", "comment"})


def _render_argv(args: List[str]) -> str:
    """``args`` for an error message, with every ``_Text`` value elided.
    Bodies never reach argv, so nothing else here is free text."""
    return " ".join(
        f"<text:{len(a)} chars>" if isinstance(a, _Text) else str(a) for a in args
    )


def _body_argv(args: List[str]) -> List[str]:
    """``args`` with ``--body-file -`` placed right after ``issue <verb>``.

    Only those two fixed tokens precede it, so no caller value can consume
    it as an option argument or end options before it, and no caller token
    is ever inspected: a title or label of ``--body``, ``-F`` or ``--``
    reaches gh exactly as given (WT-40).
    """
    if len(args) < 2 or args[0] != "issue" or args[1] not in _BODY_VERBS:
        raise GitHubBackendError(
            "gh body transport supports only `issue create|edit|comment`"
        )
    return [args[0], args[1], "--body-file", "-", *args[2:]]


def _gh_hosts_file() -> Path:
    env = os.environ.get("GH_CONFIG_DIR")
    if env:
        return Path(env).expanduser() / "hosts.yml"
    return Path.home() / ".config" / "gh" / "hosts.yml"


def _gh_child_env() -> Dict[str, str]:
    """Environment for gh subprocesses: the process env, minus the token
    variables once a scrub has been proven to unmask good stored auth."""
    env = dict(os.environ)
    if _GH_SCRUB_ENV:
        env.pop("GH_TOKEN", None)
        env.pop("GITHUB_TOKEN", None)
    return env


def _attempt_auth_self_heal() -> bool:
    """Best-effort recovery from an auth-signature gh failure. Never raises.

    Only ever called from `_run` on an actual auth-signature failure -- it
    spawns subprocesses and must never sit on a hot path. Returns True when
    gh's auth turns out to be usable after all, in which case the caller
    retries the original command once:

    a. `gh auth status` succeeds under the current env -- the failure was a
       transient read (hosts.yml mid-rewrite by another gh process).
    b. It fails, but GH_TOKEN/GITHUB_TOKEN is set while hosts.yml holds an
       oauth_token -- the env token may be masking good stored auth, so
       re-check with those scrubbed. If that passes, latch the scrub for
       every later gh call in this process (the retry included).
    """
    global _GH_SCRUB_ENV
    try:
        probe = subprocess.run(
            ["gh", "auth", "status"],
            capture_output=True,
            text=True,
            timeout=15,
            env=_gh_child_env(),
        )
        if probe.returncode == 0:
            return True
        if not (os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")):
            return False
        try:
            hosts = _gh_hosts_file().read_text()
        except OSError:
            return False
        if "oauth_token" not in hosts:
            return False
        env = dict(os.environ)
        env.pop("GH_TOKEN", None)
        env.pop("GITHUB_TOKEN", None)
        probe = subprocess.run(
            ["gh", "auth", "status"],
            capture_output=True,
            text=True,
            timeout=15,
            env=env,
        )
        if probe.returncode == 0:
            _GH_SCRUB_ENV = True
            return True
    except Exception:
        pass
    return False


def _record_gh_auth_broken(error: str) -> None:
    """Record a confirmed auth outage and hold live gh calls off.

    Modelled on ``_record_gh_rate_limited``: sets the hold without escalating
    the unreachability ladder -- GitHub is reachable, auth is just broken --
    so one bad token stops every queue from ERRORing every poll while the
    account waits on a human (`gh auth login`). `_record_gh_success` clears
    the hold, which is what makes the outage auto-fixed once auth recovers.
    """
    state = _load_connectivity()
    hold_until = datetime.now(timezone.utc) + timedelta(seconds=_GH_AUTH_HOLD_S)
    state["auth_broken_until"] = hold_until.strftime("%Y-%m-%dT%H:%M:%SZ")
    state["last_error"] = str(error)
    if state.get("broken_since") is None:
        state["broken_since"] = _now_iso()
    existing = _parse_iso(state.get("next_retry_at"))
    if existing is None or existing < hold_until:
        state["next_retry_at"] = state["auth_broken_until"]
    _save_connectivity(state)


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
        "auth_broken_until": None,
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
    # Same proof for auth: a live success means whatever gh complained about
    # (transient read, masked stored auth, since-repaired token) is over.
    state["auth_broken_until"] = None
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
    auth_broken_until = _parse_iso(state.get("auth_broken_until"))
    active = (
        (next_retry is not None and now < next_retry)
        or (rate_limited_until is not None and now < rate_limited_until)
        or (auth_broken_until is not None and now < auth_broken_until)
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


_CLAIM_META_KEYS = frozenset({
    "claimed_by", "claimed_machine", "claimed_at", "claimed_session_id",
    "claim_heartbeat",
})


def _claim_predates_close(meta: Dict[str, Any]) -> bool:
    """True when ``meta`` records a close no later claim followed. Every claim
    stamps a fresh ``claimed_at``; close keeps the claimant for attribution."""
    if not meta.get("claimed_by"):
        return False
    closed = _parse_iso(meta.get("closed_at"))
    if closed is None:
        return False
    claimed = _parse_iso(meta.get("claimed_at"))
    return claimed is None or claimed <= closed


def _defer_live(until: Any) -> bool:
    parsed = _parse_iso(until) if until else None
    return parsed is not None and parsed.timestamp() > time.time()


# OPS-215: while a gated ticket is plan-pending or in review, its issue carries
# this defer window. Every wt since OPS-100 keeps a deferred ticket out of
# claim_next / claim_by_ref / `wt run`, so a host that cannot read the pipeline
# (older than OPS-201, or no sidecar) is fenced by the issue alone. A wt that
# knows ``pipeline_hold`` reads the human defer it saved instead and lets the
# sidecar overlay decide the hold.
PIPELINE_HOLD_UNTIL = "2099-12-31T00:00:00Z"


def _effective_defer(meta: Dict[str, Any]) -> str:
    until = str(meta.get("deferred_until") or "")
    hold = meta.get("pipeline_hold")
    if isinstance(hold, dict) and until == PIPELINE_HOLD_UNTIL:
        return str(hold.get("prev_defer") or "")
    return until


def _set_pipeline_hold(meta: Dict[str, Any], why: str) -> None:
    """Write (``why`` = plan|review) or lift the issue-level pipeline hold. A
    human defer is kept in the hold and restored when it lifts; one set during
    the hold (it replaced the sentinel) is re-saved on the next write."""
    hold = meta.get("pipeline_hold")
    if why:
        if not isinstance(hold, dict) or meta.get("deferred_until") != PIPELINE_HOLD_UNTIL:
            hold = {"prev_defer": str(meta.get("deferred_until") or ""),
                    "prev_reason": str(meta.get("defer_reason") or "")}
        meta["pipeline_hold"] = dict(hold, why=why)
        meta["deferred_until"] = PIPELINE_HOLD_UNTIL
        meta["defer_reason"] = (
            f"duplicate hold: it completes with {meta.get('duplicate_of') or 'its original'}"
            if why == "duplicate" else
            f"pipeline hold ({why}): it settles on the host that "
            f"owns its gates; a wt without that pipeline must not claim it")
        return
    if not isinstance(hold, dict):
        return
    meta.pop("pipeline_hold", None)
    if meta.get("deferred_until") == PIPELINE_HOLD_UNTIL:
        if hold.get("prev_defer"):
            meta["deferred_until"] = hold["prev_defer"]
            meta["defer_reason"] = hold.get("prev_reason") or ""
        else:
            meta.pop("deferred_until", None)
            meta.pop("defer_reason", None)


_DEFER_MARKER_RE = re.compile(r'^deferred_until:\s*"([^"]+)"', re.MULTILINE)


def _defer_marker_live(body: Any) -> bool:
    """Cheap pre-parse check for a live defer window in an issue body."""
    m = _DEFER_MARKER_RE.search(str(body or ""))
    return bool(m) and _defer_live(m.group(1))


def _age_seconds(created_at: Any, *, now: Optional[datetime] = None) -> float:
    """Seconds since ``created_at``. An unparseable/missing timestamp reads as
    infinitely old: the grace period exists to protect *new* issues, and a
    ticket we cannot date is not new evidence."""
    created = _parse_iso(created_at)
    if created is None:
        return float("inf")
    return ((now or datetime.now(timezone.utc)) - created).total_seconds()


# Upper bound on reusing a converted list_items result; the real bounds are
# the raw-list identity, the ledger dir stamp and each issue's time boundary.
_LIST_ITEMS_MEMO_TTL_S = 60.0


def _copy_listed_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """Copy one listed item the way _issue_to_item's memo hit does: shallow,
    except the per-caller usage projection."""
    from . import usage
    out = dict(item)
    if "token_usage" in out:
        out["token_usage"] = usage.copy_projection(out["token_usage"])
    return out


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
    # Worker lifecycle events carry the recording machine's tag (queue.py's
    # _append_history stamps by.machine on the file backend); human-kind
    # events (answers, human reopens) don't claim a machine.
    if entry.get("worker") and entry.get("kind") != "human" and "machine" not in entry:
        tag = machine_tag()
        if tag:
            entry["machine"] = tag
    hist.append(entry)
    meta["history"] = hist


# Issue-body capacity (WT-44). GitHub rejects an issue body past 262,144 bytes
# with "GraphQL: Body is too long (updateIssue)": BECKY-2966's 256,520-byte body
# took a short answer and refused a 6.5 KB one, 248 KB of it answer/comment
# history carrying each text in full. Those texts are also posted as issue
# comments, so the history keeps a bounded excerpt plus the comment's
# reference and the full text's sha256/length; the comment is the durable copy.
_ISSUE_BODY_MAX_BYTES = 262144
# Writes stop short of the hard cap: a concurrent writer's re-apply
# (_merge_body_write) appends its own entries on top of ours.
_ISSUE_BODY_BUDGET_BYTES = 250000
_COMMENT_MAX_CHARS = 65536
_HISTORY_TEXT_INLINE_MAX = 1000
_COMMENTED_EVENTS = ("answer", "comment")
_ENTRY_MARKER = "\n\n<!-- watchtower-entry: {} -->"
_ENTRY_MARKER_RE = re.compile(r"\n\n<!-- watchtower-entry: ([0-9a-f]{16}) -->\s*$")
_TEXT_REF_KEYS = ("text", "text_len", "text_sha256", "text_ref")


def _text_sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _body_bytes(raw: str) -> int:
    return len(raw.encode("utf-8"))


def _strip_entry_marker(body: str) -> "tuple[str, str]":
    """(comment text without its WT-44 entry marker, marker key or "")."""
    match = _ENTRY_MARKER_RE.search(body or "")
    if not match:
        return body or "", ""
    return body[:match.start()], match.group(1)


def _comment_ref(comment: Dict[str, Any]) -> str:
    return str(comment.get("url") or comment.get("id") or "").strip()


def _entry_key(number: str, event: str, meta: Dict[str, Any], text: str) -> str:
    """Idempotency key for one answer/comment write, recorded on its history
    entry as ``entry_key``. It depends only on this write (issue, event,
    block generation, text) and on how many entries already carry the same
    write, never on unrelated history: a retry after a failed body edit gets
    the same key even if other entries landed in between, finds the comment
    the failed attempt posted and adopts it; once the entry lands, the same
    words again get the next key and a comment of their own."""
    raw = f"{number}|{event}|{meta.get('blocked_at') or ''}|{text}"
    base = _text_sha256(raw)[:12]
    hist = meta.get("history") if isinstance(meta.get("history"), list) else []
    seen = sum(1 for e in hist
               if isinstance(e, dict) and str(e.get("entry_key") or "").startswith(base))
    return f"{base}{seen:04x}"


def _referenced_text(entry: Dict[str, Any], text: str, ref: str) -> None:
    """Store ``text`` on a history entry by reference when it is long: the
    excerpt says how long the full text is and where it lives, and the
    sha256 lets a reader verify the comment it points at."""
    if not text:
        return
    if len(text) <= _HISTORY_TEXT_INLINE_MAX or not ref:
        entry["text"] = text
        return
    head = text[:_HISTORY_TEXT_INLINE_MAX].rstrip()
    entry["text"] = f"{head} ... [{len(text)} chars; full text: {ref}]"
    entry["text_len"] = len(text)
    entry["text_sha256"] = _text_sha256(text)
    entry["text_ref"] = ref


def _history_id(entry: Any) -> str:
    """Identity of a history entry that survives `_referenced_text`: the same
    event before and after its text moved to a comment compares equal."""
    if not isinstance(entry, dict):
        return json.dumps(entry, sort_keys=True, default=str)
    digest = entry.get("text_sha256") or (
        _text_sha256(str(entry["text"])) if "text" in entry else "")
    rest = {k: v for k, v in entry.items() if k not in _TEXT_REF_KEYS}
    return json.dumps([rest, digest], sort_keys=True, default=str)


def _compact_history(meta: Dict[str, Any], comments: List[Dict[str, Any]],
                     human_body: str) -> int:
    """Move long answer/comment texts already posted as issue comments out of
    the body, oldest first, until it fits the budget. Lossless: an entry moves
    only when an issue comment holds exactly its text. Returns entries moved."""
    hist = meta.get("history")
    if not isinstance(hist, list):
        return 0
    by_text: Dict[str, str] = {}
    for c in comments or []:
        if not isinstance(c, dict):
            continue
        text, _key = _strip_entry_marker(str(c.get("body") or ""))
        ref = _comment_ref(c)
        if ref:
            by_text.setdefault(text, ref)
    moved = 0
    size = _body_bytes(_body_with_metadata(human_body, meta))
    for i, entry in enumerate(hist):
        if size <= _ISSUE_BODY_BUDGET_BYTES:
            break
        if (not isinstance(entry, dict) or entry.get("event") not in _COMMENTED_EVENTS
                or entry.get("text_ref")):
            continue
        text = str(entry.get("text") or "")
        ref = by_text.get(text)  # exact: a comment that differs at all is not this text
        if len(text) <= _HISTORY_TEXT_INLINE_MAX or not ref:
            continue
        compact = {k: v for k, v in entry.items() if k != "text"}
        _referenced_text(compact, text, ref)
        # The metadata line is ASCII JSON (ensure_ascii), so the entry's
        # serialized length change is the body's byte change.
        size -= len(json.dumps(entry)) - len(json.dumps(compact))
        hist[i] = compact
        moved += 1
    return moved


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
    # The verified fixing commit. Kept so the issue itself records WHICH commit
    # closed it: downstream consumers (BYM's What's New carousel) gate on it
    # being deployed, and a close comment without it left them nothing to check.
    commit = _clip(resolution.get("commit", ""), 128)
    if commit:
        out["commit"] = commit
    # session_id/engine/transcript_path/machine attribute the close to the
    # session and node that did the work. Mirrors queue._normalize_resolution.
    for field, max_len in (
        ("session_id", 128),
        ("engine", 32),
        ("transcript_path", 1024),
        ("machine", 16),
    ):
        val = _clip(resolution.get(field, ""), max_len)
        if val:
            out[field] = val
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
    if resolution.get("commit"):
        # Machine-readable, one per close comment; the same value is stored as
        # ``resolution_commit`` in the issue's metadata block.
        lines.append(f"Fix-Commit: {resolution['commit']}")
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


_SPLIT_BODY_MEMO: Dict[str, tuple] = {}
_SPLIT_BODY_MEMO_MAX = 4096


def _split_body_memo(body: str) -> tuple[str, Dict[str, Any]]:
    body = body or ""
    hit = _SPLIT_BODY_MEMO.get(body)
    if hit is None:
        if len(_SPLIT_BODY_MEMO) >= _SPLIT_BODY_MEMO_MAX:
            _SPLIT_BODY_MEMO.clear()
        _SPLIT_BODY_MEMO[body] = hit = _split_body_uncached(body)
    return hit


def _split_body(body: str) -> tuple[str, Dict[str, Any]]:
    # A reconcile tick converts every issue once per queue; the metadata parse
    # is pure in the body text, so reuse it (callers get their own copy).
    human, meta = _split_body_memo(body)
    return human, copy.deepcopy(meta)


def _split_body_shared(body: str) -> tuple[str, Dict[str, Any]]:
    """Like _split_body but hands back the memoized meta itself: read-only."""
    return _split_body_memo(body)


def _split_body_uncached(body: str) -> tuple[str, Dict[str, Any]]:
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
            # GitHub still has malformed legacy values.  First repair only
            # illegal JSON escapes from pasted terminal output; separately
            # support the earliest writer's superfluous quote escapes.  Leave
            # genuinely non-JSON metadata as its original text.
            repaired = _LEGACY_INVALID_JSON_ESCAPE_RE.sub(r"\\\\", value)
            candidates = [repaired]
            if r'\"' in repaired:
                candidates.append(repaired.replace(r'\"', '"'))
            for candidate in candidates:
                try:
                    meta[key] = json.loads(candidate)
                    break
                except json.JSONDecodeError:
                    continue
            else:
                meta[key] = value
    return human, meta


def _body_with_metadata(human_body: str, meta: Dict[str, Any]) -> str:
    human_body = (human_body or "").rstrip()
    if human_body:
        return f"{human_body}\n\n{_meta_block(meta)}"
    return _meta_block(meta)


_UNSET = object()
# Re-read/re-apply rounds after a body write (WT-20); each costs one view.
_BODY_WRITE_RETRIES = 2


def _log_body_reapply(repo: str, number: str, conflicts: List[str]) -> None:
    try:
        from . import queue as _q
        _q._log("GH-REAPPLY", f"{repo}#{number} concurrent body write dropped our change; "
                f"re-applied" + (f" (theirs kept: {', '.join(conflicts)})" if conflicts else ""))
    except Exception:  # noqa: BLE001 - logging never breaks a write
        pass


def _merge_body_write(base_raw: str, ours_raw: str,
                      fresh_raw: str) -> "tuple[Optional[str], List[str]]":
    """Three-way merge of our body write into the body GitHub now holds (WT-20).

    ``gh issue edit`` has no compare-and-swap, and the per-repo flock only
    serializes writers on one host: a writer on another host that read the
    body before our edit can write it back a moment later, silently erasing
    our change. Re-apply only what *we* changed (base -> ours) and only where
    the fresh body still carries the base value; a key the other writer also
    changed is theirs and is reported as a conflict; a ``claimed_by``
    conflict re-applies nothing. History merges by appending our new
    entries. Returns (body to rewrite, or None when fresh
    already carries every change of ours; conflicting keys)."""
    base_h, base_m = _split_body_shared(base_raw)
    ours_h, ours_m = _split_body_shared(ours_raw)
    fresh_h, fresh_m = _split_body(fresh_raw)
    merged_h, merged_m = fresh_h, fresh_m
    conflicts: List[str] = []
    dirty = False
    for key in list(base_m) + [k for k in ours_m if k not in base_m]:
        b, o = base_m.get(key, _UNSET), ours_m.get(key, _UNSET)
        f = fresh_m.get(key, _UNSET)
        if b == o or f == o:
            continue
        if key == "history" and isinstance(o, list):
            have = f if isinstance(f, list) else []
            old = b if isinstance(b, list) else []
            # Compare by _history_id: an entry ours moved to a comment
            # (WT-44) is the same entry, not a new one to append twice.
            seen = {_history_id(e) for e in old} | {_history_id(e) for e in have}
            add = [e for e in o if _history_id(e) not in seen]
            moved = {_history_id(e): e for e in o
                     if isinstance(e, dict) and e.get("text_ref")}
            kept = [e if isinstance(e, dict) and e.get("text_ref")
                    else moved.get(_history_id(e), e) for e in have]
            if add or kept != have:
                merged_m["history"] = kept + add
                dirty = True
        elif f == b:
            if o is _UNSET:
                merged_m.pop(key, None)
            else:
                merged_m[key] = copy.deepcopy(o)
            dirty = True
        else:
            conflicts.append(key)
    if ours_h != base_h and fresh_h != ours_h:
        if fresh_h == base_h:
            merged_h, dirty = ours_h, True
        else:
            conflicts.append("body")
    if "claimed_by" in conflicts:
        # The ticket changed hands under us: nothing of ours belongs on the
        # new claimant's ticket (no claimed_at, no claim history entry).
        return None, conflicts
    return (_body_with_metadata(merged_h, merged_m) if dirty else None), conflicts


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
            body = _strip_entry_marker(str(comment.get("body") or ""))[0].strip()
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
        queue_label: Optional[str] = None,
    ):
        self.queue = queue
        self.repo = str(repo or "").strip()
        self.repo_path = str(repo_path or "").strip()
        self.assignee = str(assignee or "@me").strip() or "@me"
        # `queue_label` in queue-config.json overrides the default
        # `watchtower:<QUEUE>`; unset keeps the default exactly.
        self.queue_label = (
            str(queue_label).strip()
            if queue_label is not None and str(queue_label).strip()
            else self._config_queue_label()
        )
        # The catch-all queue (`queue_label: "*"`) owns every issue on a shared
        # repo that no sibling queue's label claims. It has no label of its own,
        # so nothing below ever creates, adds, or matches on one.
        self.catch_all = self.queue_label == CATCH_ALL_LABEL
        self.sibling_labels = (
            frozenset(self._config_sibling_labels()) if self.catch_all else frozenset()
        )
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

    def _usage_history(self, meta, number, event, **fields):
        _append_history(meta, event, **fields)
        from . import usage
        tracking = dict(meta, ref=f"{self.queue}-{number}",
                        _usage_namespace=f"github:{self.repo}:{self.queue}")
        entry = dict(meta["history"][-1], by={"worker": fields.get("worker", ""),
                                              "session_id": fields.get("session_id", "")})
        usage.capture(tracking, usage.on_event, entry)
        if tracking.get("token_usage"):
            meta["token_usage"] = tracking["token_usage"]

    def _config_sibling_labels(self) -> List[str]:
        try:
            from . import config
            return config.sibling_queue_labels(self.queue, self.repo)
        except Exception:
            return []

    def _config_queue_label(self) -> str:
        try:
            from . import config
            return config.queue_label(self.queue)
        except Exception:
            return f"watchtower:{self.queue}"

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

    def _run_raw(self, args: List[str], *,
                 body: Optional[str] = None) -> "subprocess.CompletedProcess[str]":
        """Run ``gh`` and hand back the whole result.

        Split out of ``_run`` because the ETag probe has to read the exit code
        and stderr itself: ``gh api`` exits 1 on a 304, which is a success for
        us, and ``_run``'s exit-code-means-failure rule cannot express that.

        An issue ``body`` travels on stdin (``--body-file -``), never argv:
        a long issue history exceeds the kernel's per-argument limit and
        ``exec`` fails with E2BIG before gh even starts (WT-40). It is passed
        separately, never found by scanning argv, so an option VALUE that
        looks like a flag (a title of ``--body``) keeps its meaning.
        """
        argv = list(args)
        extra: Dict[str, Any] = {}
        if body is not None:
            argv = _body_argv(argv)
            # gh reads the file as raw UTF-8; pin it instead of the locale.
            extra = {"input": body, "encoding": "utf-8"}
        try:
            return subprocess.run(
                ["gh", *argv],
                cwd=self.repo_path or None,
                capture_output=True,
                text=True,
                timeout=60,
                env=_gh_child_env(),
                **extra,
            )
        except FileNotFoundError as exc:
            raise GitHubBackendError(
                "GitHub backend requires the gh CLI to be installed and on PATH"
            ) from exc
        except subprocess.TimeoutExpired as exc:
            raise GitHubBackendError(f"gh {_render_argv(argv)} timed out") from exc

    def _run(self, args: List[str], *, check: bool = True,
             body: Optional[str] = None) -> str:
        # Body-less calls keep the one-argument form older stubs replace.
        raw = (lambda: self._run_raw(args)) if body is None else (
            lambda: self._run_raw(args, body=body))
        proc = raw()
        if (
            check
            and proc.returncode != 0
            and _is_auth_error((proc.stderr or proc.stdout or "").strip())
            and _attempt_auth_self_heal()
        ):
            # The heal proved gh's auth is usable (transient read, or a
            # masking env token now latched out of the child env): the
            # original command earns exactly one retry before we believe it.
            proc = raw()
        if check and proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "").strip()
            if _is_rate_limit_error(detail):
                # Observed evidence beats `gh api rate_limit`, which cannot
                # see the user-level aggregate limit (WATCHTOWER-19).
                _record_gh_rate_limited(detail)
            elif _is_auth_error(detail):
                # The heal already ran (or found nothing to heal), so this is
                # a confirmed auth outage: hold live calls off instead of
                # letting every queue ERROR every poll until a human repairs
                # the token.
                _record_gh_auth_broken(detail)
            raise GitHubBackendError(
                f"gh {_render_argv(args if body is None else _body_argv(args))} failed"
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
        if not self.catch_all:
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

    _ISSUE_TO_ITEM_MEMO: Dict[tuple, Dict[str, Any]] = {}
    _LIST_ITEMS_MEMO: Dict[tuple, tuple] = {}

    def _memo_config(self) -> tuple:
        """Every backend setting that shapes a converted item."""
        return (
            self.repo,
            self.queue,
            self.repo_path,
            self.partition_by_label,
            self.catch_all,
            frozenset(self.sibling_labels),
            self.queue_label,
            self.in_progress_label,
            self.no_auto_drain_label,
            self.run_requested_label,
            self.auto_drain,
            self.grace_s,
        )

    def _issue_to_item(self, issue: Dict[str, Any]) -> Dict[str, Any]:
        comments_raw = issue.get("comments")
        has_comments = isinstance(comments_raw, list) and bool(comments_raw)
        created_at = issue.get("createdAt")
        state = str(issue.get("state") or "").upper()
        age_settled = (
            state == "CLOSED"
            or not self.auto_drain
            or self.grace_s <= 0
            or _age_seconds(created_at) >= self.grace_s
        )
        memo_key = None
        # A `wt defer` window ends by the clock alone, with no issue edit to
        # bust the memo, so a still-deferred issue is never memoized.
        if age_settled and not has_comments and not _defer_marker_live(issue.get("body")):
            # Content, not id(issue): a freed dict's id is reused, and
            # updatedAt is per-second, so an edit plus a re-read in the same
            # second served the pre-edit item (two claim winners, WT-10).
            memo_key = (
                str(issue.get("body") or ""),
                str(issue.get("title") or ""),
                tuple(_label_names(issue.get("labels"))),
                tuple(_assignee_logins(issue.get("assignees"))),
                str(issue.get("url") or ""),
                str((issue.get("author") or {}).get("login") or "")
                if isinstance(issue.get("author"), dict) else str(issue.get("author") or ""),
                created_at,
                issue.get("closedAt"),
                issue.get("number"),
                issue.get("updatedAt"),
                state,
                self._memo_config(),
            )
            cached_base = self._ISSUE_TO_ITEM_MEMO.get(memo_key)
            if cached_base is not None:
                item = dict(cached_base)
                if "token_usage" in item:
                    item["token_usage"] = copy.copy(item["token_usage"])
                from . import usage
                item["_usage_namespace"] = f"github:{self.repo}:{self.queue}"
                usage.attach(item)
                item.pop("_usage_namespace", None)
                from . import github_pipeline
                return github_pipeline.overlay(item)
        # meta is read-only here: the two containers that reach the item are
        # copied below, so the memoized parse can be shared across queues.
        body, meta = _split_body_shared(str(issue.get("body") or ""))
        if state != "CLOSED" and _claim_predates_close(meta):
            # Reopened outside wt (a batcher's raw `gh issue reopen`, the
            # GitHub UI): the claim belongs to the run the close ended, so the
            # issue is open and unclaimed, not an orphan to recover (WT-42).
            meta = {k: v for k, v in meta.items() if k not in _CLAIM_META_KEYS}
        text = _issue_text(body, meta.get("note"), issue.get("title"), issue.get("comments"))
        labels = _label_names(issue.get("labels"))
        assignees = _assignee_logins(issue.get("assignees"))
        # Membership, not admission: under the blacklist model every issue in
        # the repo belongs to this queue unless the repo is shared, in which
        # case the legacy label is the only thing that can divide them.
        if not self.partition_by_label:
            queue_member = True
        elif self.catch_all:
            queue_member = not self.sibling_labels.intersection(labels)
        else:
            queue_member = self.queue_label in labels
        number = int(issue.get("number") or 0)
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
        deferred_until = _effective_defer(meta)
        # `wt defer`: inside its data window a ticket is out of the pool for
        # claims and spawn depth alike, ▶ included (queue._claim_candidates).
        deferred = _defer_live(deferred_until)
        # A duplicate follower waits on its original (OPS-208), whatever the defer says.
        workable = (queue_member and status == "open" and not deferred
                    and not meta.get("duplicate_of"))
        auto_eligible = bool(
            workable
            and self.auto_drain
            and not no_auto_drain
            and _age_seconds(created_at) >= self.grace_s
        )
        manual_eligible = bool(workable and run_requested)
        work_it = auto_eligible or manual_eligible

        resolution = _normalize_resolution({
            "summary": meta.get("resolution_summary", ""),
            "commit": meta.get("resolution_commit", ""),
            "caveats": meta.get("resolution_caveats", []),
            "follow_ups": meta.get("resolution_follow_ups", []),
            "unresolved": meta.get("resolution_unresolved", []),
            "caveats_ack": meta.get("resolution_caveats_ack") or {},
            "follow_ups_ack": meta.get("resolution_follow_ups_ack") or {},
            "unresolved_ack": meta.get("resolution_unresolved_ack") or {},
            "session_id": meta.get("resolution_session_id", ""),
            "engine": meta.get("resolution_engine", ""),
            "transcript_path": meta.get("resolution_transcript_path", ""),
            "machine": meta.get("resolution_machine", ""),
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
            "block_kind": str(meta.get("block_kind") or ""),
            "blocked_at": str(meta.get("blocked_at") or ""),
            # The orphan sweep's answer grace keys on this (OPS-200).
            "answered_at": meta.get("answered_at"),
            "blocked_by": [str(r) for r in (meta.get("blocked_by") or []) if r]
            if isinstance(meta.get("blocked_by"), list) else [],
            "submitter": str(meta.get("submitter") or ""),
            "submitter_explicit": bool(meta.get("submitter_explicit", False)),
            "claimed_by": (
                meta.get("claimed_by")
                or (",".join(assignees) if queue_member and status == "in_progress" and assignees else None)
            ),
            "claimed_at": meta.get("claimed_at"),
            "closed_at": issue.get("closedAt") or meta.get("closed_at"),
            "claimed_session_id": meta.get("claimed_session_id"),
            "claimed_machine": meta.get("claimed_machine"),
            # WT-39: the owning host's renewal of this claim, read by others.
            "claim_heartbeat": meta.get("claim_heartbeat")
            if isinstance(meta.get("claim_heartbeat"), dict) else None,
            "created_at": created_at or _now_iso(),
            "updated_at": issue.get("updatedAt") or created_at or _now_iso(),
            "github_repo": self.repo,
            # OPS-201: host whose local sidecar owns this ticket's gate pipeline.
            "pipeline_host": str(meta.get("pipeline_host") or ""),
            "github_labels": labels,
            "github_assignees": assignees,
            # The issue's GitHub author -- "who opened it" for synced tickets
            # (CCC-1166). Kept separate from ``submitter`` (a wt-addressable
            # notify target): a GitHub login is not resolvable by
            # messages.resolve_target.
            "github_author": str((issue.get("author") or {}).get("login") or ""),
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
                comments_raw
                if isinstance(comments_raw, list) else []
            ),
        }
        if status == "closed":
            item["closed_by"] = meta.get("closed_by") or item.get("claimed_by")
            item["closed_machine"] = meta.get("closed_machine") or item.get("claimed_machine")
        if resolution:
            item["resolution"] = resolution
        if str(meta.get("depth") or "").lower() == "deep":
            item["depth"] = "deep"
            item["budget_min"] = meta.get("budget_min")
        if deferred_until:
            item["deferred_until"] = deferred_until
            item["defer_reason"] = str(meta.get("defer_reason") or "")
            if isinstance(meta.get("pipeline_hold"), dict) and deferred_until != meta.get("deferred_until"):
                item["defer_reason"] = str(meta["pipeline_hold"].get("prev_reason") or "")
        if isinstance(meta.get("pipeline_hold"), dict):
            item["pipeline_hold"] = dict(meta["pipeline_hold"])
        if isinstance(meta.get("wait"), dict):
            item["wait"] = dict(meta["wait"])
        if meta.get("duplicate_of"):
            item["duplicate_of"] = str(meta["duplicate_of"])
            item["duplicate_gen"] = int(meta.get("duplicate_gen") or 0)
            if isinstance(meta.get("duplicate_result"), dict):
                item["duplicate_result"] = dict(meta["duplicate_result"])
        history = meta.get("history")
        if isinstance(history, list):
            item["history"] = list(history)
        if meta.get("token_usage"):
            item["token_usage"] = copy.copy(meta["token_usage"])
        if memo_key is not None:
            if len(self._ISSUE_TO_ITEM_MEMO) >= 16384:
                self._ISSUE_TO_ITEM_MEMO.clear()
            self._ISSUE_TO_ITEM_MEMO[memo_key] = dict(item)
        from . import usage
        item["_usage_namespace"] = f"github:{self.repo}:{self.queue}"
        usage.attach(item)
        item.pop("_usage_namespace", None)
        from . import github_pipeline
        return github_pipeline.overlay(item)

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
        _LIST_FETCH_COUNT["n"] += 1
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
        submitter_explicit: bool = False,
        blocked_by: Optional[List[str]] = None,
        depth_fields: Optional[Dict[str, Any]] = None,
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
            # Was that submitter NAMED by the filer (`wt add --submitter`)
            # rather than auto-detected? It decides whether they hear every
            # event or only the default set -- see queue._submitter_wants.
            "submitter_explicit": bool(submitter_explicit),
        }
        if blocked_by:
            meta["blocked_by"] = self._validate_blockers("", blocked_by)
        # Pre-validated by queue._depth_fields: {} for quick, else depth+budget_min.
        meta.update(depth_fields or {})
        issue_title = _clip(title or note or "WatchTower ticket", 200)
        body = _body_with_metadata(text, meta)
        out = self._run([
            "issue", "create",
            *self._repo_args(),
            "--title", _Text(issue_title),
            # The catch-all owns unlabelled issues, so it files without one.
            *([] if self.catch_all else ["--label", self.queue_label]),
        ], body=body)
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
        from . import usage
        open_raw = (
            self._list_issues("open", fresh=fresh, strict=strict)
            if status != "closed" else None
        )
        closed_raw = (
            self._list_issues("closed", fresh=fresh, strict=strict)
            if status in (None, "closed") else None
        )
        # A tick lists every GitHub queue dozens of times off the same cached
        # issue lists (WT-16). Reuse the converted result while the raw lists
        # are the same objects (the memo holds them, so identity is safe),
        # no usage ledger was written, and no issue crossed a time boundary.
        from . import github_pipeline
        memo_key = (
            # OPS-201: a sidecar-only transition changes eligibility too.
            self._memo_config(), status, lane, github_pipeline.signature(),
        )
        ledger_stamp = None if strict else usage.ledger_dir_stamp()
        now = time.time()
        hit = self._LIST_ITEMS_MEMO.get(memo_key)
        if (
            hit is not None
            and ledger_stamp is not None
            and hit[0] is open_raw
            and hit[1] is closed_raw
            and hit[2] == ledger_stamp
            and now < hit[3]
        ):
            return [_copy_listed_item(it) for it in hit[4]]
        valid_until = now + _LIST_ITEMS_MEMO_TTL_S
        issues: List[Dict[str, Any]] = list(open_raw or [])
        if self.auto_drain and self.grace_s > 0:
            for issue in issues:
                age = _age_seconds(issue.get("createdAt"))
                if age < self.grace_s:
                    valid_until = min(valid_until, now + self.grace_s - age)
        retention_s = COMPLETED_ISSUE_RETENTION_DAYS * 86400
        for issue in closed_raw or []:
            if _recently_closed(issue):
                issues.append(issue)
                age = _age_seconds(issue.get("closedAt"))
                valid_until = min(valid_until, now + retention_s - age)
        items = [self._issue_to_item(issue) for issue in issues]
        if self.partition_by_label:
            # Two or more queues share this repo: each shows only its own
            # slice, because nothing but the legacy label can tell them apart.
            items = [it for it in items if it.get("watchtower_runnable")]
        if status:
            items = [it for it in items if it.get("status") == status]
        if lane:
            items = [it for it in items if it.get("lane") == lane]
        items = sorted(items, key=lambda it: int(it.get("number", 0)))
        if ledger_stamp is not None:
            if len(self._LIST_ITEMS_MEMO) >= 512:
                self._LIST_ITEMS_MEMO.clear()
            self._LIST_ITEMS_MEMO[memo_key] = (
                open_raw, closed_raw, ledger_stamp, valid_until,
                [_copy_listed_item(it) for it in items],
            )
        return items

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
        if self.partition_by_label and not self.catch_all:
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
            "--json", "number,title,body,state,url,assignees,labels,author,comments,createdAt,updatedAt,closedAt",
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

    @contextlib.contextmanager
    def _body_lock(self):
        """The claim lock, reentrant within a thread (see `_serialized_body_write`)."""
        path = str(self._claim_lock_path())
        held = getattr(_BODY_LOCKS_HELD, "paths", None)
        if held is None:
            held = _BODY_LOCKS_HELD.paths = set()
        if path in held:
            yield
            return
        with _FileLock(Path(path)):
            held.add(path)
            try:
                yield
            finally:
                held.discard(path)

    def _fresh_issue_state(self, number: str) -> str:
        """The issue's live ``OPEN``/``CLOSED`` state, read just before a
        state-dependent write. Anything else (a failed read, unparsable JSON,
        an unknown state) raises: the caller must not guess."""
        raw = self._run(["issue", "view", number, *self._repo_args(), "--json", "state"])
        try:
            state = str((json.loads(raw or "{}") or {}).get("state") or "").upper()
        except (json.JSONDecodeError, AttributeError) as exc:
            raise GitHubBackendError(
                f"gh issue view {number} --json state returned invalid JSON") from exc
        if state not in ("OPEN", "CLOSED"):
            raise GitHubBackendError(
                f"gh issue view {number} returned unknown state {state!r}")
        return state

    def _fit_body(self, item: Dict[str, Any], body: str, meta: Dict[str, Any]) -> str:
        """The issue body for ``body``+``meta``, kept under GitHub's size cap
        (WT-44). Over budget, long answer/comment texts that issue comments
        already hold move out of the body (`_compact_history`); if it still
        does not fit under the hard cap, raise before writing anything rather
        than let GitHub refuse it mid-operation: nothing is truncated,
        and the caller's write stays undone and safe to retry."""
        ours = _body_with_metadata(body, meta)
        if _body_bytes(ours) <= _ISSUE_BODY_BUDGET_BYTES:
            return ours
        hist = meta.get("history") if isinstance(meta.get("history"), list) else []
        movable = any(isinstance(e, dict) and e.get("event") in _COMMENTED_EVENTS
                      and not e.get("text_ref")
                      and len(str(e.get("text") or "")) > _HISTORY_TEXT_INLINE_MAX
                      for e in hist)
        moved = 0
        if movable:
            comments = item.get("_github_comments")
            if not comments:
                fresh = self.get(item["number"])  # list-produced items carry no comments
                comments = (fresh or {}).get("_github_comments") or []
            moved = _compact_history(meta, comments, body)
            ours = _body_with_metadata(body, meta)
        size = _body_bytes(ours)
        if size > _ISSUE_BODY_MAX_BYTES:
            raise GitHubBackendError(
                f"{self.repo}#{item['number']} issue body would be {size} bytes; GitHub "
                f"refuses more than {_ISSUE_BODY_MAX_BYTES}. Moved {moved} answer/comment "
                f"history text(s) to their issue comments and it still does not fit. "
                f"Nothing was written; the change is safe to retry once it fits.")
        if moved:
            try:
                from . import queue as _q
                _q._log("GH-COMPACT", f"{self.repo}#{item['number']} moved {moved} "
                        f"answer/comment history text(s) to their issue comments "
                        f"({size} bytes)")
            except Exception:  # noqa: BLE001 - logging never breaks a write
                pass
        return ours

    def _post_entry_comment(self, item: Dict[str, Any], body: str,
                            meta: Dict[str, Any], event: str, text: str) -> str:
        """Post ``text`` as an issue comment tagged with an entry key. A comment already carrying the key --
        posted by an attempt whose body edit then failed -- is adopted, not
        reposted. Returns (entry key, reference). Refuses up front, posting nothing, when the text exceeds a
        comment or its history entry could never fit the body."""
        if len(text) > _COMMENT_MAX_CHARS - 64:
            raise GitHubBackendError(
                f"text is {len(text)} chars; a GitHub comment holds at most "
                f"{_COMMENT_MAX_CHARS - 64} here. Nothing was written: shorten it "
                f"or put the full text in a file and reference it.")
        key = _entry_key(str(item["number"]), event, meta, text)
        projected = copy.deepcopy(meta)
        _append_history(projected, event, entry_key=key)
        _referenced_text(projected["history"][-1], text, f"{item.get('url', '')}#issuecomment-0")
        self._fit_body(item, body, projected)
        for c in item.get("_github_comments") or []:
            if isinstance(c, dict):
                found_text, found = _strip_entry_marker(str(c.get("body") or ""))
                if found == key and found_text == text:
                    return key, _comment_ref(c) or f"comment:{key}"
        out = self._run(["issue", "comment", str(item["number"]), *self._repo_args()],
                        body=text + _ENTRY_MARKER.format(key))
        url = (out or "").strip().splitlines()[-1:] or [""]
        return key, url[0] if url[0].startswith("http") else f"comment:{key}"

    def _edit_body(self, item: Dict[str, Any], body: str, meta: Dict[str, Any],
                   *extra: str) -> List[str]:
        """Write ``body``+``meta`` over ``item``'s issue, then re-read and
        re-apply our change if a concurrent (cross-host) writer dropped it
        (see `_merge_body_write`). Returns keys a concurrent writer changed
        too -- theirs stand; a caller that needs one of them (a claim) must
        check. A write after our re-read can still be lost: GitHub offers no
        precondition on issue edits, so this narrows the window only."""
        number = str(item["number"])
        base = item.get("_github_body") or item.get("text", "")
        ours = self._fit_body(item, body, meta)
        self._run(["issue", "edit", number, *self._repo_args(), *extra], body=ours)
        conflicts: List[str] = []
        for _ in range(_BODY_WRITE_RETRIES):
            try:
                raw = self._run(["issue", "view", number, *self._repo_args(), "--json", "body"])
                fresh = str((json.loads(raw or "{}") or {}).get("body") or "")
            except (GitHubBackendError, json.JSONDecodeError, AttributeError):
                return conflicts  # the write itself landed; verification is best-effort
            merged, found = _merge_body_write(base, ours, fresh)
            conflicts += [k for k in found if k not in conflicts]
            if merged is None:
                return conflicts
            _log_body_reapply(self.repo, number, conflicts)
            merged_h, merged_m = _split_body(merged)
            merged = self._fit_body(item, merged_h, merged_m)
            self._run(["issue", "edit", number, *self._repo_args()], body=merged)
            base, ours = fresh, merged
        return conflicts

    def _stop_signal_path(self, session_id: str):
        try:
            from . import workers as _workers
            stop_dir = _workers.STOP_SIGNALS_DIR
        except Exception:
            from pathlib import Path
            stop_dir = Path.home() / ".watchtower" / "stop-signals"
        return stop_dir / session_id

    def _blocker_index(
        self, items: List[Dict[str, Any]], refs: List[str], *,
        fresh: bool = True, transitive: bool = False,
    ) -> Dict[str, Dict[str, Any]]:
        """Resolve dependencies through their owning queues, not this repo.

        Depth checks retain the owning backend's bounded list cache. Claims
        and validation request fresh state. Missing runtime refs stay waiting
        rather than accidentally releasing a dependent after a lookup miss.
        """
        from . import queue as q

        by_ref = _refs_index(items)
        loaded = {self.queue}
        seen = set()
        pending = list(refs)
        while pending:
            raw = str(pending.pop())
            ref = f"{self.queue}-{raw}" if raw.isdigit() else raw
            if ref in seen:
                continue
            seen.add(ref)
            project = q._project_from_ident(ref)
            if project and project not in loaded:
                backend = q._github_backend_for_project(project)
                others = (backend.list_items(fresh=fresh) if backend is not None
                          else q.list_items(project=project))
                by_ref.update(_refs_index(others))
                loaded.add(project)
            if ref not in by_ref:
                try:
                    item = q.get(ref)
                except GitHubBackendError as exc:
                    detail = str(exc).lower()
                    if "not found" not in detail and "could not resolve to an issue" not in detail:
                        raise
                    item = None
                if item is not None:
                    by_ref[ref] = item
            item = by_ref.get(ref)
            if item is not None and transitive:
                pending.extend(item.get("blocked_by") or [])
            elif item is None and not transitive:
                by_ref[ref] = {"ref": ref, "status": "open"}
        return by_ref

    def _validate_blockers(self, ref: str, blocked_by: List[str]) -> List[str]:
        items = self.list_items(fresh=True)
        by_ref = self._blocker_index(items, blocked_by, transitive=True)
        return _validate_blocked_by(list(by_ref.values()), ref,
                                    [str(r) for r in blocked_by])

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
        requested_only: bool = False,
        claimer: Optional[tuple] = None,
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
        elif manual_only or requested_only:
            eligibility = "manual_eligible"
        candidates = [
            it for it in self.list_items(status="open", lane=lane, fresh=fresh)
            if it.get(eligibility, False)
        ]
        from . import queue as _q
        now_ts = time.time()
        # WT-40: an answer reserved for its parked worker (affinity) is claimable
        # only by that worker until the reservation expires.
        candidates = [it for it in candidates
                      if not _q._affinity_reserved(it, now_ts)
                      or (claimer is not None and _q._affinity_owner(it, *claimer))]
        if any(it.get("blocked_by") for it in candidates):
            # Dependencies (WT-36): a ticket with an open (or declined /
            # unresolved) blocker is not claimable and not spawn-worthy depth.
            by_ref = self._blocker_index(
                self.list_items(fresh=fresh),
                [ref for it in candidates for ref in (it.get("blocked_by") or [])],
                fresh=fresh,
            )
            candidates = [
                it for it in candidates
                if not it.get("blocked_by")
                or blocker_verdict(it, by_ref)[0] == "ok"
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
        requested_only: bool = False,
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
            requested_only=requested_only,
            claimer=(session_id, session_uuid),
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
        with self._body_lock():
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
            if item.get("blocked_by"):
                by_ref = self._blocker_index([item], item["blocked_by"])
                state, blocker = blocker_verdict(item, by_ref)
                if state != "ok":
                    raise ValueError(f"{ref} {state} on {blocker}")
            number = str(item["number"])
            body, meta = _split_body(item.get("_github_body") or item.get("text", ""))
            # A fresh claim on an issue reopened outside wt drops the ended
            # run's claim and close stamps instead of inheriting them (WT-42).
            for key in (*_CLAIM_META_KEYS, "closed_at"):
                meta.pop(key, None)
            meta.update({
                "claimed_by": str(session_id),
                "claimed_machine": machine_tag(),
                "claimed_at": _now_iso(),
            })
            if session_uuid:
                meta["claimed_session_id"] = str(session_uuid)
            from .github_pipeline import PIPELINE_PROTO
            # Tells gating hosts this claimant honors the plan hold (OPS-203).
            self._usage_history(meta, number, "claim", session_id=str(session_uuid or ""),
                                worker=str(session_id), pipeline=PIPELINE_PROTO)
            self._ensure_labels()
            self._edit_body(item, body, meta,
                            "--add-assignee", self.assignee,
                            "--add-label", self.in_progress_label)
            # Trust the persisted body, not our intent: a claim another host
            # overwrote (or never landed) must fail, not report
            # claimed_by_you (WT-20). claim_next moves to the next candidate.
            claimed = self.get(ref)
            persisted = (_split_body_shared(claimed.get("_github_body") or "")[1]
                         if claimed else {})
            if str(persisted.get("claimed_by") or "") != str(session_id):
                raise ValueError(
                    f"{ref} claim did not persist (claimed_by="
                    f"{persisted.get('claimed_by') or 'none'}); not claimed")
            claimed["claimed_by"] = str(session_id)
            claimed["claimed_machine"] = meta.get("claimed_machine")
            claimed["status"] = "in_progress"
            return claimed

    @_serialized_body_write
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
        expect: Optional[Dict[str, Any]] = None,
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
        # WT-39: the claim identity the caller decided on (the orphan sweep's
        # dead local worker) must still be the one on the fresh read, else a
        # live claim taken since -- possibly on another host -- is cleared.
        if expect is not None:
            from . import queue as _q
            if not _q.claim_expect_holds(item, expect):
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
                "claimed_by", "claimed_machine", "claimed_at",
                "closed_by", "closed_machine", "closed_at",
                "resolution_summary", "resolution_commit", "resolution_caveats",
                "resolution_follow_ups", "resolution_unresolved",
                "resolution_caveats_ack", "resolution_follow_ups_ack",
                "resolution_unresolved_ack",
                "resolution_session_id", "resolution_engine",
                "resolution_transcript_path", "resolution_machine",
                "needs_input", "block_question",
            ):
                meta.pop(key, None)
            self._usage_history(meta, number, "reopen", reason=reason)
            self._edit_body(item, body, meta, "--remove-label", self.in_progress_label)
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
            meta["closed_machine"] = machine_tag()
            # Backfill claimed_by on a never-claimed issue so attribution
            # isn't dropped when a worker closes by ref without claiming
            # first (WT-81). Never overwrites a real claimant.
            if not meta.get("claimed_by"):
                meta["claimed_by"] = str(session_id)
                meta["claimed_machine"] = meta["closed_machine"]
        if norm:
            # session_id/machine default to this close's own attribution
            # (mirrors queue.update_status) so CCC can resolve "session X on
            # node Y" even when the caller didn't pass them explicitly.
            # Mutated in place (not just written to ``meta``) so the enriched
            # values also come back on ``closed["resolution"]`` below, which
            # overwrites the self.get() round-trip with this same ``norm``.
            if not norm.get("session_id"):
                sid = str(session_uuid or "") or meta.get("claimed_session_id", "")
                if sid:
                    norm["session_id"] = sid
            norm.setdefault("machine", meta.get("closed_machine") or machine_tag())
            meta["resolution_summary"] = norm.get("summary", "")
            # Always overwrite (possibly with ""), so a re-close without a
            # commit can't leave the previous close's SHA vouching for it.
            meta["resolution_commit"] = norm.get("commit", "")
            meta["resolution_caveats"] = norm.get("caveats", [])
            meta["resolution_follow_ups"] = norm.get("follow_ups", [])
            meta["resolution_unresolved"] = norm.get("unresolved", [])
            meta["resolution_session_id"] = norm.get("session_id", "")
            meta["resolution_engine"] = norm.get("engine", "")
            meta["resolution_transcript_path"] = norm.get("transcript_path", "")
            meta["resolution_machine"] = norm.get("machine", "")
            # A (re-)close writes a fresh resolution, so acks carry over only
            # when the caller handed them back -- same as the local backend,
            # where close replaces `resolution` wholesale.
            for field in ("caveats", "follow_ups", "unresolved"):
                acks = norm.get(f"{field}_ack")
                if acks:
                    meta[f"resolution_{field}_ack"] = acks
                else:
                    meta.pop(f"resolution_{field}_ack", None)
        self._usage_history(meta, number, "close", session_id=str(session_uuid or ""),
                         worker=str(session_id or meta.get("closed_by") or ""),
                         resolution=norm)
        self._edit_body(item, body, meta, "--remove-label", self.in_progress_label)
        # `gh issue close --comment` takes the text on argv only (no file or
        # stdin form), and a resolution with long caveats can pass the
        # per-argument limit (WT-40). So we make gh's own steps ourselves:
        # fetch the state, return early on CLOSED without commenting (gh
        # 2.101 close.go), else post the comment (on stdin), then close. The
        # race window between that read and the writes is gh's own; GitHub
        # offers no precondition on either.
        if self._fresh_issue_state(number) == "OPEN":
            self._run([
                "issue", "comment", number,
                *self._repo_args(),
            ], body=_resolution_comment(norm))
        self._run(["issue", "close", number, *self._repo_args()])
        closed = self.get(ident)
        if closed:
            closed["closed_by"] = str(session_id or closed.get("claimed_by") or "")
            closed["closed_machine"] = str(
                meta.get("closed_machine") or closed.get("claimed_machine") or ""
            )
            if norm:
                closed["resolution"] = norm
        return closed

    @_serialized_body_write
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
        self._edit_body(item, body, meta)
        return self.get(ident)

    @_serialized_body_write
    def defer(
        self,
        ident: Any,
        *,
        session_id: str,
        until_iso: str,
        reason: str,
        progress: str = "",
    ) -> Optional[Dict[str, Any]]:
        """`wt defer` on a GitHub-backed ticket (OPS-100): release the claim
        and park the issue out of the pool until ``until_iso``. Same rules as
        the local queue.defer; the window lives in the metadata block, so it
        survives across hosts and expires by the clock with no further edit."""
        item = self.get(ident)
        if item is None:
            return None
        status = item.get("status")
        if status not in ("open", "in_progress"):
            raise ValueError(f"{item.get('ref')} is {status}; only an open or in-progress "
                             "ticket can be deferred")
        holder = str(item.get("claimed_by") or "")
        if status == "in_progress" and holder and session_id and holder != str(session_id):
            raise ValueError(f"{item.get('ref')} is claimed by {holder}, not {session_id}")
        number = str(item["number"])
        body, meta = _split_body(item.get("_github_body") or item.get("text", ""))
        if progress:
            _append_history(meta, "progress", worker=str(session_id), text=_clip(progress, 24000))
        for key in ("claimed_by", "claimed_machine", "claimed_at", "claimed_session_id"):
            meta.pop(key, None)
        meta["deferred_until"] = until_iso
        meta["defer_reason"] = _clip(reason, 2000)
        self._usage_history(meta, number, "defer", worker=str(session_id),
                            until=until_iso, reason=_clip(reason, 2000))
        self._edit_body(item, body, meta, "--remove-label", self.in_progress_label)
        return self.get(ident)

    @_serialized_body_write
    def block(
        self,
        ident: Any,
        *,
        session_id: str = "",
        question: str = "",
        progress: str = "",
        origin: str = "system",
        kind: str = "input",
        wait_on: str = "",
        wait_actor: str = "",
        wait_ref: str = "",
        expect: Optional[Dict[str, Any]] = None,
        **_ignored: Any,
    ) -> Optional[Dict[str, Any]]:
        """Record a worker's request for input on a GitHub-backed ticket.
        ``origin`` is accepted and ignored: GitHub queues keep the legacy
        hold-while-blocked behaviour (WT-28b). ``kind`` and the ``wait_*``
        fields persist in the body metadata like the file backend's, so an
        external wait survives a reload (CCC-244).

        ``expect`` (WT-39, the backstop's escalation): fields the issue must
        still carry on this re-read, taken under the per-repo body lock, else
        nothing is written. The lock serializes writers on THIS host only;
        another host's renewal or reassignment landing after this read is
        not fenced (GitHub has no compare-and-swap) -- `_edit_body`'s
        three-way merge keeps that writer's claim fields, but the question
        is still posted. The window is one read-to-edit round trip, so the
        backstop only asks this for a claim held on this host; a claim held
        elsewhere is routed to its owner, never blocked (WT-38)."""
        from . import queue as _q
        item = self.get(ident)
        if item is None:
            return None
        if expect and any(item.get(k) != v for k, v in expect.items()):
            return None
        number = str(item["number"])
        body, meta = _split_body(item.get("_github_body") or item.get("text", ""))
        now = _now_iso()
        meta["needs_input"] = True
        meta["block_question"] = _clip(question, 4000)
        meta["blocked_at"] = now
        meta["block_kind"] = kind if kind in _q.BLOCK_KINDS else "input"
        if wait_on:
            meta["wait"] = _q.wait_record(wait_on, wait_actor, wait_ref,
                                          block_at=now, by=str(session_id or ""))
        else:
            meta.pop("wait", None)
        if session_id:
            if not meta.get("claimed_by"):
                meta["claimed_by"] = str(session_id)
                meta["claimed_machine"] = machine_tag()
        if progress:
            _append_history(meta, "progress", worker=str(session_id), text=_clip(progress, 24000))
        # The writing host, so a system (backstop) block is attributable (WT-39).
        self._usage_history(meta, number, "block", worker=str(session_id), question=_clip(question, 4000),
                            kind=meta["block_kind"], wait_on=wait_on, wait_actor=wait_actor,
                            wait_ref=wait_ref, machine=machine_tag())
        self._edit_body(item, body, meta)
        # Mirror the question onto the issue discussion (WT-37): the metadata
        # block is invisible to anyone reading the issue on GitHub. Best-effort,
        # the block itself is already recorded. The owner-answer marker is
        # defused so the question can never be ingested as its own answer.
        if question.strip():
            shown = re.sub(re.escape(_OWNER_ANSWER_MARKER), "owner-answer",
                           _clip(question, 4000), flags=re.IGNORECASE)
            self._run([
                "issue", "comment", number,
                *self._repo_args(),
            ], check=False,
                body=f"[watchtower] Needs input from {session_id or 'worker'}:\n\n{shown}")
        return self.get(ident)

    @_serialized_body_write
    def renew_claim_heartbeat(self, ident: Any, *, claimed_by: str,
                              session_id: str) -> bool:
        """WT-39: stamp ``claim_heartbeat`` (this machine, now) on a claim this
        host holds, so another host's backstop can tell the owner is observed.
        Re-reads the issue first and writes only while it is still the same
        in-progress claim held here; returns whether it wrote."""
        item = self.get(ident)
        if (item is None or item.get("status") != "in_progress"
                or str(item.get("claimed_by") or "") != claimed_by or not claimed_by
                or str(item.get("claimed_session_id") or "") != session_id
                or str(item.get("claimed_machine") or "") != machine_tag()):
            return False
        body, meta = _split_body(item.get("_github_body") or item.get("text", ""))
        meta["claim_heartbeat"] = {"machine": machine_tag(), "at": _now_iso(),
                                   "claimed_by": claimed_by, "session_id": session_id}
        self._edit_body(item, body, meta)
        return True

    @_serialized_body_write
    def set_wait(
        self,
        ident: Any,
        on: str,
        *,
        actor: str = "",
        ref: str = "",
        note: str = "",
        by: str = "",
    ) -> Optional[Dict[str, Any]]:
        """Say who the current block waits on, without re-blocking (CCC-244).
        Body metadata only: no issue comment, the question stays as asked."""
        from . import queue as _q
        item = self.get(ident)
        if item is None:
            return None
        body, meta = _split_body(item.get("_github_body") or item.get("text", ""))
        if not meta.get("needs_input") or item.get("status") == "closed":
            raise ValueError(f"{item.get('ref', ident)} is not blocked; nothing is waiting")
        meta["wait"] = _q.wait_record(on, actor, ref, note,
                                      block_at=str(meta.get("blocked_at") or ""), by=by)
        _append_history(
            meta, "wait", kind="worker" if by else "human", worker=str(by or ""),
            wait_on=meta["wait"]["on"], wait_actor=meta["wait"].get("actor", ""),
            wait_ref=meta["wait"].get("ref", ""), text=meta["wait"].get("note", ""),
        )
        self._edit_body(item, body, meta)
        return self.get(ident)

    @_serialized_body_write
    def answer(
        self,
        ident: Any,
        text: str,
        session_id: str = "",
        *,
        post_comment: bool = True,
        source_comment_key: str = "",
        kind: str = "human",
    ) -> Optional[Dict[str, Any]]:
        """Record a human answer on a blocked GitHub-backed ticket and post it
        as an issue comment. ``kind`` attributes it (``system`` for the
        reconciler's dependency-closed answer, WT-37).

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
        answer_text = str(text or "")
        text_ref, entry_key = source_comment_key, ""
        if post_comment:
            # WT-44: the comment is posted FIRST and is the durable full copy;
            # the body history below keeps a bounded excerpt pointing at it.
            # If the body edit then fails, the answer stays undelivered
            # (needs_input set) and a retry adopts this comment by its key.
            entry_key, text_ref = self._post_entry_comment(item, body, meta, "answer", answer_text)
        elif not text_ref:
            answer_text = _clip(answer_text, 24000)
        if item.get("status") == "awaiting_answer" and item.get("parked"):
            # WT-40: a parked ticket routes the answer through the sidecar. Written
            # BEFORE the issue edit: a crash in between re-routes (the owner-answer
            # key is not recorded yet) instead of losing the answer. WT-44: the
            # route records this write's key, so the retry of a write whose body
            # edit failed keeps the generation it already routed (no gen bump
            # that would orphan a resume_claim of it) instead of routing again.
            from . import github_pipeline, queue as _q
            route_key = entry_key or source_comment_key

            def _route(it: Dict[str, Any]) -> Any:
                if it.get("status") != "awaiting_answer" or not it.get("parked"):
                    return "skip"
                if route_key and (it.get("pending_answer") or {}).get("entry_key") == route_key:
                    return "skip"
                pa = _q._write_pending_answer_unlocked(it, answer_text, now, by_kind=kind,
                                                       clip=_COMMENT_MAX_CHARS)
                if route_key:
                    pa["entry_key"] = route_key
                return None
            github_pipeline.mutate(ident, _route)
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
            kind=kind if kind in ("human", "system", "worker") else "human",
            worker=str(session_id or ""),
            source="github-owner-comment" if source_comment_key else "",
            entry_key=entry_key,
        )
        _referenced_text(meta["history"][-1], answer_text, text_ref)
        releasing = bool(
            item.get("status") == "in_progress"
            and not item.get("claimed_session_id")
        )
        if releasing:
            for key in (
                "claimed_by", "claimed_machine", "claimed_at",
                "claimed_session_id",
                "block_question", "blocked_at",
            ):
                meta.pop(key, None)
            _append_history(
                meta,
                "reopen",
                kind=kind if kind in ("human", "system", "worker") else "human",
                worker=str(session_id or ""),
                reason="answered_without_resumable_session",
            )
        self._edit_body(item, body, meta)
        if releasing:
            self._run([
                "issue", "edit", number,
                *self._repo_args(),
                "--remove-label", self.in_progress_label,
            ])
            self._run(["issue", "reopen", number, *self._repo_args()], check=False)
        return self.get(ident)

    def _repo_owner(self) -> str:
        """The repo owner login (``amirfish1`` for ``amirfish1/BYM-Finie``).

        This is the identity authorized to answer a blocked ticket by commenting
        straight on the issue. Empty when the repo string is not ``owner/name``.
        """
        repo = self.repo or ""
        return repo.split("/", 1)[0].strip() if "/" in repo else ""

    @_serialized_body_write
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
        ], check=False, body=f"[watchtower] Ingested owner answer ({key}); {outcome}.")
        return updated

    @_serialized_body_write
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
        body, meta = _split_body(item.get("_github_body") or item.get("text", ""))
        actor_kind = by if by in ("worker", "human", "system") else "human"
        comment_text = str(text or "")
        # WT-44: comment first (the durable full copy, adopted on retry), then
        # a bounded history entry that references it.
        entry_key, ref = self._post_entry_comment(item, body, meta, "comment", comment_text)
        _append_history(
            meta,
            "comment",
            kind=actor_kind,
            worker=str(session_id or ""),
            entry_key=entry_key,
        )
        _referenced_text(meta["history"][-1], comment_text, ref)
        self._edit_body(item, body, meta)
        return self.get(ident)

    @_serialized_body_write
    def update(self, ident: Any, **fields: Any) -> Optional[Dict[str, Any]]:
        # The stage pipeline (gates, plan, verify) lives only in the local
        # store; dropping these silently printed EDITED for a no-op (OPS-201).
        unsupported = sorted(set(fields) & _LOCAL_ONLY_EDIT_FIELDS)
        if unsupported:
            raise ValueError(
                f"{', '.join(unsupported)} cannot be stored on a GitHub-backed queue "
                f"(the gate/plan/verify pipeline needs a local queue); nothing was changed")
        item = self.get(ident)
        if item is None:
            return None
        body, meta = _split_body(item.get("_github_body") or item.get("text", ""))
        title = item.get("title", "")
        for key, value in fields.items():
            if key == "blocked_by":
                blockers = self._validate_blockers(
                    str(item.get("ref") or ""), list(value or []))
                if blockers:
                    meta["blocked_by"] = blockers
                else:
                    meta.pop("blocked_by", None)
            elif key == "depth_fields":
                # Pre-computed by queue._depth_edit; {} demotes to quick.
                old = {k: meta[k] for k in ("depth", "budget_min") if k in meta}
                meta.pop("depth", None)
                meta.pop("budget_min", None)
                meta.update(value)
                if value != old:
                    _append_history(meta, "edit", by={"kind": "system"},
                                    fields={"depth": value.get("depth", "quick"),
                                            **({"budget_min": value["budget_min"]}
                                               if "budget_min" in value else {})})
            elif key == "title":
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
            elif key == "pipeline_host":
                if value:
                    meta["pipeline_host"] = str(value)
                else:
                    meta.pop("pipeline_host", None)
            elif key == "pipeline_hold":
                _set_pipeline_hold(meta, str(value or ""))
            elif key in ("duplicate_of", "duplicate_gen", "duplicate_result"):
                # OPS-208: the link is a ticket relationship every host reads.
                if value:
                    meta[key] = value
                else:
                    meta.pop(key, None)
                if key == "duplicate_of" and not value:
                    _append_history(meta, "duplicate_clear", by={"kind": "human"})
                if key == "duplicate_result" and isinstance(value, dict):
                    _append_history(meta, "duplicate_result", by={"kind": "system"},
                                    original=value.get("ref"), passed=value.get("complete"),
                                    original_status=value.get("status"),
                                    stale=value.get("stale"))
            elif key == "_history":
                event, extra = value
                _append_history(meta, event, **extra)
        self._edit_body(item, body, meta, "--title", _Text(title))
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

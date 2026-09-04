"""GraphQL-quota containment for the GitHub-backed queue (lane W4-4).

The failure these cover: the hourly GraphQL quota was exhausted inside every
window, because "current state" reads (`wt status`, `wt ls`, `wt claim`) each
went to GitHub on their own, from ~8 concurrent lanes, on top of the daemon
poller. The contract asserted here is that exactly one poller pays for live
list reads per repo, and a reader only goes live when that poller has demonstrably
stopped.
"""

from __future__ import annotations

import json
import os
import time

import pytest


@pytest.fixture(autouse=True)
def isolate_watchtower_state(tmp_path, monkeypatch):
    """Point every file this module's code touches under tmp_path."""
    monkeypatch.setenv(
        "WATCHTOWER_GH_LIST_CACHE_FILE", str(tmp_path / "gh-list-cache.json")
    )
    monkeypatch.setenv(
        "WATCHTOWER_GH_CONNECTIVITY_FILE", str(tmp_path / "gh-connectivity.json")
    )
    monkeypatch.setenv(
        "WATCHTOWER_GH_CLAIM_LOCKS_DIR", str(tmp_path / "gh-claim-locks")
    )
    monkeypatch.setenv("WATCHTOWER_GH_QUOTA_LOG", str(tmp_path / "gh-quota.log"))
    import watchtower.github_backend as github_backend

    github_backend._LIST_CACHE.clear()
    github_backend._GH_GRAPHQL_QUOTA_CACHE.update({"ts": 0.0, "snapshot": None})
    yield
    github_backend._LIST_CACHE.clear()
    github_backend._GH_GRAPHQL_QUOTA_CACHE.update({"ts": 0.0, "snapshot": None})


def _issue(number: int = 1, title: str = "cached", repo: str = "acme/repo"):
    return {
        "number": number, "title": title, "body": "", "state": "OPEN",
        "url": f"https://github.com/{repo}/issues/{number}",
        "assignees": [], "labels": [], "createdAt": "2026-07-01T00:00:00Z",
        "updatedAt": "2026-07-01T00:00:00Z", "closedAt": None,
    }


def _backend_that_must_not_call_gh(monkeypatch, repo):
    """A backend whose every route to GitHub raises."""
    import watchtower.github_backend as github_backend

    backend = github_backend.GitHubIssuesBackend("T", repo=repo)

    def forbidden(*args, **kwargs):
        raise AssertionError("reader must not call gh")

    monkeypatch.setattr(backend, "_run", forbidden)
    monkeypatch.setattr(backend, "_run_raw", forbidden)
    return backend


def _seed_snapshot(repo, issues, *, age_s=0.0, stale=False):
    import watchtower.github_backend as github_backend

    github_backend._write_persisted_list_entry(
        f"{repo}:open",
        {
            "at": time.time() - age_s, "data": issues,
            "etag": "v1", "fetched_at": time.time() - age_s, "stale": stale,
        },
    )


# --- (1) one poller owns the reads; wt status/ls/claim serve its cache -------

def test_fresh_reader_serves_poller_snapshot_without_touching_github(monkeypatch):
    """The burst source. `wt status`/`wt ls`/`wt claim` pass fresh=True; within
    the TTL that must cost zero `gh` invocations -- not even the ETag probe."""
    repo = "acme/fresh-serves-snapshot"
    issues = [_issue(repo=repo)]
    _seed_snapshot(repo, issues, age_s=1.0)

    backend = _backend_that_must_not_call_gh(monkeypatch, repo)

    assert backend._list_issues("open", fresh=True) == issues


def test_reader_goes_live_past_ttl_when_no_poll_is_in_flight(monkeypatch):
    """Self-healing: a dead poller must not freeze the queue on old data."""
    import watchtower.github_backend as github_backend

    repo = "acme/ttl-expired-no-poller"
    _seed_snapshot(repo, [_issue(repo=repo)], age_s=github_backend._PERSISTED_LIST_STALE_S + 1)

    backend = github_backend.GitHubIssuesBackend("T", repo=repo)
    live = [_issue(2, "live", repo=repo)]
    calls = {"n": 0}

    def fake_run(args, *, check=True):
        calls["n"] += 1
        return json.dumps(live)

    monkeypatch.setattr(backend, "_run", fake_run)
    monkeypatch.setattr(backend, "_probe_list_change", lambda state, etag: (None, etag))

    assert backend._list_issues("open", fresh=True) == live
    assert calls["n"] == 1


def test_reader_holds_off_while_a_poll_is_in_flight(monkeypatch):
    """Past the TTL, but the poller is fetching the replacement right now:
    N lanes each duplicating that fetch is exactly the burst we removed."""
    import watchtower.github_backend as github_backend

    repo = "acme/poll-in-flight"
    issues = [_issue(repo=repo)]
    _seed_snapshot(repo, issues, age_s=github_backend._PERSISTED_LIST_STALE_S + 1)
    github_backend._mark_poll_started(repo)

    backend = _backend_that_must_not_call_gh(monkeypatch, repo)

    assert backend._list_issues("open", fresh=True) == issues
    assert backend._list_issues("open", fresh=False) == issues


def test_an_abandoned_poll_marker_stops_being_believed(monkeypatch):
    """A poller killed mid-fetch leaves its marker behind. An unbelieved
    expiry bound is the difference between self-healing and a permanent stall."""
    import watchtower.github_backend as github_backend

    repo = "acme/abandoned-marker"
    github_backend._mark_poll_started(repo)
    assert github_backend._poll_in_flight(repo) is True

    marker = github_backend._poll_marker_path(repo)
    old = time.time() - (github_backend._GH_POLL_INFLIGHT_MAX_S + 5)
    os.utime(marker, (old, old))

    assert github_backend._poll_in_flight(repo) is False


def test_poll_marker_is_released_even_when_the_refresh_raises(monkeypatch):
    """A marker leaked by an exception would gate readers for its full
    expiry window every time the poller hit an error."""
    import watchtower.github_backend as github_backend

    repo = "acme/marker-released-on-error"

    def boom(inst, repo_arg):
        raise RuntimeError("gh exploded")

    monkeypatch.setattr(github_backend, "_refresh_persisted_list_cache_states", boom)
    monkeypatch.setattr(github_backend, "_graphql_quota_snapshot", lambda **kw: None)

    with pytest.raises(RuntimeError):
        github_backend.refresh_persisted_list_cache(repo)

    assert github_backend._poll_in_flight(repo) is False


def test_the_poller_is_not_gated_by_its_own_snapshot(monkeypatch):
    """The gate is for readers. If it caught the poller too, the snapshot
    would never be refreshed and the TTL would expire for everyone at once."""
    import watchtower.github_backend as github_backend

    repo = "acme/poller-not-gated"
    _seed_snapshot(repo, [_issue(repo=repo)], age_s=1.0)
    github_backend._mark_poll_started(repo)

    backend = github_backend.GitHubIssuesBackend("T", repo=repo)
    live = [_issue(2, "live", repo=repo)]
    calls = {"n": 0}

    def fake_run(args, *, check=True):
        calls["n"] += 1
        return json.dumps(live)

    monkeypatch.setattr(backend, "_run", fake_run)
    monkeypatch.setattr(backend, "_probe_list_change", lambda state, etag: (None, etag))

    assert backend._list_issues("open", fresh=True, poller=True) == live
    assert calls["n"] == 1


def test_a_write_invalidated_snapshot_still_forces_a_live_read(monkeypatch):
    """Read-your-own-writes outranks quota. A stale-marked snapshot is one
    some process's write already superseded; serving it would regress the
    `_invalidate_list_cache` contract."""
    import watchtower.github_backend as github_backend

    repo = "acme/stale-marked-snapshot"
    _seed_snapshot(repo, [_issue(repo=repo)], age_s=1.0, stale=True)

    backend = github_backend.GitHubIssuesBackend("T", repo=repo)
    live = [_issue(2, "post-write", repo=repo)]
    calls = {"n": 0}

    def fake_run(args, *, check=True):
        calls["n"] += 1
        return json.dumps(live)

    monkeypatch.setattr(backend, "_run", fake_run)
    monkeypatch.setattr(backend, "_probe_list_change", lambda state, etag: (None, etag))

    assert backend._list_issues("open", fresh=True) == live
    assert calls["n"] == 1


def test_strict_readers_still_pay_for_certainty(monkeypatch):
    """claim/close are about to write; a cache hit is not the guarantee they need."""
    import watchtower.github_backend as github_backend

    repo = "acme/strict-unaffected"
    _seed_snapshot(repo, [_issue(repo=repo)], age_s=1.0)
    github_backend._mark_poll_started(repo)

    backend = github_backend.GitHubIssuesBackend("T", repo=repo)
    live = [_issue(2, "live", repo=repo)]
    calls = {"n": 0}

    def fake_run(args, *, check=True):
        calls["n"] += 1
        return json.dumps(live)

    monkeypatch.setattr(backend, "_run", fake_run)
    monkeypatch.setattr(backend, "_probe_list_change", lambda state, etag: (None, etag))

    assert backend._list_issues("open", strict=True) == live
    assert calls["n"] == 1


def test_list_ttl_is_configurable(monkeypatch):
    import watchtower.github_backend as github_backend

    assert github_backend._persisted_list_ttl_s() == 300.0

    monkeypatch.setenv("WATCHTOWER_GH_LIST_TTL_S", "42")
    assert github_backend._persisted_list_ttl_s() == 42.0

    # A typo must not silently turn every read back into a live gh call.
    monkeypatch.setenv("WATCHTOWER_GH_LIST_TTL_S", "not-a-number")
    assert github_backend._persisted_list_ttl_s() == 300.0
    monkeypatch.setenv("WATCHTOWER_GH_LIST_TTL_S", "0")
    assert github_backend._persisted_list_ttl_s() == 300.0


def test_a_shorter_ttl_sends_the_reader_live_sooner(monkeypatch):
    """Both directions on one snapshot, so the TTL is provably what decides.

    The 120s age is deliberate: it is past `_LIST_FETCH_MIN_INTERVAL_S`, so
    the heavy-fetch cap is not what is holding the read back, and inside the
    300s default TTL, so the default still serves cache.
    """
    import watchtower.github_backend as github_backend

    repo = "acme/short-ttl"
    cached = [_issue(repo=repo)]
    live = [_issue(2, "live", repo=repo)]

    def read(ttl=None):
        github_backend._LIST_CACHE.clear()
        _seed_snapshot(repo, cached, age_s=120.0)
        if ttl is None:
            monkeypatch.delenv("WATCHTOWER_GH_LIST_TTL_S", raising=False)
        else:
            monkeypatch.setenv("WATCHTOWER_GH_LIST_TTL_S", ttl)
        backend = github_backend.GitHubIssuesBackend("T", repo=repo)
        monkeypatch.setattr(backend, "_run", lambda args, check=True: json.dumps(live))
        monkeypatch.setattr(
            backend, "_probe_list_change", lambda state, etag: (None, etag)
        )
        return backend._list_issues("open", fresh=True)

    assert read(ttl=None) == cached, "120s is inside the 300s default TTL"
    assert read(ttl="5") == live, "120s is outside a 5s TTL"


# --- (2) per-call list cost -------------------------------------------------

def test_list_query_is_bounded_and_windowed(monkeypatch):
    """--limit caps what one list can cost (1 GraphQL point per 100 nodes),
    and the closed list stays inside the retention window with an explicit
    sort so the rows the limit keeps are the newest ones."""
    import watchtower.github_backend as github_backend

    repo = "acme/list-args"
    backend = github_backend.GitHubIssuesBackend("T", repo=repo)
    seen = []

    def capture(args, *, check=True):
        seen.append(list(args))
        return "[]"

    monkeypatch.setattr(backend, "_run", capture)
    monkeypatch.setattr(backend, "_probe_list_change", lambda state, etag: (None, etag))

    backend._list_issues("open", strict=True)
    backend._list_issues("closed", strict=True)

    open_args, closed_args = seen
    assert open_args[open_args.index("--limit") + 1] == "200"
    assert "--search" not in open_args

    closed_search = closed_args[closed_args.index("--search") + 1]
    assert closed_search.startswith("closed:>=")
    assert "sort:updated-desc" in closed_search


def test_list_limit_is_configurable(monkeypatch):
    import watchtower.github_backend as github_backend

    assert github_backend._list_limit() == 200
    monkeypatch.setenv("WATCHTOWER_GH_LIST_LIMIT", "50")
    assert github_backend._list_limit() == 50
    monkeypatch.setenv("WATCHTOWER_GH_LIST_LIMIT", "junk")
    assert github_backend._list_limit() == 200


def test_list_still_requests_body(monkeypatch):
    """Measured 2026-09-03: `issues(first:100)` costs 1 point with `body` and
    1 point without it -- GraphQL charges per node, not per field. Dropping
    `body` would save nothing and break every ticket's status and
    eligibility, which are parsed out of the `<!-- watchtower -->` metadata
    block stored in the issue body."""
    import watchtower.github_backend as github_backend

    assert "body" in github_backend._LIST_JSON_FIELDS.split(",")
    assert "comments" not in github_backend._LIST_JSON_FIELDS.split(",")


# --- (3) quota accounting ---------------------------------------------------

def test_quota_snapshot_reads_the_in_band_ratelimit_not_the_rest_endpoint(monkeypatch):
    """The bug this whole lane turned on: `gh api rate_limit` reported
    graphql used=0/remaining=5000 at the same instant the authoritative
    in-band `rateLimit` block reported used=1025/remaining=3975. Reading the
    REST endpoint pinned the guard at "plenty of quota" forever."""
    import watchtower.github_backend as github_backend

    calls = []

    class Result:
        returncode = 0
        stdout = json.dumps(
            {"data": {"rateLimit": {"limit": 5000, "cost": 1, "remaining": 3975, "used": 1025}}}
        )
        stderr = ""

    def fake_run(args, **kwargs):
        calls.append(list(args))
        return Result()

    monkeypatch.setattr(github_backend.subprocess, "run", fake_run)

    assert github_backend._graphql_quota_snapshot(force=True) == {
        "limit": 5000, "remaining": 3975, "used": 1025,
    }
    assert calls[0][:3] == ["gh", "api", "graphql"]
    assert not any("rate_limit" in arg for arg in calls[0])


def test_an_unreadable_quota_meter_does_not_break_reads(monkeypatch):
    """An unreadable meter must return None so callers proceed normally --
    quota bookkeeping is not allowed to be what freezes the queue."""
    import watchtower.github_backend as github_backend

    class Failed:
        returncode = 1
        stdout = ""
        stderr = "boom"

    monkeypatch.setattr(github_backend.subprocess, "run", lambda *a, **k: Failed())
    assert github_backend._graphql_quota_snapshot(force=True) is None
    assert github_backend._graphql_rate_limit_remaining() is None


def test_each_poll_logs_the_points_it_spent(tmp_path, monkeypatch):
    import watchtower.github_backend as github_backend

    repo = "acme/quota-logged"
    snapshots = iter([
        {"limit": 5000, "remaining": 4000, "used": 1000},
        {"limit": 5000, "remaining": 3993, "used": 1007},
    ])
    monkeypatch.setattr(
        github_backend, "_graphql_quota_snapshot", lambda **kw: next(snapshots)
    )
    github_backend._GH_POLL_LAST_SNAPSHOT.pop(repo, None)

    def fetched(inst, r):
        github_backend._LIST_FETCH_COUNT["n"] += 2  # open + closed

    monkeypatch.setattr(
        github_backend, "_refresh_persisted_list_cache_states", fetched
    )

    github_backend.refresh_persisted_list_cache(repo)

    lines = (tmp_path / "gh-quota.log").read_text().strip().splitlines()
    record = json.loads(lines[-1])
    assert record["event"] == "poll"
    assert record["repo"] == repo
    assert record["cost"] == 7
    assert record["fetches"] == 2
    assert record["remaining"] == 3993


def test_a_poll_that_fetched_nothing_costs_no_meter_read(tmp_path, monkeypatch):
    """Metering must not become part of the burn it measures. Most sweeps on a
    quiet repo are answered entirely by 304s and have no cost to report; taking
    a `force=True` reading on each side of every one of them was ~4,300 extra
    `gh api graphql` invocations an hour at three repos on a 5s loop."""
    import watchtower.github_backend as github_backend

    repo = "acme/quiet-poll"
    reads = {"n": 0}

    def counted(*, force=False):
        reads["n"] += 1
        return {"limit": 5000, "remaining": 4000, "used": 1000}

    monkeypatch.setattr(github_backend, "_graphql_quota_snapshot", counted)
    monkeypatch.setattr(
        github_backend, "_refresh_persisted_list_cache_states", lambda inst, r: None
    )

    github_backend.refresh_persisted_list_cache(repo)

    assert not (tmp_path / "gh-quota.log").exists()
    assert reads["n"] <= 1, "a fetchless sweep must not pay for a closing reading"


def test_the_opening_reading_is_reused_across_polls(monkeypatch):
    """One meter read per metered sweep, not two: the closing reading of a
    repo's last poll is the opening reading of its next."""
    import watchtower.github_backend as github_backend

    repo = "acme/reused-reading"
    github_backend._GH_POLL_LAST_SNAPSHOT[repo] = {
        "limit": 5000, "remaining": 4000, "used": 1000,
    }
    forced = {"n": 0}

    def counted(*, force=False):
        forced["n"] += 1 if force else 0
        return {"limit": 5000, "remaining": 3990, "used": 1010}

    monkeypatch.setattr(github_backend, "_graphql_quota_snapshot", counted)
    monkeypatch.setattr(
        github_backend,
        "_refresh_persisted_list_cache_states",
        lambda inst, r: github_backend._LIST_FETCH_COUNT.__setitem__(
            "n", github_backend._LIST_FETCH_COUNT["n"] + 1
        ),
    )

    github_backend.refresh_persisted_list_cache(repo)

    assert forced["n"] == 1
    assert github_backend._GH_POLL_LAST_SNAPSHOT[repo]["used"] == 1010


def test_quota_logging_cannot_break_a_poll(tmp_path, monkeypatch):
    """The log lives on a path the poller does not own; an unwritable one
    must be swallowed, not propagated into the poll."""
    import watchtower.github_backend as github_backend

    monkeypatch.setenv(
        "WATCHTOWER_GH_QUOTA_LOG", str(tmp_path / "nope" / "x" / "gh-quota.log")
    )
    monkeypatch.setattr(
        github_backend.Path, "mkdir", lambda *a, **k: (_ for _ in ()).throw(OSError())
    )
    github_backend._log_quota("poll", repo="acme/x", cost=1)  # must not raise


# --- write-invalidation coalescing -----------------------------------------
#
# The TTL gate alone was not enough on the repo the fleet actually works in.
# Measured after it shipped: 6 reader fetches to 0 poller fetches in one 90s
# window on amirfish1/BYM-Finie, because live workers write continuously and
# every write marks the shared snapshot stale, which every reader read as
# "go live". Only the process that wrote actually needs that.

def test_a_reader_waits_out_another_processes_invalidation(monkeypatch):
    """Someone else's write marked the snapshot stale and a refresh is already
    running: this reader must wait for it, not start a duplicate."""
    import watchtower.github_backend as github_backend

    repo = "acme/foreign-write-coalesced"
    issues = [_issue(repo=repo)]
    _seed_snapshot(repo, issues, age_s=1.0, stale=True)
    github_backend._mark_poll_started(repo)

    backend = _backend_that_must_not_call_gh(monkeypatch, repo)

    assert backend._list_issues("open", fresh=True) == issues
    assert backend._list_issues("open", fresh=False) == issues


def test_this_processes_own_write_still_forces_a_live_read(monkeypatch):
    """Read-your-own-writes is the one claim coalescing may never satisfy:
    `dispatch_after_enqueue` calls count_manual_eligible() straight after
    mark_runnable() and would otherwise dispatch off pre-write data."""
    import watchtower.github_backend as github_backend

    repo = "acme/own-write-not-coalesced"
    _seed_snapshot(repo, [_issue(repo=repo)], age_s=1.0)

    backend = github_backend.GitHubIssuesBackend("T", repo=repo)
    live = [_issue(2, "post-write", repo=repo)]
    calls = {"n": 0}

    monkeypatch.setattr(
        backend, "_run", lambda args, check=True: (
            calls.update(n=calls["n"] + 1), json.dumps(live))[1]
    )
    monkeypatch.setattr(backend, "_probe_list_change", lambda state, etag: (None, etag))

    # Warm this process's cache, then let it write.
    assert backend._list_issues("open", fresh=True) == [_issue(repo=repo)]
    github_backend._invalidate_list_cache(repo)
    # A concurrent refresh is running -- irrelevant, we wrote.
    github_backend._mark_poll_started(repo)

    assert backend._list_issues("open", fresh=True) == live
    assert calls["n"] == 1

    # Same for the soft path, which seeds from the snapshot and must not let
    # that seeding wipe the local-write flag.
    github_backend._invalidate_list_cache(repo)
    assert backend._list_issues("open", fresh=False) == live
    assert calls["n"] == 2


def test_the_reader_that_does_go_live_claims_the_fetch(monkeypatch):
    """Readers race each other, not just the poller. Whoever reaches the fetch
    first publishes the marker so its peers coalesce onto it."""
    import watchtower.github_backend as github_backend

    repo = "acme/reader-claims-marker"
    backend = github_backend.GitHubIssuesBackend("T", repo=repo)
    seen = {}

    def fake_run(args, *, check=True):
        # Observed from inside the fetch: a peer arriving now must see it.
        seen["in_flight_during_fetch"] = github_backend._poll_in_flight(repo)
        return "[]"

    monkeypatch.setattr(backend, "_run", fake_run)
    monkeypatch.setattr(backend, "_probe_list_change", lambda state, etag: (None, etag))

    backend._list_issues("open", fresh=True)

    assert seen["in_flight_during_fetch"] is True
    assert github_backend._poll_in_flight(repo) is False, "marker released after"


def test_a_failed_reader_fetch_releases_its_marker(monkeypatch):
    """A marker leaked by a failing fetch would gate every peer for its full
    expiry window -- on a repo that is already erroring, that is the worst
    possible time to also stop serving reads."""
    import watchtower.github_backend as github_backend

    repo = "acme/reader-marker-on-error"
    backend = github_backend.GitHubIssuesBackend("T", repo=repo)

    def boom(args, *, check=True):
        raise github_backend.GitHubBackendError("gh exploded")

    monkeypatch.setattr(backend, "_run", boom)
    monkeypatch.setattr(backend, "_probe_list_change", lambda state, etag: (None, etag))

    with pytest.raises(github_backend.GitHubBackendError):
        backend._list_issues("open", fresh=True)

    assert github_backend._poll_in_flight(repo) is False


def test_only_one_racing_reader_wins_the_fetch(monkeypatch):
    """The claim and the check have to be one operation. `if not in_flight:
    mark()` lets every lane that runs the check in the same instant win it --
    which is the stampede the marker exists to prevent."""
    import watchtower.github_backend as github_backend

    repo = "acme/racing-readers"
    winners = [github_backend._try_claim_poll(repo) for _ in range(8)]

    assert winners.count(True) == 1
    assert github_backend._poll_in_flight(repo) is True


def test_an_expired_marker_does_not_make_the_claim_unwinnable(monkeypatch):
    """A poller killed mid-fetch leaves its marker behind. If that blocked the
    claim forever, no reader could ever fetch again."""
    import watchtower.github_backend as github_backend

    repo = "acme/expired-marker-claim"
    assert github_backend._try_claim_poll(repo) is True
    assert github_backend._try_claim_poll(repo) is False

    marker = github_backend._poll_marker_path(repo)
    old = time.time() - (github_backend._GH_POLL_INFLIGHT_MAX_S + 5)
    os.utime(marker, (old, old))

    assert github_backend._try_claim_poll(repo) is True

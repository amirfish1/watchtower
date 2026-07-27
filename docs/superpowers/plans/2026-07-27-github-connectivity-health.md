# GitHub Connectivity Health Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give WatchTower a single global "is GitHub reachable" signal, driven by the real polling GitHub-backed queues already do, with escalating backoff during sustained outages, a forced-recheck escape hatch, and visible alerting in the CCC dashboard and `wt status`.

**Architecture:** A new persisted state file (`~/.watchtower/gh-connectivity.json`) is written from inside `github_backend.py`'s `_list_issues()` at the exact point a live `gh issue list` call succeeds or fails. `health.py` reads that file to compute a global alert (sustained failure ≥ 5 minutes). `dashboard.py` and `cli.py` surface the alert through the existing red "beacon" UI element and a new `wt status` warning line, respectively. A new `wt gh recheck` command forces a live attempt that bypasses the backoff.

**Tech Stack:** Python 3, pytest, existing WatchTower `_FileLock` cross-process file locking, no new dependencies.

## Global Constraints

- State file: `~/.watchtower/gh-connectivity.json`, override via `$WATCHTOWER_GH_CONNECTIVITY_FILE` (matches the existing `$WATCHTOWER_STORE`/`$WATCHTOWER_CONFIG_FILE`/`$WATCHTOWER_ACTIVITY_LOG` convention).
- State schema: `{"last_success_at": str|null, "broken_since": str|null, "consecutive_failures": int, "next_retry_at": str|null, "last_error": str}`. Timestamps are UTC `%Y-%m-%dT%H:%M:%SZ`, matching every other WatchTower timestamp.
- Backoff ladder: `next_retry_delay = min(_GH_BACKOFF_CAP_S, _GH_BACKOFF_BASE_S * 2 ** (consecutive_failures - 1))`, with `_GH_BACKOFF_BASE_S = 60.0` and `_GH_BACKOFF_CAP_S = 600.0` (60s → 120s → 240s → 480s → capped at 600s).
- Alert threshold: `GH_ALERT_THRESHOLD_S = 300` (5 minutes) — `alert` is only `True` when `broken_since` is set and `now - broken_since >= 300`.
- Success-write throttle: `_GH_SUCCESS_WRITE_THROTTLE_S = 30.0` — a healthy poll only rewrites the state file at most once per 30s, except a recovery (clearing `broken_since`) which always writes immediately.
- `strict=True` callers (existing parameter on `_list_issues`/`list_items`, used today by claim/close) always bypass the new persisted backoff — unchanged behavior, and the mechanism `wt gh recheck` reuses to force a live attempt.
- `fresh=True` callers (`wt status`, `wt ls`) respect the persisted backoff during a known outage — a behavior change from today, where `fresh=True` always forced revalidation.
- This is a shared git clone (other sessions may have in-flight uncommitted work). Commit steps use `git commit --only <exact paths> -m "..."`, never `git add -A`/`git commit -m` against the shared index — see the repo's multi-agent git hygiene rules.

---

## File Structure

- `watchtower/github_backend.py` — **modify**: add the connectivity state file read/write/record/gate functions; wire them into `_list_issues()`.
- `watchtower/health.py` — **modify**: add `github_connectivity()`.
- `watchtower/dashboard.py` — **modify**: `status_payload()` includes the connectivity block; `render_index()` extends the beacon condition and adds a fleet-line warning.
- `watchtower/cli.py` — **modify**: `cmd_status()` prints a warning line; new `cmd_gh()` + `wt gh recheck` argparse wiring.
- `tests/test_github_backend.py` — **modify**: extend `_reload_isolated()` for test isolation; add backoff/success/failure/recheck tests.
- `tests/test_health.py` — **modify**: add `github_connectivity()` tests.
- `tests/test_dashboard_gh_alert.py` — **create**: beacon + fleet-line rendering tests (new file, following the existing one-file-per-concern convention like `test_dashboard_mobile.py`).

---

### Task 1: Connectivity state file + backoff plumbing in `github_backend.py`

**Files:**
- Modify: `watchtower/github_backend.py:1-16` (imports), `watchtower/github_backend.py:69` (after `_LIST_ERROR_BACKOFF`), `watchtower/github_backend.py:697-734` (`_list_issues` wiring)
- Test: `tests/test_github_backend.py`

**Interfaces:**
- Produces (used by Task 2, 3, 4): `github_backend._load_connectivity() -> Dict[str, Any]`, `github_backend._GH_BACKOFF_BASE_S: float`, `github_backend._GH_BACKOFF_CAP_S: float`, module-level `github_backend._LIST_CACHE: Dict[str, Dict[str, Any]]` (already exists, reused by tests).

- [ ] **Step 1: Add `$WATCHTOWER_GH_CONNECTIVITY_FILE` isolation to the shared test fixture**

Modify `tests/test_github_backend.py`'s `_reload_isolated`:

```python
def _reload_isolated(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_CONFIG_FILE", str(tmp_path / "config.json"))
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(tmp_path / "activity.log"))
    monkeypatch.setenv(
        "WATCHTOWER_CCC_SPAWN_DEFAULTS_FILE", str(tmp_path / "no-ccc-spawn-defaults.json")
    )
    monkeypatch.setenv(
        "WATCHTOWER_GH_CONNECTIVITY_FILE", str(tmp_path / "gh-connectivity.json")
    )
    import watchtower.config as config
    import watchtower.github_backend as github_backend
    import watchtower.queue as q

    importlib.reload(config)
    # Reset github_backend's module-level `_list_issues` cache (WT-87): every
    # test here reuses the same "owner/repo" placeholder, so a stale entry
    # from a prior test would otherwise leak into this one within its TTL.
    importlib.reload(github_backend)
    importlib.reload(q)
    # Pretend the one-time GitHub drain migration already ran (it has, on any
    # real install, long before these code paths run). Tests that exercise the
    # migration itself remove this marker first.
    config.GH_DRAIN_MIGRATION_MARKER.parent.mkdir(parents=True, exist_ok=True)
    config.GH_DRAIN_MIGRATION_MARKER.write_text("{}\n")
    return config, q
```

(Only the new `monkeypatch.setenv("WATCHTOWER_GH_CONNECTIVITY_FILE", ...)` line is added.)

- [ ] **Step 2: Write the failing tests for backoff escalation, cold-process gating, and strict bypass**

Append to `tests/test_github_backend.py`:

```python
# ==================================================== GitHub connectivity health

def test_gh_connectivity_backoff_escalates_and_resets_on_success(tmp_path, monkeypatch):
    """First failure backs off by the base delay; a further consecutive
    failure doubles it up to the cap; a success resets to the base again."""
    config, q = _reload_isolated(tmp_path, monkeypatch)
    import watchtower.github_backend as github_backend
    import time as _time

    monkeypatch.setattr(github_backend, "_GH_BACKOFF_BASE_S", 0.05)
    monkeypatch.setattr(github_backend, "_GH_BACKOFF_CAP_S", 0.15)

    backend = github_backend.GitHubIssuesBackend("T", repo="acme/backoff-test")

    def failing_run(args, *, check=True):
        raise github_backend.GitHubBackendError("gh auth unavailable")

    monkeypatch.setattr(backend, "_run", failing_run)

    with pytest.raises(github_backend.GitHubBackendError):
        backend._list_issues()
    state = github_backend._load_connectivity()
    assert state["consecutive_failures"] == 1
    first_broken_since = state["broken_since"]
    assert first_broken_since is not None
    first_next_retry = github_backend._parse_iso(state["next_retry_at"])

    _time.sleep(0.06)  # cross the first backoff window
    github_backend._LIST_CACHE.clear()  # simulate a fresh process: cold cache

    with pytest.raises(github_backend.GitHubBackendError):
        backend._list_issues()
    state = github_backend._load_connectivity()
    assert state["consecutive_failures"] == 2
    assert state["broken_since"] == first_broken_since  # unchanged: still the same outage
    second_next_retry = github_backend._parse_iso(state["next_retry_at"])
    assert second_next_retry > first_next_retry  # escalated

    _time.sleep(0.11)  # cross the doubled (0.10s) backoff window
    github_backend._LIST_CACHE.clear()

    def succeeding_run(args, *, check=True):
        return json.dumps([])

    monkeypatch.setattr(backend, "_run", succeeding_run)
    assert backend._list_issues() == []
    state = github_backend._load_connectivity()
    assert state["consecutive_failures"] == 0
    assert state["broken_since"] is None
    assert state["next_retry_at"] is None
    assert state["last_success_at"] is not None


def test_gh_connectivity_backoff_blocks_cold_process_until_it_expires(tmp_path, monkeypatch):
    """A failure recorded by one call must block a *different, cache-cold*
    backend instance (simulating a fresh `wt status` process) from
    re-attempting `gh` until the persisted backoff window passes."""
    config, q = _reload_isolated(tmp_path, monkeypatch)
    import watchtower.github_backend as github_backend

    monkeypatch.setattr(github_backend, "_GH_BACKOFF_BASE_S", 0.2)

    backend = github_backend.GitHubIssuesBackend("T", repo="acme/cold-backoff-test")
    calls = {"n": 0}

    def failing_run(args, *, check=True):
        calls["n"] += 1
        raise github_backend.GitHubBackendError("gh auth unavailable")

    monkeypatch.setattr(backend, "_run", failing_run)
    with pytest.raises(github_backend.GitHubBackendError):
        backend._list_issues()
    assert calls["n"] == 1

    github_backend._LIST_CACHE.clear()  # fresh-process-like: cold in-memory cache
    cold = github_backend.GitHubIssuesBackend("T", repo="acme/cold-backoff-test")
    monkeypatch.setattr(cold, "_run", failing_run)
    with pytest.raises(github_backend.GitHubBackendError) as excinfo:
        cold._list_issues()
    assert calls["n"] == 1  # no new `gh` invocation -- served from persisted backoff
    assert excinfo.value.cached is True


def test_gh_connectivity_strict_bypasses_persisted_backoff(tmp_path, monkeypatch):
    """`strict=True` (claim/close today, `wt gh recheck` after Task 4) must
    force a live attempt immediately, regardless of an active backoff."""
    config, q = _reload_isolated(tmp_path, monkeypatch)
    import watchtower.github_backend as github_backend

    monkeypatch.setattr(github_backend, "_GH_BACKOFF_BASE_S", 60.0)

    backend = github_backend.GitHubIssuesBackend("T", repo="acme/recheck-test")

    def failing_run(args, *, check=True):
        raise github_backend.GitHubBackendError("gh auth unavailable")

    monkeypatch.setattr(backend, "_run", failing_run)
    with pytest.raises(github_backend.GitHubBackendError):
        backend._list_issues()

    calls = {"n": 0}

    def succeeding_run(args, *, check=True):
        calls["n"] += 1
        return json.dumps([])

    monkeypatch.setattr(backend, "_run", succeeding_run)
    github_backend._LIST_CACHE.clear()  # fresh-process-like: cold in-memory cache

    result = backend._list_issues(fresh=True, strict=True)
    assert result == []
    assert calls["n"] == 1
    state = github_backend._load_connectivity()
    assert state["broken_since"] is None  # the successful recheck cleared it


def test_gh_connectivity_stale_data_fallback_still_records_failure(tmp_path, monkeypatch):
    """WT-87's stale-data fallback returns cached good data without raising
    to the caller -- the connectivity state must still record the failure,
    since this is exactly the "silently degrading" case a human can't see
    any other way."""
    config, q = _reload_isolated(tmp_path, monkeypatch)
    import watchtower.github_backend as github_backend

    monkeypatch.setattr(github_backend, "_LIST_CACHE_TTL", 0.05)
    monkeypatch.setattr(github_backend, "_LIST_ERROR_BACKOFF", 60.0)

    backend = github_backend.GitHubIssuesBackend("T", repo="acme/stale-fallback-test")
    good_issue = {
        "number": 1, "title": "t", "body": "", "state": "OPEN",
        "url": "https://github.com/acme/stale-fallback-test/issues/1",
        "assignees": [], "labels": [], "createdAt": "2026-07-01T00:00:00Z",
        "updatedAt": "2026-07-01T00:00:00Z", "closedAt": None,
    }
    monkeypatch.setattr(backend, "_run", lambda args, *, check=True: json.dumps([good_issue]))
    _no_etag_probe(monkeypatch, backend)
    assert backend._list_issues() == [good_issue]

    def failing_run(args, *, check=True):
        raise github_backend.GitHubBackendError("API rate limit already exceeded")

    monkeypatch.setattr(backend, "_run", failing_run)
    import time as _time
    _time.sleep(0.06)  # expire the TTL so the next call actually attempts gh
    stale = backend._list_issues()
    assert stale == [good_issue]  # served silently -- no exception reaches this caller

    state = github_backend._load_connectivity()
    assert state["broken_since"] is not None
    assert state["last_error"] == "API rate limit already exceeded"
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `python3 -m pytest tests/test_github_backend.py -k gh_connectivity -v`
Expected: FAIL — `AttributeError: module 'watchtower.github_backend' has no attribute '_load_connectivity'` (or `_GH_BACKOFF_BASE_S`).

- [ ] **Step 4: Add the connectivity imports**

In `watchtower/github_backend.py`, replace the top import block:

```python
from __future__ import annotations

import json
import re
import subprocess
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from .queue import UNCLAIMABLE_READINESS
```

with:

```python
from __future__ import annotations

import json
import os
import re
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from .queue import UNCLAIMABLE_READINESS, _FileLock
```

- [ ] **Step 5: Add the connectivity state functions**

In `watchtower/github_backend.py`, immediately after the line `_LIST_ERROR_BACKOFF = 60.0`, insert:

```python

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
    _save_connectivity(state)


def _gh_backoff_active() -> "tuple[bool, Dict[str, Any]]":
    """Whether a live GitHub fetch should be skipped right now, plus the
    current persisted state (so the caller can reuse it for the error
    message without a second read)."""
    state = _load_connectivity()
    next_retry = _parse_iso(state.get("next_retry_at"))
    active = next_retry is not None and datetime.now(timezone.utc) < next_retry
    return active, state
```

- [ ] **Step 6: Wire the gate and the success/failure recording into `_list_issues`**

In `watchtower/github_backend.py`, inside `_list_issues`, replace:

```python
                if unchanged:
                    cached["at"] = now  # unchanged is as good as re-fetched
                    return cached["data"]
        try:
            args = [
                "issue", "list",
                *self._repo_args(),
                "--state", state,
                "--json", "number,title,body,state,url,assignees,labels,comments,createdAt,updatedAt,closedAt",
                "--limit", "1000",
            ]
            if state == "closed":
                cutoff = (
                    datetime.now(timezone.utc)
                    - timedelta(days=COMPLETED_ISSUE_RETENTION_DAYS)
                ).date().isoformat()
                args.extend(["--search", f"closed:>={cutoff}"])
            raw = self._run(args)
            try:
                data = json.loads(raw or "[]")
            except json.JSONDecodeError as exc:
                raise GitHubBackendError("gh issue list returned invalid JSON") from exc
            if not isinstance(data, list):
                raise GitHubBackendError("gh issue list returned a non-list JSON value")
            result = [issue for issue in data if isinstance(issue, dict)]
        except GitHubBackendError as exc:
            prev_data = cached.get("data") if cached else None
            _LIST_CACHE[key] = {
                # The stale data keeps its own validator; nothing probes with
                # it while an error is recorded, so it cannot mislead.
                "at": now, "data": prev_data, "error": exc,
                "etag": (cached.get("etag") or "") if cached else "",
            }
            if prev_data is not None and not strict:
                return prev_data
            raise
        _LIST_CACHE[key] = {"at": now, "data": result, "error": None, "etag": etag}
        return result
```

with:

```python
                if unchanged:
                    cached["at"] = now  # unchanged is as good as re-fetched
                    return cached["data"]
        if not strict:
            backoff_active, conn_state = _gh_backoff_active()
            if backoff_active:
                if cached is not None and cached.get("data") is not None:
                    return cached["data"]
                raise GitHubBackendError(
                    str(conn_state.get("last_error") or "GitHub unreachable (backoff)"),
                    cached=True,
                )
        try:
            args = [
                "issue", "list",
                *self._repo_args(),
                "--state", state,
                "--json", "number,title,body,state,url,assignees,labels,comments,createdAt,updatedAt,closedAt",
                "--limit", "1000",
            ]
            if state == "closed":
                cutoff = (
                    datetime.now(timezone.utc)
                    - timedelta(days=COMPLETED_ISSUE_RETENTION_DAYS)
                ).date().isoformat()
                args.extend(["--search", f"closed:>={cutoff}"])
            raw = self._run(args)
            try:
                data = json.loads(raw or "[]")
            except json.JSONDecodeError as exc:
                raise GitHubBackendError("gh issue list returned invalid JSON") from exc
            if not isinstance(data, list):
                raise GitHubBackendError("gh issue list returned a non-list JSON value")
            result = [issue for issue in data if isinstance(issue, dict)]
        except GitHubBackendError as exc:
            _record_gh_failure(str(exc))
            prev_data = cached.get("data") if cached else None
            _LIST_CACHE[key] = {
                # The stale data keeps its own validator; nothing probes with
                # it while an error is recorded, so it cannot mislead.
                "at": now, "data": prev_data, "error": exc,
                "etag": (cached.get("etag") or "") if cached else "",
            }
            if prev_data is not None and not strict:
                return prev_data
            raise
        _record_gh_success()
        _LIST_CACHE[key] = {"at": now, "data": result, "error": None, "etag": etag}
        return result
```

- [ ] **Step 7: Run the tests to verify they pass**

Run: `python3 -m pytest tests/test_github_backend.py -v`
Expected: PASS — all tests, including the pre-existing WT-87 cache/backoff/strict/ETag tests (this confirms the new gate didn't regress them).

- [ ] **Step 8: Commit**

```bash
git commit --only watchtower/github_backend.py tests/test_github_backend.py \
  -m "feat: persist GitHub connectivity state with escalating backoff"
```

---

### Task 2: `health.github_connectivity()`

**Files:**
- Modify: `watchtower/health.py` (append after `all_status`, currently ending at line 249)
- Test: `tests/test_health.py`

**Interfaces:**
- Consumes: `github_backend._load_connectivity() -> Dict[str, Any]` (Task 1), `health._age_seconds`, `health._fmt_age` (already exist in `health.py`).
- Produces (used by Task 3, 4): `health.github_connectivity(now: Optional[datetime] = None) -> Dict[str, Any]` returning `{"alert": bool, "broken_since": str|None, "outage_duration_s": int|None, "outage_duration": str|None, "consecutive_failures": int, "last_error": str}`. `health.GH_ALERT_THRESHOLD_S: int`.

- [ ] **Step 1: Write the failing tests**

`tests/test_health.py` currently starts with:

```python
"""Unit tests for watchtower.health.queue_status's claimable-depth gating."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from watchtower import health
```

Add `import json` (the new tests need it; `datetime`/`timedelta`/`timezone` are already imported):

```python
"""Unit tests for watchtower.health.queue_status's claimable-depth gating."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from watchtower import health
```

Then append to the end of the file:

```python
def test_github_connectivity_healthy_by_default(tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_GH_CONNECTIVITY_FILE", str(tmp_path / "gh-connectivity.json"))
    result = health.github_connectivity()
    assert result["alert"] is False
    assert result["broken_since"] is None
    assert result["outage_duration_s"] is None
    assert result["consecutive_failures"] == 0


def test_github_connectivity_alert_false_under_threshold_true_at_it(tmp_path, monkeypatch):
    path = tmp_path / "gh-connectivity.json"
    monkeypatch.setenv("WATCHTOWER_GH_CONNECTIVITY_FILE", str(path))
    now = datetime(2026, 7, 27, 12, 0, 0, tzinfo=timezone.utc)

    just_under = (now - timedelta(seconds=299)).strftime("%Y-%m-%dT%H:%M:%SZ")
    path.write_text(json.dumps({
        "last_success_at": None, "broken_since": just_under,
        "consecutive_failures": 3, "next_retry_at": None,
        "last_error": "gh auth unavailable",
    }))
    result = health.github_connectivity(now=now)
    assert result["alert"] is False
    assert result["outage_duration_s"] == 299

    at_threshold = (now - timedelta(seconds=300)).strftime("%Y-%m-%dT%H:%M:%SZ")
    path.write_text(json.dumps({
        "last_success_at": None, "broken_since": at_threshold,
        "consecutive_failures": 5, "next_retry_at": None,
        "last_error": "gh auth unavailable",
    }))
    result = health.github_connectivity(now=now)
    assert result["alert"] is True
    assert result["outage_duration_s"] == 300
    assert result["outage_duration"] == "5m"
    assert result["last_error"] == "gh auth unavailable"
    assert result["consecutive_failures"] == 5
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python3 -m pytest tests/test_health.py -k github_connectivity -v`
Expected: FAIL — `AttributeError: module 'watchtower.health' has no attribute 'github_connectivity'`.

- [ ] **Step 3: Implement `github_connectivity`**

Append to the end of `watchtower/health.py` (after `all_status`):

```python

# GitHub connectivity alert threshold (2026-07-27 design): a single failed
# poll must not flip a global "GitHub is down" banner -- only sustained
# failure does. Time-since-`broken_since`, not a failure count, for the same
# reason `stuck` above is measured by age: it's directly displayable ("down
# for 6m") and doesn't depend on how often polling happens to run.
GH_ALERT_THRESHOLD_S = 300


def github_connectivity(now: Optional[datetime] = None) -> Dict[str, Any]:
    """Global "is GitHub reachable" signal, computed from the real polling
    every GitHub-backed queue already does (see
    ``github_backend._record_gh_success``/``_record_gh_failure``) rather than
    a synthetic ping, so it can never report healthy while the queues'
    actual fetches are failing."""
    from . import github_backend
    now = now or datetime.now(timezone.utc)
    state = github_backend._load_connectivity()
    broken_since = state.get("broken_since")
    outage_s = _age_seconds(broken_since, now) if broken_since else None
    alert = outage_s is not None and outage_s >= GH_ALERT_THRESHOLD_S
    return {
        "alert": alert,
        "broken_since": broken_since,
        "outage_duration_s": outage_s,
        "outage_duration": _fmt_age(outage_s) if outage_s is not None else None,
        "consecutive_failures": int(state.get("consecutive_failures") or 0),
        "last_error": str(state.get("last_error") or ""),
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `python3 -m pytest tests/test_health.py -v`
Expected: PASS — all tests, including pre-existing `queue_status` tests (confirms no regression).

- [ ] **Step 5: Commit**

```bash
git commit --only watchtower/health.py tests/test_health.py \
  -m "feat: add health.github_connectivity() sustained-failure signal"
```

---

### Task 3: Surface the alert in the CCC dashboard

**Files:**
- Modify: `watchtower/dashboard.py:105-119` (`status_payload`), `watchtower/dashboard.py:721-756` (`render_index`)
- Test: `tests/test_dashboard_gh_alert.py` (new file)

**Interfaces:**
- Consumes: `health.github_connectivity() -> Dict[str, Any]` (Task 2).
- Produces: `status_payload()` return value gains a `"github"` key; `render_index(payload, chat_rows=None)` (unchanged signature) reads `payload.get("github")`.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_dashboard_gh_alert.py`:

```python
"""GitHub connectivity alert surfaced on the CCC dashboard header (2026-07-27
design): the existing "beacon" element that already turns red for stuck
queues now also turns red when GitHub has been unreachable for a sustained
period, with a short text line explaining why."""

from __future__ import annotations


def _base_payload(github=None):
    return {
        "queues": [
            {"queue": "A", "depth": 0, "state": "clear", "auto_drain": True,
             "stuck": False, "workers_live": 0, "in_progress": 0,
             "drain_rate_per_min": 0, "eta_human": "empty"},
        ],
        "workers": [],
        "github": github or {"alert": False},
    }


def test_beacon_is_not_alert_when_github_healthy_and_nothing_stuck():
    import watchtower.dashboard as dashboard

    page = dashboard.render_index(_base_payload(), chat_rows=[])
    assert 'class="beacon alert"' not in page


def test_beacon_turns_alert_when_github_unreachable_even_with_no_stuck_queues():
    import watchtower.dashboard as dashboard

    payload = _base_payload(github={
        "alert": True, "outage_duration": "6m", "outage_duration_s": 360,
        "last_error": "gh auth login required", "consecutive_failures": 4,
        "broken_since": "2026-07-27T00:00:00Z",
    })
    page = dashboard.render_index(payload, chat_rows=[])
    assert 'class="beacon alert"' in page
    assert "GitHub unreachable 6m" in page
    assert "gh auth login required" in page


def test_beacon_ignores_a_github_alert_that_is_false():
    import watchtower.dashboard as dashboard

    payload = _base_payload(github={"alert": False, "outage_duration": None})
    page = dashboard.render_index(payload, chat_rows=[])
    assert 'class="beacon alert"' not in page
    assert "GitHub unreachable" not in page


def test_render_index_tolerates_a_payload_with_no_github_key():
    """Callers that built a payload before this feature (or any test payload
    that never set one) must not crash render_index."""
    import watchtower.dashboard as dashboard

    payload = {
        "queues": [
            {"queue": "A", "depth": 0, "state": "clear", "auto_drain": True,
             "stuck": False, "workers_live": 0, "in_progress": 0,
             "drain_rate_per_min": 0, "eta_human": "empty"},
        ],
        "workers": [],
    }
    page = dashboard.render_index(payload, chat_rows=[])
    assert 'class="beacon alert"' not in page


def test_status_payload_includes_github_connectivity_block(tmp_path, monkeypatch):
    import importlib
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_CONFIG_FILE", str(tmp_path / "config.json"))
    monkeypatch.setenv("WATCHTOWER_GH_CONNECTIVITY_FILE", str(tmp_path / "gh-connectivity.json"))
    import watchtower.config as config
    import watchtower.health as health
    import watchtower.dashboard as dashboard
    importlib.reload(config)
    importlib.reload(health)
    importlib.reload(dashboard)

    payload = dashboard.status_payload()
    assert "github" in payload
    assert payload["github"]["alert"] is False
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python3 -m pytest tests/test_dashboard_gh_alert.py -v`
Expected: FAIL — `KeyError: 'github'` or assertion failures (beacon never turns alert for a GitHub-only signal yet).

- [ ] **Step 3: Add the connectivity block to `status_payload`**

In `watchtower/dashboard.py`, replace:

```python
def status_payload(stuck_minutes: int = health.STUCK_MINUTES) -> Dict[str, Any]:
    """Combined queue health + per-queue worker tally + the worker roster.

    One pass over workers (``worker_counts``) annotates every queue row, so the
    dashboard never probes liveness once per queue.
    """
    rows = health.all_status(stuck_minutes=stuck_minutes)
    counts = workers.worker_counts()
    for r in rows:
        wc = counts.get(r["queue"], {"total": 0, "live": 0})
        r["workers_total"] = wc["total"]
        r["workers_live"] = wc["live"]
    wrows = workers.list_workers(prune=False)
    workers.annotate_activity(wrows, q.list_items())
    return {"queues": rows, "workers": wrows}
```

with:

```python
def status_payload(stuck_minutes: int = health.STUCK_MINUTES) -> Dict[str, Any]:
    """Combined queue health + per-queue worker tally + the worker roster.

    One pass over workers (``worker_counts``) annotates every queue row, so the
    dashboard never probes liveness once per queue.
    """
    rows = health.all_status(stuck_minutes=stuck_minutes)
    counts = workers.worker_counts()
    for r in rows:
        wc = counts.get(r["queue"], {"total": 0, "live": 0})
        r["workers_total"] = wc["total"]
        r["workers_live"] = wc["live"]
    wrows = workers.list_workers(prune=False)
    workers.annotate_activity(wrows, q.list_items())
    return {"queues": rows, "workers": wrows, "github": health.github_connectivity()}
```

- [ ] **Step 4: Extend the beacon condition and add the fleet-line warning**

In `watchtower/dashboard.py`, inside `render_index`, replace:

```python
    rows: List[Dict[str, Any]] = sorted(
        payload["queues"], key=lambda r: r.get("state") != "stuck"
    )
    wkrs: List[Dict[str, Any]] = payload["workers"]

    any_stuck = any(r.get("state") == "stuck" for r in rows)
    stuck_n = sum(1 for r in rows if r.get("state") == "stuck")
    live_workers = sum(1 for w in wkrs if w.get("alive"))

    beacon_cls = "beacon alert" if any_stuck else ("beacon dim" if not rows else "beacon")
    fleet_bits = [f'<span class="mono">{len(rows)}</span> queue{"" if len(rows)==1 else "s"}']
    if stuck_n:
        fleet_bits.append(f'<span class="hot mono">{stuck_n} stuck</span>')
    fleet_bits.append(
        f'<span class="ok mono">{live_workers}</span> '
        f'worker{"" if live_workers == 1 else "s"} live'
    )
    fleet = " · ".join(fleet_bits)
```

with:

```python
    rows: List[Dict[str, Any]] = sorted(
        payload["queues"], key=lambda r: r.get("state") != "stuck"
    )
    wkrs: List[Dict[str, Any]] = payload["workers"]
    gh: Dict[str, Any] = payload.get("github") or {}
    gh_alert = bool(gh.get("alert"))

    any_stuck = any(r.get("state") == "stuck" for r in rows)
    stuck_n = sum(1 for r in rows if r.get("state") == "stuck")
    live_workers = sum(1 for w in wkrs if w.get("alive"))

    beacon_cls = (
        "beacon alert" if (any_stuck or gh_alert)
        else ("beacon dim" if not rows else "beacon")
    )
    fleet_bits = [f'<span class="mono">{len(rows)}</span> queue{"" if len(rows)==1 else "s"}']
    if stuck_n:
        fleet_bits.append(f'<span class="hot mono">{stuck_n} stuck</span>')
    if gh_alert:
        gh_dur = html.escape(str(gh.get("outage_duration") or "?"))
        gh_err = html.escape(str(gh.get("last_error") or "")[:60])
        gh_text = f"GitHub unreachable {gh_dur}" + (f" — {gh_err}" if gh_err else "")
        fleet_bits.append(f'<span class="hot mono">{gh_text}</span>')
    fleet_bits.append(
        f'<span class="ok mono">{live_workers}</span> '
        f'worker{"" if live_workers == 1 else "s"} live'
    )
    fleet = " · ".join(fleet_bits)
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `python3 -m pytest tests/test_dashboard_gh_alert.py tests/test_dashboard_mobile.py -v`
Expected: PASS — the new file, plus `test_dashboard_mobile.py` unaffected (confirms the `payload.get("github")` default doesn't break existing payloads without that key).

- [ ] **Step 6: Commit**

```bash
git commit --only watchtower/dashboard.py tests/test_dashboard_gh_alert.py \
  -m "feat: surface GitHub connectivity alert on the CCC dashboard beacon"
```

---

### Task 4: `wt status` warning line + `wt gh recheck` command

**Files:**
- Modify: `watchtower/cli.py` (`cmd_status`, new `cmd_gh`, argparse wiring)
- Test: `tests/test_github_backend.py`

**Interfaces:**
- Consumes: `health.github_connectivity()` (Task 2), `q._github_projects() -> List[str]` and `q._github_backend_for_project(name) -> Optional[GitHubIssuesBackend]` (both already exist in `watchtower/queue.py`), `backend.list_items(fresh=True, strict=True)` (already exists, Task 1 makes `strict=True` bypass backoff).
- Produces: `wt gh recheck [--json]` CLI command; `wt status` prints a one-line warning above the table when `github_connectivity()["alert"]` is true.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_github_backend.py`:

```python
def test_cli_gh_recheck_forces_live_check_and_reports_per_queue(tmp_path, monkeypatch, capsys):
    config, q = _reload_isolated(tmp_path, monkeypatch)
    import watchtower.github_backend as github_backend
    from watchtower.cli import main

    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "owner/repo")

    def succeed(self, args, *, check=True):
        return "[]"

    monkeypatch.setattr(github_backend.GitHubIssuesBackend, "_run", succeed)

    assert main(["gh", "recheck"]) == 0
    out = capsys.readouterr().out
    assert "GHI: ok" in out
    assert "GitHub connectivity: healthy" in out


def test_cli_gh_recheck_bypasses_backoff_after_a_prior_failure(tmp_path, monkeypatch, capsys):
    config, q = _reload_isolated(tmp_path, monkeypatch)
    import watchtower.github_backend as github_backend
    from watchtower.cli import main

    monkeypatch.setattr(github_backend, "_GH_BACKOFF_BASE_S", 3600.0)  # would not expire mid-test
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "owner/repo")

    def fail(self, args, *, check=True):
        raise github_backend.GitHubBackendError("gh auth unavailable")

    monkeypatch.setattr(github_backend.GitHubIssuesBackend, "_run", fail)
    assert q.list_items() == []  # records the failure and sets a long backoff

    def succeed(self, args, *, check=True):
        return "[]"

    monkeypatch.setattr(github_backend.GitHubIssuesBackend, "_run", succeed)
    github_backend._LIST_CACHE.clear()  # fresh-process-like: cold in-memory cache

    assert main(["gh", "recheck"]) == 0
    out = capsys.readouterr().out
    assert "GHI: ok" in out
    assert "GitHub connectivity: healthy" in out


def test_cli_status_prints_warning_when_github_alert_active(tmp_path, monkeypatch, capsys):
    config, q = _reload_isolated(tmp_path, monkeypatch)
    import watchtower.github_backend as github_backend
    from watchtower.cli import main

    state = github_backend._empty_connectivity()
    state["broken_since"] = "2026-01-01T00:00:00Z"  # far enough in the past to be >= threshold
    state["last_error"] = "gh auth login required"
    state["consecutive_failures"] = 9
    github_backend._save_connectivity(state)

    assert main(["status"]) == 0
    out = capsys.readouterr().out
    assert "GitHub unreachable" in out
    assert "gh auth login required" in out
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python3 -m pytest tests/test_github_backend.py -k "gh_recheck or status_prints_warning" -v`
Expected: FAIL — `argparse` error (`invalid choice: 'gh'`) for the recheck tests, and the warning assertion fails for the status test.

- [ ] **Step 3: Add the warning line to `cmd_status`**

In `watchtower/cli.py`, replace:

```python
def cmd_status(args: argparse.Namespace) -> int:
    # fresh=True: a human asking for status gets current state, never a cached
    # snapshot. On a GitHub queue that is an ETag revalidation, so the usual
    # answer is a ~0.5s 304 that costs no rate limit.
    rows = health.all_status(
        project=args.queue, stuck_minutes=args.stuck_minutes, fresh=True
    )
    if args.json:
        print(json.dumps(rows, indent=2))
    else:
        _print_status(rows)
    return 0
```

with:

```python
def cmd_status(args: argparse.Namespace) -> int:
    # fresh=True: a human asking for status gets current state, never a cached
    # snapshot. On a GitHub queue that is an ETag revalidation, so the usual
    # answer is a ~0.5s 304 that costs no rate limit.
    rows = health.all_status(
        project=args.queue, stuck_minutes=args.stuck_minutes, fresh=True
    )
    if args.json:
        print(json.dumps(rows, indent=2))
    else:
        gh = health.github_connectivity()
        if gh.get("alert"):
            msg = f"⚠ GitHub unreachable for {gh.get('outage_duration') or '?'}"
            if gh.get("last_error"):
                msg += f" — {gh['last_error']}"
            print(msg)
        _print_status(rows)
    return 0
```

- [ ] **Step 4: Add `cmd_gh`**

In `watchtower/cli.py`, immediately after the end of `cmd_logs` (the function ending with `return 0` right before `def cmd_outbox`), insert:

```python
def cmd_gh(args: argparse.Namespace) -> int:
    """GitHub-backend diagnostics: `wt gh recheck [--json]`.

    Forces a live `gh issue list` for every GitHub-backed queue, bypassing
    the persisted connectivity backoff -- the explicit "I fixed it, check
    now" action instead of waiting out the escalated retry window.
    """
    sub = getattr(args, "gh_command", None)
    if sub != "recheck":
        print("usage: wt gh recheck [--json]", file=sys.stderr)
        return 2
    results = []
    for name in q._github_projects():
        backend = q._github_backend_for_project(name)
        if backend is None:
            continue
        try:
            backend.list_items(fresh=True, strict=True)
            results.append({"queue": name, "ok": True, "error": ""})
        except Exception as e:
            results.append({"queue": name, "ok": False, "error": str(e)})
    gh = health.github_connectivity()
    if args.json:
        print(json.dumps({"queues": results, "github": gh}, indent=2))
        return 0
    if not results:
        print("no GitHub-backed queues configured")
    for r in results:
        status = "ok" if r["ok"] else f"FAIL — {r['error']}"
        print(f"{r['queue']}: {status}")
    if gh.get("alert"):
        print(f"still unreachable — {gh.get('outage_duration')} — {gh.get('last_error')}")
    else:
        print("GitHub connectivity: healthy")
    return 0
```

- [ ] **Step 5: Wire the `wt gh recheck` subcommand**

In `watchtower/cli.py`, immediately after the `logs` subcommand block:

```python
    s = sub.add_parser("logs")
    s.set_defaults(func=cmd_logs, logs_command=None)
    lsub = s.add_subparsers(dest="logs_command")
    sl = lsub.add_parser(
        "prune", help="apply the log retention policy to ~/.watchtower/logs"
    )
    sl.add_argument("--dry-run", action="store_true", dest="dry_run")
    sl.add_argument("--json", action="store_true")
    sl.set_defaults(func=cmd_logs)
```

insert, immediately after (before the `# \`wt agents\`` comment block):

```python

    s = sub.add_parser("gh")
    s.set_defaults(func=cmd_gh, gh_command=None)
    ghsub = s.add_subparsers(dest="gh_command")
    sg = ghsub.add_parser(
        "recheck", help="force a live GitHub connectivity check now, bypassing backoff"
    )
    sg.add_argument("--json", action="store_true")
    sg.set_defaults(func=cmd_gh)
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `python3 -m pytest tests/test_github_backend.py -v`
Expected: PASS — every test in the file, confirming Task 1-4 integrate without regressions.

- [ ] **Step 7: Commit**

```bash
git commit --only watchtower/cli.py tests/test_github_backend.py \
  -m "feat: add wt gh recheck and a wt status GitHub-outage warning"
```

---

## Final Verification

- [ ] **Run the full test suite**

Run: `python3 -m pytest tests/ -v`
Expected: PASS — no regressions anywhere in the suite (in particular `tests/test_github_backend.py`, `tests/test_health.py`, `tests/test_dashboard_mobile.py`, `tests/test_dashboard_gh_alert.py`, `tests/test_smoke.py`).

- [ ] **Manual smoke check against the real BYM-GH-FINIE/CCC-GH queues**

Run: `wt status` and `wt gh recheck` against the live install. If `gh auth status` is currently healthy (it was, per the 2026-07-27 investigation), `wt gh recheck` should report `ok` for both `BYM-GH-FINIE` and `CCC-GH` (once drain is re-enabled and they're configured), and `wt status` should print no warning line. This is a manual step, not part of the automated suite — it exercises the real `gh` CLI and real repos.

"""Untimed release waits. Observing builds never deploys or restarts a service.

One read per distinct target per daemon tick, no worker slot and no timer wake.
Unknown/unreachable builds leave the wait untouched. A build must contain the
recorded patch in the ticket's repository; an unrelated deployment cannot wake it.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone

from . import config, queue as q, release_wait

STATUS = "awaiting_release"
RUNTIME_BUILD = ""  # set once by the daemon from its boot revision, never disk HEAD
CCC_PORT_FILE = Path.home() / ".claude" / "command-center" / "port.txt"
_SHA = re.compile(r"^[0-9a-fA-F]{7,40}$")
_GITHUB = re.compile(r"^github:([\w.-]+/[\w.-]+):([^:\r\n]+)$")


def _legacy_defer(item):
    """Read retired fields only for explicit migration, never admission."""
    if item.get("_github_body"):
        from .github_backend import _split_body
        return _split_body(item["_github_body"])[1]
    return item


def require_parkable(item, worker, expected_defer=None):
    if item.get("status") not in ("open", "in_progress"):
        raise ValueError(f"{item.get('ref')} is {item.get('status')}; only open/in-progress can await release")
    if item.get("needs_input") or item.get("blocked_by") or item.get("duplicate_of"):
        raise ValueError("resolve the question/dependencies/duplicate before awaiting release")
    if item.get("pipeline_foreign") or item.get("supervision") or q._plan_active_unlocked(item):
        raise ValueError("active plan/supervision owns the ticket; complete it before awaiting release")
    if (item.get("pending_answer") or {}).get("state") in q.ANSWER_INFLIGHT:
        raise ValueError("finish answer delivery before awaiting release")
    if q.group_role(item) == "parent":
        raise ValueError("a group parent cannot await release; park its members")
    if item.get("status") == "in_progress" and item.get("claimed_by") != worker:
        raise ValueError(f"ticket is claimed by {item.get('claimed_by')}, not {worker}")
    if expected_defer is not None and (item.get("status") != "open" or
                                       _legacy_defer(item).get("defer_reason") != expected_defer):
        raise ValueError("defer changed since the migration scan; not migrated")


def validate_target(target):
    if target in ("ccc:dashboard", "watchtower:local") or _GITHUB.fullmatch(target):
        return target
    # Explicit version endpoint for other services/hosts. These are read-only
    # build reports with code_rev/sha; no executable probe commands in tickets.
    if target.startswith("https://") or target.startswith("http://"):
        url = urllib.parse.urlparse(target)
        if url.hostname and not url.username and not url.password:
            return target
    raise ValueError("--target needs github:OWNER/REPO:ENV, ccc:dashboard, watchtower:local, or a build JSON URL")


def validate_wait(item, wait):
    """A release generation cannot turn into a future-evidence holding pen."""
    reason = str(wait.get("reason") or "")
    if re.search(r"(?i)natural|future (?:event|evidence)|post.release|acceptance remains|evidence window", reason):
        raise ValueError("awaiting_release is release-only; natural/future evidence needs close + --watch")
    prior = (item.get("release_event") or {}).get("wait") or {}
    commit = str(wait.get("commit") or "")
    if (commit and prior.get("target") == wait.get("target")
            and str(prior.get("commit") or "").startswith(commit)):
        raise ValueError("this release was already observed: complete its one live check; do not repark it")


def park(ref, worker, *, repo, commit, branch, target, reason, summary="", verified="",
         live_check=True, progress="", expected_defer=None):
    item = q.get(ref)
    if not item:
        raise ValueError(f"no item {ref}")
    require_parkable(item, worker, expected_defer)
    if not reason.strip():
        raise ValueError("--reason is required")
    target = validate_target(target)
    validate_wait(item, dict(reason=reason, target=target, commit=commit))
    proof = release_wait.check("release", repo, commit, branch)
    full = release_wait.on_branch(repo, commit, branch)["sha"]
    if not live_check:
        if not summary.strip() or not verified.strip():
            raise ValueError("automatic close needs --summary and --verified acceptance evidence")
        # Required gates run through wt close, not through an automatic event.
        if q.effective_gates(item):
            raise ValueError("ticket has required gates; use --live-check and complete via wt close")
    wait = dict(commit=full, branch=branch, repo=repo, target=target,
                reason=reason, entered_at=q._now_iso(), live_check=live_check,
                summary=summary, verified=verified, worker=worker, branch_proof=proof["tag"])
    if expected_defer is not None:
        wait["previous_defer"] = dict(reason=expected_defer, until=_legacy_defer(item).get("deferred_until", ""))
    return q.await_release(ref, worker, wait, progress=progress, expected_defer=expected_defer)


def observe(target, build, *, evidence, items=None):
    """Apply a successful release/adoption report once to each matching wait.

    A CAS includes the whole wait generation. Repeated reports do nothing after
    it leaves this status, including when a live check later closes with a permanent watch.
    """
    validate_target(target)
    if not _SHA.fullmatch(build or "") or not str(evidence or "").strip():
        raise ValueError("release observation needs a build commit SHA and evidence")
    results = []
    refreshed = {}
    for item in items if items is not None else q.list_items(status=STATUS):
        if item.get("status") != STATUS or config.ticket_is_archived(item):
            continue
        wait = item.get("release_wait") or {}
        if wait.get("target") != target:
            continue
        # Refresh remote tracking once per repo/event. A new deployment often
        # names a merge created after the ticket was parked. Never treat failed
        # refresh as evidence that the old branch still contains the patch.
        repo = wait["repo"]
        if repo not in refreshed:
            rc, remotes = release_wait._git(repo, "remote")
            if rc or "origin" not in remotes.splitlines():
                refreshed[repo] = rc == 0  # local-only repository/fixture
            else:
                refreshed[repo] = release_wait._git(repo, "fetch", "--prune", "--no-tags", "origin")[0] == 0
                if refreshed[repo] and release_wait._git(repo, "cat-file", "-e", f"{build}^{{commit}}")[0]:
                    refreshed[repo] = release_wait._git(repo, "fetch", "--no-tags", "origin", build)[0] == 0
        if not refreshed[repo]:
            continue
        # Both release branch reachability and actual running build are checked.
        if release_wait.on_branch(wait["repo"], wait["commit"], wait["branch"])["state"] not in ("contained", "equivalent"):
            continue
        found = release_wait.on_branch(wait["repo"], wait["commit"], build)
        if found["state"] not in ("contained", "equivalent"):
            continue
        event = dict(target=target, build=build, evidence=evidence, observed_at=q._now_iso(),
                     wait=wait)
        live = wait.get("live_check", True) or bool(q.effective_gates(item))
        resolution = None if live else dict(summary=wait["summary"], commit=wait["commit"],
                                            caveats=[], follow_ups=[], unresolved=[])
        updated = q.update_status(item["ref"], "open" if live else "closed",
                                  session_id="watchtower-release-observer", require_status=STATUS,
                                  release_expect=wait, resolution=resolution,
                                  extras={"release_event": event, **({"run_requested": True} if live else {})},
                                  reason=f"Release observed: {target} build {build}; {evidence}")
        if updated:
            q._log("RELEASE_OBSERVED", f"{item['ref']} {target} {build} -> {updated['status']}",
                   queue=item.get("project", ""))
            results.append(dict(ref=item["ref"], status=updated["status"]))
    return results


def _gh(path):
    proc = subprocess.run(["gh", "api", path], capture_output=True, text=True, timeout=15)
    if proc.returncode:
        raise ValueError("GitHub deployment evidence unavailable")
    return json.loads(proc.stdout)


def probe(target):
    """Return (build, source evidence). No successful report means no event."""
    if target == "watchtower:local":
        return RUNTIME_BUILD, "WatchTower daemon boot revision"
    match = _GITHUB.fullmatch(target)
    if match:
        repo, env = match.groups()
        # Only the newest deployment in this environment: never use an older
        # successful deploy after a rollback or while a newer deploy is pending.
        deploys = _gh(f"repos/{repo}/deployments?environment={urllib.parse.quote(env, safe='')}&per_page=1")
        if not deploys or deploys[0].get("environment") != env:
            return "", ""
        dep = deploys[0]
        states = _gh(f"repos/{repo}/deployments/{dep['id']}/statuses?per_page=1")
        if not states or states[0].get("state") != "success":
            return "", ""
        return str(dep.get("sha") or ""), f"GitHub deployment {repo}/{dep['id']} success ({env})"
    url = target
    if target == "ccc:dashboard":
        base = os.environ.get("CCC_SERVER", "").strip() or CCC_PORT_FILE.read_text().strip()
        if base.isdigit():
            base = f"http://127.0.0.1:{base}"
        validate_target(base)
        url = base.rstrip("/") + "/api/version"
    with urllib.request.urlopen(url, timeout=5) as response:
        report = json.load(response)
    return str(report.get("code_rev") or report.get("sha") or ""), f"running build from {url}"


_LAST_SCAN = 0.0


def reconcile(*, force=False):
    """Daemon event observer; coalesces target reads, never spawns waiters."""
    global _LAST_SCAN
    if not force and time.monotonic() - _LAST_SCAN < 60:
        return []
    _LAST_SCAN = time.monotonic()
    items = q.list_items(status=STATUS)
    targets = sorted({(i.get("release_wait") or {}).get("target", "") for i in items
                      if not config.ticket_is_archived(i)})
    out = []
    for target in targets:
        try:
            build, evidence = probe(target)
            if build:
                out.extend(observe(target, build, evidence=evidence, items=items))
        except (OSError, ValueError, KeyError, TypeError, AttributeError, subprocess.SubprocessError):
            # Never turn a failed probe into a release, or a worker wake.
            continue
    return out


def owner_rows(items, *, now=None):
    """Separate inventory plus only stale/unreachable waits in owner attention."""
    now = now or datetime.now(timezone.utc)
    rows = []
    for item in items:
        if item.get("status") != STATUS or config.ticket_is_archived(item):
            continue
        wait = item.get("release_wait") or {}
        try:
            age = (now - datetime.fromisoformat(wait["entered_at"].replace("Z", "+00:00"))).total_seconds()
            branch = release_wait.on_branch(wait["repo"], wait["commit"], wait["branch"])["state"]
        except (KeyError, ValueError):
            age, branch = 0, "unknown"
        rows.append(dict(ref=item["ref"], target=wait.get("target", ""), commit=wait.get("commit", ""),
                         age_days=round(age / 86400, 2), branch_state=branch,
                         attention=age >= 3 * 86400 or branch not in ("contained", "equivalent")))
    return rows


def migration_target(repo):
    """Only known release sources; never guess a service adoption from GitHub."""
    from pathlib import Path
    name = Path(repo).resolve().name.lower()
    if name == "watchtower":
        return "watchtower:local"
    if name.startswith("claude-command-center"):
        # A CCC repo includes dashboard AND worker; require explicit target.
        return ""
    if name.startswith("bym"):
        rc, remote = release_wait._git(repo, "remote", "get-url", "origin")
        match = re.search(r"github\.com[:/]([\w.-]+/[\w.-]+?)(?:\.git)?$", remote)
        if rc == 0 and match and match.group(1).lower() == "amirfish1/bym-finie":
            return f"github:{match.group(1)}:Production – bookyourmat-app"
    return ""


def migrate(*, project=None, apply=False, repo_override="", target_override=""):
    """Conservative migration of typed release defers, with a per-record report.

    Preserve integration holds and all untyped data windows. Legacy tag SHAs
    are abbreviated; resolve and verify them before writing. Retain old fields
    in the new wait for an exact reversal. Every migration requires a live check.
    """
    report = dict(apply=apply, migrated=[], candidates=[], skipped=[])
    for item in q.list_items(status="open", project=project):
        reason = _legacy_defer(item).get("defer_reason") or ""
        match = re.match(r"\[awaits release: ([0-9a-f]{7,40}) (?:on|same patch on) ([^\]]+)\]", reason)
        if not match:
            continue
        commit, branch = match.groups()
        repo = repo_override or item.get("repo_path") or config.repo_path(item.get("project", "")) or ""
        target = target_override or migration_target(repo)
        row = dict(ref=item["ref"], commit=commit, branch=branch, repo=repo, target=target)
        try:
            if not target:
                raise ValueError("release target cannot be inferred safely; use await-release with an explicit target")
            release_wait.check("release", repo, commit, branch)
            report["candidates"].append(row)
            if apply:
                park(item["ref"], "watchtower-release-migration", repo=repo, commit=commit,
                     branch=branch, target=target, reason=reason, live_check=True,
                     progress="Migrated typed release defer: no timer; one live check after release.",
                     expected_defer=reason)
                report["migrated"].append(row)
        except ValueError as exc:
            report["skipped"].append(dict(row, reason=str(exc)))
    return report

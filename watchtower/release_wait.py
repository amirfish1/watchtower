"""What a deferred ticket is really waiting for (`wt defer --awaits ...`).

A ticket deferred "for release" only wakes usefully if a release can actually
carry its fix. A commit that sits on some other branch is not waiting for a
release: no release reads that branch, so nothing will ever move it. That
ticket is waiting for *integration*, which needs a named owner and path.

Three kinds of wait:

* ``data``        -- the default: a data window, a source-only hold, anything
                     that is not a claim about a release. Unchanged.
* ``release``     -- "the fix is merged; it reaches users at the next
                     release". Needs ``--commit`` and ``--release-branch``,
                     and the commit (or an identical patch) must already be on
                     that branch. Otherwise the defer is refused.
* ``integration`` -- the fix exists but is not on the release branch yet.
                     Needs ``--integration-owner`` and ``--integration-path``.

Stdlib only; reads git, never writes it, never fetches.
"""

from __future__ import annotations

import os
import re
import subprocess
from typing import Dict, Tuple

AWAITS = ("data", "release", "integration")

# A reason that talks about shipping without saying which of the three it is.
_RELEASE_WORDS = re.compile(
    r"\b(release[ds]?|ship(s|ped|ping)?|deploy(s|ed|ment)?|rollout|goes live|"
    r"reach(es)? (prod|production|main)|merge[ds]? to (main|next))\b", re.I)


def _git(repo: str, *args: str) -> Tuple[int, str]:
    try:
        proc = subprocess.run(["git", "-C", repo, *args], capture_output=True, text=True,
                              timeout=30)
    except (OSError, subprocess.SubprocessError) as exc:
        return -1, str(exc)
    return proc.returncode, (proc.stdout or proc.stderr or "").strip()


def on_branch(repo: str, commit: str, branch: str) -> Dict[str, str]:
    """Is ``commit`` on ``branch`` in ``repo``? ``{"state", "sha", "detail"}``.

    ``state``: ``contained`` (an ancestor of the branch), ``equivalent`` (the
    same patch is on the branch under another sha, e.g. a cherry-pick),
    ``missing`` (neither), or ``unknown`` (git could not answer: no checkout,
    the commit or the branch does not resolve here). ``unknown`` is never read
    as contained."""
    repo = os.path.expanduser(str(repo or ""))
    if not repo or not os.path.isdir(repo):
        return {"state": "unknown", "sha": "", "detail": f"{repo or '(no repo)'} is not a directory"}
    rc, sha = _git(repo, "rev-parse", "--verify", "--quiet", f"{commit}^{{commit}}")
    if rc != 0 or not sha:
        return {"state": "unknown", "sha": "", "detail": f"commit {commit} does not resolve in {repo}"}
    rc, tip = _git(repo, "rev-parse", "--verify", "--quiet", f"{branch}^{{commit}}")
    if rc != 0 or not tip:
        return {"state": "unknown", "sha": sha,
                "detail": f"branch {branch} does not resolve in {repo} (try `git fetch`)"}
    rc, _out = _git(repo, "merge-base", "--is-ancestor", sha, tip)
    if rc == 0:
        return {"state": "contained", "sha": sha, "detail": ""}
    if rc != 1:
        return {"state": "unknown", "sha": sha, "detail": "git merge-base failed"}
    # Same patch under another sha (cherry-pick / re-land). `git cherry` marks
    # it "-". Merge and root commits have no single parent to compare against.
    rc, parents = _git(repo, "rev-list", "--parents", "-n", "1", sha)
    if rc == 0 and len(parents.split()) == 2:
        rc, out = _git(repo, "cherry", tip, sha, f"{sha}^")
        if rc == 0 and any(line.startswith("- ") for line in out.splitlines()):
            return {"state": "equivalent", "sha": sha, "detail": ""}
    return {"state": "missing", "sha": sha, "detail": ""}


def check(awaits: str, repo: str, commit: str = "", branch: str = "",
          owner: str = "", path: str = "") -> Dict[str, str]:
    """Validate one defer's wait claim. Returns ``{"awaits", "tag"}`` where
    ``tag`` is the text recorded in front of the defer reason; raises
    ``ValueError`` (nothing is written) when the claim does not hold."""
    awaits = str(awaits or "data").strip().lower()
    if awaits not in AWAITS:
        raise ValueError(f"--awaits must be one of {', '.join(AWAITS)}")
    commit, branch = str(commit or "").strip(), str(branch or "").strip()
    owner, path = " ".join(str(owner or "").split()), " ".join(str(path or "").split())
    if awaits == "data":
        return {"awaits": "data", "tag": ""}
    if awaits == "integration":
        if not owner or not path:
            raise ValueError(
                "--awaits integration needs --integration-owner (the session, worker or person "
                "who will land it) and --integration-path (how: the branch, PR or ticket). "
                "A fix nobody is assigned to land is not waiting, it is stranded.")
        where = f" {commit[:12]}" if commit else ""
        return {"awaits": "integration",
                "tag": f"[awaits integration{where}: owner {owner}; path {path}]"}
    if not commit or not branch:
        raise ValueError(
            "--awaits release needs --commit SHA and --release-branch REF (the branch releases "
            "are cut from, e.g. origin/next), so the claim can be checked.")
    found = on_branch(repo, commit, branch)
    if found["state"] == "unknown":
        raise ValueError(
            f"cannot confirm {commit[:12]} is on {branch}: {found['detail']}. Not recorded as "
            f"awaiting release. Fix the check, or defer with --awaits integration.")
    if found["state"] == "missing":
        raise ValueError(
            f"{found['sha'][:12]} is not on {branch}, so no release will carry it. Pushing a "
            f"feature branch is not integration. Either land it on {branch} first, or defer "
            f"with `--awaits integration --integration-owner WHO --integration-path HOW`.")
    how = "on" if found["state"] == "contained" else "same patch on"
    return {"awaits": "release", "tag": f"[awaits release: {found['sha'][:12]} {how} {branch}]"}


def hint(awaits: str, reason: str) -> str:
    """A note for a plain defer whose reason talks about a release. Advice
    only: the defer still goes through (older callers keep working)."""
    if str(awaits or "").strip() or not _RELEASE_WORDS.search(str(reason or "")):
        return ""
    return ("note: this reason mentions a release but the defer does not say what it waits on. "
            "If the fix is already on the release branch, use `--awaits release --commit SHA "
            "--release-branch REF` so that is checked. If it is not there yet, use `--awaits "
            "integration --integration-owner WHO --integration-path HOW`. A pushed feature "
            "branch is not awaiting release.")

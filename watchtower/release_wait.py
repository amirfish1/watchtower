"""Verify that code awaiting a release is already on its release branch.

Reads git only; never fetches or writes. Ticket timers are retired.
"""

from __future__ import annotations

import os
import subprocess
from typing import Dict, Tuple

AWAITS = ("release",)

INTEGRATION_GUIDANCE = (
    "--awaits integration is retired. Land it yourself on the release branch "
    "(push next/main per repo rules), then use wt await-release (code complete). "
    "If a parent must compose branches, close the source-complete child only when "
    "its own acceptance is met and make the parent depend on it with "
    "`wt edit PARENT --after CHILD`. For a real gate (owner approval, customer/money "
    "hard stop, or named reviewer), use `wt block --wait-on owner|worker "
    "--wait-actor WHO`; preserve that gate."
)

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
    """Validate a release integration claim. Returns ``{"awaits", "tag"}`` where
    ``tag`` is the text recorded in front of the defer reason; raises
    ``ValueError`` (nothing is written) when the claim does not hold."""
    awaits = str(awaits or "").strip().lower()
    if awaits == "integration":
        raise ValueError(INTEGRATION_GUIDANCE)
    if awaits not in AWAITS:
        raise ValueError(DEFER_GUIDANCE)
    commit, branch = str(commit or "").strip(), str(branch or "").strip()
    owner, path = " ".join(str(owner or "").split()), " ".join(str(path or "").split())
    if not commit or not branch:
        raise ValueError(
            "--awaits release needs --commit SHA and --release-branch REF (the branch releases "
            "are cut from, e.g. origin/next), so the claim can be checked.")
    found = on_branch(repo, commit, branch)
    if found["state"] == "unknown":
        raise ValueError(
            f"cannot confirm {commit[:12]} is on {branch}: {found['detail']}. Not recorded as "
            f"awaiting release. Fix the check. {INTEGRATION_GUIDANCE}")
    if found["state"] == "missing":
        raise ValueError(
            f"{found['sha'][:12]} is not on {branch}, so no release will carry it. Pushing a "
            f"feature branch is not integration. {INTEGRATION_GUIDANCE}")
    how = "on" if found["state"] == "contained" else "same patch on"
    return {"awaits": "release", "tag": f"[awaits release: {found['sha'][:12]} {how} {branch}]"}


DEFER_GUIDANCE = (
    "wt defer is retired: tickets never wait on a clock or a future event. "
    "Continue working; close verified work with --caveat and a registered --watch NAME "
    "for future evidence (or record an unresolved item if no runnable check exists); "
    "use wt edit REF --after DEP for another ticket, wt block with a named person "
    "for a real decision, or wt await-release for code already integrated. "
    "Do not invent an event-wait replacement."
)

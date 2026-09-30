"""Keep the installed checkout (the tree ``wt`` imports) a deploy-only copy.

WT-16: workers on the WT queue must edit a dev clone (the queue's
``repo_path``), never the installed tree, so half-written code is never live.
The installed tree only ever moves by ``git merge --ff-only origin/<branch>``,
and only when it is clean, on its branch, and strictly behind.
"""
from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any, Dict


def installed_root() -> str:
    """Toplevel of the git checkout this package runs from, or "" (pipx/venv)."""
    r = _git(str(Path(__file__).resolve().parent), "rev-parse", "--show-toplevel")
    return r.stdout.strip() if r.returncode == 0 else ""


def _git(repo: str, *args: str, timeout: int = 60) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(["git", "-C", repo, *args], capture_output=True,
                              text=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired) as e:
        return subprocess.CompletedProcess(args, 1, "", str(e))


def status(fetch: bool = True) -> Dict[str, Any]:
    """Running version vs origin: ``{path, branch, head, origin_head, behind,
    ahead, dirty}``; ``{"path": ""}`` when not a git checkout."""
    repo = installed_root()
    if not repo:
        return {"path": ""}
    if fetch:
        _git(repo, "fetch", "-q", "origin")
    branch = _git(repo, "rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
    head = _git(repo, "rev-parse", "HEAD").stdout.strip()
    origin = _git(repo, "rev-parse", f"origin/{branch}").stdout.strip()
    counts = _git(repo, "rev-list", "--left-right", "--count",
                  f"origin/{branch}...HEAD").stdout.split()
    behind, ahead = (int(counts[0]), int(counts[1])) if len(counts) == 2 else (0, 0)
    dirty = [ln[3:] for ln in _git(repo, "status", "--porcelain",
                                   "--untracked-files=no").stdout.splitlines()]
    return {"path": repo, "branch": branch, "head": head, "origin_head": origin,
            "behind": behind, "ahead": ahead, "dirty": dirty}


def sync() -> Dict[str, Any]:
    """Fast-forward the installed tree to origin. Returns ``status()`` plus
    ``action``: moved | up-to-date | refused (with ``reason``) | skipped."""
    st = status(fetch=True)
    if not st["path"]:
        return {**st, "action": "skipped", "reason": "not a git checkout"}
    if not st["behind"]:
        return {**st, "action": "up-to-date"}
    if st["dirty"]:
        return {**st, "action": "refused",
                "reason": "installed tree has uncommitted changes: "
                          + ", ".join(st["dirty"][:5])}
    if st["ahead"]:
        return {**st, "action": "refused", "reason": "installed tree has local commits"}
    r = _git(st["path"], "merge", "--ff-only", f"origin/{st['branch']}")
    if r.returncode != 0:
        return {**st, "action": "refused", "reason": (r.stderr or r.stdout).strip()}
    return {**status(fetch=False), "action": "moved", "was": st["head"]}

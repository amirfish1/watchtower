"""The one commit a plan group's integration is checked at (WT-33 D7.1).

A group's members close at their own commits. The integration gates run once,
at a single commit X that contains every member's commit: X is the first of
(the members' commits, newest committer date first; ``HEAD``; ``@{u}``) of
which every member commit is an ancestor. No-code members (empty commit) are
ignored; when every member is no-code X is ``HEAD``.

Stdlib only; reads git, never writes it.
"""

from __future__ import annotations

import os
import subprocess
from typing import Dict, List, Tuple

from . import close_proof


def _git(repo: str, *args: str) -> Tuple[int, str]:
    try:
        proc = subprocess.run(["git", "-C", repo, *args], capture_output=True, text=True,
                              timeout=30)
    except (OSError, subprocess.SubprocessError) as exc:
        return -1, str(exc)
    return proc.returncode, (proc.stdout or "").strip()


def _resolve(repo: str, rev: str) -> str:
    rc, out = _git(repo, "rev-parse", "--verify", "--quiet", f"{rev}^{{commit}}")
    return out if rc == 0 else ""


def containing_sha(repo: str, commits: Dict[str, str]) -> Tuple[str, str, str]:
    """``(sha, source, problem)`` for the child commit map ``{ref: sha|""}``.

    ``source`` is ``member:<REF>``, ``head``, ``upstream`` or ``head`` (all
    no-code). ``problem`` is ``""`` on success, ``commit_missing: <REF> <sha>``
    when a member commit does not resolve in ``repo``, ``no_repo`` when
    ``repo`` is not a git checkout, or ``diverged`` when no candidate
    contains every member commit (then ``sha`` is ``""``)."""
    repo = os.path.expanduser(str(repo or ""))
    if not repo or not os.path.isdir(repo):
        return "", "", f"no_repo: {repo or '(no repo_path)'} is not a directory"
    full: Dict[str, str] = {}
    for ref, sha in commits.items():
        if not sha:
            continue
        resolved, _err = close_proof.resolve_in_repo(repo, str(sha))
        if not resolved:
            return "", "", f"commit_missing: {ref} {sha}"
        full[ref] = resolved
    if not full:
        head = _resolve(repo, "HEAD")
        return (head, "head", "") if head else ("", "", "no_repo: HEAD does not resolve")
    dated: List[Tuple[int, str, str]] = []
    for ref, sha in full.items():
        rc, out = _git(repo, "show", "-s", "--format=%ct", sha)
        dated.append((int(out) if rc == 0 and out.isdigit() else 0, ref, sha))
    candidates: List[Tuple[str, str]] = [
        (sha, f"member:{ref}") for _, ref, sha in sorted(dated, key=lambda t: -t[0])]
    for rev, source in (("HEAD", "head"), ("@{u}", "upstream")):
        sha = _resolve(repo, rev)
        if sha:
            candidates.append((sha, source))
    seen = set()
    for cand, source in candidates:
        if cand in seen:
            continue
        seen.add(cand)
        if all(_git(repo, "merge-base", "--is-ancestor", c, cand)[0] == 0
               for c in full.values()):
            return cand, source, ""
    return "", "", "diverged"

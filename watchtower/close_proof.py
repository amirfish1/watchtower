#!/usr/bin/env python3
# Copyright (c) 2026 Amir Fish. All rights reserved.
# SPDX-License-Identifier: LicenseRef-WatchTower-Software-License

"""Resolving a close proof (`wt close --commit <SHA>`) across repositories.

A queue has ONE ``repo_path``, but a queue's tickets do not always live in one
repository. The VM-NEXT queue that tracked the 2026-08-10 Chuck incident is the
motivating case: its tickets spanned ``chuck-realtor-web``, ``watchtower``, the
VM's ``Hermes-WT-client-intake``, VM system config with no repository at all,
and the local ``wt`` store. Work was genuinely committed, but `wt close`
rejected the SHA because it was not in the queue's configured repo, and the
only way past it was to lie with ``--no-code`` or leave finished work open.

Both of those are worse than the check they route around. So instead of
loosening verification, widen where it looks:

1. the ticket's own ``repo_path``, else its queue's, else ``$PWD`` (unchanged);
2. every other repository configured on any queue;
3. a remote repository, when the path is written ``host:/path`` -- verified
   over ssh with the same ``git rev-parse``, not taken on trust.

Every path still requires the commit to actually resolve. A close proof remains
a proof; it is simply no longer required to be a proof about one repository.
"""

from __future__ import annotations

import os
import re
import subprocess
from typing import Iterable, List, Tuple

COMMIT_SHA_RE = re.compile(r"[0-9a-fA-F]{7,64}")

# ``host:/path`` (ssh) rather than a Windows-style drive or a bare relative
# path. Requires the path component to be absolute so "repo:sha"-ish typos do
# not silently become remote lookups.
_REMOTE_RE = re.compile(r"^(?P<host>[A-Za-z0-9_.\-]+):(?P<path>/.*)$")

# A remote lookup crosses the network; a local one does not. Keep it short so a
# sleeping VM cannot hang a close.
REMOTE_TIMEOUT_S = 15


def _rev_parse_argv(repo: str, candidate: str) -> Tuple[List[str], str]:
    """Return the argv that verifies ``candidate`` in ``repo``.

    Remote repos are verified with the identical ``git rev-parse`` run over
    ssh, so a remote proof is exactly as strong as a local one.
    """
    verify = ["git", "rev-parse", "--verify", "--quiet", f"{candidate}^{{commit}}"]
    remote = _REMOTE_RE.match(repo)
    if remote:
        return (
            ["ssh", "-o", "BatchMode=yes", f"-o", f"ConnectTimeout={REMOTE_TIMEOUT_S}",
             remote.group("host"), "git", "-C", remote.group("path"),
             "rev-parse", "--verify", "--quiet", f"{candidate}^{{commit}}"],
            "",
        )
    if not os.path.isdir(repo):
        return [], f"{repo} is not a directory on this machine"
    return verify, ""


def resolve_in_repo(repo: str, candidate: str) -> str:
    """Canonical SHA if ``candidate`` resolves in ``repo``, else "".

    Never raises: an unreachable host or missing directory is a miss, so one
    dead candidate cannot block the search across the others.
    """
    argv, err = _rev_parse_argv(repo, candidate)
    if err or not argv:
        return ""
    remote = bool(_REMOTE_RE.match(repo))
    try:
        result = subprocess.run(
            argv,
            cwd=None if remote else repo,
            capture_output=True,
            text=True,
            check=False,
            timeout=REMOTE_TIMEOUT_S if remote else 10,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    verified = result.stdout.strip()
    if result.returncode != 0 or not COMMIT_SHA_RE.fullmatch(verified):
        return ""
    return verified


def configured_repos() -> List[str]:
    """Every repository any queue is configured to use, deduped, order-stable.

    Read defensively: a malformed config must not make closing impossible.
    """
    try:
        from . import config

        data = config._load()
    except Exception:
        return []
    repos: List[str] = []
    if isinstance(data, dict):
        for entry in data.values():
            if not isinstance(entry, dict):
                continue
            repo = str(entry.get("repo_path") or "").strip()
            if repo and repo not in repos:
                repos.append(repo)
    return repos


def search_order(primary: str, extra: Iterable[str] = ()) -> List[str]:
    """The primary repo first, then every other configured one.

    Primary stays first so the common single-repo case is one lookup and the
    error message still names the repo the caller expected.
    """
    order: List[str] = []
    for repo in [primary, *extra, *configured_repos()]:
        repo = str(repo or "").strip()
        if repo and repo not in order:
            order.append(repo)
    return order


def verify(candidate: str, primary: str, extra: Iterable[str] = ()) -> Tuple[str, str]:
    """Resolve ``candidate`` across repositories.

    Returns ``(canonical_sha, repo_it_was_found_in)`` or ``("", "")``.
    """
    candidate = str(candidate or "").strip()
    if not COMMIT_SHA_RE.fullmatch(candidate):
        return "", ""
    for repo in search_order(primary, extra):
        verified = resolve_in_repo(repo, candidate)
        if verified:
            return verified, repo
    return "", ""

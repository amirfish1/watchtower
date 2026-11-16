#!/usr/bin/env python3
# Copyright (c) 2026 Amir Fish. All rights reserved.
# SPDX-License-Identifier: LicenseRef-WatchTower-Software-License

"""Resolving a close proof (`wt close --commit <SHA>`) across repositories.

A queue has ONE ``repo_path``, but a queue's tickets do not always live in one
repository. The VM-NEXT queue that tracked the 2026-08-10 client-site incident is the
motivating case: its tickets spanned a client web app, ``watchtower``, the
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

# Host git must never act on a repository whose metadata a sandboxed worker
# could have written (WT-61): config can name an ``uploadpack`` helper, a
# ``core.fsmonitor`` hook or a promisor remote that runs with the daemon's
# authority the moment git needs an object it lacks. The primary control is
# ``restricted_repo`` below (never run git there at all). These are defence in
# depth for every local host git invocation, restricted or not.
GIT_HARDEN_ARGS = (
    "-c", "protocol.allow=never", "-c", "core.fsmonitor=false",
    "-c", "core.hooksPath=/dev/null", "-c", "core.untrackedCache=false",
    "-c", "core.alternateRefsCommand=", "-c", "gc.auto=0",
    "-c", "maintenance.auto=false", "-c", "fetch.fsckObjects=false",
)


def git_env() -> dict:
    """Environment for host git: no lazy fetch, replacement refs or prompts."""
    env = dict(os.environ)
    env.update(GIT_NO_LAZY_FETCH="1", GIT_NO_REPLACE_OBJECTS="1",
               GIT_TERMINAL_PROMPT="0", GIT_OPTIONAL_LOCKS="0",
               GIT_ASKPASS="true", GIT_CONFIG_NOSYSTEM="1")
    for key in ("GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_OBJECT_DIRECTORY",
                "GIT_ALTERNATE_OBJECT_DIRECTORIES", "GIT_INDEX_FILE", "GIT_EXEC_PATH",
                "GIT_EXTERNAL_DIFF", "GIT_SSH", "GIT_SSH_COMMAND", "GIT_PROXY_COMMAND"):
        env.pop(key, None)
    return env


def _real(path) -> str:
    try:
        return os.path.realpath(os.path.expanduser(str(path)))
    except (OSError, ValueError):
        return os.path.abspath(os.path.expanduser(str(path)))


def _within(path: str, root: str) -> bool:
    return bool(root) and (path == root or path.startswith(root.rstrip(os.sep) + os.sep))


def _restricted_roots():
    """``(roots, unknown)`` -- every workspace/store a sandbox owns, durably.

    Sources are the live queue configuration (any queue set to
    ``workspace-write``) AND the trusted bridge scopes, which survive a queue
    setting being turned off later. ``unknown`` is True when the ledger cannot
    be read: that never grants host git access.
    """
    roots = []
    unknown = False
    try:
        from . import config
        data = config._load()
        for name, entry in (data.items() if isinstance(data, dict) else ()):
            if not isinstance(entry, dict):
                continue
            if config.codex_sandbox(name) == "workspace-write":
                for key in ("repo_path",):
                    if entry.get(key):
                        roots.append(_real(entry[key]))
                try:
                    roots.append(_real(config.codex_sandbox_store(name)))
                except Exception:  # noqa: BLE001 - store path is only extra scope
                    pass
    except Exception:  # noqa: BLE001 - unreadable queue config: fail closed
        unknown = True
    try:
        from . import sandbox_bridge
        try:
            with sandbox_bridge.directory(sandbox_bridge.authority_dir()) as fd:
                names = [n for n in os.listdir(fd) if n.endswith(".json")]
                for name in names:
                    try:
                        scope = sandbox_bridge._read(fd, name)
                    except (OSError, ValueError):
                        unknown = True
                        continue
                    for key in ("repo", "store"):
                        if isinstance(scope, dict) and scope.get(key):
                            roots.append(_real(scope[key]))
        except FileNotFoundError:
            pass
    except Exception:  # noqa: BLE001 - unreadable bridge ledger: fail closed
        unknown = True
    return roots, unknown


def _git_pointers(repo: str) -> List[str]:
    """Paths a ``.git`` file or ``commondir`` file sends git to, unresolved
    beyond one hop: pointing a trusted-looking checkout at a sandbox's git
    directory is the same escape as running git in the sandbox."""
    out: List[str] = []
    dotgit = os.path.join(repo, ".git")
    try:
        if os.path.isfile(dotgit) and not os.path.islink(dotgit):
            with open(dotgit, "r", errors="replace") as fh:
                first = fh.read(4096).strip()
            if first.startswith("gitdir:"):
                target = first[len("gitdir:"):].strip()
                target = target if os.path.isabs(target) else os.path.join(repo, target)
                out.append(_real(target))
                common = os.path.join(target, "commondir")
                if os.path.isfile(common):
                    with open(common, "r", errors="replace") as fh:
                        rel = fh.read(4096).strip()
                    out.append(_real(rel if os.path.isabs(rel) else os.path.join(target, rel)))
        elif os.path.islink(dotgit):
            out.append(_real(dotgit))
    except OSError:
        out.append(repo)
    return out


def restricted_repo(repo: str, _ledger=None) -> str:
    """Why host git must not touch ``repo`` ("" when it may).

    A repo is restricted when it is, lies inside, or is reached through the
    git pointers of a sandbox workspace/store -- by configuration or by durable
    bridge scope. Remote ``host:/path`` repos are not local metadata.
    """
    repo = str(repo or "").strip()
    if not repo or _REMOTE_RE.match(repo):
        return ""
    real = _real(repo)
    roots, unknown = _ledger if _ledger is not None else _restricted_roots()
    if unknown:
        return "sandbox restriction ledger is unreadable; refusing host git"
    for candidate in (real, *_git_pointers(real)):
        for root in roots:
            if _within(candidate, root):
                return (f"{repo} is a sandbox workspace; refusing host git "
                        "against worker-writable repository metadata")
    return ""

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
    verify = ["git", *GIT_HARDEN_ARGS, "rev-parse", "--verify", "--quiet",
              f"{candidate}^{{commit}}"]
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


def resolve_in_repo(repo: str, candidate: str) -> Tuple[str, str]:
    """Canonical SHA if ``candidate`` resolves in ``repo``, else ("", "").

    Returns a 2-tuple ``(verified_sha, error)``. An empty string for both means
    the commit simply does not exist in the repository. A non-empty ``error``
    means git itself refused to answer (e.g. ``detected dubious ownership``),
    which must be surfaced to the caller instead of being treated as a miss.
    """
    argv, setup_err = _rev_parse_argv(repo, candidate)
    if setup_err:
        # A configured checkout can disappear between queue runs. It is no
        # different from a present checkout that no longer contains the SHA:
        # continue searching rather than turning it into a proof failure.
        if not _REMOTE_RE.match(repo) and not os.path.isdir(repo):
            return "", ""
        return "", setup_err
    if not argv:
        return "", ""
    remote = bool(_REMOTE_RE.match(repo))
    if not remote:
        why = restricted_repo(repo)
        if why:
            return "", why
    try:
        result = subprocess.run(
            argv,
            cwd=None if remote else repo,
            env=None if remote else git_env(),
            capture_output=True,
            text=True,
            check=False,
            timeout=REMOTE_TIMEOUT_S if remote else 10,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        return "", str(exc)
    verified = result.stdout.strip()
    if result.returncode == 0 and COMMIT_SHA_RE.fullmatch(verified):
        return verified, ""
    stderr = getattr(result, "stderr", None) or ""
    # Queue configuration can retain a directory after it stops being a Git
    # checkout. That path is a normal search miss, not an access failure that
    # should hide the useful "commit not found" result from later candidates.
    if "not a git repository" in stderr.lower():
        return "", ""
    # Git refused to answer. Surface its stderr (dubious ownership, etc.) so the
    # user does not chase a commit that was fine all along.
    stdout = getattr(result, "stdout", None) or ""
    git_err = (stderr or stdout).strip()
    return "", git_err


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
    ledger = _restricted_roots()
    if isinstance(data, dict):
        for entry in data.values():
            if not isinstance(entry, dict):
                continue
            repo = str(entry.get("repo_path") or "").strip()
            # A sandbox queue's workspace is worker-writable: never a place
            # a different queue's close proof may run host git (WT-61).
            if repo and repo not in repos and not restricted_repo(repo, ledger):
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


def safe_search_order(primary: str, extra: Iterable[str] = ()) -> List[str]:
    """``search_order`` minus every sandbox workspace except an explicit
    ``primary`` (which ``resolve_in_repo`` itself refuses and reports)."""
    out: List[str] = []
    for repo in search_order(primary, extra):
        if repo != str(primary or "").strip() and restricted_repo(repo):
            continue
        out.append(repo)
    return out


def verify_with_errors(
    candidate: str, primary: str, extra: Iterable[str] = ()
) -> Tuple[str, str, List[str]]:
    """Resolve ``candidate`` across repositories, preserving git error text.

    Returns ``(canonical_sha, repo_it_was_found_in, errors)``.
    ``errors`` is a list of human-readable git failures (e.g. dubious ownership)
    encountered while searching; it is empty when the commit was simply absent.
    """
    candidate = str(candidate or "").strip()
    if not COMMIT_SHA_RE.fullmatch(candidate):
        return "", "", []
    errors: List[str] = []
    for repo in safe_search_order(primary, extra):
        verified, err = resolve_in_repo(repo, candidate)
        if verified:
            return verified, repo, []
        if err:
            # Keep the message compact: first line of git stderr is usually enough.
            first_line = err.splitlines()[0] if err.splitlines() else err
            errors.append(f"{repo}: {first_line}")
    return "", "", errors


def verify(candidate: str, primary: str, extra: Iterable[str] = ()) -> Tuple[str, str]:
    """Resolve ``candidate`` across repositories.

    Returns ``(canonical_sha, repo_it_was_found_in)`` or ``("", "")``.
    """
    verified, repo, _ = verify_with_errors(candidate, primary, extra)
    return verified, repo

# Copyright (c) 2026 Amir Fish. All rights reserved.
# SPDX-License-Identifier: LicenseRef-WatchTower-Software-License

"""Native Codex permission profile for sandboxed queue workers (WT-61).

One place builds everything the launcher hands to Codex for a
``workspace-write`` queue, for both ``codex exec`` and the app-server worker:

* a custom permission profile (``default_permissions``) rather than the legacy
  ``sandbox_mode``: reads limited to ``:minimal`` plus named runtime paths,
  writes limited to the workspace and the per-worker store, ``/dev/shm``
  denied, the workspace's ``.git`` / ``.codex`` / ``.agents`` read-only, no
  tool network;
* Git metadata the worker may write, kept inside the store (``GIT_DIR``) and
  only ever opened by sandboxed processes -- the workspace's own ``.git`` stays
  host-owned and read-only;
* the workspace pinned as an *untrusted* project, with a launch/turn guard
  that refuses any ``.codex`` directory Codex would load from it;
* a scrubbed environment, and unsandboxed side channels (MCP servers, hooks,
  plugins, apps, browser/computer use, web search, notify) switched off;
* ``run_sandboxed`` -- ``codex sandbox`` under the same profile -- used for the
  model-free kernel preflight, the Git bootstrap and commit-proof checks.

Trust model (what the launcher believes, and from where):

* Git is a *trusted absolute executable* (``trusted_git``): found in fixed
  system directories, never via ``PATH``, reached only through root/account
  owned non-shared components and never inside a worker-writable root. The
  worker-writable ``store/bin`` is on the model's PATH only; launcher runs get a
  system-only PATH and ``WT_GIT``. Proofs also neutralise grafts, replace refs,
  lazy fetch, hooks, commit graphs and transports.
* The base commit and roots live in an immutable *launch record* outside both
  writable roots: write-once (``os.link``), mode 0400, exactly four keys,
  validated strictly on read. An empty or short base is never recorded and a
  missing base is a refusal, never "skip the ancestry check". A store that
  already holds Git state is accepted only against a matching record.
* Workspace ``.git`` pointers are validated structurally (linked-worktree
  admin dir, ``commondir``, reciprocal ``gitdir``, no overlap with the
  workspace, no symlinks); the main checkout of a worktree and every ancestor
  of workspace, store and main checkout is pinned untrusted and checked for
  ``.codex``.
* Every root is canonical (absolute, no ``..``, no non-system symlink
  component). Writable roots may not contain or sit inside the control
  directory, queue settings, Codex homes, credentials or broker code; read
  roots may never be HOME, ``.local``/``.config``/``.cache``, those or the
  Codex home (except the release under ``packages``). A bad root raises.
* Captured command output is bounded (1 MiB; patch export 8 MiB), runs in its
  own process group killed on overflow or timeout, and is decoded with
  replacement. ``export_patch`` returns one squashed ``git diff --binary``.
* ``CODEX_HOME`` is honoured only when absolute, and is the same home for the
  MCP-disable list, protected paths and the spawned process's environment.

Observed on Codex 0.169 (Linux), and covered by tests/test_sandbox_policy.py:

* the legacy ``sandbox`` field of ``thread/start`` replaces a custom profile;
* a workspace ``.codex/config.toml`` is merged (and its MCP command started on
  the host) unless the project is marked untrusted in the thread config or by
  an inline-table ``-c projects={...}`` -- the dotted ``-c`` form is ignored;
* with ``:minimal`` reads, directories above the mounted roots are a private
  tmpfs: a write there appears to succeed inside the sandbox and never reaches
  the host, so enforcement is judged from the host side;
* socket creation for AF_INET/AF_INET6, and bind/connect on AF_UNIX, fail
  with EPERM; the ``wt`` execpolicy allow rule does not lift the sandbox when
  approvals are ``never``.

Nothing here is active unless a queue is set to ``workspace-write``.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import signal
import stat as _stat
import subprocess
import sys
import threading
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

PROFILE = "wt-sandbox"
VERIFY_PROFILE = "wt-sandbox-ro"
BOOT_PROFILE = "wt-sandbox-boot"
READ_ROOTS_ENV = "WATCHTOWER_SANDBOX_READ_ROOTS"
FULL_SHA = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_WORKER_ID = re.compile(r"[A-Za-z0-9_-]{1,80}\Z")
_RUN_TIMEOUT_S = 60.0
_MAX_OUTPUT = 1 << 20          # general captured output (stdout + stderr)
_MAX_PATCH = 8 << 20           # patch export
_MAX_RECORD = 4096

# Tool surfaces that run outside the kernel sandbox or reach the network on
# the model's behalf. A worker reading a hostile ticket must not have them.
# Codex ignores names it does not know, so the list errs on the long side:
# every entry is something that executes on the host (shell snapshots source
# the owner's shell profile, worktrees run host Git), talks to a remote
# service, or starts another agent.
# Keep Codex's Code Mode host available: current models route shell tools
# through it. Those nested tools still use the native permission profile;
# disabling this dispatcher prevents any claim/test/commit command running.
DISABLED_FEATURES = (
    "apps", "plugins", "remote_plugin", "plugin_sharing", "plugin_hooks",
    "plugin_discovery_prefetch", "recommended_plugins", "hooks",
    "browser_use", "browser_use_external", "browser_use_full_cdp_access",
    "browser_annotation_api", "in_app_browser", "computer_use",
    "in_app_local_automation", "in_app_chat", "in_app_dictation",
    "in_app_updates", "in_app_voice", "realtime_conversation",
    "image_generation", "skill_mcp_dependency_install", "skill_search",
    "tool_suggest", "memories", "goals", "multi_agent", "multi_agent_mode",
    "multi_agent_v2", "agent_message_board", "agent_message_board_mcp",
    "send_async_message", "send_message_to_user_async", "wake_on_agent_messages",
    "shell_snapshot", "shell_snapshot_v2", "worktrees", "workspace_dependencies",
    "daemon_auto_start", "js_repl",
    "js_repl_tools_only", "network_proxy", "remote_control", "remote_models",
    "standalone_web_search", "web_search_cached", "web_search_request",
    "search_tool", "tool_search",
)

# Inherited variables a sandboxed worker keeps. Everything else (credentials,
# proxies, SSH/GPG agents, daemon WATCHTOWER_* paths) is dropped.
_ENV_KEEP = ("PATH", "HOME", "USER", "LOGNAME", "SHELL", "LANG", "TERM", "TZ", "CODEX_HOME")
_ENV_KEEP_PREFIXES = ("LC_",)
_ENV_WT = (
    "WT_WORKER_COMMIT", "WT_WORKER_ID", "WT_VERIFY", "WT_SESSION_ID",
    "WT_TICKET_REF", "WT_TICKET_QUEUE", "WT_ROLE", "WT_SANDBOX_BRIDGE",
    "WT_SANDBOX_QUEUE", "WATCHTOWER_STORE", "WATCHTOWER_ACTIVITY_LOG",
    "WATCHTOWER_OUTBOX_FILE", "WATCHTOWER_AGENTS_FILE",
    "WATCHTOWER_DELIVERIES_FILE", "WATCHTOWER_DATA_DIR", "AGENT_MACHINE",
    "GIT_AUTHOR_NAME", "GIT_COMMITTER_NAME",
    # Host-side only (the app worker's steer registry lives beside it); the
    # sandboxed tools can neither read nor write that location.
    "WATCHTOWER_WORKERS_FILE",
)

# Where a trusted Git may live. Never a PATH search: PATH is where a worker
# plants a look-alike (``store/bin/git``).
_GIT_DIRS = ("/usr/bin", "/bin", "/usr/local/bin", "/opt/homebrew/bin",
             "/Library/Developer/CommandLineTools/usr/bin")
_SYSTEM_PATH = ("/usr/bin", "/bin")
_SYSTEM_READS = ("/usr", "/bin", "/sbin", "/lib", "/lib64", "/opt")

# Credential-bearing locations below the account home; none may become a read
# or write root, and each existing one is a kernel canary.
_HOME_SECRETS = (
    ".ssh", ".gnupg", ".aws", ".azure", ".kube", ".docker", ".netrc", ".npmrc",
    ".pypirc", ".git-credentials", ".gitconfig", ".config/gh", ".config/gcloud",
    ".config/git", ".claude", ".claude.json", ".codex", ".watchtower",
    ".bash_history", ".zsh_history", ".local/share/watchtower",
)
# Directories no read/write root may equal or contain: handing one over would
# expose everything the account keeps there.
_HOME_BROAD = (".local", ".config", ".cache", ".local/share", ".local/state")


class SandboxPolicyError(RuntimeError):
    """The sandbox policy cannot be built or was not enforced. Fail closed."""


# --------------------------------------------------------------------------
# paths


def _abs(path: Any) -> Path:
    return Path(os.path.abspath(os.fspath(path)))


def _real(path: Any) -> Path:
    return Path(os.path.realpath(os.fspath(path)))


def _under(path: Path, root: Path) -> bool:
    return path == root or root in path.parents


def _overlap(a: Path, b: Path) -> bool:
    return _under(a, b) or _under(b, a)


def _forms(path: Any) -> List[Path]:
    out = [_abs(path)]
    real = _real(path)
    if real not in out:
        out.append(real)
    return out


def _trusted_link(path: str) -> bool:
    """A system prefix link: root-owned, in a directory only root can change
    (macOS ``/tmp``, a relocated ``/home``)."""
    try:
        st = os.lstat(path)
        parent = os.lstat(os.path.dirname(path) or "/")
    except OSError:
        return False
    return (_stat.S_ISLNK(st.st_mode) and st.st_uid == 0 and parent.st_uid == 0
            and not parent.st_mode & 0o022)


def canonical_root(path: Any, label: str = "path", *, must_exist: bool = True) -> Path:
    """The one spelling of ``path`` the kernel profile, launch record and
    bridge all agree on. Refuses relative paths, ``..`` and any symlink
    component that is not a root-owned system link."""
    text = os.fspath(path)
    if not os.path.isabs(text) or ".." in text.split(os.sep):
        raise SandboxPolicyError(f"{label} {text!r} must be an absolute path without '..'")
    current = Path("/")
    for part in _abs(text).parts[1:]:
        current = current / part
        try:
            st = os.lstat(current)
        except FileNotFoundError:
            if must_exist:
                raise SandboxPolicyError(f"{label} {text!r} does not exist")
            break
        except OSError as exc:
            raise SandboxPolicyError(f"{label} {text!r} is unreadable: {exc}") from exc
        if _stat.S_ISLNK(st.st_mode) and not _trusted_link(str(current)):
            raise SandboxPolicyError(f"{label} {text!r} passes through a symlink at {current}")
    return _real(text)


def _roots(repo: Any, store: Any) -> Tuple[Path, Path]:
    return (canonical_root(repo, "workspace"),
            canonical_root(store, "worker store", must_exist=False))


def _account_home() -> Optional[Path]:
    try:
        import pwd
        return Path(pwd.getpwuid(os.getuid()).pw_dir)
    except (ImportError, KeyError, OSError):
        return None


def _home_dirs() -> List[Path]:
    homes = [Path.home()]
    account = _account_home()
    if account is not None and account not in homes:
        homes.append(account)
    return homes


def codex_binary() -> str:
    """The real Codex executable, or raise."""
    from . import workers
    found = workers._resolve_engine_bin("codex") or shutil.which("codex")
    if not found:
        raise SandboxPolicyError("codex executable not found")
    return found


def _env_codex_home(env: Optional[Mapping[str, str]]) -> Optional[Path]:
    """CODEX_HOME as the spawned process will see it. A relative value means
    something different to every process, so it is not honoured."""
    value = (os.environ if env is None else env).get("CODEX_HOME")
    if value and os.path.isabs(value):
        return Path(os.path.abspath(value))
    return None


def codex_homes(env: Optional[Mapping[str, str]] = None) -> List[Path]:
    """Trusted Codex homes (owner config). Their ``.codex`` is not a project.
    ``env`` is the environment the worker process will actually get; the
    daemon's own CODEX_HOME and the configured worker home are always
    included."""
    homes = [h / ".codex" for h in _home_dirs()]
    for source in (env, os.environ):
        found = _env_codex_home(source)
        if found is not None:
            homes.append(found)
    try:
        from . import config
        worker = config.codex_worker_home()
        if worker is not None:
            homes.append(Path(worker))
    except Exception:  # noqa: BLE001 -- config is advisory here
        pass
    out: List[Path] = []
    for h in homes:
        r = _real(h)
        if r not in out:
            out.append(r)
    return out


# --------------------------------------------------------------------------
# protected paths


def _safely(fn):
    try:
        return fn()
    except Exception:  # noqa: BLE001 -- best effort; the static list still applies
        return None


def protected_paths(env: Optional[Mapping[str, str]] = None) -> Tuple[List[Path], List[Path]]:
    """``(files, trees)`` the worker must never read or write: the control
    directory and everything in it, queue settings and stores, the sandbox
    bridge authority, Codex homes and account credentials. The union of this
    module's view and the bridge's own ``_protected`` so both refuse the same
    roots."""
    files: List[Path] = []
    trees: List[Path] = []
    for home in _home_dirs():
        for rel in _HOME_SECRETS:
            trees.append(home / rel)
    trees += codex_homes(env)
    from . import storage
    trees.append(_safely(storage.data_dir))
    trees.append(_safely(lambda: storage.data_dir() / "sandbox-bridge"))
    trees.append(_safely(lambda: storage.data_dir() / "sandbox-policy"))
    cfg = os.environ.get("WATCHTOWER_CONFIG_FILE")
    if cfg:
        files.append(Path(cfg).expanduser())

    def _module_paths():
        from . import config, queue as q, workers
        files.extend([config.CONFIG_FILE, getattr(config, "_LEGACY_CONFIG_FILE", config.CONFIG_FILE),
                      q._db_path(), q._lock_path(), q.store_path(), workers.WORKERS_FILE])
        trees.extend([workers.WORKERS_FILE.parent, workers.STOP_SIGNALS_DIR])
    _safely(_module_paths)

    def _bridge_paths():
        from . import sandbox_bridge
        b_files, b_trees = sandbox_bridge._protected()
        files.extend(Path(f) for f in b_files)
        trees.extend(Path(t) for t in b_trees)
    _safely(_bridge_paths)
    return ([f for f in files if f is not None], [t for t in trees if t is not None])


def _code_paths() -> List[Path]:
    """Broker/runtime code a writable root may not contain or sit inside."""
    paths = [Path(__file__).absolute().parent, Path(sys.executable)]
    binary = _safely(codex_binary)
    if binary:
        paths.append(Path(binary))
    return paths


def _broad_dirs() -> List[Path]:
    out: List[Path] = []
    for home in _home_dirs():
        out.append(home)
        out += [home / rel for rel in _HOME_BROAD]
    return out


def _read_root_problem(root: Path, env: Optional[Mapping[str, str]],
                       store_root: Optional[Path] = None) -> Optional[str]:
    """Why ``root`` may not be mounted readable, or None."""
    for form in _forms(root):
        if form == Path(form.anchor):
            return "is the filesystem root"
        for broad in _broad_dirs():
            for b in _forms(broad):
                if _under(b, form):
                    return f"is or contains {b}"
    files, trees = protected_paths(env)
    homes = codex_homes(env)
    # The broker's own code and this interpreter are meant to be readable.
    code = {f for c in _code_paths() for f in _forms(c)}
    for form in _forms(root):
        for home in homes:
            if _overlap(form, home):
                # The Codex release lives under <home>/packages; nothing else
                # in the home (auth.json, config, sessions) is ever readable.
                if not _under(form, home / "packages"):
                    return f"overlaps the Codex home {home}"
        for protected in [*files, *trees]:
            for p in _forms(protected):
                if p in homes or p in code:
                    continue
                if _overlap(form, p):
                    return f"overlaps trusted control or credential path {p}"
        if store_root is not None and _overlap(form, store_root):
            return f"overlaps the sandbox store root {store_root}"
    return None


def _checked_read_root(root: Path, env: Optional[Mapping[str, str]], what: str,
                       store_root: Optional[Path] = None) -> Path:
    real = _real(root)
    problem = _read_root_problem(real, env, store_root)
    if problem:
        raise SandboxPolicyError(f"{what} {str(root)!r} {problem}; it cannot be a read root")
    return real


def _codex_read_roots(env: Optional[Mapping[str, str]], store_root: Optional[Path]) -> List[Path]:
    """The Codex release directory when it is a dedicated, harmless directory;
    otherwise just the binary itself (the sandbox helper re-executes it)."""
    binary = _real(codex_binary())
    if any(_under(binary, Path(s)) for s in ("/usr", "/bin", "/opt", "/usr/local")):
        return []
    parent = binary.parent
    release = parent.parent if parent.name == "bin" else parent
    if release != Path(release.anchor) and _read_root_problem(release, env, store_root) is None:
        return [release]
    return [_checked_read_root(binary, env, "Codex binary", store_root)]


def runtime_read_roots(env: Optional[Mapping[str, str]] = None,
                       store: Any = None) -> List[Path]:
    """Read-only paths the worker needs beyond ``:minimal``: the Codex release
    (its sandbox helper re-executes from there), this Python, the trusted Git
    when it is outside the system prefix, and the ``watchtower`` package for
    the in-sandbox ``wt`` shim. Operators add more with
    ``WATCHTOWER_SANDBOX_READ_ROOTS`` (``os.pathsep``-separated). Nothing may
    be, contain or sit inside the account home's broad directories, the
    control directory, queue settings, Codex homes (other than the release
    under ``packages``) or credentials -- a bad root is an error, never
    silently dropped."""
    store_root = _real(Path(store).parent) if store is not None else None
    roots: List[Path] = []
    try:
        roots += _codex_read_roots(env, store_root)
    except SandboxPolicyError as exc:
        if "not found" not in str(exc):
            raise
    for prefix in (sys.prefix, sys.base_prefix, str(_real(sys.executable).parent)):
        p = _real(prefix)
        if not any(_under(p, Path(s)) for s in ("/usr", "/bin", "/lib", "/opt")):
            roots.append(_checked_read_root(p, env, "Python prefix", store_root))
    roots.append(_checked_read_root(Path(__file__).resolve().parent, env, "watchtower package",
                                    store_root))
    git = _safely(trusted_git)
    if git and not any(_under(_real(git), Path(s)) for s in _SYSTEM_READS):
        roots.append(_checked_read_root(_real(git).parent, env, "Git directory", store_root))
    for extra in (os.environ.get(READ_ROOTS_ENV) or "").split(os.pathsep):
        if extra.strip():
            text = os.path.expanduser(extra.strip())
            roots.append(_checked_read_root(canonical_root(text, "extra read root"), env,
                                            "operator read root", store_root))
    out: List[Path] = []
    for r in roots:
        if r not in out:
            out.append(r)
    return out


# --------------------------------------------------------------------------
# trusted Git


def _chain_trusted(real: Path) -> bool:
    """Every component is owned by root or this account and writable by no
    one else."""
    current = Path("/")
    uid = os.getuid()
    for part in real.parts[1:]:
        current = current / part
        try:
            st = os.lstat(current)
        except OSError:
            return False
        if st.st_uid not in (0, uid) or st.st_mode & 0o022:
            return False
    return True


def trusted_git(*worker_roots: Any) -> str:
    """Absolute path of a Git the launcher may run: found in fixed system
    directories (never ``PATH``), reached only through root/account-owned,
    non-shared components, and never inside a worker-writable root. A Git
    resolved from ``store/bin`` was the proven commit-proof forgery."""
    blocked = [_real(r) for r in worker_roots]
    for directory in _GIT_DIRS:
        candidate = os.path.join(directory, "git")
        if not (os.path.isfile(candidate) and os.access(candidate, os.X_OK)):
            continue
        real = _real(candidate)
        if any(_under(real, b) or _under(_abs(candidate), b) for b in blocked):
            continue
        if _chain_trusted(real.parent) and _chain_trusted(Path(directory)) \
                and not (os.stat(real).st_mode & 0o022):
            return candidate
    raise SandboxPolicyError("no trusted Git executable found in the system directories")


def _internal_path(git: str) -> str:
    dirs = list(_SYSTEM_PATH)
    gdir = os.path.dirname(git)
    if gdir not in dirs:
        dirs.append(gdir)
    return os.pathsep.join(dirs)


# --------------------------------------------------------------------------
# workspace inspection (no host git: the workspace is untrusted input)


def _read_small(path: Path, limit: int = _MAX_RECORD) -> str:
    """A small regular file, never through a symlink."""
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        st = os.fstat(fd)
        if not _stat.S_ISREG(st.st_mode) or st.st_size > limit:
            raise SandboxPolicyError(f"{path} is not a small regular file")
        return os.read(fd, limit + 1).decode("utf-8", "replace")
    finally:
        os.close(fd)


def _is_plain_dir(path: Path) -> bool:
    try:
        return _stat.S_ISDIR(os.lstat(path).st_mode)
    except OSError:
        return False


def _is_plain_file(path: Path) -> bool:
    try:
        return _stat.S_ISREG(os.lstat(path).st_mode)
    except OSError:
        return False


def git_metadata(repo: Any) -> Dict[str, Any]:
    """Describe the workspace's Git metadata by reading files only, and refuse
    any pointer the host did not create.

    ``kind`` is ``none``, ``repo`` (``.git`` directory) or ``worktree``
    (``.git`` file naming a linked worktree's admin directory). A linked
    worktree is accepted only when its admin directory sits at
    ``<common>/worktrees/<id>``, names that common directory through
    ``commondir`` and points back at this workspace's ``.git`` -- a
    reciprocity only the host's ``git worktree add`` can have produced. A
    ``.git`` directory may not carry a ``commondir`` of its own. The common
    directory may not sit inside the workspace. ``objects`` is the object
    store the worker's own Git directory borrows from, ``common`` the shared
    Git dir, ``main`` the main checkout of a non-bare repository."""
    repo = _abs(repo)
    dot = repo / ".git"
    none = {"kind": "none", "gitdir": None, "common": None, "objects": None, "main": None}
    try:
        st = os.lstat(dot)
    except FileNotFoundError:
        return none
    if _stat.S_ISLNK(st.st_mode):
        raise SandboxPolicyError(f"{dot} is a symlink; refusing the workspace")
    if _stat.S_ISDIR(st.st_mode):
        if os.path.lexists(dot / "commondir"):
            raise SandboxPolicyError(f"{dot} names a commondir of its own; refusing the workspace")
        return {"kind": "repo", "gitdir": dot, "common": dot, "objects": dot / "objects",
                "main": None}
    if not (_stat.S_ISREG(st.st_mode) and st.st_size < 4096):
        raise SandboxPolicyError(f"{dot} is not a usable Git directory")
    try:
        text = _read_small(dot)
    except (OSError, SandboxPolicyError) as exc:
        raise SandboxPolicyError(f"{dot} cannot be read safely: {exc}") from exc
    first = text.strip()
    if "\n" in first or not first.startswith("gitdir:"):
        raise SandboxPolicyError(f"{dot} is not a Git directory pointer")
    target = first.split(":", 1)[1].strip()
    if not target:
        raise SandboxPolicyError(f"{dot} has an empty gitdir pointer")
    target_path = target if os.path.isabs(target) else os.path.join(str(repo), target)
    gitdir = canonical_root(os.path.normpath(target_path), "worktree gitdir")
    if gitdir.parent.name != "worktrees" or not _is_plain_dir(gitdir):
        raise SandboxPolicyError(f"{dot} points at {gitdir}, which is not a linked-worktree admin directory")
    common = gitdir.parent.parent
    try:
        common = canonical_root(common, "common Git directory")
        commondir = _read_small(gitdir / "commondir").strip()
        back = _read_small(gitdir / "gitdir").strip()
    except (OSError, SandboxPolicyError) as exc:
        raise SandboxPolicyError(f"worktree metadata at {gitdir} is incomplete or unsafe: {exc}") from exc
    if _real(os.path.join(str(gitdir), commondir)) != common:
        raise SandboxPolicyError(f"worktree commondir at {gitdir} does not name {common}")
    if os.path.normpath(os.path.join(str(gitdir), back)) not in (str(dot), str(_real(dot))) \
            and _real(os.path.join(str(gitdir), back)) != _real(dot):
        raise SandboxPolicyError(f"worktree {gitdir} does not point back at {dot}; untrusted pointer")
    objects = common / "objects"
    if not (_is_plain_dir(objects) and _is_plain_file(common / "HEAD")):
        raise SandboxPolicyError(f"{common} is not a Git directory")
    if _overlap(common, _real(repo)):
        raise SandboxPolicyError(f"common Git directory {common} overlaps the workspace")
    main = common.parent if common.name == ".git" else None
    return {"kind": "worktree", "gitdir": gitdir, "common": common, "objects": objects,
            "main": main}


def _project_roots(repo: Path, store: Optional[Path], meta: Optional[Mapping[str, Any]]) -> List[Path]:
    roots: List[Path] = []
    for base in (repo, store, (meta or {}).get("main")):
        if base is None:
            continue
        for form in _forms(base):
            if form not in roots:
                roots.append(form)
    return roots


def _codex_entry_hazards(directory: Path, homes: List[Path]) -> List[str]:
    found: List[str] = []
    candidate = directory / ".codex"
    if os.path.lexists(candidate):
        trusted_home = not candidate.is_symlink() and _real(candidate) in homes
        # Codex temporarily materializes missing read-only mount targets on
        # the host while a tool is running. Empty directories contain no
        # executable config; rejecting them breaks a concurrent broker proof.
        # Symlinks, non-directories, unreadable or populated entries fail shut.
        empty = False
        try:
            empty = (not candidate.is_symlink() and candidate.is_dir()
                     and not any(candidate.iterdir()))
        except OSError:
            empty = False
        if not trusted_home and not empty:
            found.append(str(candidate))
    return found


def project_config_hazards(repo: Any, store: Any = None,
                           env: Optional[Mapping[str, str]] = None) -> List[str]:
    """``.codex`` entries Codex would treat as project config for ``repo``.

    Codex 0.169 merged a workspace ``.codex/config.toml`` into the session --
    widening the permission profile and starting its MCP command on the host --
    even from a fresh home with no trust recorded. Any ``.codex`` in the
    workspace, its store, the main checkout of a linked worktree, or any of
    their ancestors (other than the owner's Codex homes) is therefore a
    launch blocker and a reason to stop a running worker. A symlinked
    ``.agents`` (workspace or store) or ``.codex`` is refused outright, and
    unusable Git metadata is reported as a hazard rather than raised, so a
    per-turn guard fails closed instead of crashing."""
    repo_p = _abs(repo)
    homes = codex_homes(env)
    found: List[str] = []
    meta: Optional[Dict[str, Any]] = None
    try:
        meta = git_metadata(repo_p)
    except (SandboxPolicyError, OSError) as exc:
        found.append(f"{repo_p / '.git'} ({exc})")
    store_p = _abs(store) if store is not None else None
    seen = set()
    for root in _project_roots(repo_p, store_p, meta):
        for directory in (root, *root.parents):
            for hazard in _codex_entry_hazards(directory, homes):
                if hazard not in seen:
                    seen.add(hazard)
                    found.append(hazard)
    for base in (repo_p, store_p):
        if base is None:
            continue
        for name in (".agents", ".codex"):
            link = base / name
            if os.path.islink(link) and str(link) not in seen:
                seen.add(str(link))
                found.append(f"{link} (symlink)")
    return found


def untrusted_project_paths(repo: Any, store: Any = None) -> List[str]:
    """Every key Codex might consult for project trust: the workspace, its
    ancestors (an owner may have trusted ``/tmp`` or ``$HOME``), the main
    checkout of a worktree and *its* ancestors, and the store and its
    ancestors -- in both the given and the resolved spelling."""
    repo_p = _abs(repo)
    meta: Optional[Dict[str, Any]] = None
    try:
        meta = git_metadata(repo_p)
    except (SandboxPolicyError, OSError):
        pass
    paths: List[Path] = []
    for root in _project_roots(repo_p, _abs(store) if store is not None else None, meta):
        paths += [root, *(p for p in root.parents if p != Path(p.anchor))]
    out: List[str] = []
    for p in paths:
        if str(p) not in out:
            out.append(str(p))
    return out


# --------------------------------------------------------------------------
# profile and overrides


def _toml(value: Any) -> str:
    """TOML literal for a ``-c key=value`` override. Tables are inline: the
    dotted form mis-parses quoted keys (a path) in Codex 0.169."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_toml(v) for v in value) + "]"
    if isinstance(value, dict):
        return "{" + ",".join(f"{json.dumps(str(k))}={_toml(v)}" for k, v in value.items()) + "}"
    raise TypeError(f"cannot express {type(value).__name__} as TOML")


def validate_roots(repo: Any, store: Any, env: Optional[Mapping[str, str]] = None) -> None:
    """Refuse writable roots that are not canonical, are too broad, overlap
    each other, the store root of sibling workers, the control directory,
    queue settings, Codex homes, credentials or the broker's own code."""
    repo_a, store_a = _abs(repo), _abs(store)
    for label, p in (("workspace", repo_a), ("store", store_a)):
        if _under(_real(p), Path("/dev/shm")):
            raise SandboxPolicyError(
                f"{label} {p} resolves under /dev/shm, which the profile denies")
    if not repo_a.is_dir():
        raise SandboxPolicyError(f"workspace {str(repo)!r} is not a directory")
    repo_c, store_c = _roots(repo, store)
    homes = [_real(h) for h in _home_dirs()]
    for label, real in (("workspace", repo_c), ("store", store_c)):
        if real == Path(real.anchor) or any(_under(h, real) for h in homes):
            raise SandboxPolicyError(f"{label} {real} is too broad to be a writable root")
    if _overlap(store_c, repo_c):
        raise SandboxPolicyError("workspace and worker store must be separate roots")
    # The store's parent holds every sibling worker's store.
    if _under(store_c.parent, repo_c):
        raise SandboxPolicyError("workspace would contain the sandbox store root")
    files, trees = protected_paths(env)
    code = _code_paths()
    for label, root in (("workspace", repo_c), ("store", store_c)):
        for protected in [*files, *code]:
            for p in _forms(protected):
                if _under(p, root):
                    raise SandboxPolicyError(
                        f"{label} {root} contains trusted control files or broker code ({p})")
        for protected in [*trees, *code]:
            for p in _forms(protected):
                if _overlap(p, root):
                    raise SandboxPolicyError(
                        f"{label} {root} overlaps trusted control, runtime, code or credential path {p}")


def _rules(repo_c: Path, store_c: Path, reads: Iterable[Path], repo_access: str,
           store_access: str, extra: Iterable[Path] = (), objects: Optional[Path] = None) -> Dict[str, str]:
    rules: Dict[str, str] = {":minimal": "read", "/dev/shm": "deny"}
    for root in [*reads, *extra]:
        rules[str(root)] = "read"
    if objects is not None and not _under(Path(objects), repo_c):
        # A shared worktree's objects live outside the workspace: readable for
        # the store's alternates, never writable.
        rules[str(objects)] = "read"
    rules[str(repo_c)] = repo_access
    rules[str(store_c)] = store_access
    # Deny beats write and the narrower path wins: host-owned metadata and
    # anything Codex would execute as project config stay out of reach.
    for protected in (".git", ".codex", ".agents"):
        rules[str(repo_c / protected)] = "read"
    for protected in (".codex", ".agents"):
        rules[str(store_c / protected)] = "read"
    return rules


def _objects_mount(repo_c: Path) -> Optional[Path]:
    meta = git_metadata(repo_c)  # raises on an untrusted pointer: no mount, no launch
    objects = meta["objects"]
    if objects is None:
        return None
    problem = _read_root_problem(_real(objects), None)
    if problem:
        raise SandboxPolicyError(f"Git object store {objects} {problem}")
    return Path(objects)


def filesystem_rules(repo: Any, store: Any, *, writable: bool = True,
                     read_roots: Optional[Iterable[Any]] = None,
                     repo_writable: Optional[bool] = None,
                     extra_reads: Iterable[Any] = (),
                     env: Optional[Mapping[str, str]] = None) -> Dict[str, str]:
    """The profile's filesystem table. ``writable=False`` is the proof-check
    variant: same reads, nothing writable. ``repo_writable=False`` with
    ``writable=True`` is the bootstrap variant: only the store is writable."""
    repo_c, store_c = _roots(repo, store)
    reads = (runtime_read_roots(env, store_c) if read_roots is None
             else [_checked_read_root(Path(r), env, "read root", _real(store_c.parent))
                   for r in read_roots])
    rw = writable if repo_writable is None else repo_writable
    return _rules(repo_c, store_c, reads, "write" if rw else "read",
                  "write" if writable else "read",
                  extra=[_abs(e) for e in extra_reads], objects=_objects_mount(repo_c))


_EMAIL_CACHE: Dict[str, str] = {}


def _owner_git_email() -> str:
    """The owner's global Git email. Run from ``/`` so no repository config is
    read; only the owner's own global file."""
    if "v" not in _EMAIL_CACHE:
        value = ""
        try:
            git = trusted_git()
            out = subprocess.run(
                [git, "config", "--global", "--get", "user.email"], cwd="/",
                capture_output=True, text=True, timeout=10,
                env={"PATH": os.pathsep.join(_SYSTEM_PATH), "HOME": str(Path.home()),
                     "GIT_CONFIG_NOSYSTEM": "1"})
            value = out.stdout.strip() if out.returncode == 0 else ""
        except (OSError, subprocess.SubprocessError, SandboxPolicyError):
            value = ""
        _EMAIL_CACHE["v"] = value if re.fullmatch(r"[^\s<>\"']{1,200}", value or "") else ""
    return _EMAIL_CACHE["v"]


def git_env(repo: Any, store: Any) -> Dict[str, str]:
    """Environment for *sandboxed* Git only. Never export this to a host
    process: the store's Git directory is worker-writable, so its config can
    name commands (fsmonitor, hooks, pagers)."""
    repo_c, store_c = _roots(repo, store)
    email = (os.environ.get("GIT_AUTHOR_EMAIL") or os.environ.get("GIT_COMMITTER_EMAIL")
             or _owner_git_email() or "watchtower-sandbox@localhost")
    return {
        "GIT_DIR": str(store_c / "git"),
        "GIT_WORK_TREE": str(repo_c),
        "GIT_CONFIG_GLOBAL": str(store_c / "gitconfig"),
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_AUTHOR_EMAIL": email,
        "GIT_COMMITTER_EMAIL": email,
    }


def tool_env(repo: Any, store: Any) -> Dict[str, str]:
    """Variables forced into every sandboxed tool process
    (``shell_environment_policy.set``): a private HOME/TMPDIR inside the store,
    the store-side Git directory, and a PATH that finds the in-store ``wt``.
    That PATH is the *model's* view: ``store/bin`` is worker-writable, so no
    launcher-driven check may use it (see ``run_sandboxed``)."""
    _repo_c, store_c = _roots(repo, store)
    env = {
        "HOME": str(store_c / "home"),
        "TMPDIR": str(store_c / "tmp"),
        "XDG_CACHE_HOME": str(store_c / "home" / ".cache"),
        "XDG_CONFIG_HOME": str(store_c / "home" / ".config"),
        "PATH": os.pathsep.join([str(store_c / "bin"),
                                 os.environ.get("PATH") or "/usr/local/bin:/usr/bin:/bin"]),
    }
    env.update(git_env(repo, store))
    return env


def _user_mcp_servers(env: Optional[Mapping[str, str]] = None) -> List[str]:
    """Names of MCP servers in the owner's Codex config; each is disabled for
    the sandboxed session (an empty-table override would merge, not clear).
    ``env`` is the spawned process's environment, so the home it will really
    load is the home inspected."""
    names: List[str] = []
    try:
        import tomllib
    except ImportError:  # pragma: no cover - Python < 3.11
        return names
    for home in codex_homes(env):
        try:
            data = tomllib.loads((home / "config.toml").read_text())
        except (OSError, ValueError):
            continue
        for name in (data.get("mcp_servers") or {}):
            if str(name) not in names:
                names.append(str(name))
    return names


def config_overrides(repo: Any, store: Any, *, read_roots: Optional[Iterable[Any]] = None,
                     boot_reads: Iterable[Any] = (),
                     env: Optional[Mapping[str, str]] = None) -> List[str]:
    """``key=value`` overrides (each goes after a ``-c``) shared by ``codex
    exec``, ``codex app-server`` and ``codex sandbox``. ``boot_reads`` extend
    only the bootstrap profile."""
    validate_roots(repo, store, env)
    repo_c, store_c = _roots(repo, store)
    objects = _objects_mount(repo_c)
    reads = (runtime_read_roots(env, store_c) if read_roots is None
             else [_checked_read_root(Path(r), env, "read root", _real(store_c.parent))
                   for r in read_roots])
    rw = _rules(repo_c, store_c, reads, "write", "write", objects=objects)
    ro = _rules(repo_c, store_c, reads, "read", "read", objects=objects)
    boot = _rules(repo_c, store_c, reads, "read", "write", extra=[_abs(b) for b in boot_reads],
                  objects=objects)
    net = {"enabled": False}
    overrides = [
        f"default_permissions={_toml(PROFILE)}",
        f"permissions.{PROFILE}={_toml({'filesystem': rw, 'network': net})}",
        f"permissions.{VERIFY_PROFILE}={_toml({'filesystem': ro, 'network': net})}",
        f"permissions.{BOOT_PROFILE}={_toml({'filesystem': boot, 'network': net})}",
        'approval_policy="never"',
        "projects=" + _toml({p: {"trust_level": "untrusted"}
                             for p in untrusted_project_paths(repo_c, store_c)}),
        "notify=[]",
        'web_search="disabled"',
        "check_for_update_on_startup=false",
        "analytics.enabled=false",
        "feedback.enabled=false",
        f"shell_environment_policy.set={_toml(tool_env(repo_c, store_c))}",
    ]
    overrides += [f"features.{name}=false" for name in DISABLED_FEATURES]
    # Inline-table keys quote every name, including dots/spaces. Filtering
    # names to identifiers would leave inherited non-identifier servers live.
    servers = _user_mcp_servers(env)
    if servers:
        overrides.append("mcp_servers=" + _toml({name: {"enabled": False} for name in servers}))
    overrides.append("agents.enabled=false")
    return overrides


def exec_args(repo: Any, store: Any, env: Optional[Mapping[str, str]] = None) -> List[str]:
    """Arguments placed after ``codex exec`` for a sandboxed worker. The tool
    working directory is pinned to the canonical workspace: the launcher's
    own cwd is ``/`` and must not become the tool cwd."""
    repo_c, _store_c = _roots(repo, store)
    args = ["--ignore-rules", "--cd", str(repo_c)]
    for override in config_overrides(repo, store, env=env):
        args += ["-c", override]
    return args


def thread_config(repo: Any, store: Any) -> Dict[str, Any]:
    """``config`` for app-server ``thread/start``. The profile must come from
    here and the server's ``-c`` overrides -- never the legacy ``sandbox``
    field, which replaces it with a plain workspace-write policy. A
    thread-level ``projects`` entry is what stopped project config loading in
    testing; the CLI override alone did not for a dotted key."""
    repo_c, store_c = _roots(repo, store)
    return {
        "default_permissions": PROFILE,
        "projects": {p: {"trust_level": "untrusted"}
                     for p in untrusted_project_paths(repo_c, store_c)},
    }


def check_thread_sandbox(summary: Any, repo: Any, store: Any) -> None:
    """Fail closed unless ``thread/start`` reports the policy we asked for.

    The response projects the profile onto the legacy shape: workspace-write,
    no network, and writable roots that are a subset of ours."""
    if not isinstance(summary, dict) or summary.get("type") != "workspaceWrite":
        raise SandboxPolicyError(f"thread sandbox is {summary!r}, not the workspace profile")
    if summary.get("networkAccess") is not False:
        raise SandboxPolicyError("thread sandbox allows tool network access")
    allowed = {str(p) for p in (*_forms(repo), *_forms(store))}
    extra = [r for r in (summary.get("writableRoots") or []) if str(r) not in allowed]
    if extra:
        raise SandboxPolicyError(f"thread sandbox has unexpected writable roots: {extra}")
    if summary.get("excludeSlashTmp") is not True or summary.get("excludeTmpdirEnvVar") is not True:
        raise SandboxPolicyError("thread sandbox leaves the host temp directories writable")


# --------------------------------------------------------------------------
# environment


def scrub_env(env: Dict[str, str], store: Any) -> Dict[str, str]:
    """Allowlist the worker's environment: no credentials, proxies, agent
    sockets or daemon paths; TMPDIR inside the store. CODEX_HOME survives
    only as an absolute path (a relative one means something different to
    every process; Codex then uses the default home, which the policy also
    covers)."""
    kept = {k: v for k, v in env.items()
            if k in _ENV_KEEP or k in _ENV_WT or k.startswith(_ENV_KEEP_PREFIXES)}
    if "CODEX_HOME" in kept:
        if os.path.isabs(kept["CODEX_HOME"]):
            kept["CODEX_HOME"] = os.path.abspath(kept["CODEX_HOME"])
        else:
            del kept["CODEX_HOME"]
    kept["TMPDIR"] = str(_abs(store) / "tmp")
    kept.setdefault("PATH", "/usr/local/bin:/usr/bin:/bin")
    return kept


# --------------------------------------------------------------------------
# running things inside the sandbox


def _kill_group(proc: subprocess.Popen) -> None:
    try:
        os.killpg(proc.pid, signal.SIGKILL)
    except (OSError, ProcessLookupError):
        try:
            proc.kill()
        except OSError:
            pass


def _run_bounded(cmd: List[str], *, env: Dict[str, str], input_text: Optional[str],
                 timeout: float, max_output: int, truncate: bool,
                 binary: bool) -> subprocess.CompletedProcess:
    """Run ``cmd`` capturing at most ``max_output`` bytes of stdout+stderr
    together, in its own process group so a timeout or overflow kills every
    descendant. Undecodable bytes are replaced, never raised."""
    proc = subprocess.Popen(cmd, cwd="/", env=env, stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            start_new_session=True)
    lock = threading.Lock()
    chunks: Dict[str, List[bytes]] = {"out": [], "err": []}
    state = {"total": 0, "overflow": False}

    def pump(stream, key):
        fd = stream.fileno()
        while True:
            try:
                data = os.read(fd, 65536)
            except OSError:
                return
            if not data:
                return
            with lock:
                room = max_output - state["total"]
                if len(data) > room:
                    if room > 0:
                        chunks[key].append(data[:room])
                    state["total"] = max_output
                    state["overflow"] = True
                    over = True
                else:
                    chunks[key].append(data)
                    state["total"] += len(data)
                    over = False
            if over:
                _kill_group(proc)
                return

    def feed():
        try:
            if input_text:
                proc.stdin.write(input_text.encode("utf-8", "replace"))
        except OSError:
            pass
        finally:
            try:
                proc.stdin.close()
            except OSError:
                pass

    threads = [threading.Thread(target=pump, args=(proc.stdout, "out"), daemon=True),
               threading.Thread(target=pump, args=(proc.stderr, "err"), daemon=True),
               threading.Thread(target=feed, daemon=True)]
    for t in threads:
        t.start()
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        _kill_group(proc)
        proc.wait()
        raise SandboxPolicyError(f"sandboxed command timed out: {cmd[-1][:60]}") from None
    for t in threads:
        t.join(timeout=2.0)
    if any(t.is_alive() for t in threads[:2]):
        # A descendant kept the pipes open after the command finished.
        _kill_group(proc)
        for t in threads:
            t.join(timeout=2.0)
    if state["overflow"] and not truncate:
        raise SandboxPolicyError(f"sandboxed command output exceeded {max_output} bytes")
    out, err = b"".join(chunks["out"]), b"".join(chunks["err"])
    err_text = err.decode("utf-8", "replace")
    out_val: Any = out if binary else out.decode("utf-8", "replace")
    result = subprocess.CompletedProcess(cmd, proc.returncode, out_val, err_text)
    result.truncated = state["overflow"]  # type: ignore[attr-defined]
    return result


_MODES = {"rw": PROFILE, "ro": VERIFY_PROFILE, "boot": BOOT_PROFILE}


def run_sandboxed(argv: Sequence[str], repo: Any, store: Any, *, writable: bool = False,
                  mode: Optional[str] = None, env: Optional[Dict[str, str]] = None,
                  input_text: Optional[str] = None, timeout: float = _RUN_TIMEOUT_S,
                  read_roots: Optional[Iterable[Any]] = None,
                  boot_reads: Iterable[Any] = (), worker_path: bool = False,
                  max_output: int = _MAX_OUTPUT, truncate: bool = False,
                  binary: bool = False,
                  codex_env: Optional[Mapping[str, str]] = None) -> subprocess.CompletedProcess:
    """Run ``argv`` under the worker profile with ``codex sandbox`` (no model).

    ``mode`` is ``rw`` (the worker's profile; ``writable=True``), ``ro`` (the
    read-only variant for checks on worker-controlled data such as its Git
    directory; ``writable=False``) or ``boot`` (store writable, workspace
    read-only, plus ``boot_reads``). The host side starts in ``/`` so no
    project config is discovered from a worker-writable cwd.

    By default the command sees a *system-only* PATH and ``WT_GIT`` naming the
    trusted Git: ``store/bin`` is worker-writable and was how a planted
    ``git`` forged commit proofs. Pass ``worker_path=True`` for the model's
    own PATH (in-store ``wt`` first), only for running worker code. Captured
    output (stdout and stderr together) is capped at ``max_output`` bytes:
    exceeding it kills the command and raises unless ``truncate``."""
    mode = mode or ("rw" if writable else "ro")
    if mode not in _MODES:
        raise SandboxPolicyError(f"unknown sandbox mode {mode!r}")
    repo_c, store_c = _roots(repo, store)
    hazards = project_config_hazards(repo_c, store_c, codex_env)
    if hazards:
        raise SandboxPolicyError(f"untrusted Codex project config present: {', '.join(hazards)}")
    binary_path = codex_binary()
    real_binary = _real(binary_path)
    if _under(real_binary, repo_c) or _under(real_binary, store_c):
        raise SandboxPolicyError("the Codex executable resolves inside a worker-writable root")
    # `codex sandbox` hands its own environment to the command, so the command
    # is started through `env -i` with exactly the tool view.
    inner = dict(tool_env(repo_c, store_c))
    for key in ("GIT_AUTHOR_NAME", "GIT_COMMITTER_NAME"):
        inner[key] = os.environ.get(key) or "WatchTower sandbox worker"
    if not worker_path:
        git = trusted_git(repo_c, store_c)
        inner["PATH"] = _internal_path(git)
        inner["WT_GIT"] = git
    inner.update(env or {})
    cmd = [binary_path, "sandbox", "-P", _MODES[mode]]
    for override in config_overrides(repo_c, store_c, read_roots=read_roots,
                                     boot_reads=boot_reads, env=codex_env):
        cmd += ["-c", override]
    cmd += ["--", "/usr/bin/env", "-i", *[f"{k}={v}" for k, v in inner.items()], *argv]
    child_env = scrub_env(dict(os.environ if codex_env is None else codex_env), store_c)
    try:
        return _run_bounded(cmd, env=child_env, input_text=input_text, timeout=timeout,
                            max_output=max_output, truncate=truncate, binary=binary)
    except OSError as exc:
        raise SandboxPolicyError(f"cannot start the Codex sandbox: {exc}") from exc


_PROBE = r"""
import errno, json, os, socket, sys
tag, store, n = sys.argv[1], sys.argv[2], int(sys.argv[3])
targets, secrets = sys.argv[4:4 + n], sys.argv[4 + n:]
out = {"wrote": [], "net": {}, "secrets": []}
for i, directory in enumerate(targets):
    try:
        with open(os.path.join(directory, tag), "w") as f:
            f.write("x")
        out["wrote"].append(i)
    except OSError:
        pass
def attempt(name, fn):
    try:
        fn(); out["net"][name] = "ok"
    except OSError as e:
        out["net"][name] = errno.errorcode.get(e.errno, str(e.errno))
attempt("inet", lambda: socket.socket(socket.AF_INET, socket.SOCK_STREAM).close())
attempt("inet6", lambda: socket.socket(socket.AF_INET6, socket.SOCK_DGRAM).close())
def unix_bind():
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try: s.bind(os.path.join(store, tag + ".sock"))
    finally: s.close()
attempt("unix_bind", unix_bind)
def unix_connect():
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try: s.connect(chr(0) + "wt-sandbox-probe")
    finally: s.close()
attempt("unix_connect", unix_connect)
for i, path in enumerate(secrets):
    try:
        # A denied directory may still exist as a tree of empty mount-target
        # directories (Codex recreates its own tmp/arg0 scaffolding there);
        # only a file or link below it is a leak.
        if os.path.isdir(path):
            seen, budget = False, 2000
            for _root, _dirs, _files in os.walk(path):
                rel = os.path.relpath(_root, path).split(os.sep)
                # Codex's own exec helper links (symlinks to its binary and
                # an empty lock) live under <home>/tmp/arg0*; they carry no
                # credentials and must stay visible for the sandbox to exec.
                if rel[:1] == ["tmp"] and (len(rel) == 1 or rel[1].startswith("arg0")):
                    if len(rel) == 1:
                        _dirs[:] = [d for d in _dirs if not d.startswith("arg0")]
                    continue
                budget -= 1 + len(_files)
                if _files or budget < 0:
                    seen = True
                    break
            if not seen: continue
        else:
            with open(path, "rb") as f: f.read(1)
        out["secrets"].append(i)
    except OSError:
        pass
print(json.dumps(out))
"""

_DENIED_NET = ("inet", "inet6", "unix_bind", "unix_connect")
_MAX_CANARIES = 96


def _secret_canaries(env: Optional[Mapping[str, str]] = None,
                     reads: Iterable[Path] = ()) -> List[str]:
    """Existing credential and control locations the profile must hide from
    the worker: every existing protected file, each protected directory that
    is not an ancestor of a mounted read root (a listing there would show the
    mount), and a few regular files inside those that are."""
    read_list = [_real(r) for r in reads]
    files, trees = protected_paths(env)
    code = {f for c in _code_paths() for f in _forms(c)}   # meant to be readable

    def _is_code(path: Path) -> bool:
        return any(_under(path, c) or path == c for c in code)
    candidates: List[Path] = [h / "auth.json" for h in codex_homes(env)]
    candidates += [h / "config.toml" for h in codex_homes(env)]
    candidates += [f for f in files if not _is_code(f)]
    for tree in trees:
        for form in _forms(tree):
            if _is_code(form):
                continue
            if not os.path.isdir(form) or os.path.islink(form):
                candidates.append(form)
            elif not any(_under(r, form) for r in read_list):
                candidates.append(form)
            else:
                try:
                    names = sorted(n for n in os.listdir(form) if (form / n).is_file()
                                   and not (form / n).is_symlink())[:4]
                except OSError:
                    names = []
                candidates += [form / n for n in names]
    out: List[str] = []
    for c in candidates:
        if os.path.lexists(c) and str(c) not in out:
            out.append(str(c))
    return out[:_MAX_CANARIES]


def preflight(repo: Any, store: Any, env: Optional[Mapping[str, str]] = None) -> Dict[str, Any]:
    """Model-free kernel check of the exact profile, run before every launch.

    A sandboxed probe tries to drop a uniquely named file in each writable
    root and in places that must stay untouched; the *host* then looks for
    those files. (Inside the sandbox, directories above the mounted roots are
    a private tmpfs, so a write there "succeeds" without reaching the host --
    only the host-side view is evidence.) It also requires socket creation,
    bind and connect to fail and credential/control files to be unreadable.
    Raises on any surprise, removing whatever the probe managed to create."""
    import uuid
    repo_p, store_p = _roots(repo, store)
    tag = f".wt-sandbox-probe-{uuid.uuid4().hex}"
    allowed = [repo_p, store_p, store_p / "tmp"]
    denied = [repo_p.parent, store_p.parent, Path.home(), Path("/tmp"), Path("/var/tmp"),
              Path("/dev/shm")]
    denied += [d for d in (repo_p / ".git", repo_p / ".codex", repo_p / ".agents",
                           store_p / ".codex", store_p / ".agents") if d.is_dir()]
    _files, trees = protected_paths(env)
    denied += [t for t in trees if os.path.isdir(t)][:8]
    denied = [d for d in dict.fromkeys(denied) if d.is_dir() and d not in allowed]
    reads = runtime_read_roots(env, store_p)
    secrets = _secret_canaries(env, reads)
    targets = allowed + denied
    try:
        proc = run_sandboxed(
            [sys.executable, "-I", "-S", "-c", _PROBE, tag, str(store_p), str(len(targets)),
             *[str(t) for t in targets], *secrets], repo_p, store_p, writable=True)
        try:
            result = json.loads(proc.stdout.strip().splitlines()[-1])
        except (ValueError, IndexError):
            raise SandboxPolicyError(f"sandbox preflight produced no result (exit {proc.returncode}): "
                                     + (proc.stderr or proc.stdout)[-400:].strip())
        problems = [f"cannot write {d}" for d in allowed if not os.path.lexists(d / tag)]
        problems += [f"wrote outside the roots: {d}" for d in denied if os.path.lexists(d / tag)]
        net = result.get("net") or {}
        problems += [f"socket {k} was permitted" for k in _DENIED_NET if net.get(k, "ok") == "ok"]
        if os.path.lexists(store_p / (tag + ".sock")):
            problems.append("unix socket bound in the store")
        problems += [f"credential or control path readable: {secrets[i]}"
                     for i in result.get("secrets") or [] if i < len(secrets)]
    finally:
        for d in targets:
            for name in (tag, tag + ".sock"):
                try:
                    os.unlink(d / name)
                except OSError:
                    pass
    if problems:
        raise SandboxPolicyError("sandbox preflight failed: " + "; ".join(problems))
    return {"writable": [str(d) for d in allowed], "denied": [str(d) for d in denied],
            "net": net, "secrets_hidden": len(secrets)}


# --------------------------------------------------------------------------
# store layout, in-sandbox wt shim, Git metadata inside the store


def _mkdir_private(path: Path) -> None:
    """Create a store directory without following a planted symlink."""
    try:
        os.mkdir(path, 0o700)
    except FileExistsError:
        st = os.lstat(path)
        if not _stat.S_ISDIR(st.st_mode):
            raise SandboxPolicyError(f"{path} exists and is not a directory")


def ensure_store_layout(store: Any) -> None:
    store_p = canonical_root(store, "worker store", must_exist=False)
    store_p.mkdir(parents=True, exist_ok=True)
    if store_p.is_symlink():
        raise SandboxPolicyError(f"worker store {store_p} is a symlink")
    for name in ("tmp", "home", "bin"):
        _mkdir_private(store_p / name)
    for name in (".codex", ".agents", "git"):
        if os.path.islink(store_p / name):
            raise SandboxPolicyError(f"{store_p / name} is a symlink; refusing the store")


_WT_SHIM = """#!{python} -I
import sys
sys.path.insert(0, {pkg_parent!r})
from watchtower.cli import main
sys.exit(main())
"""

# Runs inside the boot sandbox (workspace read-only, store writable). Git is
# the launcher's absolute $WT_GIT and every repository read is hardened; the
# alternates path is computed by the launcher from validated metadata.
_BOOTSTRAP = r"""
set -eu
REPO="$1"; STORE="$2"
g() { "$WT_GIT" -c core.fsmonitor=false -c core.hooksPath=/dev/null -c protocol.allow=never \
        -c core.commitGraph=false -c gc.auto=0 -c core.alternateRefsCommand= "$@"; }
: > "$STORE/gitconfig.new"
mv "$STORE/gitconfig.new" "$STORE/gitconfig"
G="$STORE/git"
if [ -e "$G/HEAD" ]; then
  g --git-dir="$G" rev-parse --verify -q 'refs/wt/base^{commit}'
  exit 0
fi
unset GIT_DIR GIT_WORK_TREE
[ -n "${WT_OBJECTS:-}" ] || { echo "no Git object store for the workspace" >&2; exit 3; }
head=$(g -C "$REPO" rev-parse --verify -q 'HEAD^{commit}') || { echo "workspace has no commit to use as the base" >&2; exit 3; }
branch=$(g -C "$REPO" symbolic-ref -q --short HEAD || echo wt-sandbox)
g init -q --bare "$G.new"
g --git-dir="$G.new" config core.bare false
g --git-dir="$G.new" config core.hooksPath /dev/null
printf '%s\n' "$WT_OBJECTS" > "$G.new/objects/info/alternates"
g --git-dir="$G.new" symbolic-ref HEAD "refs/heads/$branch"
g --git-dir="$G.new" update-ref "refs/heads/$branch" "$head"
g --git-dir="$G.new" update-ref refs/wt/base "$head"
g --git-dir="$G.new" --work-tree="$REPO" read-tree "$head"
mv "$G.new" "$G"
printf '%s\n' "$head"
"""

_GIT_SAFE = ("-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false",
             "-c", "protocol.allow=never", "-c", "core.commitGraph=false",
             "-c", "gc.auto=0", "-c", "core.alternateRefsCommand=",
             "-c", "core.untrackedCache=false")
# GIT_GRAFT_FILE: Git still honours <gitdir>/info/grafts, which rewrites a
# commit's parents even under GIT_NO_REPLACE_OBJECTS -- a worker-writable file
# that could graft its own orphan commit onto the base. Reproduced.
_PROOF_ENV = {"GIT_NO_REPLACE_OBJECTS": "1", "GIT_NO_LAZY_FETCH": "1",
              "GIT_GRAFT_FILE": "/dev/null",
              "GIT_OPTIONAL_LOCKS": "0", "GIT_CONFIG_GLOBAL": "/dev/null",
              "GIT_CONFIG_NOSYSTEM": "1", "GIT_TERMINAL_PROMPT": "0"}


def _bootstrap_reads(meta: Mapping[str, Any]) -> List[Path]:
    """What resolving HEAD of a linked worktree needs beyond its object store:
    its admin directory and the few named entries of the common directory --
    never the whole directory. Only this launcher-written script gets them."""
    if meta["kind"] != "worktree":
        return []
    common, gitdir = Path(meta["common"]), Path(meta["gitdir"])
    reads = [gitdir]
    for name in ("HEAD", "config", "refs", "packed-refs", "objects", "commondir"):
        entry = common / name
        if os.path.lexists(entry) and not os.path.islink(entry):
            reads.append(entry)
    for entry in reads:
        problem = _read_root_problem(_real(entry), None)
        if problem:
            raise SandboxPolicyError(f"Git metadata {entry} {problem}")
    return reads


def bootstrap_git(repo: Any, store: Any) -> str:
    """Create the worker's Git directory inside the store, *inside the
    sandbox*, borrowing the workspace's objects read-only. Returns the base
    commit, always a full object id: a workspace with no commit, a store that
    was initialized without a base, or any malformed answer is an error.
    Idempotent: an existing store yields its recorded ``refs/wt/base``.

    The host never runs Git against the workspace or the store here: Git is
    the trusted absolute executable, run in a sandbox where only the store is
    writable, so a hostile config in either can only act inside it."""
    repo_c, store_c = _roots(repo, store)
    meta = git_metadata(repo_c)
    if meta["kind"] == "none":
        raise SandboxPolicyError("workspace has no Git repository to base worker commits on")
    proc = run_sandboxed(
        ["/bin/sh", "-c", _BOOTSTRAP, "wt-git-bootstrap", str(repo_c), str(store_c)],
        repo_c, store_c, mode="boot", boot_reads=_bootstrap_reads(meta),
        env={**_PROOF_ENV, "WT_OBJECTS": str(meta["objects"])})
    if proc.returncode != 0:
        raise SandboxPolicyError(
            "sandbox Git bootstrap failed: " + (proc.stderr or proc.stdout)[-400:].strip())
    lines = [ln.strip() for ln in proc.stdout.splitlines() if ln.strip()]
    base = lines[-1] if lines else ""
    if not FULL_SHA.fullmatch(base):
        raise SandboxPolicyError("sandbox Git bootstrap produced no valid base commit")
    return base


def write_wt_shim(store: Any) -> Path:
    """An in-store ``wt`` that imports this package without the owner's
    site-packages (HOME differs inside the sandbox). Worker-writable, so it is
    only ever executed by sandboxed processes -- never by a launcher check."""
    store_p = canonical_root(store, "worker store")
    shim = store_p / "bin" / "wt"
    tmp = store_p / "bin" / f".wt.{os.getpid()}"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o755)
    with os.fdopen(fd, "w") as f:
        f.write(_WT_SHIM.format(python=sys.executable,
                                pkg_parent=str(Path(__file__).resolve().parent.parent)))
    os.replace(tmp, shim)
    return shim


def prepare(repo: Any, store: Any, *, worker: Optional[str] = None,
            env: Optional[Mapping[str, str]] = None) -> Dict[str, Any]:
    """Everything a real launch needs, in order; raises SandboxPolicyError.

    Guard roots and project config, lay out the store, prove enforcement in
    the kernel, then bootstrap the store-side Git directory. Returns the base
    commit (always a full id) for the launcher to record outside the worker's
    reach with ``record_launch``.

    A store whose Git directory already exists is only accepted when a launch
    record for ``worker`` (default: the store directory's name) names the same
    roots and its recorded base is what the store still holds: a worker-written
    store is never the source of its own base."""
    validate_roots(repo, store, env)
    repo_c, store_c = _roots(repo, store)
    hazards = project_config_hazards(repo_c, store_c, env)
    if hazards:
        raise SandboxPolicyError(
            "untrusted Codex project config present: " + ", ".join(hazards))
    git_metadata(repo_c)  # rejects a symlinked, malformed or untrusted .git
    if os.path.lexists(store_c / "git"):
        name = worker if worker is not None else store_c.name
        rec = launch_record(name)
        if not rec or rec.get("repo") != str(repo_c) or rec.get("store") != str(store_c):
            raise SandboxPolicyError(
                "worker store already holds Git state but has no matching launch record; "
                "re-mint the store instead of trusting its contents")
    else:
        rec = None
    ensure_store_layout(store_c)
    write_wt_shim(store_c)
    probe = preflight(repo_c, store_c, env)
    base = bootstrap_git(repo_c, store_c)
    if rec is not None and rec.get("base") != base:
        raise SandboxPolicyError("worker store base does not match its launch record")
    return {"base": base, "preflight": probe}


def _git_argv(git: str, store_c: Path) -> List[str]:
    return [git, "--git-dir=" + str(store_c / "git"), *_GIT_SAFE]


def verify_commit(repo: Any, store: Any, sha: str, base: str = "") -> Dict[str, Any]:
    """Check a worker's commit proof without trusting its Git directory.

    ``sha`` must be a full object id and ``base`` (recorded by the launcher at
    bootstrap) a full object id of the same length: an absent or malformed
    base is a refusal, never "no ancestry check". Git is the trusted absolute
    executable and runs inside the read-only sandbox variant against the
    store's Git directory, so a hostile config, alternate or hook there
    cannot execute on the host, cannot write, and can only read what the
    worker itself could (never another repository). The commit must be a
    strict descendant of the base -- the worker cannot present history it did
    not extend.
    """
    sha = str(sha or "").strip().lower()
    if not FULL_SHA.fullmatch(sha):
        return {"ok": False, "error": "commit proof must be a full commit id"}
    base = str(base or "").strip().lower()
    if not FULL_SHA.fullmatch(base) or len(base) != len(sha):
        return {"ok": False, "error": "recorded base commit is missing or not a full commit id"}
    try:
        repo_c, store_c = _roots(repo, store)
        git = trusted_git(repo_c, store_c)
        argv = _git_argv(git, store_c)
        run = dict(mode="ro", env=_PROOF_ENV, max_output=1 << 16)
        proc = run_sandboxed([*argv, "rev-parse", "--verify", "-q", sha + "^{commit}"],
                             repo_c, store_c, **run)
        if proc.returncode != 0 or proc.stdout.strip() != sha:
            return {"ok": False, "error": "commit not found in the worker's repository"}
        if base == sha:
            return {"ok": False, "error": "commit proof is the unchanged base commit"}
        anc = run_sandboxed([*argv, "merge-base", "--is-ancestor", base, sha],
                            repo_c, store_c, **run)
        if anc.returncode != 0:
            return {"ok": False, "error": "commit does not descend from the assigned base"}
    except SandboxPolicyError as exc:
        return {"ok": False, "error": str(exc)}
    return {"ok": True, "commit": sha, "base": base}


def export_patch(repo: Any, store: Any, sha: str, base: str,
                 max_bytes: int = _MAX_PATCH) -> bytes:
    """The worker's work ``base..sha`` as ONE squashed ``git diff --binary``
    (bytes, exactly as Git printed them), produced in the read-only sandbox.
    Inert data for a trusted integration step (``git apply --index`` in a
    host-owned checkout). Raises if the proof does not verify, or if the diff
    is larger than ``max_bytes`` (default 8 MiB; general captured output is
    capped at 1 MiB elsewhere)."""
    checked = verify_commit(repo, store, sha, base)
    if not checked.get("ok"):
        raise SandboxPolicyError(str(checked.get("error")))
    sha, base = checked["commit"], checked["base"]
    repo_c, store_c = _roots(repo, store)
    argv = _git_argv(trusted_git(repo_c, store_c), store_c)
    try:
        proc = run_sandboxed(
            [*argv, "diff", "--binary", "--full-index", "--no-ext-diff", "--no-textconv",
             "--no-renames", "--no-color", "--ignore-submodules=none",
             "--src-prefix=a/", "--dst-prefix=b/", base, sha, "--"],
            repo_c, store_c, mode="ro", env=_PROOF_ENV, max_output=max_bytes + 1,
            binary=True)
    except SandboxPolicyError as exc:
        if "exceeded" in str(exc):
            raise SandboxPolicyError("patch export exceeds the size limit") from exc
        raise
    if proc.returncode != 0:
        raise SandboxPolicyError("patch export failed: " + proc.stderr[-300:].strip())
    if len(proc.stdout) > max_bytes:
        raise SandboxPolicyError("patch export exceeds the size limit")
    return proc.stdout


# --------------------------------------------------------------------------
# launch record (outside the worker's writable roots)


def _record_dir() -> Path:
    from . import storage
    return storage.data_dir() / "sandbox-policy"


def _check_record_dir(directory: Path) -> None:
    st = os.lstat(directory)
    if (not _stat.S_ISDIR(st.st_mode) or st.st_uid != os.getuid()
            or st.st_mode & 0o022):
        raise SandboxPolicyError(f"{directory} is not a private directory owned by this account")


def _valid_record(worker: str, data: Any) -> Optional[Dict[str, str]]:
    if not isinstance(data, dict) or set(data) != {"worker", "repo", "store", "base"}:
        return None
    if not all(isinstance(data[k], str) for k in data) or data["worker"] != worker:
        return None
    for key in ("repo", "store"):
        value = data[key]
        if (not os.path.isabs(value) or os.path.normpath(value) != value
                or "\x00" in value):
            return None
    base = data["base"]
    return dict(data) if FULL_SHA.fullmatch(base) else None


def record_launch(worker: str, repo: Any, store: Any, base: str) -> None:
    """Remember the worker's canonical roots and base commit where it cannot
    write. The record is write-once: repeating the exact same launch is a
    no-op, anything different for an existing worker id is refused, and an
    empty or malformed base is never recorded."""
    if not _WORKER_ID.fullmatch(worker or ""):
        raise SandboxPolicyError("invalid sandbox worker id")
    base = str(base or "").strip().lower()
    if not FULL_SHA.fullmatch(base):
        raise SandboxPolicyError("refusing to record a launch without a full base commit id")
    repo_c, store_c = _roots(repo, store)
    directory = _record_dir()
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    _check_record_dir(directory)
    for root in (repo_c, store_c):
        if _under(_real(directory), root):
            raise SandboxPolicyError("sandbox launch records would be worker-writable")
    payload = {"worker": worker, "repo": str(repo_c), "store": str(store_c), "base": base}
    import uuid
    tmp = directory / f".{worker}.{uuid.uuid4().hex}.tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o400)
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(payload, f)
            f.flush()
            os.fsync(f.fileno())
        try:
            os.link(tmp, directory / f"{worker}.json")   # atomic, never overwrites
        except FileExistsError:
            if launch_record(worker) != payload:
                raise SandboxPolicyError(
                    f"sandbox worker {worker} already has a different launch record") from None
    finally:
        try:
            os.unlink(tmp)
        except OSError:
            pass


def launch_record(worker: str) -> Optional[Dict[str, Any]]:
    """The launch record for ``worker`` or None. Strict: a regular file this
    account owns, not writable by others, small, exactly the four known keys
    of the right types, matching worker id, canonical paths and a full base."""
    if not _WORKER_ID.fullmatch(worker or ""):
        return None
    directory = _record_dir()
    try:
        _check_record_dir(directory)
        fd = os.open(directory / f"{worker}.json", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except (OSError, SandboxPolicyError):
        return None
    try:
        st = os.fstat(fd)
        if (not _stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or st.st_mode & 0o022
                or st.st_size > _MAX_RECORD):
            return None
        raw = os.read(fd, _MAX_RECORD + 1)
    finally:
        os.close(fd)
    try:
        return _valid_record(worker, json.loads(raw.decode("utf-8")))
    except (ValueError, UnicodeDecodeError):
        return None


def verify_worker_commit(worker: str, sha: str) -> Dict[str, Any]:
    """Commit proof for a registered sandbox worker, using only the roots and
    base recorded at launch -- nothing the worker supplies but the id."""
    rec = launch_record(worker)
    if not rec:
        return {"ok": False, "error": "no valid sandbox launch record for this worker"}
    try:
        for key in ("repo", "store"):
            if str(canonical_root(rec[key], key, must_exist=(key == "repo"))) != rec[key]:
                return {"ok": False, "error": f"launch record {key} is no longer canonical"}
    except SandboxPolicyError as exc:
        return {"ok": False, "error": str(exc)}
    return verify_commit(rec["repo"], rec["store"], sha, rec["base"])

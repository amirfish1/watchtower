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
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence

PROFILE = "wt-sandbox"
VERIFY_PROFILE = "wt-sandbox-ro"
READ_ROOTS_ENV = "WATCHTOWER_SANDBOX_READ_ROOTS"
FULL_SHA = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_RUN_TIMEOUT_S = 60.0
_MAX_OUTPUT = 1 << 20

# Tool surfaces that run outside the kernel sandbox or reach the network on
# the model's behalf. A worker reading a hostile ticket must not have them.
DISABLED_FEATURES = (
    "apps", "plugins", "remote_plugin", "plugin_sharing", "hooks",
    "browser_use", "browser_use_external", "browser_use_full_cdp_access",
    "in_app_browser", "computer_use", "in_app_local_automation",
    "image_generation", "skill_mcp_dependency_install", "memories",
    "goals", "multi_agent", "multi_agent_v2",
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


def codex_binary() -> str:
    """The real Codex executable, or raise."""
    from . import workers
    found = workers._resolve_engine_bin("codex") or shutil.which("codex")
    if not found:
        raise SandboxPolicyError("codex executable not found")
    return found


def _codex_release_dir(binary: str) -> Optional[Path]:
    """Directory holding the Codex binary and its sandbox helper. Never the
    Codex home itself (that would expose auth.json)."""
    real = _real(binary)
    parent = real.parent
    release = parent.parent if parent.name == "bin" else parent
    for candidate in (release, parent):
        if (candidate / "auth.json").exists() or candidate == Path(candidate.anchor):
            continue
        if str(candidate) in ("/usr", "/usr/local", "/bin", "/opt"):
            return None  # already covered by :minimal or too broad to add
        return candidate
    return None


def codex_homes() -> List[Path]:
    """Trusted Codex homes (owner config). Their ``.codex`` is not a project."""
    homes = [Path.home() / ".codex"]
    try:  # the account's real home, even when HOME was redirected
        import pwd
        homes.append(Path(pwd.getpwuid(os.getuid()).pw_dir) / ".codex")
    except (ImportError, KeyError, OSError):
        pass
    env = os.environ.get("CODEX_HOME")
    if env:
        homes.append(Path(env))
    try:
        from . import config
        worker = config.codex_worker_home()
        if worker is not None:
            homes.append(Path(worker))
    except Exception:  # noqa: BLE001 -- config is advisory here
        pass
    return [_real(h) for h in homes]


def runtime_read_roots() -> List[Path]:
    """Read-only paths the worker needs beyond ``:minimal``: the Codex release
    (its sandbox helper re-executes from there), this Python, and the
    ``watchtower`` package for the in-sandbox ``wt`` shim. ``/usr``-style
    locations are already readable. Operators add more with
    ``WATCHTOWER_SANDBOX_READ_ROOTS`` (``os.pathsep``-separated)."""
    roots: List[Path] = []
    try:
        release = _codex_release_dir(codex_binary())
    except SandboxPolicyError:
        release = None
    if release is not None:
        roots.append(release)
    for prefix in (sys.prefix, sys.base_prefix, str(_real(sys.executable).parent)):
        p = _real(prefix)
        if not any(_under(p, Path(s)) for s in ("/usr", "/bin", "/lib", "/opt")):
            roots.append(p)
    roots.append(Path(__file__).resolve().parent)
    for extra in (os.environ.get(READ_ROOTS_ENV) or "").split(os.pathsep):
        if extra.strip():
            roots.append(_abs(Path(extra.strip()).expanduser()))
    out: List[Path] = []
    for r in roots:
        if r not in out:
            out.append(r)
    return out


# --------------------------------------------------------------------------
# workspace inspection (no host git: the workspace is untrusted input)


def git_metadata(repo: Any) -> Dict[str, Any]:
    """Describe the workspace's Git metadata by reading files only.

    ``kind`` is ``none``, ``repo`` (``.git`` directory) or ``worktree``
    (``.git`` file naming a gitdir). ``objects`` is the object store the
    worker's own Git directory borrows from, ``common`` the shared Git dir."""
    repo = _abs(repo)
    dot = repo / ".git"
    try:
        st = os.lstat(dot)
    except FileNotFoundError:
        return {"kind": "none", "gitdir": None, "common": None, "objects": None}
    import stat as _stat
    if _stat.S_ISLNK(st.st_mode):
        raise SandboxPolicyError(f"{dot} is a symlink; refusing the workspace")
    if _stat.S_ISDIR(st.st_mode):
        gitdir, kind = dot, "repo"
    elif _stat.S_ISREG(st.st_mode) and st.st_size < 4096:
        text = dot.read_text(errors="replace").strip()
        if not text.startswith("gitdir:"):
            raise SandboxPolicyError(f"{dot} is not a Git directory pointer")
        target = Path(text.split(":", 1)[1].strip())
        gitdir = _real(target if target.is_absolute() else repo / target)
        kind = "worktree"
    else:
        raise SandboxPolicyError(f"{dot} is not a usable Git directory")
    common = gitdir
    pointer = gitdir / "commondir"
    if pointer.is_file():
        rel = Path(pointer.read_text(errors="replace").strip())
        common = _real(rel if rel.is_absolute() else gitdir / rel)
    return {"kind": kind, "gitdir": gitdir, "common": common, "objects": common / "objects"}


def project_config_hazards(repo: Any) -> List[str]:
    """``.codex`` entries Codex would treat as project config for ``repo``.

    Codex 0.169 merged a workspace ``.codex/config.toml`` into the session --
    widening the permission profile and starting its MCP command on the host --
    even from a fresh home with no trust recorded. Any ``.codex`` in the
    workspace or an ancestor (other than the owner's Codex homes) is therefore
    a launch blocker and a reason to stop a running worker."""
    repo = _abs(repo)
    homes = codex_homes()
    found: List[str] = []
    for directory in (repo, *repo.parents):
        candidate = directory / ".codex"
        if not os.path.lexists(candidate):
            continue
        if not candidate.is_symlink() and _real(candidate) in homes:
            continue
        # Codex temporarily materializes missing read-only mount targets on
        # the host while a tool is running. Empty directories contain no
        # executable config; rejecting them breaks a concurrent broker proof.
        # Symlinks, non-directories, unreadable or populated entries fail shut.
        try:
            if not candidate.is_symlink() and candidate.is_dir() and not any(candidate.iterdir()):
                continue
        except OSError:
            pass
        found.append(str(candidate))
    return found


def untrusted_project_paths(repo: Any, store: Any = None) -> List[str]:
    """Every key Codex might consult for project trust: the workspace, its
    ancestors (an owner may have trusted ``/tmp`` or ``$HOME``), the main
    checkout of a worktree, and the store."""
    repo = _abs(repo)
    paths: List[Path] = [repo, _real(repo)]
    paths += [p for p in repo.parents if p != Path(p.anchor)]
    try:
        meta = git_metadata(repo)
        if meta["kind"] == "worktree" and meta["common"] is not None:
            paths.append(Path(meta["common"]).parent)
    except (SandboxPolicyError, OSError):
        pass
    if store is not None:
        paths += [_abs(store), _real(store)]
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


def validate_roots(repo: Any, store: Any) -> None:
    repo_p, store_p = _abs(repo), _abs(store)
    if not repo_p.is_dir():
        raise SandboxPolicyError(f"workspace {str(repo)!r} is not a directory")
    for label, p in (("workspace", repo_p), ("store", store_p)):
        real = _real(p)
        if _under(real, Path("/dev/shm")):
            raise SandboxPolicyError(
                f"{label} {p} resolves under /dev/shm, which the profile denies")
        if real == Path(real.anchor) or real == _real(Path.home()):
            raise SandboxPolicyError(f"{label} {p} is too broad to be a writable root")
    if _under(_real(store_p), _real(repo_p)) or _under(_real(repo_p), _real(store_p)):
        raise SandboxPolicyError("workspace and worker store must be separate roots")
    for home in codex_homes():
        for p in (repo_p, store_p):
            if _under(home, _real(p)) or _under(_real(p), home):
                raise SandboxPolicyError(f"{p} overlaps the Codex home {home}")


def filesystem_rules(repo: Any, store: Any, *, writable: bool = True,
                     read_roots: Optional[Iterable[Any]] = None) -> Dict[str, str]:
    """The profile's filesystem table. ``writable=False`` is the proof-check
    variant: same reads, nothing writable."""
    repo_p, store_p = _abs(repo), _abs(store)
    access = "write" if writable else "read"
    rules: Dict[str, str] = {":minimal": "read", "/dev/shm": "deny"}
    for root in (runtime_read_roots() if read_roots is None else [_abs(r) for r in read_roots]):
        rules[str(root)] = "read"
    try:
        objects = git_metadata(repo_p)["objects"]
    except SandboxPolicyError:
        objects = None
    if objects is not None and not _under(Path(objects), repo_p):
        # A shared worktree's objects live outside the workspace: readable for
        # the store's alternates, never writable.
        rules[str(objects)] = "read"
    rules[str(repo_p)] = access
    rules[str(store_p)] = access
    # Deny beats write and the narrower path wins: host-owned metadata and
    # anything Codex would execute as project config stay out of reach.
    for protected in (".git", ".codex", ".agents"):
        rules[str(repo_p / protected)] = "read"
    rules[str(store_p / ".codex")] = "read"
    return rules


def git_env(repo: Any, store: Any) -> Dict[str, str]:
    """Environment for *sandboxed* Git only. Never export this to a host
    process: the store's Git directory is worker-writable, so its config can
    name commands (fsmonitor, hooks, pagers)."""
    store_p = _abs(store)
    email = (os.environ.get("GIT_AUTHOR_EMAIL") or os.environ.get("GIT_COMMITTER_EMAIL")
             or _owner_git_email() or "watchtower-sandbox@localhost")
    return {
        "GIT_DIR": str(store_p / "git"),
        "GIT_WORK_TREE": str(_abs(repo)),
        "GIT_CONFIG_GLOBAL": str(store_p / "gitconfig"),
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_AUTHOR_EMAIL": email,
        "GIT_COMMITTER_EMAIL": email,
    }


_EMAIL_CACHE: Dict[str, str] = {}


def _owner_git_email() -> str:
    """The owner's global Git email. Run from ``/`` so no repository config is
    read; only the owner's own global file."""
    if "v" not in _EMAIL_CACHE:
        value = ""
        git = shutil.which("git")
        if git:
            try:
                out = subprocess.run(
                    [git, "config", "--global", "--get", "user.email"], cwd="/",
                    capture_output=True, text=True, timeout=10,
                    env={"PATH": os.environ.get("PATH", ""), "HOME": str(Path.home()),
                         "GIT_CONFIG_NOSYSTEM": "1"})
                value = out.stdout.strip() if out.returncode == 0 else ""
            except (OSError, subprocess.SubprocessError):
                value = ""
        _EMAIL_CACHE["v"] = value if re.fullmatch(r"[^\s<>\"']{1,200}", value or "") else ""
    return _EMAIL_CACHE["v"]


def tool_env(repo: Any, store: Any) -> Dict[str, str]:
    """Variables forced into every sandboxed tool process
    (``shell_environment_policy.set``): a private HOME/TMPDIR inside the store,
    the store-side Git directory, and a PATH that finds the in-store ``wt``."""
    store_p = _abs(store)
    env = {
        "HOME": str(store_p / "home"),
        "TMPDIR": str(store_p / "tmp"),
        "XDG_CACHE_HOME": str(store_p / "home" / ".cache"),
        "XDG_CONFIG_HOME": str(store_p / "home" / ".config"),
        "PATH": os.pathsep.join([str(store_p / "bin"),
                                 os.environ.get("PATH") or "/usr/local/bin:/usr/bin:/bin"]),
    }
    env.update(git_env(repo, store))
    return env


def _user_mcp_servers() -> List[str]:
    """Names of MCP servers in the owner's Codex config; each is disabled for
    the sandboxed session (an empty-table override would merge, not clear)."""
    names: List[str] = []
    try:
        import tomllib
    except ImportError:  # pragma: no cover - Python < 3.11
        return names
    for home in codex_homes():
        try:
            data = tomllib.loads((home / "config.toml").read_text())
        except (OSError, ValueError):
            continue
        for name in (data.get("mcp_servers") or {}):
            if str(name) not in names:
                names.append(str(name))
    return names


def config_overrides(repo: Any, store: Any, *,
                     read_roots: Optional[Iterable[Any]] = None) -> List[str]:
    """``key=value`` overrides (each goes after a ``-c``) shared by ``codex
    exec``, ``codex app-server`` and ``codex sandbox``."""
    validate_roots(repo, store)
    rw = filesystem_rules(repo, store, writable=True, read_roots=read_roots)
    ro = filesystem_rules(repo, store, writable=False, read_roots=read_roots)
    overrides = [
        f"default_permissions={_toml(PROFILE)}",
        f"permissions.{PROFILE}={_toml({'filesystem': rw, 'network': {'enabled': False}})}",
        f"permissions.{VERIFY_PROFILE}={_toml({'filesystem': ro, 'network': {'enabled': False}})}",
        'approval_policy="never"',
        "projects=" + _toml({p: {"trust_level": "untrusted"}
                             for p in untrusted_project_paths(repo, store)}),
        "notify=[]",
        'web_search="disabled"',
        f"shell_environment_policy.set={_toml(tool_env(repo, store))}",
    ]
    overrides += [f"features.{name}=false" for name in DISABLED_FEATURES]
    # Inline-table keys quote every name, including dots/spaces. Filtering
    # names to identifiers would leave inherited non-identifier servers live.
    servers = _user_mcp_servers()
    if servers:
        overrides.append("mcp_servers=" + _toml({name: {"enabled": False} for name in servers}))
    overrides.append("agents.enabled=false")
    return overrides


def exec_args(repo: Any, store: Any) -> List[str]:
    """Arguments placed after ``codex exec`` for a sandboxed worker."""
    args = ["--ignore-rules"]
    for override in config_overrides(repo, store):
        args += ["-c", override]
    return args


def thread_config(repo: Any, store: Any) -> Dict[str, Any]:
    """``config`` for app-server ``thread/start``. The profile must come from
    here and the server's ``-c`` overrides -- never the legacy ``sandbox``
    field, which replaces it with a plain workspace-write policy. A
    thread-level ``projects`` entry is what stopped project config loading in
    testing; the CLI override alone did not for a dotted key."""
    return {
        "default_permissions": PROFILE,
        "projects": {p: {"trust_level": "untrusted"}
                     for p in untrusted_project_paths(repo, store)},
    }


def check_thread_sandbox(summary: Any, repo: Any, store: Any) -> None:
    """Fail closed unless ``thread/start`` reports the policy we asked for.

    The response projects the profile onto the legacy shape: workspace-write,
    no network, and writable roots that are a subset of ours."""
    if not isinstance(summary, dict) or summary.get("type") != "workspaceWrite":
        raise SandboxPolicyError(f"thread sandbox is {summary!r}, not the workspace profile")
    if summary.get("networkAccess") is not False:
        raise SandboxPolicyError("thread sandbox allows tool network access")
    allowed = {str(_abs(repo)), str(_real(repo)), str(_abs(store)), str(_real(store))}
    extra = [r for r in (summary.get("writableRoots") or []) if str(r) not in allowed]
    if extra:
        raise SandboxPolicyError(f"thread sandbox has unexpected writable roots: {extra}")
    if summary.get("excludeSlashTmp") is not True or summary.get("excludeTmpdirEnvVar") is not True:
        raise SandboxPolicyError("thread sandbox leaves the host temp directories writable")


# --------------------------------------------------------------------------
# environment


def scrub_env(env: Dict[str, str], store: Any) -> Dict[str, str]:
    """Allowlist the worker's environment: no credentials, proxies, agent
    sockets or daemon paths; TMPDIR inside the store."""
    kept = {k: v for k, v in env.items()
            if k in _ENV_KEEP or k in _ENV_WT or k.startswith(_ENV_KEEP_PREFIXES)}
    kept["TMPDIR"] = str(_abs(store) / "tmp")
    kept.setdefault("PATH", "/usr/local/bin:/usr/bin:/bin")
    return kept


# --------------------------------------------------------------------------
# running things inside the sandbox


def run_sandboxed(argv: Sequence[str], repo: Any, store: Any, *, writable: bool = False,
                  env: Optional[Dict[str, str]] = None, input_text: Optional[str] = None,
                  timeout: float = _RUN_TIMEOUT_S,
                  read_roots: Optional[Iterable[Any]] = None) -> subprocess.CompletedProcess:
    """Run ``argv`` under the worker profile with ``codex sandbox`` (no model).

    ``writable=False`` uses the read-only variant, for checks on
    worker-controlled data such as its Git directory. The host side starts in
    ``/`` so no project config is discovered from a worker-writable cwd."""
    hazards = project_config_hazards(repo)
    if hazards:
        raise SandboxPolicyError(f"untrusted Codex project config present: {', '.join(hazards)}")
    # `codex sandbox` hands its own environment to the command, so the command
    # is started through `env -i` with exactly the tool view.
    inner = dict(tool_env(repo, store))
    for key in ("GIT_AUTHOR_NAME", "GIT_COMMITTER_NAME"):
        inner[key] = os.environ.get(key) or "WatchTower sandbox worker"
    inner.update(env or {})
    cmd = [codex_binary(), "sandbox", "-P", PROFILE if writable else VERIFY_PROFILE]
    for override in config_overrides(repo, store, read_roots=read_roots):
        cmd += ["-c", override]
    cmd += ["--", "/usr/bin/env", "-i", *[f"{k}={v}" for k, v in inner.items()], *argv]
    child_env = scrub_env(dict(os.environ), store)
    try:
        return subprocess.run(cmd, cwd="/", env=child_env, input=input_text,
                              capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        raise SandboxPolicyError(f"sandboxed command timed out: {argv[0]}") from exc
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
        if os.path.isdir(path): os.listdir(path)
        else:
            with open(path, "rb") as f: f.read(1)
        out["secrets"].append(i)
    except OSError:
        pass
print(json.dumps(out))
"""

_DENIED_NET = ("inet", "inet6", "unix_bind", "unix_connect")


def _secret_canaries() -> List[str]:
    """Existing credential locations the profile must hide from the worker."""
    home = Path.home()
    candidates = [h / "auth.json" for h in codex_homes()]
    candidates += [home / ".ssh", home / ".aws", home / ".gnupg", home / ".netrc",
                   home / ".config" / "gh", home / ".claude" / ".credentials.json",
                   home / ".watchtower" / "queue.db"]
    out: List[str] = []
    for c in candidates:
        if os.path.lexists(c) and str(c) not in out:
            out.append(str(c))
    return out


def preflight(repo: Any, store: Any) -> Dict[str, Any]:
    """Model-free kernel check of the exact profile, run before every launch.

    A sandboxed probe tries to drop a uniquely named file in each writable
    root and in places that must stay untouched; the *host* then looks for
    those files. (Inside the sandbox, directories above the mounted roots are
    a private tmpfs, so a write there "succeeds" without reaching the host --
    only the host-side view is evidence.) It also requires socket creation,
    bind and connect to fail and credential files to be unreadable. Raises on
    any surprise, removing whatever the probe managed to create."""
    import uuid
    repo_p, store_p = _abs(repo), _abs(store)
    tag = f".wt-sandbox-probe-{uuid.uuid4().hex}"
    allowed = [repo_p, store_p, store_p / "tmp"]
    denied = [repo_p.parent, store_p.parent, Path.home(), Path("/tmp"), Path("/var/tmp"),
              Path("/dev/shm")]
    denied += [d for d in (repo_p / ".git", repo_p / ".codex", repo_p / ".agents",
                           store_p / ".codex") if d.is_dir()]
    denied = [d for d in denied if d.is_dir() and d not in allowed]
    secrets = _secret_canaries()
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
        problems += [f"credential path readable: {secrets[i]}"
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
        import stat as _stat
        if not _stat.S_ISDIR(st.st_mode):
            raise SandboxPolicyError(f"{path} exists and is not a directory")


def ensure_store_layout(store: Any) -> None:
    store_p = _abs(store)
    store_p.mkdir(parents=True, exist_ok=True)
    if store_p.is_symlink():
        raise SandboxPolicyError(f"worker store {store_p} is a symlink")
    for name in ("tmp", "home", "bin"):
        _mkdir_private(store_p / name)


_WT_SHIM = """#!{python} -I
import sys
sys.path.insert(0, {pkg_parent!r})
from watchtower.cli import main
sys.exit(main())
"""

_BOOTSTRAP = r"""
set -eu
REPO="$1"; STORE="$2"
: > "$STORE/gitconfig.new"
mv "$STORE/gitconfig.new" "$STORE/gitconfig"
G="$STORE/git"
if [ -e "$G/HEAD" ]; then
  git --git-dir="$G" rev-parse --verify -q refs/wt/base || true
  exit 0
fi
unset GIT_DIR GIT_WORK_TREE
head=""; branch="wt-sandbox"; objects=""
if [ -e "$REPO/.git" ]; then
  head=$(git -C "$REPO" rev-parse --verify -q 'HEAD^{commit}' || true)
  branch=$(git -C "$REPO" symbolic-ref -q --short HEAD || echo wt-sandbox)
  objects=$(git -C "$REPO" rev-parse --path-format=absolute --git-path objects)
fi
git init -q --bare "$G.new"
git --git-dir="$G.new" config core.bare false
git --git-dir="$G.new" config core.hooksPath /dev/null
if [ -n "$objects" ]; then printf '%s\n' "$objects" > "$G.new/objects/info/alternates"; fi
git --git-dir="$G.new" symbolic-ref HEAD "refs/heads/$branch"
if [ -n "$head" ]; then
  git --git-dir="$G.new" update-ref "refs/heads/$branch" "$head"
  git --git-dir="$G.new" update-ref refs/wt/base "$head"
  git --git-dir="$G.new" --work-tree="$REPO" read-tree "$head"
fi
mv "$G.new" "$G"
printf '%s\n' "$head"
"""


def bootstrap_git(repo: Any, store: Any) -> str:
    """Create the worker's Git directory inside the store, *inside the
    sandbox*, borrowing the workspace's objects read-only. Returns the base
    commit (``""`` for a workspace with no commit). Idempotent.

    The host never runs Git against the workspace or the store here: a
    hostile config in either can only act within the sandbox."""
    # Resolving HEAD of a shared worktree needs its refs, which live in the
    # main checkout's Git directory. Only this launcher-written script gets
    # that read; the worker's own profile sees the object store alone.
    read_roots = runtime_read_roots()
    meta = git_metadata(repo)
    if meta["common"] is not None and not _under(Path(meta["common"]), _abs(repo)):
        read_roots.append(Path(meta["common"]))
    proc = run_sandboxed(["/bin/sh", "-c", _BOOTSTRAP, "wt-git-bootstrap",
                          str(_abs(repo)), str(_abs(store))],
                         repo, store, writable=True, read_roots=read_roots)
    if proc.returncode != 0:
        raise SandboxPolicyError(
            "sandbox Git bootstrap failed: " + (proc.stderr or proc.stdout)[-400:].strip())
    lines = [ln.strip() for ln in proc.stdout.splitlines() if ln.strip()]
    base = lines[-1] if lines else ""
    return base if FULL_SHA.fullmatch(base) else ""


def write_wt_shim(store: Any) -> Path:
    """An in-store ``wt`` that imports this package without the owner's
    site-packages (HOME differs inside the sandbox). Worker-writable, so it is
    only ever executed by sandboxed processes."""
    store_p = _abs(store)
    shim = store_p / "bin" / "wt"
    tmp = store_p / "bin" / f".wt.{os.getpid()}"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o755)
    with os.fdopen(fd, "w") as f:
        f.write(_WT_SHIM.format(python=sys.executable,
                                pkg_parent=str(Path(__file__).resolve().parent.parent)))
    os.replace(tmp, shim)
    return shim


def prepare(repo: Any, store: Any) -> Dict[str, Any]:
    """Everything a real launch needs, in order; raises SandboxPolicyError.

    Guard project config, lay out the store, prove enforcement in the kernel,
    then bootstrap the store-side Git directory. Returns the base commit for
    the launcher to record outside the worker's reach."""
    validate_roots(repo, store)
    hazards = project_config_hazards(repo)
    if hazards:
        raise SandboxPolicyError(
            "untrusted Codex project config present: " + ", ".join(hazards))
    git_metadata(repo)  # rejects a symlinked or malformed .git
    ensure_store_layout(store)
    write_wt_shim(store)
    probe = preflight(repo, store)
    base = bootstrap_git(repo, store)
    return {"base": base, "preflight": probe}


def verify_commit(repo: Any, store: Any, sha: str, base: str = "") -> Dict[str, Any]:
    """Check a worker's commit proof without trusting its Git directory.

    ``sha`` must be a full object id. Git runs inside the read-only sandbox
    variant against the store's Git directory, so a hostile config, alternate
    or hook there cannot execute on the host, cannot write, and can only read
    what the worker itself could (never another repository). With ``base``
    (recorded by the launcher at bootstrap) the commit must be a strict
    descendant of it -- the worker cannot present history it did not extend.
    """
    sha = str(sha or "").strip().lower()
    if not FULL_SHA.fullmatch(sha):
        return {"ok": False, "error": "commit proof must be a full commit id"}
    if base and not FULL_SHA.fullmatch(base):
        return {"ok": False, "error": "recorded base commit is not a full commit id"}
    gitdir = str(_abs(store) / "git")
    git = ["git", "--git-dir=" + gitdir, "-c", "core.hooksPath=/dev/null",
           "-c", "core.fsmonitor=false", "-c", "protocol.allow=never"]
    env = {"GIT_DIR": gitdir, "GIT_NO_REPLACE_OBJECTS": "1", "GIT_NO_LAZY_FETCH": "1",
           "GIT_OPTIONAL_LOCKS": "0"}
    try:
        proc = run_sandboxed([*git, "rev-parse", "--verify", "-q", sha + "^{commit}"],
                             repo, store, writable=False, env=env)
        if proc.returncode != 0 or proc.stdout.strip() != sha:
            return {"ok": False, "error": "commit not found in the worker's repository"}
        if base:
            if base == sha:
                return {"ok": False, "error": "commit proof is the unchanged base commit"}
            anc = run_sandboxed([*git, "merge-base", "--is-ancestor", base, sha],
                                repo, store, writable=False, env=env)
            if anc.returncode != 0:
                return {"ok": False, "error": "commit does not descend from the assigned base"}
    except SandboxPolicyError as exc:
        return {"ok": False, "error": str(exc)}
    return {"ok": True, "commit": sha, "base": base}


def export_patch(repo: Any, store: Any, sha: str, base: str,
                 max_bytes: int = _MAX_OUTPUT * 8) -> str:
    """The worker's commits ``base..sha`` as an mbox, produced in the
    read-only sandbox. Data for a trusted integration step (``git am`` in a
    host-owned checkout); raises if the proof does not verify or is too big."""
    checked = verify_commit(repo, store, sha, base)
    if not checked.get("ok"):
        raise SandboxPolicyError(str(checked.get("error")))
    gitdir = str(_abs(store) / "git")
    proc = run_sandboxed(
        ["git", "--git-dir=" + gitdir, "-c", "core.hooksPath=/dev/null",
         "-c", "core.fsmonitor=false", "-c", "diff.external=", "format-patch",
         "--stdout", "--binary", "--no-ext-diff", "--no-textconv", f"{base}..{sha}"],
        repo, store, writable=False,
        env={"GIT_DIR": gitdir, "GIT_NO_REPLACE_OBJECTS": "1", "GIT_NO_LAZY_FETCH": "1"})
    if proc.returncode != 0:
        raise SandboxPolicyError("patch export failed: " + proc.stderr[-300:].strip())
    if len(proc.stdout.encode()) > max_bytes:
        raise SandboxPolicyError("patch export exceeds the size limit")
    return proc.stdout


# --------------------------------------------------------------------------
# launch record (outside the worker's writable roots)


def _record_dir() -> Path:
    from . import storage
    return storage.data_dir() / "sandbox-policy"


def record_launch(worker: str, repo: Any, store: Any, base: str) -> None:
    """Remember the worker's roots and base commit where it cannot write."""
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", worker or ""):
        raise SandboxPolicyError("invalid sandbox worker id")
    directory = _record_dir()
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    for root in (_real(repo), _real(store)):
        if _under(_real(directory), root):
            raise SandboxPolicyError("sandbox launch records would be worker-writable")
    tmp = directory / f".{worker}.{os.getpid()}.tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump({"worker": worker, "repo": str(_abs(repo)), "store": str(_abs(store)),
                   "base": base}, f)
    os.replace(tmp, directory / f"{worker}.json")


def launch_record(worker: str) -> Optional[Dict[str, Any]]:
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", worker or ""):
        return None
    try:
        data = json.loads((_record_dir() / f"{worker}.json").read_text())
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def verify_worker_commit(worker: str, sha: str) -> Dict[str, Any]:
    """Commit proof for a registered sandbox worker, using only the roots and
    base recorded at launch -- nothing the worker supplies but the id."""
    rec = launch_record(worker)
    if not rec:
        return {"ok": False, "error": "no sandbox launch record for this worker"}
    return verify_commit(rec["repo"], rec["store"], sha, str(rec.get("base") or ""))

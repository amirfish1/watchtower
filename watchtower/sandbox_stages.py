"""WT-61: isolated transports for sandbox-origin gates and stages.

A ticket whose builder ran in the Codex permission-profile sandbox is never
checked out, tested or inspected with the daemon's authority: its source, its
command gates and its independent verifier/assessor all run under the same
restricted kernel profile, with no network and exactly two writable roots, both
of them fresh directories this module allocates and later removes.

This module owns the pieces every such transport shares:

* **Source.** ``source_objects`` names the *trusted* repository's object stores
  (never a worker's store) as the only extra read roots; ``resolve_commit``
  pins the commit to a full object id; ``materialize`` writes that commit's
  tree into the fresh workspace from inside the sandbox, with the trusted Git
  executable, refusing path traversal, never following a symlink and bounding
  file count and size. ``.git``, ``.codex`` and ``.agents`` are never
  materialized (host metadata / project config Codex would execute), and the
  omission is recorded.
* **Command gates.** ``run_cmd_gate`` returns the same result shape as the host
  runner, plus ``sandboxed``/``source`` evidence. A setup failure is a refusal,
  never a host fallback.
* **Directories.** ``allocate`` / ``release`` / ``sweep`` own fresh
  ``~/.watchtower-sandbox/stages/<kind>-<random>`` directories and remove them
  with a bounded, no-follow remover.
"""
from __future__ import annotations

import json
import os
import secrets
import stat as _stat
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from . import sandbox_policy as policy

GATE_TIMEOUT_S = 900.0
# Total time one close/integration may spend in sandboxed command gates. The
# bridge client waits longer than this (sandbox_bridge.CLOSE_TIMEOUT_S).
GATES_BUDGET_S = 1500.0
MATERIALIZE_TIMEOUT_S = 300.0
GATE_OUTPUT_TAIL = 2000
# Limits for a materialized tree. A repository beyond them is refused rather
# than partially written: a gate on half a tree proves nothing.
MAX_FILES = 100_000
MAX_FILE_BYTES = 64 << 20
MAX_TREE_BYTES = 512 << 20
# `ulimit -f` blocks (512 B) for any one file the gate writes: 1 GiB.
GATE_FILE_BLOCKS = 2 << 20
# Cleanup bounds.
MAX_REMOVE_ENTRIES = 1_000_000
MAX_REMOVE_DEPTH = 256
REMOVE_DEADLINE_S = 120.0
STALE_AFTER_S = 24 * 3600
MAX_SWEEP = 32

OMITTED_ROOTS = (".git", ".codex", ".agents")
_MAX_ALTERNATES = 4


class StageSandboxError(RuntimeError):
    """A stage/gate transport cannot be set up safely; callers must refuse,
    never run the work with the daemon's authority instead."""


# --------------------------------------------------------------------------
# fresh directories


def stages_root() -> Path:
    """Where fresh stage/gate directories live (under the real account home:
    ``/tmp`` resolves to ``/dev/shm``, which the profile denies)."""
    override = os.environ.get("WATCHTOWER_SANDBOX_STAGE_ROOT", "").strip()
    return Path(override) if override else Path.home() / ".watchtower-sandbox" / "stages"


def _private_root() -> Path:
    root = stages_root()
    try:
        parent = policy.canonical_root(root.parent, "stage root parent", must_exist=False)
        root = parent / root.name
        os.makedirs(parent, mode=0o700, exist_ok=True)
        try:
            os.mkdir(root, 0o700)
        except FileExistsError:
            pass
        st = os.lstat(root)
    except (OSError, policy.SandboxPolicyError) as exc:
        raise StageSandboxError(f"cannot prepare the stage directory root: {exc}") from exc
    if not _stat.S_ISDIR(st.st_mode) or st.st_uid != os.getuid() or st.st_mode & 0o022:
        raise StageSandboxError(f"{root} is not a private directory owned by this account")
    return root


def allocate(kind: str) -> Dict[str, Path]:
    """A fresh ``{"dir", "workspace", "store"}`` triple. The workspace and
    store are siblings under one private directory (validate_roots refuses a
    workspace that would contain the store root)."""
    kind = "".join(c for c in str(kind) if c.isalnum())[:12] or "stage"
    root = _private_root()
    for _ in range(8):
        name = f"{kind}-{secrets.token_hex(6)}"
        path = root / name
        try:
            os.mkdir(path, 0o700)
        except FileExistsError:
            continue
        except OSError as exc:
            raise StageSandboxError(f"cannot create {path}: {exc}") from exc
        ws, store = path / "ws", path / "store"
        try:
            os.mkdir(ws, 0o700)
            os.mkdir(store, 0o700)
            policy.ensure_store_layout(store)
        except (OSError, policy.SandboxPolicyError) as exc:
            release(path)
            raise StageSandboxError(f"cannot lay out {path}: {exc}") from exc
        return {"dir": path, "workspace": ws, "store": store}
    raise StageSandboxError("cannot allocate a fresh stage directory")


def _remove_tree(path: Path, deadline: float) -> int:
    """Remove ``path`` without following any symlink, bounded in entries,
    depth and time. Returns the number of entries removed; raises when a bound
    is hit (the remainder is left for ``sweep``)."""
    removed = 0
    top = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        def empty(dir_fd: int, name: str, depth: int) -> None:
            nonlocal removed
            if depth > MAX_REMOVE_DEPTH:
                raise StageSandboxError("directory tree is too deep to remove")
            try:
                fd = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=dir_fd)
            except PermissionError:
                # Linux O_PATH can pin a mode-000 directory without opening
                # its contents. Chmod the pinned inode, never a path the gate
                # can swap for a symlink to another directory.
                pin = os.open(name, os.O_PATH | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=dir_fd)
                try:
                    os.chmod(f"/proc/self/fd/{pin}", 0o700)
                    fd = os.open(f"/proc/self/fd/{pin}", os.O_RDONLY | os.O_DIRECTORY)
                finally:
                    os.close(pin)
            try:
                try:
                    os.fchmod(fd, 0o700)
                except OSError:
                    pass
                for entry in os.listdir(fd):
                    removed += 1
                    if removed > MAX_REMOVE_ENTRIES or time.monotonic() > deadline:
                        raise StageSandboxError("directory tree is too large to remove in bounds")
                    try:
                        st = os.stat(entry, dir_fd=fd, follow_symlinks=False)
                    except FileNotFoundError:
                        continue
                    if _stat.S_ISDIR(st.st_mode):
                        empty(fd, entry, depth + 1)
                        os.rmdir(entry, dir_fd=fd)
                    else:
                        os.unlink(entry, dir_fd=fd)
            finally:
                os.close(fd)
        empty(top, path.name, 0)
        os.rmdir(path.name, dir_fd=top)
    finally:
        os.close(top)
    return removed


def release(path: Any) -> bool:
    """Remove a directory made by ``allocate``. Only a direct child of the
    stage root, owned by this account and not a symlink, is ever removed."""
    target = Path(os.path.abspath(os.fspath(path)))
    root = Path(os.path.abspath(os.fspath(stages_root())))
    if target.parent != root or not target.name or target.name.startswith("."):
        return False
    try:
        st = os.lstat(target)
    except FileNotFoundError:
        return True
    except OSError:
        return False
    if not _stat.S_ISDIR(st.st_mode) or st.st_uid != os.getuid():
        return False
    try:
        _remove_tree(target, time.monotonic() + REMOVE_DEADLINE_S)
    except (OSError, StageSandboxError):
        return False
    return True


def sweep(older_than: float = STALE_AFTER_S, limit: int = MAX_SWEEP) -> int:
    """Remove stale directories left by a crashed run. Bounded: at most
    ``limit`` directories per call, each older than ``older_than`` seconds."""
    try:
        root = _private_root()
        names = sorted(os.listdir(root))
    except (OSError, StageSandboxError):
        return 0
    cleared, now = 0, time.time()
    for name in names:
        if cleared >= limit:
            break
        if name.startswith(".") or "-" not in name:
            continue
        path = root / name
        try:
            st = os.lstat(path)
        except OSError:
            continue
        if _stat.S_ISDIR(st.st_mode) and now - st.st_mtime > older_than and release(path):
            cleared += 1
    return cleared


# --------------------------------------------------------------------------
# trusted source


def _small(path: Path, limit: int = 8192) -> Optional[str]:
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        return None
    try:
        st = os.fstat(fd)
        if not _stat.S_ISREG(st.st_mode) or st.st_size > limit:
            return None
        return os.read(fd, limit).decode("utf-8", "replace")
    finally:
        os.close(fd)


def source_objects(repo_path: Any) -> Tuple[Dict[str, Any], List[Path]]:
    """``(metadata, object stores)`` of the trusted repository at ``repo_path``,
    from files only. The stores are the repository's own plus any alternates it
    names (one level at a time, a few deep); each must be acceptable as a read
    root. A repository with no Git directory is refused."""
    if not repo_path:
        raise StageSandboxError("the ticket has no repository path to take source from")
    repo = Path(os.path.expanduser(str(repo_path)))
    try:
        from . import close_proof
        why = close_proof.restricted_repo(str(repo))
    except Exception as exc:  # noqa: BLE001 - an unreadable ledger never grants source
        why = f"sandbox restriction ledger is unreadable: {exc}"
    if why:
        # The trusted snapshot must come from the host-owned integration
        # repository, never from a worker-writable workspace (its HEAD and
        # refs are the worker's to choose).
        raise StageSandboxError(f"source repository is a sandbox workspace: {why}")
    try:
        meta = policy.git_metadata(repo)
    except policy.SandboxPolicyError as exc:
        raise StageSandboxError(f"source repository is not usable: {exc}") from exc
    if meta["kind"] == "none" or not meta["objects"]:
        raise StageSandboxError(f"{repo} has no Git object store to take source from")
    stores: List[Path] = []
    queue_ = [Path(meta["objects"])]
    while queue_:
        current = queue_.pop(0)
        real = Path(os.path.realpath(current))
        if real in stores:
            continue
        problem = policy._read_root_problem(real, None)
        if problem:
            raise StageSandboxError(f"object store {current} {problem}")
        stores.append(real)
        if len(stores) > _MAX_ALTERNATES:
            raise StageSandboxError("source repository has too many alternate object stores")
        text = _small(current / "info" / "alternates")
        for line in (text or "").splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                alt = line if os.path.isabs(line) else os.path.join(str(current), line)
                queue_.append(Path(os.path.normpath(alt)))
    return meta, stores


def _head_from_files(meta: Dict[str, Any]) -> str:
    """HEAD's full commit id, read from the repository's files (no Git
    process, so no repository config can run)."""
    gitdir, common = Path(meta["gitdir"]), Path(meta["common"])
    head = (_small(gitdir / "HEAD") or "").strip()
    if policy.FULL_SHA.fullmatch(head):
        return head
    if not head.startswith("ref:"):
        raise StageSandboxError("repository HEAD is not readable")
    ref = head.split(":", 1)[1].strip()
    parts = ref.split("/")
    if not ref.startswith("refs/") or any(p in ("", ".", "..") for p in parts) or "\0" in ref:
        raise StageSandboxError("repository HEAD names an unusable ref")
    for base in (gitdir, common):
        value = (_small(base / ref) or "").strip()
        if policy.FULL_SHA.fullmatch(value):
            return value
    for line in (_small(common / "packed-refs", 8 << 20) or "").splitlines():
        sha, _, name = line.partition(" ")
        if name.strip() == ref and policy.FULL_SHA.fullmatch(sha):
            return sha
    raise StageSandboxError(f"cannot resolve {ref} from the repository's files")


def pin_source(item: Dict[str, Any], commit: str = "") -> Dict[str, Any]:
    """Pin ONE trusted snapshot for every gate of an evaluation: the
    repository identity (its common Git directory), the full commit every gate
    will materialize, and -- when ``commit`` was empty -- the HEAD it was taken
    from. ``source_unchanged`` re-checks it later, at the write lock."""
    repo_path = str(item.get("repo_path") or "")
    meta, _stores = source_objects(repo_path)
    full = resolve_commit(meta, commit)
    return {"repo_path": repo_path, "common": os.path.realpath(str(meta["common"])),
            "identity": _source_identity(meta),
            "commit": full, "head": "" if str(commit or "").strip() else full}


def _source_identity(meta: Dict[str, Any]) -> Dict[str, Any]:
    # Paths alone cannot detect a repository replaced at the same location.
    # Ignore mtimes: ordinary commits and object packing must remain valid.
    result = {}
    for key in ("gitdir", "common", "objects"):
        path = os.path.realpath(str(meta[key]))
        st = os.stat(path, follow_symlinks=False)
        result[key] = [path, st.st_dev, st.st_ino]
    return result


def source_unchanged(pin: Dict[str, Any]) -> str:
    """Why ``pin`` no longer describes the trusted repository ("" when it
    still does). Reads Git metadata files only, no ledger and no lock: it is
    called inside the queue write lock."""
    try:
        meta = policy.git_metadata(Path(os.path.expanduser(str(pin.get("repo_path") or ""))))
        if meta["kind"] == "none":
            return "the trusted source repository is gone"
        if os.path.realpath(str(meta["common"])) != pin.get("common"):
            return "the trusted source repository changed identity"
        if pin.get("identity") and _source_identity(meta) != pin["identity"]:
            return "the trusted source repository was replaced"
        if pin.get("head") and _head_from_files(meta) != pin["head"]:
            return "the trusted source HEAD moved while the gates ran"
    except (StageSandboxError, policy.SandboxPolicyError, OSError) as exc:
        return f"the trusted source repository is unreadable: {exc}"
    return ""


def resolve_commit(meta: Dict[str, Any], commit: str) -> str:
    """The full object id the stage will be pinned to: ``commit`` when it is
    one, the repository's HEAD when it is empty, otherwise a refusal (a short
    id or a name could mean different things to different objects stores)."""
    commit = str(commit or "").strip().lower()
    if not commit:
        return _head_from_files(meta)
    if not policy.FULL_SHA.fullmatch(commit):
        raise StageSandboxError("the stage commit must be a full commit id")
    return commit


# --------------------------------------------------------------------------
# materialization (runs inside the sandbox)

_MATERIALIZE = r"""
import json, os, stat, subprocess, sys
git, gdir, ws, commit, alts = sys.argv[1:6]
files_cap, file_cap, tree_cap, omit = int(sys.argv[6]), int(sys.argv[7]), int(sys.argv[8]), sys.argv[9].split(",")
base = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
base.update({"GIT_NO_REPLACE_OBJECTS": "1", "GIT_NO_LAZY_FETCH": "1", "GIT_GRAFT_FILE": "/dev/null",
             "GIT_OPTIONAL_LOCKS": "0", "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1",
             "GIT_TERMINAL_PROMPT": "0"})
G = [git, "-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false", "-c", "protocol.allow=never",
     "-c", "core.commitGraph=false", "-c", "gc.auto=0", "-c", "core.alternateRefsCommand=",
     "--git-dir=" + gdir]
def fail(msg):
    sys.stderr.write(msg + "\n"); sys.exit(3)
def run(*a):
    return subprocess.run(G + list(a), env=base, capture_output=True)
if run("init", "-q", "--bare", gdir).returncode != 0 and not os.path.isdir(gdir):
    fail("cannot create the object lookup directory")
with open(os.path.join(gdir, "objects", "info", "alternates"), "w") as f:
    f.write(alts)
p = run("rev-parse", "--verify", "-q", commit + "^{commit}")
if p.returncode != 0 or p.stdout.decode().strip() != commit:
    fail("commit " + commit + " is not in the source repository")
tree = run("ls-tree", "-r", "-z", "--full-tree", commit)
if tree.returncode != 0:
    fail("cannot list the commit tree")
entries = [e for e in tree.stdout.split(b"\0") if e]
if len(entries) > files_cap:
    fail("the tree has %d entries, more than the %d limit" % (len(entries), files_cap))
cat = subprocess.Popen(G + ["cat-file", "--batch"], env=base, stdin=subprocess.PIPE,
                       stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
omitted, written, total, links, gitlinks = [], 0, 0, 0, 0
def read_blob(sha):
    cat.stdin.write(sha.encode() + b"\n"); cat.stdin.flush()
    head = cat.stdout.readline().split()
    if len(head) != 3 or head[1] != b"blob":
        fail("object " + sha + " is not a readable blob")
    size = int(head[2])
    if size > file_cap:
        fail("object " + sha + " is larger than the per-file limit")
    data = cat.stdout.read(size); cat.stdout.read(1)
    if len(data) != size:
        fail("short read for " + sha)
    return data
def open_dir(parts):
    fd = os.open(ws, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    for part in parts:
        try:
            os.mkdir(part, 0o755, dir_fd=fd)
        except FileExistsError:
            pass
        nxt = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
        os.close(fd); fd = nxt
    return fd
for raw in entries:
    meta, _, path = raw.partition(b"\t")
    try:
        mode, kind, sha = meta.decode().split()
        text = path.decode("utf-8")
    except Exception:
        fail("unusable tree entry")
    parts = text.split("/")
    if any(x in ("", ".", "..") or "\0" in x for x in parts):
        fail("tree path is not a safe relative path: " + repr(text))
    if parts[0].lower() in omit or any(x.lower() == ".git" for x in parts):
        omitted.append(text); continue
    if kind == "commit":
        gitlinks += 1; omitted.append(text); continue
    if kind != "blob" or mode not in ("100644", "100755", "120000"):
        fail("unsupported tree entry mode " + mode + " at " + repr(text))
    data = read_blob(sha)
    total += len(data)
    if total > tree_cap:
        fail("the tree exceeds the %d byte limit" % tree_cap)
    dfd = open_dir(parts[:-1])
    try:
        if mode == "120000":
            if len(data) > 4096 or b"\0" in data:
                fail("unusable symlink target at " + repr(text))
            os.symlink(data.decode("utf-8", "surrogateescape"), parts[-1], dir_fd=dfd)
            links += 1
        else:
            fd = os.open(parts[-1], os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                         0o755 if mode == "100755" else 0o644, dir_fd=dfd)
            with os.fdopen(fd, "wb") as out:
                out.write(data)
                os.fchmod(out.fileno(), 0o755 if mode == "100755" else 0o644)
            written += 1
    finally:
        os.close(dfd)
cat.stdin.close(); cat.wait()
print(json.dumps({"commit": commit, "files": written, "symlinks": links, "gitlinks": gitlinks,
                  "bytes": total, "omitted_count": len(omitted), "omitted": omitted[:20]}))
"""


def _read_roots(store: Path, extra: List[Path]) -> List[Path]:
    roots = list(policy.runtime_read_roots(None, store))
    for path in extra:
        if path not in roots:
            roots.append(path)
    return roots


def materialize(workspace: Path, store: Path, stores: List[Path], commit: str) -> Dict[str, Any]:
    """Write ``commit``'s tree into the fresh ``workspace`` inside the sandbox.
    Returns the manifest the in-sandbox script printed."""
    git = policy.trusted_git(workspace, store)
    gdir = store / "source.git"
    try:
        proc = policy.run_sandboxed(
            [sys.executable, "-I", "-c", _MATERIALIZE, git, str(gdir), str(workspace), commit,
             "\n".join(str(s) for s in stores) + "\n", str(MAX_FILES), str(MAX_FILE_BYTES),
             str(MAX_TREE_BYTES), ",".join(OMITTED_ROOTS)],
            workspace, store, mode="rw", timeout=MATERIALIZE_TIMEOUT_S,
            read_roots=_read_roots(store, stores), max_output=1 << 20)
    except policy.SandboxPolicyError as exc:
        raise StageSandboxError(f"sandbox source materialization failed: {exc}") from exc
    lines = [ln for ln in proc.stdout.splitlines() if ln.strip()]
    if proc.returncode != 0 or not lines:
        raise StageSandboxError("sandbox source materialization failed: "
                                + (proc.stderr or proc.stdout)[-400:].strip())
    try:
        manifest = json.loads(lines[-1])
    except ValueError as exc:
        raise StageSandboxError("sandbox source materialization produced no manifest") from exc
    if not isinstance(manifest, dict) or manifest.get("commit") != commit:
        raise StageSandboxError("sandbox source materialization answered for another commit")
    return manifest


def prepared_source(repo_path: Any, commit: str, kind: str) -> Dict[str, Any]:
    """Allocate fresh roots, prove the kernel profile, and materialize the
    pinned commit. The caller owns ``release(result["dir"])``."""
    meta, stores = source_objects(repo_path)
    full = resolve_commit(meta, commit)
    sweep()
    roots = allocate(kind)
    try:
        try:
            policy.validate_roots(roots["workspace"], roots["store"])
            policy.preflight(roots["workspace"], roots["store"])
        except policy.SandboxPolicyError as exc:
            raise StageSandboxError(f"sandbox is not enforcing the profile: {exc}") from exc
        manifest = materialize(roots["workspace"], roots["store"], stores, full)
    except BaseException:
        release(roots["dir"])
        raise
    return {**roots, "commit": full, "manifest": manifest, "stores": stores}


# --------------------------------------------------------------------------
# command gates

_GATE_SCRIPT = r"""
cd "$WT_GATE_ROOT" || exit 126
ulimit -f "$WT_GATE_BLOCKS" 2>/dev/null
/bin/sh -c "$WT_GATE_CMD" >"$WT_GATE_OUT" 2>&1 </dev/null
rc=$?
tail -c "$WT_GATE_TAIL" "$WT_GATE_OUT"
exit $rc
"""


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _setup_failed(command: str, commit: str, why: str, started: float) -> Dict[str, Any]:
    return {"gate": "cmd:" + command, "passed": False, "exit_code": -1,
            "output_tail": f"gate setup failed: {why}"[-GATE_OUTPUT_TAIL:],
            "commit": commit or "", "at": _now_iso(),
            "seconds": round(time.time() - started, 1), "setup_failed": True,
            "sandboxed": True}


def run_cmd_gate(item: Dict[str, Any], command: str, commit: str = "",
                 timeout: Optional[float] = None) -> Dict[str, Any]:
    """Run one ``cmd:`` gate for a sandbox-origin ticket under the restricted
    profile. Source is ``commit`` (a full id; empty means the trusted
    repository's HEAD) materialized from the trusted repository's objects.
    Never raises: every setup problem is a ``setup_failed`` result, and the
    command is never run outside the sandbox. ``timeout`` (seconds) can only
    lower the per-gate limit."""
    started = time.time()
    limit = GATE_TIMEOUT_S if timeout is None else max(1.0, min(GATE_TIMEOUT_S, float(timeout)))
    ref = str(item.get("ref") or "")
    try:
        src = prepared_source(item.get("repo_path"), commit, "gate")
    except (StageSandboxError, policy.SandboxPolicyError, OSError) as exc:
        return _setup_failed(command, commit, str(exc), started)
    try:
        out_file = src["store"] / "tmp" / "gate.out"
        env = {"WT_GATE_ROOT": str(src["workspace"]), "WT_GATE_CMD": command,
               "WT_GATE_OUT": str(out_file), "WT_GATE_TAIL": str(GATE_OUTPUT_TAIL),
               "WT_GATE_BLOCKS": str(GATE_FILE_BLOCKS), "WT_GATE_COMMIT": src["commit"],
               "WT_TICKET_REF": ref, "WT_GATE_REPO": str(src["workspace"]),
               "WT_SANDBOX_GATE": "1", "CI": "1"}
        try:
            proc = policy.run_sandboxed(["/bin/sh", "-c", _GATE_SCRIPT], src["workspace"],
                                        src["store"], mode="rw", env=env,
                                        timeout=limit,
                                        read_roots=_read_roots(src["store"], src["stores"]),
                                        max_output=1 << 20, truncate=True)
            ok, code = proc.returncode == 0, proc.returncode
            tail = (proc.stdout or "") + (proc.stderr or "")
        except policy.SandboxPolicyError as exc:
            text = str(exc)
            if "timed out" in text:
                ok, code, tail = False, -1, f"timed out after {int(limit)}s"
            else:
                return _setup_failed(command, commit, text, started)
        result = {"gate": "cmd:" + command, "passed": ok, "exit_code": code,
                  "output_tail": tail[-GATE_OUTPUT_TAIL:], "commit": commit or "",
                  "at": _now_iso(), "seconds": round(time.time() - started, 1),
                  "sandboxed": True,
                  "source": {k: src["manifest"].get(k) for k in
                             ("commit", "files", "symlinks", "gitlinks", "bytes",
                              "omitted_count", "omitted")}}
        return result
    finally:
        release(src["dir"])


# --------------------------------------------------------------------------
# group integration: the commit that contains every member's commit

_CONTAINING = r"""
import json, os, re, subprocess, sys
git, gdir, alts, head, wanted = sys.argv[1:6]
wanted = json.loads(wanted)
base = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
base.update({"GIT_NO_REPLACE_OBJECTS": "1", "GIT_NO_LAZY_FETCH": "1", "GIT_GRAFT_FILE": "/dev/null",
             "GIT_OPTIONAL_LOCKS": "0", "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_CONFIG_NOSYSTEM": "1",
             "GIT_TERMINAL_PROMPT": "0"})
G = [git, "-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false", "-c", "protocol.allow=never",
     "-c", "core.commitGraph=false", "-c", "gc.auto=0", "-c", "core.alternateRefsCommand=",
     "--git-dir=" + gdir]
def run(*a):
    return subprocess.run(G + list(a), env=base, capture_output=True, text=True)
def out(sha, source, problem):
    print(json.dumps({"sha": sha, "source": source, "problem": problem})); sys.exit(0)
if run("init", "-q", "--bare", gdir).returncode != 0 and not os.path.isdir(gdir):
    out("", "", "no_repo: cannot create the object lookup directory")
with open(os.path.join(gdir, "objects", "info", "alternates"), "w") as f:
    f.write(alts)
def resolve(rev):
    p = run("rev-parse", "--verify", "--quiet", rev + "^{commit}")
    return p.stdout.strip() if p.returncode == 0 else ""
full = {}
for ref, sha in wanted.items():
    if not sha:
        continue
    if not re.fullmatch(r"[0-9a-f]{4,64}", sha):
        out("", "", "commit_missing: %s %s" % (ref, sha))
    got = resolve(sha)
    if not got:
        out("", "", "commit_missing: %s %s" % (ref, sha))
    full[ref] = got
headsha = resolve(head) if head else ""
if not full:
    out(headsha, "head", "") if headsha else out("", "", "no_repo: HEAD does not resolve")
dated = []
for ref, sha in full.items():
    p = run("show", "-s", "--format=%ct", sha)
    t = p.stdout.strip()
    dated.append((int(t) if p.returncode == 0 and t.isdigit() else 0, ref, sha))
cands = [(sha, "member:" + ref) for _, ref, sha in sorted(dated, key=lambda x: -x[0])]
if headsha:
    cands.append((headsha, "head"))
seen = set()
for cand, source in cands:
    if cand in seen:
        continue
    seen.add(cand)
    if all(run("merge-base", "--is-ancestor", c, cand).returncode == 0 for c in full.values()):
        out(cand, source, "")
out("", "", "diverged")
"""


def containing_sha(repo_path: Any, commits: Dict[str, str]) -> Tuple[str, str, str]:
    """``(sha, source, problem)`` exactly as ``integration_git.containing_sha``
    answers, but computed by the trusted Git inside the sandbox against the
    trusted repository's objects: no host Git touches the repository. The
    upstream candidate is not considered (it needs repository config). A
    sandbox workspace, an unusable repository or any sandbox failure is a
    ``sandbox_restricted``/``no_repo`` problem, never a host fallback."""
    try:
        meta, stores = source_objects(repo_path)
        head = _head_from_files(meta)
    except StageSandboxError as exc:
        return "", "", f"sandbox_restricted: {exc}"
    sweep()
    try:
        roots = allocate("group")
    except StageSandboxError as exc:
        return "", "", f"sandbox_restricted: {exc}"
    try:
        try:
            policy.validate_roots(roots["workspace"], roots["store"])
            policy.preflight(roots["workspace"], roots["store"])
            git = policy.trusted_git(roots["workspace"], roots["store"])
            proc = policy.run_sandboxed(
                [sys.executable, "-I", "-c", _CONTAINING, git, str(roots["store"] / "source.git"),
                 "\n".join(str(s) for s in stores) + "\n", head,
                 json.dumps({str(k): str(v or "") for k, v in commits.items()})],
                roots["workspace"], roots["store"], mode="rw", timeout=MATERIALIZE_TIMEOUT_S,
                read_roots=_read_roots(roots["store"], stores), max_output=1 << 16)
        except policy.SandboxPolicyError as exc:
            return "", "", f"sandbox_restricted: {exc}"
        lines = [ln for ln in proc.stdout.splitlines() if ln.strip()]
        try:
            answer = json.loads(lines[-1])
            return str(answer["sha"]), str(answer["source"]), str(answer["problem"])
        except (ValueError, IndexError, KeyError, TypeError):
            return "", "", ("sandbox_restricted: containing-commit lookup produced no answer: "
                            + (proc.stderr or proc.stdout)[-300:].strip())
    finally:
        release(roots["dir"])

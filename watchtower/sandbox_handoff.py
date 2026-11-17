"""Trusted, data-only handoff of sandbox commits to an integration worker.

The broker exports bytes in a read-only sandbox and parks the original ticket.
Only an operator outside that sandbox can adopt those bytes in a trusted
checkout. Adoption never runs the proposed code, pushes, or closes the ticket.
"""
from __future__ import annotations

import hashlib
from itertools import islice
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import unicodedata

from . import sandbox_bridge as bridge, sandbox_policy as policy, storage

MAX_PATCH = 8 << 20
MAX_HOOK = 1 << 16
HOOK_MARKER = "wt-sandbox-aware-prepush"
TRAILER = "WT-Sandbox-Adopted"
ORIGIN_MARKER = "watchtower-sandbox-origin"

# F3: bytes from a sandbox builder must never reach a privileged reader or
# runner. Anything an agent, editor, hook runner, CI service or shell loads
# automatically is refused by name, at any depth, case-insensitively. A
# refused change is not lost: the integration worker can read it in the
# exported patch and, if it is wanted, make that edit by hand as reviewed
# integration work of their own (see docs/sandbox-adoption-safety.md).
PROTECTED_DIRS = frozenset({
    ".git", ".codex", ".agents", ".claude", ".cursor", ".windsurf", ".gemini",
    ".continue", ".cline", ".kiro", ".roo", ".aider", ".mcp", ".watchtower",
    ".github", ".gitlab", ".circleci", ".buildkite", ".azure-pipelines",
    ".githooks", ".husky", ".vscode", ".idea", ".devcontainer", ".direnv",
})
PROTECTED_NAMES = frozenset({
    "agents.md", "claude.md", "gemini.md", "codex.md", ".mcp.json", ".cursorrules",
    ".windsurfrules", ".aider.conf.yml", ".envrc", ".pre-commit-config.yaml",
    "lefthook.yml", ".lefthook.yml", "lefthook.yaml", ".gitlab-ci.yml", ".travis.yml",
    "jenkinsfile", "azure-pipelines.yml", "bitbucket-pipelines.yml", "appveyor.yml",
    ".gitattributes", ".gitmodules", ".npmrc", ".yarnrc", ".yarnrc.yml", ".pypirc",
    "sitecustomize.py", "usercustomize.py", "conftest.py", "pytest.ini", "tox.ini",
    "setup.cfg", "noxfile.py",
})
# The hook chain and the adoption/broker machinery itself.
PROTECTED_PATHS = frozenset({
    "scripts/pre-push.sh", "scripts/install-hooks.sh",
    "scripts/adopt_sandbox_patch.py", "scripts/sandbox_test_commit.py",
})
PROTECTED_PREFIXES = ("watchtower/sandbox_",)
_MODES = re.compile(rb"^(?:new file mode|new mode|old mode|deleted file mode) (120000|160000)\r?$",
                    re.MULTILINE)


def _directory():
    path = bridge.authority_dir() / "handoffs"
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    return path


def _name(worker, commit):
    if not bridge.NAME.fullmatch(worker) or not policy.FULL_SHA.fullmatch(commit):
        raise ValueError("handoff requires a worker id and full commit id")
    return worker + "-" + commit


def export(scope, resolution):
    """Export once using trusted roots/base, preserving the original claim."""
    rec = policy.launch_record(scope["worker"])
    if not rec or any(rec.get(k) != scope[k] for k in ("repo", "store")):
        raise ValueError("handoff launch roots do not match the claim")
    commit = resolution["commit"]
    name = _name(scope["worker"], commit)
    raw = policy.export_patch(rec["repo"], rec["store"], commit, rec["base"], MAX_PATCH)
    if not raw or len(raw) > MAX_PATCH:
        raise ValueError("handoff patch is empty or exceeds the size limit")
    record = {"worker": scope["worker"], "ref": scope["claim"]["ref"],
              "queue": scope["queue"], "source": scope["source"],
              "store_id": scope["store_id"], "claim": scope["claim"],
              "commit": commit, "base": rec["base"], "resolution": resolution,
              "sha256": hashlib.sha256(raw).hexdigest()}
    directory = _directory()
    with bridge.directory(directory) as fd:
        try:
            old = bridge._read(fd, name + ".json")
        except FileNotFoundError:
            patch_fd = os.open(name + ".patch", os.O_WRONLY | os.O_CREAT | os.O_EXCL
                               | os.O_NOFOLLOW, 0o600, dir_fd=fd)
            with os.fdopen(patch_fd, "wb") as out:
                out.write(raw)
                out.flush()
                os.fsync(out.fileno())
            bridge._write(fd, name + ".json", record)
        else:
            if old != record:
                raise ValueError("handoff already exists for a different claim or patch")
    return str(directory / (name + ".json"))


def _fold(part):
    """Fold one path component the way a case-insensitive, normalizing or
    NTFS-style filesystem would, so ``.GIT``, ``.cl\u200baude`` and ``.git.``
    cannot hide a protected name."""
    part = unicodedata.normalize("NFKC", part)
    part = "".join(c for c in part if unicodedata.category(c) != "Cf")
    return part.casefold().rstrip(" .")


def protected_reason(path):
    """Why ``path`` may not be adopted from a sandbox patch ('' when allowed)."""
    parts = [_fold(p) for p in str(path).replace("\\", "/").split("/") if p not in ("", ".")]
    if not parts:
        return "empty path"
    if any(p in PROTECTED_DIRS for p in parts[:-1]) or parts[-1] in PROTECTED_DIRS:
        return "agent, tool, hook or CI configuration directory"
    if parts[-1] in PROTECTED_NAMES or parts[-1].endswith(".pth"):
        return "agent instruction, tool configuration or automatically loaded file"
    joined = "/".join(parts)
    if joined in PROTECTED_PATHS:
        return "push hook chain or adoption machinery"
    if joined.startswith(PROTECTED_PREFIXES):
        return "sandbox broker code"
    return ""


def _patch_paths(numstat):
    """Every path a ``git apply --numstat -z`` listing names (renames give two)."""
    tokens, out, i = numstat.split("\0"), [], 0
    while i < len(tokens):
        parts = tokens[i].split("\t", 2)
        if len(parts) == 3 and parts[2] == "":      # rename/copy: old and new follow
            out.extend(t for t in tokens[i + 1:i + 3] if t)
            i += 3
            continue
        if len(parts) == 3:
            out.append(parts[2])
        elif tokens[i]:
            raise ValueError("handoff patch listing is not understood; refusing")
        i += 1
    return out


def check_patch_paths(raw, numstat):
    """Refuse a patch touching protected paths, symlinks or submodules."""
    bad = []
    for path in _patch_paths(numstat):
        why = protected_reason(path)
        if why:
            bad.append(f"{path!r} ({why})")
    if bad:
        raise ValueError(
            "handoff changes paths that must not be adopted from a sandbox: "
            + ", ".join(bad[:8]) + (" ..." if len(bad) > 8 else "")
            + ". Read the exported patch; if a change is wanted, make that edit by hand "
              "in your own reviewed commit (docs/sandbox-adoption-safety.md).")
    if _MODES.search(raw):
        raise ValueError("handoff creates or changes a symlink or submodule; refusing")


def _hook_file(path):
    """Content of an executable hook, '' when it is absent or inert."""
    try:
        info = os.lstat(path)
    except FileNotFoundError:
        return ""
    if not stat.S_ISREG(info.st_mode):
        raise ValueError(f"push hook {path} is not a regular file")
    if not info.st_mode & 0o111:
        return ""           # Git does not run a non-executable hook
    if info.st_size > MAX_HOOK:
        raise ValueError(f"push hook {path} is too large to verify")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        return os.read(fd, MAX_HOOK + 1).decode("utf-8", "replace")
    finally:
        os.close(fd)


def _git_query(destination, *args):
    """Read-only Git lookup with the user's real configuration (the one a
    later push would use). Never run for anything that executes project code."""
    git = shutil.which("git", path="/usr/bin:/bin:/usr/local/bin")
    if not git:
        raise ValueError("trusted system git is unavailable")
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    env["GIT_TERMINAL_PROMPT"] = "0"
    result = subprocess.run([git, "-C", str(destination), *args], env=env,
                            capture_output=True, timeout=30)
    if result.returncode:
        raise ValueError("cannot inspect the integration push hooks: "
                         + result.stderr[-300:].decode("utf-8", "replace"))
    return result.stdout.decode("utf-8", "surrogateescape").strip()


def push_hook_problem(destination):
    """Why a ``git push`` from ``destination`` could run adopted code on the
    host ('' when it cannot). Fails closed: an unreadable or unrecognized hook
    chain is a problem. Safe means either no executable pre-push hook, or one
    chain that ends in the committed ``scripts/pre-push.sh`` carrying
    ``HOOK_MARKER`` (it gates adopted commits in the sandbox, never on the
    host; adoption refuses to patch that script)."""
    destination = Path(destination)
    effective = Path(destination) / _git_query(destination, "rev-parse", "--git-path", "hooks/pre-push")
    common = Path(destination) / _git_query(destination, "rev-parse", "--git-common-dir")
    candidates = {effective, common / "hooks" / "pre-push"}
    script = ""
    try:
        script = _hook_file_text(destination / "scripts" / "pre-push.sh")
    except (OSError, ValueError):
        script = ""
    for hook in sorted(candidates, key=str):
        try:
            body = _hook_file(hook)
        except (OSError, ValueError) as exc:
            return str(exc)
        if not body:
            continue
        if HOOK_MARKER in body:
            continue
        if "scripts/pre-push.sh" in body and HOOK_MARKER in script:
            continue
        return (f"push hook {hook} is not sandbox-aware; a push would run adopted tests on "
                "the host. Update scripts/pre-push.sh to the sandbox-aware version first")
    return ""


def _hook_file_text(path):
    info = os.lstat(path)
    if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_HOOK:
        raise ValueError("unusable hook script")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        return os.read(fd, MAX_HOOK + 1).decode("utf-8", "replace")
    finally:
        os.close(fd)


def _git(destination, *args, input_bytes=None):
    git = shutil.which("git", path="/usr/bin:/bin:/usr/local/bin")
    if not git:
        raise ValueError("trusted system git is unavailable")
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    env.update(GIT_NO_LAZY_FETCH="1", GIT_NO_REPLACE_OBJECTS="1",
               GIT_TERMINAL_PROMPT="0", GIT_CONFIG_NOSYSTEM="1",
               GIT_CONFIG_GLOBAL="/dev/null", GIT_GRAFT_FILE="/dev/null")
    result = subprocess.run(
        [git, "-c", "protocol.allow=never", "-c", "core.hooksPath=/dev/null",
         "-c", "core.fsmonitor=false", "-c", "core.commitGraph=false",
         "-c", "commit.gpgsign=false", "-c", "core.protectNTFS=true",
         "-c", "core.protectHFS=true", "-C", str(destination), *args], env=env, input=input_bytes,
        capture_output=True, timeout=60)
    if result.returncode:
        raise ValueError("trusted integration git failed: " + result.stderr[-500:].decode("utf-8", "replace"))
    return result.stdout.decode("utf-8", "surrogateescape").strip()


def _check_destination_overlap(destination):
    """Refuse a destination overlapping any sandbox root in the trusted ledger.

    Bounded, no-follow reads: a hostile or corrupt scope never causes a
    traceback, an unbounded scan or a followed link, and anything that cannot
    be validated refuses the adoption with the entry named."""
    try:
        with bridge.directory(bridge.authority_dir()) as fd:
            with os.scandir(fd) as scan:
                looked_at = [e.name for e in islice(scan, bridge.LEDGER_SCAN + 1)]
            if len(looked_at) > bridge.LEDGER_SCAN:
                raise ValueError("sandbox scope ledger exceeds the scan limit; refusing adoption")
            for entry in sorted(n for n in looked_at if n.endswith(".json")):
                try:
                    scope = bridge._read(fd, entry)
                    roots = [Path(scope["repo"]), Path(scope["store"])]
                except (OSError, ValueError, KeyError, TypeError) as exc:
                    raise ValueError(f"cannot validate sandbox scope {entry!r}: {exc}") from exc
                for root in roots:
                    for left in bridge._forms(destination):
                        for right in bridge._forms(root):
                            if bridge._under(left, right) or bridge._under(right, left):
                                raise ValueError("integration destination overlaps a sandbox root")
    except FileNotFoundError:
        return      # no sandbox was ever registered on this host
    except OSError as exc:
        raise ValueError(f"cannot read the sandbox scope ledger: {exc}") from exc


def _mark_destination_origin(destination):
    """Sticky repository provenance, outside the adopted tree and history.

    Rewording/squashing a commit cannot remove the sandbox requirement in this
    destination or its linked worktrees. Write BEFORE applying untrusted bytes;
    interrupted adoption may retain a conservative marker, never miss one.
    """
    common = Path(_git(destination, "rev-parse", "--git-common-dir"))
    if not common.is_absolute():
        common = destination / common
    with bridge.directory(common.resolve(strict=True)) as fd:
        try:
            out = os.open(ORIGIN_MARKER, os.O_WRONLY | os.O_CREAT | os.O_EXCL
                          | os.O_NOFOLLOW, 0o400, dir_fd=fd)
        except FileExistsError:
            return  # Any existing object is a restriction, never permission.
        with os.fdopen(out, "w") as stream:
            stream.write("Sandbox-origin bytes adopted here; keep pushes sandboxed.\n")
            stream.flush()
            os.fsync(stream.fileno())


def adopt(worker, commit, destination):
    """Operator-only: apply the reviewed patch in a clean trusted checkout.

    The destination is supplied by the trusted caller, never the sandbox. The
    caller reviews this checkout before running tests, merging or pushing.
    """
    name = _name(worker, commit)
    directory = _directory()
    destination = bridge.canonical(destination)
    with bridge.directory(destination):
        pass
    # A handoff must never lead host Git back into ANY restricted workspace
    # or store, including old workers whose live registry row was pruned.
    _check_destination_overlap(destination)
    problem = push_hook_problem(destination)
    if problem:
        raise ValueError(problem)
    with storage.file_lock(directory / (name + ".lock")):
        with bridge.directory(directory) as fd:
            record = bridge._read(fd, name + ".json")
            from . import config, queue as q, store_identity
            if record["source"] != str(q._db_path().resolve()):
                raise ValueError("handoff belongs to a different source store")
            with q._FileLock(q._lock_path()) as source_lock:
                if source_lock._fh is None:
                    raise ValueError("handoff source lock unavailable")
                if store_identity.read_unlocked(q._resolve_store_path()) != record["store_id"]:
                    raise ValueError("handoff source identity changed")
                data = q._load_unlocked()
                item = next((x for x in data["items"] if x.get("ref") == record["ref"]), {})
                if config.product_gate(record["queue"]) and not (item.get("product_ack") or item.get("pre_ack")):
                    raise ValueError("product acknowledgement is required before adoption")
                fence = bridge._fence(item)
                before = record["claim"]["fence"]
                if (item.get("status") != q.PARKED_STATUS or not item.get("needs_input")
                        or (item.get("parked") or {}).get("worker_id") != worker
                        or (item.get("wait") or {}).get("on") != "worker"
                        or any(fence.get(k) != before.get(k) for k in
                               ("last_answer_gen", "_bridge_claim_count"))
                        or not any(h.get("event") == "progress" and
                                   name + ".json" in h.get("text", "") for h in item.get("history", []))):
                    raise ValueError("handoff no longer belongs to the current parked claim")
            patch_fd = os.open(name + ".patch", os.O_RDONLY | os.O_NOFOLLOW, dir_fd=fd)
            with os.fdopen(patch_fd, "rb") as source:
                raw = source.read(MAX_PATCH + 1)
            if len(raw) > MAX_PATCH or hashlib.sha256(raw).hexdigest() != record["sha256"]:
                raise ValueError("handoff patch integrity check failed")
            if record.get("adopted"):
                raise ValueError("handoff already adopted; inspect its recorded commit")
            if _git(destination, "status", "--porcelain", "--untracked-files=all"):
                raise ValueError("integration destination must be clean")
            if _git(destination, "rev-parse", "HEAD") != record["base"]:
                raise ValueError("integration checkout must start at the recorded base")
            # git apply rejects absolute/parent paths and symlink traversal.
            # --index also requires the working tree to match the trusted index.
            _git(destination, "apply", "--check", "--index", "--binary", "-", input_bytes=raw)
            names = _git(destination, "apply", "--numstat", "-z", "-", input_bytes=raw)
            check_patch_paths(raw, names)
            _mark_destination_origin(destination)
            _git(destination, "apply", "--index", "--binary", "-", input_bytes=raw)
            _git(destination, "-c", "user.name=WatchTower Integration", "-c",
                 "user.email=watchtower@localhost", "commit", "-m",
                 "Adopt sandbox fix for " + record["ref"], "-m",
                 f"{TRAILER}: {record['ref']} worker={worker} patch-sha256={record['sha256']}")
            sha = _git(destination, "rev-parse", "HEAD")
            record["adopted"] = {"repo": str(destination), "commit": sha}
            bridge._write(fd, name + ".json", record)
    return {"ref": record["ref"], "commit": sha, "repo": str(destination),
            "next": "Review this commit's bytes. Do NOT run its tests, scripts or hooks on the host: "
                    "test it only with `python3 scripts/sandbox_test_commit.py --repo DESTINATION "
                    "--commit COMMIT --command CMD` (restricted sandbox, no network). Push with the "
                    "repository's real hooks (never --no-verify); the sandbox-aware pre-push gate "
                    "tests adopted commits in the sandbox. Then resolve the original ticket with "
                    "that integrated commit and its required gates."}


def sandbox_test(repo, commit, command, timeout=None):
    """Run ``command`` against ``commit`` in the restricted sandbox (fresh
    materialization from the trusted repository's objects, no network, only
    fresh workspace/store writable). Never runs on the host and never falls
    back to it: every setup problem is a failed result."""
    from . import sandbox_stages
    if not policy.FULL_SHA.fullmatch(str(commit)):
        raise ValueError("sandbox test requires a full commit id")
    if not str(command).strip():
        raise ValueError("sandbox test requires a command")
    return sandbox_stages.run_cmd_gate(
        {"ref": "adopted-test", "repo_path": str(Path(repo).expanduser().absolute())},
        str(command), str(commit), timeout=timeout)

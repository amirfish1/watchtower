"""Trusted, data-only handoff of sandbox commits to an integration worker.

The broker exports bytes in a read-only sandbox and parks the original ticket.
Only an operator outside that sandbox can adopt those bytes in a trusted
checkout. Adoption never runs the proposed code, pushes, or closes the ticket.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

from . import sandbox_bridge as bridge, sandbox_policy as policy, storage

MAX_PATCH = 8 << 20


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
         "-c", "commit.gpgsign=false",
         "-C", str(destination), *args], env=env, input=input_bytes,
        capture_output=True, timeout=60)
    if result.returncode:
        raise ValueError("trusted integration git failed: " + result.stderr[-500:].decode("utf-8", "replace"))
    return result.stdout.decode("utf-8", "surrogateescape").strip()


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
    for path in bridge.authority_dir().glob("*.json"):
        scope = json.loads(path.read_text())
        for key in ("repo", "store"):
            root = Path(scope[key])
            if bridge._under(destination, root) or bridge._under(root, destination):
                raise ValueError("integration destination overlaps a sandbox root")
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
            for line in names.split("\0"):
                path = line.split("\t", 2)[-1]
                if any(p in (".git", ".codex", ".agents") for p in Path(path).parts):
                    raise ValueError("handoff changes protected project metadata")
            _git(destination, "apply", "--index", "--binary", "-", input_bytes=raw)
            _git(destination, "-c", "user.name=WatchTower Integration", "-c",
                 "user.email=watchtower@localhost", "commit", "-m",
                 "Adopt sandbox fix for " + record["ref"])
            sha = _git(destination, "rev-parse", "HEAD")
            record["adopted"] = {"repo": str(destination), "commit": sha}
            bridge._write(fd, name + ".json", record)
    return {"ref": record["ref"], "commit": sha, "repo": str(destination),
            "next": "Review and verify this commit, integrate and push it to the release branch, "
                    "then resolve the original ticket with that integrated commit and its required gates."}

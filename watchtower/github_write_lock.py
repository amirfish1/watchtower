"""Cross-host exclusion for the GitHub backend's read/write critical section.

A repository label's unique name arbitrates acquisition. Cleanup uses its
immutable node ID, never its reusable name (which could belong to a new owner).
No lease expires: GitHub issue writes cannot be fenced after a timed takeover.
"""
from __future__ import annotations

import contextlib
import hashlib
import json
import os
import re
import threading
import time
import uuid
from urllib.parse import quote

from . import github_backend as gb

LABEL = "watchtower:body-lock:v1"
WAIT_SECONDS = 30


_LOCAL = threading.local()


def child_options():
    """An orphan gh child must keep the local recovery fence until it exits."""
    fd = getattr(_LOCAL, "fd", None)
    return {"pass_fds": (fd,)} if fd is not None else {}


def _fence_path(nonce):
    from .github_pipeline import path
    return path().parent / "gh-write-fences" / nonce


def _json(proc):
    try:
        result = json.loads(proc.stdout or "{}")
    except (TypeError, ValueError) as exc:
        raise gb.GitHubBackendError("GitHub write lock received invalid JSON; refusing to write") from exc
    if proc.returncode or not isinstance(result, dict) or result.get("errors"):
        raise gb.GitHubBackendError(f"GitHub write lock API failed: {proc.stderr or proc.stdout}")
    return result


def _start(pid):
    from .workers import _pid_start_token
    token = _pid_start_token(pid, utc=True)
    return hashlib.sha256(token.encode()).hexdigest()[:16] if token else ""


def _description():
    from .github_pipeline import owner_marker
    token = _start(os.getpid())
    if not token:
        raise gb.GitHubBackendError("Cannot prove this process identity; refusing GitHub write lock")
    return f"{owner_marker()} {os.getpid()} {token} {uuid.uuid4().hex}"


def _read(backend):
    proc = backend._gh_api("GET", f"repos/{backend.repo}/labels/{quote(LABEL, safe='')}")
    if proc.returncode and "HTTP 404" in proc.stderr:
        return None
    label = _json(proc)
    if label.get("name") != LABEL or not label.get("node_id"):
        raise gb.GitHubBackendError("GitHub write lock label is malformed; refusing to guess its owner")
    return label


def _delete(backend, node_id):
    # The name may already have been reused. deleteLabel addresses ONLY the
    # captured object, including when two dead-owner recoverers race.
    result = _json(backend._gh_api("POST", "graphql", {
        "query": "mutation($id:ID!){deleteLabel(input:{id:$id}){clientMutationId}}",
        "variables": {"id": node_id},
    }))
    if not isinstance(result.get("data", {}).get("deleteLabel"), dict):
        raise gb.GitHubBackendError("GitHub write lock deletion was not confirmed; refusing to continue")


def _recover_here(backend, label):
    from .github_pipeline import owner_marker
    from .queue import fcntl
    try:
        host, raw_pid, started, nonce = str(label.get("description") or "").split()
        pid = int(raw_pid)
    except (TypeError, ValueError):
        return False
    if (host != owner_marker() or pid <= 0 or not re.fullmatch(r"[0-9a-f]{16}", started)
            or not re.fullmatch(r"[0-9a-f]{32}", nonce)):
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        dead = True
    except (PermissionError, OSError):
        return False
    else:
        current = _start(pid)
        dead = bool(current) and current != started
    # Parent death alone is insufficient: a gh child can still be writing.
    # It inherits this descriptor; only the owning host can prove it drained.
    try:
        fence = _fence_path(nonce).open("r+")
    except OSError:
        return False
    with fence:
        try:
            fcntl.flock(fence.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return False
        if not dead and fence.read() != "ended":
            return False
        try:
            _delete(backend, label["node_id"])
        except gb.GitHubBackendError:
            current = _read(backend)
            if current and current["node_id"] == label["node_id"]:
                raise
        # Keep the small fence file: a second recoverer may already have
        # opened it, and deleting/recreating its path would split the lock.
    return True


def _acquire(backend, description):
    deadline = time.monotonic() + WAIT_SECONDS
    while True:
        # Creation is atomic on GitHub. Unlike label create --force, an
        # existing name fails instead of replacing its owner's description.
        try:
            proc = backend._gh_api("POST", f"repos/{backend.repo}/labels", {
                "name": LABEL, "color": "ededed", "description": description,
            })
        except gb.GitHubBackendError as exc:
            # A lost response may have created our object. Adopt only this
            # acquisition's unique description; otherwise preserve the error.
            label = _read(backend)
            if label and label.get("description") == description:
                return label["node_id"]
            raise exc
        if proc.returncode == 0:
            label = _json(proc)
            if label.get("name") != LABEL or label.get("description") != description or not label.get("node_id"):
                raise gb.GitHubBackendError("GitHub write lock creation not confirmed; refusing to write")
            return label["node_id"]
        label = _read(backend)
        if label and label.get("description") == description:
            return label["node_id"]
        # Do not turn a permissions/quota/validation error into a retry or
        # recovery write. Only GitHub's explicit uniqueness conflict waits.
        try:
            errors = json.loads(proc.stdout or "{}").get("errors", [])
        except ValueError:
            errors = []
        if not any(isinstance(e, dict) and e.get("code") == "already_exists" for e in errors):
            raise gb.GitHubBackendError(f"Cannot acquire GitHub write lock: {proc.stderr or proc.stdout}")
        if label is None:
            if time.monotonic() < deadline:
                continue
            raise gb.GitHubBackendError("GitHub write lock changed during acquisition; retry")
        if _recover_here(backend, label):
            continue
        if time.monotonic() >= deadline:
            raise gb.GitHubBackendError(
                f"{backend.repo} write lock busy ({label.get('description')}); "
                "no issue was written. Retry, or run the write on the owning "
                "host after the recorded transaction or process ends. Never delete by name "
                "or steal a live/unproven lock.")
        time.sleep(min(1, max(0, deadline - time.monotonic())))


@contextlib.contextmanager
def hold(backend):
    """One issue transaction, with owning-host recovery of a crashed holder."""
    from .queue import fcntl
    if fcntl is None:
        raise gb.GitHubBackendError("GitHub write locking requires POSIX descriptor locks")
    description = _description()
    nonce = description.rsplit(" ", 1)[1]
    path = _fence_path(nonce)
    path.parent.mkdir(parents=True, exist_ok=True)
    previous_fd = getattr(_LOCAL, "fd", None)
    with path.open("x+") as fence:
        fcntl.flock(fence.fileno(), fcntl.LOCK_EX)
        _LOCAL.fd = fence.fileno()
        try:
            node_id = _acquire(backend, description)
            try:
                yield
            finally:
                _delete(backend, node_id)
        finally:
            try:
                # Written only AFTER the transaction and its synchronous
                # requests end, including when cleanup failed. A future
                # writer can retry cleanup without restarting a live daemon;
                # any inherited child must still release the flock first.
                fence.write("ended")
                fence.flush()
            finally:
                _LOCAL.fd = previous_fd
    # Only successful transactions remove their file. Lost responses and
    # process death leave the fence available for owning-host recovery.
    path.unlink()

"""Default-off, file-spooled Codex queue transport (WT-61).

The daemon creates authority outside the worker's writable roots. Nothing in
the spool can name a queue, identity, cwd, executable, environment or new
destination. Requests are data, never CLI argv. See docs/codex-sandbox.md.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time
import uuid

from . import storage

MAX_BYTES = 131072
MAX_RECEIPTS = 1024
NAME = re.compile(r"[a-zA-Z0-9_-]{1,80}\Z")
REQUEST_ID = re.compile(r"[a-f0-9]{32}\Z")


def authority_dir():
    return storage.data_dir() / "sandbox-bridge"


def restricted_session(session_id="", worker_id=""):
    """Only a live hosted turn may be steered; generic resumes lose policy."""
    from . import config, workers
    for worker in workers.list_workers(prune=False):
        if ((session_id and worker.get("session_id") == session_id)
                or (worker_id and worker.get("worker_id") == worker_id)):
            return config.codex_sandbox(worker.get("ticket_queue") or worker.get("queue", "")) == "workspace-write"
    return False


def worker_store(queue, worker):
    from . import config
    if not NAME.fullmatch(worker):
        raise ValueError("invalid sandbox worker id")
    return config.codex_sandbox_store(queue).expanduser().absolute() / worker


def _under(path, root):
    return path == root or root in path.parents


@contextmanager
def directory(path):
    """Open every component without following a worker-supplied symlink."""
    path = Path(path).absolute()
    fd = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts[1:]:
            new = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                          dir_fd=fd)
            os.close(fd)
            fd = new
        yield fd
    finally:
        os.close(fd)


def _read(fd, name):
    f = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
    try:
        info = os.fstat(f)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > MAX_BYTES:
            raise ValueError("bridge files must be bounded, private regular files")
        raw = os.read(f, MAX_BYTES + 1)
        if len(raw) > MAX_BYTES:
            raise ValueError("bridge file too large")
        return json.loads(raw)
    finally:
        os.close(f)


def _write(fd, name, value):
    raw = json.dumps(value, ensure_ascii=False).encode()
    if len(raw) > MAX_BYTES:
        raise ValueError("bridge response exceeds size limit")
    temporary = f".bridge-{uuid.uuid4().hex}"
    out = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                  0o600, dir_fd=fd)
    try:
        with os.fdopen(out, "wb") as f:
            f.write(raw)
            f.flush()
            os.fsync(f.fileno())
        os.rename(temporary, name, src_dir_fd=fd, dst_dir_fd=fd)
        os.fsync(fd)
    finally:
        try:
            os.unlink(temporary, dir_fd=fd)
        except FileNotFoundError:
            pass


def register(queue, worker, repo, assigned_ref=""):
    """Trusted launch only. A worker cannot register itself through the spool."""
    import sys
    from . import config, queue as q, store_identity, workers, messages
    if config.backend(queue) != "file":
        raise ValueError("sandbox bridge requires a local file-backed queue")
    store = worker_store(queue, worker)
    workspace = Path(repo).resolve(strict=True)
    # A writable checkout of the broker itself is not a security boundary.
    protected = [authority_dir(), q.store_path(), Path(__file__).resolve(),
                 config.CONFIG_FILE, workers.WORKERS_FILE, messages._outbox_file(),
                 Path(sys.executable), Path(sys.executable).resolve()]
    binary = workers._resolve_engine_bin("codex")
    if binary:
        protected.extend((Path(binary), Path(binary).resolve()))
    for root in (workspace, store):
        if any(_under(Path(p).resolve(), root.resolve()) for p in protected):
            raise ValueError("sandbox writable root contains trusted control files or broker code")
    if _under(store.resolve(), workspace) or _under(workspace, store.resolve()):
        raise ValueError("sandbox workspace and worker store must be separate roots")
    if assigned_ref and q._project_from_ident(assigned_ref) != queue:
        raise ValueError("single-ticket assignment must belong to the registered queue")
    store.mkdir(parents=True, exist_ok=True)
    authority_dir().mkdir(parents=True, exist_ok=True, mode=0o700)
    with directory(store):
        pass
    identity = store_identity.initialize()["store_id"]
    scope = {"worker": worker, "queue": queue, "repo": str(workspace),
             "store": str(store), "source": str(q._db_path().resolve()), "store_id": identity,
             "claim": None, "targets": [], "assigned_ref": assigned_ref}
    with directory(authority_dir()) as fd:
        # Never overwrite a receipt/claim ledger when retrying a launch.
        try:
            old = _read(fd, worker + ".json")
        except FileNotFoundError:
            _write(fd, worker + ".json", scope)
        else:
            if any(old.get(k) != scope[k] for k in ("worker", "queue", "repo", "store", "source", "store_id", "assigned_ref")):
                raise ValueError("sandbox worker id is already registered with another scope")
    return store


def _fence(item):
    # The claim history is monotonic even for same-second reclaims by the
    # same worker; answer generations fence later steering of that claim.
    claims = [e for e in item.get("history", []) if e.get("event") == "claim"]
    return {k: item.get(k) for k in
            ("claimed_by", "claimed_at", "claimed_session_id", "last_answer_gen")} | {
                "_bridge_claim_count": len(claims)}


def _current(scope, *, inspect=False):
    from . import queue as q
    claim = scope.get("claim") or {}
    item = q.get(claim.get("ref", "")) if claim else None
    states = ("in_progress", "in_review", "closed") if inspect else ("in_progress",)
    if (not item or item.get("status") not in states or item.get("needs_input")
            or item.get("claimed_by") != scope["worker"]
            or _fence(item) != claim.get("fence")):
        raise ValueError("sandbox claim is stale; re-check the real ticket")
    return item


def _fields(body, required, optional=()):
    if not isinstance(body, dict) or set(body) - set(required) - set(optional) or set(required) - set(body):
        raise ValueError("invalid or unknown bridge fields")


def _text(value):
    if not isinstance(value, str) or not value.strip() or len(value) > 32000:
        raise ValueError("expected nonempty bounded text")
    return value


def _message(scope, op, body, key):
    """Advisory messaging; transport acceptance never closes a ticket."""
    from . import messages, sender_envelope as envelope
    _fields(body, ("target", "text"))
    target, text = _text(body["target"]), _text(body["text"])
    if target not in scope.get("targets", []):
        raise ValueError("target is outside this claim's submitter scope")
    # Destination authority is frozen at claim time, not resolved from a
    # worker-editable agents.json or a later renamed agent.
    resolved = messages.resolve_target(target)
    if (resolved.get("session_id"), resolved.get("engine")) != tuple(scope["destination"]):
        raise ValueError("message destination changed since claim")
    if scope.get("message_count", 0) >= 10:
        raise ValueError("sandbox claim message budget exhausted")
    scope["message_count"] = scope.get("message_count", 0) + 1
    text = "[Untrusted sandbox worker report; review before acting.]\n" + text
    attr = envelope.make(target, message_id=key, basis="runtime",
                         evidence={"source": "sandbox_bridge", "untrusted": True},
                         who=envelope.author("session", scope["worker"], scope["worker"], "codex"))
    if op == "ask":
        return messages.ask(target, text, timeout_ms=30000, attr=attr)
    return messages.send(target, text, attr=attr, dedupe_key=key,
                         ticket_ref=scope["claim"]["ref"], queue_on_fail=False)


def _execute(scope, request, key):
    from . import queue as q, workers
    _fields(request, ("op", "body", "claim"))
    op, body = request["op"], request["body"]
    if op == "claim":
        _fields(body, (), ("ref",))
        if body.get("ref", "") != scope.get("assigned_ref", ""):
            raise ValueError("claim ref differs from the trusted launch assignment")
        if request["claim"] is not None:
            raise ValueError("claim request must not supply authority")
        if scope.get("claim"):
            previous = q.get(scope["claim"]["ref"])
            if (previous and previous.get("status") == "in_progress"
                    and previous.get("claimed_by") == scope["worker"]
                    and not previous.get("needs_input")):
                raise ValueError("finish the current claim before claiming again")
        # session identity comes from trusted host records, never request data.
        record = next((w for w in workers.list_workers(prune=False)
                       if w.get("worker_id") == scope["worker"]), {})
        if scope.get("assigned_ref"):
            item = q.claim_by_ref(scope["assigned_ref"], scope["worker"],
                                  session_uuid=str(record.get("session_id") or ""))
        else:
            item = q.claim_next(scope["worker"], project=scope["queue"],
                                session_uuid=str(record.get("session_id") or ""),
                                worker_model=workers.worker_model(scope["worker"], scope["queue"]))
        if not item or item.get("stop"):
            return {"ok": True, "item": item}
        scope["claim"] = {"ref": item["ref"], "fence": _fence(item)}
        scope["targets"] = []
        scope["message_count"] = 0
        submitter = item.get("submitter")
        if submitter:
            from . import messages
            try:
                resolved = messages.resolve_target(submitter)
                if resolved.get("session_id"):
                    scope["targets"] = [submitter]
                    scope["destination"] = [resolved["session_id"], resolved.get("engine")]
            except ValueError:
                pass
        return {"ok": True, "item": item, "claim": scope["claim"]}
    if request["claim"] != scope.get("claim") or not scope.get("claim"):
        raise ValueError("request does not match the assigned claim")
    item = _current(scope, inspect=op == "find")
    if op == "find":
        _fields(body, ("ref",))
        if body["ref"] != item["ref"]:
            raise ValueError("cannot inspect another ticket through this bridge")
        return {"ok": True, "item": item}
    if op in ("ask", "tell", "send"):
        return _message(scope, op, body, key)
    if op != "close":
        raise ValueError("operation is not allowed by the sandbox bridge")
    _fields(body, ("ref", "summary", "commit", "no_code"), ("caveats", "follow_ups", "unresolved"))
    if body["ref"] != item["ref"]:
        raise ValueError("cannot close another ticket through this bridge")
    _text(body["summary"])
    if type(body["no_code"]) is not bool or bool(body["commit"]) == body["no_code"]:
        raise ValueError("close requires exactly one commit or no-code proof")
    if not isinstance(body["commit"], str):
        raise ValueError("invalid commit proof")
    resolution = {"summary": body["summary"], "no_code": body["no_code"]}
    for field in ("caveats", "follow_ups", "unresolved"):
        values = body.get(field, [])
        if not isinstance(values, list):
            raise ValueError("resolution entries must be lists")
        for value in values:
            _text(value)
            if field != "caveats" and not re.search(r"\b[A-Z][A-Z0-9_-]*-\d+\b", value):
                raise ValueError("unfinished work must cite a ticket")
        resolution[field] = values
    if body["commit"]:
        # No arbitrary repo search, cwd, hooks, ssh or user-provided git argv.
        import subprocess
        if not re.fullmatch(r"[a-fA-F0-9]{7,64}", body["commit"]):
            raise ValueError("invalid commit SHA")
        # Even rev-parse can start a lazy partial-clone fetch, executing a
        # repository-selected uploadpack. Forbid every transport and hook;
        # a missing object is a failed proof, never permission to fetch it.
        result = subprocess.run(["git", "--no-optional-locks", "-c", "protocol.allow=never",
                                 "-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false",
                                 "-C", scope["repo"],
                                 "rev-parse", "--verify", body["commit"] + "^{commit}"],
                                env={"PATH": os.defpath, "HOME": "/nonexistent",
                                     "GIT_CONFIG_NOSYSTEM": "1", "GIT_NO_LAZY_FETCH": "1",
                                     "GIT_NO_REPLACE_OBJECTS": "1"},
                                capture_output=True, text=True, timeout=10)
        if result.returncode or not re.fullmatch(r"[a-f0-9]{40,64}\n?", result.stdout):
            raise ValueError("commit does not resolve in assigned workspace")
        resolution["commit"] = result.stdout.strip()
    # The regular close path still owns gates, product acknowledgement and
    # ownership checks. It refuses unsafe command gates under bridge mode.
    closed = q.close(item["ref"], scope["worker"], resolution=resolution,
                     session_uuid=str(item.get("claimed_session_id") or ""),
                     sandbox_expect=scope["claim"]["fence"])
    if not closed:
        raise ValueError("claim changed during close; nothing was closed")
    if closed.get("status") == "in_review" and closed.get("gate_pending") == "verify":
        from . import stages
        stages.request(closed["ref"], "verify")
    return {"ok": closed.get("status") in ("closed", "in_review"), "item": closed}


def process(worker):
    """Process bounded requests for one trusted registration, at most once.

    The receipt is reserved durably BEFORE effects. A daemon crash leaves an
    explicit indeterminate receipt and is never permission to replay a send.
    """
    from . import queue as q, config, store_identity, workers
    if not NAME.fullmatch(worker):
        raise ValueError("invalid sandbox worker id")
    with storage.file_lock(authority_dir() / (worker + ".lock")):
        with directory(authority_dir()) as trusted:
            scope = _read(trusted, worker + ".json")
            if scope["source"] != str(q._db_path().resolve()):
                raise ValueError("sandbox bridge source store changed")
            with q._FileLock(q._lock_path()) as lock:
                if lock._fh is None:
                    raise ValueError("sandbox source lock unavailable")
                if store_identity.read_unlocked(q._resolve_store_path()) != scope["store_id"]:
                    raise ValueError("sandbox bridge source identity changed")
            if config.codex_sandbox(scope["queue"]) != "workspace-write":
                raise ValueError("sandbox bridge policy is disabled")
            record = next((w for w in workers.list_workers(prune=False)
                           if w.get("worker_id") == worker), {})
            if not record.get("alive") or record.get("engine") != "codex" or record.get("queue") != scope["queue"]:
                raise ValueError("sandbox worker is no longer live in its registered queue")
            receipts = sum(1 for n in os.listdir(trusted)
                           if n.startswith(worker + "-") and n.endswith(".receipt")
                           and REQUEST_ID.fullmatch(n[len(worker) + 1:-len(".receipt")]))
            with directory(scope["store"]) as spool:
                names = sorted(n for n in os.listdir(spool) if n.endswith(".request")
                               and REQUEST_ID.fullmatch(n.removesuffix(".request")))[:32]
                for name in names:
                    key = name.removesuffix(".request")
                    if not REQUEST_ID.fullmatch(key):
                        continue
                    try:
                        request = _read(spool, name)
                        digest = hashlib.sha256(json.dumps(request, sort_keys=True).encode()).hexdigest()
                        receipt_name = worker + "-" + key + ".receipt"
                        try:
                            receipt = _read(trusted, receipt_name)
                        except FileNotFoundError:
                            if receipts >= MAX_RECEIPTS:
                                _write(spool, key + ".response", {"ok": False, "error": "bridge receipt budget exhausted; recycle worker"})
                                continue
                            receipt = {"digest": digest, "result": {
                                "ok": False, "error": "indeterminate: broker interrupted; inspect real ticket before retry"}}
                            _write(trusted, receipt_name, receipt)
                            receipts += 1
                            try:
                                result = _execute(scope, request, key)
                            except Exception as exc:
                                # Validation errors are deliberately public;
                                # unexpected runtime details stay in the daemon
                                # log rather than becoming a credential oracle.
                                if isinstance(exc, ValueError):
                                    error = str(exc)
                                else:
                                    q._log("SANDBOX_BRIDGE_ERROR", f"{worker} {key}: {type(exc).__name__}", queue=scope["queue"])
                                    error = "bridge operation failed: " + type(exc).__name__
                                result = {"ok": False, "error": error}
                            _write(trusted, worker + ".json", scope)
                            receipt["result"] = result
                            _write(trusted, receipt_name, receipt)
                        if receipt["digest"] != digest:
                            result = {"ok": False, "error": "request id already used with different content"}
                        else:
                            result = receipt["result"]
                        _write(spool, key + ".response", result)
                    except (OSError, ValueError):
                        # Invalid files never reach dispatch; do not copy OS
                        # errors or privileged file content into the spool.
                        pass
                    finally:
                        try:
                            os.unlink(name, dir_fd=spool)
                        except FileNotFoundError:
                            pass


def serve(stop):
    """One background broker per worker, so an ask cannot stall reconcile."""
    import threading
    running = {}
    from . import workers
    while not stop.wait(0.25):
        if not authority_dir().exists():
            continue
        live = {w["worker_id"] for w in workers.list_workers(prune=False) if w.get("alive") and w.get("engine") == "codex"}
        for path in authority_dir().glob("*.json"):
            worker = path.stem
            if worker not in live or not NAME.fullmatch(worker) or running.get(worker, None) and running[worker].is_alive():
                continue
            def run(worker=worker):
                try:
                    process(worker)
                except (OSError, ValueError):
                    pass
            thread = threading.Thread(target=run, daemon=True)
            running[worker] = thread
            thread.start()


def request(op, body, *, timeout=45):
    """Sandbox-side client. Only the request/response spool is writable."""
    spool = Path(os.environ["WT_SANDBOX_BRIDGE"])
    with directory(spool) as fd:
        try:
            claim = _read(fd, "claim.json")
        except FileNotFoundError:
            claim = None
        key = uuid.uuid4().hex
        _write(fd, key + ".request", {"op": op, "body": body,
                                      "claim": None if op == "claim" else claim})
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                result = _read(fd, key + ".response")
            except FileNotFoundError:
                time.sleep(0.05)
                continue
            if result.get("claim"):
                _write(fd, "claim.json", result["claim"])
            return result
    return {"ok": False, "error": f"bridge timeout; request {key} may still execute; inspect receipt before retry"}


def cli(args):
    """Return a CLI exit code, refusing all commands outside the contract."""
    import sys
    op = args.command
    if op == "claim":
        if args.worker and args.worker != os.environ.get("WT_WORKER_ID"):
            result = {"ok": False, "error": "cannot impersonate another worker"}
        elif args.queue != os.environ.get("WT_SANDBOX_QUEUE"):
            result = {"ok": False, "error": "claim is restricted to the assigned queue"}
        else:
            result = request(op, {"ref": args.ref} if getattr(args, "ref", None) else {})
    elif op in ("find", "close"):
        if getattr(args, "force", False) or getattr(args, "duplicate_of", "") or getattr(args, "enqueue_follow_ups", False):
            result = {"ok": False, "error": "sandbox cannot force, duplicate or enqueue a close"}
        elif getattr(args, "worker", "") and args.worker != os.environ.get("WT_WORKER_ID"):
            result = {"ok": False, "error": "cannot impersonate another worker"}
        else:
            body = {"ref": args.ref}
            if op == "close":
                body.update(summary=args.summary, commit=args.commit or "", no_code=args.no_code,
                            caveats=args.caveat or [], follow_ups=args.follow_up or [],
                            unresolved=args.unresolved or [])
            result = request(op, body)
    elif op in ("ask", "send", "tell"):
        if getattr(args, "notify_webhook", "") or getattr(args, "start_new_if_needed", False):
            result = {"ok": False, "error": "sandbox cannot use webhooks or launch sessions"}
        else:
            text = sys.stdin.read().removesuffix("\n") if args.text == "-" else args.text
            result = request(op, {"target": args.target, "text": text})
    else:
        result = {"ok": False, "error": f"{op} is outside the sandbox bridge contract"}
    if not result.get("ok") and not result.get("queued"):
        print("error: " + str(result.get("error") or "bridge operation failed"), file=sys.stderr)
        return 1
    item = result.get("item")
    if op in ("claim", "find"):
        if item is not None:
            if item.get("claimed_by") == os.environ.get("WT_WORKER_ID"):
                item["claimed_by_you"] = True
            if item.get("closed_by") == os.environ.get("WT_WORKER_ID"):
                item["closed_by_you"] = True
            for event in item.get("history", []):
                if (event.get("by") or {}).get("worker") == os.environ.get("WT_WORKER_ID"):
                    event["you"] = True
            print(json.dumps(item, indent=2))
    elif op == "close":
        print(f"{item['status'].upper()}: {item['ref']} — {item['resolution']['summary']}")
    elif getattr(args, "json", False):
        print(json.dumps(result, indent=2))
    else:
        print(result.get("answer") or "message accepted by transport; delivery receipt is authoritative")
    return 0

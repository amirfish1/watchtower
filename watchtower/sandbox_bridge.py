"""Default-off, file-spooled Codex queue transport (WT-61).

The daemon creates authority outside the worker's writable roots. Nothing in
the spool can name a queue, identity, cwd, executable, environment or new
destination. Requests are data, never CLI argv. See docs/codex-sandbox.md.

Recovery contract (all of it bounded per daemon tick):

* A crash between the real claim and the ledger write is repaired by fenced
  adoption: the queue itself must say the ticket is in progress and claimed by
  this registered worker in its registered queue.
* ``find`` / ``current`` are the recoverable claim read; they return the fresh
  fence so a worker that lost ``claim.json`` (or a response) is never stranded.
* Every result is projected under ``RESULT_MAX``. Truncation is explicit and
  every cut value stays reachable through ``find`` paging.
* One hostile spool entry (directory, FIFO, deep JSON, planted response) is
  retired on its own and cannot stop the entries after it.
* Dead workers' ledgers and receipts are retired after a retention window.
  Restricted-session tombstones are immutable and never retired.
"""
from __future__ import annotations

from contextlib import contextmanager
import errno
import hashlib
import inspect
from itertools import islice
import json
import os
from pathlib import Path
import re
import stat
import time
import uuid

from . import storage

MAX_BYTES = 131072          # any bridge file read, and every request
RESULT_MAX = 98304          # every result; leaves receipt headroom under MAX_BYTES
ITEM_MAX = RESULT_MAX - 8192
MAX_RECEIPTS = 1024
SPOOL_BATCH = 32            # requests served per worker per tick
SPOOL_SCAN = 4096           # spool entries looked at per tick
LEDGER_SCAN = 50000         # authority entries looked at per retirement pass
RETIRE_BUDGET = 64          # removals per sweep
RETIRE_EVERY_S = 30
RESPONSE_RETAIN_S = 3600
TEMP_RETAIN_S = 300
LEDGER_RETAIN_S = 86400
PAGE_CHARS = 16000
PAGE_ITEMS = 20
MESSAGE_BUDGET = 10
COMMENT_BUDGET = 50
BUDGET_REFS = 64
NAME = re.compile(r"[a-zA-Z0-9_-]{1,80}\Z")
REQUEST_ID = re.compile(r"[a-f0-9]{32}\Z")
_TOKEN = re.compile(r"[A-Za-z0-9_.:-]{1,64}\Z")
_JUNK = ".bridge-junk-"
_BOUND = set()


def authority_dir():
    return storage.data_dir() / "sandbox-bridge"


# --------------------------------------------------------------- tombstones
def _tomb_dir():
    return authority_dir() / "tombstones"


def _tomb_name(kind, value):
    return kind + "-" + hashlib.sha256(str(value).encode()).hexdigest()


def _tombstoned(kind, value):
    if not value:
        return False
    try:
        return stat.S_ISREG(os.lstat(_tomb_dir() / _tomb_name(kind, value)).st_mode)
    except (FileNotFoundError, NotADirectoryError):
        return False
    except OSError:
        return True     # an unreadable restriction ledger never grants promotion


def _tombstone(kind, value):
    """Write-once and never removed: restriction outlives pruning and config."""
    if not value or (str(_tomb_dir()), kind, value) in _BOUND:
        return
    _tomb_dir().mkdir(parents=True, exist_ok=True, mode=0o700)
    with directory(_tomb_dir()) as fd:
        try:
            os.close(os.open(_tomb_name(kind, value),
                             os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                             0o600, dir_fd=fd))
        except FileExistsError:
            pass
    _BOUND.add((str(_tomb_dir()), kind, value))


def bind_session(worker_id, session_id):
    """Trusted callers only: tie a session to an already restricted worker."""
    worker_id, session_id = str(worker_id or ""), str(session_id or "")
    if not session_id or not _tombstoned("w", worker_id):
        return False
    _tombstone("s", session_id)
    return True


def restricted_session(session_id="", worker_id=""):
    """Only a live hosted turn may be steered; generic resumes lose policy.

    The answer comes from the immutable origin recorded at trusted launch, so
    it survives ``workers.json`` pruning and later queue-policy edits. Current
    policy is still honoured for a row that predates the tombstones.
    """
    session_id, worker_id = str(session_id or ""), str(worker_id or "")
    if _tombstoned("s", session_id) or _tombstoned("w", worker_id):
        return True
    from . import config, workers
    for worker in workers.list_workers(prune=False):
        if ((session_id and worker.get("session_id") == session_id)
                or (worker_id and worker.get("worker_id") == worker_id)):
            if _tombstoned("w", str(worker.get("worker_id") or "")):
                try:
                    bind_session(worker.get("worker_id"), worker.get("session_id"))
                except OSError:
                    pass
                return True
            return config.codex_sandbox(worker.get("ticket_queue") or worker.get("queue", "")) == "workspace-write"
    return False


# -------------------------------------------------------------------- paths
def worker_store(queue, worker):
    from . import config
    if not NAME.fullmatch(worker):
        raise ValueError("invalid sandbox worker id")
    return canonical(config.codex_sandbox_store(queue)) / worker


def _under(path, root):
    return path == root or root in path.parents


def _trusted_link(parent, name):
    """A system prefix link: root-owned, in a root-owned directory that only
    root can modify (macOS ``/tmp``, a relocated ``/home``). A worker, or
    anything else running as the daemon's user, cannot create one."""
    try:
        link = os.lstat(os.path.join(parent, name))
        holder = os.lstat(parent)
    except OSError:
        return False
    return (stat.S_ISLNK(link.st_mode) and link.st_uid == 0
            and stat.S_ISDIR(holder.st_mode) and holder.st_uid == 0
            and not holder.st_mode & 0o022)


def canonical(path):
    """Resolve trusted system links only. Any other link stays a plain
    component, so the no-follow walk in ``directory`` refuses it."""
    todo = [(part, False) for part in reversed(Path(path).expanduser().absolute().parts[1:])]
    done, hops = "/", 0
    while todo:
        part, linked = todo.pop()
        if part in ("", "."):
            continue
        if part == "..":
            if not linked:
                raise ValueError("bridge paths cannot contain parent references")
            done = os.path.dirname(done)
            continue
        if _trusted_link(done, part):
            hops += 1
            if hops > 16:
                raise OSError(errno.ELOOP, "too many trusted links", str(path))
            target = os.readlink(os.path.join(done, part))
            parts = Path(target).parts
            if os.path.isabs(target):
                done, parts = "/", parts[1:]
            todo.extend((p, True) for p in reversed(parts))
            continue
        done = os.path.join(done, part)
    return Path(done)


@contextmanager
def directory(path):
    """Open every component without following a worker-supplied symlink."""
    path = canonical(path)
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
        try:
            return json.loads(raw)
        except RecursionError:
            raise ValueError("bridge file is nested too deeply") from None
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


def _retire(fd, name, budget):
    """Remove one untrusted entry, following nothing, in O(budget) steps.

    A directory is emptied one level at a time: a nested non-empty directory
    is moved up beside it under a junk name and finished by a later sweep, so
    depth costs no recursion and no file descriptors. ``budget`` is a one
    element list shared by a whole tick. Returns True once the name is gone.
    """
    try:
        os.unlink(name, dir_fd=fd)
        return True
    except FileNotFoundError:
        return True
    except (IsADirectoryError, PermissionError):
        pass
    except OSError:
        return False
    try:
        inner = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
    except OSError:
        return False
    try:
        with os.scandir(inner) as entries:
            names = [e.name for e in islice(entries, max(0, budget[0]))]
        for child in names:
            budget[0] -= 1
            try:
                os.unlink(child, dir_fd=inner)
                continue
            except FileNotFoundError:
                continue
            except OSError:
                pass
            try:
                os.rmdir(child, dir_fd=inner)
            except OSError:
                try:
                    os.rename(child, _JUNK + uuid.uuid4().hex, src_dir_fd=inner, dst_dir_fd=fd)
                except OSError:
                    pass
    except OSError:
        pass
    finally:
        os.close(inner)
    try:
        os.rmdir(name, dir_fd=fd)
        return True
    except FileNotFoundError:
        return True
    except OSError:
        return False


def _discard(fd, name, budget):
    """Retire a request name; if it cannot be removed yet, move it out of the
    request namespace so it cannot occupy a batch slot again."""
    if _retire(fd, name, budget):
        return
    try:
        os.rename(name, _JUNK + uuid.uuid4().hex, src_dir_fd=fd, dst_dir_fd=fd)
    except OSError:
        pass


# ------------------------------------------------------------- registration
def _forms(path):
    path = Path(path).expanduser().absolute()
    forms = {path}
    for resolve in (lambda p: p.resolve(), canonical):
        try:
            forms.add(resolve(path))
        except (OSError, ValueError, RuntimeError):
            pass
    return forms


def _protected():
    """(files, trees). A writable root may not contain a file; it may neither
    contain nor sit inside a tree."""
    import sys
    from . import config, queue as q, workers, messages
    home = Path.home()
    control = workers.WORKERS_FILE.parent
    files = [q.store_path(), q._db_path(), q._lock_path(), config.config_path(),
             workers.WORKERS_FILE, messages._outbox_file(), Path(sys.executable),
             home / ".netrc", home / ".git-credentials", home / ".gitconfig"]
    binary = workers._resolve_engine_bin("codex")
    if binary:
        files.append(Path(binary))
    trees = [authority_dir(), storage.data_dir(), control, Path(__file__).absolute().parent,
             workers.STOP_SIGNALS_DIR, control / "codex-app", control / "logs",
             home / ".codex", home / ".claude", home / ".ssh", home / ".gnupg",
             home / ".aws", home / ".kube", home / ".docker",
             home / ".config" / "gh", home / ".config" / "gcloud"]
    if os.environ.get("CODEX_HOME"):
        trees.append(Path(os.environ["CODEX_HOME"]))
    for getter in ("codex_worker_home", "codex_default_home"):
        try:
            value = getattr(config, getter)()
        except Exception:
            value = None
        if value:
            trees.append(Path(value))
    return files, trees


def _check_roots(workspace, store):
    files, trees = _protected()
    for root in (workspace, store):
        for r in _forms(root):
            if any(_under(f, r) for p in files for f in _forms(p)):
                raise ValueError("sandbox writable root contains trusted control files or broker code")
            if any(_under(t, r) or _under(r, t) for p in trees for t in _forms(p)):
                raise ValueError("sandbox writable root overlaps trusted control, runtime, code or credential paths")
    if any(_under(s, w) or _under(w, s) for s in _forms(store) for w in _forms(workspace)):
        raise ValueError("sandbox workspace and worker store must be separate roots")
    # Even an otherwise ordinary directory must not grant access to another
    # worker's private store. Scopes are trusted and survive worker exit.
    for path in islice(authority_dir().glob("*.json"), LEDGER_SCAN):
        try:
            peer = json.loads(path.read_text())
            peer_store, peer_repo = Path(peer["store"]), Path(peer["repo"])
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise ValueError("cannot validate existing sandbox roots") from exc
        for left, right in ((workspace, peer_store), (store, peer_repo)):
            if _under(left, right) or _under(right, left):
                raise ValueError("sandbox root overlaps another worker's private roots")
        if store != peer_store and (_under(store, peer_store) or _under(peer_store, store)):
            raise ValueError("sandbox store overlaps another worker's store")


def register(queue, worker, repo, assigned_ref=""):
    """Trusted launch only. A worker cannot register itself through the spool."""
    from . import config, queue as q, store_identity
    if config.backend(queue) != "file":
        raise ValueError("sandbox bridge requires a local file-backed queue")
    store = canonical(worker_store(queue, worker))
    workspace = canonical(repo)
    try:
        with directory(workspace):
            pass
    except OSError as exc:
        raise ValueError("sandbox workspace contains a missing or symlinked component") from exc
    # A writable checkout of the broker itself is not a security boundary.
    _check_roots(workspace, store)
    if assigned_ref and q._project_from_ident(assigned_ref) != queue:
        raise ValueError("single-ticket assignment must belong to the registered queue")
    canonical(store).mkdir(parents=True, exist_ok=True)
    authority_dir().mkdir(parents=True, exist_ok=True, mode=0o700)
    with directory(store):
        pass
    _check_roots(workspace, store)      # again, now that the store exists
    identity = store_identity.initialize()["store_id"]
    scope = {"worker": worker, "queue": queue, "repo": str(workspace),
             "store": str(store), "source": str(q._db_path().resolve()), "store_id": identity,
             "claim": None, "targets": [], "assigned_ref": assigned_ref,
             "restricted": True}
    with directory(authority_dir()) as fd:
        # Never overwrite a receipt/claim ledger when retrying a launch.
        try:
            old = _read(fd, worker + ".json")
        except FileNotFoundError:
            _write(fd, worker + ".json", scope)
        else:
            if any(old.get(k) != scope[k] for k in ("worker", "queue", "repo", "store", "source", "store_id", "assigned_ref")):
                raise ValueError("sandbox worker id is already registered with another scope")
    _tombstone("w", worker)
    return store


# ------------------------------------------------------------------ fencing
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


def _owned(scope, item):
    """The real queue, not the spool or this ledger, says the worker holds it."""
    from . import queue as q
    return bool(item and item.get("status") == "in_progress" and not item.get("needs_input")
                and item.get("claimed_by") == scope["worker"]
                and item.get("project") == scope["queue"]
                and (not scope.get("assigned_ref") or q._matches(item, scope["assigned_ref"])))


def _bind_claim(scope, item):
    """Record the claim. A destination is pinned once per ticket."""
    previous = (scope.get("claim") or {}).get("ref")
    scope["claim"] = {"ref": item["ref"], "fence": _fence(item)}
    budgets = scope.setdefault("budgets", {})
    budgets.setdefault(item["ref"], {"messages": 0, "comments": 0})
    for stale in list(budgets)[:-BUDGET_REFS]:
        del budgets[stale]
    if previous == item["ref"] and scope.get("destination"):
        return
    scope["targets"] = []
    scope.pop("destination", None)
    submitter = item.get("submitter")
    if submitter:
        from . import messages
        try:
            resolved = messages.resolve_target(submitter)
            if resolved.get("session_id"):
                scope["targets"] = [submitter]
                scope["destination"] = _pin(resolved)
        except ValueError:
            pass


def _adopt(scope, ref=""):
    """Fenced adoption of this worker's own outstanding claim, or None."""
    from . import queue as q
    if ref:
        item = q.get(ref)
        mine = [item] if _owned(scope, item) else []
    else:
        mine = [i for i in q.list_items(status="in_progress", project=scope["queue"])
                if _owned(scope, i)]
        known = (scope.get("claim") or {}).get("ref")
        mine = [i for i in mine if i.get("ref") == known] or mine
    if len(mine) > 1:
        raise ValueError("several outstanding claims (" + ", ".join(sorted(str(i.get("ref")) for i in mine)[:8])
                         + "); name one with `wt find <ref>`")
    if not mine:
        return None
    item = q.get(mine[0]["ref"])
    if not _owned(scope, item):
        return None
    if scope.get("claim") != {"ref": item["ref"], "fence": _fence(item)}:
        _bind_claim(scope, item)
    return item


def _fenced(function, scope):
    """Lock-time fence arguments, where the queue API accepts them."""
    names = inspect.signature(function).parameters
    out = {}
    if "sandbox_expect" in names:
        out["sandbox_expect"] = scope["claim"]["fence"]
    if "sandbox_store_id" in names:
        out["sandbox_store_id"] = scope["store_id"]
    return out


# --------------------------------------------------------------- projection
def _size(value):
    return len(json.dumps(value, ensure_ascii=False).encode())


_LEVELS = ((16000, 200), (8000, 100), (4000, 50), (2000, 25), (1000, 12), (400, 6), (160, 3))


def _clip(value, chars, items, path, cut, depth=0):
    if isinstance(value, str):
        if len(value) <= chars:
            return value
        cut.append({"path": path, "chars": len(value)})
        return value[:chars] + f"\n[bridge: {len(value) - chars} of {len(value)} chars omitted; page field {path!r}]"
    if isinstance(value, (list, dict)) and depth >= 12:
        cut.append({"path": path, "nested": True})
        return None
    if isinstance(value, list):
        start = max(0, len(value) - items)
        if start:
            cut.append({"path": path, "entries": len(value), "kept_from": start})
        return [_clip(v, chars, items, f"{path}.{i}" if path else str(i), cut, depth + 1)
                for i, v in enumerate(value[start:], start)]
    if isinstance(value, dict):
        return {k: _clip(v, chars, items, f"{path}.{k}" if path else str(k), cut, depth + 1)
                for k, v in value.items()}
    return value


def _fit(value, path, limit):
    """Largest explicit projection of ``value`` under ``limit`` bytes."""
    if _size(value) <= limit:
        return value, []
    for chars, items in _LEVELS:
        cut = []
        out = _clip(value, chars, items, path, cut)
        if _size(out) + _size(cut[:40]) <= limit:
            return out, cut
    return None, [{"path": path, "oversized": True}]


def _project(item):
    """A bounded, explicitly marked view. Fences use the real item, never this."""
    if not isinstance(item, dict) or _size(item) <= ITEM_MAX:
        return item
    out, cut = _fit(item, "", ITEM_MAX - 2048)
    note = {"truncated": True, "cut": cut[:40], "cut_total": len(cut),
            "page": f"wt find <ref> --field <path> --page <n> "
                    f"(text: {PAGE_CHARS} chars/page; lists: {PAGE_ITEMS} entries/page)"}
    if not isinstance(out, dict):
        out = {k: (str(item.get(k))[:200] if item.get(k) is not None else None)
               for k in ("ref", "id", "project", "status", "title", "claimed_by", "claimed_at",
                         "claimed_session_id", "last_answer_gen", "needs_input")}
        note.update(minimal=True, fields=[str(k)[:80] for k in list(item)[:100]])
    out["_bridge_projection"] = note
    return out


def _page(item, field, page):
    if (not isinstance(field, str) or not 0 < len(field) <= 200
            or type(page) is not int or page < 0):
        raise ValueError("invalid field or page")
    parts = field.split(".")
    if len(parts) > 8:
        raise ValueError("invalid field or page")
    value = item
    for part in parts:
        if isinstance(value, dict) and part in value:
            value = value[part]
        elif isinstance(value, list) and part.isdigit() and int(part) < len(value):
            value = value[int(part)]
        else:
            raise ValueError("no such field on the claimed ticket")
    if isinstance(value, str):
        pages = max(1, -(-len(value) // PAGE_CHARS))
        out = {"kind": "text", "text": value[page * PAGE_CHARS:(page + 1) * PAGE_CHARS]}
    elif isinstance(value, list):
        pages = max(1, -(-len(value) // PAGE_ITEMS))
        start = page * PAGE_ITEMS
        entries, cut = _fit(value[start:start + PAGE_ITEMS], field, ITEM_MAX - 2048)
        out = {"kind": "list", "start": start, "entries": entries, "cut": cut[:40]}
    else:
        pages = 1
        shown, cut = _fit(value, field, ITEM_MAX - 2048)
        out = {"kind": "value", "value": shown, "cut": cut[:40]}
    if page >= pages:
        raise ValueError(f"page out of range; {field} has {pages} page(s)")
    return {"field": field, "page": page, "pages": pages} | out


# ---------------------------------------------------------------- operations
def _fields(body, required, optional=()):
    if not isinstance(body, dict) or set(body) - set(required) - set(optional) or set(required) - set(body):
        raise ValueError("invalid or unknown bridge fields")


def _text(value, limit=32000):
    if not isinstance(value, str) or not value.strip() or len(value) > limit:
        raise ValueError("expected nonempty bounded text")
    return value


def _ref(value):
    if not isinstance(value, str) or not 0 < len(value) <= 80:
        raise ValueError("invalid ticket ref")
    return value


def _budget(scope, kind, limit):
    counts = scope.setdefault("budgets", {}).setdefault(
        scope["claim"]["ref"], {"messages": 0, "comments": 0})
    if counts.get(kind, 0) >= limit:
        raise ValueError(f"sandbox claim {kind[:-1]} budget exhausted")
    counts[kind] = counts.get(kind, 0) + 1


def _pin(resolved):
    worker = resolved.get("worker") or {}
    return {"session_id": str(resolved.get("session_id") or ""),
            "engine": str(resolved.get("engine") or ""),
            "worker_id": str(worker.get("worker_id") or "")}


def _message(scope, op, body, key):
    """Advisory messaging; transport acceptance never closes a ticket.

    The descriptor verified against the claim-time pin is the one delivered
    to: no second name lookup, live transports only. A sandbox report can
    never start a resumed, delegated or queued turn.
    """
    from . import messages, sender_envelope as envelope
    _fields(body, ("target", "text"))
    target, text = _text(body["target"]), _text(body["text"])
    if target not in scope.get("targets", []):
        raise ValueError("target is outside this claim's submitter scope")
    # Destination authority is frozen at claim time, not resolved from a
    # worker-editable agents.json or a later renamed agent.
    resolved = dict(messages.resolve_target(target))
    pinned = scope.get("destination")
    if not isinstance(pinned, dict) or not pinned.get("session_id") or _pin(resolved) != pinned:
        raise ValueError("message destination changed since claim")
    _budget(scope, "messages", MESSAGE_BUDGET)
    text = "[Untrusted sandbox worker report; review before acting.]\n" + text
    resolved["attr"] = envelope.make(
        pinned["session_id"], message_id=str(uuid.UUID(hex=key)), basis="runtime",
        evidence={"source": "sandbox_bridge", "untrusted": True},
        who=envelope.author("session", scope["worker"], scope["worker"], "codex"))
    sent = messages.deliver(resolved, text, "send", live_only=True)
    if not isinstance(sent, dict) or not sent.get("ok"):
        return {"ok": False, "delivered": False,
                "error": "destination is not running; nothing was delivered, queued or resumed"}
    out = {"ok": True, "delivered": True}
    for field in ("transport", "receipt_id"):
        if isinstance(sent.get(field), str) and _TOKEN.fullmatch(sent[field]):
            out[field] = sent[field]
    if op == "ask":
        # Only an explicitly correlated transport reply may cross this boundary.
        # Raw recipient logs contain unrelated host/session content.
        answer = sent.get("answer")
        if isinstance(answer, str) and answer:
            out["answer"] = answer[:PAGE_CHARS]
        else:
            out["answer_pending"] = True
    return out


def _idle(scope):
    """Nothing claimable: the CLI's claim-time surplus decision."""
    from . import config, queue as q, workers
    queue, worker = scope["queue"], scope["worker"]
    drain_on = config.auto_drain(queue)
    desired = config.desired_workers(queue) if drain_on else 0
    retained = workers.retained_parked_ids(queue)
    if worker in retained:
        return {"ok": True, "item": None, "idle": "parked"}
    live = workers.live_worker_count(queue, exclude=retained)
    if live > desired:
        why = (f"surplus at claim ({live}>{desired} desired)" if drain_on
               else "manual run done — auto-drain off and no run-requested ticket left to claim")
        workers._mark_worker_released(worker)
        q._log("STOP", f"{worker} — {why}", queue=queue)
        return {"ok": True, "item": {"stop": True}}
    return {"ok": True, "item": None}


def _claim(scope, request):
    from . import queue as q, workers
    body = request["body"]
    _fields(body, (), ("ref", "requested_only"))
    if body.get("ref", "") != scope.get("assigned_ref", ""):
        raise ValueError("claim ref differs from the trusted launch assignment")
    if type(body.get("requested_only", False)) is not bool:
        raise ValueError("invalid or unknown bridge fields")
    if request["claim"] is not None:
        raise ValueError("claim request must not supply authority")
    known = (scope.get("claim") or {}).get("ref")
    if known and _owned(scope, q.get(known)):
        raise ValueError(f"finish the current claim {known} before claiming again "
                         f"(`wt find {known}` re-reads it)")
    # The queue may hold a claim this ledger never recorded (crash between
    # the real claim and the scope write). Adopt it instead of stranding it.
    item = _adopt(scope)
    if item is not None:
        return {"ok": True, "item": _project(item), "claim": scope["claim"], "recovered": True}
    # session identity comes from trusted host records, never request data.
    record = next((w for w in workers.list_workers(prune=False)
                   if w.get("worker_id") == scope["worker"]), {})
    session = str(record.get("session_id") or "")
    if scope.get("assigned_ref"):
        item = q.claim_by_ref(scope["assigned_ref"], scope["worker"], session_uuid=session)
        if not item:
            raise ValueError("assigned ticket was not found")
    else:
        item = q.claim_next(scope["worker"], project=scope["queue"], session_uuid=session,
                            worker_model=workers.worker_model(scope["worker"], scope["queue"]),
                            requested_only=body.get("requested_only", False))
        if not item:
            return _idle(scope)
    if item.get("stop"):
        return {"ok": True, "item": {"stop": True}}
    _bind_claim(scope, item)
    return {"ok": True, "item": _project(item), "claim": scope["claim"]}


def _find(scope, request):
    """Recoverable claim read. It needs no fence: it re-derives authority from
    the real queue and hands back the fresh fence."""
    op, body = request["op"], request["body"]
    if op == "find":
        _fields(body, ("ref",), ("field", "page"))
        ref = _ref(body["ref"])
    else:
        _fields(body, ())
        ref = ""
    item, claim = None, scope.get("claim")
    if ref and claim and request["claim"] == claim and ref == claim["ref"]:
        try:
            item = _current(scope, inspect=True)
        except ValueError:
            item = None
    if item is None:
        item = _adopt(scope, ref)
    if item is None:
        raise ValueError("cannot inspect another ticket through this bridge" if ref
                         else "this worker holds no current claim")
    out = {"ok": True}
    if _owned(scope, item):
        out["claim"] = scope["claim"]
    if "field" in body:
        out["page"] = _page(item, body["field"], body.get("page", 0))
    else:
        out["item"] = _project(item)
    return out


def _resolution(body):
    resolution = {"summary": body["summary"], "no_code": body["no_code"]}
    for field in ("caveats", "follow_ups", "unresolved"):
        values = body.get(field, [])
        if not isinstance(values, list) or len(values) > 50:
            raise ValueError("resolution entries must be lists")
        for value in values:
            _text(value)
            if field != "caveats" and not re.search(r"\b[A-Z][A-Z0-9_-]*-\d+\b", value):
                raise ValueError("unfinished work must cite a ticket")
        resolution[field] = values
    return resolution


def _commit_proof(scope, commit):
    # Both roots and the original base come from the trusted launch record.
    # Never run host Git against worker-writable config, pointers or alternates.
    from . import sandbox_policy
    record = sandbox_policy.launch_record(scope["worker"])
    if (not record or record.get("repo") != scope["repo"]
            or record.get("store") != scope["store"]):
        raise ValueError("commit does not resolve: missing or mismatched sandbox launch record")
    result = sandbox_policy.verify_worker_commit(scope["worker"], commit)
    if not result.get("ok"):
        raise ValueError("commit does not resolve: " + str(result.get("error") or "invalid proof"))
    return result["commit"]


def _execute(scope, request, key):
    from . import queue as q
    _fields(request, ("op", "body", "claim"))
    op, body = request["op"], request["body"]
    if op == "claim":
        return _claim(scope, request)
    if op in ("find", "current"):
        return _find(scope, request)
    if request["claim"] != scope.get("claim") or not scope.get("claim"):
        raise ValueError("request does not match the assigned claim")
    item = _current(scope)
    if op in ("ask", "tell", "send"):
        return _message(scope, op, body, key)
    if op == "comment":
        # Progress evidence only: no state change, attestation or decision.
        _fields(body, ("ref", "text"))
        if body["ref"] != item["ref"]:
            raise ValueError("cannot comment on another ticket through this bridge")
        text = _text(body["text"], 24000)
        _budget(scope, "comments", COMMENT_BUDGET)
        noted = q.comment(item["ref"], text, by="worker", session_id=scope["worker"],
                          **_fenced(q.comment, scope))
        if not noted:
            raise ValueError("claim changed during comment; nothing was recorded")
        return {"ok": True, "item": {"ref": noted.get("ref"), "status": noted.get("status")}}
    if op == "block":
        # A real blocker for a human. No kind, wait routing, commit proof,
        # force or auto-decision can be chosen from inside the sandbox.
        _fields(body, ("ref", "question"), ("progress",))
        if body["ref"] != item["ref"]:
            raise ValueError("cannot block another ticket through this bridge")
        question = _text(body["question"], 4000)
        progress = _text(body["progress"], 24000) if body.get("progress") else ""
        blocked = q.block(item["ref"], session_id=scope["worker"], question=question,
                          progress=progress, kind="input", origin="worker",
                          **_fenced(q.block, scope))
        if not blocked:
            raise ValueError("claim changed during block; nothing was recorded")
        return {"ok": True, "item": _project(blocked)}
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
    resolution = _resolution(body)
    if body["commit"]:
        resolution["commit"] = _commit_proof(scope, body["commit"])
        from . import sandbox_handoff
        artifact = sandbox_handoff.export(scope, resolution)
        blocked = q.block(
            item["ref"], session_id=scope["worker"], origin="worker",
            question="Integration worker: review and adopt the sandbox patch, verify it, "
                     "and land it on the release branch before resolving this ticket.",
            progress="Sandbox commit " + resolution["commit"] + "; trusted handoff: " + artifact
                     + ". This is candidate source, not an integrated fix.\n\n" + resolution["summary"],
            wait_on="worker", wait_actor="sandbox integration worker",
            **_fenced(q.block, scope))
        if not blocked:
            raise ValueError("claim changed during handoff; nothing was resolved")
        return {"ok": True, "integration_pending": True, "item": _project(blocked)}
    # The regular close path still owns gates, product acknowledgement and
    # ownership checks. It refuses unsafe command gates under bridge mode.
    closed = q.close(item["ref"], scope["worker"], resolution=resolution,
                     session_uuid=str(item.get("claimed_session_id") or ""),
                     **_fenced(q.close, scope))
    if not closed:
        raise ValueError("claim changed during close; nothing was closed")
    if closed.get("status") == "in_review" and closed.get("gate_pending") == "verify":
        from . import stages
        stages.request(closed["ref"], "verify")
    return {"ok": closed.get("status") in ("closed", "in_review"), "item": _project(closed)}


def _bounded(result):
    """Last line of defence: a result can never be too large to deliver."""
    if isinstance(result, dict) and _size(result) <= RESULT_MAX:
        return result
    out = {"ok": bool(isinstance(result, dict) and result.get("ok")),
           "error": "bridge result exceeded its size limit; page it with `wt find <ref> --field <path>`"}
    if isinstance(result, dict) and isinstance(result.get("claim"), dict) and _size(result["claim"]) < 4096:
        out["claim"] = result["claim"]
    return out


# ------------------------------------------------------------------- broker
def _scan(spool):
    """One bounded look at the spool: (requests, removable leftovers)."""
    requests, leftovers = [], []
    with os.scandir(spool) as entries:
        for entry in islice(entries, SPOOL_SCAN):
            name = entry.name
            if name.endswith(".request") and REQUEST_ID.fullmatch(name[:-8]):
                requests.append(name)
            elif name.startswith(_JUNK):
                leftovers.append((name, 0))
            elif name.endswith(".response") and REQUEST_ID.fullmatch(name[:-9]):
                leftovers.append((name, RESPONSE_RETAIN_S))
            elif name.startswith(".bridge-"):
                leftovers.append((name, TEMP_RETAIN_S))
    return sorted(requests)[:SPOOL_BATCH], leftovers


def _sweep(spool, leftovers, budget, now=None):
    """Retire stale responses, temporaries and junk; bounded by ``budget``."""
    now = time.time() if now is None else now
    for name, retain in leftovers:
        if budget[0] <= 0:
            return
        try:
            if retain and now - os.stat(name, dir_fd=spool, follow_symlinks=False).st_mtime < retain:
                continue
        except OSError:
            continue
        budget[0] -= 1
        _retire(spool, name, budget)


def _respond(spool, key, result, budget):
    name = key + ".response"
    try:
        _write(spool, name, result)
    except OSError:
        # A planted directory (or anything else rename cannot replace).
        _retire(spool, name, budget)
        _write(spool, name, result)


def _serve_one(worker, scope, trusted, spool, name, state, budget):
    from . import queue as q
    key = name.removesuffix(".request")
    try:
        request = _read(spool, name)
        digest = hashlib.sha256(json.dumps(request, sort_keys=True).encode()).hexdigest()
        receipt_name = worker + "-" + key + ".receipt"
        try:
            receipt = _read(trusted, receipt_name)
        except FileNotFoundError:
            if state["receipts"] >= MAX_RECEIPTS:
                _respond(spool, key, {"ok": False, "error": "bridge receipt budget exhausted; recycle worker"}, budget)
                return
            receipt = {"digest": digest, "result": {
                "ok": False, "error": "indeterminate: broker interrupted; inspect real ticket before retry"}}
            _write(trusted, receipt_name, receipt)
            state["receipts"] += 1
            try:
                result = _execute(scope, request, key)
            except Exception as exc:
                # Validation errors are deliberately public;
                # unexpected runtime details stay in the daemon
                # log rather than becoming a credential oracle.
                if isinstance(exc, ValueError):
                    error = str(exc)[:2000]
                else:
                    q._log("SANDBOX_BRIDGE_ERROR", f"{worker} {key}: {type(exc).__name__}", queue=scope["queue"])
                    error = "bridge operation failed: " + type(exc).__name__
                result = {"ok": False, "error": error}
            result = _bounded(result)
            _write(trusted, worker + ".json", scope)
            receipt["result"] = result
            _write(trusted, receipt_name, receipt)
        if receipt["digest"] != digest:
            result = {"ok": False, "error": "request id already used with different content"}
        else:
            result = receipt["result"]
        _respond(spool, key, result, budget)
    except (OSError, ValueError, RecursionError):
        # Invalid files never reach dispatch; do not copy OS
        # errors or privileged file content into the spool.
        pass
    except Exception as exc:
        q._log("SANDBOX_BRIDGE_ERROR", f"{worker} {key}: {type(exc).__name__}", queue=scope["queue"])
    finally:
        _discard(spool, name, budget)


def process(worker):
    """Process bounded requests for one trusted registration, at most once.

    The receipt is reserved durably BEFORE effects. A daemon crash leaves an
    explicit indeterminate receipt and is never permission to replay a send.
    Each entry is isolated: a hostile one is retired and the batch continues.
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
            record = next((w for w in workers.list_workers(prune=False)
                           if w.get("worker_id") == worker), {})
            # Bind before any refusal: restriction must outlive this policy.
            _tombstone("w", worker)
            bind_session(worker, record.get("session_id"))
            if config.codex_sandbox(scope["queue"]) != "workspace-write":
                raise ValueError("sandbox bridge policy is disabled")
            if (not record.get("alive") or record.get("released_at")
                    or record.get("engine") != "codex" or record.get("queue") != scope["queue"]):
                raise ValueError("sandbox worker is no longer live in its registered queue")
            with os.scandir(trusted) as entries:
                state = {"receipts": sum(
                    1 for e in islice(entries, LEDGER_SCAN)
                    if e.name.startswith(worker + "-") and e.name.endswith(".receipt")
                    and REQUEST_ID.fullmatch(e.name[len(worker) + 1:-len(".receipt")]))}
            with directory(scope["store"]) as spool:
                names, leftovers = _scan(spool)
                budget = [RETIRE_BUDGET]
                for name in names:
                    _serve_one(worker, scope, trusted, spool, name, state, budget)
                _sweep(spool, leftovers, budget)


def _ledger_owner(name):
    for suffix in (".receipt", ".json", ".lock"):
        if name.endswith(suffix):
            stem = name[:-len(suffix)]
            if suffix == ".receipt":
                if len(stem) < 34 or stem[-33] != "-" or not REQUEST_ID.fullmatch(stem[-32:]):
                    return None, ""
                stem = stem[:-33]
            return (stem, suffix) if NAME.fullmatch(stem) else (None, "")
    return None, ""


def retire(now=None, budget=RETIRE_BUDGET):
    """Bounded retirement of dead workers' receipts, ledgers and locks.

    Only files untouched for ``LEDGER_RETAIN_S`` that belong to a worker with
    no live registration are removed, receipts first. Tombstones live in
    their own directory and are never candidates. Returns the removal count.
    """
    from . import workers
    now = time.time() if now is None else now
    live = {str(w.get("worker_id")) for w in workers.list_workers(prune=False) if w.get("alive")}
    removed = 0
    try:
        opened = directory(authority_dir())
        trusted = opened.__enter__()
    except FileNotFoundError:
        return 0
    try:
        with os.scandir(trusted) as entries:
            names = [e.name for e in islice(entries, LEDGER_SCAN)]
        held = {}       # worker -> receipts that must outlive this pass
        ordered = sorted(names, key=lambda n: (not n.endswith(".receipt"), not n.endswith(".json")))
        for name in ordered:
            if removed >= budget:
                break
            owner, kind = _ledger_owner(name)
            stray = owner is None and name.startswith(".bridge-")
            if (owner is None and not stray) or owner in live:
                continue
            if kind in (".json", ".lock") and held.get(owner):
                continue
            if kind == ".lock" and owner + ".json" in names and not _gone(trusted, owner + ".json"):
                continue
            try:
                info = os.stat(name, dir_fd=trusted, follow_symlinks=False)
                old = stat.S_ISREG(info.st_mode) and now - info.st_mtime >= LEDGER_RETAIN_S
                if old:
                    os.unlink(name, dir_fd=trusted)
                    removed += 1
            except OSError:
                old = False
            if not old and kind == ".receipt":
                held[owner] = True
        return removed
    finally:
        opened.__exit__(None, None, None)


def _gone(fd, name):
    try:
        os.stat(name, dir_fd=fd, follow_symlinks=False)
        return False
    except FileNotFoundError:
        return True
    except OSError:
        return False


def serve(stop):
    """One background broker per worker, so an ask cannot stall reconcile."""
    import threading
    running = {}
    swept = 0.0
    from . import queue as q, workers
    while not stop.wait(0.25):
        if not authority_dir().exists():
            continue
        rows = workers.list_workers(prune=False)
        for row in rows:
            # A session id can appear after launch; bind it while the row
            # still exists so restriction survives the prune that follows.
            if row.get("session_id") and (str(_tomb_dir()), "s", str(row["session_id"])) not in _BOUND:
                try:
                    bind_session(row.get("worker_id"), row.get("session_id"))
                except OSError:
                    pass
        if time.monotonic() - swept >= RETIRE_EVERY_S:
            swept = time.monotonic()
            try:
                retire()
            except (OSError, ValueError):
                pass
        live = {w["worker_id"] for w in rows if w.get("alive") and w.get("engine") == "codex"}
        for path in authority_dir().glob("*.json"):
            worker = path.stem
            if worker not in live or not NAME.fullmatch(worker) or running.get(worker, None) and running[worker].is_alive():
                continue
            def run(worker=worker):
                try:
                    process(worker)
                except (OSError, ValueError):
                    pass
                except Exception as exc:
                    q._log("SANDBOX_BRIDGE_ERROR", f"{worker}: {type(exc).__name__}")
            thread = threading.Thread(target=run, daemon=True)
            running[worker] = thread
            thread.start()
        for worker in [w for w, t in running.items() if not t.is_alive() and w not in live]:
            del running[worker]


# ------------------------------------------------------------------- client
def _exchange(fd, op, body, claim, timeout):
    key = uuid.uuid4().hex
    _write(fd, key + ".request", {"op": op, "body": body, "claim": claim})
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            result = _read(fd, key + ".response")
        except FileNotFoundError:
            time.sleep(0.05)
            continue
        try:
            os.unlink(key + ".response", dir_fd=fd)     # consumed; the receipt is the record
        except OSError:
            pass
        if not isinstance(result, dict):
            return {"ok": False, "error": "malformed bridge response"}
        if isinstance(result.get("claim"), dict):
            _write(fd, "claim.json", result["claim"])
        return result
    return {"ok": False, "error": f"bridge timeout; request {key} may still execute; inspect receipt before retry"}


def request(op, body, *, timeout=45):
    """Sandbox-side client. Only the request/response spool is writable."""
    spool = Path(os.environ["WT_SANDBOX_BRIDGE"])
    with directory(spool) as fd:
        claim = None
        if op not in ("claim", "current"):
            try:
                claim = _read(fd, "claim.json")
            except (OSError, ValueError):
                claim = None
            if claim is None and op != "find":
                # Lost local state (crash, lost response): recover the claim
                # from the daemon instead of stranding the ticket.
                recovered = _exchange(fd, "current", {}, None, timeout)
                if not recovered.get("ok") or not isinstance(recovered.get("claim"), dict):
                    return {"ok": False, "error": recovered.get("error") or "no current claim to act on"}
                claim = recovered["claim"]
        return _exchange(fd, op, body, claim, timeout)


def _mark_mine(item):
    me = os.environ.get("WT_WORKER_ID")
    if item.get("claimed_by") == me:
        item["claimed_by_you"] = True
    if item.get("closed_by") == me:
        item["closed_by_you"] = True
    for event in item.get("history") or []:
        if isinstance(event, dict) and (event.get("by") or {}).get("worker") == me:
            event["you"] = True


def cli(args):
    """Return a CLI exit code, refusing all commands outside the contract."""
    import sys
    op = args.command
    me = os.environ.get("WT_WORKER_ID")
    as_json = getattr(args, "json", False)
    if getattr(args, "worker", "") and args.worker != me and op in ("claim", "find", "close", "block", "comment"):
        result = {"ok": False, "error": "cannot impersonate another worker"}
    elif op == "claim":
        if args.queue != os.environ.get("WT_SANDBOX_QUEUE"):
            result = {"ok": False, "error": "claim is restricted to the assigned queue"}
        elif getattr(args, "oldest", False) or getattr(args, "type", None) or getattr(args, "readiness", None):
            result = {"ok": False, "error": "sandbox claim cannot reorder or filter the queue"}
        else:
            body = {"ref": args.ref} if getattr(args, "ref", None) else {}
            if getattr(args, "requested_only", False):
                body["requested_only"] = True
            result = request(op, body)
    elif op in ("find", "close"):
        if getattr(args, "force", False) or getattr(args, "duplicate_of", "") or getattr(args, "enqueue_follow_ups", False):
            result = {"ok": False, "error": "sandbox cannot force, duplicate or enqueue a close"}
        else:
            body = {"ref": args.ref}
            if op == "close":
                body.update(summary=args.summary, commit=args.commit or "", no_code=args.no_code,
                            caveats=args.caveat or [], follow_ups=args.follow_up or [],
                            unresolved=args.unresolved or [])
            elif getattr(args, "field", None):
                body.update(field=args.field, page=int(getattr(args, "page", 0) or 0))
            result = request(op, body)
    elif op == "block":
        if (getattr(args, "force", False) or getattr(args, "commit", "")
                or getattr(args, "kind", "input") not in ("", "input")
                or any(getattr(args, name, None) for name in (
                    "wait_on", "wait_actor", "wait_ref", "decision_class", "weight",
                    "recommend", "major_because", "major_detail"))
                or getattr(args, "confidence", None) is not None):
            result = {"ok": False, "error": "sandbox block carries only --question and --progress"}
        else:
            body = {"ref": args.ref, "question": args.question}
            if getattr(args, "progress", ""):
                body["progress"] = args.progress
            result = request(op, body)
    elif op == "comment":
        if getattr(args, "by", "") != "worker":
            result = {"ok": False, "error": "sandbox comments are worker progress only; pass --by worker"}
        else:
            result = request(op, {"ref": args.ref, "text": args.text})
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
    if op == "claim" and (item is None or item.get("stop")):
        if as_json:
            print(json.dumps(item or {}))
        elif item:
            print("STOP: surplus worker (live>desired); exiting")
        elif result.get("idle") == "parked":
            print(f"(nothing open in {args.queue}; you own a parked ticket awaiting an answer -- stay idle)")
        else:
            print(f"(nothing open in {args.queue})")
    elif op in ("claim", "find"):
        if result.get("recovered"):
            print("note: recovered this worker's outstanding claim", file=sys.stderr)
        page = result.get("page")
        if page is not None:
            if page.get("kind") == "text" and not as_json:
                print(page.get("text", ""))
                print(f"[page {page['page'] + 1} of {page['pages']} of {page['field']}]", file=sys.stderr)
            else:
                print(json.dumps(page, indent=2))
        elif item is not None:
            _mark_mine(item)
            print(json.dumps(item, indent=2))
    elif op == "close":
        if result.get("integration_pending"):
            print(f"AWAITING INTEGRATION: {item.get('ref')} — patch exported; ticket remains open")
        else:
            summary = (item.get("resolution") or {}).get("summary", "")
            print(f"{str(item.get('status')).upper()}: {item.get('ref')} — {summary}")
    elif op == "block":
        if as_json:
            print(json.dumps(item, indent=2))
        else:
            print(f"BLOCKED: {item.get('ref')} — {item.get('block_question') or '(no question)'}")
    elif op == "comment":
        print(f"COMMENTED: {item.get('ref')}")
    elif as_json:
        print(json.dumps(result, indent=2))
    else:
        print(result.get("answer") or "message accepted by transport; delivery receipt is authoritative")
    return 0

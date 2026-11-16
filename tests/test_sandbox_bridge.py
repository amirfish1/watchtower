"""Real-store effects and untrusted-spool rejection for WT-61."""
import json
import os
from pathlib import Path
import threading
import uuid

import pytest

from watchtower import sandbox_bridge as bridge


@pytest.fixture
def env(wt_env, monkeypatch):
    w = wt_env
    w.repo = w.tmp / "repo"
    w.repo.mkdir()
    w.config.set_codex_sandbox("SB", "workspace-write", str(w.tmp / "isolated"))
    with w.queue._FileLock(w.queue._lock_path()):
        w.queue._save_unlocked(w.queue._load_unlocked())
    w.store = bridge.register("SB", "sb-w1", str(w.repo))
    w.workers.record_worker(os.getpid(), "SB", "codex", "sb-w1", str(w.repo), str(w.tmp / "worker.log"))
    monkeypatch.setattr(w.queue, "_notify_ticket_event", lambda *a, **k: None)
    monkeypatch.setattr(w.queue, "_notify_review", lambda *a, **k: None)
    w.worker = "sb-w1"
    return w


def submit(w, op, body, claim=None, key=None):
    key = key or uuid.uuid4().hex
    request = {"op": op, "body": body, "claim": claim}
    with bridge.directory(w.store) as fd:
        bridge._write(fd, key + ".request", request)
    bridge.process(w.worker)
    return json.loads((w.store / (key + ".response")).read_text())


def claim(w, **kw):
    w.ticket = w.queue.enqueue(project="SB", note="assigned work", repo_path=str(w.repo), **kw)
    result = submit(w, "claim", {})
    assert result["ok"], result
    w.claim = result["claim"]
    return result["item"]


def close_body(ref):
    return {"ref": ref, "summary": "Fixed.\n\n- Verified the reported behavior.",
            "commit": "", "no_code": True}


def test_claim_close_real_store_and_replay(env):
    w = env
    item = claim(w)
    assert w.queue.get(item["ref"])["claimed_by"] == w.worker
    key = uuid.uuid4().hex
    body = close_body(item["ref"])
    first = submit(w, "close", body, w.claim, key)
    assert first["ok"], first
    assert w.queue.get(item["ref"])["status"] == "closed"
    replay = submit(w, "close", body, w.claim, key)
    assert replay == first
    assert len([e for e in w.queue.get(item["ref"])["history"] if e["event"] == "close"]) == 1
    assert submit(w, "find", {"ref": item["ref"]}, w.claim)["item"]["status"] == "closed"
    body["summary"] = "different"
    assert "different content" in submit(w, "close", body, w.claim, key)["error"]


@pytest.mark.parametrize("field,value", [("worker", "another-worker"), ("queue", "OTHER"),
                                         ("cwd", "/tmp"), ("argv", ["sh", "-c", "touch /tmp/bad"])])
def test_rejects_authority_fields(env, field, value):
    w = env
    key = uuid.uuid4().hex
    (w.store / (key + ".request")).write_text(json.dumps({"op": "claim", "body": {}, "claim": None, field: value}))
    bridge.process(w.worker)
    result = json.loads((w.store / (key + ".response")).read_text())
    assert not result["ok"] and "fields" in result["error"]


def test_cross_ticket_and_missing_proof_refused(env):
    w = env
    item = claim(w)
    other = w.queue.enqueue(project="SB", note="other")
    body = close_body(other["ref"])
    assert not submit(w, "close", body, w.claim)["ok"]
    assert w.queue.get(other["ref"])["status"] == "open"
    body = close_body(item["ref"])
    body["no_code"] = False
    assert not submit(w, "close", body, w.claim)["ok"]
    assert w.queue.get(item["ref"])["status"] == "in_progress"


def test_stale_answer_and_same_second_reclaim(env):
    w = env
    item = claim(w)
    with w.queue._FileLock(w.queue._lock_path()):
        data = w.queue._load_unlocked()
        it = next(i for i in data["items"] if i["ref"] == item["ref"])
        w.queue._append_history(it, "claim", by=w.queue._by("worker", w.worker))
        w.queue._save_unlocked(data)
    assert "stale" in submit(w, "close", close_body(item["ref"]), w.claim)["error"]


def test_answer_generation_fences_request(env):
    w = env
    item = claim(w)
    with w.queue._FileLock(w.queue._lock_path()):
        data = w.queue._load_unlocked()
        it = next(i for i in data["items"] if i["ref"] == item["ref"])
        it["last_answer_gen"] = 99
        w.queue._save_unlocked(data)
    assert "stale" in submit(w, "close", close_body(item["ref"]), w.claim)["error"]


def test_close_rechecks_claim_inside_write_lock(env, monkeypatch):
    w = env
    item = claim(w)
    original = w.queue.update_status
    def race(*a, **kw):
        with w.queue._FileLock(w.queue._lock_path()):
            data = w.queue._load_unlocked()
            it = next(i for i in data["items"] if i["ref"] == item["ref"])
            it["claimed_by"] = "replacement"
            w.queue._save_unlocked(data)
        return original(*a, **kw)
    monkeypatch.setattr(w.queue, "update_status", race)
    result = submit(w, "close", close_body(item["ref"]), w.claim)
    assert not result["ok"] and "changed" in result["error"]
    assert w.queue.get(item["ref"])["status"] == "in_progress"


@pytest.mark.parametrize("gate", ["review", "verify"])
def test_preserves_required_review_gates(env, gate):
    w = env
    item = claim(w, gates=[gate])
    result = submit(w, "close", close_body(item["ref"]), w.claim)
    assert result["ok"] and result["item"]["status"] == "in_review"
    assert result["item"]["gate_pending"] == gate


def test_stage_and_headless_resume_cannot_drop_policy(env, monkeypatch):
    w = env
    monkeypatch.setattr(w.workers.subprocess, "Popen", lambda *a, **k: pytest.fail("unsandboxed launch"))
    with pytest.raises(w.workers.CodexSandboxError, match="isolated stage transport"):
        w.workers.spawn_adhoc("review", engine="codex", stage="verifier", ticket_queue="SB")
    assert not w.cli._resume_session_headless("session", str(w.repo), "resume", "codex",
                                              queue="SB", worker_id=w.worker)


def test_rpc_and_message_fallback_cannot_start_unrestricted_turn(env, monkeypatch):
    from watchtower import codex_app_worker, codex_rpc, messages
    w = env
    sid = "11111111-2222-7333-8444-555555555555"
    monkeypatch.setattr(bridge, "restricted_session", lambda *a: True)
    monkeypatch.setattr(codex_rpc, "_thread_in_other_home", lambda *a: False)
    monkeypatch.setattr(codex_app_worker, "steer", lambda *a, **k: {"ok": False, "code": "no_app_worker"})
    monkeypatch.setattr(codex_rpc, "request", lambda *a, **k: pytest.fail("generic app-server resumed sandbox thread"))
    assert codex_rpc.deliver(sid, "hello")["stage"] == "sandbox-policy"
    monkeypatch.setattr(messages, "_deliver_delegate", lambda *a, **k: pytest.fail("delegate fallback escaped policy"))
    assert not messages._deliver_unreceipted({"engine": "codex", "session_id": sid}, "hello")["ok"]


def test_refuses_privileged_shell_gates(env):
    w = env
    marker = w.tmp / "escape"
    item = claim(w, gates=["cmd:touch " + str(marker)])
    result = submit(w, "close", close_body(item["ref"]), w.claim)
    assert not result["ok"] and "command gates" in result["error"]
    assert not marker.exists()
    assert w.queue.get(item["ref"])["status"] == "in_progress"


@pytest.mark.parametrize("kind", ["symlink", "hardlink", "fifo", "oversized"])
def test_untrusted_files_never_followed(env, kind):
    w = env
    secret = w.tmp / "private"
    secret.write_text('{"private":"do not copy"}')
    key = uuid.uuid4().hex
    p = w.store / (key + ".request")
    if kind == "symlink":
        p.symlink_to(secret)
    elif kind == "hardlink":
        os.link(secret, p)
    elif kind == "fifo":
        os.mkfifo(p)
    else:
        p.write_bytes(b" " * (bridge.MAX_BYTES + 1))
    bridge.process(w.worker)
    assert not (w.store / (key + ".response")).exists()
    assert secret.read_text() == '{"private":"do not copy"}'


def test_response_symlink_cannot_write_outside_spool(env):
    w = env
    key = uuid.uuid4().hex
    private = w.tmp / "private"
    private.write_text("unchanged")
    (w.store / (key + ".response")).symlink_to(private)
    result = submit(w, "claim", {}, key=key)
    assert result["ok"]
    assert private.read_text() == "unchanged"
    assert not (w.store / (key + ".response")).is_symlink()


def test_store_symlink_is_refused(env):
    w = env
    old = w.store.with_name("old")
    w.store.rename(old)
    w.store.symlink_to(w.tmp, target_is_directory=True)
    with pytest.raises(OSError):
        bridge.process(w.worker)


def test_broker_and_control_files_cannot_be_writable_roots(env):
    with pytest.raises(ValueError, match="trusted control"):
        bridge.register("SB", "bad", str(Path(bridge.__file__).resolve().parent.parent))


def test_client_roundtrip_uses_daemon_not_isolated_queue(env, monkeypatch):
    w = env
    w.queue.enqueue(project="SB", note="round trip")
    monkeypatch.setenv("WT_SANDBOX_BRIDGE", str(w.store))
    stop = threading.Event()
    server = threading.Thread(target=bridge.serve, args=(stop,), daemon=True)
    server.start()
    try:
        result = bridge.request("claim", {}, timeout=3)
        assert result["ok"] and result["item"]["claimed_by"] == w.worker
        assert not (w.store / "queue.db").exists()
        result = bridge.request("close", close_body(result["item"]["ref"]), timeout=3)
        assert result["ok"] and result["item"]["status"] == "closed"
    finally:
        stop.set()
        server.join(3)


@pytest.mark.parametrize("transport", ["exec", "app-server"])
def test_launch_transports_same_store_and_no_network(env, monkeypatch, transport):
    w = env
    w.config.set_codex_transport("SB", transport)
    monkeypatch.setattr(w.workers, "_resolve_engine_bin", lambda e: "/bin/codex")
    # Launch profiles deny /dev/shm (this host's pytest temp directory).
    # Construction only: roots validation has separate native-policy tests.
    from watchtower import sandbox_policy
    monkeypatch.setattr(sandbox_policy, "validate_roots", lambda *a: None)
    argv = w.workers.build_drain_command("SB", "codex", w.worker, str(w.repo), goal="work")
    profile = next(a for a in argv if a.startswith("permissions.wt-sandbox="))
    assert json.dumps(str(w.repo)) + '="write"' in profile
    assert json.dumps(str(w.store)) + '="write"' in profile
    assert '"network"={"enabled"=false}' in profile
    assert "--dangerously-bypass-approvals-and-sandbox" not in argv
    child = w.workers._spawn_env(w.worker, queue="SB", engine="codex")
    assert child["WT_SANDBOX_BRIDGE"] == str(w.store)


def test_crash_reserved_receipt_never_reexecutes(env, monkeypatch):
    w = env
    key = uuid.uuid4().hex
    def crash(*a):
        raise SystemExit("process interrupted")
    monkeypatch.setattr(bridge, "_execute", crash)
    with pytest.raises(SystemExit):
        submit(w, "claim", {}, key=key)
    monkeypatch.setattr(bridge, "_execute", lambda *a: pytest.fail("replayed indeterminate effect"))
    result = submit(w, "claim", {}, key=key)
    assert not result["ok"] and "indeterminate" in result["error"]


@pytest.mark.parametrize("op", ["ask", "tell"])
def test_scoped_messages_preserve_reply_and_receipt_and_do_not_replay(env, monkeypatch, op):
    # WT-61 hardening: the bridge no longer calls messages.ask/send (both can
    # fall through to an unsandboxed headless resume or the outbox). It hands
    # the descriptor it verified against the claim-time pin to the live-only
    # adapter chain, and returns a minimal schema.
    from watchtower import messages, sender_envelope as envelope
    w = env
    destination = {"session_id": "11111111-2222-7333-8444-555555555555", "engine": "claude"}
    monkeypatch.setattr(messages, "resolve_target", lambda target: destination.copy())
    calls = []
    def deliver(resolved, text, mode="send", **kw):
        calls.append((resolved, text, kw))
        return {"ok": True, "transport": "uds", "receipt_id": "receipt-1",
                "log": "/home/private/msg.log", "attr": resolved["attr"]}
    monkeypatch.setattr(messages, "deliver", deliver)
    for unsafe in ("ask", "send", "_deliver_resume", "_deliver_delegate", "outbox_add"):
        monkeypatch.setattr(messages, unsafe, lambda *a, **k: pytest.fail("unsandboxed or queued delivery"))
    claim(w, submitter="@filer")
    key = uuid.uuid4().hex
    body = {"target": "@filer", "text": "What was expected?"}
    first = submit(w, op, body, w.claim, key)
    expected = {"ok": True, "delivered": True, "transport": "uds", "receipt_id": "receipt-1"}
    if op == "ask":
        expected["answer_pending"] = True
    assert first == expected
    assert submit(w, op, body, w.claim, key) == first
    assert len(calls) == 1
    resolved, text, kw = calls[0]
    assert kw == {"live_only": True}
    assert resolved["session_id"] == destination["session_id"]
    assert resolved["attr"]["evidence"]["untrusted"] is True
    assert resolved["attr"]["author"]["id"] == w.worker
    assert resolved["attr"]["message_id"] == str(uuid.UUID(hex=key))
    assert envelope._parse_line(envelope.header(resolved["attr"])) is not None
    assert text.startswith("[Untrusted sandbox worker report")
    body["target"] = "@unrelated"
    assert not submit(w, op, body, w.claim)["ok"]
    body["target"] = "@filer"
    destination["session_id"] = "renamed-target"
    assert "destination changed" in submit(w, op, body, w.claim)["error"]
    assert len(calls) == 1


def test_policy_disable_and_dead_worker_revoke_authority(env, monkeypatch):
    w = env
    w.config.set_codex_sandbox("SB", "full-access")
    with pytest.raises(ValueError, match="disabled"):
        bridge.process(w.worker)
    w.config.set_codex_sandbox("SB", "workspace-write", str(w.tmp / "isolated"))
    monkeypatch.setattr(w.workers, "list_workers", lambda **kw: [])
    with pytest.raises(ValueError, match="no longer live"):
        bridge.process(w.worker)


def test_run_once_assignment_cannot_claim_another_ticket(env):
    w = env
    first = w.queue.enqueue(project="SB", note="first")
    second = w.queue.enqueue(project="SB", note="second")
    with bridge.directory(bridge.authority_dir()) as fd:
        scope = bridge._read(fd, w.worker + ".json")
        scope["assigned_ref"] = second["ref"]
        bridge._write(fd, w.worker + ".json", scope)
    assert not submit(w, "claim", {"ref": first["ref"]})["ok"]
    assert submit(w, "claim", {"ref": second["ref"]})["item"]["ref"] == second["ref"]
    assert w.queue.get(first["ref"])["status"] == "open"


@pytest.mark.parametrize("transport", ["exec", "app-server"])
def test_cli_processes_route_to_real_store_for_both_launch_environments(env, monkeypatch, transport):
    import subprocess
    import sys
    w = env
    item = w.queue.enqueue(project="SB", note="process round trip")
    w.config.set_codex_transport("SB", transport)
    child = w.workers._spawn_env(w.worker, queue="SB", engine="codex")
    source = str(Path(bridge.__file__).resolve().parent.parent)
    argv = [sys.executable, "-c", "import sys; sys.path.insert(0,sys.argv.pop(1)); from watchtower.cli import main; sys.exit(main())", source]
    stop = threading.Event()
    server = threading.Thread(target=bridge.serve, args=(stop,), daemon=True)
    server.start()
    try:
        result = subprocess.run(argv + ["claim", "-q", "SB", "--worker", w.worker, "--json"],
                                env=child, cwd=w.repo, capture_output=True, text=True, timeout=5)
        assert result.returncode == 0, result.stderr
        assert json.loads(result.stdout)["ref"] == item["ref"]
        result = subprocess.run(argv + ["close", item["ref"], "--worker", w.worker,
                                       "--summary", "Checked.", "--no-code"],
                                env=child, cwd=w.repo, capture_output=True, text=True, timeout=5)
        assert result.returncode == 0, result.stderr
        assert w.queue.get(item["ref"])["status"] == "closed"
        assert not (w.store / "queue.db").exists()
    finally:
        stop.set()
        server.join(5)


def test_commit_proof_never_executes_partial_clone_uploadpack(env):
    import subprocess
    w = env
    subprocess.run(["git", "init", "--bare", "-q", str(w.repo)], check=True)
    marker = w.tmp / "broker-escape"
    for key, value in (("extensions.partialClone", "origin"),
                       ("remote.origin.url", str(w.tmp / "missing-origin")),
                       ("remote.origin.uploadpack", "touch " + str(marker))):
        subprocess.run(["git", "-C", str(w.repo), "config", key, value], check=True)
    item = claim(w)
    body = close_body(item["ref"])
    body.update(commit="f" * 40, no_code=False)
    result = submit(w, "close", body, w.claim)
    assert not result["ok"] and "does not resolve" in result["error"]
    assert not marker.exists()
    assert w.queue.get(item["ref"])["status"] == "in_progress"

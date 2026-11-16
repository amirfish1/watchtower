"""WT-61 bridge hardening: crash recovery, bounded responses, hostile spool
entries, retirement, immutable restriction, scoped messaging and roots."""
import argparse
import json
import os
from pathlib import Path
import threading
import time
import uuid

import pytest

from watchtower import sandbox_bridge as bridge
from tests.test_sandbox_bridge import claim, close_body, env, submit  # noqa: F401 - fixture

SID = "11111111-2222-7333-8444-555555555555"


def scope_of(w):
    with bridge.directory(bridge.authority_dir()) as fd:
        return bridge._read(fd, w.worker + ".json")


def put(w, request, key=None):
    key = key or uuid.uuid4().hex
    with bridge.directory(w.store) as fd:
        bridge._write(fd, key + ".request", request)
    return key


def response(w, key):
    return json.loads((w.store / (key + ".response")).read_text())


# ------------------------------------------------------------ claim recovery
def test_crash_between_real_claim_and_scope_save_is_adopted(env, monkeypatch):
    w = env
    ticket = w.queue.enqueue(project="SB", note="work")
    original = bridge._write
    def crash(fd, name, value):
        if name == w.worker + ".json":
            raise SystemExit("daemon killed after the real claim")
        return original(fd, name, value)
    monkeypatch.setattr(bridge, "_write", crash)
    key = uuid.uuid4().hex
    with pytest.raises(SystemExit):
        submit(w, "claim", {}, key=key)
    monkeypatch.setattr(bridge, "_write", original)
    assert w.queue.get(ticket["ref"])["claimed_by"] == w.worker
    assert scope_of(w)["claim"] is None
    # The interrupted request stays indeterminate and is never replayed ...
    assert "indeterminate" in submit(w, "claim", {}, key=key)["error"]
    # ... while a fresh claim adopts the worker's own outstanding ticket.
    result = submit(w, "claim", {})
    assert result["ok"] and result["recovered"] and result["item"]["ref"] == ticket["ref"]
    assert len([e for e in w.queue.get(ticket["ref"])["history"] if e["event"] == "claim"]) == 1
    assert submit(w, "close", close_body(ticket["ref"]), result["claim"])["ok"]
    assert w.queue.get(ticket["ref"])["status"] == "closed"


def test_adoption_is_fenced_to_own_claim_queue_and_assignment(env):
    w = env
    other = w.queue.enqueue(project="SB", note="someone else's")
    w.queue.claim_by_ref(other["ref"], "another-worker")
    foreign = w.queue.enqueue(project="OTHER", note="other queue")
    w.queue.claim_by_ref(foreign["ref"], w.worker)
    assert "no current claim" in submit(w, "current", {})["error"]
    for ref in (other["ref"], foreign["ref"]):
        result = submit(w, "find", {"ref": ref})
        assert not result["ok"] and "another ticket" in result["error"]
    assert scope_of(w)["claim"] is None


def test_assigned_worker_cannot_adopt_a_different_ticket(env):
    w = env
    first = w.queue.enqueue(project="SB", note="first")
    second = w.queue.enqueue(project="SB", note="second")
    with bridge.directory(bridge.authority_dir()) as fd:
        scope = bridge._read(fd, w.worker + ".json")
        scope["assigned_ref"] = second["ref"]
        bridge._write(fd, w.worker + ".json", scope)
    w.queue.claim_by_ref(first["ref"], w.worker)
    assert not submit(w, "find", {"ref": first["ref"]})["ok"]


def test_find_is_a_recoverable_claim_read_with_fresh_fence(env):
    w = env
    item = claim(w)
    with w.queue._FileLock(w.queue._lock_path()):
        data = w.queue._load_unlocked()
        it = next(i for i in data["items"] if i["ref"] == item["ref"])
        it["last_answer_gen"] = 7
        w.queue._save_unlocked(data)
    assert "stale" in submit(w, "close", close_body(item["ref"]), w.claim)["error"]
    fresh = submit(w, "find", {"ref": item["ref"]}, w.claim)
    assert fresh["ok"] and fresh["claim"]["fence"]["last_answer_gen"] == 7
    assert fresh["claim"] != w.claim
    # The old fence stays dead; only the re-read one acts.
    assert not submit(w, "close", close_body(item["ref"]), w.claim)["ok"]
    assert submit(w, "close", close_body(item["ref"]), fresh["claim"])["ok"]


def test_second_claim_names_the_current_ticket(env):
    w = env
    item = claim(w)
    w.queue.enqueue(project="SB", note="next")
    result = submit(w, "claim", {})
    assert not result["ok"] and item["ref"] in result["error"]


def test_client_recovers_lost_claim_file_and_consumes_responses(env, monkeypatch):
    w = env
    ticket = w.queue.enqueue(project="SB", note="round trip")
    monkeypatch.setenv("WT_SANDBOX_BRIDGE", str(w.store))
    stop = threading.Event()
    server = threading.Thread(target=bridge.serve, args=(stop,), daemon=True)
    server.start()
    try:
        assert bridge.request("claim", {}, timeout=3)["item"]["ref"] == ticket["ref"]
        (w.store / "claim.json").unlink()
        result = bridge.request("close", close_body(ticket["ref"]), timeout=3)
        assert result["ok"] and result["item"]["status"] == "closed"
        assert not list(w.store.glob("*.response"))
    finally:
        stop.set()
        server.join(3)


# ------------------------------------------------------- oversized responses
def test_oversized_item_is_projected_and_fully_pageable(env):
    w = env
    note = "".join(f"line {i:06d} é\n" for i in range(40000))      # ~560 KiB
    ticket = w.queue.enqueue(project="SB", note="small", repo_path=str(w.repo))
    with w.queue._FileLock(w.queue._lock_path()):      # enqueue clips; stores can still hold more
        data = w.queue._load_unlocked()
        next(i for i in data["items"] if i["ref"] == ticket["ref"])["note"] = note
        w.queue._save_unlocked(data)
    key = put(w, {"op": "claim", "body": {}, "claim": None})
    bridge.process(w.worker)
    raw = (w.store / (key + ".response")).read_bytes()
    assert len(raw) <= bridge.RESULT_MAX
    result = json.loads(raw)
    assert result["ok"] and result["claim"]["ref"] == ticket["ref"]
    marker = result["item"]["_bridge_projection"]
    assert marker["truncated"] and any(c["path"] == "note" for c in marker["cut"])
    assert len(result["item"]["note"]) < len(note)
    assert scope_of(w)["claim"] == result["claim"]
    pages, text, index = 1, "", 0
    while index < pages:
        page = submit(w, "find", {"ref": ticket["ref"], "field": "note", "page": index}, result["claim"])["page"]
        pages, text, index = page["pages"], text + page["text"], index + 1
    assert text == note and pages > 1
    assert "out of range" in submit(w, "find", {"ref": ticket["ref"], "field": "note", "page": pages}, result["claim"])["error"]
    assert "no such field" in submit(w, "find", {"ref": ticket["ref"], "field": "nope"}, result["claim"])["error"]
    history = submit(w, "find", {"ref": ticket["ref"], "field": "history"}, result["claim"])["page"]
    assert history["kind"] == "list" and history["entries"]
    assert submit(w, "close", close_body(ticket["ref"]), result["claim"])["ok"]


def test_projection_always_fits_even_for_pathological_shapes():
    wide = {"ref": "SB-1", "status": "in_progress", **{f"k{i}": "x" * 300 for i in range(2000)}}
    deep = {"ref": "SB-2", "history": [{"text": "y" * 30000} for _ in range(500)]}
    for item in (wide, deep):
        out = bridge._project(item)
        assert bridge._size(out) <= bridge.ITEM_MAX and out["_bridge_projection"]["truncated"]
        assert out["ref"] == item["ref"]
    assert bridge._bounded({"ok": True, "item": "z" * 200000, "claim": {"ref": "SB-1"}}) == {
        "ok": True, "claim": {"ref": "SB-1"},
        "error": "bridge result exceeded its size limit; page it with `wt find <ref> --field <path>`"}


# ---------------------------------------------------------- hostile spool
def test_directory_and_deep_json_entries_do_not_stop_the_batch(env):
    w = env
    w.queue.enqueue(project="SB", note="real work")
    nested = w.store / ("0" * 32 + ".request")
    (nested / "a" / "b" / "c").mkdir(parents=True)
    (nested / "a" / "b" / "c" / "file").write_text("x")
    (w.store / ("1" * 32 + ".request")).write_text("[" * 60000)
    (w.store / ("2" * 32 + ".request")).write_text("not json")
    good = "f" * 32
    (w.store / (good + ".response")).mkdir()
    ((w.store / (good + ".response")) / "inner").mkdir()
    put(w, {"op": "claim", "body": {}, "claim": None}, good)
    bridge.process(w.worker)
    assert response(w, good)["ok"]
    for _ in range(6):
        bridge.process(w.worker)
    leftovers = [p.name for p in w.store.iterdir() if p.name != good + ".response"]
    assert leftovers == [], leftovers


def test_many_undeletable_looking_entries_cannot_wedge_the_worker(env):
    w = env
    w.queue.enqueue(project="SB", note="real work")
    for i in range(bridge.SPOOL_BATCH + 8):
        d = w.store / (f"{i:032x}.request")
        d.mkdir()
        (d / "keep").mkdir()
        (d / "keep" / "x").write_text("x")
    good = "f" * 32
    put(w, {"op": "claim", "body": {}, "claim": None}, good)
    for _ in range(3):
        bridge.process(w.worker)
        if (w.store / (good + ".response")).exists():
            break
    assert response(w, good)["ok"]
    assert not list(w.store.glob("*.request"))


def test_stale_responses_and_temporaries_are_retired(env):
    w = env
    old, fresh = "a" * 32 + ".response", "b" * 32 + ".response"
    for name in (old, fresh, ".bridge-" + "c" * 32):
        (w.store / name).write_text("{}")
    past = time.time() - bridge.RESPONSE_RETAIN_S - 5
    os.utime(w.store / old, (past, past))
    os.utime(w.store / (".bridge-" + "c" * 32), (past, past))
    bridge.process(w.worker)
    assert sorted(p.name for p in w.store.iterdir()) == [fresh]


# ------------------------------------------------ retirement and tombstones
def test_dead_worker_ledger_retired_in_bounded_steps_tombstones_kept(env, monkeypatch):
    w = env
    item = claim(w)
    for _ in range(5):
        submit(w, "find", {"ref": item["ref"]}, w.claim)
    authority = bridge.authority_dir()
    receipts = lambda: list(authority.glob("*.receipt"))
    assert len(receipts()) == 6
    assert bridge.retire() == 0                                   # live: untouched
    monkeypatch.setattr(w.workers, "list_workers", lambda **kw: [])
    assert bridge.retire() == 0                                   # dead but recent
    later = time.time() + bridge.LEDGER_RETAIN_S + 60
    assert bridge.retire(now=later, budget=4) == 4                # bounded work
    assert (authority / (w.worker + ".json")).exists()            # receipts first
    assert bridge.retire(now=later) >= 3
    assert not receipts() and not (authority / (w.worker + ".json")).exists()
    assert not (authority / (w.worker + ".lock")).exists()
    assert list((authority / "tombstones").iterdir())
    assert bridge.restricted_session("", w.worker)


def test_restriction_is_immutable_across_pruning_and_policy_change(env, monkeypatch):
    w = env
    w.workers.record_worker(os.getpid(), "SB", "codex", w.worker, str(w.repo),
                            str(w.tmp / "worker.log"), session_id=SID)
    bridge.process(w.worker)            # binds the session from the trusted record
    monkeypatch.setattr(bridge, "_BOUND", set())                  # a different process
    w.config.set_codex_sandbox("SB", "full-access")
    monkeypatch.setattr(w.workers, "list_workers", lambda **kw: [])
    assert bridge.restricted_session(SID)
    assert bridge.restricted_session("", w.worker)
    assert bridge.restricted_session(SID, "some-other-id")
    assert not bridge.restricted_session("99999999-2222-7333-8444-555555555555", "unrelated")
    assert not bridge.bind_session("never-registered", "aaaaaaaa-2222-7333-8444-555555555555")
    assert not bridge.restricted_session("aaaaaaaa-2222-7333-8444-555555555555")


def test_session_is_bound_even_when_policy_was_already_disabled(env):
    w = env
    w.workers.record_worker(os.getpid(), "SB", "codex", w.worker, str(w.repo),
                            str(w.tmp / "worker.log"), session_id=SID)
    w.config.set_codex_sandbox("SB", "full-access")
    with pytest.raises(ValueError, match="disabled"):
        bridge.process(w.worker)
    assert bridge._tombstoned("s", SID)


# ---------------------------------------------------------------- messaging
def _destination(monkeypatch, value):
    from watchtower import messages
    monkeypatch.setattr(messages, "resolve_target", lambda target: json.loads(json.dumps(value)))
    for unsafe in ("ask", "send", "_deliver_resume", "_deliver_delegate", "outbox_add"):
        monkeypatch.setattr(messages, unsafe, lambda *a, **k: pytest.fail("unsandboxed or queued delivery"))
    return messages


def test_failed_delivery_returns_minimal_error_without_host_paths(env, monkeypatch):
    w = env
    messages = _destination(monkeypatch, {"session_id": SID, "engine": "claude"})
    monkeypatch.setattr(messages, "deliver", lambda *a, **k: {
        "ok": False, "error": "uds: no socket /home/private/.claude/sessions/x.sock", "log": "/home/private/log"})
    claim(w, submitter="@filer")
    for op in ("ask", "tell", "send"):
        result = submit(w, op, {"target": "@filer", "text": "hello"}, w.claim)
        assert set(result) == {"ok", "delivered", "error"} and not result["ok"]
        assert "/home" not in json.dumps(result)


def test_message_schema_drops_paths_and_unknown_fields(env, monkeypatch):
    w = env
    messages = _destination(monkeypatch, {"session_id": SID, "engine": "claude"})
    monkeypatch.setattr(messages, "deliver", lambda *a, **k: {
        "ok": True, "transport": "/home/private/fifo path", "receipt_id": "r-1",
        "log": "/home/private/log", "attr": {"secret": 1}, "new_session_id": SID})
    claim(w, submitter="@filer")
    assert submit(w, "tell", {"target": "@filer", "text": "hello"}, w.claim) == {
        "ok": True, "delivered": True, "receipt_id": "r-1"}


def test_destination_must_match_the_pinned_worker_exactly(env, monkeypatch):
    w = env
    value = {"session_id": SID, "engine": "claude", "worker": {"worker_id": "filer-w1"}}
    messages = _destination(monkeypatch, value)
    sent = []
    monkeypatch.setattr(messages, "deliver", lambda resolved, *a, **k: sent.append(resolved) or {"ok": True, "transport": "fifo"})
    claim(w, submitter="@filer")
    assert scope_of(w)["destination"] == {"session_id": SID, "engine": "claude", "worker_id": "filer-w1"}
    assert submit(w, "tell", {"target": "@filer", "text": "one"}, w.claim)["ok"]
    assert sent[0]["worker"]["worker_id"] == "filer-w1"
    value["worker"]["worker_id"] = "impostor"
    assert "destination changed" in submit(w, "tell", {"target": "@filer", "text": "two"}, w.claim)["error"]
    value["worker"]["worker_id"] = "filer-w1"
    value["engine"] = "codex"
    assert "destination changed" in submit(w, "tell", {"target": "@filer", "text": "three"}, w.claim)["error"]
    assert len(sent) == 1


def test_ask_waits_only_on_a_live_fifo_reply(env, monkeypatch):
    w = env
    log = w.tmp / "filer.log"
    log.write_text("")
    messages = _destination(monkeypatch, {"session_id": SID, "engine": "claude", "worker": {
        "worker_id": "filer-w1", "fifo": str(w.tmp / "fifo"), "log": str(log)}})
    monkeypatch.setattr(messages, "deliver", lambda *a, **k: {"ok": True, "transport": "fifo"})
    monkeypatch.setattr(messages, "_await_reply", lambda path, offset, deadline, source: {
        "ok": True, "answer": "the reply", "source": source, "log": path})
    claim(w, submitter="@filer")
    assert submit(w, "ask", {"target": "@filer", "text": "question"}, w.claim) == {
        "ok": True, "delivered": True, "transport": "fifo", "answer": "the reply"}


def test_message_budget_survives_a_fence_refresh(env, monkeypatch):
    w = env
    messages = _destination(monkeypatch, {"session_id": SID, "engine": "claude"})
    monkeypatch.setattr(messages, "deliver", lambda *a, **k: {"ok": True, "transport": "uds"})
    item = claim(w, submitter="@filer")
    current = w.claim
    for i in range(bridge.MESSAGE_BUDGET):
        assert submit(w, "tell", {"target": "@filer", "text": f"m{i}"}, current)["ok"]
        if i == 3:
            with w.queue._FileLock(w.queue._lock_path()):
                data = w.queue._load_unlocked()
                next(x for x in data["items"] if x["ref"] == item["ref"])["last_answer_gen"] = 2
                w.queue._save_unlocked(data)
            current = submit(w, "find", {"ref": item["ref"]})["claim"]
    assert "budget" in submit(w, "tell", {"target": "@filer", "text": "over"}, current)["error"]


# ----------------------------------------------------------- block / comment
def test_fenced_comment_and_block(env):
    w = env
    item = claim(w)
    other = w.queue.enqueue(project="SB", note="other")
    assert not submit(w, "comment", {"ref": other["ref"], "text": "x"}, w.claim)["ok"]
    assert not submit(w, "block", {"ref": other["ref"], "question": "x"}, w.claim)["ok"]
    assert submit(w, "comment", {"ref": item["ref"], "text": "halfway there"}, w.claim)["ok"]
    event = [e for e in w.queue.get(item["ref"])["history"] if e["event"] == "comment"][-1]
    assert event["text"] == "halfway there" and event["by"]["kind"] == "worker"
    for extra in ({"kind": "awaiting-client"}, {"force": True}, {"commit": "f" * 40},
                  {"wait_on": "owner"}, {"origin": "system"}):
        result = submit(w, "block", {"ref": item["ref"], "question": "Which API?", **extra}, w.claim)
        assert not result["ok"] and "fields" in result["error"]
    assert w.queue.get(item["ref"])["status"] == "in_progress"
    stale = dict(w.claim, fence=dict(w.claim["fence"], last_answer_gen=5))
    assert not submit(w, "block", {"ref": item["ref"], "question": "Which API?"}, stale)["ok"]
    result = submit(w, "block", {"ref": item["ref"], "question": "Which API?", "progress": "read the spec"}, w.claim)
    assert result["ok"], result
    blocked = w.queue.get(item["ref"])
    assert blocked["needs_input"] and blocked["block_question"] == "Which API?"
    assert blocked["block_kind"] == "input" and blocked["block_origin"] == "worker"
    # The blocked claim is no longer actionable or adoptable ...
    assert not submit(w, "comment", {"ref": item["ref"], "text": "late"}, w.claim)["ok"]
    assert not submit(w, "close", close_body(item["ref"]), w.claim)["ok"]
    # ... and the worker may take the next ticket.
    assert submit(w, "claim", {})["item"]["ref"] == other["ref"]


def test_comment_budget(env, monkeypatch):
    w = env
    monkeypatch.setattr(bridge, "COMMENT_BUDGET", 2)
    item = claim(w)
    for _ in range(2):
        assert submit(w, "comment", {"ref": item["ref"], "text": "progress"}, w.claim)["ok"]
    assert "budget" in submit(w, "comment", {"ref": item["ref"], "text": "progress"}, w.claim)["error"]


def test_cli_adapters_refuse_authority_flags(env, monkeypatch, capsys):
    w = env
    sent = []
    monkeypatch.setattr(bridge, "request", lambda op, body, **k: sent.append((op, body)) or {
        "ok": True, "item": {"ref": "SB-1", "block_question": body.get("question")}})
    monkeypatch.setenv("WT_WORKER_ID", w.worker)
    base = dict(command="block", ref="SB-1", worker=w.worker, question="Which?", progress="", kind="input",
                commit="", force=False, json=False)
    assert bridge.cli(argparse.Namespace(**base)) == 0
    assert sent == [("block", {"ref": "SB-1", "question": "Which?"})]
    for bad in ({"force": True}, {"kind": "rationale"}, {"commit": "abc"}, {"wait_on": "owner"},
                {"weight": "major"}, {"worker": "someone-else"}, {"confidence": 0.9}):
        assert bridge.cli(argparse.Namespace(**{**base, **bad})) == 1
    comment = dict(command="comment", ref="SB-1", text="note", worker="", by="worker")
    assert bridge.cli(argparse.Namespace(**comment)) == 0
    assert bridge.cli(argparse.Namespace(**{**comment, "by": "human"})) == 1
    assert bridge.cli(argparse.Namespace(command="accept", ref="SB-1")) == 1
    assert [op for op, _ in sent] == ["block", "comment"]
    assert "COMMENTED: SB-1" in capsys.readouterr().out


# ------------------------------------------------------------ claim parity
def test_idle_claim_applies_surplus_stop_and_requested_only(env):
    w = env
    assert submit(w, "claim", {"requested_only": "yes"})["error"].startswith("invalid")
    ticket = w.queue.enqueue(project="SB", note="not run-requested")
    result = submit(w, "claim", {"requested_only": True})
    # Auto-drain is off and nothing was run-requested: the manual run is over.
    assert result == {"ok": True, "item": {"stop": True}}
    assert w.queue.get(ticket["ref"])["status"] == "open"
    row = next(r for r in w.workers.list_workers(prune=False) if r["worker_id"] == w.worker)
    assert row.get("released_at")


def test_reconciler_stop_signal_wins_over_open_work(env):
    w = env
    ticket = w.queue.enqueue(project="SB", note="open")
    w.workers.STOP_SIGNALS_DIR.mkdir(parents=True, exist_ok=True)
    (w.workers.STOP_SIGNALS_DIR / w.worker).write_text("")
    assert submit(w, "claim", {}) == {"ok": True, "item": {"stop": True}}
    assert w.queue.get(ticket["ref"])["status"] == "open"


# -------------------------------------------------------------------- roots
def test_protected_roots_refuse_overlap_in_both_directions(env, monkeypatch):
    w = env
    codex_home = w.tmp / "codex-home"
    (codex_home / "inside").mkdir(parents=True)
    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    ok_repo = w.tmp / "repo2"
    ok_repo.mkdir()
    cases = [
        (str(w.tmp / "isolated"), str(codex_home / "inside")),             # workspace inside auth tree
        (str(codex_home / "stores"), str(ok_repo)),                         # store inside auth tree
        (str(bridge.authority_dir() / "x"), str(ok_repo)),                  # store inside authority
        (str(w.workers.WORKERS_FILE.parent / "codex-app"), str(ok_repo)),   # runtime sockets
        (str(w.workers.STOP_SIGNALS_DIR / "s"), str(ok_repo)),              # control signals
        (str(Path(bridge.__file__).parent / "s"), str(ok_repo)),            # broker code
        (str(w.tmp / "isolated"), str(w.tmp)),                              # workspace contains control files
        (str(ok_repo / "store"), str(ok_repo)),                             # nested roots
    ]
    for index, (store, repo) in enumerate(cases):
        w.config.set_codex_sandbox("SB", "workspace-write", store)
        with pytest.raises(ValueError, match="trusted control|separate roots"):
            bridge.register("SB", f"bad-{index}", repo)
        assert not (Path(store) / f"bad-{index}").exists()                 # refused before any write
        assert not bridge._tombstoned("w", f"bad-{index}")
    w.config.set_codex_sandbox("SB", "workspace-write", str(w.tmp / "isolated"))
    assert bridge.register("SB", "good", str(ok_repo)).is_dir()


def test_symlinked_protected_tree_is_still_refused(env, monkeypatch):
    w = env
    codex_home = w.tmp / "codex-home"
    codex_home.mkdir()
    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    (w.tmp / "alias").symlink_to(codex_home, target_is_directory=True)
    (codex_home / "repo").mkdir()
    with pytest.raises(ValueError, match="trusted control"):
        bridge.register("SB", "via-link", str(w.tmp / "alias" / "repo"))


def test_trusted_system_prefix_links_resolve_but_worker_links_never_do(env, monkeypatch):
    w = env
    real = w.tmp / "private-tmp"
    (real / "store" / "deep").mkdir(parents=True)
    (w.tmp / "systmp").symlink_to(real, target_is_directory=True)
    (real / "store" / "escape").symlink_to(w.tmp, target_is_directory=True)
    # Unprivileged links are never trusted, whoever made them.
    assert not bridge._trusted_link(str(w.tmp), "systmp")
    with pytest.raises(OSError):
        with bridge.directory(w.tmp / "systmp" / "store"):
            pass
    # Model a root-owned system prefix link (macOS /tmp -> private/tmp).
    monkeypatch.setattr(bridge, "_trusted_link",
                        lambda parent, name: (parent, name) == (str(w.tmp), "systmp"))
    assert bridge.canonical(w.tmp / "systmp" / "store" / "deep") == real / "store" / "deep"
    with bridge.directory(w.tmp / "systmp" / "store" / "deep") as fd:
        assert os.fstat(fd).st_ino == (real / "store" / "deep").stat().st_ino
    # The attacker-controlled suffix is still walked without following.
    with pytest.raises(OSError):
        with bridge.directory(w.tmp / "systmp" / "store" / "escape"):
            pass
    with pytest.raises(ValueError, match="parent references"):
        bridge.canonical(str(w.tmp / "systmp" / "store") + "/../../..")


def test_commit_proof_refuses_symlinked_git_metadata(env):
    import subprocess
    w = env
    victim = w.tmp / "victim"
    subprocess.run(["git", "init", "-q", str(victim)], check=True)
    subprocess.run(["git", "-C", str(victim), "-c", "user.name=t", "-c", "user.email=t@example.invalid",
                    "commit", "-q", "--allow-empty", "-m", "private"], check=True)
    sha = subprocess.run(["git", "-C", str(victim), "rev-parse", "HEAD"],
                         check=True, capture_output=True, text=True).stdout.strip()
    (w.repo / ".git").symlink_to(victim / ".git", target_is_directory=True)
    item = claim(w)
    body = close_body(item["ref"])
    body.update(commit=sha, no_code=False)
    result = submit(w, "close", body, w.claim)
    assert not result["ok"] and "does not resolve" in result["error"]
    assert w.queue.get(item["ref"])["status"] == "in_progress"

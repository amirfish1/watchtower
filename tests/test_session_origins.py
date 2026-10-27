"""WT-27: durable worker-spawn provenance ledger."""
import json

from watchtower import origins, workers


def test_record_keeps_first_attribution_and_ignores_blank():
    sid = "11111111-2222-3333-4444-555555555555"
    origins.record(sid, role="verifier", ref="Q-1", queue="Q")
    origins.record(sid, role="probe", ref="", parent_session_id="aaaaaaaa-1")
    row = origins.get(sid)
    assert row["role"] == "verifier" and row["ref"] == "Q-1"
    assert row["parent_session_id"] == "aaaaaaaa-1"


def test_record_rejects_bad_ids_and_never_raises(tmp_path, monkeypatch):
    assert origins.record("", role="x") is None
    assert origins.record("has space!", role="x") is None
    monkeypatch.setattr(origins, "ORIGINS_FILE", tmp_path / "f" / "o.json")
    (tmp_path / "f").write_text("not a dir")
    assert origins.record("11111111-2222", role="x") is None


def test_env_parent_empty_for_human_shell(monkeypatch):
    assert origins.env_parent() == {}


def test_spawn_env_markers_and_stale_clearing(monkeypatch):
    monkeypatch.setenv("WT_TICKET_REF", "STALE-9")
    env = workers._spawn_env("w-1", session_id="s-1234567", ref="Q-7",
                             queue="Q", role="verifier")
    assert env["WT_SESSION_ID"] == "s-1234567" and env["WT_TICKET_REF"] == "Q-7"
    assert env["WT_ROLE"] == "verifier" and env["WT_TICKET_QUEUE"] == "Q"
    plain = workers._spawn_env("w-2")
    assert "WT_TICKET_REF" not in plain and "WT_ROLE" not in plain


def test_direct_probe_inside_a_worker_is_attributed(monkeypatch):
    monkeypatch.setattr(workers.shutil, "which", lambda b: "/bin/" + b)
    monkeypatch.setenv("WT_WORKER_ID", "planner-claude-abcd1234")
    monkeypatch.setenv("WT_SESSION_ID", "99999999-aaaa-bbbb-cccc-dddddddddddd")
    monkeypatch.setenv("WT_TICKET_REF", "WT-24")
    argv = workers.build_adhoc_command("claude", "sleep 40")
    sid = argv[argv.index("--session-id") + 1]
    row = origins.get(sid)
    assert row["role"] == "probe" and row["ref"] == "WT-24"
    assert row["parent_worker_id"] == "planner-claude-abcd1234"
    assert row["parent_session_id"] == "99999999-aaaa-bbbb-cccc-dddddddddddd"


def test_human_shell_build_adhoc_command_unchanged(monkeypatch):
    monkeypatch.setattr(workers.shutil, "which", lambda b: "/bin/" + b)
    argv = workers.build_adhoc_command("claude", "hi")
    assert "--session-id" not in argv and origins.load() == {}


def test_spawn_adhoc_stage_records_origin_and_survives_prune(wt_env, monkeypatch):
    from watchtower import workers as w
    monkeypatch.setattr(w.shutil, "which", lambda b: "/bin/" + b)
    monkeypatch.setattr(w.subprocess, "Popen", lambda *a, **k: type("P", (), {"pid": 1})())
    monkeypatch.setattr(w, "_spawn_gate", lambda e: None, raising=False)
    monkeypatch.setattr(w, "_wait_for_immediate_launch_failure", lambda *a, **k: None)
    rec = w.spawn_adhoc("go", "claude", repo_path=str(wt_env), name="verify-q-1",
                        stage="verifier", ticket_ref="Q-1", ticket_queue="Q")
    row = origins.get(rec["session_id"])
    assert row["role"] == "verifier" and row["ref"] == "Q-1" and row["queue"] == "Q"
    assert row["worker_id"] == rec["worker_id"]
    # the ledger is independent of workers.json pruning
    w._save({"workers": []})
    assert origins.get(rec["session_id"])["role"] == "verifier"


def test_spawn_adhoc_from_a_user_session_records_it_as_parent(wt_env, monkeypatch):
    # CCC-231: a reviewer a user's Codex session starts with `wt spawn` must
    # name that session as its parent, or CCC can't nest it under the idea.
    from watchtower import workers as w
    monkeypatch.setattr(w.shutil, "which", lambda b: "/bin/" + b)
    monkeypatch.setattr(w.subprocess, "Popen", lambda *a, **k: type("P", (), {"pid": 1})())
    monkeypatch.setenv("CODEX_THREAD_ID", "01a17f55-d65b-76f2-b034-9a81217f303a")
    rec = w.spawn_adhoc("review", "claude", repo_path=str(wt_env), name="rereview")
    row = origins.get(rec["session_id"])
    assert row["role"] == "adhoc"
    assert row["parent_session_id"] == "01a17f55-d65b-76f2-b034-9a81217f303a"
    assert not row.get("parent_worker_id")
    assert rec["parent_session_id"] == row["parent_session_id"]


def test_caller_session_id_prefers_worker_session(monkeypatch):
    assert origins.caller_session_id({}) == ""
    assert origins.caller_session_id({"CODEX_THREAD_ID": "c"}) == "c"
    assert origins.caller_session_id({"WT_SESSION_ID": "w", "CODEX_THREAD_ID": "c"}) == "w"

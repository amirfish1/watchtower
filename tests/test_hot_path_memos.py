"""WT-113: per-item helpers on the daemon's hot path canonicalize once."""
from watchtower import queue as q


def test_matches_canonicalizes_each_ident_once(monkeypatch):
    calls = []
    real = q.canonical_ref
    monkeypatch.setattr(q, "_MATCH_KEYS", {})
    monkeypatch.setattr(q, "canonical_ref", lambda v: calls.append(v) or real(v))
    items = [{"number": n, "ref": f"WT-{n}"} for n in range(1, 200)]
    hits = [it for it in items if q._matches(it, "wt-7:old-name")]
    assert [it["ref"] for it in hits] == ["WT-7"]
    assert [it["ref"] for it in items if q._matches(it, 9)] == ["WT-9"]
    assert [it["ref"] for it in items if q._matches(it, "9")] == ["WT-9"]
    assert calls == ["wt-7:old-name", 9, "9"]


def test_norm_project_memo_keeps_results_and_ignores_non_strings(monkeypatch):
    monkeypatch.setattr(q, "_NORM_PROJECT_MEMO", {})
    assert q._norm_project(" wt ") == "WT"
    assert q._norm_project(" wt ") == "WT"
    assert q._norm_project("-a_b-") == "A_B"
    assert q._norm_project(None) == ""
    assert set(q._NORM_PROJECT_MEMO) == {" wt ", "-a_b-"}


def _aged(path, seconds=60):
    import os
    import time
    t = time.time() - seconds
    os.utime(path, (t, t))


def test_find_transcript_lists_project_dirs_once_until_one_is_added(tmp_path, monkeypatch):
    from pathlib import Path
    from watchtower import messages

    root = tmp_path / "projects"
    for name in ("a", "b"):
        (root / name).mkdir(parents=True)
    (root / "b" / "s1.jsonl").write_text("{}\n")
    _aged(root)
    monkeypatch.setenv("WATCHTOWER_CLAUDE_PROJECTS_DIR", str(root))
    monkeypatch.setattr(messages, "_PROJECT_DIRS_MEMO", {})
    listings = []
    real_iterdir = Path.iterdir
    monkeypatch.setattr(Path, "iterdir",
                        lambda self: listings.append(self) or real_iterdir(self))
    assert messages._find_transcript("s1") == root / "b" / "s1.jsonl"
    assert messages._find_transcript("s1") == root / "b" / "s1.jsonl"
    assert messages._find_transcript("missing") is None
    assert listings == [root]
    # A new project dir moves the root's mtime: the next lookup re-lists.
    (root / "c").mkdir()
    (root / "c" / "s2.jsonl").write_text("{}\n")
    _aged(root, 30)
    assert messages._find_transcript("s2") == root / "c" / "s2.jsonl"
    assert listings == [root, root]
    # A deleted transcript is not served from the hit cache.
    (root / "b" / "s1.jsonl").unlink()
    assert messages._find_transcript("s1") is None


def test_find_transcript_does_not_memoize_a_just_changed_root(tmp_path, monkeypatch):
    from watchtower import messages

    root = tmp_path / "projects"
    (root / "a").mkdir(parents=True)
    monkeypatch.setenv("WATCHTOWER_CLAUDE_PROJECTS_DIR", str(root))
    monkeypatch.setattr(messages, "_PROJECT_DIRS_MEMO", {})
    assert messages._find_transcript("s1") is None
    assert messages._PROJECT_DIRS_MEMO == {}


# ------------------------------------------------------------------ WT-122


def test_find_transcript_miss_ttl_only_for_callers_that_ask(tmp_path, monkeypatch):
    from watchtower import messages

    root = tmp_path / "projects"
    (root / "a").mkdir(parents=True)
    _aged(root)
    monkeypatch.setenv("WATCHTOWER_CLAUDE_PROJECTS_DIR", str(root))
    monkeypatch.setattr(messages, "_PROJECT_DIRS_MEMO", {})
    monkeypatch.setattr(messages, "_TRANSCRIPT_MISSES", {})
    assert messages._find_transcript("s1", miss_ttl_s=300) is None
    (root / "a" / "s1.jsonl").write_text("{}\n")
    # Within the TTL a tolerant caller skips the rescan; everyone else finds it.
    assert messages._find_transcript("s1", miss_ttl_s=300) is None
    assert messages._find_transcript("s1") == root / "a" / "s1.jsonl"
    assert messages._find_transcript("s1", miss_ttl_s=300) == root / "a" / "s1.jsonl"


def test_latest_session_title_rescans_only_when_transcript_changes(tmp_path, monkeypatch):
    from watchtower import messages

    p = tmp_path / "s.jsonl"
    p.write_text('{"type": "custom-title", "customTitle": "one"}\n')
    monkeypatch.setattr(messages, "_TITLE_MEMO", {})
    scans = []
    real = messages._scan_session_title
    monkeypatch.setattr(messages, "_scan_session_title",
                        lambda path: scans.append(path) or real(path))
    assert messages._latest_session_title(p) == "one"
    assert messages._latest_session_title(p) == "one"
    assert len(scans) == 1
    with open(p, "a") as f:
        f.write('{"type": "custom-title", "customTitle": "two"}\n')
    assert messages._latest_session_title(p) == "two"
    assert len(scans) == 2


def test_config_stat_ttl_reuses_stat_and_own_save_invalidates(tmp_path, monkeypatch):
    import json
    from watchtower import config

    cfg = tmp_path / "queue-config.json"
    cfg.write_text(json.dumps({"AA": {"desired_workers": 1}}))
    monkeypatch.setenv("WATCHTOWER_CONFIG_FILE", str(cfg))
    monkeypatch.setattr(config, "CONFIG_FILE", cfg)
    monkeypatch.setattr(config, "_STAT_MEMO", {"at": float("-inf"), "value": None})
    monkeypatch.setattr(config, "_LOAD_MEMO", {"sig": None, "data": {}})
    monkeypatch.setattr(config, "STAT_TTL_S", 0.0)
    assert config._queue_entry("AA") == {"desired_workers": 1}
    cfg.write_text(json.dumps({"AA": {"desired_workers": 22}}))
    assert config._queue_entry("AA") == {"desired_workers": 22}  # default: no TTL
    monkeypatch.setattr(config, "STAT_TTL_S", 3600.0)
    assert config._queue_entry("AA") == {"desired_workers": 22}
    stats = []
    real = config._config_stat_uncached
    monkeypatch.setattr(config, "_config_stat_uncached", lambda: stats.append(1) or real())
    for _ in range(5):
        config._queue_entry("AA")
    assert stats == []
    data = config._load()
    data["AA"]["desired_workers"] = 3
    config._save(data)
    assert config._queue_entry("AA") == {"desired_workers": 3}


def test_pipeline_sidecar_stat_ttl_and_own_write_invalidates(tmp_path, monkeypatch):
    from watchtower import github_pipeline as gp

    side = tmp_path / "github-pipeline.json"
    monkeypatch.setattr(gp, "path", lambda: side)
    monkeypatch.setattr(gp, "_CACHE", {"sig": None, "data": None})
    gp._write({"version": 1, "items": {"r#1": {"gen": 1}}})
    monkeypatch.setattr(gp, "STAT_TTL_S", 3600.0)
    assert gp._read_cached()["items"]["r#1"]["gen"] == 1
    stats = []
    real = gp._read_cached_stat
    monkeypatch.setattr(gp, "_read_cached_stat", lambda: stats.append(1) or real())
    assert gp._read_cached()["items"]["r#1"]["gen"] == 1
    assert stats == []
    gp._write({"version": 1, "items": {"r#1": {"gen": 2}}})
    assert gp._read_cached()["items"]["r#1"]["gen"] == 2
    monkeypatch.setattr(gp, "STAT_TTL_S", 0.0)
    gp._read_cached()
    assert len(stats) == 2


def test_connectivity_state_is_reread_only_when_the_file_changes(tmp_path, monkeypatch):
    import json
    import os
    import time
    from pathlib import Path
    from watchtower import github_backend as gb

    f = tmp_path / "conn.json"
    f.write_text(json.dumps({"next_retry_at": None, "n": 1}))
    old = time.time() - 60
    os.utime(f, (old, old))
    monkeypatch.setenv("WATCHTOWER_GH_CONNECTIVITY_FILE", str(f))
    monkeypatch.setattr(gb, "_CONNECTIVITY_MEMO", {"sig": None, "text": ""})
    reads = []
    real = Path.read_text
    monkeypatch.setattr(Path, "read_text", lambda self, *a, **k: reads.append(self) or real(self, *a, **k))
    a = gb._load_connectivity()
    a["n"] = 99  # callers get private dicts
    assert gb._load_connectivity()["n"] == 1
    assert len(reads) == 1
    f.write_text(json.dumps({"next_retry_at": None, "n": 2}))  # fresh mtime: not memoized
    assert gb._load_connectivity()["n"] == 2
    assert gb._load_connectivity()["n"] == 2
    assert len(reads) == 3


def test_spawn_defaults_reparsed_only_when_file_changes(tmp_path, monkeypatch):
    import json
    import os
    from watchtower import config
    spawn = tmp_path / "spawn.json"
    spawn.write_text(json.dumps({"worker_engine": "kimi"}))
    old = 1_700_000_000
    os.utime(spawn, (old, old))
    monkeypatch.setattr(config, "CCC_SPAWN_DEFAULTS_FILE", spawn)
    monkeypatch.setattr(config, "_JSON_FILE_MEMO", {})
    loads = []
    real_load = json.load
    monkeypatch.setattr(config.json, "load", lambda f: loads.append(1) or real_load(f))
    assert config._ccc_worker_engine_default() == "kimi"
    assert config._ccc_worker_engine_default() == "kimi"
    assert config._ccc_worker_effort_default() == ""
    assert len(loads) == 1
    # Mutating the returned dict must not poison the memo.
    config._ccc_spawn_defaults()["worker_engine"] = "x"
    assert config._ccc_worker_engine_default() == "kimi"
    # A fresh write (same size, new content) is seen immediately.
    spawn.write_text(json.dumps({"worker_engine": "codx"}))
    assert config._ccc_worker_engine_default() == "codx"
    spawn.unlink()
    assert config._ccc_worker_engine_default() == ""


def test_observation_path_lookup_ttl(tmp_path, monkeypatch):
    from watchtower import observations
    root = tmp_path / "projects"
    (root / "-repo").mkdir(parents=True)
    monkeypatch.setattr(observations, "_roots", lambda: {"claude": root})
    monkeypatch.setattr(observations, "_PATHS_MEMO", {})
    sid = "11111111-2222-3333-4444-555555555555"
    calls = []
    real = observations._paths_uncached
    monkeypatch.setattr(observations, "_paths_uncached",
                        lambda e, s="": calls.append(s) or real(e, s))
    # Off by default: every call looks again.
    assert list(observations._paths("claude", sid)) == []
    assert list(observations._paths("claude", sid)) == []
    assert len(calls) == 2
    monkeypatch.setattr(observations, "PATHS_TTL_S", 60.0)
    assert observations._paths("claude", sid) == []
    found = root / "-repo" / f"{sid}.jsonl"
    found.write_text("{}\n")
    assert observations._paths("claude", sid) == []  # a miss is reused within the TTL
    assert len(calls) == 3
    observations._PATHS_MEMO.clear()
    assert observations._paths("claude", sid) == [found]
    assert observations._paths("claude", sid) == [found]
    assert len(calls) == 4
    found.unlink()  # a vanished hit is looked up again
    assert observations._paths("claude", sid) == []
    assert len(calls) == 5


def test_cached_json_file_ttl_and_errors(tmp_path, monkeypatch):
    import json
    import os
    from watchtower import config
    monkeypatch.setattr(config, "_JSON_FILE_MEMO", {})
    policy = tmp_path / "policy.json"
    monkeypatch.setattr(config, "CCC_MODEL_POLICY_FILE", policy)
    monkeypatch.delenv("WATCHTOWER_BLOCKED_MODELS", raising=False)
    assert config.blocked_models() == frozenset()   # missing file: no block
    assert config.blocked_models() == frozenset()
    policy.write_text("{not json")
    assert config.blocked_models() == frozenset()   # fresh write is seen at once
    policy.write_text(json.dumps({"blocked_models": ["Gpt-X"]}))
    assert config.is_blocked_model("gpt-x")
    monkeypatch.setattr(config, "STAT_TTL_S", 60.0)
    os.utime(policy, (1_700_000_000, 1_700_000_000))
    config._JSON_FILE_MEMO.clear()
    assert config.is_blocked_model("gpt-x")
    policy.write_text(json.dumps({"blocked_models": []}))
    assert config.is_blocked_model("gpt-x")         # within the daemon's stat TTL
    config._JSON_FILE_MEMO.clear()
    assert not config.is_blocked_model("gpt-x")
    policy.unlink()
    config._JSON_FILE_MEMO.clear()
    depths = []
    for _ in range(3):  # a remembered error re-raises without growing its traceback
        try:
            config._cached_json_file(policy)
        except FileNotFoundError as exc:
            tb, depth = exc.__traceback__, 0
            while tb is not None:
                tb, depth = tb.tb_next, depth + 1
            depths.append(depth)
        else:
            raise AssertionError("expected FileNotFoundError")
    assert len(set(depths)) == 1


def test_unscoped_status_reuses_full_list_for_group_index(monkeypatch):
    """WT-122: each queue row with group tickets re-listed every ticket."""
    from watchtower import health
    from watchtower import queue as q
    items = [
        {"ref": "GA-1", "project": "GA", "status": "open", "title": "parent",
         "created_at": "2026-11-25T00:00:00Z", "group": {"role": "parent"}},
        {"ref": "GB-2", "project": "GB", "status": "open", "title": "member",
         "created_at": "2026-11-25T00:00:00Z", "group": {"role": "child", "parent": "GA-1"}},
    ]
    seen = []
    real_status = health.queue_status
    monkeypatch.setattr(health, "queue_status",
                        lambda name, its, **kw: seen.append(kw.get("group_index")) or
                        real_status(name, its, **kw))

    def no_list(*_a, **_k):
        raise AssertionError("re-listed every ticket")
    monkeypatch.setattr(q, "list_items", no_list)
    rows = health.all_status(items=items, include_archived=True)
    assert {r["queue"] for r in rows} >= {"GA", "GB"}
    assert seen and all(idx is not None and set(idx) >= {"GA-1", "GB-2"} for idx in seen)

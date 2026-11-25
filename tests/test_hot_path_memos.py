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
    monkeypatch.setattr(config, "_SPAWN_DEFAULTS_MEMO", {"sig": None, "data": None})
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

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
    monkeypatch.setattr(config, "_STAT_MEMO", {"key": None, "at": 0.0, "value": None})
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

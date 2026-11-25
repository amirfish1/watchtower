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

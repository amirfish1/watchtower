"""Tests for watchtower/skills_sync.py: the symlink-based skill distribution
that keeps the bundled `watchtower` skill in sync across every installed
agent harness (Claude Code, Codex, ...) without a separate re-sync step."""

from __future__ import annotations

from watchtower import skills_sync


def _homes(tmp_path, present=("claude", "codex")):
    homes = {}
    for engine in ("claude", "codex"):
        home = tmp_path / f"{engine}-home"
        if engine in present:
            home.mkdir()
        homes[engine] = home
    return homes


def test_sync_links_into_every_present_harness(tmp_path):
    homes = _homes(tmp_path)
    results = skills_sync.sync(engine_homes=homes)
    actions = {r.engine: r.action for r in results}
    assert actions == {"claude": "linked", "codex": "linked"}
    for engine, home in homes.items():
        target = home / "skills" / skills_sync.SKILL_NAME
        assert target.is_symlink()
        assert target.resolve() == skills_sync.source_dir().resolve()


def test_sync_skips_harness_with_no_home(tmp_path):
    homes = _homes(tmp_path, present=("claude",))
    results = skills_sync.sync(engine_homes=homes)
    actions = {r.engine: r.action for r in results}
    assert actions["claude"] == "linked"
    assert actions["codex"] == "skipped-not-installed"
    assert not (homes["codex"] / "skills").exists()


def test_sync_is_idempotent(tmp_path):
    homes = _homes(tmp_path, present=("claude",))
    skills_sync.sync(engine_homes=homes)
    results = skills_sync.sync(engine_homes=homes)
    assert results[0].action == "up-to-date"


def test_sync_relinks_a_stale_symlink(tmp_path):
    homes = _homes(tmp_path, present=("claude",))
    target = homes["claude"] / "skills" / skills_sync.SKILL_NAME
    target.parent.mkdir(parents=True)
    stale = tmp_path / "somewhere-else"
    stale.mkdir()
    target.symlink_to(stale, target_is_directory=True)

    results = skills_sync.sync(engine_homes=homes)
    assert results[0].action == "relinked"
    assert target.resolve() == skills_sync.source_dir().resolve()


def test_sync_never_clobbers_a_real_directory(tmp_path):
    homes = _homes(tmp_path, present=("claude",))
    target = homes["claude"] / "skills" / skills_sync.SKILL_NAME
    target.mkdir(parents=True)
    (target / "mine.txt").write_text("user-owned content")

    results = skills_sync.sync(engine_homes=homes)
    assert results[0].action == "skipped-exists"
    assert not target.is_symlink()
    assert (target / "mine.txt").read_text() == "user-owned content"


def test_sync_dry_run_makes_no_changes(tmp_path):
    homes = _homes(tmp_path, present=("claude",))
    results = skills_sync.sync(dry_run=True, engine_homes=homes)
    assert results[0].action == "linked"
    assert not (homes["claude"] / "skills" / skills_sync.SKILL_NAME).exists()


def test_remove_undoes_a_managed_symlink(tmp_path):
    homes = _homes(tmp_path, present=("claude",))
    skills_sync.sync(engine_homes=homes)
    results = skills_sync.remove(engine_homes=homes)
    assert results[0].action == "removed"
    assert not (homes["claude"] / "skills" / skills_sync.SKILL_NAME).exists()


def test_remove_never_touches_a_real_directory(tmp_path):
    homes = _homes(tmp_path, present=("claude",))
    target = homes["claude"] / "skills" / skills_sync.SKILL_NAME
    target.mkdir(parents=True)
    (target / "mine.txt").write_text("user-owned content")

    results = skills_sync.remove(engine_homes=homes)
    assert results[0].action == "skipped-exists"
    assert (target / "mine.txt").exists()

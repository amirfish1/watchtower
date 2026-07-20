"""Tests for watchtower/skills_sync.py: the symlink-based skill distribution
that keeps bundled skills in sync across every installed
agent harness (Claude Code, Codex, ...) without a separate re-sync step."""

from __future__ import annotations

from pathlib import Path

from watchtower import skills_sync

EXPECTED_SKILLS = (
    "watchtower",
    "group-chat-checkin",
    "critique",
    "wt-triage-queue",
    "compact-to-queue",
    "add-annotate-widget",
)
ALL_ENGINES = ("claude", "codex", "antigravity", "kimi")


def _homes(tmp_path, present=ALL_ENGINES):
    homes = {}
    for engine in ALL_ENGINES:
        home = tmp_path / f"{engine}-home"
        if engine in present:
            home.mkdir()
        homes[engine] = home
    return homes


def _actions(results):
    return {(r.engine, r.target.name): r.action for r in results}


def test_sync_links_into_every_present_harness(tmp_path):
    homes = _homes(tmp_path)
    results = skills_sync.sync(engine_homes=homes)
    actions = _actions(results)
    assert actions == {
        (engine, skill): "linked"
        for engine in ALL_ENGINES
        for skill in EXPECTED_SKILLS
    }
    for engine, home in homes.items():
        for skill_name in EXPECTED_SKILLS:
            target = home / "skills" / skill_name
            assert target.is_symlink()
            assert target.resolve() == skills_sync.source_dir(skill_name).resolve()


def test_default_homes_cover_all_four_harnesses():
    """Antigravity (agy) reads skills from ~/.gemini -- its omission from
    ENGINE_HOMES meant `wt skills sync` never distributed skills to it."""
    assert set(skills_sync.ENGINE_HOMES) == set(ALL_ENGINES)
    assert skills_sync.ENGINE_HOMES["antigravity"] == Path.home() / ".gemini"


def test_sync_skips_harness_with_no_home(tmp_path):
    homes = _homes(tmp_path, present=("claude",))
    results = skills_sync.sync(engine_homes=homes)
    actions = _actions(results)
    for skill_name in EXPECTED_SKILLS:
        assert actions[("claude", skill_name)] == "linked"
        assert actions[("codex", skill_name)] == "skipped-not-installed"
        assert actions[("antigravity", skill_name)] == "skipped-not-installed"
    assert not (homes["codex"] / "skills").exists()
    assert not (homes["antigravity"] / "skills").exists()


def test_sync_is_idempotent(tmp_path):
    homes = _homes(tmp_path, present=("claude",))
    skills_sync.sync(engine_homes=homes)
    results = skills_sync.sync(engine_homes=homes)
    actions = _actions(results)
    for skill_name in EXPECTED_SKILLS:
        assert actions[("claude", skill_name)] == "up-to-date"


def test_sync_relinks_a_stale_symlink(tmp_path):
    homes = _homes(tmp_path, present=("claude",))
    target = homes["claude"] / "skills" / skills_sync.SKILL_NAME
    target.parent.mkdir(parents=True)
    stale = tmp_path / "somewhere-else"
    stale.mkdir()
    target.symlink_to(stale, target_is_directory=True)

    results = skills_sync.sync(engine_homes=homes)
    assert _actions(results)[("claude", skills_sync.SKILL_NAME)] == "relinked"
    assert target.resolve() == skills_sync.source_dir().resolve()


def test_sync_never_clobbers_a_real_directory(tmp_path):
    homes = _homes(tmp_path, present=("claude",))
    target = homes["claude"] / "skills" / skills_sync.SKILL_NAME
    target.mkdir(parents=True)
    (target / "mine.txt").write_text("user-owned content")

    results = skills_sync.sync(engine_homes=homes)
    assert _actions(results)[("claude", skills_sync.SKILL_NAME)] == "skipped-exists"
    assert not target.is_symlink()
    assert (target / "mine.txt").read_text() == "user-owned content"


def test_sync_dry_run_makes_no_changes(tmp_path):
    homes = _homes(tmp_path, present=("claude",))
    results = skills_sync.sync(dry_run=True, engine_homes=homes)
    actions = _actions(results)
    for skill_name in EXPECTED_SKILLS:
        assert actions[("claude", skill_name)] == "linked"
        assert not (homes["claude"] / "skills" / skill_name).exists()


def test_remove_undoes_a_managed_symlink(tmp_path):
    homes = _homes(tmp_path, present=("claude",))
    skills_sync.sync(engine_homes=homes)
    results = skills_sync.remove(engine_homes=homes)
    actions = _actions(results)
    for skill_name in EXPECTED_SKILLS:
        assert actions[("claude", skill_name)] == "removed"
        assert not (homes["claude"] / "skills" / skill_name).exists()


def test_remove_never_touches_a_real_directory(tmp_path):
    homes = _homes(tmp_path, present=("claude",))
    target = homes["claude"] / "skills" / skills_sync.SKILL_NAME
    target.mkdir(parents=True)
    (target / "mine.txt").write_text("user-owned content")

    results = skills_sync.remove(engine_homes=homes)
    assert _actions(results)[("claude", skills_sync.SKILL_NAME)] == "skipped-exists"
    assert (target / "mine.txt").exists()

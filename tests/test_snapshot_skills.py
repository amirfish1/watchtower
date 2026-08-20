# Copyright (c) 2026 Amir Fish. All rights reserved.
# SPDX-License-Identifier: LicenseRef-WatchTower-Software-License

"""The snapshot skills exist, are registered for sync, and reference real CLI verbs."""
from pathlib import Path

from watchtower import skills_sync

SNAPSHOT_SKILLS = ("auto-snapshot-on", "auto-snapshot-off",
                   "snapshot-now", "resume-from-snapshot",
                   "resume-from-session")


def test_snapshot_skills_registered_and_well_formed():
    for name in SNAPSHOT_SKILLS:
        assert name in skills_sync.SKILL_NAMES
        text = (skills_sync.source_dir(name) / "SKILL.md").read_text()
        assert text.startswith("---\n") and f"name: {name}" in text
        assert "description:" in text


def test_skills_reference_real_cli_verbs():
    on = (skills_sync.source_dir("auto-snapshot-on") / "SKILL.md").read_text()
    assert "wt snapshot arm" in on and "--engine" in on
    off = (skills_sync.source_dir("auto-snapshot-off") / "SKILL.md").read_text()
    assert "wt snapshot disarm" in off
    now = (skills_sync.source_dir("snapshot-now") / "SKILL.md").read_text()
    assert "wt snapshot path" in now and "wt snapshot record" in now
    res = (skills_sync.source_dir("resume-from-snapshot") / "SKILL.md").read_text()
    assert "wt snapshot latest" in res and "wt snapshot consume" in res
    rfs = (skills_sync.source_dir("resume-from-session") / "SKILL.md").read_text()
    assert "wt snapshot sessions" in rfs and "--exclude" in rfs

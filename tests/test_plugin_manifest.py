# tests/test_plugin_manifest.py
"""Marketplace manifest and plugin dir are valid and self-consistent."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_marketplace_manifest_points_at_real_plugin():
    mp = json.loads((ROOT / ".claude-plugin" / "marketplace.json").read_text())
    assert mp["name"] == "watchtower"
    entry = next(p for p in mp["plugins"] if p["name"] == "token-sitter")
    plugin_dir = ROOT / entry["source"]
    pj = json.loads((plugin_dir / ".claude-plugin" / "plugin.json").read_text())
    assert pj["name"] == "token-sitter"


def test_plugin_skill_symlinks_resolve():
    skills = ROOT / "plugins" / "token-sitter" / "skills"
    names = {"auto-snapshot-on", "auto-snapshot-off",
             "snapshot-now", "resume-from-snapshot",
             "resume-from-session"}
    assert {p.name for p in skills.iterdir()} == names
    for p in skills.iterdir():
        assert p.is_symlink()
        assert (p / "SKILL.md").resolve().exists()

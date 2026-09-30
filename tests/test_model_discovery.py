"""WT-13: model lists come from engine catalogs, never from code."""
import json
import re
from pathlib import Path

import pytest

from watchtower import config, models


@pytest.fixture
def catalogs(tmp_path, monkeypatch):
    """Fresh fixture catalogs the test can edit (new model release etc.)."""
    files = {}
    for attr, name, body in (
        ("CODEX_MODELS_CACHE", "codex.json", {"models": [
            {"slug": "gpt-9", "visibility": "list", "priority": 1,
             "supported_reasoning_levels": [{"effort": "low"}, {"effort": "ultra"}]},
            {"slug": "gpt-6-astra", "visibility": "list", "priority": 2},
            {"slug": "hidden-one", "visibility": "hide", "priority": 3}]}),
        ("CLAUDE_MODELS_FILE", "claude.json", {"records": [
            {"id": "fable-9", "output_per_mtok": 90, "released_at": "2026-10-01"},
            {"id": "opus-9", "output_per_mtok": 30, "released_at": "2026-09-01"}]}),
        ("DEVIN_MODELS_FILE", "devin.json", {"families": []}),
        ("USER_MODELS_FILE", "models.json", {}),
    ):
        path = tmp_path / name
        path.write_text(json.dumps(body))
        monkeypatch.setattr(models, attr, path)
        files[attr] = path
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({"blocked_models": ["gpt-6-astra"]}))
    monkeypatch.setattr(config, "CCC_MODEL_POLICY_FILE", policy)
    return files


def test_catalog_models_approved_with_efforts(catalogs):
    assert config.is_approved_model("codex", "gpt-9")
    assert config.approved_efforts("codex", "gpt-9") == ("low",)
    assert not config.is_approved_model("codex", "hidden-one")
    assert config.is_approved_model("claude", "claude-opus-9")
    assert config.canonical_model("claude", "opus-9") == "claude-opus-9"


def test_fable_is_never_a_worker_model(catalogs):
    assert "claude-fable-9" not in models.catalog("claude")
    assert not config.is_approved_model("claude", "claude-fable-9")


def test_blocked_model_still_refused_and_unlisted(catalogs):
    assert not config.is_approved_model("codex", "gpt-6-astra")
    assert "gpt-6-astra" not in config.approved_models("codex")


def test_new_release_needs_no_code_change(catalogs):
    assert not config.is_approved_model("codex", "gpt-10")
    body = json.loads(catalogs["CODEX_MODELS_CACHE"].read_text())
    body["models"].append({"slug": "gpt-10", "visibility": "list", "priority": 0})
    catalogs["CODEX_MODELS_CACHE"].write_text(json.dumps(body) + " ")  # new mtime/size
    assert config.is_approved_model("codex", "gpt-10")


def test_floor_ranking_from_prices_and_overrides(catalogs):
    assert models.rank("claude-opus-9") == 30
    assert models.rank("gpt-9") is None
    catalogs["USER_MODELS_FILE"].write_text(json.dumps({"rank": {"gpt-9": 42}}))
    assert models.rank("gpt-9") == 42
    assert config.model_meets_floor("gpt-9", "claude-opus-9")
    assert not config.model_meets_floor("claude-opus-9", "gpt-9")
    assert not config.model_meets_floor("gpt-unranked", "claude-opus-9")  # fails closed
    assert config.model_meets_floor("gpt-unranked", "")


def test_missing_catalog_accepts_pin_and_warns(catalogs, tmp_path, monkeypatch):
    monkeypatch.setattr(models, "CODEX_MODELS_CACHE", tmp_path / "nope.json")
    assert models.catalog("codex") is None
    assert models.catalog_warning("codex")
    assert config.is_approved_model("codex", "anything-pinned")
    assert config.approved_efforts("codex", "anything-pinned") == config.VALID_EFFORTS


def test_no_hardcoded_model_ids_in_code():
    src = Path(models.__file__).parent
    pat = re.compile(r"gpt-\d|claude-(opus|sonnet|haiku|fable)-\d|kimi-code/|swe-2\b|gemini-\d")
    bad = []
    for f in src.glob("*.py"):
        for n, line in enumerate(f.read_text().splitlines(), 1):
            code = line.split("#")[0]
            if pat.search(code) and not line.lstrip().startswith(('"""', "``", "-", "e.g")):
                bad.append(f"{f.name}:{n}: {line.strip()}")
    # Only docstring/comment prose may mention model ids.
    assert not [b for b in bad if not re.search(r"``|e\.g\.|\bsuch as\b|\(``", b)], bad

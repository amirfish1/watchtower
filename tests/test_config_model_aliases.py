"""Model-id normalization in config.canonical_model().

Lives in its own module rather than test_smoke.py so the two alias layers --
the explicit MODEL_ALIASES remap table and the structural claude- prefix for
versioned short forms -- are pinned together in one readable place.

The prefix layer is a fix that was written directly on the hermes VM
(e394961) while the table layer was written on the Mac (ee8c2bc); the clones
diverged and neither side had both. These tests are what keep the merged
behavior from regressing back to either half.
"""

import watchtower.config as config


def test_table_alias_wins_over_structural_prefix():
    """opus-5 is a REMAP, not a prefix: it once pointed at claude-opus-4-8.
    The table must be consulted first so a retarget stays possible."""
    assert config.canonical_model("claude", "opus-5") == "claude-opus-5"


def test_versioned_short_form_gets_the_claude_prefix():
    """The spawn crash e394961 fixed: `wt set --model sonnet-5` reached
    build_drain_command verbatim and the worker died at spawn."""
    assert config.canonical_model("claude", "sonnet-5") == "claude-sonnet-5"
    assert config.canonical_model("claude", "haiku-4-5") == "claude-haiku-4-5"
    assert config.canonical_model("claude", "fable-5") == "claude-fable-5"


def test_bare_family_names_pass_through_unchanged():
    """The claude CLI accepts bare family names; rewriting them would turn a
    working value into a nonexistent model id."""
    for value in ("sonnet", "opus", "haiku", "fable"):
        assert config.canonical_model("claude", value) == value


def test_already_prefixed_ids_are_not_double_prefixed():
    assert config.canonical_model("claude", "claude-sonnet-5") == "claude-sonnet-5"
    assert config.canonical_model("claude", "Claude-Opus-5") == "Claude-Opus-5"


def test_other_engines_are_returned_verbatim():
    """Only claude's CLI has this constraint -- a codex/kimi id must never
    acquire a claude- prefix."""
    assert config.canonical_model("codex", "gpt-5.6-sol") == "gpt-5.6-sol"
    assert config.canonical_model("kimi", "kimi-code/k3") == "kimi-code/k3"
    assert config.canonical_model("codex", "sonnet-5") == "sonnet-5"


def test_empty_and_whitespace_values_stay_empty():
    assert config.canonical_model("claude", "") == ""
    assert config.canonical_model("claude", "   ") == ""
    assert config.canonical_model("", "sonnet-5") == "sonnet-5"


def test_explicit_queue_override_is_normalized_end_to_end(tmp_path, monkeypatch):
    """The actual reported failure path: an explicit per-queue override read
    back through config.model(), which is what build_drain_command spawns."""
    monkeypatch.setattr(config, "CONFIG_FILE", tmp_path / "queue-config.json")
    config.set_engine("QALIAS", "claude")
    config.set_model("QALIAS", "sonnet-5")
    assert config.model("QALIAS") == "claude-sonnet-5"

"""messages.transcript_index / locate_indexed answer exactly what the
probing locate_transcript answers, from one listing per usage pass."""

from __future__ import annotations

import os

from watchtower import config, messages, usage

CLAUDE_SID = "11111111-2222-3333-4444-555555555555"
CODEX_SID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


def _tree(tmp_path, monkeypatch):
    projects = tmp_path / "projects"
    (projects / "-repo-a").mkdir(parents=True)
    (projects / "-repo-b").mkdir()
    (projects / "-repo-b" / f"{CLAUDE_SID}.jsonl").write_text("{}\n")
    codex_home = tmp_path / "codex"
    day = codex_home / "sessions" / "2026" / "11" / "24"
    day.mkdir(parents=True)
    old = day / f"rollout-2026-11-24T01-00-00-{CODEX_SID}.jsonl"
    new = day / f"rollout-2026-11-24T02-00-00-{CODEX_SID}.jsonl"
    old.write_text("{}\n")
    new.write_text("{}\n")
    os.utime(old, (1, 1))
    monkeypatch.setenv("WATCHTOWER_CLAUDE_PROJECTS_DIR", str(projects))
    monkeypatch.setenv("CODEX_HOME", str(codex_home))
    monkeypatch.setattr(config, "codex_homes", lambda: [codex_home])
    return new


def test_indexed_lookup_matches_probing_finders(tmp_path, monkeypatch):
    newest = _tree(tmp_path, monkeypatch)
    index = messages.transcript_index()
    for sid in (CLAUDE_SID, CODEX_SID, "99999999-9999-9999-9999-999999999999", "", "not-a-uuid"):
        for hint in ("claude", "codex", ""):
            assert messages.locate_indexed(index, sid, hint) == messages.locate_transcript(sid, hint)
    assert messages.locate_indexed(index, CODEX_SID, "codex") == str(newest)


def test_non_uuid_ids_are_answered_from_the_index(tmp_path, monkeypatch):
    """Devin-style names and codex suffixes give the probing answer without
    probing (each probe stats every project dir; WT-122)."""
    _tree(tmp_path, monkeypatch)
    projects = tmp_path / "projects"
    (projects / "-repo-a" / "scythe-statistic.jsonl").write_text("{}\n")
    day = tmp_path / "codex" / "sessions" / "2026" / "11" / "24"
    (day / "rollout-2026-11-24T03-00-00-odd-thread.jsonl").write_text("{}\n")
    index = messages.transcript_index()
    sids = ("scythe-statistic", "odd-thread", "thread", "eeeeeeeeeeee", "dddd-eeeeeeeeeeee",
            "pickle-television", "x" * 40)
    expected = {(sid, hint): messages.locate_transcript(sid, hint)
                for sid in sids for hint in ("claude", "codex", "devin", "")}
    assert expected[("eeeeeeeeeeee", "codex")].endswith(f"{CODEX_SID}.jsonl")
    assert expected[("thread", "")].endswith("odd-thread.jsonl")

    def no_probe(*_a, **_k):
        raise AssertionError("probed")
    monkeypatch.setattr(messages, "_find_transcript", no_probe)
    monkeypatch.setattr(messages, "_find_codex_rollout", no_probe)
    for (sid, hint), want in expected.items():
        if len(sid) <= 36:
            assert messages.locate_indexed(index, sid, hint) == want, (sid, hint)


def test_usage_pass_builds_one_index(tmp_path, monkeypatch):
    _tree(tmp_path, monkeypatch)
    calls = []
    real = messages.transcript_index
    monkeypatch.setattr(messages, "transcript_index", lambda: calls.append(1) or real())
    monkeypatch.setattr(messages, "_find_transcript",
                        lambda sid: (_ for _ in ()).throw(AssertionError("probed")))
    usage._PASS.cache = {"registry": {"workers": []}, "located": {}}
    try:
        a = usage.observe("w-1", CLAUDE_SID)
        b = usage.observe("w-2", CODEX_SID)
    finally:
        usage._PASS.cache = None
    assert calls == [1]
    assert a["path"].endswith(f"{CLAUDE_SID}.jsonl")
    assert b["path"].endswith(f"{CODEX_SID}.jsonl")


def test_receipt_sweep_index_matches_probing_stat(tmp_path, monkeypatch):
    from watchtower import receipts
    _tree(tmp_path, monkeypatch)
    cases = [(CLAUDE_SID, "claude"), (CODEX_SID, "codex"), (CODEX_SID, "claude"),
             (CLAUDE_SID, "codex"), ("not-a-uuid", "claude"), (CLAUDE_SID, "kimi")]
    probed = [receipts._transcript_stat(sid, eng) for sid, eng in cases]
    receipts._SWEEP.index = messages.transcript_index()
    try:
        indexed = [receipts._transcript_stat(sid, eng) for sid, eng in cases]
    finally:
        receipts._SWEEP.index = None
    assert indexed == probed
    assert probed[0]["path"] and probed[1]["path"] and not probed[2]["path"]

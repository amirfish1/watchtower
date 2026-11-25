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

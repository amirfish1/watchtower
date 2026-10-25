"""Deep depth on GitHub-backed queues (OPS-123).

`wt edit --depth deep` / `wt add --depth deep` used to refuse any GitHub-backed
queue; depth now lives in the issue's metadata block, so the ticket keeps its
queue and ref and a claim from any host gets the deep brief.
"""

from __future__ import annotations

from test_github_backend import (
    _drainable,
    _install_fake_gh,
    _reload_isolated,
    restore_watchtower_modules,  # noqa: F401  (autouse fixture)
)


def _github_queue(tmp_path, monkeypatch):
    _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    _drainable(config)
    return q


def test_github_ticket_promotes_to_deep_in_place_and_claims_with_brief(tmp_path, monkeypatch):
    q = _github_queue(tmp_path, monkeypatch)
    from watchtower import deep
    item = q.enqueue(project="GHI", note="slow page")

    edited = q.update(item["ref"], depth="deep", budget_min=45)

    assert edited["ref"] == item["ref"] and edited["project"] == "GHI"
    assert q.is_deep(edited) and edited["budget_min"] == 45
    claimed = q.claim_next("worker-1", project="GHI")
    assert claimed["ref"] == item["ref"] and q.is_deep(claimed)
    assert "45 minutes" in deep.deep_brief(claimed)

    demoted = q.update(item["ref"], depth="quick")
    assert not q.is_deep(demoted) and "budget_min" not in demoted


def test_github_enqueue_deep_round_trips_and_budget_needs_deep(tmp_path, monkeypatch):
    q = _github_queue(tmp_path, monkeypatch)
    item = q.enqueue(project="GHI", note="investigate", depth="deep")

    got = q.get(item["ref"])
    assert q.is_deep(got) and got["budget_min"] == q.DEEP_DEFAULT_BUDGET_MIN
    quick = q.enqueue(project="GHI", note="tweak")
    try:
        q.update(quick["ref"], budget_min=30)
    except ValueError as exc:
        assert "not a deep ticket" in str(exc)
    else:
        raise AssertionError("budget on a quick ticket must be rejected")

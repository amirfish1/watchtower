"""OPS-208: `wt close <ref> --duplicate-of <orig>` on a GitHub-backed queue.
The follower waits held, then inherits completion only from the original's
proven gates in the generation it was linked against."""
import json

import pytest

from tests.test_github_backend import restore_watchtower_modules  # noqa: F401  (autouse fixture)
from tests.test_github_pipeline_sidecar import _setup


def _issue(state, ref):
    num = int(ref.rsplit("-", 1)[1])
    return next(i for i in json.loads(state.read_text())["issues"] if i["number"] == num)


def _pair(tmp_path, monkeypatch, gates):
    state, _config, q, orig = _setup(tmp_path, monkeypatch)
    if gates:
        q.update(orig, gates=gates)
    dup = q.enqueue(project="GHI", note="same incident", text="body")["ref"]
    assert q.claim_by_ref(dup, "builder-2")
    q.link_duplicate(dup, orig, worker="builder-2")
    return state, q, orig, dup


def test_follower_is_held_and_inherits_only_a_proven_verify(tmp_path, monkeypatch):
    state, q, orig, dup = _pair(tmp_path, monkeypatch, ["verify"])

    f = q.get(dup)
    assert f["duplicate_of"] == orig and f["status"] == "open" and not f["work_it"]
    assert "2099-12-31" in _issue(state, dup)["body"]  # fenced for hosts without OPS-208
    assert q.claim_next("builder-3", project="GHI")["ref"] == orig
    with pytest.raises(ValueError, match="follows duplicate original"):
        q.close(dup, "builder-3", resolution="independent")

    q.close(orig, "builder-3", resolution={"summary": "fixed", "commit": "abc123"})
    assert q.accept(orig, by="amir", force=True)["status"] == "closed"
    from watchtower import github_duplicates
    github_duplicates.sync(dup)
    assert q.get(dup)["status"] == "open"              # force-accept skipped verify: no proof
    assert _issue(state, dup)["state"] == "OPEN"

    q.update_status(orig, "open", reason="redo")
    assert q.claim_by_ref(orig, "builder-3")
    q.close(orig, "builder-3", resolution={"summary": "fixed", "commit": "abc123"})
    cycle = q.get(orig)["verify_cycle"]
    q.verdict(orig, True, "checked", by="v1", expect_verify_cycle=cycle)

    f = q.get(dup)
    assert f["status"] == "closed" and _issue(state, dup)["state"] == "CLOSED"
    assert f["resolution"]["commit"] == "abc123" and f["duplicate_result"]["complete"]
    assert "2099-12-31" not in _issue(state, dup)["body"]


def test_a_regated_original_does_not_complete_a_stale_link(tmp_path, monkeypatch):
    _state, q, orig, dup = _pair(tmp_path, monkeypatch, ["review"])
    q.update(orig, gates=[])
    q.update(orig, gates=["review"])                   # generation g1 -> g2

    assert q.claim_by_ref(orig, "builder-3")
    q.close(orig, "builder-3", resolution={"summary": "fixed", "commit": "abc"})
    assert q.accept(orig, by="amir")["status"] == "closed"
    f = q.get(dup)
    assert f["status"] == "open" and f["needs_input"] and f["duplicate_result"]["stale"]
    assert "relink" in f["block_question"].lower()

    q.link_duplicate(dup, orig)                        # owner relinks to g2
    f = q.get(dup)
    assert f["status"] == "closed" and not f["needs_input"]


def test_link_refused_when_this_host_cannot_read_the_originals_gates(tmp_path, monkeypatch):
    _state, _config, q, orig = _setup(tmp_path, monkeypatch)
    q._github_backend_for_project("GHI").update(orig, pipeline_host="mac/other")
    dup = q.enqueue(project="GHI", note="same incident", text="body")["ref"]
    with pytest.raises(ValueError, match="owned by host 'mac'"):
        q.link_duplicate(dup, orig)
    assert not q.get(dup).get("duplicate_of")

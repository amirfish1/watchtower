"""WT-70: automatic dedup must preserve distinct incidents and gate proof."""
from __future__ import annotations

import pytest


def file_pair(q, **kwargs):
    fields = dict(project="DD", title="Agent loop", note="Short summary",
                  text="Started in session abc.\n13 turns: inspect, edit, test.")
    fields.update(kwargs)
    return [q.enqueue(**fields) for _ in range(2)]


def patch_item(q, ref, **fields):
    with q._FileLock(q._lock_path()):
        data = q._load_unlocked()
        next(i for i in data["items"] if i["ref"] == ref).update(fields)
        q._save_unlocked(data)


@pytest.mark.parametrize("field,value", [
    ("text", "Started in session xyz.\n23 turns: a different task."),
    ("text", "Started in session abc.\n13 turns: inspect, edit, test." + "x" * 20000),
    ("repo_path", "/other/repo"),
    ("selector", "#other-element"),
    ("url", "https://example.test/other-report"),
    ("screenshot_path", "/other/screenshot.png"),
    ("attachments", [{"path": "/other/trace.json"}]),
    ("future_source_context", {"invocation": "different-systemd-run"}),
    ("source", "different-ingest"),
    ("id", "different-annotation"),
    ("submitter", "different-session"),
    ("dedupe_key", "different-incident"),
    ("accept", "Also prove the second route works"),
    ("gates", ["verify"]),
    ("note", "Short summary!"),
    ("title", "Agent-loop"),
    ("title", "AGENT LOOP"),
    ("title", "Agent loop 🐛"),
    ("text", "Started in session abc.\n13 turns: inspect, edit, test!"),
    ("text", "Started in session abc.\n13 turns: inspect, edit, test.\n"),
])
@pytest.mark.parametrize("apply", [False, True])
def test_distinct_content_and_context_never_dedup(wt_env, run_cli, field, value, apply):
    q = wt_env.queue
    a, b = file_pair(q)
    patch_item(q, b["ref"], **{field: value})
    before = q.export_data()
    result = run_cli("dedup", "-q", "DD", *(["--apply"] if apply else []))
    assert result.code == 0, result.output
    assert "no exact duplicates" in result.out
    assert q.export_data() == before


def test_identical_tickets_preview_then_link_without_closing(wt_env, run_cli, monkeypatch):
    q = wt_env.queue
    a, b = file_pair(q, title="Agent loop 🐛 — 停止", note="Exact punctuation: A != B")
    patch_item(q, b["ref"], created_at="2026-11-14T23:00:00Z",
               updated_at="2026-11-14T23:00:00Z")
    c = q.enqueue(project="DD", title=a["title"], note=a["note"], text=a["text"])
    requests = []
    monkeypatch.setattr(wt_env.cli.stages, "request", lambda *args: requests.append(args))
    before = q.export_data()
    result = run_cli("dedup", "-q", "DD")
    assert result.code == 0 and f"keep {a['ref']}" in result.out
    assert q.export_data() == before and not requests
    result = run_cli("dedup", "-q", "DD", "--apply")
    assert result.code == 0 and "linked 2 duplicate(s)" in result.out
    for follower in (b, c):
        saved = q.get(follower["ref"])
        assert saved["duplicate_of"] == a["ref"] and saved["status"] == "open"
        assert not saved.get("resolution")  # no fabricated summary or completion proof
        with pytest.raises(ValueError, match="follows duplicate"):
            q.claim_by_ref(follower["ref"], "worker")
    assert len(requests) == 2
    assert "no exact duplicates" in run_cli("dedup", "--apply").out
    q.close(a["ref"], resolution={"summary": "Verified remedy", "no_code": True})
    for follower in (b, c):
        saved = q.get(follower["ref"])
        assert saved["status"] == "closed"
        assert saved["resolution"]["no_code"] is True
        assert a["ref"] in saved["resolution"]["summary"]


def test_duplicate_waits_for_current_verification_and_review(wt_env, run_cli):
    q = wt_env.queue
    a, b = file_pair(q, gates=["verify", "review"])
    assert run_cli("dedup", "-q", "DD", "--apply").code == 0
    q.close(a["ref"], resolution={"summary": "fixed", "no_code": True})
    assert q.get(b["ref"])["status"] == "open"
    q.verdict(a["ref"], False, "still reproduces")
    assert q.get(b["ref"])["status"] == "open"
    q.close(a["ref"], resolution={"summary": "verified fix", "no_code": True})
    q.verdict(a["ref"], True, "both reports checked")
    assert q.get(b["ref"])["status"] == "open"
    q.accept(a["ref"])
    assert q.get(b["ref"])["status"] == "closed"
    q.update_status(a["ref"], "open", reason="regression")
    assert q.get(b["ref"])["status"] == "open"


def test_no_cross_queue_dedup_even_without_filter(wt_env, run_cli):
    q = wt_env.queue
    a = q.enqueue(project="ONE", note="identical report")
    b = q.enqueue(project="TWO", note="identical report")
    assert "no exact duplicates" in run_cli("dedup", "--apply").out
    assert not q.get(a["ref"]).get("duplicate_of")
    assert not q.get(b["ref"]).get("duplicate_of")


def test_history_and_owner_wait_are_preserved(wt_env, run_cli):
    q = wt_env.queue
    a, b = file_pair(q)
    q.comment(b["ref"], "The second report affects another route")
    assert "no exact duplicates" in run_cli("dedup", "--apply").out
    assert q.get(b["ref"])["history"][-1]["event"] == "comment"
    patch_item(q, b["ref"], history=[b["history"][0]], needs_input=True,
               block_question="Which source is authoritative?")
    assert "no exact duplicates" in run_cli("dedup", "--apply").out
    assert q.get(b["ref"])["needs_input"]


@pytest.mark.parametrize("which", [0, 1])
@pytest.mark.parametrize("change", ["text", "claim", "gate"])
def test_apply_rechecks_both_tickets_under_store_lock(wt_env, run_cli, monkeypatch, which, change):
    q = wt_env.queue
    pair = file_pair(q)
    real_link = q.link_duplicate

    def changed_since_scan(ident, original_ref, **kwargs):
        ref = pair[which]["ref"]
        if change == "claim":
            q.claim_by_ref(ref, "other-worker")
        else:
            q.update(ref, **({"text": "New incident evidence"} if change == "text"
                             else {"gates": ["verify"]}))
        return real_link(ident, original_ref, **kwargs)

    monkeypatch.setattr(q, "link_duplicate", changed_since_scan)
    result = run_cli("dedup", "-q", "DD", "--apply")
    assert result.code == 1 and "tickets changed" in result.err
    assert all(not q.get(i["ref"]).get("duplicate_of") for i in pair)


def test_empty_reports_are_not_automatically_duplicates(wt_env, run_cli):
    q = wt_env.queue
    for item in file_pair(q):
        patch_item(q, item["ref"], note="", title="", text="")
    assert "no exact duplicates" in run_cli("dedup", "--apply").out


def test_filing_provenance_is_part_of_exact_content(wt_env, run_cli):
    q = wt_env.queue
    a, b = file_pair(q)
    history = b["history"]
    history[0]["invocation"] = "different-source-run"
    patch_item(q, b["ref"], history=history)
    assert "no exact duplicates" in run_cli("dedup", "--apply").out

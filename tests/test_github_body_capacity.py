"""GitHub issue-body capacity for answers and comments (WT-44).

Production: BECKY-2966's issue body reached 256,520 bytes, 248 KB of it
history, because every `wt answer`/`wt comment` copied its full text into
the body's metadata block. A 6.5 KB grant then failed both ways with
"GraphQL: Body is too long (updateIssue)". The stateful fake gh below
enforces the same cap (FAKE_GH_BODY_MAX) the way real gh reports it.

The contract: the full text is posted as an issue comment first (the
durable copy), and the history entry keeps a bounded excerpt, the comment
reference and the text's sha256/length. Legacy long entries move to their
comments only when a comment holds exactly their text. Anything that still
cannot fit is refused before any write. A failed body edit leaves the answer
undelivered, and its retry adopts the comment already posted.
"""

from __future__ import annotations

import hashlib
import json

import pytest

from tests.test_github_backend import (
    _drainable,
    _install_fake_gh,
    _reload_isolated,
)

GITHUB_CAP = 262144
REPO = "test-owner/test-repo"


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _grant(n: int, salt: str = "") -> str:
    """Multi-line text with quotes, backslashes and Unicode, so its JSON
    escaping in the metadata block is larger than its length."""
    unit = f'{salt}step "s16" \\ canonical-address é中 -> ok\n'
    return (unit * (n // len(unit) + 1))[:n]


def _blocked(tmp_path, monkeypatch):
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", REPO)
    _drainable(config)
    item = q.enqueue(project="GHI", note="needs a decision")
    q.claim_by_ref(item["ref"], "worker-1", session_uuid="session-abc")
    q.block("GHI-1", session_id="worker-1", question="Grant s16?")
    import watchtower.github_backend as gb
    return state, q, gb


def _issue(state):
    return json.loads(state.read_text())["issues"][0]


def _meta(gb, state):
    return gb._split_body(_issue(state)["body"])[1]


def _seed_legacy(state, gb, *, target: int, event: str = "answer",
                 commented: bool = True) -> int:
    """Grow the issue body to ``target`` bytes with pre-WT-44 history: long
    texts stored inline, each (when ``commented``) also an issue comment, as
    `wt answer` and `wt comment` used to write them. Returns entries added."""
    data = json.loads(state.read_text())
    issue = data["issues"][0]
    human, meta = gb._split_body(issue["body"])
    legacy = []
    while True:
        i = len(legacy)
        text = _grant(5000, salt=f"legacy {i} ")
        legacy.append({"event": event, "at": f"2026-10-01T00:{i // 60:02d}:{i % 60:02d}Z",
                       "kind": "human", "text": text})
        if commented:
            issue["comments"].append({
                "author": {"login": "test-owner"}, "body": text,
                "url": f"{issue['url']}#issuecomment-{900 + i}"})
        meta["history"] = legacy + meta["history"][-2:]
        if len(gb._body_with_metadata(human, meta).encode()) >= target:
            break
    issue["body"] = gb._body_with_metadata(human, meta)
    state.write_text(json.dumps(data))
    return len(legacy)


def _comments_with(state, gb, text):
    return [c for c in _issue(state)["comments"]
            if gb._strip_entry_marker(c["body"])[0] == text]


def _assert_referenced(state, gb, entry, text):
    """The entry is bounded and points at a comment holding exactly ``text``."""
    assert len(entry["text"]) < gb._HISTORY_TEXT_INLINE_MAX + 200
    assert entry["text_len"] == len(text)
    assert entry["text_sha256"] == _sha(text)
    (comment,) = [c for c in _issue(state)["comments"] if c["url"] == entry["text_ref"]]
    assert _sha(gb._strip_entry_marker(comment["body"])[0]) == entry["text_sha256"]


def test_near_limit_answer_lands_and_preserves_history_and_authority(tmp_path, monkeypatch):
    """The BECKY-2966 shape: a body just under GitHub's cap takes a 6.5 KB
    grant. Old code sent a body over the cap and GitHub refused it."""
    state, q, gb = _blocked(tmp_path, monkeypatch)
    monkeypatch.setenv("FAKE_GH_BODY_MAX", str(GITHUB_CAP))
    seeded = _seed_legacy(state, gb, target=256520)
    before = _meta(gb, state)
    grant = _grant(6463)

    answered = q.answer("GHI-1", grant, session_id="human-1")

    before_ids = [gb._history_id(e) for e in before["history"]]

    body = _issue(state)["body"]
    assert len(body.encode()) <= gb._ISSUE_BODY_BUDGET_BYTES
    after = _meta(gb, state)
    # Every earlier entry is still there, in order (same identity, texts
    # moved by reference), plus exactly one new answer.
    assert [gb._history_id(e) for e in after["history"][:-1]] == before_ids
    new = after["history"][-1]
    assert new["event"] == "answer" and new["kind"] == "human"
    _assert_referenced(state, gb, new, grant)
    moved = [e for e in after["history"][:-1] if e.get("text_ref")]
    assert 0 < len(moved) <= seeded
    for entry in moved:
        legacy_text = next(gb._strip_entry_marker(c["body"])[0]
                           for c in _issue(state)["comments"] if c["url"] == entry["text_ref"])
        _assert_referenced(state, gb, entry, legacy_text)
    # Claim and gate authority untouched; only the answer's own keys changed.
    changed = {k for k in set(before) | set(after)
               if k != "history" and before.get(k) != after.get(k)}
    assert changed == {"needs_input", "answered_at"}
    assert answered["claimed_by"] == "worker-1"
    assert answered["claimed_session_id"] == "session-abc"
    assert answered["status"] == "in_progress" and answered["needs_input"] is False
    assert len(_comments_with(state, gb, grant)) == 1


def test_original_failure_reproduces_against_the_fake_cap(tmp_path, monkeypatch):
    """Witness that the fake cap is the production failure: the pre-WT-44
    body (the full grant inline) is refused with GitHub's message."""
    state, _q, gb = _blocked(tmp_path, monkeypatch)
    monkeypatch.setenv("FAKE_GH_BODY_MAX", str(GITHUB_CAP))
    _seed_legacy(state, gb, target=256520)
    human, meta = gb._split_body(_issue(state)["body"])
    gb._append_history(meta, "answer", kind="human", text=_grant(6463))
    backend = gb.GitHubIssuesBackend("GHI", repo=REPO)
    with pytest.raises(gb.GitHubBackendError, match="Body is too long"):
        backend._run(["issue", "edit", "1", "--repo", REPO],
                     body=gb._body_with_metadata(human, meta))


@pytest.mark.parametrize("size", [40, 5000], ids=["short", "long"])
def test_failed_issue_edit_leaves_answer_undelivered_and_retry_is_exactly_once(
    tmp_path, monkeypatch, size,
):
    state, q, gb = _blocked(tmp_path, monkeypatch)
    text = _grant(size)
    monkeypatch.setenv("FAKE_GH_FAIL", "issue edit=fail")
    with pytest.raises(gb.GitHubBackendError):
        q.answer("GHI-1", text, session_id="human-1")
    meta = _meta(gb, state)
    assert meta["needs_input"] is True
    assert not [e for e in meta["history"] if e["event"] == "answer"]
    assert len(_comments_with(state, gb, text)) == 1  # durable copy, not yet recorded

    monkeypatch.delenv("FAKE_GH_FAIL")
    answered = q.answer("GHI-1", text, session_id="human-1")

    assert answered["needs_input"] is False
    answers = [e for e in _meta(gb, state)["history"] if e["event"] == "answer"]
    assert len(answers) == 1
    (comment,) = _comments_with(state, gb, text)  # adopted, not reposted
    if size > gb._HISTORY_TEXT_INLINE_MAX:
        assert answers[0]["text_ref"] == comment["url"]
        _assert_referenced(state, gb, answers[0], text)
    else:
        assert answers[0]["text"] == text


def test_answer_that_cannot_fit_is_refused_before_any_write(tmp_path, monkeypatch):
    """History that no comment holds (progress) cannot move: refuse up front,
    post nothing, leave the block in place; the retry is refused the same way."""
    state, q, gb = _blocked(tmp_path, monkeypatch)
    monkeypatch.setenv("FAKE_GH_BODY_MAX", str(GITHUB_CAP))
    _seed_legacy(state, gb, target=GITHUB_CAP - 2000, event="progress", commented=False)
    body_before = _issue(state)["body"]
    comments_before = len(_issue(state)["comments"])
    commands_before = len(json.loads(state.read_text())["commands"])

    for _ in range(2):
        with pytest.raises(gb.GitHubBackendError, match="Nothing was written"):
            q.answer("GHI-1", _grant(6463), session_id="human-1")

    issue = _issue(state)
    assert issue["body"] == body_before
    assert len(issue["comments"]) == comments_before
    later = json.loads(state.read_text())["commands"][commands_before:]
    assert not [c for c in later if c[:2] in (["issue", "edit"], ["issue", "comment"])]
    assert _meta(gb, state)["needs_input"] is True


def test_long_comment_does_not_grow_the_body_unboundedly(tmp_path, monkeypatch):
    state, q, gb = _blocked(tmp_path, monkeypatch)
    before = len(_issue(state)["body"].encode())
    text = _grant(40000)

    q.comment("GHI-1", text, by="worker", session_id="worker-1")

    growth = len(_issue(state)["body"].encode()) - before
    # The excerpt is capped in characters; JSON escapes é/中/quotes/newlines.
    assert growth < 2500, growth
    entry = _meta(gb, state)["history"][-1]
    assert entry["event"] == "comment" and entry["kind"] == "worker"
    _assert_referenced(state, gb, entry, text)
    assert len(_comments_with(state, gb, text)) == 1


def test_text_past_a_github_comment_is_refused_not_truncated(tmp_path, monkeypatch):
    state, q, gb = _blocked(tmp_path, monkeypatch)
    body_before = _issue(state)["body"]
    comments_before = len(_issue(state)["comments"])
    for call in (lambda t: q.comment("GHI-1", t), lambda t: q.answer("GHI-1", t)):
        with pytest.raises(gb.GitHubBackendError, match="Nothing was written"):
            call(_grant(70000))
    assert _issue(state)["body"] == body_before
    assert len(_issue(state)["comments"]) == comments_before


def test_repeated_identical_answer_in_a_new_block_posts_its_own_comment(tmp_path, monkeypatch):
    """Adoption is keyed to one write attempt, not to the text: the same
    words answering a later block are a new answer."""
    state, q, gb = _blocked(tmp_path, monkeypatch)
    q.answer("GHI-1", "yes", session_id="human-1")
    q.block("GHI-1", session_id="worker-1", question="Again?")
    q.answer("GHI-1", "yes", session_id="human-1")
    assert len(_comments_with(state, gb, "yes")) == 2
    assert len([e for e in _meta(gb, state)["history"] if e["event"] == "answer"]) == 2


def test_worker_text_hides_the_entry_marker(tmp_path, monkeypatch):
    _state, q, gb = _blocked(tmp_path, monkeypatch)
    q.comment("GHI-1", "heads up", by="worker", session_id="worker-1")
    item = gb.GitHubIssuesBackend("GHI", repo=REPO).get(1)
    assert "heads up" in item["text"]
    assert "watchtower-entry" not in item["text"]


def test_reapply_after_concurrent_write_keeps_moved_entries_single(monkeypatch):
    """A cross-host writer lands between our compacting write and our re-read:
    the re-apply must not append the moved entry again next to its original."""
    import watchtower.github_backend as gb
    long_text = _grant(5000)
    a = {"event": "answer", "at": "2026-10-01T00:00:00Z", "kind": "human", "text": long_text}
    b = {"event": "claim", "at": "2026-10-01T00:01:00Z", "worker": "w"}
    a_moved = dict(a)
    gb._referenced_text(a_moved, long_text, "https://x/issues/1#issuecomment-9")
    c = {"event": "comment", "at": "2026-10-01T00:02:00Z", "kind": "worker", "text": "ours"}
    d = {"event": "progress", "at": "2026-10-01T00:02:30Z", "text": "theirs"}
    base = gb._body_with_metadata("h", {"history": [a, b]})
    ours = gb._body_with_metadata("h", {"history": [a_moved, b, c]})
    fresh = gb._body_with_metadata("h", {"history": [a, b, d]})

    merged, conflicts = gb._merge_body_write(base, ours, fresh)

    assert conflicts == []
    hist = gb._split_body(merged)[1]["history"]
    assert hist == [a_moved, b, d, c]
    assert gb._history_id(a_moved) == gb._history_id(a)


def test_owner_ingested_long_answer_references_the_owner_comment(tmp_path, monkeypatch):
    state, _q, gb = _blocked(tmp_path, monkeypatch)
    backend = gb.GitHubIssuesBackend("GHI", repo=REPO)
    text = _grant(8000)
    comments_before = len(_issue(state)["comments"])
    backend.answer(1, text, post_comment=False,
                   source_comment_key="https://github.com/test-owner/test-repo/issues/1#issuecomment-77")
    entry = _meta(gb, state)["history"][-1]
    assert entry["text_ref"].endswith("#issuecomment-77")
    assert entry["text_sha256"] == _sha(text) and entry["text_len"] == len(text)
    assert len(_issue(state)["comments"]) == comments_before


@pytest.mark.parametrize("event", ["answer", "comment"])
def test_retry_adopts_its_comment_after_an_unrelated_write_landed(tmp_path, monkeypatch, event):
    """Verifier repro (WT-44 send-back): a near-limit answer whose body edit
    failed, then an unrelated comment lands, then the identical retry. The
    retry must adopt the first attempt's comment, not post a second one."""
    state, q, gb = _blocked(tmp_path, monkeypatch)
    monkeypatch.setenv("FAKE_GH_BODY_MAX", str(GITHUB_CAP))
    _seed_legacy(state, gb, target=256520)
    text = _grant(6463)
    write = ((lambda: q.answer("GHI-1", text, session_id="human-1")) if event == "answer"
             else (lambda: q.comment("GHI-1", text, by="worker", session_id="worker-1")))

    monkeypatch.setenv("FAKE_GH_FAIL", "issue edit=fail")
    with pytest.raises(gb.GitHubBackendError):
        write()
    monkeypatch.delenv("FAKE_GH_FAIL")
    assert len(_comments_with(state, gb, text)) == 1
    q.comment("GHI-1", "unrelated note", by="worker", session_id="worker-1")
    write()

    (comment,) = _comments_with(state, gb, text)
    hist = _meta(gb, state)["history"]
    mine = [e for e in hist if e.get("text_sha256") == _sha(text)]
    assert len(mine) == 1 and mine[0]["event"] == event
    assert mine[0]["text_ref"] == comment["url"]
    assert len(_issue(state)["body"].encode()) <= gb._ISSUE_BODY_BUDGET_BYTES
    if event == "answer":
        assert _meta(gb, state)["needs_input"] is False


def test_repeated_identical_comment_gets_its_own_comment_and_entry(tmp_path, monkeypatch):
    state, q, gb = _blocked(tmp_path, monkeypatch)
    for _ in range(2):
        q.comment("GHI-1", "same words", by="worker", session_id="worker-1")
    assert len(_comments_with(state, gb, "same words")) == 2
    keys = [e["entry_key"] for e in _meta(gb, state)["history"] if e["event"] == "comment"]
    assert len(keys) == 2 and len(set(keys)) == 2


@pytest.mark.parametrize("variant", ["trailing", "leading", "inner"])
def test_compaction_requires_exact_comment_text(monkeypatch, variant):
    """Verifier repro: a comment differing from the entry only in whitespace
    is not that text; moving to it would lose the original. Stay inline."""
    import watchtower.github_backend as gb
    base = _grant(5000)
    text, comment = {
        "trailing": (base + "     ", base),
        "leading": (base, " " + base),
        "inner": (base, base.replace("\n", "\n ", 1)),
    }[variant]
    entry = {"event": "answer", "at": "2026-10-01T00:00:00Z", "kind": "human", "text": text}
    meta = {"history": [dict(entry)]}
    monkeypatch.setattr(gb, "_ISSUE_BODY_BUDGET_BYTES", 0)  # move anything movable

    moved = gb._compact_history(meta, [{"body": comment, "url": "u#1"}], "h")

    assert moved == 0
    assert meta["history"] == [entry]


def test_near_limit_whitespace_mismatch_stays_inline_or_refuses(tmp_path, monkeypatch):
    """End to end: legacy entries whose comments differ only in trailing
    whitespace never move. Every moved entry's hash matches its comment;
    every other legacy entry is still complete inline."""
    state, q, gb = _blocked(tmp_path, monkeypatch)
    monkeypatch.setenv("FAKE_GH_BODY_MAX", str(GITHUB_CAP))
    _seed_legacy(state, gb, target=256520)
    data = json.loads(state.read_text())
    issue = data["issues"][0]
    human, meta = gb._split_body(issue["body"])
    # The oldest legacy entries carry 5 trailing spaces their comments lack.
    for i in range(5):
        meta["history"][i]["text"] += "     "
    issue["body"] = gb._body_with_metadata(human, meta)
    state.write_text(json.dumps(data))
    originals = [dict(e) for e in meta["history"]]

    q.answer("GHI-1", _grant(6463), session_id="human-1")

    after = _meta(gb, state)["history"]
    comments = {c["url"]: gb._strip_entry_marker(c["body"])[0] for c in _issue(state)["comments"]}
    for orig, now in zip(originals, after):
        if now.get("text_ref"):
            assert _sha(comments[now["text_ref"]]) == now["text_sha256"] == _sha(orig["text"])
        else:
            assert now == orig
    assert all(after[i] == originals[i] for i in range(5))

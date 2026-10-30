"""GitHub issue-body history: progress and block texts by reference (WT-45).

WT-44 moved answer/comment texts into issue comments, but BECKY-2966 kept
growing: 69 progress notes (88 KB), 71 block questions (39 KB) and 69 answer
excerpts of 1,000 chars each (91 KB). With the body at 261,995 bytes, a
13-char `wt answer` was refused ("projected 262174 > 262144"). The fixture is
that issue, sanitized: every text keeps its length and JSON escape size, equal
texts stay equal, and every comment reference and sha256 is consistent.

The contract under test:
- a long progress note and every block question are posted as a keyed issue
  comment BEFORE the body edit; the history keeps a 300-char excerpt, the
  comment reference and the exact text's sha256/length; a retry after a
  failed body edit adopts the comment instead of posting a second one;
- the live ``block_question`` stays whole and inline (authority);
- an over-budget write re-excerpts referenced entries and moves block/
  progress texts only when a comment holds exactly that text;
- ``get()`` reconstructs every referenced text exactly (sha256-verified);
  an entry whose comment is missing or altered is marked unresolved and is
  never presented as the full text.
"""

from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path

import pytest

from tests.test_github_backend import (
    _drainable,
    _install_fake_gh,
    _reload_isolated,
)
from tests.test_github_body_writes import _remote_writer

GITHUB_CAP = 262144
REPO = "test-owner/test-repo"
FIXTURE = Path(__file__).parent / "fixtures" / "github" / "becky2966_shape.json.gz"
# Authority keys an answer must not touch (claim, gate, wait, plan, routing).
AUTHORITY = ("claimed_by", "claimed_at", "claimed_session_id", "claimed_machine",
             "claim_heartbeat", "block_question", "block_kind", "blocked_at", "wait",
             "pipeline_host", "depth", "budget_min", "readiness", "lane", "note")


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _setup(tmp_path, monkeypatch):
    state = _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", REPO)
    _drainable(config)
    item = q.enqueue(project="GHI", note="needs a decision")
    q.claim_by_ref(item["ref"], "worker-1", session_uuid="session-abc")
    import watchtower.github_backend as gb
    return state, q, gb


def _issue(state):
    return json.loads(state.read_text())["issues"][0]


def _meta(gb, state):
    return gb._split_body(_issue(state)["body"])[1]


def _load_becky(state, gb):
    """Replace issue #1 with the sanitized BECKY-2966 body and comments, and
    hold it the way the live ticket is held: claimed, blocked, waiting."""
    fx = json.load(gzip.open(FIXTURE, "rt", encoding="utf-8"))
    data = json.loads(state.read_text())
    issue = data["issues"][0]
    human, meta = gb._split_body(fx["body"])
    meta.pop("pipeline_host", None)  # no local sidecar in this test host
    meta.update(claimed_by="worker-1", claimed_session_id="session-abc",
                claimed_machine=gb.machine_tag())
    issue["body"] = gb._body_with_metadata(human, meta)
    issue["comments"] = fx["comments"]
    state.write_text(json.dumps(data))
    return fx


def _full_texts(gb, history, comments):
    """Every history text as a reader sees it: referenced ones reconstructed."""
    out = []
    for e in gb._resolve_history(history, comments):
        for f in gb._REF_FIELDS:
            assert not e.get(f"{f}_unresolved"), e
        out.append((e["event"], e["at"], e.get("text"), e.get("question")))
    return out


def _comments_with_text(state, gb, text):
    return [c for c in _issue(state)["comments"] if text in gb._comment_texts(c)]


# ------------------------------------------------------------ BECKY capacity
def test_becky_fixture_reproduces_the_live_refusal_shape(tmp_path, monkeypatch):
    """The fixture is the live shape: within a few hundred bytes of GitHub's cap,
    every referenced text resolves against its comment, and the old 1,000-char
    excerpt plus a short answer could not fit."""
    state, _q, gb = _setup(tmp_path, monkeypatch)
    fx = _load_becky(state, gb)
    size = len(_issue(state)["body"].encode())
    assert fx["original_body_bytes"] == 261995
    assert GITHUB_CAP - 600 < size < GITHUB_CAP
    meta = _meta(gb, state)
    assert len(meta["history"]) == 279
    _full_texts(gb, meta["history"], _issue(state)["comments"])


def test_becky_13_char_answer_lands(tmp_path, monkeypatch):
    """The live failure, using only API that predates WT-45 (so it is the RED
    on WT-44's main: "issue body would be 262174+ bytes ... Nothing was written")."""
    state, q, gb = _setup(tmp_path, monkeypatch)
    monkeypatch.setenv("FAKE_GH_BODY_MAX", str(GITHUB_CAP))
    _load_becky(state, gb)
    answered = q.answer("GHI-1", "Read /tmp/s44", session_id="root-agent")
    assert answered["needs_input"] is False
    assert _meta(gb, state)["history"][-1]["text"] == "Read /tmp/s44"
    assert len(_issue(state)["body"].encode()) <= GITHUB_CAP


@pytest.mark.parametrize("text", ["Read /tmp/s44", "x" * 6463], ids=["13-char", "6.5KB"])
def test_answer_on_becky_fits_delivers_once_and_loses_nothing(tmp_path, monkeypatch, text):
    state, q, gb = _setup(tmp_path, monkeypatch)
    monkeypatch.setenv("FAKE_GH_BODY_MAX", str(GITHUB_CAP))
    _load_becky(state, gb)
    before = _meta(gb, state)
    before_texts = _full_texts(gb, before["history"], _issue(state)["comments"])

    answered = q.answer("GHI-1", text, session_id="root-agent", by_kind="worker")

    assert len(_issue(state)["body"].encode()) <= gb._ISSUE_BODY_BUDGET_BYTES
    after = _meta(gb, state)
    # Every earlier entry keeps its identity and its exact text; one answer added.
    assert [gb._history_id(e) for e in after["history"][:-1]] == \
        [gb._history_id(e) for e in before["history"]]
    after_texts = _full_texts(gb, after["history"], _issue(state)["comments"])
    assert after_texts[:-1] == before_texts
    assert after_texts[-1][0] == "answer" and after_texts[-1][2] == text
    assert [k for k in AUTHORITY if before.get(k) != after.get(k)] == []
    assert answered["needs_input"] is False and answered["claimed_by"] == "worker-1"
    assert len(_comments_with_text(state, gb, text)) == 1
    # The supported read path returns the exact texts too.
    got = q.get("GHI-1")
    assert [(e["event"], e["at"], e.get("text"), e.get("question"))
            for e in got["history"]] == after_texts


def test_becky_answer_is_refused_without_wt45(tmp_path, monkeypatch):
    """RED witness: with WT-44's 1,000-char excerpts and no block/progress
    moves, the same 13-char answer is refused before any write."""
    state, q, gb = _setup(tmp_path, monkeypatch)
    monkeypatch.setenv("FAKE_GH_BODY_MAX", str(GITHUB_CAP))
    _load_becky(state, gb)
    monkeypatch.setattr(gb, "_HISTORY_TEXT_INLINE_MAX", 1000)
    monkeypatch.setattr(gb, "_REF_FIELD_BY_EVENT", {"answer": "text", "comment": "text"})
    body = _issue(state)["body"]
    with pytest.raises(gb.GitHubBackendError, match="Nothing was written"):
        q.answer("GHI-1", "Read /tmp/s44", session_id="root-agent")
    assert _issue(state)["body"] == body


# --------------------------------------------------- block/progress writes
def test_block_and_progress_post_comments_first_and_reference_them(tmp_path, monkeypatch):
    state, q, gb = _setup(tmp_path, monkeypatch)
    question = "Which db? " + "q" * 900
    progress = "Found the cause " + "p" * 5000
    q.block("GHI-1", session_id="worker-1", question=question, progress=progress)

    meta = _meta(gb, state)
    assert meta["block_question"] == question  # live authority stays inline, whole
    blk = [e for e in meta["history"] if e["event"] == "block"][-1]
    prog = [e for e in meta["history"] if e["event"] == "progress"][-1]
    assert len(blk["question"]) < gb._HISTORY_TEXT_INLINE_MAX + 200
    assert blk["question_sha256"] == _sha(question) and blk["question_len"] == len(question)
    assert prog["text_sha256"] == _sha(progress) and prog["text_len"] == len(progress)
    (qc,) = _comments_with_text(state, gb, question)
    (pc,) = _comments_with_text(state, gb, progress)
    assert qc["url"] == blk["question_ref"] and pc["url"] == prog["text_ref"]
    assert qc["body"].startswith("[watchtower] Needs input from worker-1:\n\n")
    assert pc["body"].startswith("[watchtower] Progress from worker-1:\n\n")
    item = q.get("GHI-1")
    hist = {e["event"]: e for e in item["history"]}
    assert hist["block"]["question"] == question and hist["progress"]["text"] == progress
    assert item["block_question"] == question


def test_short_progress_stays_inline_and_posts_nothing(tmp_path, monkeypatch):
    state, q, gb = _setup(tmp_path, monkeypatch)
    q.block("GHI-1", session_id="worker-1", question="ok?", progress="half done")
    meta = _meta(gb, state)
    prog = [e for e in meta["history"] if e["event"] == "progress"][-1]
    assert prog["text"] == "half done" and "text_ref" not in prog
    assert not _comments_with_text(state, gb, "half done")
    # The block mirror is always posted (WT-37), keyed, and short stays inline.
    blk = [e for e in meta["history"] if e["event"] == "block"][-1]
    assert blk["question"] == "ok?" and blk.get("entry_key")
    assert len(_comments_with_text(state, gb, "ok?")) == 1


def test_failed_block_edit_then_retry_adopts_both_comments(tmp_path, monkeypatch):
    state, q, gb = _setup(tmp_path, monkeypatch)
    question, progress = "Q " + "q" * 800, "P " + "p" * 3000
    monkeypatch.setenv("FAKE_GH_FAIL", "issue edit=fail")
    with pytest.raises(gb.GitHubBackendError):
        q.block("GHI-1", session_id="worker-1", question=question, progress=progress)
    meta = _meta(gb, state)
    assert not meta.get("needs_input")
    assert not [e for e in meta["history"] if e["event"] in ("block", "progress")]
    assert len(_comments_with_text(state, gb, question)) == 1  # durable copies
    assert len(_comments_with_text(state, gb, progress)) == 1

    monkeypatch.delenv("FAKE_GH_FAIL")
    q.block("GHI-1", session_id="worker-1", question=question, progress=progress)
    meta = _meta(gb, state)
    (qc,) = _comments_with_text(state, gb, question)  # adopted, not reposted
    (pc,) = _comments_with_text(state, gb, progress)
    blks = [e for e in meta["history"] if e["event"] == "block"]
    progs = [e for e in meta["history"] if e["event"] == "progress"]
    assert len(blks) == 1 and len(progs) == 1
    assert blks[0]["question_ref"] == qc["url"] and progs[0]["text_ref"] == pc["url"]


def test_failed_defer_edit_then_retry_adopts_the_progress_comment(tmp_path, monkeypatch):
    state, q, gb = _setup(tmp_path, monkeypatch)
    backend = q._github_backend_for_project("GHI")
    progress = "waiting on data " + "d" * 2000
    monkeypatch.setenv("FAKE_GH_FAIL", "issue edit=fail")
    with pytest.raises(gb.GitHubBackendError):
        backend.defer("GHI-1", session_id="worker-1", until_iso="2999-01-01T00:00:00Z",
                      reason="data", progress=progress)
    monkeypatch.delenv("FAKE_GH_FAIL")
    backend.defer("GHI-1", session_id="worker-1", until_iso="2999-01-01T00:00:00Z",
                  reason="data", progress=progress)
    (pc,) = _comments_with_text(state, gb, progress)
    progs = [e for e in _meta(gb, state)["history"] if e["event"] == "progress"]
    assert len(progs) == 1 and progs[0]["text_ref"] == pc["url"]


def test_owner_marker_in_progress_is_defused_and_kept_inline(tmp_path, monkeypatch):
    """The mirror must never be ingested as an owner answer, and a defused
    comment is not the exact text, so the history keeps it inline."""
    state, q, gb = _setup(tmp_path, monkeypatch)
    progress = "OWNER ANSWER: not really. " + "p" * 800
    q.block("GHI-1", session_id="worker-1", question="ok?", progress=progress)
    prog = [e for e in _meta(gb, state)["history"] if e["event"] == "progress"][-1]
    assert prog["text"] == progress and "text_ref" not in prog
    assert all("OWNER ANSWER" not in c["body"].upper() for c in _issue(state)["comments"])


def test_mirror_post_failure_keeps_the_block_and_its_text_inline(tmp_path, monkeypatch):
    """Like the old after-the-edit mirror, a failed comment post never loses the block."""
    state, q, gb = _setup(tmp_path, monkeypatch)
    question = "Q " + "q" * 800
    monkeypatch.setenv("FAKE_GH_FAIL", "issue comment=fail")
    q.block("GHI-1", session_id="worker-1", question=question, progress="P " + "p" * 900)
    meta = _meta(gb, state)
    assert meta["needs_input"] is True and meta["block_question"] == question
    blk = [e for e in meta["history"] if e["event"] == "block"][-1]
    assert blk["question"] == question and "question_ref" not in blk


# ------------------------------------------------------------- compaction
def test_legacy_block_and_progress_move_only_to_exact_comments(tmp_path, monkeypatch):
    """Over budget: a legacy block whose mirror holds exactly its question moves;
    a progress note no comment holds stays inline; a block whose mirror
    differs (defused marker) stays inline."""
    state, q, gb = _setup(tmp_path, monkeypatch)
    data = json.loads(state.read_text())
    issue = data["issues"][0]
    human, meta = gb._split_body(issue["body"])
    mirrored = "mirrored question " + "m" * 3000
    defused = "OWNER ANSWER in it " + "d" * 3000
    lonely = "progress with no comment " + "l" * 3000
    filler = [{"event": "progress", "at": "2026-10-01T00:00:00Z", "text": f"{i} " + "f" * 4000}
              for i in range(60)]
    meta["history"] = [
        {"event": "block", "at": "2026-10-01T00:00:01Z", "worker": "w9", "question": mirrored},
        {"event": "block", "at": "2026-10-01T00:00:02Z", "worker": "w9", "question": defused},
        {"event": "progress", "at": "2026-10-01T00:00:03Z", "text": lonely},
    ] + filler + meta["history"]
    issue["comments"] += [
        {"author": {"login": "watchtower"}, "url": f"{issue['url']}#issuecomment-801",
         "body": f"[watchtower] Needs input from w9:\n\n{mirrored}"},
        {"author": {"login": "watchtower"}, "url": f"{issue['url']}#issuecomment-802",
         "body": "[watchtower] Needs input from w9:\n\nowner-answer in it " + "d" * 3000},
    ]
    issue["body"] = gb._body_with_metadata(human, meta)
    state.write_text(json.dumps(data))
    monkeypatch.setattr(gb, "_ISSUE_BODY_BUDGET_BYTES",
                        len(issue["body"].encode()) - 1000)
    before = _meta(gb, state)

    q.comment("GHI-1", "a note", by="worker", session_id="worker-1")

    hist = _meta(gb, state)["history"]
    assert hist[0]["question_ref"].endswith("#issuecomment-801")
    assert hist[0]["question_sha256"] == _sha(mirrored)
    assert hist[1]["question"] == defused and "question_ref" not in hist[1]
    assert hist[2]["text"] == lonely and "text_ref" not in hist[2]
    assert [gb._history_id(e) for e in hist[:len(before["history"])]] == \
        [gb._history_id(e) for e in before["history"]]
    assert q.get("GHI-1")["history"][0]["question"] == mirrored


def test_wt44_excerpts_are_re_excerpted_only_after_the_comment_verifies(tmp_path, monkeypatch):
    state, q, gb = _setup(tmp_path, monkeypatch)
    data = json.loads(state.read_text())
    issue = data["issues"][0]
    human, meta = gb._split_body(issue["body"])
    good, gone = "good " + "g" * 3000, "gone " + "x" * 3000
    old = []
    for i, text in enumerate((good, gone)):
        e = {"event": "answer", "at": f"2026-10-01T00:00:0{i}Z", "kind": "human"}
        url = f"{issue['url']}#issuecomment-70{i}"
        e.update(text=f"{text[:1000]} ... [{len(text)} chars; full text: {url}]",
                 text_len=len(text), text_sha256=_sha(text), text_ref=url)
        old.append(e)
    issue["comments"].append({"author": {"login": "watchtower"}, "body": good,
                              "url": f"{issue['url']}#issuecomment-700"})
    meta["history"] = old + meta["history"]
    issue["body"] = gb._body_with_metadata(human, meta)
    state.write_text(json.dumps(data))
    monkeypatch.setattr(gb, "_ISSUE_BODY_BUDGET_BYTES", len(issue["body"].encode()) - 500)

    q.comment("GHI-1", "a note", by="worker", session_id="worker-1")

    hist = _meta(gb, state)["history"]
    assert len(hist[0]["text"]) < gb._HISTORY_TEXT_INLINE_MAX + 200  # verified: shortened
    assert hist[0]["text_sha256"] == _sha(good)
    assert hist[1] == old[1]  # its comment is gone: left exactly as it was
    got = q.get("GHI-1")["history"]
    assert got[0]["text"] == good and not got[0].get("text_unresolved")
    assert got[1]["text_unresolved"] is True and got[1]["text"] == old[1]["text"]


def test_concurrent_answer_survives_our_compacting_write(tmp_path, monkeypatch):
    """A cross-host answer that lands right after our compacting edit is merged
    back exactly once, and our moved entries are not appended twice."""
    state, q, gb = _setup(tmp_path, monkeypatch)
    monkeypatch.setenv("FAKE_GH_BODY_MAX", str(GITHUB_CAP))
    _load_becky(state, gb)
    backend = q._github_backend_for_project("GHI")
    before = _meta(gb, state)

    def remote_answer(stale):
        human, meta = gb._split_body(stale)
        gb._append_history(meta, "answer", kind="human", worker="mac", text="fresh from the mac")
        return gb._body_with_metadata(human, meta)

    fired = _remote_writer(monkeypatch, backend, state, remote_answer)
    backend.comment("GHI-1", "local note", by="worker", session_id="worker-1")
    assert fired
    hist = _meta(gb, state)["history"]
    ids = [gb._history_id(e) for e in hist]
    assert len(ids) == len(set(ids)) == len(before["history"]) + 2
    assert [e.get("text") for e in hist if e.get("worker") == "mac"] == ["fresh from the mac"]
    assert len(_issue(state)["body"].encode()) <= GITHUB_CAP


def test_concurrent_fresh_answer_during_block_retry_keeps_both(tmp_path, monkeypatch):
    """A block retried after a failed edit, with an answer landing in between,
    still adopts its comment (same key) and keeps the answer."""
    state, q, gb = _setup(tmp_path, monkeypatch)
    question = "Q " + "q" * 800
    monkeypatch.setenv("FAKE_GH_FAIL", "issue edit=fail")
    with pytest.raises(gb.GitHubBackendError):
        q.block("GHI-1", session_id="worker-1", question=question)
    monkeypatch.delenv("FAKE_GH_FAIL")
    q.comment("GHI-1", "meanwhile", by="human")
    q.block("GHI-1", session_id="worker-1", question=question)
    assert len(_comments_with_text(state, gb, question)) == 1
    events = [e["event"] for e in _meta(gb, state)["history"]]
    assert events.count("block") == 1 and events.count("comment") == 1


# ------------------------------------------------------------------ reader
def test_reader_marks_altered_comment_unresolved_never_truncated_truth(tmp_path, monkeypatch):
    state, q, gb = _setup(tmp_path, monkeypatch)
    progress = "P " + "p" * 2000
    q.block("GHI-1", session_id="worker-1", question="ok?", progress=progress)
    data = json.loads(state.read_text())
    for c in data["issues"][0]["comments"]:
        if progress in gb._comment_texts(c):
            c["body"] = c["body"].replace("pppp", "pppX", 1)
    state.write_text(json.dumps(data))
    prog = [e for e in q.get("GHI-1")["history"] if e["event"] == "progress"][-1]
    assert prog["text_unresolved"] is True and prog["text"] != progress
    assert "full text:" in prog["text"]


def test_legacy_inline_history_reads_unchanged(tmp_path, monkeypatch):
    state, q, gb = _setup(tmp_path, monkeypatch)
    data = json.loads(state.read_text())
    issue = data["issues"][0]
    human, meta = gb._split_body(issue["body"])
    legacy = [{"event": "block", "at": "2026-01-01T00:00:00Z", "question": "q" * 2000},
              {"event": "progress", "at": "2026-01-01T00:00:01Z", "text": "p" * 9000}]
    meta["history"] = legacy + meta["history"]
    issue["body"] = gb._body_with_metadata(human, meta)
    state.write_text(json.dumps(data))
    assert q.get("GHI-1")["history"][:2] == legacy


def test_compaction_never_grows_an_entry(tmp_path, monkeypatch):
    """A mirrored question just over the excerpt length would grow by the
    reference keys (~290 bytes); compaction leaves it inline."""
    state, q, gb = _setup(tmp_path, monkeypatch)
    data = json.loads(state.read_text())
    issue = data["issues"][0]
    human, meta = gb._split_body(issue["body"])
    short = "s" * 450
    meta["history"] = [{"event": "block", "at": "2026-10-01T00:00:01Z", "worker": "w9",
                        "question": short}] + meta["history"]
    issue["comments"].append({"author": {"login": "watchtower"},
                              "url": f"{issue['url']}#issuecomment-801",
                              "body": f"[watchtower] Needs input from w9:\n\n{short}"})
    issue["body"] = gb._body_with_metadata(human, meta)
    state.write_text(json.dumps(data))
    monkeypatch.setattr(gb, "_ISSUE_BODY_BUDGET_BYTES", 10)
    monkeypatch.setattr(gb, "_ISSUE_BODY_MAX_BYTES", 10 ** 6)
    q.comment("GHI-1", "a note", by="worker", session_id="worker-1")
    first = _meta(gb, state)["history"][0]
    assert first["question"] == short and "question_ref" not in first

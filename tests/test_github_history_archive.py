"""Bounded GitHub issue-body history (WT-47, contract plan v3; tests numbered
as in its section 5). Offline: fake ``gh``, real write and read paths."""

from __future__ import annotations

import json
import random
import re
import time
from pathlib import Path

import pytest

from tests.test_github_backend import _drainable, _install_fake_gh, _reload_isolated
from tests.test_github_body_writes import _remote_writer
from watchtower import github_history_archive as gha

GITHUB_CAP = 262144
PERF_SLACK = 1   # the contract bound; raise only with a measurement
REPO = "test-owner/test-repo"


def _setup(tmp_path, monkeypatch, *, on=True):
    state = _install_fake_gh(tmp_path, monkeypatch)
    monkeypatch.setenv("WATCHTOWER_HISTORY_ARCHIVE_DIR", str(tmp_path / "archive-cache"))
    monkeypatch.setenv("FAKE_GH_BODY_MAX", str(GITHUB_CAP))
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", REPO)
    _drainable(config)
    if on:
        config.set_history_archive("GHI", True, "test", ["her"])
    item = q.enqueue(project="GHI", note="needs a decision")
    q.claim_by_ref(item["ref"], "worker-1", session_uuid="session-abc")
    import watchtower.github_backend as gb
    gb._ARCHIVE_MISSES.clear()
    gb._ARCHIVE_CACHE_MEMO.clear()
    return state, q, gb, config


def _issue(state):
    return json.loads(state.read_text())["issues"][0]


def _write_issue(state, **fields):
    data = json.loads(state.read_text())
    data["issues"][0].update(fields)
    state.write_text(json.dumps(data))


def _meta(gb, state):
    return gb._split_body(_issue(state)["body"])[1]


def _entry(i, **extra):
    return {"event": "progress", "at": f"2026-10-30T{i // 3600 % 24:02d}:{i // 60 % 60:02d}:"
            f"{i % 60:02d}Z", "worker": "w", "text": f"note {i:06d}", **extra}


def _seed(state, gb, entries):
    human, meta = gb._split_body(_issue(state)["body"])
    meta["history"] = list(meta.get("history") or []) + entries
    _write_issue(state, body=gb._body_with_metadata(human, meta))


def _archived(state, q, gb, count=600):
    """An issue with a real archive, produced by the real write path."""
    _seed(state, gb, [_entry(i) for i in range(count)])
    q.comment("GHI-1", "trigger", by="worker", session_id="worker-1")
    meta = _meta(gb, state)
    assert gha.index_of(meta), "archival did not run"
    return meta


def _segment_comments(state):
    return [c for c in _issue(state)["comments"] if gha.is_archive_comment(c["body"])]


def _degrade(state):
    """Delete the segment comments; the cache is the caller's to clear."""
    _write_issue(state, comments=[c for c in _issue(state)["comments"]
                                  if not gha.is_archive_comment(c["body"])])


def _clear_cache(tmp_path, gb):
    import shutil
    shutil.rmtree(tmp_path / "archive-cache", ignore_errors=True)
    gb._ARCHIVE_CACHE_MEMO.clear()
    gb._ARCHIVE_MISSES.clear()


def _capture_log(monkeypatch, q):
    logged = []
    real = q._log
    monkeypatch.setattr(q, "_log", lambda k, m, **kw: (logged.append(k), real(k, m, **kw))[1])
    return logged


def _witness():
    """2742 short entries, 262,225 bytes: the WT-45 verify's refusal."""
    return [_entry(i, text=f"n{i:05d}", pad="x" * 2) for i in range(2742)]


# ------------------------------------------------------------------ capacity
@pytest.mark.parametrize("op", ["block", "defer"])
def test_1_witness_block_and_defer_land(tmp_path, monkeypatch, op):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    entries = _witness()
    start = len(_meta(gb, state)["history"])
    _seed(state, gb, entries)
    assert len(_issue(state)["body"].encode()) > 250000
    if op == "block":
        q.block("GHI-1", session_id="worker-1", question="Grant?")
    else:
        q.defer("GHI-1", "worker-1", "2h", "waiting on data")
    meta = _meta(gb, state)
    body = len(_issue(state)["body"].encode())
    assert body < 30 * 1024
    assert meta["history_archive"]["n"] == 2550
    full = q.get("GHI-1")["history"]
    assert [e for e in full if e.get("worker") == "w"] == entries
    assert q.history_total(q.get("GHI-1")) == len(full) == start + 2742 + 1


def test_1_witness_refuses_with_archival_off(tmp_path, monkeypatch):
    state, q, gb, _c = _setup(tmp_path, monkeypatch, on=False)
    big = [_entry(i, pad="y" * 80) for i in range(2742)]
    _seed(state, gb, big)
    with pytest.raises(Exception, match="Nothing was written"):
        q.block("GHI-1", session_id="worker-1", question="Grant?")


def test_2_growth_stays_bounded(tmp_path, monkeypatch):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    written = 0
    for step in range(10):
        _seed(state, gb, [_entry(written + i) for i in range(1000)])
        written += 1000
        q.comment("GHI-1", f"step {step}", by="worker", session_id="worker-1")
        meta = _meta(gb, state)
        assert len(meta["history"]) <= gha.TAIL_MAX
        assert gha.tail_bytes(meta["history"]) <= gha.TAIL_MAX_BYTES
        assert len(_issue(state)["body"].encode()) <= gb._ISSUE_BODY_BUDGET_BYTES
    full = q.get("GHI-1")["history"]
    assert [e["text"] for e in full if e.get("worker") == "w"] == [
        f"note {i:06d}" for i in range(10000)]


def _saturated_entries():
    out = []
    for i in range(70):
        out.append({"event": "reopen", "orphan": True, "at": "2026-10-30T00:00:00Z",
                    "displaced_session_id": f"sid-{i}"})
    out.append({"event": "reopen", "reason": "worker gone", "at": "2026-10-30T00:00:01Z"})
    for i in range(70):
        out.append({"event": "claim", "at": "2026-10-30T00:00:02Z",
                    "by": {"session_id": f"re-{i}"}})
    for kind in gha.K_DIGEST:
        out.append({"event": kind, "at": "2026-10-30T00:00:03Z"})
    for i in range(300):
        out.append({"event": f"kind-{i}", "at": "2026-10-30T00:00:04Z"})
    out.append({"event": "block", "at": "2026-10-30T00:00:05Z", "machine": "h" * 250})
    return out


@pytest.mark.parametrize("host", [250, 4000])
def test_2b_saturated_index_stays_under_the_ceiling(host):
    entries = _saturated_entries()
    entries[-1]["machine"] = "h" * host
    digest = gha.build_digest(entries)
    for kind in digest["counts"]:
        digest["counts"][kind][0] = 9_999_999_999
    assert digest["displaced_overflow"] and digest["legacy_rebound_overflow"]
    assert len(digest["displaced"]) == 64 and len(digest["legacy_rebound"]) == 64
    assert not any(k.startswith("kind-") for k in digest["counts"])
    seg = gha._segment(1, 0, [{"event": "x"}], gha.root_zero(REPO, 1))
    idx = gha.build_index(9_999_999_999, seg, digest, "r" * 200)
    assert len(json.dumps(idx)) <= gha.INDEX_MAX_BYTES


def test_2b_saturated_digest_still_archives_every_step(tmp_path, monkeypatch):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    _seed(state, gb, _saturated_entries())
    q.comment("GHI-1", "go", by="worker", session_id="worker-1")
    last = _meta(gb, state)["history_archive"]["n"]
    for step in range(20):
        _seed(state, gb, [_entry(step * 150 + i) for i in range(150)])
        q.comment("GHI-1", f"step {step}", by="worker", session_id="worker-1")
        idx = _meta(gb, state)["history_archive"]
        assert idx["n"] > last and len(json.dumps(idx)) <= gha.INDEX_MAX_BYTES
        last = idx["n"]


def test_2b_kmin_floor_logs_and_the_budget_still_rules(tmp_path, monkeypatch):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    logged = _capture_log(monkeypatch, q)
    _seed(state, gb, [_entry(i, pad="k" * 11000) for i in range(25)])
    q.comment("GHI-1", "go", by="worker", session_id="worker-1")
    assert "GH-ARCHIVE-KMIN-FLOOR" in logged
    assert len(_issue(state)["body"].encode()) <= gb._ISSUE_BODY_BUDGET_BYTES
    assert len(_meta(gb, state)["history"]) >= gha.K_MIN


def test_2b_byte_trigger_and_floor(tmp_path, monkeypatch):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    _seed(state, gb, [_entry(i, pad="z" * 480) for i in range(280)])
    q.comment("GHI-1", "go", by="worker", session_id="worker-1")
    assert gha.tail_bytes(_meta(gb, state)["history"]) <= gha.TAIL_MAX_BYTES
    # Only K_MIN entries, each huge: the floor stops it, the budget still rules.
    move, floored = gha.choose_move([_entry(i, pad="z" * 9000) for i in range(gha.K_MIN)],
                                    100, 250000, 0)
    assert (move, floored) == (0, True)


def test_3_becky_shape_fits_without_unresolved_flags(tmp_path, monkeypatch):
    from tests.test_github_body_capacity import _grant, _seed_legacy
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    q.block("GHI-1", session_id="worker-1", question="Grant s16?")
    seeded = _seed_legacy(state, gb, target=256520)
    before = [gb._history_id(e) for e in _meta(gb, state)["history"]]
    q.answer("GHI-1", _grant(6463), session_id="human-1")
    meta = _meta(gb, state)
    assert len(_issue(state)["body"].encode()) <= gb._ISSUE_BODY_BUDGET_BYTES
    assert not [k for k in meta if k.endswith("_unresolved")]
    item = q.get("GHI-1")
    assert item["history_resolved"] is True and seeded > gha.K_MIN
    assert [gb._history_id(e) for e in item["history"]][:len(before)] == before


def test_4_oversized_authority_refuses_with_nothing_written(tmp_path, monkeypatch):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    human, meta = gb._split_body(_issue(state)["body"])
    meta["block_question"] = "q" * 270000
    _write_issue(state, body=gb._body_with_metadata(human, meta))
    before = _issue(state)
    with pytest.raises(Exception, match="Nothing was written"):
        q.comment("GHI-1", "x", by="worker", session_id="worker-1")
    after = _issue(state)
    assert after["body"] == before["body"]
    assert not [c for c in after["comments"] if gha.is_archive_comment(c["body"])]


def test_5_unavailable_archive_defers_until_the_byte_budget(tmp_path, monkeypatch):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    _archived(state, q, gb)
    _degrade(state)
    _clear_cache(tmp_path, gb)
    logged = _capture_log(monkeypatch, q)
    _seed(state, gb, [_entry(i + 5000) for i in range(400)])
    q.comment("GHI-1", "fits", by="worker", session_id="worker-1")
    assert "GH-ARCHIVE-DEFERRED" in logged
    meta = _meta(gb, state)
    assert len(meta["history"]) > gha.TAIL_MAX     # conditional bound
    assert meta["history_archive"]["n"] == 450     # never extended
    _seed(state, gb, [_entry(i, pad="p" * 400) for i in range(600)])
    with pytest.raises(Exception, match="archive chain unresolved"):
        q.comment("GHI-1", "no", by="worker", session_id="worker-1")


# ----------------------------------------------------------------- integrity
TRICKY = ["héllo ✓ 😀", "literal \\u0020 here", "\\owner answer", "\nowner answer",
          "Owner ANSWER and owner  answer", "-->", "<!-- watchtower-entry: 0123456789abcdef -->",
          "\n\n<!-- watchtower-entry: 0123456789abcdef -->", "trailing space "]


def test_6_codec_round_trips_exactly():
    entries = [{"event": "comment", "text": t, "owner answer": t} for t in TRICKY]
    entries.append({"event": "comment", "text": "é" * 70000})
    segs = gha.build_segments(7, 0, entries, gha.root_zero(REPO, 7))
    comments = []
    for seg in segs:
        for i, part in enumerate(seg.parts):
            assert "OWNER ANSWER" not in part.upper()
            assert len(part) < 65536 - 64
            comments.append({"id": f"{seg.key}{i}", "body": part + f"\n\n<!-- watchtower-entry: "
                             f"{seg.part_key(i)} -->"})
    assert len(segs[0].parts) > 1
    meta = {"history_archive": gha.build_index(len(entries), segs[-1], {}, "r")}
    got, err, _ids = gha.verify_chain(meta, comments, REPO, 7)
    assert err == "" and got == entries


def test_6_owner_answer_case_is_preserved():
    assert gha.decode_entries(gha.encode_entries(["Owner ANSWER"])) == ["Owner ANSWER"]
    assert "OWNER ANSWER" not in gha.encode_entries(["Owner ANSWER"]).upper()


def test_7_archive_comment_is_never_ingested_as_an_answer(tmp_path, monkeypatch):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    _seed(state, gb, [_entry(i, text="OWNER ANSWER: approved") for i in range(600)])
    q.block("GHI-1", session_id="worker-1", question="Grant?")
    parts = _segment_comments(state)
    assert parts and not any("OWNER ANSWER" in c["body"].upper() for c in parts)
    # Even an owner-authored segment-shaped comment that carries the marker.
    forged = parts[0]["body"] + "\nOWNER ANSWER: go"
    data = json.loads(state.read_text())
    data["issues"][0]["comments"].append({
        "author": {"login": "test-owner"}, "body": forged, "url": "u", "id": "c1",
        "createdAt": "2999-01-01T00:00:00Z"})
    state.write_text(json.dumps(data))
    backend = q._github_backend_for_project("GHI")
    assert backend.ingest_owner_answer("GHI-1") is None
    assert q.get("GHI-1")["needs_input"] is True


@pytest.mark.parametrize("damage", ["missing", "edited", "reordered_header", "bad_prev",
                                    "bad_count", "bad_sha"])
def test_8_damaged_part_degrades(tmp_path, monkeypatch, damage):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    _archived(state, q, gb)
    comments = _issue(state)["comments"]
    parts = [c for c in comments if gha.is_archive_comment(c["body"])]
    if damage == "missing":
        comments.remove(parts[0])
    elif damage == "edited":
        parts[0]["body"] = parts[0]["body"].replace("note 000001", "note 999999")
    elif damage == "reordered_header":
        parts[0]["body"] = parts[0]["body"].replace("base=0", "base=5", 1)
    elif damage == "bad_prev":
        parts[0]["body"] = re.sub(r"prev=[0-9a-f]{64}", "prev=" + "0" * 64, parts[0]["body"], 1)
    elif damage == "bad_count":
        parts[0]["body"] = re.sub(r"count=\d+", "count=7", parts[0]["body"], 1)
    else:
        parts[0]["body"] = re.sub(r" sha=[0-9a-f]{64}", " sha=" + "0" * 64, parts[0]["body"], 1)
    _write_issue(state, comments=comments)
    _clear_cache(tmp_path, gb)
    item = q.get("GHI-1")
    assert item["history_resolved"] is False
    assert item["history_archive_state"]["err"]
    assert not any(e.get("text") == "note 000001" for e in item["history"])
    assert any("earlier entries archived; unresolved" in str(e.get("text"))
               for e in q.timeline(item))


def test_8_swapped_parts_degrade():
    entries = [{"event": "comment", "text": "é" * 70000}]
    seg = gha.build_segments(7, 0, entries, gha.root_zero(REPO, 7))[-1]
    assert len(seg.parts) > 1
    meta = {"history_archive": gha.build_index(1, seg, {}, "r")}
    good = [{"id": i, "body": p} for i, p in enumerate(seg.parts)]
    assert gha.verify_chain(meta, list(reversed(good)), REPO, 7)[1] == ""  # listing order is free
    swapped = [{"id": 0, "body": seg.parts[0].replace("part=0/", "part=1/", 1)},
               {"id": 1, "body": seg.parts[1].replace("part=1/", "part=0/", 1)}]
    got, err, _ids = gha.verify_chain(meta, swapped, REPO, 7)
    assert got is None and err


def test_8_digest_mismatch_on_a_verified_chain_stays_resolved(tmp_path, monkeypatch):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    _archived(state, q, gb)
    human, meta = gb._split_body(_issue(state)["body"])
    meta["history_archive"]["dsha"] = "0" * 64
    _write_issue(state, body=gb._body_with_metadata(human, meta))
    assert q.get("GHI-1")["history_resolved"] is True
    _seed(state, gb, [_entry(i + 7000) for i in range(400)])
    q.comment("GHI-1", "again", by="worker", session_id="worker-1")
    idx = _meta(gb, state)["history_archive"]
    assert gha.digest_state(idx) == "ok"     # the next archival rewrites it


def _invalidate(gb, state, how):
    human, meta = gb._split_body(_issue(state)["body"])
    idx = meta["history_archive"]
    if how == "dsha":
        idx["dsha"] = "0" * 64
    elif how == "lowered_claim":
        idx["digest"]["counts"]["claim"] = [1, "2000-01-01T00:00:00Z"]
    elif how == "dropped_displaced":
        idx["digest"]["displaced"] = []
    elif how == "n":
        idx["n"] += 1
    elif how == "cv":
        idx["digest"]["cv"] = gha.CLASSIFIER_VERSION + 1
        idx["dsha"] = gha.digest_sha(idx)      # otherwise well formed
    _write_issue(state, body=gb._body_with_metadata(human, meta))


@pytest.mark.parametrize("how", ["dsha", "lowered_claim", "dropped_displaced", "n", "cv"])
def test_8b_invalid_digest_fails_closed_everywhere(tmp_path, monkeypatch, how):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    _seed(state, gb, [_reopen("S1", 1), {"event": "defer", "at": "2026-10-29T00:00:02Z"}])
    meta = _archived(state, q, gb)
    assert meta["history_archive"]["digest"]["displaced"]
    _degrade(state)
    _clear_cache(tmp_path, gb)
    _invalidate(gb, state, how)
    item = q.get("GHI-1")
    assert item["history_resolved"] is False
    assert item["history_archive_state"]["dstate"] == "invalid"
    assert q.history_digest(item) is None
    from watchtower import answers, deep, github_pipeline, health, messages
    assert any(k.startswith("?unresolved:") for k in github_pipeline.stale_readers([item]))
    assert messages._archive_may_displace(item, "someone", "open") is True
    assert q.history_last_at(item, "progress") == (None, False)
    assert q.history_count(item, "defer") == (0, False)
    assert health._stage_clock({**item, "history": []}) == "1970-01-01T00:00:00Z"
    assert answers._wait_host({**item, "history": []}, {}) == ""
    assert q.RETIRED_UNKNOWN in q.stage_retired_ids(item)
    tail_defer = {**item, "depth": "deep",
                  "history": item["history"] + [{"event": "defer", "until": "x"}]}
    assert ">=1x (archive unresolved)" in deep.deep_brief(tail_defer)
    idx_n = _meta(gb, state)["history_archive"]["n"]
    view = gha.ArchiveView(idx_n, False, False, [], "gone")
    assert gha.next_key_index("a" * 12, [], view) == idx_n     # keys are unchanged


def test_9_corrupt_older_segment_behind_a_valid_head(tmp_path, monkeypatch):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    _archived(state, q, gb)
    _seed(state, gb, [_entry(i + 8000) for i in range(400)])
    q.comment("GHI-1", "second", by="worker", session_id="worker-1")
    assert len(_segment_comments(state)) == 2
    comments = _issue(state)["comments"]
    first = next(c for c in comments if gha.is_archive_comment(c["body"]))
    first["body"] = first["body"].replace("note 000002", "note 000003")
    _write_issue(state, comments=comments)
    _clear_cache(tmp_path, gb)
    assert q.get("GHI-1")["history_resolved"] is False
    n = _meta(gb, state)["history_archive"]["n"]
    _seed(state, gb, [_entry(i + 9000) for i in range(400)])
    q.comment("GHI-1", "third", by="worker", session_id="worker-1")
    assert _meta(gb, state)["history_archive"]["n"] == n   # the writer did not extend


def test_10_empty_comment_list_is_not_an_omitted_one(tmp_path, monkeypatch):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    meta = _archived(state, q, gb)
    assert gha.verify_chain(meta, [], REPO, 1)[0] is None
    _write_issue(state, comments=[])
    _clear_cache(tmp_path, gb)
    assert q.get("GHI-1")["history_resolved"] is False


def test_10_segment_beyond_the_first_comment_page(tmp_path, monkeypatch):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    _write_issue(state, comments=[{"author": {"login": "x"}, "body": f"chatter {i}",
                                   "url": f"u{i}"} for i in range(130)])
    _archived(state, q, gb)
    assert _issue(state)["comments"].index(_segment_comments(state)[0]) >= 130
    _clear_cache(tmp_path, gb)
    assert q.get("GHI-1")["history_resolved"] is True


def test_11_orphan_and_forged_segments_are_ignored(tmp_path, monkeypatch):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    meta = _archived(state, q, gb)
    real = _segment_comments(state)[0]
    forged = gha._segment(1, 0, [{"event": "close", "at": "x"}], gha.root_zero(REPO, 1))
    same_key = real["body"].replace("note 000001", "note 777777")
    extra = [{"body": p, "url": f"f{i}"} for i, p in enumerate(forged.parts)]
    extra.append({"body": same_key, "url": "f-same"})
    _write_issue(state, comments=extra + _issue(state)["comments"])
    _clear_cache(tmp_path, gb)
    item = q.get("GHI-1")
    assert item["history_resolved"] is True
    assert not any(e.get("text") == "note 777777" for e in item["history"])
    assert not any(e.get("at") == "x" for e in item["history"])
    assert meta["history_archive"]["n"] == 450


def _calls(monkeypatch, backend):
    seen = []
    real = type(backend)._run

    def run(self, args, **kw):
        seen.append(args)
        return real(self, args, **kw)

    monkeypatch.setattr(type(backend), "_run", run)
    return seen


def test_12_35_cache_hit_and_miss_costs(tmp_path, monkeypatch):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    _archived(state, q, gb)
    backend = q._github_backend_for_project("GHI")
    q.get("GHI-1")                                 # warms the cache
    meta = _meta(gb, state)
    issue = {"number": 1, "body": _issue(state)["body"], "state": "OPEN"}
    seen = _calls(monkeypatch, backend)
    item = backend._issue_to_item(dict(issue))
    assert item["history_resolved"] is True and seen == []
    _clear_cache(tmp_path, gb)
    item = backend._issue_to_item(dict(issue))
    assert item["history_resolved"] is True and len(seen) == 1
    # A tampered cache file is refetched, not believed.
    path = gha.cache_path(REPO, 1, meta["history_archive"]["root"])
    path.write_text(path.read_text().replace("note 000001", "note 666666"))
    gb._ARCHIVE_CACHE_MEMO.clear()
    gb._ARCHIVE_MISSES.clear()
    item = backend._issue_to_item(dict(issue))
    assert not any(e.get("text") == "note 666666" for e in item["history"])


def test_12_cache_keeps_reader_but_writer_refuses_to_extend(tmp_path, monkeypatch):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    _archived(state, q, gb)
    q.get("GHI-1")
    _degrade(state)
    item = q.get("GHI-1")
    assert item["history_resolved"] is True        # the verified cache reads
    n = _meta(gb, state)["history_archive"]["n"]
    _seed(state, gb, [_entry(i + 3000) for i in range(400)])
    q.comment("GHI-1", "x", by="worker", session_id="worker-1")
    assert _meta(gb, state)["history_archive"]["n"] == n


# ----------------------------------------------------------------- authority
def test_13_14_archival_touches_only_history_and_never_own_entries(tmp_path, monkeypatch):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    _seed(state, gb, [_entry(i) for i in range(600)])
    before = _meta(gb, state)
    backend = q._github_backend_for_project("GHI")
    item = backend.get("GHI-1")
    human, meta = gb._split_body(item["_github_body"])
    own = [_entry(10_000 + i) for i in range(40)]
    meta["history"] += own
    backend._edit_body(item, human, meta)
    after = _meta(gb, state)
    keys = set(before) | set(after)
    assert {k for k in keys if before.get(k) != after.get(k)} == {"history", "history_archive"}
    assert after["history"][-40:] == own


# ---------------------------------------------------------------------- keys
def test_15_resolved_keys_continue_across_archival():
    base = "a" * 12
    view = gha.ArchiveView(2, True, True, [{"entry_key": base + "0000"},
                                           {"entry_key": base + "0001"}])
    assert gha.next_key_index(base, [], view) == 2
    assert gha.next_key_index(base, [{"entry_key": base + "0002"}], view) == 3


def test_15_never_archived_keys_equal_a(tmp_path, monkeypatch):
    _state, _q, gb, _c = _setup(tmp_path, monkeypatch)
    meta = {"history": [{"entry_key": "x"}]}
    key = gb._entry_key("1", "progress", meta, "words", gha.ArchiveView())
    assert key.endswith("0000") and len(key) == 16


def test_15_same_text_after_archival_is_a_new_comment(tmp_path, monkeypatch):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    q.comment("GHI-1", "same words", by="worker", session_id="worker-1")
    _archived(state, q, gb)
    q.comment("GHI-1", "same words", by="worker", session_id="worker-1")
    history = q.get("GHI-1")["history"]
    keys = [e["entry_key"] for e in history if e.get("text") == "same words"]
    assert len(keys) == len(set(keys)) == 2


def test_15_retry_across_an_archival_adopts_its_comment(tmp_path, monkeypatch):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    words = "w" * 3000
    monkeypatch.setenv("FAKE_GH_FAIL", "issue edit=fail")
    with pytest.raises(Exception):
        q.comment("GHI-1", words, by="worker", session_id="worker-1")
    monkeypatch.delenv("FAKE_GH_FAIL")
    _archived(state, q, gb)
    q.comment("GHI-1", words, by="worker", session_id="worker-1")
    assert sum(words in c["body"] for c in _issue(state)["comments"]) == 1


def test_16_unresolved_high_water_exceeds_every_landed_index():
    base = "a" * 12
    view = gha.ArchiveView(377, False, False, [], "gone")
    assert gha.next_key_index(base, [], view) == 377
    assert gha.next_key_index(base, [{"entry_key": f"{base}{377:04x}"}], view) == 378
    # The round-1 pair: a total of 122 with a landed index of 377.
    low = gha.ArchiveView(122, False, False, [], "gone")
    assert gha.next_key_index(base, [{"entry_key": f"{base}{377:04x}"}], low) == 378
    # Archived occurrence k < n can never be picked, so never adopted.
    assert all(gha.next_key_index(base, [], gha.ArchiveView(k + 1, False, False, [], "x")) > k
               for k in range(400))


def test_16b_reviewer_case_and_property():
    a, b = "a" * 12, "aa" + "b" * 10
    archived = [{"entry_key": a + "0000"}, {"entry_key": b + "0000"}, {"entry_key": b + "0001"}]
    tail = []
    got = []
    for step, mode in enumerate(("degraded", "resolved", "degraded", "resolved")):
        if step == 2:                                   # an archival in between
            archived, tail = archived + tail, []
        view = (gha.ArchiveView(len(archived), True, True, archived) if mode == "resolved"
                else gha.ArchiveView(len(archived), False, False, [], "x"))
        i = gha.next_key_index(a, tail, view)
        got.append(i)
        tail.append({"entry_key": f"{a}{i:04x}"})
    assert len(set(got)) == 4 and 0 not in got
    rng = random.Random(47)
    bases = ["a" * 12, "aa" + "b" * 10, "ab" + "c" * 10]
    for _ in range(300):
        archive, tail, landed = [], [], set()
        for _step in range(rng.randint(5, 120)):
            action = rng.random()
            base = rng.choice(bases)
            if action < 0.12 and len(tail) > 3:
                cut = rng.randint(1, len(tail) - 1)
                archive, tail = archive + tail[:cut], tail[cut:]
                continue
            if action < 0.25:
                tail.append({"event": "x"})
                continue
            resolved_mode = rng.random() < 0.5
            view = gha.ArchiveView(len(archive), resolved_mode, resolved_mode,
                                   archive if resolved_mode else [])
            i = gha.next_key_index(base, tail, view)
            key = f"{base}{i:04x}"
            assert key not in landed
            assert i <= len(archive) + len(tail)      # invariant I
            if rng.random() < 0.85:                   # else: the edit was lost
                landed.add(key)
                tail.append({"entry_key": key})


def test_17_entry_key_requires_a_view(tmp_path, monkeypatch):
    _state, _q, gb, _c = _setup(tmp_path, monkeypatch)
    with pytest.raises(TypeError):
        gb._entry_key("1", "progress", {}, "words")


def test_unparsable_index_refuses(tmp_path, monkeypatch):
    _state, _q, gb, _c = _setup(tmp_path, monkeypatch)
    base = gb._text_sha256("1|progress||words")[:12]
    meta = {"history": [{"entry_key": base + "zzzz"}]}
    with pytest.raises(gb.GitHubBackendError, match="archive index invalid"):
        gb._entry_key("1", "progress", meta, "words", gha.ArchiveView())


# ------------------------------------------------------- merge, mixed version
def _bodies(gb, base_hist, ours_hist, fresh_hist, b=None, o=None, f=None):
    def mk(hist, idx):
        meta = {"history": hist}
        if idx:
            meta["history_archive"] = idx
        return gb._body_with_metadata("human", meta)
    return mk(base_hist, b), mk(ours_hist, o), mk(fresh_hist, f)


def _idx(n, root="r"):
    return {"v": 1, "n": n, "root": root, "head": {"seg": root}}


def test_18_r1_we_archived(tmp_path, monkeypatch):
    _s, _q, gb, _c = _setup(tmp_path, monkeypatch)
    e = [_entry(i) for i in range(6)]
    new, theirs = _entry(50), _entry(60)
    args = _bodies(gb, e, e[2:] + [new], e + [theirs], o=_idx(2))
    merged, _ = gb._merge_body_write(*args)
    m = gb._split_body(merged)[1]
    assert m["history_archive"]["n"] == 2 and m["history"] == e[2:] + [theirs, new]
    # The prefix moved under us: our archival drops whole, the rest re-applies.
    args = _bodies(gb, e, e[2:] + [new], e[1:] + [theirs], o=_idx(2))
    merged, _ = gb._merge_body_write(*args)
    m = gb._split_body(merged)[1]
    assert "history_archive" not in m and m["history"] == e[1:] + [theirs, new]


def test_18_r2_they_archived_nothing_reinlined(tmp_path, monkeypatch):
    _s, _q, gb, _c = _setup(tmp_path, monkeypatch)
    e = [_entry(i) for i in range(6)]
    new = _entry(50)
    args = _bodies(gb, e, e + [new], e[3:], f=_idx(3))
    merged, _ = gb._merge_body_write(*args)
    m = gb._split_body(merged)[1]
    assert m["history_archive"]["n"] == 3 and m["history"] == e[3:] + [new]


def test_18_r3_both_archived_theirs_stands(tmp_path, monkeypatch):
    _s, _q, gb, _c = _setup(tmp_path, monkeypatch)
    e = [_entry(i) for i in range(6)]
    new = _entry(50)
    args = _bodies(gb, e, e[2:] + [new], e[3:], o=_idx(2, "ours"), f=_idx(3, "theirs"))
    merged, conflicts = gb._merge_body_write(*args)
    m = gb._split_body(merged)[1]
    assert m["history_archive"]["root"] == "theirs" and "history_archive" in conflicts
    assert m["history"] == e[3:] + [new]


def test_18_r4_never_lowers_or_removes(tmp_path, monkeypatch):
    _s, _q, gb, _c = _setup(tmp_path, monkeypatch)
    e = [_entry(i) for i in range(6)]
    new = _entry(50)
    args = _bodies(gb, e, e + [new], e, b=_idx(9), o=_idx(9))   # index gone, tail intact
    merged, _ = gb._merge_body_write(*args)
    assert gb._split_body(merged)[1]["history_archive"]["n"] == 9


def test_18_claimed_by_conflict_reapplies_nothing(tmp_path, monkeypatch):
    _s, _q, gb, _c = _setup(tmp_path, monkeypatch)
    e = [_entry(i) for i in range(6)]
    mk = lambda who, hist, idx=None: gb._body_with_metadata("h", {  # noqa: E731
        "claimed_by": who, "history": hist, **({"history_archive": idx} if idx else {})})
    merged, conflicts = gb._merge_body_write(mk("a", e), mk("b", e[2:], _idx(2)), mk("c", e))
    assert merged is None and "claimed_by" in conflicts


def test_19_boundary_changes_inside_the_reapply_loop(tmp_path, monkeypatch):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    _seed(state, gb, [_entry(i) for i in range(600)])
    backend = q._github_backend_for_project("GHI")

    def theirs(stale):
        human, meta = gb._split_body(stale)
        current = gb._split_body(pre_archive)[1]
        current["history"] = current["history"] + [_entry(70_000 + i) for i in range(300)]
        return gb._body_with_metadata(human, current)

    pre_archive = _issue(state)["body"]
    _remote_writer(monkeypatch, backend, state, theirs)
    q.comment("GHI-1", "go", by="worker", session_id="worker-1")
    final = q.get("GHI-1")
    assert final["history_resolved"] is True
    texts = [e.get("text") for e in final["history"]]
    assert texts.count("note 000000") == 1 and "go" in " ".join(map(str, texts))
    assert texts.count("note 070299") == 1


def test_19_publish_failure_in_the_reapply_loop_refuses_explicitly(tmp_path, monkeypatch):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    _seed(state, gb, [_entry(i, pad="b" * 700) for i in range(290)])
    backend = q._github_backend_for_project("GHI")
    real = type(backend)._post_keyed_comment

    def post(self, item, key, text):
        if gha.is_archive_comment(text):
            raise gb.GitHubBackendError("no")
        return real(self, item, key, text)

    def theirs(stale):
        monkeypatch.setattr(type(backend), "_post_keyed_comment", post)
        human, meta = gb._split_body(pre)
        meta["history"] += [_entry(70_000 + i, pad="b" * 700) for i in range(290)]
        meta["claimed_by_note"] = "theirs"
        return gb._body_with_metadata(human, meta)

    pre = _issue(state)["body"]
    _remote_writer(monkeypatch, backend, state, theirs)
    with pytest.raises(gb.GitHubBackendError, match="re-apply refused: archive publish failed"):
        q.comment("GHI-1", "mine", by="worker", session_id="worker-1")


@pytest.mark.parametrize("old_host", [False, True])
def test_20_22_delayed_overwrite_is_detected_and_segments_adopted(tmp_path, monkeypatch, old_host):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    extra = [{"event": "claim", "at": "2026-01-01T00:00:00Z", "machine": "gone"}] if old_host else []
    _seed(state, gb, extra + [_entry(i) for i in range(600)])
    snapshot = _issue(state)["body"]
    q.comment("GHI-1", "archives", by="worker", session_id="worker-1")
    assert gha.index_of(_meta(gb, state))
    posted = len(_segment_comments(state))
    logged = _capture_log(monkeypatch, q)
    backend = q._github_backend_for_project("GHI")
    _remote_writer(monkeypatch, backend, state, lambda stale: snapshot)
    q.comment("GHI-1", "again", by="worker", session_id="worker-1")
    assert "GH-ARCHIVE-REVERTED" in logged
    history = q.get("GHI-1")["history"]
    assert sum(e.get("text") == "note 000000" for e in history) == 1
    assert sum(str(e.get("text")).endswith("again") for e in history) == 1
    assert len(_segment_comments(state)) == posted    # adopted, not reposted


def test_20_delayed_revert_logs(tmp_path, monkeypatch):
    _s, q, gb, _c = _setup(tmp_path, monkeypatch)
    logged = []
    monkeypatch.setattr(q, "_log", lambda k, m, **kw: logged.append(k))
    e = [_entry(i) for i in range(8)]
    args = _bodies(gb, e[4:], e[4:] + [_entry(50)], e, b=_idx(4), o=_idx(4))
    merged, _ = gb._merge_body_write(*args)
    m = gb._split_body(merged)[1]
    assert "GH-ARCHIVE-REVERTED" in logged
    assert "history_archive" not in m           # fresh's inline copy stands, not doubled
    assert m["history"] == e + [_entry(50)]


def test_21_legacy_writer_after_archival_keeps_the_index(tmp_path, monkeypatch):
    state, q, gb, config = _setup(tmp_path, monkeypatch)
    meta = _archived(state, q, gb)
    config.set_history_archive("GHI", False)
    q.comment("GHI-1", "after", by="worker", session_id="worker-1")
    assert _meta(gb, state)["history_archive"] == meta["history_archive"]


@pytest.mark.parametrize("kind", ["answer", "claim", "defer", "close"])
def test_23_concurrent_write_is_reapplied_once(tmp_path, monkeypatch, kind):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    _seed(state, gb, [_entry(i) for i in range(600)])
    backend = q._github_backend_for_project("GHI")
    other = {"event": kind, "at": "2026-10-31T00:00:00Z", "text": "from elsewhere"}

    def theirs(stale):
        human, meta = gb._split_body(pre)
        meta["history"].append(other)
        return gb._body_with_metadata(human, meta)

    pre = _issue(state)["body"]
    _remote_writer(monkeypatch, backend, state, theirs)
    q.comment("GHI-1", "mine", by="worker", session_id="worker-1")
    history = q.get("GHI-1")["history"]
    assert sum(e == other for e in history) == 1
    assert sum(e.get("text") == "note 000000" for e in history) == 1
    assert sum("mine" in str(e.get("text")) for e in history) == 1


def test_lost_body_edit_adopts_its_parts(tmp_path, monkeypatch):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    _seed(state, gb, [_entry(i) for i in range(600)])
    monkeypatch.setenv("FAKE_GH_FAIL", "issue edit=fail")
    with pytest.raises(Exception):
        q.comment("GHI-1", "go", by="worker", session_id="worker-1")
    monkeypatch.delenv("FAKE_GH_FAIL")
    posted = len(_segment_comments(state))
    assert posted == 1 and not gha.index_of(_meta(gb, state))
    q.comment("GHI-1", "go", by="worker", session_id="worker-1")
    assert len(_segment_comments(state)) == posted
    assert gha.index_of(_meta(gb, state))


def test_failed_publish_writes_without_archival(tmp_path, monkeypatch):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    _seed(state, gb, [_entry(i) for i in range(600)])
    backend = q._github_backend_for_project("GHI")
    real = type(backend)._post_keyed_comment

    def post(self, item, key, text):
        if gha.is_archive_comment(text):
            raise gb.GitHubBackendError("no")
        return real(self, item, key, text)

    monkeypatch.setattr(type(backend), "_post_keyed_comment", post)
    q.comment("GHI-1", "still lands", by="worker", session_id="worker-1")
    assert not gha.index_of(_meta(gb, state))
    assert len(_meta(gb, state)["history"]) > 600


# ------------------------------------------------------------------- readers
def _control_pair(tmp_path, monkeypatch, entries):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    _seed(state, gb, entries)
    control = dict(q.get("GHI-1"))
    q.comment("GHI-1", "go", by="worker", session_id="worker-1")
    return state, q, gb, control


def test_24_resolved_consumers_match_a_never_archived_control(tmp_path, monkeypatch):
    entries = [_entry(i) for i in range(500)]
    entries[3] = {"event": "reopen", "orphan": True, "displaced_session_id": "S1",
                  "at": "2026-10-30T00:00:03Z"}
    entries[4] = {"event": "stage_retired", "worker_id": "old-stage",
                  "at": "2026-10-30T00:00:04Z"}
    entries[5] = {"event": "defer", "until": "x", "at": "2026-10-30T00:00:05Z"}
    entries[6] = {"event": "answer", "text": "a", "at": "2026-10-30T00:00:06Z"}
    entries[7] = {"event": "stage_spawn", "at": "2026-10-30T00:00:07Z"}
    state, q, gb, control = _control_pair(tmp_path, monkeypatch, entries)
    item = q.get("GHI-1")
    assert gha.index_of(_meta(gb, state))
    assert item["history"][:len(control["history"])] == control["history"]
    assert q.stage_retired_ids(item) == q.stage_retired_ids(control)
    assert q.history_count(item, "defer") == (1, True)
    from watchtower import health
    assert health._last_event_at(item) == health._last_event_at(control) is not None
    assert health._stage_clock(item) == health._stage_clock(control)
    listed = next(i for i in q.list_items(project="GHI") if i["ref"] == "GHI-1")
    assert listed["history"] == item["history"] and listed["history_resolved"] is True


def _degraded_item(q, gb, state, tmp_path):
    _degrade(state)
    _clear_cache(tmp_path, gb)
    return q.get("GHI-1")


def _displacement_case(tmp_path, monkeypatch, entries, sid, expect_guard, tail=()):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    _seed(state, gb, entries + [_entry(i) for i in range(400)] + list(tail))
    q.comment("GHI-1", "go", by="worker", session_id="worker-1")
    assert gha.index_of(_meta(gb, state))
    from watchtower import messages
    resolved = q.get("GHI-1")
    monkeypatch.setattr(q, "list_items", lambda *a, **k: [resolved])
    assert bool(messages._displaced_claim_for_session(sid)) is expect_guard
    _degraded_item_raw(q, gb, state, tmp_path, monkeypatch)
    assert bool(messages._displaced_claim_for_session(sid)) is expect_guard


def _degraded_item_raw(q, gb, state, tmp_path, monkeypatch):
    real_get = q.get
    _degrade(state)
    _clear_cache(tmp_path, gb)
    item = real_get("GHI-1")
    assert item["history_resolved"] is False
    monkeypatch.setattr(q, "list_items", lambda *a, **k: [item])
    return item


def _reopen(sid, i):
    return {"event": "reopen", "orphan": True, "displaced_session_id": sid,
            "at": f"2026-10-29T00:00:{i:02d}Z"}


def test_25a_two_displacements_both_guarded(tmp_path, monkeypatch):
    _displacement_case(tmp_path, monkeypatch, [_reopen("S1", 1), _reopen("S2", 2)], "S1", True)


def test_25a_second_sid(tmp_path, monkeypatch):
    _displacement_case(tmp_path, monkeypatch, [_reopen("S1", 1), _reopen("S2", 2)], "S2", True)


def test_25b_rebound_inside_the_archive_clears(tmp_path, monkeypatch):
    rebind = {"event": "claim", "at": "2026-10-29T00:00:05Z", "by": {"session_id": "S1"}}
    _displacement_case(tmp_path, monkeypatch, [_reopen("S1", 1), rebind], "S1", False)


def test_25b_tail_displacement_still_guards(tmp_path, monkeypatch):
    rebind = {"event": "claim", "at": "2026-10-29T00:00:05Z", "by": {"session_id": "S1"}}
    _displacement_case(tmp_path, monkeypatch, [_reopen("S1", 1), rebind], "S2", True,
                       tail=[_reopen("S2", 9)])


def test_25c_legacy_reopen_with_other_session_rebind(tmp_path, monkeypatch):
    legacy = {"event": "reopen", "reason": "worker gone", "at": "2026-10-29T00:00:01Z"}
    rebind = {"event": "claim", "at": "2026-10-29T00:00:05Z", "by": {"session_id": "other"}}
    digest = gha.build_digest([legacy, rebind])
    assert digest["legacy_reopen"] == 0
    assert digest["legacy_rebound"] == [gha.sid_hash("other")]


def test_25d_overflowed_sid_is_guarded_without_a_tail_rebind(tmp_path, monkeypatch):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    _seed(state, gb, [_reopen(f"sid-{i}", i % 60) for i in range(70)]
          + [_entry(i) for i in range(400)])
    q.comment("GHI-1", "go", by="worker", session_id="worker-1")
    item = _degraded_item(q, gb, state, tmp_path)
    assert item["history_archive_state"]["digest"]["displaced_overflow"]
    from watchtower import messages
    assert messages._archive_may_displace(item, "sid-0", "open") is True
    item["history"].append({"event": "claim", "by": {"session_id": "sid-0"}})
    assert messages._archive_may_displace(item, "sid-0", "open") is False


def test_26_retired_resolved_and_unknown_degraded(tmp_path, monkeypatch):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    _seed(state, gb, [{"event": "stage_retired", "worker_id": f"st-{i}",
                       "at": "2026-10-29T00:00:00Z"} for i in range(70)]
          + [_entry(i) for i in range(400)])
    q.comment("GHI-1", "go", by="worker", session_id="worker-1")
    ids = q.stage_retired_ids(q.get("GHI-1"))
    assert {f"st-{i}" for i in range(70)} <= ids and q.RETIRED_UNKNOWN not in ids
    assert q.RETIRED_UNKNOWN in q.stage_retired_ids(_degraded_item(q, gb, state, tmp_path))


@pytest.mark.parametrize("age_days,refused", [(1, True), (40, False)])
def test_27_stale_readers_degraded(tmp_path, monkeypatch, age_days, refused):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - age_days * 86400))
    human, meta = gb._split_body(_issue(state)["body"])
    for h in meta["history"]:
        if h.get("event") == "claim":
            h["at"] = at
    _write_issue(state, body=gb._body_with_metadata(human, meta))
    _seed(state, gb, [{"event": "claim", "at": at, "machine": f"host-{i}",
                       "hist_proto": gha.HIST_PROTO} for i in range(20)]
          + [_entry(i) for i in range(400)])
    q.comment("GHI-1", "go", by="worker", session_id="worker-1")
    assert gha.index_of(_meta(gb, state))
    from watchtower import github_pipeline
    resolved = github_pipeline.stale_readers([q.get("GHI-1")])
    assert {f"host-{i}" for i in range(20)} <= set(resolved) if refused else not resolved
    item = _degraded_item(q, gb, state, tmp_path)
    assert not [h for h in item["history"] if h.get("event") == "claim"]
    got = github_pipeline.stale_readers([item])
    assert any(k.startswith("?unresolved:") for k in got) is refused


def test_28_29_digest_counts(tmp_path, monkeypatch):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    _seed(state, gb, [{"event": "defer", "at": "2026-10-29T00:00:00Z", "until": "x"}
                      for _ in range(10)]
          + [{"event": f"kind-{i}", "at": "2026-10-29T00:00:00Z"} for i in range(100)]
          + [_entry(i) for i in range(400)])
    q.comment("GHI-1", "go", by="worker", session_id="worker-1")
    from watchtower import deep
    resolved = q.get("GHI-1")
    assert q.history_count(resolved, "defer") == (10, True)     # the view adds no digest
    assert "deferred 10x" in deep.deep_brief({**resolved, "depth": "deep"})
    item = _degraded_item(q, gb, state, tmp_path)
    assert q.history_count(item, "defer") == (10, True)
    assert q.history_count(item, "kind-3") == (0, False)
    assert q.history_count(item, "defer", lambda h: True) == (0, False)
    assert "deferred 10x" in deep.deep_brief({**item, "depth": "deep"})


def test_30_totals_match_control_with_overlay(tmp_path, monkeypatch):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    _archived(state, q, gb)
    resolved = q.get("GHI-1")
    total = q.history_total(resolved)
    from watchtower import liveness
    print_before = liveness.fingerprint(resolved, {})
    item = _degraded_item(q, gb, state, tmp_path)
    assert q.history_total(item) == total
    assert liveness.fingerprint(item, {}) == print_before
    item["history_overlay"] = 2
    item["history"] = item["history"] + [{"event": "x"}, {"event": "y"}]
    assert q.history_issue_total(item) == total


def test_30b_usage_route_carries_history_base(tmp_path, monkeypatch):
    from watchtower import usage
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    seen = {}

    def callback(tracking, *_a):
        seen["base"] = tracking.get("history_base")

    item = {"ref": "GHI-1", "history": [{}] * 193, "history_base": 2550,
            "_usage_namespace": "t"}
    usage.capture(item, callback, {"event": "claim"})
    assert seen["base"] == 2550
    tracking = {"history": [{}] * 193, "history_base": 2550, "claimed_by": "w"}
    usage.on_event(tracking, {"event": "claim", "at": "2026-10-31T00:00:00Z", "by": {}})
    runs = [a["run"] for a in tracking["token_usage"]["attempts"]]
    assert runs == ["claim:2743"]


def test_30b_claim_run_ids_end_to_end(tmp_path, monkeypatch):
    from watchtower import usage
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    runs = []
    real = usage.begin
    monkeypatch.setattr(usage, "begin", lambda *a, **k: (runs.append(k.get("run")),
                                                         real(*a, **k))[1])

    def reclaim():
        q.release("GHI-1", session_id="worker-1")
        q.claim_by_ref("GHI-1", "worker-1", session_uuid="session-abc")

    _archived(state, q, gb)
    reclaim()
    first = runs[-1]
    assert int(first.split(":")[1]) >= 450 + 150
    _seed(state, gb, [_entry(i + 5000) for i in range(400)])
    q.comment("GHI-1", "more", by="worker", session_id="worker-1")
    reclaim()
    assert runs[-1] != first and int(runs[-1].split(":")[1]) > int(first.split(":")[1])


def test_31_local_items_are_untouched():
    from watchtower import queue as q
    item = {"history": [{"event": "defer"}]}
    assert q.history_total(item) == 1 and q.history_count(item, "defer") == (1, True)
    assert q.history_digest(item) is None and q.RETIRED_UNKNOWN not in q.stage_retired_ids(item)


# Each hit audited: usage carries history_base; github_duplicates refuses when
# degraded; queue's are history_total, the resolved-only duplicate check and the
# local-only sent_back (the GitHub backend never writes one); github_pipeline
# counts its overlay; owner_wait_init's prefix compare refuses a partial view.
ALLOWED = {"usage.py": 1, "github_duplicates.py": 2, "queue.py": 3,
           "github_pipeline.py": 1, "owner_wait_init.py": 2}


def test_32_no_unaudited_history_length_or_slice():
    """A new reader of history length or position must face the archive: the
    file-by-file count here is the audit (plan §1.7)."""
    root = Path(__file__).parent.parent / "watchtower"
    pattern = re.compile(r"""len\((?:\w+\.get\(['"]history['"]\)|\w+\[['"]history['"]\])"""
                         r"""|\bhistory\[""")
    skip = {"github_backend.py", "github_history_archive.py"}
    found = {}
    for path in root.glob("*.py"):
        if path.name in skip:
            continue
        hits = len(pattern.findall(path.read_text()))
        if hits:
            found[path.name] = hits
    assert found == ALLOWED


# ------------------------------------------------------------------- rollout
def test_33_flag_off_or_unattested_never_archives(tmp_path, monkeypatch):
    state, q, gb, config = _setup(tmp_path, monkeypatch, on=False)
    _seed(state, gb, [_entry(i) for i in range(600)])
    q.comment("GHI-1", "go", by="worker", session_id="worker-1")
    assert not gha.index_of(_meta(gb, state)) and not _segment_comments(state)
    with pytest.raises(ValueError):
        config.set_history_archive("GHI", True)
    data = config._load()
    data["GHI"]["history_archive"] = True      # a hand edit is not an attestation
    config._save(data)
    assert config.history_archive_on("GHI") is False


def test_33_stale_host_claim_stops_archival(tmp_path, monkeypatch):
    state, q, gb, _c = _setup(tmp_path, monkeypatch)
    at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    _seed(state, gb, [{"event": "claim", "at": at, "machine": "old-host"}]
          + [_entry(i) for i in range(600)])
    q.comment("GHI-1", "go", by="worker", session_id="worker-1")
    assert not gha.index_of(_meta(gb, state))


def test_34_reader_works_with_the_flag_off(tmp_path, monkeypatch):
    state, q, gb, config = _setup(tmp_path, monkeypatch)
    _archived(state, q, gb)
    config.set_history_archive("GHI", False)
    item = q.get("GHI-1")
    assert item["history_resolved"] is True
    assert sum(e.get("worker") == "w" for e in item["history"]) == 600


def _best_of(rounds, run):
    best = float("inf")
    for _ in range(rounds):
        start = time.perf_counter()
        run()
        best = min(best, time.perf_counter() - start)
    return best


def test_35_planning_and_verifying_10k_is_fast():
    """The steady state of a write at 10k entries: verify the whole chain, plan
    one move. Encoding all 10k at once happens never in production; it is
    bounded separately, looser, as the contract's 50 ms was an estimate."""
    entries = [_entry(i) for i in range(10000)]
    root = gha.root_zero(REPO, 1)
    segs = gha.build_segments(1, 0, entries, root)
    comments = [{"id": f"{s.key}{i}", "body": p} for s in segs for i, p in enumerate(s.parts)]
    meta = {"history_archive": gha.build_index(10000, segs[-1], {}, "r")}
    got, err, _ids = gha.verify_chain(meta, comments, REPO, 1)
    assert err == "" and got == entries

    def steady():
        gha.verify_chain(meta, comments, REPO, 1)
        gha.build_segments(1, 10000, entries[:450], segs[-1].root)

    def one_shot():
        gha.verify_chain(meta, comments, REPO, 1)
        gha.build_segments(1, 0, entries, root)

    assert _best_of(5, steady) < 0.05
    assert _best_of(5, one_shot) < 0.15

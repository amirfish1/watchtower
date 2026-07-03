"""Log retention policy tests (WT-74) — fully sandboxed via the same env
overrides as test_messages (logs dir hangs off $WATCHTOWER_OUTBOX_FILE)."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

from test_messages import SID_A, SID_B, _write_transcript, wt  # noqa: F401

DAY = 86400.0


def _logmaint(wt):
    import importlib
    import watchtower.logmaint as logmaint
    importlib.reload(logmaint)
    return logmaint


def _mklog(wt, name, *, sid=None, age_s=0.0, size=100):
    d = Path(os.environ["WATCHTOWER_OUTBOX_FILE"]).parent / "logs"
    d.mkdir(parents=True, exist_ok=True)
    p = d / name
    line = (json.dumps({"type": "system", "session_id": sid}) + "\n") if sid else "boom, no session\n"
    p.write_text(line * max(1, size // max(1, len(line))))
    old = time.time() - age_s
    os.utime(p, (old, old))
    return p


def test_redundant_log_pruned_after_grace_and_indexed(wt):
    lm = _logmaint(wt)
    _write_transcript(wt, SID_A, age_s=2 * DAY)
    p = _mklog(wt, "worker-a.log", sid=SID_A, age_s=2 * DAY)
    report = lm.prune(min_age_s=0)
    assert [r["file"] for r in report["pruned"]] == ["worker-a.log"]
    assert report["pruned"][0]["reason"] == "transcript-redundant"
    assert not p.exists()
    index = json.loads((p.parent / lm.INDEX_FILENAME).read_text())
    assert index["worker-a.log"]["session_id"] == SID_A


def test_redundant_log_kept_within_grace(wt):
    lm = _logmaint(wt)
    _write_transcript(wt, SID_A, age_s=3600)
    p = _mklog(wt, "worker-a.log", sid=SID_A, age_s=2 * 3600)  # < 24h grace
    report = lm.prune(min_age_s=0)
    assert report["pruned"] == [] and p.exists()


def test_failure_log_kept_longer_then_pruned(wt):
    lm = _logmaint(wt)
    fresh = _mklog(wt, "died-early.log", sid=None, age_s=2 * DAY)
    stale = _mklog(wt, "died-long-ago.log", sid=None, age_s=8 * DAY)
    report = lm.prune(min_age_s=0)
    names = [r["file"] for r in report["pruned"]]
    assert names == ["died-long-ago.log"]
    assert fresh.exists() and not stale.exists()


def test_missing_transcript_treated_as_only_record(wt):
    lm = _logmaint(wt)
    # session id embedded but transcript nowhere on disk -> failure retention
    p = _mklog(wt, "worker-b.log", sid=SID_B, age_s=2 * DAY)
    report = lm.prune(min_age_s=0)
    assert report["pruned"] == [] and p.exists()


def test_size_cap_backstop_deletes_oldest_first(wt):
    lm = _logmaint(wt)
    old = _mklog(wt, "old.log", sid=None, age_s=3 * DAY, size=4000)
    new = _mklog(wt, "new.log", sid=None, age_s=1 * DAY, size=4000)
    report = lm.prune(min_age_s=0, max_total_bytes=5000)
    assert [r["file"] for r in report["pruned"]] == ["old.log"]
    assert report["pruned"][0]["reason"] == "size-cap"
    assert not old.exists() and new.exists()


def test_live_worker_log_and_min_age_protected(wt):
    lm = _logmaint(wt)
    import uuid as _uuid
    from test_messages import _live_worker
    rec = _live_worker(wt, "Q")
    live_log = Path(rec["log"])
    live_log.write_text("live worker output\n")
    old = time.time() - 30 * DAY
    os.utime(live_log, (old, old))
    young = _mklog(wt, "young.log", sid=None, age_s=10)  # < min_age
    report = lm.prune()  # default min_age 1h
    assert report["pruned"] == []
    assert live_log.exists() and young.exists()


def test_orphan_fifo_removed(wt):
    lm = _logmaint(wt)
    d = Path(os.environ["WATCHTOWER_OUTBOX_FILE"]).parent / "logs"
    d.mkdir(parents=True, exist_ok=True)
    fifo = d / "dead-worker.log.stdin"
    os.mkfifo(fifo)
    old = time.time() - 2 * 3600
    os.utime(fifo, (old, old))
    report = lm.prune(min_age_s=3600)
    assert report["fifos_removed"] == ["dead-worker.log.stdin"]
    assert not fifo.exists()


def test_dry_run_deletes_nothing(wt):
    lm = _logmaint(wt)
    p = _mklog(wt, "died-long-ago.log", sid=None, age_s=8 * DAY)
    report = lm.prune(min_age_s=0, dry_run=True)
    assert [r["file"] for r in report["pruned"]] == ["died-long-ago.log"]
    assert p.exists()
    assert not (p.parent / lm.INDEX_FILENAME).exists()


def test_maybe_prune_throttles_by_stamp(wt):
    lm = _logmaint(wt)
    _mklog(wt, "x.log", sid=None, age_s=8 * DAY)
    first = lm.maybe_prune(min_interval_s=3600)
    assert first is not None
    assert lm.maybe_prune(min_interval_s=3600) is None  # stamped, throttled

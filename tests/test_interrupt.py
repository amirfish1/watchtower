"""Tests for the worker-turn interrupt primitive (SONIA-CHAT-5).

``wt interrupt <target>`` writes claude's stream-json ``interrupt`` control
request to a live worker's stdin FIFO, aborting the in-flight turn WITHOUT
killing the process. The frame format is byte-compatible with CCC's
``_write_stream_json_interrupt`` (verified against claude 2.1.x).

Live/dead workers are simulated exactly as in test_workers_lifecycle: a live
worker is a record whose pid is this test process with a real FIFO and a held
O_RDWR reader fd; a dead worker uses a reaped child pid and a FIFO with no
reader (so an O_WRONLY|O_NONBLOCK open gets ENXIO).
"""

from __future__ import annotations

import json
import os
import select
import subprocess
import uuid

import pytest


@pytest.fixture()
def fifo_readers():
    """Track held FIFO reader fds so they are closed at teardown."""
    fds = []
    yield fds
    for fd in fds:
        try:
            os.close(fd)
        except OSError:
            pass


def _dead_pid():
    """A pid guaranteed not to be running (a child we just reaped)."""
    p = subprocess.Popen(["true"])
    p.wait()
    return p.pid


def _live_worker(wt_env, readers, queue="Q", engine="claude"):
    """Record a worker that is alive (this pid) with a real FIFO + held reader."""
    workers = wt_env.workers
    wid = f"{queue.lower()}-live-{len(readers)}"
    log = wt_env.tmp / f"{wid}.log"
    log.write_text("")  # real log file so mtime (idle clock) is resolvable
    fifo_path, rdwr_fd = workers._make_stdin_fifo(log)
    readers.append(rdwr_fd)
    rec = workers.record_worker(
        os.getpid(), queue, engine, wid, str(wt_env.tmp), str(log),
        fifo=fifo_path or "", session_id=str(uuid.uuid4()),
    )
    return rec, rdwr_fd


def _dead_worker(wt_env, readers, queue="Q", engine="claude"):
    """Record a worker whose process is gone and whose FIFO has no reader."""
    workers = wt_env.workers
    wid = f"{queue.lower()}-dead-{len(readers)}"
    log = wt_env.tmp / f"{wid}.log"
    fifo_path, rdwr_fd = workers._make_stdin_fifo(log)
    os.close(rdwr_fd)  # drop the only reader -> writes will ENXIO
    rec = workers.record_worker(
        _dead_pid(), queue, engine, wid, str(wt_env.tmp), str(log),
        fifo=fifo_path or "",
    )
    return rec


def _read_frame(fd):
    """Read one frame from a FIFO holding a delivery; fail if none arrived."""
    assert select.select([fd], [], [], 0.5)[0], "no frame was written to the FIFO"
    return os.read(fd, 65536)


def _fifo_empty(fd):
    """True when nothing is buffered on the FIFO (a bare read would hang)."""
    return not bool(select.select([fd], [], [], 0.05)[0])


# ----------------------------------------------------------------------- library
def test_interrupt_writes_ccc_compatible_frame_bytes(wt_env, fifo_readers):
    rec, fd = _live_worker(wt_env, fifo_readers)

    res = wt_env.workers.interrupt_worker_turn(rec["worker_id"])

    assert res == {"ok": True, "worker_id": rec["worker_id"], "via": "fifo-interrupt"}
    raw = _read_frame(fd)
    assert raw.endswith(b"\n")
    msg = json.loads(raw[:-1].decode("utf-8"))
    assert set(msg) == {"type", "request_id", "request", "uuid"}
    assert msg["type"] == "control_request"
    assert msg["request"] == {"subtype": "interrupt"}
    # Both id fields are real, distinct UUIDs (CCC emits one of each).
    uuid.UUID(msg["request_id"])
    uuid.UUID(msg["uuid"])
    assert msg["request_id"] != msg["uuid"]
    # Byte-parity with CCC's serialization recipe: json.dumps defaults + "\n".
    assert raw == (json.dumps(msg) + "\n").encode("utf-8")


def test_interrupt_unknown_worker_is_not_live(wt_env):
    res = wt_env.workers.interrupt_worker_turn("nope-0")
    assert res["ok"] is False
    assert res["code"] == "not_live"


def test_interrupt_dead_worker_is_not_live(wt_env, fifo_readers):
    rec = _dead_worker(wt_env, fifo_readers)

    res = wt_env.workers.interrupt_worker_turn(rec["worker_id"])

    assert res["ok"] is False
    assert res["code"] == "not_live"


def test_interrupt_worker_without_fifo_is_not_live(wt_env):
    workers = wt_env.workers
    rec = workers.record_worker(
        os.getpid(), "Q", "claude", "q-nofifo", str(wt_env.tmp),
        log="", fifo="",
    )

    res = workers.interrupt_worker_turn(rec["worker_id"])

    assert res["ok"] is False
    assert res["code"] == "not_live"


def test_interrupt_non_claude_engine_is_unsupported(wt_env, fifo_readers):
    rec, fd = _live_worker(wt_env, fifo_readers, engine="codex")

    res = wt_env.workers.interrupt_worker_turn(rec["worker_id"])

    assert res["ok"] is False
    assert res["code"] == "unsupported_engine"
    assert res["engine"] == "codex"
    assert _fifo_empty(fd)  # nothing may be written for an unsupported engine


def test_interrupt_unreachable_fifo(wt_env, fifo_readers):
    """Live process, but the FIFO has no reader: the write fails ENXIO."""
    workers = wt_env.workers
    log = wt_env.tmp / "q-nolisten.log"
    fifo_path, rdwr_fd = workers._make_stdin_fifo(log)
    os.close(rdwr_fd)  # no reader attached
    rec = workers.record_worker(
        os.getpid(), "Q", "claude", "q-nolisten", str(wt_env.tmp), str(log),
        fifo=fifo_path or "",
    )

    res = workers.interrupt_worker_turn(rec["worker_id"])

    assert res["ok"] is False
    assert res["code"] == "fifo_unreachable"


# --------------------------------------------------------------------------- cli
def test_cli_interrupt_by_ticket_ref(wt_env, run_cli, fifo_readers):
    rec, fd = _live_worker(wt_env, fifo_readers, queue="BE")
    item = wt_env.queue.enqueue(project="BE", title="t", note="t", text="")
    claimed = wt_env.queue.claim_by_ref(item["ref"], rec["worker_id"])
    assert claimed["claimed_by"] == rec["worker_id"]

    r = run_cli("interrupt", item["ref"])

    assert r.code == 0
    assert f"INTERRUPTED: {rec['worker_id']}" in r.out
    msg = json.loads(_read_frame(fd)[:-1].decode("utf-8"))
    assert msg["type"] == "control_request"
    assert msg["request"] == {"subtype": "interrupt"}


def test_cli_interrupt_ref_resolution_is_case_insensitive(wt_env, run_cli, fifo_readers):
    rec, fd = _live_worker(wt_env, fifo_readers, queue="BE")
    item = wt_env.queue.enqueue(project="BE", title="t", note="t", text="")
    wt_env.queue.claim_by_ref(item["ref"], rec["worker_id"])

    r = run_cli("interrupt", item["ref"].lower())

    assert r.code == 0
    assert f"INTERRUPTED: {rec['worker_id']}" in r.out


def test_cli_interrupt_open_ticket_has_nothing_to_interrupt(wt_env, run_cli):
    item = wt_env.queue.enqueue(project="BE", title="t", note="t", text="")

    r = run_cli("interrupt", item["ref"])

    assert r.code == 0
    assert "not claimed / nothing to interrupt" in r.out


def test_cli_interrupt_by_worker_id(wt_env, run_cli, fifo_readers):
    rec, fd = _live_worker(wt_env, fifo_readers)

    r = run_cli("interrupt", rec["worker_id"])

    assert r.code == 0
    assert f"INTERRUPTED: {rec['worker_id']}" in r.out
    assert _read_frame(fd)


def test_cli_interrupt_unknown_target_is_not_live(wt_env, run_cli):
    r = run_cli("interrupt", "ghost-0")
    assert r.code == 0
    assert "not live / nothing to interrupt" in r.out


def test_cli_interrupt_dead_worker_is_not_live(wt_env, run_cli, fifo_readers):
    rec = _dead_worker(wt_env, fifo_readers, queue="BE")

    r = run_cli("interrupt", rec["worker_id"])

    assert r.code == 0
    assert "not live / nothing to interrupt" in r.out


def test_cli_interrupt_non_claude_engine_fails(wt_env, run_cli, fifo_readers):
    rec, _fd = _live_worker(wt_env, fifo_readers, engine="codex")

    r = run_cli("interrupt", rec["worker_id"])

    assert r.code == 1
    assert "only claude workers support" in r.err
    assert "codex" in r.err


def test_cli_interrupt_unreachable_fifo_fails(wt_env, run_cli):
    workers = wt_env.workers
    log = wt_env.tmp / "q-nolisten2.log"
    fifo_path, rdwr_fd = workers._make_stdin_fifo(log)
    os.close(rdwr_fd)
    rec = workers.record_worker(
        os.getpid(), "Q", "claude", "q-nolisten2", str(wt_env.tmp), str(log),
        fifo=fifo_path or "",
    )

    r = run_cli("interrupt", rec["worker_id"])

    assert r.code == 1
    assert "fifo_unreachable" in r.err


def test_cli_interrupt_json_output(wt_env, run_cli, fifo_readers):
    rec, _fd = _live_worker(wt_env, fifo_readers)

    r = run_cli("interrupt", rec["worker_id"], "--json")

    assert r.code == 0
    payload = json.loads(r.out)
    assert payload == {
        "ok": True, "worker_id": rec["worker_id"], "via": "fifo-interrupt",
    }

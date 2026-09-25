"""CLIENT-CHAT-14: `wt block --kind awaiting-client` and `wt block --commit`.

Context: a WhatsApp client-intake worker needs to "park" a ticket between
conversation turns instead of closing it, so `wt answer` can later resume the
exact same worker session. This module pins three things:

1. ``--kind awaiting-client`` is a valid `wt block` kind (argparse choice +
   queue.block validation), alongside the pre-existing ``input``/``rationale``.
2. ``--commit <sha>`` on `wt block` is verified via local `git rev-parse`,
   with the identical failure mode `wt close --commit` already has via
   ``close_proof`` / ``_verify_close_commit``.
3. A ticket blocked ``--kind awaiting-client`` is immune to
   ``requeue_orphaned_tickets`` reopening it and ``release_idle_workers``
   releasing its worker slot -- a regression-proof that the pre-existing
   "skip any blocked ticket" logic in both sweeps still holds for the new
   kind specifically (no new logic in those two functions).
"""

from __future__ import annotations

import importlib
import subprocess

import pytest


def _git(repo, *args):
    subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True)


@pytest.fixture
def repo_with_commit(tmp_path):
    def _make(name):
        repo = tmp_path / name
        repo.mkdir()
        _git(repo, "init", "-q")
        _git(repo, "config", "user.email", "t@example.com")
        _git(repo, "config", "user.name", "T")
        (repo / "f.txt").write_text(name)
        _git(repo, "add", "f.txt")
        _git(repo, "commit", "-qm", f"commit in {name}")
        sha = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True
        ).stdout.strip()
        return str(repo), sha
    return _make


@pytest.fixture()
def wt(tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(tmp_path / "activity.log"))

    import watchtower.queue as q
    import watchtower.cli as cli

    importlib.reload(q)
    importlib.reload(cli)

    class Ns:
        pass

    ns = Ns()
    ns.store = tmp_path / "queue.json"
    ns.q = q
    ns.cli = cli
    return ns


# --------------------------------------------------------------- item 1: --kind

def test_block_kind_awaiting_client_is_a_valid_argparse_choice(wt):
    parser = wt.cli.build_parser()
    args = parser.parse_args(
        ["block", "Q-1", "--question", "waiting on client", "--kind", "awaiting-client"]
    )
    assert args.kind == "awaiting-client"


def test_block_kind_still_rejects_a_bogus_argparse_choice(wt, capsys):
    parser = wt.cli.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["block", "Q-1", "--kind", "bogus"])
    assert "invalid choice" in capsys.readouterr().err


def test_cli_block_kind_awaiting_client_end_to_end(wt, capsys):
    item = wt.q.enqueue(project="Q", note="client thread", source="test")
    wt.q.claim_by_ref(item["ref"], "worker-a")

    assert wt.cli.main([
        "block", item["ref"], "--worker", "worker-a",
        "--question", "awaiting client reply",
        "--kind", "awaiting-client",
        "--json",
    ]) == 0

    import json
    blocked = json.loads(capsys.readouterr().out)
    assert blocked["block_kind"] == "awaiting-client"
    assert blocked["needs_input"] is True


# ------------------------------------------------------------ item 2: --commit

def test_block_commit_argparse_flag_exists(wt):
    parser = wt.cli.build_parser()
    args = parser.parse_args(["block", "Q-1", "--commit", "deadbeef"])
    assert args.commit == "deadbeef"


def test_cli_block_with_valid_commit_verifies_and_records_it(
    wt, repo_with_commit, monkeypatch, capsys
):
    repo, sha = repo_with_commit("primary")
    item = wt.q.enqueue(project="Q", note="client thread", source="test",
                        repo_path=repo)
    wt.q.claim_by_ref(item["ref"], "worker-a")

    assert wt.cli.main([
        "block", item["ref"], "--worker", "worker-a",
        "--question", "waiting on client",
        "--kind", "awaiting-client",
        "--commit", sha[:10],
        "--json",
    ]) == 0

    import json
    blocked = json.loads(capsys.readouterr().out)
    # The full, canonical SHA is what gets recorded -- same normalization
    # _verify_close_commit performs for `wt close --commit`.
    assert blocked.get("block_commit") == sha


def test_cli_block_with_invalid_commit_is_refused_same_as_close(
    wt, repo_with_commit, capsys
):
    """Same failure mode `_verify_close_commit` gives `wt close` for a SHA
    that resolves in no configured/current repo."""
    repo, _sha = repo_with_commit("primary")
    item = wt.q.enqueue(project="Q", note="client thread", source="test",
                        repo_path=repo)
    wt.q.claim_by_ref(item["ref"], "worker-a")

    rc = wt.cli.main([
        "block", item["ref"], "--worker", "worker-a",
        "--question", "waiting on client",
        "--commit", "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
    ])
    assert rc == 1
    err = capsys.readouterr().err
    assert "is not a commit in" in err
    # Ticket must not have been blocked with a bogus commit recorded.
    assert wt.q.get(item["ref"]).get("needs_input") is not True


def test_cli_block_with_malformed_commit_is_rejected_without_touching_git(
    wt, capsys
):
    item = wt.q.enqueue(project="Q", note="client thread", source="test")
    wt.q.claim_by_ref(item["ref"], "worker-a")

    rc = wt.cli.main([
        "block", item["ref"], "--worker", "worker-a",
        "--question", "waiting on client",
        "--commit", "not-a-sha",
    ])
    assert rc == 1
    err = capsys.readouterr().err
    assert "must be a 7- to 64-character hexadecimal commit SHA" in err


# ------------------------------------------------------- item 3: sweep immunity

def test_requeue_orphaned_tickets_does_not_reopen_awaiting_client_block(
    wt, monkeypatch
):
    """Regression-proof: `requeue_orphaned_tickets` already skips ANY
    ``needs_input`` ticket regardless of kind; confirm that still holds when
    the kind is specifically ``awaiting-client``."""
    import watchtower.workers as workers
    importlib.reload(workers)

    item = wt.q.enqueue(project="Q", note="client thread", source="test")
    claimed = wt.q.claim_by_ref(item["ref"], "dead-worker")
    wt.q.block(claimed["ref"], session_id="dead-worker",
               question="waiting on client", kind="awaiting-client")

    # Backdate the claim so it is well past any orphan grace window, and make
    # the claimer a KNOWN-but-dead worker so the sweep would otherwise treat
    # it as orphaned.
    data = wt.q._load_unlocked()
    for it in data["items"]:
        if it["ref"] == claimed["ref"]:
            it["claimed_at"] = "2000-01-01T00:00:00Z"
    wt.q._save_unlocked(data)

    monkeypatch.setattr(workers, "list_workers", lambda *a, **k: [
        {"worker_id": "dead-worker", "alive": False, "queue": "Q"}
    ])

    reopened = workers.requeue_orphaned_tickets(grace_s=0)
    assert claimed["ref"] not in [it["ref"] for it in reopened]
    assert wt.q.get(claimed["ref"])["status"] == "in_progress"
    assert wt.q.get(claimed["ref"])["block_kind"] == "awaiting-client"


def test_release_idle_workers_does_not_release_awaiting_client_worker(
    wt, monkeypatch, tmp_path
):
    """Regression-proof: `release_idle_workers` already refuses to release a
    worker that owns a blocked (needs_input) ticket, regardless of kind;
    confirm that still holds for ``awaiting-client``."""
    import watchtower.workers as workers
    importlib.reload(workers)

    monkeypatch.setenv("WATCHTOWER_WORKERS_FILE", str(tmp_path / "workers.json"))
    monkeypatch.setenv("WATCHTOWER_STOP_SIGNALS_DIR", str(tmp_path / "stop-signals"))
    importlib.reload(workers)

    import watchtower.messages as messages
    monkeypatch.setattr(messages, "send", lambda *a, **k: {"ok": False, "error": "disabled"})

    log = tmp_path / "worker.log"
    log.write_text("")
    rec = workers.record_worker(
        __import__("os").getpid(), "Q", "codex", "q-blocked-client",
        str(tmp_path), str(log),
    )

    item = wt.q.enqueue(project="Q", note="client thread", source="test")
    wt.q.claim_by_ref(item["ref"], rec["worker_id"])
    wt.q.block(item["ref"], rec["worker_id"], "waiting on client",
               kind="awaiting-client")

    import os
    import time
    old = time.time() - workers.RELEASE_IDLE_S - 60
    os.utime(log, (old, old))

    released = workers.release_idle_workers(queue="Q")
    assert released == []
    assert not (workers.STOP_SIGNALS_DIR / rec["worker_id"]).exists()

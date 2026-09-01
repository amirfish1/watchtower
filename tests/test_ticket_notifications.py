"""Ticket push-back notifications: a ticket's ``submitter`` and a queue's
``subscribers`` are notified on claim/close/needs-input (WATCHTOWER submitter-
notify + queue-subscribe design).

Delivery is exercised by monkeypatching ``messages.send`` so these tests
assert on *who* got notified and *what* they were told, without depending on
the real delivery adapters (FIFO/resume/codex-app-server) -- those already
have their own coverage in test_messages.py. The GitHub-backed round trip at
the bottom proves the same fields/notifications work through the issue-body
metadata block, not just the file-backed store.
"""

from __future__ import annotations

import importlib
import json

import pytest

from test_messages import wt  # noqa: F401  (shared isolated-sandbox fixture)
from test_github_backend import _install_fake_gh, _reload_isolated, _drainable


@pytest.fixture()
def wt_cli(wt):  # noqa: F811 - fixture shadowing is the pytest idiom here
    """``wt`` plus a freshly reloaded ``cli`` module bound to the same
    sandboxed queue/config/messages, mirroring test_queue.py's local fixture
    (test_messages.wt itself has no ``cli`` attribute -- most of its callers
    never invoke the CLI)."""
    import watchtower.cli as cli
    importlib.reload(cli)
    wt.cli = cli
    return wt


def _record_sends(monkeypatch, messages):
    calls = []

    def _fake_send(target, text, **kwargs):
        calls.append((target, text))
        return {"ok": True, "transport": "fake"}

    monkeypatch.setattr(messages, "send", _fake_send)
    return calls


# --------------------------------------------------------------------- field
def test_enqueue_stores_submitter_on_file_backed_ticket(wt):
    item = wt.q.enqueue(project="SUB", note="filed by someone", submitter="worker-a")
    assert item["submitter"] == "worker-a"
    assert wt.q.get(item["ref"])["submitter"] == "worker-a"


def test_enqueue_without_submitter_defaults_to_empty_string(wt):
    item = wt.q.enqueue(project="SUB", note="legacy filer")
    assert item["submitter"] == ""


# ------------------------------------------------------------- notify: claim
def test_claim_by_ref_notifies_the_submitter(wt, monkeypatch):
    calls = _record_sends(monkeypatch, wt.messages)
    item = wt.q.enqueue(project="SUB", note="claim me", submitter="worker-sub")

    claimed = wt.q.claim_by_ref(item["ref"], "worker-a")

    assert claimed["status"] == "in_progress"
    assert len(calls) == 1
    target, text = calls[0]
    assert target == "worker-sub"
    assert item["ref"] in text
    assert "claimed" in text


def test_claim_next_notifies_the_submitter(wt, monkeypatch):
    calls = _record_sends(monkeypatch, wt.messages)
    item = wt.q.enqueue(project="SUB", note="claim next", submitter="worker-sub")

    claimed = wt.q.claim_next("worker-a", project="SUB")

    assert claimed["ref"] == item["ref"]
    assert len(calls) == 1
    assert calls[0][0] == "worker-sub"


# ------------------------------------------------------------- notify: close
def test_close_notifies_submitter_with_resolution_summary(wt, monkeypatch):
    item = wt.q.enqueue(project="SUB", note="close me", submitter="worker-sub")
    wt.q.claim_by_ref(item["ref"], "worker-a")
    calls = _record_sends(monkeypatch, wt.messages)

    closed = wt.q.close(item["ref"], "worker-a", resolution={"summary": "shipped the fix"})

    assert closed["status"] == "closed"
    assert len(calls) == 1
    target, text = calls[0]
    assert target == "worker-sub"
    assert "closed" in text
    assert "shipped the fix" in text


def test_close_with_no_resolution_still_notifies(wt, monkeypatch):
    item = wt.q.enqueue(project="SUB", note="close me plain", submitter="worker-sub")
    wt.q.claim_by_ref(item["ref"], "worker-a")
    calls = _record_sends(monkeypatch, wt.messages)

    wt.q.close(item["ref"], "worker-a")

    assert len(calls) == 1
    assert calls[0][0] == "worker-sub"


# ------------------------------------------------------------- notify: block
def test_block_notifies_submitter_with_question(wt, monkeypatch):
    item = wt.q.enqueue(project="SUB", note="needs a call", submitter="worker-sub")
    wt.q.claim_by_ref(item["ref"], "worker-a")
    calls = _record_sends(monkeypatch, wt.messages)

    blocked = wt.q.block(item["ref"], session_id="worker-a", question="ship it?")

    assert blocked["needs_input"] is True
    assert len(calls) == 1
    target, text = calls[0]
    assert target == "worker-sub"
    assert "needs input" in text
    assert "ship it?" in text


# --------------------------------------------------- no self-notify (actor)
def test_self_claim_does_not_notify_the_claimer(wt, monkeypatch):
    """A session that filed a ticket and then claims it must not be told it
    claimed it -- that echo steers a worker with its own command."""
    calls = _record_sends(monkeypatch, wt.messages)
    item = wt.q.enqueue(project="SUB", note="mine", submitter="worker-a")

    claimed = wt.q.claim_by_ref(item["ref"], "worker-a")

    assert claimed["status"] == "in_progress"
    assert calls == []


def test_self_claim_next_does_not_notify_the_claimer(wt, monkeypatch):
    calls = _record_sends(monkeypatch, wt.messages)
    wt.q.enqueue(project="SUB", note="mine too", submitter="worker-a")

    assert wt.q.claim_next("worker-a", project="SUB") is not None
    assert calls == []


def test_self_claim_matches_on_session_uuid_too(wt, monkeypatch):
    """The submitter may be registered under the harness session UUID rather
    than the worker id; either name identifies the same actor."""
    sid = "11111111-2222-3333-4444-555555555555"
    calls = _record_sends(monkeypatch, wt.messages)
    item = wt.q.enqueue(project="SUB", note="uuid submitter", submitter=sid)

    wt.q.claim_by_ref(item["ref"], "worker-a", session_uuid=sid)

    assert calls == []


def test_self_close_does_not_notify_the_closer(wt, monkeypatch):
    item = wt.q.enqueue(project="SUB", note="close my own", submitter="worker-a")
    wt.q.claim_by_ref(item["ref"], "worker-a")
    calls = _record_sends(monkeypatch, wt.messages)

    closed = wt.q.close(item["ref"], "worker-a", resolution={"summary": "done"})

    assert closed["status"] == "closed"
    assert calls == []


def test_self_block_does_not_notify_the_blocker(wt, monkeypatch):
    item = wt.q.enqueue(project="SUB", note="block my own", submitter="worker-a")
    wt.q.claim_by_ref(item["ref"], "worker-a")
    calls = _record_sends(monkeypatch, wt.messages)

    wt.q.block(item["ref"], session_id="worker-a", question="ship it?")

    assert calls == []


def test_actor_is_dropped_from_subscribers_but_others_still_notified(wt, monkeypatch):
    """Suppression is per-target, not all-or-nothing: a worker subscribed to
    the queue it works loses only its own echo."""
    wt.config.add_subscriber("SUB", "worker-a")
    wt.config.add_subscriber("SUB", "watcher")
    calls = _record_sends(monkeypatch, wt.messages)
    item = wt.q.enqueue(project="SUB", note="two subscribers")

    wt.q.claim_by_ref(item["ref"], "worker-a")

    assert [t for t, _ in calls] == ["watcher"]


def test_force_close_by_someone_else_still_notifies_the_claimant(wt, monkeypatch):
    """The suppressed identity is the ACTOR of this transition, not the
    ticket's claimant -- a human force-closing somebody's ticket is news."""
    item = wt.q.enqueue(project="SUB", note="force closed", submitter="worker-a")
    wt.q.claim_by_ref(item["ref"], "worker-a")
    calls = _record_sends(monkeypatch, wt.messages)

    wt.q.close(item["ref"], "human-x", resolution={"summary": "taking over"},
               force=True)

    assert [t for t, _ in calls] == ["worker-a"]


# ------------------------------------------------------- no identity: silent
def test_no_submitter_and_no_subscribers_sends_nothing_and_never_raises(wt, monkeypatch):
    calls = _record_sends(monkeypatch, wt.messages)
    item = wt.q.enqueue(project="SUB", note="legacy ticket, nobody to tell")

    claimed = wt.q.claim_by_ref(item["ref"], "worker-a")
    wt.q.block(claimed["ref"], session_id="worker-a", question="anyone home?")
    wt.q.answer(claimed["ref"], "no", session_id="human-a")
    closed = wt.q.close(claimed["ref"], "worker-a", resolution="done")

    assert closed["status"] == "closed"
    assert calls == []


def test_notification_delivery_failure_never_blocks_the_transition(wt, monkeypatch):
    """messages.send raising must not stop claim/close/block from succeeding --
    notification is strictly best-effort."""

    def _boom(*args, **kwargs):
        raise RuntimeError("delivery adapter exploded")

    monkeypatch.setattr(wt.messages, "send", _boom)
    item = wt.q.enqueue(project="SUB", note="resilient", submitter="worker-sub")

    claimed = wt.q.claim_by_ref(item["ref"], "worker-a")
    assert claimed["status"] == "in_progress"
    blocked = wt.q.block(claimed["ref"], session_id="worker-a", question="ok?")
    assert blocked["needs_input"] is True
    wt.q.answer(claimed["ref"], "yes", session_id="human-a")
    closed = wt.q.close(claimed["ref"], "worker-a", resolution="done anyway")
    assert closed["status"] == "closed"


def test_unresolvable_submitter_is_stored_but_swallowed_at_send_time(wt, monkeypatch):
    """A submitter that never resolves (unknown UUID/name) makes messages.send
    return {"ok": False} via resolve_target's ValueError -- notify must treat
    that the same as any other failed send: swallow it, no exception."""
    item = wt.q.enqueue(
        project="SUB", note="bogus submitter",
        submitter="totally-unknown-target-xyz",
    )
    # No monkeypatch here: exercise the real messages.send -> resolve_target
    # path, which raises ValueError for an unresolvable target and is caught.
    claimed = wt.q.claim_by_ref(item["ref"], "worker-a")
    assert claimed["status"] == "in_progress"


# --------------------------------------------------------------- subscribers
def test_subscriber_is_notified_without_being_the_submitter(wt, monkeypatch):
    wt.config.add_subscriber("SUB", "watcher-1")
    calls = _record_sends(monkeypatch, wt.messages)
    item = wt.q.enqueue(project="SUB", note="watched queue")

    wt.q.claim_by_ref(item["ref"], "worker-a")

    assert calls == [("watcher-1", calls[0][1])]


def test_submitter_and_subscriber_overlap_gets_exactly_one_send(wt, monkeypatch):
    """A target that is both the ticket's submitter and a queue subscriber
    must be notified once, not twice."""
    wt.config.add_subscriber("SUB", "same-target")
    calls = _record_sends(monkeypatch, wt.messages)
    item = wt.q.enqueue(project="SUB", note="dedup me", submitter="same-target")

    wt.q.claim_by_ref(item["ref"], "worker-a")

    assert len(calls) == 1
    assert calls[0][0] == "same-target"


def test_submitter_and_subscriber_both_get_notified_when_distinct(wt, monkeypatch):
    wt.config.add_subscriber("SUB", "watcher-1")
    calls = _record_sends(monkeypatch, wt.messages)
    item = wt.q.enqueue(project="SUB", note="two targets", submitter="worker-sub")

    wt.q.claim_by_ref(item["ref"], "worker-a")

    targets = sorted(t for t, _ in calls)
    assert targets == ["watcher-1", "worker-sub"]


def test_subscribers_are_scoped_per_queue(wt, monkeypatch):
    wt.config.add_subscriber("SUB", "watcher-1")
    calls = _record_sends(monkeypatch, wt.messages)
    item = wt.q.enqueue(project="OTHER", note="unrelated queue")

    wt.q.claim_by_ref(item["ref"], "worker-a")

    assert calls == []


# ---------------------------------------------------------- config.py: CRUD
def test_config_subscribers_default_empty(wt):
    assert wt.config.subscribers("NEVERCONFIGURED") == []


def test_config_add_subscriber_is_idempotent(wt):
    wt.config.add_subscriber("SUB", "watcher-1")
    wt.config.add_subscriber("SUB", "watcher-1")
    assert wt.config.subscribers("SUB") == ["watcher-1"]


def test_config_remove_subscriber_clears_the_key_when_empty(wt):
    wt.config.add_subscriber("SUB", "watcher-1")
    wt.config.remove_subscriber("SUB", "watcher-1")
    assert wt.config.subscribers("SUB") == []
    data = json.loads(wt.config.CONFIG_FILE.read_text())
    assert "subscribers" not in data.get("SUB", {})


def test_config_set_subscribers_replaces_the_whole_list(wt):
    wt.config.add_subscriber("SUB", "watcher-1")
    wt.config.set_subscribers("SUB", ["watcher-2", "watcher-3", "watcher-2"])
    assert wt.config.subscribers("SUB") == ["watcher-2", "watcher-3"]


def test_config_set_subscribers_empty_clears(wt):
    wt.config.add_subscriber("SUB", "watcher-1")
    wt.config.set_subscribers("SUB", [])
    assert wt.config.subscribers("SUB") == []


# --------------------------------------------------------------------- CLI
def test_cli_subscribe_and_unsubscribe_roundtrip(wt_cli, capsys):
    assert wt_cli.cli.main(["subscribe", "SUB", "watcher-1"]) == 0
    assert "SUBSCRIBED: watcher-1 -> SUB" in capsys.readouterr().out
    assert wt_cli.config.subscribers("SUB") == ["watcher-1"]

    assert wt_cli.cli.main(["unsubscribe", "SUB", "watcher-1"]) == 0
    assert "UNSUBSCRIBED: watcher-1 -> SUB" in capsys.readouterr().out
    assert wt_cli.config.subscribers("SUB") == []


def test_cli_subscribe_with_no_target_lists_current_subscribers(wt_cli, capsys):
    wt_cli.config.add_subscriber("SUB", "watcher-1")
    wt_cli.config.add_subscriber("SUB", "watcher-2")

    assert wt_cli.cli.main(["subscribe", "SUB"]) == 0
    out = capsys.readouterr().out
    assert "watcher-1" in out
    assert "watcher-2" in out


def test_cli_subscribe_json_lists_current_subscribers(wt_cli, capsys):
    wt_cli.config.add_subscriber("SUB", "watcher-1")

    assert wt_cli.cli.main(["subscribe", "SUB", "--json"]) == 0
    assert json.loads(capsys.readouterr().out) == ["watcher-1"]


def test_cli_add_defaults_submitter_from_claude_session_env(wt_cli, monkeypatch, capsys):
    sid = "11111111-2222-3333-4444-555555555555"
    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", sid)

    assert wt_cli.cli.main(["add", "-q", "SUB", "--note", "auto submitter"]) == 0
    capsys.readouterr()

    items = wt_cli.q.list_items(project="SUB")
    assert items[0]["submitter"] == sid


def test_cli_add_explicit_submitter_overrides_env(wt_cli, monkeypatch, capsys):
    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", "11111111-2222-3333-4444-555555555555")

    assert wt_cli.cli.main([
        "add", "-q", "SUB", "--note", "explicit submitter",
        "--submitter", "@named-agent",
    ]) == 0
    capsys.readouterr()

    items = wt_cli.q.list_items(project="SUB")
    assert items[0]["submitter"] == "@named-agent"


def test_cli_add_with_no_env_and_no_flag_leaves_submitter_empty(wt_cli, monkeypatch, capsys):
    monkeypatch.delenv("CLAUDE_CODE_SESSION_ID", raising=False)
    monkeypatch.delenv("CODEX_THREAD_ID", raising=False)

    assert wt_cli.cli.main(["add", "-q", "SUB", "--note", "no identity"]) == 0
    capsys.readouterr()

    items = wt_cli.q.list_items(project="SUB")
    assert items[0]["submitter"] == ""


# -------------------------------------------------------------- GH backend
def test_github_backend_submitter_round_trips_and_notifies_on_claim_close_block(
    tmp_path, monkeypatch,
):
    """The GitHub backend has no dedicated submitter/subscriber field of its
    own -- it stores it in the issue-body metadata block, same as every other
    ticket field GitHub has no native home for (note/resolution/history).
    Prove the round trip and the notification hook both work through it, not
    just through the file-backed store."""
    _install_fake_gh(tmp_path, monkeypatch)
    config, q = _reload_isolated(tmp_path, monkeypatch)
    config.set_backend("GHI", "github")
    config.set_github_repo("GHI", "test-owner/test-repo")
    config.add_subscriber("GHI", "watcher-1")
    _drainable(config)

    import watchtower.messages as messages
    importlib.reload(messages)
    calls = []
    monkeypatch.setattr(
        messages, "send",
        lambda target, text, **kw: calls.append((target, text)) or {"ok": True},
    )

    item = q.enqueue(project="GHI", note="gh submitter", submitter="worker-sub")
    assert item["submitter"] == "worker-sub"
    assert q.get(item["ref"])["submitter"] == "worker-sub"

    claimed = q.claim_by_ref(item["ref"], "worker-a")
    assert claimed["submitter"] == "worker-sub"
    assert {t for t, _ in calls} == {"worker-sub", "watcher-1"}

    calls.clear()
    blocked = q.block(item["ref"], session_id="worker-a", question="which repo?")
    assert blocked["needs_input"] is True
    assert {t for t, _ in calls} == {"worker-sub", "watcher-1"}

    calls.clear()
    q.answer(item["ref"], "the primary one", session_id="human-a")
    closed = q.close(item["ref"], "worker-a", resolution={"summary": "fixed on GH"})
    assert closed["status"] == "closed"
    assert {t for t, _ in calls} == {"worker-sub", "watcher-1"}

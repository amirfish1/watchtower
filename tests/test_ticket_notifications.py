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
    wt.config.set_notify_events("SUB", ["claimed", "closed", "needs_input"])
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
    wt.config.set_notify_events("SUB", ["claimed", "closed", "needs_input"])
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
    wt.config.set_notify_events("SUB", ["claimed", "closed", "needs_input"])
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
    # ...but only the subscriber hears the claim: "claimed" is off by default
    # for a ticket's own submitter (WATCHTOWER-23), through this backend too.
    assert {t for t, _ in calls} == {"watcher-1"}

    calls.clear()
    blocked = q.block(item["ref"], session_id="worker-a", question="which repo?")
    assert blocked["needs_input"] is True
    assert {t for t, _ in calls} == {"worker-sub", "watcher-1"}

    calls.clear()
    q.answer(item["ref"], "the primary one", session_id="human-a")
    closed = q.close(item["ref"], "worker-a", resolution={"summary": "fixed on GH"})
    assert closed["status"] == "closed"
    assert {t for t, _ in calls} == {"worker-sub", "watcher-1"}


# ------------------------------------------------------- delivery class (WT-22)
def test_notifications_are_sent_as_event_notices_not_ordinary_messages(
    wt, monkeypatch
):
    """WATCHTOWER-22: every transition notice must carry ``notify=True`` so
    messages.py routes it over live transports only. Without it a notice to an
    idle claude session fell through to ``_deliver_resume``, spawning a whole
    headless model turn (full context re-read) per notice."""
    seen = []

    def _fake_send(target, text, *a, **kwargs):
        seen.append(kwargs.get("notify"))
        return {"ok": True, "transport": "fake"}

    monkeypatch.setattr(wt.messages, "send", _fake_send)
    wt.config.set_notify_events("SUB", ["claimed", "closed"])
    item = wt.q.enqueue(project="SUB", note="notice class", submitter="worker-sub")

    wt.q.claim_by_ref(item["ref"], "worker-a")
    wt.q.close(item["ref"], "worker-a", resolution={"summary": "done"})

    assert seen == [True, True]


# ------------------------------------------------ notify_events filter (WT-23)
def test_a_claim_does_not_reach_the_submitter_by_default(wt, monkeypatch):
    """WATCHTOWER-23: filing a ticket opts you into nothing. A claim tells the
    filer nothing it can act on, and landing it costs that session a turn."""
    calls = _record_sends(monkeypatch, wt.messages)
    item = wt.q.enqueue(project="SUB", note="quiet claim", submitter="worker-sub")

    claimed = wt.q.claim_by_ref(item["ref"], "worker-a")

    assert claimed["status"] == "in_progress"
    assert calls == []


@pytest.mark.parametrize("event", ["closed", "needs_input"])
def test_the_actionable_events_still_reach_the_submitter_by_default(
    wt, monkeypatch, event
):
    """The default silences the claim and nothing else: closed carries the
    summary, needs_input carries a question only the filer can answer."""
    item = wt.q.enqueue(project="SUB", note="still notified", submitter="worker-sub")
    wt.q.claim_by_ref(item["ref"], "worker-a")
    calls = _record_sends(monkeypatch, wt.messages)

    if event == "closed":
        wt.q.close(item["ref"], "worker-a", resolution={"summary": "shipped"})
    else:
        wt.q.block(item["ref"], session_id="worker-a", question="which repo?")

    assert [t for t, _ in calls] == ["worker-sub"]


def test_awaits_decision_still_reaches_the_submitter_by_default(wt, monkeypatch):
    """The product gate's ask is needs_input's sibling: muting it would leave
    a worker waiting on a decision nobody was told to make."""
    wt.config.set_product_gate("SUB", True)
    item = wt.q.enqueue(project="SUB", note="gate me", submitter="worker-sub")
    wt.q.claim_by_ref(item["ref"], "worker-a")
    calls = _record_sends(monkeypatch, wt.messages)

    wt.q.block(item["ref"], session_id="worker-a", question="build it?",
               kind="rationale")

    assert [t for t, _ in calls] == ["worker-sub"]
    assert "awaits product decision" in calls[0][1]


def test_a_subscriber_still_hears_every_claim(wt, monkeypatch):
    """`wt subscribe` IS the explicit opt-in; the submitter filter never
    applies to it (and `wt unsubscribe` is its off switch)."""
    wt.config.add_subscriber("SUB", "watcher-1")
    calls = _record_sends(monkeypatch, wt.messages)
    item = wt.q.enqueue(project="SUB", note="watched", submitter="worker-sub")

    wt.q.claim_by_ref(item["ref"], "worker-a")

    assert [t for t, _ in calls] == ["watcher-1"]


def test_an_explicitly_named_submitter_hears_every_claim(wt, monkeypatch):
    """`wt add --submitter X` attaches X to THIS ticket deliberately, unlike
    the filing session recorded automatically -- so X gets the full stream."""
    calls = _record_sends(monkeypatch, wt.messages)
    item = wt.q.enqueue(project="SUB", note="watch this one",
                        submitter="worker-sub", submitter_explicit=True)

    wt.q.claim_by_ref(item["ref"], "worker-a")

    assert [t for t, _ in calls] == ["worker-sub"]


def test_a_pre_acked_ticket_notifies_its_submitter_on_claim(wt, monkeypatch):
    """--pre-ack means the filer already made a call about this ticket, so it
    is watching it -- same carve-out as an explicit submitter."""
    calls = _record_sends(monkeypatch, wt.messages)
    item = wt.q.enqueue(project="SUB", note="already acked",
                        submitter="worker-sub", pre_ack=True)

    wt.q.claim_by_ref(item["ref"], "worker-a")

    assert [t for t, _ in calls] == ["worker-sub"]


def test_notify_events_can_be_opened_up_or_shut_off_per_queue(wt, monkeypatch):
    wt.config.set_notify_events("SUB", [])
    item = wt.q.enqueue(project="SUB", note="silent queue", submitter="worker-sub")
    wt.q.claim_by_ref(item["ref"], "worker-a")
    calls = _record_sends(monkeypatch, wt.messages)

    wt.q.close(item["ref"], "worker-a", resolution={"summary": "done quietly"})

    assert calls == []


def test_notify_events_is_scoped_per_queue(wt, monkeypatch):
    """One queue's preference must not mute another's."""
    wt.config.set_notify_events("SUB", [])
    calls = _record_sends(monkeypatch, wt.messages)
    item = wt.q.enqueue(project="OTHER", note="other queue", submitter="worker-sub")
    wt.q.claim_by_ref(item["ref"], "worker-a")

    wt.q.close(item["ref"], "worker-a", resolution={"summary": "done"})

    assert [t for t, _ in calls] == ["worker-sub"]


# ------------------------------------------------------ config.py: CRUD (WT-23)
def test_notify_events_defaults_to_everything_but_claimed(wt):
    assert wt.config.notify_events("NEVERCONFIGURED") == [
        "closed", "needs_input", "awaits_decision",
    ]


def test_set_notify_events_roundtrips_and_drops_unknown_events(wt):
    wt.config.set_notify_events("SUB", ["claimed", "nonsense", "closed"])
    assert wt.config.notify_events("SUB") == ["claimed", "closed"]


def test_set_notify_events_none_restores_the_default(wt):
    wt.config.set_notify_events("SUB", [])
    assert wt.config.notify_events("SUB") == []
    wt.config.set_notify_events("SUB", None)
    assert wt.config.notify_events("SUB") == list(wt.config.DEFAULT_NOTIFY_EVENTS)


def test_wt_config_notify_events_flag(wt_cli, capsys):
    assert wt_cli.cli.main(["config", "-q", "SUB", "--notify-events",
                            "claimed,closed"]) == 0
    assert wt_cli.config.notify_events("SUB") == ["claimed", "closed"]

    assert wt_cli.cli.main(["config", "-q", "SUB", "--notify-events", "none"]) == 0
    assert wt_cli.config.notify_events("SUB") == []

    assert wt_cli.cli.main(["config", "-q", "SUB", "--notify-events",
                            "default"]) == 0
    assert wt_cli.config.notify_events("SUB") == list(
        wt_cli.config.DEFAULT_NOTIFY_EVENTS
    )

    assert wt_cli.cli.main(["config", "-q", "SUB", "--notify-events",
                            "claimed,bogus"]) == 1
    err = capsys.readouterr().err
    assert "unknown notify event(s): bogus" in err

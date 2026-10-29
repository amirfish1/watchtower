"""WT-46: `wt answer` records who answered (by.kind) instead of always
"human", and the prompt a parked worker gets names that sender."""

from __future__ import annotations

from tests.test_park_blocks import SID, _parked, wt  # noqa: F401  (fixture)


def _answer_entry(wt, ref):
    return [h for h in wt.q.get(ref)["history"] if h["event"] == "answer"][-1]


def _quiet_routing(wt, monkeypatch):
    monkeypatch.setattr(wt.answers, "worker_alive", lambda wid: True)
    monkeypatch.setattr(wt.answers, "_wake", lambda *a, **k: None)


def test_explicit_by_worker_records_kind_worker(wt, monkeypatch):
    it = _parked(wt)
    _quiet_routing(wt, monkeypatch)
    assert wt.cli.main(["answer", it["ref"], "go left", "--worker", "wt-root",
                        "--by", "worker"]) == 0
    by = _answer_entry(wt, it["ref"])["by"]
    assert by["kind"] == "worker" and by["worker"] == "wt-root"
    assert wt.q.get(it["ref"])["pending_answer"]["by_kind"] == "worker"


def test_service_answer_with_worker_flag_stays_human(wt, monkeypatch):
    """Intake bridges pass --worker on real client replies: no agent env, so human."""
    it = _parked(wt)
    _quiet_routing(wt, monkeypatch)
    assert wt.cli.main(["answer", it["ref"], "go left", "--worker", "bridge"]) == 0
    assert _answer_entry(wt, it["ref"])["by"]["kind"] == "human"
    assert wt.q.get(it["ref"])["pending_answer"]["by_kind"] == "human"


def test_dashboard_style_answer_stays_human(wt, monkeypatch):
    it = _parked(wt)
    _quiet_routing(wt, monkeypatch)
    assert wt.cli.main(["answer", it["ref"], "go left"]) == 0
    assert _answer_entry(wt, it["ref"])["by"]["kind"] == "human"


def test_answer_inside_agent_session_defaults_to_worker(wt, monkeypatch):
    it = _parked(wt)
    _quiet_routing(wt, monkeypatch)
    monkeypatch.setenv("CODEX_THREAD_ID", "01a18042-0000-0000-0000-000000000000")
    assert wt.cli.main(["answer", it["ref"], "go left", "--worker", "wt-d8587d34"]) == 0
    assert _answer_entry(wt, it["ref"])["by"]["kind"] == "worker"


def test_explicit_by_human_overrides_agent_env(wt, monkeypatch):
    it = _parked(wt)
    _quiet_routing(wt, monkeypatch)
    monkeypatch.setenv("WT_WORKER_ID", "wt-relay")
    assert wt.cli.main(["answer", it["ref"], "owner said go left", "--by", "human"]) == 0
    assert _answer_entry(wt, it["ref"])["by"]["kind"] == "human"


def test_routed_prompt_names_the_sender_kind(wt):
    item = {"ref": "PK-1", "block_kind": ""}
    agent = wt.answers._answer_prompt(item, {"gen": 1, "answer": "x", "by_kind": "worker"})
    assert "An agent session (not a human) answered" in agent
    human = wt.answers._answer_prompt(item, {"gen": 1, "answer": "x", "by_kind": "human"})
    assert "A human answered" in human
    legacy = wt.answers._answer_prompt(item, {"gen": 1, "answer": "x"})
    assert "A human answered" in legacy
    brief = wt.answers.claim_brief(
        {"ref": "PK-1", "pending_answer": {"state": "delivered", "answer": "x",
                                           "by_kind": "worker", "prior_worker_id": "w1"}},
        worker_id="w1")
    assert "Agent answered: x" in brief


def test_held_block_prompt_names_an_agent_sender(wt, monkeypatch):
    """A legacy held (unparked) block resumes via _deliver_answer's prompt."""
    item = wt.q.enqueue(project="PK", note="x", source="test")
    wt.q.claim_by_ref(item["ref"], "w1", session_uuid=SID)
    wt.q.block(item["ref"], session_id="w1", question="q?", origin="system")
    sent = {}
    monkeypatch.setattr(wt.cli, "_deliver_to_blocked_session",
                        lambda it, text, prompt, *a: sent.setdefault("p", prompt) and 0)
    assert wt.cli.main(["answer", item["ref"], "do it", "--by", "worker"]) == 0
    assert sent["p"].startswith("An agent session (not a human) answered")
    assert _answer_entry(wt, item["ref"])["by"]["kind"] == "worker"

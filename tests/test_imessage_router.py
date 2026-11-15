import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "wt_imessage_router", Path(__file__).parent.parent / "scripts" / "wt_imessage_router.py")
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)


def test_ref_answer():
    assert r.parse_reply("projects-2: yes go", ["PROJECTS-2", "PROJECTS-3"]) == ("answer", "PROJECTS-2", "yes go")


def test_bare_text_single_pending_not_consumed():
    assert r.parse_reply("yes", ["PROJECTS-2"]) is None


def test_bare_text_many_pending_ignored():
    assert r.parse_reply("yes", ["A-1", "A-2"]) is None


def test_ref_not_pending_ignored():
    assert r.parse_reply("A-9 yes", ["A-1"]) is None


def test_ref_without_answer_ignored():
    assert r.parse_reply("A-1", ["A-1"]) is None


def test_bracketed_tag_answer():
    assert r.parse_reply("[A-1] yes", ["A-1"]) == ("answer", "A-1", "yes")


def test_prefix_ref_not_matched():
    assert r.parse_reply("A-10 yes", ["A-1"]) is None


def test_ticket():
    assert r.parse_reply("Ticket: fix the thing", []) == ("ticket", "fix the thing")


def test_empty():
    assert r.parse_reply("  ", ["A-1"]) is None


# --- pre-send check (WT_IMESSAGE_PRESEND_CHECK) -------------------------------

import json
import sys
import time

import pytest

T = {"ref": "PROJECTS-7", "project": "PROJECTS", "title": "Pick a vendor", "block_question": "A or B?"}
QUESTION = '[PROJECTS-7] needs you: Pick a vendor\n\nA or B?\n\nReply "PROJECTS-7 <answer>" (the ref is required).'


def test_question_wording_is_pinned():
    # The pre-send check rebuilds this text from the ticket and compares word
    # for word; changing the format here must change it there too.
    assert r.format_question(T) == QUESTION
    assert r.parse_reply("PROJECTS-7 yes", ["PROJECTS-7"]) == ("answer", "PROJECTS-7", "yes")
    assert r.REF_RE.pattern == r"^\s*\[?([A-Za-z][A-Za-z0-9_-]*-\d+)\]?(?![\w-])[\s:,-]*(.*)$"


@pytest.fixture
def router(monkeypatch, tmp_path):
    sent, reports, logs = [], [], []
    monkeypatch.setattr(r, "STATE", tmp_path / "state.json")
    monkeypatch.setattr(r, "send", lambda text: sent.append(text))
    monkeypatch.setattr(r, "bb_recent", lambda: [])
    monkeypatch.setattr(r, "log", logs.append)
    monkeypatch.setattr(r, "report", lambda *a: reports.append(a))
    monkeypatch.setattr(r, "wt", lambda *a: type("R", (), {"stdout": json.dumps([T]), "stderr": "", "returncode": 0})())
    state = {"seeded": True, "notified": [], "seen": []}
    return type("Ctx", (), {"sent": sent, "reports": reports, "logs": logs, "state": state,
                            "tick": lambda self: r.tick(state, ["PROJECTS", "MAZKIR"], "MAZKIR")})()


def test_no_check_configured_sends_as_before(router, monkeypatch):
    monkeypatch.setattr(r, "PRESEND_CHECK", "")
    assert r.presend("PROJECTS-7", QUESTION, ["PROJECTS"]) is None
    router.tick()
    assert router.sent == [QUESTION] and router.state["notified"] == ["PROJECTS-7"] and router.reports == []


def test_passed_question_is_sent_unchanged_and_outcome_reported(router, monkeypatch):
    monkeypatch.setattr(r, "presend", lambda ref, text, queues: (True, "IMSG-1", ""))
    router.tick()
    router.tick()
    assert router.sent == [QUESTION]  # once, word for word
    assert router.reports == [("outcome", "--id", "IMSG-1", "--outcome", "accepted")]


def test_refused_question_is_not_sent_and_is_rechecked_later(router, monkeypatch):
    asked = []
    monkeypatch.setattr(r, "presend", lambda ref, text, queues: asked.append(ref) or (False, "IMSG-2", "not-on-owner"))
    router.tick()
    router.tick()
    assert router.sent == [] and router.state["notified"] == [] and asked == ["PROJECTS-7"]  # not asked again every poll
    assert r.health["held"] == ["PROJECTS-7"] and any("HELD PROJECTS-7: not-on-owner" in line for line in router.logs)
    router.state["held"]["PROJECTS-7"] = time.time() - 1  # the retry time has passed, and now it is a real owner question
    monkeypatch.setattr(r, "presend", lambda ref, text, queues: (True, "IMSG-3", ""))
    router.tick()
    assert router.sent == [QUESTION] and router.state["held"] == {}


def test_failed_send_is_reported_and_not_retried(router, monkeypatch):
    monkeypatch.setattr(r, "presend", lambda ref, text, queues: (True, "IMSG-4", ""))
    def boom(text):
        raise RuntimeError("hermes send rc=1")
    monkeypatch.setattr(r, "send", boom)
    router.tick()
    router.tick()
    assert router.reports == [("outcome", "--id", "IMSG-4", "--outcome", "failed", "--detail", "RuntimeError")]
    assert router.state["notified"] == ["PROJECTS-7"]


def test_check_that_cannot_run_or_answers_badly_is_a_no(monkeypatch, tmp_path):
    monkeypatch.setattr(r, "PRESEND_CHECK", str(tmp_path / "missing.py"))
    assert r.presend("PROJECTS-7", QUESTION, ["PROJECTS"])[0] is False
    for body, expect in [("import sys; print('{\"ok\": true, \"id\": \"IMSG-9\"}')", True),
                         ("import sys; print('{\"ok\": true}')", False),                       # no check id
                         ("import sys; print('{\"ok\": true, \"id\": \"x\"}'); sys.exit(3)", False),
                         ("import sys; print('{\"ok\": false, \"id\": \"x\", \"code\": \"not-on-owner\"}'); sys.exit(3)", False),
                         ("print('yes')", False), ("raise SystemExit(0)", False),
                         ("import sys; assert sys.stdin.read() == " + repr(QUESTION) + " and sys.argv[1:] == ['check', '--ref', 'PROJECTS-7', '--queues', 'PROJECTS,MAZKIR']; "
                          "print('{\"ok\": true, \"id\": \"IMSG-9\"}')", True)]:
        script = tmp_path / "check.py"
        script.write_text(body)
        monkeypatch.setattr(r, "PRESEND_CHECK", str(script))
        assert r.presend("PROJECTS-7", QUESTION, ["PROJECTS", "MAZKIR"])[0] is expect, body


def test_answer_is_reported(router, monkeypatch):
    router.state["notified"] = ["PROJECTS-7"]
    monkeypatch.setattr(r, "bb_recent", lambda: [{"guid": "g1", "isFromMe": False, "text": "PROJECTS-7 go with A"}])
    router.tick()
    assert ("answer", "--ref", "PROJECTS-7", "--ok", "1") in router.reports
    assert router.sent == ["Answered PROJECTS-7"]

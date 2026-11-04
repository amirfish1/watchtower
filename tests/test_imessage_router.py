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

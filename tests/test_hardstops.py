"""The bundled hard-stop floor and how ``autodecide.floor_hits`` picks its rules."""
from __future__ import annotations

import pytest

from watchtower import autodecide, hardstops

SEND = "sending to customers"
PROD = "a production change, which reaches customers"
MONEY = "money or prices"
PUBLIC = "ad spend or a public post"
UNDO = "cannot be undone"


@pytest.fixture
def no_desk(tmp_path, monkeypatch):
    """A machine with no private desk script and no override."""
    monkeypatch.delenv("WT_HARD_STOPS_FILE", raising=False)
    monkeypatch.setattr(autodecide, "DESK_SCRIPT", tmp_path / "absent" / "decision_desk.py")


def _rules(path, body):
    path.write_text(body)
    return path


def test_rule_names_are_the_five_shown_to_users():
    assert [name for name, _ in hardstops.HARD_STOPS] == [SEND, PROD, MONEY, PUBLIC, UNDO]


@pytest.mark.parametrize("rule,text", [
    (SEND, "email the customers about the schedule change"),
    (SEND, "text every lead who signed up last week"),
    (SEND, "notify subscribers that the report is ready"),
    (SEND, "flip the SMS_SEND_ENABLED flag on"),
    (SEND, "queue the next cold email batch"),
    (SEND, "the newsletter goes out on Friday"),
    (PROD, "apply the migration in production"),
    (PROD, "deploy it to prod tonight"),
    (MONEY, "refund the buyer for last month"),
    (MONEY, "raise the plan price"),
    (MONEY, "create the Stripe invoice"),
    (MONEY, "add a coupon for the launch"),
    (MONEY, "raise the cap to $40"),
    (MONEY, "the plan costs €12 a month"),
    (MONEY, "it is £9 per seat"),
    (PUBLIC, "raise the ad budget for the weekend"),
    (PUBLIC, "tweet the release notes"),
    (PUBLIC, "publish the blog post"),
    (PUBLIC, "reword the landing page headline"),
    (PUBLIC, "post the demo on LinkedIn"),
    (PUBLIC, "post the launch to Hacker News"),
    (PUBLIC, "launch it on Product Hunt"),
    (UNDO, "this is irreversible once it runs"),
    (UNDO, "the change can't be undone"),
    (UNDO, "force-push the cleaned branch"),
    (UNDO, "rewrite the git history to remove the file"),
    (UNDO, "drop table sessions"),
    (UNDO, "truncate the events log"),
    (UNDO, "delete the customer records older than a year"),
    (UNDO, "rm -rf build"),
])
def test_bundled_list_flags_each_rule(rule, text):
    assert rule in hardstops.hard_stops(text)


@pytest.mark.parametrize("text", [
    "rename the helper and add a unit test",
    "bump the retry count from 2 to 3 in the test fixture",
    "revert with git revert abc123",
    "",
])
def test_bundled_list_passes_benign_engineering_text(text):
    assert hardstops.hard_stops(text) == []


def test_hits_are_ordered_and_unique():
    text = "refund $5, then refund $9, and email the customers. Later, rm -rf the production data."
    assert hardstops.hard_stops(text) == [SEND, PROD, MONEY, UNDO]


def test_matching_ignores_case():
    assert hardstops.hard_stops("EMAIL THE CUSTOMERS") == [SEND]


def test_floor_uses_bundled_list_when_desk_script_is_absent(no_desk):
    assert autodecide.floor_hits("refund the customer $5") == ([MONEY], "")
    assert autodecide.floor_hits("rename the helper and add a unit test") == ([], "")


def test_floor_still_uses_a_present_desk_script(tmp_path, monkeypatch):
    monkeypatch.delenv("WT_HARD_STOPS_FILE", raising=False)
    script = _rules(tmp_path / "desk.py", "def hard_stops(text):\n    return ['mine'] if 'zebra' in text else []\n")
    monkeypatch.setattr(autodecide, "DESK_SCRIPT", script)
    assert autodecide.floor_hits("paint the zebra") == (["mine"], "")
    assert autodecide.floor_hits("refund the customer $5") == ([], "")   # bundled list is not mixed in


@pytest.mark.parametrize("body,err", [
    ("def hard_stops(text:\n", "SyntaxError"),
    ("raise RuntimeError('boom')\n", "RuntimeError"),
    ("x = 1\n", "AttributeError"),
])
def test_floor_fails_closed_for_a_present_but_broken_desk_script(tmp_path, monkeypatch, body, err):
    monkeypatch.delenv("WT_HARD_STOPS_FILE", raising=False)
    monkeypatch.setattr(autodecide, "DESK_SCRIPT", _rules(tmp_path / "desk.py", body))
    assert autodecide.floor_hits("refund the customer $5") == ([], f"hard-stop floor unavailable ({err})")


def test_extra_rules_add_to_a_desk_script_and_never_replace_it(tmp_path, monkeypatch):
    desk = _rules(tmp_path / "desk.py", "def hard_stops(text):\n    return ['desk']\n")
    mine = _rules(tmp_path / "mine.py", "def hard_stops(text):\n    return []\n")
    monkeypatch.setattr(autodecide, "DESK_SCRIPT", desk)
    monkeypatch.setenv("WT_HARD_STOPS_FILE", str(mine))
    assert autodecide.floor_hits("anything") == (["desk"], "")


def test_extra_rules_add_to_the_bundled_list(tmp_path, no_desk, monkeypatch):
    mine = _rules(tmp_path / "mine.py",
                  "def hard_stops(text):\n    return ['our schema'] if 'schema' in text else []\n")
    monkeypatch.setenv("WT_HARD_STOPS_FILE", str(mine))
    assert autodecide.floor_hits("refund the customer $5") == ([MONEY], "")
    assert autodecide.floor_hits("change the schema") == (["our schema"], "")
    assert autodecide.floor_hits("refund it and change the schema") == ([MONEY, "our schema"], "")


def test_an_empty_extra_file_cannot_switch_the_floor_off(tmp_path, no_desk, monkeypatch):
    # The gate runs in the worker's process. A worker pointing the variable at
    # a file that matches nothing must not remove the built-in rules.
    mine = _rules(tmp_path / "mine.py", "def hard_stops(text):\n    return []\n")
    monkeypatch.setenv("WT_HARD_STOPS_FILE", str(mine))
    assert autodecide.floor_hits("refund the customer $5") == ([MONEY], "")


def test_env_override_missing_file_fails_closed(tmp_path, no_desk, monkeypatch):
    monkeypatch.setenv("WT_HARD_STOPS_FILE", str(tmp_path / "nope.py"))
    hits, err = autodecide.floor_hits("refund the customer $5")
    assert hits == [] and err.startswith("hard-stop floor unavailable (")


def test_env_override_broken_file_fails_closed(tmp_path, no_desk, monkeypatch):
    monkeypatch.setenv("WT_HARD_STOPS_FILE", str(_rules(tmp_path / "mine.py", "x = 1\n")))
    assert autodecide.floor_hits("refund the customer $5") == ([], "hard-stop floor unavailable (AttributeError)")


def test_empty_env_override_is_ignored(no_desk, monkeypatch):
    monkeypatch.setenv("WT_HARD_STOPS_FILE", "")
    assert autodecide.floor_hits("refund the customer $5") == ([MONEY], "")


def test_gate_vetoes_with_the_bundled_floor(wt_env, no_desk):
    res = autodecide.evaluate({"project": "T"}, {"class": "", "weight": "minor", "recommend": "refund the customer $5"})
    assert res["approve"] is False and MONEY in res["matched"]

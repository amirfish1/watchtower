"""Group-chat store tests: CCC on-disk compatibility, deterministic nudge
targeting, and the scheduler tick.

Everything runs against an isolated temp chats dir via $WATCHTOWER_CHATS_DIR
(same env-override pattern as the queue store), and the shared activity log is
pointed at a temp file, so no test ever touches ~/.claude/group-chats or the
real ~/.watchtower state.

The one compatibility quirk worth calling out: CCC parses message headings
with a line-anchored regex, so a heading-shaped line INSIDE a fenced code
block is still treated as a message boundary. That is replicated here on
purpose (see test_fence_heading_quirk_matches_ccc); "fixing" it would fork
the on-disk dialect and desync WT's parse from CCC's UI.
"""

from __future__ import annotations

import importlib
import json
import os
import re
import time

import pytest

# The exact heading regex CCC's UI uses (em-dash escaped; em-dashes are banned
# in this codebase except where the CCC file format itself requires them).
CCC_RE = re.compile(r"^##\s+.+?—\s+(?:([0-9a-fA-F]{8})\b|(Human)\b)")

SID_A = "aaaa1111-0000-0000-0000-000000000001"
SID_B = "bbbb2222-0000-0000-0000-000000000002"
SID_C = "cccc3333-0000-0000-0000-000000000003"


@pytest.fixture()
def chats(tmp_path, monkeypatch):
    """Isolated chats module: temp chats dir + temp activity log."""
    monkeypatch.setenv("WATCHTOWER_CHATS_DIR", str(tmp_path / "chats"))
    import watchtower.queue as q
    import watchtower.chats as chats_mod
    importlib.reload(q)
    importlib.reload(chats_mod)
    # queue._log appends to a module-level path; keep it hermetic.
    monkeypatch.setattr(q, "_ACTIVITY_LOG", tmp_path / "activity.log")
    return chats_mod


def _new_chat(chats, topic="decide next step", parts=None, **kw):
    parts = parts if parts is not None else [
        {"session_id": SID_A, "name": "planner"},
        {"session_id": SID_B, "name": "builder"},
    ]
    return chats.create_chat(topic, parts, **kw)


def _sidecar(chats, ref):
    _, sc = chats.find_chat(ref)
    return sc


def _write_sidecar(chats, ref, sc):
    md, _ = chats.find_chat(ref)
    md.with_suffix(".json").write_text(json.dumps(sc))


# ------------------------------------------------------------ round trip
def test_create_post_read_round_trip(chats):
    info = _new_chat(chats, topic="Ship The Feature")
    md_path = info["path"]
    assert os.path.isfile(md_path)
    assert os.path.isfile(info["sidecar_path"])
    assert info["uuid"]

    text = open(md_path).read()
    # Header block mirrors the real CCC files: title with the format's
    # em-dash, then Started/Mode/Participants lines, no blank line between.
    assert text.startswith("# Group Chat — Ship The Feature\n**Started:** ")
    assert "\n**Mode:** topic\n" in text
    assert "\n**Participants:** `planner`, `builder`, `human`\n" in text

    chats.post(md_path, "First take:\n\nlet us do X.", author_sid=SID_A)
    chats.post(md_path, "sounds right, go", author_name="Human")

    parsed = chats.read_chat(md_path)
    assert parsed["topic"] == "Ship The Feature"
    assert parsed["mode"] == "topic"
    assert [p["session_id"] for p in parsed["participants"]] == [SID_A, SID_B]
    msgs = parsed["messages"]
    assert len(msgs) == 2
    assert msgs[0]["author_sid8"] == SID_A[:8]
    assert msgs[0]["author_name"] == "planner"
    assert msgs[0]["body"] == "First take:\n\nlet us do X."
    assert msgs[1]["author_sid8"] is None
    assert msgs[1]["author_name"] == "Human"
    assert msgs[1]["body"] == "sounds right, go"

    # tail=1 returns only the last message.
    assert len(chats.read_chat(md_path, tail=1)["messages"]) == 1

    # Every produced heading must match CCC's parsing regex exactly.
    headings = [l for l in open(md_path).read().splitlines() if l.startswith("## ")]
    assert len(headings) == 2
    agent_m = CCC_RE.match(headings[0])
    human_m = CCC_RE.match(headings[1])
    assert agent_m and agent_m.group(1) == SID_A[:8]
    assert human_m and human_m.group(2) == "Human"
    # Timestamp shape observed in real CCC files: date, weekday, time (tz
    # optional depending on platform strftime %Z).
    assert re.match(
        r"^## \d{4}-\d{2}-\d{2} [A-Z][a-z]+ \d{2}:\d{2}:\d{2}", headings[0]
    )


def test_read_parses_real_world_shaped_fixture(chats, tmp_path):
    """A hand-built transcript replicating the structure of a real CCC chat:
    wake-status block, system blockquotes, --- separators, multi-paragraph
    bodies, and emoji-decorated display names."""
    d = chats.chats_dir()
    md = d / "fixture-chat-20260630-094359.md"
    sc = d / "fixture-chat-20260630-094359.json"
    md.write_text(
        "# Group Chat — decide who to continue\n"
        "**Started:** 2026-06-30 Tuesday 09:43:59 PDT\n"
        "**Mode:** topic\n"
        "**Participants:** `aaaa1111`, `bbbb2222`, `human`\n"
        "**Wake-status:**\n"
        "- `aaaa1111` (aaaa1111): online\n"
        "> _2026-06-30 09:43:59 PDT — system: created empty chat_\n"
        "---\n"
        "\n"
        "## 2026-06-30 Tuesday 09:44:37 PDT — aaaa1111: aaaa1111 \U0001f4ac\n"
        "\n"
        "Present. First paragraph of a longer report.\n"
        "\n"
        "Second paragraph, still the same message.\n"
        "\n"
        "\n"
        "---\n"
        "\n"
        "## 2026-06-30 Tuesday 09:45:00 PDT — Human\n"
        "\n"
        "ok, thanks both\n"
        "\n"
        "\n"
        "---\n"
    )
    sc.write_text(json.dumps({
        "uuid": "a885710b-1659-498f-b846-08cd1439fa05",
        "session_ids": [SID_A, SID_B],
        "topic": "decide who to continue",
        "mode": "topic",
        "name_map": {SID_A: "aaaa1111", SID_B: "bbbb2222"},
        "include_human": True,
        "started_at": 1782837839.9,
        "archived": False,
        "closed_at": None,
    }))

    parsed = chats.read_chat(str(md))
    msgs = parsed["messages"]
    assert len(msgs) == 2
    assert msgs[0]["author_sid8"] == "aaaa1111"
    assert msgs[0]["ts"] == "2026-06-30 Tuesday 09:44:37 PDT"
    assert msgs[0]["body"] == (
        "Present. First paragraph of a longer report.\n"
        "\n"
        "Second paragraph, still the same message."
    )
    assert msgs[1]["author_name"] == "Human"
    assert msgs[1]["body"] == "ok, thanks both"


def test_fence_heading_quirk_matches_ccc(chats):
    """CCC's heading regex is line-anchored (^## ...), with no code-fence
    awareness. A heading-shaped line inside a fenced code block IS parsed as
    a message boundary by CCC's UI, so this parser must do the same. Do NOT
    "fix" this: fence-awareness here would make WT's message count and nudge
    targeting disagree with what CCC renders from the same bytes."""
    info = _new_chat(chats)
    body = (
        "Quoting the transcript format:\n"
        "```\n"
        "## 2026-01-01 Thursday 00:00:00 PST — deadbeef: example\n"
        "## just a markdown header, no author token\n"
        "```\n"
        "end of quote"
    )
    chats.post(info["path"], body, author_sid=SID_A)

    msgs = chats.read_chat(info["path"])["messages"]
    # The heading-shaped fence line splits the entry in two (CCC behavior);
    # the plain "## just a markdown header" line does NOT match the regex
    # (no em-dash + author token), so it stays body text.
    assert len(msgs) == 2
    assert msgs[0]["author_sid8"] == SID_A[:8]
    assert msgs[1]["author_sid8"] == "deadbeef"
    assert "## just a markdown header" in msgs[1]["body"]


# ------------------------------------------------------------- targeting
def _md(*entries):
    """Build a minimal transcript from (author, body) pairs; author is a
    sid8 string or "Human"."""
    out = ["# Group Chat — t", "---", ""]
    for author, body in entries:
        if author == "Human":
            out.append("## 2026-06-30 Tuesday 10:00:00 PDT — Human")
        else:
            out.append(f"## 2026-06-30 Tuesday 10:00:00 PDT — {author}: {author}")
        out += ["", body, "", "", "---", ""]
    return "\n".join(out)


def _sc(*sids, names=None):
    return {
        "session_ids": list(sids),
        "name_map": names or {s: s[:8] for s in sids},
    }


def test_targets_agent_last_nudges_everyone_else(chats):
    md = _md((SID_A[:8], "my update"))
    sc = _sc(SID_A, SID_B, SID_C)
    assert chats.pick_nudge_targets(md, sc) == [SID_B, SID_C]


def test_targets_human_with_name_mention(chats):
    md = _md((SID_A[:8], "hello"), ("Human", "@builder please take this"))
    sc = _sc(SID_A, SID_B, names={SID_A: "planner", SID_B: "builder"})
    assert chats.pick_nudge_targets(md, sc) == [SID_B]


def test_targets_human_with_8hex_mention(chats):
    md = _md((SID_A[:8], "hello"), ("Human", f"over to {SID_B[:8]} for review"))
    sc = _sc(SID_A, SID_B, SID_C)
    assert chats.pick_nudge_targets(md, sc) == [SID_B]


def test_targets_human_no_mention_goes_to_prior_agent(chats):
    md = _md(
        (SID_A[:8], "first"),
        (SID_B[:8], "second"),
        ("Human", "what do you think?"),
    )
    sc = _sc(SID_A, SID_B, SID_C)
    assert chats.pick_nudge_targets(md, sc) == [SID_B]


def test_targets_human_only_chat_nudges_everyone(chats):
    md = _md(("Human", "anyone there?"))
    sc = _sc(SID_A, SID_B)
    assert chats.pick_nudge_targets(md, sc) == [SID_A, SID_B]


def test_targets_empty_transcript_nudges_everyone(chats):
    sc = _sc(SID_A, SID_B)
    assert chats.pick_nudge_targets("# Group Chat — t\n---\n", sc) == [SID_A, SID_B]


# ---------------------------------------------------- sidecar preservation
def test_sidecar_unknown_keys_preserved(chats):
    info = _new_chat(chats)
    ref = info["path"]

    # Plant keys this module does not own (real CCC sidecars have several).
    sc = _sidecar(chats, ref)
    sc["ccc_custom_field"] = "keep-me"
    sc["paused"] = True
    sc["keywords"] = ["a", "b"]
    _write_sidecar(chats, ref, sc)

    chats.post(ref, "note", author_sid=SID_A)
    chats.add_participant(ref, SID_C, "reviewer")
    chats.remove_participant(ref, SID_B)
    chats.set_archived(ref, True)

    sc = _sidecar(chats, ref)
    assert sc["ccc_custom_field"] == "keep-me"
    assert sc["paused"] is True
    assert sc["keywords"] == ["a", "b"]
    # And the mutations themselves landed.
    assert SID_C in sc["session_ids"]
    assert SID_B not in sc["session_ids"]
    assert sc["name_map"][SID_C] == "reviewer"
    assert sc["archived"] is True


# --------------------------------------------------------------- find_chat
def test_find_chat_by_name_path_prefix_and_uuid(chats):
    a = _new_chat(chats, topic="alpha topic one")
    b = _new_chat(chats, topic="beta other thing")

    # Full path and bare filename (with and without extension).
    md_a, _ = chats.find_chat(a["path"])
    assert str(md_a) == a["path"]
    fname = os.path.basename(a["path"])
    assert str(chats.find_chat(fname)[0]) == a["path"]
    assert str(chats.find_chat(fname[:-3])[0]) == a["path"]

    # Slug prefix.
    assert str(chats.find_chat("alpha-topic")[0]) == a["path"]
    assert str(chats.find_chat("beta")[0]) == b["path"]

    # Sidecar uuid prefix.
    assert str(chats.find_chat(a["uuid"][:8])[0]) == a["path"]

    # Ambiguity: two chats share a slug prefix.
    _new_chat(chats, topic="alpha topic two")
    with pytest.raises(ValueError, match="ambiguous"):
        chats.find_chat("alpha-topic")

    with pytest.raises(ValueError, match="no chat"):
        chats.find_chat("zzz-nothing")


# -------------------------------------------------------------- nudge tick
def _collecting_deliver():
    calls = []

    def deliver(sid, text):
        calls.append((sid, text))
        return True

    return calls, deliver


def test_nudge_tick_fires_and_dedups(chats):
    info = _new_chat(chats)
    chats.post(info["path"], "kick off", author_sid=SID_A)
    t0 = time.time()

    calls, deliver = _collecting_deliver()
    report = chats.nudge_tick(deliver, now=t0 + 5)
    name = os.path.basename(info["path"])
    assert report["results"][name]["action"] == "nudged"
    # Agent A posted last, so only B gets the nudge, with the full contract
    # text: chat path, topic, mode, target sid, and the latest heading quoted.
    assert [sid for sid, _ in calls] == [SID_B]
    text = calls[0][1]
    assert info["path"] in text
    assert "decide next step" in text
    assert "mode=topic" in text
    assert SID_B in text
    assert "Latest entry: ## " in text
    assert "kick off" not in text  # heading only, never the body

    sc = _sidecar(chats, info["path"])
    assert len(sc["nudge_history"]) == 1
    assert sc["last_reminder_key"].startswith("1:## ")
    assert sc["last_reminder_targets"] == [SID_B[:8]]

    # Unchanged transcript: skipped via the mtime gate.
    calls2, deliver2 = _collecting_deliver()
    report2 = chats.nudge_tick(deliver2, now=t0 + 500)
    assert report2["results"][name]["action"] == "skipped"
    assert calls2 == []

    # Even if the timing gates pass again (rewind the recorded nudge time),
    # the reminder key still dedups the identical transcript state.
    sc = _sidecar(chats, info["path"])
    sc["nudge_history"] = [t0 - 500]
    sc["last_reminder_at"] = t0 - 500
    _write_sidecar(chats, info["path"], sc)
    report3 = chats.nudge_tick(deliver2, now=t0 + 5)
    assert report3["results"][name]["action"] == "skipped"
    assert report3["results"][name]["reason"] == "dedup"
    assert calls2 == []


def test_nudge_tick_respects_interval(chats):
    info = _new_chat(chats)
    chats.post(info["path"], "first", author_sid=SID_A)
    name = os.path.basename(info["path"])
    t0 = time.time()

    sc = _sidecar(chats, info["path"])
    sc["nudge_interval_s"] = 100
    _write_sidecar(chats, info["path"], sc)

    calls, deliver = _collecting_deliver()
    now1 = t0 + 5
    assert chats.nudge_tick(deliver, now=now1)["results"][name]["action"] == "nudged"

    # New post, but not enough time since the last nudge -> interval skip.
    chats.post(info["path"], "second", author_sid=SID_A)
    os.utime(info["path"], (now1 + 10, now1 + 10))
    report = chats.nudge_tick(deliver, now=now1 + 60)
    assert report["results"][name]["action"] == "skipped"
    assert report["results"][name]["reason"] == "interval"

    # Past the interval -> fires again.
    report = chats.nudge_tick(deliver, now=now1 + 150)
    assert report["results"][name]["action"] == "nudged"
    assert len(calls) == 2


def test_nudge_tick_enforces_hourly_cap(chats):
    info = _new_chat(chats)
    chats.post(info["path"], "busy chat", author_sid=SID_A)
    name = os.path.basename(info["path"])
    t0 = time.time()

    sc = _sidecar(chats, info["path"])
    sc["max_auto_nudges_per_hour"] = 2
    sc["nudge_interval_s"] = 1
    # Two nudges already recorded in the trailing hour, but older than the
    # md mtime so the changed-gate still passes.
    sc["nudge_history"] = [t0 - 600, t0 - 300]
    _write_sidecar(chats, info["path"], sc)
    os.utime(info["path"], (t0 - 100, t0 - 100))

    calls, deliver = _collecting_deliver()
    report = chats.nudge_tick(deliver, now=t0)
    assert report["results"][name]["action"] == "capped"
    assert calls == []
    # History over an hour old ages out of the cap window.
    sc = _sidecar(chats, info["path"])
    sc["nudge_history"] = [t0 - 4000, t0 - 3700]
    _write_sidecar(chats, info["path"], sc)
    report = chats.nudge_tick(deliver, now=t0)
    assert report["results"][name]["action"] == "nudged"


def test_nudge_tick_closes_idle_chat(chats):
    info = _new_chat(chats)
    chats.post(info["path"], "old discussion", author_sid=SID_A)
    name = os.path.basename(info["path"])
    t0 = time.time()
    os.utime(info["path"], (t0 - 3000, t0 - 3000))  # idle > 2700s default

    calls, deliver = _collecting_deliver()
    report = chats.nudge_tick(deliver, now=t0)
    assert report["results"][name] == {"action": "closed", "reason": "idle"}
    assert calls == []
    assert _sidecar(chats, info["path"])["closed_at"] is not None

    # A closed chat is skipped entirely on later ticks.
    report = chats.nudge_tick(deliver, now=t0 + 10)
    assert name not in report["results"]
    assert report["checked"] == 0


def test_nudge_tick_closes_on_done_marker(chats):
    for marker in ("I think WE'RE DONE here, thanks all.", "✅ done"):
        info = _new_chat(chats, topic=f"wrap {len(marker)}")
        chats.post(info["path"], marker, author_sid=SID_A)
        name = os.path.basename(info["path"])
        calls, deliver = _collecting_deliver()
        report = chats.nudge_tick(deliver, now=time.time() + 5)
        assert report["results"][name] == {"action": "closed", "reason": "done-marker"}
        assert calls == []
        assert _sidecar(chats, info["path"])["closed_at"] is not None


def test_close_and_archive_lifecycle(chats):
    info = _new_chat(chats)
    assert chats.read_chat(info["path"])["closed_at"] is None
    chats.close_chat(info["path"])
    closed_at = chats.read_chat(info["path"])["closed_at"]
    assert isinstance(closed_at, float)
    # Idempotent: closing again keeps the original stamp.
    chats.close_chat(info["path"])
    assert chats.read_chat(info["path"])["closed_at"] == closed_at

    # list_chats hides archived chats unless asked.
    assert len(chats.list_chats()) == 1
    chats.set_archived(info["path"], True)
    assert chats.list_chats() == []
    rows = chats.list_chats(include_archived=True)
    assert len(rows) == 1
    assert rows[0]["archived"] is True
    assert rows[0]["topic"] == "decide next step"
    assert rows[0]["last_post_at"] is not None

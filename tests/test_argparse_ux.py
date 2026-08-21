"""CLI argument-parsing UX guards for WATCHTOWER-3.

A rejected `wt comment -q <QUEUE> <REF> "<long text>"` used to be a silent
data-loss trap: argparse echoed the whole argument payload back through stderr,
which -- piped through `head`/`tail` -- reads like a success confirmation. Two
fixes are locked in here:

1. `-q`/`--queue` is accepted (and ignored) on ref-based commands so the habitual
   form just works instead of erroring.
2. argparse error messages are length-capped so a rejected multi-kilobyte value
   can never bury or masquerade as the real diagnostic.
"""

from __future__ import annotations

import importlib

import pytest


@pytest.fixture()
def cli(tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(tmp_path / "activity.log"))
    import watchtower.queue as q
    import watchtower.cli as cli_mod

    importlib.reload(q)
    importlib.reload(cli_mod)
    return cli_mod


def test_redundant_queue_flag_accepted_on_ref_commands(cli):
    parser = cli.build_parser()
    for cmd in ("find", "comment", "close", "release", "block", "answer", "discuss"):
        # -q <QUEUE> must parse without raising SystemExit on these ref-based commands.
        argv = [cmd, "-q", "SOMEQ", "SOMEQ-1"]
        if cmd in ("comment", "answer"):
            argv.append("some text")
        args = parser.parse_args(argv)
        assert args.command == cmd
        assert getattr(args, "_ignored_queue") == "SOMEQ"


def test_comment_with_queue_flag_persists(cli, capsys):
    import watchtower.queue as q

    item = q.enqueue(project="TESTQ", title="t", note="t", text="")
    rc = cli.main(["comment", "-q", "TESTQ", item["ref"], "a comment via -q"])
    assert rc == 0
    events = [e.get("event") for e in q.get(item["ref"]).get("history", [])]
    assert "comment" in events


def test_error_message_is_length_capped(cli, capsys):
    huge = "X" * 4000
    with pytest.raises(SystemExit) as exc:
        cli.main(["comment", "TESTQ-1", "text", "--bogus", huge])
    assert exc.value.code == 2
    err = capsys.readouterr().err
    # The rejected 4KB value must not be echoed in full; the whole error stays
    # short enough to read as an error, not as a success confirmation.
    assert huge not in err
    assert len(err) < 600
    assert "error:" in err


def test_subcommand_error_prints_subcommand_usage(cli, capsys):
    with pytest.raises(SystemExit):
        cli.main(["comment", "R-1", "text", "--by", "not-a-choice"])
    err = capsys.readouterr().err
    assert "usage: wt comment" in err

"""`wt list` is accepted as an alias of `wt ls` (OPS-1479): workers guess it first."""

from __future__ import annotations

import importlib
import json


def test_list_is_an_alias_of_ls(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_ACTIVITY_LOG", str(tmp_path / "activity.log"))
    import watchtower.queue as q
    import watchtower.cli as cli

    importlib.reload(q)
    importlib.reload(cli)
    item = q.enqueue(project="DEMO", title="t", note="a ticket", text="")

    outs = []
    for verb in ("ls", "list"):
        assert cli.main([verb, "-q", "DEMO", "--json"]) == 0
        outs.append(json.loads(capsys.readouterr().out))
    assert outs[0] == outs[1]
    assert any(r["ref"] == item["ref"] for r in outs[0])

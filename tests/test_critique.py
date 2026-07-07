"""`wt critique` (WT-100): spawn two cross-family critique agents via the
same CCC delegate bridge `wt send`/`wt ask` already use. No real CCC, no real
agent processes: everything runs against a local in-thread http.server."""

from __future__ import annotations

import json

from test_messages import delegate, wt  # noqa: F401


def _cli(wt):
    import importlib
    import watchtower.cli as cli
    importlib.reload(cli)
    return cli


def test_critique_spawns_the_two_other_families(wt, delegate, monkeypatch, capsys):
    cli = _cli(wt)
    srv, url = delegate
    monkeypatch.setenv("WATCHTOWER_DELEGATE_URL", url)
    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", "parent-sid")

    rc = cli.main(["critique", "does this design hold up?", "--json"])
    assert rc == 0

    assert len(srv.requests) == 2
    engines = []
    for path, body in srv.requests:
        assert path == "/api/sessions/spawn"
        assert body["report_to"] == "parent-sid"
        assert "does this design hold up?" in body["prompt"]
        assert "contrarian" in body["prompt"]
        engines.append(body["engine"])
    # default family is claude -> the other two, in fixed order
    assert engines == ["codex", "antigravity"]

    out = json.loads(capsys.readouterr().out)
    assert [r["engine"] for r in out] == ["codex", "antigravity"]
    assert all(r["ok"] for r in out)


def test_critique_family_excludes_itself(wt, delegate, monkeypatch):
    cli = _cli(wt)
    srv, url = delegate
    monkeypatch.setenv("WATCHTOWER_DELEGATE_URL", url)

    rc = cli.main(["critique", "goal", "--family", "codex", "--json"])
    assert rc == 0
    engines = [body["engine"] for _, body in srv.requests]
    assert engines == ["claude", "antigravity"]


def test_critique_model_overrides_win(wt, delegate, monkeypatch):
    cli = _cli(wt)
    srv, url = delegate
    monkeypatch.setenv("WATCHTOWER_DELEGATE_URL", url)

    rc = cli.main([
        "critique", "goal", "--model1", "gemini", "--model2", "hermes", "--json",
    ])
    assert rc == 0
    engines = [body["engine"] for _, body in srv.requests]
    assert engines == ["gemini", "hermes"]


def test_critique_reports_a_spawn_failure(wt, delegate, monkeypatch, capsys):
    cli = _cli(wt)
    srv, url = delegate
    srv.responses.append((200, {"ok": False, "error": "codex_unavailable"}))
    monkeypatch.setenv("WATCHTOWER_DELEGATE_URL", url)

    rc = cli.main(["critique", "goal"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "error spawning codex: codex_unavailable" in err


def test_critique_without_delegate_fails_clearly(wt, capsys):
    cli = _cli(wt)
    rc = cli.main(["critique", "goal"])
    assert rc == 1
    assert "no delegate configured" in capsys.readouterr().err

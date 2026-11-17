import json
import os
import sys

import pytest

from watchtower import related_credentials as credentials


def test_explicit_key_precedes_command_and_command_does_not_export(tmp_path, monkeypatch):
    monkeypatch.setattr(credentials.storage, "data_dir", lambda: tmp_path)
    monkeypatch.delenv("TYPESAFE_API_KEY", raising=False)
    monkeypatch.delenv("JEV_API_KEY", raising=False)
    marker = tmp_path / "invoked"
    command = [sys.executable, "-c",
               f"from pathlib import Path; Path({str(marker)!r}).touch(); print('fixture-key')"]
    (tmp_path / "related-credentials.json").write_text(json.dumps({"command": command}))
    monkeypatch.setenv("JEV_API_KEY", "explicit-key")
    assert credentials.triage_key() == "explicit-key"
    assert not marker.exists()
    monkeypatch.delenv("JEV_API_KEY")
    before = dict(os.environ)
    assert credentials.triage_key() == "fixture-key"
    assert marker.exists()
    assert dict(os.environ) == before


def test_credential_failure_never_returns_or_reports_secret_output(tmp_path, monkeypatch):
    monkeypatch.setattr(credentials.storage, "data_dir", lambda: tmp_path)
    monkeypatch.delenv("TYPESAFE_API_KEY", raising=False)
    monkeypatch.delenv("JEV_API_KEY", raising=False)
    path = tmp_path / "related-credentials.json"
    for command in ["shell commands are not argv", [],
                    [sys.executable, "-c", "import sys; print('sensitive'); sys.exit(1)"],
                    [sys.executable, "-c", "print('')"]]:
        path.write_text(json.dumps({"command": command}))
        with pytest.raises(ValueError) as failure:
            credentials.triage_key()
        assert "sensitive" not in str(failure.value)

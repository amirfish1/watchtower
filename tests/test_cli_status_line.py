"""`wt status` service line: the daemon hosts the dashboard in-process."""
from watchtower import cli


def test_status_line_reports_dashboard_in_daemon(monkeypatch, capsys):
    states = {cli.DAEMON_PID_FILE: "running  (pid 42)", cli.DASHBOARD_PID_FILE: "stopped"}
    monkeypatch.setattr(cli, "_svc_state", lambda p: states[p])
    cli._print_status([])
    out = capsys.readouterr().out
    assert "daemon=running  (pid 42)  dashboard=in daemon" in out


def test_status_line_keeps_stopped_when_daemon_is_down(monkeypatch, capsys):
    monkeypatch.setattr(cli, "_svc_state", lambda p: "stopped")
    cli._print_status([])
    assert "dashboard=stopped" in capsys.readouterr().out

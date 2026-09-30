import json
import subprocess
import sys


def test_config_view_json_is_parseable():
    out = subprocess.run(
        [sys.executable, "-m", "watchtower.cli", "config", "-q", "OPS", "--json"],
        capture_output=True, text=True, timeout=60,
    )
    assert out.returncode == 0, out.stderr
    data = json.loads(out.stdout)
    assert data["queue"] == "OPS"
    assert "grace_s" in data["config"]
    assert "builder" in data["roles"]

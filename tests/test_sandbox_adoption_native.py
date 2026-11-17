"""WT-61 F3, native: the sanctioned test path for adopted bytes is the kernel
sandbox. Skips where the Linux sandbox is unavailable."""
from __future__ import annotations

from pathlib import Path

from test_sandbox_bridge import env  # noqa: F401
from test_sandbox_gates import _git, trusted_repo  # noqa: F401
from test_sandbox_policy import native  # noqa: F401
from watchtower import sandbox_handoff as handoff


def test_native_sandbox_test_contains_hostile_adopted_code(native, env, trusted_repo):
    canary = Path(trusted_repo["root"]) / "host-canary"
    repo = trusted_repo["repo"]
    (repo / "evil.py").write_text(
        "import pathlib, socket\n"
        f"for target in ({str(canary)!r}, {str(Path.home() / '.wt-adopted-canary')!r}):\n"
        "    try:\n        pathlib.Path(target).write_text('escaped')\n"
        "        print('WROTE', target)\n    except OSError:\n        print('DENIED', target)\n"
        "try:\n    socket.create_connection(('1.1.1.1', 53), timeout=2)\n    print('NET')\n"
        "except OSError:\n    print('NONET')\n")
    _git(repo, "add", "evil.py")
    _git(repo, "commit", "-qm", "adopted hostile")
    head = _git(repo, "rev-parse", "HEAD")
    result = handoff.sandbox_test(repo, head, "python3 evil.py && test \"$(cat v.txt)\" = two")
    assert result["passed"] and result["sandboxed"] is True and result["commit"] == head
    assert "NONET" in result["output_tail"] and "NET\n" not in result["output_tail"].replace("NONET", "")
    assert "WROTE" not in result["output_tail"] or not canary.exists()
    assert not canary.exists() and not (Path.home() / ".wt-adopted-canary").exists()


def test_native_sandbox_test_failure_is_a_failed_result_not_a_host_run(native, env, trusted_repo):
    result = handoff.sandbox_test(trusted_repo["repo"], trusted_repo["head"], "exit 7")
    assert result["passed"] is False and result["exit_code"] == 7 and result["sandboxed"] is True
    missing = handoff.sandbox_test(trusted_repo["root"] / "no-such-repo", trusted_repo["head"], "true")
    assert missing["passed"] is False and missing.get("sandboxed") is not False

"""Release plumbing: the two version strings must never drift apart.

``pyproject.toml`` is what pip installs report; ``watchtower.__version__`` is
what ``wt --version`` prints. scripts/cut-release.sh bumps both in one step, so
a mismatch here means someone edited one by hand — which is exactly how you end
up unable to tell which build a worker is running.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

import pytest

import watchtower

REPO_ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = REPO_ROOT / "pyproject.toml"
CUT_RELEASE = REPO_ROOT / "scripts" / "cut-release.sh"

SEMVER = re.compile(r"^\d+\.\d+\.\d+$")


def _pyproject_version() -> str:
    for line in PYPROJECT.read_text().splitlines():
        if line.startswith("version = "):
            return line.split("=", 1)[1].strip().strip('"')
    raise AssertionError("no top-level version = \"...\" line in pyproject.toml")


def test_versions_are_in_lockstep():
    assert _pyproject_version() == watchtower.__version__


def test_version_is_semver():
    # cut-release.sh parses the current version with a X.Y.Z regex and refuses
    # to run if it cannot; keep the file in a shape the script can read.
    assert SEMVER.match(watchtower.__version__)


def test_cut_release_is_executable():
    assert CUT_RELEASE.exists()
    assert CUT_RELEASE.stat().st_mode & 0o111, "scripts/cut-release.sh is not executable"


@pytest.mark.parametrize("arg", ["1.2", "v0.2.0", "banana"])
def test_cut_release_rejects_bad_versions(arg):
    # Argument validation happens before any preflight or mutation, so this is
    # safe to run from any branch/tree state.
    proc = subprocess.run(
        [str(CUT_RELEASE), arg, "--dry-run"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 2, proc.stdout + proc.stderr


def test_cut_release_refuses_wrong_branch(tmp_path):
    # The guard that matters most: a release cut from a feature branch would tag
    # code that never landed on main.
    proc = subprocess.run(
        [str(CUT_RELEASE), "9.9.9", "--dry-run"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env={"PATH": "/usr/bin:/bin:/usr/local/bin", "WT_RELEASE_BRANCH": "no-such-branch"},
    )
    assert proc.returncode == 1
    assert "no-such-branch" in proc.stderr


def test_wt_version_flag_prints_the_package_version():
    proc = subprocess.run(
        [sys.executable, "-m", "watchtower.cli", "--version"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, proc.stderr
    assert watchtower.__version__ in proc.stdout + proc.stderr

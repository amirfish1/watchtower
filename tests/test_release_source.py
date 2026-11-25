"""The scheduled release publishes the revision it tested (OPS-1030).

CCC's release-daily.sh cuts WatchTower from the shared clone after a 40-minute
CCC run, while other sessions keep committing and pushing. The pinned cut must
tag exactly the tested source plus the version bump, keep later work on main,
and leave the shared clone's tree alone.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

FAKE_PYTHON = r'''#!/usr/bin/env bash
if [ "${1:-}" = -m ] && [ "${2:-}" = pytest ]; then
  [ "$(git rev-parse HEAD)" = "$RELEASE_EXPECTED_SHA" ] || exit 1
  [ "$PWD" != "$RELEASE_SHARED" ] || exit 1
  # A peer lands work on origin and leaves an edit in the shared clone.
  printf 'later\n' > "$RELEASE_PEER/later.txt"
  git -C "$RELEASE_PEER" add later.txt
  git -C "$RELEASE_PEER" commit -qm later
  if [ "$RELEASE_CHANGE" = tag ]; then
    git -C "$RELEASE_PEER" tag -a v0.0.1 -m competing
    git -C "$RELEASE_PEER" push -q origin main v0.0.1
  else
    git -C "$RELEASE_PEER" push -q origin main
  fi
  printf 'peer edit\n' > "$RELEASE_SHARED/notes.txt"
  exit 0
fi
exec "$RELEASE_PYTHON" "$@"
'''

FAKE_GH = r'''#!/usr/bin/env bash
[ "$1" != release ] || printf '%s\n' "$*" >> "$RELEASE_GH_LOG"
'''


@pytest.mark.parametrize("change", ["source", "tag"])
def test_pinned_cut_publishes_only_the_tested_source(tmp_path, change):
    shared, remote, peer, bin_dir = (tmp_path / n for n in ("shared", "remote.git", "peer", "bin"))
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    env.update(
        GIT_CONFIG_GLOBAL=os.devnull,
        GIT_CONFIG_NOSYSTEM="1",
        GIT_AUTHOR_NAME="Release Fixture",
        GIT_AUTHOR_EMAIL="release@example.invalid",
        GIT_COMMITTER_NAME="Release Fixture",
        GIT_COMMITTER_EMAIL="release@example.invalid",
        TMPDIR=str(tmp_path),
        RELEASE_SHARED=str(shared),
        RELEASE_PEER=str(peer),
        RELEASE_CHANGE=change,
        RELEASE_PYTHON=sys.executable,
        RELEASE_GH_LOG=str(tmp_path / "gh.log"),
    )

    def git(*args, cwd=shared):
        return subprocess.run(["git", *args], cwd=cwd, env=env, check=True,
                              capture_output=True, text=True).stdout.strip()

    (shared / "scripts").mkdir(parents=True)
    (shared / "watchtower").mkdir()
    shutil.copy2(ROOT / "scripts" / "cut-release.sh", shared / "scripts" / "cut-release.sh")
    (shared / "pyproject.toml").write_text('version = "0.0.0"\n')
    (shared / "watchtower" / "__init__.py").write_text('__version__ = "0.0.0"\n')
    (shared / "notes.txt").write_text("original\n")
    git("init", "-qb", "main")
    git("add", ".")
    git("commit", "-qm", "tested candidate")
    candidate = git("rev-parse", "HEAD")
    git("init", "--bare", "-qb", "main", str(remote), cwd=tmp_path)
    git("remote", "add", "origin", str(remote))
    git("push", "-qu", "origin", "main")
    git("clone", "-q", str(remote), str(peer), cwd=tmp_path)

    bin_dir.mkdir()
    for name, body in (("python3", FAKE_PYTHON), ("gh", FAKE_GH)):
        (bin_dir / name).write_text(body)
        (bin_dir / name).chmod(0o755)
    env.update(PATH=f"{bin_dir}:{env['PATH']}", RELEASE_EXPECTED_SHA=candidate)

    result = subprocess.run(
        ["bash", "scripts/cut-release.sh", "patch", "--source-commit", candidate],
        cwd=shared, env=env, capture_output=True, text=True,
    )

    git("fetch", "-q", "--tags", "--force", "origin")
    remote_main = git("rev-parse", "origin/main")
    later = git("rev-parse", "origin/main", cwd=peer) if change == "tag" else None
    assert git("rev-parse", "HEAD") == candidate, "shared clone HEAD must not move"
    assert (shared / "notes.txt").read_text() == "peer edit\n", "shared tree must be untouched"
    assert len(git("worktree", "list").splitlines()) == 1, "release worktree must be removed"
    assert not list(tmp_path.glob("wt-release-cut.*"))

    if change == "tag":
        # A competing release already owns the tag: nothing new is published.
        assert result.returncode != 0
        assert remote_main == later
        assert git("show", "v0.0.1:pyproject.toml") == 'version = "0.0.0"'
        assert not (tmp_path / "gh.log").exists()
        return

    assert result.returncode == 0, result.stdout + result.stderr
    tag_commit = git("rev-parse", "v0.0.1^{commit}")
    assert git("rev-parse", f"{tag_commit}^") == candidate
    assert git("show", "v0.0.1:pyproject.toml") == 'version = "0.0.1"'
    assert "later.txt" not in git("ls-tree", "--name-only", "v0.0.1")
    assert git("show", "origin/main:pyproject.toml") == 'version = "0.0.1"'
    assert git("show", "origin/main:later.txt") == "later"
    git("merge-base", "--is-ancestor", tag_commit, remote_main)
    assert "release create v0.0.1 --verify-tag" in (tmp_path / "gh.log").read_text()

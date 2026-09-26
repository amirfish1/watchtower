"""Routing a ticket filed by repo_path alone to its configured queue.

A client (CCC's Annotate, the widget) often files with only a ``repo_path``.
When that path is a second checkout of a configured repo -- an app's installed
copy, a linked worktree, a symlink -- it is the same project and must reach the
configured queue, not an auto-created queue named after the folder (which has
no workers, so the ticket is never drained).
"""

from __future__ import annotations

import os
import subprocess

import pytest

REMOTE = "https://github.com/example/widget-app"


def _git(*args, cwd):
    subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True)


def _clone_with_origin(path, url=REMOTE):
    path.mkdir(parents=True)
    _git("init", "-q", cwd=path)
    _git("remote", "add", "origin", url, cwd=path)
    return path


def test_exact_repo_path_still_routes_to_configured_queue(wt_env):
    repo = _clone_with_origin(wt_env.tmp / "Apps" / "widget-app")
    wt_env.config.set_repo_path("WA", str(repo))
    item = wt_env.queue.enqueue(note="exact", source="ccc", repo_path=str(repo))
    assert item["project"] == "WA"


def test_installed_copy_with_same_origin_routes_to_configured_queue(wt_env):
    dev = _clone_with_origin(wt_env.tmp / "Apps" / "widget-app")
    # The installed copy: a different folder, same origin written in scp form.
    installed = _clone_with_origin(
        wt_env.tmp / ".app-install" / "widget-app", "git@github.com:Example/widget-app.git"
    )
    wt_env.config.set_repo_path("WA", str(dev))
    item = wt_env.queue.enqueue(note="from installed", source="ccc", repo_path=str(installed))
    assert item["project"] == "WA"


def test_linked_worktree_routes_to_configured_queue(wt_env):
    dev = _clone_with_origin(wt_env.tmp / "Apps" / "widget-app")
    _git("-c", "user.email=t@example.com", "-c", "user.name=t",
         "commit", "-q", "--allow-empty", "-m", "init", cwd=dev)
    wt_path = wt_env.tmp / "Apps" / "widget-app-wt-feature"
    _git("worktree", "add", "-q", str(wt_path), cwd=dev)
    wt_env.config.set_repo_path("WA", str(dev))
    item = wt_env.queue.enqueue(note="from worktree", source="ccc", repo_path=str(wt_path))
    assert item["project"] == "WA"


def test_symlinked_path_routes_to_configured_queue(wt_env):
    dev = wt_env.tmp / "Apps" / "plain"
    dev.mkdir(parents=True)  # not even a git repo
    link = wt_env.tmp / "plain-link"
    os.symlink(dev, link)
    wt_env.config.set_repo_path("PL", str(dev))
    item = wt_env.queue.enqueue(note="via symlink", source="ccc", repo_path=str(link))
    assert item["project"] == "PL"


def test_unrelated_repo_still_falls_back_to_folder_name(wt_env):
    dev = _clone_with_origin(wt_env.tmp / "Apps" / "widget-app")
    other = _clone_with_origin(wt_env.tmp / "Apps" / "gadget", "https://github.com/example/gadget")
    wt_env.config.set_repo_path("WA", str(dev))
    item = wt_env.queue.enqueue(note="other repo", source="ccc", repo_path=str(other))
    assert item["project"] == "GADGET"


def test_explicit_project_beats_repo_identity(wt_env):
    dev = _clone_with_origin(wt_env.tmp / "Apps" / "widget-app")
    installed = _clone_with_origin(wt_env.tmp / "install" / "widget-app")
    wt_env.config.set_repo_path("WA", str(dev))
    item = wt_env.queue.enqueue(
        note="explicit", source="ccc", repo_path=str(installed), project="OTHER"
    )
    assert item["project"] == "OTHER"


@pytest.mark.parametrize(
    "url,expected",
    [
        ("https://github.com/Example/Widget-App.git", "github.com/example/widget-app"),
        ("https://user:tok@github.com/example/widget-app/", "github.com/example/widget-app"),
        ("ssh://git@github.com:22/example/widget-app.git", "github.com/example/widget-app"),
        ("git@github.com:example/widget-app.git", "github.com/example/widget-app"),
        ("/srv/git/widget-app.git", ""),
        ("", ""),
    ],
)
def test_normalize_remote_url(wt_env, url, expected):
    assert wt_env.queue._normalize_remote_url(url) == expected

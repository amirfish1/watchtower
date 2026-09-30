"""Keep the installed checkout (the tree ``wt`` imports) a deploy-only copy.

WT-16: workers on the WT queue must edit a dev clone (the queue's
``repo_path``), never the installed tree, so half-written code is never live.
The installed tree only ever moves by ``git merge --ff-only origin/<branch>``,
and only when it is clean, on its branch, and strictly behind.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any, Dict, List

# Last refused sync, so a refusal is visible in ``wt status`` and alerts once
# per cause instead of every daemon tick (WT-18).
STATE_FILE = Path.home() / ".watchtower" / "deploy-state.json"
_HISTORY_DEPTH = 300


def installed_root() -> str:
    """Toplevel of the git checkout this package runs from, or "" (pipx/venv)."""
    r = _git(str(Path(__file__).resolve().parent), "rev-parse", "--show-toplevel")
    return r.stdout.strip() if r.returncode == 0 else ""


def _git(repo: str, *args: str, timeout: int = 60) -> subprocess.CompletedProcess:
    try:
        return subprocess.run(["git", "-C", repo, *args], capture_output=True,
                              text=True, timeout=timeout)
    except (OSError, subprocess.TimeoutExpired) as e:
        return subprocess.CompletedProcess(args, 1, "", str(e))


def status(fetch: bool = True) -> Dict[str, Any]:
    """Running version vs origin: ``{path, branch, head, origin_head, behind,
    ahead, dirty}``; ``{"path": ""}`` when not a git checkout."""
    repo = installed_root()
    if not repo:
        return {"path": ""}
    if fetch:
        _git(repo, "fetch", "-q", "origin")
    branch = _git(repo, "rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
    head = _git(repo, "rev-parse", "HEAD").stdout.strip()
    origin = _git(repo, "rev-parse", f"origin/{branch}").stdout.strip()
    counts = _git(repo, "rev-list", "--left-right", "--count",
                  f"origin/{branch}...HEAD").stdout.split()
    behind, ahead = (int(counts[0]), int(counts[1])) if len(counts) == 2 else (0, 0)
    dirty = [ln[3:] for ln in _git(repo, "status", "--porcelain",
                                   "--untracked-files=no").stdout.splitlines()]
    return {"path": repo, "branch": branch, "head": head, "origin_head": origin,
            "behind": behind, "ahead": ahead, "dirty": dirty}


def _stale_files(repo: str, branch: str, files: List[str]) -> bool:
    """True when every dirty file's content equals a blob already in
    origin/<branch>'s history for that path: a stale checkout (the ref moved,
    the file did not), not new work. A deleted or novel file is never stale."""
    if not files:
        return False
    for f in files:
        full = Path(repo) / f
        if not full.is_file():
            return False
        h = _git(repo, "hash-object", "--", f).stdout.strip()
        revs = _git(repo, "rev-list", f"--max-count={_HISTORY_DEPTH}",
                    f"origin/{branch}", "--", f).stdout.split()
        if not h or not any(_git(repo, "rev-parse", f"{r}:{f}").stdout.strip() == h
                            for r in revs):
            return False
    return True


def _load_state() -> Dict[str, Any]:
    try:
        return json.loads(STATE_FILE.read_text())
    except (OSError, ValueError):
        return {}


def _save_state(data: Dict[str, Any]) -> None:
    try:
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        if data:
            STATE_FILE.write_text(json.dumps(data, indent=2))
        else:
            STATE_FILE.unlink(missing_ok=True)
    except OSError:
        pass


def refusal() -> Dict[str, Any]:
    """The standing refused-sync record (``{}`` when the last sync was fine)."""
    return _load_state().get("refused") or {}


def warning_line() -> str:
    """One-line ``wt status`` banner for a standing refusal, else ""."""
    r = refusal()
    if not r:
        return ""
    return f"⚠ installed WatchTower {r.get('behind', '?')} behind — refused: {r.get('reason', '?')}"


def _log(event: str, msg: str) -> None:
    try:
        from .queue import _log as qlog
        qlog(event, msg)
    except Exception:  # noqa: BLE001
        pass


def _alert(st: Dict[str, Any], reason: str) -> str:
    """File one ticket (the fleet's human-visible alert channel, like the
    launch-failure alert) describing a refused sync; returns its ref."""
    try:
        from . import queue as _q
        from . import workers
        item = _q.enqueue(
            note=f"Installed WatchTower {st.get('behind')} behind origin — sync refused: {reason}",
            title="Installed WatchTower sync refused",
            text=(f"{st.get('path')} is {st.get('behind')} commit(s) behind origin/"
                  f"{st.get('branch')} and the ff-only sync refused: {reason}\n\n"
                  "Restore or commit the listed files, then run `wt deploy --sync`."),
            project=workers._alert_launch_failure_queue("WT"),
            source="wt", item_type="bug", priority="p1",
        )
        return str(item.get("ref") or "")
    except Exception:  # noqa: BLE001
        return ""


def _refuse(st: Dict[str, Any], reason: str) -> Dict[str, Any]:
    """Record a refusal; alert once per distinct cause."""
    cause = f"{reason}|{st.get('head')}|{st.get('origin_head')}"
    state = _load_state()
    prev = state.get("refused") or {}
    rec = {"reason": reason, "behind": st.get("behind"), "cause": cause,
           "since": prev.get("since") if prev.get("cause") == cause else None,
           "alerted_ref": prev.get("alerted_ref", "") if prev.get("cause") == cause else ""}
    if not rec["alerted_ref"]:
        rec["alerted_ref"] = _alert(st, reason)
        from datetime import datetime, timezone
        rec["since"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        _log("DEPLOY_REFUSED", reason)
    state["refused"] = rec
    _save_state(state)
    return {**st, "action": "refused", "reason": reason}


def sync() -> Dict[str, Any]:
    """Fast-forward the installed tree to origin. Returns ``status()`` plus
    ``action``: moved | up-to-date | refused (with ``reason``) | skipped.

    Dirty files that only replay blobs already in origin's history are a stale
    checkout: they are restored and the sync proceeds (``restored`` lists
    them). Genuinely new edits refuse, loudly (``refusal()``/``warning_line()``)."""
    st = status(fetch=True)
    if not st["path"]:
        return {**st, "action": "skipped", "reason": "not a git checkout"}
    if not st["behind"]:
        _save_state({k: v for k, v in _load_state().items() if k != "refused"})
        return {**st, "action": "up-to-date"}
    restored: List[str] = []
    if st["dirty"]:
        if not _stale_files(st["path"], st["branch"], st["dirty"]):
            return _refuse(st, "installed tree has uncommitted changes: "
                               + ", ".join(st["dirty"][:5]))
        r = _git(st["path"], "restore", "--source=HEAD", "--staged", "--worktree",
                 "--", *st["dirty"])
        if r.returncode != 0:
            return _refuse(st, "could not restore stale files: " + (r.stderr or r.stdout).strip())
        restored = list(st["dirty"])
        _log("DEPLOY_RESTORE", "restored stale installed-tree files: " + ", ".join(restored))
        st = {**st, "dirty": []}
    if st["ahead"]:
        return _refuse(st, "installed tree has local commits")
    r = _git(st["path"], "merge", "--ff-only", f"origin/{st['branch']}")
    if r.returncode != 0:
        return _refuse(st, (r.stderr or r.stdout).strip())
    _save_state({k: v for k, v in _load_state().items() if k != "refused"})
    out = {**status(fetch=False), "action": "moved", "was": st["head"]}
    if restored:
        out["restored"] = restored
    return out

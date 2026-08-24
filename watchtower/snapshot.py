# Copyright (c) 2026 Amir Fish. All rights reserved.
# SPDX-License-Identifier: LicenseRef-WatchTower-Software-License

"""Auto-snapshot ("token-sitter"): checkpoint an idle session before the
prompt-cache cliff, with a one-shot detached timer per armed session.

Spec: docs/superpowers/specs/2026-08-19-auto-snapshot-design.md. No daemon:
`arm` spawns one detached timer process that sleeps, re-checks true idle from
the transcript mtime, and fires at most once inside the window
[threshold, CACHE_TTL) -- or skips (never injects) when it overslept past the
TTL, e.g. after a laptop suspend."""

from __future__ import annotations

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from typing import Any, Dict, Optional

CACHE_TTL_MIN = 60      # measured prompt-cache cliff (99.5% hit at 59 min, 12% at 61)
DEFAULT_IDLE_MIN = 55
TIMER_CAP_S = 24 * 3600  # hard lifetime cap so a timer can never orphan


def snapshots_dir() -> Path:
    return Path(
        os.environ.get("WATCHTOWER_SNAPSHOTS_DIR")
        or (Path.home() / ".watchtower" / "snapshots")
    )


def snapshot_path(session_id: str) -> Path:
    return snapshots_dir() / f"{session_id}.md"


def timer_state_path(session_id: str) -> Path:
    return snapshots_dir() / "timers" / f"{session_id}.json"


def cwd_slug(cwd: str) -> str:
    """Match claude's project-dir slug: every '/' and '.' becomes '-'."""
    return str(cwd).replace("/", "-").replace(".", "-")


def latest_link(cwd: str) -> Path:
    return snapshots_dir() / "by-cwd" / cwd_slug(cwd) / "latest"


def load_state(session_id: str) -> Optional[Dict[str, Any]]:
    try:
        with open(timer_state_path(session_id)) as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def save_state(session_id: str, state: Dict[str, Any]) -> None:
    path = timer_state_path(session_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2, sort_keys=True)
    tmp.replace(path)


def next_action(now: float, mtime: float, armed_at: float,
                threshold_s: float, ttl_s: float,
                cap_s: float = TIMER_CAP_S) -> tuple:
    """Decide the timer's next move from the transcript's true idle time."""
    if now - armed_at >= cap_s:
        return ("expire",)
    idle = now - mtime
    if idle < threshold_s:
        return ("sleep", threshold_s - idle)
    if idle < ttl_s:
        return ("fire",)
    return ("skip",)


def transcript_mtime(session_id: str, engine: str) -> Optional[float]:
    from . import messages
    if engine == "codex":
        path = messages._find_codex_rollout(session_id)
    else:
        path = messages._find_transcript(session_id)
    if path is None:
        return None
    try:
        return path.stat().st_mtime
    except OSError:
        return None


def ccc_handover_flag_set(session_id: str) -> bool:
    flag_file = Path.home() / ".claude" / "command-center" / "auto-handover.json"
    try:
        with open(flag_file) as f:
            flags = json.load(f)
    except (OSError, json.JSONDecodeError):
        return False
    entry = flags.get(session_id) if isinstance(flags, dict) else None
    return bool(isinstance(entry, dict) and entry.get("enabled"))


def _pid_alive(pid: int) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
    except (OSError, ProcessLookupError):
        return False
    return True


def _spawn_timer(session_id: str) -> int:
    """Detach a one-shot timer process; returns its pid."""
    log_dir = Path.home() / ".watchtower" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    logf = open(log_dir / f"snapshot-{session_id[:8]}.log", "ab")
    try:
        proc = subprocess.Popen(
            [sys.executable, "-m", "watchtower.cli",
             "snapshot", "timer-run", session_id],
            stdout=logf, stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL, start_new_session=True,
        )
    finally:
        logf.close()
    return proc.pid


MODES = ("mdfile", "compact", "both")


def arm(session_id: str, engine: str, cwd: str,
        idle_min: float = DEFAULT_IDLE_MIN, spawn: bool = True,
        mode: str = "mdfile") -> Dict[str, Any]:
    if engine not in ("claude", "codex"):
        return {"ok": False,
                "error": f"auto-fire is not supported for engine '{engine}' yet; "
                         "use /snapshot-now before stepping away"}
    if mode not in MODES:
        return {"ok": False,
                "error": f"--mode must be one of {', '.join(MODES)} (got '{mode}')"}
    if mode in ("compact", "both") and engine != "claude":
        return {"ok": False,
                "error": f"--mode {mode} needs Claude Code's /compact, which engine "
                         f"'{engine}' doesn't support yet; use --mode mdfile"}
    if idle_min >= CACHE_TTL_MIN:
        return {"ok": False,
                "error": f"--idle {idle_min:g} is at/past the {CACHE_TTL_MIN}-min "
                         "cache TTL; the fire window would be empty"}
    if idle_min <= 0:
        return {"ok": False, "error": "--idle must be positive"}
    prior = load_state(session_id)
    if prior and prior.get("outcome") == "armed":
        disarm(session_id)  # re-arm replaces
    state = {
        "session_id": session_id, "engine": engine, "cwd": cwd,
        "idle_min": float(idle_min), "armed_at": time.time(),
        "pid": 0, "outcome": "armed", "detail": "", "fired_at": None,
        "mode": mode,
    }
    save_state(session_id, state)
    if spawn:
        state["pid"] = _spawn_timer(session_id)
        save_state(session_id, state)
    return {"ok": True, "state": state,
            "ccc_handover_armed": ccc_handover_flag_set(session_id)}


def disarm(session_id: str) -> Dict[str, Any]:
    state = load_state(session_id)
    if not state:
        return {"ok": False, "error": f"no timer state for session {session_id}"}
    pid = int(state.get("pid") or 0)
    if _pid_alive(pid):
        try:
            os.kill(pid, signal.SIGTERM)
        except OSError:
            pass
    if state.get("outcome") == "armed":
        state["outcome"] = "disarmed"
        save_state(session_id, state)
    return {"ok": True, "state": state}


def status(session_id: Optional[str] = None) -> list:
    timers_dir = snapshots_dir() / "timers"
    sids = ([session_id] if session_id else
            sorted(p.stem for p in timers_dir.glob("*.json")))
    out = []
    for sid in sids:
        state = load_state(sid)
        if state:
            state["timer_alive"] = _pid_alive(int(state.get("pid") or 0))
            out.append(state)
    return out


def build_fire_prompt(session_id: str, engine: str, cwd: str, idle_min: float,
                       mode: str = "mdfile") -> str:
    if mode == "compact":
        # Literal slash command: delivered as if the user typed it, so the
        # harness's own /compact handler runs -- no model turn involved.
        return "/compact"
    path = snapshot_path(session_id)
    return (
        f"[auto-snapshot] This session has been idle ~{idle_min:.0f} minutes; "
        "its prompt cache is about to go cold, so reloading this context later "
        "would be expensive. Before anything else, write a durable snapshot so "
        "a fresh session can resume cheaply:\n"
        f"1. Write the file {path} with YAML frontmatter "
        f"(session_id: {session_id}, engine: {engine}, cwd: {cwd}, git_branch, "
        "git_commit, trigger: auto, created_at as ISO timestamp) and a body "
        "with these sections: What's done; What's in flight; Next concrete "
        "step; Key files; Gotchas & decisions.\n"
        f"2. Run: wt snapshot record --session {session_id} --cwd \"{cwd}\"\n"
        "3. Optional: if a wt queue clearly fits this work, file ONE ticket "
        "whose note points at that file.\n"
        "Take no other action; there is nothing to ask the user."
    )



# "both" mode delivers /compact, then waits for it to land before asking for
# a written snapshot too -- compaction blocks the session for tens of
# seconds (observed 0:25-1:07 on real sessions), so firing the second
# message immediately would just queue it mid-compaction. These sum to
# ~100s, comfortably past the slower observed runs.
_BOTH_MODE_WAIT_STEPS_S = (10, 15, 20, 25, 30)


def fire(session_id: str, *, sleep_fn=None) -> Dict[str, Any]:
    from . import messages
    sleep_fn = sleep_fn or time.sleep
    state = load_state(session_id)
    if not state:
        return {"ok": False, "error": f"no timer state for session {session_id}"}
    engine = str(state.get("engine") or "claude")
    cwd = str(state.get("cwd") or "")
    mode = str(state.get("mode") or "mdfile")
    mtime = transcript_mtime(session_id, engine)
    idle_min = ((time.time() - mtime) / 60.0) if mtime else 0.0
    resolved = {"session_id": session_id, "engine": engine, "cwd": cwd}

    if mode == "compact":
        return messages.deliver(resolved, build_fire_prompt(
            session_id, engine, cwd, idle_min, mode="compact"))

    if mode == "both":
        compact_result = messages.deliver(resolved, build_fire_prompt(
            session_id, engine, cwd, idle_min, mode="compact"))
        if not compact_result.get("ok"):
            return compact_result
        for wait_s in _BOTH_MODE_WAIT_STEPS_S:
            sleep_fn(wait_s)
        snapshot_result = messages.deliver(resolved, build_fire_prompt(
            session_id, engine, cwd, idle_min, mode="mdfile"))
        if not snapshot_result.get("ok"):
            return {"ok": False, "compact": compact_result,
                    "error": "compact delivered but snapshot follow-up failed: "
                             f"{snapshot_result.get('error')}"}
        return {"ok": True, "compact": compact_result, "snapshot": snapshot_result}

    prompt = build_fire_prompt(session_id, engine, cwd, idle_min, mode="mdfile")
    return messages.deliver(resolved, prompt)


def record(session_id: str, cwd: str) -> Dict[str, Any]:
    path = snapshot_path(session_id)
    if not path.exists():
        return {"ok": False, "error": f"snapshot not found at {path}"}
    link = latest_link(cwd)
    link.parent.mkdir(parents=True, exist_ok=True)
    try:
        if link.is_symlink() or link.exists():
            link.unlink()
        link.symlink_to(path)
    except OSError as e:
        return {"ok": False, "error": f"cannot update latest link: {e}"}
    return {"ok": True, "path": str(path), "latest": str(link)}


def find_latest(cwd: str) -> Optional[Path]:
    link = latest_link(cwd)
    try:
        target = link.resolve(strict=True)
    except OSError:
        return None
    return target if target.exists() else None


def consume(path: Path) -> Path:
    archive_dir = snapshots_dir() / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)
    dest = archive_dir / path.name
    by_cwd = snapshots_dir() / "by-cwd"
    if by_cwd.is_dir():
        for link in by_cwd.glob("*/latest"):
            try:
                if link.resolve() == path.resolve():
                    link.unlink()
            except OSError:
                continue
    path.replace(dest)
    return dest


def run_timer(session_id: str, *, now_fn=None, sleep_fn=None, fire_fn=None) -> str:
    """The one-shot timer loop. Runs detached via `wt snapshot timer-run`."""
    now_fn = now_fn or time.time
    sleep_fn = sleep_fn or time.sleep
    fire_fn = fire_fn or fire  # Task 3

    def finish(outcome: str, detail: str = "") -> str:
        state = load_state(session_id) or {"session_id": session_id}
        state["outcome"] = outcome
        state["detail"] = detail
        if outcome == "fired":
            state["fired_at"] = now_fn()
        save_state(session_id, state)
        return outcome

    while True:
        state = load_state(session_id)
        if not state or state.get("outcome") != "armed":
            return str((state or {}).get("outcome") or "disarmed")
        engine = str(state.get("engine") or "claude")
        threshold_s = float(state.get("idle_min") or DEFAULT_IDLE_MIN) * 60
        mtime = transcript_mtime(session_id, engine)
        if mtime is None:
            return finish("error", "transcript not found")
        action = next_action(now_fn(), mtime, float(state.get("armed_at") or 0),
                             threshold_s, CACHE_TTL_MIN * 60)
        if action[0] == "sleep":
            sleep_fn(action[1])
            continue
        if action[0] == "expire":
            return finish("expired", "24h lifetime cap reached")
        if action[0] == "skip":
            return finish("skipped-overslept",
                          "idle exceeded cache TTL (e.g. laptop sleep); "
                          "not injecting -- user decides")
        result = fire_fn(session_id)
        if isinstance(result, dict) and not result.get("ok"):
            if result.get("busy"):
                sleep_fn(60)  # user may be mid-turn; window math re-decides
                continue
            return finish("error", str(result.get("error") or "delivery failed"))
        return finish("fired")


def _first_user_text(path: Path) -> str:
    """First real user-message text in a claude transcript, whitespace-collapsed."""
    try:
        with open(path) as f:
            for line in f:
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if entry.get("type") != "user":
                    continue
                content = (entry.get("message") or {}).get("content")
                if isinstance(content, str):
                    text = content
                else:
                    text = next((b.get("text", "") for b in (content or [])
                                 if isinstance(b, dict) and b.get("type") == "text"), "")
                text = " ".join(str(text).split())
                if text:
                    return text[:100]
    except OSError:
        pass
    return ""


def list_sessions(cwd: str, limit: int = 10, exclude: str = "") -> list:
    """Newest-first claude sessions for a project dir (codex deferred: its
    rollout paths don't encode the cwd)."""
    from . import messages
    d = messages._claude_projects_root() / cwd_slug(cwd)
    rows = []
    try:
        paths = list(d.glob("*.jsonl"))
    except OSError:
        return []
    for p in paths:
        sid = p.stem
        if sid == exclude:
            continue
        try:
            st = p.stat()
        except OSError:
            continue
        rows.append({"session_id": sid, "mtime": st.st_mtime,
                     "size": st.st_size, "first_message": _first_user_text(p)})
    rows.sort(key=lambda r: r["mtime"], reverse=True)
    return rows[: max(0, int(limit))]


def _age_str(seconds: float) -> str:
    seconds = max(0.0, seconds)
    if seconds < 3600:
        return f"{int(seconds // 60)}m"
    if seconds < 86400:
        return f"{int(seconds // 3600)}h"
    return f"{int(seconds // 86400)}d"


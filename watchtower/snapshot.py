# Copyright (c) 2026 Amir Fish. All rights reserved.
# SPDX-License-Identifier: LicenseRef-WatchTower-Software-License

"""Auto-snapshot ("token-parachute"): checkpoint an idle session before the
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


def arm(session_id: str, engine: str, cwd: str,
        idle_min: float = DEFAULT_IDLE_MIN, spawn: bool = True) -> Dict[str, Any]:
    if engine not in ("claude", "codex"):
        return {"ok": False,
                "error": f"auto-fire is not supported for engine '{engine}' yet; "
                         "use /snapshot-now before stepping away"}
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


def fire(session_id: str) -> Dict[str, Any]:
    """Stub replaced in Task 3."""
    raise NotImplementedError


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


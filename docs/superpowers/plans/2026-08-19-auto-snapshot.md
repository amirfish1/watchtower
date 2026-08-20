# Auto-Snapshot ("token-parachute") Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Daemon-free auto-snapshot of idle agent sessions before the prompt-cache cliff, plus resume commands, shipped as watchtower-bundled skills and a Claude marketplace plugin.

**Architecture:** A new `watchtower/snapshot.py` module holds all logic: pure fire-window math, per-session state JSON, a one-shot detached timer process, and a fire path that reuses `messages.deliver()` (which already reaches idle Claude sessions via tty-keystroke/headless-resume and Codex via the private app-server). Four thin skills call `wt snapshot ...` subcommands. `skills_sync.py` already installs bundled skills into Claude/Codex/Antigravity/Kimi homes.

**Tech Stack:** Python 3 stdlib only (repo convention), pytest with the existing `wt_env`/`run_cli` conftest fixtures, SKILL.md prompt files, Claude plugin marketplace JSON manifests.

**Spec:** `docs/superpowers/specs/2026-08-19-auto-snapshot-design.md`

## Global Constraints

- Stdlib only; no new dependencies (matches every existing watchtower module).
- Every new `.py` file starts with the repo's two-line header: `# Copyright (c) 2026 Amir Fish. All rights reserved.` / `# SPDX-License-Identifier: LicenseRef-WatchTower-Software-License`.
- Cache TTL is fixed at 60 minutes (`CACHE_TTL_MIN = 60`); default idle threshold 55. Fire window is `[threshold, TTL)`; the upper bound is ALWAYS the TTL regardless of custom threshold. `arm` rejects `--idle >= 60`.
- One-shot: a timer fires (or skips) at most once per arm, then exits. Never re-fire on the fire's own transcript footprint.
- No persistent process: the timer is a detached one-shot with a 24h hard lifetime cap.
- All state paths honor a `WATCHTOWER_SNAPSHOTS_DIR` env override (default `~/.watchtower/snapshots`) and the override MUST be added to `tests/conftest.py` so tests stay hermetic.
- Never sleep for real in tests: `run_timer` takes injectable `now_fn`/`sleep_fn`.
- Brand placeholder is `token-parachute` everywhere it appears; Task 7 applies the final name.
- Commit style: conventional commits, `git add <explicit paths>` then `git commit --only <paths>` (multi-session shared clone — never `git add -A`).

## File Structure

- `watchtower/snapshot.py` — new; all snapshot logic (paths, window math, state, timer, fire, latest/consume).
- `watchtower/cli.py` — modify; register `wt snapshot` subcommands in `build_parser()` + handler.
- `watchtower/skills_sync.py` — modify; add 4 skill names to `SKILL_NAMES`.
- `watchtower/skills/{auto-snapshot-on,auto-snapshot-off,snapshot-now,resume-from-snapshot}/SKILL.md` — new.
- `.claude-plugin/marketplace.json`, `plugins/token-parachute/.claude-plugin/plugin.json`, `plugins/token-parachute/skills/*` (symlinks) — new.
- `tests/test_snapshot.py`, `tests/test_snapshot_cli.py`, `tests/test_plugin_manifest.py` — new; `tests/conftest.py`, `tests/test_skills_sync.py` — modify.

---

### Task 1: Snapshot core — paths, state, fire-window math

**Files:**
- Create: `watchtower/snapshot.py`
- Modify: `tests/conftest.py` (add `WATCHTOWER_SNAPSHOTS_DIR` to the `_ENV_DIRS` mapping, value `"snapshots"`)
- Test: `tests/test_snapshot.py`

**Interfaces:**
- Consumes: nothing new (stdlib + `watchtower` package layout).
- Produces (used by Tasks 2–4):
  - `CACHE_TTL_MIN = 60`, `DEFAULT_IDLE_MIN = 55`, `TIMER_CAP_S = 24 * 3600`
  - `snapshots_dir() -> Path`
  - `snapshot_path(session_id: str) -> Path` (`<dir>/<sid>.md`)
  - `timer_state_path(session_id: str) -> Path` (`<dir>/timers/<sid>.json`)
  - `cwd_slug(cwd: str) -> str` (mirror claude's own slug: `/` and `.` → `-`)
  - `latest_link(cwd: str) -> Path` (`<dir>/by-cwd/<slug>/latest`)
  - `load_state(session_id) -> dict | None`, `save_state(session_id, state: dict) -> None` (atomic tmp+replace, like `config.py` stores)
  - `next_action(now: float, mtime: float, armed_at: float, threshold_s: float, ttl_s: float, cap_s: float = TIMER_CAP_S) -> tuple` returning `("expire",)` | `("sleep", seconds: float)` | `("fire",)` | `("skip",)`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_snapshot.py
"""Fire-window math and state-file plumbing for watchtower.snapshot."""
import json

from watchtower import snapshot


def test_next_action_sleeps_remainder_when_user_was_active():
    # armed at t=0, threshold 55m; user last active 20m ago at now=100m
    now, mtime = 6000.0, 6000.0 - 20 * 60
    action = snapshot.next_action(now, mtime, armed_at=0.0,
                                  threshold_s=55 * 60, ttl_s=60 * 60)
    assert action == ("sleep", 35 * 60)


def test_next_action_fires_inside_window():
    now = 100_000.0
    for idle_min in (55, 57, 59.9):
        action = snapshot.next_action(now, now - idle_min * 60, armed_at=now - 7200,
                                      threshold_s=55 * 60, ttl_s=60 * 60)
        assert action == ("fire",), idle_min


def test_next_action_skips_when_overslept_past_ttl():
    now = 100_000.0
    action = snapshot.next_action(now, now - 61 * 60, armed_at=now - 7200,
                                  threshold_s=55 * 60, ttl_s=60 * 60)
    assert action == ("skip",)


def test_next_action_expires_at_lifetime_cap():
    now = 200_000.0
    action = snapshot.next_action(now, now - 10, armed_at=now - snapshot.TIMER_CAP_S,
                                  threshold_s=55 * 60, ttl_s=60 * 60)
    assert action == ("expire",)


def test_state_roundtrip_and_paths(wt_env):
    sid = "abc123"
    snapshot.save_state(sid, {"session_id": sid, "outcome": "armed"})
    assert snapshot.load_state(sid)["outcome"] == "armed"
    assert snapshot.load_state("missing") is None
    assert snapshot.snapshot_path(sid).name == "abc123.md"
    assert snapshot.timer_state_path(sid).parent.name == "timers"
    link = snapshot.latest_link("/Users/x/Apps/demo.app")
    assert link.name == "latest"
    assert link.parent.name == "-Users-x-Apps-demo-app"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m pytest tests/test_snapshot.py -v`
Expected: FAIL with `ModuleNotFoundError`/`AttributeError` (module doesn't exist).

- [ ] **Step 3: Implement `watchtower/snapshot.py` (core section)**

```python
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
```

- [ ] **Step 4: Add the env knob to conftest**

In `tests/conftest.py`, find the `_ENV_DIRS` dict (near `_ENV_FILES`) and add the line `"WATCHTOWER_SNAPSHOTS_DIR": "snapshots",`. (If `_ENV_DIRS` maps var→subdir-name like `_ENV_FILES` maps var→filename, follow that exact shape.)

- [ ] **Step 5: Run tests to verify they pass**

Run: `python3 -m pytest tests/test_snapshot.py -v`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add watchtower/snapshot.py tests/test_snapshot.py tests/conftest.py
git commit --only watchtower/snapshot.py tests/test_snapshot.py tests/conftest.py -m "feat(snapshot): fire-window math and state plumbing"
```

---

### Task 2: Timer loop + arm/disarm/status

**Files:**
- Modify: `watchtower/snapshot.py`
- Test: `tests/test_snapshot.py`

**Interfaces:**
- Consumes: Task 1 (`next_action`, `load_state`, `save_state`, constants).
- Produces (used by Tasks 3–4):
  - `transcript_mtime(session_id: str, engine: str) -> float | None` — claude via `messages._find_transcript(sid)`, codex via `messages._find_codex_rollout(sid)`; `None` when not found.
  - `arm(session_id: str, engine: str, cwd: str, idle_min: float = DEFAULT_IDLE_MIN, spawn: bool = True) -> dict` — validates `engine in ("claude", "codex")` and `idle_min < CACHE_TTL_MIN`, disarms any existing timer, writes state `{session_id, engine, cwd, idle_min, armed_at, pid, outcome: "armed", detail: ""}`, spawns the detached timer (unless `spawn=False`, the test seam), returns `{"ok": True, "state": ..., "ccc_handover_armed": bool}`; error dicts `{"ok": False, "error": ...}` otherwise.
  - `disarm(session_id: str) -> dict` — SIGTERM the recorded pid if alive, set `outcome: "disarmed"`.
  - `status(session_id: str | None = None) -> list[dict]` — all timer states (or one), each with a live-pid boolean.
  - `run_timer(session_id: str, *, now_fn=time.time, sleep_fn=time.sleep, fire_fn=None) -> str` — the loop; returns final outcome string.
  - `ccc_handover_flag_set(session_id: str) -> bool` — True when `~/.claude/command-center/auto-handover.json` has an enabled entry for this session.

- [ ] **Step 1: Write the failing tests** (append to `tests/test_snapshot.py`)

```python
def test_arm_rejects_threshold_at_or_past_ttl(wt_env):
    r = snapshot.arm("s1", "claude", "/tmp/x", idle_min=60, spawn=False)
    assert not r["ok"] and "60" in r["error"]


def test_arm_writes_state_and_disarm_marks_it(wt_env):
    r = snapshot.arm("s1", "claude", "/tmp/x", idle_min=10, spawn=False)
    assert r["ok"] and snapshot.load_state("s1")["outcome"] == "armed"
    assert snapshot.disarm("s1")["ok"]
    assert snapshot.load_state("s1")["outcome"] == "disarmed"
    assert snapshot.status("s1")[0]["outcome"] == "disarmed"


def test_run_timer_sleeps_then_fires_once(wt_env, monkeypatch):
    fired = []
    clock = {"t": 1000.0}
    mtimes = iter([1000.0, 1000.0])  # active at first wake, then idle long enough
    monkeypatch.setattr(snapshot, "transcript_mtime", lambda sid, eng: next(mtimes, 1000.0))
    snapshot.arm("s1", "claude", "/tmp/x", idle_min=55, spawn=False)

    def fake_sleep(s):
        clock["t"] += s

    outcome = snapshot.run_timer(
        "s1", now_fn=lambda: clock["t"], sleep_fn=fake_sleep,
        fire_fn=lambda sid: fired.append(sid) or {"ok": True},
    )
    assert outcome == "fired" and fired == ["s1"]
    assert snapshot.load_state("s1")["outcome"] == "fired"


def test_run_timer_skips_when_overslept(wt_env, monkeypatch):
    clock = {"t": 100_000.0}
    # transcript last touched 61 minutes before the (single) wake
    monkeypatch.setattr(snapshot, "transcript_mtime",
                        lambda sid, eng: clock["t"] - 61 * 60)
    snapshot.arm("s1", "claude", "/tmp/x", idle_min=55, spawn=False)
    outcome = snapshot.run_timer("s1", now_fn=lambda: clock["t"],
                                 sleep_fn=lambda s: None,
                                 fire_fn=lambda sid: {"ok": True})
    assert outcome == "skipped-overslept"
    assert snapshot.load_state("s1")["outcome"] == "skipped-overslept"


def test_run_timer_exits_when_disarmed_meanwhile(wt_env, monkeypatch):
    clock = {"t": 1000.0}
    monkeypatch.setattr(snapshot, "transcript_mtime", lambda sid, eng: clock["t"])
    snapshot.arm("s1", "claude", "/tmp/x", idle_min=55, spawn=False)

    def sleep_and_disarm(s):
        clock["t"] += s
        snapshot.disarm("s1")

    outcome = snapshot.run_timer("s1", now_fn=lambda: clock["t"],
                                 sleep_fn=sleep_and_disarm,
                                 fire_fn=lambda sid: {"ok": True})
    assert outcome == "disarmed"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m pytest tests/test_snapshot.py -v -k "arm or timer"`
Expected: FAIL with AttributeError (functions not defined).

- [ ] **Step 3: Implement** (append to `watchtower/snapshot.py`; add `import signal, subprocess, sys, time` at top)

```python
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
```

Note: `fire` does not exist until Task 3 — add a module-level stub so imports work: `def fire(session_id): raise NotImplementedError` (Task 3 replaces it).

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 -m pytest tests/test_snapshot.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add watchtower/snapshot.py tests/test_snapshot.py
git commit --only watchtower/snapshot.py tests/test_snapshot.py -m "feat(snapshot): one-shot timer loop with arm/disarm/status"
```

---

### Task 3: Fire path, latest/consume, snapshot prompt

**Files:**
- Modify: `watchtower/snapshot.py`
- Test: `tests/test_snapshot.py`

**Interfaces:**
- Consumes: Task 2 state helpers; `watchtower.messages.deliver(resolved: dict, text: str) -> dict` (existing; takes `{"session_id", "engine", "cwd"}` and tries fifo → tty (claude) → headless resume (claude) → codex app-server/delegate).
- Produces (used by Task 4 CLI and the skills):
  - `build_fire_prompt(session_id, engine, cwd, idle_min) -> str`
  - `fire(session_id: str) -> dict` (replaces Task 2 stub)
  - `record(session_id: str, cwd: str) -> dict` — validate snapshot file exists, refresh `latest_link(cwd)` symlink to it.
  - `find_latest(cwd: str) -> Path | None`
  - `consume(path: Path) -> Path` — move to `<dir>/archive/<name>`, drop any `latest` symlink pointing at it.

- [ ] **Step 1: Write the failing tests** (append to `tests/test_snapshot.py`)

```python
def test_fire_delivers_prompt_with_paths(wt_env, monkeypatch):
    sent = {}
    snapshot.arm("s1", "claude", "/tmp/proj", idle_min=55, spawn=False)
    monkeypatch.setattr(snapshot, "transcript_mtime",
                        lambda sid, eng: 0.0)  # idle forever -> prompt says ~cold

    def fake_deliver(resolved, text):
        sent.update(resolved=resolved, text=text)
        return {"ok": True, "transport": "tty"}

    monkeypatch.setattr("watchtower.messages.deliver", fake_deliver)
    r = snapshot.fire("s1")
    assert r["ok"]
    assert sent["resolved"]["session_id"] == "s1"
    assert sent["resolved"]["engine"] == "claude"
    assert str(snapshot.snapshot_path("s1")) in sent["text"]
    assert "wt snapshot record" in sent["text"]


def test_record_and_find_latest_and_consume(wt_env):
    p = snapshot.snapshot_path("s1")
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text("---\nsession_id: s1\n---\nstate\n")
    assert snapshot.record("s1", "/tmp/proj")["ok"]
    assert snapshot.find_latest("/tmp/proj") == p
    archived = snapshot.consume(p)
    assert archived.exists() and archived.parent.name == "archive"
    assert snapshot.find_latest("/tmp/proj") is None


def test_record_fails_when_snapshot_missing(wt_env):
    r = snapshot.record("ghost", "/tmp/proj")
    assert not r["ok"] and "not found" in r["error"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m pytest tests/test_snapshot.py -v -k "fire or record or latest"`
Expected: FAIL (NotImplementedError / AttributeError).

- [ ] **Step 3: Implement** (replace the Task 2 `fire` stub; append the rest)

```python
def build_fire_prompt(session_id: str, engine: str, cwd: str, idle_min: float) -> str:
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


def fire(session_id: str) -> Dict[str, Any]:
    from . import messages
    state = load_state(session_id)
    if not state:
        return {"ok": False, "error": f"no timer state for session {session_id}"}
    engine = str(state.get("engine") or "claude")
    cwd = str(state.get("cwd") or "")
    mtime = transcript_mtime(session_id, engine)
    idle_min = ((time.time() - mtime) / 60.0) if mtime else 0.0
    prompt = build_fire_prompt(session_id, engine, cwd, idle_min)
    resolved = {"session_id": session_id, "engine": engine, "cwd": cwd}
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
```

- [ ] **Step 4: Run the full snapshot suite**

Run: `python3 -m pytest tests/test_snapshot.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add watchtower/snapshot.py tests/test_snapshot.py
git commit --only watchtower/snapshot.py tests/test_snapshot.py -m "feat(snapshot): fire via messages.deliver, latest/consume bookkeeping"
```

---

### Task 4: CLI wiring — `wt snapshot ...`

**Files:**
- Modify: `watchtower/cli.py` (subparser registration in `build_parser()`, handler dispatch in `main()`; follow the shape of an existing multi-verb subcommand like `agents`, cli.py:3217)
- Test: `tests/test_snapshot_cli.py`

**Interfaces:**
- Consumes: every Task 1–3 public function.
- Produces: subcommands used verbatim by the skills:
  - `wt snapshot arm --session <sid> --engine claude|codex --cwd <dir> [--idle <min>]`
  - `wt snapshot disarm --session <sid>`
  - `wt snapshot status [--session <sid>]` (prints one line per timer: sid8, engine, outcome, idle_min, timer_alive)
  - `wt snapshot fire --session <sid>` (manual/testing)
  - `wt snapshot timer-run <session_id>` (hidden: `help=argparse.SUPPRESS`; runs `snapshot.run_timer`)
  - `wt snapshot path --session <sid>` (prints `snapshot_path`)
  - `wt snapshot record --session <sid> --cwd <dir>`
  - `wt snapshot latest --cwd <dir>` (prints path, exit 1 + message when none)
  - `wt snapshot consume --path <file>`
- Exit codes: 0 on `{"ok": True}`, 1 with the error on stderr otherwise. `arm` prints a warning line when `ccc_handover_armed` is true: `warning: CCC auto-handover is also armed for this session; expect a double snapshot`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_snapshot_cli.py
"""wt snapshot CLI surface, via the in-process run_cli fixture."""
from watchtower import snapshot


def test_arm_status_disarm_roundtrip(run_cli, monkeypatch):
    monkeypatch.setattr(snapshot, "_spawn_timer", lambda sid: 4242)
    r = run_cli("snapshot", "arm", "--session", "s1", "--engine", "claude",
                "--cwd", "/tmp/proj", "--idle", "55")
    assert r.code == 0, r.stderr
    r = run_cli("snapshot", "status", "--session", "s1")
    assert r.code == 0 and "armed" in r.stdout
    assert run_cli("snapshot", "disarm", "--session", "s1").code == 0
    assert "disarmed" in run_cli("snapshot", "status").stdout


def test_arm_rejects_bad_engine_and_threshold(run_cli):
    r = run_cli("snapshot", "arm", "--session", "s1", "--engine", "kimi",
                "--cwd", "/tmp/x")
    assert r.code == 1 and "snapshot-now" in r.stderr
    r = run_cli("snapshot", "arm", "--session", "s1", "--engine", "claude",
                "--cwd", "/tmp/x", "--idle", "75")
    assert r.code == 1 and "TTL" in r.stderr


def test_path_record_latest_consume_flow(run_cli):
    p_out = run_cli("snapshot", "path", "--session", "s1")
    assert p_out.code == 0
    path = p_out.stdout.strip()
    import pathlib
    pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
    pathlib.Path(path).write_text("---\nsession_id: s1\n---\nbody\n")
    assert run_cli("snapshot", "record", "--session", "s1",
                   "--cwd", "/tmp/proj").code == 0
    latest = run_cli("snapshot", "latest", "--cwd", "/tmp/proj")
    assert latest.code == 0 and latest.stdout.strip() == path
    assert run_cli("snapshot", "consume", "--path", path).code == 0
    assert run_cli("snapshot", "latest", "--cwd", "/tmp/proj").code == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m pytest tests/test_snapshot_cli.py -v`
Expected: FAIL (argparse: invalid choice 'snapshot').

- [ ] **Step 3: Implement the subparser + handler in `cli.py`**

In `build_parser()` add (mirroring the `agents` sub-subparser pattern):

```python
s = sub.add_parser("snapshot")
ssub = s.add_subparsers(dest="snapshot_command", metavar="<verb>")
sa = ssub.add_parser("arm")
sa.add_argument("--session", required=True)
sa.add_argument("--engine", required=True)
sa.add_argument("--cwd", required=True)
sa.add_argument("--idle", type=float, default=None)
sd = ssub.add_parser("disarm"); sd.add_argument("--session", required=True)
st = ssub.add_parser("status"); st.add_argument("--session", default=None)
sf = ssub.add_parser("fire"); sf.add_argument("--session", required=True)
tr = ssub.add_parser("timer-run", help=argparse.SUPPRESS)
tr.add_argument("session_id")
sp = ssub.add_parser("path"); sp.add_argument("--session", required=True)
sr = ssub.add_parser("record")
sr.add_argument("--session", required=True); sr.add_argument("--cwd", required=True)
sl = ssub.add_parser("latest"); sl.add_argument("--cwd", required=True)
sc = ssub.add_parser("consume"); sc.add_argument("--path", required=True)
```

In the command dispatch, add a `snapshot` branch (lazy `from . import snapshot as snapshot_mod` inside the branch, matching how other branches import):

```python
if args.command == "snapshot":
    from . import snapshot as snap
    cmd = args.snapshot_command
    if cmd == "arm":
        r = snap.arm(args.session, args.engine, args.cwd,
                     idle_min=args.idle if args.idle is not None else snap.DEFAULT_IDLE_MIN)
        if r.get("ok"):
            if r.get("ccc_handover_armed"):
                print("warning: CCC auto-handover is also armed for this "
                      "session; expect a double snapshot", file=sys.stderr)
            st = r["state"]
            print(f"armed: snapshots after {st['idle_min']:g} idle minutes; "
                  f"window closes at {snap.CACHE_TTL_MIN} (timer pid {st['pid']})")
            return 0
        print(r.get("error"), file=sys.stderr); return 1
    if cmd == "disarm":
        r = snap.disarm(args.session)
        print("disarmed" if r.get("ok") else r.get("error"),
              file=sys.stdout if r.get("ok") else sys.stderr)
        return 0 if r.get("ok") else 1
    if cmd == "status":
        rows = snap.status(args.session)
        for s_ in rows:
            print(f"{s_['session_id'][:8]}  {s_.get('engine','?'):7} "
                  f"{s_.get('outcome','?'):18} idle_min={s_.get('idle_min','?')} "
                  f"alive={s_.get('timer_alive')}")
        if not rows:
            print("no snapshot timers")
        return 0
    if cmd == "fire":
        r = snap.fire(args.session)
        print(r if r.get("ok") else r.get("error"),
              file=sys.stdout if r.get("ok") else sys.stderr)
        return 0 if r.get("ok") else 1
    if cmd == "timer-run":
        outcome = snap.run_timer(args.session_id)
        print(f"timer outcome: {outcome}")
        return 0
    if cmd == "path":
        print(snap.snapshot_path(args.session)); return 0
    if cmd == "record":
        r = snap.record(args.session, args.cwd)
        print(r.get("path") if r.get("ok") else r.get("error"),
              file=sys.stdout if r.get("ok") else sys.stderr)
        return 0 if r.get("ok") else 1
    if cmd == "latest":
        p = snap.find_latest(args.cwd)
        if p is None:
            print("no snapshot for this directory", file=sys.stderr); return 1
        print(p); return 0
    if cmd == "consume":
        from pathlib import Path as _P
        print(snap.consume(_P(args.path))); return 0
```

(Adapt return-vs-sys.exit and printing to whatever the surrounding `main()` actually does — read two neighboring branches first and copy their idiom exactly.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 -m pytest tests/test_snapshot_cli.py tests/test_snapshot.py -v`
Expected: all PASS.

- [ ] **Step 5: Sanity-check the detached spawn entrypoint**

Run: `python3 -c "import watchtower.cli"` and confirm `watchtower/cli.py` has an `if __name__ == "__main__": main()` guard (it does, cli.py:3847) so `python -m watchtower.cli snapshot timer-run <sid>` works.

- [ ] **Step 6: Commit**

```bash
git add watchtower/cli.py tests/test_snapshot_cli.py
git commit --only watchtower/cli.py tests/test_snapshot_cli.py -m "feat(cli): wt snapshot subcommand family"
```

---

### Task 5: The four skills + cross-engine sync

**Files:**
- Create: `watchtower/skills/auto-snapshot-on/SKILL.md`, `watchtower/skills/auto-snapshot-off/SKILL.md`, `watchtower/skills/snapshot-now/SKILL.md`, `watchtower/skills/resume-from-snapshot/SKILL.md`
- Modify: `watchtower/skills_sync.py` (`SKILL_NAMES` tuple)
- Test: `tests/test_skills_sync.py` (extend), plus a new frontmatter check in `tests/test_snapshot_cli.py` or a small `tests/test_snapshot_skills.py`

**Interfaces:**
- Consumes: the exact `wt snapshot` verbs from Task 4.
- Produces: skill names `auto-snapshot-on`, `auto-snapshot-off`, `snapshot-now`, `resume-from-snapshot` (slash-command names come from these directory names).

- [ ] **Step 1: Write the failing test** (new file `tests/test_snapshot_skills.py`)

```python
"""The snapshot skills exist, are registered for sync, and reference real CLI verbs."""
from pathlib import Path

from watchtower import skills_sync

SNAPSHOT_SKILLS = ("auto-snapshot-on", "auto-snapshot-off",
                   "snapshot-now", "resume-from-snapshot")


def test_snapshot_skills_registered_and_well_formed():
    for name in SNAPSHOT_SKILLS:
        assert name in skills_sync.SKILL_NAMES
        text = (skills_sync.source_dir(name) / "SKILL.md").read_text()
        assert text.startswith("---\n") and f"name: {name}" in text
        assert "description:" in text


def test_skills_reference_real_cli_verbs():
    on = (skills_sync.source_dir("auto-snapshot-on") / "SKILL.md").read_text()
    assert "wt snapshot arm" in on and "--engine" in on
    off = (skills_sync.source_dir("auto-snapshot-off") / "SKILL.md").read_text()
    assert "wt snapshot disarm" in off
    now = (skills_sync.source_dir("snapshot-now") / "SKILL.md").read_text()
    assert "wt snapshot path" in now and "wt snapshot record" in now
    res = (skills_sync.source_dir("resume-from-snapshot") / "SKILL.md").read_text()
    assert "wt snapshot latest" in res and "wt snapshot consume" in res
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_snapshot_skills.py -v`
Expected: FAIL (names not in SKILL_NAMES; files missing).

- [ ] **Step 3: Add the names to `skills_sync.SKILL_NAMES`**

```python
SKILL_NAMES: Tuple[str, ...] = (
    "watchtower", "group-chat-checkin", "critique", "wt-triage-queue",
    "compact-to-queue", "add-annotate-widget",
    "auto-snapshot-on", "auto-snapshot-off", "snapshot-now",
    "resume-from-snapshot",
)
```

- [ ] **Step 4: Write the four SKILL.md files** (full content below — write verbatim, adjusting nothing but the final brand name in Task 7)

`watchtower/skills/auto-snapshot-on/SKILL.md`:

````markdown
---
name: auto-snapshot-on
description: Arm auto-snapshot for this session — if you go idle near the prompt-cache cliff (default 55 min), the session writes a durable state snapshot so a fresh session can resume without re-paying the full context cost. Use when the user says "/auto-snapshot-on", "auto snapshot on", or "snapshot me if I step away".
---

# Auto-snapshot: arm

Arm a one-shot idle timer for THIS session. Accepts an optional argument:
idle minutes (default 55; must be below 60, the cache TTL).

1. Determine your session id and engine:
   - Claude Code: session id = `$CLAUDE_SESSION_ID` if set; otherwise the
     basename (without `.jsonl`) of the newest `*.jsonl` under
     `~/.claude/projects/<slugified-cwd>/` (slug: `/` and `.` become `-`).
     Engine is `claude`.
   - Codex: your thread id; engine is `codex`.
   - Any other engine (kimi, grok, ...): auto-fire is not supported — tell
     the user to run /snapshot-now before stepping away, and stop.
2. Run (network sandbox not required, but run outside any restricted shell):
   `wt snapshot arm --session <SID> --engine <ENGINE> --cwd "$PWD" --idle <MIN>`
3. Relay the confirmation (including the fire window) and any warning about
   CCC auto-handover being armed too. If the command errors, show the error
   verbatim — do not retry with different numbers unless the user asks.

The timer is one-shot: after it fires (or skips because you were idle past
the 60-min TTL), it will not fire again until re-armed.
````

`watchtower/skills/auto-snapshot-off/SKILL.md`:

````markdown
---
name: auto-snapshot-off
description: Disarm auto-snapshot for this session (the user is back and no checkpoint is needed). Use when the user says "/auto-snapshot-off" or "auto snapshot off".
---

# Auto-snapshot: disarm

1. Determine your session id (same procedure as auto-snapshot-on step 1).
2. Run: `wt snapshot disarm --session <SID>`
3. Confirm to the user, or show the error verbatim ("no timer state" just
   means nothing was armed — say so plainly).
````

`watchtower/skills/snapshot-now/SKILL.md`:

````markdown
---
name: snapshot-now
description: Write a durable state snapshot of this session immediately (no timer) so a fresh session can resume it cheaply after /clear. Works on every engine. Use when the user says "/snapshot-now" or is about to step away on an engine without auto-fire.
---

# Snapshot now

1. Determine your session id and engine (same as auto-snapshot-on step 1;
   any engine is fine here).
2. Get the canonical path: `wt snapshot path --session <SID>`
3. Write that file with YAML frontmatter — `session_id`, `engine`, `cwd`
   (absolute), `git_branch`, `git_commit`, `trigger: manual`, `created_at`
   (ISO) — and a body with sections: What's done; What's in flight; Next
   concrete step; Key files; Gotchas & decisions. Write for a reader with
   ZERO context: no session-local shorthand.
4. Run: `wt snapshot record --session <SID> --cwd "$PWD"`
5. Tell the user the snapshot is saved and that after /clear they can run
   /resume-from-snapshot in this directory.
````

`watchtower/skills/resume-from-snapshot/SKILL.md`:

````markdown
---
name: resume-from-snapshot
description: Resume work from the most recent auto/manual snapshot for this project directory — run after /clear (or in a brand-new session) to continue without reloading the old session's full context. Use when the user says "/resume-from-snapshot" or "restore from snapshot".
---

# Resume from snapshot

Accepts an optional argument: an explicit snapshot file path or session id.

1. Locate the snapshot:
   - No argument: `wt snapshot latest --cwd "$PWD"` (if it exits 1, tell the
     user no snapshot exists for this directory and stop).
   - Session-id argument: `wt snapshot path --session <ARG>`.
   - Path argument: use it directly.
2. Read the file. Compare its `git_branch`/`git_commit` frontmatter to the
   current repo state; if they differ, warn the user the tree has moved
   since the snapshot and summarize the drift (branch name, commits between).
3. State in one short paragraph: what was done, what was in flight, and the
   next concrete step. Then continue that work (or await the user's go-ahead
   if the next step is destructive/outward-facing).
4. Archive it so `latest` stops pointing at consumed state:
   `wt snapshot consume --path <FILE>`
````

- [ ] **Step 5: Run tests to verify they pass**

Run: `python3 -m pytest tests/test_snapshot_skills.py tests/test_skills_sync.py -v`
Expected: all PASS (if `test_skills_sync.py` asserts an exact SKILL_NAMES list, update that assertion too).

- [ ] **Step 6: Commit**

```bash
git add watchtower/skills/auto-snapshot-on watchtower/skills/auto-snapshot-off watchtower/skills/snapshot-now watchtower/skills/resume-from-snapshot watchtower/skills_sync.py tests/test_snapshot_skills.py tests/test_skills_sync.py
git commit --only watchtower/skills/auto-snapshot-on watchtower/skills/auto-snapshot-off watchtower/skills/snapshot-now watchtower/skills/resume-from-snapshot watchtower/skills_sync.py tests/test_snapshot_skills.py tests/test_skills_sync.py -m "feat(skills): auto-snapshot-on/off, snapshot-now, resume-from-snapshot"
```

---

### Task 6: Marketplace plugin packaging

**Files:**
- Create: `.claude-plugin/marketplace.json`
- Create: `plugins/token-parachute/.claude-plugin/plugin.json`
- Create: `plugins/token-parachute/skills/{auto-snapshot-on,auto-snapshot-off,snapshot-now,resume-from-snapshot}` — each a **relative symlink** to `../../../watchtower/skills/<name>` (single source of truth; git stores symlinks fine)
- Create: `plugins/token-parachute/README.md`
- Test: `tests/test_plugin_manifest.py`

**Interfaces:**
- Consumes: skill directories from Task 5.
- Produces: installable marketplace entry — users run `/plugin marketplace add amirfish/watchtower` then `/plugin install token-parachute@watchtower`.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_plugin_manifest.py
"""Marketplace manifest and plugin dir are valid and self-consistent."""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_marketplace_manifest_points_at_real_plugin():
    mp = json.loads((ROOT / ".claude-plugin" / "marketplace.json").read_text())
    assert mp["name"] == "watchtower"
    entry = next(p for p in mp["plugins"] if p["name"] == "token-parachute")
    plugin_dir = ROOT / entry["source"]
    pj = json.loads((plugin_dir / ".claude-plugin" / "plugin.json").read_text())
    assert pj["name"] == "token-parachute"


def test_plugin_skill_symlinks_resolve():
    skills = ROOT / "plugins" / "token-parachute" / "skills"
    names = {"auto-snapshot-on", "auto-snapshot-off",
             "snapshot-now", "resume-from-snapshot"}
    assert {p.name for p in skills.iterdir()} == names
    for p in skills.iterdir():
        assert p.is_symlink()
        assert (p / "SKILL.md").resolve().exists()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_plugin_manifest.py -v`
Expected: FAIL (files missing).

- [ ] **Step 3: Create the manifests and symlinks**

`.claude-plugin/marketplace.json`:

```json
{
  "name": "watchtower",
  "owner": { "name": "Amir Fish" },
  "plugins": [
    {
      "name": "token-parachute",
      "source": "./plugins/token-parachute",
      "description": "Pulls the ripcord before your idle session goes cold — auto-snapshots long sessions near the prompt-cache cliff so you can /clear and resume a 300K-token session for pennies."
    }
  ]
}
```

`plugins/token-parachute/.claude-plugin/plugin.json`:

```json
{
  "name": "token-parachute",
  "description": "Auto-snapshot idle sessions before the prompt-cache cliff; resume after /clear without re-paying the context cost.",
  "version": "0.1.0",
  "author": { "name": "Amir Fish" }
}
```

Symlinks:

```bash
mkdir -p plugins/token-parachute/skills
for s in auto-snapshot-on auto-snapshot-off snapshot-now resume-from-snapshot; do
  ln -s ../../../watchtower/skills/$s plugins/token-parachute/skills/$s
done
```

`plugins/token-parachute/README.md` — short: what it does (3 sentences), the four commands, requirement (`wt` CLI: `pipx install git+https://github.com/amirfish/watchtower` until PyPI publishing resumes — verify the actual GitHub URL from `git remote -v` before writing it), and the engine-support tiers (Claude full, Codex auto-fire headless, Kimi/Grok manual `/snapshot-now`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 -m pytest tests/test_plugin_manifest.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add .claude-plugin plugins/token-parachute tests/test_plugin_manifest.py
git commit --only .claude-plugin plugins/token-parachute tests/test_plugin_manifest.py -m "feat(plugin): token-parachute marketplace packaging"
```

---

### Task 7: Final name, docs, and end-to-end smoke

**Files:**
- Modify: whatever the final name touches — `plugins/<name>/` dir, both manifests, `README.md`, spec header, this plan's references (`token-parachute` placeholder throughout)
- Modify: `README.md` (repo root — add a short Auto-snapshot section next to the existing feature list)
- Test: full suite

**Interfaces:** none new.

- [ ] **Step 1: Resolve the final name.** Check with Amir (a Sonnet CCC naming session, id `e2ae615c-4fa2-4786-8132-ee3dda6976b0`, was deciding between token-parachute / cache-parachute / session-saver and fresh alternatives). If undecided, keep `token-parachute` and say so in the report. If a new name won: `git mv plugins/token-parachute plugins/<name>`, update both JSON manifests, the plugin README, `tests/test_plugin_manifest.py` strings, and the spec's title line.

- [ ] **Step 2: Add the root-README section** — 6–10 lines: the problem (idle session, 60-min cache cliff, 300K re-read), the four commands, one-liner on the daemon-free timer, marketplace install line, tier table (Claude/Codex auto; Kimi/Grok manual).

- [ ] **Step 3: Run the whole test suite**

Run: `python3 -m pytest tests/ -x -q`
Expected: PASS (no regressions in messages/skills_sync/cli suites).

- [ ] **Step 4: Live smoke (Claude, short threshold).** In a scratch Claude session in any repo: run `/auto-snapshot-on 1` (1-minute threshold), leave the session idle ≥1 min with the TUI open, and verify: (a) `wt snapshot status` shows `fired`, (b) the prompt arrived in the TUI and the session wrote the snapshot file, (c) the `by-cwd` latest link resolves, (d) `/resume-from-snapshot` in a fresh session loads and summarizes it. Then repeat arm→`wt snapshot disarm` and confirm the timer pid dies. Record actual outputs in the task report — do not claim success without them.

- [ ] **Step 5: Live smoke (Codex).** Arm a codex session (`--engine codex`) with `--idle 1`, verify delivery lands via the app-server transport (`wt snapshot status` shows `fired`; the rollout gains the snapshot turn). If no codex session is convenient, run `wt snapshot fire --session <thread-id>` against an idle test thread and verify the same. Report honestly if blocked.

- [ ] **Step 6: Commit**

```bash
git add README.md
git commit --only README.md -m "docs: auto-snapshot feature section"
```

---

## Addendum (2026-08-20, approved): /resume-from-session

Amir approved a fifth command after v1 shipped: resume WITHOUT a snapshot,
by picking a recent session and briefing the fresh session from its
transcript — like CCC's F2 "continue new". Total Recall is a
recommendation when installed; when not installed it is NOT mentioned at
all. Final plugin name is now `token-sitter` (commit f49c7ef) — use that
name, not the old placeholder, in any new file.

### Task 8: `wt snapshot sessions` listing verb

**Files:**
- Modify: `watchtower/snapshot.py`
- Modify: `watchtower/cli.py` (extend the `snapshot` subparser + dispatch from Task 4)
- Test: `tests/test_snapshot.py`, `tests/test_snapshot_cli.py`

**Interfaces:**
- Consumes: `cwd_slug` (Task 1); `messages._claude_projects_root()` (existing, env-overridable via `WATCHTOWER_CLAUDE_PROJECTS_DIR`).
- Produces:
  - `list_sessions(cwd: str, limit: int = 10, exclude: str = "") -> list[dict]` — newest-first `{"session_id", "mtime", "size", "first_message"}` for the cwd's Claude transcripts (claude-only in v1; codex listing deferred — rollout paths don't encode cwd).
  - CLI: `wt snapshot sessions --cwd <dir> [-n 10] [--exclude <sid>]` printing `N) <full-session-id>  <age>  <first-message snippet>` rows; prints `no sessions found for this directory` and exits 0 when empty.

- [ ] **Step 1: Write the failing tests** (append to `tests/test_snapshot.py`)

```python
def _write_transcript(root, slug, sid, mtime, first_text="hello world"):
    import json as _json, os as _os
    d = root / slug
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{sid}.jsonl"
    lines = [
        _json.dumps({"type": "summary", "summary": "x"}),
        _json.dumps({"type": "user",
                     "message": {"role": "user", "content": [
                         {"type": "text", "text": first_text}]}}),
    ]
    p.write_text("\n".join(lines) + "\n")
    _os.utime(p, (mtime, mtime))
    return p


def test_list_sessions_orders_excludes_and_snippets(wt_env, tmp_path, monkeypatch):
    root = tmp_path / "claude-projects"
    monkeypatch.setenv("WATCHTOWER_CLAUDE_PROJECTS_DIR", str(root))
    slug = snapshot.cwd_slug("/tmp/proj")
    _write_transcript(root, slug, "old1", 1000.0, "first task ever")
    _write_transcript(root, slug, "new1", 3000.0, "  newest   task  ")
    _write_transcript(root, slug, "self1", 4000.0, "my own fresh session")
    rows = snapshot.list_sessions("/tmp/proj", limit=10, exclude="self1")
    assert [r["session_id"] for r in rows] == ["new1", "old1"]
    assert rows[0]["first_message"] == "newest task"


def test_list_sessions_respects_limit_and_missing_dir(wt_env, tmp_path, monkeypatch):
    root = tmp_path / "claude-projects"
    monkeypatch.setenv("WATCHTOWER_CLAUDE_PROJECTS_DIR", str(root))
    assert snapshot.list_sessions("/tmp/nowhere") == []
    slug = snapshot.cwd_slug("/tmp/proj")
    for i in range(4):
        _write_transcript(root, slug, f"s{i}", 1000.0 + i)
    assert len(snapshot.list_sessions("/tmp/proj", limit=2)) == 2
```

And append to `tests/test_snapshot_cli.py`:

```python
def test_sessions_verb_lists_and_handles_empty(run_cli, tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_CLAUDE_PROJECTS_DIR", str(tmp_path))
    r = run_cli("snapshot", "sessions", "--cwd", "/tmp/proj")
    assert r.code == 0 and "no sessions found" in r.stdout
    from tests.test_snapshot import _write_transcript
    from watchtower import snapshot as snap
    _write_transcript(tmp_path, snap.cwd_slug("/tmp/proj"), "abc12345-full-id",
                      5000.0, "build the widget")
    r = run_cli("snapshot", "sessions", "--cwd", "/tmp/proj", "-n", "5")
    assert r.code == 0
    assert "abc12345-full-id" in r.stdout and "build the widget" in r.stdout
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m pytest tests/test_snapshot.py tests/test_snapshot_cli.py -v -k sessions`
Expected: FAIL (AttributeError / argparse invalid choice).

- [ ] **Step 3: Implement** (append to `watchtower/snapshot.py`)

```python
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
```

CLI (extend the Task 4 subparser + dispatch, same idiom):

```python
se = ssub.add_parser("sessions")
se.add_argument("--cwd", required=True)
se.add_argument("-n", type=int, default=10, dest="limit")
se.add_argument("--exclude", default="")
```

```python
    if cmd == "sessions":
        rows = snap.list_sessions(args.cwd, limit=args.limit, exclude=args.exclude)
        if not rows:
            print("no sessions found for this directory")
            return 0
        now = time.time()
        for i, r_ in enumerate(rows, 1):
            print(f"{i}) {r_['session_id']}  {snap._age_str(now - r_['mtime'])}  "
                  f"{r_['first_message']}")
        return 0
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python3 -m pytest tests/test_snapshot.py tests/test_snapshot_cli.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add watchtower/snapshot.py watchtower/cli.py tests/test_snapshot.py tests/test_snapshot_cli.py
git commit --only watchtower/snapshot.py watchtower/cli.py tests/test_snapshot.py tests/test_snapshot_cli.py -m "feat(snapshot): wt snapshot sessions listing verb"
```

### Task 9: `resume-from-session` skill + registration

**Files:**
- Create: `watchtower/skills/resume-from-session/SKILL.md`
- Modify: `watchtower/skills_sync.py` (`SKILL_NAMES` += `"resume-from-session"`)
- Create symlink: `plugins/token-sitter/skills/resume-from-session -> ../../../watchtower/skills/resume-from-session`
- Modify: `tests/test_snapshot_skills.py` (add to `SNAPSHOT_SKILLS` + a verb-reference assertion), `tests/test_plugin_manifest.py` (symlink-set assertion now expects 5 names), `tests/test_skills_sync.py` (if it asserts an exact list), root `README.md` (add the command line to the Auto-snapshot section)

**Interfaces:**
- Consumes: `wt snapshot sessions` (Task 8).
- Produces: skill name `resume-from-session` (slash command `/resume-from-session`).

- [ ] **Step 1: Extend the failing tests** — in `tests/test_snapshot_skills.py` add `"resume-from-session"` to `SNAPSHOT_SKILLS` and this to `test_skills_reference_real_cli_verbs`:

```python
    rfs = (skills_sync.source_dir("resume-from-session") / "SKILL.md").read_text()
    assert "wt snapshot sessions" in rfs and "--exclude" in rfs
```

In `tests/test_plugin_manifest.py`, add `"resume-from-session"` to the expected symlink-name set.

- [ ] **Step 2: Run tests to verify they fail**

Run: `python3 -m pytest tests/test_snapshot_skills.py tests/test_plugin_manifest.py -v`
Expected: FAIL.

- [ ] **Step 3: Write the skill** — `watchtower/skills/resume-from-session/SKILL.md`, verbatim:

````markdown
---
name: resume-from-session
description: Resume work from a recent prior session WITHOUT a snapshot — lists the last 10 sessions for this project, lets the user pick one, and briefs this fresh session from that transcript. Use when the user says "/resume-from-session", "continue from an old session", or wants to pick up prior work but no snapshot exists.
---

# Resume from a prior session (no snapshot needed)

1. Determine your own session id (same procedure as auto-snapshot-on step 1)
   so you can exclude yourself from the list.
2. Run: `wt snapshot sessions --cwd "$PWD" -n 10 --exclude <YOUR-SID>`
   If it prints "no sessions found", tell the user and stop.
3. Show the numbered list (id shortened to 8 chars, age, first-message
   snippet) and ask the user to pick one. In Claude Code, use the
   AskUserQuestion tool with the top choices; elsewhere ask for a number.
4. The chosen session's transcript is
   `~/.claude/projects/<slugified-cwd>/<SID>.jsonl` (slug: `/` and `.`
   become `-`). Read its TAIL (roughly the last 150-300 lines) and, if
   needed for orientation, the first few user messages. Do NOT ingest the
   whole file — these transcripts can be enormous; the point of this
   command is briefing, not full replay.
5. Brief the user in one short paragraph: what that session was doing, what
   it finished, and what appears to have been left open. Mention the
   transcript path so deeper digs are one command away.
6. Only if a Total Recall install is detected (the `/recall` skill is
   available, or `command -v total-recall` succeeds): recommend running
   `/recall <topic of that session>` to pull richer cross-session context,
   and offer to do it. If Total Recall is not installed, do not mention it
   at all.
7. Continue the open work (or await the user's go-ahead if the next step is
   destructive/outward-facing).
````

- [ ] **Step 4: Register + symlink + README**

Add `"resume-from-session"` to `SKILL_NAMES` in `watchtower/skills_sync.py`. Then:

```bash
ln -s ../../../watchtower/skills/resume-from-session plugins/token-sitter/skills/resume-from-session
```

Add one line to root `README.md`'s Auto-snapshot command list: `- /resume-from-session — pick one of the last 10 sessions for this project and continue from its transcript (no snapshot needed).`

- [ ] **Step 5: Run tests to verify they pass**

Run: `python3 -m pytest tests/test_snapshot_skills.py tests/test_plugin_manifest.py tests/test_skills_sync.py -v`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add watchtower/skills/resume-from-session watchtower/skills_sync.py plugins/token-sitter/skills/resume-from-session tests/test_snapshot_skills.py tests/test_plugin_manifest.py tests/test_skills_sync.py README.md
git commit --only watchtower/skills/resume-from-session watchtower/skills_sync.py plugins/token-sitter/skills/resume-from-session tests/test_snapshot_skills.py tests/test_plugin_manifest.py tests/test_skills_sync.py README.md -m "feat(skills): resume-from-session — continue from a prior transcript without a snapshot"
```

## Self-review notes (already applied)

- Spec deltas discovered during planning, now authoritative: no new Codex adapter (existing `messages.deliver()` app-server path covers it); no `wt snapshot install --engine` (existing `skills_sync` covers all four engine homes — grok is not in `ENGINE_HOMES`, so grok users copy the skill files manually until a grok home is added, which matches Tier 3).
- Type consistency verified: `arm/disarm/record/fire` all return `{"ok": bool, ...}` dicts; `next_action` tuples are matched exactly in `run_timer`; CLI verbs match the skill texts and tests.
- The Task 2 `fire` stub is intentional and replaced in Task 3; tests for Task 2 inject `fire_fn` so ordering is safe.

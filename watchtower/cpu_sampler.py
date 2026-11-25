"""CPU-weighted stack sampler for the daemon (WT-122).

``echo SECONDS > <data dir>/profile-cpu`` makes the daemon sample every
thread's stack for SECONDS (max 900) and write ``profile-cpu.out`` (a
summary) and ``profile-cpu.folded`` (flamegraph "folded" stacks) next to it.

cProfile cannot answer "where does the CPU go": on Python 3.12 it sees every
thread and its times include GIL waits, and SIGUSR1 stacks show where threads
*wait*. This sampler weights each sample by the CPU the thread actually
consumed since its previous sample (per-thread CPU clock), so a thread
blocked on the GIL, a lock or I/O contributes nothing. Short-lived threads
(HTTP handlers, status refreshes) are counted from creation: a thread's CPU
clock starts at zero.

Costs nothing until asked: the daemon tick only checks for the request file.
"""
from __future__ import annotations

import collections
import os
import re
import sys
import threading
import time
from typing import Any, Counter, Dict, Optional, Tuple

MAX_SECONDS = 900.0
INTERVAL_S = 0.01

_running = threading.Event()
_THREAD_NUM = re.compile(r"^Thread-\d+ ?")
_DIGITS = re.compile(r"\d+")


def _thread_label(thread: Optional[threading.Thread]) -> str:
    if thread is None:
        return "?"
    name = _THREAD_NUM.sub("", thread.name or "").strip("() ") or "thread"
    return _DIGITS.sub("N", name)


def _frame_label(frame: Any) -> str:
    code = frame.f_code
    mod = os.path.basename(code.co_filename)
    if mod.endswith(".py"):
        mod = mod[:-3]
    return f"{mod}:{getattr(code, 'co_qualname', code.co_name)}"


def _stack(frame: Any, limit: int = 80) -> Tuple[str, ...]:
    out = []
    while frame is not None and len(out) < limit:
        out.append(_frame_label(frame))
        frame = frame.f_back
    out.reverse()  # root first
    return tuple(out)


def _thread_cpu(ident: int) -> Optional[float]:
    try:
        return time.clock_gettime(time.pthread_getcpuclockid(ident))
    except (OSError, OverflowError, ValueError):
        return None


def sample(seconds: float, interval: float = INTERVAL_S) -> Dict[str, Any]:
    """Sample for ``seconds``; return CPU seconds per (thread label, stack)."""
    me = threading.get_ident()
    last: Dict[Tuple[int, Any], float] = {}
    stacks: Counter = collections.Counter()
    seen = set()
    # Threads alive now: baseline their clocks so their pre-sample CPU is not
    # charged to the first stack we see.
    for ident in sys._current_frames():
        thread = threading._active.get(ident)  # type: ignore[attr-defined]
        cpu = _thread_cpu(ident)
        if cpu is not None:
            last[(ident, getattr(thread, "native_id", None))] = cpu
            seen.add((ident, getattr(thread, "native_id", None)))
    proc0 = time.process_time()
    wall0 = time.monotonic()
    self0 = time.thread_time()
    samples = 0
    end = wall0 + min(float(seconds), MAX_SECONDS)
    while time.monotonic() < end:
        time.sleep(interval)
        samples += 1
        frames = sys._current_frames()
        for ident, frame in frames.items():
            if ident == me:
                continue
            thread = threading._active.get(ident)  # type: ignore[attr-defined]
            key = (ident, getattr(thread, "native_id", None))
            cpu = _thread_cpu(ident)
            if cpu is None:
                continue
            # A thread first seen mid-run started after the baseline: its
            # clock began at zero.
            prev = last.get(key, 0.0 if key not in seen else cpu)
            seen.add(key)
            last[key] = cpu
            delta = cpu - prev
            if delta > 0:
                stacks[(_thread_label(thread), _stack(frame))] += delta
        del frames
    wall = time.monotonic() - wall0
    return {
        "wall_s": wall,
        "process_cpu_s": time.process_time() - proc0,
        "sampler_cpu_s": time.thread_time() - self0,
        "samples": samples,
        "stacks": stacks,
    }


def render(result: Dict[str, Any], top: int = 60) -> str:
    stacks: Counter = result["stacks"]
    wall = result["wall_s"] or 1.0
    attributed = sum(stacks.values())
    pct = lambda s: 100.0 * s / wall  # noqa: E731 - % of one core
    lines = [
        f"wall {wall:.1f} s, {result['samples']} samples, pid {os.getpid()}",
        f"process cpu {pct(result['process_cpu_s']):.1f}% of one core; "
        f"attributed {pct(attributed):.1f}%; sampler itself "
        f"{pct(result['sampler_cpu_s']):.1f}%",
        "",
        "== by thread (% of one core) ==",
    ]
    by_thread: Counter = collections.Counter()
    selfc: Counter = collections.Counter()
    incl: Counter = collections.Counter()
    for (label, stack), cpu in stacks.items():
        by_thread[label] += cpu
        if stack:
            selfc[stack[-1]] += cpu
        for fn in set(stack):
            incl[fn] += cpu
    for label, cpu in by_thread.most_common(15):
        lines.append(f"{pct(cpu):6.1f}  {label}")
    lines += ["", f"== self (leaf) top {top} =="]
    for fn, cpu in selfc.most_common(top):
        lines.append(f"{pct(cpu):6.2f}  {fn}")
    lines += ["", f"== inclusive top {top * 2} =="]
    for fn, cpu in incl.most_common(top * 2):
        lines.append(f"{pct(cpu):6.2f}  {fn}")
    return "\n".join(lines) + "\n"


def folded(result: Dict[str, Any]) -> str:
    out = []
    for (label, stack), cpu in result["stacks"].most_common():
        ms = int(round(cpu * 1000))
        if ms:
            out.append(";".join((label,) + stack) + f" {ms}")
    return "\n".join(out) + "\n"


def _write(dest: Any, text: str) -> None:
    tmp = dest.with_name(dest.name + ".tmp")
    tmp.write_text(text)
    os.replace(tmp, dest)


def _run(seconds: float) -> None:
    from . import storage
    try:
        result = sample(seconds)
        base = storage.data_dir()
        _write(base / "profile-cpu.folded", folded(result))
        _write(base / "profile-cpu.out", render(result))
    except Exception as e:  # noqa: BLE001 - a diagnostic must never raise
        print(f"[watchtower] cpu sampler failed: {e}", flush=True)
    finally:
        _running.clear()


def maybe_start() -> bool:
    """Start a background sampling run if ``profile-cpu`` was requested."""
    if _running.is_set():
        return False
    from . import storage
    req = storage.data_dir() / "profile-cpu"
    try:
        text = req.read_text().strip()
    except OSError:
        return False
    try:
        req.unlink()
    except OSError:
        pass
    try:
        seconds = float(text or "60")
    except ValueError:
        seconds = 60.0
    seconds = max(1.0, min(seconds, MAX_SECONDS))
    _running.set()
    threading.Thread(target=_run, args=(seconds,), daemon=True,
                     name="wt-cpu-sampler").start()
    print(f"[watchtower] cpu sampler: {seconds:.0f} s", flush=True)
    return True

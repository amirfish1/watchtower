"""Tell a resumed worker when its rules changed while it was blocked.

A parked session resumes with the rules it read at spawn still in its
context. If the shared runbook or the queue's learnings file was revised
after the block, the session would keep applying the old text (a retired
"ask the owner first" rule keeps producing owner questions long after it was
removed). The answer prompt therefore names the current revision and says to
re-read it.

Only a notice: nothing here grants or removes authority. An explicit owner
stop or hold recorded on the ticket outlives any rule revision.
"""
from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def rule_sources(queue: str) -> List[Tuple[str, Path]]:
    """(label, path) of every rule file a worker of ``queue`` is told to read."""
    from . import workers
    out: List[Tuple[str, Path]] = [("worker runbook", Path(workers._WORKER_RUNBOOK_PATH))]
    name = str(queue or "").strip().upper()
    if name:
        home = Path(os.environ.get("WATCHTOWER_LEARNINGS_DIR")
                    or (Path.home() / ".watchtower" / "learnings"))
        out.append((f"{name} learnings", home / f"{name}.md"))
    return out


def _parse(value: Any) -> Optional[float]:
    try:
        dt = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def blocked_since(item: Dict[str, Any]) -> Optional[float]:
    """When the session last stopped to wait: the block being answered. The
    answer path may already have cleared ``blocked_at``, so the last block
    event in history and the park stamp count too. None = unknown."""
    stamps = [_parse(item.get("blocked_at")), _parse((item.get("parked") or {}).get("at"))]
    for h in reversed(item.get("history") or []):
        if isinstance(h, dict) and h.get("event") == "block":
            stamps.append(_parse(h.get("at")))
            break
    known = [s for s in stamps if s is not None]
    return max(known) if known else None


def revisions(queue: str) -> List[Dict[str, Any]]:
    """Current revision of each rule file that exists: label, path, short
    content hash, modification time."""
    out: List[Dict[str, Any]] = []
    for label, path in rule_sources(queue):
        try:
            data = path.read_bytes()
            mtime = path.stat().st_mtime
        except OSError:
            continue
        out.append({"label": label, "path": str(path), "mtime": mtime,
                    "revision": hashlib.sha256(data).hexdigest()[:12]})
    return out


def notice(item: Dict[str, Any]) -> str:
    """Sentence to append to a resume/answer prompt, or "" when no rule file
    changed after the block (or the block time is unknown and nothing can be
    compared: an unknown block time still gets the notice, since silence
    would be the unsafe guess)."""
    queue = str(item.get("project") or "")
    since = blocked_since(item)
    changed = [r for r in revisions(queue) if since is None or r["mtime"] > since]
    if not changed:
        return ""
    parts = "; ".join(
        f"{r['label']} {r['path']} (revision {r['revision']}, changed "
        f"{datetime.fromtimestamp(r['mtime'], timezone.utc).strftime('%Y-%m-%d %H:%M UTC')})"
        for r in changed)
    return (f" Rules changed while this ticket was blocked: {parts}. Re-read them before "
            f"acting. The current text replaces any earlier version of those rules in your "
            f"context. It does not lift an explicit owner stop, hold or spending limit "
            f"recorded on this ticket, and it does not approve anything by itself.")

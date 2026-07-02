"""Keep the bundled `watchtower` agent skill in sync across every installed
agent harness (Claude Code, Codex, ...) that reads skills from a per-user
directory.

Ships as a symlink, not a copy: editing the skill source (or `git pull`ing a
newer watchtower) updates every synced target instantly, with no separate
"re-sync" step required. `wt install` calls `sync()` on every run so this
never goes stale; `wt skills sync` exposes the same idempotent operation
standalone.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, NamedTuple, Optional

SKILL_NAME = "watchtower"

# One entry per agent harness this machine might have. A harness is skipped
# entirely if its home directory doesn't exist -- we never create ~/.codex on
# a machine that doesn't have Codex installed, and vice versa.
ENGINE_HOMES: Dict[str, Path] = {
    "claude": Path.home() / ".claude",
    "codex": Path.home() / ".codex",
}


def source_dir() -> Path:
    return Path(__file__).parent / "skills" / SKILL_NAME


class SyncResult(NamedTuple):
    engine: str
    target: Path
    action: str  # linked | relinked | up-to-date | skipped-exists | skipped-not-installed | removed | not-installed


def sync(dry_run: bool = False, engine_homes: Optional[Dict[str, Path]] = None) -> List[SyncResult]:
    """Symlink the bundled skill into every present harness's skills dir.

    Idempotent: re-running leaves an already-correct symlink untouched, fixes
    one pointing at a stale location, and never clobbers a real
    directory/file a user placed at the target by hand (reported as
    skipped-exists instead)."""
    homes = ENGINE_HOMES if engine_homes is None else engine_homes
    source = source_dir()
    results: List[SyncResult] = []
    for engine, home in homes.items():
        target = home / "skills" / SKILL_NAME
        if not home.exists():
            results.append(SyncResult(engine, target, "skipped-not-installed"))
            continue
        if target.is_symlink():
            if target.resolve() == source.resolve():
                results.append(SyncResult(engine, target, "up-to-date"))
                continue
            action = "relinked"
        elif target.exists():
            results.append(SyncResult(engine, target, "skipped-exists"))
            continue
        else:
            action = "linked"
        if not dry_run:
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.is_symlink() or target.exists():
                target.unlink()
            target.symlink_to(source, target_is_directory=True)
        results.append(SyncResult(engine, target, action))
    return results


def remove(engine_homes: Optional[Dict[str, Path]] = None) -> List[SyncResult]:
    """Undo sync(): remove only the symlinks we manage. Never touches a real
    directory/file a user placed at the target by hand."""
    homes = ENGINE_HOMES if engine_homes is None else engine_homes
    source = source_dir()
    results: List[SyncResult] = []
    for engine, home in homes.items():
        target = home / "skills" / SKILL_NAME
        if target.is_symlink() and target.resolve() == source.resolve():
            target.unlink()
            results.append(SyncResult(engine, target, "removed"))
        elif target.exists():
            results.append(SyncResult(engine, target, "skipped-exists"))
        else:
            results.append(SyncResult(engine, target, "not-installed"))
    return results


def format_result(r: SyncResult) -> str:
    return f"  {r.engine:<8} {r.target}  [{r.action}]"

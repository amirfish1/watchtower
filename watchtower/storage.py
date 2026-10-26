"""User-owned queue data, independent of either application's installation.

Legacy files are migration sources only. Never delete them during migration,
and never fall back to them once a durable destination exists.
"""

from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import tempfile


def data_dir() -> Path:
    override = os.environ.get("WATCHTOWER_DATA_DIR")
    if override:
        return Path(override).expanduser().absolute()
    xdg = os.environ.get("XDG_DATA_HOME", "")
    base = Path(xdg) if xdg and Path(xdg).is_absolute() else Path.home() / ".local" / "share"
    return base / "watchtower"


@contextmanager
def file_lock(path: Path):
    """Fail closed if migration cannot be serialized across processes."""
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with path.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def migration_lock(directory: Path):
    return file_lock(directory / ".migration.lock")


@contextmanager
def atomic_destination(destination: Path):
    """Publish only complete, flushed migration results; clean up on failure."""
    fd, name = tempfile.mkstemp(prefix=f".{destination.name}.", dir=destination.parent)
    os.close(fd)
    temporary = Path(name)
    try:
        yield temporary
        with temporary.open("rb") as result:
            os.fsync(result.fileno())
        os.replace(temporary, destination)
        directory_fd = os.open(destination.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)


def migrate_config(source: Path) -> Path:
    destination = data_dir() / "queue-config.json"
    if destination.exists() or source == destination:
        return destination
    with migration_lock(destination.parent):
        if not destination.exists():
            # Persist an empty config too: a reinstalled app must never revive
            # stale legacy settings after all queues have been removed.
            data = json.loads(source.read_text()) if source.exists() else {}
            if not isinstance(data, dict):
                raise ValueError(f"queue config must be a JSON object: {source}")
            # Preserve the completed eligibility migration so reinstalling
            # does not unexpectedly turn GitHub queue draining off again.
            marker = source.parent / "gh-drain-migration.done"
            saved_marker = destination.parent / marker.name
            if marker.exists() and not saved_marker.exists():
                with atomic_destination(saved_marker) as temporary:
                    temporary.write_bytes(marker.read_bytes())
            with atomic_destination(destination) as temporary:
                temporary.write_text(json.dumps(data, indent=2) + "\n")
    return destination

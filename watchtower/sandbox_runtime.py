"""Pinned offline pytest for trusted-controller command gates (WT-86).

Assets are DATA from this trusted controller package, never from the source
being tested or a discovered host environment. They are copied into a fresh
store before any gate code runs. No installation, extraction or host import.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import stat
import zipfile

PINS = {
    "sandbox_pytest_runtime.zip": "62bbf81bd912bdf5e6736933ad5cbf175c3d2e65e375dfb234551ba41a20cc3c",
    "sandbox_pytest_runtime.json": "0d0ea9b3f361890e24874d79d9b36db65df61810e593be80634e53073aadc407",
}
MAX_ASSET_BYTES = 4 << 20
MAX_EXPANDED_BYTES = 16 << 20


def _asset(name: str) -> bytes:
    path = Path(__file__).parent / name
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        st = os.fstat(fd)
        if (not stat.S_ISREG(st.st_mode) or st.st_nlink != 1
                or st.st_uid not in (0, os.getuid()) or st.st_mode & 0o022
                or st.st_size > MAX_ASSET_BYTES):
            raise ValueError(f"unsafe offline runtime asset: {name}")
        with os.fdopen(fd, "rb", closefd=False) as src:
            data = src.read(MAX_ASSET_BYTES + 1)
        if len(data) > MAX_ASSET_BYTES or hashlib.sha256(data).hexdigest() != PINS[name]:
            raise ValueError(f"offline runtime digest mismatch: {name}")
        return data
    finally:
        os.close(fd)


def prepare(store: Path) -> tuple[dict[str, str], dict]:
    """Fresh store only. Failure must refuse the gate; never use host pytest."""
    blob = _asset("sandbox_pytest_runtime.zip")
    manifest_bytes = _asset("sandbox_pytest_runtime.json")
    manifest = json.loads(manifest_bytes)
    if manifest["schema"] != 1 or manifest["archive_sha256"] != PINS["sandbox_pytest_runtime.zip"]:
        raise ValueError("offline runtime manifest mismatch")
    rows = [row for dist in manifest["distributions"] for row in dist["files"]]
    expected = {row["path"]: row for row in rows}
    if len(expected) != len(rows):
        raise ValueError("duplicate offline runtime manifest path")
    with zipfile.ZipFile(io.BytesIO(blob)) as archive:
        entries = archive.infolist()
        if (len(entries) != len(expected) or {e.filename for e in entries} != set(expected)
                or sum(e.file_size for e in entries) > MAX_EXPANDED_BYTES):
            raise ValueError("offline runtime archive inventory mismatch")
        for entry in entries:
            path = PurePosixPath(entry.filename)
            if (path.is_absolute() or any(p in (".", "..") for p in path.parts)
                    or "\\" in entry.filename or entry.is_dir()
                    or not stat.S_ISREG(entry.external_attr >> 16)
                    or path.suffix in {".pth", ".pyc", ".so"}
                    or path.name in {"sitecustomize.py", "usercustomize.py"}):
                raise ValueError("unsafe offline runtime archive entry")
            row = expected[entry.filename]
            if (entry.file_size != row["bytes"]
                    or hashlib.sha256(archive.read(entry)).hexdigest() != row["sha256"]):
                raise ValueError("offline runtime file digest mismatch")
    # store is a controller-allocated, unused directory. Exclusive creation
    # refuses any unexpected prior occupant instead of following/reusing it.
    dest = store / "offline-pytest.zip"
    fd = os.open(dest, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o400)
    with os.fdopen(fd, "wb") as out:
        out.write(blob)
    proof = store / "offline-pytest.manifest.json"
    fd = os.open(proof, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o400)
    with os.fdopen(fd, "wb") as out:
        out.write(manifest_bytes)
    return ({"PYTHONPATH": str(dest), "PYTHONNOUSERSITE": "1",
             "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1"},
            {"archive_sha256": manifest["archive_sha256"],
             "manifest_sha256": PINS["sandbox_pytest_runtime.json"],
             "distributions": {d["name"]: d["version"] for d in manifest["distributions"]},
             "files": len(rows), "offline": True})

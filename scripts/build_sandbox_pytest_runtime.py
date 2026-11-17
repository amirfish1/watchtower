#!/usr/bin/env python3
"""Offline maintainer tool. Read pinned installed distributions as DATA only.

Run with the trusted development venv. Review generated manifest/archive and
update sandbox_runtime.PINS only as part of a reviewed controller change.
No pip, download, package import or gate-time environment discovery.
"""
import base64
import hashlib
import importlib.metadata as metadata
import json
from pathlib import Path, PurePosixPath
import stat
import zipfile

VERSIONS = {"pytest": "9.1.1", "pluggy": "1.6.0", "packaging": "26.3",
            "iniconfig": "2.3.0", "pygments": "2.21.0"}
TOPS = {"pytest": {"pytest", "_pytest", "py.py"}, "pluggy": {"pluggy"},
        "packaging": {"packaging"}, "iniconfig": {"iniconfig"}, "pygments": {"pygments"}}
META = {"METADATA", "WHEEL", "LICENSE", "LICENSE.txt", "entry_points.txt", "top_level.txt"}


def build():
    payload, provenance = {}, []
    for name, version in VERSIONS.items():
        dist = metadata.distribution(name)
        if dist.version != version:
            raise ValueError(f"{name}: expected {version}, got {dist.version}")
        if "Root-Is-Purelib: true" not in (dist.read_text("WHEEL") or ""):
            raise ValueError(f"{name}: not a pure Python wheel")
        rows = []
        for item in dist.files or []:
            rel = PurePosixPath(str(item))
            # Console scripts, bytecode and installation-local metadata are
            # intentionally excluded. No path is followed or imported.
            if rel.is_absolute() or ".." in rel.parts or "__pycache__" in rel.parts:
                continue
            top = rel.parts[0]
            is_meta = top.endswith(".dist-info")
            if top not in TOPS[name] and not is_meta:
                continue
            if is_meta and not (rel.name in META or "licenses" in rel.parts[1:]):
                continue
            if not is_meta and rel.suffix not in {".py", ".typed"}:
                raise ValueError(f"unexpected package payload: {rel}")
            path = Path(dist.locate_file(item))
            if any(p.is_symlink() for p in [path, *path.parents]):
                raise ValueError(f"linked source: {path}")
            st = path.stat()
            if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1 or st.st_size > 4 << 20:
                raise ValueError(f"unsafe source: {path}")
            data = path.read_bytes()
            record = item.hash
            digest = hashlib.sha256(data).digest()
            if (not record or record.mode != "sha256" or
                    base64.urlsafe_b64encode(digest).rstrip(b"=").decode() != record.value):
                raise ValueError(f"installed RECORD mismatch: {rel}")
            key = rel.as_posix()
            if key in payload:
                raise ValueError(f"duplicate payload: {key}")
            payload[key] = data
            rows.append({"path": key, "sha256": digest.hex(), "bytes": len(data)})
        if not rows:
            raise ValueError(f"missing installed files: {name}")
        provenance.append({"name": name, "version": version,
                           "source": "installed pure-Python wheel; each file verified against SHA256 RECORD",
                           "files": sorted(rows, key=lambda r: r["path"])})
    root = Path(__file__).resolve().parent.parent / "watchtower"
    archive = root / "sandbox_pytest_runtime.zip"
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as out:
        for name, data in sorted(payload.items()):
            info = zipfile.ZipInfo(name, (2026, 1, 1, 0, 0, 0))
            info.external_attr = 0o100644 << 16
            info.compress_type = zipfile.ZIP_DEFLATED
            out.writestr(info, data)
    manifest = {"schema": 1, "archive_sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
                "distributions": provenance}
    target = root / "sandbox_pytest_runtime.json"
    target.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    for path in (archive, target):
        path.chmod(0o644)
        print(path.name, path.stat().st_size, hashlib.sha256(path.read_bytes()).hexdigest())


if __name__ == "__main__":
    build()

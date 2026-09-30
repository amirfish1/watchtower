"""Model catalogs, read from each engine's own model list at call time (WT-13).

Nothing here (or anywhere in WatchTower's code) names a model. Sources:

- codex:  ``~/.codex/models_cache.json``  (Codex's own list: slugs + efforts)
- claude: ``~/.claude/command-center/claude-models.json``  (CCC refreshes it
  from Anthropic's models page: ids + prices)
- devin:  ``~/.claude/command-center/devin_models_catalog.json``  (CCC's)
- any engine, plus rank overrides: ``~/.watchtower/models.json`` (user-edited)::

      {"engines": {"kimi": [{"id": "x/y", "output_per_mtok": 4},
                            {"id": "z", "label": "Display Name"}]},
       "rank": {"x/y": 3.5}}

A source that is missing or unreadable yields ``None`` (an *unknown* catalog,
distinct from an empty one): callers then accept any explicitly pinned model
and surface :func:`catalog_warning` rather than rejecting or rewriting it.
The policy deny-list stays in ``config`` and applies on top of everything here.

Floor ranking is data-derived: ``rank`` override, else output price per Mtok
from the catalog. Models with neither are unranked.
"""
from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ENGINES = ("codex", "claude", "kimi", "devin", "antigravity")
VALID_EFFORTS = ("low", "medium", "high", "xhigh", "max")


def _path(env: str, default: Path) -> Path:
    return Path(os.environ.get(env) or default)


CODEX_MODELS_CACHE = _path("WATCHTOWER_CODEX_MODELS_CACHE",
                           Path.home() / ".codex" / "models_cache.json")
CLAUDE_MODELS_FILE = _path("WATCHTOWER_CLAUDE_MODELS_FILE",
                           Path.home() / ".claude" / "command-center" / "claude-models.json")
DEVIN_MODELS_FILE = _path("WATCHTOWER_DEVIN_MODELS_FILE",
                          Path.home() / ".claude" / "command-center" / "devin_models_catalog.json")
USER_MODELS_FILE = _path("WATCHTOWER_MODELS_FILE",
                         Path.home() / ".watchtower" / "models.json")

# Fable is a frontier tier reserved for interactive use, never a worker model.
_EXCLUDED_WORKER = re.compile(r"fable", re.I)

_cache: Dict[Tuple[str, int, int], Any] = {}


def _read_json(path: Path) -> Any:
    """Parsed JSON, memoised on (path, mtime, size); None if absent/unreadable."""
    try:
        p = Path(path).expanduser()
        st = p.stat()
        key = (str(p), st.st_mtime_ns, st.st_size)
        if key not in _cache:
            _cache[key] = json.loads(p.read_text())
        return _cache[key]
    except (OSError, ValueError):
        return None


def _entry(model_id: str, efforts: tuple = (), price: Any = None,
           label: str = "") -> Dict[str, Any]:
    return {"id": model_id, "efforts": efforts, "price": price, "label": label}


def _codex() -> Optional[List[dict]]:
    data = _read_json(CODEX_MODELS_CACHE)
    rows = data.get("models") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        return None
    out = []
    listed = [r for r in rows if isinstance(r, dict) and r.get("slug")
              and r.get("visibility") == "list"]
    for r in sorted(listed, key=lambda r: r.get("priority") or 0):
        levels = [str(x.get("effort") if isinstance(x, dict) else x).lower()
                  for x in r.get("supported_reasoning_levels") or ()]
        out.append(_entry(str(r["slug"]),
                          tuple(e for e in VALID_EFFORTS if e in levels) or VALID_EFFORTS))
    return out


def _claude() -> Optional[List[dict]]:
    data = _read_json(CLAUDE_MODELS_FILE)
    rows = data.get("records") if isinstance(data, dict) else None
    if not isinstance(rows, list):
        return None
    rows = [r for r in rows if isinstance(r, dict) and r.get("id")]
    out = []
    for r in sorted(rows, key=lambda r: str(r.get("released_at") or ""), reverse=True):
        mid = str(r["id"])
        full = mid if mid.startswith("claude-") else f"claude-{mid}"
        out.append(_entry(full, VALID_EFFORTS, r.get("output_per_mtok"), label=mid))
    return out


def _devin() -> Optional[List[dict]]:
    data = _read_json(DEVIN_MODELS_FILE)
    fams = data.get("families") if isinstance(data, dict) else None
    if not isinstance(fams, list):
        return None
    out = []
    for fam in fams:
        if not isinstance(fam, dict):
            continue
        if fam.get("slug"):
            out.append(_entry(str(fam["slug"])))
        for v in fam.get("variants") or ():
            if isinstance(v, dict) and v.get("model_uid"):
                out.append(_entry(str(v["model_uid"]), label=str(v.get("label") or "")))
    return out


def _user(eng: str) -> Optional[List[dict]]:
    data = _read_json(USER_MODELS_FILE)
    rows = ((data or {}).get("engines") or {}).get(eng) if isinstance(data, dict) else None
    if not isinstance(rows, list):
        return None
    out = []
    for r in rows:
        if isinstance(r, str):
            out.append(_entry(r))
        elif isinstance(r, dict) and r.get("id"):
            eff = tuple(e for e in VALID_EFFORTS if e in (r.get("efforts") or ()))
            out.append(_entry(str(r["id"]), eff, r.get("output_per_mtok"),
                              str(r.get("label") or "")))
    return out


_SOURCES = {"codex": _codex, "claude": _claude, "devin": _devin}


def entries(eng: str) -> Optional[List[dict]]:
    """Catalog rows for ``eng`` (native source, then the user file), or None
    when no source is readable."""
    eng = str(eng or "").strip().lower()
    native = _SOURCES[eng]() if eng in _SOURCES else None
    user = _user(eng)
    if native is None and user is None:
        return None
    seen, out = set(), []
    for row in (native or []) + (user or []):
        if row["id"] in seen or (eng == "claude" and _EXCLUDED_WORKER.search(row["id"])):
            continue
        seen.add(row["id"])
        out.append(row)
    return out


def catalog(eng: str) -> Optional[Dict[str, tuple]]:
    """``{model id: efforts}`` in catalog order, or None when unknown."""
    rows = entries(eng)
    return None if rows is None else {r["id"]: r["efforts"] for r in rows}


def catalog_warning(eng: str) -> str:
    if catalog(eng) is None:
        return (f"no model catalog readable for {eng}; any pinned model is "
                f"accepted unchecked (sources: see watchtower/models.py)")
    return ""


def aliases(eng: str) -> Dict[str, str]:
    """Short forms that resolve to a catalog id: a claude id without its
    ``claude-`` prefix, and catalog display labels (antigravity's picker)."""
    out: Dict[str, str] = {}
    for r in entries(eng) or ():
        if r["label"] and r["label"] != r["id"]:
            out[r["label"]] = r["id"]
        if eng == "claude" and r["id"].startswith("claude-"):
            out[r["id"][len("claude-"):]] = r["id"]
    return out


def rank(model_id: str) -> Optional[float]:
    """Floor rank of a model: user ``rank`` override, else catalog output
    price per Mtok; None when neither exists (unranked)."""
    model_id = str(model_id or "").strip()
    data = _read_json(USER_MODELS_FILE)
    override = ((data or {}).get("rank") or {}).get(model_id) if isinstance(data, dict) else None
    if isinstance(override, (int, float)):
        return float(override)
    for eng in ENGINES:
        for r in entries(eng) or ():
            if r["id"] == model_id and isinstance(r["price"], (int, float)):
                return float(r["price"])
    return None


def known(model_id: str) -> bool:
    """Whether any catalog (or a rank override) mentions ``model_id``."""
    model_id = str(model_id or "").strip()
    return rank(model_id) is not None or any(
        model_id in (catalog(e) or ()) for e in ENGINES)


def engine_of(model_id: str, prefer: str = "") -> str:
    """Engine whose catalog lists ``model_id`` (``prefer`` checked first);
    "" when none does. Strict: an unknown catalog never matches."""
    model_id = str(model_id or "").strip()
    order = ([prefer] if prefer else []) + [e for e in ENGINES if e != prefer]
    for eng in order:
        if model_id and model_id in (catalog(eng) or ()):
            return eng
    return ""

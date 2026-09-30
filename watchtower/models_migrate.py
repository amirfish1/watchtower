"""`wt models migrate` / `wt models unpin`: bulk-move queues (and optionally the
host worker default) from one model to another, or clear pins so queues follow
the default. Dry-run unless ``apply=True`` (WT-7).

A queue's *effective* model is ``config.raw_model`` (explicit pin, else CCC's
worker default), the same resolution ``wt config`` uses -- never the legacy
queue-config.json. Only queues with an explicit pin are rewritten; queues that
merely inherit the default move when the default does (``include_default``).
"""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional

from . import config

ENGINES = ("claude", "codex", "kimi", "devin", "antigravity")


def _engine_of(model_id: str, prefer: str = "") -> str:
    """Engine whose approved list contains ``model_id`` (``prefer`` first)."""
    order = ([prefer] if prefer else []) + [e for e in ENGINES if e != prefer]
    for eng in order:
        if model_id and config.is_approved_model(eng, config.canonical_model(eng, model_id)):
            return eng
    return ""


def _counts_and_items() -> Dict[str, List[dict]]:
    from . import queue as _queue
    out: Dict[str, List[dict]] = {}
    for it in _queue.list_items():
        if it.get("status") in ("open", "in_progress"):
            out.setdefault(it.get("project") or "GEN", []).append(it)
    return out


def _floor_warnings(queue_name: str, items: List[dict], new_model: str) -> List[str]:
    tiers = config.MODEL_FLOOR_TIERS
    if new_model not in tiers:
        return []
    out = []
    for it in items:
        floor = str(it.get("model_floor") or "")
        if floor in tiers and tiers.index(floor) > tiers.index(new_model):
            out.append(
                f"{it.get('ref', '?')} ({it.get('status')}) has model floor {floor}, "
                f"above {new_model}: it will BLOCK on {queue_name}"
            )
    return out


def _row(queue_name: str, entry: dict, items: List[dict]) -> dict:
    return {
        "queue": queue_name,
        "engine": config.engine(queue_name),
        "model": config.raw_model(queue_name),
        "source": "pinned" if entry.get("model") else "default",
        "auto_drain": config.auto_drain(queue_name),
        "open": sum(1 for i in items if i.get("status") == "open"),
        "in_progress": sum(1 for i in items if i.get("status") == "in_progress"),
    }


def _default_changes(frm: str, to: str) -> List[dict]:
    """Keys in CCC spawn-defaults.json that resolve to ``frm`` and should become
    ``to``. Original short-form style ("sonnet-5" vs "claude-sonnet-5") kept."""
    path = config.CCC_SPAWN_DEFAULTS_FILE
    try:
        data = json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return []
    eng = _engine_of(frm)
    changes = []

    def consider(keypath, cur, cur_engine):
        cur = str(cur or "").strip()
        if not cur or config.canonical_model(cur_engine, cur) != frm:
            return
        new = to[len("claude-"):] if cur_engine == "claude" and not cur.startswith("claude-") \
            and to.startswith("claude-") else to
        changes.append({"key": keypath, "from": cur, "to": new})

    consider("worker_model", data.get("worker_model"),
             str(data.get("worker_engine") or "").strip().lower())
    consider("models." + eng, (data.get("models") or {}).get(eng), eng)
    return changes


def _write_defaults(changes: List[dict]) -> str:
    path = os.path.realpath(config.CCC_SPAWN_DEFAULTS_FILE)
    data = json.loads(Path(path).read_text())
    for ch in changes:
        if ch["key"] == "worker_model":
            data["worker_model"] = ch["to"]
        else:
            data.setdefault("models", {})[ch["key"].split(".", 1)[1]] = ch["to"]
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), prefix=".spawn-defaults.")
    with os.fdopen(fd, "w") as f:
        json.dump(data, f, indent=2)
        f.write("\n")
    os.replace(tmp, path)
    return path


def plan_migrate(frm: str, to: str, *, queues: Optional[List[str]] = None,
                 engine: str = "", include_default: bool = False) -> dict:
    """Build the migration plan. Raises ValueError on an unapproved/blocked
    ``to`` or an undeclared cross-engine move."""
    frm_raw, to_raw = str(frm or "").strip(), str(to or "").strip()
    to_eng = _engine_of(to_raw, prefer=engine)
    if not to_raw or not to_eng or (engine and to_eng != engine):
        raise ValueError(
            f"{to_raw!r} is not an approved model"
            + (f" for {engine}" if engine else "")
            + "; run `wt models --engine <engine>` for the list"
        )
    to_m = config.canonical_model(to_eng, to_raw)
    if config.is_blocked_model(to_m):
        raise ValueError(f"{to_m!r} is blocked by model policy; refusing to migrate onto it")
    frm_m = config.canonical_model(_engine_of(frm_raw) or to_eng, frm_raw)
    wanted = set(queues) if queues else None
    open_items = _counts_and_items()
    rows, warnings, refused = [], [], []
    for name, entry in sorted(config.all_queues().items()):
        if wanted is not None and name not in wanted:
            continue
        if config.raw_model(name) != frm_m:
            continue
        items = open_items.get(name, [])
        row = _row(name, entry, items)
        row["action"] = "set model" if row["source"] == "pinned" else "follows default"
        if row["engine"] != to_eng:
            if engine == to_eng:
                row["action"] += f" + engine {row['engine']}->{to_eng}"
            else:
                row["action"] = "REFUSED (cross-engine; pass --engine)"
                refused.append(name)
        rows.append(row)
        if row["source"] == "pinned" and name not in refused:
            warnings += _floor_warnings(name, items, to_m)
            eff = str(entry.get("effort") or "")
            if eff and not config.is_approved_effort(to_eng, to_m, eff):
                warnings.append(f"{name}: effort {eff!r} is not supported by {to_m}")
    if wanted:
        for miss in sorted(wanted - {r["queue"] for r in rows}):
            warnings.append(f"{miss}: not on {frm_m} (skipped)")
    for name in sorted(config.all_queues()):
        m = config.raw_model(name)
        if m and config.is_blocked_model(m):
            warnings.append(f"{name}: model {m} is blocked by model policy")
    return {
        "from": frm_m, "to": to_m, "engine": to_eng, "rows": rows,
        "refused": refused, "warnings": warnings,
        "default_changes": _default_changes(frm_m, to_m) if include_default else [],
        "include_default": include_default,
        "defaults_file": str(config.CCC_SPAWN_DEFAULTS_FILE),
    }


def apply_migrate(plan: dict) -> List[str]:
    """Write the plan; returns human lines describing what changed."""
    done = []
    for row in plan["rows"]:
        if row["queue"] in plan["refused"] or row["source"] != "pinned":
            continue
        if row["engine"] != plan["engine"]:
            config.set_engine(row["queue"], plan["engine"])
        config.set_model(row["queue"], plan["to"])
        done.append(f"{row['queue']}: model {plan['from']} -> {plan['to']}")
    if plan["default_changes"]:
        path = _write_defaults(plan["default_changes"])
        for ch in plan["default_changes"]:
            done.append(f"{path}: {ch['key']} {ch['from']} -> {ch['to']}")
    return done


def plan_unpin(*, queues: Optional[List[str]] = None, matching: str = "") -> dict:
    match_m = ""
    if matching:
        match_m = config.canonical_model(_engine_of(matching) or "", matching)
    wanted = set(queues) if queues else None
    open_items = _counts_and_items()
    rows = []
    for name, entry in sorted(config.all_queues().items()):
        if wanted is not None and name not in wanted:
            continue
        if not entry.get("model"):
            continue
        if match_m and config.canonical_model(config.engine(name), entry["model"]) != match_m:
            continue
        row = _row(name, entry, open_items.get(name, []))
        row["after"] = config.canonical_model(
            row["engine"], config._ccc_worker_model_default(row["engine"])
            or config.default_model(row["engine"])) or "(engine ambient default)"
        rows.append(row)
    return {"rows": rows}


def apply_unpin(plan: dict) -> List[str]:
    done = []
    for row in plan["rows"]:
        config.set_model(row["queue"], "")
        done.append(f"{row['queue']}: unpinned {row['model']} -> follows default ({row['after']})")
    return done


def format_rows(rows: List[dict], last_col: str) -> str:
    head = ["QUEUE", "ENGINE", "MODEL", "SOURCE", "DRAIN", "OPEN", "WIP", last_col.upper()]
    body = [[r["queue"], r["engine"], r["model"], r["source"],
             "on" if r["auto_drain"] else "off", str(r["open"]),
             str(r["in_progress"]), r[last_col]] for r in rows]
    widths = [max(len(x[i]) for x in [head] + body) for i in range(len(head))]
    return "\n".join("  ".join(c.ljust(w) for c, w in zip(line, widths)).rstrip()
                     for line in [head] + body)

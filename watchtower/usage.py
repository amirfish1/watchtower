"""Measured ticket usage. Never infer counters or actual models from prompts.

Claude input excludes cache reads/writes; Codex input includes cache reads.
Snapshots are observations, not bills: a close tool can run before its enclosing
turn's final usage event. Missing/partial telemetry remains explicit.
"""
from __future__ import annotations

import json
import uuid
import socket
import os
import hashlib
from collections import defaultdict
from pathlib import Path
from typing import Any

COUNTERS = ("input", "cache_read", "output", "cache_write")


def _count(value):
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 0 else None


def _subtract(a, b):
    return a - b if a is not None and b is not None and a >= b else None


def parse(path: str, engine: str) -> dict:
    """Return unique provider observations; malformed/truncated lines are ignored.

    Repeated Claude content blocks share a message id and are replacements,
    never additive. Codex cumulative totals are differenced exactly once, with
    each delta assigned to the model in the preceding turn_context.
    """
    units = {}
    model = None
    previous = {}
    started = False
    malformed = 0
    terminal = False
    try:
        with open(path, encoding="utf-8") as stream:
            for index, line in enumerate(stream):
                try:
                    row = json.loads(line)
                except (ValueError, UnicodeError):
                    malformed += 1
                    continue
                if not isinstance(row, dict):
                    continue
                if row.get("type") == "result":
                    terminal = True
                elif row.get("type") in ("user", "assistant"):
                    terminal = False
                if engine == "claude":
                    msg = row.get("message") or {}
                    if row.get("type") != "assistant" or not isinstance(msg, dict):
                        continue
                    raw = msg.get("usage")
                    mid = msg.get("id")
                    if not isinstance(raw, dict) or not mid:
                        continue
                    # requestId is provider provenance, message id is the
                    # deduplication identity across streaming content blocks.
                    unit = {"model": msg.get("model") or None,
                            "timestamp": row.get("timestamp"),
                            "input": _count(raw.get("input_tokens")),
                            "cache_read": _count(raw.get("cache_read_input_tokens")),
                            "output": _count(raw.get("output_tokens")),
                            "cache_write": _count(raw.get("cache_creation_input_tokens")),
                            "provider_usage": raw, "line": index + 1,
                            "request_id": row.get("requestId")}
                    old = units.get(str(mid))
                    if old:
                        # Later streaming blocks can carry fuller counters.
                        for key in COUNTERS:
                            values = [v for v in (old[key], unit[key]) if v is not None]
                            unit[key] = max(values) if values else None
                    units[str(mid)] = unit
                elif engine == "codex":
                    pl = row.get("payload") or {}
                    if not isinstance(pl, dict):
                        continue
                    if row.get("type") == "session_meta":
                        started = True
                    if pl.get("type") in ("task_complete", "turn_complete"):
                        terminal = True
                    if row.get("type") == "turn_context":
                        model = pl.get("model") or None
                        terminal = False
                    if row.get("type") != "event_msg" or pl.get("type") != "token_count":
                        continue
                    info = pl.get("info") or {}
                    raw = info.get("total_token_usage") if isinstance(info, dict) else None
                    if not isinstance(raw, dict):
                        continue
                    current = {key: _count(raw.get(field)) for key, field in
                               (("input", "input_tokens"), ("cache_read", "cached_input_tokens"),
                                ("output", "output_tokens"))}
                    if current == previous:
                        continue
                    delta = {key: _subtract(value, previous.get(key, 0 if started else None))
                             for key, value in current.items()}
                    delta["input"] = _subtract(delta["input"], delta["cache_read"])
                    delta["cache_write"] = None  # not exposed by Codex
                    # Stable across appends; duplicate token_count lines do not
                    # become new billable units. Keep raw cumulative provenance.
                    uid = json.dumps(current, sort_keys=True)
                    units[uid] = dict(delta, model=model, provider_usage=raw, line=index + 1)
                    previous = current
        return {"status": "observed" if units else "missing", "units": units,
                "path": path, "engine": engine, "malformed_lines": malformed, "terminal": terminal,
                "unsupported_counters": ["cache_write"] if engine == "codex" else [],
                "input_semantics": "includes_cache_read" if engine == "codex" else "excludes_cache_read_and_write"}
    except (OSError, UnicodeError) as exc:
        return {"status": "unavailable", "units": {}, "path": path,
                "engine": engine, "error": type(exc).__name__}


def observe(worker: str, sid: str = "") -> dict:
    """Resolve engine/session from the engine registry, never the requested tier."""
    from . import messages, workers
    rec = next((r for r in workers._load().get("workers", [])
                if r.get("worker_id") == worker or (sid and r.get("session_id") == sid)), {})
    if sid and rec.get("session_id") and rec["session_id"] != sid:
        # A fallback/rebound registry row may now describe a different process.
        # Do not parse the old session with the new attempt's provider semantics.
        rec = next((r for r in workers._load().get("workers", [])
                    if r.get("session_id") == sid), {})
    engine = str(rec.get("engine") or "")
    sid = sid or str(rec.get("session_id") or "")
    if not sid and rec.get("log"):
        sid = workers.resolve_session_id_from_log(str(rec["log"]))
    path = messages.locate_transcript(sid, engine) if sid else ""
    # Only Claude's stream log contains the same per-message usage as its
    # transcript. Plain output and result/modelUsage aggregates are not mixed.
    if not path and engine == "claude":
        path = str(rec.get("log") or "")
    if path and not engine:
        engine = "codex" if Path(path).name.startswith("rollout-") else "claude"
    snap = parse(path, engine) if path and engine in ("claude", "codex") else {
        "status": "unsupported" if engine and engine not in ("claude", "codex") else "missing",
        "units": {}, "engine": engine, "path": path}
    return dict(snap, session_id=sid, worker_id=worker,
                requested_model=rec.get("model") or None,
                process={k: rec.get(k) for k in ("pid", "pid_started", "exit_file")})


def begin(item: dict, role: str, worker: str, sid: str, at: str, *, run: str = "", attempt: int = 1,
          dedicated: bool = False) -> None:
    ledger = item.setdefault("token_usage", {"schema": 1, "attempts": []})
    snap = observe(worker, sid)
    if any(a["role"] == role and a["worker_id"] == worker and a["run"] == run
           and a["attempt"] == attempt and a["outcome"] == "running" for a in ledger["attempts"]):
        return
    identity = str(uuid.uuid4())
    prior = next((a for a in reversed(ledger["attempts"])
                  if (snap["session_id"] and a["session_id"] == snap["session_id"])
                  or a["worker_id"] == worker), None)
    baseline = prior["observed"] if dedicated and prior else (
        dict(snap, units={}) if dedicated else snap)
    ledger["attempts"].append({"id": identity, "role": role, "run": run or at,
        "attempt": attempt, "worker_id": worker, "session_id": snap["session_id"],
        "started_at": at, "outcome": "running", "dedicated": dedicated,
        "engine": snap.get("engine"), "machine": socket.gethostname(),
        "baseline": baseline,
        "observed": snap, "models": []})
    ledger["totals"] = totals(ledger["attempts"])


def finish(item: dict, role: str, at: str, outcome: str, worker: str = "") -> None:
    ledger = item.get("token_usage") or {}
    for attempt in ledger.get("attempts", []):
        if attempt["role"] != role or attempt["outcome"] != "running":
            continue
        if worker and attempt["worker_id"] != worker:
            continue
        snap = observe(attempt["worker_id"], attempt["session_id"])
        saved = attempt["observed"]
        if not snap.get("path") and saved.get("path"):
            snap = dict(parse(saved["path"], saved["engine"]),
                        session_id=saved["session_id"], worker_id=attempt["worker_id"],
                        requested_model=saved.get("requested_model"))
        if not any((snap.get("process") or {}).get(k) for k in ("pid", "exit_file")):
            snap["process"] = saved.get("process", {})
        baseline = attempt["baseline"]
        models = defaultdict(list)
        attributable = attempt["dedicated"] or (
            baseline["status"] in ("observed", "missing") and bool(baseline["path"])
            and baseline["session_id"] == snap["session_id"]
            and baseline["path"] == snap["path"])
        if attributable:
            for uid, unit in snap["units"].items():
                old = baseline["units"].get(uid)
                if old == unit:
                    continue
                delta = {k: _subtract(unit[k], old[k]) if old else unit[k] for k in COUNTERS}
                if old and any(value is not None for value in delta.values()) and all(
                        value == 0 for value in delta.values() if value is not None):
                    continue
                models[unit["model"]].append(dict(delta, unit_id=uid))
        attempt.update(ended_at=at, outcome=outcome, observed=snap,
                       session_id=snap.get("session_id") or attempt["session_id"],
                       engine=snap.get("engine") or attempt.get("engine"),
                       attribution="measured_interval" if attributable else "missing_baseline",
                       completeness="snapshot_at_transition", models=[
                           dict(model=model, counters=_sum(rows), units=rows)
                           for model, rows in models.items()])
    if ledger:
        ledger["totals"] = totals(ledger["attempts"])


def _sum(rows: list) -> dict:
    # Each field is independent: unknown cache writes do not erase known output.
    return {key: sum(row[key] for row in rows) if rows and all(row.get(key) is not None for row in rows)
            else None for key in COUNTERS}


def totals(attempts: list) -> dict:
    rows = []
    for attempt in attempts:
        models = attempt.get("models") or []
        rows.extend(m["counters"] for m in models)
        if not models or (attempt.get("backfill") and attempt.get("completeness") != "complete"):
            rows.append(dict.fromkeys(COUNTERS))
    return _sum(rows)


def on_event(item: dict, event: dict) -> None:
    """Called in the existing queue transaction; retries never add totals twice."""
    kind, at = event["event"], event["at"]
    actor = event.get("by") or {}
    if kind == "claim":
        finish(item, "worker", at, "replaced")
        begin(item, "worker", str(actor.get("worker") or item.get("claimed_by") or ""),
              str(actor.get("session_id") or item.get("claimed_session_id") or ""), at, run=f"claim:{len(item.get('history') or [])}",
              attempt=1 + sum(a["role"] == "worker" for a in (item.get("token_usage") or {}).get("attempts", [])))
    elif kind in ("close", "reopen", "release", "block"):
        finish(item, "worker", at, kind)
    elif kind == "stage_death":
        finish(item, event["role"], at, event.get("reason") or "failed")
    elif kind in ("plan", "plan_review", "verify", "assessment", "accept", "plan_failed", "plan_superseded"):
        roles = {"plan": ("planner",), "plan_review": ("plan_reviewer",),
                 "verify": ("verifier",), "assessment": ("assessor",),
                 "accept": ("verifier", "reviewer"),
                 "plan_failed": ("planner", "plan_reviewer"),
                 "plan_superseded": ("planner", "plan_reviewer")}[kind]
        for role in roles:
            finish(item, role, at, kind)


def track_stage(item: dict, previous: dict, current: dict, at: str) -> None:
    if not current.get("worker_id") or all(previous.get(k) == current.get(k)
                                             for k in ("worker_id", "key", "attempt")):
        return
    finish(item, str(previous.get("role") or ""), at, "superseded")
    begin(item, str(current.get("role") or "stage"), str(current["worker_id"]), "", at,
          run=str(current.get("key") or ""), attempt=int(current.get("attempt") or 0), dedicated=True)


def _location(item: dict):
    from . import queue
    base = queue._resolve_store_path()
    root = Path(os.environ.get("WATCHTOWER_USAGE_DIR") or (base.parent / "usage"))
    namespace = item.get("_usage_namespace") or f"local:{base}"
    key = hashlib.sha256(f"{namespace}:{item.get('ref')}".encode()).hexdigest()
    return root / (key + ".json")


def _read(path, *, strict=False):
    try:
        record = json.loads(path.read_text())
        if not isinstance(record, dict):
            raise ValueError("invalid usage ledger")
        return record
    except FileNotFoundError:
        return {}
    except (OSError, ValueError):
        if strict:
            raise
        return {}


def projection(ledger: dict) -> dict:
    """Bounded public view: no machine, session, transcript, request ids or raw usage."""
    attempts = []
    for a in ledger.get("attempts", [])[-32:]:
        attempts.append({k: a.get(k) for k in (
            "id", "role", "run", "attempt", "started_at", "ended_at", "outcome",
            "engine", "attribution", "completeness")})
        attempts[-1]["models"] = [{"model": m["model"], "counters": m["counters"]}
                                    for m in a.get("models", [])]
        attempts[-1]["telemetry_status"] = a.get("observed", {}).get("status", "missing")
        attempts[-1]["input_semantics"] = a.get("observed", {}).get("input_semantics")
        attempts[-1]["unsupported_counters"] = a.get("observed", {}).get("unsupported_counters", [])
    return {"schema": 1, "attempts": attempts, "attempt_count": len(ledger.get("attempts", [])),
            "totals": ledger.get("totals", dict.fromkeys(COUNTERS)),
            "measured_totals": {k: sum(m["counters"][k] for a in ledger.get("attempts", [])
                                      for m in a.get("models", []) if m["counters"].get(k) is not None)
                                if any(m["counters"].get(k) is not None for a in ledger.get("attempts", [])
                                       for m in a.get("models", [])) else None for k in COUNTERS},
            "errors": ledger.get("errors", [])[-4:],
            "completeness": "complete" if attempts and all(a.get("completeness") == "complete"
                                for a in ledger.get("attempts", [])) else "partial"}


def _write(path, record):
    from . import storage
    record = {k: v for k, v in record.items() if k in ("ref", "_usage_namespace", "token_usage")}
    encoded = json.dumps(record, separators=(",", ":"))
    # Hard bound on each private ticket ledger. Keep the safe summaries and
    # explicitly lose attribution capability rather than growing without limit.
    if len(encoded.encode()) > 8 * 1024 * 1024:
        for a in record["token_usage"]["attempts"]:
            a["baseline"] = dict(a["baseline"], units={})
            a["observed"] = dict(a["observed"], units={})
            for model in a.get("models", []):
                model["units"] = []
            a["attribution"] = "retention_limit"
            a["completeness"] = "partial"
        record["token_usage"].setdefault("errors", []).append({"error": "retention_limit"})
        record["token_usage"]["totals"] = dict.fromkeys(COUNTERS)
        encoded = json.dumps(record, separators=(",", ":"))
    if len(encoded.encode()) > 8 * 1024 * 1024:
        raise OSError("usage retention limit")
    with storage.atomic_destination(path) as temp:
        os.chmod(temp, 0o600)
        temp.write_text(encoded)


def attach(item: dict) -> dict:
    """Merge latest private projection at read time; never contacts a provider."""
    record = _read(_location(item))
    if record.get("token_usage"):
        item["token_usage"] = projection(record["token_usage"])
    return item


def capture(item: dict, callback, *args) -> None:
    from . import storage
    path = _location(item)
    # Non-usage events on untouched tickets have no telemetry side effects.
    if callback is on_event and not item.get("token_usage") and args[0]["event"] != "claim" and not path.exists():
        return
    if callback is track_stage and not args[1].get("worker_id") and not path.exists():
        return
    try:
        with storage.file_lock(path.with_suffix(".lock")):
            record = _read(path, strict=True)
            tracking = {"ref": item.get("ref"), "history": item.get("history", []),
                        "claimed_by": item.get("claimed_by"),
                        "claimed_session_id": item.get("claimed_session_id"),
                        "_usage_namespace": item.get("_usage_namespace")}
            if record.get("token_usage"):
                tracking["token_usage"] = record["token_usage"]
            callback(tracking, *args)
            if tracking.get("token_usage"):
                _write(path, tracking)
                item["token_usage"] = projection(tracking["token_usage"])
    except Exception as exc:  # Telemetry must never prevent a lifecycle write.
        item["token_usage"] = {"schema": 1, "attempts": [],
                               "totals": dict.fromkeys(COUNTERS),
                               "errors": [{"error": type(exc).__name__}], "completeness": "partial"}


def reconcile() -> None:
    """Re-read late telemetry idempotently. Only exclusive, ended sessions settle.

    A cumulative engine turn spanning several ticket claims cannot be split
    honestly. Preserve its provenance and mark it partial instead of estimating.
    No remote issue writes and no lifecycle changes occur here.
    """
    from . import queue, workers, storage
    root = Path(os.environ.get("WATCHTOWER_USAGE_DIR") or (queue._resolve_store_path().parent / "usage"))
    paths = list(root.glob("*.json")) if root.exists() else []
    owners = defaultdict(set)
    for path in paths:
        for a in _read(path).get("token_usage", {}).get("attempts", []):
            owners[a.get("session_id") or a["worker_id"]].add((str(path), a["id"]))
    for path in paths:
        with storage.file_lock(path.with_suffix(".lock")):
            record = _read(path)
            ledger = record.get("token_usage") or {}
            changed = False
            for a in ledger.get("attempts", []):
                if (a.get("backfill") or {}).get("finalization") == "frozen_historical_interval":
                    continue  # Historical bounds must never expand to later session usage.
                if a.get("outcome") == "running" or a.get("completeness") == "complete" or a.get("attribution") == "retention_limit":
                    continue
                key = a.get("session_id") or a["worker_id"]
                snap = observe(a["worker_id"], a.get("session_id") or "")
                saved = a["observed"]
                if not snap.get("path") and saved.get("path"):
                    snap = dict(parse(saved["path"], saved["engine"]), session_id=saved.get("session_id", ""),
                                worker_id=a["worker_id"], requested_model=saved.get("requested_model"))
                proc = saved.get("process") or {}
                exited = bool(workers.read_exit_file(str(proc.get("exit_file") or "")).get("ended_at"))
                if not exited and not snap.get("terminal"):
                    continue
                if len(owners[key]) != 1:
                    # Keep late raw observations private for later inspection.
                    a["late_observed"] = snap
                    a["completeness"] = "ambiguous_shared_session"
                    changed = True
                    continue
                original_outcome = a["outcome"]
                a["outcome"] = "running"
                # Recompute from the original baseline, replacing prior totals.
                # finish observes again; no additive billable entries are made.
                finish(record, a["role"], a["ended_at"], original_outcome, a["worker_id"])
                a["completeness"] = "complete" if a.get("models") and a.get("attribution") == "measured_interval" else "partial"
                changed = True
            if changed:
                _write(path, record)


def summary(item: dict) -> str:
    ledger = item.get("token_usage") or {}
    models = sorted({str(m["model"] or "?") for a in ledger.get("attempts", [])
                     for m in a.get("models", [])})
    c = ledger.get("measured_totals") or ledger.get("totals") or {}
    values = "/".join("?" if c.get(k) is None else str(c[k]) for k in ("input", "cache_read", "output"))
    return f"measured subtotal in/cache/out={values}; actual models={','.join(models) or '?'}; {ledger.get('completeness', 'partial')}"


def render(item: dict) -> str:
    ledger = item.get("token_usage") or {}
    def counters(c):
        return " / ".join("?" if c.get(k) is None else f"{c[k]:,}" for k in ("input", "cache_read", "output"))
    lines = [f"{item.get('ref', '?')} tokens (input / cache read / output)"]
    for a in ledger.get("attempts", []):
        prefix = f"  {a['role']} run={a['run']} attempt={a['attempt']} [{a['outcome']}; {a.get('completeness') or 'partial'}]"
        if not a.get("models"):
            lines.append(prefix + " actual model=?  ? / ? / ?")
        for model in a.get("models", []):
            lines.append(prefix + f" actual model={model['model'] or '?'}  " + counters(model["counters"]))
    lines.append("  measured subtotal: " + counters(ledger.get("measured_totals") or ledger.get("totals") or {}))
    lines.append("  status: " + ledger.get("completeness", "partial"))
    lines.append("  ? = unavailable; partial snapshots can omit late or unattributable usage.")
    return "\n".join(lines)


if __name__ == "__main__":
    import argparse
    from . import queue
    parser = argparse.ArgumentParser(description="Show measured ticket token usage")
    parser.add_argument("ref")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    reconcile()
    item = queue.get(args.ref)
    if item is None:
        parser.error("ticket not found")
    print(json.dumps(item.get("token_usage"), indent=2) if args.json else render(item))

"""Stage sessions owned by the reconciler daemon (WT-24).

The planner, plan reviewer, verifier and assessor are ad-hoc agent sessions
that a ticket's *state* calls for. Before WT-24 they were spawned as a side
effect of whichever build worker ran ``wt claim`` / ``wt close``, outside every
worker protection: a dead session left ``plan.status=planning`` or
``status=in_review`` forever.

Model:

* **Intent is the persisted ticket state** (``plan.status``, ``in_review`` +
  ``gate_pending=verify`` + ``verify_cycle``, ``assessment.status``).
  ``desired()`` derives the needed sessions from the store, so a lost wake or a
  down daemon loses nothing: the next tick of any daemon spawns from state.
* **CLI transitions only ``request()``**: append to the wake file, log
  STAGE_QUEUED, return. This module's ``request`` / ``request_assessment`` never
  reach ``spawn_adhoc`` / ``Popen`` (a test enforces it). Only the daemon tick
  (``reconcile_stages``) or an explicit ``wt stages tick`` spawns.
* **Supervision** (``item["stage_session"]``): per supervision key
  (``plan:r<round>``, ``review:r<round>``, ``verify:<cycle>``, ``assess:<cycle>``)
  two attempts. A dead / idle / over-age session is a death; the first death
  respawns once, the second escalates to ``needs_input`` with the reasons.
* **Launch failures**: a *classified* failure (auth, usage limit, ...) sets the
  shared ``(queue, engine)`` cooldown and is refunded (attempt not consumed,
  capped at 3 per key); an unclassified immediate death consumes an attempt and
  sets no cooldown.
* Every spawn and death is in the activity log: STAGE_QUEUED / STAGE_SPAWN /
  STAGE_DEAD / STAGE_WAIT / STAGE_DEFER / STAGE_BLOCK / STAGE_QUEUED, and
  ASSESS_RESUME for stale ``filing`` recovery.
"""

from __future__ import annotations

import json
import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from . import queue as q
from . import workers

ROLES = ("planner", "plan_reviewer", "verifier", "assessor")
MAX_ATTEMPTS = 2
MAX_REFUNDS = 3
_ADOPT_DEAD_AFTER_S = 30.0
_ASSESSOR_ROTATE_GAP_S = 60.0


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except (TypeError, ValueError):
        return float(default)


def idle_limit_s() -> float:
    return _env_float("WATCHTOWER_STAGE_IDLE_S", 1200)


def max_age_s() -> float:
    return _env_float("WATCHTOWER_STAGE_MAX_AGE_S", 5400)


def spawns_per_tick() -> int:
    return int(_env_float("WATCHTOWER_STAGE_SPAWNS_PER_TICK", 3))


def _now() -> float:
    return time.time()


def _iso(ts: Optional[float] = None) -> str:
    return datetime.fromtimestamp(ts if ts is not None else _now(), tz=timezone.utc
                                  ).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_iso(value: Any) -> float:
    try:
        return datetime.strptime(str(value), "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc).timestamp()
    except (TypeError, ValueError):
        return 0.0


def _log(verb: str, detail: str, queue: str = "") -> None:
    try:
        q._log(verb, detail, queue=queue)
    except Exception:  # noqa: BLE001 - logging never breaks supervision
        pass


# --------------------------------------------------------------------- request

def wake_path():
    return workers.WORKERS_FILE.parent / "stage-wake"


def daemon_running() -> bool:
    """Whether a ``wt start`` daemon's pidfile names a live process."""
    path = os.environ.get("WATCHTOWER_DAEMON_PID") or os.path.join(
        os.path.expanduser("~"), ".watchtower", "daemon.pid")
    try:
        pid = int(open(path).read().strip())
        os.kill(pid, 0)
        return True
    except (OSError, ValueError):
        return False


def _touch_wake(ref: str, why: str) -> None:
    import fcntl
    path = wake_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps({"ref": ref, "why": why, "at": _iso()}) + "\n"
    with open(path, "a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        f.write(line)
        f.flush()
    # A fresh mtime even when the filesystem's timestamp granularity is coarse.
    now = _now()
    try:
        os.utime(path, (now, max(now, os.stat(path).st_mtime + 0.001)))
    except OSError:
        pass


def request(ref: str, why: str = "") -> Dict[str, Any]:
    """A ticket transition wants a stage session: persist nothing here (the
    ticket state already is the intent), wake the daemon and log. Never spawns."""
    role = ""
    try:
        item = q.get(ref)
        if item:
            for d in desired([item]):
                role = d["role"]
                break
    except Exception:  # noqa: BLE001
        pass
    try:
        _touch_wake(ref, why)
    except OSError:
        pass
    watcher = daemon_running()
    _log("STAGE_QUEUED", f"{ref} {role or 'stage'} ({why})"
         + ("" if watcher else " (no watcher running: run 'wt start' or 'wt stages tick')"),
         queue=ref.rsplit("-", 1)[0])
    return {"ref": ref, "role": role, "watcher": watcher}


def request_assessment(ref: str, force: bool = False) -> Optional[Dict[str, Any]]:
    """Queue a (forced) assessment cycle: one store transaction that bumps the
    cycle (fencing the old token / assessor, abandoning a half-filed one) and
    resets supervision; then wake the daemon. Never spawns."""
    def _do(it, a, data):
        if it.get("status") != "closed":
            return "skip"
        st = a.get("status", "")
        if not a:
            a.update(status="due", attempt=0, token="", cycle=1)
        elif st == "due" and not force:
            return "skip"
        elif st in ("due", "running", "filing", "failed", "done") and (force or st == "failed"):
            if st == "filing":
                done = [{"attempt": a.get("attempt", 0), "ref": o.get("result_ref"),
                         "kind": o.get("kind")}
                        for o in (a.get("pending") or {}).get("ops", [])
                        if o.get("result_ref") and o.get("outcome") == "filed"]
                a["abandoned"] = list(a.get("abandoned") or []) + done
            a.pop("pending", None)
            q._assessment_new_cycle(a, "forced run" if force else "retry")
        else:
            return "skip"
        it["stage_session"] = {"key": f"assess:{a.get('cycle', 1)}", "role": "assessor",
                               "attempt": 0, "escalated": False,
                               "deaths": list((it.get("stage_session") or {}).get("deaths") or [])[-10:]}
    item = q._assessment_update(ref, _do)
    if item is None:
        return None
    request(item["ref"], "assess run")
    return item


# --------------------------------------------------------------------- desired

def _github_backed(item: Dict[str, Any]) -> bool:
    try:
        return q._github_backend_for_project(str(item.get("project") or "")) is not None
    except Exception:  # noqa: BLE001
        return False


def desired(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """The stage sessions the stored ticket states call for: one entry
    ``{ref, role, key, project}`` per ticket at most."""
    out: List[Dict[str, Any]] = []
    for it in items:
        ref = str(it.get("ref") or "")
        if not ref:
            continue
        if (it.get("pending_answer") or {}).get("state") in q.ANSWER_INFLIGHT:
            # WT-31 D1a: an answer in flight owns the ticket until it settles
            # (row answer.* / answer.plan_conflict); no stage session meanwhile.
            continue
        project = str(it.get("project") or "")
        status = str(it.get("status") or "")
        plan = it.get("plan") or {}
        entry: Optional[Tuple[str, str]] = None
        if (status == "in_review" and it.get("gate_pending") == "verify"
                and not it.get("needs_input")):
            entry = ("verifier", f"verify:{int(it.get('verify_cycle') or 0)}")
        elif status == "closed" and (it.get("assessment") or {}).get("status") in ("due", "running"):
            if not _github_backed(it):
                entry = ("assessor", f"assess:{int((it.get('assessment') or {}).get('cycle') or 1)}")
        elif (status in ("open", "in_progress") and q.plan_gate(it) is not None
                and not it.get("needs_input") and not _github_backed(it)):
            pst = str(plan.get("status") or "")
            rnd = int(plan.get("round") or 1)
            if pst == "":
                if status == "open" or it.get("claimed_by"):
                    entry = ("planner", "plan:r1")
            elif pst == "planning":
                entry = ("planner", f"plan:r{rnd}")
            elif pst == "reviewing" and (plan.get("discussion") or {}).get("status") != "active":
                entry = ("plan_reviewer", f"review:r{rnd}")
            else:
                # WT-29: each planner<->reviewer discussion turn is its own
                # supervised session (own key, own attempt budget).
                disc = plan.get("discussion") or {}
                if disc.get("status") == "active":
                    if pst == "discussing" and disc.get("awaiting") == "planner":
                        entry = ("planner", f"discuss:r{rnd}:d{int(disc.get('round') or 1)}:planner")
                    elif pst == "reviewing" and disc.get("awaiting") == "reviewer":
                        entry = ("plan_reviewer",
                                 f"discuss:r{rnd}:v{int(plan.get('version') or rnd)}:reviewer")
        if entry:
            out.append({"ref": ref, "role": entry[0], "key": entry[1], "project": project})
    return out


# ------------------------------------------------------------- role metadata

def _legacy_meta(item: Dict[str, Any], role: str) -> Dict[str, Any]:
    if role == "planner":
        return dict((item.get("plan") or {}).get("planner") or {})
    if role == "plan_reviewer":
        return dict((item.get("plan") or {}).get("reviewer") or {})
    if role == "verifier":
        return dict(item.get("verifier") or {})
    return dict((item.get("assessment") or {}).get("assessor") or {})


# ---------------------------------------------------------------- liveness

def _read_exit(path: str) -> Dict[str, Any]:
    try:
        with open(path) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _activity_mtime(rec: Dict[str, Any]) -> float:
    """Last sign of life. claude -p (text mode) writes NOTHING to its log until
    it exits, so claude sessions use the transcript mtime; other engines (and a
    claude session whose transcript is not found) use the log mtime."""
    mt = 0.0
    sid = str(rec.get("session_id") or "")
    if str(rec.get("engine") or "") == "claude" and sid:
        try:
            from . import messages
            tp = messages.locate_transcript(sid, "claude")
            if tp:
                mt = max(mt, os.stat(tp).st_mtime)
        except Exception:  # noqa: BLE001
            pass
    if not mt:
        try:
            mt = os.stat(str(rec.get("log") or "")).st_mtime
        except OSError:
            pass
    return mt


def _record_for(worker_id: str, rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for w in rows:
        if str(w.get("worker_id") or "") == worker_id:
            return w
    return None


def _log_facts(rec: Optional[Dict[str, Any]]) -> str:
    if not rec or not rec.get("log"):
        return ""
    try:
        size = os.path.getsize(str(rec["log"]))
    except OSError:
        return ""
    tail = workers._last_log_line(Path(str(rec["log"]))) if size else ""
    return f"{size}-byte log" + (f": {tail}" if tail else "")


def _check_session(item: Dict[str, Any], ss: Dict[str, Any],
                   rows: List[Dict[str, Any]], adopted: bool
                   ) -> Tuple[str, str, Dict[str, Any]]:
    """('alive'|'dead', reason, extra). ``extra`` may carry rc/signal."""
    wid = str(ss.get("worker_id") or "")
    rec = _record_for(wid, rows) if wid else None
    now = _now()
    if rec is None:
        ref_ts = _parse_iso(ss.get("spawned_at")) or _parse_iso(item.get("updated_at"))
        if wid and (now - ref_ts) < _ADOPT_DEAD_AFTER_S:
            return "alive", "", {}
        return "dead", ("worker record gone (pre-supervision spawn)" if adopted
                        else "worker record gone"), {}
    exit_info = _read_exit(str(rec.get("exit_file") or ss.get("exit_file") or ""))
    if exit_info.get("ended_at"):
        rc, sig = exit_info.get("rc"), int(exit_info.get("signal") or 0)
        why = (f"killed by signal {sig}" if sig else
               "exited without submitting" if not rc else f"exited rc {rc}")
        facts = _log_facts(rec)
        return "dead", why + (f"; {facts}" if facts else ""), {"rc": rc, "signal": sig}
    if not rec.get("alive"):
        why = "process gone" + (" (wrapper missing ended_at: killed along with it)"
                                if exit_info else "")
        facts = _log_facts(rec)
        return "dead", why + (f"; {facts}" if facts else ""), {}
    token = str(rec.get("pid_started") or "")
    if token:
        cur = workers._pid_start_token(int(rec.get("pid") or 0))
        if cur and cur != token:
            return "dead", "pid reused (start token mismatch)", {}
    spawned = _parse_iso(rec.get("started_at")) or _parse_iso(ss.get("spawned_at"))
    age = now - spawned if spawned else 0
    if max_age_s() > 0 and spawned and age >= max_age_s():
        _kill(int(rec.get("pid") or 0))
        return "dead", f"no progress (age {int(age)}s)", {"killed": True}
    last = max(_activity_mtime(rec), spawned)
    idle = now - last if last else 0
    if idle_limit_s() > 0 and last and idle >= idle_limit_s():
        _kill(int(rec.get("pid") or 0))
        return "dead", f"no progress (idle {int(idle)}s)", {"killed": True}
    return "alive", "", {}


def session_alive(worker_id: str) -> bool:
    """True when the worker's process is running right now: a worker record,
    a live pid with the matching start token, and no ``ended_at`` in its exit
    file. The same facts ``_check_session`` uses, minus the age/idle limits."""
    if not worker_id:
        return False
    try:
        rec = _record_for(worker_id, workers.list_workers(prune=False))
    except Exception:  # noqa: BLE001
        return False
    if rec is None or not rec.get("alive"):
        return False
    if _read_exit(str(rec.get("exit_file") or "")).get("ended_at"):
        return False
    token = str(rec.get("pid_started") or "")
    if token:
        cur = workers._pid_start_token(int(rec.get("pid") or 0))
        if cur and cur != token:
            return False
    return True


def _kill(pid: int) -> None:
    if not pid:
        return
    try:
        os.killpg(pid, signal.SIGTERM)
    except (ProcessLookupError, PermissionError, OSError):
        return
    deadline = _now() + 5.0
    while _now() < deadline:
        if not workers._pid_alive(pid):
            return
        time.sleep(0.1)
    try:
        os.killpg(pid, signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError):
        pass


# ------------------------------------------------------------------- targets

def _role_target(item: Dict[str, Any], role: str) -> Dict[str, Any]:
    from . import config as _config, roles
    if role == "verifier":
        return q.verifier_target(item)
    if role == "assessor":
        return q.assessor_target(item)
    ticket = dict(item)
    gate_model = q.plan_gate(item)
    if role == "planner" and gate_model:
        ticket["planner_model"] = gate_model
    eng, mdl, source = roles.effective_role_model(item.get("project", ""), ticket, role)
    return {"engine": eng, "model": mdl, "source": source,
            "blocked": bool(mdl and _config.is_blocked_model(mdl))}


def _goal(item: Dict[str, Any], role: str, *, token: str, respawn: bool,
          note: str, key: str = "") -> str:
    from . import cli
    if key.startswith("discuss:") and role in ("planner", "plan_reviewer"):
        goal = cli._plan_discussion_goal(item, "planner" if role == "planner" else "reviewer")
    elif role == "planner":
        plan = item.get("plan") or {}
        reviews = plan.get("reviews") or []
        feedback = ""
        if int(plan.get("round") or 1) > 1 and reviews:
            feedback = str(reviews[-1].get("reasons") or "") or "see the review history"
        goal = cli._plan_goal(item, feedback=feedback)
    elif role == "plan_reviewer":
        goal = cli._plan_review_goal(item)
    elif role == "verifier":
        goal = cli._verifier_goal(item)
    else:
        goal = workers.assessor_goal(item, token)
    if respawn:
        goal += (f"\n\nNote: a previous {role.replace('_', ' ')} session for this stage "
                 f"died without finishing; start fresh.")
    if note:
        goal += f"\n\nHuman note: {note}"
    return goal


# -------------------------------------------------------------------- spawn

class _Budget:
    def __init__(self, spawns: int):
        self.spawns = spawns
        self.acted: List[Tuple[str, str]] = []


def _ss_set(ref: str, **fields: Any) -> Optional[Dict[str, Any]]:
    def _do(it, ss):
        ss.update(fields)
    return q.stage_session_update(ref, _do)


def _record_death(ref: str, project: str, role: str, key: str, reason: str,
                  extra: Dict[str, Any], rec: Optional[Dict[str, Any]],
                  refunded: bool = False, unspawned: bool = False) -> Dict[str, Any]:
    captured: Dict[str, Any] = {}

    def _do(it, ss):
        attempt_no = int(ss.get("attempt") or 0)
        death = {"key": key, "attempt": attempt_no, "worker_id": ss.get("worker_id", ""),
                 "at": _iso(), "reason": reason[:500], "rc": extra.get("rc"),
                 "signal": extra.get("signal"), "log": (rec or {}).get("log", "")}
        if refunded:
            death["refunded"] = True
            ss["refunds"] = int(ss.get("refunds") or 0) + 1
            if not unspawned:   # a spawn that never started did not bump the attempt
                ss["attempt"] = max(0, attempt_no - 1)
        ss["deaths"] = (list(ss.get("deaths") or []) + [death])[-10:]
        ss["worker_id"] = ""
        ss["dead_reason"] = reason[:500]
        captured.update(death)
        q._append_history(it, "stage_death", by=q._by("system"), at=q._now_iso(),
                          role=role, key=key, reason=reason[:500], attempt=attempt_no)
    q.stage_session_update(ref, _do)
    _log("STAGE_DEAD", f"{ref} {role} {captured.get('worker_id') or '-'} "
         f"{reason} attempt {captured.get('attempt')}/{MAX_ATTEMPTS}"
         + (" (refunded)" if refunded else ""), queue=project)
    return captured


def _escalate(item: Dict[str, Any], role: str, key: str, ss: Dict[str, Any]) -> None:
    ref, project = str(item["ref"]), str(item.get("project") or "")
    deaths = [d for d in (ss.get("deaths") or []) if d.get("key") == key]
    reasons = "; ".join(f"attempt {d.get('attempt')}: {d.get('reason')}" for d in deaths[-2:]) \
        or str(ss.get("dead_reason") or "unknown")
    logs = ", ".join(str(d.get("log")) for d in deaths[-2:] if d.get("log"))
    exits = {
        "planner": f"`wt answer {ref} \"retry\"` to retry, or `wt plan decide {ref} --accept` / `--retry`",
        "plan_reviewer": f"`wt answer {ref} \"retry\"` to retry, or `wt plan decide {ref} --accept` / `--retry`",
        "verifier": f"`wt answer {ref} \"retry\"`, `wt verdict {ref} --pass|--fail`, or `wt accept {ref} --force`",
        "assessor": f"`wt answer {ref} \"retry\"` or `wt assess run {ref}`",
    }[role]
    question = (f"The {role.replace('_', ' ')} {'discussion turn' if key.startswith('discuss:') else 'stage'} ({key}) died twice: {reasons}."
                + (f" Logs: {logs}." if logs else "") + f" Decide: {exits}.")
    plan_status = (item.get("plan") or {}).get("status", "")

    def _do(it, s):
        s["escalated"] = True
        s["escalated_from"] = plan_status
        q._append_history(it, "stage_escalated", by=q._by("system"), at=q._now_iso(),
                          role=role, key=key, reason=reasons[:500])
        if role in ("planner", "plan_reviewer"):
            plan = dict(it.get("plan") or {})
            plan.update(status="blocked", escalated="stage_watch")
            it["plan"] = plan
    q.stage_session_update(ref, _do)
    if role == "assessor":
        a = item.get("assessment") or {}
        q.assessment_fail(ref, a.get("token", ""), f"assessor died twice: {reasons}")
    q.block(ref, "", origin="stage", question=question, kind="input")
    _log("STAGE_BLOCK", f"{ref} {role} {key}: {reasons}", queue=project)


def _spawn(item: Dict[str, Any], role: str, key: str, ss: Dict[str, Any],
           cause: str, budget: _Budget) -> str:
    """Spawn one stage session. Returns a short action tag; never raises."""
    ref, project = str(item["ref"]), str(item.get("project") or "")
    attempt = int(ss.get("attempt") or 0) + 1
    engine = ""
    try:
        target = _role_target(item, role)
        if target["blocked"]:
            raise ValueError(f"model {target['model']!r} for the {role} ({target['engine']}) "
                             f"is blocked by model policy; not substituting another model")
        engine = target["engine"]
        cooldown = workers.active_launch_failure_cooldown(project, engine)
        if cooldown:
            until = cooldown.get("cooldown_until")
            if ss.get("waiting_until") != until:
                _ss_set(ref, waiting_until=until)
                _log("STAGE_WAIT", f"{ref} {role} {key}: {engine} launch cooldown until "
                     f"{cooldown.get('cooldown_until_human', '')} ({cooldown.get('reason', '')})",
                     queue=project)
            return "wait"
        # ticket repo_path, else the queue's repo; never the daemon's cwd
        repo = workers.assessment_repo(item)
        token = ""
        if role == "assessor":
            a = item.get("assessment") or {}
            if a.get("status") == "due":
                token = q.assessment_reserve(ref) or ""
            elif a.get("status") == "running":
                token = q.assessment_rotate(ref, str(a.get("token") or "")) or ""
            if not token:
                return "skip"
            item = q.get(ref) or item
        goal = _goal(item, role, token=token, respawn=cause in ("respawn", "adopted"),
                     note=str(ss.get("retry_note") or ""), key=key)
        plan_now = item.get("plan") or {}
        name = {"planner": f"plan-{ref}-r{plan_now.get('round', 1)}",
                "plan_reviewer": f"plan-review-{ref}",
                "verifier": f"verify-{ref}", "assessor": f"assess-{ref}"}[role]
        if key.startswith("discuss:"):
            name = (f"plan-{ref}-r{plan_now.get('round', 1)}-d"
                    f"{(plan_now.get('discussion') or {}).get('round', 1)}"
                    if role == "planner"
                    else f"plan-review-{ref}-v{plan_now.get('version', 1)}")
        rec = workers.spawn_adhoc(goal, engine, model=target["model"], repo_path=repo,
                                  name=name, report_to="", verify=(role == "verifier"),
                                  stage=role, ticket_ref=ref, ticket_queue=project)
    except Exception as exc:  # noqa: BLE001
        return _spawn_error(item, role, key, ss, exc, engine=engine)
    info = {"engine": engine, "model": target["model"], "source": target["source"],
            "worker_id": rec.get("worker_id", ""), "stage_key": key}
    if role == "planner":
        q.plan_set_role(ref, "planner", info)
    elif role == "plan_reviewer":
        q.plan_set_role(ref, "reviewer", info)
    elif role == "verifier":
        q.set_verifier_info(ref, info)
    else:
        q.assessment_set_running(ref, token, info)

    def _do(it, s):
        s.update(key=key, role=role, attempt=attempt, worker_id=rec.get("worker_id", ""),
                 pid=rec.get("pid"), pid_started=rec.get("pid_started", ""), engine=engine,
                 model=target["model"], log=rec.get("log", ""),
                 exit_file=rec.get("exit_file", ""), spawned_at=_iso(), escalated=False,
                 waiting_until=None)
        s.pop("retry_note", None)
        s.pop("retry_at", None)
        q._append_history(it, "stage_spawn", by=q._by("system"), at=q._now_iso(),
                          role=role, key=key, attempt=attempt, cause=cause,
                          worker_id=rec.get("worker_id", ""))
    q.stage_session_update(ref, _do)
    budget.spawns -= 1
    _log("STAGE_SPAWN", f"{ref} {role} {rec.get('worker_id', '?')} (pid {rec.get('pid')}) "
         f"[{engine}{':' + target['model'] if target['model'] else ''}] attempt "
         f"{attempt}/{MAX_ATTEMPTS} cause={cause}", queue=project)
    failure = rec.get("launch_failure")
    if failure:
        classified = bool(failure.get("classified"))
        refunded = classified and int(ss.get("refunds") or 0) < MAX_REFUNDS
        _record_death(ref, project, role, key,
                      str(failure.get("reason") or "launch failed"),
                      {"rc": failure.get("exit_code")}, rec, refunded=refunded)
        return "launch_failed"
    return "spawned"


def _spawn_error(item: Dict[str, Any], role: str, key: str, ss: Dict[str, Any],
                 exc: Exception, engine: str = "") -> str:
    """A spawn that could not start: blocked model / missing repo / engine. The
    pre-WT-24 semantics: plan stage fails open; verifier and assessor need a human.
    A missing engine CLI is the exception: it is an engine-missing launch failure
    (shared cooldown + refunded death), so the plan gate is not released."""
    ref, project = str(item["ref"]), str(item.get("project") or "")
    _log("STAGE_DEAD", f"{ref} {role} - could not spawn: {exc}", queue=project)
    if engine and "CLI not found" in str(exc):
        reason = f"engine CLI missing: {exc}"[:300]
        workers._record_launch_failure(
            queue=project, engine=engine, worker_id="", pid=0, log_path=Path(os.devnull),
            reason=reason, exit_code=None)
        refunded = int(ss.get("refunds") or 0) < MAX_REFUNDS
        _record_death(ref, project, role, str(key), reason, {"rc": None}, None,
                      refunded=refunded, unspawned=True)
        return "launch_failed"
    if role in ("planner", "plan_reviewer"):
        q.plan_fail(ref, f"could not spawn the {role}: {exc}")
        return "failed"
    if role == "assessor":
        a = item.get("assessment") or {}
        q.assessment_fail(ref, a.get("token", ""), f"could not spawn the assessor: {exc}")
        q.block(ref, "", origin="stage", question=f"Could not spawn the assessor: {exc}. Fix it, then "
                                   f"`wt assess run {ref}`.", kind="input")
        return "failed"
    q.block(ref, "", origin="stage", question=f"Could not spawn the verifier: {exc}. File a verdict with "
                              f"`wt verdict {ref}` or `wt accept {ref} --force`.", kind="input")
    _log("STAGE_BLOCK", f"{ref} {role} {key}: {exc}", queue=project)
    return "failed"


# --------------------------------------------------------------------- tick

def _supervise(d: Dict[str, Any], rows: List[Dict[str, Any]], budget: _Budget) -> None:
    ref, role, key = d["ref"], d["role"], d["key"]
    item = q.get(ref)
    if not item:
        return
    still = [x for x in desired([item]) if x["role"] == role and x["key"] == key]
    if not still:
        return
    project = str(item.get("project") or "")
    ss = dict(item.get("stage_session") or {})
    ss_old = dict(ss)
    adopted = False
    if ss.get("escalated") and ss.get("key") == key:
        return  # waiting for a human (needs_input is set)
    if plan_no_start(item, role):
        # First touch of a plan-gated ticket: plan_start, then the planner.
        started = q.plan_start(ref)
        if started is None:
            return
        item = q.get(ref) or item
    if ss.get("key") != key:
        legacy = _legacy_meta(item, role)
        if (not ss and legacy.get("worker_id")
                and (not legacy.get("stage_key") or legacy.get("stage_key") == key)):
            adopted = True
            ss = {"key": key, "role": role, "attempt": 1, "worker_id": legacy["worker_id"],
                  "engine": legacy.get("engine", ""), "model": legacy.get("model", ""),
                  "spawned_at": "", "escalated": False, "deaths": []}
            _ss_set(ref, **ss)
            _log("STAGE_SPAWN", f"{ref} {role} {legacy['worker_id']} (legacy) attempt 1/"
                 f"{MAX_ATTEMPTS} cause=adopted", queue=project)
        else:
            ss = {"key": key, "role": role, "attempt": 0, "worker_id": "", "escalated": False,
                  "deaths": list(ss.get("deaths") or [])[-10:], "refunds": 0}
            if key.startswith("discuss:"):
                # WT-29: a participant that is still running is adopted (no
                # message, no budget); otherwise a fresh turn spawns this tick.
                prior = _legacy_meta(item, role)
                pwid = str(prior.get("worker_id") or "")
                if pwid and session_alive(pwid):
                    prec = _record_for(pwid, workers.list_workers(prune=False)) or {}
                    ss.update(worker_id=pwid, pid=prec.get("pid"),
                              pid_started=prec.get("pid_started", ""),
                              exit_file=prec.get("exit_file", ""), log=prec.get("log", ""),
                              engine=prior.get("engine", ""), model=prior.get("model", ""),
                              spawned_at=str(prec.get("started_at") or _iso()))
                    _log("STAGE_SPAWN", f"{ref} {role} {pwid} (live) attempt 0/"
                         f"{MAX_ATTEMPTS} cause=adopted_prior", queue=project)
            for carry in ("retry_note", "retry_at"):   # a human retry moved the key (assess:N+1)
                if ss_old.get(carry):
                    ss[carry] = ss_old[carry]
            _ss_set(ref, **ss)
    elif not ss.get("role"):
        ss["role"] = role

    if ss.get("worker_id"):
        state, reason, extra = _check_session(item, ss, rows, adopted or ss.get("spawned_at") == "")
        if state == "alive":
            return
        death = _record_death(ref, project, role, key, reason, extra,
                              _record_for(str(ss.get("worker_id")), rows))
        ss = dict((q.get(ref) or item).get("stage_session") or ss)
        if int(ss.get("attempt") or 0) >= MAX_ATTEMPTS:
            _escalate(q.get(ref) or item, role, key, ss)
            budget.acted.append((ref, "blocked"))
            return
        if (role == "assessor" and (q.get(ref) or {}).get("assessment", {}).get("status")
                not in ("running", "due")):
            return  # the ticket moved on (submitted / forced): not a death to act on
        cause = "respawn"
    elif role == "assessor" and int(ss.get("attempt") or 0) > 0 and ss.get("escalated") is False \
            and (item.get("assessment") or {}).get("status") == "running" \
            and not (item.get("assessment") or {}).get("assessor") \
            and _now() - _parse_iso((item.get("assessment") or {}).get("reserved_at")) \
            > _ASSESSOR_ROTATE_GAP_S and not ss.get("dead_reason_seen"):
        # Crash between rotate and set_running: the lost spawn is a death.
        _record_death(ref, project, role, key, "spawn interrupted (no assessor recorded)",
                      {}, None)
        ss = dict((q.get(ref) or item).get("stage_session") or ss)
        if int(ss.get("attempt") or 0) >= MAX_ATTEMPTS:
            _escalate(q.get(ref) or item, role, key, ss)
            budget.acted.append((ref, "blocked"))
            return
        cause = "respawn"
    else:
        last = (ss.get("deaths") or [{}])[-1]
        if (int(ss.get("attempt") or 0) >= MAX_ATTEMPTS and last.get("key") == key
                and not ss.get("retry_at")):
            # The last attempt died at launch (no worker left to check): out of budget.
            _escalate(q.get(ref) or item, role, key, ss)
            budget.acted.append((ref, "blocked"))
            return
        cause = "retry" if ss.get("retry_at") else (
            "respawn" if ss.get("deaths") and ss.get("attempt") else "initial")
    if budget.spawns <= 0:
        if not ss.get("deferred_key") == key:
            _ss_set(ref, deferred_key=key)
            _log("STAGE_DEFER", f"{ref} {role} {key}: per-tick spawn cap reached", queue=project)
        return
    item = q.get(ref) or item
    tag = _spawn(item, role, key, ss, cause, budget)
    budget.acted.append((ref, tag))


def plan_no_start(item: Dict[str, Any], role: str) -> bool:
    return role == "planner" and not (item.get("plan") or {}).get("status")


def _filing_maintenance(budget: _Budget, only_ref: str) -> None:
    for it in q.assessment_targets_for_sweep()["filing"]:
        if only_ref and it["ref"] != only_ref:
            continue
        try:
            q.assessment_run_ops(it["ref"])
            _log("ASSESS_RESUME", str(it["ref"]), queue=str(it.get("project") or ""))
            budget.acted.append((it["ref"], "resumed"))
        except Exception:  # noqa: BLE001
            pass


def reconcile_stages(only_ref: str = "") -> List[Tuple[str, str]]:
    """One supervision pass (daemon tick, ``wt stages tick``). Returns
    ``(ref, action)`` pairs: spawned / respawn-related / blocked / resumed / wait."""
    budget = _Budget(spawns_per_tick())
    try:
        _filing_maintenance(budget, only_ref)
    except Exception:  # noqa: BLE001
        pass
    items = q.list_items() or []
    try:
        from . import cli
        for project in sorted({str(i.get("project") or "") for i in items
                               if (i.get("plan") or {}).get("discussion", {}).get("status") == "active"
                               and (not only_ref or i.get("ref") == only_ref)}):
            cli.recover_plan_discussions(project)
    except Exception:  # noqa: BLE001
        pass
    targets = [d for d in desired(items) if not only_ref or d["ref"] == only_ref]
    if not targets:
        return budget.acted
    rows = workers.list_workers(prune=True)
    for d in targets:
        try:
            _supervise(d, rows, budget)
            rows = workers.list_workers(prune=False) if budget.acted else rows
        except Exception as exc:  # noqa: BLE001 - one ticket never stops the pass
            _log("STAGE_DEAD", f"{d['ref']} {d['role']} supervision error: {exc}",
                 queue=d.get("project", ""))
    return budget.acted


def sleep_until_wake(interval: float, sleep: Callable[[float], None] = time.sleep) -> bool:
    """Sleep up to ``interval`` in 0.5 s chunks; return True early when the wake
    file's mtime advances (a CLI transition queued a stage)."""
    path = wake_path()

    def mt() -> float:
        try:
            return os.stat(path).st_mtime
        except OSError:
            return 0.0

    base = mt()
    remaining = float(interval)
    while remaining > 0:
        chunk = min(0.5, remaining)
        sleep(chunk)
        remaining -= chunk
        if mt() > base:
            return True
    return False

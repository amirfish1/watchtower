"""Verified, per-decision outcomes; historical undo text is never executed."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

from . import queue as q
from .refs import display_ref


def record_outcome(ref, decision_id, event, **fields):
    """Persist idempotently on the owning backend and require fresh readback."""
    if event not in ("auto_decision_reversed", "auto_decision_consumed", "auto_decision_cancelled"):
        raise ValueError("unsupported decision outcome")

    def append(item):
        decisions = [h for h in item.get("history", [])
                     if h.get("event") == "auto_decision" and h.get("decision_id") == decision_id]
        if len(decisions) != 1:
            raise ValueError("one actual persisted decision is required")
        expected_class = "code-change-revertable" if event == "auto_decision_reversed" else "capped-replay-spend"
        if decisions[0].get("class") != expected_class or decisions[0].get("by", {}).get("kind") != "system":
            raise ValueError("outcome does not match an actual system approval of this class")
        matches = [h for h in item.get("history", [])
                   if h.get("event") == event and h.get("decision_id") == decision_id]
        if matches:
            if len(matches) != 1 or any(matches[0].get(k) != v for k, v in fields.items() if k != "worker"):
                raise ValueError("conflicting decision outcome")
            # Retrying in a replacement session retains the original actor.
            fields["worker"] = matches[0].get("worker", "")
            return
        q._append_history(item, event, by=q._by("system"), at=q._now_iso(),
                          decision_id=decision_id, **fields)

    item = q.get(ref)
    if not item:
        raise ValueError("ticket not found")
    # Validate before persistence; the backend also enforces idempotence.
    append(item)
    backend = q._github_backend_for_project(item["project"])
    if backend is not None:
        # Store on GitHub even when an approval lives in a host-owned sidecar.
        # A sidecar-only outcome would disappear to readers on another host.
        try:
            result = backend.update(ref, _system_history=(event, dict(decision_id=decision_id, **fields)))
        except Exception as exc:
            raise ValueError("decision outcome NOT persisted; retry with the same evidence") from exc
        if result is None:
            raise ValueError("decision outcome NOT persisted; retry with the same evidence")
    else:
        with q._FileLock(q._lock_path()):
            data = q._load_unlocked()
            current = next((it for it in data["items"] if it.get("ref") == ref), None)
            if current is None:
                raise ValueError("ticket disappeared before outcome persistence")
            append(current)
            q._save_unlocked(data)
    fresh = q.get(ref) or {}
    matches = [h for h in fresh.get("history", [])
               if h.get("event") == event and h.get("decision_id") == decision_id
               and all(h.get(k) == v for k, v in fields.items())]
    if len(matches) != 1:
        raise ValueError("decision outcome readback failed; success NOT reported")
    return fresh


def consumed_proof(path, ref, decision_id):
    path = Path(path).resolve()
    raw = path.read_bytes()
    state = json.loads(raw)
    if not isinstance(state, dict):
        raise ValueError("replay evidence must be a JSON object")
    count = state.get("forwarded", state.get("httpAttempts"))
    cap = state.get("cap", state.get("capRequests"))
    if type(count) is not int or type(cap) is not int or not 0 < count <= cap:
        raise ValueError("evidence must show positive consumed requests within its cap")
    if state.get("decision_id") and state["decision_id"] != decision_id:
        raise ValueError("evidence belongs to a different decision")
    if state.get("ticket") != ref and state.get("approval") != f"WatchTower capped-replay-spend for {ref}":
        if not str(state.get("decision_id") or "").startswith(ref + "@"):
            raise ValueError("evidence does not identify this ticket")
    return dict(path=str(path), sha256=hashlib.sha256(raw).hexdigest(), requests=count,
                cap=cap, terminal=state.get("terminal"), association="operator-selected")


def _revert(item, event, worker):
    from . import config
    sha = str(event.get("commit") or "")
    if event.get("class") != "code-change-revertable" or not sha:
        raise ValueError("unsupported decision class; replay outcomes require --consumed or --cancel")
    repo = str(item.get("repo_path") or config.repo_path(item.get("project", "")) or os.getcwd())

    def git(*args):
        return subprocess.run(["git", "-C", repo, *args], capture_output=True, text=True)

    if git("cat-file", "-e", f"{sha}^{{commit}}").returncode:
        raise ValueError(f"commit {sha[:10]} not found in {repo}")
    if git("status", "--porcelain").stdout.strip():
        raise ValueError("git worktree is dirty; no revert attempted")
    before = git("rev-parse", "HEAD").stdout.strip()
    result = git("revert", "--no-edit", sha)
    after = git("rev-parse", "HEAD").stdout.strip()
    if result.returncode or not after or after == before:
        git("revert", "--abort")
        raise ValueError(f"git revert failed, decision NOT marked reversed:\n{result.stderr.strip()}")
    original = git("rev-parse", sha).stdout.strip()
    if f"This reverts commit {original}." not in git("log", "-1", "--format=%B", after).stdout:
        raise ValueError("revert commit not verified, decision NOT marked reversed")
    record_outcome(item["ref"], event["decision_id"], "auto_decision_reversed",
                   original=sha, revert=after, worker=worker)
    print(f"REVERTED: {display_ref(item)} — {sha[:10]} undone by {after[:10]} (not pushed; push it yourself)")


def run(args, worker):
    try:
        item = q.get(args.ref)
        if not item:
            raise ValueError("ticket not found")
        rows = q._auto_decision_rows(item)
        if args.decision:
            rows = [r for r in rows if r["event"].get("decision_id") == args.decision]
        else:
            rows = [r for r in rows if not r["reversed"]]
        if len(rows) != 1:
            raise ValueError("select one actual auto-decision with --decision ID (missing or ambiguous decision)")
        row = rows[0]
        event = row["event"]
        if row["reversed"] or event.get("legacy"):
            raise ValueError("decision already reversed or lacks an actual approval receipt")
        decision_id = event.get("decision_id")
        if not decision_id:
            raise ValueError("decision ID missing")
        if args.consumed or args.cancel:
            if event.get("class") != "capped-replay-spend" or not args.evidence:
                raise ValueError("replay outcome requires capped-replay-spend and --evidence JSON_PATH")
            if args.consumed:
                if not args.note.strip():
                    raise ValueError("--consumed requires --note explaining the evidence-to-decision association")
                proof = consumed_proof(args.evidence, item["ref"], decision_id)
                if proof.get("terminal") is not None and proof["terminal"] is not True:
                    raise ValueError("runner is not terminal; cancel it separately before recording completion")
                record_outcome(item["ref"], decision_id, "auto_decision_consumed",
                               proof=proof, note=args.note, worker=worker)
                print(f"CONSUMED / NON-REVERSIBLE: {decision_id} — {proof['requests']} requests; "
                      "evidence preserved, cancellation NOT implied")
            else:
                from .replay_control import ReplayControl
                proof = ReplayControl(args.evidence, decision_id).cancel()
                record_outcome(item["ref"], decision_id, "auto_decision_cancelled", proof=proof, worker=worker)
                print(f"CANCELLED: {decision_id} — runner stopped accepting requests; "
                      f"{proof['requests']} consumed requests remain spent")
        else:
            _revert(item, event, worker)
        return 0
    except (OSError, ValueError, TypeError, KeyError, ImportError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

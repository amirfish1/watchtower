"""GitHub Issues-backed WatchTower queue backend.

The public queue module stays the stable API. This module is an opt-in backing
store for a queue configured with ``backend=github`` and uses the installed
``gh`` CLI for auth, repository selection, and issue operations.
"""

from __future__ import annotations

import json
import re
import subprocess
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .queue import UNCLAIMABLE_READINESS

VALID_LANES = ("normal", "express")
VALID_READINESS = ("ready", "needs-shaping", "needs-spec", "")
VALID_PRIORITIES = ("p0", "p1", "p2", "p3", "p4", "")
VALID_VALUES = ("H", "M", "L", "")
VALID_CONFIDENCES = ("H", "M", "L", "")
DEFAULT_ITEM_TYPE = "bug"

_ISSUE_URL_RE = re.compile(r"/issues/(\d+)(?:\D*)?$")
_META_START = "<!-- watchtower"
_META_END = "-->"


class GitHubBackendError(RuntimeError):
    """Raised when the configured ``gh`` backend cannot complete an operation."""


# `_list_issues` is on the hot path of a live dashboard (CCC polls
# list_items() every few seconds per open conversation-list refresh) but each
# GitHubIssuesBackend is a fresh, state-less instance per call (see
# `_github_backend_for_project`), so per-instance caching would do nothing --
# the cache has to live at module level, keyed by repo. Without it, a
# rate-limited repo got re-hit on every single poll, which never let the
# limit recover and flooded the activity log with an identical ERROR line
# every couple of seconds (WT-87). `_LIST_CACHE_TTL` reuses a recent good
# result instead of re-listing; `_LIST_ERROR_BACKOFF` throttles how often a
# failing repo is retried at all, and falls back to the last known-good list
# (silently, if we have one) rather than re-raising the same error forever.
_LIST_CACHE: Dict[str, Dict[str, Any]] = {}
_LIST_CACHE_TTL = 20.0
_LIST_ERROR_BACKOFF = 60.0


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _clip(value: Any, max_len: int) -> str:
    s = "" if value is None else str(value)
    s = " ".join(s.split()) if max_len <= 240 else s
    return s if len(s) <= max_len else s[:max_len].rstrip() + "..."


def _append_history(meta: Dict[str, Any], event: str, **fields: Any) -> None:
    """Append-only lifecycle trail (WT-87), mirroring the local backend's
    ``queue._append_history`` — stored in the issue-body metadata block so it
    survives round-trips through ``_split_body``/``_body_with_metadata``."""
    hist = meta.get("history")
    if not isinstance(hist, list):
        hist = []
    entry: Dict[str, Any] = {"event": event, "at": _now_iso()}
    for key, value in fields.items():
        if value:
            entry[key] = value
    hist.append(entry)
    meta["history"] = hist


def _norm_choice(value: Any, valid_values: tuple, default: str = "") -> str:
    s = str(value or "").strip()
    if s in valid_values:
        return s
    return default


def _effective_type(value: Any) -> str:
    s = str(value or "").strip().lower()
    return s if s in ("bug", "feature") else DEFAULT_ITEM_TYPE


def _prio_rank(it: Dict[str, Any]) -> int:
    return {"p0": 0, "p1": 1, "p2": 2, "p3": 3, "p4": 4}.get(
        it.get("priority", ""), 5
    )


def _type_rank(it: Dict[str, Any]) -> int:
    return {"bug": 0, "feature": 1}.get(_effective_type(it.get("type")), 2)


def _normalize_resolution(resolution: Any) -> Optional[Dict[str, Any]]:
    if resolution is None:
        return None
    if isinstance(resolution, str):
        resolution = {"summary": resolution}
    if not isinstance(resolution, dict):
        return None
    out: Dict[str, Any] = {}
    summary = _clip(resolution.get("summary", ""), 4000)
    if summary:
        out["summary"] = summary
    for field in ("caveats", "follow_ups", "unresolved"):
        raw = resolution.get(field)
        if raw is None:
            continue
        if isinstance(raw, str):
            raw = [raw]
        vals = [_clip(v, 4000) for v in raw if str(v or "").strip()]
        if vals:
            out[field] = vals
    return out or None


def _resolution_comment(resolution: Optional[Dict[str, Any]]) -> str:
    if not resolution:
        return "WatchTower closed this ticket."
    lines = [f"WatchTower resolution: {resolution.get('summary', '')}".rstrip()]
    for key, label in (
        ("caveats", "Caveats"),
        ("follow_ups", "Follow-ups"),
        ("unresolved", "Unresolved"),
    ):
        values = resolution.get(key) or []
        if values:
            lines.append(f"{label}:")
            lines.extend(f"- {value}" for value in values)
    return "\n".join(lines)


def _meta_block(meta: Dict[str, Any]) -> str:
    lines = [_META_START]
    for key in sorted(meta):
        value = meta.get(key)
        if value in (None, ""):
            continue
        lines.append(f"{key}: {json.dumps(value)}")
    lines.append(_META_END)
    return "\n".join(lines)


def _split_body(body: str) -> tuple[str, Dict[str, Any]]:
    body = body or ""
    start = body.find(_META_START)
    if start < 0:
        return body.rstrip(), {}
    end = body.find(_META_END, start)
    if end < 0:
        return body.rstrip(), {}
    human = body[:start].rstrip()
    raw = body[start + len(_META_START):end].strip()
    meta: Dict[str, Any] = {}
    for line in raw.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue
        try:
            meta[key] = json.loads(value)
        except json.JSONDecodeError:
            meta[key] = value
    return human, meta


def _body_with_metadata(human_body: str, meta: Dict[str, Any]) -> str:
    human_body = (human_body or "").rstrip()
    if human_body:
        return f"{human_body}\n\n{_meta_block(meta)}"
    return _meta_block(meta)


def _first_line(value: str) -> str:
    for line in (value or "").splitlines():
        line = line.strip()
        if line:
            return line
    return ""


def _label_names(raw: Any) -> List[str]:
    out: List[str] = []
    if not isinstance(raw, list):
        return out
    for label in raw:
        if isinstance(label, dict):
            name = label.get("name")
        else:
            name = label
        if name:
            out.append(str(name))
    return out


def _assignee_logins(raw: Any) -> List[str]:
    out: List[str] = []
    if not isinstance(raw, list):
        return out
    for assignee in raw:
        if isinstance(assignee, dict):
            login = assignee.get("login")
        else:
            login = assignee
        if login:
            out.append(str(login))
    return out


class GitHubIssuesBackend:
    """A WatchTower queue backed by GitHub Issues via ``gh``."""

    def __init__(
        self,
        queue: str,
        *,
        repo: str = "",
        repo_path: str = "",
        assignee: str = "@me",
    ):
        self.queue = queue
        self.repo = str(repo or "").strip()
        self.repo_path = str(repo_path or "").strip()
        self.assignee = str(assignee or "@me").strip() or "@me"
        self.queue_label = f"watchtower:{queue}"
        self.in_progress_label = "watchtower:in-progress"

    def _repo_args(self) -> List[str]:
        return ["--repo", self.repo] if self.repo else []

    def _run(self, args: List[str], *, check: bool = True) -> str:
        try:
            proc = subprocess.run(
                ["gh", *args],
                cwd=self.repo_path or None,
                capture_output=True,
                text=True,
                timeout=60,
            )
        except FileNotFoundError as exc:
            raise GitHubBackendError(
                "GitHub backend requires the gh CLI to be installed and on PATH"
            ) from exc
        except subprocess.TimeoutExpired as exc:
            raise GitHubBackendError(f"gh {' '.join(args)} timed out") from exc
        if check and proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or "").strip()
            raise GitHubBackendError(
                f"gh {' '.join(args)} failed"
                + (f": {detail}" if detail else "")
            )
        return proc.stdout

    def _ensure_label(self, name: str, color: str, description: str) -> None:
        self._run(
            [
                "label", "create", name,
                *self._repo_args(),
                "--color", color,
                "--description", description,
                "--force",
            ],
            check=False,
        )

    def _ensure_labels(self) -> None:
        self._ensure_label(
            self.queue_label,
            "5319e7",
            f"WatchTower queue {self.queue}",
        )
        self._ensure_label(
            self.in_progress_label,
            "fbca04",
            "Claimed by a WatchTower worker",
        )

    def _issue_number(self, ident: Any) -> Optional[int]:
        s = str(ident or "").strip()
        if not s:
            return None
        if s.isdigit():
            return int(s)
        prefix = f"{self.queue}-"
        if s.upper().startswith(prefix.upper()):
            suffix = s[len(prefix):]
            return int(suffix) if suffix.isdigit() else None
        return None

    def _issue_to_item(self, issue: Dict[str, Any]) -> Dict[str, Any]:
        body, meta = _split_body(str(issue.get("body") or ""))
        labels = _label_names(issue.get("labels"))
        assignees = _assignee_logins(issue.get("assignees"))
        queue_member = self.queue_label in labels
        number = int(issue.get("number") or 0)
        state = str(issue.get("state") or "").upper()
        if state == "CLOSED":
            status = "closed"
        elif queue_member and (meta.get("claimed_by") or self.in_progress_label in labels):
            status = "in_progress"
        else:
            status = "open"

        resolution = _normalize_resolution({
            "summary": meta.get("resolution_summary", ""),
            "caveats": meta.get("resolution_caveats", []),
            "follow_ups": meta.get("resolution_follow_ups", []),
            "unresolved": meta.get("resolution_unresolved", []),
        })

        item: Dict[str, Any] = {
            "number": number,
            "project": self.queue,
            "seq": number,
            "ref": f"{self.queue}-{number}",
            "id": f"github:{number}",
            "status": status,
            "lane": _norm_choice(meta.get("lane", "normal"), VALID_LANES, "normal"),
            "source": str(meta.get("source") or "github"),
            "note": _clip(meta.get("note") or _first_line(body) or issue.get("title", ""), 4000),
            "text": _clip(body or meta.get("note") or issue.get("title", ""), 24000),
            "url": str(issue.get("url") or ""),
            "title": _clip(issue.get("title", ""), 200),
            "selector": "",
            "screenshot_path": "",
            "repo_path": self.repo_path,
            "type": _effective_type(meta.get("type")),
            "readiness": _norm_choice(meta.get("readiness", ""), VALID_READINESS),
            "priority": _norm_choice(meta.get("priority", ""), VALID_PRIORITIES),
            "value": _norm_choice(meta.get("value", ""), VALID_VALUES),
            "confidence": _norm_choice(meta.get("confidence", ""), VALID_CONFIDENCES),
            "needs_input": bool(meta.get("needs_input", False)),
            "block_question": str(meta.get("block_question") or ""),
            "claimed_by": (
                meta.get("claimed_by")
                or (",".join(assignees) if queue_member and status == "in_progress" and assignees else None)
            ),
            "claimed_at": meta.get("claimed_at"),
            "closed_at": issue.get("closedAt") or meta.get("closed_at"),
            "claimed_session_id": meta.get("claimed_session_id"),
            "created_at": issue.get("createdAt") or _now_iso(),
            "updated_at": issue.get("updatedAt") or issue.get("createdAt") or _now_iso(),
            "github_repo": self.repo,
            "github_labels": labels,
            "github_assignees": assignees,
            "watchtower_label": self.queue_label,
            "watchtower_runnable": bool(queue_member),
            "claimable": bool(queue_member and status == "open"),
            "_github_body": str(issue.get("body") or ""),
        }
        if status == "closed":
            item["closed_by"] = meta.get("closed_by") or item.get("claimed_by")
        if resolution:
            item["resolution"] = resolution
        history = meta.get("history")
        if isinstance(history, list):
            item["history"] = history
        return item

    def _list_issues(self, state: str = "open", *, fresh: bool = False) -> List[Dict[str, Any]]:
        key = f"{self.repo}:{state}"
        now = time.time()
        cached = _LIST_CACHE.get(key)
        if cached is not None and not fresh:
            age = now - cached["at"]
            if cached.get("error") is not None:
                if age < _LIST_ERROR_BACKOFF:
                    if cached.get("data") is not None:
                        return cached["data"]
                    raise cached["error"]
            elif age < _LIST_CACHE_TTL:
                return cached["data"]
        try:
            raw = self._run([
                "issue", "list",
                *self._repo_args(),
                "--state", state,
                "--json", "number,title,body,state,url,assignees,labels,createdAt,updatedAt,closedAt",
                "--limit", "1000",
            ])
            try:
                data = json.loads(raw or "[]")
            except json.JSONDecodeError as exc:
                raise GitHubBackendError("gh issue list returned invalid JSON") from exc
            if not isinstance(data, list):
                raise GitHubBackendError("gh issue list returned a non-list JSON value")
            result = [issue for issue in data if isinstance(issue, dict)]
        except GitHubBackendError as exc:
            prev_data = cached.get("data") if cached else None
            _LIST_CACHE[key] = {"at": now, "data": prev_data, "error": exc}
            if prev_data is not None:
                return prev_data
            raise
        _LIST_CACHE[key] = {"at": now, "data": result, "error": None}
        return result

    def enqueue(
        self,
        *,
        note: str,
        text: str = "",
        source: str = "wt",
        annotation_id: str = "",
        url: str = "",
        title: str = "",
        selector: str = "",
        screenshot_path: str = "",
        repo_path: str = "",
        lane: str = "normal",
        item_type: str = "",
        readiness: str = "",
        priority: str = "",
        value: str = "",
        confidence: str = "",
    ) -> Dict[str, Any]:
        note = _clip(note, 4000)
        text = _clip(text or note, 24000)
        if not note and not text:
            raise ValueError("note or text is required")
        self._ensure_labels()
        meta = {
            "queue": self.queue,
            "annotation_id": str(annotation_id or ""),
            "source": str(source or "wt"),
            "note": note,
            "url": _clip(url, 1000),
            "selector": _clip(selector, 1000),
            "screenshot_path": str(screenshot_path or ""),
            "repo_path": str(repo_path or self.repo_path or ""),
            "lane": lane if lane in VALID_LANES else "normal",
            "type": _effective_type(item_type),
            "readiness": _norm_choice(readiness, VALID_READINESS),
            "priority": _norm_choice(priority, VALID_PRIORITIES),
            "value": _norm_choice(value, VALID_VALUES),
            "confidence": _norm_choice(confidence, VALID_CONFIDENCES),
        }
        issue_title = _clip(title or note or "WatchTower ticket", 200)
        body = _body_with_metadata(text, meta)
        out = self._run([
            "issue", "create",
            *self._repo_args(),
            "--title", issue_title,
            "--body", body,
            "--label", self.queue_label,
        ])
        match = _ISSUE_URL_RE.search(out.strip())
        if not match:
            raise GitHubBackendError("gh issue create did not print an issue URL")
        return self.get(f"{self.queue}-{match.group(1)}") or {
            "number": int(match.group(1)),
            "project": self.queue,
            "seq": int(match.group(1)),
            "ref": f"{self.queue}-{match.group(1)}",
            "status": "open",
            "note": note,
            "text": text,
            "title": issue_title,
            "type": meta["type"],
            "readiness": meta["readiness"],
            "priority": meta["priority"],
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
        }

    def list_items(
        self,
        status: Optional[str] = None,
        lane: Optional[str] = None,
        *,
        fresh: bool = False,
    ) -> List[Dict[str, Any]]:
        if status == "closed":
            return []
        items = [self._issue_to_item(issue) for issue in self._list_issues("open", fresh=fresh)]
        if status:
            items = [it for it in items if it.get("status") == status]
        if lane:
            items = [it for it in items if it.get("lane") == lane]
        return sorted(items, key=lambda it: int(it.get("number", 0)))

    def mark_runnable(self, ident: Any) -> Optional[Dict[str, Any]]:
        item = self.get(ident)
        if item is None:
            return None
        if item.get("status") == "closed":
            raise ValueError(f"{item.get('ref', ident)} is closed")
        self._ensure_labels()
        self._run([
            "issue", "edit", str(item["number"]),
            *self._repo_args(),
            "--add-label", self.queue_label,
        ])
        return self.get(ident)

    def get(self, ident: Any) -> Optional[Dict[str, Any]]:
        number = self._issue_number(ident)
        if number is None:
            return None
        raw = self._run([
            "issue", "view", str(number),
            *self._repo_args(),
            "--json", "number,title,body,state,url,assignees,labels,createdAt,updatedAt,closedAt",
        ])
        try:
            issue = json.loads(raw or "{}")
        except json.JSONDecodeError as exc:
            raise GitHubBackendError("gh issue view returned invalid JSON") from exc
        if not isinstance(issue, dict) or not issue:
            return None
        return self._issue_to_item(issue)

    def _stop_signal_path(self, session_id: str):
        try:
            from . import workers as _workers
            stop_dir = _workers.STOP_SIGNALS_DIR
        except Exception:
            from pathlib import Path
            stop_dir = Path.home() / ".watchtower" / "stop-signals"
        return stop_dir / session_id

    def _claim_candidates(
        self,
        *,
        lane: Optional[str] = None,
        shaping: bool = False,
        oldest: bool = False,
        item_types: Optional[List[str]] = None,
        readiness_filters: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        # fresh=True: claiming must see the current claimed/open state, not a
        # cached snapshot up to _LIST_CACHE_TTL stale -- otherwise two workers
        # could both pick a ticket that was already claimed moments ago.
        candidates = [
            it for it in self.list_items(status="open", lane=lane, fresh=True)
            if it.get("claimable", True)
        ]
        if readiness_filters:
            candidates = [
                it for it in candidates
                if it.get("readiness", "") in readiness_filters
            ]
        elif not shaping:
            candidates = [
                it for it in candidates
                if it.get("readiness", "") not in UNCLAIMABLE_READINESS
            ]
        if item_types:
            candidates = [
                it for it in candidates
                if _effective_type(it.get("type")) in item_types
            ]
        if oldest:
            candidates.sort(key=lambda it: int(it.get("number", 0)))
        else:
            candidates.sort(
                key=lambda it: (
                    0 if it.get("lane") == "express" else 1,
                    _prio_rank(it),
                    _type_rank(it),
                    int(it.get("number", 0)),
                )
            )
        return candidates

    def claim_next(
        self,
        session_id: str,
        *,
        lane: Optional[str] = None,
        session_uuid: str = "",
        shaping: bool = False,
        oldest: bool = False,
        item_types: Optional[List[str]] = None,
        readiness_filters: Optional[List[str]] = None,
    ) -> Optional[Dict[str, Any]]:
        if not session_id:
            raise ValueError("session_id is required")
        signal_file = self._stop_signal_path(session_id)
        has_stop_signal = signal_file.exists()
        candidates = self._claim_candidates(
            lane=lane,
            shaping=shaping,
            oldest=oldest,
            item_types=item_types,
            readiness_filters=readiness_filters,
        )
        if has_stop_signal:
            try:
                signal_file.unlink()
            except OSError:
                pass
            if not candidates:
                return {"stop": True}
        if not candidates:
            return None
        return self.claim_by_ref(
            candidates[0]["ref"],
            session_id,
            session_uuid=session_uuid,
        )

    def claim_by_ref(
        self,
        ref: str,
        session_id: str,
        session_uuid: str = "",
    ) -> Optional[Dict[str, Any]]:
        if not session_id:
            raise ValueError("session_id is required")
        item = self.get(ref)
        if item is None:
            return None
        status = item.get("status", "open")
        if status != "open":
            raise ValueError(f"{ref} is not open (status={status})")
        if not item.get("claimable", True):
            raise ValueError(
                f"{ref} is missing label {self.queue_label}; "
                f"run `wt run {ref}` before claiming it"
            )
        number = str(item["number"])
        body, meta = _split_body(item.get("_github_body") or item.get("text", ""))
        meta.update({
            "claimed_by": str(session_id),
            "claimed_at": _now_iso(),
        })
        if session_uuid:
            meta["claimed_session_id"] = str(session_uuid)
        _append_history(meta, "claim", session_id=str(session_uuid or ""), worker=str(session_id))
        self._ensure_labels()
        self._run([
            "issue", "edit", number,
            *self._repo_args(),
            "--body", _body_with_metadata(body, meta),
            "--add-assignee", self.assignee,
            "--add-label", self.in_progress_label,
        ])
        claimed = self.get(ref)
        if claimed:
            claimed["claimed_by"] = str(session_id)
            claimed["status"] = "in_progress"
        return claimed

    def update_status(
        self,
        ident: Any,
        status: str,
        session_id: str = "",
        session_uuid: str = "",
        resolution: Any = None,
        reason: str = "",
    ) -> Optional[Dict[str, Any]]:
        if status not in ("open", "in_progress", "closed"):
            raise ValueError("status must be one of ('open', 'in_progress', 'closed')")
        if status == "in_progress":
            return self.claim_by_ref(str(ident), session_id, session_uuid=session_uuid)

        item = self.get(ident)
        if item is None:
            return None
        if status == "closed" and not item.get("watchtower_runnable", True):
            raise ValueError(
                f"{ident} is missing label {self.queue_label}; "
                f"run `wt run {ident}` before closing it"
            )
        number = str(item["number"])
        body, meta = _split_body(item.get("_github_body") or item.get("text", ""))

        if status == "open":
            for key in (
                "claimed_by", "claimed_at", "closed_by", "closed_at",
                "resolution_summary", "resolution_caveats",
                "resolution_follow_ups", "resolution_unresolved",
                "needs_input", "block_question",
            ):
                meta.pop(key, None)
            _append_history(meta, "reopen", reason=reason)
            self._run([
                "issue", "edit", number,
                *self._repo_args(),
                "--body", _body_with_metadata(body, meta),
                "--remove-label", self.in_progress_label,
            ])
            self._run(["issue", "reopen", number, *self._repo_args()], check=False)
            return self.get(ident)

        norm = _normalize_resolution(resolution)
        now = _now_iso()
        meta["closed_at"] = now
        if session_id:
            meta["closed_by"] = str(session_id)
            # Backfill claimed_by on a never-claimed issue so attribution
            # isn't dropped when a worker closes by ref without claiming
            # first (WT-81). Never overwrites a real claimant.
            if not meta.get("claimed_by"):
                meta["claimed_by"] = str(session_id)
        if norm:
            meta["resolution_summary"] = norm.get("summary", "")
            meta["resolution_caveats"] = norm.get("caveats", [])
            meta["resolution_follow_ups"] = norm.get("follow_ups", [])
            meta["resolution_unresolved"] = norm.get("unresolved", [])
        _append_history(meta, "close", session_id=str(session_uuid or ""),
                         worker=str(session_id or meta.get("closed_by") or ""),
                         resolution=norm)
        self._run([
            "issue", "edit", number,
            *self._repo_args(),
            "--body", _body_with_metadata(body, meta),
            "--remove-label", self.in_progress_label,
        ])
        self._run([
            "issue", "close", number,
            *self._repo_args(),
            "--comment", _resolution_comment(norm),
        ])
        closed = self.get(ident)
        if closed:
            closed["closed_by"] = str(session_id or closed.get("claimed_by") or "")
            if norm:
                closed["resolution"] = norm
        return closed

    def update(self, ident: Any, **fields: Any) -> Optional[Dict[str, Any]]:
        item = self.get(ident)
        if item is None:
            return None
        body, meta = _split_body(item.get("_github_body") or item.get("text", ""))
        title = item.get("title", "")
        for key, value in fields.items():
            if key == "title":
                title = _clip(value, 200)
            elif key == "item_type":
                meta["type"] = _effective_type(value)
            elif key == "type":
                meta["type"] = _effective_type(value)
            elif key in {
                "readiness", "priority", "value", "confidence", "note",
                "url", "selector", "screenshot_path", "repo_path",
                "needs_input", "block_question",
            }:
                meta[key] = value
        self._run([
            "issue", "edit", str(item["number"]),
            *self._repo_args(),
            "--title", title,
            "--body", _body_with_metadata(body, meta),
        ])
        return self.get(ident)

    def peek_next(
        self,
        *,
        lane: Optional[str] = None,
        item_types: Optional[List[str]] = None,
    ) -> Optional[Dict[str, Any]]:
        candidates = self._claim_candidates(lane=lane, item_types=item_types)
        return dict(candidates[0]) if candidates else None

    def count_claimable(
        self,
        *,
        lane: Optional[str] = None,
        item_types: Optional[List[str]] = None,
    ) -> int:
        """How many tickets claim_next() would currently pick from — the
        reconciler's single source of truth for spawn-worthy depth on a
        GitHub-backed queue (see queue.count_claimable)."""
        return len(self._claim_candidates(lane=lane, item_types=item_types))

    def last_progress_iso(self) -> Optional[str]:
        latest: Optional[str] = None
        for it in self.list_items(status="closed"):
            closed_at = it.get("closed_at")
            if closed_at and (latest is None or closed_at > latest):
                latest = closed_at
        return latest

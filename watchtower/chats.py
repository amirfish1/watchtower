#!/usr/bin/env python3
"""Group-chat store: CCC-compatible on disk, deterministic nudge targeting.

WatchTower's group chats live as pairs of files that are byte-compatible with
Claude Command Center's existing group chats, so both systems interoperate on
the same directory from day one (CCC's UI keeps reading them unchanged):

  * ``<slug>-<YYYYmmdd-HHMMSS>.md``: a markdown transcript. Header block
    (title, Started, Mode, Participants), then append-only message entries.
    Each entry heading has the shape (observed in real CCC files)::

        ## 2026-06-30 Tuesday 09:44:37 PDT — 59ad443c: display name

    or, for the human, ``## <ts> — Human``. CCC parses headings with the
    line-anchored regex ``^##\\s+.+?—\\s+(?:([0-9a-fA-F]{8})\\b|(Human)\\b)``,
    which this module reuses verbatim (see :data:`CCC_HEADING_RE`).

    Note on the em-dash: this codebase bans em-dashes everywhere EXCEPT the
    two places the CCC file format requires them, the ``# Group Chat —``
    title and the ``## <ts> — <author>`` entry headings. In this file they
    appear only in docstring examples depicting that format; code strings
    use the ``\\u2014`` escape.

  * ``<slug>-<ts>.json`` sidecar: chat metadata (uuid, session_ids, topic,
    mode, name_map, include_human, started_at, archived, closed_at) plus
    WatchTower nudge-policy keys (nudge_interval_s, idle_close_s,
    max_auto_nudges_per_hour, last_reminder_key, nudge_history). Real CCC
    sidecars carry extra keys this module does not own (lane, keywords,
    paused, last_reminder_at, ...): every mutation here loads the full dict,
    changes only its own keys, and dumps the full dict back, so unknown keys
    always survive a rewrite.

Directory resolution (same pattern as queue.py's store, evaluated per call):

  1. ``$WATCHTOWER_CHATS_DIR`` (explicit override, used by tests).
  2. ``~/.claude/group-chats`` if that directory already exists (share chats
     with CCC when co-located on a machine).
  3. ``~/.watchtower/chats`` (WatchTower's own default, created on demand).

Nudge orchestration is deterministic, no LLM in the loop: pure functions pick
targets from the transcript tail (:func:`pick_nudge_targets`), and the daemon
calls :func:`nudge_tick` each tick with an injected ``deliver`` callable, so
this module stays decoupled from delivery transports (fifo/resume/delegate).
"""

from __future__ import annotations

import json
import os
import re
import time
import uuid as _uuid
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from . import queue as _queue

# The exact heading regex CCC's UI uses. Line-anchored: a heading-shaped line
# inside a fenced code block still matches, and that is deliberate, this
# module replicates CCC's parser byte-for-byte rather than improving on it
# (a "fix" here would silently fork the on-disk dialect).
CCC_HEADING_RE = re.compile(r"^##\s+.+?\u2014\s+(?:([0-9a-fA-F]{8})\b|(Human)\b)")

# Nudge-policy defaults (overridable per chat via sidecar keys).
DEFAULT_NUDGE_INTERVAL_S = 60
DEFAULT_IDLE_CLOSE_S = 2700  # 45 minutes
DEFAULT_MAX_AUTO_NUDGES_PER_HOUR = 30

# Case-insensitive done markers: when the latest message body contains one of
# these, the chat is considered concluded and gets closed on the next tick.
_DONE_MARKERS = ("we're done", "✅ done")

_CCC_SHARED_DIR = Path.home() / ".claude" / "group-chats"
_WT_DEFAULT_DIR = Path.home() / ".watchtower" / "chats"


def chats_dir() -> Path:
    """Resolve the active chats directory. Read fresh each call so tests can
    flip ``$WATCHTOWER_CHATS_DIR`` between runs (same contract as
    ``queue._resolve_store_path``)."""
    env = os.environ.get("WATCHTOWER_CHATS_DIR")
    if env:
        d = Path(env).expanduser()
        d.mkdir(parents=True, exist_ok=True)
        return d
    if _CCC_SHARED_DIR.is_dir():
        return _CCC_SHARED_DIR
    _WT_DEFAULT_DIR.mkdir(parents=True, exist_ok=True)
    return _WT_DEFAULT_DIR


def _lock_path() -> Path:
    return chats_dir() / ".chats.lock"


def _local_ts(epoch: Optional[float] = None) -> str:
    """Heading/Started timestamp in CCC's observed local format, e.g.
    ``2026-06-30 Tuesday 09:44:37 PDT`` (weekday and timezone included)."""
    t = time.localtime(epoch if epoch is not None else time.time())
    return time.strftime("%Y-%m-%d %A %H:%M:%S %Z", t).strip()


def _slugify(topic: str) -> str:
    s = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(topic or ""))
    s = re.sub(r"-+", "-", s).strip("-")
    # Real CCC filenames truncate long slugs around 60 chars.
    return (s[:60].rstrip("-")) or "chat"


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        with open(path, "r") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _save_json(path: Path, data: Dict[str, Any]) -> None:
    """Atomic sidecar write (tmp + os.replace), single line like CCC's."""
    tmp = str(path) + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f)
    os.replace(tmp, path)


def _write_text_atomic(path: Path, text: str) -> None:
    tmp = str(path) + ".tmp"
    with open(tmp, "w") as f:
        f.write(text)
    os.replace(tmp, path)


# --------------------------------------------------------------------- parse
def _split_heading(line: str) -> Optional[Tuple[str, Optional[str], str]]:
    """If ``line`` is a CCC message heading, return (ts, sid8_or_None, name).

    Detection uses :data:`CCC_HEADING_RE` verbatim so parse behavior matches
    CCC exactly (including its quirks, see module docstring)."""
    m = CCC_HEADING_RE.match(line)
    if not m:
        return None
    sid8, human = m.group(1), m.group(2)
    author_start = m.start(1) if sid8 else m.start(2)
    pre = line[:author_start]
    dash = pre.rfind("\u2014")
    ts = line[2:dash].strip() if dash != -1 else ""
    if sid8:
        rest = line[m.end(1):].strip()
        if rest.startswith(":"):
            rest = rest[1:].strip()
        return ts, sid8, (rest or sid8)
    return ts, None, "Human"


def _parse_messages(md_text: str) -> List[Dict[str, Any]]:
    """Parse a chat transcript into message dicts.

    Returns ``[{ts, author_sid8, author_name, body}, ...]`` in file order.
    Anything before the first heading (title, metadata, wake-status, system
    blockquotes) is header material and is skipped. The trailing ``---``
    separator each entry ends with is stripped from the body."""
    messages: List[Dict[str, Any]] = []
    current: Optional[Dict[str, Any]] = None
    body_lines: List[str] = []

    def _flush() -> None:
        if current is None:
            return
        body = "\n".join(body_lines)
        # Drop the trailing entry separator ("---" on its own line) + blanks.
        body = body.rstrip()
        if body.endswith("\n---"):
            body = body[: -len("\n---")].rstrip()
        elif body == "---":
            body = ""
        current["body"] = body.strip("\n")
        messages.append(current)

    for line in md_text.splitlines():
        parts = _split_heading(line)
        if parts is not None:
            _flush()
            ts, sid8, name = parts
            current = {"ts": ts, "author_sid8": sid8, "author_name": name}
            body_lines = []
        elif current is not None:
            body_lines.append(line)
    _flush()
    return messages


# ------------------------------------------------------------------- lookup
def find_chat(ref: str) -> Tuple[Path, Dict[str, Any]]:
    """Resolve ``ref`` to ``(md_path, sidecar_dict)``.

    ``ref`` may be: a path to the .md or .json, a bare filename (with or
    without extension), a slug prefix, or a sidecar uuid prefix. Raises
    ValueError when nothing matches or when a prefix is ambiguous."""
    ref = str(ref or "").strip()
    if not ref:
        raise ValueError("empty chat ref")

    # Direct path (either half of the pair, extension optional).
    p = Path(ref).expanduser()
    for cand in (p, p.with_suffix(".md"), Path(str(p) + ".md")):
        if cand.suffix == ".json":
            cand = cand.with_suffix(".md")
        if cand.suffix == ".md" and cand.is_file():
            return cand, _load_json(cand.with_suffix(".json"))

    d = chats_dir()
    stem = ref
    for ext in (".md", ".json"):
        if stem.endswith(ext):
            stem = stem[: -len(ext)]
    exact = d / (stem + ".md")
    if exact.is_file():
        return exact, _load_json(exact.with_suffix(".json"))

    matches: List[Path] = []
    ref_l = stem.lower()
    for md in sorted(d.glob("*.md")):
        if md.stem.lower().startswith(ref_l):
            matches.append(md)
            continue
        sc = _load_json(md.with_suffix(".json"))
        if str(sc.get("uuid") or "").lower().startswith(ref_l):
            matches.append(md)
    if not matches:
        raise ValueError(f"no chat matches {ref!r} in {d}")
    if len(matches) > 1:
        names = ", ".join(m.name for m in matches[:5])
        raise ValueError(f"ambiguous chat ref {ref!r}: matches {names}")
    md = matches[0]
    return md, _load_json(md.with_suffix(".json"))


# ------------------------------------------------------------------- create
def create_chat(
    topic: str,
    participants: List[Dict[str, Any]],
    include_human: bool = True,
    mode: str = "topic",
) -> Dict[str, Any]:
    """Create a new chat pair on disk and return
    ``{"path", "sidecar_path", "uuid"}``.

    ``participants`` is a list of ``{"session_id": ..., "name": ...}`` dicts;
    ``name`` defaults to the session id's first 8 hex chars."""
    now = time.time()
    d = chats_dir()
    base = f"{_slugify(topic)}-{time.strftime('%Y%m%d-%H%M%S', time.localtime(now))}"
    stem, n = base, 1
    while (d / (stem + ".md")).exists() or (d / (stem + ".json")).exists():
        n += 1
        stem = f"{base}-{n}"
    md_path = d / (stem + ".md")
    sc_path = d / (stem + ".json")

    session_ids: List[str] = []
    name_map: Dict[str, str] = {}
    for part in participants or []:
        sid = str(part.get("session_id") or "").strip()
        if not sid:
            continue
        session_ids.append(sid)
        name_map[sid] = str(part.get("name") or sid[:8])

    names = [f"`{name_map[s]}`" for s in session_ids]
    if include_human:
        names.append("`human`")
    header = (
        f"# Group Chat \u2014 {topic}\n"
        f"**Started:** {_local_ts(now)}\n"
        f"**Mode:** {mode}\n"
        f"**Participants:** {', '.join(names)}\n"
        "---\n"
    )
    sidecar: Dict[str, Any] = {
        "uuid": str(_uuid.uuid4()),
        "session_ids": session_ids,
        "topic": str(topic),
        "mode": str(mode),
        "name_map": name_map,
        "include_human": bool(include_human),
        "started_at": now,
        "archived": False,
        "closed_at": None,
    }
    with _queue._FileLock(_lock_path()):
        _write_text_atomic(md_path, header)
        _save_json(sc_path, sidecar)
    _queue._log("POST", f"{md_path.name} created ({len(session_ids)} participants)", queue="CHAT")
    return {"path": str(md_path), "sidecar_path": str(sc_path), "uuid": sidecar["uuid"]}


# --------------------------------------------------------------------- post
def post(
    ref: str,
    body: str,
    author_sid: Optional[str] = None,
    author_name: str = "Human",
) -> Dict[str, Any]:
    """Append one message entry to the chat's markdown transcript.

    With ``author_sid``: the heading uses the sid's first 8 hex chars plus the
    display name from the sidecar's name_map (falling back to ``author_name``
    when it is not the default, else to the sid8). Without: a Human entry."""
    md_path, sidecar = find_chat(ref)
    ts = _local_ts()
    if author_sid:
        sid8 = str(author_sid)[:8]
        name_map = sidecar.get("name_map") or {}
        display = name_map.get(str(author_sid))
        if display is None:
            for k, v in name_map.items():
                if str(k)[:8].lower() == sid8.lower():
                    display = v
                    break
        if display is None:
            display = author_name if author_name and author_name != "Human" else sid8
        heading = f"## {ts} \u2014 {sid8}: {display}"
    else:
        heading = f"## {ts} \u2014 Human"
        if author_name and author_name != "Human":
            heading = f"## {ts} \u2014 Human: {author_name}"

    entry = f"\n{heading}\n\n{body.rstrip()}\n\n\n---\n"
    with _queue._FileLock(_lock_path()):
        try:
            text = md_path.read_text()
        except OSError:
            text = ""
        if text and not text.endswith("\n"):
            text += "\n"
        _write_text_atomic(md_path, text + entry)
    _queue._log("POST", f"{md_path.name} by {(author_sid or 'human')[:8]}", queue="CHAT")
    return {"path": str(md_path), "heading": heading, "ts": ts,
            "author_sid8": (str(author_sid)[:8] if author_sid else None)}


# --------------------------------------------------------------------- read
def read_chat(ref: str, tail: Optional[int] = None) -> Dict[str, Any]:
    """Parse a chat into a dict: topic, mode, participants, messages,
    archived, closed_at. ``tail`` limits to the last N messages."""
    md_path, sidecar = find_chat(ref)
    try:
        md_text = md_path.read_text()
    except OSError:
        md_text = ""
    messages = _parse_messages(md_text)
    if tail is not None:
        messages = messages[-int(tail):] if tail > 0 else []
    name_map = sidecar.get("name_map") or {}
    participants = [
        {"session_id": sid, "name": name_map.get(sid, str(sid)[:8])}
        for sid in (sidecar.get("session_ids") or [])
    ]
    return {
        "topic": sidecar.get("topic", ""),
        "mode": sidecar.get("mode", "topic"),
        "participants": participants,
        "messages": messages,
        "archived": bool(sidecar.get("archived")),
        "closed_at": sidecar.get("closed_at"),
    }


def list_chats(include_archived: bool = False) -> List[Dict[str, Any]]:
    """List chats in the active directory, newest started_at first."""
    out: List[Dict[str, Any]] = []
    for md in chats_dir().glob("*.md"):
        sc_path = md.with_suffix(".json")
        if not sc_path.is_file():
            continue  # a lone .md is not a chat pair
        sidecar = _load_json(sc_path)
        if sidecar.get("archived") and not include_archived:
            continue
        name_map = sidecar.get("name_map") or {}
        try:
            last_post_at = md.stat().st_mtime
        except OSError:
            last_post_at = None
        out.append({
            "path": str(md),
            "topic": sidecar.get("topic", md.stem),
            "started_at": sidecar.get("started_at"),
            "participants": [
                {"session_id": sid, "name": name_map.get(sid, str(sid)[:8])}
                for sid in (sidecar.get("session_ids") or [])
            ],
            "archived": bool(sidecar.get("archived")),
            "closed_at": sidecar.get("closed_at"),
            "last_post_at": last_post_at,
        })
    out.sort(key=lambda c: c.get("started_at") or 0, reverse=True)
    return out


# ---------------------------------------------------------- sidecar updates
def _mutate_sidecar(ref: str, fn: Callable[[Dict[str, Any]], None]) -> Dict[str, Any]:
    """Locked load-mutate-dump of a chat's sidecar. ``fn`` edits the dict in
    place; every key it does not touch is preserved byte-for-byte in intent
    (full dict round-trip, never rebuilt from a schema)."""
    md_path, _ = find_chat(ref)
    sc_path = md_path.with_suffix(".json")
    with _queue._FileLock(_lock_path()):
        sidecar = _load_json(sc_path)
        fn(sidecar)
        _save_json(sc_path, sidecar)
    return sidecar


def add_participant(ref: str, session_id: str, name: str) -> Dict[str, Any]:
    """Add a participant to the sidecar (session_ids + name_map)."""
    sid = str(session_id)

    def _fn(sc: Dict[str, Any]) -> None:
        sids = sc.get("session_ids")
        if not isinstance(sids, list):
            sids = []
        if sid not in sids:
            sids.append(sid)
        sc["session_ids"] = sids
        nm = sc.get("name_map")
        if not isinstance(nm, dict):
            nm = {}
        nm[sid] = str(name or sid[:8])
        sc["name_map"] = nm

    return _mutate_sidecar(ref, _fn)


def remove_participant(ref: str, session_id: str) -> Dict[str, Any]:
    """Remove a participant. The name_map entry is kept so old transcript
    headings still resolve to a display name."""
    sid = str(session_id)

    def _fn(sc: Dict[str, Any]) -> None:
        sids = sc.get("session_ids")
        if isinstance(sids, list):
            sc["session_ids"] = [s for s in sids if s != sid]

    return _mutate_sidecar(ref, _fn)


def set_archived(ref: str, archived: bool) -> Dict[str, Any]:
    def _fn(sc: Dict[str, Any]) -> None:
        sc["archived"] = bool(archived)
        if archived:
            sc["archived_at"] = time.time()

    return _mutate_sidecar(ref, _fn)


def close_chat(ref: str) -> Dict[str, Any]:
    """Mark the chat closed (epoch float, matching real CCC sidecars)."""
    def _fn(sc: Dict[str, Any]) -> None:
        if not sc.get("closed_at"):
            sc["closed_at"] = time.time()

    sidecar = _mutate_sidecar(ref, _fn)
    _queue._log("CLOSE", f"chat {Path(find_chat(ref)[0]).name} closed", queue="CHAT")
    return sidecar


# ---------------------------------------------------------------- targeting
def _full_sid_for(sid8: str, session_ids: List[str]) -> Optional[str]:
    for sid in session_ids:
        if str(sid)[:8].lower() == sid8.lower():
            return sid
    return None


def _mentioned_sids(body: str, sidecar: Dict[str, Any]) -> List[str]:
    """Participants a human message explicitly addresses: @name mentions
    (matched against name_map display names, case-insensitive prefix) and
    bare 8-hex ids matching a participant's sid prefix."""
    session_ids = [str(s) for s in (sidecar.get("session_ids") or [])]
    name_map = {str(k): str(v) for k, v in (sidecar.get("name_map") or {}).items()}
    picked: List[str] = []

    def _add(sid: str) -> None:
        if sid and sid not in picked:
            picked.append(sid)

    for token in re.findall(r"@([A-Za-z0-9_-]+)", body or ""):
        tok = token.lower()
        full = _full_sid_for(token, session_ids) if re.fullmatch(r"[0-9a-fA-F]{8}", token) else None
        if full:
            _add(full)
            continue
        for sid in session_ids:
            nm = name_map.get(sid, "").lower()
            if nm and (nm == tok or nm.startswith(tok)):
                _add(sid)
    for token in re.findall(r"\b([0-9a-fA-F]{8})\b", body or ""):
        full = _full_sid_for(token, session_ids)
        if full:
            _add(full)
    return picked


def pick_nudge_targets(md_text: str, sidecar: Dict[str, Any]) -> List[str]:
    """Deterministic nudge targeting, a pure function of the transcript and
    sidecar (ported from CCC's coordination watcher):

    * last entry by an agent: everyone in session_ids except that agent,
    * last entry by the Human with @mentions or 8-hex ids: only those,
    * last entry by the Human, no mentions: the most recent prior agent
      author still in the chat, else everyone,
    * empty transcript: everyone (kick the conversation off).
    """
    session_ids = [str(s) for s in (sidecar.get("session_ids") or [])]
    messages = _parse_messages(md_text)
    if not messages:
        return list(session_ids)
    last = messages[-1]
    if last["author_sid8"]:
        sid8 = last["author_sid8"].lower()
        return [s for s in session_ids if s[:8].lower() != sid8]
    mentioned = _mentioned_sids(last.get("body", ""), sidecar)
    if mentioned:
        return mentioned
    for msg in reversed(messages[:-1]):
        if msg["author_sid8"]:
            full = _full_sid_for(msg["author_sid8"], session_ids)
            if full:
                return [full]
    return list(session_ids)


def build_nudge_text(md_path: Any, topic: str, mode: str, target_sid: str) -> str:
    """Engine-agnostic nudge prose. Works for any harness: skill invocation if
    available, plain file append otherwise. Ends with the latest heading only
    (never a body) so the nudge stays small."""
    last_heading = ""
    try:
        for line in Path(md_path).read_text().splitlines():
            if CCC_HEADING_RE.match(line):
                last_heading = line
    except OSError:
        pass
    text = (
        f'Group-chat check-in: invoke your group-chat-checkin skill with '
        f'chat="{md_path}" topic="{topic}" mode={mode} sid="{target_sid}". '
        f"If you cannot invoke skills, read the chat file and respond by "
        f"appending a properly formatted entry."
    )
    if last_heading:
        text += f"\nLatest entry: {last_heading}"
    return text


# ---------------------------------------------------------------- scheduler
def _reminder_key(md_text: str, messages: List[Dict[str, Any]]) -> str:
    """Dedup key: post count + the last heading truncated at the author token,
    mirroring the shape observed in real CCC sidecars
    (``"11:## 2026-06-30 Tuesday 09:49:23 PDT — 13c6bbe3"``)."""
    last = ""
    for line in md_text.splitlines():
        m = CCC_HEADING_RE.match(line)
        if m:
            last = line[: m.end()]
    return f"{len(messages)}:{last}"


def nudge_tick(
    deliver: Callable[[str, str], bool],
    now: Optional[float] = None,
) -> Dict[str, Any]:
    """One scheduler pass over every open chat. Called from the daemon loop.

    For each non-archived, non-closed chat: close it when concluded (done
    marker in the latest message) or idle past ``idle_close_s``; otherwise
    nudge the computed targets when the transcript changed since the last
    nudge, the per-chat interval has elapsed, and the reminder key is new.
    ``deliver(session_id, text) -> bool`` is injected so this module never
    touches transports. Per-chat rate cap: ``max_auto_nudges_per_hour``.

    Returns ``{"checked": n, "results": {<md filename>: {"action": ...}}}``
    where action is one of nudged / capped / closed / skipped."""
    now = time.time() if now is None else float(now)
    report: Dict[str, Any] = {"checked": 0, "results": {}}

    for md_path in sorted(chats_dir().glob("*.md")):
        sc_path = md_path.with_suffix(".json")
        if not sc_path.is_file():
            continue
        sidecar = _load_json(sc_path)
        if sidecar.get("archived") or sidecar.get("closed_at"):
            continue
        report["checked"] += 1
        name = md_path.name
        try:
            md_text = md_path.read_text()
            mtime = md_path.stat().st_mtime
        except OSError:
            report["results"][name] = {"action": "skipped", "reason": "unreadable"}
            continue
        messages = _parse_messages(md_text)

        idle_close_s = float(sidecar.get("idle_close_s") or DEFAULT_IDLE_CLOSE_S)
        last_body = (messages[-1].get("body", "") if messages else "").lower()
        if any(marker in last_body for marker in _DONE_MARKERS):
            close_chat(str(md_path))
            report["results"][name] = {"action": "closed", "reason": "done-marker"}
            continue
        if now - mtime > idle_close_s:
            close_chat(str(md_path))
            report["results"][name] = {"action": "closed", "reason": "idle"}
            continue

        interval = float(sidecar.get("nudge_interval_s") or DEFAULT_NUDGE_INTERVAL_S)
        history = [float(t) for t in (sidecar.get("nudge_history") or [])]
        last_at = sidecar.get("last_reminder_at") or 0
        last_nudge = max(history + [float(last_at)]) if (history or last_at) else 0.0

        if last_nudge and mtime <= last_nudge:
            report["results"][name] = {"action": "skipped", "reason": "unchanged"}
            continue
        if now - last_nudge < interval:
            report["results"][name] = {"action": "skipped", "reason": "interval"}
            continue
        key = _reminder_key(md_text, messages)
        if key == sidecar.get("last_reminder_key"):
            report["results"][name] = {"action": "skipped", "reason": "dedup"}
            continue
        cap = int(sidecar.get("max_auto_nudges_per_hour") or DEFAULT_MAX_AUTO_NUDGES_PER_HOUR)
        recent = [t for t in history if now - t < 3600]
        if len(recent) >= cap:
            _queue._log("NUDGE", f"{name} capped ({cap}/h)", queue="CHAT")
            report["results"][name] = {"action": "capped", "cap": cap}
            continue

        targets = pick_nudge_targets(md_text, sidecar)
        delivered = []
        for sid in targets:
            text = build_nudge_text(
                str(md_path), sidecar.get("topic", ""), sidecar.get("mode", "topic"), sid
            )
            try:
                ok = bool(deliver(sid, text))
            except Exception:
                ok = False
            delivered.append({"session_id": sid, "ok": ok})

        def _record(sc: Dict[str, Any]) -> None:
            hist = sc.get("nudge_history")
            if not isinstance(hist, list):
                hist = []
            hist.append(now)
            sc["nudge_history"] = hist
            sc["last_reminder_key"] = key
            sc["last_reminder_at"] = now
            sc["last_reminder_targets"] = [str(s)[:8] for s in targets]

        _mutate_sidecar(str(md_path), _record)
        _queue._log(
            "NUDGE",
            f"{name} -> {', '.join(str(s)[:8] for s in targets) or 'nobody'}",
            queue="CHAT",
        )
        report["results"][name] = {"action": "nudged", "targets": delivered}

    return report

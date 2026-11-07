"""Bounded GitHub issue-body history (WT-47, contract: plan v3).

The issue body keeps a bounded tail of ``history`` and an O(1) index
(``history_archive``). Older entries move, as a contiguous prefix, into
hash-chained immutable comment segments, and readers rebuild the full history
from them. An archive that cannot be rebuilt is *degraded*: reported, and
failing closed per consumer, never presented as complete.

This module is pure: no gh calls, no store access. github_backend does the I/O.

Trust: a segment is recognised by the chain sha pinned in the body index,
never by comment author. ``dsha`` detects a corrupt digest, not a recomputing
writer; body writers are trusted, as for ``claimed_by``.
"""

from __future__ import annotations

import hashlib
import itertools
import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

VERSION = 1
CLASSIFIER_VERSION = 1
HIST_PROTO = 1              # stamped on claims by a release >= 1 reader

K_MIN = 20                  # newest entries that never move
TAIL_MAX = 300              # entries
TAIL_TARGET = 150
TAIL_STEP = 50
TAIL_MAX_BYTES = 96 * 1024
CHUNK_MAX_CHARS = 60000
PARTS_MAX = 16
# Structural ceiling. The contract's 7,032 B estimate was measured at 7,073 B
# by the reviewer; test 2b computes the real maximum, this is what is enforced.
INDEX_MAX_BYTES = 8192

DIGEST_CAP = 64
HOST_MAX_BYTES = 256
K_DIGEST = ("claim", "defer", "block", "reopen", "release", "close",
            "stage_retired", "answer")
CLOCK_CLASSES = ("progress", "stage", "readiness")

_HEADER_RE = re.compile(
    r"\A<!-- watchtower-history-archive v=(\d+) seg=([0-9a-f]{16}) "
    r"part=(\d+)/(\d+) base=(\d+) count=(\d+) sha=([0-9a-f]{64}) "
    r"prev=([0-9a-f]{64}) -->\n")
_HEADER_PREFIX = "<!-- watchtower-history-archive "
_TRAILER_RE = re.compile(r"\n\n<!-- watchtower-entry: [0-9a-f]{16} -->\s*\Z")
_OWNER_ANSWER_RE = re.compile(r"(?i)(owner) (answer)")
_COMBINATIONS_MAX = 256


class ArchiveError(Exception):
    """A plan or encoding that must not be written; nothing was."""


def is_archive_comment(body: Any) -> bool:
    """A comment shaped like a segment part. Recognition for *display and
    ingest skipping* only; whether it is part of the chain is decided by sha."""
    return str(body or "").startswith(_HEADER_PREFIX)


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sid_hash(sid: str) -> str:
    return sha256_text(str(sid))[:32]


def root_zero(repo: str, number: Any) -> str:
    return sha256_text(f"wt-history-v1:{repo}#{number}")


def next_root(prev_root: str, seg_sha: str) -> str:
    return sha256_text(f"{prev_root}:{seg_sha}")


def escape_payload(raw: str) -> str:
    """Defuse the owner-answer marker. Every match lies inside a JSON string
    (literals hold no spaces; separators put a quote between words), and the
    inserted ``\\u0020`` is a fresh escape json.loads decodes to the space:
    the text before it ends in ``r``, so its backslash cannot be consumed."""
    return _OWNER_ANSWER_RE.sub(r"\1\\u0020\2", raw)


def encode_entries(entries: List[Any], lines: Optional[List[str]] = None) -> str:
    escaped = escape_payload("\n".join(lines or [json.dumps(e) for e in entries]))
    if "OWNER ANSWER" in escaped.upper():
        raise ArchiveError("owner-answer marker survived escaping; nothing posted")
    return escaped


def decode_entries(escaped: str) -> List[Any]:
    # JSON strings hold no raw newline, so every newline separates entries.
    return json.loads("[" + escaped.replace("\n", ",") + "]") if escaped else []


@dataclass
class Segment:
    key: str
    base: int
    count: int
    sha: str
    prev: str
    parts: List[str]        # full comment text per part, without entry marker

    def part_key(self, i: int) -> str:
        return f"{self.key[:14]}{i:02x}"

    @property
    def root(self) -> str:
        return next_root(self.prev, self.sha)


def _segment(number: Any, base: int, entries: List[Any], prev: str,
             lines: Optional[List[str]] = None) -> Segment:
    escaped = encode_entries(entries, lines)
    sha = sha256_text(escaped)
    key = sha256_text(f"{number}|{base}|{len(entries)}|{sha}|{prev}")[:16]
    chunks = [escaped[i:i + CHUNK_MAX_CHARS]
              for i in range(0, len(escaped), CHUNK_MAX_CHARS)] or [""]
    total = len(chunks)
    parts = [f"<!-- watchtower-history-archive v={VERSION} seg={key} part={i}/{total} "
             f"base={base} count={len(entries)} sha={sha} prev={prev} -->\n{chunk}"
             for i, chunk in enumerate(chunks)]
    return Segment(key, base, len(entries), sha, prev, parts)


def build_segments(number: Any, base: int, entries: List[Any], prev: str) -> List[Segment]:
    """Encode ``entries`` as one segment, or a chain of them when one would
    exceed PARTS_MAX chunks. Deterministic: the same plan yields the same keys,
    so a retry adopts what an earlier attempt posted."""
    limit = CHUNK_MAX_CHARS * PARTS_MAX
    lines = [json.dumps(e) for e in entries]
    out: List[Segment] = []
    i = 0
    while i < len(entries):
        size, j = 0, i
        while j < len(entries):
            # Escaping grows a match by five characters; +8 stays conservative.
            cost = len(lines[j]) * 2 + 8
            if j > i and size + cost > limit:
                break
            size += cost
            j += 1
        seg = _segment(number, base + i, entries[i:j], prev, lines[i:j])
        if len(seg.parts) > PARTS_MAX:
            raise ArchiveError(f"one history entry needs {len(seg.parts)} parts")
        out.append(seg)
        prev = seg.root
        i = j
    return out


# --------------------------------------------------------------------- views

@dataclass
class ArchiveView:
    """What a reader knows about one issue's archive. ``live`` means the chain
    was verified against comments fetched for this read; a writer extends a
    chain only from a live view."""
    n: int = 0
    resolved: bool = True
    live: bool = True
    entries: List[Any] = field(default_factory=list)
    err: str = ""
    head_ref: str = ""


EMPTY_VIEW = ArchiveView()


def index_of(meta: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    idx = meta.get("history_archive")
    return idx if isinstance(idx, dict) and idx else None


def index_n(meta: Dict[str, Any]) -> int:
    idx = index_of(meta)
    try:
        return max(0, int((idx or {}).get("n") or 0))
    except (TypeError, ValueError):
        return 0


def _parse_part(comment: Any) -> Optional[Dict[str, Any]]:
    if not isinstance(comment, dict):
        return None
    body = str(comment.get("body") or "")
    match = _HEADER_RE.match(body)
    if not match:
        return None
    version, seg, part, parts, base, count, sha, prev = match.groups()
    rest = body[match.end():]
    # The trailer is anchored at the end: payload text may quote a marker.
    trailer = _TRAILER_RE.search(rest)
    return {"v": int(version), "seg": seg, "part": int(part), "parts": int(parts),
            "base": int(base), "count": int(count), "sha": sha, "prev": prev,
            "chunk": rest[:trailer.start()] if trailer else rest,
            "id": comment.get("id"), "url": comment.get("url")}


def _candidates(comments: Iterable[Any]) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
    """Whole, sha-valid segments in ``comments``; the bad ones by key, with why."""
    groups: Dict[Tuple[str, str], Dict[int, List[Dict[str, Any]]]] = {}
    declared: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for c in comments or []:
        p = _parse_part(c)
        if p is None or p["v"] != VERSION:
            continue
        ident = (p["seg"], p["sha"])
        groups.setdefault(ident, {}).setdefault(p["part"], []).append(p)
        declared.setdefault(ident, p)
    good: List[Dict[str, Any]] = []
    bad: Dict[str, str] = {}
    for ident, parts in groups.items():
        first = declared[ident]
        total = first["parts"]
        if total < 1 or total > PARTS_MAX or any(i not in parts for i in range(total)):
            bad[first["seg"]] = "part missing"
            continue
        choices = [parts[i] for i in range(total)]
        for combo in itertools.islice(itertools.product(*choices), _COMBINATIONS_MAX):
            joined = "".join(p["chunk"] for p in combo)
            if sha256_text(joined) != first["sha"]:
                continue
            try:
                entries = decode_entries(joined)
            except ValueError:
                bad[first["seg"]] = "payload does not parse"
                break
            if len(entries) != first["count"]:
                bad[first["seg"]] = "entry count mismatch"
                break
            good.append({**{k: first[k] for k in ("seg", "sha", "prev", "base", "count")},
                         "entries": entries, "parts": total,
                         "ids": [p["id"] or p["url"] for p in combo],
                         "url": combo[0]["url"]})
            bad.pop(first["seg"], None)
            break
        else:
            bad.setdefault(first["seg"], "sha mismatch")
    return good, bad


def verify_chain(meta: Dict[str, Any], comments: Iterable[Any], repo: str,
                 number: Any) -> Tuple[Optional[List[Any]], str, List[Any]]:
    """Walk the whole chain from base 0 to the index head: (archived entries,
    "", part comment ids) or (None, reason, []). Every part, sha, ``prev``,
    contiguous base, count and the root must agree; an orphan or forged
    segment is simply never on the path."""
    idx = index_of(meta)
    if idx is None:
        return [], "", []
    try:
        n = int(idx["n"])
        head = idx["head"]
        root = str(idx["root"])
        head_seg, head_sha = str(head["seg"]), str(head["sha"])
    except (KeyError, TypeError, ValueError):
        return None, "index malformed", []
    if idx.get("v") != VERSION:
        return None, f"index version {idx.get('v')!r} unknown", []
    good, bad = _candidates(comments)
    by_start: Dict[Tuple[int, str], List[Dict[str, Any]]] = {}
    for seg in good:
        by_start.setdefault((seg["base"], seg["prev"]), []).append(seg)
    start = root_zero(repo, number)
    stack = [(0, start, [])]
    seen = set()
    deepest = 0
    while stack:
        base, cur, path = stack.pop()
        deepest = max(deepest, base)
        if base == n:
            if cur == root and (not path or (path[-1]["seg"] == head_seg
                                             and path[-1]["sha"] == head_sha)):
                return ([e for s in path for e in s["entries"]], "",
                        [i for s in path for i in s["ids"]])
            continue
        if (base, cur) in seen:
            continue
        seen.add((base, cur))
        for seg in by_start.get((base, cur), []):
            if seg["count"] > 0 and base + seg["count"] <= n:
                stack.append((base + seg["count"], next_root(cur, seg["sha"]), path + [seg]))
    if bad:
        seg, why = next(iter(sorted(bad.items())))
        return None, f"{seg}: {why}", []
    return None, f"{head_seg}: no valid segment continues base {deepest}", []


# -------------------------------------------------------------------- digest

def _class_of(entry: Dict[str, Any]) -> List[str]:
    from . import health
    event = entry.get("event")
    out = []
    if health._progress_event(event):
        out.append("progress")
    if str(event or "").startswith("stage_"):
        out.append("stage")
    if event not in health._NOT_READINESS_EVENTS:
        out.append("readiness")
    return out


def _session_of(entry: Dict[str, Any]) -> str:
    by = entry.get("by") if isinstance(entry.get("by"), dict) else {}
    return str(by.get("session_id") or "")


def build_digest(entries: List[Any]) -> Dict[str, Any]:
    """Fixed-width summary of the archived prefix. Read only when the chain is
    unresolved; recomputed from the verified archive on every archival."""
    counts: Dict[str, List[Any]] = {}
    clocks: Dict[str, str] = {}
    displaced: Dict[str, int] = {}
    legacy_reopen = -1
    legacy_rebound: List[str] = []
    rebound_seen = set()
    host: Optional[str] = None
    for i, e in enumerate(entries):
        if not isinstance(e, dict):
            continue
        kind, at = e.get("event"), str(e.get("at") or "")
        if kind in K_DIGEST:
            slot = counts.setdefault(kind, [0, ""])
            slot[0] += 1
            slot[1] = max(slot[1], at)
        for cls in _class_of(e):
            clocks[cls] = max(clocks.get(cls, ""), at)
        if kind in ("claim", "reopen"):
            sid = _session_of(e)
            if sid:
                displaced.pop(sid_hash(sid), None)  # re-bound itself
                if legacy_reopen >= 0 and sid_hash(sid) not in rebound_seen:
                    rebound_seen.add(sid_hash(sid))
                    legacy_rebound.append(sid_hash(sid))
        if kind == "reopen":
            sid = str(e.get("displaced_session_id") or "")
            if e.get("orphan") is True and sid:
                displaced.pop(sid_hash(sid), None)
                displaced[sid_hash(sid)] = i
            elif not sid and "worker gone" in str(e.get("reason") or "").lower():
                legacy_reopen = i
                legacy_rebound, rebound_seen = [], set()
        if kind == "block":
            by = e.get("by") if isinstance(e.get("by"), dict) else {}
            host = str(e.get("machine") or by.get("machine") or "")
    pairs = sorted(([h, i] for h, i in displaced.items()), key=lambda p: p[1])
    out: Dict[str, Any] = {"cv": CLASSIFIER_VERSION, "counts": counts, "clocks": clocks,
                           "displaced": pairs[-DIGEST_CAP:], "legacy_reopen": legacy_reopen,
                           "legacy_rebound": legacy_rebound[-DIGEST_CAP:]}
    if len(pairs) > DIGEST_CAP:
        out["displaced_overflow"] = True
    if len(legacy_rebound) > DIGEST_CAP:
        out["legacy_rebound_overflow"] = True
    if host is not None and len(json.dumps(host)) <= HOST_MAX_BYTES:
        out["last_block_host"] = host
    elif host is not None:
        out["last_block_host"] = None
        out["last_block_host_overflow"] = True
    return out


def digest_sha(idx: Dict[str, Any]) -> str:
    return sha256_text(json.dumps(
        [idx.get("v"), idx.get("n"), idx.get("head"), idx.get("root"), idx.get("digest")],
        sort_keys=True))


def digest_state(idx: Optional[Dict[str, Any]]) -> str:
    """``ok`` or ``invalid``, for an unresolved chain. An invalid digest is
    never read for any decision."""
    if not idx:
        return "invalid"
    digest, head = idx.get("digest"), idx.get("head")
    if not isinstance(digest, dict) or not isinstance(head, dict):
        return "invalid"
    if idx.get("dsha") != digest_sha(idx) or idx.get("v") != VERSION:
        return "invalid"
    if digest.get("cv") != CLASSIFIER_VERSION:
        return "invalid"  # classified under other rules: its counts mean something else
    try:
        if int(idx["n"]) != int(head["base"]) + int(head["count"]):
            return "invalid"
    except (KeyError, TypeError, ValueError):
        return "invalid"
    return "ok"


def build_index(n: int, head: Segment, digest: Dict[str, Any], ref: str) -> Dict[str, Any]:
    idx: Dict[str, Any] = {
        "v": VERSION, "n": n,
        "head": {"seg": head.key, "sha": head.sha, "base": head.base, "count": head.count,
                 "parts": len(head.parts), "ref": ref},
        "root": head.root, "digest": digest}
    idx["dsha"] = digest_sha(idx)
    size = len(json.dumps(idx))
    if size > INDEX_MAX_BYTES:
        raise ArchiveError(f"history index is {size} bytes, over {INDEX_MAX_BYTES}")
    return idx


# ------------------------------------------------------------------ planning

@dataclass
class Plan:
    move: int
    segments: List[Segment]
    moved: List[Any]
    floored: bool = False

    @property
    def head(self) -> Segment:
        return self.segments[-1]


def tail_bytes(tail: List[Any]) -> int:
    return len(json.dumps(tail))


def needs_archive(tail: List[Any], body_bytes: int, budget: int) -> bool:
    return len(tail) > TAIL_MAX or tail_bytes(tail) > TAIL_MAX_BYTES or body_bytes > budget


def choose_move(tail: List[Any], fixed_bytes: int, budget: int,
                own_count: int) -> Tuple[int, bool]:
    """How many of the oldest tail entries to move, and whether the K_MIN /
    own-entry floor stopped it before all three caps held. ``fixed_bytes`` is
    the body without ``history`` entries or the index."""
    keep = max(K_MIN, own_count)
    limit = max(0, len(tail) - keep)
    sizes = [len(json.dumps(e)) + 2 for e in tail]
    total = sum(sizes)
    move = 0
    if len(tail) > TAIL_MAX:
        move = ((len(tail) - TAIL_TARGET) // TAIL_STEP) * TAIL_STEP
    move = min(move, limit)

    def fits(m: int) -> bool:
        rest = total - sum(sizes[:m])
        return (len(tail) - m <= TAIL_MAX and rest <= TAIL_MAX_BYTES
                and fixed_bytes + rest + INDEX_MAX_BYTES <= budget)

    while move < limit and not fits(move):
        move = min(move + TAIL_STEP, limit)
    return move, not fits(move)


def plan_archive(meta: Dict[str, Any], fixed_bytes: int, view: ArchiveView, repo: str,
                 number: Any, budget: int, own_count: int = 0) -> Optional[Plan]:
    """The archival for this write, or None when nothing needs to move. Requires
    a verified full view of the archive, because the digest covers all of it.
    ``fixed_bytes`` is the body size without history entries or the index."""
    if not view.resolved:
        raise ArchiveError("archive chain unresolved")
    tail = meta.get("history") if isinstance(meta.get("history"), list) else []
    move, floored = choose_move(tail, fixed_bytes, budget, own_count)
    if move <= 0:
        return Plan(0, [], [], floored=True) if floored else None
    idx = index_of(meta)
    prev = str(idx["root"]) if idx else root_zero(repo, number)
    moved = tail[:move]
    return Plan(move, build_segments(number, view.n, moved, prev), moved, floored)


def apply_plan(meta: Dict[str, Any], plan: Plan, view: ArchiveView, ref: str) -> None:
    """Move the plan's entries out of ``meta``. Archival writes only ``history``
    and ``history_archive``."""
    digest = build_digest(list(view.entries) + list(plan.moved))
    meta["history_archive"] = build_index(view.n + plan.move, plan.head, digest, ref)
    meta["history"] = meta["history"][plan.move:]


# ---------------------------------------------------------------- entry keys

def fold_keys(high: int, entries: Iterable[Any], base: str) -> int:
    """High-water fold for one key base: never below ``high``, past every
    landed index. Dense keys give A's count; sparse ones are covered by max."""
    for e in entries:
        key = str(e.get("entry_key") or "") if isinstance(e, dict) else ""
        if key.startswith(base):
            try:
                high = max(high, int(key[len(base):], 16) + 1)
            except ValueError as exc:
                raise ArchiveError(f"archive index invalid; nothing written ({key})") from exc
    return high


def next_key_index(base: str, tail: List[Any], view: ArchiveView) -> int:
    """Resolved: fold(0, archive+tail). Unresolved: fold(n, tail). Since the
    high-water after a prefix of length m is at most m, n bounds every archived
    index, so no landed key is reused under any sequence of modes."""
    if view.resolved:
        return fold_keys(0, itertools.chain(view.entries, tail), base)
    return fold_keys(view.n, tail, base)


# --------------------------------------------------------------------- cache

def cache_root() -> Path:
    return Path(os.environ.get("WATCHTOWER_HISTORY_ARCHIVE_DIR")
                or Path.home() / ".watchtower" / "history-archive")


def cache_path(repo: str, number: Any, root: str) -> Path:
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", repo) or "unknown"
    return cache_root() / safe / str(number) / f"{root}.jsonl"


def cache_store(path: Path, comments: Iterable[Dict[str, Any]]) -> None:
    """Best-effort: the cache is a copy of the comments, re-verified on load,
    safe to delete, and never authoritative."""
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(f".tmp{os.getpid()}")
        tmp.write_text("\n".join(
            json.dumps({k: c.get(k) for k in ("id", "url", "body")}) for c in comments))
        os.replace(tmp, path)
    except OSError:
        pass


def cache_load(path: Path) -> List[Dict[str, Any]]:
    try:
        return [json.loads(line) for line in path.read_text().splitlines() if line]
    except (OSError, ValueError):
        return []

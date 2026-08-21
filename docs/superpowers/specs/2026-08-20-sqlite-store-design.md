# SQLite queue store (Option A from the 2026-08-13 storage discussion)

**Decision** (Codex session `019ffc23`, 2026-08-13): the live JSON store had
grown to 6 MB / 1,793 tickets (94% closed); every claim/progress/close
reparsed and rewrote the whole file under one global lock (measured: list
74 ms, find 453 ms, enqueue 194 ms at ~1,000 events/day). Chosen option:
**A — SQLite as the authoritative local store, JSON demoted to an
import/export format.** CCC needs queue *semantics*, not JSON: it already
prefers `watchtower.queue` as its engine, so the swap happens inside this
module and callers don't change.

## Design

The entire module already funnels storage through three seams —
`_load_unlocked()`, `_save_unlocked()`, `_FileLock` — so the backend swap
happens there and every public function (enqueue/claim/close/timeline/…)
is untouched.

- **DB path**: `_resolve_store_path().with_suffix(".db")` — sits beside the
  legacy JSON (`ux-fixes-queue.db` / `queues.db`). `$WATCHTOWER_STORE`
  semantics unchanged; tests that point it at `x.json` get `x.db` beside it.
- **Backend choice per call**: if the `.db` exists it is authoritative;
  otherwise the JSON file is read (legacy behavior). No process restart or
  config needed to flip — long-running servers pick up the DB the moment it
  appears.
- **Migration is just the first locked save.** `_save_unlocked()` writes to
  SQLite always; when the DB doesn't exist yet it is built from the full
  data dict (which was loaded from JSON) at a temp path and `os.replace`d in
  — atomic vs unlocked readers, and no nested locking (`_FileLock` flock is
  not reentrant). `wt migrate-store` = locked load + save + report. The JSON
  file is left in place, frozen, so path resolution (`legacy exists → legacy`)
  and older readers stay stable.
- **Schema**: `items(number INTEGER PRIMARY KEY, project, ref, status,
  updated_at, item_json)` + `meta(key, value)` holding `counter`,
  `revision`, `schema_version`. `item_json` (canonical
  `sort_keys`/compact dump) is authoritative per item; the other columns are
  denormalized for indexes and ad-hoc `sqlite3` inspection.
- **Diff-based writes**: save re-reads `number → item_json` under the writer
  lock, then upserts only changed/new rows and deletes missing ones. A
  claim/close now writes one row instead of 6 MB.
- **Concurrency**: unchanged model — writers serialize on the existing
  `.lock` flock; SQLite (WAL, busy_timeout) is a second belt. Readers never
  lock, same as today.
- **`revision`** bumps on every save; `queue.revision()` exposes it so CCC
  can replace its mtime/size watcher with a change token later. Until then,
  `os.utime(db)` after each commit keeps mtime-watchers working (WAL commits
  don't touch the main file's mtime).
- **`store_path()`** returns the authoritative file (DB once it exists) —
  CCC's SSE watcher calls this, so it follows automatically.
- **JSON as interchange**: `wt export-json [-o PATH]` dumps the classic
  `{"counter", "items"}` shape. Rollback = export + delete the `.db`.
- **Failure semantics preserved**: corrupt JSON (no DB yet) or corrupt DB →
  `strict=True` raises, otherwise empty store; a corrupt source never
  silently creates/overwrites a DB.

## Live-machine migration order

1. Ship code; restart long-running importers (CCC server) **first** — new
   code still reads JSON while no DB exists.
2. Back up the JSON store, then `wt migrate-store`.
3. Every process flips to the DB on its next call; verify counts + CCC.

## Out of scope (deliberate)

- Indexed SQL query paths for list/find (the dict-shaped API stays; full
  load is ~tens of ms and mutation cost was the real complaint).
- CCC's fallback `ux_fixes_queue.py` (only used where watchtower isn't
  installed — those machines stay on JSON until watchtower arrives).
- Postgres / multi-machine (Option B) — explicitly deferred.

# WT-26: WatchTower as CCC's single queue engine

## Goal

One queue engine, no divergence. CCC bundles WatchTower as a dependency and
delegates queue operations to `watchtower.queue` directly (Python import)
instead of maintaining its own copy. The shared store file path and every
`/api/ux-fixes/*` response shape stay identical — the migration is invisible to
external callers and to the browser UI.

---

## Current state

### Files

| File | Role |
|---|---|
| `~/Apps/claude-command-center/ux_fixes_queue.py` | CCC's own queue engine (661 lines) |
| `~/Apps/watchtower/watchtower/queue.py` | WT's queue engine (569 lines) |

### Shared store

Both engines read and write the same file:
`~/.claude/command-center/ux-fixes-queue.json`

CCC uses a fixed module-level constant `QUEUE_FILE`. WT uses
`_resolve_store_path()` which already falls back to the CCC path when it
exists on disk — so from day one, both processes share state with zero config.

### What's in CCC but not in WT

- `VALID_TYPES`, `VALID_READINESS`, `VALID_PRIORITY`, `VALID_LMH` — extended
  triage dimension constants
- `_prio_rank()` — priority-aware sort (p0..p3, with express lane as legacy p0)
- `update()` — in-place triage field editor (note, priority, readiness, etc.)
- `_norm_choice()` — case-insensitive enum coercion
- `enqueue()` fields: `item_type`, `readiness`, `priority`, `value`,
  `confidence` (triage metadata)
- `claim_next()` params: `item_type`, `shaping` (readiness-gate claim)
- `update_status()` / `close()`: `needs_input`/`block_question`/`blocked_at`
  clear-on-reopen and `closed_by` attribution
- `_SOURCE_PROJECT` / `_REPO_PROJECT` — hardcoded project-code maps

### What's in WT but not in CCC

- `_resolve_store_path()` — dynamic path with env override + CCC legacy fallback
- `block()` — park a ticket for human input (WT's version; CCC sets `needs_input`
  inside `update_status` instead)
- `queues()` — per-queue counts dict
- `list_blocked()` — filter for `needs_input` tickets
- `last_progress_iso()` — ground-truth "did a worker make progress" signal
- `_normalize_resolution()` + `resolution` field on `close()` — structured HOW-
  it-was-fixed record
- `close()` accepts a `resolution` kwarg

### Functions that are functionally identical

- `answer()` — same shape and behavior in both; safe Phase 1 shim target
- `enqueue()` core — same item shape (triage extras are additive/optional)
- `list_items()`, `get()`, `claim_next()` core, `update_status()` core,
  `close()` core, `next_item()`

### Risk factors

1. **Triage fields gap**: WT's `enqueue()` doesn't accept `item_type`,
   `readiness`, `priority`, `value`, `confidence`. CCC's annotate endpoint
   passes these. Until WT gains them, Phase 2 (full delegation of `enqueue`)
   requires WT to absorb these params or CCC wraps the call.

2. **`_project_for` divergence**: CCC has hardcoded `_SOURCE_PROJECT` /
   `_REPO_PROJECT` maps (e.g. `claude-command-center` → `CCC`). WT derives the
   code generically from the basename. Items enqueued via WT will get different
   project codes unless WT adopts the same maps or CCC passes `project=` explicitly.

3. **`claim_next` readiness gate**: CCC's version refuses to claim `needs-shaping`
   / `needs-spec` items unless `shaping=True`. WT's version has no such gate.
   Workers that call `/api/ux-fixes/claim` depend on this filter.

4. **`close()` / `update_status()` signature**: WT's `close()` accepts a
   `resolution` kwarg; CCC's doesn't. Backwards compatible (additive), but CCC
   endpoints will need to pass it through once WT is the engine.

5. **`block()` semantics**: WT has a standalone `block()` function. CCC sets
   `needs_input` inline inside `update_status`. They converge on the same store
   shape (both write `needs_input`, `block_question`, `blocked_at`), but CCC
   has no `/api/ux-fixes/block` endpoint yet.

---

## Migration phases

### Phase 0 — Install path (LOW RISK, do now)

Make WT importable inside CCC's Python environment. CCC's `install.sh` (and
`run.sh`) don't set up a venv — they rely on system Python. The install step is:

```bash
pip install -e ~/Apps/watchtower --quiet   # WT-26: bundle WT as CCC's queue engine
```

Added to `install.sh` after `sync_repo`, before `launch_server`. Wrapped in a
guard: if neither `~/Apps/watchtower` nor `~/dev/watchtower` exists, print a
warning but continue (CCC still works via `ux_fixes_queue`).

**Deliverable**: `scripts/install.sh` updated. WT is importable.

### Phase 1 — Shim `answer()` (LOW RISK, do now)

Replace `ux_fixes_queue.answer(...)` in the `/api/ux-fixes/answer` endpoint with
a try/except that prefers `watchtower.queue.answer` and falls back to
`ux_fixes_queue.answer`. Both functions have the same signature and write the
same store shape, so interop is guaranteed.

```python
try:
    from watchtower.queue import answer as _wt_answer
    _queue_answer = _wt_answer
except ImportError:
    _queue_answer = ux_fixes_queue.answer
```

This proves the shared-store round-trip works in production before committing to
a broader migration.

**Deliverable**: `server.py` updated for the `answer` endpoint.

### Phase 2 — Full delegation (MEDIUM RISK, next sprint)

Prerequisites: WT's `enqueue()` absorbs the triage fields (item_type, readiness,
priority, value, confidence), and `_project_for` / `_SOURCE_PROJECT` maps are
aligned.

Steps:
1. Add triage params to `watchtower.queue.enqueue()`.
2. Align `_project_for` maps (copy `_SOURCE_PROJECT`/`_REPO_PROJECT` into WT or
   make them configurable).
3. Add `shaping` + `item_type` params to `watchtower.queue.claim_next()`.
4. Replace all `ux_fixes_queue.*` calls in `server.py` with `watchtower.queue.*`.
5. Run the smoke suite and a full queue round-trip against the live store.

**Deliverable**: every `server.py` queue call goes through `watchtower.queue`.
`ux_fixes_queue.py` is kept but no longer called — deprecated in-file.

### Phase 3 — Remove `ux_fixes_queue.py` (LOW RISK, after Phase 2 soaks)

After Phase 2 runs cleanly in production for one sprint:
- Delete `ux_fixes_queue.py` from the CCC repo.
- Remove the `import ux_fixes_queue` line from `server.py`.
- Update smoke tests to remove `ux_fixes_queue` import assertions.

**Deliverable**: WT is the only queue engine. Repo divergence is gone.

---

## Back-compat guarantees (all phases)

- **Store file path**: `~/.claude/command-center/ux-fixes-queue.json` — never
  changes. WT's `_resolve_store_path()` already defaults to this path when it
  exists.
- **Item shape**: additive fields only. Existing items gain `resolution` and
  richer triage fields when touched, but old readers see the same keys they
  already expect.
- **`/api/ux-fixes/*` response shape**: unchanged. The HTTP layer in `server.py`
  controls serialization; the engine is an internal detail.
- **`wt` CLI**: continues to work against the same store file. Workers using the
  CLI are unaffected.

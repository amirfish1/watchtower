# Durable Worker Session Names Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make WT-created worker sessions keep meaningful names and backfill recent worker transcripts.

**Architecture:** Remove the static Claude spawn name that overwrites WT renames. Centralize ticket-aware title formatting in `watchtower.workers`, reuse it from claim/close/backfill paths, and expose a dry-runnable CLI backfill command.

**Tech Stack:** Python 3.11, stdlib-only runtime, pytest.

## Global Constraints

- Runtime remains stdlib-only.
- No production behavior change without a failing pytest first.
- Missing logs or transcripts are best-effort no-ops.
- Backfill defaults to the past 24 hours.
- Do not hand-edit Claude transcript files outside the tested `messages.set_session_title` path.

---

### Task 1: Stop Generic Spawn Names

**Files:**
- Modify: `tests/test_smoke.py`
- Modify: `watchtower/workers.py`

**Interfaces:**
- Consumes: `workers.build_drain_command(queue, engine, worker_id, repo_path="") -> list[str]`
- Produces: Claude worker argv without `--name`.

- [ ] Write a failing assertion in `test_spawn_worker_dry_run`:

```python
assert "--name" not in s["argv"]
assert f"{s['queue']} queue worker" not in " ".join(s["argv"])
```

- [ ] Run:

```bash
pytest -q tests/test_smoke.py::test_spawn_worker_dry_run
```

Expected before implementation: failure because `--name` is still present.

- [ ] Remove the `--name`, `f"{queue} queue worker"` pair from the Claude argv in `workers.build_drain_command`.

- [ ] Re-run the same pytest command and confirm it passes.

### Task 2: Ticket-Aware Canonical Titles

**Files:**
- Modify: `tests/test_smoke.py`
- Modify: `tests/test_messages.py`
- Modify: `watchtower/workers.py`
- Modify: `watchtower/cli.py`

**Interfaces:**
- Produces: `workers.ticket_context(item: dict, summary: str = "") -> str`
- Produces: `workers.display_name(queue: str, ref: str | None = None, summary: str | None = None) -> str`

- [ ] Update lifecycle tests so in-progress names include ticket title:

```python
assert rows[0]["display_name"] == f"NAMEQ worker: {item['ref']} - fix the thing"
```

- [ ] Update `test_backfill_renames_session_once_uuid_resolves` to expect:

```python
f"NAMEQ worker: {item['ref']} - do a thing"
```

- [ ] Run both targeted tests and confirm they fail on the old behavior.

- [ ] Add `ticket_context(item, summary="")` in `workers.py`: use non-empty `summary`, else `title`, else `note`, clipped to 60 chars.

- [ ] Update `annotate_activity`, `backfill_claimed_session_ids`, and `_rename_claiming_session` to pass the context string into `display_name`.

- [ ] Re-run targeted tests and confirm they pass.

### Task 3: Recent Worker Backfill

**Files:**
- Modify: `tests/test_messages.py`
- Modify: `watchtower/workers.py`
- Modify: `watchtower/cli.py`

**Interfaces:**
- Produces: `workers.backfill_recent_session_titles(hours: float = 24.0, dry_run: bool = False) -> list[dict]`
- Produces CLI: `wt session-names backfill [--hours N] [--dry-run]`

- [ ] Add a test that creates two worker log files, one recent and one stale, each with a session id, and two claimed/closed tickets. Assert dry-run returns only the recent title plan.

- [ ] Add a test that writes a fake transcript for the recent session and asserts non-dry-run appends the final canonical `custom-title`.

- [ ] Run the new tests and confirm they fail because the function/CLI does not exist.

- [ ] Implement `backfill_recent_session_titles`: scan `WORKERS_FILE.parent / "logs"` for `*.log`, keep files modified within `hours`, resolve session ids with `resolve_session_id_from_log`, derive worker id from filename stem, find the newest closed ticket by `closed_at` else active ticket by `claimed_at`, and call `messages.set_session_title` unless dry-run.

- [ ] Add hidden maintenance parser command `session-names backfill` with `--hours` and `--dry-run`, printing JSON records.

- [ ] Re-run the new tests and confirm they pass.

### Task 4: Verification And Live Backfill

**Files:**
- Modify: `docs/session-naming.md`

**Interfaces:**
- Consumes: `wt session-names backfill --hours 24 --dry-run`
- Consumes: `wt session-names backfill --hours 24`

- [ ] Run:

```bash
pytest -q
```

- [ ] Run:

```bash
wt session-names backfill --hours 24 --dry-run
```

Review the JSON for expected recent worker sessions.

- [ ] Run:

```bash
wt session-names backfill --hours 24
```

- [ ] Inspect representative recent transcripts for final `custom-title`
events containing `worker: <REF> - <context>`.

- [ ] Update `docs/session-naming.md` with the durable-name fix and backfill
command.

# Graceful Worker Engine and Model Rotation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Gracefully retire workers whose engine or model no longer matches a queue's configuration.

**Architecture:** A worker selector will reuse the existing stop sentinel so an active ticket finishes before the worker exits its drain loop. The CLI exposes direct engine-based retirement and invokes mismatch retirement after queue configuration writes.

**Tech Stack:** Python 3.11, argparse, pytest, WatchTower JSON stores.

## Global Constraints

- Never kill, interrupt, or reopen a current ticket.
- Exclude adhoc, dead, already-released, and nonmatching workers.
- Use `request_stop(worker_id)` for durable next-claim semantics.

### Task 1: Add lifecycle selection and regression coverage

**Files:**

- Modify: `watchtower/workers.py`
- Test: `tests/test_workers_lifecycle.py`

**Interfaces:** Produces `release_workers(engine: str = "", queue: str = "", mismatched: bool = False) -> List[Dict[str, Any]]`.

- [ ] Write tests proving engine selection only releases matching live non-adhoc workers, preserves an active ticket, and makes the worker's following claim return `{"stop": True}`.
- [ ] Run `pytest tests/test_workers_lifecycle.py -k 'release_workers_by_engine or release_mismatched_workers' -v`; it must fail because the function is absent.
- [ ] Implement `release_workers` by iterating `list_workers(prune=False)`, comparing to `config.engine(queue)` and `config.model(queue)` in mismatch mode, calling `request_stop`, and returning only successfully released records.
- [ ] Re-run the focused tests; they must pass.
- [ ] Commit `watchtower/workers.py` and `tests/test_workers_lifecycle.py` with `feat(workers): add graceful worker retirement`.

### Task 2: Expose rotation and wire configuration changes

**Files:**

- Modify: `watchtower/cli.py`
- Modify: `tests/test_queue_settings.py`
- Modify: `tests/test_argparse_ux.py`
- Modify: `README.md`

**Interfaces:** Consumes `workers.release_workers(engine=..., queue=...)` and `workers.release_workers(queue=..., mismatched=True)`.

- [ ] Write tests for `wt workers release --engine claude [--queue Q] [--json]` and for automatic release when `wt set` or `wt config` changes either `--engine` or `--model`.
- [ ] Run `pytest tests/test_queue_settings.py tests/test_argparse_ux.py -k 'release_engine or retires_mismatched' -v`; it must fail before parser/wiring exists.
- [ ] Add a `workers` subparser with a `release` subcommand. Its handler reports released worker IDs (or an explicit no-match result) and JSON mode. After each successful engine/model config write, invoke mismatch release for that queue.
- [ ] Document the command and automatic graceful rotation in the README.
- [ ] Re-run the focused tests; they must pass.
- [ ] Commit the CLI, docs, and tests with `feat(cli): rotate stale workers after settings changes`.

### Task 3: Verify and rotate live stale workers

**Files:**

- Verify: `watchtower/workers.py`, `watchtower/cli.py`, affected tests, and `README.md`

- [ ] Run `pytest tests/test_workers_lifecycle.py tests/test_queue_settings.py tests/test_argparse_ux.py -v` and then `pytest -q`; both must pass.
- [ ] Inspect `wt workers --json`, compare each live queue worker to its effective queue engine/model, and invoke graceful retirement for every mismatch.
- [ ] Confirm released workers retain any current in-progress tickets and are excluded from staffing.

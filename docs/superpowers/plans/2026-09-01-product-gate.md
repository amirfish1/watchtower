# Product Gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A per-queue "product gate": workers on opted-in queues must post a decision-grade pitch after minimal diagnosis and stop until the user Acks (continue), Nacks (icebox), or Closes (declined).

**Architecture:** The gate-pending state rides the existing `wt block` machinery with a new `block_kind: "input"|"rationale"` field (no new status). The icebox is a new `readiness` value `needs-rationale` (unclaimable). Ack is the phase-2 go signal delivered through the existing `wt answer` steer/resume path. A hard backstop in `queue.close()` refuses ungated implemented closes on gated queues.

**Tech Stack:** Python 3 (stdlib argparse CLI, sqlite-backed store behind `_load_unlocked`/`_save_unlocked`), pytest with the `wt_env`/`run_cli` fixtures from `tests/conftest.py`, vanilla-JS dashboards (watchtower `dashboard.py` generated HTML; CCC `static/app.js` + `static/q2.js`).

**Spec:** `docs/superpowers/specs/2026-09-01-product-gate-design.md` — read it first; every task below implements a section of it.

## Global Constraints

- Repos: Tasks 1–7 in `/Users/amirfish/Apps/watchtower`; Task 8 in `/Users/amirfish/Apps/claude-command-center`. Both shared clones on `main` — never branch.
- Git: commit early and often; stage by explicit path only (never `git add -A`/`.`/`-a`); commit with `git commit --only <paths> -m "..."`.
- v1 excludes GitHub-backed queues: never forward new fields to `github_backend`; `wt config --product-gate on` on a `backend=github` queue prints a warning.
- The pending state is `block_kind`, NOT a new status — `VALID_STATUSES` is untouched.
- Timeline vocabulary (small deviation from the spec, agreed): the pitch is recorded as the existing `block` history event carrying a `kind` field (so existing UIs keep rendering it); only `gate_ack` and `gate_nack` are new event names. There is no separate `gate_pitch` event.
- `product_ack` persists across reopen; `block_kind` is cleared on reopen like `block_question`.
- Run the affected test file after each task; run the full suite (`python -m pytest tests/ -x -q`) at the end of Task 7.

## File Structure

| File | Change |
|---|---|
| `watchtower/cli.py` | rename `ack`→`unresolved-ack`; new `ack` (gate), `nack`, `gated`; `--kind` on block; `--pre-ack` on add; `--product-gate` on config; readiness choices += `needs-rationale`; extract `_deliver_to_blocked_session` from `cmd_answer` |
| `watchtower/config.py` | `set_product_gate` / `product_gate` |
| `watchtower/queue.py` | `block_kind` on block(); `gate_ack()` / `gate_nack()`; close guard; `pre_ack` on enqueue; readiness constants; notify verb; event precedence |
| `watchtower/workers.py` | `PRODUCT_GATE_CONTRACT` appended to both goal templates when gated |
| `watchtower/dashboard.py` | gated chip + Ack/Nack buttons + `/api/ticket/<ref>/gate-ack` & `/gate-nack` routes |
| `tests/test_product_gate.py` | new suite |
| `tests/test_queue_settings.py` | `--product-gate` row in `SETTINGS_MATRIX` |
| `tests/test_resolution_ack.py` | command rename |
| CCC `server.py`, `static/app.js`, `static/q2.js` | gate endpoints, chips/buttons, settings toggle (Task 8) |

---

### Task 1: Rename `wt ack` → `wt unresolved-ack`

Frees the name "ack" for the gate decision. Today's `wt ack` acknowledges caveat/follow-up/unresolved chips on *closed* tickets and pairs with `wt unresolved`.

**Files:**
- Modify: `watchtower/cli.py` (handler `cmd_ack` at :1117, `_cmd_ack_bulk` at :1049, parser at :4128-4154, `COMMAND_HELP` dict)
- Test: `tests/test_resolution_ack.py`

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: the CLI name `unresolved-ack` (parser `sub.add_parser("unresolved-ack", ...)` bound to handler renamed `cmd_unresolved_ack`). After this task `wt ack` does not exist (Task 5 re-adds it with gate semantics). Core `queue.ack_resolution()` keeps its name.

- [ ] **Step 1: Update the existing tests to the new name**

In `tests/test_resolution_ack.py`, replace every CLI invocation of the command `"ack"` with `"unresolved-ack"` (only the subcommand token in `run_cli(...)`-style calls / argv lists — do not touch `queue.ack_resolution` API calls or `_ack` helper names). Add one new test:

```python
def test_old_ack_name_is_gone_until_gate_lands(wt_env, run_cli):
    """`wt ack` was renamed; between Task 1 and Task 5 it must not silently
    keep the old resolution-ack behavior."""
    res = run_cli("ack", "X-1", "--all")
    assert res.code != 0
```

If this file does not use the `wt_env`/`run_cli` fixtures (it may build argv directly), follow the file's own established invocation pattern for both the renames and the new test.

- [ ] **Step 2: Run to verify the suite fails**

Run: `python -m pytest tests/test_resolution_ack.py -x -q`
Expected: FAIL (CLI still only knows `ack`, not `unresolved-ack`).

- [ ] **Step 3: Rename in cli.py**

1. Rename `def cmd_ack(` (:1117) to `def cmd_unresolved_ack(` and `def _cmd_ack_bulk(` to `def _cmd_unresolved_ack_bulk(` (update the internal call at :1128 and the docstrings' `wt ack` mentions to `wt unresolved-ack`).
2. In `_print_resolution_items` (:1012), change the hint line to `wt unresolved-ack <ref> --unresolved N   (or --all)`.
3. Parser (:4128): `s = sub.add_parser("unresolved-ack", help=COMMAND_HELP.get("unresolved-ack", ""))`, `s.set_defaults(func=cmd_unresolved_ack)`. Update the `COMMAND_HELP` dict key `"ack"` → `"unresolved-ack"` (search `"ack":` near the `"edit":` entry at ~:3727).
4. `rg -n '"ack"|wt ack' watchtower/` and fix any other reference (e.g. help epilogs, `workers.py` runbook text) to the new name.

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/test_resolution_ack.py tests/test_argparse_ux.py -q`
Expected: PASS (fix any argparse-UX snapshot that lists commands, following that test's update convention).

- [ ] **Step 5: Commit**

```bash
git commit --only watchtower/cli.py tests/test_resolution_ack.py -m "refactor: rename wt ack to wt unresolved-ack (frees ack for the product gate)"
```
(Include any other file you had to touch in Step 3/4 in the `--only` list.)

---

### Task 2: `product_gate` queue setting

**Files:**
- Modify: `watchtower/config.py` (add after `auto_drain`, ~:255)
- Modify: `watchtower/cli.py` (config parser :4504-4537, `cmd_config` :2719)
- Test: `tests/test_queue_settings.py`

**Interfaces:**
- Produces: `config.set_product_gate(queue: str, enabled: bool) -> Dict[str, Any]` and `config.product_gate(queue: str) -> bool` (default False). CLI flag `wt config -q Q --product-gate on|off`. Tasks 4, 6, 7, 8 read `config.product_gate(queue)`.

- [ ] **Step 1: Write the failing test**

In `tests/test_queue_settings.py`, add to `SETTINGS_MATRIX` (:127):

```python
    ("--product-gate", "on", "product_gate", True),
```

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/test_queue_settings.py -q -k product_gate`
Expected: FAIL (unrecognized argument `--product-gate`).

- [ ] **Step 3: Implement**

`watchtower/config.py`, directly after the `auto_drain()` function (:254), following the `set_auto_drain`/`auto_drain` pattern exactly:

```python
def set_product_gate(queue: str, enabled: bool) -> Dict[str, Any]:
    data = _load()
    q = data.setdefault(queue, {})
    q["product_gate"] = bool(enabled)
    _save(data)
    return q


def product_gate(queue: str) -> bool:
    """False unless explicitly opted in. When on, workers must post a
    decision-grade pitch (wt block --kind rationale) and wait for a human
    Ack before implementing — see the 2026-09-01 product-gate design."""
    return bool(_queue_entry(queue).get("product_gate", False))
```

`watchtower/cli.py` config parser (after the `--grace-s` argument, :4536):

```python
    s.add_argument("--product-gate", default=None, choices=["on", "off"],
                   dest="product_gate",
                   help="on = workers must get a human Ack (wt ack) after a "
                        "minimal-diagnosis pitch before implementing")
```

`cmd_config` (after the `grace_s` block, :2797):

```python
    if getattr(args, "product_gate", None) is not None:
        enabled = args.product_gate == "on"
        if enabled and config.backend(args.queue) == "github":
            print("warning: product_gate is not enforced on GitHub-backed "
                  "queues yet (v1 gates file-backed queues only)",
                  file=sys.stderr)
        config.set_product_gate(args.queue, enabled)
        changed.append(f"product_gate={'on' if enabled else 'off'}")
```

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/test_queue_settings.py -q`
Expected: PASS (both parametrized round-trip tests pick up the new row).

- [ ] **Step 5: Commit**

```bash
git commit --only watchtower/config.py watchtower/cli.py tests/test_queue_settings.py -m "feat: product_gate queue setting (wt config --product-gate on|off)"
```

---

### Task 3: Data model — block kind, needs-rationale readiness, pre_ack

**Files:**
- Modify: `watchtower/queue.py` (constants :125-134, `_EVENT_PRECEDENCE` :796, `_NOTIFY_VERBS` :925, `enqueue` :1030-1141, `update_status` open-branch :2010-2023, `block` :2246, `answer` reopen-branch :2344)
- Modify: `watchtower/cli.py` (add/import common args :3979, edit parser :4049, block parser :4166, `cmd_add` :541/:570)
- Test: `tests/test_product_gate.py` (create)

**Interfaces:**
- Produces:
  - `queue.block(ident, session_id="", question="", progress="", kind="input")` — stores `item["block_kind"]` (`"input"` or `"rationale"`); rationale blocks notify with verb "awaits product decision".
  - `queue.enqueue(..., pre_ack: bool = False)` — stores `item["pre_ack"]` (bool).
  - Readiness value `"needs-rationale"` valid and unclaimable.
  - CLI: `wt block --kind {input,rationale}`, `wt add --pre-ack`, `--readiness needs-rationale` accepted by add/import/edit.
- Consumed by Tasks 4–8.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_product_gate.py`:

```python
"""Product gate (2026-09-01 design): block kinds, the needs-rationale icebox,
pre-ack, gate ack/nack, and the close guard."""
import json

import pytest

QUEUE = "GATEQ"


def _file_ticket(wt_env, **kw):
    return wt_env.queue.enqueue(project=QUEUE, note=kw.pop("note", "a bug"), **kw)


# ---------------------------------------------------------------- data model

def test_block_kind_defaults_to_input(wt_env):
    it = _file_ticket(wt_env)
    blocked = wt_env.queue.block(it["ref"], session_id="w1", question="which db?")
    assert blocked["block_kind"] == "input"
    assert blocked["needs_input"] is True


def test_block_kind_rationale_is_stored_and_survives_reload(wt_env):
    it = _file_ticket(wt_env)
    wt_env.queue.block(it["ref"], session_id="w1",
                       question="PITCH: worth fixing?", kind="rationale")
    fresh = wt_env.queue.get(it["ref"])
    assert fresh["block_kind"] == "rationale"
    assert fresh["block_question"] == "PITCH: worth fixing?"


def test_unknown_block_kind_degrades_to_input(wt_env):
    it = _file_ticket(wt_env)
    blocked = wt_env.queue.block(it["ref"], session_id="w1",
                                 question="q", kind="bogus")
    assert blocked["block_kind"] == "input"


def test_reopen_clears_block_kind(wt_env):
    it = _file_ticket(wt_env)
    wt_env.queue.block(it["ref"], session_id="w1", question="q", kind="rationale")
    wt_env.queue.update_status(it["ref"], "open")
    fresh = wt_env.queue.get(it["ref"])
    assert fresh["block_kind"] == ""
    assert fresh["needs_input"] is False


def test_needs_rationale_is_valid_and_unclaimable(wt_env):
    assert "needs-rationale" in wt_env.queue.VALID_READINESS
    assert "needs-rationale" in wt_env.queue.UNCLAIMABLE_READINESS
    _file_ticket(wt_env, readiness="needs-rationale")
    assert wt_env.queue.claim_next(QUEUE, "w1") is None


def test_enqueue_pre_ack(wt_env):
    it = _file_ticket(wt_env, pre_ack=True)
    assert it["pre_ack"] is True
    assert _file_ticket(wt_env)["pre_ack"] is False


# ---------------------------------------------------------------------- CLI

def test_wt_add_pre_ack_and_block_kind(wt_env, run_cli):
    res = run_cli("add", "-q", QUEUE, "--note", "ship the widget", "--pre-ack")
    assert res.code == 0, res.output
    items = wt_env.queue.list_items(project=QUEUE)
    assert items[-1]["pre_ack"] is True
    ref = items[-1]["ref"]
    res = run_cli("block", ref, "--worker", "w1",
                  "--kind", "rationale", "--question", "PITCH: worth it?")
    assert res.code == 0, res.output
    assert wt_env.queue.get(ref)["block_kind"] == "rationale"
```

(If `run_cli` in `tests/conftest.py` uses a different signature than positional argv strings, follow `tests/test_queue_settings.py`'s usage exactly — it is the reference consumer of these fixtures. Same for how `wt_env.queue` exposes the module.)

- [ ] **Step 2: Run to verify they fail**

Run: `python -m pytest tests/test_product_gate.py -q`
Expected: FAIL (`unexpected keyword argument 'kind'` / `'pre_ack'`, missing `--pre-ack`, `needs-rationale` not in `VALID_READINESS`).

- [ ] **Step 3: Implement in queue.py**

1. Constants (:125, :134):

```python
VALID_READINESS = ("ready", "needs-shaping", "needs-spec", "needs-rationale", "")
```
```python
UNCLAIMABLE_READINESS = ("needs-shaping", "needs-spec", "needs-rationale")
```
Extend the comment above `UNCLAIMABLE_READINESS` with one line: `needs-rationale` is the product-gate icebox (a human Nacked; revival needs a new rationale — see the 2026-09-01 design).

2. `_EVENT_PRECEDENCE` (:796) — add `"gate_ack": 3, "gate_nack": 3,` (same tier as `answer`; used by Task 4).

3. `_NOTIFY_VERBS` (:925) — add `"awaits_decision": "awaits product decision",`.

4. `enqueue` — add keyword param `pre_ack: bool = False` to the signature (:1050 region) and `"pre_ack": bool(pre_ack),` to the item dict next to `"run_requested": False,` (:1126). Do NOT forward `pre_ack` to `backend.enqueue(...)` (v1 excludes GitHub-backed queues).

5. `block` — signature becomes `def block(ident, session_id="", question="", progress="", kind="input")`. At the top of the function body: `kind = kind if kind in ("input", "rationale") else "input"`. In the file-backed branch, next to `it["block_question"] = ...` (:2280) add `it["block_kind"] = kind`. Pass `kind=kind` into the `_append_history(it, "block", ...)` call (:2293) as an extra keyword. Change the final notify call (:2306) to:

```python
                _notify_ticket_event(
                    it,
                    "awaits_decision" if kind == "rationale" else "needs_input",
                    detail=question, actor=session_id,
                )
```

The GitHub-backend branch (:2264-2273) keeps its existing behavior (ignore `kind` there; do not forward it).

6. Reopen paths clear the kind: in `update_status`'s `if status == "open":` branch (:2020-2022), after `it["block_question"] = ""` add `it["block_kind"] = ""`. In `answer()`'s no-resumable-session branch (:2344-2349), after `it["block_question"] = ""` add `it["block_kind"] = ""`.

- [ ] **Step 4: Implement in cli.py**

1. Block parser (:4166-4174): add

```python
    s.add_argument("--kind", default="input", choices=["input", "rationale"],
                   help="input = implementation question; rationale = product-"
                        "gate pitch awaiting a human Ack/Nack (wt ack / wt nack)")
```

and pass it through in `cmd_block` (:1198): `kind=args.kind`.

2. Readiness choices: in the shared add/import args block (:3979) and the edit parser (:4049), add `"needs-rationale"` to the choices lists.

3. `wt add --pre-ack`: in the same shared add/import args block, add

```python
        subparser.add_argument("--pre-ack", action="store_true", dest="pre_ack",
                               help="skip the product gate for this ticket "
                                    "(the decision to build it is already made)")
```

and in `cmd_add`'s `q.enqueue(...)` call (:570 region) pass `pre_ack=bool(getattr(args, "pre_ack", False))`.

- [ ] **Step 5: Run to verify they pass**

Run: `python -m pytest tests/test_product_gate.py tests/test_queue.py -q`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git commit --only watchtower/queue.py watchtower/cli.py tests/test_product_gate.py -m "feat: block kinds, needs-rationale icebox readiness, pre_ack (product gate data model)"
```

---

### Task 4: Gate core — `gate_ack()`, `gate_nack()`, close guard

**Files:**
- Modify: `watchtower/queue.py` (new functions after `answer()` ~:2365; `close()` :2045)
- Test: `tests/test_product_gate.py` (extend)

**Interfaces:**
- Consumes: Task 3's `block_kind`, `pre_ack`, `needs-rationale`.
- Produces:
  - `queue.gate_ack(ident, comment: str = "", by: str = "human", session_id: str = "") -> Optional[Dict]` — raises `ValueError` unless the ticket is rationale-blocked; clears the block, records `product_ack={"by","at","comment"}` and a `gate_ack` history event. Returns the updated item (or None if no match).
  - `queue.gate_nack(ident, reason: str, by: str = "human", session_id: str = "", close: bool = False) -> Optional[Dict]` — requires rationale-blocked and a non-empty reason. Default: icebox (needs_input cleared, claim released, `readiness="needs-rationale"`, `product_nack={"by","at","comment"}`, `gate_nack` event). `close=True`: `gate_nack` event then closed via `close(..., declined=True, force=True)` with resolution summary `Declined at product gate: <reason>`.
  - `queue.close(..., declined: bool = False)` — new keyword; the guard: on a `product_gate` queue a close with `declined=False` and `force=False` raises `ValueError` unless the ticket has `product_ack` or `pre_ack`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_product_gate.py`:

```python
# ----------------------------------------------------------------- gate core

def _gated_pitch(wt_env, **enqueue_kw):
    wt_env.config.set_product_gate(QUEUE, True)
    it = _file_ticket(wt_env, **enqueue_kw)
    wt_env.queue.claim_by_ref(QUEUE, it["ref"], "w1")
    wt_env.queue.block(it["ref"], session_id="w1",
                       question="PITCH: costs 2k tokens/day", kind="rationale")
    return wt_env.queue.get(it["ref"])


def test_gate_ack_records_decision_and_clears_block(wt_env):
    it = _gated_pitch(wt_env)
    out = wt_env.queue.gate_ack(it["ref"], comment="go, but keep it small",
                                by="amir")
    assert out["needs_input"] is False
    assert out["product_ack"]["by"] == "amir"
    assert out["product_ack"]["comment"] == "go, but keep it small"
    assert out["status"] == "in_progress"  # still bound to its session
    assert any(e.get("event") == "gate_ack"
               for e in wt_env.queue.timeline(out))


def test_gate_ack_requires_a_rationale_block(wt_env):
    it = _file_ticket(wt_env)
    with pytest.raises(ValueError):
        wt_env.queue.gate_ack(it["ref"])
    wt_env.queue.block(it["ref"], session_id="w1", question="q")  # kind=input
    with pytest.raises(ValueError):
        wt_env.queue.gate_ack(it["ref"])


def test_gate_nack_iceboxes(wt_env):
    it = _gated_pitch(wt_env)
    out = wt_env.queue.gate_nack(it["ref"], reason="not this quarter", by="amir")
    assert out["status"] == "open"
    assert out["claimed_by"] is None
    assert out["needs_input"] is False
    assert out["readiness"] == "needs-rationale"
    assert out["product_nack"]["comment"] == "not this quarter"
    assert wt_env.queue.claim_next(QUEUE, "w2") is None  # unclaimable


def test_gate_nack_requires_a_reason(wt_env):
    it = _gated_pitch(wt_env)
    with pytest.raises(ValueError):
        wt_env.queue.gate_nack(it["ref"], reason="")


def test_gate_nack_close_declines(wt_env):
    it = _gated_pitch(wt_env)
    out = wt_env.queue.gate_nack(it["ref"], reason="wrong product direction",
                                 by="amir", close=True)
    assert out["status"] == "closed"
    assert "Declined at product gate" in (out.get("resolution") or {}).get("summary", "")


def test_close_guard_refuses_ungated_implemented_close(wt_env):
    it = _gated_pitch(wt_env)
    with pytest.raises(ValueError):
        wt_env.queue.close(it["ref"], "w1",
                           resolution={"summary": "implemented it anyway"})


def test_close_guard_allows_acked_pre_acked_and_ungated_queues(wt_env):
    it = _gated_pitch(wt_env)
    wt_env.queue.gate_ack(it["ref"], by="amir")
    assert wt_env.queue.close(it["ref"], "w1",
                              resolution={"summary": "done"})["status"] == "closed"

    it2 = _gated_pitch(wt_env, pre_ack=True)
    assert wt_env.queue.close(it2["ref"], "w1",
                              resolution={"summary": "done"})["status"] == "closed"

    wt_env.config.set_product_gate(QUEUE, False)
    it3 = _file_ticket(wt_env)
    wt_env.queue.claim_by_ref(QUEUE, it3["ref"], "w1")
    assert wt_env.queue.close(it3["ref"], "w1",
                              resolution={"summary": "done"})["status"] == "closed"


def test_ack_persists_across_reopen(wt_env):
    it = _gated_pitch(wt_env)
    wt_env.queue.gate_ack(it["ref"], by="amir")
    wt_env.queue.close(it["ref"], "w1", resolution={"summary": "done"})
    wt_env.queue.update_status(it["ref"], "open")
    fresh = wt_env.queue.get(it["ref"])
    assert fresh["product_ack"]["by"] == "amir"
    wt_env.queue.claim_by_ref(QUEUE, fresh["ref"], "w2")
    assert wt_env.queue.close(fresh["ref"], "w2",
                              resolution={"summary": "redone"})["status"] == "closed"
```

(`claim_by_ref`'s exact signature: check its definition at `queue.py:1725` and adjust the two-arg/three-arg order in the helper if needed — it is `claim_by_ref(project, ref, worker...)`-shaped; mirror how `tests/test_queue.py` calls it.)

- [ ] **Step 2: Run to verify they fail**

Run: `python -m pytest tests/test_product_gate.py -q`
Expected: new tests FAIL (`gate_ack` does not exist).

- [ ] **Step 3: Implement `gate_ack` / `gate_nack` in queue.py**

Insert after `answer()` (~:2365):

```python
def _require_rationale_block(it: Dict[str, Any], ident: Any) -> None:
    if not it.get("needs_input") or it.get("block_kind") != "rationale":
        status = it.get("status")
        hint = (
            "it is closed — resolution-caveat acks moved to `wt unresolved-ack`"
            if status == "closed" else
            f"it is {status} with block_kind="
            f"{it.get('block_kind') or '(none)'} — the gate applies only to a "
            f"ticket a worker parked with `wt block --kind rationale`"
        )
        raise ValueError(
            f"{it.get('ref', ident)} is not awaiting a product decision: {hint}"
        )


def gate_ack(
    ident: Any, comment: str = "", by: str = "human", session_id: str = "",
) -> Optional[Dict[str, Any]]:
    """Approve a product-gate pitch: clear the block and record the decision.

    The decision survives reopen (``product_ack`` is never cleared by the
    reopen path), so a ticket approved once is never re-gated. Delivery of
    the go-signal to the parked worker is the CLI/CCC layer's job (same
    steer/resume path as ``wt answer``); this function only owns state."""
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        for it in data["items"]:
            if not _matches(it, ident):
                continue
            _require_rationale_block(it, ident)
            now = _now_iso()
            it["needs_input"] = False
            it["answered_at"] = now
            it["updated_at"] = now
            it["product_ack"] = {
                "by": _clip(str(by or "human"), 128),
                "at": now,
                "comment": _clip(comment, 4000),
            }
            _append_history(
                it, "gate_ack",
                by=_by("human", str(by or ""), str(session_id or "")),
                at=now, text=_clip(comment, 24000),
            )
            _save_unlocked(data)
            _log("GATE-ACK", f"{it.get('ref', '?')} — {_clip(comment, 240)}",
                 queue=it.get("project", ""))
            return it
    return None


def gate_nack(
    ident: Any, reason: str, by: str = "human", session_id: str = "",
    close: bool = False,
) -> Optional[Dict[str, Any]]:
    """Decline a product-gate pitch.

    Default is the icebox: the claim is released and the ticket parked
    unclaimable under ``readiness: needs-rationale`` — "not now"; the value
    name records what revives it (someone brings a new rationale, via
    ``wt edit --readiness ready``). ``close=True`` is "not ever": closed via
    the normal close path with a Declined resolution (reopen stays available).
    ``reason`` is mandatory either way — the why must survive."""
    if not str(reason or "").strip():
        raise ValueError("gate_nack requires a reason (-m): record WHY this "
                         "is not being built")
    if close:
        with _FileLock(_lock_path()):
            data = _load_unlocked()
            for it in data["items"]:
                if _matches(it, ident):
                    _require_rationale_block(it, ident)
                    now = _now_iso()
                    it["product_nack"] = {
                        "by": _clip(str(by or "human"), 128), "at": now,
                        "comment": _clip(reason, 4000),
                    }
                    _append_history(
                        it, "gate_nack",
                        by=_by("human", str(by or ""), str(session_id or "")),
                        at=now, text=_clip(reason, 24000), closed=True,
                    )
                    _save_unlocked(data)
                    break
            else:
                return None
        return globals()["close"](
            ident, session_id=str(by or ""),
            resolution={"summary": f"Declined at product gate: {reason}"},
            force=True, declined=True,
        )
    with _FileLock(_lock_path()):
        data = _load_unlocked()
        for it in data["items"]:
            if not _matches(it, ident):
                continue
            _require_rationale_block(it, ident)
            now = _now_iso()
            it["needs_input"] = False
            it["block_question"] = ""
            it["block_kind"] = ""
            it["blocked_at"] = None
            it["status"] = "open"
            it["claimed_by"] = None
            it["claimed_at"] = None
            it["readiness"] = "needs-rationale"
            it["updated_at"] = now
            it["product_nack"] = {
                "by": _clip(str(by or "human"), 128), "at": now,
                "comment": _clip(reason, 4000),
            }
            _append_history(
                it, "gate_nack",
                by=_by("human", str(by or ""), str(session_id or "")),
                at=now, text=_clip(reason, 24000),
            )
            _save_unlocked(data)
            _log("GATE-NACK", f"{it.get('ref', '?')} — {_clip(reason, 240)}",
                 queue=it.get("project", ""))
            return it
    return None
```

Note the `globals()["close"](...)` indirection: `close` is both the parameter name and the module function — do it exactly this way (or bind `_close_fn = close` at module level after `close`'s definition and call that).

- [ ] **Step 4: Implement the close guard**

Change `close()`'s signature (:2045) to:

```python
def close(
    ident: Any, session_id: str = "", resolution: Any = None, force: bool = False,
    declined: bool = False,
) -> Optional[Dict[str, Any]]:
```

and insert at the top of the body, before the `update_status` call, plus a line in the docstring ("``declined``: this close IS the product-gate Nack — exempt from the gate guard"):

```python
    if not force and not declined:
        current = get(ident)
        if current is not None:
            try:
                from . import config as _config
                gated = _config.product_gate(str(current.get("project") or ""))
            except Exception:
                gated = False
            if (
                gated
                and not current.get("product_ack")
                and not current.get("pre_ack")
            ):
                raise ValueError(
                    f"{current.get('ref', ident)}: this queue has the product "
                    f"gate on and the ticket was never Acked. Post your pitch "
                    f"and wait for the decision: `wt block "
                    f"{current.get('ref', ident)} --worker <your-id> --kind "
                    f"rationale --question \"<pitch>\"`. (--force overrides "
                    f"deliberately.)"
                )
```

The check reads outside the store lock — acceptable: the guard is a backstop against a worker skipping the protocol, not a concurrency primitive.

- [ ] **Step 5: Run to verify they pass**

Run: `python -m pytest tests/test_product_gate.py tests/test_queue.py tests/test_close_proof.py -q`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git commit --only watchtower/queue.py tests/test_product_gate.py -m "feat: gate_ack/gate_nack and the product-gate close guard"
```

---

### Task 5: CLI verbs — new `wt ack`, `wt nack`, `wt gated`, blocked split

**Files:**
- Modify: `watchtower/cli.py` (`cmd_answer` :1326 — extract delivery helper; new handlers near it; parsers near :4176; `cmd_blocked` :1217; `COMMAND_HELP`)
- Test: `tests/test_product_gate.py` (extend)

**Interfaces:**
- Consumes: `queue.gate_ack` / `queue.gate_nack` (Task 4), Task 1's freed name.
- Produces:
  - `wt ack <ref> [-m TEXT] [--by WHO] [--engine E] [--json]` → `cmd_gate_ack`.
  - `wt nack <ref> -m TEXT [--close] [--by WHO] [--json]` → `cmd_gate_nack`.
  - `wt gated [-q Q] [--json]` and `wt blocked` rows showing kind.
  - `_deliver_to_blocked_session(item, answer_text, prompt, engine_arg, worker) -> int` — the extracted tail of `cmd_answer` (context-budget escalation + steer/resume/outbox + headless fallback), reused by `cmd_gate_ack`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_product_gate.py`:

```python
# ------------------------------------------------------------------ CLI verbs

def test_wt_ack_acks_the_gate(wt_env, run_cli):
    it = _gated_pitch(wt_env)
    res = run_cli("ack", it["ref"], "-m", "yes but small")
    assert res.code == 0, res.output
    fresh = wt_env.queue.get(it["ref"])
    assert fresh["product_ack"]["comment"] == "yes but small"
    assert fresh["needs_input"] is False


def test_wt_ack_on_closed_ticket_points_at_unresolved_ack(wt_env, run_cli):
    it = _file_ticket(wt_env)
    wt_env.queue.claim_by_ref(QUEUE, it["ref"], "w1")
    wt_env.queue.close(it["ref"], "w1", resolution={"summary": "done"})
    res = run_cli("ack", it["ref"])
    assert res.code != 0
    assert "unresolved-ack" in res.output


def test_wt_nack_iceboxes_and_requires_reason(wt_env, run_cli):
    it = _gated_pitch(wt_env)
    assert run_cli("nack", it["ref"]).code != 0  # no -m
    res = run_cli("nack", it["ref"], "-m", "not now")
    assert res.code == 0, res.output
    fresh = wt_env.queue.get(it["ref"])
    assert fresh["readiness"] == "needs-rationale"


def test_wt_nack_close(wt_env, run_cli):
    it = _gated_pitch(wt_env)
    res = run_cli("nack", it["ref"], "-m", "wrong direction", "--close")
    assert res.code == 0, res.output
    assert wt_env.queue.get(it["ref"])["status"] == "closed"


def test_wt_gated_lists_only_rationale_blocks(wt_env, run_cli):
    gated = _gated_pitch(wt_env)
    plain = _file_ticket(wt_env)
    wt_env.queue.block(plain["ref"], session_id="w2", question="impl q?")
    res = run_cli("gated", "-q", QUEUE, "--json")
    assert res.code == 0, res.output
    refs = [r["ref"] for r in json.loads(res.output)]
    assert gated["ref"] in refs and plain["ref"] not in refs
```

- [ ] **Step 2: Run to verify they fail**

Run: `python -m pytest tests/test_product_gate.py -q`
Expected: new tests FAIL (no `ack`/`nack`/`gated` commands).

- [ ] **Step 3: Extract the delivery helper from `cmd_answer`**

In `cmd_answer` (:1326-1446), everything AFTER the `q.answer(...)` call and its not-found/no-sid handling (i.e. the context-budget escalation at :1349-1386 and the steer/resume/outbox/headless block at :1387-1445) moves verbatim into:

```python
def _deliver_to_blocked_session(item: dict, answer_text: str, prompt: str,
                                engine_arg: str, worker: str) -> int:
    """Deliver a human decision to the session parked on ``item``. Shared by
    `wt answer` (implementation answers) and `wt ack` (product-gate go
    signal): context-budget escalation, then steer/resume/outbox with the
    headless-fork fallback. Returns the process exit code."""
```

Inside the moved code replace `args.text` with `answer_text`, `args.engine` with `engine_arg`, `args.worker`/`args.ref` with `worker`/`item["ref"]`, and `return 0` stays. `cmd_answer` then ends with:

```python
    return _deliver_to_blocked_session(
        item, args.text,
        prompt, args.engine, args.worker,
    )
```

(keep building `prompt` in `cmd_answer` exactly as today, before the call). This is a pure move — behavior of `wt answer` must not change; `tests/test_answer_resume.py` is the regression net.

- [ ] **Step 4: Add the new handlers and parsers**

Handlers (place after `cmd_answer`):

```python
def cmd_gate_ack(args: argparse.Namespace) -> int:
    """Approve a product-gate pitch (2026-09-01 design): record the decision,
    clear the block, and deliver the go-signal to the parked worker through
    the same steer/resume path as `wt answer`."""
    try:
        item = q.gate_ack(args.ref, comment=args.comment or "",
                          by=args.by or "human")
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    if not item:
        print(f"(no item {args.ref})", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(item, indent=2))
        return 0
    print(f"ACKED: {item['ref']} — product gate approved."
          + (f" Comment: {args.comment}" if args.comment else ""))
    sid = item.get("claimed_session_id")
    if not sid:
        print("  (no resumable session; the next worker to claim it will see "
              "product_ack and implement directly)")
        return 0
    comment_line = (
        f" The approver added: {args.comment}." if args.comment else ""
    )
    prompt = (
        f"Your product-gate pitch on ticket {item['ref']} was APPROVED — "
        f"proceed to implementation now.{comment_line} Implement, verify, "
        f"and close with `wt close {item['ref']} --worker <your-id> "
        f"--summary \"...\" --commit <SHA>` (or `--no-code`)."
    )
    return _deliver_to_blocked_session(
        item, args.comment or "approved", prompt, args.engine, args.by or "")


def cmd_gate_nack(args: argparse.Namespace) -> int:
    """Decline a product-gate pitch: icebox it (readiness=needs-rationale),
    or with --close, close it as Declined."""
    try:
        item = q.gate_nack(args.ref, reason=args.comment or "",
                           by=args.by or "human", close=bool(args.close))
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    if not item:
        print(f"(no item {args.ref})", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(item, indent=2))
        return 0
    if args.close:
        print(f"NACKED+CLOSED: {item['ref']} — declined: {args.comment}")
    else:
        print(f"NACKED: {item['ref']} — iceboxed (readiness=needs-rationale): "
              f"{args.comment}")
        print(f"  revive later with: wt edit {item['ref']} --readiness ready")
    return 0
```

Parsers (place next to the `blocked` parser, :4176):

```python
    s = sub.add_parser("ack", help="approve a product-gate pitch (Ack); "
                                   "resolution-caveat acks moved to unresolved-ack")
    s.add_argument("ref")
    s.add_argument("-m", "--comment", default="",
                   help="optional steering comment, delivered to the worker")
    s.add_argument("--by", default="", help="who decided (default: human)")
    s.add_argument("--engine", default="",
                   help="override the delivery engine (as in wt answer)")
    s.add_argument("--json", action="store_true")
    _add_redundant_queue_flag(s)
    s.set_defaults(func=cmd_gate_ack)

    s = sub.add_parser("nack", help="decline a product-gate pitch: icebox it, "
                                    "or --close to close as Declined")
    s.add_argument("ref")
    s.add_argument("-m", "--comment", default="",
                   help="REQUIRED: why this is not being built")
    s.add_argument("--close", action="store_true",
                   help="close as Declined instead of iceboxing")
    s.add_argument("--by", default="", help="who decided (default: human)")
    s.add_argument("--json", action="store_true")
    _add_redundant_queue_flag(s)
    s.set_defaults(func=cmd_gate_nack)
```

Add `COMMAND_HELP` entries for `"ack"` and `"nack"` mirroring the parser help strings, following how the dict's other entries read.

`cmd_blocked` (:1217): add kind to the human output and a `gated` variant:

```python
def cmd_blocked(args: argparse.Namespace) -> int:
    """List tickets parked for a human (WT-28). With kind_filter (wt gated),
    only product-gate pitches awaiting Ack/Nack."""
    rows = q.list_blocked(project=args.queue)
    kind_filter = getattr(args, "kind_filter", "")
    if kind_filter:
        rows = [it for it in rows if it.get("block_kind") == kind_filter]
    if args.json:
        print(json.dumps(rows, indent=2))
        return 0
    if not rows:
        print("(nothing gated)" if kind_filter else "(nothing blocked)")
        return 0
    for it in rows:
        kind = "GATE" if it.get("block_kind") == "rationale" else "input"
        print(f"{it['ref']:<12} [{kind}] {it.get('block_question') or '(no question)'}")
        print(f"             session={it.get('claimed_session_id') or '-'}  "
              f"repo={it.get('repo_path') or '-'}")
        if it.get("block_kind") == "rationale":
            print(f"             decide with: wt ack {it['ref']} [-m ...]  |  "
                  f"wt nack {it['ref']} -m \"why not\" [--close]")
    return 0
```

and after the `blocked` parser:

```python
    s = sub.add_parser("gated",
                       help="product-gate pitches awaiting your Ack/Nack")
    s.add_argument("-q", "--queue", default=None)
    s.add_argument("--json", action="store_true")
    s.set_defaults(func=cmd_blocked, kind_filter="rationale")
```

- [ ] **Step 5: Run to verify everything passes**

Run: `python -m pytest tests/test_product_gate.py tests/test_answer_resume.py tests/test_argparse_ux.py -q`
Expected: PASS (`test_answer_resume.py` proves the `cmd_answer` extraction changed nothing).

- [ ] **Step 6: Commit**

```bash
git commit --only watchtower/cli.py tests/test_product_gate.py -m "feat: wt ack/nack/gated — product-gate decisions from the CLI"
```

---

### Task 6: Worker goal templates — the gate phase

**Files:**
- Modify: `watchtower/workers.py` (constant near :268; `drain_goal` :3438-3466; the run-once goal builder containing :5254)
- Test: `tests/test_product_gate.py` (extend)

**Interfaces:**
- Consumes: `config.product_gate(queue)` (Task 2); CLI verbs (Task 5) referenced in prompt text.
- Produces: `PRODUCT_GATE_CONTRACT` (module constant) appended to both goal texts iff the queue is gated.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_product_gate.py`:

```python
# ------------------------------------------------------------- goal templates

def test_drain_goal_carries_gate_contract_only_when_gated(wt_env):
    from watchtower import workers
    wt_env.config.set_product_gate(QUEUE, True)
    gated_goal = workers.drain_goal(QUEUE, "w1", repo_path="/tmp/x")
    assert "PRODUCT GATE" in gated_goal
    assert "--kind rationale" in gated_goal
    wt_env.config.set_product_gate(QUEUE, False)
    assert "PRODUCT GATE" not in workers.drain_goal(QUEUE, "w1", repo_path="/tmp/x")
```

(If `wt_env` reloads modules, import `workers` the way other tests in the suite do — check how `tests/test_workers_lifecycle.py` or `test_queue_settings.py` reach `workers`, and mirror it, e.g. `wt_env.workers` if the fixture exposes it.)

- [ ] **Step 2: Run to verify it fails**

Run: `python -m pytest tests/test_product_gate.py -q -k gate_contract`
Expected: FAIL.

- [ ] **Step 3: Implement**

In `workers.py`, after `SUMMARY_STYLE` (:267):

```python
# Appended to a worker's goal when its queue has product_gate on (2026-09-01
# design). The gate is also enforced server-side: `wt close` refuses an
# implemented close on a gated queue when the ticket has no product_ack.
PRODUCT_GATE_CONTRACT = (
    " PRODUCT GATE — THIS QUEUE REQUIRES A HUMAN GO-DECISION BEFORE YOU "
    "IMPLEMENT ANYTHING. After claiming a ticket, first check its JSON: if it "
    "already has product_ack or pre_ack set, the decision is made — implement "
    "directly. Otherwise your first phase is DIAGNOSIS ONLY: understand the "
    "problem, NOT the solution. Do not design, do not write code, do not spend "
    "more than a few minutes. Then post a decision-grade pitch and stop: "
    "`wt block <ref> --worker {worker_id} --kind rationale --question "
    "\"<pitch>\"`. The pitch MUST contain: (1) the problem in 2-3 sentences, "
    "in product terms; (2) evidence links — the originating conversation, "
    "source ticket, or failing surface; (3) for inefficiency/tech-debt "
    "claims, magnitude numbers (tokens, seconds, $/day), each labeled "
    "measured vs estimated; (4) a rough size gut call (S/M/L) — explicitly "
    "not a design. After posting the pitch, treat the ticket like any "
    "blocked ticket and move on (or stop, on a one-off run). If the human "
    "Acks, you will be resumed with their go-signal — implement then. Never "
    "try to close an ungated ticket as implemented: `wt close` will refuse "
    "it on this queue. "
)
```

In `drain_goal` (:3445), after the `goal = DRAIN_GOAL_TEMPLATE.format(...)` call:

```python
    if config.product_gate(queue):
        goal += PRODUCT_GATE_CONTRACT.format(worker_id=worker_id)
```

Find the function that formats `RUN_ONCE_GOAL_TEMPLATE` (the `return RUN_ONCE_GOAL_TEMPLATE.format(` at :5254) and apply the same two lines there before returning (bind the formatted string to a variable first if it currently returns directly; it has `queue` and `worker_id` in scope — confirm the parameter names and use them; import `config` the way `drain_goal` does with `from . import config` if not already in scope).

- [ ] **Step 4: Run to verify it passes**

Run: `python -m pytest tests/test_product_gate.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git commit --only watchtower/workers.py tests/test_product_gate.py -m "feat: product-gate phase in worker goal templates"
```

---

### Task 7: WatchTower dashboard — gated chip + Ack/Nack

**Files:**
- Modify: `watchtower/dashboard.py` (ticket rows :1121-1152; POST routes near `/api/ticket/<ref>/run` :1385 and the renamed resolution-ack route :1417; `_QUEUE_SCRIPT`)
- Test: `tests/test_product_gate.py` (extend)

**Interfaces:**
- Consumes: `queue.gate_ack`/`gate_nack` (Task 4), `block_kind` (Task 3).
- Produces: HTTP `POST /api/ticket/<ref>/gate-ack` (JSON body `{"comment": str}`) and `POST /api/ticket/<ref>/gate-nack` (JSON body `{"reason": str, "close": bool}`); both return `{"ok": true, "item": {...}}` or `{"ok": false, "error": str}` with 400. Task 8's CCC UI may reuse these shapes.

- [ ] **Step 1: Write the failing tests**

Look at how existing dashboard tests drive the handler (`tests/test_dashboard_auth.py` / `test_dashboard_mobile.py` show the harness — an in-process server or handler invocation; mirror it exactly). Add to `tests/test_product_gate.py` (adapting the request helper to that harness):

```python
# --------------------------------------------------------------- dashboard

def test_dashboard_gate_ack_endpoint(wt_env, dashboard_client):
    it = _gated_pitch(wt_env)
    resp = dashboard_client.post_json(
        f"/api/ticket/{it['ref']}/gate-ack", {"comment": "go"})
    assert resp["ok"] is True
    assert wt_env.queue.get(it["ref"])["product_ack"]["comment"] == "go"


def test_dashboard_gate_nack_endpoint(wt_env, dashboard_client):
    it = _gated_pitch(wt_env)
    resp = dashboard_client.post_json(
        f"/api/ticket/{it['ref']}/gate-nack", {"reason": "not now"})
    assert resp["ok"] is True
    assert wt_env.queue.get(it["ref"])["readiness"] == "needs-rationale"


def test_queue_page_renders_gate_actions(wt_env, dashboard_client):
    it = _gated_pitch(wt_env)
    html_out = dashboard_client.get(f"/q/{QUEUE}")
    assert "gate-ack" in html_out and "gate-nack" in html_out
```

If no reusable `dashboard_client` fixture exists, build a minimal one at the top of this test section using the same pattern the existing dashboard tests use (do not invent a new harness style).

- [ ] **Step 2: Run to verify they fail**

Run: `python -m pytest tests/test_product_gate.py -q -k dashboard`
Expected: FAIL (404 / missing markup).

- [ ] **Step 3: Implement**

1. **Row rendering** in `render_queue` (:1126-1152): before building `action`, compute `gated = bool(it.get("needs_input")) and it.get("block_kind") == "rationale"`. When `gated`, render the status cell as `awaiting decision` (add CSS class `gated`, styled like the existing status classes — amber/attention tone) and set:

```python
        if gated:
            action = (
                f'<button class="run-btn" onclick="wtGateAck(\'{ref}\')">Ack</button>'
                f'<button class="run-btn" onclick="wtGateAckC(\'{ref}\')">Ack+</button>'
                f'<button class="run-btn warn" onclick="wtGateNack(\'{ref}\')">Nack</button>'
            )
```

2. **JS** in `_QUEUE_SCRIPT` (find it near the bottom of the module; it already defines `wtRun`): add, matching its existing fetch/reload style:

```javascript
function wtGateAck(ref) { wtGatePost(ref, 'gate-ack', {comment: ''}); }
function wtGateAckC(ref) {
  const c = prompt('Ack with comment — steering note for the worker:');
  if (c === null) return;
  wtGatePost(ref, 'gate-ack', {comment: c});
}
function wtGateNack(ref) {
  const r = prompt('Nack — WHY is this not being built? (required)');
  if (!r) return;
  const close = confirm('OK = icebox (not now).\nCancel then re-Nack with --close in the CLI for "not ever".\n\nIcebox this ticket?');
  if (!close) return;
  wtGatePost(ref, 'gate-nack', {reason: r, close: false});
}
function wtGatePost(ref, verb, body) {
  fetch('/api/ticket/' + encodeURIComponent(ref) + '/' + verb, {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(body),
  }).then(r => r.json()).then(d => {
    if (!d.ok) alert(d.error || 'failed');
    location.reload();
  });
}
```

(The watchtower dashboard is the minimal surface — icebox-only Nack in its UI is fine; `--close` lives in the CLI and CCC. If `_QUEUE_SCRIPT` uses a different request helper than raw `fetch`, use that helper.)

3. **Routes**: next to the `/api/ticket/<ref>/run` POST route (:1385), following its exact route-matching and JSON-body pattern:

```python
        # POST /api/ticket/<ref>/gate-ack  {"comment": str}
        # POST /api/ticket/<ref>/gate-nack {"reason": str, "close": bool}
```

both parse the JSON body, call `q.gate_ack(ref, comment=body.get("comment", ""), by="dashboard")` / `q.gate_nack(ref, reason=body.get("reason", ""), by="dashboard", close=bool(body.get("close")))`, catch `ValueError` → `{"ok": False, "error": str(e)}` with status 400, not-found → 404, else `{"ok": True, "item": item}`. The dashboard gate-ack does NOT deliver the go-signal to the worker session (that path needs the messages layer; CCC and the CLI own delivery) — after a dashboard Ack the ticket simply shows unblocked and the worker resumes on outbox/next claim; note this in a comment on the route.

4. Relabel the existing resolution-ack UI text (if the ack route/buttons at :1417 render a label containing "ack") to "unresolved-ack" — labels only, route path may stay.

- [ ] **Step 4: Run to verify they pass, then the full suite**

Run: `python -m pytest tests/test_product_gate.py -q` → PASS.
Run: `python -m pytest tests/ -x -q` → PASS (full watchtower suite; fix regressions before committing).

- [ ] **Step 5: Commit**

```bash
git commit --only watchtower/dashboard.py tests/test_product_gate.py -m "feat: product-gate chip and Ack/Nack on the WatchTower dashboard"
```

---

### Task 8: CCC surfaces (repo: `/Users/amirfish/Apps/claude-command-center`)

This task runs in the claude-command-center repo. The watchtower package is imported by CCC's `server.py` as `_wt_config` / `_wt_workers` (and queue functions via its own wrappers) — Tasks 1–7 must be committed first.

**Files:**
- Modify: `server.py` — gate endpoints near the ux-fixes POST family (`/api/ux-fixes/answer` at ~:27017-27066 is the model); `product_gate` in `_queue_config_options()` (:1781), `_queue_config_from_payload()` (:1720), and the write-through block (~:27784-27799).
- Modify: `static/q2.js` — status model (:207-213), chips, decision buttons (near the answer band ~:1464-1600).
- Modify: `static/app.js` — ticket-detail chips (~:44499) and answer box area (~:43883).
- Test: CCC's existing test conventions if present for server endpoints; otherwise verify by driving the running server (see Step 5).

**Interfaces:**
- Consumes: watchtower `queue.gate_ack` / `queue.gate_nack` / `config.set_product_gate` / `config.product_gate`; ticket JSON fields `block_kind`, `product_ack`, `pre_ack`, readiness `needs-rationale`.
- Produces: `POST /api/ux-fixes/gate-ack` (body `{"ref", "comment"}`), `POST /api/ux-fixes/gate-nack` (body `{"ref", "reason", "close"}`); `product_gate` boolean in the queue-config payloads.

- [ ] **Step 1: Server endpoints**

Read `/api/ux-fixes/answer`'s handler (~:27017-27066) and `_answer_queue_item_and_notify_worker` (~:2495-2512). Add two POST routes in the same style and location:

```python
        if path == "/api/ux-fixes/gate-ack":
            # Approve a product-gate pitch, then hand the parked worker its
            # go-signal through the same notify path answers use.
            ref = str(payload.get("ref") or "")
            comment = str(payload.get("comment") or "")
            try:
                item = _wt_queue.gate_ack(ref, comment=comment, by="ccc")
            except ValueError as e:
                self.send_json({"ok": False, "error": str(e)}, 400)
                return
            if not item:
                self.send_json({"ok": False, "error": f"no item {ref}"}, 404)
                return
            prompt = (
                f"Your product-gate pitch on ticket {ref} was APPROVED — "
                f"proceed to implementation now."
                + (f" The approver added: {comment}." if comment else "")
                + f" Implement, verify, and close {ref} with wt close."
            )
            # Reuse the answer-notify machinery for delivery (resume/steer);
            # mirror how _answer_queue_item_and_notify_worker builds and sends
            # its prompt, but WITHOUT calling _wt_queue.answer (gate_ack
            # already cleared the block).
            _notify_gate_decision_worker(item, prompt)
            self.send_json({"ok": True, "item": item})
            return
        if path == "/api/ux-fixes/gate-nack":
            ref = str(payload.get("ref") or "")
            reason = str(payload.get("reason") or "")
            close = bool(payload.get("close"))
            try:
                item = _wt_queue.gate_nack(ref, reason=reason, by="ccc",
                                           close=close)
            except ValueError as e:
                self.send_json({"ok": False, "error": str(e)}, 400)
                return
            if not item:
                self.send_json({"ok": False, "error": f"no item {ref}"}, 404)
                return
            self.send_json({"ok": True, "item": item})
            return
```

Adapt the mechanics to the file's reality: the exact name of the imported watchtower queue module (search `import` lines near `_wt_config`; if the server shells out to `wt` instead of importing queue functions, shell out to `wt ack <ref> -m <comment> --json` / `wt nack <ref> -m <reason> [--close] --json` the same way neighboring endpoints shell out — note `wt ack` then owns delivery and `_notify_gate_decision_worker` is unnecessary). If importing directly, implement `_notify_gate_decision_worker(item, prompt)` next to `_answer_queue_item_and_notify_worker`, copying its delivery portion (the part after it mutates the ticket) with the gate prompt. Payload parsing (`payload = ...` from the request body) must copy the neighboring endpoints' exact pattern.

- [ ] **Step 2: Queue settings write-through**

1. `_queue_config_from_payload` (:1720): accept and normalize a boolean `product_gate` field (default False), following how `auto_drain` flows through.
2. `_queue_config_options` (:1781): include the current `product_gate` per queue (via `_wt_config.product_gate(name)` guarded by `hasattr(_wt_config, "product_gate")` for older installs, else the raw JSON entry).
3. Write-through block (~:27799, after `set_auto_drain`):

```python
                    if hasattr(_wt_config, "set_product_gate"):
                        _wt_config.set_product_gate(
                            queue_name, conf.get("product_gate", False))
```

4. The direct-JSON-write fallback below the write-through (for installs without the watchtower package) must also carry `product_gate` — mirror `auto_drain`'s handling there.

- [ ] **Step 3: q2.js**

Read the status model (:207-213), the blocked counters (:339, :375), and the answer band (:1464-1600). Changes:
1. Status derivation: a ticket with `needs_input && block_kind === 'rationale'` gets status/chip `gated` with label **"Awaiting product decision"** (distinct color from the existing blocked/"Waiting for your answer" chip — amber vs the blocked color).
2. Where the answer band renders an input + send for a blocked ticket, when `block_kind === 'rationale'` render instead: the pitch text (`block_question`), an optional comment input, and three buttons — **Ack** (`POST /api/ux-fixes/gate-ack` with `{ref, comment}`), **Nack — not now** (`POST /api/ux-fixes/gate-nack` `{ref, reason, close:false}`, reason prompted/required), **Nack — won't do** (same with `close:true`, confirm() first). Follow the band's existing fetch/refresh helpers.
3. Tickets with `readiness === 'needs-rationale'` show an "iceboxed" chip with the stored `product_nack.comment` as its title/tooltip.

- [ ] **Step 4: app.js**

Same three changes in the main ticket-detail view: chip derivation near :43622/:44428-44538, decision buttons near the answer box at :43883. Reuse whatever request helper the answer box uses. Also add the `product_gate` toggle to the queue-settings form that posts to `/api/queue/config` (search for the `auto_drain` checkbox in the form markup/JS and clone its wiring; label: "Product gate — require my Ack before workers implement").

- [ ] **Step 5: Verify against the running server**

If CCC has an endpoint test suite covering `/api/ux-fixes/*`, add gate-ack/gate-nack tests there following its conventions. Otherwise verify live: with the CCC server running, file a ticket into a test queue, `wt config -q <Q> --product-gate on`, `wt block <ref> --kind rationale --question "PITCH: test"`, then via `curl` confirm gate-ack (200, `product_ack` set) and gate-nack on a second ticket (readiness `needs-rationale`), and load the q2 page to confirm the "Awaiting product decision" chip and buttons render. Record the exact commands and outputs in the task report.

- [ ] **Step 6: Commit (in the CCC repo)**

```bash
git commit --only server.py static/q2.js static/app.js -m "feat: WatchTower product gate — Ack/Nack UI, gate endpoints, queue setting"
```

---

## Final verification (after Task 8)

- [ ] In watchtower: `python -m pytest tests/ -q` — full suite green.
- [ ] End-to-end smoke on a scratch queue: `wt config -q GATESMOKE --product-gate on` → `wt add -q GATESMOKE --note "smoke"` → claim + `wt block --kind rationale` as a fake worker → `wt gated` lists it → `wt close` as implemented FAILS → `wt ack` → `wt close --summary ... --no-code` succeeds. Then a second ticket through `wt nack -m "not now"` → unclaimable. Clean up the scratch queue's tickets afterward (`wt nack --close` or `wt close --force`).

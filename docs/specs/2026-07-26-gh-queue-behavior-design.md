# GitHub-backed queue behaviour + WatchTower install/update

Date: 2026-07-26
Status: approved design, ready for implementation
Repos: `amirfish1/watchtower` (WT), `amirfish1/claude-command-center` (CCC)

## Problem

Four separate defects, all surfaced by one user session:

1. **Slow visibility.** A new GitHub issue took a browser refresh to appear on
   the CCC board. Requirement: visible within 5s, and `wt ls` always current.
2. **Whitelist gating is backwards.** An issue is only workable once it carries
   `watchtower:<QUEUE>`. Users who don't want auto-work should turn drain off
   instead; the label should be an opt-*out*, not an opt-in.
3. **The play button is a dead end.** With drain off, ▶ marks a ticket runnable,
   reports "Running", and nothing runs. A latched spinner then hides the second
   button that would have worked.
4. **WatchTower isn't installed.** On macOS, no install path (curl, brew, DMG)
   installs or updates WatchTower. CCC silently degrades to a fallback engine.

Explicitly out of scope: closed-issue visibility (landed upstream in 25b9b05).

## Part 1 — Eligibility state model

Replace the single `claimable` boolean with three independent inputs.

| Input | Lives on | Default | Meaning |
|---|---|---|---|
| `auto_drain` | queue config (exists) | off | May this queue work tickets unattended? |
| `no-auto-drain` | ticket | absent | Skip this ticket when auto-draining |
| `run_requested` | ticket | absent | A human pressed ▶ |
| `grace_s` | queue config (**new**) | 180 | Ignore tickets younger than this when auto-draining |

Two derived predicates replace the one:

```
auto_eligible   = auto_drain AND NOT no-auto-drain AND age >= grace_s
manual_eligible = run_requested
work_it         = auto_eligible OR manual_eligible
```

`manual_eligible` ignores all three auto conditions. That is the override: ▶
beats the label, beats the grace period, beats drain being off.

**Storage.** Both ticket-level inputs are one concept each, persisted per
backend exactly as `in_progress` already is:

- GitHub backend → labels `watchtower:no-auto-drain`, `watchtower:play`
  (visible and editable from GitHub's own UI)
- File backend → boolean fields on the item

The backend normalises both to plain booleans, so `_claim_candidates` never
learns where they came from.

**Consumers.**

- `count_claimable()` (reconciler auto-spawn) → `auto_eligible`
- `_claim_candidates()` (worker picking work) → `work_it`
- `claim_by_ref()` (targeted claim) → `work_it`

`claim_by_ref` currently *raises* `"missing label ..., run wt run first"`. That
error is deleted — under the blacklist there is nothing to admit.

**Invariant to test:** the auto-eligible set is always a subset of the
work_it set. The two filters must not drift.

### Grace period

Reason: with auto-drain on, the reconciler claims a new issue within ~30s, so
a human never gets a chance to label it `no-auto-drain`. The grace period is
what makes the opt-out usable for inbound issues rather than only pre-existing
ones.

- Per-queue config `grace_s`, default 180.
- `0` disables it (fast queues that should drain immediately).
- Applies to `auto_eligible` only. ▶ ignores it.

### Public-repo warning

When `auto_drain` is switched **on** for a queue whose `github_repo` is public
(`gh repo view <repo> --json visibility`), warn before applying — agents will
work strangers' issues. Fires in both `wt drain on <QUEUE>` and the dashboard
drain toggle.

## Part 2 — Freshness

**Requirement:** board within 5s; `wt ls` always current.

**Mechanism: ETag as a change *detector*, gating today's fetch.**

```
poll:
  conditional GET with If-None-Match
    304 (unchanged) -> keep cached list, reset TTL     ~0.5s, 0 rate-limit cost
    200 (changed)   -> run today's `gh issue list --json ...`, store new ETag
```

Measured on `amirfish1/claude-command-center` 2026-07-26:

| | today | with ETag |
|---|---|---|
| unchanged poll | 1.2s, consumes quota | 0.49s, consumes nothing |
| `X-RateLimit-Remaining` across a 304 | — | 4837 → 4837 |

**Why a detector and not a replacement fetcher.** The REST issues endpoint
returns `comments` as a *count*. `_issue_to_item` embeds full comment **bodies**
in every list row (`_issue_text(..., issue.get("comments"))`). Swapping the
fetcher would silently strip comment context from worker tickets. Gating keeps
`_issue_to_item` and everything downstream untouched, and sidesteps having to
filter pull requests out of the REST payload.

**Landmine — must be handled explicitly.** `gh api` **exits 1** on a 304, with
`gh: HTTP 304` on stderr. `_run()` raises on non-zero, so a naive port turns
every unchanged poll into an error, which trips `_LIST_ERROR_BACKOFF` (60s) and
freezes the queue. Treat exit 1 + `HTTP 304` as "unchanged"; only other
failures are errors.

**Fallback.** Any unexpected probe failure falls through to today's
unconditional `gh issue list`. Worst case equals current behaviour.

**Settings that change:**

- `_GH_POLL_INTERVAL_S`: 20.0 → 5.0 (CCC `ccc_server/queue_events.py`)
- `_LIST_CACHE_TTL`: 20.0 → 2.0 (WT `github_backend.py`) — just enough to
  dedupe bursts within one agent turn
- CLI reads revalidate every time

At 5s with near-universal 304s this consumes *less* quota than today's 20s
polling. This also removes any case for a GitHub webhook: 5s freshness without
exposing a localhost-bound server is not worth a tunnel.

## Part 3 — The ▶ button

**One button, one meaning: run this ticket.** The `fq-run` / `fq-run-once` pair
collapses into a single control.

Pressing ▶ sets `run_requested` on the ticket. It does **not** spawn directly.
The reconciler — which already enforces `desired_workers` — does the spawning.

Consequences, all free:

- Three ▶ presses run **serially**, in order, respecting the worker budget.
  Today `spawn_run_once_worker` calls `subprocess.Popen` with no concurrency
  check at all, so three presses spawn three workers.
- Survives restart: the mark is on the ticket, not in page memory.
- ▶ on a `no-auto-drain` ticket runs it anyway.
- Press again to cancel, while still queued.

**Visible states:** `open` → `queued to run` → `running` → `needs input` /
`closed`.

**Latency.** ▶ triggers the same immediate nudge `dispatch_after_enqueue` uses,
so it does not wait for the next 30s reconciler tick.

**Bugs this fixes.** `_uxqPendingRunRefs` currently clears only when a ticket
leaves `open`, so with drain off the "Starting worker…" spinner latches forever
and covers the working button. The toast says "Running <ref>" when nothing runs.
Both go away, because queued-to-run becomes real observable state.

**Parity.** Identical behaviour on GitHub-backed and file-backed queues. Today
GitHub queues carry an extra admission step nothing else has.

## Part 4 — Migration

The dangerous moment is the flip itself: someone upgrades with `auto_drain` on
and agents immediately start working every open issue in their repo.

**On upgrade, turn `auto_drain` off for every GitHub-backed queue, once**, and
say why in the log and on the board:

> WatchTower changed how GitHub queues pick work; drain was turned off for
> `<QUEUE>` so nothing runs unexpectedly. Turn it back on when ready.

Re-enabling is where the public-repo warning fires. Guard with a one-time
migration marker so it cannot fire twice.

Rejected alternative: bulk-labelling every existing issue `no-auto-drain` to
preserve behaviour exactly. It writes to hundreds of issues on someone's repo
to preserve a default we are deliberately changing.

**Old labels stay.** `watchtower:<QUEUE>` becomes inert. We do not delete labels
from a user's repo automatically.

**One job the old label keeps.** If two or more queues point at the *same*
`github_repo`, it remains the only way to partition issues between them. Rule:
one queue per repo → label ignored entirely; two or more → still used to
divide. Narrow, and only fires when actually needed.

**In-flight tickets are unaffected** — claim state lives in the issue body
metadata, not the label. The `watchtower:in-progress` label keeps its job.

**Dropped rule:** `close()` refusing when the queue label is absent.

## Part 5 — WatchTower install / update (CCC)

**Today, on macOS, no path installs WatchTower:**

| Path | Installs WT? |
|---|---|
| `curl \| bash` (`install.sh`) | only if a checkout already exists locally |
| `brew install ccc` | never — formula copies the repo and runs `run.sh` |
| DMG / Sparkle | never |
| in-app "check for updates" (`_self_update`) | updates CCC's checkout only |

`install.sh` also runs **only on first launch** (`scripts/macapp/main.swift`),
so fixing it alone reaches new installs and nobody else.

`scripts/install.ps1` already implements the correct chain. Port its logic.

**Decision: one shared script, invoked from `run.sh`.** Every launch path ends
up executing `run.sh`, so that single hook covers curl, brew, DMG and
from-source, new installs and existing ones.

Order:

1. `import watchtower` succeeds → done (~50ms, the normal case)
2. Python < 3.11 → skip with a clear reason (WT needs 3.11; CCC supports 3.9)
3. Local dev checkout (`$WATCHTOWER_DIR`, `~/Apps/watchtower`,
   `~/dev/watchtower`) → `pip install -e`
4. `git clone --depth 1 https://github.com/amirfish1/watchtower` into
   `~/.ccc/watchtower` → `pip install`
5. Tarball `main.tar.gz` → `pip install --user`
6. All fail → loud warning + exact retry command; CCC still starts, degraded

**Not PyPI.** `watchtower-cli` is pinned at `0.1.0`, set in WatchTower's first
commit, 257 commits behind main. Installing it would look successful and be
badly wrong. PyPI stays last-resort only.

**Same interpreter.** CCC does `import watchtower.queue` inside `server.py`, so
it needs the library, not just the `wt` binary. This rules out pipx.

**Staying current.**

- CCC-managed clone (`~/.ccc/watchtower`) → `git pull --ff-only`, at most once
  per day, plus on CCC version change.
- **Never auto-update a dev checkout.** `~/Apps/watchtower` is a working tree
  that may hold uncommitted work — this machine's own has four modified files
  and a diverged branch right now. If it is behind, say so and stop.
- Tarball/PyPI installs → no SHA to compare; reinstall on the same cadence.

**Daemon.** After install or update, run `wt start`. It writes and loads the
LaunchAgent on first run, so it survives reboot and login, and is a no-op when
already running.

**Restart ordering after an update** — all three, in order:

1. update WatchTower
2. `wt stop` && `wt start` — the daemon holds old code in memory; skipping this
   makes the update a no-op
3. restart CCC — `server.py` caches WatchTower capability probes
   (`_WT_IMPORT_AVAILABLE_CACHE`) for the process lifetime

Guard: if `wt workers` shows live workers mid-ticket, defer the restart.

**Also extend `_self_update()`** to refresh WatchTower and restart its daemon.
Otherwise the button whose entire purpose is "bring me up to date" leaves half
the system stale.

**Fix the README.** Line 50 claims WatchTower is "installed by default as CCC's
queue engine" — untrue on every Mac today.

## Part 6 — WatchTower release process

WatchTower has **one tag** (`v0.1.0`), no `scripts/` directory, no changelog,
no release script. CCC has 36 tags and a 9-step `cut-release.sh`. The version
has never moved in 257 commits, so `wt --version` is meaningless and nothing can
express "CCC needs WatchTower ≥ X".

Build a minimal `scripts/cut-release.sh` for WT, modelled on CCC's: bump
`pyproject.toml` + `watchtower/__init__.py`, tag, push, create the GitHub
release. PyPI publishing is out of scope — no credentials on this machine.

## Testing

- **Eligibility truth table:** drain on/off × labelled/not × inside/outside
  grace × ▶/not.
- **Invariant:** auto-eligible ⊆ work_it.
- **ETag:** a 304 counts as unchanged, never as failure; a real failure still
  backs off. This is the most likely silent regression.
- **Serialisation:** three ▶ presses with `desired_workers=1` start one worker.
- **Migration:** upgrading with drain on disables it exactly once.
- **Install:** missing WatchTower is fetched; a dev checkout is never pulled.
- **Live check:** file an issue, expect it on the board within 5s.

## Notes for implementers

- Do not write WatchTower's JSON directly from CCC. Go through `_q` /
  `watchtower.config` / `watchtower.workers` / the `wt` CLI. See
  `docs/watchtower-migration-state.md`.
- `ux_fixes_queue.py` (CCC's no-WT fallback) has **no** GitHub support, so none
  of Part 1–3 needs a fallback path.

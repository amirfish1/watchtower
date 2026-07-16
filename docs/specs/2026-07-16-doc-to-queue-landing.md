# Landing feat/doc-to-queue into main (W63)

Prep notes for Amir. Everything here was proven in a throwaway worktree; the
real merge (below) still needs Amir's go-ahead and has not been run against
`watchtower` main.

## 1. Merge command sequence (mechanical, one shot)

```bash
cd /Users/amirfish/Apps/watchtower
git checkout main            # main only, never a feature branch in the shared clone
git merge feat/doc-to-queue
```

No conflict markers should appear. See "Conflict rehearsal" below for why.
After the merge:

```bash
python3 -m venv .venv && .venv/bin/pip install -e ".[dev]"
.venv/bin/python3 -m pytest -q
```

Expect **460 passed**. That is the exact count from a full rehearsal merge in
a scratch worktree (`scratch/merge-rehearsal`, forked from main at `8b09273`,
merging in `feat/doc-to-queue` at `614ab88`) — the same count the feat branch
already carries on its own, so the merge adds zero regressions and zero new
failures.

## 2. Conflict rehearsal: what actually happened

Main advanced two commits past the fork point (`9bd4922`) before this branch
landed:

- `e53e87e` fix(skills): sync wt-triage-queue skill into agent harnesses —
  touches `tests/test_skills_sync.py`, `watchtower/skills_sync.py`.
- `8b09273` docs: standalone positioning, runnable quickstart demo, README
  cleanup (W44) — touches `README.md`, `examples/quickstart-demo.sh`.

`feat/doc-to-queue` (`614ab88`) touches `README.md`,
`watchtower/skills/watchtower/SKILL.md`, `watchtower/cli.py`,
`watchtower/document_import.py`, plus new test fixtures and docs.

The only file both sides touch is `README.md`. `git merge` (ort strategy)
auto-merged it cleanly with **no conflict markers** — the two sides edited
non-overlapping regions (W44 rewrote the standalone-positioning framing near
the top and added a quickstart section; W43 added a `wt import` usage section
further down). Verified in the rehearsal merge that both survive intact:

- W44's standalone framing: "WatchTower is a standalone product..." (README
  intro paragraph) and the `off` config note ("means fully standalone").
- W43's import section: the `wt import plan.md -q DEMO` usage block and the
  longer `wt import` explanation paragraph.

**No manual conflict resolution is required.** If a future rebase of either
branch changes README structure enough to force a real conflict, resolve by
union: keep W44's intro paragraph and quickstart section, keep W43's `wt
import` usage section, in whatever relative order reads best — they don't
overlap in subject matter.

## 3. CCC end-to-end proof (against the branch build, not main)

Proven without touching the installed `wt` or the live CCC on port 8090:

- A scratch venv (`pip install -e .` against the
  `watchtower-wt-doc-to-queue` worktree, `feat/doc-to-queue` @ `614ab88`) put
  a feat-branch `wt` first on `PATH`.
- A scratch CCC instance was started on port 8099 with `CCC_EPHEMERAL=1`
  (skips the shared `~/.claude/command-center/port.txt` discovery file so it
  never hijacks the live instance) and `CCC_BIND_HOST=127.0.0.1` (loopback
  only), plus `WATCHTOWER_HOME` / `WATCHTOWER_STORE` /
  `WATCHTOWER_ACTIVITY_LOG` / `WATCHTOWER_CONFIG_FILE` /
  `WATCHTOWER_WORKERS_FILE` all pointed at an isolated scratch state
  directory under `/tmp`.
- `GET /api/queue/import-doc` on the scratch instance returned
  `{"ok": true, "available": true}` — the probe (`wt import --help`) detects
  the feat-branch CLI. The dashboard's "Import doc" button
  (`#queueImportDoc`) unhid itself accordingly.
- Headless Puppeteer drove the actual UI: opened the Queue rail tab, clicked
  Import doc, entered a real mission-brief markdown path from
  `~/Desktop/fable-goal-briefs-2026-07-16/`, previewed (dry-run), filed the
  new tickets (apply), confirmed the queue via `wt ls -q SCRATCHW63`, then
  reopened the dialog and re-ran the same doc/queue — the second preview
  reported 0 new (everything already `exists`). Screenshots captured at each
  step.

## 4. IMPORTANT: the live CCC probe cache is process-lifetime

`_wt_import_available()` in `claude-command-center/server.py` caches the
`wt import --help` result in a module-level variable
(`_WT_IMPORT_AVAILABLE_CACHE`) for the lifetime of the running CCC process —
**one probe per process, never rechecked.**

Separately, the currently-running live CCC process (port 8090) started
*before* the CCC-side `import-doc` feature (`cc77b3b5`) even existed in its
own repo, so today `GET /api/queue/import-doc` on port 8090 returns
`{"error": "Not found"}` — that route isn't loaded yet either.

Net effect: landing this merge on watchtower main does **not** by itself make
the Import-doc affordance appear in the live dashboard. Amir must **restart
the live CCC process** (`port 8090`) after both (a) this merge lands and (b)
the new `wt` is reinstalled/on PATH, so the process picks up the newer
`claude-command-center` code and re-probes `wt import --help` fresh. There is
no separate cache-bust endpoint — a process restart is the only way to
re-arm `_WT_IMPORT_AVAILABLE_CACHE`.

## 5. Is the merge mechanically safe?

**Yes.** The only shared file (`README.md`) auto-merges without conflict
markers, the rehearsal merge's full suite passes at the same 460-test count
the feature branch already carries standalone, and the CCC integration was
proven end-to-end against the exact code this merge would introduce. The one
operational follow-up is the CCC restart noted above — not a merge risk, a
deploy step.

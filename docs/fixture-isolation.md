# Disposable queue reproductions

Run `python3 scripts/repro_sbq_group.py` to replay the synthetic SBQ group
without touching the fleet. It uses a fresh child interpreter, explicit
store/config/activity/worker paths, initialized empty destinations, and the
actual top-level queue config format. It suppresses notifications and daemon
wakeups. The missing-commit result is injected: this is a persistence isolation
check, **not** a security acceptance test or a successful group integration.

`WATCHTOWER_DIR` does not redirect runtime storage. `WATCHTOWER_DATA_DIR` is
the persistent-data override, but explicit `WATCHTOWER_STORE` and
`WATCHTOWER_CONFIG_FILE` take precedence. Set those before importing WatchTower,
and initialize empty destinations so legacy migration cannot copy live data.
Path settings cached at import time require a fresh interpreter. Pytest's
autouse fixtures do not protect one-off `python3 -c` reproductions.

## WT-72 provenance

The Nov 16, 2026 SBQ leak came from the Antigravity security-review conversation
`944641cd-cebe-48fe-8c61-bbde2121a7fd`, worker
`critique-antigravity-62a43cb9`, reviewing the WT-61 branch. Its SQLite transcript
is `~/.gemini/antigravity-cli/conversations/<conversation-id>.db`, table `steps`.
Step 168 contains the creating command. It:

- Creates `wt-final-verify-*` and sets only `WATCHTOWER_DIR` around queue calls.
- Writes `{"queues": {"SBQ": ...}}` into the unused control directory,
  rather than the authoritative config's top-level `{"SBQ": ...}`.
- Creates Parent/Child1/Child2, the `plan`/`cmd:echo hi` group, `sb-w1`/`sb-w2`,
  the fake session IDs ending in `111`/`112`, and the `deadbeef` commit.
- Calls `group_sweep` and removes the workspace in `finally`.

The live log records the matching SBQ-1/2/3 creation at 01:35:43–01:35:51 UTC,
child closes at 01:36:06/01:36:14, and missing-commit integration at 01:36:18.
The persisted workspace was `/home/hermes/wt-final-verify-4me01ynt/ws`.
The command's own output says `parent _sandbox_restricted: False`, followed by
`commit_missing`. That run does **not** establish execution against a restricted
queue. WT-61 must evaluate its security findings against correctly isolated,
correctly configured reproductions; WT-72 does not resolve those findings.

The original one-off command is historical transcript, not a maintained script.
The maintained replacement above prevents reuse of its invalid isolation recipe.
Saved command and live before/after evidence: `~/dev/scratch/WT-72/`.

## Retiring proven fixtures

After confirming provenance and scope, use the audited
`wt queue archive SBQ --force --apply` lifecycle. Forced archival records the
existing open ticket refs; it suppresses their active status, owner questions
and automatic group integration. It does not mark gates passed, forge proof,
or delete/edit ticket history. Tickets filed later remain visible. Raw
`wt find`, `wt status --all` and explicit queue status retain inspection access.
`wt queue unarchive SBQ` restores the outstanding work and original gates.

Archive only verified fixture queues under explicit cleanup authorization.
An ordinary archive still refuses unclosed tickets; even `--force` refuses
an enabled auto-drain queue until staffing is explicitly disabled.

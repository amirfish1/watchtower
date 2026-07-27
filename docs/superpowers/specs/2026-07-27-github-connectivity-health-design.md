# GitHub Connectivity Health

Date: 2026-07-27

## Problem

GitHub-backed queues (`backend=github`, e.g. `BYM-GH-FINIE`, `CCC-GH`) poll
`gh issue list` on every reconcile tick and every CCC dashboard refresh. When
`gh` cannot authenticate or reach GitHub, WatchTower already avoids hammering
the API (a 60-second error-backoff cache added for WT-87), but the failure is
otherwise invisible: it is a single `ERROR` line in `~/.watchtower/activity.log`
that nobody is watching. Observed incident: `BYM-GH-FINIE` polling failed
continuously for over 12 hours (network errors, then `gh auth login` required,
then GitHub rate-limit-exceeded) with no visible signal anywhere in the CCC
dashboard, and no escalation — the retry stayed flat at 60 seconds the entire
time. The outage was only discovered by manually reading the activity log.

Separately, `queue.list_items()` silently drops a GitHub-backed queue's items
when its fetch fails (`except Exception: ... continue`), so a broken queue
currently reports `depth=0` / `state="clear"` in `health.all_status()` —
indistinguishable from a genuinely empty, healthy queue.

## Goals

1. A single global "is GitHub reachable" signal, computed from the real
   polling that queues already do — not a synthetic ping — so it can never
   report healthy while the actual queue operations are failing.
2. Only alert on **sustained** failure (default: no successful poll in 5
   minutes), not a single transient blip.
3. Escalating backoff while broken (60s → 120s → 240s → 480s → capped at
   600s), to cut log noise and API pressure during a prolonged outage.
4. A way to force an immediate recheck once the underlying problem (e.g.
   `gh auth login`) is fixed, bypassing the escalated backoff.
5. Surface the alert somewhere a human will actually see it: the existing
   CCC dashboard header beacon, plus `wt status`.

## Non-goals

- **Per-queue precision.** The signal is global by explicit choice: if two
  GitHub-backed queues point at different repos and only one is broken, the
  healthy queue's successes reset the shared `broken_since`/backoff state for
  both. Every failure mode observed in practice (auth expired, network/TLS,
  rate-limit) is global to the `gh` CLI and would affect every repo
  identically, so this trade-off does not bite for the incident that
  motivated this design. A future per-queue signal is not precluded, just not
  built now.
- Does not change how `list_items()` degrades when a fetch fails (it still
  omits that queue's items rather than raising to callers that don't ask for
  `strict`). The new alert makes that degradation visible instead of silent;
  it does not change the degradation itself.
- Does not add a dedicated `gh auth status`-style ping. Rejected in favor of
  reusing real polling (see Approach discussion below) — a synthetic ping
  could pass while the actual per-repo calls the queues depend on still fail.

## State file

New persisted file: `~/.watchtower/gh-connectivity.json`, written under the
existing `_FileLock` pattern (same lock style as `queue.py`'s other JSON state
files).

```json
{
  "last_success_at": "2026-07-27T03:12:04Z",
  "broken_since": null,
  "consecutive_failures": 0,
  "next_retry_at": null,
  "last_error": ""
}
```

- `broken_since`: set the moment a live poll fails immediately after a
  success (or on the very first poll if it fails before any success has ever
  been recorded); cleared to `null` the moment any live poll succeeds. This
  gives outage duration directly (`now - broken_since`) without comparing two
  timestamps at every read site.
- `next_retry_at`: the backoff gate. No live `gh` call is attempted while
  `now < next_retry_at`, except via an explicit forced recheck.
- Updated by every GitHub-backed queue's poll, success or failure — this is
  what makes the signal global rather than per-queue.

State file is process-shared, not in-memory: `wt status`/`wt ls` each run in
a fresh short-lived process, so the existing in-memory `_LIST_CACHE` /
`_LIST_ERROR_BACKOFF` (module-level dict) is invisible across CLI
invocations. Persisting to disk is required for "sustained over 5 minutes"
to mean anything when the CLI is what's checking.

## Write path

The write hook lives inside `github_backend.py`'s `_list_issues()`, at the
point where the raw `gh issue list` call (`self._run(args)`) actually
succeeds or fails — not in the outer `except` block in `queue.py`. This
matters: when a fetch fails but stale cached data from a prior success is
still available, `_list_issues()` today silently returns that stale data
instead of raising (the WT-87 behavior), so `queue.py`'s outer `except` never
fires in that case. Hooking only the outer catch would miss every
"quietly degrading to stale data" failure — exactly the case that most needs
to feed a sustained-failure detector, since it's the one that currently
produces zero visible signal at all.

- **On a real success:** clear `broken_since`, `consecutive_failures`, and
  `next_retry_at` (back to `null`/0), refresh `last_success_at`. Throttled to
  at most one disk write per 30 seconds — this sits on a hot path (CCC's
  dashboard polls every few seconds), and a healthy queue doesn't need
  sub-second precision on "last success."
- **On a real failure:** set `broken_since` if not already set, increment
  `consecutive_failures`, compute
  `next_retry_at = now + min(600, 60 * 2 ** (consecutive_failures - 1))`,
  store `last_error`. Write frequency here is naturally throttled by the
  backoff itself once it escalates past the first failure.
- **Before attempting a live fetch:** read `next_retry_at` from the
  persisted file; if still inside the window, skip the live call (same
  intent as today's in-memory `_LIST_ERROR_BACKOFF`, now backed by a value
  that's consistent across processes).

### Interaction with `fresh=True` / `strict=True`

`wt status` and `wt ls` pass `fresh=True`, which today always forces a live
revalidation against GitHub. Under this design, `fresh=True` still **respects**
the persisted backoff window during a known outage — otherwise every manual
status check during an outage would itself re-hit GitHub, defeating the
purpose of backoff. `strict=True` callers (`claim`, `close` — real writes
that need certainty) are unchanged: they always bypass backoff and attempt
live, exactly as today.

## Forced recheck

New CLI command: `wt gh recheck`. For every configured GitHub-backed queue,
it attempts a live fetch that ignores `next_retry_at` (bypassing backoff
without waiting for it to expire), updates the persisted state exactly as a
normal poll would, and prints a per-queue result plus the resulting global
connectivity state. This is the explicit "I fixed it, check now" action —
the alternative to waiting out an up-to-10-minute backoff window after
re-authenticating `gh`.

## Surfacing the alert

`health.py` gains a `github_connectivity()` function reading the state file
and returning:

```json
{
  "alert": true,
  "broken_since": "2026-07-27T04:20:11Z",
  "outage_duration_s": 1834,
  "consecutive_failures": 4,
  "last_error": "To get started with GitHub CLI, please run: gh auth login"
}
```

`alert` is `true` only when `broken_since` is set **and** `now - broken_since
>= 300` seconds (the 5-minute sustained-failure threshold) — a single blip
that recovers on the next poll never sets `alert`.

This is added as a new top-level `"github"` key in `dashboard.status_payload()`
(alongside the existing `"queues"`/`"workers"` keys), not folded into
`health.all_status()`'s return shape — `all_status()` returns a flat list of
per-queue rows today and several callers (`cli.py`'s `_print_status`, etc.)
depend on that list shape; adding an unrelated global key there would be a
breaking change for no reason.

**Dashboard beacon:** the CCC header already has a "beacon" element that
turns red (`beacon alert` CSS class) when `any_stuck` is true across queues
(`dashboard.py` ~line 737). Extend that same condition:
`beacon_cls = "beacon alert" if (any_stuck or gh_alert) else (...)`. This
reuses the existing red-alert affordance rather than inventing new UI. Add a
short text line near the beacon when `gh_alert` is true — e.g. "GitHub
unreachable 30m — gh auth login" (truncated `last_error`) — so the red dot is
actionable rather than a mystery signal.

**`wt status`:** when `alert` is true, print a one-line warning above the
queue table (e.g. `⚠ GitHub unreachable for 30m — gh auth login`), mirroring
the dashboard text.

## Testing

- `github_backend.py`: backoff sequence escalates correctly (60/120/240/480/
  600/600...) and resets to 60 on the next success; `broken_since` set/cleared
  at the right transitions; success-write throttling; live calls are skipped
  while `next_retry_at` is in the future and attempted once it has passed.
- `github_backend.py`: the stale-data-return-without-raising path (existing
  WT-87 behavior) still updates connectivity state as a failure even though
  no exception propagates to the caller.
- `health.py`: `github_connectivity()` alert boundary — false just under 5
  minutes of `broken_since`, true at/after.
- `wt gh recheck`: bypasses an active backoff window, updates state on both
  outcomes, prints per-queue results.
- Regression: `strict=True` callers (claim/close) are never gated by the new
  backoff — confirm existing claim/close paths are unaffected during a
  simulated outage.
- `dashboard.py`: beacon renders `alert` class when `gh_alert` is true even
  with zero stuck queues, and vice versa (existing stuck-only case still
  works unchanged).

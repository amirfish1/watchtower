# Related work at claim time (WT-83)

`wt claim` includes a `related` object in JSON and a readable list otherwise.
`wt related REF` searches full ticket prose across queues, including discussion
and resolutions; it does not invoke gh or a model. The first result is a
candidate, not evidence that a fix shipped. See the worker runbook for triage,
confirmation, repeat handling and close feedback.

## Design and operating limits

The owner selected all queues, open and closed tickets, a roughly 30-day
window, full prose rather than titles, local FTS5/porter/BM25 retrieval, and
Jev as a cheap semantic judge. Options considered before implementation:

1. Reuse CCC ticket FTS directly: less code, but incomplete GitHub discussion,
   resolution coverage and an application dependency.
2. Maintain a WatchTower projection of its own raw sources: chosen. Complete
   authored prose participates in indexing; metadata and actor IDs do not.
3. Search GitHub live: rejected for the claim path; misses local queues and
   adds network latency/quota consumption.
4. Product option: immediate local candidates, explicit worker semantic triage
   afterward. Chosen alongside option 2 to keep model calls off claims.

The disposable SQLite index is `storage.data_dir()/related.db`. It includes
tickets created, updated or closed within 30 days. A one-minute refresh floor
amortizes local snapshot reads; a missing index rebuilds from local files.
New/older query tickets can be read directly from local sources, even before
the projection refresh. Search terms come from all authored prose, excluding
reference labels and common boilerplate, with 64 terms to bound expression
cost. Title weight is 2, body weight 1. Cached contexts may be one minute old.
Recall failures are visible on the successful claim; they do not undo a claim
or pretend that no related work exists.

GitHub's ordinary list cache deliberately omits comments and caps closed rows.
`wt related --refresh-github [--repo OWNER/REPO]` is explicit maintenance: it
fetches all issues updated within 30 days, paginates issues and comments, skips
PRs, retains private raw snapshots and records per-repository coverage. It
does not send, edit or close issues. Re-running it skips unchanged full details.
Normal issue detail reads preserve prose already fetched, without another
request. Newer shallow bodies supersede stale details; known discussion is
retained with a freshness warning. Missing, capped and stale coverage remains
visible. The refresh is resumable after a failed repo/comment request; no
completion marker is written for an unfinished repository. The initial backfill
was run, not merely supplied as a script.

`wt related` separately searches CCC's existing recent commit FTS with a
three-second instruction deadline, deduplicates cloned commit hashes, and
labels its result as index evidence rather than deployment proof. It never
rebuilds CCC or runs git. Copying CCC's 4.9 GB graph into the ticket projection
was measured and rejected: 109,307 recent clone/commit rows made the first
probe take 42 seconds. Commit search is off the claim critical path.

`wt related REF --triage` needs `TYPESAFE_API_KEY` or `JEV_API_KEY`, supplied
from the authorized secret store. The API key is neither persisted nor logged.

For workers that do not inherit a key, opt in to a machine-local credential
command in `related-credentials.json` under WatchTower's persistent data
directory (normally `~/.local/share/watchtower/`). For example:

```json
{"command": ["/absolute/path/to/authorized-key-reader"]}
```

The reader must print exactly one key and exit successfully within ten seconds.
It runs as an argv array, without a shell, only for explicit `--triage` when
neither environment key is set. Its output is captured in memory, never exported
to worker environments or stored; command errors suppress stdout and stderr.
Keep the configuration and reader private to the account. A missing or failing
reader leaves local retrieval available and reports a triage error. Remove the
configuration to undo the opt-in. Ordinary claims and local recall never execute
the reader. On hermes, `/home/hermes/.local/bin/wt-jev-key` uses the existing
Bitwarden secret below; no daemon restart is needed.

On the hermes VM, workers do not inherit the Jev key. The supported invocation
loads the existing key from Bitwarden for this command only (OPS-630):

```bash
(
  set +x
  set -o pipefail
  set -a
  source ~/.config/bws/token.env
  set +a
  JEV_API_KEY=$(bws secret get 363f445c-1fd1-4785-80cf-b4c8011831a9 |
    python3 -c 'import json,sys; key=json.load(sys.stdin)["value"]; assert key; print(key)') || exit 1
  export JEV_API_KEY
  unset BWS_ACCESS_TOKEN
  wt related REF --triage --worker WORKER_ID --json
)
```

Replace `REF` and `WORKER_ID`; do not paste key values into tickets, shell
profiles or tracked files. The subshell leaves the caller environment unchanged,
and `pipefail` prevents a failed secret fetch from running triage. This uses the
existing machine-account permission; it creates or rotates no credentials.
If the vault or Jev is unavailable, use `wt related REF --json`, inspect useful
candidates with `wt find OTHER --json`, and verify their source evidence manually.
Record the triage error in ticket progress, confirm only checked relationships
with `--confirm`, and report recall coverage gaps rather than treating missing
context as no related work. Local retrieval itself requires no credential.

Jev Choice questions are independent and share state only within a bounded
batch. Long documents supply selected matching passages, original request and
resolution, with `partial_context: true`; retrieval itself remains full-text.
This avoids the observed `max_tokens_exceeded` failure of a full ten-document
batch. Triage returns probabilities and usage, and never confirms a link.
No security decision, automatic close or deployment claim follows a judgment.

`--confirm OTHER --kind KIND --evidence TEXT --worker ID` appends a durable
`RELATED_LINK` comment. Confirming a closed bug's `earlier-attempt` sets the
current ticket to deep and records "fix in OTHER did not hold". Three connected
confirmed same-job filings within seven days produce one fragile-job line in
`wt owner-view --json` warnings, which the existing decision desk morning
digest renders. Generic similarity alone creates no fragile-job alert.

`wt close ... --related-feedback useful|not-useful|unavailable` records durable
feedback only after the close is accepted. `wt related --feedback-report
--since ISO --json` deduplicates retry ratings by ticket/worker and reports
useful/(useful + not-useful); unavailable and an absent sample are distinct.

## Launch evidence, November 17, 2026 UTC

The initial full GitHub backfill completed for all seven configured repositories:
1,841 issues, including 1,621 in BYM-Finie and 129 in stramp-platform, with
paginated comments. Alongside local queues the projection contained 4,729
tickets. Three warm retrieval probes were 286, 167 and 166 ms. The earlier
3,330-ticket projection measured 211–227 ms. These are retrieval timings, not
total claim-process latency; existing claim transport costs remain separate.

`python3 scripts/evaluate-related.py --out /private/path/report.json` rebuilds
a disposable FTS projection at each later ticket's first claim instant. Future
tickets, comments and resolutions are excluded, and reference labels are
removed from query terms. It reports duplicates and normalized watchdog
templates separately. No new GitHub calls or Jev calls run in evaluation.

| Corpus | Recorded duplicate top 5 | Monitor-template top 5 | Combined |
| --- | --- | --- | --- |
| Initial shallow snapshots | 85/105 (81.0%) | 135/136 (99.3%) | 220/241 (91.3%) |
| After full-history backfill | 88/107 (82.2%) | 136/137 (99.3%) | 224/244 (91.8%) |

The filing's five named repeats also all retrieved their known earlier work:
OPS-591 at rank 1, OPS-572 at rank 4, CCC-433 at rank 5, BECKY-3454 at rank 1,
and OPS-189 at rank 1, each with future tickets/activity excluded.

The current durable sources contain more pairs than the filing's earlier
49-pair sample; the evaluator uses all eligible recorded pairs rather than
selecting that easier subset. Historical descriptions are not versioned, so
their current authored text is retained. This residual historical limitation
and locally unavailable source coverage are included in the machine report.
The retroactive candidate window uses creation time to avoid borrowing future
updates to old tickets. The live index additionally includes recent activity
on older tickets. Results are retrospective retrieval evidence, not a
controlled measurement of future worker behavior.

Manual review selected the first 20 distinct highest-ranked non-answer-key
hits from the duplicate cohort, in chronological order: 10 useful, 10 noise.
These are deliberately candidates outside the known duplicate answers, not a
precision estimate of all results. No historical tickets were auto-linked.

| Query → candidate | Verdict | Checked relationship |
| --- | --- | --- |
| OPS-53 → REPO-FIXER-154 | useful | Same Taskfilm Windows branch and denied push |
| OPS-60 → OPS-59 | useful | Same Luna replay/key pipeline, different blocker |
| OPS-61 → OPS-60 | useful | Same exhausted OpenRouter account; prior diagnosis |
| SMALL-BUGS-5 → CCC-94 | noise | Mobile load versus transcript controls |
| OPS-72 → OPS-69 | noise | BlueBubbles outage versus CCC process count |
| IDEAS-75 → CCC-55 | noise | iOS build versus host Jobs display |
| VM-NEXT-26 → VM-NEXT-27 | useful | FUB person deduplication and cleanup |
| REPO-FIXER-198 → REPO-FIXER-161 | useful | Same preview-export worktree family |
| REPO-FIXER-201 → REPO-FIXER-188 | noise | Different repos; identical sweep template |
| REPO-FIXER-221 → REPO-FIXER-81 | noise | Different inbox feature worktrees |
| REPO-FIXER-223 → OPS-163 | useful | Same BYM dirty file set blocks sync |
| SMALL-BUGS-40 → IDEAS-79 | noise | Shared screenshot path, different issue |
| REPO-FIXER-225 → REPO-FIXER-226 | useful | Same files in mirrored BYM checkouts |
| REPO-FIXER-226 → REPO-FIXER-225 | useful | Reverse of same mirrored-file incident |
| REPO-FIXER-232 → REPO-FIXER-198 | useful | Recurring preview-export job/history |
| CCC-229 → CCC-168 | useful | Shared latency sampling safeguards, different metric |
| REPO-FIXER-234 → REPO-FIXER-222 | noise | Different inbox worktree scopes |
| REPO-FIXER-237 → REPO-FIXER-218 | noise | BYM review versus STRAMP analytics |
| REPO-FIXER-240 → REPO-FIXER-216 | noise | BYM review versus intake router |
| REPO-FIXER-243 → REPO-FIXER-159 | noise | Review-feedback versus payment-match |

Live Jev validation used `jev-1.14.0`. A small three-candidate OPS-572 test
used 3,232 input tokens and returned plausible earlier-attempt suggestions.
After the context fix, the default ten-candidate WT-83 triage succeeded in
eight bounded requests / 42,052 input tokens; all ten were unrelated, consistent
with review of the suggestions. Partial-context indicators were preserved.

Raw benchmark, manual-review and live API artifacts live under
`/home/hermes/dev/scratch/wt83/`; they include ticket prose and are not committed.
Tests cover source completeness, no retrieval subprocesses, claim JSON,
visible recall errors, retrospective boundaries, FTS escaping/stemming,
custom/catch-all queue membership, explicit confirmation/deep escalation,
fragile-job digest routing, feedback retry deduplication, bounded semantic
batches and deduplicated recent commits.

## Remaining launch acceptance

WT-83 stays open until its own seven-day feedback check is reported. Once
the implementation is integrated and adopted, record the launch instant and
defer WT-83 for seven days as a data wait; this is not a release wait. On return:
verify current claim output and no added retrieval gh calls, report
`wt related --feedback-report --since LAUNCH_ISO --json`, including missing or
unavailable data, and inspect a few confirmed links/fragile-job rows. Do not
turn an unresolved original symptom into a follow-up success close.

CCC-574 tracks the genuinely separate retirement of the ineffective
spawn-time queue-worker `shipped_check`; it depends on WT-83 completion.
Rollback is a normal revert of the implementation commit. The local source
snapshots/index can remain as inert private data; they never control ticket
state without an explicit worker confirmation.

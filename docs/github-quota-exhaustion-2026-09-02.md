# Why the GraphQL quota is still exhausted (measured 2026-09-02)

Companion to [`github-read-caps.md`](./github-read-caps.md). That document
explains the caps' *design*. This one measures what they actually do on this
machine, and why WATCHTOWER-16 (`4ed9b11`), WATCHTOWER-19 (`38b4a42`),
`97e2bd8` and OPS-589 (`b285ac3`) have not stopped the account
(`amirfish1`, user ID 255024423) from exhausting GraphQL every hour.

**No code was changed. This is diagnosis only.**

## Headline

| Measurement | Value |
|---|---|
| GraphQL quota | 5,000 points/hour |
| Measured burn, **idle** (16:56–17:05Z, no human input, nothing dispatched) | **5,383 pts/hr** |
| Measured burn, **active** (17:09–17:14Z, two workers claiming) | **6,550 pts/hr** |
| Share of heavy GraphQL calls issued by `ai.watchtower.watcher` | **91 % (59 / 65)** |

The fleet's *idle background polling alone* costs more than the entire hourly
quota. Exhaustion is therefore not an incident — it is the steady state. Every
hour the account burns to zero, and whatever real work needs GraphQL at that
moment (a worker's `wt claim` / `wt close`) fails.

Method: `gh api graphql -f query='{rateLimit{used}}'` sampled every 15–60 s
(that query itself is free — measured cost 0), correlated with a 0.1 s `ps`
sampler recording every `gh` invocation with its parent PID. Raw data in
`~/Library/Logs/gh-quota-probe/`.

## Part 1 — the `gh api rate_limit` discrepancy, settled

At 09:35 PDT GraphQL returned rate-limit-exceeded while `gh api rate_limit`
reported `graphql: 5000 remaining, used 0`. Reproduced live at 16:48:27Z, both
counters read back-to-back through the same `gh` binary and the same keyring
token:

```
REST  /rate_limit  →  graphql: {limit:5000, used:0,    remaining:5000, reset:1788371307}
GraphQL rateLimit  →           {limit:5000, used:5000, remaining:0,    resetAt:"16:53:43Z"}
```

Three facts pin it down:

1. **The REST view never increments.** Sampled once a minute for six minutes,
   `resources.graphql.used` stayed at `0` and its `reset` slid forward with the
   clock (`…307 → …368 → …430 → …491 → …552 → …612`) — always "now + 1 h,
   nothing used". It is a bucket that is allocated on read and never written to.
2. **The GraphQL view is the enforced one.** It reported `used: 5000` with a
   *fixed* `resetAt` of 16:53:43Z, and at 16:53:44Z the block lifted and `used`
   dropped to 0. The counter that predicted the recovery is the real one.
3. **It is the primary limit, not a secondary one.** The error is
   `{"type":"RATE_LIMIT","code":"graphql_rate_limit","message":"API rate limit
   already exceeded for user ID 255024423."}` — GitHub's per-user *primary*
   GraphQL point limit. No `403`, no "you have exceeded a secondary rate limit",
   no concurrency message. And `used` genuinely reads 5000/5000.

Not a second token: `gh auth status` shows one account, one `gho_` keyring
token; no `GH_TOKEN`/`GITHUB_TOKEN` in the environment, no `.netrc`, no second
credential helper.

Why `used` reads 0 *during* a block is simply that rejected requests cost
nothing, so the phantom bucket has nothing to count even in principle.

**Consequence — and this is the important part.** Two independent pre-emptive
guards are wired to that phantom counter:

- `github_backend._graphql_rate_limit_remaining()` → `gh api rate_limit --jq
  .resources.graphql.remaining`, compared against `_GH_GRAPHQL_LOW_THRESHOLD =
  300` at `_list_issues` (github_backend.py:1303).
- CCC's `ccc_server/github_issues._check_gh_rate_limit()` → the same jq path,
  compared against `_GH_RATE_LIMIT_LOW_THRESHOLD = 200`, driving the 5 s queue
  watch loop's back-off.

Both read a value that is **always 5000**. Neither has ever fired. We measured
21 of these guard calls in a 5-minute window, every one of them returning
"plenty of headroom" while the account was burning at 109 points/minute.
WATCHTOWER-19's commit message already identified this for the *error* path and
correctly switched that path to observed evidence — but the **pre-emptive**
guard was left reading the phantom counter, in both codebases.

The reliable probe, for anything that needs one:
`gh api graphql -f query='{rateLimit{remaining resetAt}}'` — free, and truthful.

## Part 2 — where the 5,000 points go

Measured per-call costs (BYM-Finie, deltas on `{rateLimit{used}}`):

| call | shape | points |
|---|---|---|
| `gh issue list --state closed --limit 1000 --search closed:>=…` | **wt** | **26 – 32** |
| `gh issue list --state open --limit 1000` | wt | 4 – 6 |
| `gh issue view N --json …,comments` | wt `get()` | ~3 |
| `gh pr list --limit 100 --json …statusCheckRollup` | CCC | 3 – 5 |
| `gh issue list --state open --limit 100` | CCC cross-repo | ~1 |
| `gh issue list --state closed --limit 60` | CCC cross-repo | ~1 |
| REST `GET /repos/{o}/{r}/issues?per_page=100` | — | **0** |
| conditional REST probe returning 304 | — | **0** (also free on REST core) |

The closed-state list is **5–30× everything else**, because BYM-Finie has 742
issues closed in the last 45 days and `--limit 1000` pages through them with
`body`, `labels` and `assignees` attached. Nothing in the design prices the two
states differently — they share one cadence and one cap, so the cheap state's
polling frequency sets the expensive state's bill.

### Attribution, 294-second window (17:08:57 – 17:13:51Z)

Burn over the window: `used` 1484 → 2019 = **535 points / 294 s = 109 pts/min**.

| process | heavy GraphQL calls |
|---|---|
| pid 26170 — `ai.watchtower.watcher` (`watchtower.cli start --foreground`) | **59** |
| pid 26097 — CCC server (`claude-command-center/server.py`) | 3 |
| short-lived `wt` CLI (worker claims) | 3 |

Also in the window: 145 conditional ETag probes (free), 24 **unconditional**
`gh api -i repos/…/issues?…` probes with no `If-None-Match` (always 200 → always
"changed" → always fetch), 21 dead `rate_limit` guard calls, 2 mutations.

### Top 3 consumers

**#1 — `poll_owner_answers_once()` re-reading permanently blocked tickets.
~1,800 pts/hr (≈36 % of quota).**

53 of the daemon's 59 heavy calls were `gh issue view`. Two tickets account for
50 of them:

```
25 × gh issue view 739  --repo amirfish1/BYM-Finie
25 × gh issue view 1135 --repo amirfish1/BYM-Finie
```

That is one live GraphQL fetch **per ticket per ~12 seconds, indefinitely**.
`ingest_owner_answer()` calls `self.get(ident)` — the only path that fetches
comments — *before* it can decide there is nothing to ingest, and there is no
cache, no throttle and no "I already checked this comment set" short-circuit on
that call. The sweep runs from the daemon's 5 s poller loop. Its docstring says
"a live `gh issue view` is spent only on tickets that are actually blocked —
normally zero"; that is true only when *no* ticket is blocked. Two stuck tickets
cost ~600 views/hr ≈ 1,800 points/hr, forever, whether or not anyone is working.

This path is entirely outside the read-cap design: it is not `_list_issues`, so
neither the ETag probe nor `_LIST_FETCH_MIN_INTERVAL_S` applies to it.

**#2 — BYM-Finie closed-state list refetch. ~1,800 pts/hr (≈36 %).**

5 closed-state fetches in 5 minutes ≈ 1/min ≈ the intended 60 s cap — but at
~30 points each that cap still authorises 1,800 pts/hr from a single repo/state
pair, before any duplication. Plus ~4 open-state fetches (~5 pts) ≈ 240 pts/hr.

**#3 — duplicated polling across processes + CCC's own fan-out. ~900–1,400 pts/hr.**

CCC runs a structurally identical poll in a *different process*: its 5 s
`_gh_queue_watch_loop` calls WatchTower's `list_items()`, which re-enters
`github_backend` with its own module-level `_LIST_CACHE`, its own ETag, and its
own 60 s cap. The caps are per-process, not per-account, so N processes buy N ×
the cap. In the window CCC was observed issuing the same ETag probes against
BYM-Finie that the daemon was issuing, seconds apart. On top of that CCC calls
`gh issue list` **directly** (not through `_list_issues`) in
`ccc_server/cross_repo_issues.py:96,118` across 11 amirfish1 repos on a 5 min
TTL, and `gh pr list --json …statusCheckRollup` on a **30 s** TTL
(`morning_launch.py:_OPEN_PRS_TTL`) — precisely the "direct `gh issue list`
anywhere else re-opens the OPS-838 hole" failure that `github-read-caps.md`
warns about, committed in a different repo where that warning isn't read.

## Part 3 — why the shipped mitigations don't hold

Diffing observed behaviour against what each commit claims:

### `97e2bd8` (refresh invalidated snapshots) is the amplifier

`_invalidate_list_cache(repo)` **deletes** the persisted key for both states
after every mutating `gh issue` verb (`_run`, github_backend.py:1007 —
`create`/`edit`/`close`/`reopen`/`comment`). `refresh_persisted_list_cache()`
then does:

```python
if key not in _read_persisted_list_cache():
    _LIST_CACHE.pop(key, None)
```

so the poller also throws away its *in-memory* entry. With no cached entry,
`_list_issues` skips the ETag probe **and** `_LIST_FETCH_MIN_INTERVAL_S` — both
require `cached is not None` — and goes straight to a full uncapped GraphQL
fetch of **both** states. That is ~35 points on BYM-Finie, guaranteed, per
mutation.

BECKY/BECKY-TEACH/BECKY-DESIGN ran 12 claims + 12 closes in the sample hour, and
each claim/close is several mutating verbs. Caught live in the correlated log:
`gh issue edit 1170` (a BECKY-TEACH claim) at 17:09:13 and `gh issue edit 1210`
(BECKY-DESIGN) at 17:09:30, bracketing full open+closed refetches at 17:09:00,
17:09:17 and 17:09:33 — **one uncapped ~35-point double fetch every ~15 s**,
four times the intended rate, exactly while workers were claiming.

There is a second-order ratchet: a fetch reached via the no-cache path stores
`etag = ""` (only a 200 from the probe sets an ETag). So the *next* probe is
unconditional, returns 200, and authorises another fetch. That is the 24
`If-None-Match`-less probes in the window.

**WATCHTOWER-16 shares the ETag and fetch clock across processes; `97e2bd8`
deletes them on every write.** The two mitigations work against each other, and
writes are exactly what a busy queue does.

### The warm cache has zero coverage for the repo that matters

`~/.watchtower/gh-list-cache.json`, sampled repeatedly through the
investigation:

```
amirfish1/claude-command-center:open    at_age=1168s  fetch_age=3091s   n=7
amirfish1/claude-command-center:closed  at_age=1168s  fetch_age=6663s   n=10
amirfish1/stramp-platform:open          at_age=1177s  fetch_age=152717s n=2
amirfish1/stramp-platform:closed        at_age=1177s  fetch_age=6293s   n=13
test-owner/test-repo:open               (test fixture, 21 days old)
```

**`amirfish1/BYM-Finie` is absent entirely** — the repo behind BECKY,
BECKY-TEACH, BECKY-DESIGN and BYM-GH-FINIE (4 of the 6 GitHub-backed queues, 38
open / 742 recently-closed issues). It is deleted by every claim/close and only
restored by a *successful* GraphQL fetch, which fails while rate-limited. So the
busiest repo is permanently in the cold, uncapped path, and every reader —
daemon, CCC, and each short-lived `wt` process — pays full price for it. That is
self-sustaining: rate-limited → no cache → uncapped fetches → rate-limited.

The entries that do exist are also stale beyond `_PERSISTED_LIST_STALE_S`
(300 s) at `at_age` ≈ 1170 s, so soft readers reject them and fall back to live
reads anyway.

### WATCHTOWER-19 holds off only *after* the damage

`gh-connectivity.json` showed the hold working as designed
(`rate_limited_until: 16:52:53Z`, serving stale data). But it is reactive: it
trips on an observed failure, which by definition means the quota is already
gone. The pre-emptive guard that was supposed to prevent reaching that point
reads the phantom counter (Part 1) and never fires. And the hold is wt-local —
CCC does not read `gh-connectivity.json`, so it keeps calling GitHub throughout
wt's hold window.

### OPS-589 / `b285ac3` works, and hides the problem

Serving persisted stale data during backoff is why `wt status` keeps working and
why this has read as intermittent worker deaths rather than a permanent quota
deficit. It is the right behaviour; it just means the only symptom is the one
thing that *can't* be served from cache — `strict=True` writes, i.e. `wt claim`
and `wt close`. Which is precisely what playbook §32 documents.

## Recommended fixes, ranked by effort

Nothing below is implemented. Items 1–3 are the ones that change the arithmetic;
the rest is cleanup.

### 1. Throttle `ingest_owner_answer`'s live read — ~10 lines, ≈1,800 pts/hr

The blocked-ticket sweep calls `get()` (a GraphQL `gh issue view`) every ~12 s
per blocked ticket forever. Give `poll_owner_answers_once()` a per-ticket
interval (60–120 s is plenty for a human typing a comment), or check the
already-warm list snapshot's `updatedAt` and skip the view when the issue hasn't
moved. Biggest saving per line changed of anything here, and it touches one
function that no correctness path depends on.

### 2. Stop `_invalidate_list_cache` from destroying the ETag and fetch clock — small, ≈1,000–2,000 pts/hr

Mark the entry stale instead of deleting it: keep `etag` and `fetched_at`, clear
only the data (or set an `invalidated_at` that forces exactly *one* strict
re-read by the poller while every other reader keeps revalidating conditionally).
Read-your-own-writes is preserved — the writer already has its own result — but
a mutation stops handing every reader in the fleet an uncapped double fetch.
This is also what would let BYM-Finie stay in the persisted cache at all.

Companion: when a fetch is reached via the no-cache path, capture the response
ETag so the next probe is conditional, closing the unconditional-probe ratchet.

### 3. Split the closed-state cadence from the open-state cadence — small

Closed lists cost 26–32 points; open lists cost 4–6. A closed-issue list does
not need 60-second freshness. A separate `_LIST_FETCH_MIN_INTERVAL_CLOSED_S` of
5–10 minutes cuts ~1,800 pts/hr to ~200–350 with no user-visible change (the
closed list only backs completed-ticket views and the 14-day retention window).

### 4. Fix both pre-emptive quota guards to read the truthful counter — small, high leverage

Replace `gh api rate_limit --jq .resources.graphql.remaining` with
`gh api graphql -f query='{rateLimit{remaining resetAt}}'` (free) in
`github_backend._graphql_rate_limit_remaining()` **and** in
`ccc_server/github_issues._check_gh_rate_limit()`. Today both guards are dead
code that costs a subprocess each and protects nothing. This alone converts the
existing back-off machinery from reactive to actually pre-emptive.

### 5. Move wt's hot read paths from `gh issue` to REST — medium, structural

Every field wt asks for is available on REST
`GET /repos/{o}/{r}/issues?state=…&per_page=100`: `number, title, body, state,
html_url, assignees, labels, created_at, updated_at, closed_at`. REST costs
**zero GraphQL points**, supports `If-None-Match` (a 304 is free on REST core
too), and REST core is currently ~97 % idle — measured 123–150 of 5,000 used per
hour while GraphQL sat at 5,000/5,000. Mutations map cleanly too
(`PATCH /issues/{n}`, `POST /issues/{n}/comments`). One caveat: the REST issues
endpoint includes pull requests, so filter items carrying a `pull_request` key.
This is the fix that removes the class of problem rather than tuning it, and the
ETag probe already proves the REST path works from this machine.

### 6. Give CCC's GitHub reads a shared budget — medium, cross-repo

CCC duplicates wt's poll in a second process and additionally calls
`gh issue list` directly across 11 repos, bypassing `_list_issues` entirely. Two
options, cheapest first: (a) have CCC's queue watch loop read
`~/.watchtower/gh-list-cache.json` and `gh-connectivity.json` rather than
re-entering `github_backend`, so wt's hold and cap actually bind it; (b) route
`cross_repo_issues` through `_list_issues`, as `github-read-caps.md` already
instructs. Also raise `_OPEN_PRS_TTL` from 30 s — there are 14 open PRs across
all 11 repos, they do not need two-minute-per-hour resolution.

### 7. A second credential for automation — larger, last resort

A GitHub App installation token (or a separate PAT) for the daemons would give
polling its own 5,000 pts/hr bucket and stop background reads from starving
interactive `wt claim`/`wt close`. Worth doing *after* 1–5: at today's burn a
second bucket would also be exhausted within the hour, so it buys headroom, not
a fix. GitHub App installation tokens additionally get a higher GraphQL limit
that scales with repo/user count, which would help if the fleet grows.

## Verification

Re-measure with the same method — after any change, the number to move is:

```bash
gh api graphql -f query='{rateLimit{used remaining resetAt}}'   # free; sample every 60s
```

Target: idle burn comfortably under ~2,000 pts/hr, so that interactive
`wt claim`/`wt close` always have headroom. Items 1–3 alone should get there.

## Post-deploy measurement — fix 1 shipped (2026-09-02, 18:25–18:45Z)

Fix 1 landed as `05fa957` and went live with
`launchctl kickstart -k gui/501/ai.watchtower.watcher` (wt is an editable
install, so the change is inert until the daemon is restarted — OPS-589).

**Fix 1 did exactly what it claimed, and total burn did not move.** Both halves
of that sentence are measured, and the second one is the useful finding.

The call it targeted is gone. `gh issue view` over a 180-second process sample:

| Window | `gh issue view` |
|---|---|
| Before (294 s, 17:08–17:13Z) | 53 |
| After (180 s, 18:39–18:42Z) | 2 |

Total burn over the same period, sampled every 60 s:

| Window | Burn |
|---|---|
| Before (idle) | 5,383 pts/hr |
| Before (active) | 6,550 pts/hr |
| After (active, 10 samples) | **5,760 pts/hr** |

Against an active-window baseline that is a real but small improvement, and the
account still hit `used=5000 remaining=0` before the 18:53Z reset. Removing
~1,800 pts/hr from a ~6,550 pts/hr active burn cannot by itself get under 5,000,
and the composition of the remaining traffic had already shifted.

### What is burning it now

Distinct `gh` invocations, 180 s, with parent attribution:

| Invocation | Count | GraphQL? |
|---|---|---|
| `gh api -i -H 'If-None-Match: ...'` | 118 | no — conditional REST, free |
| `gh issue list` | 27 | **yes** |
| `gh api rate_limit` | 18 | no — and it is the phantom counter (Part 1) |
| `gh issue edit` | 9 | **yes**, and each one invalidates the cache |
| `gh pr list` | 3 | yes |
| `gh issue view` | 2 | yes |

The ETag layer is working where it has an ETag: 118 conditional probes in three
minutes cost nothing, and the repos holding a real ETag
(`claude-command-center`, `stramp-platform`) were last fetched 2.6–44 hours ago.
That is the read-cap design behaving exactly as `docs/github-read-caps.md`
describes.

### The residual is one repo, and it is fix 2

`~/.watchtower/gh-list-cache.json`, read live during the sample:

| Key | ETag | Age |
|---|---|---|
| `amirfish1/BYM-Finie:open` | **`""` (empty)** | 53 s |
| `amirfish1/claude-command-center:open` | present | 2.6 h |
| `amirfish1/claude-command-center:closed` | present | 3.6 h |
| `amirfish1/stramp-platform:open` | present | 44 h |
| `amirfish1/stramp-platform:closed` | present | 3.5 h |

`BYM-Finie` — the one repo with live workers, and the source of the 9
`gh issue edit` calls in the sample — is stuck in the ratchet described under
"`97e2bd8` is the amplifier". Every mutation makes `_invalidate_list_cache`
delete the persisted key; the next fetch takes the no-cache path, which stores
`etag=""`; an empty ETag makes the following probe unconditional; that fetch
mutates nothing but re-stores `etag=""`. The entry can never heal on its own,
so this repo pays full price on every refresh, forever, at ~9 `gh issue list`
per minute.

Fix 1 was still worth shipping — it removed a genuinely uncapped path and it
holds. But the number that has to move next is fix 2, and the empty-string ETag
above is the single most direct evidence of it: the repo the fleet actually
works in is the one repo the warm cache cannot keep.

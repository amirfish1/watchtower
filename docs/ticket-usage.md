# Measured ticket token usage

WatchTower records token usage from engine telemetry when a worker claims and
closes, releases, blocks or loses a ticket. Supervised planners, plan reviewers,
verifiers and assessors are recorded per stage run and execution attempt,
including failed and fallback attempts with attributable telemetry. A model
requested in settings is provenance only: actual model names come from Claude
assistant messages or Codex turn context. No agent estimates token counts.

The same collector handles local and GitHub-backed worker claims and closes.
Stage recording follows WatchTower's existing stage supervisor; it does not add
new planning or verification support to backend workflows that lack those stages.

View a readable report with:

```sh
python3 -m watchtower.usage QUEUE-123
python3 -m watchtower.usage QUEUE-123 --json
```

The existing `wt find QUEUE-123 --json` response includes `token_usage`.
Close activity lines and supervised role completion/death activity lines include
measured input/cache/output subtotals and actual models. Reports show each role,
run, attempt, actual model and completeness. An unavailable value is `null` in
JSON and `?` in text; a measured zero remains `0`.

## Provider semantics

| Report counter | Claude telemetry | Codex telemetry |
| --- | --- | --- |
| `input` | `input_tokens` (ordinary input) | `input_tokens - cached_input_tokens` |
| `cache_read` | `cache_read_input_tokens` | `cached_input_tokens` |
| `output` | `output_tokens` | `output_tokens` |
| `cache_write` | `cache_creation_input_tokens` | Unsupported; `null` |

Claude input excludes cache reads and cache writes. Cache writes are preserved
separately, rather than silently folded into ordinary input. Codex input includes
cache reads, so they are subtracted exactly once. Missing Codex cache counts make
ordinary input unknown. Raw provider counters and source semantics remain in the
private ledger. Codex reasoning tokens are not added to output a second time.

Repeated Claude stream blocks are deduplicated by message id. Codex cumulative
observations are differenced; repeated totals are ignored, and resets or truncated
initial totals remain unknown. Per-ticket claim snapshots prevent charging a
reused session's earlier work to its next ticket. Settlement replaces an attempt's
observations and recomputes totals; it never adds the same attempt twice.

## Partial and settled usage

A close tool can run before the enclosing engine turn emits its final telemetry.
The immediate report is explicitly partial. The existing stage reconciliation
pass and the report command re-read late telemetry. A uniquely attributable
session can settle after a provider completion marker or a recorded process exit.
The report's completeness then becomes `complete` for the exposed observations.
This means the available telemetry was accounted for, not that every provider
supports every counter.

When a shared engine turn/session spans multiple ticket or role attempts, late
cumulative usage cannot be assigned honestly to one attempt. It is preserved
privately as `late_observed`; attempts remain `ambiguous_shared_session` and the
public report remains partial. WatchTower does not estimate a split or change
execution to one ticket per turn. Ambient human/manual review sessions without a
supervised role run do not have attributable stage counters. Missing transcripts,
unsupported engines, unavailable baselines, malformed lines and unavailable
actual models are never treated as measured zeroes.

`totals` is conservative: a counter is unknown if any included attempt lacks it.
`measured_totals` is the subtotal of counters that were actually observed; it is
always displayed with completeness, so it cannot be mistaken for a final bill.

## Storage and privacy

Raw transcripts are not copied. Provider observations, baseline samples, session
and worker provenance, source paths and late unallocated usage stay in a local
private ledger (`usage/` beside the queue store; override with
`WATCHTOWER_USAGE_DIR`). Ledger filenames hash the backend/repository/queue/ticket
identity, files are mode `0600`, and writes are locked and atomic. Each ledger has
an 8 MiB retention bound; exceeding it drops raw snapshots, marks the record
partial with `retention_limit`, and makes conservative totals unknown.

Ticket metadata contains a bounded projection: the last 32 attempt summaries,
all-attempt counts and totals, actual model names, provider semantics and telemetry
status. It contains no transcript paths, baselines, raw provider observations or
session identifiers. GitHub issue bodies receive only this safe projection during
existing lifecycle writes. Late reconciliation performs no remote writes; local
reads expose the latest private projection. A different machine without the
private ledger sees the last safe summary written to the issue.

Telemetry collection failures annotate an unknown/partial result and do not block
a ticket's lifecycle transition. Existing tickets are not retroactively charged
an entire transcript when no claim-boundary baseline exists.

## Integration

The implementation is independent of execution fallback selection and the CCC
Settings tiers. It adds no runtime dependencies and no model calls. Integrate and
test it together with concurrent WatchTower changes before release. Restart the
WatchTower daemon and any CCC dashboard process importing `watchtower.queue` to
load the collector; already-running agent CLI processes need no restart.

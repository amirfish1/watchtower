# Graceful worker engine and model rotation

## Goal

Allow an operator to retire live workers that no longer match a queue's
configured engine or model, without interrupting their current ticket. The
next staffing slot must be filled using the updated queue configuration.

## Interface

`wt workers release --engine ENGINE [--queue QUEUE] [--json]` explicitly
retires live, non-adhoc workers selected by their recorded engine and optional
queue. `--engine` is required so an operator cannot accidentally retire an
entire fleet.

Changing either `wt set -q QUEUE --engine ...` or `--model ...` also invokes
the same retirement logic for that queue. A worker is mismatched when its
recorded engine differs from the configured engine, or when its recorded model
differs from the configured explicit model (including clearing an explicit
model override).

## Lifecycle

Retirement is graceful. WatchTower writes the worker's existing one-shot stop
sentinel and marks it released from queue staffing. It never signals or sends a
mid-task instruction to the worker and never changes its current ticket. The
worker completes its claim normally; when it next calls `wt claim`, it receives
`{"stop": true}` and exits its drain loop before claiming additional work.

Because the released worker no longer occupies a staffing slot, the next
reconciler pass can spawn a replacement using the queue's current engine and
model. A selected idle Claude worker is not woken merely to stop it; it remains
detached and is collected by the existing released-worker TTL path.

## Error handling and reporting

Already released, dead, adhoc, and nonmatching workers are skipped. The command
reports released workers and exits successfully with an explicit no-matches
message when nothing is eligible. JSON mode returns the same selected release
records for automation. A failure to persist release state rolls back the newly
written stop sentinel via the existing `request_stop` behavior.

## Tests

Lifecycle tests cover selecting only matching live queue workers, preserving an
in-progress ticket, and a replacement being eligible after release. CLI tests
cover the explicit subcommand and automatic retirement on both engine and model
configuration changes. Existing worker and full-suite tests provide regression
coverage.

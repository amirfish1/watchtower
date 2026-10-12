# Run a ticket during submission

Approved by the user on 2026-10-12: add `wt add --run`, equivalent to filing
the ticket and immediately calling `wt run <ref>`.

The command records the existing run request before dispatching through
`workers.dispatch_after_enqueue`. It bypasses the GitHub grace period,
auto-drain being off, and the auto-drain opt-out for that ticket only. Queue
policy and normal `wt add` behavior stay unchanged. Worker capacity, model
requirements, readiness, dependencies, and planning gates still apply;
worker startup means assignment cannot be promised at zero latency.

Reject `--run --claim` and `--run --group-parent` before filing: claiming for
the caller conflicts with requesting a worker, and group parents never run.
Group children may request a run but still wait for their accepted group plan.

Reuse `queue.mark_runnable` for both storage backends. If recording the run
request fails, return an error naming the already-filed ticket and do not
dispatch it. Dispatch remains best-effort, matching `wt run`.

Verification covers real CLI submission and reconciliation in an isolated
store, GitHub label persistence under the default grace period, invalid
combinations without filing, dependencies, and run-request failure.

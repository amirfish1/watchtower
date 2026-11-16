# Blocked-ticket polling (WT-81)

The local WatchTower HTTP server (default `127.0.0.1:8787`) exposes:

- `GET /api/blocked`: the same ticket array as `wt blocked --json`.
- `GET /api/blocked?queue=WT`: scope that shared snapshot to a queue.
- `GET /api/ticket/WT-81`: the current ticket, or HTTP 404 when absent.
  This returns the store projection, without CLI caller attribution or timeline
  decoration. Named refs are accepted, including stale suffixes.

Blocked reads share one process-wide snapshot for at most five seconds. Only
one request computes it; other requests reuse it. Reads honor existing GitHub
poller snapshots and quota/backoff rules, rather than forcing live GitHub reads.
A source failure without a usable fallback returns HTTP 503, not an empty list.
A corrupt local store also fails the blocked read rather than clearing pending
notifications. Upstream GitHub snapshot freshness remains governed by its own
poller contract, not by the five-second HTTP projection TTL.

GitHub listing parses lightweight issue metadata before projecting blocked
candidates. It includes local sidecar answers in routing, even when issue-level
`needs_input` has already cleared. Normal list projections use a distinct memo
key. Local reads copy only the selected blocked tickets or the requested single
ticket out of the read-only SQLite snapshot; callers still receive private data.

The iMessage router and the intake bridge's `WatchTowerClient.find/blocked` use
HTTP reads. `WATCHTOWER_API_URL` overrides the base URL. They never fall back to
spawning a CLI on an outage. Blocked reads raise without clearing notification
state. Intake ticket lookups preserve the existing unknown-state result on
transport failure: reconciliation skips it, and the keyed-create guard must
not dispatch it. Write commands retain their existing CLI path.
Deploy the server endpoints before restarting the clients. Restart only
`wt-imessage-router.service` and the four `client-intake@...` units; no gateway
or WhatsApp bridge restart is required.

Regression checks: `tests/test_blocked_polling.py`,
`tests/test_imessage_router.py`, the GitHub backend/sidecar suites, and the intake
repo's `scripts/test_watchtower_http_reads.py`. CPU evidence and adoption are
recorded in WT-81 and `/home/hermes/dev/scratch/wt81/` on host `her`.

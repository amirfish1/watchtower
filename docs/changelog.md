# Changelog

## 2026-10-31 — same-session takeover

- Add `wt takeover`, status, cancellation, dry-run, native observations and
  fenced legacy imports for existing Claude/Codex sessions on local queues.
- Persist supervision and delivery intents atomically with ticket adoption;
  reconcile independently of automatic queue draining and CCC list requests.
- Retain same-session answers, bounded reminders and ordinary closure gates.
- Add explicitly scoped, disabled-by-default candidate enrollment and a
  versioned optional CCC integration boundary. See [takeover](takeover.md).

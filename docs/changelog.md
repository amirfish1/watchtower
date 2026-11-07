# Changelog

## 2026-11-06 — bounded GitHub issue history (WT-47)

- Move the oldest history entries into hash-chained comment segments, so a long
  ticket's issue body stays bounded instead of refusing writes at GitHub's cap.
- Readers rebuild full history; an unverifiable chain is reported and each
  consumer fails closed.
- Off by default: `wt history-archive` turns it on per queue, with an attestation.
  See [history archive](github-history-archive.md).

## 2026-10-31 — same-session takeover

- Add `wt takeover`, status, cancellation, dry-run, native observations and
  fenced legacy imports for existing Claude/Codex sessions on local queues.
- Persist supervision and delivery intents atomically with ticket adoption;
  reconcile independently of automatic queue draining and CCC list requests.
- Retain same-session answers, bounded reminders and ordinary closure gates.
- Add explicitly scoped, disabled-by-default candidate enrollment and a
  versioned optional CCC integration boundary. See [takeover](takeover.md).

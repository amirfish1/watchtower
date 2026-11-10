# Changelog

## 2026-11-10 — built-in hard-stop floor

- Auto-decisions now work on a fresh install: the hard-stop floor is bundled, and
  `WT_HARD_STOPS_FILE` can add rules from your own file. See
  [worker runbook](worker-runbook.md#minor-blocks-proposing-an-auto-decision-ccc-370).

## 2026-11-09 — opt-in outcomes

- Add durable outcomes outside claimable tickets, with temporary bounded leads,
  ordinary executors, independent verification and evidence-based acceptance.
- Reconcile explicit session provenance and exact saved-decision receipts;
  retain ambiguous Unfiled work and source coverage without creating new goals.
- Route linked technical exhaustion through outcome recovery and preserve
  existing behavior when disabled. See [outcomes](outcomes.md).
- Keep dated lead recovery checks eligible while a ticket retains its claim,
  and avoid repeated lead reviews for unchanged Unfiled provenance.

## 2026-10-31 — same-session takeover

- Add `wt takeover`, status, cancellation, dry-run, native observations and
  fenced legacy imports for existing Claude/Codex sessions on local queues.
- Persist supervision and delivery intents atomically with ticket adoption;
  reconcile independently of automatic queue draining and CCC list requests.
- Retain same-session answers, bounded reminders and ordinary closure gates.
- Add explicitly scoped, disabled-by-default candidate enrollment and a
  versioned optional CCC integration boundary. See [takeover](takeover.md).

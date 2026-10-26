# Ticket notice length investigation (WATCHTOWER-2)

Verified 2026-10-21 against the local repository history, sender and receiver
transcripts, and real OS FIFO delivery in an isolated WatchTower store.

## Finding and decision

The 200-character detail limit is original to ticket notifications, not a later
regression. Commit `6d351674c5a35e07c958a2e18841f1b21cfd259c`
(2026-08-29, authored/committed as Amir Fish) introduced `_notify_ticket_event`
and `_clip(detail, 200)` together. The author field identifies the commit account;
it does not establish which human or agent chose the constant. Neither its
message nor the available code history documents a reason for choosing 200.
`git log --all -S '_clip(detail, 200)'` finds that introduction only; searching
notification changes across queue.py, github_backend.py, messages.py and their
tests found no earlier notification prototype or different detail limit in the
available refs. Unavailable branches cannot be ruled out.

`_clip` collapses whitespace for limits <=240, so this also erased bullets and
line breaks. Reuse of a short-preview helper is consistent with an arbitrary
preview default; that is an inference, not a recorded author explanation.

Choose option (c): preserve full actionable detail for `needs_input`, `closed`,
and `awaits_decision`. Only `claimed` uses a short one-line preview; if that
preview is clipped it includes `(full text: wt find <ref>)`. Claims currently
usually have no detail and remain subject to existing event preferences.

## History and evidence

- `fa060ed07a3bf2d08b70cd8ac417eb1c7db32eac` (2026-09-04,
  WATCHTOWER-22) added `notify=True` and the UDS/FIFO/delegate delivery class.
  Its commit message reports three claim notices costing about $0.24 because
  the previous adapter chain started headless full-context resumes. Its diff
  leaves the 200-character limit untouched. No evidence ties 200 to that cost.
- WT-31 history (including `5b75ae9`, `81eaafa`, `9aae91e`) concerns verified
  delivery, receipts and liveness, not a notification detail length budget.
  `wt find WATCHTOWER-22` and `wt find WT-31` return not found in today's
  migrated queue. GitHub issue searches for both old refs also returned no
  results. The commits and docs/worker-lifecycle.md preserve the available
  evidence; the missing historical ticket comments are not assumed reviewed.
- The original worker command is in
  `~/.watchtower/logs/bym-future-1832d05f.log`; the receiver transcript is
  `~/.claude/projects/-home-hermes-Apps-BYM-Finie/5de1b7ae-ccba-4d0d-9dc4-679e262505f8.jsonl`.
  At 2026-10-20 01:59:22 UTC the receiver got BYM-FUTURE-2419 ending
  `Luna replies plus one So…` inside a complete cross-session wrapper. The
  sender's command contains the complete question. This confirms sender-side
  clipping for this incident, not additional receiver clipping.

## Transport and storage constraints

| Path | Observed constraint and behavior |
| --- | --- |
| UDS | peer_uds.MAX_LINE_BYTES is 1 MiB per JSON frame; oversized frames raise rather than truncate. Wrapping preserves the body, escaping the closing wrapper tag. |
| FIFO | workers._stream_json_user_line serializes the whole body. OS pipe capacity and PIPE_BUF apply to the nonblocking write, not a 200-character semantic limit. Extremely large frames remain subject to existing FIFO write behavior. |
| Delegate | messages._delegate_payload sends full text in JSON; CCC's /api/inject-input reads Content-Length and retains payload.text. No 200-character clip found in this path. |
| Outbox | messages.outbox_add stores str(text) without clipping, and preserves notify=True on retry. |
| GitHub | Block questions and resolution summaries have existing 4,000-character storage caps. Generic comments have a 24,000-character application cap. These are distinct from the former push preview. |
| Receiver | CCC's peer wrapper retains the body; server._peer_body_from_wrapped extracts it intact. inject_receipts.text_preview is a separate 160-character diagnostic field, not the delivered body. Held-peer previews are not proof of the full delivered body. |

This change removes the extra notification preview cut; it does not remove
upstream ticket storage limits or claim unlimited transport capacity. Delivery
class, actor suppression, forwarding, retries and event preferences are unchanged.

## Measured cost comparison

The recovered original question is 462 characters. With its original ref and
notice prefix, the old notice is 244 characters / 248 UTF-8 bytes; the complete
notice is 505 characters / 507 UTF-8 bytes. Local tiktoken measurements:

| Encoding | Clipped notice | Complete notice | Additional input tokens |
| --- | ---: | ---: | ---: |
| cl100k_base | 74 | 145 | 71 |
| o200k_base | 75 | 146 | 71 |

These are reproducible tokenizer measurements, not Claude billing counts or a
claim that these encodings match Claude. The extra 71 tokens cost
`71 * input_price_per_million / 1,000,000` for one uncached inclusion, with later
context reads/caching depending on the receiver. They do not themselves create
another API turn. A mid-turn UDS notice adds message content to that turn; FIFO
or delegate behavior can still depend on receiver state. No live paid model call
was made for this measurement. The historical ~$0.08 per notice was a whole
resumed context, not the price of 261 additional characters.

Clipping here removed the requested repetitions, approval wording and cost-cap
question. A receiver would need another lookup and potentially another turn to
recover them. That incident is observed; the counterfactual recovery dollar
cost was not measured and is not presented as fact.

## Verification receipt

The original BYM-FUTURE-2419 question was replayed through enqueue -> claim ->
block -> messages.send(notify=True) -> a real OS FIFO -> parsed stream-json
receiver in an isolated store (SUB-1). No unrelated session received a test
message and no model was launched. Exact received notice:

```text
[watchtower] SUB-1 needs input — Owner approval needed before spending tokens (the ticket requires it). OK to run: (1) becky:eval before/after on 39b18cf90/f523b830b with --model=openrouter:openai/gpt-6-luna (Luna replies plus one Sonnet grader call per finding); (2) about 7 sibling replays via replay-fixture.ts; (3) 5 repeats of the '\$816 or \$812?' run? Reply 'go' (and any token or cost cap) and the worker will run them, fix the prompt if the question recurs, and report the score change.
```

Tests additionally assert exact multiline preservation above 1,500 characters,
Unicode text, complete close summaries, the clipped-claim pointer, real FIFO
block/close delivery (including long Hebrew text with JSON-escaped frames larger
than PIPE_BUF), and GitHub-backed block/close notification bodies. Existing
messaging tests verify the live-only delivery class and outbox behavior.

Validation results: 58 notification tests passed after the final edits; the
initial combined notification/messaging run passed 187 tests (56 notification
plus 131 messaging). The broader queue/GitHub-backend rerun passed all 160 tests.
The first broader run had one transient failure in
`test_github_backend_rejects_crossworker_close` (read `open` instead of
`in_progress`); that test passed both on unchanged HEAD in an isolated archive
and in the full rerun. It has no notification targets. No unrelated test or
ownership code was changed.

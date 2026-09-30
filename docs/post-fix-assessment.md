# Post-fix assessment (WT-21)

After a bug ticket closes as completed, an independent **assessor** session
answers six questions about the fix, and WatchTower files deduplicated
follow-up tickets. It is opt-in per queue.

| point | question |
|---|---|
| `logging` | Was logging adequate to diagnose this without guessing? |
| `ui_message` | Was the message the user saw adequate? |
| `automation` | Can it be fully automated (detect, ask, act, recover, notify)? |
| `monitoring` | Did internal monitoring alert the owner? |
| `auditors` | Would the auditors / self-healing have caught it? |
| `other` | Any other small improvements? |

## Enable

```bash
wt config -q CHUCK --repo-path /path/to/local/git/checkout --post-fix-assessment on
wt config -q CHUCK --assessor-engine codex --assessor-model <model>   # optional
```

`--post-fix-assessment on` is refused (exit 2) unless the queue has a local git
`--repo-path`. The assessor runs there, never in the caller's cwd. The default
assessor is a different engine family than the builder.

## When it fires

On close (or `wt accept` / a passing `wt verdict`) of a ticket that is a bug,
on an opted-in file-backed queue, not declined, not a "duplicate of" close, and
not itself filed by an assessment. Gated tickets wait until they really close.
Spawn failures never change `wt close`'s exit code; the assessment is marked
`failed` (retry with `wt assess run REF`). The reconcile sweep spawns due
assessors, resumes half-filed ones, and retries a dead assessor once.

## Commands

- `wt assess show REF [--json]`: state, points, follow-ups.
- `wt assess run REF [--force] [--dry-run]`: (re)start the assessor; `--dry-run` prints repo, assessor, prompt and reserves nothing.
- `wt assess submit REF --token T (--json OBJ | --file PATH|-)`: what the assessor runs. `--force` (human) reserves a fresh attempt.
- `wt assess resume REF`: finish filing; no LLM needed.
- `wt assess new -q QUEUE --title T [--text X] (--commit SHA | --no-code) [--json]`: for fixes that had no ticket (e.g. an auditor self-heal). Files a bug and closes it through the normal close path, so gates run and the assessment fires on real close.

Submit payload: all six keys required, each `{"verdict": "adequate"|"gap", "note": "...", "followups": [{"title","note","queue"}], "existing": ["REF"]}`. A `gap` needs a follow-up or an existing open ref; max 3 follow-ups per point, 8 total. Follow-ups are filed `source=post-fix-assessment`, `blocked_by=[REF]`, deduplicated by title against active tickets (a hit gets a marker comment instead of a new ticket).

## Exit codes

`assess submit`: 0 filed; 1 superseded (`ASSESS SUPERSEDED`) or incomplete (`ASSESS INCOMPLETE`, resume with `wt assess resume`); 2 invalid payload/token. `assess new`: the close exit code (0 closed or in review), 2 for bad flags. `--json` prints `{ref, status, assessment, close_exit}`.

## Auditor contract

An auditor that self-heals a bug calls, after the heal succeeds and only then:

```
wt assess new -q <queue> --title "<what was healed>" --text "<detail>" (--commit SHA | --no-code) --json
```

No call after a failed heal.

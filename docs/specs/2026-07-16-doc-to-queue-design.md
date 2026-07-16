# Document to Queue Design

## Goal

`wt import <file> -q <QUEUE>` uses one reasoning-model call to turn a plan,
specification, meeting note, mission brief, or other text document into a
trustworthy Watchtower queue. Tasks may be implicit in prose. Markdown
structure is evidence for the model, not an extraction mechanism.

The command previews by default and files only with `--apply`. Precision is
more important than recall because garbage tickets destroy queue trust.

## Why extraction requires reasoning

A deterministic Markdown parser can find checkboxes but cannot reliably decide
whether a paragraph implies work, whether three bullets are one coherent task,
whether a decision is already complete, or which task depends on another. The
importer therefore sends the entire numbered document to one LLM call. Line
numbers and structural syntax remain intact as hints and provenance anchors.
No per-line or per-section model calls are allowed.

The default adapter shells out to the locally authenticated Claude CLI. It uses
non-interactive print mode, safe mode, Sonnet by default, no tools, JSON output
constrained by a JSON Schema, and no session persistence. Safe mode prevents
local skills and plugins from inflating the reasoning context. The full prompt
travels on stdin so large documents do not hit operating-system argument-size
limits. Environment overrides allow a different Claude binary or model without
adding runtime dependencies.

## Command surface

```text
wt import FILE -q QUEUE [--apply] [--type bug|feature]
```

Like `wt dedup`, preview is the default and `--apply` is the only mutation
switch. Every invocation, including a repeat import, performs exactly one
reasoning call so the current document is assessed as a whole. `--type`
overrides the model's classification for all newly filed tickets.

## Reasoning prompt and schema

The prompt includes the resolved source path and the complete document with
stable display line labels such as `L0001`. It instructs the model to:

- infer explicit and implicit work actually called for by the document;
- return one ticket per coherent unit that a worker can complete in one focused
  session;
- merge micro-steps that share one outcome and split giant tasks with distinct
  deliverables or independently verifiable outcomes;
- keep enough context, constraints, and acceptance criteria in each body for a
  worker to act without rereading the whole document;
- order prerequisites before dependents;
- use exact earlier ticket titles in `depends_on`;
- classify each ticket as `bug` or `feature`;
- cite the narrowest source line or range supporting the task;
- omit background, context, code examples, alternatives not selected,
  decisions already made, completed work, and observations with no requested
  action;
- prefer omitting an ambiguous item over inventing work.

The strict response shape is:

```json
{
  "tickets": [
    {
      "title": "Short imperative title",
      "body": "Full actionable context and completion criteria",
      "type": "feature",
      "depends_on": ["Exact earlier ticket title"],
      "source_anchor": "L12-L24"
    }
  ]
}
```

No extra object keys are accepted. Titles must be unique, non-empty, at most
200 characters, and free of newlines. Bodies must be non-empty. Anchors must
refer to real ascending lines in the source. Dependencies must refer to unique
earlier tickets, cannot refer to self, and therefore cannot form cycles.

## Granularity target

One ticket represents one worker session's coherent unit of work. A useful
ticket has one main outcome, enough local context to implement it, and a clear
way to know it is complete. Setup and tests that exist only to deliver that
outcome stay in the same ticket. Work with a separate deliverable, separate
ownership, or an independent verification gate becomes another ticket.

The prompt includes positive and negative examples: it rejects one giant
"build the feature" ticket and dozens of mechanical line-item tickets. The
model must choose the smallest set of independently drainable tickets that
still preserves meaningful outcomes and dependencies.

## What is not a task

The model must not turn these into tickets unless the document explicitly calls
for new work based on them:

- background and rationale;
- context, goals, and non-goals;
- code snippets and examples;
- decisions already made;
- completed checkboxes and status reports;
- risks or observations with no requested mitigation;
- references and source lists;
- rejected alternatives;
- headings used only for navigation.

Structural cues such as unchecked checkboxes, action-item headings, and
numbered steps increase confidence but never override semantic judgment.

## Validation and mutation safety

The complete model response is parsed and validated before reading queue state
or filing anything. A missing Claude binary, timeout, nonzero exit, invalid JSON,
missing or extra field, invalid type, bad anchor, duplicate title, forward or
unknown dependency, or empty value produces a clear nonzero error. The importer
files no tickets from a malformed batch.

Dry-run prints every validated ticket, its type, dependency titles, body, and
source reference, followed by new and existing counts. `--apply` uses that same
validated in-memory batch. There is no second reasoning call.

Queue writes are sequential because refs are assigned at enqueue time.
Dependencies are ordered earlier by validation, so a dependent ticket body can
record resolved Watchtower refs for prerequisites. If a storage failure occurs
after a successful validation, the command reports the exact created count;
rerunning remains safe because every created ticket already has its import key.

## Provenance and idempotence

Each validated ticket records:

- `source`: `doc-import`
- `url`: `<resolved-source-path>#<source_anchor>`
- `repo_path`: the source document's parent directory
- body prefix: `Imported from: <resolved-source-path>#<source_anchor>`
- `id`: a versioned stable import key

The key is:

```text
doc-import:v3:<sha256(canonical-source-path + normalized-cited-source + occurrence)>
```

Generated title and body wording are not identity because repeated model calls
can phrase the same work differently. The importer normalizes the exact source
passage cited by the model, so moving unchanged task prose to different lines
does not change its key. An occurrence counter keeps distinct tickets grounded
in the same passage separate. Before filing, the importer compares keys with
tickets in every state in the target queue. A repeat import skips existing
keys, while a newly cited source passage creates a new key and ticket.

## Components

- `watchtower/document_import.py` owns prompt construction, the single Claude
  call, response decoding, strict validation, candidate identity, provenance,
  dependency validation, and import planning.
- `watchtower/cli.py` owns arguments, preview presentation, type override, and
  sequential enqueue with resolved dependency refs.
- `tests/fixtures/doc_import/` provides checkbox, heading, numbered, and messy
  prose documents as whole-document reasoning inputs.
- `tests/test_document_import.py` uses controlled model responses to test the
  reasoning contract without network or model cost, plus subprocess adapter
  tests for malformed and unavailable responses.

## Cost and determinism guardrails

- Exactly one model process is started per command invocation.
- The full document is sent once, not once per structural item.
- The model has no tools and cannot mutate files or external systems.
- JSON Schema constrains generation and local validation distrusts the result.
- The prompt fixes granularity, ordering, exclusion, and schema rules.
- Preview is mandatory unless the user explicitly passes `--apply`.
- No retry occurs automatically because a retry would violate the one-call
  guardrail and incur hidden cost.

## Verification

Automated tests cover all four fixture shapes, implicit prose work, one-call
counting, prompt line labels, granularity instructions, dependencies,
provenance, type override, idempotent re-import after edits, dry-run mutation
safety, malformed JSON, invalid schemas, unavailable and failed model calls,
and the guarantee that invalid batches file nothing.

Manual evidence uses the real Claude CLI and an isolated Watchtower store. One
dry-run previews the reasoned queue, one apply creates it, and a second apply
creates zero tickets. The full repository suite must pass.

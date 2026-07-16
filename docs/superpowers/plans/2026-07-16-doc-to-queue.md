# LLM-Powered Document to Queue Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace deterministic document parsing with one strict, whole-document reasoning call that produces validated, provenance-preserving, idempotent Watchtower tickets.

**Architecture:** `document_import.py` numbers the whole source, calls the Claude CLI exactly once with JSON Schema output, validates the complete ticket graph, and computes stable source keys. The CLI previews the validated graph by default and enqueues it only with `--apply`, resolving dependency titles to earlier Watchtower refs.

**Tech Stack:** Python 3.11, Claude CLI, `subprocess`, `json`, `hashlib`, `dataclasses`, `pathlib`, pytest

## Global Constraints

- Work only in `/Users/amirfish/Apps/watchtower-wt-doc-to-queue` on `feat/doc-to-queue`.
- Extraction is a reasoning problem; regex and Markdown parsing cannot be the mechanism.
- Run exactly one reasoning call per import and never retry automatically.
- Preview by default and require `--apply` for queue mutation.
- Validate the full model batch before filing any ticket.
- Preserve source document and anchor provenance on every ticket.
- Re-importing the same inferred task must not duplicate it.
- Keep Watchtower runtime dependencies stdlib-only.
- Do not use em dashes in new user-facing copy.
- Do not push, merge, or publish.

---

### Task 1: Specify the whole-document reasoning contract

**Files:**
- Modify: `tests/test_document_import.py`
- Modify: `tests/fixtures/doc_import/messy.md`

**Interfaces:**
- Produces test contract for `extract_document(path, reasoner=None)`.
- A reasoner receives one prompt plus one JSON Schema and returns one decoded object.

- [ ] Replace parser-output tests with controlled whole-document responses for checkbox, heading, numbered, and implicit-prose fixtures.
- [ ] Assert one reasoner invocation, full numbered source in the prompt, explicit granularity rules, and no deterministic fallback.
- [ ] Add malformed response, invalid dependency, invalid anchor, duplicate title, and unavailable reasoner tests.
- [ ] Run `python3 -m pytest tests/test_document_import.py -q` and confirm failures against the parser implementation.

### Task 2: Implement one-call extraction and strict validation

**Files:**
- Replace: `watchtower/document_import.py`

**Interfaces:**
- Produces `ImportCandidate`, `ImportPlan`, `ReasoningError`.
- Produces `extract_document(path, reasoner=None) -> list[ImportCandidate]`.
- Produces `plan_import(candidates, existing_items) -> ImportPlan`.
- Default adapter invokes Claude once using `--json-schema`, `--output-format json`, no tools, and no session persistence.

- [ ] Delete regex and Markdown task classification.
- [ ] Implement full-document line labeling and the granularity/exclusion/dependency prompt.
- [ ] Implement the single subprocess call with configurable binary, model, and timeout.
- [ ] Decode Claude's structured output and reject unavailable, failed, timed-out, or malformed responses.
- [ ] Validate exact fields, values, unique titles, real anchors, and earlier-only dependencies.
- [ ] Build v3 keys from canonical source path, normalized cited source, and same-passage occurrence, independent of generated wording and line movement.
- [ ] Run focused tests and confirm all reasoning-contract tests pass.

### Task 3: Make preview and apply consume one validated graph

**Files:**
- Modify: `watchtower/cli.py`
- Modify: `tests/test_document_import.py`

**Interfaces:**
- CLI consumes `extract_document` exactly once.
- Apply resolves dependency titles to refs from existing or newly filed prerequisite tickets.

- [ ] Add failing CLI tests for one call, dry-run detail, malformed-batch no-op, type override, dependency refs, and idempotent edits.
- [ ] Replace `parse_document` use with `extract_document` and catch `ReasoningError` loudly.
- [ ] Print title, type, dependencies, body, and provenance during dry-run.
- [ ] Enqueue only after complete validation and append resolved prerequisite refs to dependent bodies.
- [ ] Run `python3 -m pytest tests/test_document_import.py tests/test_smoke.py -q`.

### Task 4: Update documentation and verify end to end

**Files:**
- Modify: `README.md`
- Modify: `watchtower/skills/watchtower/SKILL.md`

- [ ] Explain that one whole-document reasoning call infers implicit work and chooses worker-session granularity.
- [ ] Document Claude CLI availability, preview-first safety, malformed-response failure, provenance, dependencies, and idempotence.
- [ ] Run the real Claude CLI against one sample plan with a fresh isolated store: preview, apply, repeat apply, inspect JSON.
- [ ] Run `python3 -m pytest -q`, all CLI help paths, `python3 -m compileall -q watchtower`, and `git diff --check`.
- [ ] Commit only feature paths using conventional commit copy and preserve the worktree without push or merge.

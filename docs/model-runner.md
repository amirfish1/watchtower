# Managed structured model execution

Applications can request a capability profile through `wt model run --json`.
The command reads one JSON request from stdin and emits one normalized response.
Configure `model_profiles.fast`, `.standard` and `.deep` in CCC Settings. Each
profile contains an ordered `models` list with engine, model and effort. The
caller supplies a profile, not a provider or model identifier.

```json
{"profile":"deep","system":"Return a JSON object.","prompt":"Answer true.","json_schema":{"type":"object","properties":{"ok":{"type":"boolean"}},"required":["ok"],"additionalProperties":false},"tools":"none","max_budget_usd":5,"timeout_seconds":480,"max_output_bytes":2097152}
```

The shared `worker_fallback.enabled` switch controls whether execution may
advance beyond the first configured route. Only a confirmed provider quota
error advances to the next candidate; authentication errors, timeouts, invalid
JSON/schema and other failures terminate immediately. Each candidate is tried
once. The elapsed-time limit covers the whole invocation, and output is bounded.
A successful result includes `structured_output`, provider usage and an
`execution` record identifying the selected profile/model/effort and attempts.
No queue claims or delivery actions occur.

## Current capability limitation

Only Claude supports the enforced `tools:none` contract in this version:
`--tools ''`, strict MCP configuration, no session persistence and a maximum
USD budget of 5. Profile configurations containing an unsupported candidate
fail with `profile_capability_unavailable` before any prompt is sent.

The installed Codex 0.159.3 runtime rejects `tools.disable_defaults` and
`include_apply_patch_tool` under strict configuration. Disabling shell, app,
plugin, browser and subagent features still leaves execution/collaboration
tools visible. Read-only sandboxing is insufficient for a tool-free consumer,
so Codex execution remains disabled here until an enforceable supported tool
restriction is available. Queue-worker Codex fallback is unaffected.

Supported schema vocabulary: type, properties, required, additionalProperties,
items, enum, const, anyOf, minimum/maximum, min/maxItems, min/maxLength, pattern,
and descriptive title/description/default. Unsupported keywords fail before
invocation. Provider output is validated against the original schema.

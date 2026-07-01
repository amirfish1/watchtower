# Cross-harness agent discovery of WatchTower

**Problem:** a session (Claude Code, Codex, or any other agent harness) is
asked to look up an existing ticket (e.g. "check HERMES-20") and has no
reliable way to know that WatchTower exists, that the `wt` CLI can answer
that question, or how to call it. Today that only works if the session
happens to have filesystem access to `~/.watchtower/` and either already
knows the CLI or reverse-engineers it from the raw JSON.

Current state, for reference:
- `wt` is a stdlib-only Python CLI (`watchtower/cli.py`) — no daemon
  required to read data (`wt ls -q <QUEUE> --json`, `wt status --json`).
- Tickets live in one JSON store (`~/.watchtower/queues.json` by default,
  see `store_path()` in `watchtower/queue.py`), keyed by queue/ref.
- There's a local HTTP API (`watchtower/dashboard.py`), but only when the
  service is running (`wt start`/`wt dashboard`): `GET /api/queue/<name>`,
  `GET /api/status`, `POST /api/queue/<name>/add`, `POST
  /api/ticket/<ref>/run`. No cross-queue search or single-ticket-by-ref
  lookup endpoint exists yet (`wt ls` requires knowing the queue name up
  front).
- No MCP server, no Claude Code skill, ships today.

## Option 1 — Repo convention: AGENTS.md / CLAUDE.md snippet

Ship a canonical snippet (e.g. `contrib/AGENTS.snippet.md`) that
`wt install`/`wt set` offers to append to the target repo's `AGENTS.md` or
`CLAUDE.md`. It tells any agent reading its own project instructions: "this
repo uses WatchTower; tickets are `<PROJECT>-<N>`; run `wt ls -q <PROJECT>
--json` or `wt claim ...` to interact with them."

- **Pros:** Zero new infrastructure — reuses a convention nearly every
  coding-agent harness already reads at session start (Claude Code's
  `CLAUDE.md`, Codex's `AGENTS.md`, and most others). Works identically
  regardless of vendor, since it's just text in a file the harness already
  loads. Cheap to build (one template + one CLI flag).
- **Cons:** Only fires for sessions rooted in that repo — a session in an
  unrelated repo (like the one that triggered this ticket, asking about
  `HERMES-20` from outside the HERMES repo) never sees it. Requires the
  snippet to be installed per-repo and kept in sync by hand if the CLI
  surface changes. Purely advisory text, not an enforced contract.

## Option 2 — Claude Code skill (`SKILL.md`)

Package a `watchtower` skill (matching the existing Superpowers skill
pattern already in use in this environment) that teaches a Claude Code
session the `wt` command surface and gets auto-discovered via the `Skill`
tool once installed.

- **Pros:** Matches this environment's existing skill-discovery UX exactly
  (see `using-superpowers`) — very low friction for Claude Code users
  specifically, since skill matching happens automatically before every
  response. Can encode judgment ("if asked about `<PROJECT>-<N>`, run `wt
  ls -q <PROJECT> --json` and grep the ref"), not just static facts.
- **Cons:** Claude-Code-only. Does nothing for Codex or other harnesses —
  directly fails the "across Claude, Codex, etc." requirement in this
  ticket. Also requires a manual skill-store install step per machine, same
  distribution problem as option 1 but narrower payoff.

## Option 3 — MCP server

Build a small `watchtower-mcp` server exposing tools like `wt_status`,
`wt_search(ref)`, `wt_claim`, `wt_close` over the Model Context Protocol,
backed by the same `watchtower/queue.py` read functions the CLI already
uses.

- **Pros:** MCP is the one part of this stack that's genuinely
  cross-vendor — Claude Code, and a growing set of other agent harnesses,
  speak it natively, so a single server could serve "Claude, Codex, etc."
  the way the ticket asks for. Structured tool schema means an agent
  doesn't need to know shell syntax at all, just call `wt_search`. Also
  gives WatchTower a versioned API surface instead of scraping CLI output.
- **Cons:** The most infrastructure of the four — a long-running process to
  install, configure, and keep patched; MCP support is inconsistent in
  non-Claude harnesses today (Codex CLI support is partial/evolving as of
  this writing); over-built for the actual ask in this ticket ("find an
  existing ticket by ref").

## Option 4 — Total Recall indexing

Ingest `~/.watchtower/queues.json` (or the per-queue exports) into Total
Recall so `/recall HERMES-20` surfaces it from any session's memory layer.

- **Pros:** No new server or install step — reuses infrastructure already
  running on this machine. Good for the "did we discuss this before"
  fuzzy-recall use case.
- **Cons:** Total Recall itself is local to this machine/session history,
  not something a fresh `pip install watchtower` user gets by installing
  WatchTower from GitHub — so it doesn't generalize to "external user...
  across Claude, Codex, etc." It's also read-only and lagged (index
  refresh interval), so it can't claim/close a ticket or reflect state
  filed seconds ago — it answers "what was said," not "what's the current
  ticket status."

## Recommendation

**Ship Option 1 now, treat Option 3 as the real fix, skip 2 and 4 as
primary investments.**

The ticket's own framing — "external user who installs WatchTower from
GitHub and needs to use it across Claude, Codex, etc." — rules out anything
Claude-Code-specific (Option 2) and anything tied to this machine's local
history (Option 4) as the *primary* answer; at best they're nice-to-have
add-ons for power users already on this stack.

That leaves a build-vs-convention choice between Option 1 and Option 3.
`wt` is deliberately a zero-dependency, stdlib-only CLI (see README) — its
whole design bet is "the binary is the API." An MCP server is the properly
general answer for structured, ref-lookup, cross-harness use ("find ticket
X regardless of which queue it's in") and is worth building, but it's real,
ongoing infrastructure (a process to install/run/patch) for a feature that
today is "one command's worth of missing functionality" (`wt` has no
single-ticket-by-ref lookup across queues — `wt ls` needs the queue name
up front; that's the actual gap this ticket's transcript hit).

So: land Option 1 immediately (cheap, works today, matches how both Claude
Code and Codex already discover per-repo conventions) as the near-term fix,
and treat Option 3 (MCP server) as the follow-up investment once there's a
concrete cross-queue lookup API worth exposing — starting with adding a
`wt find <ref>` command (search all queues for one ref, no `-q` required)
as the underlying primitive both the AGENTS.md snippet and a future MCP
server would call.

# Cross-harness agent discovery of WatchTower

**Status: implemented.** `wt find <ref>` ships (`watchtower/cli.py`), and the
bundled skills (`watchtower`, `critique`, `group-chat-checkin` under
`watchtower/skills/*/SKILL.md`) are synced into every installed harness —
Claude Code (`~/.claude`), Codex (`~/.codex`), and Antigravity (`~/.gemini`)
— by `wt install` / `wt skills sync` (`watchtower/skills_sync.py`); see the
README's "Agent skills" section. The
repo-convention (Option 1) and MCP server (Option 3) below remain unbuilt;
this doc's option comparison and reasoning are kept as the design record.

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
- No MCP server ships today. Skills for WatchTower don't exist yet either,
  but the skill *mechanism* itself is already cross-harness: this machine
  has both `~/.claude/skills/<name>/SKILL.md` and `~/.codex/skills/<name>/SKILL.md`
  populated (confirmed by inspection — Superpowers skills are mirrored,
  same YAML-frontmatter-plus-body format, into both directories). Skills
  are a **global, user-level** install, not scoped to any repo.

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
- **Cons — insufficient alone:** it's repo-scoped, so it only fires for a
  session whose cwd *is* the specific repo that got the snippet installed.
  It cannot help a session in an unrelated repo, or a session with no repo
  context at all (a dashboard chat, a Slack bot, a session in a different
  project referencing another project's ticket by ref) — which is exactly
  the case that prompted this ticket ("check HERMES-20" from elsewhere).
  It also doesn't reach an external user automatically: `pip install
  watchtower` gives them the CLI, but the snippet only lands in a given
  repo if someone remembers to run the install step *in that repo*, once
  per repo that touches a WT queue. Best treated as a cheap complement, not
  the primary fix.

## Option 2 — Skill, built for both Claude Code and Codex concurrently

Package a `watchtower` skill (`SKILL.md` + reference docs, matching the
Superpowers pattern) that teaches a session the `wt` command surface —
e.g. "if asked about a ref like `<PROJECT>-<N>`, run `wt ls -q <PROJECT>
--json` and grep the ref; to see what's stuck, run `wt status --json`."
Ship one skill source and install/symlink it into **both**
`~/.claude/skills/watchtower/` and `~/.codex/skills/watchtower/` — the same
mirroring approach already used for the Superpowers skill set on this
machine, so there's a working precedent for "one skill, both harnesses."

- **Pros:** Global, not repo-scoped — once installed, it's available
  regardless of which repo (or no repo) the session is rooted in, which is
  the actual gap Option 1 leaves open. Cross-harness: Codex resolves skills
  from its own global `~/.codex/skills/` directory using the identical
  `SKILL.md` format, so "Claude-only" is not a real constraint here — a
  single source can target both engines concurrently with no forking.
  Auto-discovered before every response (matches this environment's
  `using-superpowers` UX) rather than needing to be remembered. No daemon,
  no server — it just teaches the agent to shell out to the already-simple
  `wt` CLI. Can encode judgment, not just static facts.
- **Cons:** Still needs a one-time install step per machine (no
  auto-distribution yet) — mitigated by having `wt install` (or a new `wt
  skill install`) drop/symlink it into both directories automatically, the
  same way `pip install -e .` today installs the `wt` binary. No
  marketplace/registry entry exists yet for either ecosystem, so discovery
  still depends on the user running that install step once.

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

**Ship Option 2 (skill, both harnesses) as the primary near-term fix. Add
Option 1 (AGENTS.md/CLAUDE.md snippet) as a cheap complement, not a
substitute. Treat Option 3 (MCP server) as the longer-term structured-API
investment. Skip Option 4 as a primary investment.**

*(Revised from an earlier draft of this doc, which wrongly called the skill
option Claude-only and recommended shipping the repo snippet alone as the
near-term fix — corrected after confirming Codex resolves skills from its
own global `~/.codex/skills/` directory, in the same `SKILL.md` format, on
this machine.)*

The ticket's framing — "external user who installs WatchTower from GitHub
and needs to use it across Claude, Codex, etc." — is really asking for
*global* recognition, independent of which repo (if any) the session
happens to be rooted in. Option 1 alone doesn't clear that bar: it's
repo-scoped, so it's silent for exactly the case that prompted this ticket
(a session referencing another project's ticket from outside that
project). Option 2 does clear it — a skill installs once, at the user
level, and is available regardless of cwd, for both Claude Code and Codex
concurrently, with no server to run.

Total Recall (Option 4) is out as a primary answer for the same reason as
before: it's local-machine history, not something a fresh `pip install
watchtower` gives an external user, and it's read-only/lagged.

`wt` is deliberately a zero-dependency, stdlib-only CLI (see README) — its
whole design bet is "the binary is the API," and a skill is the cheapest
way to make an agent aware that binary exists and how to call it, with no
new runtime component. An MCP server (Option 3) is the properly general
long-term answer for structured, versioned, ref-lookup access, but it's
real ongoing infrastructure (install/run/patch a process) for what's
"one command's worth of missing functionality" today — `wt` has no
single-ticket-by-ref lookup across queues yet (`wt ls` needs the queue name
up front; that's the actual gap this ticket's transcript hit).

So, concretely:
1. Add `wt find <ref>` — search all queues for one ref, no `-q` required.
   This is the one missing CLI primitive; both the skill and a future MCP
   server call it under the hood.
2. Build the `watchtower` skill now, sourced once, installed into both
   `~/.claude/skills/watchtower/` and `~/.codex/skills/watchtower/` (mirror
   the mechanism already used for Superpowers skills on this machine).
   Wire the install into `wt install`/`pip install -e .` so it isn't a
   manual step external users have to know to take.
3. Ship the AGENTS.md/CLAUDE.md snippet too — it's nearly free once the
   skill's content exists (it's a subset), and it covers the case where a
   user hasn't installed the skill on that machine but is working directly
   in a WT-integrated repo.
4. Treat the MCP server as the follow-up once `wt find` and the skill are
   live and there's a concrete structured-API need beyond "shell out to a
   CLI."

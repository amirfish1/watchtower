# Token Sitter

Auto-snapshot idle agent sessions before the prompt-cache cliff. When a long session sits idle, token-sitter triggers a lightweight checkpoint just before the 60-minute cache expiration window closes. After `/clear`, resume your work seamlessly from the saved snapshot without paying the cold-start context penalty.

## Commands

- `/auto-snapshot-on [minutes]` — Arm auto-snapshot for the current session (default: 55 idle minutes).
- `/auto-snapshot-off` — Disarm auto-snapshot and cancel pending timers.
- `/snapshot-now` — Immediately write a snapshot checkpoint without waiting for the timer.
- `/resume-from-snapshot [id|path]` — Load the newest snapshot for the current directory (or a specific snapshot) after `/clear`.

## Requirements

Requires the `wt` (WatchTower) CLI:

```bash
pipx install git+https://github.com/amirfish1/watchtower.git
```

## Engine Support Tiers

- **Claude**: Full auto-fire via live TUI keystrokes (Terminal.app/iTerm2) or headless resume.
- **Codex**: Auto-fire via headless app-server delivery.
- **Kimi / Grok / Devin**: Manual `/snapshot-now` and `/resume-from-snapshot` commands (auto-fire unsupported). Skills install into each harness's user-skills dir via `wt skills sync` (`~/.kimi-code/skills`, `~/.grok/skills`, `~/.config/devin/skills`).

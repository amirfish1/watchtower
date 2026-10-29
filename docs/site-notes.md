# WatchTower site (site/) - notes and deploy record

Standalone static marketing/docs site. Self-contained HTML/CSS, no build step, no
framework, mobile-clean. GitHub Pages deployment is live at
https://amirfish1.github.io/watchtower/. Custom domain https://watchtower.amirfish.ai
is configured in Pages but is not yet reachable (DNS setup pending, OPS-1476).

## Layout: what is public and what is not

Only `site/` is published. The Pages workflow (`.github/workflows/pages.yml`)
uploads that directory and nothing else. `docs/` holds internal design notes and is
never uploaded (the code cites several of them, and `workers.py` fetches
`docs/worker-runbook.md` from raw GitHub, so they stay where they are).

Public manifest (`site/MANIFEST.txt` is the source of truth):

- `index.html` - hero (the `wt wait` signature move), install, quickstart, honest scope
- `cli.html` - full CLI reference, checked against `wt --help` and every subcommand's `--help`
- `integration.html` - WT / CCC ownership boundary
- `styles.css` - shared stylesheet
- `.nojekyll` - serve files as-is
- `CNAME` - `watchtower.amirfish.ai`

Everything else in the repo is internal for the purposes of the site, including this
file, `docs/site-proof/` (render proofs), and `scripts/snapshot-site.js`.

## Checks

```bash
# puppeteer or puppeteer-core must resolve (NODE_PATH), else CCC's copy, else any
# ~/Apps/* checkout or PUPPETEER_DIR with it installed
node scripts/snapshot-site.js                         # serves site/, shots to docs/site-proof/shots/
SITE_URL=https://watchtower.amirfish.ai node scripts/snapshot-site.js   # check the live URL
```

The script fails if `site/` differs from `MANIFEST.txt`, if any public file contains an
em-dash or en-dash, or if a same-site link is broken.

## Refreshing cli.html

Dump `wt <cmd> --help` for every command in `wt --help`, diff against `site/cli.html`,
update the version in the eyebrow, the footer and the meta description. Last refresh: wt 0.8.3.

## Hosting

GitHub Pages, source "GitHub Actions" (not a branch folder, so `docs/` is never a
candidate). DNS for amirfish.ai is at Spaceship; the intended record is
`watchtower CNAME amirfish1.github.io`. At 2026-10-29
04:55 UTC, authoritative `launch1.spaceship.net` had no record for `watchtower`.
Spaceship login requires the owner's TOTP challenge before it can be added (OPS-1476). Copy rules: no
em-dashes, no new licence or commercial terms (the footer restates the README's
licence line), no promises about GitHub workflows beyond what the CLI flags show.

## Verification follow-up (SIDE-32, 2026-10-29)

- Pages run 40820258531 deployed main SHA 88e32bf successfully.
- All 56 documented top-level commands return successful `--help` output on wt 0.8.3.
- `wt wait` reads open-only health depth (`health.py`, `cmd_wait`); it does not
  wait for in-progress/review/blocked tickets. Site copy now states that limit.
- Default screenshot dependency fallback fails on this host (OPS-1477). Working
  dependency: `NODE_PATH=/Users/amirfish/.cache/bym-worktree-deps/ff166a8a00aede75/tree/node_modules`.
- Live Chromium checks failed with ERR_NAME_NOT_RESOLVED even for the working
  GitHub Pages hostname; local rendering remains available. Do not claim live
  browser verification until that check and the custom domain both pass.
- Independent local browser review passed all three pages at 1440 and 390 widths,
  same-site links and the manifest; mobile screenshots had no visible cutoff.
- Focused health tests: 12 passed; documented command signatures match current
  CLI help (nested model commands checked at their own help level).

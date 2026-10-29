# WatchTower site (site/) - notes and deploy record

Standalone static marketing/docs site. Self-contained HTML/CSS, no build step, no
framework, mobile-clean. Published at https://watchtower.amirfish.ai.

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
# puppeteer or puppeteer-core must resolve (NODE_PATH), or CCC's copy is used
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
candidate). DNS for amirfish.ai is at Spaceship; `watchtower` is a CNAME to
`amirfish1.github.io`, the same target `ccc.amirfish.ai` uses. Copy rules: no
em-dashes, no new licence or commercial terms (the footer restates the README's
licence line), no promises about GitHub workflows beyond what the CLI flags show.

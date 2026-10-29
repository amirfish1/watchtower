# WatchTower site (site/) - notes and deploy record

Standalone static marketing/docs site. Self-contained HTML/CSS, no build step, no
framework, mobile-clean. Published at https://watchtower.amirfish.ai with a valid
TLS certificate and HTTPS enforcement. Live desktop/mobile verification passed
on 2026-10-29 (SIDE-32).

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
candidate). DNS for amirfish.ai is at Spaceship; the authoritative record is
`watchtower CNAME amirfish1.github.io` (OPS-1476 resolved). GitHub Pages has
`https_enforced: true` and an approved certificate for `watchtower.amirfish.ai`.
Copy rules: no
em-dashes, no new licence or commercial terms (the footer restates the README's
licence line), no promises about GitHub workflows beyond what the CLI flags show.

## Verification follow-up (SIDE-32, 2026-10-29)

- Pages run 40820258531 deployed main SHA 88e32bf successfully.
- All 56 documented top-level commands return successful `--help` output on wt 0.8.3.
- `wt wait` reads open-only health depth (`health.py`, `cmd_wait`); it does not
  wait for in-progress/review/blocked tickets. Site copy now states that limit.
- Initial screenshot dependency/browser DNS failures were recorded in OPS-1477.
  The current default script now resolves its dependency and passes against the
  live HTTPS custom domain without overrides.
- Independent local browser review passed all three pages at 1440 and 390 widths,
  same-site links and the manifest; mobile screenshots had no visible cutoff.
- Focused health tests: 12 passed; documented command signatures match current
  CLI help (nested model commands checked at their own help level).

## Completed public delivery (SIDE-32, 2026-10-29 22:58 UTC)

- DNS is authoritative and correct. The initially absent certificate was recovered
  by removing/readding the same custom domain using GitHub's documented procedure
  (OPS-1497); no DNS targets, account scope or licence terms were changed.
- Valid TLS certificate for `watchtower.amirfish.ai` expires 2027-01-27. Pages
  HTTPS enforcement is enabled; an uncached HTTP path returns a 301 to HTTPS.
  Existing HTTP responses can remain in the edge cache for their 600-second TTL.
- `SITE_URL=https://watchtower.amirfish.ai node scripts/snapshot-site.js` passed
  all three live pages at 1440/390 and their same-site links/manifest checks.
- Independent live browser review confirmed viewport/scroll widths of 1440/1440
  and 390/390 on each page after layout settled. Screenshots:
  `/tmp/side32-live-{index,cli,integration}-{1440,390}.png`.
- Public HTML/CSS bytes match `site/`. Internal `/docs/site-notes.md`,
  `/docs/worker-runbook.md` and `/README.md` return 404; licence link preserved.

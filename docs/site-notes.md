# WatchTower site (docs/) - notes and deploy checklist

Standalone static marketing/docs site for WatchTower, built to be served from
GitHub Pages. Self-contained HTML/CSS, no build step, no framework, mobile-clean.

## Files

- `index.html` - hero (the `wt wait` signature move), what-it-is, install,
  quickstart, honest scope + engine matrix.
- `cli.html` - full CLI reference. Flags verified against `wt --help` (wt 0.1.0):
  status, workers, monitor, config, set, drain, add/take/edit/ls/find, import,
  wait, dashboard (serve), start/stop, and the worker protocol.
- `integration.html` - WT / CCC ownership boundary: WT owns the durable control
  plane, CCC is one client. Call direction, loop prevention, delivery states.
- `styles.css` - shared stylesheet (control-room theme; green = draining,
  red = stuck, amber = the watch light).
- `.nojekyll` - tells Pages to serve files as-is (no Jekyll processing).
- `snapshot-site.js` - headless render check (puppeteer): serves docs/ locally
  and screenshots every page at 1440 and 390 widths into `shots/`.
- `shots/` - render proof at both widths.

Voice/positioning reused from the CCC message-architecture and the WatchTower
product manifest. No em-dashes (owner rule).

## Verify locally

```bash
node docs/snapshot-site.js          # regenerates docs/shots/*.png
# or just open any page directly:
python3 -m http.server -d docs 8000 # then visit http://127.0.0.1:8000
```

## Deploy checklist (Amir's call - not done here)

1. **Choose the Pages source.** GitHub repo Settings > Pages > Build from a
   branch > `main` / `/docs`. (The site lives in `docs/`, alongside the existing
   internal design markdown, which Pages will also serve; that content is
   already public in the repo.)
2. **Custom domain (optional).** If you want e.g. `watchtower.amirfish.ai`, add
   a `docs/CNAME` file with that hostname and point a DNS CNAME at
   `<user>.github.io`. Not added here so it does not collide with any existing
   DNS plan.
3. **Confirm `.nojekyll` is present** (it is) so nothing gets Jekyll-mangled.
4. **Check the GitHub URL in the nav/footer.** Links point at
   `https://github.com/amirfish1/watchtower`; update if the canonical repo URL
   differs.
5. **Push.** Not done here per the brief (no push, no DNS/deploy).

## Known follow-ups (optional polish)

- The `shots/` PNGs are committed as proof; you may prefer to gitignore them and
  keep the snapshot script as the regeneration path.
- If Pages-serving the internal `docs/*.md` design files is undesirable, move the
  site into its own `site/` (or `docs/site/`) subtree and point Pages there.

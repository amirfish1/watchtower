// Headless render verification for the WatchTower static site.
// Adapted from the CCC snapshot.js puppeteer pattern. Serves docs/ over a
// throwaway localhost http server, then captures every page at desktop (1440)
// and mobile (390) widths. Uses puppeteer or puppeteer-core from NODE_PATH, else
// CCC's bundled copy. Set CHROME_PATH to pick a browser.
//
//   node scripts/snapshot-site.js                       (serve site/ locally)
//   SITE_URL=https://watchtower.amirfish.ai node scripts/snapshot-site.js
//                                                       (check a live URL)
//
// Output: docs/site-proof/shots/<page>-<width>.png (internal proof, not published)
// Also fails on: a file in site/ not listed in site/MANIFEST.txt, em-dashes in
// public copy, and broken same-site links.

const fs = require('fs');
const http = require('http');
const path = require('path');
const os = require('os');

const CCC = process.env.CCC_DIR || path.join(os.homedir(), 'Apps', 'claude-command-center');
let puppeteer;
for (const mod of ['puppeteer', 'puppeteer-core']) {
  try { puppeteer = require(mod); break; } catch (_) {}
}
if (!puppeteer) puppeteer = require(path.join(CCC, 'require-puppeteer.js'));

const DOCS_DIR = path.resolve(__dirname, '..', 'site');
const OUT_DIR = path.resolve(__dirname, '..', 'docs', 'site-proof', 'shots');

const PAGES = ['index.html', 'cli.html', 'integration.html'];
const WIDTHS = [1440, 390];

const MIME = {
  '.html': 'text/html', '.css': 'text/css', '.js': 'text/javascript',
  '.svg': 'image/svg+xml', '.png': 'image/png',
};

function findChromePath() {
  if (process.env.CHROME_PATH) return process.env.CHROME_PATH;
  const macs = [
    '/Applications/Google Chrome Beta.app/Contents/MacOS/Google Chrome Beta',
    '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
  ];
  for (const p of macs) {
    try { fs.accessSync(p, fs.constants.X_OK); return p; } catch (_) {}
  }
  return undefined;
}

function startServer() {
  const server = http.createServer((req, res) => {
    let rel = decodeURIComponent(req.url.split('?')[0]);
    if (rel === '/' || rel === '') rel = '/index.html';
    const filePath = path.join(DOCS_DIR, rel);
    // clamp to DOCS_DIR
    if (!filePath.startsWith(DOCS_DIR)) { res.writeHead(403); res.end(); return; }
    fs.readFile(filePath, (err, data) => {
      if (err) { res.writeHead(404); res.end('not found'); return; }
      res.writeHead(200, { 'Content-Type': MIME[path.extname(filePath)] || 'application/octet-stream' });
      res.end(data);
    });
  });
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => resolve({ server, port: server.address().port }));
  });
}

(async () => {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  // Manifest guard: site/ must contain exactly the files in MANIFEST.txt.
  const listed = fs.readFileSync(path.join(DOCS_DIR, 'MANIFEST.txt'), 'utf8')
    .split('\n').map((l) => l.trim()).filter((l) => l && !l.startsWith('#')).sort();
  const actual = fs.readdirSync(DOCS_DIR).filter((f) => f !== 'MANIFEST.txt').sort();
  if (JSON.stringify(listed) !== JSON.stringify(actual)) {
    throw new Error(`site/ does not match MANIFEST.txt\n listed: ${listed}\n actual: ${actual}`);
  }
  for (const f of listed) {
    if (/[\u2014\u2013]/.test(fs.readFileSync(path.join(DOCS_DIR, f), 'utf8'))) {
      throw new Error(`em/en-dash found in public file ${f}`);
    }
  }
  const live = process.env.SITE_URL ? process.env.SITE_URL.replace(/\/$/, '') : null;
  const { server, port } = live ? { server: { close() {} }, port: 0 } : await startServer();
  const base = live || `http://127.0.0.1:${port}`;
  console.log(`[snapshot] serving ${DOCS_DIR} at ${base}`);

  const chromePath = findChromePath();
  if (chromePath) console.log(`[snapshot] using chrome: ${path.basename(chromePath)}`);
  const browser = await puppeteer.launch({ executablePath: chromePath, args: ['--no-sandbox'] });

  try {
    for (const pageName of PAGES) {
      for (const width of WIDTHS) {
        const page = await browser.newPage();
        await page.setViewport({ width, height: width === 390 ? 844 : 900, deviceScaleFactor: 2 });
        const url = `${base}/${pageName}`;
        await page.goto(url, { waitUntil: 'load', timeout: 30000 });
        await page.waitForNetworkIdle({ idleTime: 500, timeout: 3000 }).catch(() => {});
        const bad = await page.evaluate(async () => {
          const out = [];
          for (const a of document.querySelectorAll('a[href]')) {
            const u = new URL(a.href);
            if (u.origin !== location.origin) continue;
            const r = await fetch(u.pathname).catch(() => null);
            if (!r || !r.ok) out.push(a.getAttribute('href'));
            else if (u.hash && !document.getElementById(u.hash.slice(1)) && u.pathname === location.pathname) out.push(a.getAttribute('href'));
          }
          return out;
        });
        if (bad.length) throw new Error(`broken links on ${pageName}: ${bad.join(', ')}`);
        const outName = `${pageName.replace('.html', '')}-${width}.png`;
        const outPath = path.join(OUT_DIR, outName);
        await page.screenshot({ path: outPath, fullPage: true });
        console.log(`[snapshot] wrote ${outName}  (${url})`);
        await page.close();
      }
    }
  } finally {
    await browser.close();
    server.close();
  }
  console.log('[snapshot] done');
})().catch((e) => { console.error(e); process.exit(1); });

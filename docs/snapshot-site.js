// Headless render verification for the WatchTower static site.
// Adapted from the CCC snapshot.js puppeteer pattern. Serves docs/ over a
// throwaway localhost http server, then captures every page at desktop (1440)
// and mobile (390) widths. Reuses CCC's bundled puppeteer + Chrome resolution.
//
//   node docs/snapshot-site.js            (run from repo root or docs/)
//
// Output: docs/shots/<page>-<width>.png

const fs = require('fs');
const http = require('http');
const path = require('path');
const os = require('os');

const CCC = '/Users/amirfish/Apps/claude-command-center';
const puppeteer = require(path.join(CCC, 'require-puppeteer.js'));

const DOCS_DIR = path.resolve(__dirname);
const OUT_DIR = path.join(DOCS_DIR, 'shots');

const PAGES = ['index.html', 'cli.html', 'integration.html'];
const WIDTHS = [1440, 390];

const MIME = {
  '.html': 'text/html', '.css': 'text/css', '.js': 'text/javascript',
  '.svg': 'image/svg+xml', '.png': 'image/png',
};

function findChromePath() {
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
  const { server, port } = await startServer();
  const base = `http://127.0.0.1:${port}`;
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
        const outName = `${pageName.replace('.html', '')}-${width}.png`;
        const outPath = path.join(OUT_DIR, outName);
        await page.screenshot({ path: outPath, fullPage: true });
        console.log(`[snapshot] wrote shots/${outName}  (${url})`);
        await page.close();
      }
    }
  } finally {
    await browser.close();
    server.close();
  }
  console.log('[snapshot] done');
})().catch((e) => { console.error(e); process.exit(1); });

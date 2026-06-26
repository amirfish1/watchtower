#!/usr/bin/env python3
"""WatchTower HTTP dashboard — a phone-first, read-only viewer.

Phase-2 surface over the same queue engine. Stdlib-only (``http.server`` +
``json``): no framework, no template engine, no runtime dependencies. It binds
``127.0.0.1`` by default (local-first) and renders live queue + worker health.

Routes:

    GET  /              mobile-first HTML page (auto-refreshing)
    GET  /api/status    {"queues": [...health rows + workers...], "workers": [...]}
    GET  /api/queues    raw per-queue counts (mirrors `wt queues`)

It reuses :mod:`watchtower.health` for the stuck computation and
:mod:`watchtower.workers` for liveness — neither is duplicated here.
"""

from __future__ import annotations

import html
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional

from . import health, queue as q, workers

REFRESH_SECONDS = 5


# --------------------------------------------------------------------------- data
def status_payload(stuck_minutes: int = health.STUCK_MINUTES) -> Dict[str, Any]:
    """Combined queue health + per-queue worker tally + the worker roster.

    One pass over workers (``worker_counts``) annotates every queue row, so the
    dashboard never probes liveness once per queue.
    """
    rows = health.all_status(stuck_minutes=stuck_minutes)
    counts = workers.worker_counts()
    for r in rows:
        wc = counts.get(r["queue"], {"total": 0, "live": 0})
        r["workers_total"] = wc["total"]
        r["workers_live"] = wc["live"]
    wrows = workers.list_workers(prune=False)
    workers.annotate_activity(wrows, q.list_items())
    return {"queues": rows, "workers": wrows}


# --------------------------------------------------------------------------- html
def _eta_line(row: Dict[str, Any]) -> str:
    """Prominent ETA readout for a card: 'clearing in ~20m' / 'stalled' / 'clear'.

    This is the line that makes the dashboard feel smart — a live estimate of
    when the queue empties, not just a static count."""
    if row.get("depth", 0) == 0:
        return '<span class="eta-clear">clear</span>'
    rate = row.get("drain_rate_per_min") or 0
    if not rate:
        return '<span class="eta-stalled">stalled</span>'
    eta = html.escape(str(row.get("eta_human") or "?"))
    return (
        f'<span class="eta-rate">~{html.escape(str(rate))}/min</span> · '
        f'clearing in <span class="eta-time">{eta}</span>'
    )


def _badge(row: Dict[str, Any]) -> str:
    if row["stuck"]:
        return '<span class="badge stuck">STUCK</span>'
    if row["depth"] > 0:
        return '<span class="badge live">LIVE</span>'
    return '<span class="badge clear">clear</span>'


def render_html(payload: Dict[str, Any]) -> str:
    rows: List[Dict[str, Any]] = payload["queues"]
    wkrs: List[Dict[str, Any]] = payload["workers"]

    if rows:
        cards = []
        for r in rows:
            workers_cell = f"{r.get('workers_live', 0)} live"
            if r.get("workers_total", 0) != r.get("workers_live", 0):
                workers_cell = (
                    f"{r.get('workers_live', 0)} live / {r.get('workers_total', 0)}"
                )
            cards.append(
                f"""    <div class="card {'is-stuck' if r['stuck'] else ''}">
      <div class="card-head">
        <span class="qname">{html.escape(str(r['queue']))}</span>
        {_badge(r)}
      </div>
      <div class="metrics">
        <div><span class="num">{r['depth']}</span><span class="lbl">open</span></div>
        <div><span class="num">{html.escape(str(r['oldest_open_age']))}</span><span class="lbl">oldest</span></div>
        <div><span class="num">{html.escape(workers_cell)}</span><span class="lbl">workers</span></div>
      </div>
      <div class="eta">{_eta_line(r)}</div>
    </div>"""
            )
        queues_block = "\n".join(cards)
    else:
        queues_block = '    <p class="empty">All queues clear.</p>'

    if wkrs:
        wrows = []
        for w in wkrs:
            live = w.get("alive")
            ref = w.get("active_ref")
            if ref:
                since = w.get("active_since_human")
                activity = html.escape(str(ref)) + (
                    f" <span class=\"ago\">({html.escape(str(since))})</span>"
                    if since
                    else ""
                )
            else:
                activity = '<span class="idle">idle</span>'
            wrows.append(
                f"""      <tr>
        <td class="wid">{html.escape(str(w.get('worker_id', '')))}</td>
        <td>{html.escape(str(w.get('queue', '')))}</td>
        <td>{w.get('pid', 0)}</td>
        <td><span class="badge {'live' if live else 'stuck'}">{'LIVE' if live else 'DEAD'}</span></td>
        <td class="activity">{activity}</td>
      </tr>"""
            )
        workers_block = (
            "    <table class=\"workers\">\n"
            "      <tr><th>worker</th><th>queue</th><th>pid</th><th>state</th>"
            "<th>activity</th></tr>\n"
            + "\n".join(wrows)
            + "\n    </table>"
        )
    else:
        workers_block = '    <p class="empty">No workers tracked.</p>'

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="{REFRESH_SECONDS}">
  <title>WatchTower</title>
  <style>
    :root {{ color-scheme: dark; }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0; padding: 16px;
      font: 16px/1.4 -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: #0f1115; color: #e6e8eb;
    }}
    h1 {{ font-size: 20px; margin: 0 0 4px; }}
    .sub {{ color: #8b93a1; font-size: 13px; margin: 0 0 18px; }}
    h2 {{ font-size: 15px; color: #8b93a1; text-transform: uppercase;
          letter-spacing: .06em; margin: 24px 0 10px; }}
    .card {{
      background: #181b22; border: 1px solid #232733; border-radius: 12px;
      padding: 14px 16px; margin-bottom: 12px;
    }}
    .card.is-stuck {{ border-color: #b3261e; background: #221413; }}
    .card-head {{ display: flex; align-items: center; justify-content: space-between; }}
    .qname {{ font-size: 19px; font-weight: 600; }}
    .metrics {{ display: flex; gap: 24px; margin-top: 12px; }}
    .metrics .num {{ display: block; font-size: 22px; font-weight: 600; }}
    .metrics .lbl {{ display: block; font-size: 12px; color: #8b93a1; }}
    .eta {{ margin-top: 12px; font-size: 14px; color: #8b93a1; }}
    .eta .eta-rate, .eta .eta-time {{ color: #e6e8eb; font-weight: 600; }}
    .eta .eta-stalled {{ color: #f3b14b; font-weight: 700; }}
    .eta .eta-clear {{ color: #5fd18a; font-weight: 600; }}
    td.activity {{ color: #e6e8eb; }}
    td.activity .ago {{ color: #8b93a1; }}
    td.activity .idle {{ color: #8b93a1; }}
    .badge {{
      font-size: 12px; font-weight: 700; padding: 4px 10px; border-radius: 999px;
      letter-spacing: .04em;
    }}
    .badge.stuck {{ background: #b3261e; color: #fff; }}
    .badge.live  {{ background: #15391f; color: #5fd18a; }}
    .badge.clear {{ background: #1d2230; color: #8b93a1; }}
    .empty {{ color: #5fd18a; background: #11231a; border: 1px solid #1c3a28;
              border-radius: 12px; padding: 16px; text-align: center; }}
    table.workers {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
    table.workers th {{ text-align: left; color: #8b93a1; font-weight: 600;
                        padding: 6px 8px; border-bottom: 1px solid #232733; }}
    table.workers td {{ padding: 8px; border-bottom: 1px solid #1a1d24; }}
    td.wid {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 13px; }}
  </style>
</head>
<body>
  <h1>WatchTower</h1>
  <p class="sub">{html.escape(str(q.store_path()))} · refreshes every {REFRESH_SECONDS}s</p>
  <section>
{queues_block}
  </section>
  <h2>Workers</h2>
{workers_block}
</body>
</html>
"""


# --------------------------------------------------------------------------- server
class _Handler(BaseHTTPRequestHandler):
    server_version = "WatchTower/dashboard"

    def _send(self, code: int, body: bytes, content_type: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _json(self, code: int, obj: Any) -> None:
        self._send(code, json.dumps(obj, indent=2).encode(), "application/json")

    def do_GET(self) -> None:  # noqa: N802 (http.server contract)
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        if path == "/":
            self._send(200, render_html(status_payload()).encode(), "text/html; charset=utf-8")
        elif path == "/api/status":
            self._json(200, status_payload())
        elif path == "/api/queues":
            self._json(200, q.queues())
        else:
            self._json(404, {"error": "not found", "path": path})

    do_HEAD = do_GET

    def log_message(self, *args: Any) -> None:  # silence per-request stderr noise
        pass


def serve(
    host: str = "127.0.0.1",
    port: int = 8787,
    once: bool = False,
) -> int:
    """Start the dashboard HTTP server.

    ``once=True`` handles a single request then returns — used by tests so the
    server doesn't block. Otherwise it serves forever until interrupted.
    """
    httpd = ThreadingHTTPServer((host, port), _Handler)
    bound_host, bound_port = httpd.server_address[0], httpd.server_address[1]
    if not once:
        print(f"WatchTower dashboard on http://{bound_host}:{bound_port}")
        print("  GET /            mobile-first viewer")
        print("  GET /api/status  queues + workers (JSON)")
        print("  GET /api/queues  per-queue counts (JSON)")
    try:
        if once:
            httpd.handle_request()
        else:
            httpd.serve_forever()
    except KeyboardInterrupt:
        if not once:
            print("\nstopped")
    finally:
        httpd.server_close()
    return 0

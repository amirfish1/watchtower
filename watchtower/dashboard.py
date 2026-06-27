#!/usr/bin/env python3
"""WatchTower HTTP dashboard — the night-watch operations console.

A read-only viewer over the same queue engine. Stdlib-only (``http.server`` +
``json``): no framework, no template engine, no runtime dependencies. It binds
``127.0.0.1`` by default (local-first) and renders live queue + worker health as
an instrument panel — calm and dark until a queue needs you, then it lights up.

Routes:

    GET  /                 the tower: fleet summary + queue instrument grid
    GET  /q/<queue>        per-queue drill-down (tickets, mirrors `wt ls`)
    GET  /api/status       {"queues": [...health rows + workers...], "workers": [...]}
    GET  /api/queues       raw per-queue counts (mirrors `wt queues`)
    GET  /api/queue/<name> active + closed tickets (closed carry resolution)

It reuses :mod:`watchtower.health` for the stuck computation and
:mod:`watchtower.workers` for liveness — neither is duplicated here.
"""

from __future__ import annotations

import html
import json
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List

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


CLOSED_LIMIT = 50  # cap the drill-down's closed section to the most-recent N.


def queue_tickets(name: str) -> List[Dict[str, Any]]:
    """Active (open + in_progress) tickets for one queue, mirroring ``wt ls``."""
    items = q.list_items(project=name)
    return [it for it in items if it.get("status") in ("open", "in_progress")]


def closed_tickets(name: str, limit: int = CLOSED_LIMIT) -> List[Dict[str, Any]]:
    """Closed tickets for one queue, most-recent first, capped to ``limit``.

    Each carries its ``resolution`` (when the closer recorded one)."""
    items = [it for it in q.list_items(project=name) if it.get("status") == "closed"]
    items.sort(key=lambda it: str(it.get("closed_at") or ""), reverse=True)
    return items[:limit]


# --------------------------------------------------------------------------- css
# The night-watch design system. One stylesheet, shared by every page.
_STYLE = """
    :root {
      --bg: #0C121E; --panel: #141D2C; --panel-2: #1B2638; --line: #25324A;
      --ink: #EAF1FB; --muted: #7E90AE;
      --calm: #38D39F; --beam: #6FB3FF; --warn: #FFB020; --alarm: #FF5C5C;
      color-scheme: dark;
    }
    * { box-sizing: border-box; }
    html { -webkit-text-size-adjust: 100%; }
    body {
      margin: 0;
      font-family: "Inter", system-ui, -apple-system, BlinkMacSystemFont,
                   "Segoe UI", Roboto, sans-serif;
      font-size: 15px; line-height: 1.5;
      background:
        radial-gradient(1200px 600px at 50% -200px, rgba(111,179,255,.07), transparent 70%),
        var(--bg);
      color: var(--ink);
      min-height: 100vh;
      -webkit-font-smoothing: antialiased;
      animation: pagefade .5s ease both;
    }
    @keyframes pagefade { from { opacity: 0; } to { opacity: 1; } }
    .wrap { max-width: 1100px; margin: 0 auto; padding: 20px 16px 64px; }

    .mono {
      font-family: "JetBrains Mono", ui-monospace, "SFMono-Regular", Menlo,
                   Consolas, monospace;
      font-variant-numeric: tabular-nums;
    }
    .disp {
      font-family: "Space Grotesk", system-ui, -apple-system, sans-serif;
      letter-spacing: -0.01em;
    }

    /* ---- header: the tower ---- */
    header {
      display: flex; align-items: center; justify-content: space-between;
      gap: 16px; flex-wrap: wrap;
      padding-bottom: 16px;
    }
    .brand { display: flex; align-items: center; gap: 12px; }
    .wordmark { font-size: 26px; font-weight: 600; letter-spacing: -0.02em; }
    .wordmark .lo { color: var(--muted); font-weight: 500; }
    .beacon {
      width: 13px; height: 13px; border-radius: 50%;
      background: var(--calm);
      box-shadow: 0 0 0 0 rgba(56,211,159,.55), 0 0 14px 2px rgba(56,211,159,.5);
      flex: none;
    }
    .beacon.alert {
      background: var(--warn);
      box-shadow: 0 0 0 0 rgba(255,176,32,.6), 0 0 16px 3px rgba(255,176,32,.55);
      animation: beat 2.4s ease-in-out infinite;
    }
    .beacon.dim { background: var(--line); box-shadow: none; }
    @keyframes beat {
      0%, 100% { box-shadow: 0 0 0 0 rgba(255,176,32,.55), 0 0 16px 3px rgba(255,176,32,.5); }
      50% { box-shadow: 0 0 0 7px rgba(255,176,32,0), 0 0 22px 6px rgba(255,176,32,.7); }
    }
    .fleet { font-size: 13.5px; color: var(--muted); text-align: right; }
    .fleet .hot { color: var(--warn); }
    .fleet .ok { color: var(--calm); }
    .divider { height: 1px; background: var(--line); margin: 0 0 26px; border: 0; }

    /* ---- queue grid: instruments ---- */
    .grid {
      display: grid; gap: 14px;
      grid-template-columns: 1fr;
    }
    @media (min-width: 620px) { .grid { grid-template-columns: 1fr 1fr; } }
    @media (min-width: 980px) { .grid { grid-template-columns: 1fr 1fr 1fr; } }

    .card {
      display: block; text-decoration: none; color: inherit;
      background: var(--panel);
      border: 1px solid var(--line);
      border-left: 3px solid var(--line);
      border-radius: 14px;
      padding: 16px 16px 14px;
      transition: transform .16s ease, box-shadow .16s ease, border-color .16s ease;
    }
    .card:hover { transform: translateY(-2px); box-shadow: 0 8px 24px rgba(0,0,0,.35); }
    .card:focus-visible {
      outline: 2px solid var(--beam); outline-offset: 2px;
    }
    .card.draining { border-left-color: var(--calm); }
    .card.clear { border-left-color: var(--line); }
    .card.stuck {
      border-left-color: var(--warn);
      background:
        linear-gradient(180deg, rgba(255,176,32,.06), transparent 60%), var(--panel-2);
      box-shadow: 0 0 0 1px rgba(255,176,32,.18), 0 0 26px -4px rgba(255,176,32,.45);
      animation: glow 2.6s ease-in-out infinite;
    }
    @keyframes glow {
      0%, 100% { box-shadow: 0 0 0 1px rgba(255,176,32,.16), 0 0 22px -6px rgba(255,176,32,.35); }
      50% { box-shadow: 0 0 0 1px rgba(255,176,32,.32), 0 0 34px -2px rgba(255,176,32,.6); }
    }
    .card-top {
      display: flex; align-items: baseline; justify-content: space-between; gap: 8px;
    }
    .qname { font-size: 18px; font-weight: 600; }
    .state {
      font-size: 11px; font-weight: 700; letter-spacing: .08em;
      text-transform: uppercase; color: var(--muted);
    }
    .state.draining { color: var(--calm); }
    .state.stuck { color: var(--warn); }
    .readout {
      margin: 14px 0 4px; font-size: 21px; font-weight: 500;
      color: var(--ink); line-height: 1.25;
    }
    .readout .dim { color: var(--muted); }
    .readout.stalled { color: var(--warn); }
    .readout.clear { color: var(--calm); }
    .bar {
      height: 5px; border-radius: 99px; background: var(--line);
      overflow: hidden; margin: 12px 0 12px;
    }
    .bar > span { display: block; height: 100%; border-radius: 99px; }
    .bar > span.calm { background: var(--calm); }
    .bar > span.warn { background: var(--warn); }
    .card-foot {
      display: flex; align-items: center; justify-content: space-between;
      font-size: 12.5px; color: var(--muted);
    }
    .card-foot .wk { color: var(--ink); }

    /* ---- workers ---- */
    h2 {
      font-size: 12px; color: var(--muted); text-transform: uppercase;
      letter-spacing: .14em; margin: 38px 0 14px; font-weight: 600;
    }
    .workers { display: flex; flex-direction: column; gap: 0; }
    .wrow {
      display: grid;
      grid-template-columns: minmax(0,1.4fr) minmax(0,.9fr) minmax(0,1.6fr) auto;
      gap: 12px; align-items: center;
      padding: 12px 4px; border-bottom: 1px solid var(--line);
    }
    .wrow:last-child { border-bottom: 0; }
    .wid { font-size: 13px; color: var(--ink); overflow: hidden; text-overflow: ellipsis; }
    .wq { font-size: 13px; color: var(--muted); }
    .wact { font-size: 13px; color: var(--ink); }
    .wact .arrow { color: var(--beam); }
    .wact .ago { color: var(--muted); }
    .wact .idle { color: var(--muted); }
    .pill {
      font-size: 11px; font-weight: 700; letter-spacing: .05em;
      padding: 4px 11px; border-radius: 99px; white-space: nowrap;
    }
    .pill.live { background: rgba(56,211,159,.14); color: var(--calm); }
    .pill.dead { background: rgba(255,92,92,.14); color: var(--alarm); }

    /* ---- empty state ---- */
    .empty {
      text-align: center; padding: 64px 20px; color: var(--muted);
    }
    .empty .beacon { margin: 0 auto 18px; }
    .empty .line { font-size: 19px; color: var(--ink); font-weight: 500; }
    .empty .sub { font-size: 13.5px; margin-top: 6px; }

    /* ---- drill-down ---- */
    .back {
      display: inline-flex; align-items: center; gap: 6px;
      color: var(--beam); text-decoration: none; font-size: 13.5px;
      margin-bottom: 18px;
    }
    .back:hover { text-decoration: underline; }
    .back:focus-visible { outline: 2px solid var(--beam); outline-offset: 3px; border-radius: 4px; }
    .tickets { display: flex; flex-direction: column; gap: 0; }
    .trow {
      display: grid;
      grid-template-columns: minmax(0,.7fr) minmax(0,.7fr) minmax(0,1fr) minmax(0,2.4fr);
      gap: 12px; align-items: baseline;
      padding: 13px 4px; border-bottom: 1px solid var(--line);
    }
    .thead { color: var(--muted); font-size: 11px; letter-spacing: .1em;
             text-transform: uppercase; }
    .tref { font-size: 13px; color: var(--beam); }
    .tstatus { font-size: 12px; }
    .tstatus.open { color: var(--muted); }
    .tstatus.in_progress { color: var(--calm); }
    .tworker { font-size: 12px; color: var(--muted); overflow: hidden;
               text-overflow: ellipsis; }
    .ttitle { font-size: 13.5px; color: var(--ink); }
    .tstatus.closed { color: var(--muted); }

    /* ---- closed tickets + resolution ---- */
    .closed-head {
      display: flex; align-items: baseline; gap: 8px;
    }
    .closed-head .count { color: var(--muted); font-size: 11px; font-weight: 600; }
    .crow {
      padding: 13px 4px; border-bottom: 1px solid var(--line);
    }
    .crow:last-child { border-bottom: 0; }
    .crow-top {
      display: grid;
      grid-template-columns: minmax(0,.7fr) minmax(0,1fr) minmax(0,2.4fr);
      gap: 12px; align-items: baseline;
    }
    .csummary { font-size: 13.5px; color: var(--ink); }
    .csummary.none { color: var(--muted); font-style: italic; }
    .chips { margin-top: 8px; display: flex; flex-wrap: wrap; gap: 6px; }
    .chip {
      font-size: 11px; font-weight: 600; letter-spacing: .02em;
      padding: 3px 9px; border-radius: 99px; white-space: nowrap;
      border: 1px solid transparent;
    }
    .chip .lbl { opacity: .8; }
    .chip.caveat { background: rgba(255,176,32,.12); color: var(--warn);
                   border-color: rgba(255,176,32,.25); }
    .chip.unresolved { background: rgba(255,92,92,.12); color: var(--alarm);
                       border-color: rgba(255,92,92,.25); }
    .chip.follow { background: rgba(111,179,255,.12); color: var(--beam);
                   border-color: rgba(111,179,255,.25); }

    .foot { margin-top: 40px; font-size: 12px; color: var(--muted); }
    .foot .mono { color: var(--muted); }

    @media (prefers-reduced-motion: reduce) {
      body { animation: none; }
      .beacon.alert, .card.stuck { animation: none; }
      .card:hover { transform: none; }
    }
"""

_FONT_LINK = (
    '<link rel="preconnect" href="https://fonts.googleapis.com">'
    '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>'
    '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?'
    "family=Space+Grotesk:wght@500;600;700&"
    "family=JetBrains+Mono:wght@400;500;700&"
    'family=Inter:wght@400;500;600&display=swap">'
)


def _page(title: str, body: str, refresh: bool = True) -> str:
    # In-place poll instead of a full-page <meta refresh>: the meta version
    # reloaded the whole document every few seconds, flashing the screen and
    # losing scroll position. This fetches the same page and swaps only the
    # .wrap contents, so the console updates without a flicker (WT-BUGS-1).
    poll = (
        "<script>\n"
        f"  const _WT_MS = {int(REFRESH_SECONDS)} * 1000;\n"
        "  setInterval(async () => {\n"
        "    try {\n"
        "      const r = await fetch(location.href, {cache: 'no-store'});\n"
        "      const d = new DOMParser().parseFromString(await r.text(), 'text/html');\n"
        "      const fresh = d.querySelector('.wrap'), cur = document.querySelector('.wrap');\n"
        "      if (fresh && cur) cur.innerHTML = fresh.innerHTML;\n"
        "    } catch (e) {}\n"
        "  }, _WT_MS);\n"
        "</script>"
    ) if refresh else ""
    return (
        "<!doctype html>\n"
        '<html lang="en">\n<head>\n'
        '  <meta charset="utf-8">\n'
        '  <meta name="viewport" content="width=device-width, initial-scale=1">\n'
        f"  <title>{html.escape(title)}</title>\n"
        f"  {_FONT_LINK}\n"
        f"  <style>{_STYLE}</style>\n"
        "</head>\n<body>\n"
        f'  <div class="wrap">\n{body}\n  </div>\n'
        f"  {poll}\n"
        "</body>\n</html>\n"
    )


# --------------------------------------------------------------------------- html
def _readout(row: Dict[str, Any]) -> str:
    """The big mono readout: depth + ETA, or STALLED, or clear."""
    depth = row.get("depth", 0)
    if depth == 0:
        return '<div class="readout clear mono">clear</div>'
    rate = row.get("drain_rate_per_min") or 0
    open_lbl = "open"
    if not rate:
        return (
            f'<div class="readout stalled mono">{depth} {open_lbl} '
            '<span style="font-size:14px">·</span> STALLED</div>'
        )
    eta = html.escape(str(row.get("eta_human") or "?"))
    return (
        f'<div class="readout mono">{depth} <span class="dim">{open_lbl}</span> '
        f'<span class="dim">· empty in</span> {eta}</div>'
    )


def _state_word(row: Dict[str, Any]) -> str:
    if row["stuck"]:
        return "stalled"
    if row["depth"] > 0:
        return "draining"
    return "clear"


def _card_class(row: Dict[str, Any]) -> str:
    if row["stuck"]:
        return "stuck"
    if row["depth"] > 0:
        return "draining"
    return "clear"


def _drain_bar(row: Dict[str, Any]) -> str:
    """A slim fill. Calm width proportional to live-vs-total; warn when stuck."""
    if row["stuck"]:
        return '<div class="bar"><span class="warn" style="width:100%"></span></div>'
    depth = row.get("depth", 0)
    if depth == 0:
        return '<div class="bar"><span class="calm" style="width:100%"></span></div>'
    # A calm proportion: how much of the work is in flight (in_progress / total active).
    wip = row.get("in_progress", 0)
    total = depth + wip
    pct = int(round((wip / total) * 100)) if total else 0
    pct = max(8, pct)  # always show a sliver so the instrument never reads dead
    return f'<div class="bar"><span class="calm" style="width:{pct}%"></span></div>'


def render_index(payload: Dict[str, Any]) -> str:
    rows: List[Dict[str, Any]] = payload["queues"]
    wkrs: List[Dict[str, Any]] = payload["workers"]

    any_stuck = any(r["stuck"] for r in rows)
    stuck_n = sum(1 for r in rows if r["stuck"])
    live_workers = sum(1 for w in wkrs if w.get("alive"))

    # Header / the tower.
    beacon_cls = "beacon alert" if any_stuck else "beacon"
    if not rows:
        beacon_cls = "beacon dim"
    fleet_bits = [f'<span class="mono">{len(rows)}</span> queues']
    if stuck_n:
        fleet_bits.append(f'<span class="hot mono">{stuck_n} stuck</span>')
    fleet_bits.append(
        f'<span class="ok mono">{live_workers}</span> '
        f'worker{"" if live_workers == 1 else "s"} live'
    )
    fleet = " · ".join(fleet_bits)

    header = (
        '    <header>\n'
        '      <div class="brand">\n'
        f'        <span class="{beacon_cls}" aria-hidden="true"></span>\n'
        '        <span class="wordmark disp">Watch<span class="lo">Tower</span></span>\n'
        "      </div>\n"
        f'      <div class="fleet mono">{fleet}</div>\n'
        "    </header>\n"
        '    <hr class="divider">\n'
    )

    if not rows:
        body = header + (
            '    <div class="empty">\n'
            '      <div class="beacon dim" aria-hidden="true"></div>\n'
            '      <div class="line disp">All queues clear</div>\n'
            '      <div class="sub mono">Nothing open. The tower is quiet.</div>\n'
            "    </div>\n"
        )
        return _page("WatchTower", body)

    cards = []
    for r in rows:
        cls = _card_class(r)
        name = html.escape(str(r["queue"]))
        live = r.get("workers_live", 0)
        total = r.get("workers_total", 0)
        wk = f"{live} live"
        if total != live:
            wk = f"{live} live / {total}"
        href = "/q/" + urllib.parse.quote(str(r["queue"]), safe="")
        cards.append(
            f'      <a class="card {cls}" href="{href}">\n'
            f'        <div class="card-top">\n'
            f'          <span class="qname disp">{name}</span>\n'
            f'          <span class="state {cls}">{_state_word(r)}</span>\n'
            f"        </div>\n"
            f"        {_readout(r)}\n"
            f"        {_drain_bar(r)}\n"
            f'        <div class="card-foot">\n'
            f'          <span class="wk mono">{html.escape(wk)}</span>\n'
            f'          <span class="mono">oldest {html.escape(str(r["oldest_open_age"]))}</span>\n'
            f"        </div>\n"
            f"      </a>"
        )
    grid = '    <div class="grid">\n' + "\n".join(cards) + "\n    </div>\n"

    # Workers.
    if wkrs:
        wlist = []
        for w in wkrs:
            alive = w.get("alive")
            ref = w.get("active_ref")
            if ref:
                since = w.get("active_since_human")
                ago = (
                    f' <span class="ago">({html.escape(str(since))})</span>'
                    if since
                    else ""
                )
                activity = (
                    f'<span class="arrow">&rarr;</span> '
                    f"{html.escape(str(ref))}{ago}"
                )
            else:
                activity = '<span class="idle">idle</span>'
            pill = (
                '<span class="pill live">LIVE</span>'
                if alive
                else '<span class="pill dead">DEAD</span>'
            )
            wlist.append(
                f'      <div class="wrow">\n'
                f'        <span class="wid mono">{html.escape(str(w.get("worker_id", "")))}</span>\n'
                f'        <span class="wq mono">{html.escape(str(w.get("queue", "")))}</span>\n'
                f'        <span class="wact mono">{activity}</span>\n'
                f"        {pill}\n"
                f"      </div>"
            )
        workers_block = (
            "    <h2>Workers</h2>\n"
            '    <div class="workers">\n' + "\n".join(wlist) + "\n    </div>\n"
        )
    else:
        workers_block = (
            "    <h2>Workers</h2>\n"
            '    <div class="empty" style="padding:32px">\n'
            '      <div class="sub mono">No workers tracked.</div>\n'
            "    </div>\n"
        )

    foot = (
        '    <div class="foot mono">store: '
        f"{html.escape(str(q.store_path()))} · refreshes every {REFRESH_SECONDS}s</div>\n"
    )
    return _page("WatchTower", header + grid + workers_block + foot)


def _resolution_chips(res: Dict[str, Any]) -> str:
    """Small palette chips for a resolution's caveats / follow-ups / unresolved.

    Caveats/unresolved lean --warn/--alarm; follow-ups lean --beam."""
    specs = (
        ("caveats", "caveat", "caveat"),
        ("follow_ups", "follow", "follow-up"),
        ("unresolved", "unresolved", "unresolved"),
    )
    chips = []
    for key, cls, label in specs:
        for val in res.get(key) or []:
            chips.append(
                f'<span class="chip {cls}">'
                f'<span class="lbl">{label}:</span> {html.escape(str(val))}</span>'
            )
    if not chips:
        return ""
    return '        <div class="chips">\n          ' + "\n          ".join(chips) + "\n        </div>\n"


def _closed_block(closed: List[Dict[str, Any]], total_closed: int) -> str:
    """The 'Closed' section: each row shows its resolution summary + chips."""
    if not closed:
        return ""
    extra = f' <span class="count mono">{total_closed}</span>' if total_closed else ""
    crows = []
    for it in closed:
        ref = html.escape(str(it.get("ref", "")))
        worker = html.escape(
            str(it.get("closed_by") or it.get("claimed_by") or "—")[:28]
        )
        res = it.get("resolution") or {}
        summary = res.get("summary", "")
        if summary:
            summary_html = f'<span class="csummary">{html.escape(str(summary))}</span>'
        else:
            title = it.get("title") or it.get("note") or "(no resolution recorded)"
            summary_html = f'<span class="csummary none">{html.escape(str(title))}</span>'
        crows.append(
            f'      <div class="crow">\n'
            f'        <div class="crow-top">\n'
            f'          <span class="tref mono">{ref}</span>\n'
            f'          <span class="tworker mono">{worker}</span>\n'
            f'          {summary_html}\n'
            f'        </div>\n'
            f"{_resolution_chips(res)}"
            f"      </div>"
        )
    return (
        f'    <h2 class="closed-head">Closed{extra}</h2>\n'
        '    <div class="tickets">\n' + "\n".join(crows) + "\n    </div>\n"
    )


def render_queue(
    name: str,
    payload: Dict[str, Any],
    tickets: List[Dict[str, Any]],
    closed: List[Dict[str, Any]] = None,
    total_closed: int = 0,
) -> str:
    """Per-queue drill-down page: the queue's instrument header + its tickets.

    ``closed`` (most-recent first) renders below the active tickets, each with
    its resolution; ``total_closed`` is the full count for the section header."""
    rows = payload["queues"]
    row = next((r for r in rows if r["queue"] == name), None)
    closed = closed or []

    safe_name = html.escape(name)
    header = (
        '    <a class="back" href="/">&larr; all queues</a>\n'
        '    <header>\n'
        '      <div class="brand">\n'
        f'        <span class="{"beacon alert" if (row and row["stuck"]) else "beacon"}" aria-hidden="true"></span>\n'
        f'        <span class="wordmark disp">{safe_name}</span>\n'
        "      </div>\n"
    )
    if row:
        header += f'      <div class="fleet">{_readout(row)}</div>\n'
    header += "    </header>\n    <hr class=\"divider\">\n"

    closed_block = _closed_block(closed, total_closed)

    if not tickets:
        body = header + (
            '    <div class="empty">\n'
            '      <div class="beacon dim" aria-hidden="true"></div>\n'
            '      <div class="line disp">No active tickets</div>\n'
            '      <div class="sub mono">This queue is clear.</div>\n'
            "    </div>\n"
        ) + closed_block
        return _page(f"{name} · WatchTower", body)

    trows = [
        '      <div class="trow thead">\n'
        '        <span>ref</span><span>status</span><span>worker</span><span>title</span>\n'
        "      </div>"
    ]
    for it in tickets:
        ref = html.escape(str(it.get("ref", "")))
        status = str(it.get("status", ""))
        worker = html.escape(
            str(it.get("claimed_by") or it.get("claimed_session_id") or "—")[:28]
        )
        title = html.escape(str(it.get("title") or it.get("note") or "")[:120])
        trows.append(
            f'      <div class="trow">\n'
            f'        <span class="tref mono">{ref}</span>\n'
            f'        <span class="tstatus {status} mono">{html.escape(status)}</span>\n'
            f'        <span class="tworker mono">{worker}</span>\n'
            f'        <span class="ttitle">{title}</span>\n'
            f"      </div>"
        )
    tickets_block = '    <div class="tickets">\n' + "\n".join(trows) + "\n    </div>\n"
    return _page(f"{name} · WatchTower", header + tickets_block + closed_block)


# Back-compat shim: the old single entry point. Tests + any caller that asked for
# the index HTML still work.
def render_html(payload: Dict[str, Any]) -> str:
    return render_index(payload)


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

    def _html(self, code: int, page: str) -> None:
        self._send(code, page.encode(), "text/html; charset=utf-8")

    def do_GET(self) -> None:  # noqa: N802 (http.server contract)
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        if path == "/":
            self._html(200, render_index(status_payload()))
        elif path.startswith("/q/"):
            name = urllib.parse.unquote(path[len("/q/"):])
            norm = q._norm_project(name)
            payload = status_payload()
            all_closed = [
                it for it in q.list_items(project=norm)
                if it.get("status") == "closed"
            ]
            self._html(200, render_queue(
                norm, payload, queue_tickets(norm),
                closed=closed_tickets(norm), total_closed=len(all_closed),
            ))
        elif path == "/api/status":
            self._json(200, status_payload())
        elif path == "/api/queues":
            self._json(200, q.queues())
        elif path.startswith("/api/queue/"):
            name = q._norm_project(urllib.parse.unquote(path[len("/api/queue/"):]))
            # Closed tickets (with resolution) are always included; ?status=all
            # additionally widens it, but closed is the default extra payload.
            self._json(200, {
                "queue": name,
                "tickets": queue_tickets(name),
                "closed": closed_tickets(name),
            })
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
    """Start the dashboard HTTP server (blocking).

    ``once=True`` handles a single request then returns — used by tests so the
    server doesn't block. Otherwise it serves forever until interrupted. This is
    the foreground path; ``wt dashboard`` normally launches it detached.
    """
    httpd = ThreadingHTTPServer((host, port), _Handler)
    bound_host, bound_port = httpd.server_address[0], httpd.server_address[1]
    if not once:
        print(f"WatchTower dashboard on http://{bound_host}:{bound_port}")
        print("  GET /            the night-watch console")
        print("  GET /q/<queue>   per-queue drill-down")
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

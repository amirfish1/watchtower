#!/usr/bin/env python3
"""WatchTower HTTP dashboard — the night-watch operations console.

A read-only viewer over the same queue engine. Stdlib-only (``http.server`` +
``json``): no framework, no template engine, no runtime dependencies. It binds
``127.0.0.1`` by default (local-first) and renders live queue + worker health as
an instrument panel — calm and dark until a queue needs you, then it lights up.

Routes:

    GET  /                          the tower: fleet summary + queue instrument grid
    GET  /q/<queue>                 per-queue drill-down (tickets, mirrors `wt ls`)
    GET  /api/status                {"queues": [...health rows + workers...], "workers": [...]}
    GET  /api/queues                raw per-queue counts (mirrors `wt queues`)
    GET  /api/queue/<name>          active + closed tickets (closed carry resolution)
    POST /api/ticket/<ref>/run      mark a ticket runnable and spawn one scoped worker
    POST /api/queue/<name>/add      ingest a ticket — {"note": "...", "url": "...",
                                      "selector": "...", "repo_path": "...",
                                      "title": "...", "source": "...", "text": "..."}
                                    → {"ok": true, "ref": "MYAPP-7", "number": 7,
                                       "project": "MYAPP"}
    POST /api/send                  {"to", "text", "mode"}: messages.send
    POST /api/ask                   {"to", "text", "timeout_ms"}: messages.ask
    POST /api/chat/create           {"topic", "participants", "include_human"}
    POST /api/chat/post             {"ref", "body", "author"}
    GET  /api/chats                 chats.list_chats (open chats)
    GET  /api/chat/<ref>            chats.read_chat
    GET  /chat/<ref>                group-chat transcript page (WT-60): messages,
                                     participants, effective nudge policy when available

The index page (``GET /``) also renders a "Group chats" section below the
queue grid, listing active (non-archived) chats with a link to their
transcript page, reusing :func:`watchtower.chats.list_chats`.

It reuses :mod:`watchtower.health` for the stuck computation and
:mod:`watchtower.workers` for liveness — neither is duplicated here. The
messaging endpoints reuse :mod:`watchtower.messages` and :mod:`watchtower.chats`
the same way (see docs/messaging-design.md).
"""

from __future__ import annotations

import hmac
import html
import json
import os
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional

from . import health, queue as q, workers

REFRESH_SECONDS = 5


def _check_same_origin(handler: BaseHTTPRequestHandler) -> bool:
    """True when the request's ``Origin`` header, if present, is localhost.

    No ``Origin`` header at all (curl, server-to-server calls, `wt` itself)
    is allowed through; a foreign ``Origin`` is rejected with 403 by the
    caller. This guard is applied ONLY to the new messaging endpoints
    (``/api/send``, ``/api/ask``, ``/api/chat/create``, ``/api/chat/post``,
    see ``do_POST``): sending a message or posting to a chat is an action
    a random web page should not be able to trigger cross-origin.

    Deliberately NOT applied to ``/api/queue/<name>/add`` or
    ``/api/ticket/<ref>/run``: the annotate widget
    (``contrib/annotate-widget.js``) intentionally POSTs to those from
    arbitrary third-party pages so a user can file a ticket with one click.
    That asymmetry is on purpose; do not "fix" it into consistency."""
    origin = handler.headers.get("Origin")
    if not origin:
        return True
    try:
        host = urllib.parse.urlparse(origin).hostname or ""
    except ValueError:
        return False
    return host in ("localhost", "127.0.0.1")


def _check_bearer_token(handler: BaseHTTPRequestHandler) -> bool:
    """WT-65: bearer-token gate for the messaging endpoints.

    With ``WATCHTOWER_API_TOKEN`` unset (the default) this is a no-op and
    the posture stays what it always was: bind 127.0.0.1 + same-origin.
    Once the token is set, every messaging POST must carry
    ``Authorization: Bearer <token>`` — the prerequisite for ever pointing
    ``WATCHTOWER_DELEGATE_URL`` off-box (remote-WT federation): the moment
    the server is reachable from another machine, "anyone who can connect
    can make agents do things" stops being acceptable.

    Constant-time compare; applied on top of (not instead of) the
    same-origin check."""
    token = (os.environ.get("WATCHTOWER_API_TOKEN") or "").strip()
    if not token:
        return True
    auth = (handler.headers.get("Authorization") or "").strip()
    supplied = auth[7:].strip() if auth.lower().startswith("bearer ") else ""
    return bool(supplied) and hmac.compare_digest(supplied, token)


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
    .state.backlog { color: var(--beam); }
    .readout.backlog { color: var(--beam); }
    .card.backlog { opacity: 0.92; }
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
      grid-template-columns: minmax(0,.7fr) minmax(0,.7fr) minmax(0,1fr) minmax(0,2.4fr) minmax(72px,.4fr);
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
    .run-btn {
      justify-self: end;
      border: 1px solid rgba(111,179,255,.35);
      background: rgba(111,179,255,.12);
      color: var(--beam);
      border-radius: 6px;
      padding: 5px 10px;
      font: inherit;
      font-size: 12px;
      cursor: pointer;
    }
    .run-btn:hover { border-color: var(--beam); }
    .run-spacer { min-width: 1px; min-height: 1px; }

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
      .beacon.alert, .card.stuck, .queue-group.is-stuck, .qh-state.stuck { animation: none; }
      .card:hover { transform: none; }
    }

    /* ---- new list layout ---- */
    .layout { display: grid; grid-template-columns: 1fr; gap: 24px; margin-top: 0; }
    @media (min-width: 700px) { .layout { grid-template-columns: 1.8fr 1fr; } }

    .queue-list { display: flex; flex-direction: column; gap: 2px; }
    .queue-group { background: var(--panel); border: 1px solid var(--line); border-radius: 12px; overflow: hidden; margin-bottom: 8px; }
    /* WT-26: alarm red (not warn amber) to match .qh-state.stuck and read as
       higher-contrast/higher-urgency than the ordinary "backlog" amber. */
    .queue-group.is-stuck {
      border: 2px solid rgba(255,92,92,.6);
      box-shadow: 0 0 0 1px rgba(255,92,92,.2), 0 0 30px -4px rgba(255,92,92,.55);
      animation: stuckglow 2.2s ease-in-out infinite;
    }
    @keyframes stuckglow {
      0%, 100% { box-shadow: 0 0 0 1px rgba(255,92,92,.2), 0 0 24px -6px rgba(255,92,92,.4); }
      50% { box-shadow: 0 0 0 1px rgba(255,92,92,.42), 0 0 40px -2px rgba(255,92,92,.75); }
    }
    .queue-header { display: flex; align-items: center; gap: 6px; padding: 8px 12px; border-bottom: 1px solid var(--line); text-decoration: none; color: inherit; cursor: pointer; transition: background .12s; }
    .queue-header:hover { background: var(--panel-2); }
    .qh-name { font-size: 12px; font-weight: 700; text-transform: uppercase; letter-spacing: .05em; color: var(--ink); flex-shrink: 0; }
    .qh-meta { display: flex; align-items: center; gap: 5px; font-size: 11px; color: var(--muted); flex: 1; min-width: 0; overflow: hidden; }
    .qh-sep { opacity: .4; }
    .qh-drain.on { color: #3fb950; }
    .qh-drain.off { opacity: .6; }
    .qh-state { margin-left: auto; flex-shrink: 0; font-size: 9.5px; font-weight: 700; text-transform: uppercase; letter-spacing: .05em; padding: 2px 8px; border-radius: 999px; }
    .qh-state.ready { background: rgba(63,185,80,.15); color: #3fb950; }
    .qh-state.draining { background: rgba(63,185,80,.15); color: #3fb950; }
    .qh-state.stuck { background: rgba(255,92,92,.18); color: #ff5c5c; animation: beat 2.4s ease-in-out infinite; }
    .qh-state.backlog { background: rgba(139,148,158,.16); color: var(--muted); }

    .wk-row { display: flex; align-items: center; gap: 8px; padding: 6px 12px 6px 16px; border-bottom: 1px solid rgba(37,50,74,.6); font-size: 12px; color: var(--muted); }
    .wk-row:last-child { border-bottom: 0; }
    .wk-dot { color: #3fb950; font-size: 7px; flex-shrink: 0; }
    .wk-dot.dead { color: var(--alarm); }
    .wk-id { font-family: ui-monospace, monospace; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; max-width: 140px; }
    .wk-act { margin-left: 4px; }
    .wk-act .arrow { color: var(--beam); }
    .wk-pill { margin-left: auto; font-size: 9px; font-weight: 700; letter-spacing: .05em; padding: 1px 7px; border-radius: 999px; flex-shrink: 0; }
    .wk-pill.live { background: rgba(56,211,159,.14); color: var(--calm); }
    .wk-pill.dead { background: rgba(255,92,92,.14); color: var(--alarm); }

    /* ---- add ticket panel ---- */
    .add-panel { background: var(--panel); border: 1px solid var(--line); border-radius: 12px; padding: 18px; position: sticky; top: 20px; }
    .add-panel h3 { margin: 0 0 14px; font-size: 13px; font-weight: 700; text-transform: uppercase; letter-spacing: .1em; color: var(--muted); }
    .add-panel label { display: block; font-size: 11.5px; color: var(--muted); margin-bottom: 4px; margin-top: 12px; }
    .add-panel label:first-of-type { margin-top: 0; }
    .add-panel select, .add-panel input, .add-panel textarea {
      width: 100%; background: var(--bg); border: 1px solid var(--line); border-radius: 7px;
      color: var(--ink); padding: 7px 10px; font-size: 13px; font-family: inherit;
      outline: none; transition: border-color .15s;
    }
    .add-panel select:focus, .add-panel input:focus, .add-panel textarea:focus { border-color: var(--beam); }
    .add-panel textarea { resize: vertical; min-height: 72px; }
    .add-btn { margin-top: 14px; width: 100%; padding: 9px; background: var(--beam); color: #0c121e; border: 0; border-radius: 8px; font-size: 13px; font-weight: 700; cursor: pointer; transition: opacity .15s; }
    .add-btn:hover { opacity: .88; }
    .add-btn:active { opacity: .75; }
    .add-msg { margin-top: 10px; font-size: 12px; padding: 7px 10px; border-radius: 7px; display: none; }
    .add-msg.ok { background: rgba(56,211,159,.14); color: var(--calm); display: block; }
    .add-msg.err { background: rgba(255,92,92,.14); color: var(--alarm); display: block; }

    /* ---- group chats (WT-60) ---- */
    .chat-list { display: flex; flex-direction: column; gap: 0; }
    .chat-row {
      display: grid; grid-template-columns: minmax(0,1.4fr) minmax(0,1.6fr) auto;
      gap: 12px; align-items: center;
      background: var(--panel); border: 1px solid var(--line); border-radius: 12px;
      padding: 12px 14px; margin-bottom: 8px;
      text-decoration: none; color: inherit;
      transition: transform .16s ease, box-shadow .16s ease, border-color .16s ease;
    }
    .chat-row:hover { transform: translateY(-2px); box-shadow: 0 8px 24px rgba(0,0,0,.35); border-color: var(--beam); }
    .chat-row:focus-visible { outline: 2px solid var(--beam); outline-offset: 2px; }
    .chat-topic {
      font-size: 14px; font-weight: 600; color: var(--ink);
      overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
    }
    .chat-meta { font-size: 11.5px; color: var(--muted); display: flex; align-items: center; gap: 5px; overflow: hidden; }
    .chat-status {
      font-size: 9.5px; font-weight: 700; text-transform: uppercase; letter-spacing: .05em;
      padding: 2px 8px; border-radius: 999px; white-space: nowrap;
    }
    .chat-status.open { background: rgba(56,211,159,.14); color: var(--calm); }
    .chat-status.closed { background: rgba(126,144,174,.16); color: var(--muted); }
    .chat-status.archived { background: rgba(126,144,174,.16); color: var(--muted); }

    /* ---- chat transcript page ---- */
    .chat-participant {
      padding: 5px 0; font-size: 12.5px; color: var(--ink);
      display: flex; align-items: center; gap: 6px;
    }
    .chat-transcript { display: flex; flex-direction: column; gap: 12px; }
    .msg { background: var(--panel); border: 1px solid var(--line); border-radius: 10px; padding: 12px 14px; }
    .msg-head {
      font-size: 11.5px; color: var(--muted); margin-bottom: 6px;
      display: flex; gap: 8px; align-items: baseline;
    }
    .msg-author { color: var(--beam); font-weight: 600; }
    .msg-body {
      margin: 0; font-family: inherit; font-size: 13.5px; color: var(--ink);
      white-space: pre-wrap; word-break: break-word; line-height: 1.55;
    }

    /* ---- mobile pane (WT-26): phone-width viewers ----
       The grid/layout already collapse to one column below 620-700px; this
       block is about the two things a narrower breakpoint still needs:
       (1) rows built from multi-column CSS grid (.trow/.wrow/.crow-top/
       .chat-row/.queue-header) reflow to stacked flex so nothing forces
       horizontal scroll, and (2) every tappable element grows to a >=44px
       touch target. */
    @media (max-width: 480px) {
      .wrap { padding: 14px 12px 48px; }
      header { gap: 10px; }
      .wordmark { font-size: 22px; }
      .fleet { text-align: left; font-size: 12.5px; }

      .queue-header {
        flex-wrap: wrap; row-gap: 6px;
        padding: 12px; min-height: 44px;
      }
      .qh-meta { flex-basis: 100%; order: 3; }
      .qh-state { order: 2; }

      .wk-row { padding: 10px 12px 10px 18px; flex-wrap: wrap; row-gap: 4px; }
      .wk-id { max-width: 100%; }

      .trow {
        display: flex; flex-direction: column; align-items: flex-start;
        gap: 4px; padding: 14px 8px; min-height: 44px;
      }
      .trow.thead { display: none; }
      .run-btn { align-self: flex-end; padding: 10px 16px; min-height: 44px; }

      .crow-top {
        display: flex; flex-direction: column; align-items: flex-start; gap: 4px;
      }

      .chat-row {
        display: flex; flex-direction: column; align-items: flex-start;
        gap: 8px; min-height: 44px; padding: 14px;
      }
      .chat-status { align-self: flex-start; }

      .add-btn { padding: 12px; min-height: 44px; }
      .add-panel select, .add-panel input, .add-panel textarea { padding: 10px; }

      .back { padding: 8px 0; min-height: 44px; }

      .msg-head { flex-wrap: wrap; }

      .wrow {
        display: flex; flex-direction: column; align-items: flex-start;
        gap: 4px; padding: 12px 4px;
      }
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
        '  <meta name="mobile-web-app-capable" content="yes">\n'
        '  <meta name="apple-mobile-web-app-capable" content="yes">\n'
        '  <meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">\n'
        '  <meta name="apple-mobile-web-app-title" content="WatchTower">\n'
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
    if row.get("state") == "backlog":
        return (
            f'<div class="readout backlog mono">{depth} open '
            '<span style="font-size:14px">·</span> backlog</div>'
        )
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
    return {
        "stuck": "stalled", "backlog": "backlog",
        "active": "draining", "clear": "clear",
    }.get(row.get("state"), "clear")


def _card_class(row: Dict[str, Any]) -> str:
    return {
        "stuck": "stuck", "backlog": "backlog",
        "active": "draining", "clear": "clear",
    }.get(row.get("state"), "clear")


def _drain_bar(row: Dict[str, Any]) -> str:
    """A slim fill. Calm width proportional to live-vs-total; warn when stuck."""
    if row.get("state") == "stuck":
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


def _epoch_age_human(epoch: Optional[float], now: Optional[float] = None) -> str:
    """Compact age string for an epoch timestamp (e.g. '4m', '2h05m'), same
    shorthand as :func:`watchtower.workers._age_human` but for an epoch float
    (``list_chats``' ``last_post_at``/``started_at``) rather than an ISO
    string. Returns ``"—"`` for a missing/zero timestamp."""
    if not epoch:
        return "—"
    secs = max(0, int((now if now is not None else time.time()) - float(epoch)))
    if secs < 60:
        return f"{secs}s"
    mins = secs // 60
    if mins < 60:
        return f"{mins}m"
    hours = mins // 60
    if hours < 24:
        return f"{hours}h{mins % 60:02d}m"
    days = hours // 24
    return f"{days}d{hours % 24:02d}h"


def _chat_ref(chat_row: Dict[str, Any]) -> str:
    """The short ref used in ``/chat/<ref>`` links: the transcript filename's
    stem (a slug, resolvable by ``chats.find_chat`` as an exact-name match),
    not the full on-disk path."""
    path = str(chat_row.get("path") or "")
    return Path(path).stem if path else ""


def _chat_group_section(chat_rows: List[Dict[str, Any]]) -> str:
    """The index page's "Group chats" section (WT-60): active (non-archived)
    chats, each linking to its ``/chat/<ref>`` transcript page. Message count
    is read per-chat (the chat directory is small compared to the
    conversation/session universe the perf-budget rules guard, so a per-row
    read here is fine; see CLAUDE.md's Performance gates)."""
    if not chat_rows:
        return (
            '    <h2>Group chats</h2>\n'
            '    <div class="empty" style="padding:28px 20px;">\n'
            '      <div class="line disp" style="font-size:15px;">No active chats</div>\n'
            '      <div class="sub mono">wt chat new "&lt;topic&gt;" --with @agent to start one.</div>\n'
            "    </div>\n"
        )
    from . import chats as _chats
    now = time.time()
    rows_html = []
    for row in chat_rows:
        ref = _chat_ref(row)
        href = "/chat/" + urllib.parse.quote(ref, safe="")
        topic = html.escape(str(row.get("topic") or ref or "untitled")[:120])
        names = [str(p.get("name") or "") for p in (row.get("participants") or [])]
        names_safe = html.escape(", ".join(n for n in names if n) or "—")
        try:
            msg_count = len((_chats.read_chat(row.get("path")) or {}).get("messages") or [])
        except Exception:
            msg_count = 0
        age = _epoch_age_human(row.get("last_post_at"), now=now)
        closed = row.get("closed_at")
        status_cls = "closed" if closed else "open"
        status_lbl = "closed" if closed else "open"
        rows_html.append(
            f'      <a class="chat-row" href="{href}">\n'
            f'        <span class="chat-topic">{topic}</span>\n'
            f'        <span class="chat-meta mono">\n'
            f'          <span>{names_safe}</span>\n'
            f'          <span class="qh-sep">·</span>\n'
            f'          <span>{msg_count} msg{"" if msg_count == 1 else "s"}</span>\n'
            f'          <span class="qh-sep">·</span>\n'
            f'          <span>{html.escape(age)} ago</span>\n'
            f'        </span>\n'
            f'        <span class="chat-status {status_cls}">{status_lbl}</span>\n'
            f'      </a>\n'
        )
    return (
        '    <h2>Group chats</h2>\n'
        '    <div class="chat-list">\n' + "".join(rows_html) + "    </div>\n"
    )


def render_index(payload: Dict[str, Any], chat_rows: Optional[List[Dict[str, Any]]] = None) -> str:
    # Triage-first (WT-26): STUCK queues always render at the top of the grid
    # so a phone-width glance surfaces the alarm first. `health.all_status`
    # already sorts stuck-first for its own callers, but the dashboard must
    # not depend on the payload arriving pre-sorted (tests, future callers) —
    # a stable sort here is cheap and makes the ordering a render-time
    # guarantee rather than an incidental upstream side effect.
    rows: List[Dict[str, Any]] = sorted(
        payload["queues"], key=lambda r: r.get("state") != "stuck"
    )
    wkrs: List[Dict[str, Any]] = payload["workers"]

    any_stuck = any(r.get("state") == "stuck" for r in rows)
    stuck_n = sum(1 for r in rows if r.get("state") == "stuck")
    live_workers = sum(1 for w in wkrs if w.get("alive"))

    beacon_cls = "beacon alert" if any_stuck else ("beacon dim" if not rows else "beacon")
    fleet_bits = [f'<span class="mono">{len(rows)}</span> queue{"" if len(rows)==1 else "s"}']
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

    # Build per-queue worker index: queue -> list of workers
    workers_by_queue: Dict[str, List[Dict[str, Any]]] = {}
    for w in wkrs:
        qn = str(w.get("queue", ""))
        workers_by_queue.setdefault(qn, []).append(w)

    # Get closed/total counts per queue from one pass over all items
    closed_by_q: Dict[str, int] = {}
    total_by_q: Dict[str, int] = {}
    try:
        for it in (q.list_items() or []):
            qn = str(it.get("project") or "").upper()
            if not qn:
                continue
            total_by_q[qn] = total_by_q.get(qn, 0) + 1
            if it.get("status") == "closed":
                closed_by_q[qn] = closed_by_q.get(qn, 0) + 1
    except Exception:
        pass

    def _queue_state_class(r: Dict[str, Any]) -> str:
        s = r.get("state", "clear")
        if s == "stuck":
            return "stuck"
        depth = r.get("depth", 0)
        auto = r.get("auto_drain", False)
        workers_live = r.get("workers_live", 0)
        if depth == 0 and auto:
            return "ready"
        if depth > 0 and workers_live > 0:
            return "draining"
        if not auto:
            return "backlog"
        return "ready"

    queue_groups = []
    for r in rows:
        qname = str(r["queue"])
        qname_safe = html.escape(qname)
        href = "/q/" + urllib.parse.quote(qname, safe="")
        qwkrs = workers_by_queue.get(qname, [])
        live_w = r.get("workers_live", 0)
        total_closed = closed_by_q.get(qname, 0)
        total_items = total_by_q.get(qname, 0)
        progress = f"{total_closed}/{total_items}" if total_items else "—"

        auto = r.get("auto_drain", False)
        drain_cls = "on" if auto else "off"
        drain_lbl = "drain on" if auto else "drain off"

        sc = _queue_state_class(r)
        state_labels = {
            "ready": "READY", "draining": "DRAINING",
            "stuck": "STUCK", "backlog": "BACKLOG",
        }
        state_lbl = state_labels.get(sc, sc.upper())

        wk_count = f'{live_w} worker{"" if live_w == 1 else "s"}'

        group_cls = "queue-group" + (" is-stuck" if sc == "stuck" else "")

        qh = (
            f'      <a class="queue-header" href="{href}">\n'
            f'        <span class="qh-name">{qname_safe}</span>\n'
            f'        <span class="qh-meta">\n'
            f'          <span class="qh-sep">·</span>\n'
            f'          <span>{html.escape(progress)}</span>\n'
            f'          <span class="qh-sep">·</span>\n'
            f'          <span>{html.escape(wk_count)}</span>\n'
            f'          <span class="qh-sep">·</span>\n'
            f'          <span class="qh-drain {drain_cls}">{drain_lbl}</span>\n'
            f'        </span>\n'
            f'        <span class="qh-state {sc}">{state_lbl}</span>\n'
            f'      </a>\n'
        )

        wk_rows = ""
        for w in qwkrs:
            alive = w.get("alive")
            dot_cls = "wk-dot" if alive else "wk-dot dead"
            pill_cls = "wk-pill live" if alive else "wk-pill dead"
            pill_lbl = "LIVE" if alive else "DEAD"
            wid = html.escape(str(w.get("worker_id", ""))[:24])
            ref = w.get("active_ref")
            if ref:
                since = w.get("active_since_human", "")
                ago = (
                    f' <span style="opacity:.6">({html.escape(str(since))})</span>'
                    if since else ""
                )
                act = f'<span class="arrow">→</span> {html.escape(str(ref))}{ago}'
            elif w.get("last_closed_ref"):
                act = (
                    '<span style="opacity:.5">idle (last: '
                    f'{html.escape(str(w["last_closed_ref"]))})</span>'
                )
            else:
                act = '<span style="opacity:.5">idle</span>'
            wk_rows += (
                f'      <div class="wk-row">\n'
                f'        <span class="{dot_cls}">●</span>\n'
                f'        <span class="wk-id mono">{wid}</span>\n'
                f'        <span class="wk-act mono">{act}</span>\n'
                f'        <span class="{pill_cls}">{pill_lbl}</span>\n'
                f'      </div>\n'
            )

        queue_groups.append(
            f'    <div class="{group_cls}">\n'
            f'{qh}'
            f'{wk_rows}'
            f'    </div>\n'
        )

    if rows:
        queue_list_html = '    <div class="queue-list">\n' + "".join(queue_groups) + "    </div>\n"
    else:
        queue_list_html = (
            '    <div class="empty">\n'
            '      <div class="beacon dim" aria-hidden="true"></div>\n'
            '      <div class="line disp">All queues clear</div>\n'
            '      <div class="sub mono">No queues configured yet. Run <code>wt drain on MYAPP</code> to get started.</div>\n'
            "    </div>\n"
        )

    # Queue names for the add-ticket dropdown
    queue_options = "\n".join(
        f'<option value="{html.escape(str(r["queue"]))}">{html.escape(str(r["queue"]))}</option>'
        for r in rows
    )

    add_panel = (
        '    <div class="add-panel" id="add-panel">\n'
        '      <h3>Add Ticket</h3>\n'
        '      <label for="ap-queue">Queue</label>\n'
        f'      <select id="ap-queue">\n        {queue_options}\n      </select>\n'
        '      <label for="ap-title">Title</label>\n'
        '      <input id="ap-title" type="text" placeholder="Short description" autocomplete="off">\n'
        '      <label for="ap-text">Details (optional)</label>\n'
        '      <textarea id="ap-text" placeholder="Steps to reproduce, links, context…"></textarea>\n'
        '      <button class="add-btn" onclick="apSubmit()">File Ticket</button>\n'
        '      <div class="add-msg" id="ap-msg"></div>\n'
        '    </div>\n'
        '    <script>\n'
        '    async function apSubmit() {\n'
        "      const q = document.getElementById('ap-queue').value;\n"
        "      const title = document.getElementById('ap-title').value.trim();\n"
        "      const text = document.getElementById('ap-text').value.trim();\n"
        "      const msg = document.getElementById('ap-msg');\n"
        "      if (!title) { msg.className='add-msg err'; msg.textContent='Title is required.'; return; }\n"
        "      msg.className='add-msg'; msg.textContent='Filing…';\n"
        '      try {\n'
        "        const r = await fetch('/api/queue/' + encodeURIComponent(q) + '/add', {\n"
        "          method: 'POST',\n"
        "          headers: {'Content-Type': 'application/json'},\n"
        '          body: JSON.stringify({title, text})\n'
        '        });\n'
        '        const d = await r.json();\n'
        '        if (d.ok) {\n'
        "          msg.className='add-msg ok'; msg.textContent='Filed: ' + d.ref;\n"
        "          document.getElementById('ap-title').value='';\n"
        "          document.getElementById('ap-text').value='';\n"
        '        } else {\n'
        "          msg.className='add-msg err'; msg.textContent='Error: ' + (d.error||'unknown');\n"
        '        }\n'
        '      } catch(e) {\n'
        "        msg.className='add-msg err'; msg.textContent='Network error.';\n"
        '      }\n'
        '    }\n'
        '    </script>\n'
    )

    foot = (
        '    <div class="foot mono">store: '
        f"{html.escape(str(q.store_path()))} · refreshes every {REFRESH_SECONDS}s</div>\n"
    )

    layout = (
        '    <div class="layout">\n'
        f'      <div>\n{queue_list_html}      </div>\n'
        f'      <div>\n{add_panel}      </div>\n'
        '    </div>\n'
    )

    chats_section = _chat_group_section(chat_rows or [])

    # WT-26: prefix the <title> with the stuck count so a pinned phone tab
    # surfaces trouble without opening the page ("2 STUCK — WatchTower").
    title = f"{stuck_n} STUCK — WatchTower" if stuck_n else "WatchTower"

    return _page(title, header + layout + chats_section + foot)


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
        '        <span>ref</span><span>status</span><span>worker</span><span>title</span><span></span>\n'
        "      </div>"
    ]
    for it in tickets:
        ref = html.escape(str(it.get("ref", "")))
        status = str(it.get("status", ""))
        worker = html.escape(
            str(it.get("claimed_by") or it.get("claimed_session_id") or "—")[:28]
        )
        title = html.escape(str(it.get("title") or it.get("note") or "")[:120])
        action = '<span class="run-spacer"></span>'
        if status == "open" and not it.get("watchtower_runnable", True):
            action = (
                f'<button class="run-btn" title="Mark runnable" '
                f'onclick="wtRun(\'{ref}\')">Run</button>'
            )
        trows.append(
            f'      <div class="trow">\n'
            f'        <span class="tref mono">{ref}</span>\n'
            f'        <span class="tstatus {status} mono">{html.escape(status)}</span>\n'
            f'        <span class="tworker mono">{worker}</span>\n'
            f'        <span class="ttitle">{title}</span>\n'
            f'        {action}\n'
            f"      </div>"
        )
    tickets_block = '    <div class="tickets">\n' + "\n".join(trows) + "\n    </div>\n"
    script = (
        "    <script>\n"
        "    async function wtRun(ref) {\n"
        "      const res = await fetch('/api/ticket/' + encodeURIComponent(ref) + '/run', {method: 'POST'});\n"
        "      if (!res.ok) {\n"
        "        let msg = 'Run failed';\n"
        "        try { const data = await res.json(); msg = data.error || msg; } catch (_) {}\n"
        "        alert(msg);\n"
        "        return;\n"
        "      }\n"
        "      location.reload();\n"
        "    }\n"
        "    </script>\n"
    )
    return _page(f"{name} · WatchTower", header + tickets_block + closed_block + script)


def _chat_not_found_page(ref: str) -> str:
    body = (
        '    <a class="back" href="/">&larr; dashboard</a>\n'
        '    <div class="empty">\n'
        '      <div class="beacon dim" aria-hidden="true"></div>\n'
        '      <div class="line disp">Chat not found</div>\n'
        f'      <div class="sub mono">No chat matches {html.escape(str(ref))}</div>\n'
        "    </div>\n"
    )
    return _page("Not found · WatchTower", body, refresh=False)


def render_chat(
    ref: str,
    data: Dict[str, Any],
    policy: Optional[Dict[str, int]] = None,
) -> str:
    """The ``/chat/<ref>`` transcript page (WT-60): messages, participants,
    and the effective nudge policy when available. ``data`` is the dict
    returned by :func:`watchtower.chats.read_chat`; every value from it that
    reaches the page is escaped, chat bodies included (they may contain
    markdown, rendered here as plain escaped text, never interpreted)."""
    topic_raw = str(data.get("topic") or ref)
    topic = html.escape(topic_raw[:160])
    mode = html.escape(str(data.get("mode") or "topic"))
    participants = data.get("participants") or []
    messages = data.get("messages") or []
    archived = bool(data.get("archived"))
    closed_at = data.get("closed_at")

    if closed_at:
        status_cls, status_lbl = "closed", "CLOSED"
    elif archived:
        status_cls, status_lbl = "archived", "ARCHIVED"
    else:
        status_cls, status_lbl = "open", "OPEN"

    header = (
        '    <a class="back" href="/">&larr; dashboard</a>\n'
        '    <header>\n'
        '      <div class="brand">\n'
        f'        <span class="wordmark disp">{topic}</span>\n'
        "      </div>\n"
        f'      <div class="fleet mono"><span class="chat-status {status_cls}">{status_lbl}</span></div>\n'
        "    </header>\n"
        '    <hr class="divider">\n'
    )

    part_items = []
    for p in participants:
        name = html.escape(str(p.get("name") or p.get("session_id") or "")[:40])
        sid = str(p.get("session_id") or "")
        sid8 = html.escape(sid[:8]) if sid else ""
        tail = f' <span style="opacity:.5">({sid8})</span>' if sid8 else ""
        part_items.append(
            f'      <div class="chat-participant mono"><span class="wk-dot">●</span> {name}{tail}</div>\n'
        )
    participants_html = (
        '    <div class="add-panel">\n'
        '      <h3>Participants</h3>\n'
        + ("".join(part_items) if part_items else '      <div class="sub mono">none</div>\n')
        + f'      <label style="margin-top:16px;">Mode</label>\n      <div class="mono">{mode}</div>\n'
    )
    if policy:
        participants_html += (
            '      <label style="margin-top:16px;">Nudge policy</label>\n'
            '      <div class="mono" style="font-size:12px; line-height:1.7;">\n'
            f'        interval: {int(policy.get("nudge_interval_s", 0))}s<br>\n'
            f'        idle close: {int(policy.get("idle_close_s", 0))}s<br>\n'
            f'        max/hr: {int(policy.get("max_auto_nudges_per_hour", 0))}\n'
            '      </div>\n'
        )
    participants_html += '    </div>\n'

    if not messages:
        transcript_html = (
            '    <div class="empty">\n'
            '      <div class="line disp">No messages yet</div>\n'
            '      <div class="sub mono">This chat is empty.</div>\n'
            "    </div>\n"
        )
    else:
        msg_rows = []
        for m in messages:
            author = html.escape(str(m.get("author_name") or m.get("author_sid8") or "unknown")[:60])
            ts = html.escape(str(m.get("ts") or ""))
            # Markdown-safe: escaped plain text in a <pre>, never rendered as
            # markdown/HTML (no markdown library, no innerHTML). XSS discipline
            # applies here in particular: message bodies are the one field on
            # this page a chat participant fully controls.
            body = html.escape(str(m.get("body") or ""))
            msg_rows.append(
                '      <div class="msg">\n'
                f'        <div class="msg-head mono"><span class="msg-author">{author}</span>'
                f' <span class="msg-ts">{ts}</span></div>\n'
                f'        <pre class="msg-body">{body}</pre>\n'
                '      </div>\n'
            )
        transcript_html = '    <div class="chat-transcript">\n' + "".join(msg_rows) + "    </div>\n"

    layout = (
        '    <div class="layout">\n'
        f'      <div>\n{transcript_html}      </div>\n'
        f'      <div>\n{participants_html}      </div>\n'
        '    </div>\n'
    )
    return _page(f"{topic_raw} · WatchTower", header + layout)


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

    def _read_json_body(self) -> Optional[Any]:
        """Parse the POST body as JSON. Returns None on malformed JSON (the
        caller turns that into a 400); a non-dict payload is still returned
        so the caller can report the right shape mismatch."""
        try:
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length) if length > 0 else b"{}"
            return json.loads(raw)
        except (ValueError, json.JSONDecodeError):
            return None

    def do_GET(self) -> None:  # noqa: N802 (http.server contract)
        path = self.path.split("?", 1)[0].rstrip("/") or "/"
        if path == "/":
            from . import chats
            try:
                chat_rows = chats.list_chats(include_archived=False)
            except Exception:
                chat_rows = []
            self._html(200, render_index(status_payload(), chat_rows))
        elif path.startswith("/chat/"):
            from . import chats
            ref = urllib.parse.unquote(path[len("/chat/"):])
            try:
                data = chats.read_chat(ref)
            except ValueError:
                self._html(404, _chat_not_found_page(ref))
                return
            policy_fn = getattr(chats, "get_chat_policy", None)
            policy = None
            if policy_fn is not None:
                try:
                    policy = policy_fn(ref)
                except Exception:
                    policy = None
            self._html(200, render_chat(ref, data, policy))
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
        elif path == "/api/chats":
            from . import chats
            self._json(200, {"chats": chats.list_chats(include_archived=False)})
        elif path.startswith("/api/chat/"):
            from . import chats
            ref = urllib.parse.unquote(path[len("/api/chat/"):])
            try:
                data = chats.read_chat(ref)
            except ValueError as exc:
                self._json(404, {"error": str(exc)})
            else:
                self._json(200, data)
        else:
            self._json(404, {"error": "not found", "path": path})

    do_HEAD = do_GET

    def do_POST(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0].rstrip("/")
        if path.startswith("/api/ticket/") and path.endswith("/run"):
            ref = urllib.parse.unquote(path[len("/api/ticket/"):-len("/run")])
            try:
                item = q.mark_runnable(ref)
                if item is None:
                    self._json(404, {"error": f"{ref} not found"})
                    return
                if item.get("status") != "open":
                    self._json(400, {"error": f"{item.get('ref', ref)} is not open"})
                    return
                worker = workers.spawn_run_once_worker(
                    item.get("project", ""),
                    item.get("ref", ""),
                    repo_path=str(item.get("repo_path") or ""),
                )
            except ValueError as exc:
                self._json(400, {"error": str(exc)})
                return
            except Exception as exc:  # noqa: BLE001
                self._json(500, {"error": str(exc)})
                return
            self._json(200, {"ok": True, "ticket": item, "worker": worker})
            return
        # POST /api/send: {"to", "text", "mode"} -> messages.send
        if path == "/api/send":
            if not _check_same_origin(self):
                self._json(403, {"error": "cross-origin request rejected"})
                return
            if not _check_bearer_token(self):
                self._json(401, {"error": "missing or invalid bearer token"})
                return
            data = self._read_json_body()
            if not isinstance(data, dict):
                self._json(400, {"error": "invalid JSON body"})
                return
            to = str(data.get("to") or "")
            text = str(data.get("text") or "")
            if not to or not text:
                self._json(400, {"error": "to and text are required"})
                return
            from . import messages
            res = messages.send(to, text, mode=str(data.get("mode") or "send"))
            self._json(200, res)
            return
        # POST /api/ask: {"to", "text", "timeout_ms"} -> messages.ask
        if path == "/api/ask":
            if not _check_same_origin(self):
                self._json(403, {"error": "cross-origin request rejected"})
                return
            if not _check_bearer_token(self):
                self._json(401, {"error": "missing or invalid bearer token"})
                return
            data = self._read_json_body()
            if not isinstance(data, dict):
                self._json(400, {"error": "invalid JSON body"})
                return
            to = str(data.get("to") or "")
            text = str(data.get("text") or "")
            if not to or not text:
                self._json(400, {"error": "to and text are required"})
                return
            from . import messages
            timeout_ms = int(data.get("timeout_ms") or 30000)
            res = messages.ask(to, text, timeout_ms=timeout_ms)
            self._json(200 if res.get("ok") else 504, res)
            return
        # POST /api/chat/create: {"topic", "participants", "include_human"}
        if path == "/api/chat/create":
            if not _check_same_origin(self):
                self._json(403, {"error": "cross-origin request rejected"})
                return
            if not _check_bearer_token(self):
                self._json(401, {"error": "missing or invalid bearer token"})
                return
            data = self._read_json_body()
            if not isinstance(data, dict):
                self._json(400, {"error": "invalid JSON body"})
                return
            topic = str(data.get("topic") or "")
            if not topic:
                self._json(400, {"error": "topic is required"})
                return
            participants = data.get("participants")
            if not isinstance(participants, list):
                self._json(400, {"error": "participants must be a list"})
                return
            from . import chats
            try:
                info = chats.create_chat(
                    topic, participants,
                    include_human=bool(data.get("include_human", True)),
                )
            except Exception as exc:  # noqa: BLE001
                self._json(500, {"error": str(exc)})
                return
            self._json(200, {"ok": True, **info})
            return
        # POST /api/chat/post: {"ref", "body", "author"}
        if path == "/api/chat/post":
            if not _check_same_origin(self):
                self._json(403, {"error": "cross-origin request rejected"})
                return
            if not _check_bearer_token(self):
                self._json(401, {"error": "missing or invalid bearer token"})
                return
            data = self._read_json_body()
            if not isinstance(data, dict):
                self._json(400, {"error": "invalid JSON body"})
                return
            ref = str(data.get("ref") or "")
            body_text = str(data.get("body") or "")
            if not ref or not body_text:
                self._json(400, {"error": "ref and body are required"})
                return
            from . import chats
            author = str(data.get("author") or "")
            try:
                res = chats.post(ref, body_text, author_name=author or "Human")
            except ValueError as exc:
                self._json(404, {"error": str(exc)})
                return
            self._json(200, {"ok": True, **res})
            return
        # POST /api/queue/<name>/add  — ingest a ticket
        # NOTE: no same-origin check below (or on /api/ticket/<ref>/run above),
        # unlike the messaging endpoints above them. That is deliberate, not an
        # oversight: contrib/annotate-widget.js is designed to POST here from
        # any third-party page a user has it embedded on. See
        # _check_same_origin's docstring for the full rationale; do not
        # "fix" this into consistency.
        if path.startswith("/api/queue/") and path.endswith("/add"):
            name = path[len("/api/queue/"):-len("/add")]
            # Read and parse the JSON body.
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length > 0 else b"{}"
                data = json.loads(body)
            except (ValueError, json.JSONDecodeError) as exc:
                self._json(400, {"error": f"invalid JSON: {exc}"})
                return
            note = data.get("note", "") or data.get("title", "")
            if not isinstance(note, str) or not note.strip():
                self._json(400, {"error": "note or title is required"})
                return
            # Derive project from repo_path if provided; queue.enqueue handles it.
            repo_path = str(data.get("repo_path") or "")
            try:
                item = q.enqueue(
                    note=note,
                    title=str(data.get("title") or ""),
                    url=str(data.get("url") or ""),
                    selector=str(data.get("selector") or ""),
                    repo_path=repo_path,
                    source=str(data.get("source") or "api"),
                    text=str(data.get("text") or ""),
                    project=q._norm_project(name),
                )
            except Exception as exc:  # noqa: BLE001
                self._json(500, {"error": str(exc)})
                return
            self._json(200, {
                "ok": True,
                "ref": item["ref"],
                "number": item["number"],
                "project": item["project"],
            })
        else:
            self._json(404, {"error": "not found", "path": path})

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
        print("  GET /chat/<ref>  group-chat transcript")
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

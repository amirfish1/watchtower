---
name: add-annotate-widget
description: Use when the user asks to add WatchTower annotation, add an annotate/report button to an HTML page, or "let me report issues from this page directly to a <QUEUE>". Installs the drop-in ⚑ annotate widget so clicking any element on the page files a ticket into a WatchTower queue, then verifies the ingest works.
---

# Add the WatchTower annotate widget to an HTML app

Goal: after this, the user opens their HTML page, clicks a floating **⚑** button,
picks an element, types a note, and a ticket lands in their WatchTower queue
(`<QUEUE>-1`, `<QUEUE>-2`, ...). A worker drains it with `wt claim -q <QUEUE>`.

## Steps

1. **Queue name.** Use the queue the user named (e.g. `MYAPP`). If they didn't give
   one, derive it from the app/repo name and confirm in one line. Tickets become
   `MYAPP-1`, `MYAPP-2`, ...

2. **Copy the widget** into the app's dev assets (next to the HTML):

   ```bash
   cp /Users/amirfish/Apps/watchtower/contrib/annotate-widget.js <app-dir>/annotate-widget.js
   ```

   (If a local `watchtower/contrib/annotate-widget.js` exists in the repo, use that.)

3. **Include it in the HTML**, dev-only, with the config set BEFORE the script:

   ```html
   <!-- dev only: WatchTower annotate widget -->
   <script>
     window.WT_ANNOTATE = { queue: 'MYAPP', endpoint: 'http://127.0.0.1:8787' };
   </script>
   <script src="annotate-widget.js"></script>
   ```

4. **Make sure the ingest server is up on 8787.** Prefer the background dashboard
   server (binds cleanly; `wt start` runs foreground and may not bind):

   ```bash
   wt dashboard --no-open --port 8787
   # verify:
   curl -s -o /dev/null -w "%{http_code}" http://127.0.0.1:8787/api/queues   # expect 200
   ```

5. **Verify the whole path** before telling the user it works — POST a test
   annotation and confirm a ticket lands, then delete/close it:

   ```bash
   curl -s -X POST http://127.0.0.1:8787/api/queue/MYAPP/add \
     -H 'Content-Type: application/json' \
     -d '{"note":"annotate wiring test","url":"http://localhost/","selector":"body"}'
   wt ls -q MYAPP
   ```

6. **Tell the user**: open the page, click the floating **⚑** (bottom-right, draggable),
   pick an element, type a note → ticket lands in `MYAPP`. Drain with `wt claim -q MYAPP`,
   close with `wt close MYAPP-1 --summary "..."`.

## Notes
- Guard the widget behind a dev-only check so it never ships to production.
- The widget POSTs to `POST <endpoint>/api/queue/<QUEUE>/add` with `{note, url, selector}`.
- Optional server enrichment: add a thin route in the app that forwards the
  annotation and appends `repo_path: <abs path>` so CCC routes the ticket to the
  right project. See `watchtower/contrib/annotate-widget.md`.
- For real end-user bug reports (production), use the GitHub-Issues recipe
  (`watchtower/cookbook/bug-report-widget-github-issues.md`) instead of this dev widget.

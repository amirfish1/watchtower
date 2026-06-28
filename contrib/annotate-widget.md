# Annotate widget

Copy `annotate-widget.js` into your project's dev assets. Guard it behind a
dev-only check. Set config before the script runs:

    window.WT_ANNOTATE = { queue: 'MYAPP', endpoint: 'http://127.0.0.1:8787' };

Start WatchTower: `wt start` (serves the ingest API on port 8787).

That's it. Click the ⚑ button, pick an element, type a note — the ticket lands
in the MYAPP queue and a worker can claim it with `wt claim -q MYAPP`.

## Server-side enrichment (optional)

If you want to attach a `repo_path` (so tickets get routed to the right project
in CCC), add a thin server route in your app that forwards the annotation and
appends `repo_path: <absolute path to repo on disk>`. POST to the same endpoint:

    POST /api/queue/MYAPP/add
    { "note": "...", "url": "...", "selector": "...", "repo_path": "/path/to/repo" }

The project code is derived from `repo_path`'s basename when no explicit queue
name is embedded in the path segment.

## How workers drain it

Claim the oldest open ticket:

    wt claim -q MYAPP

When done, close it with a summary:

    wt close MYAPP-7 --summary "fixed the button alignment"

List what is open:

    wt ls -q MYAPP

## Production: GitHub Issues

For production bug reports from real users, use the GitHub Issues recipe instead:
[cookbook/bug-report-widget-github-issues.md](../cookbook/bug-report-widget-github-issues.md)

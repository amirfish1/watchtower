"""Full GitHub prose snapshots. Network refresh is an explicit maintenance task.

Normal get() calls preserve the detail they already fetched, without making
another request. Claims and local recall only read these files.
"""
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import subprocess

from . import storage


def directory():
    return storage.data_dir() / "related-github"


def _repo_key(repo):
    return hashlib.sha256(repo.encode()).hexdigest()[:24]


def remember(repo, issue):
    path = directory() / _repo_key(repo)
    path.mkdir(parents=True, exist_ok=True)
    target = path / f"{int(issue['number'])}.json"
    with storage.atomic_destination(target) as temp:
        temp.write_text(json.dumps(dict(repo=repo, issue=issue)))


def read():
    repos = {}
    for p in sorted(directory().glob("*/*.json")):
        try:
            data = json.loads(p.read_text())
            repos.setdefault(data["repo"], {})[int(data["issue"]["number"])] = data["issue"]
        except (OSError, ValueError, KeyError, TypeError):
            continue
    return repos


def stamps():
    # Atomic replacement of an issue changes its directory mtime.
    return [(str(p), p.stat().st_mtime_ns) for p in sorted(directory().glob("*")) if p.is_dir()]


def coverage():
    out = {}
    for p in directory().glob("*/coverage.json"):
        try:
            value = json.loads(p.read_text())
            out[value["repo"]] = value
        except (OSError, ValueError, KeyError):
            continue
    return out


def hydrate(*, repo=""):
    from . import config, github_backend as gh
    repos = [repo] if repo else sorted({v.get("github_repo") for v in
        config.all_queues(include_archived=True).values() if isinstance(v, dict)
        and v.get("backend") == "github" and v.get("github_repo")})
    existing = read()
    since = (datetime.now(timezone.utc)-timedelta(days=30)).isoformat()
    results = []
    for name in repos:
        # Paginate complete recent history; do not inherit the display-list cap.
        proc = subprocess.run(["gh", "api", "--paginate", "--slurp",
            f"repos/{name}/issues?state=all&since={since}&per_page=100"],
            capture_output=True, text=True, timeout=120)
        if proc.returncode:
            raise RuntimeError(f"GitHub history refresh failed for {name}: {proc.stderr[-500:]}")
        count = changed = 0
        for page in json.loads(proc.stdout):
            for raw in page:
                if "pull_request" in raw:
                    continue
                count += 1
                old = existing.get(name, {}).get(int(raw["number"]))
                if old and old.get("updatedAt") == raw.get("updated_at") and "comments" in old:
                    continue
                issue = gh._rest_issue_to_gh(raw)
                comments = []
                if raw.get("comments"):
                    detail = subprocess.run(["gh", "api", "--paginate", "--slurp",
                        f"repos/{name}/issues/{raw['number']}/comments?per_page=100"],
                        capture_output=True, text=True, timeout=60)
                    if detail.returncode:
                        raise RuntimeError(f"GitHub comments refresh failed for {name}#{raw['number']}: {detail.stderr[-500:]}")
                    comments = [gh._rest_comment_to_gh(c) for pg in json.loads(detail.stdout) for c in pg]
                issue["comments"] = comments
                remember(name, issue)
                changed += 1
        marker = directory() / _repo_key(name) / "coverage.json"
        marker.parent.mkdir(parents=True, exist_ok=True)
        with storage.atomic_destination(marker) as temp:
            temp.write_text(json.dumps(dict(repo=name, since=since, rows=count,
                completed_at=datetime.now(timezone.utc).isoformat())))
        results.append(dict(repo=name, rows=count, refreshed=changed))
    return results

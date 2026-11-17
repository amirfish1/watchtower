#!/usr/bin/env python3
"""Apply an explicitly reviewed WT-89 operation list (dry run by default).

Use --snapshot for extra historical dependency records absent the display cache.
No ticket classification or question rewriting is automated here.
"""
import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from watchtower import person_blocks, watches
from watchtower.github_backend import GitHubBackendError


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('plan', type=Path)
    parser.add_argument('--worker', required=True)
    parser.add_argument('--snapshot', type=Path)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--receipts', type=Path, required=True)
    args = parser.parse_args()
    operations = json.loads(args.plan.read_text())
    items, coverage = watches.inventory()
    by_ref = {i['ref']: i for i in items}
    if args.snapshot:
        for item in json.loads(args.snapshot.read_text()):
            by_ref.setdefault(item['ref'], item)
    results = []
    # Fences/validation also run in dry run. A plan never auto-refreshes its
    # fingerprint: changed rows need a fresh human/worker review.
    for operation in operations:
        try:
            if args.apply:
                result = person_blocks.apply(operation, list(by_ref.values()), args.worker)
            else:
                import copy
                result = copy.deepcopy(by_ref[operation['ref']])
                person_blocks.transform(result, operation, list(by_ref.values()), args.worker)
            results.append(dict(ref=operation['ref'], action=operation['action'], status='applied' if args.apply else 'validated'))
        except (ValueError, KeyError, GitHubBackendError) as exc:
            results.append(dict(ref=operation['ref'], status='refused', error=str(exc)))
        args.receipts.write_text(json.dumps(dict(results=results, coverage=coverage), indent=2)+'\n')
    print(json.dumps({'processed':len(results), 'refused':[r for r in results if r['status']=='refused']}))
    return int(any(r['status']=='refused' for r in results))


if __name__ == '__main__':
    raise SystemExit(main())

#!/usr/bin/env python3
"""Permanent WT-89 audit of current ticket metadata; no network or sends."""
import json
from pathlib import Path
import sys
import time
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from watchtower import config, liveness, queue as q, watches


def audit():
    items, coverage = watches.inventory()
    active = [i for i in items if i.get('status') != 'closed' and not config.ticket_is_archived(i)]
    violations = []
    ctx = liveness.ResolverContext()
    for item in active:
        ref = item['ref']
        wait = item.get('wait') or {}
        if item.get('needs_input'):
            try:
                q.validate_person_block(wait.get('on'), wait.get('actor',''), item.get('block_question'))
            except ValueError as exc:
                violations.append(dict(ref=ref, reason=str(exc)))
        release = item.get('release_wait')
        if release:
            try:
                from watchtower.releases import validate_wait
                validate_wait(item, release)
            except ValueError as exc:
                violations.append(dict(ref=ref, reason=str(exc)))
        if (item.get('status') == 'in_progress' and not item.get('needs_input')
                and time.time()-max(q._iso_ts(item.get('updated_at')),q._iso_ts(item.get('claimed_at'))) > liveness.stall_s()):
            if liveness._foreign_host(item):
                age = liveness.claim_heartbeat_age(item, ctx.now)
                if age is None or age > liveness.stall_s():
                    violations.append(dict(ref=ref, reason='stale foreign claim signal; owning-host proof required, not proof of death'))
            else:
                verdict = liveness.claim_owner(item, ctx)
                if verdict.verdict != 'alive':
                    violations.append(dict(ref=ref, reason='stale local claim: '+verdict.evidence))
    stale = [c for c in coverage if c['source'].endswith(':open') and (c['missing'] or c['stale'])]
    return dict(status='fail' if violations else 'pending' if stale else 'pass', real_data=True,
                evidence=dict(active=len(active), violations=violations, stale_open_sources=stale))


if __name__ == '__main__':
    print(json.dumps(audit()))

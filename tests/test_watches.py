"""A closed ticket's future evidence is watched, never parked or reopened."""
import json
import shlex
import sys

import pytest

from tests.test_owner_view import _load, _ticket
from watchtower import watches


@pytest.fixture(params=["file", "github"])
def env(request, tmp_path, monkeypatch):
    loaded = _load(tmp_path, monkeypatch, request.param)
    monkeypatch.setattr(watches, "_path", lambda: tmp_path / "watches.json")
    return loaded


def check(tmp_path, result=None):
    receipt = tmp_path / "receipt.json"
    receipt.write_text(json.dumps(result or {"status": "pending", "evidence": "no qualifying records"}))
    command = shlex.join([sys.executable, "-c", "from pathlib import Path; print(Path('receipt.json').read_text())"])
    watches.register("natural-check", command=command, cwd=str(tmp_path), where="test host",
                     schedule="daemon every minute", every=60, queue="WATCH-FAIL")
    return receipt


def close(env, tmp_path):
    q, cli, _a, _ov, name = env
    ref = _ticket(q, name)
    assert cli.main(["close", ref, "--worker", "w1", "--summary", "Verified source; watching natural behavior",
                     "--no-code", "--caveat", "Natural behavior remains unverified", "--watch", "natural-check"]) == 0
    return ref


def test_unknown_watch_refuses_close_without_mutation(env):
    q, cli, _a, _ov, name = env
    ref = _ticket(q, name)
    before = q.get(ref)
    assert cli.main(["close", ref, "--worker", "w1", "--summary", "fixed", "--no-code", "--watch", "missing"]) == 1
    assert q.get(ref) == before


def test_watch_roundtrip_pending_real_pass_and_permanent_regression(env, tmp_path):
    q, _cli, _a, ov, _name = env
    receipt = check(tmp_path)
    ref = close(env, tmp_path)
    item = q.get(ref)
    assert item["resolution"]["watches"][0]["origin"] == ref
    assert not item.get("deferred_until") and not item.get("release_wait")
    assert ov.build([item])["watched"][0]["last"]["status"] == "not-run"
    result = watches.run("natural-check", items=[item])[0]
    assert result["status"] == "pending" and watches.owner_rows([item])
    receipt.write_text(json.dumps(dict(status="pass", real_data=False, evidence="fixture only")))
    assert watches.run("natural-check", items=[item])[0]["status"] == "pending"
    assert watches.owner_rows([item])
    receipt.write_text(json.dumps(dict(status="pass", real_data=True, evidence="production record 123")))
    assert watches.run("natural-check", items=[item])[0]["status"] == "pass"
    assert not watches.owner_rows([item])
    # First real pass clears attention, not the permanent regression check.
    receipt.write_text(json.dumps(dict(status="fail", real_data=True, evidence="production record 124 violates assertion")))
    fail = watches.run("natural-check", items=[item])[0]
    again = watches.run("natural-check", items=[item])[0]
    assert fail["failure_ref"] == again["failure_ref"]
    new = q.get(fail["failure_ref"])
    from watchtower import related
    assert any(link["ref"] == ref and link["kind"] == "earlier-attempt" for link in related.links(new))
    assert new["dedupe_key"].startswith(f"watch:{ref}:natural-check:")
    assert q.get(ref)["status"] == "closed"
    assert not q.get(ref).get("needs_input")


def test_real_pending_allows_a_new_failure_after_the_previous_ticket_closes(env, tmp_path):
    q, *_ = env
    receipt = check(tmp_path)
    ref = close(env, tmp_path)
    items = [q.get(ref)]

    def run(status, real_data, evidence):
        receipt.write_text(json.dumps(dict(status=status, real_data=real_data, evidence=evidence)))
        return watches.run("natural-check", items=items)[0]

    first = run("fail", False, "query HTTP 504")["failure_ref"]
    assert q.close(first, resolution="Upstream outage resolved")["status"] == "closed"
    assert run("pending", False, "no data available")["status"] == "pending"
    assert run("fail", True, "new diagnostic")["failure_ref"] == first

    pending = run("pending", True, "query healthy; no qualifying event")
    assert pending["status"] == "pending" and pending["real_data"] is True
    assert not pending.get("failure_ref")
    assert watches.owner_rows(items)[0]["last"]["status"] == "pending"
    # An existing registry has the pending receipt from before this fix.
    with watches._lock():
        data = watches._load()
        binding = data["bindings"][ref + ":natural-check"]
        binding.pop("pending_after_failure", None)
        watches._save(data)

    assert run("fail", False, "query HTTP 504")["failure_ref"] == first
    new = run("fail", True, "new diagnostic")["failure_ref"]
    assert new != first and q.get(new)["status"] == "open"
    assert q.get(first)["status"] == q.get(ref)["status"] == "closed"
    assert run("fail", True, "new diagnostic")["failure_ref"] == new
    assert run("pending", True, "healthy again")["status"] == "pending"
    assert run("fail", True, "another new diagnostic")["failure_ref"] == new
    assert q.close(new, resolution="Diagnostic resolved")["status"] == "closed"
    assert run("fail", True, "new diagnostic")["failure_ref"] == new
    assert run("fail", True, "another new diagnostic")["failure_ref"] != new


def test_shared_watch_rearms_once_when_originals_change(env, tmp_path):
    from watchtower import related

    q, *_ = env
    receipt = check(tmp_path, dict(status="fail", real_data=False, evidence="outage"))
    ref = close(env, tmp_path)
    second = close(env, tmp_path)
    original = q.get(ref)
    other = q.get(second)
    first, again = watches.run("natural-check", items=[original, other])
    assert first["failure_ref"] == again["failure_ref"]
    assert q.close(first["failure_ref"], resolution="Outage resolved")["status"] == "closed"

    receipt.write_text(json.dumps(dict(status="pending", real_data=True, evidence="healthy read")))
    assert {row["status"] for row in watches.run("natural-check", items=[original, other])} == {"pending"}
    third = close(env, tmp_path)
    later = q.get(third)
    # The first binding is temporarily ineligible when a new sibling joins.
    reopened = dict(original, status="open")
    before_failure = watches.sync([later])
    receipt.write_text(json.dumps(dict(status="fail", real_data=True, evidence="distinct regression")))
    results = watches.run("natural-check", items=[reopened, other, later])
    assert {row["origin"] for row in results} == {second, third}
    new = results[0]["failure_ref"]
    assert new != first["failure_ref"] and q.get(new)["status"] == "open"
    assert {row["failure_ref"] for row in results} == {new}
    # Replay from the registry state before the receipt write. The queue has
    # already committed the replacement ticket.
    with watches._lock():
        watches._save(before_failure)
    results = watches.run("natural-check", items=[original, other, later])
    assert {row["failure_ref"] for row in results} == {new}
    assert {link["ref"] for link in related.links(q.get(new))} == {ref, second, third}
    assert all(q.get(origin)["status"] == "closed" for origin in (ref, second, third))


def test_schedule_changes_only_check_cadence_not_original_state(env, tmp_path):
    q, *_ = env
    check(tmp_path)
    ref = close(env, tmp_path)
    item = q.get(ref)
    assert watches.run(now=1000, items=[item])
    assert watches.run(now=1030, items=[item]) == []
    assert watches.run(now=1060, items=[item])
    assert q.get(ref)["status"] == "closed"


def test_registered_spec_is_immutable(env, tmp_path):
    check(tmp_path)
    with pytest.raises(ValueError, match="immutable"):
        watches.register("natural-check", command="true", cwd=str(tmp_path), where="test host",
                         schedule="every minute", every=60, queue="WATCH-FAIL")


def test_broken_runtime_source_returns_error_instead_of_silent_skip(env, tmp_path):
    q, *_ = env
    check(tmp_path)
    ref = close(env, tmp_path)
    # A registered working directory can disappear after registration.
    original = watches._execute
    spec = watches.snapshots(['natural-check'], ref)[0]
    spec['cwd'] = str(tmp_path/'gone')
    result = original(spec)
    assert result['status'] == 'error' and result['real_data'] is False
    with pytest.raises(ValueError, match='not registered'):
        watches.run('missing', items=[q.get(ref)])


def test_issue_body_cannot_introduce_or_change_an_executable_check(env, tmp_path):
    q, *_ = env
    check(tmp_path)
    ref = close(env, tmp_path)
    item = q.get(ref)
    item['resolution']['watches'][0]['command'] = 'touch SHOULD_NOT_EXIST'
    assert watches.run(items=[item]) == []
    assert not (tmp_path/'SHOULD_NOT_EXIST').exists()
    item['resolution']['watches'][0]['name'] = 'unregistered'
    assert watches.run(items=[item]) == []
    assert not watches._load()['bindings']
    assert watches.owner_rows([item])[0]['last']['status'] == 'not-run'


def test_pending_watch_reaches_digest_warning_with_result_then_real_pass_removes_it(env, tmp_path, monkeypatch):
    q, _cli, _a, ov, _name = env
    receipt = check(tmp_path)
    ref = close(env, tmp_path)
    item = q.get(ref)
    watches.run('natural-check', items=[item])
    # The existing morning digest consumes snapshot warnings.
    monkeypatch.setattr(ov.q, 'list_items', lambda *a, **k: [item])
    from watchtower import related
    monkeypatch.setattr(related, 'cached_tickets', lambda: ([], {}))
    view = ov.snapshot()
    warning = next(w for w in view['warnings'] if f'Watching closed {ref}' in w)
    assert 'natural-check' in warning and 'test host' in warning
    assert 'pending' in warning and 'no qualifying records' in warning
    assert 'no qualifying records' in ov.render(view)
    receipt.write_text(json.dumps(dict(status='pass', real_data=True, evidence='natural production event')))
    watches.run('natural-check', items=[item])
    assert not any('Watching closed' in w for w in ov.snapshot()['warnings'])


def test_custom_queue_store_isolates_watch_registry(tmp_path, monkeypatch):
    monkeypatch.setenv('WATCHTOWER_STORE', str(tmp_path/'isolated.json'))
    assert watches._path() == tmp_path/'isolated.watches.json'
    assert not watches._load()['bindings']


def test_checks_and_owner_visibility_outlive_closed_display_retention(env, tmp_path):
    q, *_ = env
    receipt = check(tmp_path)
    ref = close(env, tmp_path)
    watches.run('natural-check', items=[q.get(ref)])
    assert watches.owner_rows([])[0]['ref'] == ref
    assert watches.owner_rows([], project='OTHER') == []
    # The original ages out of the poller's closed list; registration is durable.
    receipt.write_text(json.dumps(dict(status='fail', real_data=True, evidence='new natural regression')))
    result = watches.run('natural-check', items=[])[0]
    new = q.get(result['failure_ref'])
    from watchtower import related
    assert any(link['ref'] == ref for link in related.links(new))
    assert q.get(ref)['status'] == 'closed'


def test_default_run_uses_raw_inventory_without_ticket_conversion(env, tmp_path, monkeypatch):
    q, *_ = env
    check(tmp_path)
    ref = close(env, tmp_path)
    item = q.get(ref)
    monkeypatch.setattr(watches, 'inventory', lambda: ([item], []))
    monkeypatch.setattr(q, 'list_items', lambda *a, **k: pytest.fail('full ticket conversion on watch path'))
    assert watches.run('natural-check')[0]['status'] == 'pending'


def test_legacy_resolution_cannot_prevent_other_registered_checks_or_owner_view(env, tmp_path):
    q, *_ = env
    check(tmp_path)
    ref = close(env, tmp_path)
    watched = q.get(ref)
    legacy = dict(ref='OLD-1', project='OLD', status='closed', resolution='historical outcome')
    # Raw inventories deliberately retain the old on-disk representation.
    results = watches.run('natural-check', items=[legacy, watched])
    assert len(results) == 1 and results[0]['origin'] == ref
    rows = watches.owner_rows([legacy, watched])
    assert len(rows) == 1 and rows[0]['ref'] == ref


def test_inventory_retains_github_update_time_for_claim_lease(env, monkeypatch):
    from watchtower import config, github_backend as gh
    q, *_ = env
    monkeypatch.setattr(config, 'all_queues', lambda: {'G': {'backend':'github','github_repo':'owner/repo'}})
    monkeypatch.setattr(config, 'github_queues_for_repo', lambda repo: ['G'])
    monkeypatch.setattr(gh, '_read_persisted_list_cache', lambda: {'owner/repo:open': {
        'at': 0, 'data': [{'number':1, 'title':'Active claim', 'body':'', 'labels':[],
                          'state':'OPEN', 'createdAt':'2026-01-01T00:00:00Z',
                          'updatedAt':'2026-11-17T17:25:00Z'}]}})
    items, _ = watches.inventory()
    assert next(i for i in items if i['ref']=='G-1')['updated_at']=='2026-11-17T17:25:00Z'


def test_inherited_origin_shares_execution_and_receipt(env, tmp_path, monkeypatch):
    q, *_ = env
    check(tmp_path)
    ref = close(env, tmp_path)
    item = q.get(ref)
    other = dict(item, ref='COPY-551')
    # Actual CCC-551 defect: duplicate close retained CCC-550's origin.
    assert other['resolution']['watches'][0]['origin'] == ref
    calls = []
    monkeypatch.setattr(watches, '_execute', lambda spec: calls.append(spec) or dict(
        status='pending', real_data=True, evidence='shared production receipt'))
    results = watches.run('natural-check', items=[item, other])
    assert len(calls) == 1
    assert {r['origin'] for r in results} == {ref, 'COPY-551'}
    assert all(r['last']['status'] == 'pending' for r in watches.owner_rows([item, other]))
    assert watches.run(now=results[0]['at'] + 30, items=[item, other]) == []
    # Another duplicate arriving after execution inherits the receipt immediately.
    third = dict(item, ref='COPY-552')
    watches.sync([third])
    assert watches.owner_rows([third])[-1]['last']['status'] == 'pending'


@pytest.mark.parametrize('schedule', ['hourly; pass after 2026-11-18T15:45:47Z',
                                     'Hourly; original 24h window', 'passes only on 2026-12-01'])
def test_date_keyed_rules_rejected_at_registration_and_close(env, tmp_path, schedule):
    q, cli, *_rest, name = env
    with pytest.raises(ValueError, match='date-keyed'):
        watches.register('clock', command='true', cwd=str(tmp_path), where='her',
                         schedule=schedule, queue='WATCH-FAIL')
    check(tmp_path)
    with watches._lock():
        data = watches._load()
        data['checks']['natural-check']['schedule'] = schedule
        watches._save(data)
    ref = _ticket(q, name)
    assert cli.main(['close', ref, '--worker', 'w1', '--summary', 'fixed', '--no-code',
                     '--watch', 'natural-check']) == 1
    assert q.get(ref)['status'] != 'closed'


def test_explicit_replacement_preserves_audit_and_invalidates_acceptance(env, tmp_path):
    q, *_ = env
    check(tmp_path, dict(status='pass', real_data=True, evidence='old live acceptance'))
    ref = close(env, tmp_path)
    item = q.get(ref)
    watches.run('natural-check', items=[item])
    old = watches._load()['checks']['natural-check']
    kwargs = {k: old[k] for k in ('command', 'cwd', 'where', 'schedule', 'queue', 'every')}
    kwargs['command'] = 'python3 missing-stable-check.py'
    new = watches.register('natural-check', **kwargs, replace=True, reason='move off disposable worktree')
    assert new['revision'] != old['revision']
    bound = watches.sync([item])['bindings'][ref + ':natural-check']
    assert bound['spec']['command'] == kwargs['command']
    assert not bound['first_real_pass'] and not bound['last']
    assert bound['receipt_history'][0]['last']['status'] == 'pass'
    failure = watches.run('natural-check', items=[item])[0]
    assert failure['status'] == 'error' and failure['failure_ref']
    assert watches.owner_rows([item])[0]['last']['status'] == 'error'
    assert q.get(ref)['status'] == 'closed'


def test_missing_command_turns_previously_passing_watch_red(env, tmp_path):
    q, *_ = env
    script = tmp_path / 'monitor.py'
    script.write_text('print(\'{"status":"pass","real_data":true,"evidence":"natural sample"}\')')
    watches.register('file-check', command=shlex.join([sys.executable, str(script)]), cwd=str(tmp_path),
                     where='her', schedule='every minute', every=60, queue='WATCH-FAIL')
    ref = _ticket(q, env[-1])
    assert env[1].main(['close', ref, '--worker', 'w1', '--summary', 'fixed', '--no-code',
                        '--watch', 'file-check']) == 0
    item = q.get(ref)
    assert watches.run('file-check', items=[item])[0]['status'] == 'pass'
    script.unlink()
    result = watches.run('file-check', items=[item])[0]
    assert result['status'] == 'error' and result['failure_ref']
    assert watches.owner_rows([item])[0]['last']['status'] == 'error'


def test_never_run_overdue_flag_reaches_owner_render_and_digest(env, tmp_path, monkeypatch):
    q, _cli, _a, ov, _name = env
    check(tmp_path)
    ref = close(env, tmp_path)
    item = q.get(ref)
    registered = watches._load()['registered_at']['natural-check']
    monkeypatch.setattr(watches.time, 'time', lambda: registered + 119)
    assert not watches.owner_rows([item])[0]['overdue']
    monkeypatch.setattr(watches.time, 'time', lambda: registered + 120)
    assert watches.owner_rows([item])[0]['overdue']
    monkeypatch.setattr(ov.q, 'list_items', lambda *a, **k: [item])
    from watchtower import related
    monkeypatch.setattr(related, 'cached_tickets', lambda: ([], {}))
    view = ov.snapshot()
    assert 'OVERDUE' in ov.render(view)
    assert any('OVERDUE' in w for w in view['warnings'])


def test_replacement_cli_requires_reason_and_preserves_execution_authority(env, tmp_path):
    _q, cli, *_ = env
    check(tmp_path)
    args = ['monitor', '--register', 'natural-check', '--cmd', 'true', '--cwd', str(tmp_path),
            '--where', 'her', '--schedule', 'pass on matching natural evidence; hourly',
            '-q', 'WATCH-FAIL', '--replace']
    assert cli.main(args) == 1
    assert cli.main(args + ['--reason', 'stable committed entrypoint']) == 0
    assert watches._load()['revisions']['natural-check'][0]['reason'] == 'stable committed entrypoint'


def _remote_registry(tmp_path, *, last=None):
    spec = dict(name='mac-check', command='touch MUST_NOT_EXECUTE', cwd='/remote/repo',
                where='mac', schedule='hourly permanently', queue='OPS', every=3600,
                revision='remote-v2')
    old = dict(spec, schedule='old schedule', revision='remote-v1')
    return dict(checks={'mac-check': spec}, revisions={'mac-check': [{'spec': old}]},
                registered_at={'mac-check': 1000}, bindings={'G-1:mac-check': dict(
                    spec=dict(spec, origin='G-1'), attached_at='1970-01-01T00:16:40Z',
                    last=last or {}, first_real_pass='')})


def test_remote_source_replaces_historical_specs_and_shares_receipts(env, tmp_path, monkeypatch):
    from types import SimpleNamespace
    remote = _remote_registry(tmp_path, last=dict(status='pending', at=9000, evidence='natural Mac capture'))
    watches.add_source('macbook', '/remote/watches.json')
    calls = []
    def fetch(argv, **kwargs):
        calls.append(argv)
        assert argv[-1] == 'cat -- /remote/watches.json'
        assert kwargs['timeout'] == 5
        return SimpleNamespace(returncode=0, stdout=json.dumps(remote))
    monkeypatch.setattr(watches.subprocess, 'run', fetch)
    monkeypatch.setattr(watches.time, 'time', lambda: 10000)
    snapshot = dict(remote['revisions']['mac-check'][0]['spec'], origin='G-1')
    items = [dict(ref='G-2', project='G', status='closed', closed_at='1970-01-01T00:17:00Z',
                  resolution={'watches': [snapshot]})]
    rows = watches.owner_rows(items, project='G')
    assert {r['ref'] for r in rows} == {'G-1', 'G-2'}
    for row in rows:
        assert row['schedule'] == 'hourly permanently'
        assert row['last']['evidence'] == 'natural Mac capture'
        assert row['source_host'] == 'macbook'
        assert not row['overdue']
    assert watches.owner_rows(items) == rows
    assert len(calls) == 1  # bounded cache across owner reads
    assert not watches._load()['checks'] and not watches._load()['bindings']
    monkeypatch.setattr(watches, '_execute', lambda spec: pytest.fail('remote execution'))
    assert watches.run(items=items) == []
    # A modified issue body cannot borrow remote execution evidence.
    items[0]['resolution']['watches'][0]['command'] = 'other command'
    row = next(r for r in watches.owner_rows(items) if r['ref'] == 'G-2')
    assert row['last']['status'] == 'not-run'


def test_remote_never_run_overdue_and_source_failure_are_visible(env, tmp_path, monkeypatch):
    from types import SimpleNamespace
    _q, _cli, _a, ov, _name = env
    remote = _remote_registry(tmp_path)
    watches.add_source('macbook', '/remote/watches.json')
    monkeypatch.setattr(watches.time, 'time', lambda: 8200)
    monkeypatch.setattr(watches.subprocess, 'run', lambda *a, **k: SimpleNamespace(
        returncode=0, stdout=json.dumps(remote)))
    row = watches.owner_rows([])[0]
    assert row['overdue'] and 'OVERDUE' in row['warning']
    monkeypatch.setattr(watches.time, 'time', lambda: 8261)
    monkeypatch.setattr(watches.subprocess, 'run', lambda *a, **k: SimpleNamespace(returncode=255))
    view = ov.build([])
    assert view['watch_source_warnings']
    assert 'OVERDUE' in ov.render(view) and 'unavailable' in ov.render(view)
    assert 'cached metadata may be stale' in view['watched'][0]['warning']


def test_failed_source_without_cache_reaches_digest_and_local_overdue(env, tmp_path, monkeypatch):
    from types import SimpleNamespace
    q, _cli, _a, ov, _name = env
    check(tmp_path)
    ref = close(env, tmp_path)
    item = q.get(ref)
    registered = watches._load()['registered_at']['natural-check']
    watches.add_source('macbook', '/remote/watches.json')
    monkeypatch.setattr(watches.time, 'time', lambda: registered + 120)
    monkeypatch.setattr(watches.subprocess, 'run', lambda *a, **k: SimpleNamespace(returncode=255))
    monkeypatch.setattr(ov.q, 'list_items', lambda **k: [item])
    from watchtower import related
    monkeypatch.setattr(related, 'cached_tickets', lambda: ([], {}))
    view = ov.snapshot()
    assert any('Watch source macbook unavailable' in w for w in view['warnings'])
    assert view['watched'][0]['overdue']


def test_existing_empty_duplicate_uses_latest_identical_revision(env, tmp_path):
    q, *_ = env
    check(tmp_path)
    ref = close(env, tmp_path)
    item = q.get(ref)
    watches.run('natural-check', items=[item])
    with watches._lock():
        data = watches._load()
        original = data['bindings'][ref + ':natural-check']
        data['bindings']['COPY-2:natural-check'] = dict(original, spec=dict(original['spec'], origin='COPY-2'), last={})
        watches._save(data)
    rows = watches.owner_rows([])
    assert len(rows) == 2
    assert all(r['last']['status'] == 'pending' for r in rows)


@pytest.mark.parametrize('host,path', [('-oProxyCommand=oops', '/x'), ('mac;cmd', '/x'), ('mac', 'relative')])
def test_source_registration_rejects_unsafe_arguments(env, host, path):
    with pytest.raises(ValueError):
        watches.add_source(host, path)


def test_forged_local_snapshot_cannot_borrow_receipt_by_revision_alone(env, tmp_path):
    q, *_ = env
    check(tmp_path, dict(status='pass', real_data=True, evidence='real local event'))
    ref = close(env, tmp_path)
    item = q.get(ref)
    watches.run('natural-check', items=[item])
    forged = dict(item, ref='FORGED-1', resolution={'watches': [dict(
        item['resolution']['watches'][0], command='not the authorized check')]})
    row = watches.owner_rows([forged])[0]
    assert row['ref'] == 'FORGED-1' and row['last']['status'] == 'not-run'


def test_source_cli_and_nonobject_registry_error(env, tmp_path, monkeypatch):
    from types import SimpleNamespace
    cli = env[1]
    assert cli.main(['monitor', '--add-source', 'macbook', '--registry', '/remote/watch.json']) == 0
    monkeypatch.setattr(watches.subprocess, 'run', lambda *a, **k: SimpleNamespace(returncode=0, stdout='[]'))
    warnings = []
    assert watches.owner_rows([], source_warnings=warnings) == []
    assert warnings and 'invalid watch registry' in warnings[0]


def test_stale_remote_pass_cannot_silently_clear_owner_attention(env, tmp_path, monkeypatch):
    from types import SimpleNamespace
    remote = _remote_registry(tmp_path, last=dict(status='pass', real_data=True, at=1000, evidence='natural evidence'))
    remote['bindings']['G-1:mac-check']['first_real_pass'] = '1970-01-01T00:16:40Z'
    watches.add_source('macbook', '/remote/watches.json')
    monkeypatch.setattr(watches.time, 'time', lambda: 2000)
    monkeypatch.setattr(watches.subprocess, 'run', lambda *a, **k: SimpleNamespace(
        returncode=0, stdout=json.dumps(remote)))
    assert watches.owner_rows([]) == []
    monkeypatch.setattr(watches.time, 'time', lambda: 2061)
    monkeypatch.setattr(watches.subprocess, 'run', lambda *a, **k: SimpleNamespace(returncode=255))
    row = watches.owner_rows([])[0]
    assert row['last']['status'] == 'pass' and 'stale' in row['warning']

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

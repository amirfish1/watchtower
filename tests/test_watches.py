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

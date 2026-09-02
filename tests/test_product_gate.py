"""Product gate (2026-09-01 design): block kinds, the needs-rationale icebox,
pre-ack, gate ack/nack, and the close guard."""
import json
import threading
import urllib.error
import urllib.request

import pytest

QUEUE = "GATEQ"


def _file_ticket(wt_env, **kw):
    return wt_env.queue.enqueue(project=QUEUE, note=kw.pop("note", "a bug"), **kw)


# ---------------------------------------------------------------- data model

def test_block_kind_defaults_to_input(wt_env):
    it = _file_ticket(wt_env)
    blocked = wt_env.queue.block(it["ref"], session_id="w1", question="which db?")
    assert blocked["block_kind"] == "input"
    assert blocked["needs_input"] is True


def test_block_kind_rationale_is_stored_and_survives_reload(wt_env):
    it = _file_ticket(wt_env)
    wt_env.queue.block(it["ref"], session_id="w1",
                       question="PITCH: worth fixing?", kind="rationale")
    fresh = wt_env.queue.get(it["ref"])
    assert fresh["block_kind"] == "rationale"
    assert fresh["block_question"] == "PITCH: worth fixing?"


def test_unknown_block_kind_degrades_to_input(wt_env):
    it = _file_ticket(wt_env)
    blocked = wt_env.queue.block(it["ref"], session_id="w1",
                                 question="q", kind="bogus")
    assert blocked["block_kind"] == "input"


def test_reopen_clears_block_kind(wt_env):
    it = _file_ticket(wt_env)
    wt_env.queue.block(it["ref"], session_id="w1", question="q", kind="rationale")
    wt_env.queue.update_status(it["ref"], "open")
    fresh = wt_env.queue.get(it["ref"])
    assert fresh["block_kind"] == ""
    assert fresh["needs_input"] is False


def test_needs_rationale_is_valid_and_unclaimable(wt_env):
    assert "needs-rationale" in wt_env.queue.VALID_READINESS
    assert "needs-rationale" in wt_env.queue.UNCLAIMABLE_READINESS
    _file_ticket(wt_env, readiness="needs-rationale")
    assert wt_env.queue.claim_next("w1", project=QUEUE) is None


def test_enqueue_pre_ack(wt_env):
    it = _file_ticket(wt_env, pre_ack=True)
    assert it["pre_ack"] is True
    assert _file_ticket(wt_env)["pre_ack"] is False


# ---------------------------------------------------------------------- CLI

def test_wt_add_pre_ack_and_block_kind(wt_env, run_cli):
    res = run_cli("add", "-q", QUEUE, "--note", "ship the widget", "--pre-ack")
    assert res.code == 0, res.output
    items = wt_env.queue.list_items(project=QUEUE)
    assert items[-1]["pre_ack"] is True
    ref = items[-1]["ref"]
    res = run_cli("block", ref, "--worker", "w1",
                  "--kind", "rationale", "--question", "PITCH: worth it?")
    assert res.code == 0, res.output
    assert wt_env.queue.get(ref)["block_kind"] == "rationale"


# ----------------------------------------------------------------- gate core

def _gated_pitch(wt_env, **enqueue_kw):
    wt_env.config.set_product_gate(QUEUE, True)
    it = _file_ticket(wt_env, **enqueue_kw)
    wt_env.queue.claim_by_ref(it["ref"], "w1")
    wt_env.queue.block(it["ref"], session_id="w1",
                       question="PITCH: costs 2k tokens/day", kind="rationale")
    return wt_env.queue.get(it["ref"])


def test_gate_ack_records_decision_and_clears_block(wt_env):
    it = _gated_pitch(wt_env)
    out = wt_env.queue.gate_ack(it["ref"], comment="go, but keep it small",
                                by="amir")
    assert out["needs_input"] is False
    assert out["product_ack"]["by"] == "amir"
    assert out["product_ack"]["comment"] == "go, but keep it small"
    assert out["status"] == "in_progress"  # still bound to its session
    assert any(e.get("event") == "gate_ack"
               for e in wt_env.queue.timeline(out))


def test_gate_ack_requires_a_rationale_block(wt_env):
    it = _file_ticket(wt_env)
    with pytest.raises(ValueError):
        wt_env.queue.gate_ack(it["ref"])
    wt_env.queue.block(it["ref"], session_id="w1", question="q")  # kind=input
    with pytest.raises(ValueError):
        wt_env.queue.gate_ack(it["ref"])


def test_gate_nack_iceboxes(wt_env):
    it = _gated_pitch(wt_env)
    out = wt_env.queue.gate_nack(it["ref"], reason="not this quarter", by="amir")
    assert out["status"] == "open"
    assert out["claimed_by"] is None
    assert out["needs_input"] is False
    assert out["readiness"] == "needs-rationale"
    assert out["product_nack"]["comment"] == "not this quarter"
    assert wt_env.queue.claim_next("w2", project=QUEUE) is None  # unclaimable


def test_gate_nack_requires_a_reason(wt_env):
    it = _gated_pitch(wt_env)
    with pytest.raises(ValueError):
        wt_env.queue.gate_nack(it["ref"], reason="")


def test_gate_nack_close_declines(wt_env):
    it = _gated_pitch(wt_env)
    out = wt_env.queue.gate_nack(it["ref"], reason="wrong product direction",
                                 by="amir", close=True)
    assert out["status"] == "closed"
    assert "Declined at product gate" in (out.get("resolution") or {}).get("summary", "")


def test_close_guard_refuses_ungated_implemented_close(wt_env):
    it = _gated_pitch(wt_env)
    with pytest.raises(ValueError):
        wt_env.queue.close(it["ref"], "w1",
                           resolution={"summary": "implemented it anyway"})


def test_close_guard_allows_acked_pre_acked_and_ungated_queues(wt_env):
    it = _gated_pitch(wt_env)
    wt_env.queue.gate_ack(it["ref"], by="amir")
    assert wt_env.queue.close(it["ref"], "w1",
                              resolution={"summary": "done"})["status"] == "closed"

    it2 = _gated_pitch(wt_env, pre_ack=True)
    assert wt_env.queue.close(it2["ref"], "w1",
                              resolution={"summary": "done"})["status"] == "closed"

    wt_env.config.set_product_gate(QUEUE, False)
    it3 = _file_ticket(wt_env)
    wt_env.queue.claim_by_ref(it3["ref"], "w1")
    assert wt_env.queue.close(it3["ref"], "w1",
                              resolution={"summary": "done"})["status"] == "closed"


def test_ack_persists_across_reopen(wt_env):
    it = _gated_pitch(wt_env)
    wt_env.queue.gate_ack(it["ref"], by="amir")
    wt_env.queue.close(it["ref"], "w1", resolution={"summary": "done"})
    wt_env.queue.update_status(it["ref"], "open")
    fresh = wt_env.queue.get(it["ref"])
    assert fresh["product_ack"]["by"] == "amir"
    wt_env.queue.claim_by_ref(fresh["ref"], "w2")
    assert wt_env.queue.close(fresh["ref"], "w2",
                              resolution={"summary": "redone"})["status"] == "closed"


# ------------------------------------------------------------------ CLI verbs

def test_wt_ack_acks_the_gate(wt_env, run_cli):
    it = _gated_pitch(wt_env)
    res = run_cli("ack", it["ref"], "-m", "yes but small")
    assert res.code == 0, res.output
    fresh = wt_env.queue.get(it["ref"])
    assert fresh["product_ack"]["comment"] == "yes but small"
    assert fresh["needs_input"] is False


def test_wt_ack_on_closed_ticket_points_at_unresolved_ack(wt_env, run_cli):
    it = _file_ticket(wt_env)
    wt_env.queue.claim_by_ref(it["ref"], "w1")
    wt_env.queue.close(it["ref"], "w1", resolution={"summary": "done"})
    res = run_cli("ack", it["ref"])
    assert res.code != 0
    assert "unresolved-ack" in res.output


def test_wt_nack_iceboxes_and_requires_reason(wt_env, run_cli):
    it = _gated_pitch(wt_env)
    assert run_cli("nack", it["ref"]).code != 0  # no -m
    res = run_cli("nack", it["ref"], "-m", "not now")
    assert res.code == 0, res.output
    fresh = wt_env.queue.get(it["ref"])
    assert fresh["readiness"] == "needs-rationale"


def test_wt_nack_close(wt_env, run_cli):
    it = _gated_pitch(wt_env)
    res = run_cli("nack", it["ref"], "-m", "wrong direction", "--close")
    assert res.code == 0, res.output
    assert wt_env.queue.get(it["ref"])["status"] == "closed"


def test_wt_gated_lists_only_rationale_blocks(wt_env, run_cli):
    gated = _gated_pitch(wt_env)
    plain = _file_ticket(wt_env)
    wt_env.queue.block(plain["ref"], session_id="w2", question="impl q?")
    res = run_cli("gated", "-q", QUEUE, "--json")
    assert res.code == 0, res.output
    refs = [r["ref"] for r in json.loads(res.output)]
    assert gated["ref"] in refs and plain["ref"] not in refs


# ------------------------------------------------------------- goal templates

def test_drain_goal_carries_gate_contract_only_when_gated(wt_env):
    from watchtower import workers
    wt_env.config.set_product_gate(QUEUE, True)
    gated_goal = workers.drain_goal(QUEUE, "w1", repo_path="/tmp/x")
    assert "PRODUCT GATE" in gated_goal
    assert "--kind rationale" in gated_goal
    wt_env.config.set_product_gate(QUEUE, False)
    assert "PRODUCT GATE" not in workers.drain_goal(QUEUE, "w1", repo_path="/tmp/x")


def test_run_once_goal_carries_gate_contract_only_when_gated(wt_env):
    from watchtower import workers
    wt_env.config.set_product_gate(QUEUE, True)
    gated_goal = workers.run_once_goal(QUEUE, "w1", f"{QUEUE}-1", repo_path="/tmp/x")
    assert "PRODUCT GATE" in gated_goal
    assert "--kind rationale" in gated_goal
    wt_env.config.set_product_gate(QUEUE, False)
    assert "PRODUCT GATE" not in workers.run_once_goal(QUEUE, "w1", f"{QUEUE}-1", repo_path="/tmp/x")


# --------------------------------------------------------------- dashboard

class _DashboardClient:
    def __init__(self, base_url):
        self.base_url = base_url

    def get(self, path):
        req = urllib.request.Request(f"{self.base_url}{path}")
        with urllib.request.urlopen(req) as resp:
            return resp.read().decode()

    def post_json(self, path, body):
        req = urllib.request.Request(
            f"{self.base_url}{path}",
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req) as resp:
                return json.load(resp)
        except urllib.error.HTTPError as exc:
            return json.load(exc)


@pytest.fixture
def dashboard_client(wt_env):
    from watchtower import dashboard
    srv = dashboard.ThreadingHTTPServer(("127.0.0.1", 0), dashboard._Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{srv.server_address[1]}"
    try:
        yield _DashboardClient(base)
    finally:
        srv.shutdown()


def test_dashboard_gate_ack_endpoint(wt_env, dashboard_client):
    it = _gated_pitch(wt_env)
    resp = dashboard_client.post_json(
        f"/api/ticket/{it['ref']}/gate-ack", {"comment": "go"})
    assert resp["ok"] is True
    assert wt_env.queue.get(it["ref"])["product_ack"]["comment"] == "go"


def test_dashboard_gate_nack_endpoint(wt_env, dashboard_client):
    it = _gated_pitch(wt_env)
    resp = dashboard_client.post_json(
        f"/api/ticket/{it['ref']}/gate-nack", {"reason": "not now"})
    assert resp["ok"] is True
    assert wt_env.queue.get(it["ref"])["readiness"] == "needs-rationale"


def test_queue_page_renders_gate_actions(wt_env, dashboard_client):
    it = _gated_pitch(wt_env)
    html_out = dashboard_client.get(f"/q/{QUEUE}")
    assert "wtGateAck" in html_out and "wtGateNack" in html_out





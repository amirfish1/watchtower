import json
import os

from watchtower.watch_checks import production_logs


def test_real_alert_success_failure_and_absence_are_distinct(tmp_path):
    state = tmp_path / "state.json"
    state.write_text('{"fails": 0}')
    os.utime(state, (1000, 1000))
    args = (tmp_path, "/auth/callback", "Falling through", 900, "code=false")
    assert production_logs(*args, now=1000)["status"] == "pending"
    alerts = tmp_path / "alerted.jsonl"
    alerts.write_text(json.dumps(dict(ts=999, path="/auth/callback", level="warning",
                                     status=307, excerpt="Falling through code=false", log_id="natural-1"))+'\n')
    assert production_logs(*args, now=1000) == dict(status="pass", real_data=True,
                                                 evidence="natural warning-level 307 with expected condition; log natural-1")
    with alerts.open("a") as f:
        f.write(json.dumps(dict(ts=1000, path="/auth/callback", level="error",
                               excerpt="Falling through code=false", log_id="regression-2"))+'\n')
    result = production_logs(*args, now=1000)
    assert result["status"] == "fail" and "regression-2" in result["evidence"]
    assert production_logs(*args, now=3000)["status"] == "fail"  # lost collector isn't success


def test_historical_errors_and_unrelated_routes_do_not_claim_failure(tmp_path):
    (tmp_path / "state.json").write_text('{"fails": 0}')
    (tmp_path / "alerted.jsonl").write_text(json.dumps(dict(ts=50, path="/a", level="error", excerpt="timeout"))+'\n')
    assert production_logs(tmp_path, "/a", "timeout", 60)["status"] == "pending"
    assert production_logs(tmp_path, "/b", "timeout", 0)["status"] == "pending"

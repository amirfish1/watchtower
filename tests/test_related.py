import json
import sqlite3
import time
from datetime import datetime, timezone

import pytest

from watchtower import related, related_cli, queue as q, config, github_backend as gh


def iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).isoformat()


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("WATCHTOWER_DATA_DIR", str(tmp_path / "state"))
    monkeypatch.setenv("WATCHTOWER_STORE", str(tmp_path / "queue.json"))
    monkeypatch.setenv("WATCHTOWER_SHIP_GRAPH", str(tmp_path / "missing-ccc.db"))
    monkeypatch.setattr(gh, "_GH_LIST_CACHE_FILE", tmp_path / "gh.json")


def item(ref, text, **kw):
    return dict(ref=ref, title="reporter=unknown-session", text=text,
                created_at=iso(time.time()-3600), updated_at=iso(time.time()-30),
                status="open", **kw)


def insert(conn, it):
    related.upsert(conn, it["ref"], "ticket", related.instant(it["created_at"]),
                   related.instant(it["updated_at"]), it["title"], related.full_text(it), it)


def test_full_text_search_cross_queue_comments_resolution_and_empty_title(tmp_path):
    a = item("A-1", "The booking recovery flow cannot find vouchers")
    b = item("B-1", "irrelevant", comments=[{"body": "booking recovery vouchers"}],
             resolution={"summary": "Restored vouchers after booking recovery"})
    b["title"] = ""
    with related.connect(tmp_path / "recall.db") as conn:
        insert(conn, a); insert(conn, b)
        hits = related.search(conn, a)
    assert [h["ref"] for h in hits] == ["B-1"]
    assert "vouchers" in hits[0]["excerpt"]


def test_excludes_future_and_old_includes_recently_updated_old_ticket(tmp_path):
    now = time.time()
    a = item("A-1", "payment voucher")
    future = item("B-1", "payment voucher")
    future["created_at"] = iso(now+100)
    old = item("B-2", "payment voucher")
    old["updated_at"] = iso(now-31*related.DAY)
    recent = item("B-3", "payment voucher")
    recent["created_at"] = iso(now-60*related.DAY)
    with related.connect(tmp_path / "recall.db") as conn:
        for it in (a, future, old, recent): insert(conn, it)
        assert [h["ref"] for h in related.search(conn, a, at=now)] == ["B-3"]


def test_asof_no_future_comments_or_resolution():
    now = time.time()
    it = item("A-1", "voucher", closed_at=iso(now+10),
              resolution={"summary": "future answer"}, history=[
                  {"at": iso(now-20), "text": "earlier evidence"},
                  {"at": iso(now+20), "text": "future answer"}])
    before = related.as_of(it, now)
    assert "future answer" not in related.full_text(before)
    assert "earlier evidence" in related.full_text(before)
    assert before["status"] == "open"


def test_fts_escaping_stemming_no_reference_leak(tmp_path):
    assert "987" not in related.query_text('OPS-987 "booking" OR NEAR(foo)')
    with related.connect(tmp_path / "recall.db") as conn:
        insert(conn, item("B-1", "Cancelling bookings"))
        assert related.search(conn, item("A-1", 'cancel booking " OR * --'))


def test_local_cache_refresh_never_runs_subprocess(tmp_path, monkeypatch):
    import subprocess
    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: pytest.fail("network or subprocess"))
    q.enqueue(project="A", title="voucher repair", note="voucher recovery")
    it = q.enqueue(project="B", title="voucher missing", note="voucher recovery")
    result = related.lookup(it, worker="w")
    assert result["hits"][0]["ref"] == "A-1"
    with related.connect() as conn:
        assert conn.execute("SELECT count(*) FROM exposures").fetchone()[0] == 1
    assert related.index_path().stat().st_mode & 0o077 == 0


def test_raw_github_full_body_and_metadata_resolution_not_clipped(tmp_path, monkeypatch):
    config.set_backend("GH", "github")
    config.set_github_repo("GH", "fixture/tickets")
    raw = dict(number=7, body="x"*25000 + "\nvoucher\n<!-- watchtower\nresolution_summary: \"recovered bookings\"\n-->",
               comments=[{"body": "valuable comment"}], state="CLOSED", title="", labels=[],
               createdAt=iso(time.time()-100), updatedAt=iso(time.time()), closedAt=iso(time.time()))
    gh._GH_LIST_CACHE_FILE.write_text(json.dumps({"fixture/tickets:closed": {"at": time.time(), "data": [raw]}}))
    items, coverage = related.cached_tickets()
    it = next(i for i in items if i["ref"] == "GH-7")
    assert "valuable comment" in related.full_text(it)
    assert "recovered bookings" in related.full_text(it)
    assert len(related.full_text(it)) > 25000
    assert any(c["missing"] for c in coverage)


def test_claim_failure_is_visible_and_nonfatal(monkeypatch):
    monkeypatch.setattr(related, "lookup", lambda *a, **kw: (_ for _ in ()).throw(sqlite3.OperationalError("locked")))
    result = related_cli.claim_context(item("A-1", "a"), "w")
    assert "locked" in related_cli.render(result)
    assert "Do not interpret" in result["instructions"]


def test_jev_explicit_missing_key(monkeypatch):
    monkeypatch.delenv("TYPESAFE_API_KEY", raising=False)
    monkeypatch.delenv("JEV_API_KEY", raising=False)
    with pytest.raises(ValueError, match="API_KEY"):
        related.triage(item("A-1", "a"), [])


def test_jev_uses_current_contract_and_does_not_confirm(monkeypatch):
    monkeypatch.setenv("JEV_API_KEY", "fixture-key")
    seen = []
    class Response:
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def read(self): return json.dumps({"answers": {"c0": {"choice": "earlier-attempt", "confidence": .9}}, "usage": {"input_tokens": 10}}).encode()
    def request(req, timeout):
        seen.append(json.loads(req.data)); assert timeout == 20
        return Response()
    monkeypatch.setattr(related.urllib.request, "urlopen", request)
    hits = [dict(ref="B-1", status="closed", item=item("B-1", "voucher earlier repair"))]
    related.triage(item("A-1", "voucher recurrence"), hits)
    assert seen[0]["questions"]["c0"]["type"] == "choice"
    assert hits[0]["judgment"]["choice"] == "earlier-attempt"
    assert related.links(hits[0]["item"]) == []


def test_confirm_repeat_is_durable_deep_and_idempotent():
    a = q.enqueue(project="A", title="voucher failed again", note="voucher failed")
    b = item("B-1", "voucher repaired")
    b["status"] = "closed"
    message = related.confirm(a, b, "earlier-attempt", "Same voucher job failed again after its logged repair", "w")
    assert "did not hold" in message
    saved = q.get(a["ref"])
    assert saved["depth"] == "deep"
    assert related.links(saved)[0]["ref"] == "B-1"
    assert related.confirm(saved, b, "earlier-attempt", "evidence", "w") == "Link already recorded"


def test_fragile_jobs_only_confirmed_same_job_three_in_seven_days():
    its = [item(f"OPS-{n}", "voucher job") for n in (1,2,3)]
    for a,b in zip(its[1:],its):
        a["history"] = [dict(text=related.LINK_MARKER+json.dumps(dict(ref=b["ref"],kind="earlier-attempt")), at=iso(time.time()))]
    rows = related.fragile_jobs(its)
    assert len(rows) == 1 and len(rows[0]["refs"]) == 3
    its[0]["created_at"] = iso(time.time()-8*related.DAY)
    assert related.fragile_jobs(its) == []


def test_feedback_ratio_no_retry_inflation():
    a = q.enqueue(project="A", title="voucher", note="voucher")
    related.feedback(a["ref"], "w", "useful")
    related.feedback(a["ref"], "w", "useful")
    report = related.feedback_report([q.get(a["ref"])])
    assert report["rated"] == 1 and report["useful_ratio"] == 1


def test_fragile_job_reaches_existing_digest_warning_channel(monkeypatch):
    from watchtower import owner_view
    its = [item(f"OPS-{n}", "voucher job") for n in (1,2,3)]
    for a,b in zip(its[1:],its):
        a["history"] = [dict(text=related.LINK_MARKER+json.dumps(dict(ref=b["ref"],kind="earlier-attempt")), at=iso(time.time()))]
    monkeypatch.setattr(q, "list_items", lambda **kw: its)
    view = owner_view.snapshot()
    assert len(view["fragile_jobs"]) == 1
    assert sum("Fragile job:" in w for w in view["warnings"]) == 1


def test_full_detail_retention_enriches_shallow_list(tmp_path):
    from watchtower import related_snapshots
    config.set_backend("GH", "github")
    config.set_github_repo("GH", "fixture/tickets")
    raw = dict(number=7, title="", body="voucher", state="CLOSED", labels=[],
               createdAt=iso(time.time()-100), updatedAt=iso(time.time()-10), closedAt=iso(time.time()-10))
    gh._GH_LIST_CACHE_FILE.write_text(json.dumps({"fixture/tickets:closed": {"at": time.time(), "data": [raw]}}))
    related_snapshots.remember("fixture/tickets", dict(raw, comments=[{"body": "comments-only booking recovery"}]))
    items, _ = related.cached_tickets()
    assert "comments-only booking recovery" in related.full_text(items[0])
    # Newer body supersedes detail, retaining known discussion without false freshness.
    raw["updatedAt"] = iso(time.time())
    raw["body"] = "newer body"
    gh._GH_LIST_CACHE_FILE.write_text(json.dumps({"fixture/tickets:closed": {"at": time.time(), "data": [raw]}}))
    items, coverage = related.cached_tickets()
    assert items[0]["text"] == "newer body"
    assert "comments-only booking recovery" in related.full_text(items[0])
    assert not next(c for c in coverage if c["source"].endswith(":closed"))["comments_complete"]


def test_recent_commit_search_uses_existing_fts_and_deduplicates_clones(tmp_path, monkeypatch):
    p = tmp_path / "ship.db"
    monkeypatch.setenv("WATCHTOWER_SHIP_GRAPH", str(p))
    with sqlite3.connect(p) as conn:
        conn.executescript("""CREATE TABLE commits(commit_id TEXT PRIMARY KEY,repo TEXT,hash TEXT,
            subject TEXT,on_main INTEGER,ts REAL);
            CREATE VIRTUAL TABLE commits_fts USING fts5(commit_id UNINDEXED,subject,tokenize='porter unicode61');
            CREATE VIRTUAL TABLE commits_vocab USING fts5vocab(commits_fts,'row');""")
        for n,ts in ((1,time.time()),(2,time.time()),(3,time.time()-40*related.DAY)):
            sha = "a"*40 if n<3 else "b"*40
            conn.execute("INSERT INTO commits VALUES(?,?,?,?,?,?)", (str(n),f"repo{n}",sha,"Voucher recovery",1,ts))
            conn.execute("INSERT INTO commits_fts VALUES(?,?)", (str(n),"Voucher recovery"))
    hits = related.commit_hits(item("A-1", "voucher recovery"))["hits"]
    assert len(hits) == 1 and hits[0]["commit"] == "a"*40


def test_jev_large_default_list_batches_and_labels_partial_context(monkeypatch):
    monkeypatch.setenv("TYPESAFE_API_KEY", "fixture-key")
    sizes = []
    class Response:
        def __init__(self, answers): self.answers=answers
        def __enter__(self): return self
        def __exit__(self, *a): pass
        def read(self): return json.dumps(dict(answers=self.answers,usage={"input_tokens": 1})).encode()
    def request(req, timeout):
        sizes.append(len(req.data))
        data=json.loads(req.data)
        assert len(req.data) < 32000
        return Response({k: {"choice": "unrelated", "confidence": .7} for k in data["questions"]})
    monkeypatch.setattr(related.urllib.request,"urlopen",request)
    hits=[dict(ref=f"B-{n}", status="closed", excerpt="matching voucher evidence",
               item=item(f"B-{n}", "voucher "*18000 + "שלום "*18000)) for n in range(10)]
    result=related.triage(item("A-1","voucher "*3000),hits)
    assert len(sizes)>1 and result["requests"]==len(sizes)
    assert all(h["judgment"]["partial_context"] for h in hits)


def test_shared_github_repo_custom_labels_and_catchall():
    for name in ("A", "B"):
        config.set_backend(name,"github")
        config.set_github_repo(name,"fixture/tickets")
    config.set_queue_label("A","priority-fixes")
    config.set_queue_label("B", config.CATCH_ALL_LABEL)
    base=dict(body="voucher",state="OPEN",title="",createdAt=iso(time.time()),updatedAt=iso(time.time()))
    rows=[dict(base,number=1,labels=[{"name":"priority-fixes"}]),dict(base,number=2,labels=[])]
    gh._GH_LIST_CACHE_FILE.write_text(json.dumps({"fixture/tickets:open":{"data":rows}}))
    items,_=related.cached_tickets()
    assert {i["ref"] for i in items} == {"A-1", "B-2"}


def test_parser_and_json_claim_include_related(wt_env, monkeypatch, capsys):
    from watchtower import cli
    monkeypatch.setattr(cli, "_rename_claiming_session", lambda *a: None)
    monkeypatch.setattr(cli, "_start_plan_stage", lambda i: i)
    q.enqueue(project="A", title="vouchers", note="voucher recovery")
    it = q.enqueue(project="B", title="vouchers fail", note="voucher recovery")
    assert cli.main(["claim", "-q", "B", it["ref"], "--worker", "test-worker", "--json"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["related"]["hits"][0]["ref"] == "A-1"
    assert "item" not in out["related"]["hits"][0]

import subprocess

from watchtower import deploy


alerts = []


def _g(cwd, *a):
    subprocess.run(["git", "-C", str(cwd), *a], check=True, capture_output=True)


def _setup(tmp_path, monkeypatch):
    monkeypatch.setattr(deploy, "STATE_FILE", tmp_path / "deploy-state.json")
    monkeypatch.setattr(deploy, "_alert", lambda st, reason: alerts.append(reason) or "WT-X")
    origin, dev, inst = (tmp_path / n for n in ("origin.git", "dev", "inst"))
    _g(tmp_path, "init", "-q", "--bare", "-b", "main", str(origin))
    _g(tmp_path, "clone", "-q", str(origin), str(dev))
    for n in ("user.email", "user.name"):
        _g(dev, "config", n, "t")
    (dev / "a").write_text("1")
    _g(dev, "add", "a"); _g(dev, "commit", "-qm", "1")
    _g(dev, "push", "-q", "origin", "HEAD:main")
    _g(tmp_path, "clone", "-q", str(origin), str(inst))
    monkeypatch.setattr(deploy, "installed_root", lambda: str(inst))
    return origin, dev, inst


def _land(dev):
    (dev / "a").write_text("2")
    _g(dev, "commit", "-qam", "2"); _g(dev, "push", "-q", "origin", "main")


def test_sync_fast_forwards_clean_tree(tmp_path, monkeypatch):
    _, dev, inst = _setup(tmp_path, monkeypatch)
    assert deploy.sync()["action"] == "up-to-date"
    _land(dev)
    assert deploy.status()["behind"] == 1
    assert deploy.sync()["action"] == "moved"
    assert (inst / "a").read_text() == "2"


def test_sync_refuses_dirty_tree(tmp_path, monkeypatch):
    _, dev, inst = _setup(tmp_path, monkeypatch)
    _land(dev)
    (inst / "a").write_text("wip")
    res = deploy.sync()
    assert res["action"] == "refused" and "a" in res["reason"]
    assert (inst / "a").read_text() == "wip"


def test_stale_revert_is_restored_and_synced(tmp_path, monkeypatch):
    _, dev, inst = _setup(tmp_path, monkeypatch)
    _land(dev)  # origin a: 1 -> 2
    _g(inst, "fetch", "-q")
    # installed tree is at "1"; a stale dirty file replays an old blob
    (dev / "a").write_text("3"); _g(dev, "commit", "-qam", "3"); _g(dev, "push", "-q", "origin", "main")
    (inst / "a").write_text("2")  # content of a past origin commit, not HEAD
    res = deploy.sync()
    assert res["action"] == "moved" and res["restored"] == ["a"]
    assert (inst / "a").read_text() == "3"
    assert deploy.warning_line() == ""


def test_novel_edit_refuses_loudly_and_alerts_once(tmp_path, monkeypatch):
    alerts.clear()
    _, dev, inst = _setup(tmp_path, monkeypatch)
    _land(dev)
    (inst / "a").write_text("brand new work")
    assert deploy.sync()["action"] == "refused"
    assert deploy.sync()["action"] == "refused"
    assert len(alerts) == 1
    assert "1 behind" in deploy.warning_line() and "refused" in deploy.warning_line()
    assert (inst / "a").read_text() == "brand new work"
    (inst / "a").write_text("1")
    assert deploy.sync()["action"] == "moved"
    assert deploy.warning_line() == ""

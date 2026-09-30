import subprocess

from watchtower import deploy


def _g(cwd, *a):
    subprocess.run(["git", "-C", str(cwd), *a], check=True, capture_output=True)


def _setup(tmp_path, monkeypatch):
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

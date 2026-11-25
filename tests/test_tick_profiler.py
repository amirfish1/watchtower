"""WT-113: an opt-in cProfile of the daemon's own reconcile ticks."""


def test_tick_profiler_profiles_requested_ticks_then_stops(wt_env):
    from watchtower import cli, storage

    prof = cli._TickProfiler()
    prof.tick()
    assert prof.prof is None  # not requested: nothing runs

    req = storage.data_dir() / "profile-ticks"
    out = storage.data_dir() / "profile-ticks.out"
    req.parent.mkdir(parents=True, exist_ok=True)
    req.write_text("2\n")

    def busy_tick_work():
        return sum(i * i for i in range(2000))

    prof.tick()  # starts profiling and consumes the request
    assert prof.prof is not None and not req.exists()
    busy_tick_work()
    prof.tick()
    assert not out.exists()  # one of two ticks done
    busy_tick_work()
    prof.tick()
    report = out.read_text()
    assert report.startswith("2 loop iteration(s)")
    assert "busy_tick_work" in report
    assert (storage.data_dir() / "profile-ticks.prof").stat().st_size > 0
    assert prof.prof is None


def test_tick_profiler_never_raises(wt_env, monkeypatch):
    from watchtower import cli, storage

    prof = cli._TickProfiler()
    (storage.data_dir()).mkdir(parents=True, exist_ok=True)
    (storage.data_dir() / "profile-ticks").write_text("1")
    prof.tick()
    monkeypatch.setattr(storage, "data_dir", lambda: (_ for _ in ()).throw(OSError("gone")))
    prof.tick()  # writing the report fails: logged, not raised
    assert prof.prof is None

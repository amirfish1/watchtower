"""CPU-weighted sampler (WT-122): busy threads get CPU, idle threads none."""
import threading
import time

from watchtower import cpu_sampler


def _busy(seconds):
    end = time.time() + seconds
    while time.time() < end:
        sum(range(500))


def test_sampler_charges_busy_threads_not_idle_ones():
    hot = threading.Thread(target=_busy, args=(1.2,), name="hot")
    cold = threading.Thread(target=time.sleep, args=(1.2,), name="cold")
    hot.start()
    cold.start()
    # A short-lived thread started mid-run is charged from its creation.
    late = threading.Timer(0.2, _busy, args=(0.3,))
    late.name = "late"
    late.start()
    result = cpu_sampler.sample(1.0, interval=0.01)
    for t in (hot, cold, late):
        t.join()
    by_thread = {}
    for (label, _stack), cpu in result["stacks"].items():
        by_thread[label] = by_thread.get(label, 0.0) + cpu
    assert by_thread.get("hot", 0) > 0.2
    assert by_thread.get("late", 0) > 0.05
    assert by_thread.get("cold", 0) < 0.02
    text = cpu_sampler.render(result, top=5)
    assert "by thread" in text and "test_cpu_sampler:_busy" in text
    assert "test_cpu_sampler:_busy" in cpu_sampler.folded(result)


def test_maybe_start_consumes_request(tmp_path, monkeypatch):
    from watchtower import storage
    monkeypatch.setattr(storage, "data_dir", lambda: tmp_path)
    assert cpu_sampler.maybe_start() is False
    (tmp_path / "profile-cpu").write_text("1")
    assert cpu_sampler.maybe_start() is True
    assert not (tmp_path / "profile-cpu").exists()
    deadline = time.time() + 10
    while cpu_sampler._running.is_set() and time.time() < deadline:
        time.sleep(0.05)
    assert (tmp_path / "profile-cpu.out").read_text().startswith("wall ")
    assert (tmp_path / "profile-cpu.folded").exists()

"""Daemon-only memo TTLs never outlive the test that enabled them (OPS-1126).

The daemon loop turns on process-global memo TTLs. Tests that drive the loop
in-process (test_chat_cli, test_dispatch_audit) used to leave them on, and the
next GitHub test inside the 1 s window was served the previous test's pipeline
sidecar: 147 order- and timing-dependent failures in the daily release run.
The two tests below run in definition order.
"""
import importlib

from watchtower import cli


def _ttls():
    return {f"{m}.{a}": getattr(importlib.import_module(f"watchtower.{m}"), a)
            for m, a, _ in cli.DAEMON_MEMO_TTLS}


def test_daemon_enables_every_listed_memo_ttl():
    cli._enable_daemon_memo_ttls()
    assert _ttls() == {f"{m}.{a}": ttl for m, a, ttl in cli.DAEMON_MEMO_TTLS}
    assert all(ttl > 0 for ttl in _ttls().values())


def test_the_next_test_starts_with_every_memo_ttl_off():
    assert _ttls() and set(_ttls().values()) == {0}

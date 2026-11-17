"""Seed pre-WT-89 records to test compatibility readers and recovery.

These are fixture writes, never a supported way to create new live waits.
New writer rejection is exercised separately in test_person_blocks.
"""
from unittest.mock import patch


def block(q, *args, **kwargs):
    with patch.object(q, "validate_person_block", lambda on, *a, **k: on):
        return q.block(*args, **kwargs)


def set_wait(q, *args, **kwargs):
    with patch.object(q, "validate_person_block", lambda on, *a, **k: on):
        return q.set_wait(*args, **kwargs)

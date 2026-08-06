# Copyright (c) 2026 Amir Fish. All rights reserved.
# SPDX-License-Identifier: LicenseRef-WatchTower-Software-License

"""Shared boot-death verification for freshly spawned resume children."""

from __future__ import annotations

import math
import os
import time
from typing import Any, Optional, Tuple

_DEFAULT_VERIFY_S = 2.0


def verify_window_s() -> float:
    """Return the positive, finite resume-child verification window."""
    try:
        value = float(
            os.environ.get("WATCHTOWER_RESUME_VERIFY_S", str(_DEFAULT_VERIFY_S))
        )
    except ValueError:
        return _DEFAULT_VERIFY_S
    return value if math.isfinite(value) and value > 0 else _DEFAULT_VERIFY_S


def verify_resume_child(proc: Any) -> Tuple[bool, Optional[int]]:
    """Poll through the boot window, returning whether the child died and rc."""
    deadline = time.monotonic() + verify_window_s()
    poll = getattr(proc, "poll", None)
    while callable(poll):
        rc = poll()
        if rc is not None:
            return True, rc
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(0.1, remaining))
    return False, None

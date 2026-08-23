# Copyright (c) 2026 Amir Fish. All rights reserved.
# SPDX-License-Identifier: LicenseRef-WatchTower-Software-License

"""WatchTower — a queue-focused tool for running fleets of AI coding-agent
workers and knowing which queues are stuck.

CLI-first (binary ``wt``); the queue engine in :mod:`watchtower.queue` is
self-contained and stdlib-only. An HTTP viewer arrives in phase 2.
"""

__version__ = "0.5.0"

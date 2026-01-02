#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — WS Switchboard Bot Shim

Why this exists
---------------
Older supervisor paths start the WS switchboard from app.bots.ws_switchboard.

We now want EXACTLY ONE WS implementation (label-safe buses, windows-safe writes)
living in app.core.ws_switchboard.

So this file is a thin wrapper that forwards to the core switchboard.

This prevents:
- duplicate WS implementations
- conflicting file writers
- Windows rename collisions on *.tmp -> real files
"""

from __future__ import annotations

import logging


def _configure_logging() -> None:
    root_logger = logging.getLogger()
    if root_logger.handlers:
        return
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    )


def main() -> None:
    _configure_logging()

    # Single source of truth: core switchboard
    from app.core.ws_switchboard import main as core_main  # import at runtime

    core_main()


def loop() -> None:
    # Backwards compatible alias
    main()


if __name__ == "__main__":
    main()

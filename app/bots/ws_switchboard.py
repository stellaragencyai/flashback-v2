#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — WS Switchboard bot wrapper v4.3

Thin wrapper so we can run the WS switchboard as:

    python -m app.bots.ws_switchboard

All the real logic lives in app.core.ws_switchboard.
"""

from __future__ import annotations

from app.core.ws_switchboard import main as core_ws_main


def main() -> None:
    """
    Delegate to the core ws_switchboard.main().

    This keeps all behavior and configuration in a single place
    (app.core.ws_switchboard) while giving the supervisor and
    CLI a stable entrypoint under app.bots.
    """
    core_ws_main()


if __name__ == "__main__":
    main()

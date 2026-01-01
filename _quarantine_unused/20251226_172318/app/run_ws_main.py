#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — WS launcher for MAIN account

This is a thin wrapper around app.ws.ws_switchboard.main(),
with ACCOUNT_LABEL hard-bound to "main".
"""

import os

from app.ws.ws_switchboard import main as run_ws_switchboard


def main() -> None:
    # Hard-bind this process to the MAIN unified account
    os.environ["ACCOUNT_LABEL"] = "main"
    run_ws_switchboard()


if __name__ == "__main__":
    main()

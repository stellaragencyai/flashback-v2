#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — WS launcher for flashback05
"""

import os

from app.ws.ws_switchboard import main as run_ws_switchboard


def main() -> None:
    os.environ["ACCOUNT_LABEL"] = "flashback05"
    run_ws_switchboard()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DEPRECATED ADAPTER — app.ws.ws_switchboard

This file exists only to preserve legacy imports.
Canonical implementation lives in:
  - app.core.ws_switchboard (logic)
  - app.bots.ws_switchboard (stable entrypoint wrapper)

Do not add logic here.
"""

from __future__ import annotations

from app.core.ws_switchboard import main

__all__ = ["main"]

if __name__ == "__main__":
    main()

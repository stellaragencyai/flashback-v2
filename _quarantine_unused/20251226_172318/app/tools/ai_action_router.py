#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Tools wrapper for AI Action Router

This is intentionally thin.
Supervisor may launch app.tools.ai_action_router, so we just call the bot adapter.
"""

from __future__ import annotations

from app.bots.ai_action_router import loop

def main() -> None:
    loop()

if __name__ == "__main__":
    main()

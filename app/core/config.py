#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Core Config (app.core.config)

Provides a central `settings` object with important paths and defaults.
"""

from __future__ import annotations

from pathlib import Path
import os


class Settings:
    def __init__(self) -> None:
        # app/core/config.py -> parents[0]=core, [1]=app, [2]=Flashback
        self.ROOT: Path = Path(__file__).resolve().parents[2]

        self.CONFIG_DIR: Path = self.ROOT / "config"
        self.STATE_DIR: Path = self.ROOT / "state"

        self.STATE_DIR.mkdir(parents=True, exist_ok=True)

        db_env = os.getenv("DB_PATH")
        if db_env:
            self.DB_PATH: Path = Path(db_env)
        else:
            self.DB_PATH: Path = self.STATE_DIR / "flashback.db"


settings = Settings()

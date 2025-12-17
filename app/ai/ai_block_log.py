#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict

import orjson

BLOCK_LOG = Path("state/ai_blocks.jsonl")
BLOCK_LOG.parent.mkdir(parents=True, exist_ok=True)

def _now_ms() -> int:
    return int(time.time() * 1000)

def log_block(payload: Dict[str, Any]) -> None:
    try:
        row = dict(payload)
        row.setdefault("ts_ms", _now_ms())
        with BLOCK_LOG.open("ab") as f:
            f.write(orjson.dumps(row))
            f.write(b"\n")
    except Exception:
        return

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Snapshot Dumper (v1)

Purpose
-------
Convenience tool to inspect the current AI state snapshot for ONE account:

    - Calls app.core.ai_state_bus.build_ai_snapshot().
    - Writes a pretty-printed JSON file to:
          state/ai_snapshot_debug.json
    - Prints a short console summary:
          * snapshot_version
          * snapshot_health.status
          * bus ages

Usage
-----
From project root:

    python tools/dump_ai_snapshot.py

Notes
-----
- This is read-only: it does not mutate any state.
- Assumes ACCOUNT_LABEL / env are already configured as usual.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

from app.core.config import settings
from app.core.ai_state_bus import build_ai_snapshot


ROOT: Path = settings.ROOT
STATE_DIR: Path = settings.STATE_DIR
OUT_PATH: Path = STATE_DIR / "ai_snapshot_debug.json"


def main() -> int:
    print("=== Flashback AI Snapshot Dumper ===")
    print(f"ROOT:      {ROOT}")
    print(f"STATE_DIR: {STATE_DIR}")
    print(f"OUT_PATH:  {OUT_PATH}")
    print("")

    snap: Dict[str, Any] = build_ai_snapshot(
        focus_symbols=None,
        include_trades=False,
        trades_limit=0,
        include_orderbook=True,
    )

    version = snap.get("snapshot_version")
    health = snap.get("snapshot_health") or {}
    status = health.get("status")
    pos_age = health.get("positions_age_sec")
    ob_age = health.get("orderbook_age_sec")
    tr_age = health.get("trades_age_sec")

    print(f"snapshot_version: {version}")
    print(f"snapshot_status : {status}")
    print(f"positions_age_s : {pos_age}")
    print(f"orderbook_age_s : {ob_age}")
    print(f"trades_age_s    : {tr_age}")
    print("")

    try:
        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with OUT_PATH.open("w", encoding="utf-8") as f:
            json.dump(snap, f, indent=2, sort_keys=True)
    except Exception as exc:
        print(f"[ERROR] Failed to write snapshot to {OUT_PATH}: {exc}")
        return 1

    print(f"[OK] Snapshot written to {OUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

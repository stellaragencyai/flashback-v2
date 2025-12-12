#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Flashback — WS Health Check

What it does:
- Watches the WS-fed state files for freshness:
    state/positions_bus.json
    state/orderbook_bus.json
    state/trades_bus.json
    state/ws_switchboard_heartbeat_<ACCOUNT_LABEL>.txt

- Prints age seconds for each.
- Exits non-zero if any critical bus is stale.

Why this matters:
- If these buses aren't fresh, everything downstream (TP/SL, outcomes, AI)
  is reading dead air and you’ll chase phantom bugs forever.
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from typing import Optional

try:
    from app.core.config import settings
    ROOT: Path = settings.ROOT  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[2]


STATE_DIR = ROOT / "state"
POSITIONS = STATE_DIR / "positions_bus.json"
ORDERBOOK = STATE_DIR / "orderbook_bus.json"
TRADES = STATE_DIR / "trades_bus.json"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _read_json_updated_ms(path: Path) -> Optional[int]:
    try:
        if not path.exists():
            return None
        import json
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return None
        v = data.get("updated_ms")
        return int(v) if v is not None else None
    except Exception:
        return None


def _file_age_sec(path: Path) -> Optional[float]:
    try:
        if not path.exists():
            return None
        mtime = path.stat().st_mtime
        return float(time.time() - mtime)
    except Exception:
        return None


def _age_from_updated_ms(updated_ms: Optional[int]) -> Optional[float]:
    if not updated_ms or updated_ms <= 0:
        return None
    return (_now_ms() - updated_ms) / 1000.0


def main() -> int:
    account_label = os.getenv("ACCOUNT_LABEL", "main")
    hb = STATE_DIR / f"ws_switchboard_heartbeat_{account_label}.txt"

    pos_age = _age_from_updated_ms(_read_json_updated_ms(POSITIONS))
    ob_age = _age_from_updated_ms(_read_json_updated_ms(ORDERBOOK))
    tr_age = _age_from_updated_ms(_read_json_updated_ms(TRADES))
    hb_age = _file_age_sec(hb)

    def fmt(x: Optional[float]) -> str:
        return "MISSING" if x is None else f"{x:.2f}s"

    print("\n=== WS HEALTH CHECK ===")
    print(f"ACCOUNT_LABEL: {account_label}")
    print(f"positions_bus.json  age: {fmt(pos_age)}")
    print(f"orderbook_bus.json  age: {fmt(ob_age)}")
    print(f"trades_bus.json     age: {fmt(tr_age)}")
    print(f"heartbeat file      age: {fmt(hb_age)}")

    # Guardrails (tuneable via env)
    max_pos = float(os.getenv("WS_MAX_POS_AGE_SEC", "30"))
    max_ob  = float(os.getenv("WS_MAX_OB_AGE_SEC", "10"))
    max_tr  = float(os.getenv("WS_MAX_TRADES_AGE_SEC", "10"))
    max_hb  = float(os.getenv("WS_MAX_HB_AGE_SEC", "60"))

    failures = []
    if pos_age is None or pos_age > max_pos:
        failures.append(f"positions stale ({fmt(pos_age)} > {max_pos}s)")
    if ob_age is None or ob_age > max_ob:
        failures.append(f"orderbook stale ({fmt(ob_age)} > {max_ob}s)")
    if tr_age is None or tr_age > max_tr:
        failures.append(f"trades stale ({fmt(tr_age)} > {max_tr}s)")
    if hb_age is None or hb_age > max_hb:
        failures.append(f"heartbeat stale ({fmt(hb_age)} > {max_hb}s)")

    if failures:
        print("\nFAIL:")
        for f in failures:
            print(f" - {f}")
        return 2

    print("\nPASS ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

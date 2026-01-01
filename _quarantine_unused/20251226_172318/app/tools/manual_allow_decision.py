#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — manual allow decision writer (hardened)

- Writes a manual override ALLOW decision row to state/ai_decisions.jsonl
- Uses hardened ai_decision_logger when available (lock + dedupe + reject routing)
- Supports BOTH styles:
    A) positional:
       python app/tools/manual_allow_decision.py TRADE_ID "reason" [size_multiplier]
    B) flags:
       python app/tools/manual_allow_decision.py --trade-id X --account-label flashback02 --symbol BTCUSDT --timeframe 5m --reason "ok" --size-multiplier 1.0
"""

from __future__ import annotations

import sys
import json
import time
from pathlib import Path


try:
    from app.core.config import settings
    ROOT = settings.ROOT
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

DECISIONS_PATH = ROOT / "state" / "ai_decisions.jsonl"
DECISIONS_PATH.parent.mkdir(parents=True, exist_ok=True)

# --- manual_allow: hardened logger (lock + dedupe + reject routing) ---
try:
    from app.core.ai_decision_logger import append_decision as _append_decision_logged  # type: ignore
except Exception:
    _append_decision_logged = None  # type: ignore


def _get_flag(name: str, default: str = "") -> str:
    try:
        if name not in sys.argv:
            return default
        i = sys.argv.index(name)
        if i + 1 >= len(sys.argv):
            return default
        return str(sys.argv[i + 1]).strip()
    except Exception:
        return default


def _has_any_flag() -> bool:
    for f in ("--trade-id", "--reason", "--account-label", "--symbol", "--timeframe", "--size-multiplier"):
        if f in sys.argv:
            return True
    return False


def main() -> int:
    # Flag mode
    if _has_any_flag():
        trade_id = _get_flag("--trade-id", "").strip()
        reason = _get_flag("--reason", "manual_allow").strip() or "manual_allow"
        account_label = _get_flag("--account-label", "").strip()
        symbol = _get_flag("--symbol", "").strip().upper()
        timeframe = _get_flag("--timeframe", "").strip()
        sm_raw = _get_flag("--size-multiplier", "").strip()

        size_multiplier = None
        if sm_raw != "":
            try:
                size_multiplier = float(sm_raw)
            except Exception:
                size_multiplier = None

    # Positional mode (backward compatible)
    else:
        if len(sys.argv) < 3:
            print('usage:')
            print('  python app/tools/manual_allow_decision.py TRADE_ID "reason" [size_multiplier]')
            print('  python app/tools/manual_allow_decision.py --trade-id X --account-label flashback02 --symbol BTCUSDT --timeframe 5m --reason "ok" --size-multiplier 1.0')
            return 2

        trade_id = str(sys.argv[1]).strip()
        reason = str(sys.argv[2]).strip() or "manual_allow"
        account_label = ""
        symbol = ""
        timeframe = ""

        size_multiplier = None
        if len(sys.argv) >= 4:
            raw = str(sys.argv[3]).strip()
            if raw != "":
                try:
                    size_multiplier = float(raw)
                except Exception:
                    size_multiplier = None

    row = {
        "ts_ms": int(time.time() * 1000),
        "event_type": "ai_decision",
        "trade_id": trade_id,
        "allow": True,
        "decision_code": "ALLOW_TRADE",
        "reason": reason,
        "extra": {"stage": "manual_override"},
    }

    # Always include context when provided (makes enforcement + audits deterministic)
    if account_label:
        row["account_label"] = account_label
    if symbol:
        row["symbol"] = symbol
    if timeframe:
        row["timeframe"] = timeframe

    if size_multiplier is not None:
        row["size_multiplier"] = float(size_multiplier)

    if _append_decision_logged is not None:
        _append_decision_logged(row)
    else:
        with DECISIONS_PATH.open("ab") as f:
            f.write(json.dumps(row, ensure_ascii=False).encode("utf-8") + b"\n")

    print(
        "allow_written",
        trade_id,
        reason,
        ("size_multiplier=" + str(size_multiplier) if size_multiplier is not None else "size_multiplier=None"),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

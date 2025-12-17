#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Manual Block Decision Tool

What this is:
- A tiny utility to inject a deterministic BLOCK decision row into state/ai_decisions.jsonl.
- Usable both as:
    1) importable function: write_manual_block(...)
    2) CLI script: python app/tools/manual_block_decision.py TRADE_ID [SYMBOL] [ACCOUNT_LABEL]

Why:
- Lets us hard-test decision gating in executor_v2 without waiting for AI-pilot logic.
- Writes through app.core.ai_decision_logger.append_decision() so we inherit rotation + dedupe.
"""

from __future__ import annotations

import sys
import time
from typing import Any, Dict, Optional

from app.core.ai_decision_logger import append_decision


def write_manual_block(
    trade_id: str,
    *,
    symbol: str = "BTCUSDT",
    account_label: str = "main",
    timeframe: str = "5m",
    side: str = "buy",
    reason: str = "manual_block",
    decision_code: str = "BLOCK_TRADE",
) -> Dict[str, Any]:
    """
    Write a deterministic BLOCK decision row.

    Returns the row dict (for debugging / printing).
    """
    tid = (trade_id or "").strip()
    if not tid:
        raise ValueError("trade_id is required")  # caller can handle; CLI prints cleanly

    row: Dict[str, Any] = {
        "ts_ms": int(time.time() * 1000),
        "event_type": "ai_decision",
        "trade_id": tid,
        "client_trade_id": tid,
        "symbol": (symbol or "BTCUSDT").strip(),
        "account_label": (account_label or "main").strip(),
        "sub_uid": None,
        "strategy_id": "manual",
        "strategy_name": "manual",
        "timeframe": (timeframe or "5m").strip(),
        "side": (side or "buy").strip(),
        "mode": "PAPER",
        "allow": False,
        "decision_code": (decision_code or "BLOCK_TRADE").strip(),
        "reason": (reason or "manual_block").strip(),
        "ai_score": None,
        "tier_used": None,
        "gates_reason": None,
        "memory_id": None,
        "memory_score": None,
        "size_multiplier": 0.0,
    }

    # Writes to state/ai_decisions.jsonl with rotation + dedupe.
    append_decision(row)
    return row


def main(argv: Optional[list[str]] = None) -> int:
    argv = list(argv or sys.argv[1:])

    if not argv:
        print("usage: python app/tools/manual_block_decision.py TRADE_ID [SYMBOL] [ACCOUNT_LABEL]")
        return 2

    trade_id = argv[0]
    symbol = argv[1] if len(argv) >= 2 else "BTCUSDT"
    account_label = argv[2] if len(argv) >= 3 else "main"

    try:
        row = write_manual_block(trade_id, symbol=symbol, account_label=account_label)
    except Exception as e:
        print(f"error: {e}")
        return 1

    print("blocked_written", row.get("trade_id"), row.get("symbol"), row.get("account_label"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

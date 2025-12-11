#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Exec Signal Schema

Canonical schema for "execution signals" produced by higher-level logic
(AI action router, signal engine, etc.) and consumed by an executor
(e.g. executor_v2).

This is the LAST abstraction boundary before anything talks to Bybit.

Key idea:
    - AI / strategy layers work with AIAction / setups / outcomes.
    - This layer works with normalized, concrete "do X" instructions
      that an executor can safely interpret.

This module does NOT place orders. It just defines the shape of
execution signals.
"""

from __future__ import annotations

from typing import TypedDict, Literal, Optional, Dict, Any, List


ExecSide = Literal[
    "buy",
    "sell",
]

ExecAction = Literal[
    "open",
    "add",
    "reduce",
    "close",
    "close_all_symbol",
    "close_all_account",
]


ExecOrderType = Literal[
    "limit",
    "market",
    "post_only_limit",
]


TimeInForce = Literal[
    "GTC",   # good till cancel
    "IOC",   # immediate or cancel
    "FOK",   # fill or kill
]


class ExecSignal(TypedDict, total=False):
    """
    Execution signal record.

    These are written to a JSONL queue (e.g. state/exec_signals.jsonl)
    and then consumed by an executor process.

    REQUIRED for trade-bearing signals:
        - ts_ms
        - account_label
        - symbol
        - side
        - action
        - qty
        - order_type
    """
    # Identity / routing
    ts_ms: int
    account_label: str           # e.g. "main", "flashback01"
    signal_id: str               # unique ID for this exec signal
    source: str                  # e.g. "ai_router", "signal_engine"

    # Core instruction
    symbol: str                  # "BTCUSDT"
    side: ExecSide               # buy / sell
    action: ExecAction           # open / add / reduce / close / close_all_*

    # Sizing & price
    qty: float                   # contract size / units
    order_type: ExecOrderType    # limit / market / post_only_limit
    time_in_force: TimeInForce   # GTC / IOC / FOK
    price: Optional[float]       # limit price (None for pure market)
    sl_price: Optional[float]    # optional stop-loss hint
    tp_price: Optional[float]    # optional take-profit hint

    # Strategy / AI linkage
    ai_action_id: Optional[str]  # link back to AIAction if applicable
    strategy_role: Optional[str] # e.g. "trend_follow", "breakout"
    tags: List[str]

    # Safety / mode flags
    dry_run: bool                # if True, executor MUST NOT send real orders
    reduce_only: Optional[bool]
    post_only: Optional[bool]

    # Freeform metadata
    extra: Dict[str, Any]


REQUIRED_EXEC_FIELDS = [
    "ts_ms",
    "account_label",
    "symbol",
    "side",
    "action",
    "qty",
    "order_type",
]


def missing_exec_fields(signal: Dict[str, Any]) -> Dict[str, bool]:
    """
    Return dict of {field_name: True} for each required field missing
    from an ExecSignal candidate.
    """
    missing = {}
    for key in REQUIRED_EXEC_FIELDS:
        if key not in signal or signal.get(key) in (None, ""):
            missing[key] = True
    return missing

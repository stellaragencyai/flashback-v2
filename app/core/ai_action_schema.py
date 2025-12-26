#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Action Schema

Canonical schema for AI actions written to state/ai_actions.jsonl.

This is the SINGLE source of truth for what an "AI action" should look like.

Notes
-----
- Not every field is required for every action_type.
- But ANY trade-bearing action MUST at least define:
    • account_label
    • type
    • symbol
    • side
    • trade_id        (canonical join key for the learning loop)
    • risk_R
    • expected_R

- We also allow "heartbeat" / "noop" style actions which may omit symbol/side,
  but they should still identify the account_label and type.
"""

from __future__ import annotations

from typing import TypedDict, Literal, Optional, Dict, Any, List


ActionType = Literal[
    "open",         # propose opening a new position
    "add",          # add to existing position
    "reduce",       # partially close an existing position
    "close",        # close position on the symbol
    "close_all",    # emergency: close all positions for account_label
    "hold",         # explicit "no action" but meaningful decision
    "heartbeat",    # simple liveness ping
    "noop",         # filler / discarded
]

SideType = Literal[
    "long",
    "short",
    "flat",
]

RiskMode = Literal[
    "R",            # risk expressed in R-multiples
    "notional_pct", # percentage of equity
    "qty",          # raw quantity (fallback)
]


TRADE_BEARING_TYPES = {"open", "add", "reduce", "close", "close_all", "hold"}


class AIAction(TypedDict, total=False):
    """
    Canonical AI action record.

    This is what ai_pilot SHOULD emit and ai_action_router SHOULD consume.
    """
    # Timestamps / identity
    ts_ms: int
    account_label: str           # e.g. "main", "flashback01"
    action_id: str               # optional unique ID for deduping

    # Join keys (learning loop)
    trade_id: str                # canonical join key for Decision -> Action -> Outcome
    decision_id: Optional[str]   # optional if you emit it
    client_trade_id: Optional[str]
    source_trade_id: Optional[str]

    # Core decision
    type: ActionType             # open / add / reduce / close / close_all / hold / heartbeat / noop
    symbol: str                  # e.g. "BTCUSDT"
    side: SideType               # long / short / flat

    # Risk & sizing
    risk_mode: RiskMode          # R / notional_pct / qty
    risk_R: float                # "R" at stake (e.g. 1.0 = 1R risk)
    expected_R: float            # expected reward in R
    size_fraction: float         # e.g. 1.0 = full allowed size, 0.5 = half size

    # Prices (optional, can be hints)
    entry_hint: Optional[float]
    sl_hint: Optional[float]
    tp_hint: Optional[float]

    # Meta / reasoning
    confidence: Optional[float]  # 0..1 or 0..100 scaled, depending on your convention
    reason: str                  # brief human-readable summary
    tags: List[str]
    dry_run: bool                # true = do NOT execute real orders
    model_id: Optional[str]      # which AI model or profile handled this
    extra: Dict[str, Any]        # freeform metadata


REQUIRED_FOR_TRADE = [
    "account_label",
    "type",
    "symbol",
    "side",
    "trade_id",
    "risk_R",
    "expected_R",
]


def is_trade_bearing(action: Dict[str, Any]) -> bool:
    """
    Return True if this looks like a trade-bearing action (should have join keys + risk fields).
    """
    atype = str(action.get("type") or "").lower()
    return atype in TRADE_BEARING_TYPES


def is_heartbeat(action: Dict[str, Any]) -> bool:
    """
    Return True if this looks like a heartbeat / noop style action that
    does NOT represent a real trade-bearing decision.
    """
    atype = str(action.get("type") or "").lower()
    if atype in ("heartbeat", "noop"):
        return True

    # If literally nothing of interest is present, treat as heartbeat/noise.
    symbol = action.get("symbol")
    side = action.get("side")
    if not symbol and not side and not action.get("risk_R") and not action.get("expected_R"):
        return True

    return False


def missing_trade_fields(action: Dict[str, Any]) -> Dict[str, bool]:
    """
    Return a dict of {field_name: True} for each required field missing on a
    trade-bearing action.
    """
    missing: Dict[str, bool] = {}
    for key in REQUIRED_FOR_TRADE:
        if key not in action or action.get(key) in (None, ""):
            missing[key] = True
    return missing

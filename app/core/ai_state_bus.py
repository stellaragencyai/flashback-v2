#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI State Bus (WS-first account + market snapshot, hardened)

Purpose
-------
Provide AI / signal engines with a SINGLE, structured snapshot of:

- Account state:
    • equity_usdt, mmr_pct
    • tier, level
    • tier size cap %, max concurrent symbols

- Positions (per ACCOUNT_LABEL):
    • raw WS-fed rows (via position_bus)
    • by-symbol map for quick lookup
    • snapshot_age_sec  (freshness of positions_bus.json)

- Market data (WS-first where available):
    • last_price_ws_first
    • spread_bps_ws
    • orderbook snapshot (bids/asks trimmed)
    • recent public trades (optional)
    • orderbook / trades bus ages (if market_bus exposes them)

Everything is WS-first where possible and falls back gracefully.
"""

from __future__ import annotations

import time
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

# Core account / risk primitives & WS-first prices
from app.core.flashback_common import (
    get_equity_usdt,
    get_mmr_pct,
    tier_from_equity,
    cap_pct_for_tier,
    max_conc_for_tier,
    last_price_ws_first,
    spread_bps_ws,
    record_heartbeat,
)

# Positions via WS-first position_bus
from app.core.position_bus import (
    get_positions_for_current_label,
    get_position_map_for_label,
    get_snapshot as _pos_get_snapshot,
)

# Market bus (WS orderbook + trades); optional import for robustness
try:
    from app.core import market_bus as _market_bus  # type: ignore
except Exception:
    _market_bus = None  # type: ignore


CATEGORY = "linear"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _now_ms() -> int:
    return int(time.time() * 1000)


def _decimal_or_none(val: Any) -> Optional[Decimal]:
    if val is None:
        return None
    try:
        d = Decimal(str(val))
        return d
    except Exception:
        return None


def _decimal_to_str(d: Optional[Decimal]) -> Optional[str]:
    if d is None:
        return None
    return str(d)


def _account_state() -> Dict[str, Any]:
    """
    Return core account state (REST-driven, WS-agnostic).

    All numeric fields are stringified so downstream JSON is stable.
    """
    eq = get_equity_usdt()
    mmr = get_mmr_pct()
    tier, level = tier_from_equity(eq)
    cap_pct = cap_pct_for_tier(tier)
    max_conc = max_conc_for_tier(tier)

    return {
        "equity_usdt": str(eq),
        "mmr_pct": str(mmr),
        "tier": tier,
        "level": level,
        "tier_size_cap_pct": str(cap_pct),
        "tier_max_conc": max_conc,
    }


def _positions_state() -> Dict[str, Any]:
    """
    Return WS-first positions for current ACCOUNT_LABEL.

    Structure:
        {
          "raw": [ ... Bybit rows ... ],
          "by_symbol": { "BTCUSDT": {...}, ... },
          "snapshot_age_sec": float | None
        }
    """
    # 1) Try to get snapshot age
    snap_age_sec: Optional[float] = None
    try:
        snap, age = _pos_get_snapshot()
        if age is not None:
            snap_age_sec = float(age)
    except Exception:
        snap_age_sec = None

    # 2) WS-first rows / map with REST fallback (per position_bus rules)
    rows = get_positions_for_current_label(
        category=CATEGORY,
        max_age_seconds=None,
        allow_rest_fallback=True,
    )
    pos_map = get_position_map_for_label(
        label=None,
        category=CATEGORY,
        max_age_seconds=None,
        allow_rest_fallback=True,
    )

    # Normalize keys to UPPER symbols
    norm_map: Dict[str, Dict[str, Any]] = {}
    for k, v in pos_map.items():
        norm_map[str(k).upper()] = v

    return {
        "raw": rows,
        "by_symbol": norm_map,
        "snapshot_age_sec": snap_age_sec,
    }


def _market_bus_ages() -> Tuple[Optional[float], Optional[float]]:
    """
    Try to expose global orderbook / trades bus ages, if market_bus provides them.

    Returns (orderbook_bus_age_sec, trades_bus_age_sec).
    """
    if _market_bus is None:
        return None, None

    ob_age: Optional[float] = None
    tr_age: Optional[float] = None

    # Prefer explicit age helpers if present
    try:
        if hasattr(_market_bus, "orderbook_bus_age_sec"):
            ob_age_val = _market_bus.orderbook_bus_age_sec()  # type: ignore[attr-defined]
            if ob_age_val is not None:
                ob_age = float(ob_age_val)
    except Exception:
        ob_age = None

    try:
        if hasattr(_market_bus, "trades_bus_age_sec"):
            tr_age_val = _market_bus.trades_bus_age_sec()  # type: ignore[attr-defined]
            if tr_age_val is not None:
                tr_age = float(tr_age_val)
    except Exception:
        tr_age = None

    return ob_age, tr_age


def _symbol_market_block(
    symbol: str,
    *,
    include_orderbook: bool,
    include_trades: bool,
    trades_limit: int,
) -> Dict[str, Any]:
    """
    Build a per-symbol market snapshot block.

    Uses WS-first sources where possible, falls back gracefully when WS missing.
    """
    sym = symbol.upper()
    last_px = last_price_ws_first(sym)
    spread_bps_val = spread_bps_ws(sym)

    ob_block: Optional[Dict[str, Any]] = None
    trades_block: Optional[List[Dict[str, Any]]] = None
    ob_updated_ms: Optional[int] = None
    trades_updated_ms: Optional[int] = None

    if _market_bus is not None:
        try:
            if include_orderbook and hasattr(_market_bus, "get_orderbook_snapshot"):
                ob = _market_bus.get_orderbook_snapshot(sym)  # type: ignore[attr-defined]
                if isinstance(ob, dict):
                    # Trim depth for AI; they don't need 50 levels.
                    bids = ob.get("bids") or ob.get("b") or []
                    asks = ob.get("asks") or ob.get("a") or []
                    ob_block = {
                        "bids": bids[:10],
                        "asks": asks[:10],
                        "ts_ms": ob.get("ts_ms", 0),
                        "updated_ms": ob.get("updated_ms", 0),
                    }
                    ob_updated_ms = ob_block["updated_ms"]

            if include_trades and hasattr(_market_bus, "get_recent_trades"):
                trades = _market_bus.get_recent_trades(sym, limit=trades_limit)  # type: ignore[attr-defined]
                if isinstance(trades, list):
                    trades_block = trades
                # If market_bus exposes a single updated_ms, we use that
                if hasattr(_market_bus, "trades_bus_updated_ms"):
                    try:
                        trades_updated_ms = _market_bus.trades_bus_updated_ms()  # type: ignore[attr-defined]
                    except Exception:
                        trades_updated_ms = None
        except Exception:
            # If anything explodes, we just return what we already have.
            pass

    return {
        "symbol": sym,
        "last_price": str(last_px),
        "spread_bps": _decimal_to_str(spread_bps_val),
        "orderbook": ob_block,
        "trades": trades_block,
        "orderbook_updated_ms": ob_updated_ms,
        "trades_updated_ms": trades_updated_ms,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_symbol_state(
    symbol: str,
    *,
    include_trades: bool = True,
    trades_limit: int = 100,
    include_orderbook: bool = True,
) -> Dict[str, Any]:
    """
    Build a focused state snapshot for a SINGLE symbol:

    {
      "ts_ms": ...,
      "account": {...},
      "position": { ... or None ... },
      "market": {
          "symbol": "BTCUSDT",
          "last_price": "12345.6",
          "spread_bps": "3.2" or None,
          "orderbook": { "bids": [...], "asks": [...], ... } or None,
          "trades": [ ... ] or None,
          "orderbook_updated_ms": ...,
          "trades_updated_ms": ...,
      }
    }
    """
    record_heartbeat("ai_state_bus_symbol")
    sym = symbol.upper()

    account = _account_state()
    positions = _positions_state()
    pos_map = positions.get("by_symbol", {}) or {}
    pos = pos_map.get(sym)

    market = _symbol_market_block(
        sym,
        include_orderbook=include_orderbook,
        include_trades=include_trades,
        trades_limit=trades_limit,
    )

    return {
        "ts_ms": _now_ms(),
        "account": account,
        "position": pos,
        "market": market,
    }


def build_ai_snapshot(
    focus_symbols: Optional[List[str]] = None,
    *,
    include_trades: bool = False,
    trades_limit: int = 50,
    include_orderbook: bool = True,
) -> Dict[str, Any]:
    """
    Build a global AI snapshot for current ACCOUNT_LABEL.

    Parameters
    ----------
    focus_symbols : Optional[List[str]]
        If provided, only include these symbols in the `symbols` block.
        If None, we include:
          - all symbols with open positions.
    include_trades : bool
        If True, include recent public trades per symbol (up to trades_limit).
    trades_limit : int
        Max number of trades per symbol when include_trades=True.
    include_orderbook : bool
        If True, attach trimmed orderbook (bids/asks) for each symbol.

    Returns
    -------
    dict
        {
          "ts_ms": ...,
          "account": {...},
          "positions": {...},
          "symbols": {...},
          "positions_bus_age_sec": float | None,
          "orderbook_bus_age_sec": float | None,
          "trades_bus_age_sec": float | None,
        }
    """
    record_heartbeat("ai_state_bus_global")

    account = _account_state()
    positions = _positions_state()

    # Global bus ages for AI Pilot / policies
    pos_age_sec = positions.get("snapshot_age_sec")
    ob_age_sec, tr_age_sec = _market_bus_ages()

    # Determine which symbols to include in market view
    symbols_set = set()
    if focus_symbols:
        for s in focus_symbols:
            s_norm = str(s).upper().strip()
            if s_norm:
                symbols_set.add(s_norm)
    else:
        # Default: all open-position symbols
        for s in positions.get("by_symbol", {}).keys():
            symbols_set.add(str(s).upper())

    symbols_block: Dict[str, Dict[str, Any]] = {}
    for sym in sorted(symbols_set):
        symbols_block[sym] = _symbol_market_block(
            sym,
            include_orderbook=include_orderbook,
            include_trades=include_trades,
            trades_limit=trades_limit,
        )

    return {
        "ts_ms": _now_ms(),
        "account": account,
        "positions": positions,
        "symbols": symbols_block,
        "positions_bus_age_sec": pos_age_sec,
        "orderbook_bus_age_sec": ob_age_sec,
        "trades_bus_age_sec": tr_age_sec,
    }

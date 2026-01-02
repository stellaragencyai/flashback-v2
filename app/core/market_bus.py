#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Market Bus (WS-fed orderbook + trades)

Purpose
-------
Read the WS-fed JSON snapshots written by ws_switchboard:

  - state/orderbook_bus_<ACCOUNT_LABEL>.json   (preferred)
  - state/trades_bus_<ACCOUNT_LABEL>.json      (preferred)

Legacy fallback (main/older):
  - state/orderbook_bus.json
  - state/trades_bus.json

and expose simple helpers for:

  - Getting a per-symbol orderbook snapshot.
  - Getting recent public trades per symbol.
  - Getting last updated timestamps and ages for monitoring.

This is **read-only** and WS-first by design.
"""

from __future__ import annotations

import os
import time
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import orjson

# Tolerant config import
try:
    from app.core.config import settings
except Exception:  # pragma: no cover
    class _DummySettings:  # type: ignore
        ROOT: Path = Path(__file__).resolve().parents[2]
    settings = _DummySettings()  # type: ignore

ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])
STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

ACCOUNT_LABEL: str = (os.getenv("ACCOUNT_LABEL") or "main").strip() or "main"

# Preferred label-specific paths
ORDERBOOK_PATH_LABELED: Path = STATE_DIR / f"orderbook_bus_{ACCOUNT_LABEL}.json"
TRADES_PATH_LABELED: Path = STATE_DIR / f"trades_bus_{ACCOUNT_LABEL}.json"

# Legacy fallbacks
ORDERBOOK_PATH_LEGACY: Path = STATE_DIR / "orderbook_bus.json"
TRADES_PATH_LEGACY: Path = STATE_DIR / "trades_bus.json"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _load_json(path: Path) -> Dict[str, Any]:
    """
    Load a JSON file via orjson; return {} on any error.
    """
    try:
        if not path.exists():
            return {}
        raw = path.read_bytes()
        if not raw:
            return {}
        data = orjson.loads(raw)
        if not isinstance(data, dict):
            return {}
        return data
    except Exception:
        return {}


def _load_json_with_fallback(preferred: Path, fallback: Path) -> Dict[str, Any]:
    """
    Load preferred path; if missing/empty/invalid, load fallback.
    """
    data = _load_json(preferred)
    if data:
        return data
    return _load_json(fallback)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _decimal_or_none(v: Any) -> Optional[Decimal]:
    if v is None:
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Orderbook bus API
# ---------------------------------------------------------------------------

def orderbook_bus_updated_ms() -> Optional[int]:
    """
    Return the last 'updated_ms' from orderbook bus, or None.
    Prefers label-specific bus; falls back to legacy.
    """
    data = _load_json_with_fallback(ORDERBOOK_PATH_LABELED, ORDERBOOK_PATH_LEGACY)
    try:
        return int(data.get("updated_ms"))
    except Exception:
        return None


def orderbook_bus_age_sec() -> Optional[float]:
    """
    Return age (seconds) of the orderbook bus, or None if unknown.
    """
    updated_ms = orderbook_bus_updated_ms()
    if updated_ms is None or updated_ms <= 0:
        return None
    now_ms = _now_ms()
    if now_ms <= updated_ms:
        return None
    return (now_ms - updated_ms) / 1000.0


def get_orderbook_snapshot(symbol: str) -> Dict[str, Any]:
    """
    Return a shallow orderbook snapshot for `symbol`:

      {
        "symbol": "BTCUSDT",
        "bids": [[price, size], ...],
        "asks": [[price, size], ...],
        "ts_ms": <exchange_ts_or_now>,
        "updated_ms": <local_snapshot_ts_or_now>,
      }

    If missing, returns an empty skeleton with empty bids/asks and 0 ts_ms.
    """
    sym = symbol.upper()
    data = _load_json_with_fallback(ORDERBOOK_PATH_LABELED, ORDERBOOK_PATH_LEGACY)

    symbols = data.get("symbols") or {}
    ob = symbols.get(sym)
    if not isinstance(ob, dict):
        return {
            "symbol": sym,
            "bids": [],
            "asks": [],
            "ts_ms": 0,
            "updated_ms": int(data.get("updated_ms", 0) or 0),
        }

    bids = ob.get("bids") or []
    asks = ob.get("asks") or []
    ts_ms = ob.get("ts_ms", 0)
    updated_ms = data.get("updated_ms", 0) or 0

    try:
        ts_ms_int = int(ts_ms)
    except Exception:
        ts_ms_int = 0

    try:
        upd_int = int(updated_ms)
    except Exception:
        upd_int = 0

    return {
        "symbol": sym,
        "bids": bids,
        "asks": asks,
        "ts_ms": ts_ms_int,
        "updated_ms": upd_int,
    }


def best_bid_ask(symbol: str) -> Tuple[Optional[Decimal], Optional[Decimal]]:
    """
    Convenience: return (best_bid, best_ask) as Decimals or (None, None) if missing.
    """
    ob = get_orderbook_snapshot(symbol)
    bids = ob.get("bids") or []
    asks = ob.get("asks") or []

    best_bid = None
    best_ask = None

    if bids:
        try:
            best_bid = _decimal_or_none(bids[0][0])
        except Exception:
            best_bid = None

    if asks:
        try:
            best_ask = _decimal_or_none(asks[0][0])
        except Exception:
            best_ask = None

    return best_bid, best_ask


# ---------------------------------------------------------------------------
# Trades bus API
# ---------------------------------------------------------------------------

def trades_bus_updated_ms() -> Optional[int]:
    """
    Return the last 'updated_ms' from trades bus, or None.
    Prefers label-specific bus; falls back to legacy.
    """
    data = _load_json_with_fallback(TRADES_PATH_LABELED, TRADES_PATH_LEGACY)
    try:
        return int(data.get("updated_ms"))
    except Exception:
        return None


def trades_bus_age_sec() -> Optional[float]:
    """
    Return age (seconds) of the trades bus, or None if unknown.
    """
    updated_ms = trades_bus_updated_ms()
    if updated_ms is None or updated_ms <= 0:
        return None
    now_ms = _now_ms()
    if now_ms <= updated_ms:
        return None
    return (now_ms - updated_ms) / 1000.0


def get_recent_trades(symbol: str, limit: int = 100) -> List[Dict[str, Any]]:
    """
    Return up to `limit` most recent trades for `symbol` from trades bus.
    Prefers label-specific bus; falls back to legacy.
    """
    if limit <= 0:
        limit = 1

    sym = symbol.upper()
    data = _load_json_with_fallback(TRADES_PATH_LABELED, TRADES_PATH_LEGACY)
    symbols = data.get("symbols") or {}
    blk = symbols.get(sym) or {}
    if not isinstance(blk, dict):
        return []

    trades = blk.get("trades") or []
    if not isinstance(trades, list):
        return []

    if len(trades) > limit:
        trades = trades[-limit:]

    out: List[Dict[str, Any]] = []
    for t in trades:
        if isinstance(t, dict):
            out.append(t)
    return out

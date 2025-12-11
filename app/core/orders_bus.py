# app/core/orders_bus.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Orders Bus v1.1

Purpose
-------
Single source of truth for normalized order events.

This module writes to:
  state/orders_bus.json

Schema (events is a list of normalized order events):
{
  "schema_version": 1,
  "updated_ms": 1763752000999,
  "events": [
      {
        "ts_ms": 1763751999000,
        "account_label": "main",
        "symbol": "BTCUSDT",
        "order_id": "string",
        "client_order_id": "MAIN_12345",
        "side": "Buy",
        "position_side": "Long",
        "order_type": "Limit",
        "status": "New",
        "event_type": "NEW",
        "price": "65000.0",
        "qty": "0.010",
        "cum_exec_qty": "0.005",
        "cum_exec_value": "325.0",
        "cum_exec_fee": "0.02",
        "reduce_only": false,
        "raw": { ... }
      }
  ]
}

New in v1.1
-----------
- orjson is now optional (falls back to stdlib json).
- Added read helpers:
    * get_orders_snapshot()
    * get_recent_events(account_label=None, symbol=None, limit=200)
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

try:
    import importlib

    _log_module = importlib.import_module("app.core.log")
    get_logger = getattr(_log_module, "get_logger")
except Exception:
    import logging

    def get_logger(name: str) -> "logging.Logger":  # type: ignore
        logger_ = logging.getLogger(name)
        if not logger_.handlers:
            handler = logging.StreamHandler()
            fmt = logging.Formatter(
                "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
            )
            handler.setFormatter(fmt)
            logger_.addHandler(handler)
        logger_.setLevel(logging.INFO)
        return logger_


logger = get_logger("orders_bus")

# ---------------------------------------------------------------------------
# Config / paths
# ---------------------------------------------------------------------------

try:
    from app.core.config import settings
except Exception:
    class _DummySettings:  # type: ignore
        from pathlib import Path
        ROOT = Path(__file__).resolve().parents[2]
    settings = _DummySettings()  # type: ignore

ROOT: Path = getattr(settings, "ROOT")
STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

ORDERS_BUS_PATH: Path = STATE_DIR / "orders_bus.json"

# ---------------------------------------------------------------------------
# JSON helpers (orjson optional)
# ---------------------------------------------------------------------------

try:
    import orjson  # type: ignore

    def _dumps(obj: Any) -> bytes:
        return orjson.dumps(obj)  # type: ignore[arg-type]

    def _loads(b: bytes) -> Any:
        return orjson.loads(b)  # type: ignore[arg-type]

except Exception:
    import json as _json  # type: ignore

    def _dumps(obj: Any) -> bytes:
        return _json.dumps(obj, separators=(",", ":")).encode("utf-8")

    def _loads(b: bytes) -> Any:
        return _json.loads(b.decode("utf-8"))


# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------

def _now_ms() -> int:
    return int(time.time() * 1000)


def _empty_snapshot() -> Dict[str, Any]:
    return {
        "schema_version": 1,
        "updated_ms": 0,
        "events": [],
    }


def _load_orders_bus() -> Dict[str, Any]:
    if not ORDERS_BUS_PATH.exists():
        return _empty_snapshot()
    try:
        data = _loads(ORDERS_BUS_PATH.read_bytes())
        if not isinstance(data, dict):
            raise ValueError("orders_bus root is not dict")
        if "events" not in data or not isinstance(data["events"], list):
            data["events"] = []
        if "schema_version" not in data:
            data["schema_version"] = 1
        if "updated_ms" not in data:
            data["updated_ms"] = 0
        return data
    except Exception as e:
        logger.warning("Failed to load orders_bus.json (starting fresh): %s", e)
        return _empty_snapshot()


def _save_orders_bus(snapshot: Dict[str, Any]) -> None:
    snapshot["schema_version"] = 1
    snapshot["updated_ms"] = _now_ms()
    if "events" not in snapshot or not isinstance(snapshot["events"], list):
        snapshot["events"] = []
    try:
        ORDERS_BUS_PATH.write_bytes(_dumps(snapshot))
    except Exception as e:
        logger.error("Failed to save orders_bus.json: %s", e)


# ---------------------------------------------------------------------------
# Public writer API
# ---------------------------------------------------------------------------

def record_order_event(
    *,
    account_label: str,
    symbol: str,
    order_id: str,
    side: str,
    order_type: str,
    status: str,
    event_type: str,
    price: Optional[str] = None,
    qty: Optional[str] = None,
    cum_exec_qty: Optional[str] = None,
    cum_exec_value: Optional[str] = None,
    cum_exec_fee: Optional[str] = None,
    position_side: Optional[str] = None,
    reduce_only: Optional[bool] = None,
    client_order_id: Optional[str] = None,
    ts_ms: Optional[int] = None,
    raw: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Normalize and append a single order event to orders_bus.json.

    event_type:
        - "NEW"     : order placed
        - "UPDATE"  : partial fill / status change
        - "FILL"    : fully filled
        - "CANCEL"  : cancelled
    """
    snapshot = _load_orders_bus()
    events: List[Dict[str, Any]] = snapshot.get("events", [])

    ts = ts_ms if ts_ms is not None else _now_ms()

    event: Dict[str, Any] = {
        "ts_ms": ts,
        "account_label": account_label,
        "symbol": symbol,
        "order_id": order_id,
        "client_order_id": client_order_id,
        "side": side,
        "position_side": position_side,
        "order_type": order_type,
        "status": status,
        "event_type": event_type,
        "price": price,
        "qty": qty,
        "cum_exec_qty": cum_exec_qty,
        "cum_exec_value": cum_exec_value,
        "cum_exec_fee": cum_exec_fee,
        "reduce_only": bool(reduce_only) if reduce_only is not None else None,
        "raw": raw or {},
    }

    events.append(event)

    # Optional: trim to last N events to prevent unbounded growth.
    max_events = 5000
    if len(events) > max_events:
        events = events[-max_events:]

    snapshot["events"] = events
    _save_orders_bus(snapshot)

    logger.debug(
        "Recorded order event: %s %s %s %s",
        account_label,
        symbol,
        order_id,
        event_type,
    )


# ---------------------------------------------------------------------------
# Public reader API
# ---------------------------------------------------------------------------

def get_orders_snapshot() -> Dict[str, Any]:
    """
    Return a shallow copy of the current orders_bus snapshot.

    Shape:
    {
      "schema_version": 1,
      "updated_ms": <int>,
      "events": [ ... ]
    }
    """
    snap = _load_orders_bus()
    # Shallow copy to avoid accidental in-place mutation
    return {
        "schema_version": snap.get("schema_version", 1),
        "updated_ms": snap.get("updated_ms", 0),
        "events": list(snap.get("events", [])),
    }


def get_recent_events(
    account_label: Optional[str] = None,
    symbol: Optional[str] = None,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    """
    Return the most recent order events, optionally filtered.

    Parameters
    ----------
    account_label : filter by account_label if provided
    symbol        : filter by symbol (e.g. "BTCUSDT") if provided
    limit         : max number of events to return (default 200)

    Events are returned sorted by ts_ms ascending (oldest -> newest).
    """
    snap = _load_orders_bus()
    events: List[Dict[str, Any]] = snap.get("events", [])

    acc = account_label.strip() if isinstance(account_label, str) else None
    sym = symbol.strip().upper() if isinstance(symbol, str) else None

    def _match(ev: Dict[str, Any]) -> bool:
        if acc is not None and str(ev.get("account_label")) != acc:
            return False
        if sym is not None and str(ev.get("symbol", "")).upper() != sym:
            return False
        return True

    filtered = [ev for ev in events if _match(ev)]

    # We already append in chronological order, but sort by ts_ms just in case.
    try:
        filtered.sort(key=lambda e: int(e.get("ts_ms", 0)))
    except Exception:
        pass

    if limit and limit > 0 and len(filtered) > limit:
        filtered = filtered[-limit:]

    return filtered

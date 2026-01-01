from __future__ import annotations

import time
from typing import Any, Dict, Optional

# NOTE:
# This module is a core dependency of the runtime pipeline.
# executor_v2 / ai_pilot / ai_decision_logger expect build_ai_snapshot + validate_snapshot_v2 to exist.
# It was previously clobbered during cleanup. This restores a minimal, robust snapshot bus.

def _now_ms() -> int:
    return int(time.time() * 1000)

def _safe_call(getter, *args, **kwargs):
    try:
        return getter(*args, **kwargs)
    except Exception:
        return None

def _safe_import(path: str):
    try:
        mod = __import__(path, fromlist=["*"])
        return mod
    except Exception:
        return None

def build_ai_snapshot(
    account_label: str,
    symbol: Optional[str] = None,
    timeframe: Optional[str] = None,
    mode: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build a lightweight runtime snapshot for decisioning + logging.

    This is intentionally fail-soft:
    - If any sub-snapshot provider is missing or throws, we still return a snapshot with None fields.
    - Consumers should treat missing fields as "unknown" and degrade gracefully.
    """
    ts_ms = _now_ms()

    # Optional snapshot sources (fail-soft)
    market_bus = _safe_import("app.core.market_bus")
    orders_bus = _safe_import("app.core.orders_bus")
    position_bus = _safe_import("app.core.position_bus")
    balance_snap = _safe_import("app.exchange.balance_snapshot")

    snap: Dict[str, Any] = {
        "schema_version": "snapshot.v2",
        "ts_ms": ts_ms,
        "account_label": account_label,
        "mode": mode,
        "symbol": symbol,
        "timeframe": timeframe,
        "market": None,
        "orders": None,
        "positions": None,
        "balances": None,
        "extra": extra or {},
    }

    # Pull sub-snapshots if providers exist
    if market_bus and hasattr(market_bus, "get_snapshot"):
        snap["market"] = _safe_call(market_bus.get_snapshot, account_label, symbol=symbol)

    if orders_bus and hasattr(orders_bus, "get_snapshot"):
        snap["orders"] = _safe_call(orders_bus.get_snapshot, account_label, symbol=symbol)

    if position_bus and hasattr(position_bus, "get_snapshot"):
        snap["positions"] = _safe_call(position_bus.get_snapshot, account_label, symbol=symbol)

    if balance_snap and hasattr(balance_snap, "get_snapshot"):
        snap["balances"] = _safe_call(balance_snap.get_snapshot, account_label)

    return snap

def validate_snapshot_v2(snapshot: Dict[str, Any]) -> bool:
    """
    Minimal validator: ensures required top-level keys exist.
    We do NOT hard-fail on missing sub-fields because snapshot providers can degrade.
    """
    if not isinstance(snapshot, dict):
        return False

    required = ["schema_version", "ts_ms", "account_label"]
    for k in required:
        if k not in snapshot:
            return False

    if snapshot.get("schema_version") not in ("snapshot.v2",):
        return False

    # ts_ms should be int-like
    try:
        int(snapshot.get("ts_ms"))
    except Exception:
        return False

    return True

def get_snapshot(
    account_label: str,
    symbol: Optional[str] = None,
    timeframe: Optional[str] = None,
    mode: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Convenience alias used by some call sites.
    """
    return build_ai_snapshot(
        account_label=account_label,
        symbol=symbol,
        timeframe=timeframe,
        mode=mode,
        extra=extra,
    )

# ---- Existing helper retained (was the only thing left after the clobber) ----
from app.core.ai_profile import get_district_profile

def allow_capital_transfer(source, target):
    src = get_district_profile(source)
    dst = get_district_profile(target)

    if not src or not dst:
        return False

    if src.get('role') != 'worker':
        return False

    if dst.get('role') != 'treasury':
        return False

    return True

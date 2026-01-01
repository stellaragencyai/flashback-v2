from __future__ import annotations

import hashlib
import time
from typing import Any, Dict

OBSERVED_SCHEMA_VERSION = "observed.v1"

def _now_ms() -> int:
    return int(time.time() * 1000)

def compute_observed_uid(row: Dict[str, Any]) -> str:
    sub_uid = str(row.get("sub_uid") or "")
    symbol = str(row.get("symbol") or "")
    timeframe = str(row.get("timeframe") or "")
    ts_ms = str(row.get("ts_ms") or "")
    side = str(row.get("side") or "")
    reason = str(row.get("reason") or "")
    key = f"{sub_uid}|{symbol}|{timeframe}|{ts_ms}|{side}|{reason}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()

def normalize_observed_row(row: Dict[str, Any]) -> Dict[str, Any]:
    dbg = row.get("debug") or {}
    if not isinstance(dbg, dict):
        dbg = {}

    out = {
        "schema_version": OBSERVED_SCHEMA_VERSION,
        "observed_uid": compute_observed_uid(row),
        "emitted_at_ms": int(row.get("emitted_at_ms") or _now_ms()),

        "sub_uid": row.get("sub_uid"),
        "strategy_name": row.get("strategy_name"),

        "symbol": row.get("symbol"),
        "timeframe": row.get("timeframe"),
        "ts_ms": row.get("ts_ms"),
        "side": row.get("side"),
        "reason": row.get("reason"),
        "setup_type": row.get("setup_type"),

        "est_rr": row.get("est_rr"),
        "price": row.get("price"),

        "debug": {
            "engine": dbg.get("engine"),
            "raw_reason": dbg.get("raw_reason"),
            "regime": dbg.get("regime"),
            "last_close": dbg.get("last_close"),
            "prev_close": dbg.get("prev_close"),
            "ma": dbg.get("ma"),

        },
    }
    return out

def assert_normalized_row_ok(row: Dict[str, Any]) -> None:
    # hard-required
    req = ["schema_version","observed_uid","emitted_at_ms","sub_uid","symbol","timeframe","ts_ms","side","reason","setup_type"]
    missing = [k for k in req if row.get(k) in (None, "", [])]
    if missing:
        raise ValueError(f"missing_required={missing}")

    dbg = row.get("debug") or {}
    for k in ["last_close","prev_close","ma"]:
        if dbg.get(k) is None:
            raise ValueError(f"debug_missing={k}")

    # numeric sanity (lightweight)
    for k in ["ts_ms","emitted_at_ms"]:
        v = row.get(k)
        if not isinstance(v, int):
            raise ValueError(f"{k}_not_int={type(v).__name__}")

    # side normalization not forced here (Signal Engine already emits 'Sell'/'Buy')

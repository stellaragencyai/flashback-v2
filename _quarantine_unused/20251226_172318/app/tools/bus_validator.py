#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Bus Validator (schema validator for WS-fed state files)

Validates:
- state/positions_bus.json  (v2 normalized schema)
- state/orderbook_bus.json  (v1 schema)
- state/trades_bus.json     (v1 schema)

Outputs:
- PASS ✅ / FAIL ❌
- Detailed reasons per bus
Exit codes:
- 0 = PASS
- 2 = FAIL (any bus schema invalid / missing)
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# -------------------------
# ROOT resolution
# -------------------------
try:
    from app.core.config import settings  # type: ignore
    ROOT: Path = Path(settings.ROOT)  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

STATE_DIR: Path = ROOT / "state"
POSITIONS_PATH: Path = STATE_DIR / "positions_bus.json"
ORDERBOOK_PATH: Path = STATE_DIR / "orderbook_bus.json"
TRADES_PATH: Path = STATE_DIR / "trades_bus.json"

# -------------------------
# JSON loading (orjson if available)
# -------------------------
def _load_json(path: Path) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not path.exists():
        return None, "missing file"
    try:
        raw = path.read_bytes()
        if not raw:
            return None, "empty file"
        try:
            import orjson  # type: ignore
            data = orjson.loads(raw)
        except Exception:
            import json
            data = json.loads(raw.decode("utf-8", errors="replace"))
        if not isinstance(data, dict):
            return None, "root is not a JSON object"
        return data, None
    except Exception as e:
        return None, f"read/parse error: {e}"

def _is_int(x: Any) -> bool:
    try:
        int(x)
        return True
    except Exception:
        return False

def _is_str(x: Any) -> bool:
    return isinstance(x, str) and len(x.strip()) > 0

def _now_ms() -> int:
    return int(time.time() * 1000)

# -------------------------
# Validation helpers
# -------------------------
def _require_keys(obj: Dict[str, Any], keys: List[str], ctx: str) -> List[str]:
    errs: List[str] = []
    for k in keys:
        if k not in obj:
            errs.append(f"{ctx}: missing key '{k}'")
    return errs

def _validate_positions_bus(data: Dict[str, Any]) -> List[str]:
    errs: List[str] = []

    errs += _require_keys(data, ["version", "updated_ms", "labels"], "positions_bus")
    if errs:
        return errs

    if not _is_int(data.get("version")):
        errs.append("positions_bus: version must be int")
    if not _is_int(data.get("updated_ms")):
        errs.append("positions_bus: updated_ms must be int")

    labels = data.get("labels")
    if not isinstance(labels, dict):
        errs.append("positions_bus: labels must be an object/dict")
        return errs

    # Validate each label block
    for label, blk in labels.items():
        if not _is_str(label):
            errs.append("positions_bus: label keys must be non-empty strings")
            continue
        if not isinstance(blk, dict):
            errs.append(f"positions_bus.labels[{label}]: must be object/dict")
            continue

        # Required label keys
        if "category" not in blk:
            errs.append(f"positions_bus.labels[{label}]: missing 'category'")
        if "positions" not in blk:
            errs.append(f"positions_bus.labels[{label}]: missing 'positions'")

        cat = blk.get("category")
        if cat is not None and not isinstance(cat, str):
            errs.append(f"positions_bus.labels[{label}].category must be str")

        positions = blk.get("positions")
        if positions is None:
            continue
        if not isinstance(positions, list):
            errs.append(f"positions_bus.labels[{label}].positions must be list")
            continue

        # Validate each normalized position row (your Position Bus expects these keys)
        required_row_keys = ["symbol", "side", "size", "avgPrice", "stopLoss", "sub_uid", "account_label", "category"]
        for i, row in enumerate(positions):
            if not isinstance(row, dict):
                errs.append(f"positions_bus.labels[{label}].positions[{i}] must be object/dict")
                continue

            for k in required_row_keys:
                if k not in row:
                    errs.append(f"positions_bus.labels[{label}].positions[{i}]: missing '{k}'")

            # Minimal type checks
            if "symbol" in row and not _is_str(row.get("symbol")):
                errs.append(f"positions_bus.labels[{label}].positions[{i}].symbol must be non-empty str")
            if "account_label" in row and not _is_str(row.get("account_label")):
                errs.append(f"positions_bus.labels[{label}].positions[{i}].account_label must be non-empty str")
            if "category" in row and not isinstance(row.get("category"), str):
                errs.append(f"positions_bus.labels[{label}].positions[{i}].category must be str")
            if "size" in row:
                try:
                    float(row.get("size"))
                except Exception:
                    errs.append(f"positions_bus.labels[{label}].positions[{i}].size must be numeric")
            if "avgPrice" in row:
                try:
                    float(row.get("avgPrice"))
                except Exception:
                    errs.append(f"positions_bus.labels[{label}].positions[{i}].avgPrice must be numeric")
            if "stopLoss" in row:
                try:
                    float(row.get("stopLoss"))
                except Exception:
                    errs.append(f"positions_bus.labels[{label}].positions[{i}].stopLoss must be numeric")

    return errs

def _validate_orderbook_bus(data: Dict[str, Any]) -> List[str]:
    errs: List[str] = []
    errs += _require_keys(data, ["version", "updated_ms", "symbols"], "orderbook_bus")
    if errs:
        return errs

    if not _is_int(data.get("version")):
        errs.append("orderbook_bus: version must be int")
    if not _is_int(data.get("updated_ms")):
        errs.append("orderbook_bus: updated_ms must be int")

    symbols = data.get("symbols")
    if not isinstance(symbols, dict):
        errs.append("orderbook_bus: symbols must be object/dict")
        return errs

    for sym, ob in symbols.items():
        if not _is_str(sym):
            errs.append("orderbook_bus: symbol keys must be non-empty strings")
            continue
        if not isinstance(ob, dict):
            errs.append(f"orderbook_bus.symbols[{sym}]: must be object/dict")
            continue

        # Required keys per ws_switchboard writer
        for k in ["bids", "asks", "ts_ms"]:
            if k not in ob:
                errs.append(f"orderbook_bus.symbols[{sym}]: missing '{k}'")

        bids = ob.get("bids")
        asks = ob.get("asks")
        if bids is not None and not isinstance(bids, list):
            errs.append(f"orderbook_bus.symbols[{sym}].bids must be list")
        if asks is not None and not isinstance(asks, list):
            errs.append(f"orderbook_bus.symbols[{sym}].asks must be list")
        if "ts_ms" in ob and not _is_int(ob.get("ts_ms")):
            errs.append(f"orderbook_bus.symbols[{sym}].ts_ms must be int")

    return errs

def _validate_trades_bus(data: Dict[str, Any]) -> List[str]:
    errs: List[str] = []
    errs += _require_keys(data, ["version", "updated_ms", "symbols"], "trades_bus")
    if errs:
        return errs

    if not _is_int(data.get("version")):
        errs.append("trades_bus: version must be int")
    if not _is_int(data.get("updated_ms")):
        errs.append("trades_bus: updated_ms must be int")

    symbols = data.get("symbols")
    if not isinstance(symbols, dict):
        errs.append("trades_bus: symbols must be object/dict")
        return errs

    for sym, blk in symbols.items():
        if not _is_str(sym):
            errs.append("trades_bus: symbol keys must be non-empty strings")
            continue
        if not isinstance(blk, dict):
            errs.append(f"trades_bus.symbols[{sym}]: must be object/dict")
            continue

        trades = blk.get("trades")
        if trades is None:
            # allowed: symbol block with no trades yet
            continue
        if not isinstance(trades, list):
            errs.append(f"trades_bus.symbols[{sym}].trades must be list")
            continue

        # Trades are raw dicts from Bybit publicTrade stream. We only enforce "dict-ness".
        for i, t in enumerate(trades):
            if not isinstance(t, dict):
                errs.append(f"trades_bus.symbols[{sym}].trades[{i}] must be object/dict")

    return errs

# -------------------------
# CLI
# -------------------------
def main() -> int:
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    results: List[Tuple[str, List[str], Optional[str]]] = []

    # positions
    p_data, p_err = _load_json(POSITIONS_PATH)
    if p_data is None:
        results.append(("positions_bus", [f"positions_bus: {p_err}"], None))
    else:
        results.append(("positions_bus", _validate_positions_bus(p_data), None))

    # orderbook
    o_data, o_err = _load_json(ORDERBOOK_PATH)
    if o_data is None:
        results.append(("orderbook_bus", [f"orderbook_bus: {o_err}"], None))
    else:
        results.append(("orderbook_bus", _validate_orderbook_bus(o_data), None))

    # trades
    t_data, t_err = _load_json(TRADES_PATH)
    if t_data is None:
        results.append(("trades_bus", [f"trades_bus: {t_err}"], None))
    else:
        results.append(("trades_bus", _validate_trades_bus(t_data), None))

    print("\n=== BUS VALIDATOR ===")
    print(f"ROOT: {ROOT}")
    print(f"NOW_MS: {_now_ms()}")

    any_fail = False
    for name, errs, _ in results:
        if errs:
            any_fail = True
            print(f"\n{name}: FAIL ❌ ({len(errs)} issues)")
            for e in errs[:60]:
                print(f" - {e}")
            if len(errs) > 60:
                print(f" - ...and {len(errs) - 60} more")
        else:
            print(f"\n{name}: PASS ✅")

    if any_fail:
        print("\nOVERALL: FAIL ❌")
        return 2

    print("\nOVERALL: PASS ✅")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

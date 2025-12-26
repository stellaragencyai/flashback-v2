#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Execution (WS-first helpers)

Purpose
-------
Provide a small, consistent surface for higher-level logic (AI, scripts)
to:

    • Open positions sized by % of equity (notional-based).
    • Flatten a single symbol.
    • Flatten all symbols.
    • List open symbols.

All of this is built on top of app.core.flashback_common and is designed
to be:

    • WS-first for pricing (via last_price_ws_first / spread_bps_ws).
    • Bybit v5 REST for actual order placement.
    • Safe, with spread caps and Telegram notifications.

This module intentionally does NOT manage TP/SL; that is the job of
tp_sl_manager, which reacts to positions via position_bus.

CRITICAL SAFETY RULE
--------------------
If EXEC_DRY_RUN=true, this module MUST NOT place live orders,
even if called directly (bypassing routers/adapters).
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, List, Optional, Set

from app.core.flashback_common import (
    send_tg,
    alert_bot_error,
    record_heartbeat,
    get_equity_usdt,
    list_open_positions,
    qty_from_pct,
    last_price,
    last_price_ws_first,
    spread_bps_ws,
    place_market_entry,
    reduce_only_market,
    cancel_all,
    EXEC_DRY_RUN,
)


CATEGORY = "linear"
QUOTE = "USDT"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _normalize_symbol(symbol: str) -> str:
    s = str(symbol or "").strip().upper()
    if not s:
        raise ValueError("symbol is required")
    return s


def _log(msg: str) -> None:
    """
    Best-effort Telegram log for execution events.

    HARD RULE:
      - In EXEC_DRY_RUN, send_tg() is a no-op by default (no network).
      - In LIVE, send_tg() uses short timeouts and never raises.
    """
    try:
        send_tg(msg)
    except Exception:
        # Don't ever let logging kill execution
        pass


# ---------------------------------------------------------------------------
# Public: list open symbols
# ---------------------------------------------------------------------------

def list_open_symbols() -> List[str]:
    """
    Return a sorted list of symbols with non-zero linear positions.

    In EXEC_DRY_RUN, list_open_positions() returns [] by design.
    """
    try:
        rows = list_open_positions(category=CATEGORY, settle_coin=QUOTE)
    except Exception as e:
        alert_bot_error("execution_ws", f"list_open_positions error: {e}", "ERROR")
        return []

    syms: Set[str] = set()
    for p in rows:
        try:
            size = Decimal(str(p.get("size", "0")))
        except Exception:
            size = Decimal("0")
        if size > 0:
            sym = str(p.get("symbol") or "").strip().upper()
            if sym:
                syms.add(sym)

    return sorted(syms)


# ---------------------------------------------------------------------------
# Public: open position (WS-first pricing)
# ---------------------------------------------------------------------------

def open_position_ws_first(
    symbol: str,
    side: str,
    risk_pct_notional: Decimal,
    max_spread_bps: Optional[Decimal] = None,
    leverage_override: Optional[int] = None,
    notify: bool = True,
) -> Dict[str, Any]:
    """
    Open a position sized by % of equity (notional-based).

    DRY-RUN:
      - If EXEC_DRY_RUN=true, this function returns ok=True with dry_run=True
        and does NOT place any orders (defense-in-depth).
    """
    record_heartbeat("execution_ws")

    sym = _normalize_symbol(symbol)
    side_norm = str(side or "").strip().upper()
    if side_norm not in ("LONG", "SHORT"):
        raise ValueError("side must be 'LONG' or 'SHORT'")

    # HARD GATE: never place orders in dry-run, even if called directly.
    if EXEC_DRY_RUN:
        if notify:
            _log(f"🧪 DRY_RUN OPEN blocked at execution layer: {sym} {side_norm} | risk={risk_pct_notional}%")
        return {
            "ok": True,
            "error": None,
            "dry_run": True,
            "symbol": sym,
            "side": side_norm,
            "qty": None,
            "px_ref": None,
            "spread_bps": None,
            "skipped": "EXEC_DRY_RUN",
        }

    # 1) Optionally check spread from WS orderbook
    spread_val: Optional[Decimal] = None
    if max_spread_bps is not None and max_spread_bps > 0:
        try:
            spread_val = spread_bps_ws(sym)
        except Exception as e:
            alert_bot_error("execution_ws", f"{sym} spread_bps_ws error: {e}", "WARN")
            spread_val = None

        if spread_val is not None and spread_val > max_spread_bps:
            msg = (
                f"Spread too wide for {sym}: {spread_val:.2f} bps > "
                f"cap {max_spread_bps:.2f} bps. Skipping entry."
            )
            _log(f"🚫 {msg}")
            return {
                "ok": False,
                "error": msg,
                "symbol": sym,
                "side": side_norm,
                "qty": None,
                "px_ref": None,
                "spread_bps": f"{spread_val:.4f}",
            }

    # 2) Determine equity and qty from notional %
    try:
        equity = get_equity_usdt()
    except Exception as e:
        alert_bot_error("execution_ws", f"get_equity_usdt error: {e}", "ERROR")
        return {
            "ok": False,
            "error": f"get_equity_usdt error: {e}",
            "symbol": sym,
            "side": side_norm,
            "qty": None,
            "px_ref": None,
            "spread_bps": str(spread_val) if spread_val is not None else None,
        }

    if equity <= 0:
        msg = "Equity is zero or negative; cannot size position."
        _log(f"🚫 {msg}")
        return {
            "ok": False,
            "error": msg,
            "symbol": sym,
            "side": side_norm,
            "qty": None,
            "px_ref": None,
            "spread_bps": str(spread_val) if spread_val is not None else None,
        }

    try:
        qty = qty_from_pct(sym, equity, risk_pct_notional)
    except Exception as e:
        alert_bot_error("execution_ws", f"{sym} qty_from_pct error: {e}", "ERROR")
        return {
            "ok": False,
            "error": f"qty_from_pct error: {e}",
            "symbol": sym,
            "side": side_norm,
            "qty": None,
            "px_ref": None,
            "spread_bps": str(spread_val) if spread_val is not None else None,
        }

    if qty <= 0:
        msg = f"Computed qty <= 0 for {sym} at {risk_pct_notional}% of equity."
        _log(f"🚫 {msg}")
        return {
            "ok": False,
            "error": msg,
            "symbol": sym,
            "side": side_norm,
            "qty": None,
            "px_ref": None,
            "spread_bps": str(spread_val) if spread_val is not None else None,
        }

    # 3) Reference price (WS-first, fallback to REST ticker)
    try:
        px_ref = last_price_ws_first(sym)
        if px_ref <= 0:
            px_ref = last_price(sym)
    except Exception:
        try:
            px_ref = last_price(sym)
        except Exception:
            px_ref = Decimal("0")

    # 4) Place market order
    try:
        res = place_market_entry(
            symbol=sym,
            side=side_norm,
            qty=qty,
            leverage=leverage_override,
        )
    except Exception as e:
        alert_bot_error("execution_ws", f"{sym} place_market_entry error: {e}", "ERROR")
        return {
            "ok": False,
            "error": f"place_market_entry error: {e}",
            "symbol": sym,
            "side": side_norm,
            "qty": str(qty),
            "px_ref": str(px_ref),
            "spread_bps": str(spread_val) if spread_val is not None else None,
        }

    if notify:
        if spread_val is not None:
            msg = (
                f"✅ OPEN {sym} {side_norm} | "
                f"risk={risk_pct_notional}% | qty={qty} | "
                f"px_ref={px_ref} | spread_bps={spread_val:.2f}"
            )
        else:
            msg = (
                f"✅ OPEN {sym} {side_norm} | "
                f"risk={risk_pct_notional}% | qty={qty} | "
                f"px_ref={px_ref}"
            )
        _log(msg)

    return {
        "ok": True,
        "error": None,
        "symbol": sym,
        "side": side_norm,
        "qty": str(qty),
        "px_ref": str(px_ref),
        "spread_bps": str(spread_val) if spread_val is not None else None,
        "raw": res,
    }


# ---------------------------------------------------------------------------
# Public: flatten a single symbol
# ---------------------------------------------------------------------------

def flatten_symbol_ws_first(
    symbol: str,
    notify: bool = True,
) -> Dict[str, Any]:
    """
    Flatten all linear positions for a given symbol via reduce-only market.

    DRY-RUN:
      - If EXEC_DRY_RUN=true, returns ok=True dry_run=True and does not place orders.
    """
    record_heartbeat("execution_ws")

    sym = _normalize_symbol(symbol)

    # HARD GATE: never place orders in dry-run, even if called directly.
    if EXEC_DRY_RUN:
        if notify:
            _log(f"🧪 DRY_RUN FLATTEN blocked at execution layer: {sym}")
        return {
            "ok": True,
            "error": None,
            "dry_run": True,
            "symbol": sym,
            "flattened_qty": "0",
            "skipped": "EXEC_DRY_RUN",
        }

    try:
        rows = list_open_positions(category=CATEGORY, settle_coin=QUOTE)
    except Exception as e:
        alert_bot_error("execution_ws", f"{sym} list_open_positions error: {e}", "ERROR")
        return {
            "ok": False,
            "error": f"list_open_positions error: {e}",
            "symbol": sym,
            "flattened_qty": "0",
        }

    total_flattened = Decimal("0")
    errors: List[str] = []

    for p in rows:
        p_sym = str(p.get("symbol") or "").strip().upper()
        if p_sym != sym:
            continue

        try:
            size = Decimal(str(p.get("size", "0")))
        except Exception:
            size = Decimal("0")

        if size <= 0:
            continue

        side = str(p.get("side") or "").strip()
        try:
            res = reduce_only_market(sym, side, size)
            _ = res  # trade result is for journal/Bybit
            total_flattened += size
        except Exception as e:
            msg = f"{sym} reduce_only_market error: {e}"
            alert_bot_error("execution_ws", msg, "ERROR")
            errors.append(str(e))

    # Best-effort cancel open orders for that symbol
    try:
        cancel_all(sym)
    except Exception as e:
        alert_bot_error("execution_ws", f"{sym} cancel_all error: {e}", "WARN")

    if notify:
        if total_flattened > 0:
            _log(f"🧹 FLATTEN {sym} | qty={total_flattened} | errors={len(errors)}")
        else:
            _log(f"ℹ️ FLATTEN {sym} requested but no open position found.")

    if errors:
        return {
            "ok": total_flattened > 0,
            "error": "; ".join(errors),
            "symbol": sym,
            "flattened_qty": str(total_flattened),
        }

    return {
        "ok": True,
        "error": None,
        "symbol": sym,
        "flattened_qty": str(total_flattened),
    }


# ---------------------------------------------------------------------------
# Public: flatten all symbols (convenience)
# ---------------------------------------------------------------------------

def flatten_all_ws_first(notify: bool = True) -> Dict[str, Any]:
    """
    Flatten all linear symbols with open positions.

    DRY-RUN:
      - If EXEC_DRY_RUN=true, returns ok=True dry_run=True and does not place orders.
    """
    # HARD GATE: never place orders in dry-run, even if called directly.
    if EXEC_DRY_RUN:
        if notify:
            _log("🧪 DRY_RUN FLATTEN_ALL blocked at execution layer")
        return {
            "ok": True,
            "dry_run": True,
            "results": {},
            "skipped": "EXEC_DRY_RUN",
        }

    syms = list_open_symbols()
    results: Dict[str, Any] = {}

    for sym in syms:
        res = flatten_symbol_ws_first(sym, notify=notify)
        results[sym] = res

    if notify:
        _log(f"🧹 FLATTEN_ALL requested for {len(syms)} symbols.")

    return {
        "ok": True,
        "results": results,
    }


if __name__ == "__main__":
    print("execution_ws module loaded. Use from other scripts; no CLI here.")

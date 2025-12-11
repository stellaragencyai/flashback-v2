#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — PaperBroker v1.0

Role
----
Central PAPER engine for Flashback:

    • Track open PAPER positions per trade_id / symbol / strategy / account_label.
    • React to live price ticks (hooked from WS Switchboard).
    • Detect TP / SL hits and close PAPER trades.
    • Emit AI OutcomeRecord events for every closed PAPER trade
      so the AI Setup Memory can learn from full trade lifecycles.

Design
------
- Disk-based shared state:
      state/paper_positions.json   (open paper positions)
      state/paper_trades.jsonl     (open/close events for audit/debug)

- All mutations are:
      load → modify → save
  This is crude but robust enough for small concurrency across processes
  (executor_v2 opening trades, WS Switchboard ticking prices).

- Public API:

      open_paper_position(...)
      close_paper_position(...)
      on_price_tick(symbol, price, ts_ms=None)

  WS Switchboard only calls on_price_tick(...).
  Executor v2 (or future entry logic) will call open_paper_position(...)
  once we wire it.

Notes
-----
- This module is PAPER-only. It never touches live orders.
- OutcomeRecords are published via app.ai.ai_events_spine, which will
  merge them with SetupContext events (by trade_id) and compute R-multiples.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, Optional

# ---------------------------------------------------------------------------
# ROOT + logger
# ---------------------------------------------------------------------------

try:
    from app.core.config import settings  # type: ignore
    ROOT: Path = settings.ROOT  # type: ignore
except Exception:
    ROOT = Path(__file__).resolve().parents[2]

STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

PAPER_POSITIONS_PATH: Path = STATE_DIR / "paper_positions.json"
PAPER_TRADES_LOG_PATH: Path = STATE_DIR / "paper_trades.jsonl"

# Logging
try:
    from app.core.logger import get_logger  # type: ignore
except Exception:  # pragma: no cover
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

log = get_logger("paper_broker")

# ---------------------------------------------------------------------------
# AI events spine (OutcomeRecord emission)
# ---------------------------------------------------------------------------

try:
    from app.ai.ai_events_spine import (  # type: ignore
        build_outcome_record,
        publish_ai_event,
    )
    _AI_EVENTS_AVAILABLE = True
except Exception:  # pragma: no cover
    log.warning(
        "ai_events_spine not available; PaperBroker will NOT emit AI events. "
        "Check imports if you expected Setup/Outcome logging."
    )

    def build_outcome_record(
        *,
        trade_id: str,
        symbol: str,
        account_label: str,
        strategy: str,
        pnl_usd: float,
        r_multiple: Optional[float] = None,
        win: Optional[bool] = None,
        exit_reason: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        # Fallback: return a minimal dict; nothing will be published.
        payload: Dict[str, Any] = {
            "event_type": "outcome_record",
            "trade_id": trade_id,
            "symbol": symbol,
            "account_label": account_label,
            "strategy": strategy,
            "payload": {
                "pnl_usd": float(pnl_usd),
                "r_multiple": float(r_multiple) if r_multiple is not None else None,
                "win": bool(win) if win is not None else None,
                "exit_reason": exit_reason,
                "extra": extra or {},
            },
        }
        return payload

    def publish_ai_event(event: Dict[str, Any]) -> None:
        return None

    _AI_EVENTS_AVAILABLE = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def _safe_price(x: Any) -> float:
    try:
        v = float(x)
        if v <= 0:
            return 0.0
        return v
    except Exception:
        return 0.0


def _load_positions() -> Dict[str, Dict[str, Any]]:
    """
    Load open PAPER positions from disk.

    File shape:
        {
          "version": 1,
          "updated_ms": 1763...,
          "positions": {
             "<trade_id>": { ...position dict... },
             ...
          }
        }
    """
    if not PAPER_POSITIONS_PATH.exists():
        return {}
    try:
        raw = PAPER_POSITIONS_PATH.read_text(encoding="utf-8")
        if not raw.strip():
            return {}
        data = json.loads(raw)
        if not isinstance(data, dict):
            return {}
        pos = data.get("positions")
        if not isinstance(pos, dict):
            return {}
        # Ensure trade_id keys map to dicts
        out: Dict[str, Dict[str, Any]] = {}
        for tid, pd in pos.items():
            if isinstance(tid, str) and isinstance(pd, dict):
                out[tid] = pd
        return out
    except Exception as e:
        log.warning("Failed to load PAPER positions: %r", e)
        return {}


def _save_positions(positions: Dict[str, Dict[str, Any]]) -> None:
    payload = {
        "version": 1,
        "updated_ms": _now_ms(),
        "positions": positions,
    }
    try:
        PAPER_POSITIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
        PAPER_POSITIONS_PATH.write_text(
            json.dumps(payload, separators=(",", ":")),
            encoding="utf-8",
        )
    except Exception as e:
        log.warning("Failed to save PAPER positions: %r", e)


def _append_trade_log(row: Dict[str, Any]) -> None:
    try:
        PAPER_TRADES_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with PAPER_TRADES_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(row, separators=(",", ":")) + "\n")
    except Exception as e:
        log.warning("Failed to append paper trade log: %r", e)


# ---------------------------------------------------------------------------
# Outcome emission
# ---------------------------------------------------------------------------

def _emit_outcome_for_closed_position(
    pos: Dict[str, Any],
    *,
    exit_price: float,
    exit_reason: str,
    ts_ms: Optional[int] = None,
) -> None:
    """
    Compute PnL for a closed PAPER position and emit OutcomeRecord → AI spine.
    """
    trade_id = str(pos.get("trade_id") or "")
    symbol = str(pos.get("symbol") or "")
    side = str(pos.get("side") or "").title()  # "Buy" / "Sell"
    qty = _safe_float(pos.get("qty"))
    entry_price = _safe_price(pos.get("entry_price"))

    account_label = str(pos.get("account_label") or "main")
    strategy = str(pos.get("strategy") or "unknown_strategy")

    if not trade_id or not symbol or qty <= 0 or entry_price <= 0:
        log.warning(
            "Skipping outcome emission for malformed PAPER position: trade_id=%r symbol=%r qty=%r entry_price=%r",
            trade_id,
            symbol,
            qty,
            entry_price,
        )
        return

    exit_price = _safe_price(exit_price)
    if exit_price <= 0:
        log.warning(
            "Skipping outcome emission for trade_id=%s symbol=%s: invalid exit_price=%r",
            trade_id,
            symbol,
            exit_price,
        )
        return

    # PnL: positive = profit, negative = loss
    if side == "Buy":
        pnl_usd = (exit_price - entry_price) * qty
    elif side == "Sell":
        pnl_usd = (entry_price - exit_price) * qty
    else:
        log.warning(
            "Unknown side %r for PAPER trade_id=%s symbol=%s; assuming no PnL.",
            side,
            trade_id,
            symbol,
        )
        pnl_usd = 0.0

    ts_close = ts_ms if ts_ms is not None else _now_ms()

    # Extra payload for AI logging (entry/exit timestamps etc.)
    extra: Dict[str, Any] = {
        "mode": pos.get("mode"),
        "sub_uid": pos.get("sub_uid"),
        "entry_ts_ms": pos.get("entry_ts_ms"),
        "exit_ts_ms": ts_close,
        "stop_price": pos.get("stop_price"),
        "tp_price": pos.get("tp_price"),
        "meta": pos.get("meta") or {},
    }

    event = build_outcome_record(
        trade_id=trade_id,
        symbol=symbol,
        account_label=account_label,
        strategy=strategy,
        pnl_usd=float(pnl_usd),
        r_multiple=None,   # Let ai_events_spine compute from risk_usd in SetupContext
        win=None,
        exit_reason=exit_reason,
        extra=extra,
    )

    try:
        publish_ai_event(event)
    except Exception as e:
        log.warning(
            "Failed to publish AI OutcomeRecord for trade_id=%s symbol=%s: %r",
            trade_id,
            symbol,
            e,
        )

    # Also append to local audit log
    try:
        row = {
            "ts_ms": ts_close,
            "event": "close",
            "trade_id": trade_id,
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "account_label": account_label,
            "strategy": strategy,
            "exit_reason": exit_reason,
            "pnl_usd": float(pnl_usd),
        }
        _append_trade_log(row)
    except Exception:
        # Non-fatal if audit log fails
        pass


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def open_paper_position(
    *,
    trade_id: str,
    symbol: str,
    side: str,
    entry_price: float,
    qty: float,
    account_label: str,
    strategy: str,
    mode: str = "PAPER",
    sub_uid: Optional[str] = None,
    stop_price: Optional[float] = None,
    tp_price: Optional[float] = None,
    risk_usd: Optional[float] = None,
    meta: Optional[Dict[str, Any]] = None,
    ts_ms: Optional[int] = None,
) -> None:
    """
    Register a new PAPER position.

    Typical caller: executor_v2 after AI gate + sizing, instead of placing
    a live order when automation_mode is LEARN_DRY.

    Parameters
    ----------
    trade_id      : same ID as SetupContext / orderLinkId (unique per trade)
    symbol        : "BTCUSDT", etc.
    side          : "Buy" | "Sell" (case-insensitive, normalized internally)
    entry_price   : fill price for PAPER entry
    qty           : position size in contract units (matching live logic)
    account_label : "flashback01", "main", etc.
    strategy      : human-readable strategy label (e.g. "Sub1_Trend")
    mode          : "PAPER" (default) or whatever you want to tag it with
    sub_uid       : Bybit subaccount UID string (optional)
    stop_price    : optional SL level for auto-close on tick
    tp_price      : optional TP level for auto-close on tick
    risk_usd      : optional risk at entry; used later for analytics
    meta          : any misc fields you want to keep with the position
    ts_ms         : entry timestamp (ms). Defaults to now if not provided.
    """
    trade_id = str(trade_id or "").strip()
    symbol = str(symbol or "").upper().strip()
    side_norm = str(side or "").title().strip()  # "Buy" / "Sell"
    account_label = str(account_label or "main").strip()
    strategy = str(strategy or "unknown_strategy").strip()
    mode = str(mode or "PAPER").strip()

    entry_price_f = _safe_price(entry_price)
    qty_f = _safe_float(qty)

    if not trade_id or not symbol or qty_f <= 0 or entry_price_f <= 0:
        log.warning(
            "Refusing to open PAPER position: invalid inputs trade_id=%r symbol=%r qty=%r entry_price=%r",
            trade_id,
            symbol,
            qty,
            entry_price,
        )
        return

    ts_open = ts_ms if ts_ms is not None else _now_ms()

    positions = _load_positions()
    if trade_id in positions:
        log.warning(
            "PAPER trade_id %s already exists; overwriting existing position.",
            trade_id,
        )

    pos: Dict[str, Any] = {
        "trade_id": trade_id,
        "symbol": symbol,
        "side": side_norm,
        "qty": qty_f,
        "entry_price": entry_price_f,
        "entry_ts_ms": ts_open,
        "account_label": account_label,
        "strategy": strategy,
        "mode": mode,
        "sub_uid": str(sub_uid) if sub_uid is not None else None,
        "stop_price": _safe_price(stop_price) if stop_price is not None else None,
        "tp_price": _safe_price(tp_price) if tp_price is not None else None,
        "risk_usd": _safe_float(risk_usd) if risk_usd is not None else None,
        "meta": meta or {},
    }

    positions[trade_id] = pos
    _save_positions(positions)

    # Audit log
    try:
        row = {
            "ts_ms": ts_open,
            "event": "open",
            "trade_id": trade_id,
            "symbol": symbol,
            "side": side_norm,
            "qty": qty_f,
            "entry_price": entry_price_f,
            "account_label": account_label,
            "strategy": strategy,
            "mode": mode,
        }
        _append_trade_log(row)
    except Exception:
        pass

    log.info(
        "Opened PAPER position trade_id=%s symbol=%s side=%s qty=%s entry=%s account_label=%s strategy=%s",
        trade_id,
        symbol,
        side_norm,
        qty_f,
        entry_price_f,
        account_label,
        strategy,
    )


def close_paper_position(
    trade_id: str,
    *,
    exit_price: float,
    exit_reason: str = "manual_close",
    ts_ms: Optional[int] = None,
) -> None:
    """
    Manually close a PAPER position by trade_id and emit OutcomeRecord.

    Typical caller:
        - Manual interventions / testing.
        - Future scripts that decide to flatten PAPER trades at time-based exits.
    """
    trade_id = str(trade_id or "").strip()
    if not trade_id:
        return

    positions = _load_positions()
    pos = positions.pop(trade_id, None)
    if pos is None:
        log.warning("close_paper_position called for unknown trade_id=%s", trade_id)
        return

    _save_positions(positions)

    exit_price_f = _safe_price(exit_price)
    _emit_outcome_for_closed_position(
        pos,
        exit_price=exit_price_f,
        exit_reason=exit_reason,
        ts_ms=ts_ms,
    )


def on_price_tick(
    *,
    symbol: str,
    price: float,
    ts_ms: Optional[int] = None,
) -> None:
    """
    Main entrypoint for live market ticks.

    This is wired from WS Switchboard:

        paper_broker.on_price_tick(symbol=symbol, price=mid_or_trade, ts_ms=ts)

    Behavior:
        - Load all open PAPER positions.
        - For each position with matching symbol:
            • Check SL hit
            • Check TP hit
        - Close positions that triggered.
        - Emit OutcomeRecords for each closed trade.

    For now:
        - Single TP / single SL per position (enough for v1).
        - Full close on hit (no partials).
    """
    symbol = str(symbol or "").upper().strip()
    price_f = _safe_price(price)
    if not symbol or price_f <= 0:
        return

    ts = ts_ms if ts_ms is not None else _now_ms()

    positions = _load_positions()
    if not positions:
        return

    changed = False

    # Iterate over a list() copy so we can safely mutate positions dict
    for trade_id, pos in list(positions.items()):
        try:
            if str(pos.get("symbol") or "").upper() != symbol:
                continue

            side = str(pos.get("side") or "").title()
            sl = pos.get("stop_price")
            tp = pos.get("tp_price")

            exit_reason: Optional[str] = None
            exit_price_f: float = price_f

            if side == "Buy":
                # SL: price <= stop
                if isinstance(sl, (int, float)) and _safe_price(sl) > 0 and price_f <= _safe_price(sl):
                    exit_reason = "sl_hit"
                    exit_price_f = _safe_price(sl)
                # TP: price >= tp
                elif isinstance(tp, (int, float)) and _safe_price(tp) > 0 and price_f >= _safe_price(tp):
                    exit_reason = "tp_hit"
                    exit_price_f = _safe_price(tp)

            elif side == "Sell":
                # SL for shorts: price >= stop
                if isinstance(sl, (int, float)) and _safe_price(sl) > 0 and price_f >= _safe_price(sl):
                    exit_reason = "sl_hit"
                    exit_price_f = _safe_price(sl)
                # TP for shorts: price <= tp
                elif isinstance(tp, (int, float)) and _safe_price(tp) > 0 and price_f <= _safe_price(tp):
                    exit_reason = "tp_hit"
                    exit_price_f = _safe_price(tp)

            # No trigger → keep open
            if not exit_reason:
                continue

            # Remove from open positions and emit outcome
            positions.pop(trade_id, None)
            changed = True

            _emit_outcome_for_closed_position(
                pos,
                exit_price=exit_price_f,
                exit_reason=exit_reason,
                ts_ms=ts,
            )

            log.info(
                "Closed PAPER position trade_id=%s symbol=%s reason=%s exit_price=%s",
                trade_id,
                symbol,
                exit_reason,
                exit_price_f,
            )

        except Exception as e:
            log.warning(
                "Error processing PAPER tick for trade_id=%s symbol=%s: %r",
                trade_id,
                symbol,
                e,
            )

    if changed:
        _save_positions(positions)


def list_open_positions() -> Dict[str, Dict[str, Any]]:
    """
    Utility helper for debugging / introspection.

    Returns the current open PAPER positions dict, keyed by trade_id.
    """
    return _load_positions()
